from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .company_registry import normalize_company_key
from .storage import ControlPlaneStore


def _normalize_string_list(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
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


def _run_sync_step(callback: Callable[[], Any]) -> dict[str, Any]:
    try:
        return {
            "status": "completed",
            "result": callback(),
        }
    except Exception as exc:
        return {
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
        }


def _summarize_sync_status(sync_status: dict[str, Any]) -> str:
    for key, value in dict(sync_status or {}).items():
        if key == "overall_status" or not isinstance(value, dict):
            continue
        if str(value.get("status") or "") == "failed":
            return "partial_failure"
    return "completed"


def _aggregate_snapshot_step(entries: list[dict[str, Any]]) -> dict[str, Any]:
    if not entries:
        return {"status": "skipped", "results": []}
    failed_entries = [item for item in entries if str(item.get("status") or "") == "failed"]
    payload = {
        "status": "failed" if failed_entries else "completed",
        "results": entries,
    }
    if failed_entries:
        first_error = str(failed_entries[0].get("error") or "").strip()
        if first_error:
            payload["error"] = first_error
    return payload


def sync_company_asset_registration(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    target_company: str,
    snapshot_id: str,
    asset_view: str = "canonical_merged",
    company_key: str = "",
    registry_summary: dict[str, Any] | None = None,
    source_path: str = "",
    source_job_id: str = "",
    authoritative: bool = True,
    selected_snapshot_ids: list[str] | None = None,
    registry_refresh_mode: str = "guarded_upsert",
    refresh_company_identity: bool = True,
) -> dict[str, Any]:
    from .asset_reuse_planning import (
        backfill_organization_asset_registry_for_company,
        build_organization_asset_registry_record,
        ensure_acquisition_shard_registry_for_snapshot,
        upsert_organization_asset_registry_with_guard,
    )
    from .company_registry import refresh_company_identity_registry
    from .organization_assets import (
        ensure_acquisition_shard_bundles_for_snapshot,
        ensure_organization_completeness_ledger,
    )

    normalized_target_company = str(target_company or "").strip()
    normalized_company_key = str(company_key or "").strip() or normalize_company_key(normalized_target_company)
    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"

    sync_status: dict[str, Any] = {}
    selected_ids = _normalize_string_list(selected_snapshot_ids or [normalized_snapshot_id])

    if registry_summary:
        registry_record = build_organization_asset_registry_record(
            target_company=normalized_target_company,
            company_key=normalized_company_key,
            snapshot_id=normalized_snapshot_id,
            asset_view=normalized_asset_view,
            summary=dict(registry_summary or {}),
            source_path=str(source_path or ""),
            source_job_id=str(source_job_id or ""),
            authoritative=bool(authoritative),
        )
        selected_ids = _normalize_string_list(
            selected_ids
            or registry_record.get("selected_snapshot_ids")
            or [normalized_snapshot_id]
        )
        if registry_refresh_mode == "force_upsert":
            sync_status["organization_asset_registry_refresh"] = _run_sync_step(
                lambda: store.upsert_organization_asset_registry(
                    registry_record,
                    authoritative=bool(authoritative),
                )
            )
        else:
            sync_status["organization_asset_registry_refresh"] = _run_sync_step(
                lambda: upsert_organization_asset_registry_with_guard(
                    store=store,
                    candidate_record=registry_record,
                )
            )
    else:
        sync_status["organization_asset_registry_refresh"] = _run_sync_step(
            lambda: backfill_organization_asset_registry_for_company(
                runtime_dir=runtime_dir,
                store=store,
                target_company=normalized_target_company,
                asset_view=normalized_asset_view,
            )
        )
        registry_refresh_result = dict(sync_status["organization_asset_registry_refresh"].get("result") or {})
        selected_record = dict(registry_refresh_result.get("selected_record") or {})
        selected_ids = _normalize_string_list(
            selected_ids
            or selected_record.get("selected_snapshot_ids")
            or [selected_record.get("snapshot_id"), normalized_snapshot_id]
        )
        if not normalized_snapshot_id:
            normalized_snapshot_id = str(
                registry_refresh_result.get("selected_snapshot_id")
                or selected_record.get("snapshot_id")
                or (selected_ids[0] if selected_ids else "")
            ).strip()

    if not selected_ids and normalized_snapshot_id:
        selected_ids = [normalized_snapshot_id]

    if refresh_company_identity:
        sync_status["company_identity_registry_refresh"] = _run_sync_step(
            lambda: refresh_company_identity_registry(
                runtime_dir=runtime_dir,
                companies=[normalized_target_company, normalized_company_key],
            )
        )
    else:
        sync_status["company_identity_registry_refresh"] = {
            "status": "skipped",
            "reason": "caller_handles_company_identity_refresh",
        }

    shard_registry_entries: list[dict[str, Any]] = []
    shard_bundle_entries: list[dict[str, Any]] = []
    for selected_id in selected_ids:
        if not str(selected_id or "").strip():
            continue
        shard_registry_entries.append(
            {
                "snapshot_id": selected_id,
                **_run_sync_step(
                    lambda selected_id=selected_id: ensure_acquisition_shard_registry_for_snapshot(
                        runtime_dir=runtime_dir,
                        store=store,
                        target_company=normalized_target_company,
                        snapshot_id=str(selected_id),
                    )
                ),
            }
        )
        shard_bundle_entries.append(
            {
                "snapshot_id": selected_id,
                **_run_sync_step(
                    lambda selected_id=selected_id: ensure_acquisition_shard_bundles_for_snapshot(
                        runtime_dir=runtime_dir,
                        store=store,
                        target_company=normalized_target_company,
                        snapshot_id=str(selected_id),
                        asset_view=normalized_asset_view,
                    )
                ),
            }
        )

    sync_status["acquisition_shard_registry_refresh"] = _aggregate_snapshot_step(shard_registry_entries)
    sync_status["acquisition_shard_bundle_refresh"] = _aggregate_snapshot_step(shard_bundle_entries)

    primary_snapshot_id = normalized_snapshot_id or (selected_ids[0] if selected_ids else "")
    if primary_snapshot_id:
        sync_status["organization_completeness_ledger_refresh"] = _run_sync_step(
            lambda: ensure_organization_completeness_ledger(
                runtime_dir=runtime_dir,
                store=store,
                target_company=normalized_target_company,
                snapshot_id=primary_snapshot_id,
                asset_view=normalized_asset_view,
                selected_snapshot_ids=selected_ids,
            )
        )
        ledger_result = dict(sync_status["organization_completeness_ledger_refresh"].get("result") or {})
        ledger_sync_status = dict(ledger_result.get("sync_status") or {})
        execution_profile_refresh = dict(ledger_sync_status.get("organization_execution_profile_refresh") or {})
        if execution_profile_refresh:
            sync_status["organization_execution_profile_refresh"] = execution_profile_refresh
    else:
        sync_status["organization_completeness_ledger_refresh"] = {
            "status": "skipped",
            "reason": "missing_snapshot_id",
        }
        sync_status["organization_execution_profile_refresh"] = {
            "status": "skipped",
            "reason": "missing_snapshot_id",
        }

    sync_status.setdefault(
        "organization_execution_profile_refresh",
        {
            "status": "skipped",
            "reason": "organization_completeness_ledger_refresh_missing",
        },
    )
    sync_status["overall_status"] = _summarize_sync_status(sync_status)
    return {
        "status": "completed" if sync_status["overall_status"] == "completed" else "partial_failure",
        "target_company": normalized_target_company,
        "company_key": normalized_company_key,
        "snapshot_id": primary_snapshot_id,
        "asset_view": normalized_asset_view,
        "selected_snapshot_ids": selected_ids,
        "sync_status": sync_status,
        "explanation": {
            "source": "asset_registration_sync",
            "runtime_dir": str(Path(runtime_dir)),
            "registry_refresh_mode": registry_refresh_mode,
            "refreshes_company_identity_registry": bool(refresh_company_identity),
            "refreshes_organization_asset_registry": True,
            "refreshes_shard_registries": True,
            "refreshes_completeness_ledger": True,
            "refreshes_organization_execution_profile": True,
        },
    }
