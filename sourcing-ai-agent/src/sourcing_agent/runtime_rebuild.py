from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .asset_paths import (
    iter_company_asset_snapshot_dirs,
    load_company_snapshot_identity,
    normalize_company_key,
    resolve_company_snapshot_dir,
    resolve_snapshot_serving_artifact_path,
)
from .asset_reuse_planning import ensure_acquisition_shard_registry_for_snapshot
from .candidate_artifacts import build_company_candidate_artifacts
from .organization_assets import ensure_organization_completeness_ledger
from .request_matching import request_signature
from .retrieval_runtime import candidate_source_is_snapshot_authoritative
from .search_seed_registry import backfill_search_seed_lane_assets
from .storage import SQLiteStore

_SUPPORTED_ASSET_VIEWS = ("canonical_merged", "strict_roster_only")
_TERMINAL_JOB_STATUSES = {"completed", "failed", "superseded", "cancelled"}


def _snapshot_company_filter_keys(snapshot_dir: Path, identity_payload: dict[str, Any]) -> set[str]:
    normalized_keys: set[str] = set()
    for value in (
        identity_payload.get("company_key"),
        identity_payload.get("linkedin_slug"),
        snapshot_dir.parent.name,
    ):
        normalized_value = normalize_company_key(value)
        if normalized_value:
            normalized_keys.add(normalized_value)
    for value in list(identity_payload.get("aliases") or []):
        normalized_value = normalize_company_key(value)
        if normalized_value:
            normalized_keys.add(normalized_value)
    return normalized_keys


def rebuild_runtime_control_plane(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    companies: list[str] | None = None,
    snapshot_id: str = "",
    rebuild_missing_artifacts: bool = True,
    rebuild_company_assets: bool = True,
    rebuild_jobs: bool = True,
) -> dict[str, Any]:
    runtime_root = Path(runtime_dir).expanduser()
    normalized_companies = [str(item or "").strip() for item in list(companies or []) if str(item or "").strip()]
    company_asset_result = {
        "status": "skipped",
        "reason": "disabled",
    }
    if rebuild_company_assets:
        company_asset_result = rebuild_runtime_company_asset_control_plane(
            runtime_dir=runtime_root,
            store=store,
            companies=normalized_companies or None,
            snapshot_id=snapshot_id,
            rebuild_missing_artifacts=rebuild_missing_artifacts,
        )

    job_result = {
        "status": "skipped",
        "reason": "disabled",
    }
    if rebuild_jobs:
        job_result = rebuild_runtime_jobs_control_plane(
            runtime_dir=runtime_root,
            store=store,
        )

    status = "completed"
    if str(company_asset_result.get("status") or "").startswith("completed_with_errors") or str(
        job_result.get("status") or ""
    ).startswith("completed_with_errors"):
        status = "completed_with_errors"
    return {
        "status": status,
        "runtime_dir": str(runtime_root),
        "companies": normalized_companies,
        "snapshot_id": str(snapshot_id or "").strip(),
        "company_assets": company_asset_result,
        "jobs": job_result,
    }


def rebuild_runtime_company_asset_control_plane(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    companies: list[str] | None = None,
    snapshot_id: str = "",
    rebuild_missing_artifacts: bool = True,
) -> dict[str, Any]:
    runtime_root = Path(runtime_dir).expanduser()
    normalized_company_filters = {
        normalize_company_key(item)
        for item in list(companies or [])
        if normalize_company_key(item)
    }
    requested_snapshot_id = str(snapshot_id or "").strip()
    repair_result: dict[str, Any] = {
        "status": "disabled" if not rebuild_missing_artifacts else "completed",
        "repair_mode": "lazy_on_demand",
        "repair_candidate_count": 0,
        "repaired_snapshot_count": 0,
        "missing_candidate_documents_count": 0,
        "errors": [],
    }

    scanned_snapshot_count = 0
    rebuilt_view_count = 0
    registered_generation_count = 0
    search_seed_lane_backfill_count = 0
    acquisition_shard_registry_sync_count = 0
    errors: list[dict[str, Any]] = []
    company_summaries: list[dict[str, Any]] = []

    for snapshot_dir in iter_company_asset_snapshot_dirs(runtime_root, prefer_hot_cache=False):
        normalized_snapshot_id = str(snapshot_dir.name or "").strip()
        if requested_snapshot_id and normalized_snapshot_id != requested_snapshot_id:
            continue
        identity_payload = load_company_snapshot_identity(snapshot_dir, fallback_payload={})
        candidate_company_names = [
            str(identity_payload.get("company_key") or "").strip(),
            str(identity_payload.get("canonical_name") or "").strip(),
            str(identity_payload.get("requested_name") or "").strip(),
            str(snapshot_dir.parent.name or "").strip(),
        ]
        normalized_identity_keys = _snapshot_company_filter_keys(snapshot_dir, identity_payload)
        if normalized_company_filters and not normalized_identity_keys.intersection(normalized_company_filters):
            continue
        target_company = next((item for item in candidate_company_names if item), snapshot_dir.parent.name)
        scanned_snapshot_count += 1
        company_summary: dict[str, Any] = {
            "target_company": target_company,
            "company_key": str(identity_payload.get("company_key") or normalize_company_key(target_company)),
            "snapshot_id": normalized_snapshot_id,
            "views": [],
        }
        search_seed_backfill = backfill_search_seed_lane_assets(snapshot_dir)
        company_summary["search_seed_lane_backfill"] = search_seed_backfill
        if str(search_seed_backfill.get("status") or "").strip() == "backfilled":
            search_seed_lane_backfill_count += 1
        try:
            shard_registry_sync: dict[str, Any] = ensure_acquisition_shard_registry_for_snapshot(
                runtime_dir=runtime_root,
                store=store,
                target_company=target_company,
                snapshot_id=normalized_snapshot_id,
            )
        except Exception as exc:  # pragma: no cover - defensive guard for live data repair
            shard_registry_sync = {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }
            errors.append(
                {
                    "target_company": target_company,
                    "snapshot_id": normalized_snapshot_id,
                    "error": f"search_seed_registry_sync_failed: {type(exc).__name__}: {exc}",
                }
            )
        company_summary["acquisition_shard_registry_sync"] = shard_registry_sync
        if int(dict(shard_registry_sync or {}).get("shard_records") or 0) > 0:
            acquisition_shard_registry_sync_count += 1
        supported_views = _supported_snapshot_asset_views(snapshot_dir)
        supported_views, snapshot_repair = _repair_snapshot_artifacts_if_needed(
            runtime_dir=runtime_root,
            snapshot_dir=snapshot_dir,
            store=store,
            target_company=target_company,
            rebuild_missing_artifacts=rebuild_missing_artifacts,
            supported_views=supported_views,
        )
        if snapshot_repair.get("attempted"):
            repair_result["repair_candidate_count"] = int(repair_result.get("repair_candidate_count") or 0) + 1
        if str(snapshot_repair.get("status") or "") == "repaired":
            repair_result["repaired_snapshot_count"] = int(repair_result.get("repaired_snapshot_count") or 0) + 1
        if str(snapshot_repair.get("status") or "") == "missing_candidate_documents":
            repair_result["missing_candidate_documents_count"] = int(
                repair_result.get("missing_candidate_documents_count") or 0
            ) + 1
        if snapshot_repair.get("error"):
            repair_error = {
                "target_company": target_company,
                "snapshot_id": normalized_snapshot_id,
                "error": str(snapshot_repair.get("error") or ""),
            }
            repair_result.setdefault("errors", []).append(repair_error)
            errors.append(repair_error)
        if not supported_views:
            continue
        for asset_view in supported_views:
            existing_generation = store.get_asset_materialization_generation(
                target_company=target_company,
                snapshot_id=normalized_snapshot_id,
                asset_view=asset_view,
                artifact_kind="organization_asset",
                artifact_key=asset_view,
            )
            try:
                rebuild_result = ensure_organization_completeness_ledger(
                    runtime_dir=runtime_root,
                    store=store,
                    target_company=target_company,
                    snapshot_id=normalized_snapshot_id,
                    asset_view=asset_view,
                )
            except Exception as exc:  # pragma: no cover - defensive guard for live data repair
                errors.append(
                    {
                        "target_company": target_company,
                        "snapshot_id": normalized_snapshot_id,
                        "asset_view": asset_view,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            rebuilt_view_count += 1
            generation = store.get_asset_materialization_generation(
                target_company=target_company,
                snapshot_id=normalized_snapshot_id,
                asset_view=asset_view,
                artifact_kind="organization_asset",
                artifact_key=asset_view,
            )
            _align_snapshot_registry_generation(
                store=store,
                target_company=target_company,
                snapshot_id=normalized_snapshot_id,
                asset_view=asset_view,
                generation=generation,
            )
            if not existing_generation and generation:
                registered_generation_count += 1
            company_summary["views"].append(
                {
                    "asset_view": asset_view,
                    "status": str(rebuild_result.get("status") or "completed"),
                    "generation_key": str((generation or {}).get("generation_key") or ""),
                    "membership_summary": dict(
                        rebuild_result.get("organization_asset", {}).get("exact_membership_summary")
                        or rebuild_result.get("registry_refresh", {}).get("result", {})
                        or {}
                    ),
                }
            )
        for view_summary in list(company_summary.get("views") or []):
            finalized_generation = store.get_asset_materialization_generation(
                target_company=target_company,
                snapshot_id=normalized_snapshot_id,
                asset_view=str(view_summary.get("asset_view") or "").strip() or "canonical_merged",
                artifact_kind="organization_asset",
                artifact_key=str(view_summary.get("asset_view") or "").strip() or "canonical_merged",
            )
            _align_snapshot_registry_generation(
                store=store,
                target_company=target_company,
                snapshot_id=normalized_snapshot_id,
                asset_view=str(view_summary.get("asset_view") or "").strip() or "canonical_merged",
                generation=finalized_generation,
            )
        if company_summary["views"]:
            company_summaries.append(company_summary)

    for company_summary in company_summaries:
        target_company = str(company_summary.get("target_company") or "").strip()
        normalized_snapshot_id = str(company_summary.get("snapshot_id") or "").strip()
        for view_summary in list(company_summary.get("views") or []):
            asset_view = str(view_summary.get("asset_view") or "").strip() or "canonical_merged"
            finalized_generation = store.get_asset_materialization_generation(
                target_company=target_company,
                snapshot_id=normalized_snapshot_id,
                asset_view=asset_view,
                artifact_kind="organization_asset",
                artifact_key=asset_view,
            )
            _align_snapshot_registry_generation(
                store=store,
                target_company=target_company,
                snapshot_id=normalized_snapshot_id,
                asset_view=asset_view,
                generation=finalized_generation,
            )

    if repair_result.get("errors"):
        repair_result["status"] = "completed_with_errors"
    status = "completed_with_errors" if errors else "completed"
    return {
        "status": status,
        "runtime_dir": str(runtime_root),
        "company_filters": list(companies or []),
        "snapshot_id": requested_snapshot_id,
        "rebuild_missing_artifacts": rebuild_missing_artifacts,
        "repair_result": repair_result,
        "scanned_snapshot_count": scanned_snapshot_count,
        "rebuilt_view_count": rebuilt_view_count,
        "registered_generation_count": registered_generation_count,
        "search_seed_lane_backfill_count": search_seed_lane_backfill_count,
        "acquisition_shard_registry_sync_count": acquisition_shard_registry_sync_count,
        "errors": errors,
        "companies": company_summaries,
    }


def rebuild_runtime_jobs_control_plane(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
) -> dict[str, Any]:
    runtime_root = Path(runtime_dir).expanduser()
    jobs_dir = runtime_root / "jobs"
    if not jobs_dir.exists():
        return {
            "status": "missing_runtime_jobs",
            "runtime_dir": str(runtime_root),
            "scanned_job_file_count": 0,
            "restored_job_count": 0,
            "restored_result_view_count": 0,
            "errors": [],
        }

    scanned_job_file_count = 0
    restored_job_count = 0
    restored_result_view_count = 0
    errors: list[dict[str, Any]] = []

    for job_path in sorted(jobs_dir.glob("*.json")):
        job_name = str(job_path.name or "")
        if job_name.endswith(".preview.json"):
            continue
        if job_name.startswith("tmp_"):
            continue
        scanned_job_file_count += 1
        try:
            payload = json.loads(job_path.read_text(encoding="utf-8"))
        except Exception as exc:  # pragma: no cover - live runtime defensive guard
            errors.append({"job_path": str(job_path), "error": f"{type(exc).__name__}: {exc}"})
            continue
        if not isinstance(payload, dict):
            continue
        job_id = str(payload.get("job_id") or job_path.stem).strip()
        if not job_id:
            continue
        request_payload = _resolved_job_request_payload(payload)
        if not request_payload:
            continue
        summary_payload = dict(payload.get("summary") or {})
        plan_payload = dict(payload.get("plan") or {})
        execution_bundle_payload = dict(payload.get("execution_bundle") or {})
        candidate_source = _resolved_candidate_source(payload)
        resolved_source_path = _resolved_runtime_job_source_path(
            runtime_dir=runtime_root,
            request_payload=request_payload,
            candidate_source=candidate_source,
        )
        artifact_path = (
            resolved_source_path
            or str(payload.get("artifact_path") or "").strip()
            or str(candidate_source.get("source_path") or "").strip()
        )
        status = str(payload.get("status") or "completed").strip() or "completed"
        store.save_job(
            job_id=job_id,
            job_type=_infer_runtime_job_type(payload),
            status=status,
            stage=_infer_runtime_job_stage(payload),
            request_payload=request_payload,
            plan_payload=plan_payload,
            execution_bundle_payload=execution_bundle_payload,
            summary_payload=summary_payload,
            artifact_path=artifact_path,
        )
        restored_job_count += 1
        result_view = _persist_runtime_job_result_view(
            runtime_dir=runtime_root,
            store=store,
            job_id=job_id,
            request_payload=request_payload,
            summary_payload=summary_payload,
            candidate_source=candidate_source,
            resolved_source_path=resolved_source_path,
        )
        if result_view:
            restored_result_view_count += 1

    status = "completed_with_errors" if errors else "completed"
    return {
        "status": status,
        "runtime_dir": str(runtime_root),
        "scanned_job_file_count": scanned_job_file_count,
        "restored_job_count": restored_job_count,
        "restored_result_view_count": restored_result_view_count,
        "errors": errors,
        "frontend_history_links_rebuilt": False,
        "frontend_history_links_reason": "history_id is not persisted in runtime/jobs payloads",
    }


def _supported_snapshot_asset_views(snapshot_dir: Path) -> list[str]:
    supported: list[str] = []
    for asset_view in _SUPPORTED_ASSET_VIEWS:
        artifact_path = resolve_snapshot_serving_artifact_path(
            snapshot_dir,
            asset_view=asset_view,
            allow_candidate_documents_fallback=asset_view == "canonical_merged",
        )
        if artifact_path is not None:
            supported.append(asset_view)
    return supported


def _repair_snapshot_artifacts_if_needed(
    *,
    runtime_dir: Path,
    snapshot_dir: Path,
    store: SQLiteStore,
    target_company: str,
    rebuild_missing_artifacts: bool,
    supported_views: list[str],
) -> tuple[list[str], dict[str, Any]]:
    if supported_views:
        return supported_views, {"status": "skipped_existing", "attempted": False}
    if not rebuild_missing_artifacts:
        return supported_views, {"status": "disabled", "attempted": False}

    candidate_doc_path = snapshot_dir / "candidate_documents.json"
    if not candidate_doc_path.exists():
        return supported_views, {"status": "missing_candidate_documents", "attempted": False}

    try:
        build_company_candidate_artifacts(
            runtime_dir=runtime_dir,
            store=store,
            target_company=target_company,
            snapshot_id=snapshot_dir.name,
        )
    except Exception as exc:  # pragma: no cover - defensive guard for live data repair
        return supported_views, {"status": "repair_failed", "attempted": True, "error": f"{type(exc).__name__}: {exc}"}

    repaired_views = _supported_snapshot_asset_views(snapshot_dir)
    if repaired_views:
        return repaired_views, {"status": "repaired", "attempted": True}
    return repaired_views, {
        "status": "repair_completed_without_supported_views",
        "attempted": True,
        "error": "Supported asset views remain unavailable after candidate artifact rebuild.",
    }


def _align_snapshot_registry_generation(
    *,
    store: SQLiteStore,
    target_company: str,
    snapshot_id: str,
    asset_view: str,
    generation: dict[str, Any] | None,
) -> None:
    generation_payload = dict(generation or {})
    generation_key = str(generation_payload.get("generation_key") or "").strip()
    if not generation_key:
        return
    rows = store.list_organization_asset_registry(
        target_company=target_company,
        asset_view=asset_view,
        limit=500,
    )
    row: dict[str, Any] = next(
        (item for item in rows if str(item.get("snapshot_id") or "").strip() == snapshot_id),
        {},
    )
    if not row:
        return
    if str(row.get("materialization_generation_key") or "").strip() == generation_key:
        return
    updated_row = dict(row)
    updated_summary = dict(updated_row.get("summary") or {})
    updated_summary["materialization_generation_key"] = generation_key
    updated_summary["materialization_generation_sequence"] = int(generation_payload.get("generation_sequence") or 0)
    updated_summary["materialization_watermark"] = str(generation_payload.get("generation_watermark") or "").strip()
    updated_row["materialization_generation_key"] = generation_key
    updated_row["materialization_generation_sequence"] = int(generation_payload.get("generation_sequence") or 0)
    updated_row["materialization_watermark"] = str(generation_payload.get("generation_watermark") or "").strip()
    updated_row["summary"] = updated_summary
    store.upsert_organization_asset_registry(updated_row, authoritative=bool(updated_row.get("authoritative")))


def _resolved_job_request_payload(payload: dict[str, Any]) -> dict[str, Any]:
    request_payload = dict(payload.get("effective_request") or payload.get("request") or {})
    if request_payload:
        return request_payload
    request_preview = dict(payload.get("request_preview") or {})
    request_view = dict(request_preview.get("request_view") or {})
    return request_view


def _resolved_candidate_source(payload: dict[str, Any]) -> dict[str, Any]:
    summary_payload = dict(payload.get("summary") or {})
    source_payload = dict(payload.get("candidate_source") or summary_payload.get("candidate_source") or {})
    return source_payload


def _infer_runtime_job_type(payload: dict[str, Any]) -> str:
    normalized = str(payload.get("job_type") or "").strip()
    if normalized:
        return normalized
    if payload.get("workflow_stage_summaries") or payload.get("runtime_controls"):
        return "workflow"
    return "retrieval"


def _infer_runtime_job_stage(payload: dict[str, Any]) -> str:
    normalized = str(payload.get("stage") or "").strip()
    if normalized:
        return normalized
    status = str(payload.get("status") or "").strip().lower()
    if status == "preview_ready":
        return "planning"
    if status in _TERMINAL_JOB_STATUSES:
        return "completed"
    if status in {"queued", "accepted"}:
        return "queued"
    if status in {"running", "retrieving", "executing"}:
        return "retrieving"
    return "completed" if status else "unknown"


def _resolved_runtime_job_source_path(
    *,
    runtime_dir: Path,
    request_payload: dict[str, Any],
    candidate_source: dict[str, Any],
) -> str:
    if not candidate_source_is_snapshot_authoritative(candidate_source):
        return str(candidate_source.get("source_path") or "").strip()
    target_company = str(request_payload.get("target_company") or "").strip()
    snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
    asset_view = str(candidate_source.get("asset_view") or request_payload.get("asset_view") or "canonical_merged").strip()
    if not target_company or not snapshot_id:
        return str(candidate_source.get("source_path") or "").strip()
    snapshot_dir = resolve_company_snapshot_dir(runtime_dir, target_company=target_company, snapshot_id=snapshot_id)
    if snapshot_dir is None:
        return str(candidate_source.get("source_path") or "").strip()
    resolved = resolve_snapshot_serving_artifact_path(
        snapshot_dir,
        asset_view=asset_view,
        allow_candidate_documents_fallback=asset_view == "canonical_merged",
    )
    return str(resolved or candidate_source.get("source_path") or "").strip()


def _persist_runtime_job_result_view(
    *,
    runtime_dir: Path,
    store: SQLiteStore,
    job_id: str,
    request_payload: dict[str, Any],
    summary_payload: dict[str, Any],
    candidate_source: dict[str, Any],
    resolved_source_path: str,
) -> dict[str, Any]:
    source_kind = str(candidate_source.get("source_kind") or "").strip()
    if not source_kind:
        return {}
    target_company = str(request_payload.get("target_company") or "").strip()
    asset_view = str(candidate_source.get("asset_view") or request_payload.get("asset_view") or "canonical_merged").strip()
    snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
    authoritative_snapshot_id = str(candidate_source.get("authoritative_snapshot_id") or "").strip()
    materialization_generation_key = str(candidate_source.get("materialization_generation_key") or "").strip()
    if target_company and candidate_source_is_snapshot_authoritative(candidate_source):
        registry_row = store.get_authoritative_organization_asset_registry(
            target_company=target_company,
            asset_view=asset_view,
        )
        if not authoritative_snapshot_id:
            authoritative_snapshot_id = str(registry_row.get("snapshot_id") or "").strip() or snapshot_id
        if not materialization_generation_key:
            materialization_generation_key = str(registry_row.get("materialization_generation_key") or "").strip()
    summary_candidate_source = dict(summary_payload.get("candidate_source") or {})
    candidate_count = (
        int(summary_candidate_source.get("candidate_count") or 0)
        or int(candidate_source.get("candidate_count") or 0)
        or int(candidate_source.get("unfiltered_candidate_count") or 0)
        or int(summary_payload.get("returned_matches") or 0)
    )
    return store.upsert_job_result_view(
        job_id=job_id,
        target_company=target_company,
        source_kind=source_kind,
        view_kind="asset_population" if candidate_source_is_snapshot_authoritative(candidate_source) else "ranked_results",
        snapshot_id=snapshot_id,
        asset_view=asset_view or "canonical_merged",
        source_path=resolved_source_path or str(candidate_source.get("source_path") or "").strip(),
        authoritative_snapshot_id=authoritative_snapshot_id,
        materialization_generation_key=materialization_generation_key,
        request_signature_value=request_signature(request_payload),
        summary={
            "candidate_count": candidate_count,
            "returned_matches": int(summary_payload.get("returned_matches") or 0),
            "total_matches": int(summary_payload.get("total_matches") or 0),
            "analysis_stage": str(summary_payload.get("analysis_stage") or "").strip(),
            "summary_provider": str(summary_payload.get("summary_provider") or "").strip(),
        },
        metadata={
            "rebuild_source": "runtime_jobs_json",
            "target_scope": str(request_payload.get("target_scope") or "").strip(),
            "analysis_stage_mode": str(request_payload.get("analysis_stage_mode") or "").strip(),
            "runtime_dir": str(runtime_dir),
        },
    )
