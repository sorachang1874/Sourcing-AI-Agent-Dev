"""Owner-neutral Public Web execution core.

CRM Public Web imports this implementation through `crm_public_web_runtime.py`.
The target-candidate surface is retired and may only observe fail-closed legacy
facades; it is not the physical owner of this shared runtime.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .durable_runtime import CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE
from .legacy_public_web_storage import list_legacy_target_public_web_runs
from .public_web_search import (
    DEFAULT_TARGET_CANDIDATE_SOURCE_FAMILIES,
    CandidateSearchOutcome,
    ClassifiedEntryLink,
    PublicWebCandidateContext,
    PublicWebExperimentOptions,
    PublicWebModelClient,
    PublicWebQuerySpec,
    adjudicate_candidate_public_web_experiment_from_document_fetch_payload,
    candidate_context_from_target_candidate,
    canonicalize_profile_link_type_from_url,
    execute_single_candidate_searches,
    fetch_candidate_public_web_experiment_documents,
    fetch_ready_specs_isolated,
    finalize_candidate_public_web_experiment_from_adjudication_payload,
    is_clean_profile_link,
    is_publishable_profile_link,
    normalize_public_web_url_key,
    plan_candidate_public_web_queries,
    prepare_candidate_search_plan,
    public_web_link_shape_warnings,
    public_web_query_identity_key,
    record_candidate_search_response,
    write_search_execution_artifacts,
)
from .public_web_signal_identity import (
    public_web_signal_id_for_identity,
    public_web_signal_identity_key,  # noqa: F401  (re-exported for downstream modules)
)
from .repositories import serving_projection_repo
from .search_provider import BaseSearchProvider

PUBLIC_WEB_JOB_TYPE = "target_candidate_public_web_search"
CRM_PUBLIC_WEB_JOB_TYPE = "crm_public_web_search"
CRM_PUBLIC_WEB_EXECUTION_BACKEND = "crm_public_web_v1"
TARGET_CANDIDATE_PUBLIC_WEB_EXECUTION_BACKEND = "target_candidate_public_web_v1"
PUBLIC_WEB_WORKER_LANE = "exploration_specialist"
PUBLIC_WEB_WORKER_RECOVERY_KIND = "target_candidate_public_web_search"
CRM_PUBLIC_WEB_WORKER_RECOVERY_KIND = "crm_public_web_search"
PUBLIC_WEB_TERMINAL_STATUSES = {
    "completed",
    "completed_with_errors",
    "needs_review",
    "failed",
    "cancelled",
}

_PUBLIC_WEB_MODEL_USAGE_TOKEN_FIELDS = (
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cached_input_tokens",
    "reasoning_output_tokens",
)
_PUBLIC_WEB_MODEL_USAGE_TOKEN_LIMIT = 1_000_000_000

PUBLIC_WEB_RETRYABLE_TERMINAL_STATUSES = {
    "completed_with_errors",
    "needs_review",
    "failed",
    "cancelled",
}

LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS_ENV = "SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS"
LEGACY_TARGET_PUBLIC_WEB_MIGRATION_PHASE = "W7_target_candidate_public_web_execution_retired"


@dataclass(frozen=True)
class PublicWebRunOwner:
    storage_owner: str
    execution_backend: str
    job_type: str
    recovery_kind: str
    artifact_scope: str


TARGET_PUBLIC_WEB_OWNER = PublicWebRunOwner(
    storage_owner="legacy_target_candidate_public_web_v1",
    execution_backend=TARGET_CANDIDATE_PUBLIC_WEB_EXECUTION_BACKEND,
    job_type=PUBLIC_WEB_JOB_TYPE,
    recovery_kind=PUBLIC_WEB_WORKER_RECOVERY_KIND,
    artifact_scope="target_candidate_search",
)

CRM_PUBLIC_WEB_OWNER = PublicWebRunOwner(
    storage_owner="crm_public_web_v1",
    execution_backend=CRM_PUBLIC_WEB_EXECUTION_BACKEND,
    job_type=CRM_PUBLIC_WEB_JOB_TYPE,
    recovery_kind=CRM_PUBLIC_WEB_WORKER_RECOVERY_KIND,
    artifact_scope="crm_record_search",
)


def legacy_target_public_web_execution_enabled() -> bool:
    """Legacy target-candidate Public Web execution is permanently retired.

    The env var is retained only as report-visible historical context. It must
    not re-enable target-candidate owned execution after W7e; normal execution
    is CRM-owned through `crm_public_web_runtime.py`.
    """
    return False


def _is_legacy_target_public_web_owner(owner: PublicWebRunOwner) -> bool:
    return owner.storage_owner == TARGET_PUBLIC_WEB_OWNER.storage_owner


def _legacy_target_public_web_execution_disabled_result(
    *,
    operation: str,
    run_id: str = "",
    batch_id: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": "retired",
        "reason": "legacy_target_public_web_execution_disabled",
        "operation": str(operation or ""),
        "normal_path": False,
        "owner": TARGET_PUBLIC_WEB_OWNER.storage_owner,
        "execution_backend": TARGET_PUBLIC_WEB_OWNER.execution_backend,
        "canonical_owner": CRM_PUBLIC_WEB_OWNER.storage_owner,
        "canonical_execution_backend": CRM_PUBLIC_WEB_OWNER.execution_backend,
        "canonical_endpoint": "/api/crm/records/public-web-search",
        "migration_override_env": LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS_ENV,
        "migration_override_status": "removed",
        "retirement_mode": "permanent_hard_disable",
        "migration_phase": LEGACY_TARGET_PUBLIC_WEB_MIGRATION_PHASE,
        "report_visible": True,
    }
    if run_id:
        payload["run_id"] = str(run_id)
    if batch_id:
        payload["batch_id"] = str(batch_id)
    return payload


def start_target_candidate_public_web_batch(
    *,
    store: Any,
    target_candidates: list[dict[str, Any]],
    runtime_dir: str | Path,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        **_legacy_target_public_web_execution_disabled_result(operation="start_batch"),
        "batch": {},
        "runs": [],
        "summary": {
            "status": "retired",
            "requested_record_count": len(list(target_candidates or [])),
            "owner": TARGET_PUBLIC_WEB_OWNER.storage_owner,
            "execution_backend": TARGET_PUBLIC_WEB_OWNER.execution_backend,
        },
    }


def start_crm_public_web_batch(
    *,
    store: Any,
    crm_records: list[dict[str, Any]],
    runtime_dir: str | Path,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_payload = dict(payload or {})
    options = normalize_public_web_product_options(request_payload)
    force_refresh = bool(request_payload.get("force_refresh"))
    caller_refresh_nonce = str(request_payload.get("refresh_nonce") or request_payload.get("nonce") or "").strip()
    refresh_nonce = caller_refresh_nonce
    if force_refresh and not refresh_nonce:
        refresh_nonce = f"force-{utc_compact_timestamp()}-{uuid.uuid4().hex[:12]}"
    workspace_id = str(request_payload.get("workspace_id") or "default").strip() or "default"
    requested_by = str(request_payload.get("requested_by") or request_payload.get("user_id") or "").strip()
    request_metadata = dict(request_payload.get("metadata") or {})
    requested_record_ids = [
        str(record.get("crm_record_id") or record.get("record_id") or record.get("id") or "").strip()
        for record in crm_records
        if str(record.get("crm_record_id") or record.get("record_id") or record.get("id") or "").strip()
    ]
    idempotency_key = build_crm_public_web_batch_idempotency_key(
        workspace_id=workspace_id,
        requested_record_ids=requested_record_ids,
        options=options,
        force_refresh=force_refresh,
        nonce=refresh_nonce,
    )
    if not force_refresh or caller_refresh_nonce:
        existing = store.get_crm_public_web_batch(idempotency_key=idempotency_key)
        if existing is not None:
            runs = store.list_crm_public_web_runs(
                batch_id=str(existing.get("batch_id") or ""),
                workspace_id=workspace_id,
            )
            return {
                "status": "joined",
                "batch": existing,
                "runs": runs,
                "summary": summarize_public_web_runs(runs),
                "idempotency_key": idempotency_key,
            }

    batch_id = str(request_payload.get("batch_id") or "").strip()
    if not batch_id:
        if not force_refresh or caller_refresh_nonce:
            batch_id = f"crm-public-web-batch-{short_hash(idempotency_key)}"
        else:
            batch_id = f"crm-public-web-batch-{utc_compact_timestamp()}-{short_hash(idempotency_key)}"
    batch_artifact_root = Path(runtime_dir).expanduser() / "public_web" / CRM_PUBLIC_WEB_OWNER.artifact_scope / batch_id
    batch_artifact_root.mkdir(parents=True, exist_ok=True)
    source_families = list(options.source_families)
    created_runs: list[dict[str, Any]] = []
    for record in crm_records:
        context = candidate_context_from_target_candidate(record)
        if not context.record_id:
            continue
        record_person_identity_key = str(record.get("person_identity_key") or "").strip()
        run_idempotency_key = build_crm_public_web_run_idempotency_key(
            workspace_id=workspace_id,
            record_id=context.record_id,
            linkedin_url_key=context.linkedin_url_key,
            options=options,
            force_refresh=force_refresh,
            nonce=refresh_nonce,
        )
        existing_run = None if force_refresh else store.get_crm_public_web_run(idempotency_key=run_idempotency_key)
        if existing_run is not None:
            created_runs.append(existing_run)
            continue
        run_id = f"crm-public-web-run-{short_hash(run_idempotency_key)}"
        run_artifact_root = batch_artifact_root / "runs" / run_id
        run_artifact_root.mkdir(parents=True, exist_ok=True)
        query_manifest = [query.to_record() for query in _plan_queries_for_context(context, options=options)]
        created_runs.append(
            store.upsert_crm_public_web_run(
                {
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "crm_record_id": context.record_id,
                    "workspace_id": workspace_id,
                    "candidate_id": context.candidate_id,
                    "candidate_name": context.candidate_name,
                    "current_company": context.current_company,
                    "linkedin_url": context.linkedin_url,
                    "linkedin_url_key": context.linkedin_url_key,
                    "person_identity_key": record_person_identity_key or person_identity_key_for_context(context),
                    "idempotency_key": run_idempotency_key,
                    "status": "queued",
                    "phase": "queued",
                    "source_families": source_families,
                    "options": asdict(options),
                    "query_manifest": query_manifest,
                    "artifact_root": str(run_artifact_root),
                    "worker_key": public_web_worker_key(run_id),
                    "summary": {
                        "record_id": context.record_id,
                        "crm_record_id": context.record_id,
                        "candidate_name": context.candidate_name,
                        "current_company": context.current_company,
                        "linkedin_url_key": context.linkedin_url_key,
                        "owner": CRM_PUBLIC_WEB_OWNER.storage_owner,
                        "execution_backend": CRM_PUBLIC_WEB_OWNER.execution_backend,
                    },
                    "search_checkpoint": {
                        "stage": "queued",
                        "status": "queued",
                        "query_count": len(query_manifest),
                        "phase_metrics": {
                            "stage": "queued",
                            "status": "queued",
                            "submitted_task_count": 0,
                            "pending_task_count": 0,
                            "fetched_task_count": 0,
                        },
                    },
                    "metadata": {
                        **request_metadata,
                        "raw_asset_policy": (
                            "Raw HTML/PDF/search payloads are internal analysis inputs and are excluded "
                            "from default export packages."
                        ),
                        "reuse_policy": "v1 automatic reuse requires normalized LinkedIn URL key match.",
                        "owner": CRM_PUBLIC_WEB_OWNER.storage_owner,
                        "force_refresh": force_refresh,
                        "refresh_nonce": refresh_nonce,
                    },
                    "execution_backend": CRM_PUBLIC_WEB_OWNER.execution_backend,
                    "source_target_run_id": "",
                }
            )
        )

    summary = {
        **summarize_public_web_runs(created_runs),
        "owner": CRM_PUBLIC_WEB_OWNER.storage_owner,
        "execution_backend": CRM_PUBLIC_WEB_OWNER.execution_backend,
    }
    batch = store.upsert_crm_public_web_batch(
        {
            "batch_id": batch_id,
            "idempotency_key": idempotency_key,
            "workspace_id": workspace_id,
            "status": summary["status"],
            "requested_crm_record_ids": requested_record_ids,
            "source_families": source_families,
            "options": asdict(options),
            "run_ids": [str(run.get("run_id") or "") for run in created_runs if str(run.get("run_id") or "")],
            "summary": summary,
            "requested_by": requested_by,
            "force_refresh": force_refresh,
            "metadata": {
                **request_metadata,
                "workflow_boundary": "user_triggered_crm_public_web_search",
                "default_workflow_stage": "not_enabled",
                "owner": CRM_PUBLIC_WEB_OWNER.storage_owner,
                "execution_backend": CRM_PUBLIC_WEB_OWNER.execution_backend,
                "force_refresh": force_refresh,
                "refresh_nonce": refresh_nonce,
            },
            "execution_backend": CRM_PUBLIC_WEB_OWNER.execution_backend,
            "source_target_batch_id": "",
        }
    )
    return {
        "status": "queued",
        "batch": batch,
        "runs": created_runs,
        "summary": summary,
        "idempotency_key": idempotency_key,
    }


def sync_public_web_batch_summary(store: Any, batch_id: str) -> dict[str, Any]:
    return _legacy_target_public_web_execution_disabled_result(
        operation="sync_batch_summary",
        batch_id=str(batch_id or ""),
    )


def sync_crm_public_web_batch_summary(store: Any, batch_id: str, *, workspace_id: str = "default") -> dict[str, Any]:
    batch = store.get_crm_public_web_batch(batch_id=batch_id)
    if batch is None:
        return {"status": "not_found", "batch_id": batch_id}
    runs = store.list_crm_public_web_runs(batch_id=batch_id, workspace_id=workspace_id)
    summary = {
        **summarize_public_web_runs(runs),
        "owner": CRM_PUBLIC_WEB_OWNER.storage_owner,
        "execution_backend": CRM_PUBLIC_WEB_OWNER.execution_backend,
    }
    updated = store.upsert_crm_public_web_batch(
        {
            **batch,
            "status": summary["status"],
            "summary": summary,
            "run_ids": [str(run.get("run_id") or "") for run in runs if str(run.get("run_id") or "")],
            "execution_backend": CRM_PUBLIC_WEB_OWNER.execution_backend,
            "source_target_batch_id": "",
            "metadata": {
                **dict(batch.get("metadata") or {}),
                "owner": CRM_PUBLIC_WEB_OWNER.storage_owner,
                "execution_backend": CRM_PUBLIC_WEB_OWNER.execution_backend,
            },
        }
    )
    return {"status": "updated", "batch": updated, "summary": summary}


def _public_web_get_run(store: Any, run_id: str, *, owner: PublicWebRunOwner) -> dict[str, Any] | None:
    if owner.storage_owner == CRM_PUBLIC_WEB_OWNER.storage_owner:
        return store.get_crm_public_web_run(run_id=run_id)
    return None


def _public_web_update_run(
    store: Any,
    run_id: str,
    patch: dict[str, Any],
    *,
    owner: PublicWebRunOwner,
) -> dict[str, Any] | None:
    payload = dict(patch or {})
    if owner.storage_owner == CRM_PUBLIC_WEB_OWNER.storage_owner:
        payload["execution_backend"] = CRM_PUBLIC_WEB_OWNER.execution_backend
        payload["source_target_run_id"] = ""
        return store.update_crm_public_web_run(run_id, payload)
    return None


def _public_web_sync_batch_summary(store: Any, run: dict[str, Any], *, owner: PublicWebRunOwner) -> None:
    batch_id = str(run.get("batch_id") or "").strip()
    if not batch_id:
        return
    if owner.storage_owner == CRM_PUBLIC_WEB_OWNER.storage_owner:
        sync_crm_public_web_batch_summary(
            store,
            batch_id,
            workspace_id=str(run.get("workspace_id") or "default").strip() or "default",
        )
        return
    sync_public_web_batch_summary(store, batch_id)


def _crm_public_web_projection_context(store: Any, crm_record: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    projection_id = str(crm_record.get("source_projection_id") or "").strip()
    candidate_identity_key = str(crm_record.get("candidate_identity_key") or "").strip()
    projection_repository = serving_projection_repo(store)
    get_member = getattr(projection_repository, "get_member", None)
    if not projection_id or not candidate_identity_key or not callable(get_member):
        return {}, {}
    try:
        member = get_member(projection_id, candidate_identity_key)
    except Exception:
        return {}, {}
    member_payload = dict(member or {})
    public_summary = dict(member_payload.get("public_summary") or {})
    if not public_summary:
        return member_payload, {}
    return member_payload, public_summary


def _candidate_metadata_with_projection_context(
    metadata: dict[str, Any],
    *,
    crm_record: dict[str, Any],
    projection_member: dict[str, Any],
    public_summary: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(metadata or {})
    candidate_context = dict(merged.get("candidate") or {})
    for source_key, target_key in (
        ("name", "name"),
        ("display_name", "name"),
        ("headline", "headline"),
        ("summary", "summary"),
        ("current_company", "current_company"),
        ("company", "organization"),
        ("title", "title"),
        ("role", "role"),
        ("linkedin_url", "linkedin_url"),
        ("profile_url", "profile_url"),
        ("education_lines", "education_lines"),
        ("experience_lines", "experience_lines"),
        ("education", "education"),
        ("experience", "experience"),
        ("work_history", "work_history"),
        ("location", "location"),
    ):
        value = public_summary.get(source_key)
        if value not in (None, "", [], {}, ()) and target_key not in candidate_context:
            candidate_context[target_key] = value
    if candidate_context:
        merged["candidate"] = candidate_context
    merged["source_projection_id"] = str(crm_record.get("source_projection_id") or "").strip()
    merged["candidate_identity_key"] = str(crm_record.get("candidate_identity_key") or "").strip()
    merged["projection_member_readiness"] = {
        "row_readiness": str(projection_member.get("row_readiness") or ""),
        "profile_readiness": str(projection_member.get("profile_readiness") or ""),
        "card_readiness": str(projection_member.get("card_readiness") or ""),
    }
    return merged


def _public_web_candidate_record_from_run(
    store: Any, run: dict[str, Any], *, owner: PublicWebRunOwner
) -> dict[str, Any] | None:
    run_payload = dict(run or {})
    record_id = str(run_payload.get("record_id") or run_payload.get("crm_record_id") or "").strip()
    crm_record: dict[str, Any] = {}
    projection_member: dict[str, Any] = {}
    public_summary: dict[str, Any] = {}
    if owner.storage_owner == TARGET_PUBLIC_WEB_OWNER.storage_owner:
        record = store.get_target_candidate(record_id) if record_id else None
        if record is not None:
            return {
                **dict(record),
                **{key: value for key, value in run_payload.items() if value not in (None, "", [], {})},
            }
    if owner.storage_owner == CRM_PUBLIC_WEB_OWNER.storage_owner and record_id:
        crm_record = dict(store.get_crm_record(record_id) or {})
        if not crm_record:
            return None
        projection_member, public_summary = _crm_public_web_projection_context(store, crm_record)
    if not record_id:
        return None
    metadata = dict(run_payload.get("metadata") or {})
    if str(run_payload.get("person_identity_key") or "").strip():
        metadata["person_identity_key"] = str(run_payload.get("person_identity_key") or "").strip()
    if owner.storage_owner == CRM_PUBLIC_WEB_OWNER.storage_owner and crm_record:
        metadata = _candidate_metadata_with_projection_context(
            metadata,
            crm_record=crm_record,
            projection_member=projection_member,
            public_summary=public_summary,
        )
    candidate_name = str(
        run_payload.get("candidate_name")
        or crm_record.get("display_name_cache")
        or public_summary.get("name")
        or public_summary.get("display_name")
        or ""
    )
    headline = str(
        run_payload.get("headline") or crm_record.get("headline_cache") or public_summary.get("headline") or ""
    )
    current_company = str(
        run_payload.get("current_company")
        or crm_record.get("primary_company_cache")
        or public_summary.get("current_company")
        or public_summary.get("company")
        or ""
    )
    linkedin_url = str(
        run_payload.get("linkedin_url")
        or dict(crm_record.get("metadata") or {}).get("linkedin_url_cache")
        or public_summary.get("linkedin_url")
        or public_summary.get("profile_url")
        or ""
    )
    return {
        "id": record_id,
        "record_id": record_id,
        "crm_record_id": record_id if owner.storage_owner == CRM_PUBLIC_WEB_OWNER.storage_owner else "",
        "candidate_id": str(run_payload.get("candidate_id") or record_id),
        "candidate_name": candidate_name,
        "display_name": candidate_name,
        "current_company": current_company,
        "headline": headline,
        "linkedin_url": linkedin_url,
        "primary_email": str(run_payload.get("primary_email") or ""),
        "job_id": str(run_payload.get("source_run_id") or ""),
        "history_id": "",
        "metadata": metadata,
    }


def sync_public_web_batch_to_crm_owner(
    store: Any,
    batch: dict[str, Any],
    *,
    runs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Legacy migration bridge from target-candidate Public Web rows to CRM owner tables.

    Normal CRM Public Web execution is owned by `crm_public_web_v1`. This bridge is
    disabled by default so target-candidate summaries cannot recreate retired
    backend evidence on ordinary reads or worker ticks.
    """

    if not _legacy_target_public_web_to_crm_owner_sync_enabled():
        return {
            "status": "skipped",
            "reason": "legacy_target_public_web_to_crm_owner_sync_disabled",
            "owner": CRM_PUBLIC_WEB_OWNER.storage_owner,
            "execution_backend": CRM_PUBLIC_WEB_OWNER.execution_backend,
            "migration_bridge": "target_candidate_public_web_to_crm_public_web",
            "report_visible": True,
        }

    source_batch = dict(batch or {})
    batch_id = str(source_batch.get("batch_id") or "").strip()
    source_runs = [dict(run) for run in list(runs or []) if isinstance(run, dict)]
    if not source_runs and batch_id:
        source_runs = [dict(run) for run in list_legacy_target_public_web_runs(store, batch_id=batch_id)]
    synced_runs: list[dict[str, Any]] = []
    skipped_record_ids: list[str] = []
    workspace_ids: set[str] = set()
    for run in source_runs:
        record_id = str(run.get("record_id") or "").strip()
        if not record_id:
            continue
        crm_record = store.get_crm_record(record_id)
        if not crm_record:
            skipped_record_ids.append(record_id)
            continue
        workspace_id = str(crm_record.get("workspace_id") or "default").strip() or "default"
        workspace_ids.add(workspace_id)
        synced_runs.append(
            store.upsert_crm_public_web_run(
                {
                    **run,
                    "crm_record_id": record_id,
                    "workspace_id": workspace_id,
                    "execution_backend": TARGET_CANDIDATE_PUBLIC_WEB_EXECUTION_BACKEND,
                    "source_target_run_id": str(run.get("run_id") or ""),
                    "summary": {
                        **dict(run.get("summary") or {}),
                        "crm_record_id": record_id,
                        "record_id": record_id,
                        "owner": "crm_public_web_v1",
                        "execution_backend": TARGET_CANDIDATE_PUBLIC_WEB_EXECUTION_BACKEND,
                        "synced_from_target_public_web": True,
                    },
                }
            )
        )
    if not synced_runs:
        return {
            "status": "skipped",
            "reason": "no_crm_owned_public_web_runs",
            "batch_id": batch_id,
            "skipped_record_ids": sorted(set(skipped_record_ids)),
        }
    run_ids = [str(run.get("run_id") or "") for run in synced_runs if str(run.get("run_id") or "")]
    requested_record_ids = [str(run.get("crm_record_id") or run.get("record_id") or "") for run in synced_runs]
    workspace_id = sorted(workspace_ids)[0] if len(workspace_ids) == 1 else "default"
    crm_batch = store.upsert_crm_public_web_batch(
        {
            "batch_id": batch_id,
            "idempotency_key": str(source_batch.get("idempotency_key") or ""),
            "workspace_id": workspace_id,
            "status": str(source_batch.get("status") or "queued"),
            "requested_crm_record_ids": requested_record_ids,
            "source_families": list(source_batch.get("source_families") or []),
            "options": dict(source_batch.get("options") or {}),
            "run_ids": run_ids,
            "summary": {
                **dict(source_batch.get("summary") or summarize_public_web_runs(synced_runs)),
                "owner": "crm_public_web_v1",
                "execution_backend": TARGET_CANDIDATE_PUBLIC_WEB_EXECUTION_BACKEND,
                "synced_run_count": len(synced_runs),
            },
            "metadata": {
                **dict(source_batch.get("metadata") or {}),
                "owner": "crm_public_web_v1",
                "execution_backend": TARGET_CANDIDATE_PUBLIC_WEB_EXECUTION_BACKEND,
                "source_target_batch_id": batch_id,
                "synced_from_target_public_web": True,
            },
            "requested_by": str(source_batch.get("requested_by") or ""),
            "force_refresh": bool(source_batch.get("force_refresh")),
            "execution_backend": TARGET_CANDIDATE_PUBLIC_WEB_EXECUTION_BACKEND,
            "source_target_batch_id": batch_id,
            "started_at": str(source_batch.get("started_at") or ""),
            "completed_at": str(source_batch.get("completed_at") or ""),
            "created_at": str(source_batch.get("created_at") or ""),
        }
    )
    return {
        "status": "synced",
        "owner": "crm_public_web_v1",
        "execution_backend": TARGET_CANDIDATE_PUBLIC_WEB_EXECUTION_BACKEND,
        "batch_id": batch_id,
        "crm_batch_id": str(crm_batch.get("batch_id") or batch_id),
        "synced_run_count": len(synced_runs),
        "skipped_record_ids": sorted(set(skipped_record_ids)),
    }


def _legacy_target_public_web_to_crm_owner_sync_enabled() -> bool:
    return str(os.getenv("SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_TO_CRM_SYNC") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def cancel_target_candidate_public_web_run(
    *,
    store: Any,
    run_id: str,
    reason: str = "",
    operator: str = "",
) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return {"status": "invalid", "reason": "run_id is required"}
    return _legacy_target_public_web_execution_disabled_result(
        operation="cancel_run",
        run_id=normalized_run_id,
    )


def cancel_crm_public_web_run(
    *,
    store: Any,
    run_id: str,
    reason: str = "",
    operator: str = "",
) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return {"status": "invalid", "reason": "run_id is required"}
    run = store.get_crm_public_web_run(run_id=normalized_run_id)
    if run is None:
        return {"status": "not_found", "reason": "public_web_run_not_found", "run_id": normalized_run_id}
    current_status = str(run.get("status") or "").strip().lower()
    if current_status in PUBLIC_WEB_TERMINAL_STATUSES:
        return {"status": "skipped", "reason": "run_already_terminal", "run": run}
    cancelled_at = utc_sql_timestamp()
    cancellation = {
        "reason": str(reason or "").strip() or "cancelled_by_operator",
        "operator": str(operator or "").strip() or "operator",
        "cancelled_at": cancelled_at,
        "previous_status": current_status or "queued",
    }
    summary = dict(run.get("summary") or {})
    summary.update(
        {
            "run_id": normalized_run_id,
            "record_id": str(run.get("record_id") or run.get("crm_record_id") or ""),
            "crm_record_id": str(run.get("crm_record_id") or run.get("record_id") or ""),
            "candidate_name": str(run.get("candidate_name") or ""),
            "status": "cancelled",
            "owner": CRM_PUBLIC_WEB_OWNER.storage_owner,
            "execution_backend": CRM_PUBLIC_WEB_OWNER.execution_backend,
            "cancellation": cancellation,
        }
    )
    search_checkpoint = dict(run.get("search_checkpoint") or {})
    search_checkpoint.update({"stage": "cancelled", "status": "cancelled", "cancellation": cancellation})
    analysis_checkpoint = dict(run.get("analysis_checkpoint") or {})
    analysis_checkpoint.update({"stage": "cancelled", "status": "cancelled", "cancellation": cancellation})
    cancelled = store.update_crm_public_web_run(
        normalized_run_id,
        {
            "status": "cancelled",
            "phase": "cancelled",
            "summary": summary,
            "search_checkpoint": search_checkpoint,
            "analysis_checkpoint": analysis_checkpoint,
            "last_error": f"cancelled:{cancellation['reason']}",
            "completed_at": cancelled_at,
            "execution_backend": CRM_PUBLIC_WEB_OWNER.execution_backend,
            "source_target_run_id": "",
        },
    )
    if cancelled and str(cancelled.get("batch_id") or ""):
        sync_crm_public_web_batch_summary(
            store,
            str(cancelled.get("batch_id") or ""),
            workspace_id=str(cancelled.get("workspace_id") or "default") or "default",
        )
    return {"status": "cancelled", "run": cancelled or run, "cancellation": cancellation}


def _execute_public_web_run_once(
    *,
    store: Any,
    search_provider: BaseSearchProvider,
    model_client: PublicWebModelClient | None,
    runtime_dir: str | Path,
    run_id: str,
    worker: dict[str, Any] | None = None,
    owner: PublicWebRunOwner = CRM_PUBLIC_WEB_OWNER,
    phase_command_type: str = "",
) -> dict[str, Any]:
    if _is_legacy_target_public_web_owner(owner) and not legacy_target_public_web_execution_enabled():
        disabled = _legacy_target_public_web_execution_disabled_result(
            operation="execute_run",
            run_id=str(run_id or ""),
        )
        return {
            "worker_status": "completed",
            "run_status": "retired",
            "summary": disabled,
            **disabled,
        }
    run = _public_web_get_run(store, run_id, owner=owner)
    if run is None:
        return {"worker_status": "completed", "run_status": "failed", "reason": "run_not_found", "run_id": run_id}
    if str(run.get("status") or "") in PUBLIC_WEB_TERMINAL_STATUSES:
        return {
            "worker_status": "completed",
            "run_status": str(run.get("status") or ""),
            "summary": run.get("summary") or {},
        }
    run_status = str(run.get("status") or "")
    if run_status == "analysis_completed":
        return _materialize_public_web_analysis_result(store=store, run=run, owner=owner)
    if run_status == "adjudication_completed":
        return _finalize_public_web_adjudication_result(store=store, run=run, runtime_dir=runtime_dir, owner=owner)
    record = _public_web_candidate_record_from_run(store, run, owner=owner)
    if record is None:
        failed = _public_web_update_run(
            store,
            run_id,
            {
                "status": "failed",
                "phase": "failed",
                "last_error": "public_web_record_not_found",
                "summary": {
                    "run_id": run_id,
                    "error": "public_web_record_not_found",
                    "owner": owner.storage_owner,
                    "execution_backend": owner.execution_backend,
                },
            },
            owner=owner,
        )
        if failed:
            _public_web_sync_batch_summary(store, failed, owner=owner)
        return {"worker_status": "completed", "run_status": "failed", "summary": (failed or {}).get("summary") or {}}

    context = candidate_context_from_target_candidate(record)
    options = public_web_options_from_record(dict(run.get("options") or {}))
    artifact_root = Path(
        str(run.get("artifact_root") or "") or _default_run_artifact_root(runtime_dir, run)
    ).expanduser()
    artifact_root.mkdir(parents=True, exist_ok=True)
    experiment_dir = _public_web_experiment_dir_for_artifact_root(artifact_root)
    plan = prepare_candidate_search_plan(
        candidate=context,
        experiment_dir=experiment_dir,
        options=options,
        ordinal=1,
    )
    checkpoint = dict(run.get("search_checkpoint") or {})
    checkpoint.setdefault("query_results", [])
    checkpoint.setdefault("raw_links", [])
    checkpoint.setdefault("errors", [])
    outcome = _outcome_from_checkpoint(checkpoint)

    if run_status != "documents_fetched" and not checkpoint.get("tasks") and options.use_batch_search:
        submitted = _submit_public_web_batch_search(
            plan=plan,
            search_provider=search_provider,
            options=options,
            checkpoint=checkpoint,
        )
        run = (
            _public_web_update_run(
                store,
                run_id,
                {
                    "status": "search_submitted",
                    "phase": "search_submitted",
                    "artifact_root": str(plan.candidate_dir),
                    "query_manifest": [query.to_record() for query in plan.queries],
                    "search_checkpoint": submitted,
                    "started_at": str(run.get("started_at") or "") or utc_sql_timestamp(),
                },
                owner=owner,
            )
            or run
        )
        checkpoint = dict(run.get("search_checkpoint") or submitted)
        if str(phase_command_type or "").strip() == CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE:
            _public_web_sync_batch_summary(store, run, owner=owner)
            return {
                "worker_status": "running",
                "run_status": "search_submitted",
                "summary": dict(run.get("summary") or {}),
            }

    if run_status != "documents_fetched" and checkpoint.get("tasks"):
        checkpoint, outcome = _poll_and_fetch_ready_public_web_tasks(
            plan=plan,
            search_provider=search_provider,
            options=options,
            checkpoint=checkpoint,
        )
        pending_count = len(_pending_task_records(checkpoint))
        if pending_count > 0:
            running_summary = _public_web_run_progress_summary(
                run_id=run_id,
                context=context,
                checkpoint=checkpoint,
            )
            updated = _public_web_update_run(
                store,
                run_id,
                {
                    "status": "searching",
                    "phase": "searching",
                    "artifact_root": str(plan.candidate_dir),
                    "query_manifest": [query.to_record() for query in plan.queries],
                    "search_checkpoint": checkpoint,
                    "summary": {
                        **running_summary,
                        "owner": owner.storage_owner,
                        "execution_backend": owner.execution_backend,
                    },
                },
                owner=owner,
            )
            _checkpoint_waiting_worker(
                store=store,
                worker=worker,
                run=updated or run,
                checkpoint=checkpoint,
                pending_count=pending_count,
            )
            _public_web_sync_batch_summary(store, updated or run, owner=owner)
            return {
                "worker_status": "running",
                "run_status": "searching",
                "summary": dict((updated or run).get("summary") or {}),
            }
        cancelled_result = _public_web_cancelled_result_if_needed(store=store, run_id=run_id, owner=owner)
        if cancelled_result is not None:
            return cancelled_result
        if _public_web_should_checkpoint_entry_links_ready(run=run, checkpoint=checkpoint):
            ready_summary = _public_web_run_progress_summary(
                run_id=run_id,
                context=context,
                checkpoint=checkpoint,
            )
            updated = _public_web_update_run(
                store,
                run_id,
                {
                    "status": "entry_links_ready",
                    "phase": "entry_links_ready",
                    "artifact_root": str(plan.candidate_dir),
                    "query_manifest": [query.to_record() for query in plan.queries],
                    "search_checkpoint": checkpoint,
                    "summary": {
                        **ready_summary,
                        "owner": owner.storage_owner,
                        "execution_backend": owner.execution_backend,
                    },
                },
                owner=owner,
            )
            _checkpoint_entry_links_ready_worker(
                store=store,
                worker=worker,
                run=updated or run,
                checkpoint=checkpoint,
            )
            _public_web_sync_batch_summary(store, updated or run, owner=owner)
            return {
                "worker_status": "running",
                "run_status": "entry_links_ready",
                "summary": dict((updated or run).get("summary") or ready_summary),
            }
    elif str(checkpoint.get("status") or "") == "batch_unavailable":
        checkpoint["errors"] = []
        outcome = CandidateSearchOutcome(search_mode="sequential")
        execute_single_candidate_searches(
            plan=plan,
            search_provider=search_provider,
            options=options,
            outcome=outcome,
        )
    elif str(checkpoint.get("status") or "") == "submit_failed":
        outcome = _outcome_from_checkpoint(checkpoint)
    elif not options.use_batch_search:
        execute_single_candidate_searches(
            plan=plan,
            search_provider=search_provider,
            options=options,
            outcome=outcome,
        )

    cancelled_result = _public_web_cancelled_result_if_needed(store=store, run_id=run_id, owner=owner)
    if cancelled_result is not None:
        return cancelled_result

    checkpoint["stage"] = "analysis"
    checkpoint["status"] = "search_completed"
    checkpoint["query_results"] = list(outcome.query_results)
    checkpoint["raw_links"] = [link.to_record() for link in outcome.raw_links]
    checkpoint["errors"] = list(outcome.errors)
    _refresh_public_web_phase_metrics(checkpoint)
    if run_status != "documents_fetched":
        document_fetch_started_at = utc_iso_timestamp()
        document_fetch_started_monotonic = time.monotonic()
        _public_web_update_run(
            store,
            run_id,
            {
                "status": "fetching",
                "phase": "fetching",
                "search_checkpoint": checkpoint,
                "analysis_checkpoint": {
                    "stage": "document_fetch",
                    "status": "fetching",
                    "document_fetch_started_at": document_fetch_started_at,
                    "phase_metrics": dict(checkpoint.get("phase_metrics") or {}),
                },
                "artifact_root": str(plan.candidate_dir),
            },
            owner=owner,
        )
        document_fetch_payload = fetch_candidate_public_web_experiment_documents(
            plan=plan,
            outcome=outcome,
            model_client=model_client,
            options=options,
        )
        phase_metrics = _refresh_public_web_phase_metrics(checkpoint)
        phase_metrics.update(
            {
                "document_fetch_started_at": document_fetch_started_at,
                "document_fetch_completed_at": utc_iso_timestamp(),
                "document_fetch_duration_ms": _elapsed_ms(document_fetch_started_monotonic),
                "fetched_document_count": len(
                    [
                        item
                        for item in list(document_fetch_payload.get("fetched_documents") or [])
                        if isinstance(item, dict) and not item.get("error")
                    ]
                ),
                "email_signal_count": len(list(document_fetch_payload.get("email_candidates") or [])),
            }
        )
        documents_fetched = _public_web_update_run(
            store,
            run_id,
            {
                "status": "documents_fetched",
                "phase": "documents_fetched",
                "summary": {
                    **_public_web_run_progress_summary(
                        run_id=run_id,
                        context=context,
                        checkpoint={**checkpoint, "phase_metrics": phase_metrics},
                    ),
                    "owner": owner.storage_owner,
                    "execution_backend": owner.execution_backend,
                },
                "search_checkpoint": checkpoint,
                "analysis_checkpoint": {
                    "stage": "documents_fetched",
                    "status": "documents_fetched",
                    "document_fetch_started_at": document_fetch_started_at,
                    "document_fetch_completed_at": phase_metrics["document_fetch_completed_at"],
                    "document_fetch_duration_ms": phase_metrics["document_fetch_duration_ms"],
                    "document_fetch_payload_path": str(plan.candidate_dir / "document_fetch_payload.json"),
                    "phase_metrics": phase_metrics,
                },
                "artifact_root": str(plan.candidate_dir),
            },
            owner=owner,
        )
        _checkpoint_documents_fetched_worker(
            store=store,
            worker=worker,
            run=documents_fetched or run,
            phase_metrics=phase_metrics,
        )
        _public_web_sync_batch_summary(store, documents_fetched or run, owner=owner)
        return {
            "worker_status": "running",
            "run_status": "documents_fetched",
            "summary": dict((documents_fetched or {}).get("summary") or {}),
        }

    analysis_checkpoint = dict(run.get("analysis_checkpoint") or {})
    document_fetch_payload_path = (
        Path(str(analysis_checkpoint.get("document_fetch_payload_path") or ""))
        if str(analysis_checkpoint.get("document_fetch_payload_path") or "").strip()
        else plan.candidate_dir / "document_fetch_payload.json"
    )
    document_fetch_payload = _load_json_from_path(document_fetch_payload_path)
    if not document_fetch_payload:
        return _fail_public_web_run(
            store=store,
            run=run,
            reason="document_fetch_payload_missing",
            detail=str(document_fetch_payload_path),
            owner=owner,
        )
    analysis_started_at = utc_iso_timestamp()
    _public_web_update_run(
        store,
        run_id,
        {
            "status": "analyzing",
            "phase": "analyzing",
            "search_checkpoint": checkpoint,
            "analysis_checkpoint": {
                **analysis_checkpoint,
                "stage": "analysis",
                "status": "analyzing",
                "analysis_started_at": analysis_started_at,
                "phase_metrics": dict(
                    analysis_checkpoint.get("phase_metrics") or checkpoint.get("phase_metrics") or {}
                ),
            },
            "artifact_root": str(plan.candidate_dir),
        },
        owner=owner,
    )
    analysis_started_monotonic = time.monotonic()
    adjudication_payload = adjudicate_candidate_public_web_experiment_from_document_fetch_payload(
        plan=plan,
        outcome=outcome,
        document_fetch_payload=document_fetch_payload,
        model_client=model_client,
        options=options,
    )
    phase_metrics = _public_web_phase_metrics_from_summary(
        checkpoint=checkpoint,
        summary={
            "entry_link_count": len(list(adjudication_payload.get("entry_links") or [])),
            "fetchable_entry_link_count": len(
                [
                    item
                    for item in list(adjudication_payload.get("entry_links") or [])
                    if isinstance(item, dict) and item.get("fetchable")
                ]
            ),
            "fetched_document_count": len(
                [item for item in list(adjudication_payload.get("fetched_documents") or []) if isinstance(item, dict)]
            ),
            "email_candidate_count": len(list(adjudication_payload.get("email_candidates") or [])),
        },
        analysis_duration_ms=_elapsed_ms(analysis_started_monotonic),
        signals={
            "entry_links": list(adjudication_payload.get("entry_links") or []),
            "email_candidates": list(adjudication_payload.get("email_candidates") or []),
            "ai_adjudication": dict(adjudication_payload.get("ai_adjudication") or {}),
        },
    )
    phase_metrics["signal_materialized_count"] = 0
    phase_metrics["adjudication_duration_ms"] = phase_metrics["analysis_duration_ms"]
    adjudication_completed = _public_web_update_run(
        store,
        run_id,
        {
            "status": "adjudication_completed",
            "phase": "adjudication_completed",
            "summary": {
                **_public_web_run_progress_summary(
                    run_id=run_id,
                    context=context,
                    checkpoint={**checkpoint, "phase_metrics": phase_metrics},
                ),
                "owner": owner.storage_owner,
                "execution_backend": owner.execution_backend,
            },
            "analysis_checkpoint": {
                **analysis_checkpoint,
                "stage": "adjudication_completed",
                "status": "adjudication_completed",
                "analysis_started_at": analysis_started_at,
                "adjudication_started_at": analysis_started_at,
                "adjudication_completed_at": utc_iso_timestamp(),
                "adjudication_duration_ms": phase_metrics["analysis_duration_ms"],
                "adjudication_payload_path": str(plan.candidate_dir / "adjudication_payload.json"),
                "adjudication_input_payload_path": str(
                    adjudication_payload.get("adjudication_input_payload_path") or ""
                ),
                "adjudication_input_contract": dict(adjudication_payload.get("adjudication_input_contract") or {}),
                "ai_adjudication_status": str(
                    dict(adjudication_payload.get("ai_adjudication") or {}).get("status") or ""
                ),
                "adjudicated_profile_link_count": len(
                    [item for item in list(adjudication_payload.get("entry_links") or []) if isinstance(item, dict)]
                ),
                "adjudicated_email_candidate_count": len(
                    [
                        item
                        for item in list(adjudication_payload.get("email_candidates") or [])
                        if isinstance(item, dict)
                    ]
                ),
                "phase_metrics": phase_metrics,
            },
            "artifact_root": str(plan.candidate_dir),
        },
        owner=owner,
    )
    _checkpoint_adjudication_completed_worker(
        store=store,
        worker=worker,
        run=adjudication_completed or run,
        phase_metrics=phase_metrics,
    )
    _public_web_sync_batch_summary(store, adjudication_completed or run, owner=owner)
    return {
        "worker_status": "running",
        "run_status": "adjudication_completed",
        "summary": dict((adjudication_completed or {}).get("summary") or {}),
    }


def execute_target_candidate_public_web_run_once(
    *,
    store: Any,
    search_provider: BaseSearchProvider,
    model_client: PublicWebModelClient | None,
    runtime_dir: str | Path,
    run_id: str,
    worker: dict[str, Any] | None = None,
    owner: PublicWebRunOwner = TARGET_PUBLIC_WEB_OWNER,
) -> dict[str, Any]:
    disabled = _legacy_target_public_web_execution_disabled_result(
        operation="execute_run",
        run_id=str(run_id or ""),
    )
    return {
        "worker_status": "completed",
        "run_status": "retired",
        "summary": disabled,
        **disabled,
    }


def _execute_public_web_run_to_local_idle(
    *,
    store: Any,
    search_provider: BaseSearchProvider,
    model_client: PublicWebModelClient | None,
    runtime_dir: str | Path,
    run_id: str,
    worker: dict[str, Any] | None = None,
    max_local_steps: int = 8,
    owner: PublicWebRunOwner = CRM_PUBLIC_WEB_OWNER,
) -> dict[str, Any]:
    """Advance local-only phases without waiting for another daemon tick.

    Remote batch search can legitimately return "searching" while the provider
    is still working. Once all search tasks are fetched, document fetch,
    adjudication, artifact finalization, and signal materialization are local
    work and should not be stretched by the worker poll interval.
    """

    final_result: dict[str, Any] = {}
    steps = 0
    while steps < max(1, int(max_local_steps or 1)):
        steps += 1
        result = _execute_public_web_run_once(
            store=store,
            search_provider=search_provider,
            model_client=model_client,
            runtime_dir=runtime_dir,
            run_id=run_id,
            worker=worker,
            owner=owner,
        )
        final_result = dict(result or {})
        run_status = str(final_result.get("run_status") or "").strip()
        worker_status = str(final_result.get("worker_status") or "").strip()
        if worker_status == "completed" or run_status in PUBLIC_WEB_TERMINAL_STATUSES:
            break
        if run_status in {"searching", "search_submitted", "queued"}:
            break
    if final_result:
        final_result["local_step_count"] = steps
    return final_result


def execute_target_candidate_public_web_run_to_local_idle(
    *,
    store: Any,
    search_provider: BaseSearchProvider,
    model_client: PublicWebModelClient | None,
    runtime_dir: str | Path,
    run_id: str,
    worker: dict[str, Any] | None = None,
    max_local_steps: int = 8,
    owner: PublicWebRunOwner = TARGET_PUBLIC_WEB_OWNER,
) -> dict[str, Any]:
    disabled = _legacy_target_public_web_execution_disabled_result(
        operation="execute_run",
        run_id=str(run_id or ""),
    )
    return {
        "worker_status": "completed",
        "run_status": "retired",
        "summary": disabled,
        "local_step_count": 1,
        **disabled,
    }


def execute_crm_public_web_run_once(
    *,
    store: Any,
    search_provider: BaseSearchProvider,
    model_client: PublicWebModelClient | None,
    runtime_dir: str | Path,
    run_id: str,
    worker: dict[str, Any] | None = None,
    phase_command_type: str = "",
) -> dict[str, Any]:
    return _execute_public_web_run_once(
        store=store,
        search_provider=search_provider,
        model_client=model_client,
        runtime_dir=runtime_dir,
        run_id=run_id,
        worker=worker,
        owner=CRM_PUBLIC_WEB_OWNER,
        phase_command_type=phase_command_type,
    )


def execute_crm_public_web_run_to_local_idle(
    *,
    store: Any,
    search_provider: BaseSearchProvider,
    model_client: PublicWebModelClient | None,
    runtime_dir: str | Path,
    run_id: str,
    worker: dict[str, Any] | None = None,
    max_local_steps: int = 8,
) -> dict[str, Any]:
    return _execute_public_web_run_to_local_idle(
        store=store,
        search_provider=search_provider,
        model_client=model_client,
        runtime_dir=runtime_dir,
        run_id=run_id,
        worker=worker,
        max_local_steps=max_local_steps,
        owner=CRM_PUBLIC_WEB_OWNER,
    )


def _finalize_public_web_adjudication_result(
    *,
    store: Any,
    run: dict[str, Any],
    runtime_dir: str | Path,
    owner: PublicWebRunOwner = CRM_PUBLIC_WEB_OWNER,
) -> dict[str, Any]:
    record = _public_web_candidate_record_from_run(store, run, owner=owner)
    if record is None:
        return _fail_public_web_run(store=store, run=run, reason="public_web_record_not_found", owner=owner)
    run_id = str(run.get("run_id") or "")
    context = candidate_context_from_target_candidate(record)
    options = public_web_options_from_record(dict(run.get("options") or {}))
    artifact_root = Path(
        str(run.get("artifact_root") or "") or _default_run_artifact_root(runtime_dir, run)
    ).expanduser()
    experiment_dir = _public_web_experiment_dir_for_artifact_root(artifact_root)
    plan = prepare_candidate_search_plan(
        candidate=context,
        experiment_dir=experiment_dir,
        options=options,
        ordinal=1,
    )
    checkpoint = dict(run.get("search_checkpoint") or {})
    outcome = _outcome_from_checkpoint(checkpoint)
    analysis_checkpoint = dict(run.get("analysis_checkpoint") or {})
    adjudication_payload_path = (
        Path(str(analysis_checkpoint.get("adjudication_payload_path") or ""))
        if str(analysis_checkpoint.get("adjudication_payload_path") or "").strip()
        else plan.candidate_dir / "adjudication_payload.json"
    )
    adjudication_payload = _load_json_from_path(adjudication_payload_path)
    if not adjudication_payload:
        return _fail_public_web_run(
            store=store,
            run=run,
            reason="adjudication_payload_missing",
            detail=str(adjudication_payload_path),
            owner=owner,
        )
    analysis_started_monotonic = time.monotonic()
    summary = finalize_candidate_public_web_experiment_from_adjudication_payload(
        plan=plan,
        outcome=outcome,
        adjudication_payload=adjudication_payload,
        options=options,
    )
    signals_path = Path(str(summary.get("artifact_root") or plan.candidate_dir)) / "signals.json"
    signals = _load_json_from_path(signals_path)
    if not signals:
        return _fail_public_web_run(
            store=store,
            run=run,
            reason="signals_payload_missing",
            detail=str(signals_path),
            owner=owner,
        )
    previous_phase_metrics = dict(analysis_checkpoint.get("phase_metrics") or {})
    phase_metrics = _public_web_phase_metrics_from_summary(
        checkpoint=checkpoint,
        summary=summary,
        analysis_duration_ms=_elapsed_ms(analysis_started_monotonic),
        signals=signals,
    )
    phase_metrics = {**previous_phase_metrics, **phase_metrics}
    phase_metrics["signal_materialized_count"] = 0
    analysis_completed = _public_web_update_run(
        store,
        run_id,
        {
            "status": "analysis_completed",
            "phase": "analysis_completed",
            "summary": {
                **summary,
                "phase_metrics": phase_metrics,
                "owner": owner.storage_owner,
                "execution_backend": owner.execution_backend,
            },
            "analysis_checkpoint": {
                **analysis_checkpoint,
                "stage": "analysis_completed",
                "status": "analysis_completed",
                "analysis_completed_at": utc_iso_timestamp(),
                "analysis_duration_ms": phase_metrics["analysis_duration_ms"],
                "ai_adjudication_status": str(dict(signals.get("ai_adjudication") or {}).get("status") or ""),
                "signal_materialization_required_count": phase_metrics["signal_materialization_required_count"],
                "signal_materialized_count": 0,
                "phase_metrics": phase_metrics,
            },
            "artifact_root": str(summary.get("artifact_root") or plan.candidate_dir),
        },
        owner=owner,
    )
    _checkpoint_analysis_completed_worker(
        store=store,
        worker=None,
        run=analysis_completed or run,
        phase_metrics=phase_metrics,
    )
    _public_web_sync_batch_summary(store, analysis_completed or run, owner=owner)
    return {
        "worker_status": "running",
        "run_status": "analysis_completed",
        "summary": {**summary, "phase_metrics": phase_metrics},
    }


def _public_web_cancelled_result_if_needed(
    *,
    store: Any,
    run_id: str,
    owner: PublicWebRunOwner = CRM_PUBLIC_WEB_OWNER,
) -> dict[str, Any] | None:
    current = _public_web_get_run(store, run_id, owner=owner)
    if current is None or str(current.get("status") or "") != "cancelled":
        return None
    _public_web_sync_batch_summary(store, current, owner=owner)
    return {
        "worker_status": "completed",
        "run_status": "cancelled",
        "summary": dict(current.get("summary") or {}),
    }


def _fail_public_web_run(
    *,
    store: Any,
    run: dict[str, Any],
    reason: str,
    detail: str = "",
    owner: PublicWebRunOwner = CRM_PUBLIC_WEB_OWNER,
) -> dict[str, Any]:
    run_id = str(run.get("run_id") or "")
    error = str(reason or "public_web_run_failed")
    if detail:
        error = f"{error}:{detail}"
    summary = dict(run.get("summary") or {})
    summary.update(
        {
            "run_id": run_id,
            "status": "failed",
            "error": error,
            "owner": owner.storage_owner,
            "execution_backend": owner.execution_backend,
        }
    )
    failed = _public_web_update_run(
        store,
        run_id,
        {
            "status": "failed",
            "phase": "failed",
            "summary": summary,
            "last_error": error,
            "completed_at": utc_sql_timestamp(),
        },
        owner=owner,
    )
    _public_web_sync_batch_summary(store, failed or run, owner=owner)
    return {"worker_status": "completed", "run_status": "failed", "summary": summary}


def _materialize_public_web_analysis_result(
    *,
    store: Any,
    run: dict[str, Any],
    owner: PublicWebRunOwner = CRM_PUBLIC_WEB_OWNER,
) -> dict[str, Any]:
    summary = dict(run.get("summary") or {})
    artifact_root = str(summary.get("artifact_root") or run.get("artifact_root") or "").strip()
    signals = _load_json_from_path(Path(artifact_root) / "signals.json") if artifact_root else {}
    phase_metrics = dict(
        summary.get("phase_metrics") or dict(run.get("analysis_checkpoint") or {}).get("phase_metrics") or {}
    )
    materialization_started_monotonic = time.monotonic()
    materialization_started_at = utc_iso_timestamp()
    run_status = str(summary.get("status") or "completed").strip() or "completed"
    completion_payload = {
        **run,
        "status": run_status,
        "phase": "completed",
        "summary": {**summary, "phase_metrics": phase_metrics},
        "artifact_root": artifact_root,
    }
    asset = _upsert_person_asset_from_run(store, completion_payload, signals=signals)
    materialized_signal_rows = _replace_person_public_web_signals_from_run(
        store,
        completion_payload,
        signals=signals,
        asset=asset,
    )
    materialized_signal_counts = _public_web_materialized_signal_counts(materialized_signal_rows)
    canonical_asset = _upsert_canonical_public_web_person_asset_from_run(
        store,
        completion_payload,
        signals=signals,
        legacy_asset=asset,
    )
    canonical_evidence_count = _upsert_canonical_public_web_person_evidence_from_signal_rows(
        store,
        rows=materialized_signal_rows,
        canonical_asset=canonical_asset,
    )
    phase_metrics.update(materialized_signal_counts)
    phase_metrics["person_asset_materialized"] = asset is not None
    phase_metrics["canonical_person_asset_materialized"] = bool(canonical_asset)
    phase_metrics["canonical_person_evidence_materialized_count"] = canonical_evidence_count
    phase_metrics["signal_materialization_started_at"] = materialization_started_at
    phase_metrics["signal_materialization_completed_at"] = utc_iso_timestamp()
    phase_metrics["signal_materialization_duration_ms"] = _elapsed_ms(materialization_started_monotonic)
    completed_at = utc_sql_timestamp()
    analysis_checkpoint = dict(run.get("analysis_checkpoint") or {})
    completed_run = _public_web_update_run(
        store,
        str(run.get("run_id") or ""),
        {
            "status": run_status,
            "phase": "completed",
            "summary": {
                **summary,
                "phase_metrics": phase_metrics,
                "owner": owner.storage_owner,
                "execution_backend": owner.execution_backend,
            },
            "analysis_checkpoint": {
                **analysis_checkpoint,
                "stage": "completed",
                "status": run_status,
                "signal_materialization_started_at": materialization_started_at,
                "signal_materialization_completed_at": phase_metrics["signal_materialization_completed_at"],
                "signal_materialization_duration_ms": phase_metrics["signal_materialization_duration_ms"],
                "signal_materialized_count": materialized_signal_counts["signal_materialized_count"],
                "email_signal_materialized_count": materialized_signal_counts["email_signal_materialized_count"],
                "profile_link_signal_materialized_count": materialized_signal_counts[
                    "profile_link_signal_materialized_count"
                ],
                "person_asset_materialized": asset is not None,
                "canonical_person_asset_materialized": bool(canonical_asset),
                "canonical_person_evidence_materialized_count": canonical_evidence_count,
                "phase_metrics": phase_metrics,
            },
            "artifact_root": artifact_root,
            "completed_at": completed_at,
        },
        owner=owner,
    )
    _public_web_sync_batch_summary(store, completed_run or run, owner=owner)
    return {
        "worker_status": "completed",
        "run_status": run_status,
        "summary": {**summary, "phase_metrics": phase_metrics},
    }


def summarize_public_web_runs(runs: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(run.get("status") or "unknown") for run in runs)
    terminal_count = sum(counts.get(status, 0) for status in PUBLIC_WEB_TERMINAL_STATUSES)
    running_count = len(runs) - terminal_count
    if not runs:
        status = "queued"
    elif counts.get("failed", 0) == len(runs):
        status = "failed"
    elif running_count == 0:
        status = (
            "completed_with_errors"
            if counts.get("completed_with_errors", 0) or counts.get("needs_review", 0)
            else "completed"
        )
    elif counts.get("searching", 0) or counts.get("search_submitted", 0):
        status = "searching"
    elif counts.get("fetching", 0) or counts.get("documents_fetched", 0):
        status = "fetching"
    elif counts.get("analyzing", 0) or counts.get("adjudication_completed", 0) or counts.get("analysis_completed", 0):
        status = "analyzing"
    else:
        status = "queued"
    return {
        "status": status,
        "run_count": len(runs),
        "queued_count": int(counts.get("queued", 0)),
        "search_submitted_count": int(counts.get("search_submitted", 0)),
        "searching_count": int(counts.get("searching", 0)),
        "entry_links_ready_count": int(counts.get("entry_links_ready", 0)),
        "fetching_count": int(counts.get("fetching", 0)),
        "documents_fetched_count": int(counts.get("documents_fetched", 0)),
        "analyzing_count": int(counts.get("analyzing", 0)),
        "adjudication_completed_count": int(counts.get("adjudication_completed", 0)),
        "analysis_completed_count": int(counts.get("analysis_completed", 0)),
        "completed_count": int(counts.get("completed", 0)),
        "completed_with_errors_count": int(counts.get("completed_with_errors", 0)),
        "needs_review_count": int(counts.get("needs_review", 0)),
        "failed_count": int(counts.get("failed", 0)),
        "cancelled_count": int(counts.get("cancelled", 0)),
        "running_count": running_count,
        "phase_metrics": _aggregate_public_web_phase_metrics(runs),
        "updated_at": utc_iso_timestamp(),
    }


def normalize_public_web_product_options(payload: dict[str, Any] | None = None) -> PublicWebExperimentOptions:
    payload = dict(payload or {})
    raw_options = dict(payload.get("options") or {})
    source_families = _normalize_source_families(
        raw_options.get("source_families") or payload.get("source_families") or DEFAULT_TARGET_CANDIDATE_SOURCE_FAMILIES
    )
    return PublicWebExperimentOptions(
        source_families=tuple(source_families),
        max_queries_per_candidate=_bounded_int(
            raw_options.get("max_queries_per_candidate"), default=10, low=1, high=16
        ),
        max_results_per_query=_bounded_int(raw_options.get("max_results_per_query"), default=10, low=1, high=20),
        max_entry_links_per_candidate=_bounded_int(
            raw_options.get("max_entry_links_per_candidate"),
            default=40,
            low=1,
            high=80,
        ),
        max_fetches_per_candidate=_bounded_int(raw_options.get("max_fetches_per_candidate"), default=5, low=0, high=12),
        max_ai_evidence_documents=_bounded_int(
            raw_options.get("max_ai_evidence_documents"),
            default=8,
            low=1,
            high=20,
        ),
        max_ai_entry_links=_bounded_int(
            raw_options.get("max_ai_entry_links"),
            default=8,
            low=1,
            high=20,
        ),
        fetch_content=_coerce_bool(raw_options.get("fetch_content"), default=True),
        extract_contact_signals=_coerce_bool(raw_options.get("extract_contact_signals"), default=True),
        ai_extraction=str(raw_options.get("ai_extraction") or "auto").strip().lower() or "auto",
        timeout_seconds=_bounded_int(raw_options.get("timeout_seconds"), default=30, low=5, high=90),
        use_batch_search=_coerce_bool(raw_options.get("use_batch_search"), default=True),
        batch_ready_poll_interval_seconds=float(raw_options.get("batch_ready_poll_interval_seconds") or 0.0),
        max_batch_ready_polls=_bounded_int(raw_options.get("max_batch_ready_polls"), default=18, low=1, high=60),
        max_remote_search_wait_seconds=_bounded_int(
            raw_options.get("max_remote_search_wait_seconds"),
            default=1800,
            low=30,
            high=7200,
        ),
        max_provider_pending_wait_seconds=_bounded_int(
            raw_options.get("max_provider_pending_wait_seconds"),
            default=7200,
            low=30,
            high=21600,
        ),
        max_provider_task_reset_attempts=_bounded_int(
            raw_options.get("max_provider_task_reset_attempts"),
            default=1,
            low=0,
            high=3,
        ),
        max_concurrent_fetches_per_candidate=_bounded_int(
            raw_options.get("max_concurrent_fetches_per_candidate"),
            default=4,
            low=1,
            high=8,
        ),
        max_concurrent_candidate_analyses=_bounded_int(
            raw_options.get("max_concurrent_candidate_analyses"),
            default=2,
            low=1,
            high=4,
        ),
        document_fetch_total_timeout_seconds=_optional_positive_float(
            raw_options.get("document_fetch_total_timeout_seconds"),
            low=0.01,
            high=600.0,
        ),
    )


def public_web_options_from_record(payload: dict[str, Any]) -> PublicWebExperimentOptions:
    return normalize_public_web_product_options({"options": payload})


def build_public_web_batch_idempotency_key(
    *,
    requested_record_ids: list[str],
    options: PublicWebExperimentOptions,
    force_refresh: bool = False,
    nonce: str = "",
) -> str:
    payload = {
        "record_ids": sorted(str(item or "").strip() for item in requested_record_ids if str(item or "").strip()),
        "options": asdict(options),
        "force_refresh": bool(force_refresh),
        "nonce": str(nonce or "") if force_refresh else "",
    }
    return "target-candidate-public-web-batch:" + short_hash(json.dumps(payload, sort_keys=True, ensure_ascii=False))


def build_crm_public_web_batch_idempotency_key(
    *,
    workspace_id: str,
    requested_record_ids: list[str],
    options: PublicWebExperimentOptions,
    force_refresh: bool = False,
    nonce: str = "",
) -> str:
    payload = {
        "workspace_id": str(workspace_id or "default").strip() or "default",
        "record_ids": sorted(str(item or "").strip() for item in requested_record_ids if str(item or "").strip()),
        "options": asdict(options),
        "force_refresh": bool(force_refresh),
        "nonce": str(nonce or "") if force_refresh else "",
    }
    return "crm-public-web-batch:" + short_hash(json.dumps(payload, sort_keys=True, ensure_ascii=False))


def build_public_web_run_idempotency_key(
    *,
    record_id: str,
    linkedin_url_key: str,
    options: PublicWebExperimentOptions,
    force_refresh: bool = False,
    nonce: str = "",
) -> str:
    payload = {
        "record_id": str(record_id or "").strip(),
        "linkedin_url_key": str(linkedin_url_key or "").strip(),
        "options": asdict(options),
        "force_refresh": bool(force_refresh),
        "nonce": str(nonce or "") if force_refresh else "",
    }
    return "target-candidate-public-web-run:" + short_hash(json.dumps(payload, sort_keys=True, ensure_ascii=False))


def build_crm_public_web_run_idempotency_key(
    *,
    workspace_id: str,
    record_id: str,
    linkedin_url_key: str,
    options: PublicWebExperimentOptions,
    force_refresh: bool = False,
    nonce: str = "",
) -> str:
    payload = {
        "workspace_id": str(workspace_id or "default").strip() or "default",
        "record_id": str(record_id or "").strip(),
        "linkedin_url_key": str(linkedin_url_key or "").strip(),
        "options": asdict(options),
        "force_refresh": bool(force_refresh),
        "nonce": str(nonce or "") if force_refresh else "",
    }
    return "crm-public-web-run:" + short_hash(json.dumps(payload, sort_keys=True, ensure_ascii=False))


def public_web_worker_key(run_id: str) -> str:
    return f"public_web_run::{str(run_id or '').strip()}"


def person_identity_key_for_context(context: PublicWebCandidateContext) -> str:
    if context.linkedin_url_key:
        return f"linkedin:{context.linkedin_url_key}"
    return f"target_candidate:{context.record_id}"


def short_hash(value: str) -> str:
    return sha1(str(value or "").encode("utf-8")).hexdigest()[:16]


def utc_iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_sql_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def utc_compact_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _elapsed_ms(started_monotonic: float) -> float:
    return round(max(0.0, time.monotonic() - float(started_monotonic or 0.0)) * 1000.0, 3)


def _parse_utc_iso_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _public_web_remote_search_wait_exceeded(
    checkpoint: dict[str, Any],
    options: PublicWebExperimentOptions,
) -> bool:
    max_wait_seconds = max(1, int(options.max_remote_search_wait_seconds or 1))
    elapsed_seconds = _public_web_remote_search_elapsed_seconds(checkpoint)
    if elapsed_seconds is None:
        return False
    return elapsed_seconds >= max_wait_seconds


def _public_web_provider_pending_wait_exceeded(
    checkpoint: dict[str, Any],
    options: PublicWebExperimentOptions,
) -> bool:
    max_wait_seconds = max(1, int(options.max_provider_pending_wait_seconds or 1))
    elapsed_seconds = _public_web_remote_search_elapsed_seconds(checkpoint)
    if elapsed_seconds is None:
        return False
    return elapsed_seconds >= max_wait_seconds


def _public_web_remote_search_elapsed_seconds(checkpoint: dict[str, Any]) -> float | None:
    submitted_at = _parse_utc_iso_timestamp(
        checkpoint.get("search_submit_started_at") or checkpoint.get("submitted_at")
    )
    if submitted_at is None:
        return None
    return (datetime.now(timezone.utc) - submitted_at).total_seconds()


def _public_web_task_status_counts(checkpoint: dict[str, Any]) -> dict[str, int]:
    counts = Counter(
        str(dict(task).get("status") or "unknown")
        for task in dict(checkpoint.get("tasks") or {}).values()
        if isinstance(task, dict)
    )
    return {str(key): int(value) for key, value in sorted(counts.items())}


def _public_web_phase_metrics(checkpoint: dict[str, Any]) -> dict[str, Any]:
    pending = _pending_task_records(checkpoint)
    fetched = _fetched_task_records(checkpoint)
    task_status_counts = _public_web_task_status_counts(checkpoint)
    provider_pending_deferred_count = sum(
        1 for task in pending if bool(dict(task).get("provider_pending_wait_deferred"))
    )
    metrics = {
        "stage": str(checkpoint.get("stage") or ""),
        "status": str(checkpoint.get("status") or ""),
        "provider_name": str(checkpoint.get("provider_name") or ""),
        "search_mode": str(checkpoint.get("search_mode") or ""),
        "submitted_task_count": int(checkpoint.get("submitted_task_count") or len(dict(checkpoint.get("tasks") or {}))),
        "ready_poll_count": int(checkpoint.get("ready_poll_count") or 0),
        "ready_task_count_last_poll": int(checkpoint.get("ready_task_count_last_poll") or 0),
        "pending_task_count": len(pending),
        "provider_pending_deferred_count": provider_pending_deferred_count,
        "fetched_task_count": len(fetched),
        "failed_task_count": int(task_status_counts.get("failed") or 0),
        "timeout_task_count": int(task_status_counts.get("timeout") or 0),
        "query_result_count": len(list(checkpoint.get("query_results") or [])),
        "raw_link_count": len(list(checkpoint.get("raw_links") or [])),
        "error_count": len(list(checkpoint.get("errors") or [])),
        "task_status_counts": task_status_counts,
    }
    for key in (
        "search_submit_started_at",
        "search_submit_completed_at",
        "search_poll_started_at",
        "search_poll_completed_at",
        "search_fetch_started_at",
        "search_fetch_completed_at",
    ):
        if str(checkpoint.get(key) or "").strip():
            metrics[key] = str(checkpoint.get(key) or "")
    for key in (
        "search_submit_duration_ms",
        "search_poll_duration_ms",
        "search_fetch_duration_ms",
    ):
        if checkpoint.get(key) is not None:
            metrics[key] = float(checkpoint.get(key) or 0.0)
    if checkpoint.get("fetched_task_count_last_poll") is not None:
        metrics["fetched_task_count_last_poll"] = int(checkpoint.get("fetched_task_count_last_poll") or 0)
    if checkpoint.get("fetch_error_count_last_poll") is not None:
        metrics["fetch_error_count_last_poll"] = int(checkpoint.get("fetch_error_count_last_poll") or 0)
    if checkpoint.get("provider_task_reset_count") is not None:
        metrics["provider_task_reset_count"] = int(checkpoint.get("provider_task_reset_count") or 0)
    if checkpoint.get("provider_task_reset_error_count") is not None:
        metrics["provider_task_reset_error_count"] = int(checkpoint.get("provider_task_reset_error_count") or 0)
    if checkpoint.get("provider_task_reset_duration_ms") is not None:
        metrics["provider_task_reset_duration_ms"] = float(checkpoint.get("provider_task_reset_duration_ms") or 0.0)
    return metrics


def _refresh_public_web_phase_metrics(checkpoint: dict[str, Any]) -> dict[str, Any]:
    previous_metrics = dict(checkpoint.get("phase_metrics") or {})
    metrics = {**previous_metrics, **_public_web_phase_metrics(checkpoint)}
    checkpoint["phase_metrics"] = metrics
    return metrics


def _public_web_run_progress_summary(
    *,
    run_id: str,
    context: PublicWebCandidateContext,
    checkpoint: dict[str, Any],
) -> dict[str, Any]:
    metrics = _refresh_public_web_phase_metrics(checkpoint)
    return {
        "run_id": run_id,
        "record_id": context.record_id,
        "candidate_id": context.candidate_id,
        "candidate_name": context.candidate_name,
        "current_company": context.current_company,
        "status": str(metrics.get("status") or ""),
        "stage": str(metrics.get("stage") or ""),
        "pending_search_task_count": int(metrics.get("pending_task_count") or 0),
        "fetched_search_task_count": int(metrics.get("fetched_task_count") or 0),
        "ready_poll_count": int(metrics.get("ready_poll_count") or 0),
        "phase_metrics": metrics,
    }


def _public_web_should_checkpoint_entry_links_ready(*, run: dict[str, Any], checkpoint: dict[str, Any]) -> bool:
    if str(run.get("status") or "").strip() in {"entry_links_ready", "fetching", "documents_fetched", "analyzing"}:
        return False
    if str(checkpoint.get("status") or "") != "search_completed":
        return False
    if _pending_task_records(checkpoint):
        return False
    return bool(checkpoint.get("tasks"))


def _public_web_phase_metrics_from_summary(
    *,
    checkpoint: dict[str, Any],
    summary: dict[str, Any],
    analysis_duration_ms: float,
    signals: dict[str, Any],
) -> dict[str, Any]:
    metrics = _refresh_public_web_phase_metrics(checkpoint)
    metrics.update(
        {
            "analysis_duration_ms": float(analysis_duration_ms),
            "entry_link_count": int(summary.get("entry_link_count") or 0),
            "fetchable_entry_link_count": int(summary.get("fetchable_entry_link_count") or 0),
            "fetched_document_count": int(summary.get("fetched_document_count") or 0),
            "email_candidate_count": int(summary.get("email_candidate_count") or 0),
            "profile_link_signal_count": len(
                [
                    item
                    for item in list(signals.get("entry_links") or [])
                    if isinstance(item, dict) and _should_materialize_profile_link_signal(item)
                ]
            ),
            "email_signal_count": len(
                [
                    item
                    for item in list(signals.get("email_candidates") or [])
                    if isinstance(item, dict) and _should_materialize_email_signal(item)
                ]
            ),
        }
    )
    metrics["signal_materialization_required_count"] = int(metrics.get("profile_link_signal_count") or 0) + int(
        metrics.get("email_signal_count") or 0
    )
    ai_adjudication = dict(signals.get("ai_adjudication") or {})
    ai_result = dict(ai_adjudication.get("result") or {}) if isinstance(ai_adjudication.get("result"), dict) else {}
    model_provider, model_version = _model_identity_from_signals(signals)
    if model_provider:
        metrics["model_provider"] = model_provider
    if model_version:
        metrics["model"] = model_version
        metrics["model_version"] = model_version
    requested_model = str(ai_adjudication.get("requested_model") or ai_result.get("requested_model") or "").strip()
    response_model = str(ai_adjudication.get("response_model") or ai_result.get("response_model") or "").strip()
    effective_model = str(ai_adjudication.get("effective_model") or ai_result.get("effective_model") or "").strip()
    model_identity_provenance = str(
        ai_adjudication.get("model_identity_provenance") or ai_result.get("model_identity_provenance") or ""
    ).strip()
    if requested_model:
        metrics["requested_model"] = requested_model
    if response_model:
        metrics["response_model"] = response_model
    if effective_model:
        metrics["effective_model"] = effective_model
    if response_model and effective_model and model_identity_provenance == "provider_response":
        metrics["model_identity_provenance"] = model_identity_provenance
    raw_model_usage = ai_adjudication.get("model_usage")
    if not isinstance(raw_model_usage, dict):
        raw_model_usage = ai_result.get("model_usage")
    if isinstance(raw_model_usage, dict):
        metrics["model_usage"] = _bounded_public_web_model_usage(raw_model_usage)
    model_fallback_used = bool(ai_adjudication.get("fallback_used")) or bool(ai_result.get("fallback_used"))
    metrics["model_fallback_used"] = model_fallback_used
    if model_fallback_used:
        metrics["model_fallback_policy"] = "fail_closed_requires_ai_review"
        fallback_reason = str(ai_adjudication.get("fallback_reason") or ai_result.get("fallback_reason") or "").strip()
        model_error = str(ai_adjudication.get("model_error") or ai_result.get("model_error") or "").strip()
        if fallback_reason:
            metrics["model_fallback_reason"] = fallback_reason
        if model_error:
            metrics["model_error"] = model_error[:500]
    return metrics


def _bounded_public_web_model_usage(raw_usage: dict[str, Any]) -> dict[str, int]:
    usage: dict[str, int] = {}
    for field in _PUBLIC_WEB_MODEL_USAGE_TOKEN_FIELDS:
        value = raw_usage.get(field)
        if value is None or isinstance(value, bool):
            continue
        try:
            parsed = int(value)
        except (TypeError, ValueError, OverflowError):
            continue
        usage[field] = min(_PUBLIC_WEB_MODEL_USAGE_TOKEN_LIMIT, max(0, parsed))
    return usage


def _aggregate_public_web_phase_metrics(runs: list[dict[str, Any]]) -> dict[str, Any]:
    metric_rows: list[dict[str, Any]] = []
    run_metric_rows: list[tuple[str, dict[str, Any]]] = []
    phase_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    for run in runs:
        if not isinstance(run, dict):
            continue
        phase = str(run.get("phase") or "").strip() or "unknown"
        status = str(run.get("status") or "").strip() or "unknown"
        phase_counts[phase] += 1
        status_counts[status] += 1
        metrics = dict(dict(run.get("summary") or {}).get("phase_metrics") or {})
        if not metrics:
            metrics = dict(dict(run.get("analysis_checkpoint") or {}).get("phase_metrics") or {})
        if metrics:
            metric_rows.append(metrics)
            run_metric_rows.append((status, metrics))

    sum_fields = (
        "submitted_task_count",
        "pending_task_count",
        "fetched_task_count",
        "failed_task_count",
        "timeout_task_count",
        "fetch_error_count_last_poll",
        "provider_task_reset_count",
        "provider_task_reset_error_count",
        "query_result_count",
        "raw_link_count",
        "error_count",
        "entry_link_count",
        "fetchable_entry_link_count",
        "fetched_document_count",
        "email_candidate_count",
        "profile_link_signal_count",
        "email_signal_count",
        "signal_materialization_required_count",
        "signal_materialized_count",
        "email_signal_materialized_count",
        "profile_link_signal_materialized_count",
    )
    max_fields = (
        "ready_poll_count",
        "search_submit_duration_ms",
        "search_poll_duration_ms",
        "search_fetch_duration_ms",
        "provider_task_reset_duration_ms",
        "document_fetch_duration_ms",
        "analysis_duration_ms",
        "adjudication_duration_ms",
        "signal_materialization_duration_ms",
    )
    aggregate: dict[str, Any] = {
        "run_count": len(runs),
        "metric_run_count": len(metric_rows),
        "phase_counts": {key: int(value) for key, value in sorted(phase_counts.items())},
        "status_counts": {key: int(value) for key, value in sorted(status_counts.items())},
    }
    for field in sum_fields:
        aggregate[field] = sum(_coerce_int_metric(row.get(field)) for row in metric_rows)
    for field in max_fields:
        values = [_coerce_float_metric(row.get(field)) for row in metric_rows if row.get(field) is not None]
        aggregate[f"{field}_max"] = max(values) if values else 0.0

    duration_field_by_phase = {
        "search_submit": "search_submit_duration_ms",
        "search_poll": "search_poll_duration_ms",
        "search_fetch": "search_fetch_duration_ms",
        "document_fetch": "document_fetch_duration_ms",
        "analysis": "analysis_duration_ms",
        "adjudication": "adjudication_duration_ms",
        "signal_materialization": "signal_materialization_duration_ms",
    }
    duration_by_phase = {
        phase: float(aggregate.get(f"{field}_max") or 0.0) for phase, field in duration_field_by_phase.items()
    }
    aggregate["duration_by_phase_ms"] = {}
    for phase, field in duration_field_by_phase.items():
        values = [_coerce_float_metric(row.get(field)) for row in metric_rows if row.get(field) is not None]
        total = sum(values)
        aggregate["duration_by_phase_ms"][phase] = {
            "count": len(values),
            "total": round(total, 2),
            "avg": round(total / len(values), 2) if values else 0.0,
            "max": max(values) if values else 0.0,
        }
    aggregate["duration_by_phase_ms_max"] = duration_by_phase
    aggregate["slowest_phase"] = (
        max(duration_by_phase, key=duration_by_phase.get) if any(duration_by_phase.values()) else ""
    )
    aggregate["has_pending_remote_search"] = int(aggregate.get("pending_task_count") or 0) > 0
    required_signals = int(
        aggregate.get("signal_materialization_required_count")
        or (int(aggregate.get("email_signal_count") or 0) + int(aggregate.get("profile_link_signal_count") or 0))
    )
    aggregate["has_unmaterialized_signals"] = required_signals > int(aggregate.get("signal_materialized_count") or 0)
    terminal_error_statuses = {"completed_with_errors", "needs_review", "failed"}
    terminal_with_errors_count = sum(int(status_counts.get(status) or 0) for status in terminal_error_statuses)
    remote_pending_run_count = sum(
        1 for _status, row in run_metric_rows if _coerce_int_metric(row.get("pending_task_count")) > 0
    )
    unmaterialized_signal_gap_count = 0
    completed_without_materialized_signals_count = 0
    runs_with_metric_errors_count = 0
    for status, row in run_metric_rows:
        row_required_signals = _coerce_int_metric(row.get("signal_materialization_required_count"))
        if row_required_signals <= 0:
            row_required_signals = _coerce_int_metric(row.get("email_signal_count")) + _coerce_int_metric(
                row.get("profile_link_signal_count")
            )
        row_materialized_signals = _coerce_int_metric(row.get("signal_materialized_count"))
        if row_required_signals > row_materialized_signals:
            unmaterialized_signal_gap_count += 1
            if status in PUBLIC_WEB_TERMINAL_STATUSES:
                completed_without_materialized_signals_count += 1
        if (
            _coerce_int_metric(row.get("error_count")) > 0
            or _coerce_int_metric(row.get("failed_task_count")) > 0
            or _coerce_int_metric(row.get("timeout_task_count")) > 0
        ):
            runs_with_metric_errors_count += 1
    missing_phase_metric_count = max(0, len(runs) - len(metric_rows))
    runs_with_errors_count = max(terminal_with_errors_count, runs_with_metric_errors_count)
    risk_reasons: list[str] = []
    if remote_pending_run_count:
        risk_reasons.append("remote_search_pending")
    if unmaterialized_signal_gap_count:
        risk_reasons.append("signal_materialization_gap")
    if completed_without_materialized_signals_count:
        risk_reasons.append("terminal_signal_materialization_gap")
    if terminal_with_errors_count:
        risk_reasons.append("terminal_run_errors")
    if runs_with_metric_errors_count:
        risk_reasons.append("provider_or_fetch_errors")
    if missing_phase_metric_count:
        risk_reasons.append("missing_phase_metrics")
    aggregate["remote_search_pending_run_count"] = remote_pending_run_count
    aggregate["unmaterialized_signal_gap_count"] = unmaterialized_signal_gap_count
    aggregate["completed_without_materialized_signals_count"] = completed_without_materialized_signals_count
    aggregate["terminal_with_errors_count"] = terminal_with_errors_count
    aggregate["partial_failure_count"] = terminal_with_errors_count
    aggregate["runs_with_errors_count"] = runs_with_errors_count
    aggregate["runs_with_metric_errors_count"] = runs_with_metric_errors_count
    aggregate["missing_phase_metric_count"] = missing_phase_metric_count
    aggregate["provider_or_fetch_failure_count"] = (
        int(aggregate.get("failed_task_count") or 0)
        + int(aggregate.get("timeout_task_count") or 0)
        + int(aggregate.get("fetch_error_count_last_poll") or 0)
    )
    aggregate["local_processing_error_count"] = int(aggregate.get("error_count") or 0)
    aggregate["phase_lag_risk_reasons"] = risk_reasons
    aggregate["service_guardrail_violation_detected"] = completed_without_materialized_signals_count > 0
    return aggregate


def _coerce_int_metric(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _coerce_float_metric(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _submit_public_web_batch_search(
    *,
    plan: Any,
    search_provider: BaseSearchProvider,
    options: PublicWebExperimentOptions,
    checkpoint: dict[str, Any],
) -> dict[str, Any]:
    query_specs = _batch_query_specs_for_plan(plan, options=options)
    submit_started_monotonic = time.monotonic()
    submit_started_at = utc_iso_timestamp()
    checkpoint.update(
        {
            "stage": "search_submitted",
            "status": "search_submitted",
            "search_mode": "batch_queue",
            "submitted_at": submit_started_at,
            "search_submit_started_at": submit_started_at,
            "queries": [query.to_record() for query in plan.queries],
            "query_results": list(checkpoint.get("query_results") or []),
            "raw_links": list(checkpoint.get("raw_links") or []),
            "errors": list(checkpoint.get("errors") or []),
        }
    )
    try:
        submission = search_provider.submit_batch_queries(query_specs)
    except Exception as exc:
        checkpoint["status"] = "submit_failed"
        checkpoint.setdefault("errors", []).append(f"batch_submit_failed:{str(exc)[:200]}")
        checkpoint["search_submit_completed_at"] = utc_iso_timestamp()
        checkpoint["search_submit_duration_ms"] = _elapsed_ms(submit_started_monotonic)
        _refresh_public_web_phase_metrics(checkpoint)
        return checkpoint
    if submission is None:
        checkpoint["status"] = "batch_unavailable"
        checkpoint.setdefault("errors", []).append("batch_submit_failed:provider_batch_unavailable")
        checkpoint["search_submit_completed_at"] = utc_iso_timestamp()
        checkpoint["search_submit_duration_ms"] = _elapsed_ms(submit_started_monotonic)
        _refresh_public_web_phase_metrics(checkpoint)
        return checkpoint
    write_search_execution_artifacts(plan.logger, artifacts=submission.artifacts, prefix="submit")
    tasks: dict[str, dict[str, Any]] = {}
    query_by_task_key = {str(spec.get("task_key") or ""): dict(spec) for spec in query_specs}
    for task in submission.tasks:
        task_key = str(task.task_key or "")
        original = query_by_task_key.get(task_key) or {}
        task_checkpoint = dict(task.checkpoint or {})
        checkpoint_status = str(task_checkpoint.get("status") or "").strip()
        task_status = "submitted"
        task_error = str(task_checkpoint.get("error") or dict(task.metadata or {}).get("error") or "").strip()
        if checkpoint_status.startswith("submit_failed"):
            task_status = "failed"
            query_id = str(dict(original.get("query") or {}).get("query_id") or task_key)
            if task_error:
                checkpoint.setdefault("errors", []).append(f"search_failed:{query_id}:{task_error[:200]}")
        tasks[task_key] = {
            "task_key": task_key,
            "query_identity_key": task_key,
            "task_id": str(task_checkpoint.get("task_id") or dict(task.metadata or {}).get("task_id") or ""),
            "query_text": task.query_text,
            "query": dict(original.get("query") or {}),
            "query_index": int(original.get("query_index") or 0),
            "checkpoint": task_checkpoint,
            "metadata": dict(task.metadata or {}),
            "provider_name": submission.provider_name,
            "status": task_status,
            "poll_count": 0,
        }
    checkpoint["provider_name"] = submission.provider_name
    checkpoint["tasks"] = tasks
    checkpoint["submitted_task_count"] = len(tasks)
    checkpoint["search_submit_completed_at"] = utc_iso_timestamp()
    checkpoint["search_submit_duration_ms"] = _elapsed_ms(submit_started_monotonic)
    _refresh_public_web_phase_metrics(checkpoint)
    return checkpoint


def _poll_and_fetch_ready_public_web_tasks(
    *,
    plan: Any,
    search_provider: BaseSearchProvider,
    options: PublicWebExperimentOptions,
    checkpoint: dict[str, Any],
) -> tuple[dict[str, Any], CandidateSearchOutcome]:
    outcome = _outcome_from_checkpoint(checkpoint)
    tasks = {str(key): dict(value) for key, value in dict(checkpoint.get("tasks") or {}).items()}
    pending = [task for task in tasks.values() if str(task.get("status") or "") not in {"fetched", "failed", "timeout"}]
    if not pending:
        _refresh_public_web_phase_metrics(checkpoint)
        return checkpoint, outcome
    ready_specs = []
    poll_started_monotonic = time.monotonic()
    checkpoint["search_poll_started_at"] = utc_iso_timestamp()
    checkpoint["pending_task_count_before_poll"] = len(pending)
    remote_wait_exceeded = _public_web_remote_search_wait_exceeded(checkpoint, options)
    provider_pending_wait_exceeded = _public_web_provider_pending_wait_exceeded(checkpoint, options)
    elapsed_seconds = _public_web_remote_search_elapsed_seconds(checkpoint)
    try:
        ready = search_provider.poll_ready_batch([_task_poll_spec(task) for task in pending])
    except Exception as exc:
        checkpoint.setdefault("errors", []).append(f"batch_ready_failed:{str(exc)[:200]}")
        checkpoint["search_poll_completed_at"] = utc_iso_timestamp()
        checkpoint["search_poll_duration_ms"] = _elapsed_ms(poll_started_monotonic)
        _refresh_public_web_phase_metrics(checkpoint)
        return checkpoint, outcome
    if ready is None:
        checkpoint.setdefault("errors", []).append("batch_ready_failed:provider_returned_none")
        checkpoint["search_poll_completed_at"] = utc_iso_timestamp()
        checkpoint["search_poll_duration_ms"] = _elapsed_ms(poll_started_monotonic)
        _refresh_public_web_phase_metrics(checkpoint)
        return checkpoint, outcome
    write_search_execution_artifacts(
        plan.logger,
        artifacts=ready.artifacts,
        prefix=f"ready_{int(checkpoint.get('ready_poll_count') or 0) + 1:02d}",
    )
    checkpoint["ready_poll_count"] = int(checkpoint.get("ready_poll_count") or 0) + 1
    checkpoint["ready_task_count_last_poll"] = 0
    for task in ready.tasks:
        task_key = str(task.task_key or "")
        current = tasks.get(task_key)
        if current is None:
            continue
        task_metadata = dict(task.metadata or {})
        current["checkpoint"] = dict(task.checkpoint or current.get("checkpoint") or {})
        current["task_id"] = str(task.task_id or current.get("task_id") or "")
        current["metadata"] = {**dict(current.get("metadata") or {}), **task_metadata}
        for key in (
            "provider_status_code",
            "provider_status_message",
            "provider_wait_state",
            "readiness_strategy",
        ):
            if task_metadata.get(key) not in {None, ""}:
                current[key] = task_metadata.get(key)
        current["poll_count"] = int(current.get("poll_count") or 0) + 1
        if bool(task.metadata.get("ready")) or str(task.checkpoint.get("status") or "") == "ready_cached":
            current["status"] = "ready"
            ready_specs.append(_task_poll_spec(current))
            checkpoint["ready_task_count_last_poll"] = int(checkpoint.get("ready_task_count_last_poll") or 0) + 1
        elif remote_wait_exceeded and _task_has_provider_pending_status(current) and not provider_pending_wait_exceeded:
            current["status"] = "waiting"
            current["ready_poll_budget_exhausted"] = True
            current["ready_poll_budget_exhausted_at"] = (
                current.get("ready_poll_budget_exhausted_at") or utc_iso_timestamp()
            )
            current["provider_pending_wait_deferred"] = True
            current["provider_pending_wait_deferred_at"] = utc_iso_timestamp()
            current["provider_pending_wait_elapsed_seconds"] = round(float(elapsed_seconds or 0.0), 3)
            current["provider_pending_wait_budget_seconds"] = int(options.max_provider_pending_wait_seconds or 0)
            current["provider_pending_wait_reason"] = "dataforseo_task_handed_or_in_queue"
        elif remote_wait_exceeded or provider_pending_wait_exceeded:
            current["status"] = "timeout"
            timeout_budget = (
                int(options.max_provider_pending_wait_seconds or 0)
                if _task_has_provider_pending_status(current)
                else int(options.max_remote_search_wait_seconds or 0)
            )
            timeout_reason = (
                "provider pending task not ready"
                if _task_has_provider_pending_status(current)
                else "batch task not ready"
            )
            outcome.errors.append(
                f"search_timeout:{dict(current.get('query') or {}).get('query_id') or task_key}:"
                f"{timeout_reason} after {timeout_budget}s"
            )
        else:
            current["status"] = "waiting"
            if int(current.get("poll_count") or 0) >= int(options.max_batch_ready_polls or 1):
                current["ready_poll_budget_exhausted"] = True
                current["ready_poll_budget_exhausted_at"] = utc_iso_timestamp()
        tasks[task_key] = current
    _reset_waiting_public_web_provider_tasks(
        plan=plan,
        search_provider=search_provider,
        options=options,
        tasks=tasks,
        checkpoint=checkpoint,
    )
    if ready_specs:
        fetch_started_monotonic = time.monotonic()
        checkpoint["search_fetch_started_at"] = utc_iso_timestamp()
        fetched_tasks, fetch_artifacts, fetch_errors = fetch_ready_specs_isolated(
            search_provider=search_provider,
            ready_specs=ready_specs,
        )
        write_search_execution_artifacts(
            plan.logger,
            artifacts=fetch_artifacts,
            prefix=f"fetch_{int(checkpoint.get('fetch_count') or 0) + 1:02d}",
        )
        checkpoint["fetch_count"] = int(checkpoint.get("fetch_count") or 0) + len(fetched_tasks)
        checkpoint["fetched_task_count_last_poll"] = len(fetched_tasks)
        checkpoint["fetch_error_count_last_poll"] = len(fetch_errors)
        checkpoint["search_fetch_completed_at"] = utc_iso_timestamp()
        checkpoint["search_fetch_duration_ms"] = _elapsed_ms(fetch_started_monotonic)
        query_specs = {query.query_id: query for query in plan.queries}
        for task in fetched_tasks:
            task_key = str(task.task_key or "")
            current = tasks.get(task_key)
            if current is None:
                continue
            current["checkpoint"] = dict(task.checkpoint or current.get("checkpoint") or {})
            current["status"] = "fetched"
            current["task_id"] = str(task.task_id or current.get("task_id") or "")
            if task.response is not None:
                query_record = dict(current.get("query") or {})
                query = query_specs.get(str(query_record.get("query_id") or "")) or PublicWebQuerySpec(
                    query_id=str(query_record.get("query_id") or task_key),
                    source_family=str(query_record.get("source_family") or "profile_web_presence"),
                    query_text=str(task.query_text or query_record.get("query_text") or ""),
                    objective=str(query_record.get("objective") or ""),
                )
                record_candidate_search_response(
                    plan=plan,
                    query=query,
                    query_index=int(current.get("query_index") or 1),
                    response=task.response,
                    outcome=outcome,
                    duration_seconds=0.0,
                    search_mode="batch_queue",
                )
            tasks[task_key] = current
        for task_key, error_text in fetch_errors.items():
            current = tasks.get(str(task_key))
            if current is not None:
                current["status"] = "failed"
                current["error"] = str(error_text)
                tasks[str(task_key)] = current
            outcome.errors.append(f"search_failed:{task_key}:{str(error_text)[:200]}")
    checkpoint["tasks"] = tasks
    checkpoint["stage"] = "waiting_remote_search" if _pending_task_records(checkpoint) else "search_completed"
    checkpoint["status"] = "searching" if _pending_task_records(checkpoint) else "search_completed"
    checkpoint["query_results"] = list(outcome.query_results)
    checkpoint["raw_links"] = [link.to_record() for link in outcome.raw_links]
    checkpoint["errors"] = list(dict.fromkeys([*list(checkpoint.get("errors") or []), *outcome.errors]))
    checkpoint["search_poll_completed_at"] = utc_iso_timestamp()
    checkpoint["search_poll_duration_ms"] = _elapsed_ms(poll_started_monotonic)
    _refresh_public_web_phase_metrics(checkpoint)
    return checkpoint, _outcome_from_checkpoint(checkpoint)


def _checkpoint_waiting_worker(
    *,
    store: Any,
    worker: dict[str, Any] | None,
    run: dict[str, Any],
    checkpoint: dict[str, Any],
    pending_count: int,
) -> None:
    worker_id = int(dict(worker or {}).get("worker_id") or 0)
    if worker_id <= 0:
        return
    worker_checkpoint = dict(dict(worker or {}).get("checkpoint") or {})
    worker_checkpoint.update(
        {
            "stage": "waiting_remote_search",
            "run_id": str(run.get("run_id") or ""),
            "batch_id": str(run.get("batch_id") or ""),
            "pending_search_task_count": pending_count,
            "search_checkpoint": checkpoint,
        }
    )
    store.checkpoint_agent_worker(
        worker_id,
        checkpoint_payload=worker_checkpoint,
        output_payload={
            "summary": dict(run.get("summary") or {}),
            "run_id": str(run.get("run_id") or ""),
            "run_status": str(run.get("status") or ""),
        },
        status="running",
    )


def _checkpoint_entry_links_ready_worker(
    *,
    store: Any,
    worker: dict[str, Any] | None,
    run: dict[str, Any],
    checkpoint: dict[str, Any],
) -> None:
    worker_id = int(dict(worker or {}).get("worker_id") or 0)
    if worker_id <= 0:
        return
    worker_checkpoint = dict(dict(worker or {}).get("checkpoint") or {})
    worker_checkpoint.update(
        {
            "stage": "entry_links_ready",
            "status": "ready_for_analysis",
            "run_id": str(run.get("run_id") or ""),
            "batch_id": str(run.get("batch_id") or ""),
            "search_checkpoint": checkpoint,
        }
    )
    store.checkpoint_agent_worker(
        worker_id,
        checkpoint_payload=worker_checkpoint,
        output_payload={
            "summary": dict(run.get("summary") or {}),
            "run_id": str(run.get("run_id") or ""),
            "run_status": str(run.get("status") or ""),
        },
        status="running",
    )


def _checkpoint_analysis_completed_worker(
    *,
    store: Any,
    worker: dict[str, Any] | None,
    run: dict[str, Any],
    phase_metrics: dict[str, Any],
) -> None:
    worker_id = int(dict(worker or {}).get("worker_id") or 0)
    if worker_id <= 0:
        return
    worker_checkpoint = dict(dict(worker or {}).get("checkpoint") or {})
    worker_checkpoint.update(
        {
            "stage": "analysis_completed",
            "status": "ready_for_signal_materialization",
            "run_id": str(run.get("run_id") or ""),
            "batch_id": str(run.get("batch_id") or ""),
            "phase_metrics": phase_metrics,
        }
    )
    store.checkpoint_agent_worker(
        worker_id,
        checkpoint_payload=worker_checkpoint,
        output_payload={
            "summary": dict(run.get("summary") or {}),
            "run_id": str(run.get("run_id") or ""),
            "run_status": str(run.get("status") or ""),
        },
        status="running",
    )


def _checkpoint_adjudication_completed_worker(
    *,
    store: Any,
    worker: dict[str, Any] | None,
    run: dict[str, Any],
    phase_metrics: dict[str, Any],
) -> None:
    worker_id = int(dict(worker or {}).get("worker_id") or 0)
    if worker_id <= 0:
        return
    worker_checkpoint = dict(dict(worker or {}).get("checkpoint") or {})
    worker_checkpoint.update(
        {
            "stage": "adjudication_completed",
            "status": "ready_for_model_safe_artifacts",
            "run_id": str(run.get("run_id") or ""),
            "batch_id": str(run.get("batch_id") or ""),
            "phase_metrics": phase_metrics,
        }
    )
    store.checkpoint_agent_worker(
        worker_id,
        checkpoint_payload=worker_checkpoint,
        output_payload={
            "summary": dict(run.get("summary") or {}),
            "run_id": str(run.get("run_id") or ""),
            "run_status": str(run.get("status") or ""),
        },
        status="running",
    )


def _checkpoint_documents_fetched_worker(
    *,
    store: Any,
    worker: dict[str, Any] | None,
    run: dict[str, Any],
    phase_metrics: dict[str, Any],
) -> None:
    worker_id = int(dict(worker or {}).get("worker_id") or 0)
    if worker_id <= 0:
        return
    worker_checkpoint = dict(dict(worker or {}).get("checkpoint") or {})
    worker_checkpoint.update(
        {
            "stage": "documents_fetched",
            "status": "ready_for_analysis",
            "run_id": str(run.get("run_id") or ""),
            "batch_id": str(run.get("batch_id") or ""),
            "phase_metrics": phase_metrics,
        }
    )
    store.checkpoint_agent_worker(
        worker_id,
        checkpoint_payload=worker_checkpoint,
        output_payload={
            "summary": dict(run.get("summary") or {}),
            "run_id": str(run.get("run_id") or ""),
            "run_status": str(run.get("status") or ""),
        },
        status="running",
    )


def _batch_query_specs_for_plan(plan: Any, *, options: PublicWebExperimentOptions) -> list[dict[str, Any]]:
    specs = []
    for query_index, query in enumerate(plan.queries, start=1):
        task_key = public_web_query_identity_key(
            candidate_record_id=plan.candidate.record_id,
            query_id=query.query_id,
            query_text=query.query_text,
        )
        specs.append(
            {
                "task_key": task_key,
                "query_identity_key": task_key,
                "query_text": query.query_text,
                "max_results": max(1, int(options.max_results_per_query or 1)),
                "query_index": query_index,
                "query": query.to_record(),
                "metadata": {
                    "record_id": plan.candidate.record_id,
                    "candidate_id": plan.candidate.candidate_id,
                    "query_id": query.query_id,
                    "query_identity_key": task_key,
                    "source_family": query.source_family,
                },
            }
        )
    return specs


def _task_poll_spec(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_key": str(task.get("task_key") or ""),
        "query_identity_key": str(task.get("query_identity_key") or task.get("task_key") or ""),
        "task_id": str(task.get("task_id") or dict(task.get("checkpoint") or {}).get("task_id") or ""),
        "query_text": str(task.get("query_text") or ""),
        "checkpoint": dict(task.get("checkpoint") or {}),
        "metadata": dict(task.get("metadata") or {}),
        "provider_name": str(
            task.get("provider_name") or dict(task.get("checkpoint") or {}).get("provider_name") or ""
        ),
    }


def _task_query_spec(task: dict[str, Any], *, options: PublicWebExperimentOptions) -> dict[str, Any]:
    query = dict(task.get("query") or {})
    metadata = dict(task.get("metadata") or {})
    task_key = str(task.get("query_identity_key") or task.get("task_key") or "").strip()
    return {
        "task_key": task_key,
        "query_identity_key": task_key,
        "query_text": str(task.get("query_text") or query.get("query_text") or ""),
        "max_results": max(1, int(options.max_results_per_query or 1)),
        "query_index": int(task.get("query_index") or 0),
        "query": query,
        "metadata": {
            **metadata,
            "query_id": str(query.get("query_id") or metadata.get("query_id") or task_key),
            "query_identity_key": task_key,
            "source_family": str(query.get("source_family") or metadata.get("source_family") or ""),
            "provider_task_reset": True,
        },
    }


def _task_provider_status_code(task: dict[str, Any]) -> int:
    for source in (task, dict(task.get("metadata") or {}), dict(task.get("checkpoint") or {})):
        try:
            parsed = int(dict(source).get("provider_status_code") or 0)
        except (TypeError, ValueError):
            parsed = 0
        if parsed:
            return parsed
    return 0


def _task_has_provider_pending_status(task: dict[str, Any]) -> bool:
    wait_state = str(
        task.get("provider_wait_state")
        or dict(task.get("metadata") or {}).get("provider_wait_state")
        or dict(task.get("checkpoint") or {}).get("provider_wait_state")
        or ""
    ).strip()
    return wait_state == "provider_pending" or _task_provider_status_code(task) in {40601, 40602}


def _should_reset_public_web_provider_task(task: dict[str, Any], *, options: PublicWebExperimentOptions) -> bool:
    if int(options.max_provider_task_reset_attempts or 0) <= 0:
        return False
    if int(task.get("provider_task_reset_attempt_count") or 0) >= int(options.max_provider_task_reset_attempts or 0):
        return False
    if int(task.get("poll_count") or 0) < int(options.max_batch_ready_polls or 1):
        return False
    return _task_has_provider_pending_status(task)


def _reset_waiting_public_web_provider_tasks(
    *,
    plan: Any,
    search_provider: BaseSearchProvider,
    options: PublicWebExperimentOptions,
    tasks: dict[str, dict[str, Any]],
    checkpoint: dict[str, Any],
) -> None:
    reset_candidates = [
        dict(task)
        for task in tasks.values()
        if str(task.get("status") or "") == "waiting" and _should_reset_public_web_provider_task(task, options=options)
    ]
    if not reset_candidates:
        return
    reset_started_at = utc_iso_timestamp()
    reset_started_monotonic = time.monotonic()
    query_specs = [_task_query_spec(task, options=options) for task in reset_candidates]
    try:
        submission = search_provider.submit_batch_queries(query_specs)
    except Exception as exc:
        checkpoint.setdefault("errors", []).append(f"batch_task_reset_failed:{str(exc)[:200]}")
        checkpoint["provider_task_reset_error_count"] = int(
            checkpoint.get("provider_task_reset_error_count") or 0
        ) + len(reset_candidates)
        return
    if submission is None:
        checkpoint.setdefault("errors", []).append("batch_task_reset_failed:provider_returned_none")
        checkpoint["provider_task_reset_error_count"] = int(
            checkpoint.get("provider_task_reset_error_count") or 0
        ) + len(reset_candidates)
        return
    reset_index = int(checkpoint.get("provider_task_reset_count") or 0) + 1
    write_search_execution_artifacts(plan.logger, artifacts=submission.artifacts, prefix=f"reset_{reset_index:02d}")
    submitted_by_key = {str(task.task_key or ""): task for task in submission.tasks}
    reset_query_count = 0
    for previous in reset_candidates:
        task_key = str(previous.get("task_key") or previous.get("query_identity_key") or "").strip()
        submitted = submitted_by_key.get(task_key)
        if submitted is None:
            continue
        current = tasks.get(task_key)
        if current is None:
            continue
        previous_task_id = str(current.get("task_id") or dict(current.get("checkpoint") or {}).get("task_id") or "")
        previous_task_ids = [
            str(item or "").strip() for item in list(current.get("previous_task_ids") or []) if str(item or "").strip()
        ]
        if previous_task_id and previous_task_id not in previous_task_ids:
            previous_task_ids.append(previous_task_id)
        submitted_checkpoint = dict(submitted.checkpoint or {})
        submitted_metadata = dict(submitted.metadata or {})
        reset_attempt_count = int(current.get("provider_task_reset_attempt_count") or 0) + 1
        current.update(
            {
                "task_id": str(
                    submitted_checkpoint.get("task_id")
                    or submitted_metadata.get("task_id")
                    or current.get("task_id")
                    or ""
                ),
                "checkpoint": submitted_checkpoint,
                "metadata": submitted_metadata,
                "provider_name": submission.provider_name,
                "status": "submitted",
                "poll_count": 0,
                "previous_task_ids": previous_task_ids,
                "provider_task_reset_attempt_count": reset_attempt_count,
                "provider_task_reset_at": reset_started_at,
                "provider_task_reset_reason": "provider_pending_after_poll_budget",
                "provider_task_reset_previous_status_code": _task_provider_status_code(previous),
            }
        )
        current.pop("ready_poll_budget_exhausted", None)
        current.pop("ready_poll_budget_exhausted_at", None)
        tasks[task_key] = current
        reset_query_count += 1
    if reset_query_count:
        checkpoint["provider_task_reset_count"] = (
            int(checkpoint.get("provider_task_reset_count") or 0) + reset_query_count
        )
        checkpoint["provider_task_reset_started_at"] = reset_started_at
        checkpoint["provider_task_reset_completed_at"] = utc_iso_timestamp()
        checkpoint["provider_task_reset_duration_ms"] = _elapsed_ms(reset_started_monotonic)


def _pending_task_records(checkpoint: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(task)
        for task in dict(checkpoint.get("tasks") or {}).values()
        if str(dict(task).get("status") or "") not in {"fetched", "failed", "timeout"}
    ]


def _fetched_task_records(checkpoint: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(task)
        for task in dict(checkpoint.get("tasks") or {}).values()
        if str(dict(task).get("status") or "") == "fetched"
    ]


def _outcome_from_checkpoint(checkpoint: dict[str, Any]) -> CandidateSearchOutcome:
    return CandidateSearchOutcome(
        query_results=[dict(item) for item in list(checkpoint.get("query_results") or []) if isinstance(item, dict)],
        raw_links=[
            _classified_link_from_record(item)
            for item in list(checkpoint.get("raw_links") or [])
            if isinstance(item, dict)
        ],
        errors=[str(item) for item in list(checkpoint.get("errors") or []) if str(item or "").strip()],
        search_mode=str(checkpoint.get("search_mode") or "batch_queue"),
    )


def _classified_link_from_record(record: dict[str, Any]) -> ClassifiedEntryLink:
    return ClassifiedEntryLink(
        url=str(record.get("url") or ""),
        normalized_url=str(record.get("normalized_url") or record.get("url") or ""),
        title=str(record.get("title") or ""),
        snippet=str(record.get("snippet") or ""),
        source_domain=str(record.get("source_domain") or ""),
        entry_type=str(record.get("entry_type") or "other"),
        source_family=str(record.get("source_family") or ""),
        score=float(record.get("score") or 0.0),
        reasons=tuple(str(item) for item in list(record.get("reasons") or [])),
        query_id=str(record.get("query_id") or ""),
        query_text=str(record.get("query_text") or ""),
        provider_name=str(record.get("provider_name") or ""),
        result_rank=int(record.get("result_rank") or 0),
        fetchable=bool(record.get("fetchable")),
        identity_match_label=str(record.get("identity_match_label") or "unreviewed"),
        identity_match_score=float(record.get("identity_match_score") or 0.0),
        confidence_label=str(record.get("confidence_label") or "medium"),
        adjudication=dict(record.get("adjudication") or {}),
    )


def build_person_public_web_signal_rows(
    *,
    run: dict[str, Any],
    signals: dict[str, Any],
    asset: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    run_id = str(run.get("run_id") or "").strip()
    if not run_id:
        return []
    person_identity_key = str(run.get("person_identity_key") or "").strip()
    linkedin_url_key = str(run.get("linkedin_url_key") or "").strip()
    if not person_identity_key and linkedin_url_key:
        person_identity_key = f"linkedin:{linkedin_url_key}"
    asset_id = str(dict(asset or {}).get("asset_id") or "").strip()
    if not asset_id and person_identity_key.startswith("linkedin:"):
        asset_id = f"person-public-web-{short_hash(person_identity_key)}"
    base = {
        "run_id": run_id,
        "asset_id": asset_id,
        "person_identity_key": person_identity_key,
        "record_id": str(run.get("record_id") or ""),
        "candidate_id": str(run.get("candidate_id") or ""),
        "candidate_name": str(run.get("candidate_name") or dict(run.get("summary") or {}).get("candidate_name") or ""),
        "current_company": str(
            run.get("current_company") or dict(run.get("summary") or {}).get("current_company") or ""
        ),
        "linkedin_url_key": linkedin_url_key,
    }
    model_provider, model_version = _model_identity_from_signals(signals)
    common_artifact_refs = _model_safe_common_artifact_refs(run=run, signals=signals)
    artifact_refs_by_url = _artifact_refs_by_source_url(signals)
    rows: list[dict[str, Any]] = []

    for index, email in enumerate(list(signals.get("email_candidates") or []), start=1):
        if not isinstance(email, dict):
            continue
        if not _should_materialize_email_signal(email):
            continue
        source_url = str(email.get("source_url") or "").strip()
        source_domain = str(email.get("source_domain") or "").strip() or _domain_from_url(source_url)
        normalized_value = str(email.get("normalized_value") or email.get("value") or "").strip().lower()
        rows.append(
            {
                **base,
                "signal_id": public_web_signal_id_for_identity(
                    person_identity_key=person_identity_key,
                    record_id=base["record_id"],
                    signal_kind="email_candidate",
                    signal_type=str(email.get("email_type") or "unknown").strip() or "unknown",
                    normalized_value=normalized_value,
                    source_url=source_url,
                ),
                "signal_kind": "email_candidate",
                "signal_type": str(email.get("email_type") or "unknown").strip() or "unknown",
                "email_type": str(email.get("email_type") or "").strip(),
                "value": str(email.get("value") or normalized_value).strip(),
                "normalized_value": normalized_value,
                "url": source_url,
                "source_url": source_url,
                "source_domain": source_domain,
                "source_family": str(email.get("source_family") or "").strip(),
                "source_title": str(email.get("source_title") or "").strip(),
                "confidence_label": str(email.get("confidence_label") or "").strip(),
                "confidence_score": _coerce_float(email.get("confidence_score")),
                "identity_match_label": str(email.get("identity_match_label") or "needs_review").strip(),
                "identity_match_score": _coerce_float(email.get("identity_match_score")),
                "publishable": bool(email.get("publishable")),
                "promotion_status": str(email.get("promotion_status") or "not_promoted").strip(),
                "suppression_reason": str(email.get("suppression_reason") or "").strip(),
                "evidence_excerpt": _truncate_signal_text(email.get("evidence_excerpt"), limit=700),
                "artifact_refs": _merge_artifact_refs(
                    common_artifact_refs,
                    artifact_refs_by_url.get(normalize_public_web_url_key(source_url), {}),
                ),
                "model_provider": model_provider,
                "model_version": model_version,
                "metadata": {
                    "adjudication": dict(email.get("adjudication") or {}),
                    "raw_asset_policy": "raw_html_pdf_and_search_payloads_excluded_from_default_export",
                },
            }
        )

    for index, link in enumerate(list(signals.get("entry_links") or []), start=1):
        if not isinstance(link, dict):
            continue
        url = str(link.get("normalized_url") or link.get("url") or "").strip()
        if not url:
            continue
        identity_match_label = str(link.get("identity_match_label") or "unreviewed").strip()
        source_domain = str(link.get("source_domain") or "").strip() or _domain_from_url(url)
        signal_type = canonicalize_profile_link_type_from_url(
            str(link.get("entry_type") or "other").strip() or "other",
            url,
        )
        link_shape_warnings = public_web_link_shape_warnings(signal_type, url)
        clean_profile_link = is_clean_profile_link(signal_type, url)
        publishable_profile_link = is_publishable_profile_link(signal_type, url)
        publishable = identity_match_label in {"confirmed", "likely_same_person"} and publishable_profile_link
        if not _should_materialize_profile_link_signal(
            link,
            signal_type=signal_type,
            clean_profile_link=clean_profile_link,
            publishable_profile_link=publishable_profile_link,
        ):
            continue
        link_suppression_reason = ""
        if not publishable_profile_link:
            link_suppression_reason = "non_publishable_link_type" if clean_profile_link else "non_profile_link_shape"
        link_adjudication = dict(link.get("adjudication") or {})
        rows.append(
            {
                **base,
                "signal_id": public_web_signal_id_for_identity(
                    person_identity_key=person_identity_key,
                    record_id=base["record_id"],
                    signal_kind="profile_link",
                    signal_type=signal_type,
                    normalized_value=url,
                    url=url,
                    source_url=url,
                ),
                "signal_kind": "profile_link",
                "signal_type": signal_type,
                "email_type": "",
                "value": url,
                "normalized_value": url,
                "url": url,
                "source_url": url,
                "source_domain": source_domain,
                "source_family": str(link.get("source_family") or "").strip(),
                "source_title": str(link.get("title") or "").strip(),
                "confidence_label": str(link.get("confidence_label") or "").strip(),
                "confidence_score": _coerce_float(link.get("score")),
                "identity_match_label": identity_match_label,
                "identity_match_score": _coerce_float(link.get("identity_match_score")),
                "publishable": publishable,
                "promotion_status": "",
                "suppression_reason": link_suppression_reason,
                "evidence_excerpt": _truncate_signal_text(link.get("snippet"), limit=700),
                "artifact_refs": _merge_artifact_refs(
                    common_artifact_refs,
                    artifact_refs_by_url.get(normalize_public_web_url_key(url), {}),
                ),
                "model_provider": model_provider,
                "model_version": model_version,
                "metadata": {
                    "query_id": str(link.get("query_id") or ""),
                    "query_text": str(link.get("query_text") or ""),
                    "provider_name": str(link.get("provider_name") or ""),
                    "result_rank": int(link.get("result_rank") or 0),
                    "fetchable": bool(link.get("fetchable")),
                    "reasons": list(link.get("reasons") or []),
                    "adjudication": link_adjudication,
                    "link_shape_warnings": link_shape_warnings,
                    "clean_profile_link": clean_profile_link,
                    "publishable_profile_link": publishable_profile_link,
                    "raw_asset_policy": "raw_html_pdf_and_search_payloads_excluded_from_default_export",
                },
            }
        )
    return rows


def _should_materialize_email_signal(email: dict[str, Any]) -> bool:
    normalized_value = str(email.get("normalized_value") or email.get("value") or "").strip().lower()
    if not normalized_value:
        return False
    identity_match_label = str(email.get("identity_match_label") or "needs_review").strip()
    promotion_status = str(email.get("promotion_status") or "not_promoted").strip()
    suppression_reason = str(email.get("suppression_reason") or "").strip()
    if identity_match_label == "not_same_person":
        return False
    if promotion_status in {"suppressed", "rejected"} or suppression_reason:
        return False
    if bool(email.get("publishable")) or promotion_status == "promotion_recommended":
        return True
    if identity_match_label in {"confirmed", "likely_same_person", "ambiguous_identity"}:
        return True
    if identity_match_label == "needs_review" and _coerce_float(email.get("identity_match_score")) >= 0.5:
        return True
    return False


def _profile_link_adjudication_reason(link: dict[str, Any]) -> str:
    adjudication = dict(link.get("adjudication") or {})
    return str(adjudication.get("reason") or "").strip()


def _profile_link_has_model_assessment(link: dict[str, Any]) -> bool:
    adjudication = dict(link.get("adjudication") or {})
    if bool(adjudication.get("fallback_used")):
        return False
    if _profile_link_adjudication_reason(link) in {
        "model_no_assessment",
        "model_fallback_requires_ai_review",
    }:
        return False
    return bool(
        str(adjudication.get("identity_match_label") or "").strip()
        or str(adjudication.get("rationale") or "").strip()
        or str(adjudication.get("signal_type") or "").strip()
    )


def _profile_link_user_visible_signal(link: dict[str, Any]) -> bool:
    adjudication = dict(link.get("adjudication") or {})
    return bool(adjudication.get("user_visible_signal"))


def _should_materialize_profile_link_signal(
    link: dict[str, Any],
    *,
    signal_type: str = "",
    clean_profile_link: bool | None = None,
    publishable_profile_link: bool | None = None,
) -> bool:
    """Keep raw search noise in artifacts, not in user-visible signal rows."""
    url = str(link.get("normalized_url") or link.get("url") or "").strip()
    if not url:
        return False
    resolved_signal_type = signal_type or canonicalize_profile_link_type_from_url(
        str(link.get("entry_type") or "other").strip() or "other",
        url,
    )
    if clean_profile_link is None:
        clean_profile_link = is_clean_profile_link(resolved_signal_type, url)
    if publishable_profile_link is None:
        publishable_profile_link = is_publishable_profile_link(resolved_signal_type, url)
    identity_match_label = str(link.get("identity_match_label") or "unreviewed").strip()
    if identity_match_label in {"not_same_person", "unreviewed"}:
        return False
    if not publishable_profile_link:
        return False
    if identity_match_label in {"confirmed", "likely_same_person"}:
        return _profile_link_has_model_assessment(link) or bool(publishable_profile_link)
    if identity_match_label in {"needs_review", "ambiguous_identity"}:
        return bool(
            clean_profile_link and _profile_link_has_model_assessment(link) and _profile_link_user_visible_signal(link)
        )
    return False


def _public_web_materialized_signal_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    signal_rows = [dict(row) for row in list(rows or []) if isinstance(row, dict)]
    by_kind = Counter(str(row.get("signal_kind") or "").strip() for row in signal_rows)
    return {
        "signal_materialized_count": len(signal_rows),
        "email_signal_materialized_count": int(by_kind.get("email_candidate") or 0),
        "profile_link_signal_materialized_count": int(by_kind.get("profile_link") or 0),
    }


def _replace_person_public_web_signals_from_run(
    store: Any,
    run: dict[str, Any],
    *,
    signals: dict[str, Any],
    asset: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    rows = build_person_public_web_signal_rows(run=run, signals=signals, asset=asset)
    run_id = str(run.get("run_id") or "").strip()
    store.replace_person_public_web_signals_for_run(run_id=run_id, signals=rows)
    if not run_id:
        return []
    return store.list_person_public_web_signals(
        run_id=run_id,
        record_id=str(run.get("record_id") or run.get("crm_record_id") or "").strip(),
        limit=max(1000, len(rows)),
    )


def _upsert_canonical_public_web_person_asset_from_run(
    store: Any,
    run: dict[str, Any],
    *,
    signals: dict[str, Any],
    legacy_asset: dict[str, Any] | None,
) -> dict[str, Any]:
    person_identity_key = str(run.get("person_identity_key") or "").strip()
    if not person_identity_key and str(run.get("linkedin_url_key") or "").strip():
        person_identity_key = f"linkedin:{str(run.get('linkedin_url_key') or '').strip()}"
    run_id = str(run.get("run_id") or "").strip()
    if not person_identity_key or not run_id:
        return {}
    summary = dict(run.get("summary") or {})
    artifact_root = str(run.get("artifact_root") or summary.get("artifact_root") or "").strip()
    common_refs = _model_safe_common_artifact_refs(run=run, signals=signals)
    model_provider, model_version = _model_identity_from_signals(signals)
    return store.upsert_person_asset(
        {
            "asset_id": f"person-asset-public-web-{short_hash(f'{person_identity_key}|{run_id}')}",
            "person_identity_key": person_identity_key,
            "asset_type": "public_web_summary",
            "source_kind": "crm_public_web" if str(run.get("crm_record_id") or "").strip() else "public_web",
            "source_run_id": run_id,
            "source_projection_id": "",
            "content_ref": artifact_root,
            "content_hash": short_hash(json.dumps(common_refs, ensure_ascii=False, sort_keys=True)),
            "source_url": "",
            "fetched_at": str(run.get("completed_at") or summary.get("completed_at") or ""),
            "visibility_scope": "internal_evidence",
            "status": "available",
            "metadata": {
                "schema_version": "public_web_person_asset_v1",
                "legacy_person_public_web_asset_id": str(dict(legacy_asset or {}).get("asset_id") or ""),
                "run_id": run_id,
                "artifact_refs": common_refs,
                "model_provider": model_provider,
                "model_version": model_version,
                "raw_asset_policy": "raw_html_pdf_and_search_payloads_excluded_from_default_export",
                "migration_phase": "Phase_7c_public_web_asset_unification",
            },
        }
    )


def _upsert_canonical_public_web_person_evidence_from_signal_rows(
    store: Any,
    *,
    rows: list[dict[str, Any]],
    canonical_asset: dict[str, Any],
) -> int:
    canonical_asset_payload = dict(canonical_asset or {})
    canonical_asset_id = str(canonical_asset_payload.get("asset_id") or "").strip()
    if not canonical_asset_id:
        return 0
    written_count = 0
    for row in rows:
        signal = dict(row or {})
        signal_id = str(signal.get("signal_id") or "").strip()
        person_identity_key = str(signal.get("person_identity_key") or "").strip()
        if not signal_id or not person_identity_key:
            continue
        signal_kind = str(signal.get("signal_kind") or "").strip()
        signal_type = str(signal.get("signal_type") or "").strip()
        metadata = dict(signal.get("metadata") or {})
        evidence = store.upsert_person_evidence(
            {
                "evidence_id": f"person-evidence-public-web-{short_hash(signal_id)}",
                "person_identity_key": person_identity_key,
                "asset_id": canonical_asset_id,
                "evidence_type": "_".join(part for part in ("public_web", signal_kind, signal_type) if part),
                "value": str(signal.get("value") or "").strip(),
                "normalized_value": str(signal.get("normalized_value") or signal.get("value") or "").strip(),
                "source_url": str(signal.get("source_url") or signal.get("url") or "").strip(),
                "source_domain": str(signal.get("source_domain") or "").strip(),
                "confidence_score": _coerce_float(signal.get("confidence_score")),
                "identity_match_score": _coerce_float(signal.get("identity_match_score")),
                "publishable": bool(signal.get("publishable")),
                "evidence_excerpt": str(signal.get("evidence_excerpt") or "").strip(),
                "artifact_refs": dict(signal.get("artifact_refs") or {}),
                "status": "observed",
                "metadata": {
                    **metadata,
                    "schema_version": "public_web_person_evidence_v1",
                    "source_signal_id": signal_id,
                    "source_run_id": str(signal.get("run_id") or ""),
                    "source_family": str(signal.get("source_family") or ""),
                    "source_title": str(signal.get("source_title") or ""),
                    "confidence_label": str(signal.get("confidence_label") or ""),
                    "identity_match_label": str(signal.get("identity_match_label") or ""),
                    "promotion_status": str(signal.get("promotion_status") or ""),
                    "suppression_reason": str(signal.get("suppression_reason") or ""),
                    "migration_phase": "Phase_7c_public_web_asset_unification",
                    "raw_asset_policy": "raw_html_pdf_and_search_payloads_excluded_from_default_export",
                },
            }
        )
        if evidence:
            written_count += 1
    return written_count


def _upsert_person_asset_from_run(store: Any, run: dict[str, Any], *, signals: dict[str, Any]) -> dict[str, Any] | None:
    person_identity_key = str(run.get("person_identity_key") or "").strip()
    if not person_identity_key.startswith("linkedin:"):
        return None
    summary = dict(run.get("summary") or {})
    return store.upsert_person_public_web_asset(
        {
            "person_identity_key": person_identity_key,
            "linkedin_url_key": str(run.get("linkedin_url_key") or ""),
            "latest_run_id": str(run.get("run_id") or ""),
            "target_candidate_record_id": str(run.get("record_id") or ""),
            "candidate_name": str(run.get("candidate_name") or summary.get("candidate_name") or ""),
            "current_company": str(run.get("current_company") or summary.get("current_company") or ""),
            "status": str(run.get("status") or summary.get("status") or "completed"),
            "summary": summary,
            "signals": signals,
            "source_run_ids": [str(run.get("run_id") or "")],
            "artifact_root": str(run.get("artifact_root") or summary.get("artifact_root") or ""),
            "metadata": {
                "reuse_policy": "normalized_linkedin_url_key",
                "raw_assets_export_default": "excluded",
            },
        }
    )


def _model_identity_from_signals(signals: dict[str, Any]) -> tuple[str, str]:
    adjudication = dict(signals.get("ai_adjudication") or {})
    result = dict(adjudication.get("result") or {})
    model_provider = str(
        adjudication.get("provider")
        or result.get("provider")
        or result.get("model_provider")
        or result.get("provider_name")
        or ""
    ).strip()
    model_version = str(
        adjudication.get("effective_model")
        or result.get("effective_model")
        or adjudication.get("model_version")
        or adjudication.get("model")
        or result.get("model_version")
        or result.get("model")
        or ""
    ).strip()
    return model_provider, model_version


def _model_safe_common_artifact_refs(*, run: dict[str, Any], signals: dict[str, Any]) -> dict[str, Any]:
    artifact_root = str(run.get("artifact_root") or dict(run.get("summary") or {}).get("artifact_root") or "").strip()
    if not artifact_root:
        artifact_root = str(dict(signals.get("candidate") or {}).get("artifact_root") or "").strip()
    if not artifact_root:
        return {}
    root = Path(artifact_root)
    return {
        "candidate_summary_path": str(root / "candidate_summary.json"),
        "entry_links_path": str(root / "entry_links.json"),
        "signals_path": str(root / "signals.json"),
    }


def _artifact_refs_by_source_url(signals: dict[str, Any]) -> dict[str, dict[str, Any]]:
    refs_by_url: dict[str, dict[str, Any]] = {}
    for document in list(signals.get("fetched_documents") or []):
        if not isinstance(document, dict):
            continue
        refs = {
            key: str(document.get(key) or "").strip()
            for key in ("analysis_path", "evidence_slice_path")
            if str(document.get(key) or "").strip()
        }
        if not refs:
            continue
        for url_key in (
            normalize_public_web_url_key(str(document.get("source_url") or "")),
            normalize_public_web_url_key(str(document.get("final_url") or "")),
        ):
            if url_key:
                refs_by_url[url_key] = refs
    return refs_by_url


def _merge_artifact_refs(*refs: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    allowed = {
        "candidate_summary_path",
        "entry_links_path",
        "signals_path",
        "analysis_path",
        "evidence_slice_path",
    }
    for ref in refs:
        for key, value in dict(ref or {}).items():
            if key in allowed and str(value or "").strip():
                merged[key] = str(value or "").strip()
    return merged


def _domain_from_url(url: str) -> str:
    try:
        return urlparse(str(url or "")).netloc.lower()
    except Exception:
        return ""


def _truncate_signal_text(value: Any, *, limit: int) -> str:
    text = " ".join(str(value or "").strip().split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _coerce_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _load_json_from_path(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _default_run_artifact_root(runtime_dir: str | Path, run: dict[str, Any]) -> Path:
    batch_id = str(run.get("batch_id") or "unbatched")
    run_id = str(run.get("run_id") or "run")
    execution_backend = str(run.get("execution_backend") or "").strip()
    artifact_scope = (
        CRM_PUBLIC_WEB_OWNER.artifact_scope
        if execution_backend == CRM_PUBLIC_WEB_OWNER.execution_backend or run_id.startswith("crm-public-web-run-")
        else TARGET_PUBLIC_WEB_OWNER.artifact_scope
    )
    return Path(runtime_dir).expanduser() / "public_web" / artifact_scope / batch_id / "runs" / run_id


def _public_web_experiment_dir_for_artifact_root(artifact_root: Path) -> Path:
    root = Path(artifact_root).expanduser()
    if root.parent.name == "candidates" and root.name.startswith("01_"):
        return root.parent.parent
    return root.parent


def _plan_queries_for_context(
    context: PublicWebCandidateContext,
    *,
    options: PublicWebExperimentOptions,
) -> list[PublicWebQuerySpec]:
    return plan_candidate_public_web_queries(
        context,
        source_families=options.source_families,
        max_queries=options.max_queries_per_candidate,
    )


def _normalize_source_families(value: Any) -> list[str]:
    raw_items = [value] if isinstance(value, str) else list(value or [])
    allowed = set(DEFAULT_TARGET_CANDIDATE_SOURCE_FAMILIES)
    items: list[str] = []
    for raw_item in raw_items:
        item = str(raw_item or "").strip()
        if item in allowed and item not in items:
            items.append(item)
    return items or list(DEFAULT_TARGET_CANDIDATE_SOURCE_FAMILIES)


def _bounded_int(value: Any, *, default: int, low: int, high: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, low), high)


def _optional_positive_float(value: Any, *, low: float, high: float) -> float | None:
    if value in {None, ""}:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return min(max(parsed, low), high)


def _coerce_bool(value: Any, *, default: bool) -> bool:
    if value in {None, ""}:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default
