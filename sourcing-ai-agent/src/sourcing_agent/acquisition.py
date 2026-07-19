from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, replace
from datetime import datetime
from hashlib import sha1, sha256
from pathlib import Path
from typing import Any, Callable

from .agent_runtime import AgentRuntimeCoordinator
from .artifact_cache import mirror_tree_link_first
from .asset_catalog import AssetCatalog
from .asset_logger import AssetLogger
from .asset_paths import (
    canonical_company_assets_dir,
    hot_cache_company_assets_dir,
    resolve_snapshot_serving_artifact_path,
)
from .asset_reuse_planning import build_asset_reuse_baseline_selection_explanation
from .authoritative_candidates import load_authoritative_candidate_snapshot
from .candidate_artifacts import (
    CandidateArtifactError,
    build_company_candidate_artifacts,
    load_authoritative_company_snapshot_candidate_documents,
    load_snapshot_candidate_artifact_payload,
)
from .canonicalization import canonicalize_company_records
from .cohort_provider_compiler import (
    COHORT_CANONICAL_PROFILE_URL_FIELD,
    CohortHeadlineRoleProofVerifier,
    CohortProviderCompiler,
    cohort_execution_capability_for_runtime,
)
from .cohort_selection import explicit_cohort_selection
from .company_registry import upsert_company_identity_registry_entry
from .company_shard_planning import (
    build_request_scoped_company_employee_query_plan,
    normalize_company_employee_shard_policy,
    plan_company_employee_shards_from_policy,
    request_scoped_roster_function_ids,
)
from .connectors import (
    CompanyIdentity,
    CompanyRosterSnapshot,
    LinkedInCompanyRosterConnector,
    build_candidates_from_roster,
    load_rapidapi_accounts,
    profile_detail_accounts,
    resolve_company_identity,
    resolve_manual_company_identity,
)
from .domain import (
    AcquisitionTask,
    Candidate,
    EvidenceRecord,
    JobRequest,
    make_evidence_id,
    normalize_candidate,
    normalize_name_token,
)
from .enrichment import MultiSourceEnricher
from .harvest_connectors import (
    HarvestCompanyEmployeesConnector,
    HarvestProfileConnector,
    HarvestProfileSearchConnector,
    discover_legacy_harvest_token,
    harvest_connector_available,
    write_harvest_execution_artifact,
)
from .ingestion import load_bootstrap_bundle
from .model_provider import ModelClient
from .provider_execution_policy import normalize_former_member_search_contract
from .request_normalization import build_effective_request_payload, resolve_request_intent_view
from .runtime_tuning import (
    acquire_runtime_provider_limiter_slot,
    release_runtime_provider_limiter_slot,
    resolve_runtime_timing_overrides,
    resolved_harvest_company_roster_global_inflight,
    resolved_harvest_company_roster_parallel_shards,
    runtime_inflight_slot,
    runtime_provider_limiter_slot,
)
from .search_provider import SearchProviderError, build_search_provider
from .search_seed_registry import (
    COHORT_PUBLICATION_DIGEST_FIELD,
    cohort_publication_commit_digest_from_candidate_documents,
)
from .search_seed_registry import (
    dedupe_search_seed_entries as _registry_dedupe_search_seed_entries,
)
from .search_seed_registry import (
    dedupe_search_seed_records as _registry_dedupe_search_seed_records,
)
from .search_seed_registry import (
    load_search_seed_snapshot_from_snapshot_dir as _registry_load_search_seed_snapshot_from_snapshot_dir,
)
from .search_seed_registry import (
    merge_search_seed_snapshots as _registry_merge_search_seed_snapshots,
)
from .search_seed_registry import (
    persist_search_seed_snapshot as _registry_persist_search_seed_snapshot,
)
from .search_seed_registry import (
    project_search_seed_snapshot_to_candidate_documents as _registry_project_search_seed_snapshot_to_candidate_documents,
)
from .seed_discovery import (
    PROVIDER_SEARCH_RETRY_ITEM_KIND,
    SearchSeedAcquirer,
    SearchSeedSnapshot,
    build_candidates_from_seed_entries,
    build_candidates_from_seed_snapshot,
    collect_search_seed_provider_retry_items,
)
from .settings import AppSettings
from .snapshot_state import company_identity_from_record as _company_identity_from_record
from .snapshot_state import merge_background_reconcile_candidate as _merge_background_reconcile_candidate
from .storage import ControlPlaneStore

_FULL_ROSTER_BASELINE_REUSE_MIN_CANDIDATES = 1000
_FULL_ROSTER_BASELINE_REUSE_CURRENT_RATIO_THRESHOLD = 0.6
_FULL_ROSTER_BASELINE_REUSE_MIN_CURRENT_COUNT = 250
_FULL_ROSTER_BASELINE_REUSE_REFERENCE_DISTANCE_RATIO = 0.45
_FULL_ROSTER_BASELINE_REUSE_REFERENCE_DISTANCE_FLOOR = 250


def _read_company_asset_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _read_company_asset_json_list(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return [dict(item) for item in list(payload or []) if isinstance(item, dict)]


@dataclass(slots=True)
class AcquisitionExecution:
    task_id: str
    status: str
    detail: str
    payload: dict[str, Any]
    state_updates: dict[str, Any] = field(default_factory=dict)


def _build_cohort_execution_result(
    *,
    manifest: dict[str, Any],
    execution: dict[str, Any],
    entries: list[dict[str, Any]],
    lane_summaries: list[dict[str, Any]],
    execution_capability: dict[str, Any],
    artifact_path: Path,
    commit_marker_path: Path,
) -> dict[str, Any]:
    core = {
        "schema_version": "cohort_execution_result.v1",
        "provider": str(manifest.get("provider") or ""),
        "cohort_selection_digest": str(manifest.get("cohort_selection_digest") or ""),
        "cohort_provider_manifest_digest": str(manifest.get("manifest_digest") or ""),
        "result_digest": str(execution.get("result_digest") or ""),
        "candidate_count": len(entries),
        "truncated_count": int(execution.get("truncated_count") or 0),
        "rejected_unverified_count": int(execution.get("rejected_unverified_count") or 0),
        "missing_required_lane_count": int(execution.get("missing_required_lane_count") or 0),
        "lane_summaries": [dict(item) for item in lane_summaries],
        "execution_capability": dict(execution_capability),
    }
    publication_digest = sha256(
        json.dumps(
            core,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    return {
        **core,
        "artifact_path": str(artifact_path),
        "commit_marker_path": str(commit_marker_path),
        COHORT_PUBLICATION_DIGEST_FIELD: publication_digest,
        "publication_contract": {
            "schema_version": "cohort_publication_commit.v1",
            "state": "requires_candidate_documents_digest_match",
            "digest_field": COHORT_PUBLICATION_DIGEST_FIELD,
        },
    }


def _effective_cost_policy(
    task_cost_policy: dict[str, Any] | None,
    job_request: JobRequest | None = None,
) -> dict[str, Any]:
    effective = dict(task_cost_policy or {})
    execution_preferences = dict(getattr(job_request, "execution_preferences", {}) or {})
    if bool(execution_preferences.get("force_fresh_run")):
        effective["allow_cached_roster_fallback"] = False
        effective["allow_historical_profile_inheritance"] = False
        effective["allow_shared_provider_cache"] = False
    return effective


def _effective_request_payload(job_request: JobRequest | dict[str, Any]) -> dict[str, Any]:
    if isinstance(job_request, JobRequest):
        intent_view = resolve_request_intent_view(job_request)
        return build_effective_request_payload(job_request, intent_view=intent_view)
    return build_effective_request_payload(job_request)


def _runtime_timing_overrides_from_request_payload(request_payload: dict[str, Any] | None) -> dict[str, Any]:
    if not request_payload:
        return {}
    execution_preferences = dict(JobRequest.from_payload(dict(request_payload or {})).execution_preferences or {})
    return resolve_runtime_timing_overrides(execution_preferences)


def _apply_optional_runtime_timing_overrides(
    fn: Any,
    *args: Any,
    runtime_timing_overrides: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Any:
    if runtime_timing_overrides:
        try:
            return fn(*args, runtime_timing_overrides=runtime_timing_overrides, **kwargs)
        except TypeError as exc:
            if "runtime_timing_overrides" not in str(exc):
                raise
    return fn(*args, **kwargs)


def _dedupe_search_seed_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _registry_dedupe_search_seed_entries(entries)


def _candidate_like_record(candidate: Candidate | dict[str, Any]) -> dict[str, Any]:
    if isinstance(candidate, Candidate):
        return candidate.to_record()
    return dict(candidate or {})


def _evidence_like_record(evidence: EvidenceRecord | dict[str, Any]) -> dict[str, Any]:
    if isinstance(evidence, EvidenceRecord):
        return evidence.to_record()
    return dict(evidence or {})


def _stable_candidate_evidence_fingerprint(
    candidates: list[Candidate] | list[dict[str, Any]],
    evidence: list[EvidenceRecord] | list[dict[str, Any]],
) -> str:
    candidate_records = sorted(
        [_candidate_like_record(item) for item in list(candidates or [])],
        key=lambda item: (
            str(item.get("candidate_id") or "").strip(),
            str(item.get("linkedin_url") or "").strip(),
            json.dumps(item, ensure_ascii=False, sort_keys=True),
        ),
    )
    evidence_records = sorted(
        [_evidence_like_record(item) for item in list(evidence or [])],
        key=lambda item: (
            str(item.get("evidence_id") or "").strip(),
            str(item.get("candidate_id") or "").strip(),
            json.dumps(item, ensure_ascii=False, sort_keys=True),
        ),
    )
    payload = {
        "candidates": candidate_records,
        "evidence": evidence_records,
    }
    return sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _snapshot_materialization_artifacts_ready(snapshot_dir: Path) -> bool:
    required_paths = (
        snapshot_dir / "normalized_artifacts" / "manifest.json",
        snapshot_dir / "normalized_artifacts" / "artifact_summary.json",
        snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json",
        snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json",
        snapshot_dir / "retrieval_index_summary.json",
    )
    return all(path.exists() for path in required_paths)


def _snapshot_can_reuse_equivalent_baseline_materialization(snapshot_dir: Path) -> bool:
    if _snapshot_materialization_artifacts_ready(snapshot_dir):
        return True
    canonical_manifest_path = snapshot_dir / "normalized_artifacts" / "manifest.json"
    retrieval_index_path = snapshot_dir / "retrieval_index_summary.json"
    return canonical_manifest_path.exists() and retrieval_index_path.exists()


def _dedupe_search_seed_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _registry_dedupe_search_seed_records(records)


def _dedupe_string_values(values: list[Any]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _merge_profile_prefetch_dispatch_summaries(
    summaries: list[dict[str, Any]],
    *,
    requested_profile_urls: set[str] | None = None,
) -> dict[str, Any]:
    normalized_summaries = [
        dict(item or {}) for item in list(summaries or []) if isinstance(item, dict) and dict(item or {})
    ]
    if not normalized_summaries:
        return {"status": "skipped", "reason": "no_prefetch_dispatch"}

    requested_urls = {
        str(profile_url or "").strip()
        for profile_url in list(requested_profile_urls or set())
        if str(profile_url or "").strip()
    }
    queued_urls = _dedupe_string_values(
        [url for item in normalized_summaries for url in list(item.get("queued_urls") or [])]
    )
    failed_urls = _dedupe_string_values(
        [url for item in normalized_summaries for url in list(item.get("failed_urls") or [])]
    )
    summary_paths = _dedupe_string_values(
        [path for item in normalized_summaries for path in list(item.get("summary_paths") or [])]
    )
    batch_envelopes = [
        dict(envelope)
        for item in normalized_summaries
        for envelope in list(item.get("batch_envelopes") or [])
        if isinstance(envelope, dict)
    ]
    prefetch_events: list[dict[str, Any]] = []
    batch_plans: list[dict[str, Any]] = []
    queue_snapshots: list[dict[str, Any]] = []
    for event_index, item in enumerate(normalized_summaries, start=1):
        item_events = item.get("profile_prefetch_events")
        if isinstance(item_events, list) and item_events:
            for nested_event in item_events:
                if isinstance(nested_event, dict) and dict(nested_event or {}):
                    prefetch_events.append(dict(nested_event))
            continue
        event_batch_plans = [
            dict(plan)
            for plan in list(item.get("batch_plans") or item.get("profile_prefetch_batch_plans") or [])
            if isinstance(plan, dict) and dict(plan or {})
        ]
        single_batch_plan = dict(item.get("batch_plan") or item.get("profile_prefetch_batch_plan") or {})
        if single_batch_plan and not event_batch_plans:
            event_batch_plans = [single_batch_plan]
        event_queue_snapshots = [
            dict(snapshot)
            for snapshot in list(item.get("profile_prefetch_queues") or [])
            if isinstance(snapshot, dict) and dict(snapshot or {})
        ]
        single_queue_snapshot = dict(item.get("profile_prefetch_queue") or {})
        if single_queue_snapshot and not event_queue_snapshots:
            event_queue_snapshots = [single_queue_snapshot]
        event_payload = {
            "event_index": event_index,
            "status": str(item.get("status") or "").strip(),
            "reason": str(item.get("reason") or "").strip(),
            "requested_url_count": int(item.get("requested_url_count") or 0),
            "dispatched_url_count": int(item.get("dispatched_url_count") or 0),
            "cached_profile_count": int(item.get("cached_profile_count") or 0),
            "queued_worker_count": int(item.get("queued_worker_count") or 0),
            "deferred_url_count": int(item.get("deferred_url_count") or 0),
            "failed_url_count": len(list(item.get("failed_urls") or [])),
            "queued_url_count": len(list(item.get("queued_urls") or [])),
            "batch_plan_count": len(event_batch_plans),
            "profile_prefetch_queue_count": len(event_queue_snapshots),
            "batch_plans": event_batch_plans,
            "profile_prefetch_queues": event_queue_snapshots,
        }
        candidate_documents_projection = dict(item.get("candidate_documents_projection") or {})
        if candidate_documents_projection:
            event_payload["candidate_documents_projection"] = candidate_documents_projection
        prefetch_events.append(event_payload)
    for event in prefetch_events:
        batch_plans.extend(
            dict(plan)
            for plan in list(dict(event or {}).get("batch_plans") or [])
            if isinstance(plan, dict) and dict(plan or {})
        )
        queue_snapshots.extend(
            dict(snapshot)
            for snapshot in list(dict(event or {}).get("profile_prefetch_queues") or [])
            if isinstance(snapshot, dict) and dict(snapshot or {})
        )
    candidate_documents_projections = [
        dict(dict(event or {}).get("candidate_documents_projection") or {})
        for event in prefetch_events
        if dict(dict(event or {}).get("candidate_documents_projection") or {})
    ]
    errors = _dedupe_string_values([error for item in normalized_summaries for error in list(item.get("errors") or [])])
    queued_worker_count = sum(int(item.get("queued_worker_count") or 0) for item in normalized_summaries)
    cached_profile_count = sum(int(item.get("cached_profile_count") or 0) for item in normalized_summaries)
    dispatched_url_count = sum(int(item.get("dispatched_url_count") or 0) for item in normalized_summaries)
    # Fail-closed terminal bubble (decision #1 / invariant 7,
    # PROFILE_PREFETCH_SCHEDULER_CONTRACT.md:61): a blocked input summary
    # (store/infrastructure unavailable, queued_worker_count=0) must NOT merge
    # into "completed" (which would claim "all satisfied"). Blocked wins over
    # every other merged status and its two-signal shape is preserved so the
    # recovery chain can tell "infrastructure missing" from "nothing to do".
    blocked_urls_merged = _dedupe_string_values(
        [profile_url for item in normalized_summaries for profile_url in list(item.get("blocked_urls") or [])]
    )
    blocked_inputs = [
        item
        for item in normalized_summaries
        if str(item.get("status") or "").strip().lower() == "blocked" or bool(item.get("blocked"))
    ]
    merged_status = "queued" if queued_worker_count > 0 else "completed"
    merged_reason = ""
    if all(str(item.get("status") or "").strip().lower() == "skipped" for item in normalized_summaries):
        merged_status = "skipped"
        merged_reason = str(normalized_summaries[-1].get("reason") or "no_prefetch_dispatch").strip()
    if blocked_inputs:
        merged_status = "blocked"
        merged_reason = str(
            blocked_inputs[0].get("blocked_reason")
            or blocked_inputs[0].get("reason")
            or "profile_refill_store_unavailable"
        ).strip()

    merged_payload = {
        "status": merged_status,
        "requested_url_count": len(requested_urls)
        if requested_urls
        else max(int(item.get("requested_url_count") or 0) for item in normalized_summaries),
        "dispatched_url_count": dispatched_url_count,
        "cached_profile_count": cached_profile_count,
        "queued_worker_count": queued_worker_count,
        "queued_urls": queued_urls,
        "failed_urls": failed_urls,
        "summary_paths": summary_paths,
        "errors": errors,
        **(
            {
                "blocked": True,
                "blocked_url_count": len(blocked_urls_merged),
                "blocked_urls": blocked_urls_merged,
                "blocked_reason": merged_reason or "profile_refill_store_unavailable",
            }
            if blocked_inputs
            else {}
        ),
        "dispatch_event_count": len(normalized_summaries),
        "profile_prefetch_event_count": len(prefetch_events),
        "batch_plan_count": len(batch_plans),
        "profile_prefetch_queue_count": len(queue_snapshots),
        "profile_prefetch_events": prefetch_events,
        "candidate_documents_projection_count": len(candidate_documents_projections),
        "batch_envelopes": batch_envelopes,
        "tiny_batch_count": sum(1 for item in batch_envelopes if bool(item.get("is_tiny_batch"))),
        "unexplained_tiny_batch_count": sum(
            1
            for item in batch_envelopes
            if bool(item.get("is_tiny_batch")) and not bool(item.get("tiny_batch_allowed"))
        ),
        "provider_slot_underuse_with_backlog_count": sum(
            1 for item in batch_envelopes if bool(item.get("provider_slot_underuse_with_backlog"))
        ),
        "tiny_batch_coalesced_count": sum(
            int(item.get("tiny_batch_coalesced_count") or 0) for item in normalized_summaries
        ),
    }
    if batch_plans:
        merged_payload["latest_batch_plan"] = batch_plans[-1]
        merged_payload["batch_plan"] = batch_plans[-1]
        merged_payload["batch_plans"] = batch_plans
    if queue_snapshots:
        merged_payload["latest_profile_prefetch_queue"] = queue_snapshots[-1]
        merged_payload["profile_prefetch_queue"] = queue_snapshots[-1]
        merged_payload["profile_prefetch_queues"] = queue_snapshots
    if candidate_documents_projections:
        merged_payload["candidate_documents_projections"] = candidate_documents_projections
        merged_payload["latest_candidate_documents_projection"] = candidate_documents_projections[-1]
    if merged_reason:
        merged_payload["reason"] = merged_reason
    terminal_proof_summary = _profile_prefetch_terminal_proof_summary(
        normalized_summaries,
        requested_url_count=int(merged_payload.get("requested_url_count") or 0),
    )
    if terminal_proof_summary:
        merged_payload.update(terminal_proof_summary)
    return merged_payload


def _profile_prefetch_terminal_proof_summary(
    summaries: list[dict[str, Any]],
    *,
    requested_url_count: int,
) -> dict[str, Any]:
    terminal_summary: dict[str, Any] = {}
    terminal_queue: dict[str, Any] = {}
    for item in reversed([dict(summary or {}) for summary in list(summaries or []) if isinstance(summary, dict)]):
        candidate_terminal_summary = dict(item.get("registry_terminal_summary") or {})
        candidate_queue = dict(item.get("latest_profile_prefetch_queue") or item.get("profile_prefetch_queue") or {})
        all_requested_terminal = bool(
            candidate_terminal_summary.get("all_requested_terminal")
            or candidate_queue.get("registry_all_requested_terminal")
        )
        if not all_requested_terminal:
            continue
        terminal_summary = candidate_terminal_summary
        terminal_queue = candidate_queue
        break
    if not terminal_summary and not terminal_queue:
        return {}

    unrecoverable_count = int(
        terminal_summary.get("unrecoverable_url_count") or terminal_queue.get("registry_unrecoverable_url_count") or 0
    )
    fetched_count = int(
        terminal_summary.get("fetched_url_count") or terminal_queue.get("registry_fetched_url_count") or 0
    )
    terminal_count = int(
        terminal_summary.get("terminal_url_count")
        or terminal_queue.get("registry_terminal_url_count")
        or fetched_count + unrecoverable_count
        or 0
    )
    missing_count = int(
        terminal_summary.get("missing_url_count") or terminal_queue.get("registry_missing_url_count") or 0
    )
    open_count = int(terminal_summary.get("open_url_count") or terminal_queue.get("registry_open_url_count") or 0)
    reason = (
        "registry_all_requested_profiles_terminal"
        if unrecoverable_count > 0
        else "registry_all_requested_profiles_fetched"
    )
    queue = dict(terminal_queue)
    queue.update(
        {
            "queued_url_count": 0,
            "newly_queued_url_count": 0,
            "deferred_url_count": 0,
            "failed_url_count": 0,
            "pending_url_count": 0,
            "ready_after_dispatch_url_count": 0,
            "queue_quiescent": True,
            "local_queue_quiescent": True,
            "remote_queue_quiescent": True,
            "registry_all_requested_terminal": True,
        }
    )
    if requested_url_count > 0:
        queue.setdefault("requested_url_count", requested_url_count)
    return {
        "status": "completed",
        "reason": reason,
        "terminal_proof_only": True,
        "submit_anchor": False,
        "dispatched_url_count": 0,
        "queued_worker_count": 0,
        "active_worker_count": 0,
        "deferred_url_count": 0,
        "queued_urls": [],
        "failed_urls": [],
        "summary_paths": [],
        "batch_envelopes": [],
        "batch_plan": {},
        "latest_batch_plan": {},
        "batch_plans": [],
        "profile_prefetch_batch_plan": {},
        "profile_prefetch_batch_plans": [],
        "profile_prefetch_queue": queue,
        "latest_profile_prefetch_queue": queue,
        "registry_terminal_summary": {
            **terminal_summary,
            "all_requested_terminal": True,
            "terminal_url_count": terminal_count,
            "fetched_url_count": fetched_count,
            "unrecoverable_url_count": unrecoverable_count,
            "missing_url_count": missing_count,
            "open_url_count": open_count,
        },
        "tiny_batch_count": 0,
        "unexplained_tiny_batch_count": 0,
        "provider_slot_underuse_with_backlog_count": 0,
        "tiny_batch_coalesced_count": 0,
    }


def _persist_search_seed_snapshot(snapshot: SearchSeedSnapshot) -> SearchSeedSnapshot:
    return _registry_persist_search_seed_snapshot(snapshot)


def _merge_search_seed_snapshots(
    existing: SearchSeedSnapshot | None,
    incoming: SearchSeedSnapshot | None,
) -> SearchSeedSnapshot | None:
    return _registry_merge_search_seed_snapshots(existing, incoming)


def _search_seed_snapshot_lane_entries(snapshot: SearchSeedSnapshot | None, lane: str) -> list[dict[str, Any]]:
    if not isinstance(snapshot, SearchSeedSnapshot):
        return []
    normalized_lane = str(lane or "").strip().lower()
    if not normalized_lane:
        return []
    lane_entries = dict(snapshot.lane_entries or {})
    direct_entries = lane_entries.get(normalized_lane)
    if direct_entries:
        return [dict(item) for item in list(direct_entries or []) if isinstance(item, dict)]
    return [
        dict(entry)
        for entry in list(snapshot.entries or [])
        if isinstance(entry, dict)
        and str(entry.get("employment_status") or entry.get("employment_scope") or "").strip().lower()
        == normalized_lane
    ]


def _search_seed_incomplete_provider_query_count(snapshot: SearchSeedSnapshot | None) -> int:
    if not isinstance(snapshot, SearchSeedSnapshot):
        return 0
    summary_payload = dict(snapshot.summary_payload or {})
    try:
        summary_count = int(summary_payload.get("incomplete_provider_query_count") or 0)
    except (TypeError, ValueError):
        summary_count = 0
    provider_retry_items = collect_search_seed_provider_retry_items(snapshot)
    exhausted_retry_count = len(
        [
            item
            for item in provider_retry_items
            if str(item.get("status") or "").strip().lower() == "exhausted"
            or str(item.get("queue_status") or "").strip().lower() == "failed"
        ]
    )
    query_count = 0
    for summary in list(snapshot.query_summaries or []):
        if not isinstance(summary, dict):
            continue
        status = str(summary.get("status") or "").strip().lower()
        if (
            status in {"incomplete", "retry_wait"}
            or bool(summary.get("provider_search_incomplete"))
            or bool(summary.get("provider_search_retryable"))
        ):
            query_count += 1
    return max(summary_count, query_count, exhausted_retry_count)


def _snapshot_query_tokens_for_lane(snapshot: SearchSeedSnapshot | None, lane: str) -> set[str]:
    if not isinstance(snapshot, SearchSeedSnapshot):
        return set()
    normalized_lane = str(lane or "").strip().lower()
    observed: set[str] = set()
    for summary in list(snapshot.query_summaries or []):
        if not isinstance(summary, dict):
            continue
        summary_lane = (
            str(summary.get("employment_status") or summary.get("employment_scope") or summary.get("lane") or "")
            .strip()
            .lower()
        )
        if normalized_lane and summary_lane and summary_lane != normalized_lane:
            continue
        for key in ("query", "source_query", "effective_query_text", "seed_query"):
            value = str(summary.get(key) or "").strip().lower()
            if value:
                observed.add(value)
    for entry in _search_seed_snapshot_lane_entries(snapshot, normalized_lane):
        for key in ("source_query", "query", "seed_query"):
            value = str(entry.get(key) or "").strip().lower()
            if value:
                observed.add(value)
        metadata = dict(entry.get("metadata") or {})
        for key in ("source_query", "query", "seed_query"):
            value = str(metadata.get(key) or "").strip().lower()
            if value:
                observed.add(value)
    return observed


def _search_seed_snapshot_satisfies_lane_queries(
    snapshot: SearchSeedSnapshot | None,
    *,
    lane: str,
    expected_queries: list[str],
) -> bool:
    if not _search_seed_snapshot_lane_entries(snapshot, lane):
        return False
    normalized_expected = [
        str(query or "").strip().lower() for query in list(expected_queries or []) if str(query or "").strip()
    ]
    if not normalized_expected:
        return True
    observed = _snapshot_query_tokens_for_lane(snapshot, lane)
    if not observed:
        return False
    return all(
        any(expected == token or expected in token or token in expected for token in observed)
        for expected in normalized_expected
    )


class AcquisitionEngine:
    def __init__(
        self,
        catalog: AssetCatalog,
        settings: AppSettings,
        store: ControlPlaneStore,
        model_client: ModelClient,
        worker_runtime: AgentRuntimeCoordinator | None = None,
    ) -> None:
        self.catalog = catalog
        self.settings = settings
        self.store = store
        self.worker_runtime = worker_runtime
        self.company_assets_dir = canonical_company_assets_dir(settings.runtime_dir)
        self.company_assets_dir.mkdir(parents=True, exist_ok=True)
        self.hot_cache_company_assets_dir = hot_cache_company_assets_dir(settings.runtime_dir)
        if self.hot_cache_company_assets_dir is not None:
            self.hot_cache_company_assets_dir.mkdir(parents=True, exist_ok=True)
        self.accounts = load_rapidapi_accounts(settings.secrets_file, catalog.legacy_api_accounts)
        self.roster_connector = LinkedInCompanyRosterConnector(self.accounts)
        legacy_harvest_token = ""
        if Path(settings.project_root).resolve() == Path(catalog.project_root).resolve():
            legacy_harvest_token = discover_legacy_harvest_token(catalog.legacy_api_accounts)
        profile_scraper_settings = self._resolve_harvest_settings(
            settings.harvest.profile_scraper, legacy_harvest_token
        )
        profile_search_settings = self._resolve_harvest_settings(settings.harvest.profile_search, legacy_harvest_token)
        company_employees_settings = self._resolve_harvest_settings(
            settings.harvest.company_employees, legacy_harvest_token
        )
        harvest_profile_search_connector = HarvestProfileSearchConnector(profile_search_settings)
        self.harvest_profile_search_connector = harvest_profile_search_connector
        search_provider = build_search_provider(settings.search)
        self.model_client = model_client
        self.search_provider = search_provider
        self.harvest_company_connector = HarvestCompanyEmployeesConnector(company_employees_settings)
        self.search_seed_acquirer = SearchSeedAcquirer(
            self.accounts,
            model_client,
            harvest_search_connector=harvest_profile_search_connector,
            search_provider=search_provider,
        )
        self.multi_source_enricher = MultiSourceEnricher(
            catalog,
            self.accounts,
            HarvestProfileConnector(profile_scraper_settings),
            harvest_profile_search_connector,
            model_client,
            search_provider=search_provider,
            worker_runtime=worker_runtime,
            store=self.store,
        )
        self.inline_worker_completion_callback: Callable[[dict[str, Any]], None] | None = None

    def _reawaken_waiting_prerequisite_after_candidate_documents_write(
        self,
        *,
        job_id: str,
        snapshot: SearchSeedSnapshot | None,
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id or not isinstance(snapshot, SearchSeedSnapshot):
            return {"status": "skipped", "reason": "scope_missing"}
        candidate_doc_path = snapshot.snapshot_dir / "candidate_documents.json"
        if not candidate_doc_path.exists():
            return {
                "status": "skipped",
                "reason": "candidate_documents_missing",
                "snapshot_id": snapshot.snapshot_id,
                "candidate_doc_path": str(candidate_doc_path),
            }
        reawaken = getattr(self.store, "reawaken_waiting_prerequisite_job_materialization_items", None)
        if not callable(reawaken):
            return {"status": "skipped", "reason": "store_reawaken_helper_missing"}
        reawakened_count = int(
            reawaken(
                job_id=normalized_job_id,
                snapshot_id=snapshot.snapshot_id,
                item_kind="local_apply_closure",
                source="search_seed_candidate_documents_projection",
            )
            or 0
        )
        return {
            "status": "completed",
            "snapshot_id": snapshot.snapshot_id,
            "candidate_doc_path": str(candidate_doc_path),
            "reawakened_count": reawakened_count,
        }

    def _project_incremental_search_seed_candidate_documents(
        self,
        *,
        job_id: str,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        entries: list[dict[str, Any]],
        summary: dict[str, Any],
        raw_path: str,
        employment_status: str,
    ) -> dict[str, Any]:
        """Write candidate shell rows before dispatching profile prefetch.

        Incremental search-seed results are allowed to feed the profile scheduler
        before the full `discover()` call returns. The matching candidate shell
        rows must therefore be projected first; otherwise a fast profile batch can
        complete while `candidate_documents.json` still lacks its URLs and local
        apply falls back to `waiting_prerequisite` timer recovery.
        """

        normalized_entries = [dict(item) for item in list(entries or []) if isinstance(item, dict)]
        if not normalized_entries:
            return {"status": "skipped", "reason": "no_incremental_entries"}
        discovery_dir = snapshot_dir / "search_seed_discovery"
        summary_path = discovery_dir / "summary.json"
        entries_path = (
            Path(str(raw_path or "")).expanduser() if str(raw_path or "").strip() else discovery_dir / "entries.json"
        )
        query_summary = dict(summary or {})
        if raw_path and not str(query_summary.get("raw_path") or "").strip():
            query_summary["raw_path"] = str(raw_path)
        incremental_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=normalized_entries,
            query_summaries=[query_summary] if query_summary else [],
            accounts_used=[],
            errors=[],
            stop_reason="incremental_query_result",
            summary_path=summary_path,
            entries_path=entries_path,
            summary_payload={
                "snapshot_id": snapshot_dir.name,
                "target_company": identity.canonical_name,
                "company_identity": identity.to_record(),
                "entry_count": len(normalized_entries),
                "query_summaries": [query_summary] if query_summary else [],
                "stop_reason": "incremental_query_result",
            },
            lane_payloads={
                str(employment_status or "all").strip() or "all": {
                    "employment_scope": str(employment_status or "all").strip() or "all",
                    "employment_status": str(employment_status or "all").strip() or "all",
                    "query_summaries": [query_summary] if query_summary else [],
                }
            },
            lane_entries={str(employment_status or "all").strip() or "all": normalized_entries},
        )
        projection = _registry_project_search_seed_snapshot_to_candidate_documents(
            incremental_snapshot,
            source_kind="search_seed_discovery:incremental_candidate_documents_projection",
            reason="incremental_query_result_before_profile_prefetch",
        )
        reawakened = self._reawaken_waiting_prerequisite_after_candidate_documents_write(
            job_id=job_id,
            snapshot=incremental_snapshot,
        )
        return {
            **dict(projection or {}),
            "prerequisite_ready_event": reawakened,
            "reawakened_count": int(dict(reawakened or {}).get("reawakened_count") or 0),
        }

    def _resolve_harvest_settings(self, actor_settings: Any, legacy_harvest_token: str) -> Any:
        if getattr(actor_settings, "enabled", False):
            return actor_settings
        if not legacy_harvest_token:
            return actor_settings
        return replace(actor_settings, enabled=True, api_token=legacy_harvest_token)

    def detect_reusable_roster_snapshot(self, target_company: str) -> dict[str, Any] | None:
        identity = resolve_company_identity(
            target_company,
            self.catalog.legacy_company_ids,
            model_client=self.model_client,
        )
        snapshot = self._load_cached_roster_snapshot(identity)
        if snapshot is None:
            return None
        source_kind = (
            "harvest_company_employees"
            if snapshot.summary_path.parent.name == "harvest_company_employees"
            else "linkedin_company_people"
        )
        return {
            "target_company": identity.canonical_name,
            "company_key": identity.company_key,
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_dir": str(snapshot.snapshot_dir),
            "visible_entry_count": len(snapshot.visible_entries),
            "headless_entry_count": len(snapshot.headless_entries),
            "source_kind": source_kind,
            "summary_path": str(snapshot.summary_path),
            "stop_reason": snapshot.stop_reason,
        }

    def execute_task(
        self,
        task: AcquisitionTask,
        job_request: JobRequest,
        target_company: str,
        state: dict[str, Any],
        bootstrap_summary: dict[str, Any] | None = None,
    ) -> AcquisitionExecution:
        if task.status == "needs_input":
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Target company is missing, so acquisition cannot proceed.",
                payload={},
            )

        company_key = target_company.strip().lower()
        use_local_anthropic_assets = self._should_use_local_anthropic_assets(job_request)
        if task.task_type == "resolve_company_identity":
            return self._resolve_company(target_company, task, state, job_request)
        explicit_cohort = explicit_cohort_selection(_effective_request_payload(job_request))
        explicit_cohort_is_authoritative = bool(
            explicit_cohort is not None and str(explicit_cohort.get("source") or "") == "user_explicit"
        )
        if explicit_cohort_is_authoritative and task.task_type == "acquire_full_roster":
            # Explicit cohort acquisition owns the entire physical provider
            # route. Mutable/legacy strategy metadata cannot redirect this
            # task into company-roster or investor-roster connectors.
            return self._acquire_search_seed_pool(task, state, job_request)
        if explicit_cohort_is_authoritative and task.task_type == "acquire_former_search_seed":
            # New plans omit this legacy task. Hydrated historical plans may
            # only reuse a verified result from the already-executed full
            # manifest; _acquire_former_search_seed never recompiles a narrow
            # task-local manifest for an explicit cohort.
            return self._acquire_former_search_seed(task, state, job_request)
        strategy_type = self._task_strategy_type(task, job_request)
        if (
            company_key == "anthropic"
            and use_local_anthropic_assets
            and task.task_type
            in {
                "acquire_full_roster",
                "normalize_asset_snapshot",
                "build_retrieval_index",
            }
        ):
            self._ensure_anthropic_local_candidate_documents(
                task=task,
                state=state,
                job_request=job_request,
                bootstrap_summary=bootstrap_summary or {},
            )
        if task.task_type == "acquire_full_roster":
            strategy_type = strategy_type or "full_company_roster"
            if strategy_type == "full_company_roster":
                if company_key == "anthropic" and use_local_anthropic_assets:
                    return self._anthropic_local_asset_task(task, bootstrap_summary or {}, state)
                return self._acquire_full_roster(task, state, job_request)
            if strategy_type in {"scoped_search_roster", "former_employee_search"}:
                return self._acquire_search_seed_pool(task, state, job_request)
            if strategy_type == "investor_firm_roster":
                return self._acquire_investor_firm_roster(task, state, job_request)
            return self._acquire_full_roster(task, state, job_request)
        if task.task_type == "acquire_former_search_seed":
            return self._acquire_former_search_seed(task, state, job_request)
        if company_key == "anthropic" and strategy_type != "investor_firm_roster" and use_local_anthropic_assets:
            if task.task_type in {
                "enrich_profiles_multisource",
                "enrich_linkedin_profiles",
                "enrich_public_web_signals",
            }:
                materialized_local_reuse = self._materialize_local_reuse_candidate_documents(
                    task=task,
                    state=state,
                    job_request=job_request,
                )
                if materialized_local_reuse is not None:
                    return materialized_local_reuse
            if task.task_type == "normalize_asset_snapshot" and (
                list(state.get("candidates") or [])
                or list(state.get("evidence") or [])
                or isinstance(state.get("candidate_doc_path"), Path)
            ):
                return self._normalize_snapshot(task, state, job_request)
            if task.task_type == "build_retrieval_index" and (
                list(state.get("candidates") or [])
                or list(state.get("evidence") or [])
                or isinstance(state.get("candidate_doc_path"), Path)
            ):
                return self._build_retrieval_index(task, state)
            return self._anthropic_local_asset_task(task, bootstrap_summary or {}, state)
        if task.task_type in {"enrich_profiles_multisource", "enrich_linkedin_profiles", "enrich_public_web_signals"}:
            return self._enrich_profiles(task, state, job_request)
        if task.task_type == "normalize_asset_snapshot":
            return self._normalize_snapshot(task, state, job_request)
        if task.task_type == "build_retrieval_index":
            return self._build_retrieval_index(task, state)
        return AcquisitionExecution(
            task_id=task.task_id,
            status="blocked",
            detail="No connector is implemented yet for this acquisition step.",
            payload={"task_type": task.task_type, "target_company": target_company},
        )

    def _should_use_local_anthropic_assets(self, job_request: JobRequest) -> bool:
        intent_view = resolve_request_intent_view(job_request)
        execution_preferences = dict(intent_view.get("execution_preferences") or {})
        if bool(execution_preferences.get("allow_local_bootstrap_fallback")):
            return True
        return not bool(execution_preferences.get("force_fresh_run"))

    def _resolve_company(
        self,
        target_company: str,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
    ) -> AcquisitionExecution:
        identity = resolve_manual_company_identity(
            target_company,
            job_request.execution_preferences,
            legacy_company_ids_path=self.catalog.legacy_company_ids,
        )
        observed_companies: list[dict[str, Any]] = []
        if identity is None:
            identity = resolve_company_identity(target_company, self.catalog.legacy_company_ids)
        if (
            identity.resolver != "manual_review_override"
            and identity.confidence == "low"
            and not identity.local_asset_available
        ):
            observed_companies = self._discover_company_identity_candidates(target_company)
            if observed_companies:
                upgraded = resolve_company_identity(
                    target_company,
                    self.catalog.legacy_company_ids,
                    model_client=self.model_client,
                    observed_companies=observed_companies,
                )
                if upgraded.resolver != "heuristic" or upgraded.confidence != "low":
                    upgraded.metadata = {
                        **dict(upgraded.metadata or {}),
                        "observed_company_candidates": observed_companies,
                    }
                    identity = upgraded
                else:
                    identity.metadata = {
                        **dict(identity.metadata or {}),
                        "observed_company_candidates": observed_companies,
                    }
        if not identity.linkedin_slug and not identity.local_asset_available:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=f"Could not resolve a usable company identity for {target_company}.",
                payload={"target_company": target_company},
            )

        snapshot_id, snapshot_dir = self._ensure_snapshot_dir(identity, state)
        identity_path = snapshot_dir / "identity.json"
        AssetLogger(snapshot_dir).write_json(
            identity_path,
            identity.to_record(),
            asset_type="company_identity",
            source_kind="resolve_company_identity",
            is_raw_asset=False,
            model_safe=True,
        )
        identity_registry_refresh = upsert_company_identity_registry_entry(
            runtime_dir=self.settings.runtime_dir,
            identity_payload=identity.to_record(),
            snapshot_dir=snapshot_dir,
        )

        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=f"Resolved {identity.canonical_name} to LinkedIn slug '{identity.linkedin_slug or 'local-only'}'.",
            payload={
                "company_identity": identity.to_record(),
                "snapshot_id": snapshot_id,
                "snapshot_dir": str(snapshot_dir),
                "identity_path": str(identity_path),
                "identity_registry_refresh": identity_registry_refresh,
                "observed_company_candidate_count": len(observed_companies),
            },
            state_updates={
                "company_identity": identity,
                "snapshot_id": snapshot_id,
                "snapshot_dir": snapshot_dir,
            },
        )

    def _anthropic_local_asset_task(
        self,
        task: AcquisitionTask,
        bootstrap_summary: dict[str, Any],
        state: dict[str, Any],
    ) -> AcquisitionExecution:
        identity = state.get("company_identity")
        if not isinstance(identity, CompanyIdentity):
            identity = resolve_company_identity("Anthropic", self.catalog.legacy_company_ids)
        detail = _anthropic_detail(task.task_type, bootstrap_summary)
        snapshot_dir = state.get("snapshot_dir")
        if isinstance(snapshot_dir, Path):
            logger = AssetLogger(snapshot_dir)
            local_summary_path = snapshot_dir / "local_asset_summary.json"
            logger.write_json(
                local_summary_path,
                {
                    "company_identity": identity.to_record(),
                    "bootstrap_summary": bootstrap_summary,
                    "detail": detail,
                    "task_type": task.task_type,
                },
                asset_type="local_asset_summary",
                source_kind="anthropic_local_asset",
                is_raw_asset=False,
                model_safe=True,
            )
        state_updates: dict[str, Any] = {"company_identity": identity}
        if isinstance(snapshot_dir, Path):
            candidate_doc_path = snapshot_dir / "candidate_documents.json"
            linkedin_stage_candidate_doc_path = snapshot_dir / "candidate_documents.linkedin_stage_1.json"
            public_web_stage_candidate_doc_path = snapshot_dir / "candidate_documents.public_web_stage_2.json"
            if candidate_doc_path.exists():
                state_updates["candidate_doc_path"] = candidate_doc_path
            if task.task_type in {"enrich_profiles_multisource", "enrich_linkedin_profiles"}:
                state_updates["linkedin_stage_completed"] = True
                if linkedin_stage_candidate_doc_path.exists():
                    state_updates["linkedin_stage_candidate_doc_path"] = linkedin_stage_candidate_doc_path
                elif candidate_doc_path.exists():
                    state_updates["linkedin_stage_candidate_doc_path"] = candidate_doc_path
            if task.task_type == "enrich_public_web_signals":
                state_updates["public_web_stage_completed"] = True
                if public_web_stage_candidate_doc_path.exists():
                    state_updates["public_web_stage_candidate_doc_path"] = public_web_stage_candidate_doc_path
                elif candidate_doc_path.exists():
                    state_updates["public_web_stage_candidate_doc_path"] = candidate_doc_path
        candidates = list(state.get("candidates") or [])
        evidence = list(state.get("evidence") or [])
        if candidates:
            state_updates["candidates"] = candidates
        if evidence:
            state_updates["evidence"] = evidence
        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=detail,
            payload={
                "company_identity": identity.to_record(),
                "bootstrap_summary": bootstrap_summary,
            },
            state_updates=state_updates,
        )

    def _ensure_anthropic_local_candidate_documents(
        self,
        *,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
        bootstrap_summary: dict[str, Any],
    ) -> None:
        snapshot_dir = state.get("snapshot_dir")
        if not isinstance(snapshot_dir, Path):
            return
        identity = state.get("company_identity")
        if not isinstance(identity, CompanyIdentity):
            identity = resolve_company_identity("Anthropic", self.catalog.legacy_company_ids)
            state["company_identity"] = identity
        if normalize_name_token(identity.canonical_name) != normalize_name_token("Anthropic"):
            return

        candidate_doc_path = state.get("candidate_doc_path")
        if not isinstance(candidate_doc_path, Path):
            candidate_doc_path = snapshot_dir / "candidate_documents.json"
        if candidate_doc_path.exists():
            candidates, evidence = _load_snapshot_candidate_payload(candidate_doc_path, identity.canonical_name)
            if candidates or evidence:
                state["candidate_doc_path"] = candidate_doc_path
                if candidates and not list(state.get("candidates") or []):
                    state["candidates"] = candidates
                if evidence and not list(state.get("evidence") or []):
                    state["evidence"] = evidence
                return

        bundle = load_bootstrap_bundle(self.catalog)
        candidates = [
            normalize_candidate(candidate)
            for candidate in bundle.candidates
            if normalize_name_token(candidate.target_company) == normalize_name_token(identity.canonical_name)
        ]
        if not candidates:
            return
        candidate_ids = {candidate.candidate_id for candidate in candidates}
        evidence = [item for item in bundle.evidence if item.candidate_id in candidate_ids]
        bootstrap_assets = dict(bootstrap_summary.get("asset_paths") or {})
        if not bootstrap_assets:
            bootstrap_assets = dict(bundle.stats.get("assets") or {})
        bootstrap_breakdown = dict(bootstrap_summary.get("candidate_breakdown") or {})
        if not bootstrap_breakdown:
            bootstrap_breakdown = dict(bundle.stats.get("candidate_counts") or {})

        logger = AssetLogger(snapshot_dir)
        logger.write_json(
            candidate_doc_path,
            {
                "snapshot": {
                    "snapshot_id": str(state.get("snapshot_id") or snapshot_dir.name),
                    "target_company": identity.canonical_name,
                    "company_identity": identity.to_record(),
                },
                "acquisition_sources": {
                    "anthropic_local_bootstrap_bundle": {
                        "asset_paths": bootstrap_assets,
                        "candidate_breakdown": bootstrap_breakdown,
                        "evidence_count": int(
                            bootstrap_summary.get("evidence_count")
                            or bundle.stats.get("evidence_count")
                            or len(evidence)
                        ),
                    }
                },
                "candidates": [candidate.to_record() for candidate in candidates],
                "evidence": [item.to_record() for item in evidence],
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "enrichment_mode": "local_bootstrap_reuse",
                "enrichment_scope": "bootstrap_local_assets",
                "enrichment_summary": {
                    "status": "local_bootstrap_materialized",
                    "resolved_profile_count": 0,
                    "publication_match_count": 0,
                    "lead_candidate_count": 0,
                    "artifact_paths": [],
                },
                "acquisition_stage": {
                    **self._task_execution_phase(task, job_request),
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                },
            },
            asset_type="candidate_documents",
            source_kind="anthropic_local_bootstrap_bundle",
            is_raw_asset=False,
            model_safe=True,
        )
        state["candidate_doc_path"] = candidate_doc_path
        state["candidates"] = candidates
        state["evidence"] = evidence

    def _materialize_local_reuse_candidate_documents(
        self,
        *,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
    ) -> AcquisitionExecution | None:
        identity = state.get("company_identity")
        snapshot_dir = state.get("snapshot_dir")
        if not isinstance(identity, CompanyIdentity) or not isinstance(snapshot_dir, Path):
            return None
        enrichment_scope = self._task_enrichment_scope(task, job_request)
        stage_source_kind = (
            "linkedin_profile_enrichment"
            if enrichment_scope == "linkedin_stage_1"
            else ("public_web_enrichment" if enrichment_scope == "public_web_stage_2" else "multisource_enrichment")
        )
        stage_mode = (
            "linkedin_stage_1"
            if enrichment_scope == "linkedin_stage_1"
            else ("linkedin_plus_public_web" if enrichment_scope == "public_web_stage_2" else "roster_plus_multisource")
        )
        stage_archive_path = (
            snapshot_dir / "candidate_documents.linkedin_stage_1.json"
            if enrichment_scope == "linkedin_stage_1"
            else (
                snapshot_dir / "candidate_documents.public_web_stage_2.json"
                if enrichment_scope == "public_web_stage_2"
                else None
            )
        )

        source_snapshots: dict[str, Any] = {}
        candidates: list[Candidate] = []
        evidence: list[EvidenceRecord] = []
        canonicalization_summary: dict[str, Any] = {}

        current_candidate_doc_path = state.get("candidate_doc_path")
        if isinstance(current_candidate_doc_path, Path) and current_candidate_doc_path.exists():
            candidates, evidence = _load_snapshot_candidate_payload(current_candidate_doc_path, identity.canonical_name)
            source_snapshots["current_snapshot_candidate_documents"] = {
                "candidate_doc_path": str(current_candidate_doc_path),
                "snapshot_id": str(state.get("snapshot_id") or snapshot_dir.name),
            }
        else:
            delta_baseline_snapshot_id = self._delta_baseline_snapshot_id(task, job_request)
            delta_baseline_material = self._load_delta_baseline_material(
                job_request,
                baseline_snapshot_id=delta_baseline_snapshot_id,
                enrichment_scope="linkedin_stage_1" if enrichment_scope == "linkedin_stage_1" else "public_web_stage_2",
            )
            baseline_document_payload = dict(delta_baseline_material.get("candidate_document_payload") or {})
            for item in list(baseline_document_payload.get("candidates") or []):
                candidate = _candidate_from_payload(item)
                if candidate is None:
                    continue
                if normalize_name_token(candidate.target_company) != normalize_name_token(identity.canonical_name):
                    continue
                candidates.append(candidate)
            baseline_candidate_ids = {candidate.candidate_id for candidate in candidates}
            for item in list(baseline_document_payload.get("evidence") or []):
                evidence_record = _evidence_record_from_payload(item)
                if evidence_record is None:
                    continue
                if baseline_candidate_ids and evidence_record.candidate_id not in baseline_candidate_ids:
                    continue
                evidence.append(evidence_record)
            if candidates or evidence:
                source_snapshots["delta_baseline_snapshot"] = {
                    "snapshot_id": str(delta_baseline_material.get("snapshot_id") or delta_baseline_snapshot_id),
                    "snapshot_dir": str(delta_baseline_material.get("snapshot_dir") or ""),
                    "source_candidate_doc_path": str(delta_baseline_material.get("source_candidate_doc_path") or ""),
                }
            search_seed_snapshot = state.get("search_seed_snapshot")
            if isinstance(search_seed_snapshot, SearchSeedSnapshot):
                search_candidates, search_evidence = build_candidates_from_seed_snapshot(search_seed_snapshot)
                if search_candidates or search_evidence:
                    candidates.extend(search_candidates)
                    evidence.extend(search_evidence)
                    source_snapshots["search_seed_snapshot"] = search_seed_snapshot.to_record()
            if len(candidates) > 1:
                candidates, evidence, canonicalization_summary = canonicalize_company_records(candidates, evidence)

        if not candidates and not evidence:
            return None

        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        payload = {
            "snapshot": source_snapshots.get("delta_baseline_snapshot")
            or source_snapshots.get("current_snapshot_candidate_documents")
            or source_snapshots.get("search_seed_snapshot")
            or {},
            "acquisition_sources": source_snapshots,
            "candidates": [candidate.to_record() for candidate in candidates],
            "evidence": [item.to_record() for item in evidence],
            "candidate_count": len(candidates),
            "evidence_count": len(evidence),
            "enrichment_mode": stage_mode,
            "enrichment_scope": enrichment_scope,
            "acquisition_canonicalization": canonicalization_summary,
            "enrichment_summary": {
                "status": "local_reuse_materialized",
                "resolved_profile_count": 0,
                "publication_match_count": 0,
                "lead_candidate_count": 0,
                "artifact_paths": [],
            },
            "acquisition_stage": {
                **self._task_execution_phase(task, job_request),
                "task_id": task.task_id,
                "task_type": task.task_type,
            },
        }
        logger = AssetLogger(snapshot_dir)
        logger.write_json(
            candidate_doc_path,
            payload,
            asset_type="candidate_documents",
            source_kind=f"{stage_source_kind}:local_reuse_materialized",
            is_raw_asset=False,
            model_safe=True,
        )
        if isinstance(stage_archive_path, Path):
            logger.write_json(
                stage_archive_path,
                payload,
                asset_type="candidate_documents",
                source_kind=f"{stage_source_kind}:local_reuse_materialized",
                is_raw_asset=False,
                model_safe=True,
            )

        state_updates: dict[str, Any] = {
            "candidates": candidates,
            "evidence": evidence,
            "candidate_doc_path": candidate_doc_path,
        }
        if enrichment_scope == "linkedin_stage_1":
            state_updates["linkedin_stage_completed"] = True
            state_updates["linkedin_stage_candidate_doc_path"] = (
                stage_archive_path if isinstance(stage_archive_path, Path) else candidate_doc_path
            )
        elif enrichment_scope == "public_web_stage_2":
            state_updates["public_web_stage_completed"] = True
            state_updates["public_web_stage_candidate_doc_path"] = (
                stage_archive_path if isinstance(stage_archive_path, Path) else candidate_doc_path
            )

        detail = (
            "Materialized LinkedIn stage-1 candidate documents from local baseline reuse."
            if enrichment_scope == "linkedin_stage_1"
            else (
                "Materialized Public Web stage-2 candidate documents from local baseline reuse."
                if enrichment_scope == "public_web_stage_2"
                else "Materialized candidate documents from local baseline reuse."
            )
        )
        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=detail,
            payload={
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "candidate_doc_path": str(candidate_doc_path),
                "stage_archive_path": str(stage_archive_path or ""),
                "enrichment_mode": stage_mode,
                "enrichment_scope": enrichment_scope,
                "local_reuse_materialized": True,
            },
            state_updates=state_updates,
        )

    def _delta_baseline_snapshot_id(self, task: AcquisitionTask, job_request: JobRequest) -> str:
        delta_execution_plan = self._task_delta_execution_plan(task, job_request)
        planned_snapshot_id = str(delta_execution_plan.get("baseline_snapshot_id") or "").strip()
        if planned_snapshot_id:
            return planned_snapshot_id
        asset_reuse_plan = self._task_asset_reuse_plan(task, job_request)
        planned_snapshot_id = str(asset_reuse_plan.get("baseline_snapshot_id") or "").strip()
        if planned_snapshot_id:
            return planned_snapshot_id
        task_snapshot_id = self._task_execution_str(task, job_request, "baseline_snapshot_id")
        if task_snapshot_id:
            return task_snapshot_id
        execution_preferences = dict(getattr(job_request, "execution_preferences", {}) or {})
        return str(execution_preferences.get("delta_baseline_snapshot_id") or "").strip()

    def _equivalent_delta_baseline_snapshot(
        self,
        *,
        task: AcquisitionTask,
        job_request: JobRequest | None,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        candidates: list[Candidate],
        evidence: list[EvidenceRecord],
    ) -> dict[str, Any]:
        if job_request is None:
            return {}
        execution_preferences = dict(getattr(job_request, "execution_preferences", {}) or {})
        preferred_snapshot_ids = [
            str(item or "").strip()
            for item in list(execution_preferences.get("delta_baseline_snapshot_ids") or [])
            if str(item or "").strip()
        ]
        delta_baseline_snapshot_id = self._delta_baseline_snapshot_id(task, job_request)
        if delta_baseline_snapshot_id and delta_baseline_snapshot_id not in preferred_snapshot_ids:
            preferred_snapshot_ids.append(delta_baseline_snapshot_id)
        if not preferred_snapshot_ids:
            return {}

        current_fingerprint = _stable_candidate_evidence_fingerprint(candidates, evidence)
        company_dir = self.settings.company_assets_dir / identity.company_key
        for baseline_snapshot_id in preferred_snapshot_ids:
            if not baseline_snapshot_id or baseline_snapshot_id == snapshot_dir.name:
                continue
            baseline_snapshot_dir = company_dir / baseline_snapshot_id
            baseline_candidate_doc_path = baseline_snapshot_dir / "candidate_documents.json"
            baseline_manifest_path = baseline_snapshot_dir / "manifest.json"
            if (
                not baseline_candidate_doc_path.exists()
                or not baseline_manifest_path.exists()
                or not _snapshot_can_reuse_equivalent_baseline_materialization(baseline_snapshot_dir)
            ):
                continue
            try:
                baseline_payload = json.loads(baseline_candidate_doc_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            baseline_fingerprint = _stable_candidate_evidence_fingerprint(
                list(baseline_payload.get("candidates") or []),
                list(baseline_payload.get("evidence") or []),
            )
            if baseline_fingerprint != current_fingerprint:
                continue
            return {
                "snapshot_id": baseline_snapshot_id,
                "snapshot_dir": baseline_snapshot_dir,
                "candidate_doc_path": baseline_candidate_doc_path,
                "manifest_path": baseline_manifest_path,
                "normalized_artifact_dir": baseline_snapshot_dir / "normalized_artifacts",
                "retrieval_index_summary_path": baseline_snapshot_dir / "retrieval_index_summary.json",
            }
        return {}

    def _build_equivalent_delta_baseline_execution(
        self,
        *,
        task: AcquisitionTask,
        identity: CompanyIdentity,
        candidates: list[Candidate] | list[dict[str, Any]],
        evidence: list[EvidenceRecord] | list[dict[str, Any]],
        equivalent_baseline_snapshot: dict[str, Any],
        inheritance_summary: dict[str, Any] | None = None,
        canonicalization_summary: dict[str, Any] | None = None,
    ) -> AcquisitionExecution:
        baseline_snapshot_dir = Path(equivalent_baseline_snapshot["snapshot_dir"])
        baseline_snapshot_id = str(equivalent_baseline_snapshot.get("snapshot_id") or "").strip()
        baseline_candidate_doc_path = Path(equivalent_baseline_snapshot["candidate_doc_path"])
        baseline_manifest_path = Path(equivalent_baseline_snapshot["manifest_path"])
        baseline_artifact_dir = Path(equivalent_baseline_snapshot["normalized_artifact_dir"])
        baseline_retrieval_index_path = Path(equivalent_baseline_snapshot["retrieval_index_summary_path"])
        detail = (
            f"Persisted {len(candidates)} candidates for {identity.canonical_name} by reusing equivalent "
            f"baseline snapshot {baseline_snapshot_id} instead of rebuilding artifacts."
        )
        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=detail,
            payload={
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "candidate_doc_path": str(baseline_candidate_doc_path),
                "manifest_path": str(baseline_manifest_path),
                "historical_profile_inheritance": dict(inheritance_summary or {}),
                "canonicalization": dict(canonicalization_summary or {}),
                "artifact_materialization_status": "reused_equivalent_snapshot",
                "artifact_materialization_error": "",
                "artifact_dir": str(baseline_artifact_dir),
                "artifact_paths": {
                    "manifest": str(baseline_artifact_dir / "manifest.json"),
                    "artifact_summary": str(baseline_artifact_dir / "artifact_summary.json"),
                    "strict_manifest": str(baseline_artifact_dir / "strict_roster_only" / "manifest.json"),
                    "strict_artifact_summary": str(
                        baseline_artifact_dir / "strict_roster_only" / "artifact_summary.json"
                    ),
                    "retrieval_index_summary": str(baseline_retrieval_index_path),
                },
                "sync_status": {
                    "status": "reused_equivalent_snapshot",
                    "snapshot_id": baseline_snapshot_id,
                    "normalized_artifact_dir": str(baseline_artifact_dir),
                },
                "hot_cache_sync": {
                    "status": "reused_equivalent_snapshot",
                    "snapshot_dir": str(baseline_snapshot_dir),
                },
                "reused_equivalent_snapshot_id": baseline_snapshot_id,
                "equivalent_baseline_reused": True,
            },
            state_updates={
                "snapshot_id": baseline_snapshot_id,
                "snapshot_dir": baseline_snapshot_dir,
                "candidates": candidates,
                "evidence": evidence,
                "candidate_doc_path": baseline_candidate_doc_path,
                "manifest_path": baseline_manifest_path,
                "equivalent_baseline_reused": True,
                "equivalent_baseline_snapshot_id": baseline_snapshot_id,
            },
        )

    def _task_intent_view(self, task: AcquisitionTask, job_request: JobRequest) -> dict[str, Any]:
        metadata_intent_view = dict(task.metadata.get("intent_view") or {})
        if metadata_intent_view:
            return metadata_intent_view
        return resolve_request_intent_view(job_request)

    def _task_execution_view(self, task: AcquisitionTask, job_request: JobRequest) -> dict[str, Any]:
        execution_view = dict(self._task_intent_view(task, job_request))
        metadata = dict(task.metadata or {})
        for key in (
            "strategy_type",
            "company_scope",
            "filter_hints",
            "search_seed_queries",
            "search_query_bundles",
            "search_channel_order",
            "employment_statuses",
            "cost_policy",
            "delta_execution_plan",
            "asset_reuse_plan",
            "delta_execution_noop",
            "delta_execution_reason",
            "company_employee_shards",
            "company_employee_shard_policy",
            "company_employee_shard_strategy",
            "max_pages",
            "page_limit",
            "reuse_existing_roster",
            "include_former_search_seed",
            "enrichment_scope",
            "acquisition_phase",
            "acquisition_phase_title",
            "slug_resolution_limit",
            "profile_detail_limit",
            "publication_scan_limit",
            "publication_lead_limit",
            "exploration_limit",
            "scholar_coauthor_follow_up_limit",
            "publication_source_families",
            "publication_extraction_strategy",
            "full_roster_profile_prefetch",
            "former_provider_people_search_min_expected_results",
            "baseline_snapshot_id",
        ):
            if key not in metadata:
                continue
            value = metadata.get(key)
            if key in execution_view:
                existing = execution_view.get(key)
                if isinstance(existing, dict):
                    execution_view[key] = dict(existing)
                    continue
                if isinstance(existing, list):
                    execution_view[key] = [dict(item) if isinstance(item, dict) else item for item in existing]
                    continue
                if existing not in {None, ""}:
                    continue
            if isinstance(value, dict):
                execution_view[key] = dict(value)
            elif isinstance(value, list):
                if key == "search_query_bundles":
                    execution_view[key] = [dict(item) for item in value if isinstance(item, dict)]
                else:
                    execution_view[key] = list(value)
            elif value is not None:
                execution_view[key] = value
        return execution_view

    def _task_execution_value(
        self,
        task: AcquisitionTask,
        job_request: JobRequest,
        key: str,
        default: Any = None,
    ) -> Any:
        task_view = self._task_execution_view(task, job_request)
        if key not in task_view:
            return default
        value = task_view.get(key)
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, list):
            return [dict(item) if isinstance(item, dict) else item for item in value]
        if value is None:
            return default
        return value

    def _task_execution_str(
        self,
        task: AcquisitionTask,
        job_request: JobRequest,
        key: str,
        *,
        default: str = "",
    ) -> str:
        value = self._task_execution_value(task, job_request, key)
        text = str(value or "").strip()
        return text or default

    def _task_execution_int(
        self,
        task: AcquisitionTask,
        job_request: JobRequest,
        key: str,
        *,
        default: int = 0,
    ) -> int:
        value = self._task_execution_value(task, job_request, key)
        if value is None or value == "":
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _task_execution_bool(
        self,
        task: AcquisitionTask,
        job_request: JobRequest,
        key: str,
        *,
        default: bool = False,
    ) -> bool:
        value = self._task_execution_value(task, job_request, key)
        if value is None:
            return default
        return bool(value)

    def _task_execution_dict(
        self,
        task: AcquisitionTask,
        job_request: JobRequest,
        key: str,
    ) -> dict[str, Any]:
        value = self._task_execution_value(task, job_request, key, default={})
        return dict(value or {})

    def _task_execution_list(
        self,
        task: AcquisitionTask,
        job_request: JobRequest,
        key: str,
    ) -> list[Any]:
        value = self._task_execution_value(task, job_request, key, default=[])
        return list(value or [])

    def _task_execution_phase(self, task: AcquisitionTask, job_request: JobRequest) -> dict[str, str]:
        return {
            "phase": self._task_execution_str(task, job_request, "acquisition_phase"),
            "phase_title": self._task_execution_str(task, job_request, "acquisition_phase_title"),
        }

    def _task_stage_request_limits(self, task: AcquisitionTask, job_request: JobRequest) -> dict[str, int]:
        return {
            "slug_resolution_limit": self._task_execution_int(
                task,
                job_request,
                "slug_resolution_limit",
                default=job_request.slug_resolution_limit,
            ),
            "profile_detail_limit": self._task_execution_int(
                task,
                job_request,
                "profile_detail_limit",
                default=job_request.profile_detail_limit,
            ),
            "publication_scan_limit": self._task_execution_int(
                task,
                job_request,
                "publication_scan_limit",
                default=job_request.publication_scan_limit,
            ),
            "publication_lead_limit": self._task_execution_int(
                task,
                job_request,
                "publication_lead_limit",
                default=job_request.publication_lead_limit,
            ),
            "exploration_limit": self._task_execution_int(
                task,
                job_request,
                "exploration_limit",
                default=job_request.exploration_limit,
            ),
            "scholar_coauthor_follow_up_limit": self._task_execution_int(
                task,
                job_request,
                "scholar_coauthor_follow_up_limit",
                default=job_request.scholar_coauthor_follow_up_limit,
            ),
        }

    def _task_delta_noop(self, task: AcquisitionTask, job_request: JobRequest) -> bool:
        return self._task_execution_bool(task, job_request, "delta_execution_noop")

    def _task_delta_reason(self, task: AcquisitionTask, job_request: JobRequest) -> str:
        return self._task_execution_str(task, job_request, "delta_execution_reason")

    def _task_enrichment_scope(self, task: AcquisitionTask, job_request: JobRequest) -> str:
        scope = self._task_execution_str(task, job_request, "enrichment_scope").lower()
        if scope:
            return scope
        if task.task_type == "enrich_linkedin_profiles":
            return "linkedin_stage_1"
        if task.task_type == "enrich_public_web_signals":
            return "public_web_stage_2"
        return "full"

    def _task_full_roster_profile_prefetch(self, task: AcquisitionTask, job_request: JobRequest) -> bool:
        return self._task_execution_bool(task, job_request, "full_roster_profile_prefetch")

    def _task_include_former_search_seed(self, task: AcquisitionTask, job_request: JobRequest) -> bool:
        return self._task_execution_bool(task, job_request, "include_former_search_seed")

    def _task_company_employee_shard_policy(
        self,
        task: AcquisitionTask,
        job_request: JobRequest,
    ) -> dict[str, Any]:
        return normalize_company_employee_shard_policy(
            self._task_execution_dict(task, job_request, "company_employee_shard_policy")
        )

    def _task_roster_paging(self, task: AcquisitionTask, job_request: JobRequest) -> tuple[int, int]:
        return (
            self._task_execution_int(task, job_request, "max_pages", default=10),
            self._task_execution_int(task, job_request, "page_limit", default=50),
        )

    def _normalize_zero_result_search_seed_execution(
        self,
        execution: AcquisitionExecution,
        *,
        detail: str,
    ) -> AcquisitionExecution:
        payload = dict(execution.payload or {})
        queued_query_count = int(payload.get("queued_query_count") or 0)
        stop_reason = str(payload.get("stop_reason") or "").strip()
        search_seed_snapshot = execution.state_updates.get("search_seed_snapshot")
        search_seed_entry_count = (
            len(search_seed_snapshot.entries)
            if isinstance(search_seed_snapshot, SearchSeedSnapshot)
            else int(payload.get("entry_count") or 0)
        )
        valid_zero_result = bool(
            isinstance(search_seed_snapshot, SearchSeedSnapshot)
            and stop_reason in {"completed", "cohort_provider_no_results"}
        )
        if (
            execution.status == "blocked"
            and valid_zero_result
            and search_seed_entry_count == 0
            and queued_query_count == 0
            and stop_reason != "queued_background_search"
        ):
            return AcquisitionExecution(
                task_id=execution.task_id,
                status="completed",
                detail=detail,
                payload={
                    **payload,
                    "former_search_zero_result": True,
                },
                state_updates=dict(execution.state_updates or {}),
            )
        return execution

    def _task_filter_hints(self, task: AcquisitionTask, job_request: JobRequest) -> dict[str, Any]:
        return self._task_execution_dict(task, job_request, "filter_hints")

    def _task_search_query_bundles(self, task: AcquisitionTask, job_request: JobRequest) -> list[dict[str, Any]]:
        return [
            dict(item)
            for item in self._task_execution_list(task, job_request, "search_query_bundles")
            if isinstance(item, dict)
        ]

    def _task_delta_execution_plan(
        self, task: AcquisitionTask, job_request: JobRequest | None = None
    ) -> dict[str, Any]:
        if isinstance(job_request, JobRequest):
            return self._task_execution_dict(task, job_request, "delta_execution_plan")
        return dict(task.metadata.get("delta_execution_plan") or {})

    def _task_asset_reuse_plan(self, task: AcquisitionTask, job_request: JobRequest) -> dict[str, Any]:
        return self._task_execution_dict(task, job_request, "asset_reuse_plan")

    def _task_baseline_selection_explanation(self, task: AcquisitionTask, job_request: JobRequest) -> dict[str, Any]:
        delta_execution_plan = self._task_delta_execution_plan(task, job_request)
        explanation = dict(delta_execution_plan.get("baseline_selection_explanation") or {})
        if explanation:
            return explanation
        asset_reuse_plan = self._task_asset_reuse_plan(task, job_request)
        explanation = dict(asset_reuse_plan.get("baseline_selection_explanation") or {})
        if explanation:
            return explanation
        baseline_snapshot_id = str(asset_reuse_plan.get("baseline_snapshot_id") or "").strip()
        if not baseline_snapshot_id or not str(job_request.target_company or "").strip():
            return {}
        try:
            registry_row = self.store.get_authoritative_organization_asset_registry(
                target_company=job_request.target_company,
                asset_view=str(job_request.asset_view or "canonical_merged").strip() or "canonical_merged",
            )
        except Exception:
            registry_row = {}
        if not registry_row:
            return {}
        return build_asset_reuse_baseline_selection_explanation(
            request=job_request,
            baseline=registry_row,
            asset_reuse_plan=asset_reuse_plan,
            current_lane_coverage=dict(registry_row.get("current_lane_coverage") or {}),
            former_lane_coverage=dict(registry_row.get("former_lane_coverage") or {}),
        )

    def _task_cost_policy(self, task: AcquisitionTask, job_request: JobRequest | None = None) -> dict[str, Any]:
        task_cost_policy = (
            self._task_execution_dict(task, job_request, "cost_policy") if isinstance(job_request, JobRequest) else {}
        )
        return _effective_cost_policy(task_cost_policy, job_request)

    def _task_strategy_type(self, task: AcquisitionTask, job_request: JobRequest) -> str:
        return self._task_execution_str(task, job_request, "strategy_type")

    def _task_search_channel_order(self, task: AcquisitionTask, job_request: JobRequest) -> list[str]:
        return [
            str(item).strip()
            for item in self._task_execution_list(task, job_request, "search_channel_order")
            if str(item).strip()
        ]

    def _task_employment_statuses(self, task: AcquisitionTask, job_request: JobRequest) -> list[str]:
        return [
            str(item).strip()
            for item in self._task_execution_list(task, job_request, "employment_statuses")
            if str(item).strip()
        ]

    def _effective_company_employee_shards(
        self, task: AcquisitionTask, job_request: JobRequest
    ) -> list[dict[str, Any]]:
        delta_execution_plan = self._task_delta_execution_plan(task, job_request)
        delta_shards = list(delta_execution_plan.get("missing_company_employee_shards") or [])
        if delta_shards:
            return _normalize_company_employee_shards(delta_shards)
        return _normalize_company_employee_shards(
            self._task_execution_list(task, job_request, "company_employee_shards")
        )

    def _effective_search_seed_queries(self, task: AcquisitionTask, job_request: JobRequest) -> list[str]:
        delta_execution_plan = self._task_delta_execution_plan(task, job_request)
        delta_queries = [
            str(item).strip()
            for item in list(delta_execution_plan.get("missing_profile_search_queries") or [])
            if str(item).strip()
        ]
        if delta_queries:
            return delta_queries
        cost_policy = self._task_cost_policy(task, job_request)
        if (
            self._task_strategy_type(task, job_request) == "former_employee_search"
            and bool(cost_policy.get("former_broad_past_company_only"))
            and not bool(cost_policy.get("former_keyword_queries_only"))
        ):
            return []
        return [
            str(item).strip()
            for item in self._task_execution_list(task, job_request, "search_seed_queries")
            if str(item).strip()
        ]

    def _load_delta_baseline_material(
        self,
        job_request: JobRequest,
        *,
        baseline_snapshot_id: str = "",
        enrichment_scope: str = "linkedin_stage_1",
    ) -> dict[str, Any]:
        execution_preferences = dict(getattr(job_request, "execution_preferences", {}) or {})
        snapshot_id = str(baseline_snapshot_id or execution_preferences.get("delta_baseline_snapshot_id") or "").strip()
        if not snapshot_id or not str(job_request.target_company or "").strip():
            return {}
        try:
            snapshot_payload = load_authoritative_company_snapshot_candidate_documents(
                runtime_dir=self.settings.runtime_dir,
                store=self.store,
                target_company=job_request.target_company,
                snapshot_id=snapshot_id,
                view="canonical_merged",
                prefer_hot_cache=False,
                allow_materialization_fallback=True,
                allow_candidate_documents_fallback=True,
            )
        except CandidateArtifactError:
            return {}

        baseline_snapshot_dir = Path(str(snapshot_payload.get("snapshot_dir") or "")).expanduser()
        if not baseline_snapshot_dir.exists():
            return {}

        candidate_document_paths: list[Path] = []
        if enrichment_scope == "linkedin_stage_1":
            candidate_document_paths.append(baseline_snapshot_dir / "candidate_documents.linkedin_stage_1.json")
        elif enrichment_scope == "public_web_stage_2":
            candidate_document_paths.append(baseline_snapshot_dir / "candidate_documents.public_web_stage_2.json")
        candidate_document_paths.append(baseline_snapshot_dir / "candidate_documents.json")

        document_payload: dict[str, Any] = {}
        source_candidate_doc_path: Path | None = None
        for candidate_path in candidate_document_paths:
            if not candidate_path.exists():
                continue
            try:
                loaded_payload = json.loads(candidate_path.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(loaded_payload, dict):
                document_payload = loaded_payload
                source_candidate_doc_path = candidate_path
                break

        if not document_payload:
            document_payload = {
                "candidates": [candidate.to_record() for candidate in list(snapshot_payload.get("candidates") or [])],
                "evidence": [dict(item or {}) for item in list(snapshot_payload.get("evidence") or [])],
                "candidate_count": len(list(snapshot_payload.get("candidates") or [])),
                "evidence_count": len(list(snapshot_payload.get("evidence") or [])),
            }

        authoritative_candidates = [
            candidate.to_record()
            for candidate in list(snapshot_payload.get("candidates") or [])
            if isinstance(candidate, Candidate)
        ]
        authoritative_evidence = [dict(item or {}) for item in list(snapshot_payload.get("evidence") or [])]
        authoritative_document_payload = {
            "target_company": str(snapshot_payload.get("target_company") or job_request.target_company or "").strip(),
            "snapshot_id": str(snapshot_payload.get("snapshot_id") or snapshot_id),
            "company_identity": dict(snapshot_payload.get("company_identity") or {}),
            "candidates": authoritative_candidates,
            "evidence": authoritative_evidence,
            "candidate_count": len(authoritative_candidates),
            "evidence_count": len(authoritative_evidence),
        }

        return {
            "snapshot_id": str(snapshot_payload.get("snapshot_id") or snapshot_id),
            "snapshot_dir": baseline_snapshot_dir,
            "company_identity": dict(snapshot_payload.get("company_identity") or {}),
            "candidate_document_payload": document_payload,
            "authoritative_candidate_document_payload": authoritative_document_payload,
            "source_candidate_doc_path": str(source_candidate_doc_path)
            if isinstance(source_candidate_doc_path, Path)
            else "",
            "authoritative_source_path": str(snapshot_payload.get("source_path") or "").strip(),
        }

    def _acquire_full_roster(
        self,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
    ) -> AcquisitionExecution:
        identity = state.get("company_identity")
        snapshot_dir = state.get("snapshot_dir")
        if not isinstance(identity, CompanyIdentity) or not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Company identity must be resolved before roster acquisition.",
                payload={},
            )
        cost_policy = self._task_cost_policy(task, job_request)
        baseline_snapshot_id = self._delta_baseline_snapshot_id(task, job_request)
        delta_execution_plan = self._task_delta_execution_plan(task, job_request)
        baseline_selection_explanation = self._task_baseline_selection_explanation(task, job_request)
        if (
            self._task_delta_noop(task, job_request) or bool(delta_execution_plan.get("delta_noop"))
        ) and baseline_snapshot_id:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="completed",
                detail=(
                    f"Current roster acquisition skipped; baseline snapshot {baseline_snapshot_id} already covers "
                    "the required current-member lane."
                ),
                payload={
                    "strategy_type": self._task_strategy_type(task, job_request),
                    "baseline_snapshot_id": baseline_snapshot_id,
                    "delta_execution_noop": True,
                    "delta_execution_reason": self._task_delta_reason(task, job_request),
                    "baseline_selection_explanation": baseline_selection_explanation,
                },
            )
        intent_view = resolve_request_intent_view(job_request)
        effective_execution_preferences = dict(intent_view.get("execution_preferences") or {})
        max_pages, page_limit = self._task_roster_paging(task, job_request)
        company_employee_shards = self._effective_company_employee_shards(task, job_request)
        company_employee_shard_policy = self._task_company_employee_shard_policy(task, job_request)
        request_roster_plan = build_request_scoped_company_employee_query_plan(
            target_locations=job_request.target_locations,
            function_ids=request_scoped_roster_function_ids(job_request.to_record()),
            max_pages=max_pages,
            page_limit=page_limit,
        )
        if not company_employee_shards and not bool(cost_policy.get("large_org_keyword_probe_mode")):
            # Explicit request function selection owns the roster lane for every
            # company (no company-name branches) and pre-empts the generic
            # adaptive probe policy; planner-emitted metadata shards and delta
            # missing shards still win when present.  large_org_keyword_probe_mode
            # keeps its own keyword-union sharding contract.
            company_employee_shards = list(request_roster_plan.get("shards") or [])
        unsharded_company_filters = dict(request_roster_plan.get("company_filters") or {})
        job_id = str(state.get("job_id") or "")
        request_payload = job_request.to_record()
        plan_payload = dict(state.get("plan_payload") or {})
        runtime_mode = str(state.get("runtime_mode") or "workflow")
        allow_cached_roster_fallback = bool(cost_policy.get("allow_cached_roster_fallback", True))
        allow_shared_provider_cache = bool(cost_policy.get("allow_shared_provider_cache", True))
        reuse_existing_roster = bool(
            effective_execution_preferences.get("reuse_existing_roster")
            or self._task_execution_bool(task, job_request, "reuse_existing_roster")
        )
        acquisition_mode = "live_roster_acquisition"
        reuse_existing_roster_miss = False
        explicitly_requested_former_seed = "run_former_search_seed" in effective_execution_preferences
        should_run_former_search_seed = self._should_run_default_former_search_seed(task, job_request)
        former_parallel_executor: ThreadPoolExecutor | None = None
        former_parallel_future = None

        def _start_parallel_former_search_seed_if_needed() -> None:
            nonlocal former_parallel_executor, former_parallel_future
            if not should_run_former_search_seed:
                return
            if former_parallel_future is not None:
                return
            former_parallel_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="acquisition-former-seed",
            )
            former_parallel_future = former_parallel_executor.submit(
                self._acquire_default_former_search_seed,
                task,
                dict(state),
                job_request,
                identity,
            )

        def _shutdown_parallel_former(wait: bool) -> None:
            nonlocal former_parallel_executor, former_parallel_future
            if former_parallel_executor is None:
                return
            former_parallel_executor.shutdown(wait=wait, cancel_futures=not wait)
            former_parallel_executor = None
            former_parallel_future = None

        def _kickoff_former_search_seed_for_background_roster() -> tuple[SearchSeedSnapshot | None, str, str, str]:
            if not should_run_former_search_seed:
                return None, "", "", "skipped"
            try:
                former_execution = self._acquire_default_former_search_seed(task, state, job_request, identity)
            except Exception as exc:
                return None, "", str(exc), "failed"
            former_snapshot = former_execution.state_updates.get("search_seed_snapshot")
            return (
                former_snapshot if isinstance(former_snapshot, SearchSeedSnapshot) else None,
                str(former_execution.detail or ""),
                str(former_execution.detail or "") if former_execution.status == "blocked" else "",
                str(former_execution.status or ""),
            )

        runtime_timing_overrides = _runtime_timing_overrides_from_request_payload(job_request.to_record())

        try:
            if reuse_existing_roster:
                cached_snapshot = self._load_cached_roster_snapshot(identity, exclude_snapshot_dir=snapshot_dir)
                if cached_snapshot is not None:
                    snapshot = self._materialize_reused_roster_snapshot(identity, cached_snapshot, snapshot_dir)
                    acquisition_mode = "reused_cached_roster"
                else:
                    reuse_existing_roster_miss = True
            if (
                acquisition_mode != "reused_cached_roster"
                and harvest_connector_available(self.harvest_company_connector.settings)
                and bool(cost_policy.get("allow_company_employee_api", True))
            ):
                adaptive_shard_plan: dict[str, Any] = {}
                if not company_employee_shards and company_employee_shard_policy:
                    adaptive_shard_plan = self._resolve_adaptive_company_employee_shards(
                        identity=identity,
                        snapshot_dir=snapshot_dir,
                        policy=company_employee_shard_policy,
                        runtime_timing_overrides=runtime_timing_overrides,
                    )
                    if str(adaptive_shard_plan.get("status") or "") != "planned":
                        return AcquisitionExecution(
                            task_id=task.task_id,
                            status="blocked",
                            detail=str(adaptive_shard_plan.get("detail") or "Adaptive shard planning failed."),
                            payload={
                                "target_company": identity.canonical_name,
                                "adaptive_shard_plan": adaptive_shard_plan,
                            },
                        )
                    company_employee_shards = _normalize_company_employee_shards(adaptive_shard_plan.get("shards"))
                if company_employee_shards:
                    if self.worker_runtime is not None and job_id:
                        shard_worker_summary = self._execute_segmented_harvest_company_roster_workers(
                            identity=identity,
                            snapshot_dir=snapshot_dir,
                            shards=company_employee_shards,
                            job_id=job_id,
                            request_payload=request_payload,
                            plan_payload=plan_payload,
                            runtime_mode=runtime_mode,
                            allow_shared_provider_cache=allow_shared_provider_cache,
                        )
                        if int(shard_worker_summary.get("queued_count") or 0) > 0:
                            (
                                former_search_seed_snapshot,
                                former_search_detail,
                                former_search_error,
                                former_search_status,
                            ) = _kickoff_former_search_seed_for_background_roster()
                            queued_payload: dict[str, Any] = {
                                "queued_harvest_worker_count": int(shard_worker_summary.get("queued_count") or 0),
                                "completed_harvest_worker_count": int(shard_worker_summary.get("completed_count") or 0),
                                "shard_count": int(shard_worker_summary.get("shard_count") or 0),
                                "stop_reason": "queued_background_harvest_shards",
                                "harvest_workers": list(shard_worker_summary.get("queued_summaries") or []),
                                "completed_harvest_workers": list(
                                    shard_worker_summary.get("completed_summaries") or []
                                ),
                                "former_search_seed_status": former_search_status,
                            }
                            if reuse_existing_roster_miss:
                                queued_payload["reuse_existing_roster_miss"] = True
                                queued_payload["reuse_existing_roster_miss_reason"] = "no_cached_roster_snapshot"
                            if former_search_detail:
                                queued_payload["former_search_seed_detail"] = former_search_detail
                            if former_search_error:
                                queued_payload["former_search_seed_error"] = former_search_error
                            queued_state_updates: dict[str, Any] = {}
                            if isinstance(former_search_seed_snapshot, SearchSeedSnapshot):
                                queued_payload["former_search_seed_snapshot"] = former_search_seed_snapshot.to_record()
                                queued_payload["former_search_seed_entry_count"] = len(
                                    former_search_seed_snapshot.entries
                                )
                                queued_state_updates["search_seed_snapshot"] = former_search_seed_snapshot
                            return self._continue_full_roster_with_available_baseline(
                                task=task,
                                roster_snapshot=None,
                                search_seed_snapshot=former_search_seed_snapshot,
                                snapshot_dir=snapshot_dir,
                                job_id=job_id,
                                request_payload=request_payload,
                                plan_payload=plan_payload,
                                runtime_mode=runtime_mode,
                                allow_shared_provider_cache=allow_shared_provider_cache,
                                queued_payload=queued_payload,
                                queued_state_updates=queued_state_updates,
                                blocked_detail=(
                                    "Roster acquisition queued background segmented Harvest company-employees runs; "
                                    "former seed discovery started in parallel. Resume after worker recovery completes remote dataset fetch."
                                ),
                                completion_detail=(
                                    "Current-roster Harvest is continuing in the background; workflow will proceed "
                                    "from the available full-roster baseline while profile detail hydrates incrementally."
                                ),
                            )
                    _start_parallel_former_search_seed_if_needed()
                    snapshot = self._fetch_segmented_harvest_company_roster(
                        identity=identity,
                        snapshot_dir=snapshot_dir,
                        shards=company_employee_shards,
                        allow_shared_provider_cache=allow_shared_provider_cache,
                        runtime_timing_overrides=runtime_timing_overrides,
                    )
                elif self.worker_runtime is not None and job_id:
                    harvest_worker = self._execute_harvest_company_roster_worker(
                        identity=identity,
                        snapshot_dir=snapshot_dir,
                        max_pages=max_pages,
                        page_limit=page_limit,
                        job_id=job_id,
                        request_payload=request_payload,
                        plan_payload=plan_payload,
                        runtime_mode=runtime_mode,
                        allow_shared_provider_cache=allow_shared_provider_cache,
                        company_filters=unsharded_company_filters,
                    )
                    if str(harvest_worker.get("worker_status") or "") == "queued":
                        summary = dict(harvest_worker.get("summary") or {})
                        (
                            former_search_seed_snapshot,
                            former_search_detail,
                            former_search_error,
                            former_search_status,
                        ) = _kickoff_former_search_seed_for_background_roster()
                        queued_payload = {
                            "queued_harvest_worker_count": 1,
                            "stop_reason": "queued_background_harvest",
                            "harvest_worker": summary,
                            "former_search_seed_status": former_search_status,
                        }
                        if reuse_existing_roster_miss:
                            queued_payload["reuse_existing_roster_miss"] = True
                            queued_payload["reuse_existing_roster_miss_reason"] = "no_cached_roster_snapshot"
                        if former_search_detail:
                            queued_payload["former_search_seed_detail"] = former_search_detail
                        if former_search_error:
                            queued_payload["former_search_seed_error"] = former_search_error
                        queued_state_updates = {}
                        if isinstance(former_search_seed_snapshot, SearchSeedSnapshot):
                            queued_payload["former_search_seed_snapshot"] = former_search_seed_snapshot.to_record()
                            queued_payload["former_search_seed_entry_count"] = len(former_search_seed_snapshot.entries)
                            queued_state_updates["search_seed_snapshot"] = former_search_seed_snapshot
                        return self._continue_full_roster_with_available_baseline(
                            task=task,
                            roster_snapshot=None,
                            search_seed_snapshot=former_search_seed_snapshot,
                            snapshot_dir=snapshot_dir,
                            job_id=job_id,
                            request_payload=request_payload,
                            plan_payload=plan_payload,
                            runtime_mode=runtime_mode,
                            allow_shared_provider_cache=allow_shared_provider_cache,
                            queued_payload=queued_payload,
                            queued_state_updates=queued_state_updates,
                            blocked_detail=(
                                "Roster acquisition queued a background Harvest company-employees run; "
                                "former seed discovery started in parallel. Resume after worker recovery completes remote dataset fetch."
                            ),
                            completion_detail=(
                                "Current-roster Harvest is continuing in the background; workflow will proceed "
                                "from the available full-roster baseline while profile detail hydrates incrementally."
                            ),
                        )
                    with runtime_provider_limiter_slot(
                        self.store,
                        limiter_key="harvest_company_employees_actor",
                        budget=resolved_harvest_company_roster_global_inflight(runtime_timing_overrides),
                        lease_owner=f"harvest_company_employees:direct:{job_id or identity.company_key}",
                        lease_seconds=7200,
                        metadata={"company": identity.company_key, "source": "direct_full_roster"},
                    ):
                        with runtime_inflight_slot(
                            "harvest_company_roster",
                            budget=resolved_harvest_company_roster_global_inflight(runtime_timing_overrides),
                            metadata={"company": identity.company_key, "source": "direct_full_roster"},
                        ):
                            snapshot = _apply_optional_runtime_timing_overrides(
                                self.harvest_company_connector.fetch_company_roster,
                                identity,
                                snapshot_dir,
                                asset_logger=AssetLogger(snapshot_dir),
                                max_pages=max_pages,
                                page_limit=page_limit,
                                company_filters=unsharded_company_filters,
                                allow_shared_provider_cache=allow_shared_provider_cache,
                                runtime_timing_overrides=runtime_timing_overrides,
                            )
                else:
                    _start_parallel_former_search_seed_if_needed()
                    with runtime_provider_limiter_slot(
                        self.store,
                        limiter_key="harvest_company_employees_actor",
                        budget=resolved_harvest_company_roster_global_inflight(runtime_timing_overrides),
                        lease_owner=f"harvest_company_employees:direct:{job_id or identity.company_key}",
                        lease_seconds=7200,
                        metadata={"company": identity.company_key, "source": "direct_full_roster"},
                    ):
                        with runtime_inflight_slot(
                            "harvest_company_roster",
                            budget=resolved_harvest_company_roster_global_inflight(runtime_timing_overrides),
                            metadata={"company": identity.company_key, "source": "direct_full_roster"},
                        ):
                            snapshot = _apply_optional_runtime_timing_overrides(
                                self.harvest_company_connector.fetch_company_roster,
                                identity,
                                snapshot_dir,
                                asset_logger=AssetLogger(snapshot_dir),
                                max_pages=max_pages,
                                page_limit=page_limit,
                                company_filters=unsharded_company_filters,
                                allow_shared_provider_cache=allow_shared_provider_cache,
                                runtime_timing_overrides=runtime_timing_overrides,
                            )
            elif acquisition_mode != "reused_cached_roster":
                _start_parallel_former_search_seed_if_needed()
                snapshot = self.roster_connector.fetch_company_roster(
                    identity,
                    snapshot_dir,
                    asset_logger=AssetLogger(snapshot_dir),
                    max_pages=max_pages,
                    page_limit=page_limit,
                )
        except Exception as exc:
            _shutdown_parallel_former(wait=True)
            cached_snapshot = (
                self._load_cached_roster_snapshot(identity, exclude_snapshot_dir=snapshot_dir)
                if allow_cached_roster_fallback
                else None
            )
            if cached_snapshot is not None:
                self._write_latest_snapshot_pointer(identity, cached_snapshot.snapshot_id, cached_snapshot.snapshot_dir)
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail=(
                        f"Live roster acquisition failed ({exc}); reused cached snapshot "
                        f"{cached_snapshot.snapshot_id} with {len(cached_snapshot.visible_entries)} visible members."
                    ),
                    payload={
                        **cached_snapshot.to_record(),
                        "acquisition_mode": "cached_snapshot_fallback",
                        "fallback_reason": str(exc),
                    },
                    state_updates={
                        "roster_snapshot": cached_snapshot,
                        "snapshot_id": cached_snapshot.snapshot_id,
                        "snapshot_dir": cached_snapshot.snapshot_dir,
                    },
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=f"Roster acquisition failed: {exc}",
                payload={"target_company": identity.canonical_name},
            )

        if not snapshot.visible_entries:
            _shutdown_parallel_former(wait=True)
            cached_snapshot = (
                self._load_cached_roster_snapshot(identity, exclude_snapshot_dir=snapshot_dir)
                if allow_cached_roster_fallback
                else None
            )
            if cached_snapshot is not None:
                self._write_latest_snapshot_pointer(identity, cached_snapshot.snapshot_id, cached_snapshot.snapshot_dir)
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail=(
                        f"Live roster acquisition returned no visible members; reused cached snapshot "
                        f"{cached_snapshot.snapshot_id} with {len(cached_snapshot.visible_entries)} visible members."
                    ),
                    payload={
                        **cached_snapshot.to_record(),
                        "acquisition_mode": "cached_snapshot_fallback",
                        "fallback_reason": "no_visible_members",
                    },
                    state_updates={
                        "roster_snapshot": cached_snapshot,
                        "snapshot_id": cached_snapshot.snapshot_id,
                        "snapshot_dir": cached_snapshot.snapshot_dir,
                    },
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Roster acquisition finished but no visible company members were recovered.",
                payload=snapshot.to_record(),
                state_updates={"roster_snapshot": snapshot},
            )

        self._write_latest_snapshot_pointer(identity, snapshot.snapshot_id, snapshot.snapshot_dir)

        former_search_seed_snapshot = None
        former_search_detail = ""
        former_search_error = ""
        if should_run_former_search_seed:
            if former_parallel_future is not None:
                try:
                    former_execution = former_parallel_future.result()
                finally:
                    _shutdown_parallel_former(wait=True)
            else:
                former_execution = self._acquire_default_former_search_seed(task, state, job_request, identity)
            former_search_seed_snapshot = former_execution.state_updates.get("search_seed_snapshot")
            former_search_detail = former_execution.detail
            if former_execution.status == "blocked":
                former_search_error = former_execution.detail
                former_payload = dict(former_execution.payload or {})
                former_stop_reason = str(former_payload.get("stop_reason") or "").strip()
                former_queued_query_count = int(former_payload.get("queued_query_count") or 0)
                should_wait_for_former_background = (
                    former_stop_reason == "queued_background_search" or former_queued_query_count > 0
                )
                if explicitly_requested_former_seed or should_wait_for_former_background:
                    queued_payload = {
                        **snapshot.to_record(),
                        "acquisition_mode": acquisition_mode,
                        "former_search_seed_status": "blocked",
                        "former_search_seed_detail": former_search_detail,
                        "former_search_seed_stop_reason": former_stop_reason,
                        "former_search_seed_queued_query_count": former_queued_query_count,
                    }
                    queued_state_updates = {
                        "roster_snapshot": snapshot,
                        **(
                            {"search_seed_snapshot": former_search_seed_snapshot}
                            if isinstance(former_search_seed_snapshot, SearchSeedSnapshot)
                            else {}
                        ),
                    }
                    return self._continue_full_roster_with_available_baseline(
                        task=task,
                        roster_snapshot=snapshot,
                        search_seed_snapshot=former_search_seed_snapshot,
                        snapshot_dir=snapshot_dir,
                        job_id=job_id,
                        request_payload=request_payload,
                        plan_payload=plan_payload,
                        runtime_mode=runtime_mode,
                        allow_shared_provider_cache=allow_shared_provider_cache,
                        queued_payload=queued_payload,
                        queued_state_updates=queued_state_updates,
                        blocked_detail=former_execution.detail,
                        completion_detail=(
                            "Current roster is ready; former-member search is continuing in the background while "
                            "workflow proceeds from the available full-roster baseline."
                        ),
                    )

        detail = (
            f"Captured {len(snapshot.raw_entries)} roster rows "
            f"({len(snapshot.visible_entries)} visible, {len(snapshot.headless_entries)} headless)."
        )
        if acquisition_mode == "reused_cached_roster":
            detail = (
                f"Reused local roster snapshot {snapshot.snapshot_id} with "
                f"{len(snapshot.visible_entries)} visible members."
            )
        if company_employee_shards:
            detail += f" Used {len(company_employee_shards)} segmented Harvest company-employee shards."
        payload = snapshot.to_record()
        payload["acquisition_mode"] = acquisition_mode
        if reuse_existing_roster_miss:
            payload["reuse_existing_roster_miss"] = True
            payload["reuse_existing_roster_miss_reason"] = "no_cached_roster_snapshot"
        final_state_updates: dict[str, Any] = {"roster_snapshot": snapshot}
        if isinstance(former_search_seed_snapshot, SearchSeedSnapshot):
            detail += f" Added {len(former_search_seed_snapshot.entries)} former-member search seeds."
            payload["former_search_seed_snapshot"] = former_search_seed_snapshot.to_record()
            payload["former_search_seed_entry_count"] = len(former_search_seed_snapshot.entries)
            final_state_updates["search_seed_snapshot"] = former_search_seed_snapshot
        elif former_search_error:
            payload["former_search_seed_error"] = former_search_error
        if former_search_detail:
            payload["former_search_seed_detail"] = former_search_detail

        _shutdown_parallel_former(wait=True)
        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=detail,
            payload=payload,
            state_updates=final_state_updates,
        )

    @staticmethod
    def _provider_search_retry_materialization_item_id(
        *,
        job_id: str,
        snapshot_id: str,
        retry_item: dict[str, Any],
    ) -> str:
        item_key = str(retry_item.get("item_key") or "").strip()
        if not item_key:
            signature_payload = {
                "provider_retry_type": str(retry_item.get("provider_retry_type") or ""),
                "provider": str(retry_item.get("provider") or ""),
                "query": str(retry_item.get("query") or ""),
                "effective_query_text": str(retry_item.get("effective_query_text") or ""),
                "employment_status": str(retry_item.get("employment_status") or ""),
                "incomplete_reason": str(retry_item.get("incomplete_reason") or ""),
            }
            item_key = sha1(
                json.dumps(signature_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest()[:24]
        return f"{job_id}|{snapshot_id}|{PROVIDER_SEARCH_RETRY_ITEM_KIND}|{item_key}"

    def _record_search_seed_provider_retry_items(
        self,
        *,
        job_id: str,
        target_company: str,
        snapshot: SearchSeedSnapshot | None,
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id or not isinstance(snapshot, SearchSeedSnapshot):
            return {"status": "skipped", "reason": "job_or_snapshot_missing", "item_count": 0}
        retry_items = collect_search_seed_provider_retry_items(snapshot)
        if not retry_items:
            return {"status": "skipped", "reason": "no_provider_retry_items", "item_count": 0}
        persisted_items: list[dict[str, Any]] = []
        skipped_terminal_count = 0
        for retry_item in retry_items:
            retry_item = dict(retry_item)
            snapshot_id = str(
                retry_item.get("snapshot_id") or snapshot.snapshot_id or snapshot.snapshot_dir.name
            ).strip()
            item_id = self._provider_search_retry_materialization_item_id(
                job_id=normalized_job_id,
                snapshot_id=snapshot_id,
                retry_item=retry_item,
            )
            existing = self.store.get_job_materialization_item(item_id)
            existing_status = str(dict(existing or {}).get("status") or "").strip().lower()
            if existing_status in {"completed", "cancelled", "canceled", "superseded"}:
                skipped_terminal_count += 1
                persisted_items.append(dict(existing))
                continue
            queue_status = str(retry_item.get("queue_status") or "").strip().lower()
            if not queue_status:
                queue_status = (
                    "failed" if str(retry_item.get("status") or "").strip().lower() == "exhausted" else "queued"
                )
            phase = str(retry_item.get("phase") or "").strip().lower()
            if not phase:
                phase = "terminal" if queue_status == "failed" else queue_status
            max_attempts = max(1, int(retry_item.get("max_attempts") or retry_item.get("provider_attempt_count") or 1))
            item = self.store.upsert_job_materialization_item(
                item_id=item_id,
                job_id=normalized_job_id,
                target_company=target_company or snapshot.target_company,
                snapshot_id=snapshot_id,
                item_kind=PROVIDER_SEARCH_RETRY_ITEM_KIND,
                source="search_seed_discovery",
                reason=str(retry_item.get("provider_retry_type") or "provider_retry"),
                status=queue_status,
                phase=phase,
                priority=-20,
                idempotency_key=item_id,
                max_attempts=max_attempts,
                metadata={
                    "provider_retry_item": retry_item,
                    "summary_path": str(snapshot.summary_path),
                    "entries_path": str(snapshot.entries_path or ""),
                    "target_company": target_company or snapshot.target_company,
                    "snapshot_id": snapshot_id,
                },
            )
            if queue_status == "failed":
                item = self.store.mark_job_materialization_item_failed(
                    item_id,
                    error_text=str(
                        retry_item.get("incomplete_reason")
                        or retry_item.get("provider_retry_type")
                        or "provider_search_retry_exhausted"
                    ),
                    retryable=False,
                    metadata={
                        "provider_retry_item": retry_item,
                        "terminal_writer": "search_seed_provider_retry_projection",
                    },
                )
            persisted_items.append(dict(item or {}))
        return {
            "status": "recorded",
            "item_kind": PROVIDER_SEARCH_RETRY_ITEM_KIND,
            "item_count": len(retry_items),
            "persisted_count": len([item for item in persisted_items if item]),
            "failed_count": len(
                [item for item in persisted_items if str(item.get("status") or "").strip().lower() == "failed"]
            ),
            "retryable_count": len(
                [
                    item
                    for item in persisted_items
                    if str(item.get("status") or "").strip().lower() in {"queued", "deferred", "failed_retryable"}
                ]
            ),
            "skipped_terminal_count": skipped_terminal_count,
            "item_ids": [str(item.get("item_id") or "") for item in persisted_items if str(item.get("item_id") or "")],
        }

    def _acquire_cohort_search_seed_pool(
        self,
        *,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
        identity: CompanyIdentity,
        snapshot_dir: Path,
    ) -> AcquisitionExecution:
        """Execute the canonical cohort lanes in isolated non-live runtimes.

        The provider compiler remains the sole owner of the physical lane set.
        This adapter only turns its bounded, combined result into the existing
        durable SearchSeedSnapshot contract so downstream acquisition and
        retrieval do not gain a second candidate representation.
        """

        request_payload = _effective_request_payload(job_request)
        plan_acquisition_strategy = dict(dict(state.get("plan_payload") or {}).get("acquisition_strategy") or {})
        # The full cohort manifest is plan-owned. A later compatibility task
        # must not narrow its compiler inputs (for example, to past-only
        # filters) and thereby create a second physical provider plan.
        filter_hints = dict(plan_acquisition_strategy.get("filter_hints") or {})
        compiler = CohortProviderCompiler()
        preview_manifest = compiler.compile(
            request_payload,
            base_filter_hints=filter_hints,
        )
        stored_manifest = dict(plan_acquisition_strategy.get("provider_execution_manifest") or {})
        if stored_manifest != preview_manifest:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Stored cohort provider manifest no longer matches the canonical request and plan inputs.",
                payload={
                    "reason": "cohort_provider_manifest_semantic_mismatch",
                    "cohort_provider_manifest": stored_manifest,
                    "expected_cohort_provider_manifest": preview_manifest,
                },
            )
        capability = cohort_execution_capability_for_runtime(
            runtime_dir=self.settings.runtime_dir,
        )
        manifest = compiler.compile(
            request_payload,
            base_filter_hints=filter_hints,
            execution_capability=capability,
        )
        if capability is None or not bool(manifest.get("execution_ready")):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Cohort execution is not enabled for this provider runtime.",
                payload={
                    "reason": str(manifest.get("execution_blocker") or "cohort_selection_execution_not_ready"),
                    "cohort_provider_manifest": manifest,
                },
            )

        cost_policy = self._task_cost_policy(task, job_request)
        allow_shared_provider_cache = bool(cost_policy.get("allow_shared_provider_cache", True))
        execution = self.harvest_profile_search_connector.search_profiles_for_cohort_manifest(
            manifest=manifest,
            execution_capability=capability,
            discovery_dir=snapshot_dir / "cohort_provider_discovery",
            role_proof_verifier=CohortHeadlineRoleProofVerifier(),
            asset_logger=AssetLogger(snapshot_dir),
            allow_shared_provider_cache=allow_shared_provider_cache,
            runtime_timing_overrides=_runtime_timing_overrides_from_request_payload(request_payload),
        )
        entries = [
            self._cohort_provider_row_to_search_seed_entry(
                row,
                manifest_digest=str(manifest.get("manifest_digest") or ""),
            )
            for row in list(execution.get("rows") or [])
            if isinstance(row, dict)
        ]
        lane_summaries = [dict(item) for item in list(execution.get("lane_summaries") or [])]
        query_summaries = [
            {
                "index": index,
                "lane": "profile_search",
                "lane_id": str(item.get("lane_id") or ""),
                "status": "completed",
                "employment_scope": str(item.get("employment_status") or "all"),
                "employment_status": str(item.get("employment_status") or "all"),
                "role_bucket_id": str(item.get("role_bucket_id") or ""),
                "result_count": int(item.get("row_count") or 0),
                "raw_path": str(item.get("raw_path") or ""),
                "strategy_type": self._task_strategy_type(task, job_request),
            }
            for index, item in enumerate(lane_summaries, start=1)
        ]
        lane_entries: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            memberships = [
                dict(item) for item in list(dict(entry.get("metadata") or {}).get("cohort_lane_membership") or [])
            ]
            statuses = {
                str(item.get("employment_status") or "").strip().lower()
                for item in memberships
                if str(item.get("employment_status") or "").strip()
            }
            for status in sorted(statuses or {str(entry.get("employment_status") or "all")}):
                lane_entries.setdefault(status, []).append(dict(entry))
        lane_payloads = {
            status: {
                "lane": "profile_search",
                "employment_scope": status,
                "employment_status": status,
                "strategy_type": self._task_strategy_type(task, job_request),
                "query_summaries": [
                    dict(item) for item in query_summaries if str(item.get("employment_status") or "all") == status
                ],
                "requested_filter_hints": dict(filter_hints),
                "effective_filter_hints": dict(filter_hints),
                "cohort_provider_manifest_digest": str(manifest.get("manifest_digest") or ""),
            }
            for status in lane_entries
        }
        result_summary_path = snapshot_dir / "cohort_provider_discovery" / "cohort_execution_result.json"
        candidate_documents_path = snapshot_dir / "candidate_documents.json"
        result_summary = _build_cohort_execution_result(
            manifest=manifest,
            execution=execution,
            entries=entries,
            lane_summaries=lane_summaries,
            execution_capability=capability.to_record(),
            artifact_path=result_summary_path,
            commit_marker_path=candidate_documents_path,
        )
        if not entries:
            all_rows_rejected = int(execution.get("rejected_unverified_count") or 0) > 0
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=(
                    "Cohort provider execution produced rows, but none had the required exact role proof."
                    if all_rows_rejected
                    else "Cohort provider execution completed but returned no candidate leads."
                ),
                payload={
                    "reason": (
                        "cohort_provider_all_rows_rejected" if all_rows_rejected else "cohort_provider_no_results"
                    ),
                    "cohort_execution_attempt": result_summary,
                },
            )
        AssetLogger(snapshot_dir).write_json(
            result_summary_path,
            result_summary,
            asset_type="cohort_execution_result",
            source_kind="cohort_provider_runtime",
            is_raw_asset=False,
            model_safe=True,
        )
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        snapshot = _persist_search_seed_snapshot(
            SearchSeedSnapshot(
                snapshot_id=snapshot_dir.name,
                target_company=identity.canonical_name,
                company_identity=identity,
                snapshot_dir=snapshot_dir,
                entries=entries,
                query_summaries=query_summaries,
                accounts_used=[str(manifest.get("provider") or "harvest_profile_search")],
                errors=[],
                stop_reason="cohort_provider_completed" if entries else "cohort_provider_no_results",
                summary_path=summary_path,
                entries_path=summary_path.parent / "entries.json",
                summary_payload={
                    "strategy_type": self._task_strategy_type(task, job_request),
                    "requested_filter_hints": dict(filter_hints),
                    "effective_filter_hints": dict(filter_hints),
                    COHORT_PUBLICATION_DIGEST_FIELD: str(result_summary.get(COHORT_PUBLICATION_DIGEST_FIELD) or ""),
                    "cohort_provider_manifest": manifest,
                    "cohort_execution_result": result_summary,
                    "cohort_execution_result_path": str(result_summary_path),
                },
                lane_payloads=lane_payloads,
                lane_entries=lane_entries,
            )
        )
        publication_digest = str(result_summary.get(COHORT_PUBLICATION_DIGEST_FIELD) or "")
        committed_publication_digest = cohort_publication_commit_digest_from_candidate_documents(snapshot_dir)
        if not publication_digest or committed_publication_digest != publication_digest:
            raise RuntimeError("Cohort publication did not commit its candidate-document digest marker.")
        committed_result_summary = {
            **result_summary,
            "publication_committed": True,
        }
        candidate_documents_projection = self._reawaken_waiting_prerequisite_after_candidate_documents_write(
            job_id=str(state.get("job_id") or ""),
            snapshot=snapshot,
        )
        profile_prefetch = self._queue_background_profile_prefetch_for_search_seed_entries(
            identity=identity,
            entries=entries,
            source_path=str(snapshot.summary_path),
            snapshot_dir=snapshot_dir,
            job_id=str(state.get("job_id") or ""),
            request_payload=request_payload,
            plan_payload=dict(state.get("plan_payload") or {}),
            runtime_mode=str(state.get("runtime_mode") or "workflow"),
            allow_shared_provider_cache=allow_shared_provider_cache,
            priority=True,
        )
        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=(
                f"Recovered {len(entries)} cohort candidates across {len(lane_summaries)} deterministic provider lanes."
            ),
            payload={
                **snapshot.to_record(),
                "strategy_type": self._task_strategy_type(task, job_request),
                "cost_policy": cost_policy,
                "profile_prefetch": dict(profile_prefetch),
                "candidate_documents_projection": candidate_documents_projection,
                "cohort_provider_manifest": manifest,
                "cohort_execution_result": committed_result_summary,
                "cohort_execution_result_path": str(result_summary_path),
            },
            state_updates={"search_seed_snapshot": snapshot},
        )

    @staticmethod
    def _cohort_provider_row_to_search_seed_entry(
        row: dict[str, Any],
        *,
        manifest_digest: str,
    ) -> dict[str, Any]:
        normalized = dict(row or {})
        memberships = [dict(item) for item in list(normalized.get("cohort_lane_membership") or [])]
        statuses = list(
            dict.fromkeys(
                str(item.get("employment_status") or "").strip().lower()
                for item in memberships
                if str(item.get("employment_status") or "").strip()
            )
        )
        role_bucket_ids = list(
            dict.fromkeys(
                str(item.get("role_bucket_id") or "").strip()
                for item in memberships
                if str(item.get("role_bucket_id") or "").strip()
            )
        )
        profile_url = str(normalized.get(COHORT_CANONICAL_PROFILE_URL_FIELD) or "").strip()
        username = str(normalized.get("username") or "").strip().strip("/")
        if not username and profile_url:
            username = profile_url.rstrip("/").rsplit("/", 1)[-1]
        metadata = {
            **dict(normalized.get("metadata") or {}),
            "cohort_provider_manifest_digest": manifest_digest,
            "cohort_lane_membership": memberships,
            "cohort_role_bucket_ids": role_bucket_ids,
            "cohort_employment_statuses": statuses,
            **(
                {"cohort_role_proof": dict(normalized.get("cohort_role_proof") or {})}
                if normalized.get("cohort_role_proof")
                else {}
            ),
        }
        return {
            **normalized,
            "profile_url": profile_url,
            "slug": username,
            "employment_status": (
                statuses[0]
                if len(statuses) == 1
                else ("current" if "current" in statuses else (statuses[0] if statuses else "all"))
            ),
            "source_query": f"cohort:{manifest_digest}",
            "source_type": "harvest_profile_search",
            "metadata": metadata,
        }

    def _acquire_search_seed_pool(
        self,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
    ) -> AcquisitionExecution:
        identity = state.get("company_identity")
        snapshot_dir = state.get("snapshot_dir")
        if not isinstance(identity, CompanyIdentity) or not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Company identity must be resolved before search-seed acquisition.",
                payload={},
            )

        cohort = explicit_cohort_selection(_effective_request_payload(job_request))
        if cohort is not None and str(cohort.get("source") or "") == "user_explicit":
            return self._acquire_cohort_search_seed_pool(
                task=task,
                state=state,
                job_request=job_request,
                identity=identity,
                snapshot_dir=snapshot_dir,
            )

        filter_hints = self._task_filter_hints(task, job_request)
        search_seed_queries = self._effective_search_seed_queries(task, job_request)
        query_bundles = self._task_search_query_bundles(task, job_request)
        cost_policy = self._task_cost_policy(task, job_request)
        allow_shared_provider_cache = bool(cost_policy.get("allow_shared_provider_cache", True))
        job_id = str(state.get("job_id") or "")
        effective_request_payload = _effective_request_payload(job_request)
        plan_payload = dict(state.get("plan_payload") or {})
        runtime_mode = str(state.get("runtime_mode") or "workflow")
        incremental_prefetched_profile_urls: set[str] = set()
        incremental_requested_profile_urls: set[str] = set()
        incremental_prefetch_events: list[dict[str, Any]] = []
        baseline_snapshot_id = self._delta_baseline_snapshot_id(task, job_request)
        delta_execution_plan = self._task_delta_execution_plan(task, job_request)
        baseline_selection_explanation = self._task_baseline_selection_explanation(task, job_request)
        parallel_former_executor: ThreadPoolExecutor | None = None
        parallel_former_future = None
        parallel_former_execution: AcquisitionExecution | None = None
        should_run_parallel_former_search_seed = False

        def _start_scoped_parallel_former_search_seed_if_needed() -> None:
            nonlocal parallel_former_executor, parallel_former_future
            if not should_run_parallel_former_search_seed or parallel_former_future is not None:
                return
            parallel_former_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="acquisition-scoped-former-seed",
            )
            parallel_former_future = parallel_former_executor.submit(
                self._acquire_default_former_search_seed,
                task,
                dict(state),
                job_request,
                identity,
            )

        def _wait_for_scoped_parallel_former_search_seed() -> AcquisitionExecution | None:
            nonlocal parallel_former_executor, parallel_former_future, parallel_former_execution
            if parallel_former_future is None:
                return parallel_former_execution
            try:
                parallel_former_execution = parallel_former_future.result()
            finally:
                if parallel_former_executor is not None:
                    parallel_former_executor.shutdown(wait=True, cancel_futures=False)
                parallel_former_executor = None
                parallel_former_future = None
            return parallel_former_execution

        def _cancel_scoped_parallel_former_search_seed() -> None:
            nonlocal parallel_former_executor, parallel_former_future
            if parallel_former_future is not None:
                parallel_former_future.cancel()
            if parallel_former_executor is not None:
                parallel_former_executor.shutdown(wait=True, cancel_futures=True)
            parallel_former_executor = None
            parallel_former_future = None

        if (
            self._task_delta_noop(task, job_request) or bool(delta_execution_plan.get("delta_noop"))
        ) and baseline_snapshot_id:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="completed",
                detail=(
                    f"Search-seed acquisition skipped; baseline snapshot {baseline_snapshot_id} already covers "
                    "the required query scope."
                ),
                payload={
                    "strategy_type": self._task_strategy_type(task, job_request),
                    "baseline_snapshot_id": baseline_snapshot_id,
                    "delta_execution_noop": True,
                    "delta_execution_reason": self._task_delta_reason(task, job_request),
                    "baseline_selection_explanation": baseline_selection_explanation,
                },
            )
        employment_statuses = self._task_employment_statuses(task, job_request)
        employment_status = employment_statuses[0] if employment_statuses else "current"
        should_run_parallel_former_search_seed = (
            self._task_strategy_type(task, job_request) == "scoped_search_roster"
            and self._task_include_former_search_seed(task, job_request)
            and str(employment_status or "").strip().lower() != "former"
        )
        provider_only_former_pass = employment_status == "former" and bool(
            list(filter_hints.get("past_companies") or []) or str(identity.linkedin_company_url or "").strip()
        )
        if not search_seed_queries and not query_bundles and not provider_only_former_pass:
            if baseline_snapshot_id:
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail=(
                        f"No uncovered search-seed queries remain; baseline snapshot {baseline_snapshot_id} "
                        "will be reused downstream."
                    ),
                    payload={
                        "strategy_type": self._task_strategy_type(task, job_request),
                        "baseline_snapshot_id": baseline_snapshot_id,
                        "delta_execution_noop": True,
                        "baseline_selection_explanation": baseline_selection_explanation,
                    },
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Search-seed acquisition requires compiled seed queries.",
                payload={"strategy_type": self._task_strategy_type(task, job_request)},
            )

        _start_scoped_parallel_former_search_seed_if_needed()

        def _queue_incremental_search_seed_prefetch(result: dict[str, Any]) -> None:
            incremental_entries = list(result.get("entries") or [])
            if not incremental_entries:
                return
            for entry in incremental_entries:
                profile_url = str(dict(entry or {}).get("profile_url") or "").strip()
                if profile_url:
                    incremental_requested_profile_urls.add(profile_url)
            source_path = str(result.get("raw_path") or "").strip()
            if not source_path:
                source_path = str(snapshot_dir / "search_seed_discovery" / "summary.json")
            candidate_documents_projection = self._project_incremental_search_seed_candidate_documents(
                job_id=job_id,
                identity=identity,
                snapshot_dir=snapshot_dir,
                entries=[dict(entry) for entry in incremental_entries if isinstance(entry, dict)],
                summary=dict(result.get("summary") or {}),
                raw_path=source_path,
                employment_status=str(result.get("employment_status") or employment_status),
            )
            prefetch_result = self._queue_background_profile_prefetch_for_search_seed_entries(
                identity=identity,
                entries=incremental_entries,
                source_path=source_path,
                snapshot_dir=snapshot_dir,
                job_id=job_id,
                request_payload=effective_request_payload,
                plan_payload=plan_payload,
                runtime_mode=runtime_mode,
                allow_shared_provider_cache=allow_shared_provider_cache,
                priority=True,
                exclude_profile_urls=incremental_prefetched_profile_urls,
            )
            prefetch_result["candidate_documents_projection"] = dict(candidate_documents_projection or {})
            incremental_prefetch_events.append(dict(prefetch_result))
            failed_urls = {
                str(profile_url or "").strip()
                for profile_url in list(prefetch_result.get("failed_urls") or [])
                if str(profile_url or "").strip()
            }
            queued_urls = [
                str(profile_url or "").strip()
                for profile_url in list(prefetch_result.get("queued_urls") or [])
                if str(profile_url or "").strip()
            ]
            if queued_urls:
                incremental_prefetched_profile_urls.update(queued_urls)
            if str(prefetch_result.get("status") or "").strip().lower() in {"queued", "completed"}:
                for entry in incremental_entries:
                    profile_url = str(dict(entry or {}).get("profile_url") or "").strip()
                    if profile_url and profile_url not in failed_urls:
                        incremental_prefetched_profile_urls.add(profile_url)

        try:
            snapshot = self.search_seed_acquirer.discover(
                identity,
                snapshot_dir,
                asset_logger=AssetLogger(snapshot_dir),
                search_seed_queries=search_seed_queries,
                query_bundles=query_bundles,
                filter_hints=filter_hints,
                cost_policy=cost_policy,
                employment_status=employment_status,
                worker_runtime=self.worker_runtime,
                job_id=job_id,
                request_payload=effective_request_payload,
                plan_payload=plan_payload,
                runtime_mode=runtime_mode,
                intent_view=self._task_execution_view(task, job_request),
                delta_execution_plan=self._task_delta_execution_plan(task, job_request),
                lane_context={
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                    "strategy_type": self._task_strategy_type(task, job_request),
                    "employment_status": employment_status,
                    "baseline_snapshot_id": baseline_snapshot_id,
                    "asset_reuse_plan": self._task_asset_reuse_plan(task, job_request),
                },
                on_incremental_query_result=_queue_incremental_search_seed_prefetch,
            )
        except Exception:
            _cancel_scoped_parallel_former_search_seed()
            raise
        snapshot = _persist_search_seed_snapshot(snapshot)
        candidate_documents_projection = self._reawaken_waiting_prerequisite_after_candidate_documents_write(
            job_id=job_id,
            snapshot=snapshot,
        )
        profile_prefetch = self._queue_background_profile_prefetch_for_search_seed_entries(
            identity=identity,
            entries=list(snapshot.entries or []),
            source_path=str(snapshot.summary_path),
            snapshot_dir=snapshot_dir,
            job_id=job_id,
            request_payload=effective_request_payload,
            plan_payload=plan_payload,
            runtime_mode=runtime_mode,
            allow_shared_provider_cache=allow_shared_provider_cache,
            priority=True,
            exclude_profile_urls=incremental_prefetched_profile_urls,
        )
        requested_profile_urls = set(incremental_requested_profile_urls)
        requested_profile_urls.update(
            {
                str(dict(entry or {}).get("profile_url") or "").strip()
                for entry in list(snapshot.entries or [])
                if str(dict(entry or {}).get("profile_url") or "").strip()
            }
        )
        profile_prefetch = _merge_profile_prefetch_dispatch_summaries(
            [*incremental_prefetch_events, dict(profile_prefetch)],
            requested_profile_urls=requested_profile_urls,
        )
        former_profile_prefetch: dict[str, Any] = {}
        former_search_seed_status = ""
        former_search_seed_detail = ""
        former_search_seed_error = ""
        former_execution = _wait_for_scoped_parallel_former_search_seed()
        if former_execution is not None:
            former_search_seed_status = str(former_execution.status or "").strip()
            former_search_seed_detail = str(former_execution.detail or "").strip()
            if former_search_seed_status == "blocked":
                former_search_seed_error = former_search_seed_detail
            incoming_snapshot = former_execution.state_updates.get("search_seed_snapshot")
            if isinstance(incoming_snapshot, SearchSeedSnapshot):
                merged_snapshot = _merge_search_seed_snapshots(snapshot, incoming_snapshot)
                if isinstance(merged_snapshot, SearchSeedSnapshot):
                    snapshot = merged_snapshot
                    candidate_documents_projection = (
                        self._reawaken_waiting_prerequisite_after_candidate_documents_write(
                            job_id=job_id,
                            snapshot=snapshot,
                        )
                    )
            former_profile_prefetch = dict(dict(former_execution.payload or {}).get("profile_prefetch") or {})
            if former_profile_prefetch:
                requested_profile_urls.update(
                    {
                        str(dict(entry or {}).get("profile_url") or "").strip()
                        for entry in list(snapshot.entries or [])
                        if str(dict(entry or {}).get("profile_url") or "").strip()
                    }
                )
                profile_prefetch = _merge_profile_prefetch_dispatch_summaries(
                    [dict(profile_prefetch), former_profile_prefetch],
                    requested_profile_urls=requested_profile_urls,
                )
        provider_retry_items = collect_search_seed_provider_retry_items(snapshot)
        provider_retry_projection = self._record_search_seed_provider_retry_items(
            job_id=job_id,
            target_company=identity.canonical_name,
            snapshot=snapshot,
        )
        provider_retry_payload = {
            "provider_retry_items": provider_retry_items,
            "provider_retry_item_count": len(provider_retry_items),
            "provider_retry_projection": provider_retry_projection,
        }
        queued_query_count = len(
            [item for item in list(snapshot.query_summaries or []) if str(item.get("status") or "") == "queued"]
        )
        incomplete_provider_query_count = _search_seed_incomplete_provider_query_count(snapshot)
        if snapshot.stop_reason == "queued_background_search" or queued_query_count > 0:
            payload = {
                **snapshot.to_record(),
                "strategy_type": self._task_strategy_type(task, job_request),
                "cost_policy": cost_policy,
                "queued_query_count": queued_query_count,
                "profile_prefetch": dict(profile_prefetch),
                "candidate_documents_projection": dict(candidate_documents_projection),
                **provider_retry_payload,
            }
            if former_search_seed_status:
                payload["parallel_former_search_seed_status"] = former_search_seed_status
            if former_search_seed_detail:
                payload["parallel_former_search_seed_detail"] = former_search_seed_detail
            if former_search_seed_error:
                payload["parallel_former_search_seed_error"] = former_search_seed_error
            if snapshot.entries:
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail=(
                        f"Recovered {len(snapshot.entries)} search-seed candidates while "
                        f"{queued_query_count} background web searches continue; downstream enrichment can proceed "
                        "and completed workers will reconcile later."
                    ),
                    payload=payload,
                    state_updates={"search_seed_snapshot": snapshot},
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=(
                    f"Search-seed acquisition queued {queued_query_count} background web searches; "
                    "resume the worker daemon to finish remote task_get before fallback or downstream enrichment."
                ),
                payload=payload,
                state_updates={"search_seed_snapshot": snapshot},
            )
        if snapshot.stop_reason == "provider_people_search_incomplete" or incomplete_provider_query_count > 0:
            payload = {
                **snapshot.to_record(),
                "strategy_type": self._task_strategy_type(task, job_request),
                "cost_policy": cost_policy,
                "incomplete_provider_query_count": incomplete_provider_query_count,
                "profile_prefetch": dict(profile_prefetch),
                "candidate_documents_projection": dict(candidate_documents_projection),
                **provider_retry_payload,
            }
            if former_search_seed_status:
                payload["parallel_former_search_seed_status"] = former_search_seed_status
            if former_search_seed_detail:
                payload["parallel_former_search_seed_detail"] = former_search_seed_detail
            if former_search_seed_error:
                payload["parallel_former_search_seed_error"] = former_search_seed_error
            blocked_detail = (
                "Search-seed acquisition exhausted at least one provider retry item. The workflow is blocked "
                "on the durable provider_search_retry item instead of treating this as a normal zero-result lane."
                if provider_retry_items
                else (
                    "Search-seed acquisition recovered partial provider results, but at least one Harvest "
                    "profile-search query reported more remote results than were fetched. Retry after provider "
                    "page-chunk recovery completes before marking Stage 1 final."
                )
            )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=blocked_detail,
                payload=payload,
                state_updates={"search_seed_snapshot": snapshot},
            )
        if not snapshot.entries:
            if baseline_snapshot_id:
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail=(
                        f"Search-seed acquisition found no new candidates; baseline snapshot {baseline_snapshot_id} "
                        "will carry downstream normalization and retrieval."
                    ),
                    payload={
                        **snapshot.to_record(),
                        "strategy_type": self._task_strategy_type(task, job_request),
                        "baseline_snapshot_id": baseline_snapshot_id,
                        "delta_execution_noop": True,
                        "delta_execution_reason": "no_new_search_seed_entries",
                        "baseline_selection_explanation": baseline_selection_explanation,
                        "candidate_documents_projection": dict(candidate_documents_projection),
                    },
                    state_updates={"search_seed_snapshot": snapshot},
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Search-seed acquisition finished but did not recover any candidate leads.",
                payload=snapshot.to_record(),
                state_updates={"search_seed_snapshot": snapshot},
            )

        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=(
                f"Recovered {len(snapshot.entries)} search-seed candidates using "
                f"{'/'.join(self._task_search_channel_order(task, job_request) or ['web_search'])}."
            ),
            payload={
                **snapshot.to_record(),
                "strategy_type": self._task_strategy_type(task, job_request),
                "cost_policy": cost_policy,
                "profile_prefetch": dict(profile_prefetch),
                "candidate_documents_projection": dict(candidate_documents_projection),
                **provider_retry_payload,
                **(
                    {
                        "parallel_former_search_seed_status": former_search_seed_status,
                        "parallel_former_search_seed_detail": former_search_seed_detail,
                    }
                    if former_search_seed_status
                    else {}
                ),
            },
            state_updates={"search_seed_snapshot": snapshot},
        )

    def _acquire_former_search_seed(
        self,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
    ) -> AcquisitionExecution:
        identity = state.get("company_identity")
        snapshot_dir = state.get("snapshot_dir")
        if not isinstance(identity, CompanyIdentity) or not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Company identity must be resolved before former-member LinkedIn search can run.",
                payload={},
            )
        expected_queries = [
            str(query or "").strip()
            for query in self._task_execution_list(task, job_request, "search_seed_queries")
            if str(query or "").strip()
        ]
        explicit_cohort = explicit_cohort_selection(_effective_request_payload(job_request))
        explicit_user_cohort = bool(
            explicit_cohort is not None and str(explicit_cohort.get("source") or "") == "user_explicit"
        )
        plan_acquisition_strategy: dict[str, Any] = {}
        stored_manifest: dict[str, Any] = {}
        if explicit_user_cohort:
            # Loading with lane backfill and merging snapshots are both durable writers. The exact
            # plan-owned manifest must therefore be authorized before either compatibility action.
            plan_acquisition_strategy = dict(dict(state.get("plan_payload") or {}).get("acquisition_strategy") or {})
            stored_manifest = dict(plan_acquisition_strategy.get("provider_execution_manifest") or {})
            expected_manifest = CohortProviderCompiler().compile(
                _effective_request_payload(job_request),
                base_filter_hints=dict(plan_acquisition_strategy.get("filter_hints") or {}),
            )
            if stored_manifest != expected_manifest:
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="blocked",
                    detail="Stored cohort provider manifest no longer matches the canonical request and plan inputs.",
                    payload={
                        "reason": "cohort_provider_manifest_semantic_mismatch",
                        "cohort_provider_manifest": stored_manifest,
                        "expected_cohort_provider_manifest": expected_manifest,
                    },
                )

        existing_search_seed_snapshot = state.get("search_seed_snapshot")
        durable_search_seed_snapshot = _registry_load_search_seed_snapshot_from_snapshot_dir(
            snapshot_dir,
            identity=identity,
            auto_backfill_lanes=not explicit_user_cohort,
        )
        reusable_search_seed_snapshot = (
            durable_search_seed_snapshot
            if explicit_user_cohort
            else _merge_search_seed_snapshots(
                existing_search_seed_snapshot
                if isinstance(existing_search_seed_snapshot, SearchSeedSnapshot)
                else None,
                durable_search_seed_snapshot,
            )
        )
        if explicit_user_cohort:
            summary_payload = (
                dict(reusable_search_seed_snapshot.summary_payload or {})
                if isinstance(reusable_search_seed_snapshot, SearchSeedSnapshot)
                else {}
            )
            executed_manifest = dict(summary_payload.get("cohort_provider_manifest") or {})
            execution_result = dict(summary_payload.get("cohort_execution_result") or {})
            stored_compiler_inputs = dict(stored_manifest.get("compiler_inputs") or {})
            executed_compiler_inputs = dict(executed_manifest.get("compiler_inputs") or {})
            stored_lane_digests = {
                str(lane.get("lane_id") or ""): str(lane.get("lane_digest") or "")
                for lane in list(stored_manifest.get("lanes") or [])
                if isinstance(lane, dict) and str(lane.get("lane_id") or "").strip()
            }
            executed_lane_digests = {
                str(lane.get("lane_id") or ""): str(lane.get("lane_digest") or "")
                for lane in list(executed_manifest.get("lanes") or [])
                if isinstance(lane, dict) and str(lane.get("lane_id") or "").strip()
            }
            expected_former_lane_ids = {
                str(lane.get("lane_id") or "")
                for lane in list(stored_manifest.get("lanes") or [])
                if isinstance(lane, dict)
                and str(lane.get("employment_status") or "").strip().lower() == "former"
                and str(lane.get("lane_id") or "").strip()
            }
            completed_former_lane_ids = {
                str(lane.get("lane_id") or "")
                for lane in list(execution_result.get("lane_summaries") or [])
                if isinstance(lane, dict)
                and str(lane.get("employment_status") or "").strip().lower() == "former"
                and str(lane.get("lane_id") or "").strip()
            }
            manifest_digest = str(stored_manifest.get("manifest_digest") or "")
            executed_manifest_digest = str(executed_manifest.get("manifest_digest") or "")
            reusable_full_manifest = bool(
                isinstance(reusable_search_seed_snapshot, SearchSeedSnapshot)
                and expected_former_lane_ids
                and expected_former_lane_ids.issubset(completed_former_lane_ids)
                and manifest_digest
                and executed_manifest_digest
                and bool(executed_manifest.get("execution_ready"))
                and stored_lane_digests == executed_lane_digests
                and stored_compiler_inputs.get("cohort_selection") == executed_compiler_inputs.get("cohort_selection")
                and stored_compiler_inputs.get("base_filter_hints") == executed_compiler_inputs.get("base_filter_hints")
                and stored_compiler_inputs.get("requested_result_limit")
                == executed_compiler_inputs.get("requested_result_limit")
                and str(execution_result.get("cohort_provider_manifest_digest") or "") == executed_manifest_digest
                and str(execution_result.get("result_digest") or "")
                and int(execution_result.get("missing_required_lane_count") or 0) == 0
            )
            if reusable_full_manifest:
                former_entries = _search_seed_snapshot_lane_entries(reusable_search_seed_snapshot, "former")
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail=(
                        "The canonical cohort manifest already completed every requested former-member lane; "
                        "skipped duplicate provider execution."
                    ),
                    payload={
                        **reusable_search_seed_snapshot.to_record(),
                        "lane_entry_count": len(former_entries),
                        "reused_existing_full_manifest": True,
                        "planned_cohort_provider_manifest_digest": manifest_digest,
                        "cohort_provider_manifest_digest": executed_manifest_digest,
                        "completed_former_lane_ids": sorted(completed_former_lane_ids),
                    },
                    state_updates={"search_seed_snapshot": reusable_search_seed_snapshot},
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=(
                    "The compatibility former-member task requires a durable result for every former lane in "
                    "the canonical cohort manifest."
                ),
                payload={
                    "reason": "cohort_full_manifest_result_not_reusable",
                    "planned_cohort_provider_manifest_digest": manifest_digest,
                    "cohort_provider_manifest_digest": executed_manifest_digest,
                    "expected_former_lane_ids": sorted(expected_former_lane_ids),
                    "completed_former_lane_ids": sorted(completed_former_lane_ids),
                },
            )
        if _search_seed_snapshot_satisfies_lane_queries(
            reusable_search_seed_snapshot,
            lane="former",
            expected_queries=expected_queries,
        ):
            former_entries = _search_seed_snapshot_lane_entries(reusable_search_seed_snapshot, "former")
            return AcquisitionExecution(
                task_id=task.task_id,
                status="completed",
                detail=(
                    f"Former-member search seed lane already has {len(former_entries)} durable entries; "
                    "skipped duplicate provider execution."
                ),
                payload={
                    **reusable_search_seed_snapshot.to_record(),
                    "lane_entry_count": len(former_entries),
                    "reused_existing_search_seed_lane": True,
                    "reused_existing_search_seed_lane_key": "former",
                    "profile_prefetch": {
                        "status": "reused_existing_lane",
                        "requested_url_count": len(former_entries),
                        "dispatched_url_count": 0,
                        "queued_worker_count": 0,
                        "cached_profile_count": 0,
                        "queued_urls": [],
                        "failed_urls": [],
                    },
                },
                state_updates={"search_seed_snapshot": reusable_search_seed_snapshot},
            )
        filter_hints = _build_former_filter_hints(
            identity=identity,
            base_filter_hints=self._task_filter_hints(task, job_request),
        )
        cost_policy = self._task_cost_policy(task, job_request)
        former_cost_policy = {
            **cost_policy,
        }
        base_intent_view = self._task_execution_view(task, job_request)
        former_search_contract = normalize_former_member_search_contract(
            strategy_type="former_employee_search",
            employment_statuses=list(
                base_intent_view.get("employment_statuses") or task.metadata.get("employment_statuses") or ["former"]
            ),
            search_channel_order=list(
                base_intent_view.get("search_channel_order")
                or task.metadata.get("search_channel_order")
                or ["harvest_profile_search"]
            ),
            cost_policy=former_cost_policy,
            min_expected_results=int(
                base_intent_view.get("former_provider_people_search_min_expected_results")
                or task.metadata.get("former_provider_people_search_min_expected_results")
                or 50
            ),
        )
        former_cost_policy = dict(former_search_contract.get("cost_policy") or former_cost_policy)
        former_search_channel_order = list(
            former_search_contract.get("search_channel_order") or task.metadata.get("search_channel_order") or []
        )
        provider_search_only = bool(former_search_contract.get("provider_search_only"))
        delegated_intent_view = {
            **base_intent_view,
            "strategy_type": "former_employee_search",
            "employment_statuses": ["former"],
            "filter_hints": filter_hints,
            "filter_keywords": list(filter_hints.get("keywords") or []),
            "function_ids": list(filter_hints.get("function_ids") or []),
            "cost_policy": former_cost_policy,
        }
        if former_search_channel_order:
            delegated_intent_view["search_channel_order"] = former_search_channel_order
        if provider_search_only:
            delegated_intent_view["search_query_bundles"] = []
        delegated_metadata = {
            **dict(task.metadata or {}),
            "strategy_type": "former_employee_search",
            "employment_statuses": ["former"],
            "filter_hints": filter_hints,
            "cost_policy": former_cost_policy,
            "intent_view": delegated_intent_view,
        }
        if former_search_channel_order:
            delegated_metadata["search_channel_order"] = former_search_channel_order
        if provider_search_only:
            delegated_metadata["search_query_bundles"] = []
        explicit_task = AcquisitionTask(
            task_id=task.task_id,
            task_type="acquire_search_seed_pool",
            title=task.title,
            description=task.description,
            source_hint=task.source_hint,
            status="ready",
            blocking=task.blocking,
            metadata=delegated_metadata,
        )
        delegated_execution = self._normalize_zero_result_search_seed_execution(
            self._acquire_search_seed_pool(explicit_task, state, job_request),
            detail="Former-member search completed but did not add any new candidates.",
        )
        incoming_snapshot = delegated_execution.state_updates.get("search_seed_snapshot")
        existing_snapshot = state.get("search_seed_snapshot")
        if not isinstance(incoming_snapshot, SearchSeedSnapshot):
            return delegated_execution
        if not isinstance(existing_snapshot, SearchSeedSnapshot):
            return delegated_execution

        merged_snapshot = _merge_search_seed_snapshots(existing_snapshot, incoming_snapshot)
        if not isinstance(merged_snapshot, SearchSeedSnapshot):
            return delegated_execution
        payload = {
            **dict(delegated_execution.payload or {}),
            **merged_snapshot.to_record(),
            "lane_entry_count": len(incoming_snapshot.entries),
            "merged_search_seed_entry_count": len(merged_snapshot.entries),
            "merged_search_seed_query_count": len(merged_snapshot.query_summaries),
        }
        state_updates = {
            **dict(delegated_execution.state_updates or {}),
            "search_seed_snapshot": merged_snapshot,
        }
        return AcquisitionExecution(
            task_id=delegated_execution.task_id,
            status=delegated_execution.status,
            detail=delegated_execution.detail,
            payload=payload,
            state_updates=state_updates,
        )

    def _enrich_profiles(
        self, task: AcquisitionTask, state: dict[str, Any], job_request: JobRequest
    ) -> AcquisitionExecution:
        snapshot = state.get("roster_snapshot")
        search_seed_snapshot = state.get("search_seed_snapshot")
        snapshot_dir = state.get("snapshot_dir")
        strategy_type = self._task_strategy_type(task, job_request)
        cost_policy = self._task_cost_policy(task, job_request)
        enrichment_scope = self._task_enrichment_scope(task, job_request)
        if not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Snapshot directory must exist before profile enrichment.",
                payload={},
            )
        effective_request_payload = _effective_request_payload(job_request)
        effective_target_company = str(
            effective_request_payload.get("target_company") or job_request.target_company or ""
        ).strip()
        stage_source_kind = (
            "linkedin_profile_enrichment"
            if enrichment_scope == "linkedin_stage_1"
            else ("public_web_enrichment" if enrichment_scope == "public_web_stage_2" else "multisource_enrichment")
        )
        stage_mode = (
            "linkedin_stage_1"
            if enrichment_scope == "linkedin_stage_1"
            else ("linkedin_plus_public_web" if enrichment_scope == "public_web_stage_2" else "roster_plus_multisource")
        )
        stage_archive_path = (
            snapshot_dir / "candidate_documents.linkedin_stage_1.json"
            if enrichment_scope == "linkedin_stage_1"
            else (
                snapshot_dir / "candidate_documents.public_web_stage_2.json"
                if enrichment_scope == "public_web_stage_2"
                else None
            )
        )

        def _stage_request() -> JobRequest:
            base_payload = dict(effective_request_payload)
            source_limits = self._task_stage_request_limits(task, job_request)
            if enrichment_scope == "linkedin_stage_1":
                base_payload.update(
                    {
                        "slug_resolution_limit": int(source_limits.get("slug_resolution_limit") or 0),
                        "profile_detail_limit": int(source_limits.get("profile_detail_limit") or 0),
                        "publication_scan_limit": 0,
                        "publication_lead_limit": 0,
                        "exploration_limit": 0,
                        "scholar_coauthor_follow_up_limit": 0,
                    }
                )
                return JobRequest.from_payload(base_payload)
            if enrichment_scope == "public_web_stage_2":
                base_payload.update(
                    {
                        "slug_resolution_limit": 0,
                        "profile_detail_limit": 0,
                        "publication_scan_limit": int(source_limits.get("publication_scan_limit") or 0),
                        "publication_lead_limit": int(source_limits.get("publication_lead_limit") or 0),
                        "exploration_limit": int(source_limits.get("exploration_limit") or 0),
                        "scholar_coauthor_follow_up_limit": int(
                            source_limits.get("scholar_coauthor_follow_up_limit") or 0
                        ),
                    }
                )
                return JobRequest.from_payload(base_payload)
            base_payload.update(
                {
                    "slug_resolution_limit": int(source_limits.get("slug_resolution_limit") or 0),
                    "profile_detail_limit": int(source_limits.get("profile_detail_limit") or 0),
                    "publication_scan_limit": int(source_limits.get("publication_scan_limit") or 0),
                    "publication_lead_limit": int(source_limits.get("publication_lead_limit") or 0),
                    "exploration_limit": int(source_limits.get("exploration_limit") or 0),
                    "scholar_coauthor_follow_up_limit": int(source_limits.get("scholar_coauthor_follow_up_limit") or 0),
                }
            )
            return JobRequest.from_payload(base_payload)

        canonicalization_summary: dict[str, Any] = {}
        source_snapshots: dict[str, Any] = {}
        candidates: list[Candidate] = []
        evidence: list[EvidenceRecord] = []

        snapshot, search_seed_snapshot = self._hydrate_acquisition_snapshots_for_enrichment(
            state=state,
            roster_snapshot=snapshot if isinstance(snapshot, CompanyRosterSnapshot) else None,
            search_seed_snapshot=(
                search_seed_snapshot if isinstance(search_seed_snapshot, SearchSeedSnapshot) else None
            ),
            snapshot_dir=snapshot_dir,
            fallback_target_company=effective_target_company,
        )

        def _reuse_delta_baseline_if_available() -> AcquisitionExecution | None:
            delta_baseline_snapshot_id = self._delta_baseline_snapshot_id(task, job_request)
            baseline_selection_explanation = self._task_baseline_selection_explanation(task, job_request)
            delta_baseline_material = self._load_delta_baseline_material(
                job_request,
                baseline_snapshot_id=delta_baseline_snapshot_id,
                enrichment_scope=enrichment_scope,
            )
            if not delta_baseline_snapshot_id or not delta_baseline_material:
                return None
            logger = AssetLogger(snapshot_dir)
            candidate_doc_path = snapshot_dir / "candidate_documents.json"
            baseline_document_payload = dict(delta_baseline_material.get("candidate_document_payload") or {})
            authoritative_document_payload = dict(
                delta_baseline_material.get("authoritative_candidate_document_payload") or {}
            )
            baseline_source_candidate_doc_path = str(delta_baseline_material.get("source_candidate_doc_path") or "")
            authoritative_source_candidate_doc_path = str(
                delta_baseline_material.get("authoritative_source_path") or ""
            )
            downstream_document_payload = dict(baseline_document_payload)
            downstream_source_candidate_doc_path = baseline_source_candidate_doc_path
            downstream_public_web_ready = False
            if enrichment_scope == "linkedin_stage_1" and authoritative_document_payload:
                downstream_document_payload = dict(authoritative_document_payload)
                downstream_source_candidate_doc_path = (
                    authoritative_source_candidate_doc_path or baseline_source_candidate_doc_path
                )
                downstream_public_web_ready = True

            def _materialized_delta_payload(
                source_payload: dict[str, Any],
                *,
                source_candidate_doc_path: str,
                payload_stage_mode: str,
                payload_enrichment_scope: str,
            ) -> tuple[dict[str, Any], int, int]:
                materialized_candidate_count = int(
                    source_payload.get("candidate_count") or len(list(source_payload.get("candidates") or []))
                )
                materialized_evidence_count = int(
                    source_payload.get("evidence_count") or len(list(source_payload.get("evidence") or []))
                )
                return (
                    {
                        **source_payload,
                        "candidate_count": materialized_candidate_count,
                        "evidence_count": materialized_evidence_count,
                        "enrichment_mode": payload_stage_mode,
                        "enrichment_scope": payload_enrichment_scope,
                        "delta_baseline_reuse": {
                            "baseline_snapshot_id": delta_baseline_snapshot_id,
                            "baseline_snapshot_dir": str(delta_baseline_material.get("snapshot_dir") or ""),
                            "source_candidate_doc_path": source_candidate_doc_path,
                            "baseline_selection_explanation": baseline_selection_explanation,
                        },
                        "acquisition_stage": {
                            **self._task_execution_phase(task, job_request),
                            "task_id": task.task_id,
                            "task_type": task.task_type,
                        },
                    },
                    materialized_candidate_count,
                    materialized_evidence_count,
                )

            materialized_payload, candidate_count, evidence_count = _materialized_delta_payload(
                downstream_document_payload,
                source_candidate_doc_path=downstream_source_candidate_doc_path,
                payload_stage_mode=(
                    "linkedin_plus_public_web"
                    if downstream_public_web_ready and enrichment_scope == "linkedin_stage_1"
                    else stage_mode
                ),
                payload_enrichment_scope=(
                    "public_web_stage_2"
                    if downstream_public_web_ready and enrichment_scope == "linkedin_stage_1"
                    else enrichment_scope
                ),
            )
            logger.write_json(
                candidate_doc_path,
                materialized_payload,
                asset_type="candidate_documents",
                source_kind=f"{stage_source_kind}:delta_baseline_reuse",
                is_raw_asset=False,
                model_safe=True,
            )
            if isinstance(stage_archive_path, Path):
                stage_archive_payload, _, _ = _materialized_delta_payload(
                    baseline_document_payload,
                    source_candidate_doc_path=baseline_source_candidate_doc_path,
                    payload_stage_mode=stage_mode,
                    payload_enrichment_scope=enrichment_scope,
                )
                logger.write_json(
                    stage_archive_path,
                    stage_archive_payload,
                    asset_type="candidate_documents_stage_archive",
                    source_kind=f"{stage_source_kind}:delta_baseline_reuse",
                    is_raw_asset=False,
                    model_safe=True,
                )
            state_updates: dict[str, Any] = {
                "candidate_doc_path": candidate_doc_path,
            }
            if enrichment_scope == "linkedin_stage_1":
                state_updates["linkedin_stage_completed"] = True
                if isinstance(stage_archive_path, Path):
                    state_updates["linkedin_stage_candidate_doc_path"] = stage_archive_path
                if downstream_public_web_ready:
                    state_updates["public_web_stage_completed"] = True
                    state_updates["public_web_stage_candidate_doc_path"] = candidate_doc_path
            elif enrichment_scope == "public_web_stage_2":
                state_updates["public_web_stage_completed"] = True
                if isinstance(stage_archive_path, Path):
                    state_updates["public_web_stage_candidate_doc_path"] = stage_archive_path
            return AcquisitionExecution(
                task_id=task.task_id,
                status="completed",
                detail=(
                    f"No new {enrichment_scope.replace('_', ' ')} candidates were acquired; "
                    f"reused baseline snapshot {delta_baseline_snapshot_id} for downstream retrieval."
                ),
                payload={
                    "candidate_count": candidate_count,
                    "evidence_count": evidence_count,
                    "candidate_doc_path": str(candidate_doc_path),
                    "baseline_snapshot_id": delta_baseline_snapshot_id,
                    "delta_baseline_reuse": True,
                    "baseline_selection_explanation": baseline_selection_explanation,
                    "enrichment_mode": stage_mode,
                    "enrichment_scope": enrichment_scope,
                },
                state_updates=state_updates,
            )

        reuse_existing_stage_candidates = enrichment_scope == "public_web_stage_2" and (
            list(state.get("candidates") or []) or isinstance(state.get("candidate_doc_path"), Path)
        )
        if strategy_type == "investor_firm_roster" and state.get("candidates"):
            candidates = list(state.get("candidates") or [])
            evidence = list(state.get("evidence") or [])
        elif reuse_existing_stage_candidates:
            candidates = list(state.get("candidates") or [])
            evidence = list(state.get("evidence") or [])
            candidate_doc_path = state.get("candidate_doc_path")
            if isinstance(candidate_doc_path, Path):
                source_snapshots["candidate_documents"] = {
                    "candidate_doc_path": str(candidate_doc_path),
                    "enrichment_scope": "linkedin_stage_1",
                }
        elif isinstance(snapshot, CompanyRosterSnapshot) or isinstance(search_seed_snapshot, SearchSeedSnapshot):
            if isinstance(snapshot, CompanyRosterSnapshot):
                roster_candidates, roster_evidence = build_candidates_from_roster(snapshot)
                candidates.extend(roster_candidates)
                evidence.extend(roster_evidence)
                source_snapshots["roster_snapshot"] = snapshot.to_record()
            if isinstance(search_seed_snapshot, SearchSeedSnapshot):
                search_candidates, search_evidence = build_candidates_from_seed_snapshot(search_seed_snapshot)
                candidates.extend(search_candidates)
                evidence.extend(search_evidence)
                source_snapshots["search_seed_snapshot"] = search_seed_snapshot.to_record()
            if len(candidates) > 1:
                candidates, evidence, canonicalization_summary = canonicalize_company_records(candidates, evidence)
        else:
            baseline_reuse_execution = _reuse_delta_baseline_if_available()
            if baseline_reuse_execution is not None:
                return baseline_reuse_execution
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="An acquired roster or search-seed snapshot must exist before profile enrichment.",
                payload={},
            )
        if not candidates:
            baseline_reuse_execution = _reuse_delta_baseline_if_available()
            if baseline_reuse_execution is not None:
                return baseline_reuse_execution
        if not candidates:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Acquisition returned no candidates that could be normalized into candidate documents.",
                payload=source_snapshots
                if source_snapshots
                else {"strategy_type": strategy_type, "enrichment_scope": enrichment_scope},
            )

        logger = AssetLogger(snapshot_dir)
        if strategy_type == "investor_firm_roster":
            candidate_doc_path = snapshot_dir / "candidate_documents.json"
            logger.write_json(
                candidate_doc_path,
                {
                    "target_company": effective_target_company,
                    "strategy_type": strategy_type,
                    "candidate_count": len(candidates),
                    "evidence_count": len(evidence),
                    "investor_firm_plan": state.get("investor_firm_plan") or {},
                    "candidates": [candidate.to_record() for candidate in candidates],
                    "evidence": [item.to_record() for item in evidence],
                    "enrichment_mode": "investor_firm_roster_existing_assets",
                },
                asset_type="candidate_documents",
                source_kind="investor_firm_roster",
                is_raw_asset=False,
                model_safe=True,
            )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="completed",
                detail=f"Prepared {len(candidates)} investor-firm candidate documents from tiered firm roster assets.",
                payload={
                    "candidate_count": len(candidates),
                    "evidence_count": len(evidence),
                    "candidate_doc_path": str(candidate_doc_path),
                    "enrichment_mode": "investor_firm_roster_existing_assets",
                },
                state_updates={
                    "candidates": candidates,
                    "evidence": evidence,
                    "candidate_doc_path": candidate_doc_path,
                },
            )

        enrichment_request = _stage_request()
        downstream_request_payload = dict(effective_request_payload)
        enrichment_identity = (
            snapshot.company_identity
            if isinstance(snapshot, CompanyRosterSnapshot)
            else (
                search_seed_snapshot.company_identity
                if isinstance(search_seed_snapshot, SearchSeedSnapshot)
                else state.get("company_identity")
            )
        )
        if not isinstance(enrichment_identity, CompanyIdentity):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Company identity must be available before enrichment can continue.",
                payload={"enrichment_scope": enrichment_scope},
            )
        full_roster_profile_prefetch = (
            self._task_full_roster_profile_prefetch(task, job_request) and enrichment_scope != "public_web_stage_2"
        )
        enrichment = self.multi_source_enricher.enrich(
            enrichment_identity,
            snapshot_dir,
            candidates,
            enrichment_request,
            asset_logger=logger,
            job_id=str(state.get("job_id") or ""),
            request_payload=downstream_request_payload,
            plan_payload=dict(state.get("plan_payload") or {}),
            runtime_mode=str(state.get("runtime_mode") or "workflow"),
            cost_policy=cost_policy,
            full_roster_profile_prefetch=full_roster_profile_prefetch,
            enrichment_scope=enrichment_scope,
        )

        candidate_doc_path = snapshot_dir / "candidate_documents.json"

        def _write_candidate_documents(
            *,
            stage_candidates: list[Candidate],
            stage_evidence: list[EvidenceRecord],
            stage_enrichment: Any,
            write_stage_archive: bool = True,
            preserve_root_candidate_superset: bool = False,
        ) -> None:
            payload = {
                "snapshot": source_snapshots.get("roster_snapshot")
                or source_snapshots.get("search_seed_snapshot")
                or {},
                "acquisition_sources": source_snapshots,
                "candidates": [candidate.to_record() for candidate in stage_candidates],
                "evidence": [item.to_record() for item in stage_evidence],
                "candidate_count": len(stage_candidates),
                "evidence_count": len(stage_evidence),
                "enrichment_mode": stage_mode,
                "enrichment_scope": enrichment_scope,
                "acquisition_canonicalization": canonicalization_summary,
                "enrichment_summary": stage_enrichment.to_record(),
                "acquisition_stage": {
                    **self._task_execution_phase(task, job_request),
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                },
            }
            root_payload = dict(payload)
            if preserve_root_candidate_superset and candidate_doc_path.exists():
                existing_payload = _read_company_asset_json(candidate_doc_path)
                existing_candidates, existing_evidence = _load_snapshot_candidate_payload(
                    candidate_doc_path,
                    effective_target_company,
                )
                if existing_candidates or existing_evidence:
                    merged_candidates = {
                        candidate.candidate_id: candidate
                        for candidate in existing_candidates
                        if str(candidate.candidate_id or "").strip()
                    }
                    for candidate in stage_candidates:
                        candidate_id = str(candidate.candidate_id or "").strip()
                        if not candidate_id:
                            continue
                        merged_candidates[candidate_id] = _merge_background_reconcile_candidate(
                            merged_candidates.get(candidate_id),
                            candidate,
                        )
                    merged_evidence = {
                        item.evidence_id: item for item in existing_evidence if str(item.evidence_id or "").strip()
                    }
                    for item in stage_evidence:
                        evidence_id = str(item.evidence_id or "").strip()
                        if evidence_id:
                            merged_evidence[evidence_id] = item
                    merged_candidate_list = sorted(
                        list(merged_candidates.values()),
                        key=lambda item: item.display_name,
                    )
                    merged_evidence_list = list(merged_evidence.values())
                    if len(merged_candidate_list) > len(stage_candidates):
                        root_payload = {
                            **existing_payload,
                            **payload,
                            "snapshot": {
                                **dict(existing_payload.get("snapshot") or {}),
                                **dict(payload.get("snapshot") or {}),
                            },
                            "acquisition_sources": {
                                **dict(existing_payload.get("acquisition_sources") or {}),
                                **dict(payload.get("acquisition_sources") or {}),
                            },
                            "candidates": [candidate.to_record() for candidate in merged_candidate_list],
                            "evidence": [item.to_record() for item in merged_evidence_list],
                            "candidate_count": len(merged_candidate_list),
                            "evidence_count": len(merged_evidence_list),
                            "root_candidate_documents_contract": {
                                "status": "superset_preserved",
                                "reason": "profile_prefetch_pending",
                                "existing_candidate_count": len(existing_candidates),
                                "stage_candidate_count": len(stage_candidates),
                                "merged_candidate_count": len(merged_candidate_list),
                            },
                        }
            if int(getattr(stage_enrichment, "queued_harvest_worker_count", 0) or 0) > 0:
                background_reconcile = {
                    "kind": "harvest_profile_prefetch",
                    "queued_harvest_worker_count": int(
                        getattr(stage_enrichment, "queued_harvest_worker_count", 0) or 0
                    ),
                    "stop_reason": str(getattr(stage_enrichment, "stop_reason", "") or ""),
                }
                payload["background_reconcile"] = dict(background_reconcile)
                root_payload["background_reconcile"] = dict(background_reconcile)
            elif int(getattr(stage_enrichment, "queued_exploration_count", 0) or 0) > 0:
                background_reconcile = {
                    "kind": "public_web_exploration",
                    "queued_exploration_count": int(getattr(stage_enrichment, "queued_exploration_count", 0) or 0),
                    "stop_reason": str(getattr(stage_enrichment, "stop_reason", "") or ""),
                }
                payload["background_reconcile"] = dict(background_reconcile)
                root_payload["background_reconcile"] = dict(background_reconcile)
            if enrichment_scope == "linkedin_stage_1":
                next_connectors = {
                    "profile_detail_accounts": [
                        account.account_id for account in profile_detail_accounts(self.accounts)
                    ],
                    "note": "LinkedIn stage-1 baseline is ready and can power deterministic layering or retrieval preview.",
                }
                payload["next_connectors"] = dict(next_connectors)
                root_payload["next_connectors"] = dict(next_connectors)
            elif enrichment_scope == "public_web_stage_2":
                next_connectors = {
                    "note": "Public-web stage-2 enrichment extended the LinkedIn baseline with publications, coauthor, and exploration evidence.",
                }
                payload["next_connectors"] = dict(next_connectors)
                root_payload["next_connectors"] = dict(next_connectors)
            logger.write_json(
                candidate_doc_path,
                root_payload,
                asset_type="candidate_documents",
                source_kind=stage_source_kind,
                is_raw_asset=False,
                model_safe=True,
            )
            job_id_for_reawaken = str(state.get("job_id") or "").strip()
            if job_id_for_reawaken:
                try:
                    self.store.reawaken_waiting_prerequisite_job_materialization_items(
                        job_id=job_id_for_reawaken,
                        snapshot_id=snapshot_dir.name,
                        item_kind="local_apply_closure",
                        source="acquisition_candidate_documents_write",
                    )
                except Exception:
                    pass
            if write_stage_archive and isinstance(stage_archive_path, Path):
                logger.write_json(
                    stage_archive_path,
                    payload,
                    asset_type="candidate_documents",
                    source_kind=stage_source_kind,
                    is_raw_asset=False,
                    model_safe=True,
                )

        state_updates: dict[str, Any] = {}
        if full_roster_profile_prefetch and int(getattr(enrichment, "queued_harvest_worker_count", 0) or 0) > 0:
            candidates = enrichment.candidates
            _write_candidate_documents(
                stage_candidates=candidates,
                stage_evidence=evidence,
                stage_enrichment=enrichment,
                write_stage_archive=True,
                preserve_root_candidate_superset=True,
            )
            state_updates = {
                "candidates": candidates,
                "evidence": evidence,
                "candidate_doc_path": candidate_doc_path,
            }
            if enrichment_scope == "linkedin_stage_1":
                state_updates["linkedin_stage_completed"] = True
                state_updates["linkedin_stage_candidate_doc_path"] = stage_archive_path or candidate_doc_path
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=(
                    "Built LinkedIn stage-1 candidate documents and queued "
                    f"{int(getattr(enrichment, 'queued_harvest_worker_count', 0) or 0)} background Harvest profile "
                    "prefetch workers; waiting for profile detail before final results."
                ),
                payload={
                    "candidate_count": len(candidates),
                    "evidence_count": len(evidence),
                    "candidate_doc_path": str(candidate_doc_path),
                    "stage_archive_path": str(stage_archive_path or ""),
                    "artifact_paths": enrichment.artifact_paths,
                    "queued_harvest_worker_count": int(getattr(enrichment, "queued_harvest_worker_count", 0) or 0),
                    "stop_reason": str(getattr(enrichment, "stop_reason", "") or ""),
                    "enrichment_mode": stage_mode,
                    "enrichment_scope": enrichment_scope,
                    "resolved_profile_count": len(enrichment.resolved_profiles),
                    "publication_match_count": len(enrichment.publication_matches),
                    "lead_candidate_count": len(enrichment.lead_candidates),
                    "acquisition_canonicalization": canonicalization_summary,
                    "profile_prefetch": dict(getattr(enrichment, "profile_prefetch", {}) or {}),
                    "background_reconcile_pending": True,
                    "blocking_reason": "harvest_profile_prefetch_pending",
                },
                state_updates=state_updates,
            )

        candidates = enrichment.candidates
        evidence.extend(enrichment.evidence)
        _write_candidate_documents(
            stage_candidates=candidates,
            stage_evidence=evidence,
            stage_enrichment=enrichment,
            preserve_root_candidate_superset=(
                enrichment_scope != "public_web_stage_2"
                and int(getattr(enrichment, "queued_harvest_worker_count", 0) or 0) > 0
            ),
        )
        state_updates = {
            "candidates": candidates,
            "evidence": evidence,
            "candidate_doc_path": candidate_doc_path,
        }
        linkedin_profile_prefetch_pending = (
            enrichment_scope != "public_web_stage_2"
            and int(getattr(enrichment, "queued_harvest_worker_count", 0) or 0) > 0
        )
        if enrichment_scope == "linkedin_stage_1":
            state_updates["linkedin_stage_candidate_doc_path"] = stage_archive_path
            if not linkedin_profile_prefetch_pending:
                state_updates["linkedin_stage_completed"] = True
        elif enrichment_scope == "public_web_stage_2":
            state_updates["public_web_stage_candidate_doc_path"] = stage_archive_path
            state_updates["public_web_stage_completed"] = True

        detail_prefix = (
            "Built LinkedIn stage-1 candidate documents."
            if enrichment_scope == "linkedin_stage_1"
            else (
                "Extended candidate documents with public-web stage-2 evidence."
                if enrichment_scope == "public_web_stage_2"
                else "Built candidate documents from roster + multisource enrichment."
            )
        )
        payload = {
            "candidate_count": len(candidates),
            "evidence_count": len(evidence),
            "candidate_doc_path": str(candidate_doc_path),
            "stage_archive_path": str(stage_archive_path or ""),
            "enrichment_mode": stage_mode,
            "enrichment_scope": enrichment_scope,
            "resolved_profile_count": len(enrichment.resolved_profiles),
            "publication_match_count": len(enrichment.publication_matches),
            "lead_candidate_count": len(enrichment.lead_candidates),
            "artifact_paths": enrichment.artifact_paths,
            "profile_prefetch": dict(getattr(enrichment, "profile_prefetch", {}) or {}),
            "acquisition_canonicalization": canonicalization_summary,
            "queued_harvest_worker_count": int(getattr(enrichment, "queued_harvest_worker_count", 0) or 0),
            "queued_exploration_count": int(getattr(enrichment, "queued_exploration_count", 0) or 0),
            "stop_reason": str(getattr(enrichment, "stop_reason", "") or ""),
        }
        if linkedin_profile_prefetch_pending:
            payload["background_reconcile_pending"] = True
            payload["blocking_reason"] = "harvest_profile_prefetch_pending"
        return AcquisitionExecution(
            task_id=task.task_id,
            status="blocked" if linkedin_profile_prefetch_pending else "completed",
            detail=(
                (
                    f"{detail_prefix} Queued {int(getattr(enrichment, 'queued_harvest_worker_count', 0) or 0)} "
                    "Harvest profile prefetch workers; waiting for profile detail before final results."
                )
                if linkedin_profile_prefetch_pending
                else (
                    f"{detail_prefix} Resolved {len(enrichment.resolved_profiles)} profile details and "
                    f"matched {len(enrichment.publication_matches)} publications."
                )
            ),
            payload=payload,
            state_updates=state_updates,
        )

    def _should_run_default_former_search_seed(self, task: AcquisitionTask, job_request: JobRequest) -> bool:
        if self._task_strategy_type(task, job_request) != "full_company_roster":
            return False
        intent_view = resolve_request_intent_view(job_request)
        effective_execution_preferences = dict(intent_view.get("execution_preferences") or {})
        if "run_former_search_seed" in effective_execution_preferences:
            return bool(effective_execution_preferences.get("run_former_search_seed"))
        if not self._task_include_former_search_seed(task, job_request):
            return False
        cost_policy = self._task_cost_policy(task, job_request)
        if str(cost_policy.get("provider_people_search_mode") or "").strip().lower() == "disabled":
            return False
        return True

    def _queue_background_profile_prefetch_for_candidates(
        self,
        *,
        candidates: list[Candidate],
        snapshot_dir: Path,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool,
        priority: bool = False,
        exclude_profile_urls: set[str] | None = None,
        load_cached_profile_payloads: bool = True,
        extra_profile_urls: list[str] | None = None,
        submit_provider: bool = True,
        nonblocking_submit: bool = False,
        allow_under_target_final_tail_dispatch: bool | None = None,
    ) -> dict[str, Any]:
        excluded_urls = {
            str(profile_url or "").strip()
            for profile_url in list(exclude_profile_urls or set())
            if str(profile_url or "").strip()
        }
        filtered_extra_profile_urls = [
            str(profile_url or "").strip()
            for profile_url in list(extra_profile_urls or [])
            if str(profile_url or "").strip() and str(profile_url or "").strip() not in excluded_urls
        ]
        filtered_candidates: list[Candidate] = []
        if excluded_urls:
            for candidate in list(candidates or []):
                profile_urls = [
                    normalized_url
                    for normalized_url in [
                        str(candidate.linkedin_url or "").strip(),
                        str(dict(candidate.metadata or {}).get("profile_url") or "").strip(),
                    ]
                    if normalized_url
                ]
                if profile_urls and all(profile_url in excluded_urls for profile_url in profile_urls):
                    continue
                filtered_candidates.append(candidate)
        else:
            filtered_candidates = list(candidates or [])
        if not filtered_candidates and not filtered_extra_profile_urls:
            return {"status": "skipped", "reason": "no_candidates"}
        return self.multi_source_enricher.queue_background_profile_prefetch(
            candidates=filtered_candidates,
            extra_profile_urls=filtered_extra_profile_urls,
            snapshot_dir=snapshot_dir,
            job_id=job_id,
            request_payload=request_payload,
            plan_payload=plan_payload,
            runtime_mode=runtime_mode,
            allow_shared_provider_cache=allow_shared_provider_cache,
            priority=priority,
            load_cached_profile_payloads=load_cached_profile_payloads,
            submit_provider=submit_provider,
            nonblocking_submit=nonblocking_submit,
            allow_under_target_final_tail_dispatch=allow_under_target_final_tail_dispatch,
        )

    def _queue_background_profile_prefetch_for_search_seed_entries(
        self,
        *,
        identity: CompanyIdentity,
        entries: list[dict[str, Any]],
        source_path: str,
        snapshot_dir: Path,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool,
        priority: bool = False,
        exclude_profile_urls: set[str] | None = None,
        load_cached_profile_payloads: bool = True,
        submit_provider: bool = True,
    ) -> dict[str, Any]:
        normalized_entries: list[dict[str, Any]] = []
        for entry in list(entries or []):
            normalized_entry = dict(entry or {})
            profile_url = str(normalized_entry.get("profile_url") or "").strip()
            if not profile_url:
                continue
            normalized_entries.append(normalized_entry)
        if not normalized_entries:
            return {"status": "skipped", "reason": "no_profile_urls"}
        seed_candidates, _ = build_candidates_from_seed_entries(
            company_identity=identity,
            target_company=identity.canonical_name,
            entries=normalized_entries,
            source_path=source_path,
        )
        raw_profile_urls = [
            str(entry.get("profile_url") or "").strip()
            for entry in normalized_entries
            if str(entry.get("profile_url") or "").strip()
        ]
        return self._queue_background_profile_prefetch_for_candidates(
            candidates=seed_candidates,
            snapshot_dir=snapshot_dir,
            job_id=job_id,
            request_payload=request_payload,
            plan_payload=plan_payload,
            runtime_mode=runtime_mode,
            allow_shared_provider_cache=allow_shared_provider_cache,
            priority=priority,
            exclude_profile_urls=exclude_profile_urls,
            load_cached_profile_payloads=load_cached_profile_payloads,
            extra_profile_urls=raw_profile_urls,
            submit_provider=submit_provider,
        )

    def _queue_background_profile_prefetch_from_full_roster_baselines(
        self,
        *,
        roster_snapshot: CompanyRosterSnapshot | None,
        search_seed_snapshot: SearchSeedSnapshot | None,
        snapshot_dir: Path,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool,
        load_cached_profile_payloads: bool = True,
        submit_provider: bool = True,
    ) -> dict[str, Any]:
        baseline_candidates: list[Candidate] = []
        if isinstance(roster_snapshot, CompanyRosterSnapshot):
            roster_candidates, _ = build_candidates_from_roster(roster_snapshot)
            baseline_candidates.extend(roster_candidates)
        if isinstance(search_seed_snapshot, SearchSeedSnapshot):
            search_seed_candidates, _ = build_candidates_from_seed_snapshot(search_seed_snapshot)
            baseline_candidates.extend(search_seed_candidates)
        raw_search_seed_profile_urls = (
            [
                str(entry.get("profile_url") or "").strip()
                for entry in list(getattr(search_seed_snapshot, "entries", []) or [])
                if str(entry.get("profile_url") or "").strip()
            ]
            if isinstance(search_seed_snapshot, SearchSeedSnapshot)
            else []
        )
        return self._queue_background_profile_prefetch_for_candidates(
            candidates=baseline_candidates,
            snapshot_dir=snapshot_dir,
            job_id=job_id,
            request_payload=request_payload,
            plan_payload=plan_payload,
            runtime_mode=runtime_mode,
            allow_shared_provider_cache=allow_shared_provider_cache,
            priority=True,
            load_cached_profile_payloads=load_cached_profile_payloads,
            extra_profile_urls=raw_search_seed_profile_urls,
            submit_provider=submit_provider,
            nonblocking_submit=True,
            allow_under_target_final_tail_dispatch=True,
        )

    def _continue_full_roster_with_available_baseline(
        self,
        *,
        task: AcquisitionTask,
        roster_snapshot: CompanyRosterSnapshot | None,
        search_seed_snapshot: SearchSeedSnapshot | None,
        snapshot_dir: Path,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool,
        queued_payload: dict[str, Any],
        queued_state_updates: dict[str, Any],
        blocked_detail: str,
        completion_detail: str,
    ) -> AcquisitionExecution:
        roster_ready = isinstance(roster_snapshot, CompanyRosterSnapshot) and bool(
            list(roster_snapshot.visible_entries or []) or list(roster_snapshot.raw_entries or [])
        )
        search_seed_ready = isinstance(search_seed_snapshot, SearchSeedSnapshot) and bool(
            list(search_seed_snapshot.entries or [])
        )
        if not roster_ready and not search_seed_ready:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=blocked_detail,
                payload=queued_payload,
                state_updates=queued_state_updates,
            )
        profile_prefetch = self._queue_background_profile_prefetch_from_full_roster_baselines(
            roster_snapshot=roster_snapshot,
            search_seed_snapshot=search_seed_snapshot,
            snapshot_dir=snapshot_dir,
            job_id=job_id,
            request_payload=request_payload,
            plan_payload=plan_payload,
            runtime_mode=runtime_mode,
            allow_shared_provider_cache=allow_shared_provider_cache,
        )
        baseline_ready_sources: list[str] = []
        if roster_ready:
            baseline_ready_sources.append("current_roster")
            queued_payload["baseline_ready_from_roster"] = True
        if search_seed_ready:
            baseline_ready_sources.append("former_search_seed")
            queued_payload["baseline_ready_from_search_seed"] = True
            if not roster_ready:
                queued_state_updates["allow_search_seed_baseline_for_full_roster"] = True
        queued_payload["baseline_ready_sources"] = baseline_ready_sources
        queued_payload["profile_prefetch"] = dict(profile_prefetch)
        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=completion_detail,
            payload=queued_payload,
            state_updates=queued_state_updates,
        )

    def _acquire_default_former_search_seed(
        self,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
        identity: CompanyIdentity,
    ) -> AcquisitionExecution:
        filter_hints = _build_former_filter_hints(
            identity=identity,
            base_filter_hints=self._task_filter_hints(task, job_request),
        )
        task_view = self._task_execution_view(task, job_request)
        cost_policy = self._task_cost_policy(task, job_request)
        former_cost_policy = {
            **cost_policy,
            "provider_people_search_mode": "fallback_only",
            "provider_people_search_min_expected_results": int(
                self._task_execution_int(
                    task,
                    job_request,
                    "former_provider_people_search_min_expected_results",
                    default=50,
                )
            ),
        }
        former_search_seed_queries: list[str] = []
        should_preserve_former_keywords = (
            bool(former_cost_policy.get("large_org_keyword_probe_mode"))
            or bool(former_cost_policy.get("former_keyword_queries_only"))
            or self._task_strategy_type(task, job_request) == "scoped_search_roster"
        )
        if should_preserve_former_keywords:
            former_search_seed_queries = [
                str(item).strip()
                for item in self._task_execution_list(task, job_request, "search_seed_queries")
                if str(item).strip()
            ]
            former_search_seed_queries = list(dict.fromkeys(former_search_seed_queries))
        former_intent_view = {
            **task_view,
            "strategy_type": "former_employee_search",
            "employment_statuses": ["former"],
            "search_seed_queries": former_search_seed_queries,
            "search_query_bundles": [],
            "filter_hints": filter_hints,
            "filter_keywords": list(filter_hints.get("keywords") or []),
            "function_ids": list(filter_hints.get("function_ids") or []),
            "cost_policy": former_cost_policy,
        }
        former_task = AcquisitionTask(
            task_id=f"{task.task_id}-former-search-seed",
            task_type="acquire_search_seed_pool",
            title="Acquire former-member search seeds",
            description="Run a provider former-member recall pass after the current roster baseline is available.",
            source_hint="Harvest profile search (past company filter)",
            status="ready",
            blocking=False,
            metadata={
                "strategy_type": "former_employee_search",
                "employment_statuses": ["former"],
                "search_seed_queries": former_search_seed_queries,
                "search_query_bundles": [],
                "filter_hints": filter_hints,
                "cost_policy": former_cost_policy,
                "intent_view": former_intent_view,
            },
        )
        return self._normalize_zero_result_search_seed_execution(
            self._acquire_search_seed_pool(former_task, state, job_request),
            detail="Former-member search completed but did not add any new candidates.",
        )

    def _discover_company_identity_candidates(self, target_company: str) -> list[dict[str, Any]]:
        observed: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for query in _company_identity_search_queries(target_company):
            try:
                response = self.search_provider.search(query, max_results=5)
            except SearchProviderError:
                continue
            except Exception:
                continue
            for result in response.results:
                candidate = _company_identity_candidate_from_search_result(result.to_record())
                if candidate is None:
                    continue
                key = (
                    str(candidate.get("label") or "").strip().lower(),
                    str(candidate.get("linkedin_slug") or "").strip().lower(),
                )
                if key in seen:
                    continue
                seen.add(key)
                observed.append(candidate)
        return observed

    def _resolve_adaptive_company_employee_shards(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        policy: dict[str, Any],
        runtime_timing_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        snapshot_root = Path(snapshot_dir).resolve()
        normalized_policy = normalize_company_employee_shard_policy(policy)
        if not normalized_policy:
            return {
                "status": "blocked",
                "reason": "invalid_shard_policy",
                "detail": "Adaptive shard policy is missing or invalid.",
                "shards": [],
            }

        logger = AssetLogger(snapshot_root)
        plan = plan_company_employee_shards_from_policy(
            normalized_policy,
            probe_fn=lambda company_filters, context: self.harvest_company_connector.probe_company_roster_query(
                identity,
                snapshot_root,
                asset_logger=logger,
                company_filters=company_filters,
                probe_id=str(context.get("probe_id") or ""),
                title=str(context.get("title") or ""),
                max_pages=int(normalized_policy.get("probe_max_pages") or 1),
                page_limit=int(normalized_policy.get("probe_page_limit") or 25),
                runtime_timing_overrides=runtime_timing_overrides,
            ),
        )
        harvest_dir = snapshot_root / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        plan_path = harvest_dir / "adaptive_shard_plan.json"
        logger.write_json(
            plan_path,
            plan,
            asset_type="harvest_company_employees_adaptive_shard_plan",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        planned = dict(plan)
        planned["plan_path"] = str(plan_path)
        if str(planned.get("status") or "") == "planned" and not str(planned.get("detail") or "").strip():
            planned["detail"] = (
                f"Adaptive Harvest shard planner produced {len(list(planned.get('shards') or []))} shard(s) "
                f"for {identity.canonical_name}."
            )
        elif str(planned.get("status") or "") != "planned" and not str(planned.get("detail") or "").strip():
            planned["detail"] = "Adaptive Harvest shard planning did not produce an executable shard set."
        return planned

    def _execute_harvest_company_roster_worker(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        max_pages: int,
        page_limit: int,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool,
        company_filters: dict[str, Any] | None = None,
        worker_key_suffix: str = "",
        span_name_suffix: str = "",
        root_snapshot_dir: Path | None = None,
    ) -> dict[str, Any]:
        if self.worker_runtime is None or not job_id:
            return {"worker_status": "skipped"}

        effective_request_payload = build_effective_request_payload(request_payload)
        normalized_company_filters = dict(company_filters or {})
        effective_root_snapshot_dir = Path(root_snapshot_dir) if root_snapshot_dir is not None else snapshot_dir
        worker_handle = self.worker_runtime.begin_worker(
            job_id=job_id,
            request=JobRequest.from_payload(effective_request_payload),
            plan_payload=plan_payload,
            runtime_mode=runtime_mode,
            lane_id="acquisition_specialist",
            worker_key=f"harvest_company_employees::{identity.company_key}{worker_key_suffix}",
            stage="acquiring",
            span_name=f"harvest_company_employees:{identity.canonical_name}{span_name_suffix}",
            budget_payload={"max_pages": max_pages, "page_limit": page_limit},
            input_payload={
                "company_identity": identity.to_record(),
                "company_filters": normalized_company_filters,
            },
            metadata={
                "recovery_kind": "harvest_company_employees",
                "identity": identity.to_record(),
                "snapshot_dir": str(snapshot_dir),
                "root_snapshot_dir": str(effective_root_snapshot_dir),
                "max_pages": max_pages,
                "page_limit": page_limit,
                "company_filters": normalized_company_filters,
                "worker_key_suffix": str(worker_key_suffix or ""),
                "span_name_suffix": str(span_name_suffix or ""),
                "request_payload": effective_request_payload,
                "plan_payload": plan_payload,
                "runtime_mode": runtime_mode,
                "allow_shared_provider_cache": allow_shared_provider_cache,
            },
            handoff_from_lane="triage_planner",
        )
        existing = self.worker_runtime.get_worker(worker_handle.worker_id) or {}
        checkpoint = dict(existing.get("checkpoint") or {})
        output_payload = dict(existing.get("output") or {})
        if str(existing.get("status") or "") == "completed" and output_payload:
            self.worker_runtime.complete_worker(
                worker_handle,
                status="completed",
                checkpoint_payload=checkpoint,
                output_payload=output_payload,
                handoff_to_lane="acquisition_specialist",
            )
            return {
                "worker_id": worker_handle.worker_id,
                "worker_status": "completed",
                "summary": dict(output_payload.get("summary") or {}),
                "daemon_action": "reused_output",
            }

        logger = AssetLogger(snapshot_dir)
        harvest_dir = snapshot_dir / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        artifact_default_path = harvest_dir / "harvest_company_employees_queue.json"
        runtime_timing_overrides = _runtime_timing_overrides_from_request_payload(effective_request_payload)
        provider_limiter_lease = dict(checkpoint.get("provider_limiter_lease") or {})
        if not provider_limiter_lease:
            provider_limiter_lease = acquire_runtime_provider_limiter_slot(
                self.store,
                limiter_key="harvest_company_employees_actor",
                budget=resolved_harvest_company_roster_global_inflight(runtime_timing_overrides),
                lease_owner=f"harvest_company_employees:{job_id}:{identity.company_key}{worker_key_suffix}",
                lease_seconds=7200,
                metadata={
                    "source": "checkpoint_worker",
                    "job_id": job_id,
                    "company_key": identity.company_key,
                    "max_pages": max_pages,
                    "page_limit": page_limit,
                },
            )
        try:
            with runtime_inflight_slot(
                "harvest_company_roster",
                budget=resolved_harvest_company_roster_global_inflight(runtime_timing_overrides),
                metadata={"company": identity.company_key, "source": "checkpoint_worker"},
            ):
                execution = self.harvest_company_connector.execute_with_checkpoint(
                    identity,
                    snapshot_dir,
                    max_pages=max_pages,
                    page_limit=page_limit,
                    company_filters=normalized_company_filters,
                    checkpoint=checkpoint,
                    allow_shared_provider_cache=allow_shared_provider_cache,
                    runtime_timing_overrides=runtime_timing_overrides,
                )
        except Exception:
            release_runtime_provider_limiter_slot(self.store, provider_limiter_lease)
            raise
        artifact_paths = {
            str(key): str(value)
            for key, value in dict(checkpoint.get("artifact_paths") or {}).items()
            if str(key).strip()
        }
        for artifact in list(execution.artifacts or []):
            artifact_path = write_harvest_execution_artifact(
                logger=logger,
                artifact=artifact,
                default_path=artifact_default_path,
                asset_type="harvest_company_employees_queue_payload",
                source_kind="harvest_company_employees",
                metadata={"company_key": identity.company_key, "logical_name": execution.logical_name},
            )
            artifact_paths[str(artifact.label)] = str(artifact_path)
        execution_request_context = dict(dict(execution.checkpoint or {}).get("request_context") or {})
        probe_context = dict(execution_request_context.get("probe") or {})
        summary = {
            "logical_name": execution.logical_name,
            "company_identity": identity.to_record(),
            "status": "queued" if execution.pending else "completed",
            "message": str(execution.message or ""),
            "run_id": str(execution.checkpoint.get("run_id") or ""),
            "dataset_id": str(execution.checkpoint.get("dataset_id") or ""),
            "artifact_paths": artifact_paths,
            "requested_pages": max_pages,
            "requested_item_limit": page_limit,
            "effective_take_pages": int(execution_request_context.get("effective_take_pages") or max_pages or 1),
            "effective_max_items": int(execution_request_context.get("effective_max_items") or 0),
            "requested_item_count_before_probe": int(
                probe_context.get("requested_items_before_probe")
                or execution_request_context.get("requested_item_count")
                or 0
            ),
            "estimated_total_count": int(probe_context.get("estimated_total_count") or 0),
            "provider_result_cap": int(probe_context.get("provider_result_cap") or 2500),
            "provider_cap_hit": bool(
                probe_context.get("provider_cap_hit") or probe_context.get("provider_result_limited")
            ),
            "requested_limit_would_truncate": bool(probe_context.get("requested_limit_would_truncate")),
            "probe": probe_context,
            "company_filters": normalized_company_filters,
            "snapshot_dir": str(snapshot_dir),
            "root_snapshot_dir": str(effective_root_snapshot_dir),
            "provider_limiter": {
                "limiter_key": str(provider_limiter_lease.get("limiter_key") or ""),
                "active_count": int(provider_limiter_lease.get("active_count") or 0),
                "budget": int(provider_limiter_lease.get("budget") or 0),
                "wait_ms": provider_limiter_lease.get("wait_ms"),
            },
        }
        summary_path = harvest_dir / "harvest_company_employees_queue_summary.json"
        logger.write_json(
            summary_path,
            summary,
            asset_type="harvest_company_employees_queue_summary",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        updated_checkpoint = {
            **dict(execution.checkpoint or {}),
            "artifact_paths": artifact_paths,
            "summary_path": str(summary_path),
            "stage": "waiting_remote_harvest" if execution.pending else "completed",
            "provider_limiter_lease": provider_limiter_lease,
        }
        updated_output = {
            "summary": {**summary, "summary_path": str(summary_path)},
        }
        if execution.pending:
            self.worker_runtime.complete_worker(
                worker_handle,
                status="queued",
                checkpoint_payload=updated_checkpoint,
                output_payload=updated_output,
            )
            return {
                "worker_id": worker_handle.worker_id,
                "worker_status": "queued",
                "summary": updated_output["summary"],
            }

        release_runtime_provider_limiter_slot(self.store, provider_limiter_lease)
        self.worker_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload=updated_checkpoint,
            output_payload=updated_output,
            handoff_to_lane="acquisition_specialist",
        )
        return {
            "worker_id": worker_handle.worker_id,
            "worker_status": "completed",
            "summary": updated_output["summary"],
        }

    def _execute_segmented_harvest_company_roster_workers(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        shards: list[dict[str, Any]],
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool,
    ) -> dict[str, Any]:
        runtime_timing_overrides = _runtime_timing_overrides_from_request_payload(request_payload)
        shard_root = snapshot_dir / "harvest_company_employees" / "shards"
        shard_root.mkdir(parents=True, exist_ok=True)

        shard_specs: list[dict[str, Any]] = []
        for index, shard in enumerate(shards, start=1):
            shard_id = _normalize_shard_id(str(shard.get("shard_id") or "shard"))
            shard_title = str(shard.get("title") or "").strip()
            shard_snapshot_dir = shard_root / shard_id
            shard_snapshot_dir.mkdir(parents=True, exist_ok=True)
            shard_specs.append(
                {
                    "index": index,
                    "shard": dict(shard),
                    "shard_id": shard_id,
                    "shard_title": shard_title,
                    "shard_snapshot_dir": shard_snapshot_dir,
                }
            )

        def _run_shard(spec: dict[str, Any]) -> dict[str, Any]:
            shard_payload = dict(spec.get("shard") or {})
            worker_result = self._execute_harvest_company_roster_worker(
                identity=identity,
                snapshot_dir=Path(spec["shard_snapshot_dir"]),
                max_pages=int(shard_payload.get("max_pages") or 1),
                page_limit=int(shard_payload.get("page_limit") or 25),
                job_id=job_id,
                request_payload=request_payload,
                plan_payload=plan_payload,
                runtime_mode=runtime_mode,
                allow_shared_provider_cache=allow_shared_provider_cache,
                company_filters=dict(shard_payload.get("company_filters") or {}),
                worker_key_suffix=f"::{spec['shard_id']}",
                span_name_suffix=f":{spec['shard_id']}",
                root_snapshot_dir=snapshot_dir,
            )
            summary = {
                **dict(worker_result.get("summary") or {}),
                "shard_id": spec["shard_id"],
                "title": spec["shard_title"],
                "strategy_id": str(shard_payload.get("strategy_id") or "").strip(),
                "scope_note": str(shard_payload.get("scope_note") or "").strip(),
                "company_filters": dict(shard_payload.get("company_filters") or {}),
                "snapshot_dir": str(spec["shard_snapshot_dir"]),
                "root_snapshot_dir": str(snapshot_dir),
            }
            return {
                "index": int(spec.get("index") or 0),
                "worker_id": int(worker_result.get("worker_id") or 0),
                "worker_status": str(worker_result.get("worker_status") or ""),
                "summary": summary,
            }

        shard_results_by_index: dict[int, dict[str, Any]] = {}
        if shard_specs:
            parallel_workers = resolved_harvest_company_roster_parallel_shards(
                runtime_timing_overrides,
                shard_count=len(shard_specs),
                default=8,
            )
            if parallel_workers == 1:
                for spec in shard_specs:
                    result = _run_shard(spec)
                    shard_results_by_index[int(result.get("index") or 0)] = result
            else:
                with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                    future_map = {
                        executor.submit(_run_shard, spec): int(spec.get("index") or 0) for spec in shard_specs
                    }
                    for future in as_completed(future_map):
                        result = dict(future.result() or {})
                        index = int(result.get("index") or future_map.get(future) or 0)
                        shard_results_by_index[index] = result

        queued_summaries: list[dict[str, Any]] = []
        completed_summaries: list[dict[str, Any]] = []
        completed_worker_results: list[dict[str, Any]] = []
        for spec in sorted(shard_specs, key=lambda item: int(item.get("index") or 0)):
            result = dict(shard_results_by_index.get(int(spec.get("index") or 0)) or {})
            summary = dict(result.get("summary") or {})
            if str(result.get("worker_status") or "") == "queued":
                queued_summaries.append(summary)
            else:
                completed_summaries.append(summary)
                if int(result.get("worker_id") or 0) > 0:
                    completed_worker_results.append(result)
        if queued_summaries and completed_worker_results and callable(self.inline_worker_completion_callback):
            self.inline_worker_completion_callback(
                {
                    "worker_id": int(completed_worker_results[0].get("worker_id") or 0),
                    "worker_status": str(completed_worker_results[0].get("worker_status") or "completed"),
                    "source": "segmented_harvest_company_roster_inprocess",
                }
            )
        return {
            "queued_count": len(queued_summaries),
            "completed_count": len(completed_summaries),
            "queued_summaries": queued_summaries,
            "completed_summaries": completed_summaries,
            "shard_count": len(shards),
        }

    def _fetch_segmented_harvest_company_roster(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        shards: list[dict[str, Any]],
        allow_shared_provider_cache: bool,
        runtime_timing_overrides: dict[str, Any] | None = None,
        expected_shard_count: int | None = None,
        summary_stop_reason: str | None = None,
    ) -> CompanyRosterSnapshot:
        logger = AssetLogger(snapshot_dir)
        harvest_dir = snapshot_dir / "harvest_company_employees"
        shard_root = harvest_dir / "shards"
        shard_root.mkdir(parents=True, exist_ok=True)

        merged_entries: list[dict[str, Any]] = []
        visible_entries: list[dict[str, Any]] = []
        headless_entries: list[dict[str, Any]] = []
        page_summaries: list[dict[str, Any]] = []
        shard_summaries: list[dict[str, Any]] = []
        errors: list[str] = []
        seen_keys: set[str] = set()

        shard_specs: list[dict[str, Any]] = []
        for index, shard in enumerate(shards, start=1):
            shard_id = str(shard.get("shard_id") or "shard").strip() or "shard"
            shard_snapshot_dir = shard_root / shard_id
            shard_snapshot_dir.mkdir(parents=True, exist_ok=True)
            shard_specs.append(
                {
                    "index": index,
                    "shard": dict(shard),
                    "shard_id": shard_id,
                    "shard_snapshot_dir": shard_snapshot_dir,
                }
            )

        def _fetch_shard(spec: dict[str, Any]) -> dict[str, Any]:
            shard = dict(spec.get("shard") or {})
            shard_id = str(spec.get("shard_id") or "")
            with runtime_provider_limiter_slot(
                self.store,
                limiter_key="harvest_company_employees_actor",
                budget=resolved_harvest_company_roster_global_inflight(runtime_timing_overrides),
                lease_owner=f"harvest_company_employees:shard:{identity.company_key}:{shard_id}",
                lease_seconds=7200,
                metadata={"company": identity.company_key, "source": "segmented_shard", "shard_id": shard_id},
            ):
                with runtime_inflight_slot(
                    "harvest_company_roster",
                    budget=resolved_harvest_company_roster_global_inflight(runtime_timing_overrides),
                    metadata={"company": identity.company_key, "source": "segmented_shard"},
                ):
                    shard_snapshot = _apply_optional_runtime_timing_overrides(
                        self.harvest_company_connector.fetch_company_roster,
                        identity,
                        Path(spec["shard_snapshot_dir"]),
                        max_pages=int(shard.get("max_pages") or 1),
                        page_limit=int(shard.get("page_limit") or 25),
                        company_filters=dict(shard.get("company_filters") or {}),
                        allow_shared_provider_cache=allow_shared_provider_cache,
                        runtime_timing_overrides=runtime_timing_overrides,
                    )
            return {
                "index": int(spec.get("index") or 0),
                "shard": shard,
                "shard_id": shard_id,
                "snapshot": shard_snapshot,
            }

        fetched_shards_by_index: dict[int, dict[str, Any]] = {}
        if shard_specs:
            parallel_workers = resolved_harvest_company_roster_parallel_shards(
                runtime_timing_overrides,
                shard_count=len(shard_specs),
                default=8,
            )
            if parallel_workers == 1:
                for spec in shard_specs:
                    result = _fetch_shard(spec)
                    fetched_shards_by_index[int(result.get("index") or 0)] = result
            else:
                with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                    future_map = {
                        executor.submit(_fetch_shard, spec): int(spec.get("index") or 0) for spec in shard_specs
                    }
                    for future in as_completed(future_map):
                        result = dict(future.result() or {})
                        index = int(result.get("index") or future_map.get(future) or 0)
                        fetched_shards_by_index[index] = result

        for spec in sorted(shard_specs, key=lambda item: int(item.get("index") or 0)):
            shard = dict(spec.get("shard") or {})
            shard_id = str(spec.get("shard_id") or "shard").strip() or "shard"
            shard_snapshot = dict(fetched_shards_by_index.get(int(spec.get("index") or 0)) or {}).get("snapshot")
            if not isinstance(shard_snapshot, CompanyRosterSnapshot):
                continue
            unique_count = 0
            duplicate_count = 0
            for entry in shard_snapshot.raw_entries:
                normalized_entry = dict(entry)
                normalized_entry["source_shard_id"] = shard_id
                normalized_entry["source_shard_title"] = str(shard.get("title") or "").strip()
                normalized_entry["source_shard_filters"] = dict(shard.get("company_filters") or {})
                shard_filters = dict(shard.get("company_filters") or {})
                shard_function_ids = [
                    str(item).strip() for item in list(shard_filters.get("function_ids") or []) if str(item).strip()
                ]
                exclude_function_ids = {
                    str(item).strip()
                    for item in list(shard_filters.get("exclude_function_ids") or [])
                    if str(item).strip()
                }
                shard_function_ids = [item for item in shard_function_ids if item not in exclude_function_ids]
                if shard_function_ids and not normalized_entry.get("function_ids"):
                    normalized_entry["function_ids"] = shard_function_ids
                member_key = _roster_entry_key(normalized_entry)
                if member_key in seen_keys:
                    duplicate_count += 1
                    continue
                seen_keys.add(member_key)
                unique_count += 1
                merged_entries.append(normalized_entry)
                if bool(normalized_entry.get("is_headless")):
                    headless_entries.append(normalized_entry)
                else:
                    visible_entries.append(normalized_entry)
            for page_summary in shard_snapshot.page_summaries:
                page_summaries.append(
                    {
                        **dict(page_summary),
                        "shard_id": shard_id,
                        "shard_title": str(shard.get("title") or "").strip(),
                    }
                )
            errors.extend(f"{shard_id}: {error}" for error in shard_snapshot.errors)
            shard_summaries.append(
                {
                    "shard_id": shard_id,
                    "title": str(shard.get("title") or "").strip(),
                    "strategy_id": str(shard.get("strategy_id") or "").strip(),
                    "scope_note": str(shard.get("scope_note") or "").strip(),
                    "company_filters": dict(shard.get("company_filters") or {}),
                    "requested_max_pages": int(shard.get("max_pages") or 1),
                    "requested_page_limit": int(shard.get("page_limit") or 25),
                    "returned_entry_count": len(shard_snapshot.raw_entries),
                    "returned_visible_entry_count": len(shard_snapshot.visible_entries),
                    "returned_headless_entry_count": len(shard_snapshot.headless_entries),
                    "unique_entry_count": unique_count,
                    "duplicate_entry_count": duplicate_count,
                    "stop_reason": shard_snapshot.stop_reason,
                    "summary_path": str(shard_snapshot.summary_path),
                }
            )

        merged_path = harvest_dir / "harvest_company_employees_merged.json"
        visible_path = harvest_dir / "harvest_company_employees_visible.json"
        headless_path = harvest_dir / "harvest_company_employees_headless.json"
        summary_path = harvest_dir / "harvest_company_employees_summary.json"
        raw_manifest_path = harvest_dir / "harvest_company_employees_raw.json"

        logger.write_json(
            raw_manifest_path,
            {
                "segmented": True,
                "strategy_id": str(shards[0].get("strategy_id") or "").strip() if shards else "",
                "company_identity": identity.to_record(),
                "expected_shard_count": int(expected_shard_count or len(shards)),
                "available_shard_count": len(shard_summaries),
                "completion_status": (
                    "completed" if int(expected_shard_count or len(shards)) <= len(shard_summaries) else "partial"
                ),
                "shards": shard_summaries,
            },
            asset_type="harvest_company_employees_payload",
            source_kind="harvest_company_employees",
            is_raw_asset=True,
            model_safe=False,
        )
        logger.write_json(
            merged_path,
            merged_entries,
            asset_type="company_roster_merged",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            visible_path,
            visible_entries,
            asset_type="company_roster_visible",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            headless_path,
            headless_entries,
            asset_type="company_roster_headless",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            summary_path,
            {
                "snapshot_id": snapshot_dir.name,
                "target_company": identity.canonical_name,
                "company_identity": identity.to_record(),
                "segmented": True,
                "strategy_id": str(shards[0].get("strategy_id") or "").strip() if shards else "",
                "expected_shard_count": int(expected_shard_count or len(shards)),
                "available_shard_count": len(shard_summaries),
                "completion_status": (
                    "completed" if int(expected_shard_count or len(shards)) <= len(shard_summaries) else "partial"
                ),
                "raw_entry_count": len(merged_entries),
                "visible_entry_count": len(visible_entries),
                "headless_entry_count": len(headless_entries),
                "page_summaries": page_summaries,
                "shard_summaries": shard_summaries,
                "accounts_used": ["harvest_company_employees"],
                "errors": errors,
                "stop_reason": str(summary_stop_reason or "completed_segmented"),
            },
            asset_type="company_roster_summary",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        return CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=merged_entries,
            visible_entries=visible_entries,
            headless_entries=headless_entries,
            page_summaries=page_summaries,
            accounts_used=["harvest_company_employees"],
            errors=errors,
            stop_reason=str(summary_stop_reason or "completed_segmented"),
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=summary_path,
        )

    def _acquire_investor_firm_roster(
        self,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
    ) -> AcquisitionExecution:
        identity = state.get("company_identity")
        snapshot_dir = state.get("snapshot_dir")
        if not isinstance(identity, CompanyIdentity) or not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Company identity must be resolved before investor-firm roster acquisition.",
                payload={},
            )

        candidates: list[Candidate] = []
        evidence: list[EvidenceRecord] = []
        try:
            authoritative_snapshot = load_authoritative_candidate_snapshot(
                runtime_dir=str(self.settings.runtime_dir),
                target_company=identity.canonical_name,
                snapshot_id=snapshot_dir.name,
                store=self.store,
                allow_candidate_documents_fallback=True,
            )
            candidates = [
                candidate
                for candidate in authoritative_snapshot.candidates
                if candidate.target_company.strip().lower() == identity.canonical_name.strip().lower()
                and candidate.category == "investor"
            ]
            if candidates:
                candidate_ids = {
                    str(candidate.candidate_id or "").strip()
                    for candidate in candidates
                    if str(candidate.candidate_id or "").strip()
                }
                evidence = [
                    item
                    for item in authoritative_snapshot.evidence_records
                    if str(item.candidate_id or "").strip() in candidate_ids
                ]
        except CandidateArtifactError:
            candidates = []
            evidence = []
        if not candidates:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=(
                    "Investor-firm roster acquisition needs either an investment-graph connector or existing investor "
                    "assets for this company, but none were found."
                ),
                payload={"strategy_type": "investor_firm_roster", "target_company": identity.canonical_name},
            )
        firm_plan = _build_investor_firm_plan(identity.canonical_name, candidates, job_request)
        logger = AssetLogger(snapshot_dir)
        firm_plan_path = snapshot_dir / "investor_firm_plan.json"
        logger.write_json(
            firm_plan_path,
            firm_plan,
            asset_type="investor_firm_plan",
            source_kind="investor_firm_roster",
            is_raw_asset=False,
            model_safe=True,
        )
        roster_path = snapshot_dir / "investor_firm_candidates.json"
        logger.write_json(
            roster_path,
            {
                "target_company": identity.canonical_name,
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "firms": firm_plan["firms"],
                "candidates": [candidate.to_record() for candidate in candidates],
            },
            asset_type="investor_firm_candidates",
            source_kind="investor_firm_roster",
            is_raw_asset=False,
            model_safe=True,
        )
        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=(
                f"Recovered {len(candidates)} investor-firm members across {len(firm_plan['firms'])} firms "
                f"for {identity.canonical_name} using existing investor assets."
            ),
            payload={
                "strategy_type": "investor_firm_roster",
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "firm_count": len(firm_plan["firms"]),
                "firm_plan_path": str(firm_plan_path),
                "roster_path": str(roster_path),
            },
            state_updates={
                "candidates": candidates,
                "evidence": evidence,
                "investor_firm_plan": firm_plan,
                "normalization_scope": {"mode": "category", "category": "investor"},
            },
        )

    def _normalize_snapshot(
        self,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest | None = None,
    ) -> AcquisitionExecution:
        identity = state.get("company_identity")
        snapshot_dir = state.get("snapshot_dir")
        candidates = state.get("candidates") or []
        evidence = state.get("evidence") or []

        if not isinstance(identity, CompanyIdentity) or not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Normalization requires a resolved company identity and snapshot directory.",
                payload={},
            )

        normalization_scope = dict(state.get("normalization_scope") or {})
        if str(normalization_scope.get("mode") or "") != "category":
            equivalent_baseline_snapshot = self._equivalent_delta_baseline_snapshot(
                task=task,
                job_request=job_request,
                identity=identity,
                snapshot_dir=snapshot_dir,
                candidates=candidates,
                evidence=evidence,
            )
            if equivalent_baseline_snapshot:
                return self._build_equivalent_delta_baseline_execution(
                    task=task,
                    identity=identity,
                    candidates=candidates,
                    evidence=evidence,
                    equivalent_baseline_snapshot=equivalent_baseline_snapshot,
                )
        cost_policy = self._task_cost_policy(task, job_request)
        inheritance_summary: dict[str, Any] = {}
        canonicalization_summary: dict[str, Any] = {}
        if str(normalization_scope.get("mode") or "") != "category":
            allow_historical_profile_inheritance = bool(cost_policy.get("allow_historical_profile_inheritance", True))
            if allow_historical_profile_inheritance:
                candidates, evidence, inheritance_summary = _inherit_historical_profile_captures(
                    self.store,
                    identity,
                    snapshot_dir,
                    candidates,
                    evidence,
                    job_request=job_request,
                )
            else:
                inheritance_summary = _profile_inheritance_summary()
            candidates, evidence, canonicalization_summary = canonicalize_company_records(candidates, evidence)

        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        existing_candidate_doc_payload: dict[str, Any] = {}
        if candidate_doc_path.exists():
            try:
                loaded_payload = json.loads(candidate_doc_path.read_text())
                if isinstance(loaded_payload, dict):
                    existing_candidate_doc_payload = loaded_payload
            except (OSError, json.JSONDecodeError):
                existing_candidate_doc_payload = {}

        logger = AssetLogger(snapshot_dir)
        logger.write_json(
            candidate_doc_path,
            {
                **existing_candidate_doc_payload,
                "candidates": [candidate.to_record() for candidate in candidates],
                "evidence": [item.to_record() for item in evidence],
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "historical_profile_inheritance": inheritance_summary,
                "canonicalization": canonicalization_summary,
            },
            asset_type="candidate_documents",
            source_kind="normalize_asset_snapshot",
            is_raw_asset=False,
            model_safe=True,
        )

        if (
            str(normalization_scope.get("mode") or "") == "category"
            and str(normalization_scope.get("category") or "").strip()
        ):
            self.store.replace_company_category_data(
                identity.canonical_name,
                str(normalization_scope.get("category") or ""),
                candidates,
                evidence,
            )
        else:
            self.store.replace_company_data(identity.canonical_name, candidates, evidence)
        manifest_path = snapshot_dir / "manifest.json"
        logger.write_json(
            manifest_path,
            {
                "snapshot_id": state.get("snapshot_id"),
                "company_identity": identity.to_record(),
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "normalization_scope": normalization_scope or {"mode": "company"},
                "historical_profile_inheritance": inheritance_summary,
                "canonicalization": canonicalization_summary,
                "storage": {
                    "execution": "local_runtime",
                    "candidate_store": str(self.store.db_path),
                    "future_cloud_direction": (
                        "Persist high-value LinkedIn profile assets and final result artifacts to cloud object "
                        "storage or a managed document store while keeping transient workflow logs local."
                    ),
                },
            },
            asset_type="snapshot_manifest",
            source_kind="normalize_asset_snapshot",
            is_raw_asset=False,
            model_safe=True,
        )
        if str(normalization_scope.get("mode") or "") != "category":
            self._write_latest_snapshot_pointer(
                identity,
                str(state.get("snapshot_id") or snapshot_dir.name).strip() or snapshot_dir.name,
                snapshot_dir,
            )

        artifact_build: dict[str, Any] = {}
        artifact_build_error = ""
        try:
            artifact_build = self._build_snapshot_candidate_artifacts(
                identity=identity,
                snapshot_dir=snapshot_dir,
                job_request=job_request,
            )
        except Exception as exc:  # pragma: no cover - runtime fallback path
            artifact_build_error = str(exc)

        artifact_paths = {
            str(key): str(value)
            for key, value in dict(artifact_build.get("artifact_paths") or {}).items()
            if str(key).strip() and str(value).strip()
        }
        sync_status = dict(artifact_build.get("sync_status") or {})
        hot_cache_sync = self._sync_snapshot_hot_cache(identity, snapshot_dir)
        if artifact_build_error:
            detail = (
                f"Persisted {len(candidates)} candidates for {identity.canonical_name} into the control-plane store. "
                f"Candidate artifact materialization failed: {artifact_build_error}"
            )
        else:
            detail = (
                f"Persisted {len(candidates)} candidates for {identity.canonical_name} into the control-plane store and "
                "materialized candidate artifacts."
            )
        if hot_cache_sync.get("status") == "failed":
            detail = f"{detail} Hot-cache sync failed: {hot_cache_sync.get('error', '')}".strip()

        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=detail,
            payload={
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "candidate_doc_path": str(candidate_doc_path),
                "manifest_path": str(manifest_path),
                "historical_profile_inheritance": inheritance_summary,
                "canonicalization": canonicalization_summary,
                "artifact_materialization_status": "failed" if artifact_build_error else "completed",
                "artifact_materialization_error": artifact_build_error,
                "artifact_dir": str(artifact_build.get("artifact_dir") or ""),
                "artifact_paths": artifact_paths,
                "sync_status": sync_status,
                "hot_cache_sync": hot_cache_sync,
            },
            state_updates={
                "candidates": candidates,
                "evidence": evidence,
                "candidate_doc_path": candidate_doc_path,
                "manifest_path": manifest_path,
            },
        )

    def _build_retrieval_index(self, task: AcquisitionTask, state: dict[str, Any]) -> AcquisitionExecution:
        identity = state.get("company_identity")
        snapshot_dir = state.get("snapshot_dir")
        if not isinstance(identity, CompanyIdentity) or not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Retrieval index build requires a resolved company identity and snapshot directory.",
                payload={},
            )

        candidate_count = self.store.candidate_count_for_company(identity.canonical_name)
        logger = AssetLogger(snapshot_dir)
        retrieval_summary_path = snapshot_dir / "retrieval_index_summary.json"
        logger.write_json(
            retrieval_summary_path,
            {
                "company_identity": identity.to_record(),
                "candidate_count": candidate_count,
                "index_type": "sqlite_structured_plus_semantic_ready_documents",
            },
            asset_type="retrieval_index_summary",
            source_kind="build_retrieval_index",
            is_raw_asset=False,
            model_safe=True,
        )
        hot_cache_sync = self._sync_snapshot_hot_cache(identity, snapshot_dir)
        detail = f"Prepared retrieval-ready candidate documents for {candidate_count} members."
        if hot_cache_sync.get("status") == "failed":
            detail = f"{detail} Hot-cache sync failed: {hot_cache_sync.get('error', '')}".strip()
        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=detail,
            payload={
                "candidate_count": candidate_count,
                "retrieval_index_summary_path": str(retrieval_summary_path),
                "hot_cache_sync": hot_cache_sync,
            },
        )

    def _build_snapshot_candidate_artifacts(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        job_request: JobRequest | None = None,
    ) -> dict[str, Any]:
        runtime_tuning_overrides = resolve_runtime_timing_overrides(
            dict(getattr(job_request, "execution_preferences", {}) or {})
        )
        preferred_source_snapshot_ids = [
            str(item or "").strip()
            for item in list(
                dict(getattr(job_request, "execution_preferences", {}) or {}).get("delta_baseline_snapshot_ids") or []
            )
            if str(item or "").strip()
        ]
        build_profile = (
            str(
                dict(getattr(job_request, "execution_preferences", {}) or {}).get("artifact_build_profile") or ""
            ).strip()
            or "full"
        )
        build_kwargs: dict[str, Any] = {
            "runtime_dir": self.settings.runtime_dir,
            "store": self.store,
            "target_company": identity.canonical_name,
            "snapshot_id": snapshot_dir.name,
            "snapshot_dir": snapshot_dir,
            "company_identity": identity.to_record(),
            "preferred_source_snapshot_ids": preferred_source_snapshot_ids or None,
            "build_profile": build_profile,
        }
        if runtime_tuning_overrides:
            build_kwargs["runtime_tuning_overrides"] = runtime_tuning_overrides
        return build_company_candidate_artifacts(**build_kwargs)

    def _hot_cache_snapshot_dir(self, identity: CompanyIdentity, snapshot_id: str) -> Path | None:
        if self.hot_cache_company_assets_dir is None:
            return None
        return self.hot_cache_company_assets_dir / identity.company_key / str(snapshot_id or "").strip()

    def _mirror_snapshot_to_hot_cache(self, identity: CompanyIdentity, snapshot_dir: Path) -> dict[str, Any]:
        hot_cache_snapshot_dir = self._hot_cache_snapshot_dir(identity, snapshot_dir.name)
        if hot_cache_snapshot_dir is None:
            return {
                "status": "disabled",
                "source_dir": str(snapshot_dir),
                "destination_dir": "",
                "file_count": 0,
                "directory_count": 0,
                "mode_counts": {},
            }
        hot_cache_snapshot_dir.parent.mkdir(parents=True, exist_ok=True)
        summary = mirror_tree_link_first(snapshot_dir, hot_cache_snapshot_dir)
        summary["snapshot_dir"] = str(hot_cache_snapshot_dir)
        return summary

    def _sync_snapshot_hot_cache(self, identity: CompanyIdentity, snapshot_dir: Path) -> dict[str, Any]:
        if self.hot_cache_company_assets_dir is None:
            return {"status": "disabled", "snapshot_dir": ""}
        try:
            mirror_summary = self._mirror_snapshot_to_hot_cache(identity, snapshot_dir)
        except OSError as exc:
            return {
                "status": "failed",
                "snapshot_dir": "",
                "error": str(exc),
            }
        return {
            "status": str(mirror_summary.get("status") or "completed"),
            "snapshot_dir": str(mirror_summary.get("snapshot_dir") or ""),
            "file_count": int(mirror_summary.get("file_count") or 0),
            "directory_count": int(mirror_summary.get("directory_count") or 0),
            "mode_counts": dict(mirror_summary.get("mode_counts") or {}),
        }

    def _write_latest_snapshot_pointer_to_dir(
        self,
        *,
        company_dir: Path,
        identity: CompanyIdentity,
        snapshot_id: str,
        snapshot_dir: Path,
    ) -> None:
        latest_pointer = company_dir / "latest_snapshot.json"
        AssetLogger(company_dir).write_json(
            latest_pointer,
            {
                "company_identity": identity.to_record(),
                "snapshot_id": snapshot_id,
                "snapshot_dir": str(snapshot_dir),
            },
            asset_type="latest_snapshot_pointer",
            source_kind="snapshot_registry",
            is_raw_asset=False,
            model_safe=True,
        )

    def _ensure_snapshot_dir(self, identity: CompanyIdentity, state: dict[str, Any]) -> tuple[str, Path]:
        snapshot_id = str(state.get("snapshot_id") or "").strip()
        if not snapshot_id:
            snapshot_id = datetime.now().strftime("%Y%m%dT%H%M%S")
        snapshot_dir = self.company_assets_dir / identity.company_key / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        hot_cache_snapshot_dir = self._hot_cache_snapshot_dir(identity, snapshot_id)
        if hot_cache_snapshot_dir is not None:
            hot_cache_snapshot_dir.mkdir(parents=True, exist_ok=True)
        return snapshot_id, snapshot_dir

    def _write_latest_snapshot_pointer(self, identity: CompanyIdentity, snapshot_id: str, snapshot_dir: Path) -> None:
        canonical_company_dir = self.company_assets_dir / identity.company_key
        self._write_latest_snapshot_pointer_to_dir(
            company_dir=canonical_company_dir,
            identity=identity,
            snapshot_id=snapshot_id,
            snapshot_dir=snapshot_dir,
        )
        hot_cache_snapshot_dir = self._hot_cache_snapshot_dir(identity, snapshot_id)
        if hot_cache_snapshot_dir is not None:
            self._write_latest_snapshot_pointer_to_dir(
                company_dir=hot_cache_snapshot_dir.parent,
                identity=identity,
                snapshot_id=snapshot_id,
                snapshot_dir=hot_cache_snapshot_dir,
            )

    def _hydrate_acquisition_snapshots_for_enrichment(
        self,
        *,
        state: dict[str, Any],
        roster_snapshot: CompanyRosterSnapshot | None,
        search_seed_snapshot: SearchSeedSnapshot | None,
        snapshot_dir: Path,
        fallback_target_company: str,
    ) -> tuple[CompanyRosterSnapshot | None, SearchSeedSnapshot | None]:
        identity = self._resolve_snapshot_hydration_identity(
            state=state,
            roster_snapshot=roster_snapshot,
            search_seed_snapshot=search_seed_snapshot,
            snapshot_dir=snapshot_dir,
            fallback_target_company=fallback_target_company,
        )
        restored_search_seed = _registry_load_search_seed_snapshot_from_snapshot_dir(
            snapshot_dir,
            identity=identity,
        )
        if restored_search_seed is not None:
            search_seed_snapshot = _merge_search_seed_snapshots(search_seed_snapshot, restored_search_seed)
            if isinstance(search_seed_snapshot, SearchSeedSnapshot):
                state["search_seed_snapshot"] = search_seed_snapshot
                identity = search_seed_snapshot.company_identity
                state["company_identity"] = identity
        if not isinstance(identity, CompanyIdentity):
            identity = self._resolve_snapshot_hydration_identity(
                state=state,
                roster_snapshot=roster_snapshot,
                search_seed_snapshot=search_seed_snapshot,
                snapshot_dir=snapshot_dir,
                fallback_target_company=fallback_target_company,
            )
        if isinstance(identity, CompanyIdentity):
            restored_roster = self._load_roster_snapshot_from_snapshot_dir(identity, snapshot_dir)
            if restored_roster is not None and self._roster_snapshot_entry_count(
                restored_roster
            ) >= self._roster_snapshot_entry_count(roster_snapshot):
                roster_snapshot = restored_roster
                state["roster_snapshot"] = roster_snapshot
                state["company_identity"] = roster_snapshot.company_identity
        elif roster_snapshot is not None:
            state["roster_snapshot"] = roster_snapshot
            state["company_identity"] = roster_snapshot.company_identity
        if isinstance(search_seed_snapshot, SearchSeedSnapshot):
            state["search_seed_snapshot"] = search_seed_snapshot
            state.setdefault("company_identity", search_seed_snapshot.company_identity)
        return roster_snapshot, search_seed_snapshot

    def _resolve_snapshot_hydration_identity(
        self,
        *,
        state: dict[str, Any],
        roster_snapshot: CompanyRosterSnapshot | None,
        search_seed_snapshot: SearchSeedSnapshot | None,
        snapshot_dir: Path,
        fallback_target_company: str,
    ) -> CompanyIdentity | None:
        state_identity = state.get("company_identity")
        if isinstance(state_identity, CompanyIdentity):
            return state_identity
        if isinstance(roster_snapshot, CompanyRosterSnapshot):
            return roster_snapshot.company_identity
        if isinstance(search_seed_snapshot, SearchSeedSnapshot):
            return search_seed_snapshot.company_identity
        artifact_payloads: list[dict[str, Any]] = [_read_company_asset_json(snapshot_dir / "identity.json")]
        candidate_doc_payload = _read_company_asset_json(snapshot_dir / "candidate_documents.json")
        if candidate_doc_payload:
            artifact_payloads.append(dict(candidate_doc_payload.get("snapshot") or {}))
            acquisition_sources = dict(candidate_doc_payload.get("acquisition_sources") or {})
            artifact_payloads.extend(
                dict(acquisition_sources.get(key) or {}) for key in ("roster_snapshot", "search_seed_snapshot")
            )
        artifact_payloads.append(_read_company_asset_json(snapshot_dir / "search_seed_discovery" / "summary.json"))
        artifact_payloads.append(
            _read_company_asset_json(
                snapshot_dir / "harvest_company_employees" / "harvest_company_employees_summary.json"
            )
        )
        artifact_payloads.append(_read_company_asset_json(snapshot_dir / "linkedin_company_people_summary.json"))
        for payload in artifact_payloads:
            identity = _company_identity_from_record(dict(payload.get("company_identity") or payload))
            if identity is not None:
                return identity
        normalized_target_company = str(fallback_target_company or "").strip()
        if not normalized_target_company:
            return None
        return CompanyIdentity(
            requested_name=normalized_target_company,
            canonical_name=normalized_target_company,
            company_key=normalize_name_token(normalized_target_company),
            linkedin_slug="",
            resolver="snapshot_hydration_fallback",
        )

    @staticmethod
    def _roster_snapshot_entry_count(snapshot: CompanyRosterSnapshot | None) -> int:
        if not isinstance(snapshot, CompanyRosterSnapshot):
            return 0
        visible_count = len(list(snapshot.visible_entries or []))
        raw_count = len(list(snapshot.raw_entries or []))
        return max(visible_count, raw_count)

    def _load_cached_roster_snapshot(
        self,
        identity: CompanyIdentity,
        *,
        exclude_snapshot_dir: Path | None = None,
    ) -> CompanyRosterSnapshot | None:
        company_dir = self.company_assets_dir / identity.company_key
        if not company_dir.exists():
            return None

        summary_paths = list(company_dir.glob("*/linkedin_company_people_summary.json"))
        summary_paths.extend(company_dir.glob("*/harvest_company_employees/harvest_company_employees_summary.json"))
        summary_paths = sorted(summary_paths, key=lambda item: item.stat().st_mtime, reverse=True)
        for summary_path in summary_paths:
            is_harvest_snapshot = summary_path.parent.name == "harvest_company_employees"
            snapshot_dir = summary_path.parent.parent if is_harvest_snapshot else summary_path.parent
            if exclude_snapshot_dir is not None and snapshot_dir == exclude_snapshot_dir:
                continue
            try:
                summary = json.loads(summary_path.read_text())
                completion_status = (
                    str(summary.get("completion_status") or summary.get("segmented_completion_status") or "completed")
                    .strip()
                    .lower()
                )
                if completion_status and completion_status not in {"completed", "ready"}:
                    continue
                if is_harvest_snapshot:
                    roster_dir = snapshot_dir / "harvest_company_employees"
                    visible_path = roster_dir / "harvest_company_employees_visible.json"
                    merged_path = roster_dir / "harvest_company_employees_merged.json"
                    headless_path = roster_dir / "harvest_company_employees_headless.json"
                else:
                    visible_path = snapshot_dir / "linkedin_company_people_visible.json"
                    merged_path = snapshot_dir / "linkedin_company_people_all.json"
                    headless_path = snapshot_dir / "linkedin_company_people_headless.json"
                visible_entries = json.loads(visible_path.read_text())
                if not visible_entries:
                    continue
                return CompanyRosterSnapshot(
                    snapshot_id=str(summary.get("snapshot_id") or snapshot_dir.name),
                    target_company=str(summary.get("target_company") or identity.canonical_name),
                    company_identity=identity,
                    snapshot_dir=snapshot_dir,
                    raw_entries=json.loads(merged_path.read_text()) if merged_path.exists() else visible_entries,
                    visible_entries=visible_entries,
                    headless_entries=json.loads(headless_path.read_text()) if headless_path.exists() else [],
                    page_summaries=list(summary.get("page_summaries") or []),
                    accounts_used=list(summary.get("accounts_used") or []),
                    errors=list(summary.get("errors") or []),
                    stop_reason=str(summary.get("stop_reason") or "cached_snapshot"),
                    merged_path=merged_path,
                    visible_path=visible_path,
                    headless_path=headless_path,
                    summary_path=summary_path,
                )
            except (OSError, json.JSONDecodeError):
                continue
        return None

    def _load_roster_snapshot_from_snapshot_dir(
        self,
        identity: CompanyIdentity,
        snapshot_dir: Path,
    ) -> CompanyRosterSnapshot | None:
        roster_layouts = (
            {
                "summary_path": snapshot_dir / "harvest_company_employees" / "harvest_company_employees_summary.json",
                "merged_path": snapshot_dir / "harvest_company_employees" / "harvest_company_employees_merged.json",
                "visible_path": snapshot_dir / "harvest_company_employees" / "harvest_company_employees_visible.json",
                "headless_path": snapshot_dir / "harvest_company_employees" / "harvest_company_employees_headless.json",
            },
            {
                "summary_path": snapshot_dir / "linkedin_company_people_summary.json",
                "merged_path": snapshot_dir / "linkedin_company_people_all.json",
                "visible_path": snapshot_dir / "linkedin_company_people_visible.json",
                "headless_path": snapshot_dir / "linkedin_company_people_headless.json",
            },
        )
        for layout in roster_layouts:
            summary_path = Path(layout["summary_path"])
            visible_path = Path(layout["visible_path"])
            merged_path = Path(layout["merged_path"])
            headless_path = Path(layout["headless_path"])
            if not summary_path.exists() or not visible_path.exists():
                continue
            summary = _read_company_asset_json(summary_path)
            visible_entries = _read_company_asset_json_list(visible_path)
            if not visible_entries:
                continue
            summary_identity = _company_identity_from_record(dict(summary.get("company_identity") or {}))
            effective_identity = summary_identity or identity
            return CompanyRosterSnapshot(
                snapshot_id=str(summary.get("snapshot_id") or snapshot_dir.name),
                target_company=str(summary.get("target_company") or effective_identity.canonical_name),
                company_identity=effective_identity,
                snapshot_dir=snapshot_dir,
                raw_entries=_read_company_asset_json_list(merged_path) if merged_path.exists() else visible_entries,
                visible_entries=visible_entries,
                headless_entries=_read_company_asset_json_list(headless_path) if headless_path.exists() else [],
                page_summaries=[
                    dict(item) for item in list(summary.get("page_summaries") or []) if isinstance(item, dict)
                ],
                accounts_used=[
                    str(item or "").strip()
                    for item in list(summary.get("accounts_used") or [])
                    if str(item or "").strip()
                ],
                errors=[
                    str(item or "").strip() for item in list(summary.get("errors") or []) if str(item or "").strip()
                ],
                stop_reason=str(summary.get("stop_reason") or "restored_snapshot_roster"),
                merged_path=merged_path,
                visible_path=visible_path,
                headless_path=headless_path,
                summary_path=summary_path,
            )
        return None

    def _materialize_reused_roster_snapshot(
        self,
        identity: CompanyIdentity,
        source_snapshot: CompanyRosterSnapshot,
        target_snapshot_dir: Path,
    ) -> CompanyRosterSnapshot:
        logger = AssetLogger(target_snapshot_dir)
        try:
            source_summary = json.loads(source_snapshot.summary_path.read_text())
        except (OSError, json.JSONDecodeError):
            source_summary = {}
        is_harvest_snapshot = source_snapshot.summary_path.parent.name == "harvest_company_employees"
        if is_harvest_snapshot:
            roster_dir = target_snapshot_dir / "harvest_company_employees"
            merged_path = roster_dir / "harvest_company_employees_merged.json"
            visible_path = roster_dir / "harvest_company_employees_visible.json"
            headless_path = roster_dir / "harvest_company_employees_headless.json"
            summary_path = roster_dir / "harvest_company_employees_summary.json"
        else:
            roster_dir = target_snapshot_dir
            merged_path = roster_dir / "linkedin_company_people_all.json"
            visible_path = roster_dir / "linkedin_company_people_visible.json"
            headless_path = roster_dir / "linkedin_company_people_headless.json"
            summary_path = roster_dir / "linkedin_company_people_summary.json"
        roster_dir.mkdir(parents=True, exist_ok=True)
        logger.write_json(
            merged_path,
            source_snapshot.raw_entries,
            asset_type="company_roster_merged",
            source_kind="reused_cached_roster",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            visible_path,
            source_snapshot.visible_entries,
            asset_type="company_roster_visible",
            source_kind="reused_cached_roster",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            headless_path,
            source_snapshot.headless_entries,
            asset_type="company_roster_headless",
            source_kind="reused_cached_roster",
            is_raw_asset=False,
            model_safe=True,
        )
        summary_payload = {
            **dict(source_summary),
            "company_identity": identity.to_record(),
            "raw_entry_count": len(source_snapshot.raw_entries),
            "visible_entry_count": len(source_snapshot.visible_entries),
            "headless_entry_count": len(source_snapshot.headless_entries),
            "page_summaries": source_snapshot.page_summaries,
            "accounts_used": list(source_snapshot.accounts_used or []),
            "errors": list(source_snapshot.errors or []),
            "stop_reason": "reused_cached_roster",
            "reused_from_snapshot_id": source_snapshot.snapshot_id,
            "reused_from_snapshot_dir": str(source_snapshot.snapshot_dir),
            "reuse_source_summary_path": str(source_snapshot.summary_path),
        }
        logger.write_json(
            summary_path,
            summary_payload,
            asset_type="company_roster_summary",
            source_kind="reused_cached_roster",
            is_raw_asset=False,
            model_safe=True,
        )
        return CompanyRosterSnapshot(
            snapshot_id=target_snapshot_dir.name,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=target_snapshot_dir,
            raw_entries=list(source_snapshot.raw_entries),
            visible_entries=list(source_snapshot.visible_entries),
            headless_entries=list(source_snapshot.headless_entries),
            page_summaries=list(source_snapshot.page_summaries),
            accounts_used=list(source_snapshot.accounts_used),
            errors=list(source_snapshot.errors),
            stop_reason="reused_cached_roster",
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=summary_path,
        )


def _anthropic_detail(task_type: str, bootstrap_summary: dict[str, Any]) -> str:
    mapping = {
        "resolve_company_identity": "Anthropic is already mapped to the local asset package.",
        "acquire_full_roster": "Used the local Anthropic asset snapshot as the full roster baseline.",
        "enrich_profiles_multisource": "Local asset already contains workbook rows, investor members, and scholar evidence.",
        "normalize_asset_snapshot": "Snapshot-authoritative candidate/evidence artifacts were refreshed.",
        "build_retrieval_index": "Candidate documents are available for structured and semantic-ready retrieval.",
    }
    base = mapping.get(task_type, "Completed using the local Anthropic asset snapshot.")
    if bootstrap_summary:
        base += f" Candidate count: {bootstrap_summary.get('candidate_count', 0)}."
    return base


def _inherit_historical_profile_captures(
    store: ControlPlaneStore,
    identity: CompanyIdentity,
    snapshot_dir: Path,
    candidates: list[Candidate],
    evidence: list[EvidenceRecord],
    *,
    job_request: JobRequest | None = None,
) -> tuple[list[Candidate], list[EvidenceRecord], dict[str, Any]]:
    company_dir = snapshot_dir.parent
    historical_sources: list[dict[str, Any]] = []

    if company_dir.exists():
        for candidate_snapshot_dir in sorted(
            path for path in company_dir.iterdir() if path.is_dir() and path != snapshot_dir
        ):
            payload_path = resolve_snapshot_serving_artifact_path(
                candidate_snapshot_dir,
                asset_view="canonical_merged",
                allow_candidate_documents_fallback=False,
            )
            if payload_path is None or not payload_path.exists():
                continue
            if payload_path.name == "manifest.json":
                source_type = "materialized_candidate_documents_manifest"
            elif payload_path.name in {"artifact_summary.json", "snapshot_manifest.json"}:
                source_type = "candidate_serving_manifest"
            else:
                source_type = payload_path.stem
            snapshot_candidates, snapshot_evidence = _load_snapshot_candidate_payload(
                payload_path, identity.canonical_name
            )
            if not snapshot_candidates and not snapshot_evidence:
                continue
            historical_sources.append(
                {
                    "source_type": source_type,
                    "snapshot_id": candidate_snapshot_dir.name,
                    "candidate_count": len(snapshot_candidates),
                    "evidence_count": len(snapshot_evidence),
                    "candidates": snapshot_candidates,
                    "evidence": snapshot_evidence,
                }
            )

    if not historical_sources:
        return list(candidates), list(evidence), _profile_inheritance_summary()

    baseline_source, baseline_summary = _select_full_roster_baseline_source(
        historical_sources=historical_sources,
        current_candidates=candidates,
        job_request=job_request,
    )
    if baseline_source is not None:
        baseline_candidates = list(baseline_source.get("candidates") or [])
        baseline_evidence = list(baseline_source.get("evidence") or [])
        baseline_merged_evidence_by_key: dict[tuple[str, str, str, str], EvidenceRecord] = {
            _evidence_dedupe_key(item): item for item in [*baseline_evidence, *evidence]
        }
        summary = _profile_inheritance_summary(
            history_source_count=len(historical_sources),
            history_candidate_count=sum(int(item.get("candidate_count") or 0) for item in historical_sources),
            matched_candidate_count=_count_historical_candidate_matches(candidates, baseline_candidates),
            inherited_candidate_count=0,
            inherited_evidence_count=max(len(baseline_merged_evidence_by_key) - len(evidence), 0),
            inherited_membership_review_count=0,
            carried_forward_candidate_count=len(baseline_candidates),
            history_sources=[
                {
                    "snapshot_id": str(item.get("snapshot_id") or ""),
                    "source_type": str(item.get("source_type") or ""),
                    "candidate_count": int(item.get("candidate_count") or 0),
                    "evidence_count": int(item.get("evidence_count") or 0),
                }
                for item in historical_sources
            ],
        )
        summary.update(baseline_summary)
        merged_evidence = sorted(
            baseline_merged_evidence_by_key.values(),
            key=lambda item: (item.candidate_id, item.source_type, item.title, item.url or item.source_path),
        )
        return list(baseline_candidates) + list(candidates), merged_evidence, summary

    match_index: dict[str, list[int]] = {}
    historical_candidates: list[Candidate] = []
    historical_evidence_by_candidate: dict[str, list[EvidenceRecord]] = {}
    for source in historical_sources:
        source_candidates = list(source.get("candidates") or [])
        source_evidence = list(source.get("evidence") or [])
        for candidate in source_candidates:
            history_index = len(historical_candidates)
            historical_candidates.append(candidate)
            for key in _candidate_match_keys(candidate):
                match_index.setdefault(key, []).append(history_index)
        for item in source_evidence:
            historical_evidence_by_candidate.setdefault(item.candidate_id, []).append(item)

    merged_candidates: list[Candidate] = []
    merged_evidence_by_key: dict[tuple[str, str, str, str], EvidenceRecord] = {
        _evidence_dedupe_key(item): item for item in evidence
    }
    matched_candidate_count = 0
    inherited_candidate_count = 0
    inherited_evidence_count = 0
    inherited_review_count = 0
    matched_historical_candidate_ids: set[str] = set()
    carried_forward_candidate_count = 0

    for candidate in candidates:
        matched_history = _match_historical_candidates(candidate, historical_candidates, match_index)
        if not matched_history:
            merged_candidates.append(candidate)
            continue

        matched_candidate_count += 1
        matched_historical_candidate_ids.update(item.candidate_id for item in matched_history if item.candidate_id)
        donor = _select_profile_field_donor(matched_history, historical_evidence_by_candidate)
        review_donor = _select_membership_review_donor(matched_history, historical_evidence_by_candidate)
        merged_candidate = candidate
        if donor is not None:
            merged_candidate = _inherit_profile_fields(merged_candidate, donor)
        review_candidate = merged_candidate
        if review_donor is not None:
            review_candidate = _inherit_membership_review_fields(merged_candidate, review_donor)
            if review_candidate.to_record() != merged_candidate.to_record():
                inherited_review_count += 1
        merged_candidate = review_candidate
        if merged_candidate.to_record() != candidate.to_record():
            inherited_candidate_count += 1
        if bool(merged_candidate.metadata.get("membership_review_required")) and not bool(
            candidate.metadata.get("membership_review_required")
        ):
            inherited_review_count += 1
        merged_candidates.append(merged_candidate)

        seen_history_candidate_ids: set[str] = set()
        for historical_candidate in matched_history:
            if historical_candidate.candidate_id in seen_history_candidate_ids:
                continue
            seen_history_candidate_ids.add(historical_candidate.candidate_id)
            for historical_evidence in historical_evidence_by_candidate.get(historical_candidate.candidate_id, []):
                if not _is_inheritable_profile_evidence(historical_evidence):
                    continue
                rebound = _rebind_evidence_candidate(historical_evidence, merged_candidate.candidate_id)
                evidence_key = _evidence_dedupe_key(rebound)
                if evidence_key in merged_evidence_by_key:
                    continue
                merged_evidence_by_key[evidence_key] = rebound
                inherited_evidence_count += 1

    carry_forward_groups: dict[str, list[Candidate]] = {}
    for historical_candidate in historical_candidates:
        if historical_candidate.candidate_id in matched_historical_candidate_ids:
            continue
        group_key = (
            str(historical_candidate.candidate_id or "")
            or f"name:{normalize_name_token(historical_candidate.display_name or historical_candidate.name_en)}"
        )
        carry_forward_groups.setdefault(group_key, []).append(historical_candidate)

    for group in carry_forward_groups.values():
        group_evidence: list[EvidenceRecord] = []
        for historical_candidate in group:
            group_evidence.extend(historical_evidence_by_candidate.get(historical_candidate.candidate_id, []))
        donor = (
            _select_membership_review_donor(group, historical_evidence_by_candidate)
            or _select_profile_field_donor(group, historical_evidence_by_candidate)
            or max(
                group,
                key=lambda item: _historical_candidate_rank(
                    item, historical_evidence_by_candidate.get(item.candidate_id, [])
                ),
            )
        )
        if not _should_carry_forward_historical_candidate(donor, group_evidence):
            continue
        merged_candidates.append(donor)
        carried_forward_candidate_count += 1
        for item in group_evidence:
            evidence_key = _evidence_dedupe_key(item)
            if evidence_key in merged_evidence_by_key:
                continue
            merged_evidence_by_key[evidence_key] = item
            inherited_evidence_count += 1

    merged_evidence = sorted(
        merged_evidence_by_key.values(),
        key=lambda item: (item.candidate_id, item.source_type, item.title, item.url or item.source_path),
    )
    return (
        merged_candidates,
        merged_evidence,
        _profile_inheritance_summary(
            history_source_count=len(historical_sources),
            history_candidate_count=len(historical_candidates),
            matched_candidate_count=matched_candidate_count,
            inherited_candidate_count=inherited_candidate_count,
            inherited_evidence_count=inherited_evidence_count,
            inherited_membership_review_count=inherited_review_count,
            carried_forward_candidate_count=carried_forward_candidate_count,
            history_sources=[
                {
                    "snapshot_id": str(item.get("snapshot_id") or ""),
                    "source_type": str(item.get("source_type") or ""),
                    "candidate_count": int(item.get("candidate_count") or 0),
                    "evidence_count": int(item.get("evidence_count") or 0),
                }
                for item in historical_sources
            ],
        ),
    )


def _profile_inheritance_summary(
    *,
    history_source_count: int = 0,
    history_candidate_count: int = 0,
    matched_candidate_count: int = 0,
    inherited_candidate_count: int = 0,
    inherited_evidence_count: int = 0,
    inherited_membership_review_count: int = 0,
    carried_forward_candidate_count: int = 0,
    history_sources: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "history_source_count": history_source_count,
        "history_candidate_count": history_candidate_count,
        "matched_candidate_count": matched_candidate_count,
        "inherited_candidate_count": inherited_candidate_count,
        "inherited_evidence_count": inherited_evidence_count,
        "inherited_membership_review_count": inherited_membership_review_count,
        "carried_forward_candidate_count": carried_forward_candidate_count,
        "history_sources": list(history_sources or []),
    }


def _select_full_roster_baseline_source(
    *,
    historical_sources: list[dict[str, Any]],
    current_candidates: list[Candidate],
    job_request: JobRequest | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if job_request is None:
        return None, {"baseline_snapshot_reused": False, "baseline_reason": "job_request_missing"}
    if str(job_request.target_scope or "").strip().lower() != "full_company_asset":
        return None, {"baseline_snapshot_reused": False, "baseline_reason": "target_scope_not_full_company_asset"}
    intent_view = resolve_request_intent_view(job_request)
    if bool(dict(intent_view.get("execution_preferences") or {}).get("force_fresh_run")):
        return None, {"baseline_snapshot_reused": False, "baseline_reason": "force_fresh_run"}

    current_candidate_count = len(current_candidates)
    if current_candidate_count >= _FULL_ROSTER_BASELINE_REUSE_MIN_CANDIDATES:
        return None, {
            "baseline_snapshot_reused": False,
            "baseline_reason": "current_snapshot_already_large_enough",
            "current_candidate_count": current_candidate_count,
        }

    reference_candidate_count = max((int(item.get("candidate_count") or 0) for item in historical_sources), default=0)
    if reference_candidate_count < _FULL_ROSTER_BASELINE_REUSE_MIN_CANDIDATES:
        return None, {
            "baseline_snapshot_reused": False,
            "baseline_reason": "no_large_historical_baseline_available",
            "current_candidate_count": current_candidate_count,
        }

    sparse_threshold = max(
        _FULL_ROSTER_BASELINE_REUSE_MIN_CURRENT_COUNT,
        max(
            1,
            int(reference_candidate_count * _FULL_ROSTER_BASELINE_REUSE_CURRENT_RATIO_THRESHOLD),
        ),
    )
    if current_candidate_count >= sparse_threshold:
        return None, {
            "baseline_snapshot_reused": False,
            "baseline_reason": "current_snapshot_not_sparse_enough",
            "current_candidate_count": current_candidate_count,
            "reference_candidate_count": reference_candidate_count,
        }

    max_distance = max(
        _FULL_ROSTER_BASELINE_REUSE_REFERENCE_DISTANCE_FLOOR,
        int(reference_candidate_count * _FULL_ROSTER_BASELINE_REUSE_REFERENCE_DISTANCE_RATIO),
    )
    reusable_sources: list[dict[str, Any]] = []
    for source in historical_sources:
        source_count = int(source.get("candidate_count") or 0)
        if source_count < _FULL_ROSTER_BASELINE_REUSE_MIN_CANDIDATES:
            continue
        if abs(source_count - reference_candidate_count) > max_distance:
            continue
        reusable_sources.append(source)

    if not reusable_sources:
        return None, {
            "baseline_snapshot_reused": False,
            "baseline_reason": "no_reusable_baseline_selected",
            "current_candidate_count": current_candidate_count,
            "reference_candidate_count": reference_candidate_count,
        }

    minimum_distance = min(
        abs(int(item.get("candidate_count") or 0) - reference_candidate_count) for item in reusable_sources
    )
    tied_sources = [
        item
        for item in reusable_sources
        if abs(int(item.get("candidate_count") or 0) - reference_candidate_count) == minimum_distance
    ]
    selected = max(tied_sources, key=lambda item: str(item.get("snapshot_id") or ""))
    return selected, {
        "baseline_snapshot_reused": True,
        "baseline_snapshot_id": str(selected.get("snapshot_id") or ""),
        "baseline_source_type": str(selected.get("source_type") or ""),
        "baseline_candidate_count": int(selected.get("candidate_count") or 0),
        "baseline_reason": (
            "Current scoped snapshot is sparse relative to the reusable full-company baseline; "
            "reuse the closest historical baseline before canonicalization."
        ),
        "current_candidate_count": current_candidate_count,
        "reference_candidate_count": reference_candidate_count,
    }


def _count_historical_candidate_matches(
    candidates: list[Candidate],
    historical_candidates: list[Candidate],
) -> int:
    if not candidates or not historical_candidates:
        return 0
    match_index: dict[str, list[int]] = {}
    for index, candidate in enumerate(historical_candidates):
        for key in _candidate_match_keys(candidate):
            match_index.setdefault(key, []).append(index)
    matched = 0
    for candidate in candidates:
        if _match_historical_candidates(candidate, historical_candidates, match_index):
            matched += 1
    return matched


def _load_snapshot_candidate_payload(
    payload_path: Path, target_company: str
) -> tuple[list[Candidate], list[EvidenceRecord]]:
    payload: dict[str, Any] = {}
    if (
        payload_path.name
        in {
            "manifest.json",
            "artifact_summary.json",
            "snapshot_manifest.json",
            "materialized_candidate_documents.json",
            "normalized_candidates.json",
            "reusable_candidate_documents.json",
        }
        and "normalized_artifacts" in payload_path.parts
    ):
        artifact_dir = payload_path.parent
        snapshot_dir = (
            artifact_dir.parent if artifact_dir.name == "normalized_artifacts" else artifact_dir.parent.parent
        )
        try:
            loaded = load_snapshot_candidate_artifact_payload(
                snapshot_dir=snapshot_dir,
                target_company=target_company,
                asset_view="canonical_merged" if artifact_dir.name == "normalized_artifacts" else artifact_dir.name,
                company_key=snapshot_dir.parent.name,
                company_identity={},
                allow_candidate_documents_fallback=False,
            )
            payload = dict(loaded.get("source_payload") or {})
        except CandidateArtifactError:
            return [], []
    else:
        try:
            payload = json.loads(payload_path.read_text())
        except (OSError, json.JSONDecodeError):
            return [], []
    if not isinstance(payload, dict):
        return [], []
    candidates: list[Candidate] = []
    candidate_ids: set[str] = set()
    for item in list(payload.get("candidates") or []):
        candidate = _candidate_from_payload(item)
        if candidate is None:
            continue
        if normalize_name_token(candidate.target_company) != normalize_name_token(target_company):
            continue
        candidates.append(candidate)
        candidate_ids.add(candidate.candidate_id)
    evidence: list[EvidenceRecord] = []
    for item in list(payload.get("evidence") or []):
        evidence_record = _evidence_record_from_payload(item)
        if evidence_record is None:
            continue
        if candidate_ids and evidence_record.candidate_id not in candidate_ids:
            continue
        evidence.append(evidence_record)
    return candidates, evidence


def _candidate_from_payload(payload: dict[str, Any]) -> Candidate | None:
    if not isinstance(payload, dict):
        return None
    record = {
        "candidate_id": str(payload.get("candidate_id") or "").strip(),
        "name_en": str(payload.get("name_en") or "").strip(),
        "name_zh": str(payload.get("name_zh") or "").strip(),
        "display_name": str(payload.get("display_name") or "").strip(),
        "category": str(payload.get("category") or "").strip(),
        "target_company": str(payload.get("target_company") or "").strip(),
        "organization": str(payload.get("organization") or "").strip(),
        "employment_status": str(payload.get("employment_status") or "").strip(),
        "role": str(payload.get("role") or "").strip(),
        "team": str(payload.get("team") or "").strip(),
        "joined_at": str(payload.get("joined_at") or "").strip(),
        "left_at": str(payload.get("left_at") or "").strip(),
        "current_destination": str(payload.get("current_destination") or "").strip(),
        "ethnicity_background": str(payload.get("ethnicity_background") or "").strip(),
        "investment_involvement": str(payload.get("investment_involvement") or "").strip(),
        "focus_areas": str(payload.get("focus_areas") or "").strip(),
        "education": str(payload.get("education") or "").strip(),
        "work_history": str(payload.get("work_history") or "").strip(),
        "notes": str(payload.get("notes") or "").strip(),
        "linkedin_url": str(payload.get("linkedin_url") or "").strip(),
        "media_url": str(payload.get("media_url") or "").strip(),
        "source_dataset": str(payload.get("source_dataset") or "").strip(),
        "source_path": str(payload.get("source_path") or "").strip(),
        "metadata": dict(payload.get("metadata") or {}),
    }
    if not record["candidate_id"] or not record["name_en"] or not record["target_company"]:
        return None
    if not record["display_name"]:
        record["display_name"] = record["name_en"]
    return normalize_candidate(Candidate(**record))


def _evidence_record_from_payload(payload: dict[str, Any]) -> EvidenceRecord | None:
    if not isinstance(payload, dict):
        return None
    candidate_id = str(payload.get("candidate_id") or "").strip()
    source_type = str(payload.get("source_type") or "").strip()
    title = str(payload.get("title") or "").strip()
    url = str(payload.get("url") or "").strip()
    source_dataset = str(payload.get("source_dataset") or "").strip() or source_type
    source_path = str(payload.get("source_path") or "").strip()
    if not candidate_id or not source_type:
        return None
    return EvidenceRecord(
        evidence_id=str(payload.get("evidence_id") or "").strip()
        or make_evidence_id(candidate_id, source_dataset, title, url or source_path),
        candidate_id=candidate_id,
        source_type=source_type,
        title=title,
        url=url,
        summary=str(payload.get("summary") or "").strip(),
        source_dataset=source_dataset,
        source_path=source_path,
        metadata=dict(payload.get("metadata") or {}),
    )


def _candidate_match_keys(candidate: Candidate) -> list[str]:
    keys: list[str] = []
    candidate_id = str(candidate.candidate_id or "").strip()
    if candidate_id:
        keys.append(f"id:{candidate_id}")
    for identifier in sorted(_candidate_linkedin_identifiers(candidate)):
        keys.append(f"linkedin:{identifier}")
    name_key = normalize_name_token(candidate.display_name or candidate.name_en)
    if name_key:
        keys.append(f"name:{name_key}")
    return keys


def _candidate_linkedin_identifiers(candidate: Candidate) -> set[str]:
    metadata = dict(candidate.metadata or {})
    identifiers: set[str] = set()
    for value in [
        candidate.linkedin_url,
        metadata.get("profile_url"),
        metadata.get("linkedin_url"),
        metadata.get("public_identifier"),
        metadata.get("seed_slug"),
        metadata.get("more_profiles"),
    ]:
        _append_linkedin_identifier(identifiers, value)
    return identifiers


def _append_linkedin_identifier(identifiers: set[str], value: Any) -> None:
    if isinstance(value, dict):
        for key in ["url", "profile_url", "linkedin_url", "public_identifier", "username"]:
            _append_linkedin_identifier(identifiers, value.get(key))
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _append_linkedin_identifier(identifiers, item)
        return
    normalized = _normalize_linkedin_identifier(str(value or ""))
    if normalized:
        identifiers.add(normalized)


def _normalize_linkedin_identifier(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    lowered = raw.lower().rstrip("/")
    if "linkedin.com" in lowered:
        slug = _extract_linkedin_slug(raw)
        if slug:
            return slug.lower()
        return lowered
    return raw.strip().strip("/").lower()


def _extract_linkedin_slug(url: str) -> str:
    match = re.search(r"linkedin\.com/in/([^/?#]+)", str(url or ""), re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()


def _match_historical_candidates(
    candidate: Candidate,
    historical_candidates: list[Candidate],
    match_index: dict[str, list[int]],
) -> list[Candidate]:
    matched: list[Candidate] = []
    seen_indexes: set[int] = set()

    candidate_id = str(candidate.candidate_id or "").strip()
    if candidate_id:
        for index in match_index.get(f"id:{candidate_id}", []):
            if index in seen_indexes:
                continue
            seen_indexes.add(index)
            matched.append(historical_candidates[index])
    if matched:
        return matched

    for identifier in sorted(_candidate_linkedin_identifiers(candidate)):
        for index in match_index.get(f"linkedin:{identifier}", []):
            if index in seen_indexes:
                continue
            seen_indexes.add(index)
            matched.append(historical_candidates[index])
    if matched:
        return matched

    name_key = normalize_name_token(candidate.display_name or candidate.name_en)
    if not name_key:
        return matched
    for index in match_index.get(f"name:{name_key}", []):
        if index in seen_indexes:
            continue
        seen_indexes.add(index)
        matched.append(historical_candidates[index])
    return matched


def _select_profile_field_donor(
    historical_candidates: list[Candidate],
    historical_evidence_by_candidate: dict[str, list[EvidenceRecord]],
) -> Candidate | None:
    ranked = sorted(
        historical_candidates,
        key=lambda item: _historical_candidate_rank(item, historical_evidence_by_candidate.get(item.candidate_id, [])),
        reverse=True,
    )
    for candidate in ranked:
        metadata = dict(candidate.metadata or {})
        if candidate.category == "non_member" or bool(metadata.get("target_company_mismatch")):
            continue
        if _historical_candidate_rank(candidate, historical_evidence_by_candidate.get(candidate.candidate_id, [])) <= 0:
            continue
        return candidate
    return None


def _select_membership_review_donor(
    historical_candidates: list[Candidate],
    historical_evidence_by_candidate: dict[str, list[EvidenceRecord]],
) -> Candidate | None:
    ranked = sorted(
        historical_candidates,
        key=lambda item: _membership_review_rank(item, historical_evidence_by_candidate.get(item.candidate_id, [])),
        reverse=True,
    )
    for candidate in ranked:
        if _membership_review_rank(candidate, historical_evidence_by_candidate.get(candidate.candidate_id, [])) > 0:
            return candidate
    return None


def _historical_candidate_rank(candidate: Candidate, evidence: list[EvidenceRecord]) -> int:
    score = 0
    if any(_is_inheritable_profile_evidence(item) for item in evidence):
        score += 10
    if str(candidate.education or "").strip():
        score += 3
    work_history = str(candidate.work_history or "").strip()
    if work_history and not _is_roster_baseline_only(candidate):
        score += 3
    if bool(dict(candidate.metadata or {}).get("membership_review_required")):
        score += 4
    if _has_explicit_manual_review_resolution(candidate, evidence):
        score += 6
    if (
        str(candidate.focus_areas or "").strip()
        and str(candidate.focus_areas or "").strip() != str(candidate.role or "").strip()
    ):
        score += 1
    if str(candidate.linkedin_url or "").strip():
        score += 1
    return score


def _membership_review_rank(candidate: Candidate, evidence: list[EvidenceRecord]) -> int:
    metadata = dict(candidate.metadata or {})
    score = 0
    if _has_explicit_manual_review_resolution(candidate, evidence):
        score += 100
    if bool(metadata.get("membership_review_required")):
        score += 20
    if str(metadata.get("membership_review_decision") or "").strip():
        score += 8
    if candidate.category == "non_member":
        score += 6
    if bool(metadata.get("target_company_mismatch")):
        score += 4
    if score <= 0:
        return 0
    return score + _historical_candidate_rank(candidate, evidence)


def _inherit_profile_fields(candidate: Candidate, historical_candidate: Candidate) -> Candidate:
    current_record = candidate.to_record()
    historical_record = historical_candidate.to_record()
    current_metadata = dict(current_record.get("metadata") or {})
    historical_metadata = dict(historical_record.get("metadata") or {})
    baseline_only = _is_roster_baseline_only(candidate)

    if (not str(current_record.get("role") or "").strip() or baseline_only) and str(
        historical_record.get("role") or ""
    ).strip():
        current_record["role"] = historical_record["role"]
    if (
        not str(current_record.get("focus_areas") or "").strip()
        or baseline_only
        or str(current_record.get("focus_areas") or "").strip() == str(candidate.role or "").strip()
    ) and str(historical_record.get("focus_areas") or "").strip():
        current_record["focus_areas"] = historical_record["focus_areas"]
    if not str(current_record.get("education") or "").strip() and str(historical_record.get("education") or "").strip():
        current_record["education"] = historical_record["education"]
    if (
        not str(current_record.get("work_history") or "").strip()
        or baseline_only
        or str(current_record.get("work_history") or "").strip() == str(candidate.role or "").strip()
    ) and str(historical_record.get("work_history") or "").strip():
        current_record["work_history"] = historical_record["work_history"]
    if (
        not str(current_record.get("linkedin_url") or "").strip()
        and str(historical_record.get("linkedin_url") or "").strip()
    ):
        current_record["linkedin_url"] = historical_record["linkedin_url"]

    for key in [
        "profile_url",
        "public_identifier",
        "profile_account_id",
        "profile_location",
        "more_profiles",
        "membership_claim_category",
        "membership_claim_employment_status",
        "membership_review_required",
        "membership_review_reason",
        "membership_review_decision",
        "membership_review_confidence",
        "membership_review_rationale",
        "membership_review_triggers",
        "membership_review_trigger_keywords",
    ]:
        incoming_value = historical_metadata.get(key)
        if incoming_value in ("", None, [], {}):
            continue
        existing_value = current_metadata.get(key)
        if existing_value in ("", None, [], {}):
            current_metadata[key] = _copy_metadata_value(incoming_value)

    if bool(historical_metadata.get("membership_review_required")):
        current_metadata["membership_review_required"] = True
        if str(historical_metadata.get("membership_review_reason") or "").strip():
            current_metadata["membership_review_reason"] = str(
                historical_metadata.get("membership_review_reason") or ""
            ).strip()
        if str(historical_metadata.get("membership_review_decision") or "").strip():
            current_metadata["membership_review_decision"] = str(
                historical_metadata.get("membership_review_decision") or ""
            ).strip()
        if str(historical_metadata.get("membership_review_confidence") or "").strip():
            current_metadata["membership_review_confidence"] = str(
                historical_metadata.get("membership_review_confidence") or ""
            ).strip()
        if historical_metadata.get("membership_review_triggers"):
            current_metadata["membership_review_triggers"] = list(
                historical_metadata.get("membership_review_triggers") or []
            )
        if historical_metadata.get("membership_review_trigger_keywords"):
            current_metadata["membership_review_trigger_keywords"] = list(
                historical_metadata.get("membership_review_trigger_keywords") or []
            )
        review_rationale = str(historical_metadata.get("membership_review_rationale") or "").strip()
        if review_rationale:
            current_metadata["membership_review_rationale"] = review_rationale
            current_record["notes"] = _append_unique_note(
                str(current_record.get("notes") or "").strip(), review_rationale
            )

    current_record["metadata"] = current_metadata
    if str(historical_record.get("source_path") or "").strip() and current_record != candidate.to_record():
        current_record["source_path"] = historical_record["source_path"]
    return Candidate(**current_record)


def _inherit_membership_review_fields(candidate: Candidate, historical_candidate: Candidate) -> Candidate:
    current_record = candidate.to_record()
    historical_record = historical_candidate.to_record()
    current_metadata = dict(current_record.get("metadata") or {})
    historical_metadata = dict(historical_record.get("metadata") or {})

    if _has_explicit_manual_review_resolution(historical_candidate):
        current_record["category"] = str(historical_record.get("category") or "").strip() or current_record["category"]
        current_record["employment_status"] = str(historical_record.get("employment_status") or "").strip()
        current_record["organization"] = str(historical_record.get("organization") or "").strip()
        if str(historical_record.get("role") or "").strip() or historical_candidate.category == "non_member":
            current_record["role"] = str(historical_record.get("role") or "").strip()
        if str(historical_record.get("team") or "").strip():
            current_record["team"] = str(historical_record.get("team") or "").strip()
        if str(historical_record.get("linkedin_url") or "").strip() or historical_candidate.category == "non_member":
            current_record["linkedin_url"] = str(historical_record.get("linkedin_url") or "").strip()
        if str(historical_record.get("media_url") or "").strip() or historical_candidate.category == "non_member":
            current_record["media_url"] = str(historical_record.get("media_url") or "").strip()
        if str(historical_record.get("current_destination") or "").strip():
            current_record["current_destination"] = str(historical_record.get("current_destination") or "").strip()
        if str(historical_record.get("source_path") or "").strip():
            current_record["source_path"] = str(historical_record.get("source_path") or "").strip()
        if str(historical_record.get("source_dataset") or "").strip():
            current_record["source_dataset"] = str(historical_record.get("source_dataset") or "").strip()
        if str(historical_record.get("notes") or "").strip():
            current_record["notes"] = _append_unique_note(
                str(current_record.get("notes") or "").strip(),
                str(historical_record.get("notes") or "").strip(),
            )

        decision = str(historical_metadata.get("membership_review_decision") or "").strip()
        if not decision:
            if current_record["category"] == "non_member" or bool(historical_metadata.get("target_company_mismatch")):
                decision = "manual_non_member"
            elif (
                current_record["category"] in {"employee", "former_employee"}
                and str(current_record["employment_status"] or "").strip()
            ):
                decision = "manual_confirmed_member"

        current_metadata["membership_review_required"] = bool(historical_metadata.get("membership_review_required"))
        current_metadata["membership_review_reason"] = str(
            historical_metadata.get("membership_review_reason") or ""
        ).strip()
        current_metadata["membership_review_decision"] = decision
        current_metadata["membership_review_confidence"] = str(
            historical_metadata.get("membership_review_confidence") or ""
        ).strip()
        current_metadata["membership_review_rationale"] = str(
            historical_metadata.get("membership_review_rationale") or ""
        ).strip()
        current_metadata["membership_review_triggers"] = list(
            historical_metadata.get("membership_review_triggers") or []
        )
        current_metadata["membership_review_trigger_keywords"] = list(
            historical_metadata.get("membership_review_trigger_keywords") or []
        )
        current_metadata["target_company_mismatch"] = bool(historical_metadata.get("target_company_mismatch"))
        if "manual_review_links" in historical_metadata or historical_metadata.get("manual_review_links"):
            current_metadata["manual_review_links"] = _copy_metadata_value(
                historical_metadata.get("manual_review_links") or []
            )
        if "manual_review_artifact_root" in historical_metadata or historical_metadata.get(
            "manual_review_artifact_root"
        ):
            current_metadata["manual_review_artifact_root"] = str(
                historical_metadata.get("manual_review_artifact_root") or ""
            ).strip()
        if "manual_review_signals" in historical_metadata:
            current_metadata["manual_review_signals"] = _copy_metadata_value(
                historical_metadata.get("manual_review_signals") or {}
            )
        if str(historical_record.get("linkedin_url") or "").strip():
            current_metadata["profile_url"] = str(historical_record.get("linkedin_url") or "").strip()
        for key in ["public_identifier", "profile_account_id", "profile_location", "more_profiles"]:
            if key in historical_metadata and historical_metadata.get(key) not in ("", None, [], {}):
                current_metadata[key] = _copy_metadata_value(historical_metadata.get(key))
    elif bool(historical_metadata.get("membership_review_required")):
        current_metadata["membership_review_required"] = True
        if str(historical_metadata.get("membership_review_reason") or "").strip():
            current_metadata["membership_review_reason"] = str(
                historical_metadata.get("membership_review_reason") or ""
            ).strip()
        if str(historical_metadata.get("membership_review_decision") or "").strip():
            current_metadata["membership_review_decision"] = str(
                historical_metadata.get("membership_review_decision") or ""
            ).strip()
        if str(historical_metadata.get("membership_review_confidence") or "").strip():
            current_metadata["membership_review_confidence"] = str(
                historical_metadata.get("membership_review_confidence") or ""
            ).strip()
        if historical_metadata.get("membership_review_triggers"):
            current_metadata["membership_review_triggers"] = list(
                historical_metadata.get("membership_review_triggers") or []
            )
        if historical_metadata.get("membership_review_trigger_keywords"):
            current_metadata["membership_review_trigger_keywords"] = list(
                historical_metadata.get("membership_review_trigger_keywords") or []
            )
        review_rationale = str(historical_metadata.get("membership_review_rationale") or "").strip()
        if review_rationale:
            current_metadata["membership_review_rationale"] = review_rationale
            current_record["notes"] = _append_unique_note(
                str(current_record.get("notes") or "").strip(), review_rationale
            )

    current_record["metadata"] = current_metadata
    return Candidate(**current_record)


def _copy_metadata_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _copy_metadata_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_metadata_value(item) for item in value]
    return value


def _append_unique_note(existing: str, incoming: str) -> str:
    normalized_existing = str(existing or "").strip()
    normalized_incoming = str(incoming or "").strip()
    if not normalized_incoming:
        return normalized_existing
    if not normalized_existing:
        return normalized_incoming
    if normalized_incoming in normalized_existing:
        return normalized_existing
    return f"{normalized_existing} | {normalized_incoming}"


def _is_roster_baseline_only(candidate: Candidate) -> bool:
    source_dataset = str(candidate.source_dataset or "").strip()
    role = str(candidate.role or "").strip()
    work_history = str(candidate.work_history or "").strip()
    notes = str(candidate.notes or "")
    return (
        source_dataset.endswith("_linkedin_company_people")
        and bool(role)
        and (not work_history or work_history == role)
        and "LinkedIn company roster baseline." in notes
        and not str(candidate.education or "").strip()
    )


def _is_inheritable_profile_evidence(evidence: EvidenceRecord) -> bool:
    source_type = str(evidence.source_type or "").strip()
    source_dataset = str(evidence.source_dataset or "").strip()
    if source_dataset == "manual_review" or source_type == "manual_review_link":
        return True
    if source_type == "manual_review_analysis":
        return True
    return source_type.startswith("linkedin_profile_")


def _has_explicit_manual_review_resolution(candidate: Candidate, evidence: list[EvidenceRecord] | None = None) -> bool:
    metadata = dict(candidate.metadata or {})
    if str(metadata.get("manual_review_artifact_root") or "").strip():
        return True
    if list(metadata.get("manual_review_links") or []):
        return True
    decision = str(metadata.get("membership_review_decision") or "").strip().lower()
    if decision.startswith("manual_"):
        return True
    for item in list(evidence or []):
        if str(item.source_dataset or "").strip() == "manual_review":
            return True
    return False


def _should_carry_forward_historical_candidate(candidate: Candidate, evidence: list[EvidenceRecord]) -> bool:
    metadata = dict(candidate.metadata or {})
    if bool(metadata.get("membership_review_required")):
        return True
    if candidate.category in {"lead", "former_employee", "investor", "non_member"}:
        return True
    if str(candidate.education or "").strip():
        return True
    if str(candidate.work_history or "").strip() and not _is_roster_baseline_only(candidate):
        return True
    for item in evidence:
        if _is_inheritable_profile_evidence(item):
            return True
        if str(item.source_type or "").strip() not in {"linkedin_company_people", "company_roster"}:
            return True
    return False


def _rebind_evidence_candidate(evidence: EvidenceRecord, candidate_id: str) -> EvidenceRecord:
    title = str(evidence.title or "").strip()
    url = str(evidence.url or "").strip()
    source_path = str(evidence.source_path or "").strip()
    source_dataset = str(evidence.source_dataset or evidence.source_type or "").strip()
    return EvidenceRecord(
        evidence_id=make_evidence_id(candidate_id, source_dataset, title, url or source_path),
        candidate_id=candidate_id,
        source_type=str(evidence.source_type or "").strip(),
        title=title,
        url=url,
        summary=str(evidence.summary or "").strip(),
        source_dataset=source_dataset,
        source_path=source_path,
        metadata=_copy_metadata_value(dict(evidence.metadata or {})),
    )


def _evidence_dedupe_key(evidence: EvidenceRecord) -> tuple[str, str, str, str]:
    return (
        str(evidence.candidate_id or "").strip(),
        str(evidence.source_dataset or evidence.source_type or "").strip(),
        str(evidence.title or "").strip(),
        str(evidence.url or evidence.source_path or "").strip(),
    )


def _build_former_filter_hints(
    *,
    identity: CompanyIdentity,
    base_filter_hints: dict[str, Any] | None = None,
) -> dict[str, list[str]]:
    base = {
        str(key): [str(item).strip() for item in list(values or []) if str(item).strip()]
        for key, values in dict(base_filter_hints or {}).items()
    }
    company_reference = str(identity.linkedin_company_url or identity.canonical_name or identity.requested_name).strip()
    former_filter_hints: dict[str, list[str]] = {}
    company_candidates: list[str] = []
    for key in ["past_companies", "current_companies"]:
        for item in list(base.get(key) or []):
            normalized = str(item).strip()
            if normalized and normalized not in company_candidates:
                company_candidates.append(normalized)
    if not company_candidates and company_reference:
        company_candidates = [company_reference]
    if company_candidates:
        former_filter_hints["past_companies"] = company_candidates
    for key in [
        "scope_keywords",
        "job_titles",
        "keywords",
        "locations",
        "exclude_locations",
        "function_ids",
        "exclude_function_ids",
    ]:
        values = list(base.get(key) or [])
        if values:
            former_filter_hints[key] = values
    return former_filter_hints


def _normalize_company_employee_shards(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(value, start=1):
        if not isinstance(item, dict):
            continue
        shard_id = _normalize_shard_id(str(item.get("shard_id") or f"shard_{index}"))
        if not shard_id or shard_id in seen_ids:
            continue
        seen_ids.add(shard_id)
        company_filters = dict(item.get("company_filters") or {})
        normalized_filters: dict[str, list[str] | str] = {}
        for key in [
            "companies",
            "locations",
            "exclude_locations",
            "function_ids",
            "exclude_function_ids",
            "job_titles",
            "exclude_job_titles",
            "seniority_level_ids",
            "exclude_seniority_level_ids",
            "schools",
        ]:
            raw_values = company_filters.get(key)
            if isinstance(raw_values, (list, tuple, set)):
                values = [str(entry).strip() for entry in raw_values if str(entry).strip()]
            else:
                text = str(raw_values or "").strip()
                values = [text] if text else []
            if values:
                normalized_filters[key] = list(dict.fromkeys(values))
        search_query = str(company_filters.get("search_query") or "").strip()
        if search_query:
            normalized_filters["search_query"] = search_query
        try:
            max_pages = max(1, int(item.get("max_pages") or 1))
        except (TypeError, ValueError):
            max_pages = 1
        try:
            page_limit = max(1, int(item.get("page_limit") or 25))
        except (TypeError, ValueError):
            page_limit = 25
        normalized.append(
            {
                "strategy_id": str(item.get("strategy_id") or "").strip(),
                "shard_id": shard_id,
                "title": str(item.get("title") or shard_id).strip() or shard_id,
                "scope_note": str(item.get("scope_note") or "").strip(),
                "max_pages": max_pages,
                "page_limit": page_limit,
                "company_filters": normalized_filters,
            }
        )
    return normalized


def _normalize_shard_id(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return cleaned or "shard"


def _roster_entry_key(entry: dict[str, Any]) -> str:
    linkedin_url = str(entry.get("linkedin_url") or entry.get("profile_url") or "").strip().lower()
    if linkedin_url:
        return linkedin_url
    member_key = str(entry.get("member_key") or entry.get("member_id") or "").strip().lower()
    if member_key:
        return member_key
    return "|".join(
        [
            str(entry.get("full_name") or "").strip().lower(),
            str(entry.get("headline") or "").strip().lower(),
            str(entry.get("location") or "").strip().lower(),
        ]
    )


def _company_identity_search_queries(target_company: str) -> list[str]:
    requested = " ".join(str(target_company or "").split()).strip()
    if not requested:
        return []
    queries = [
        f'site:linkedin.com/company "{requested}" LinkedIn',
        f'"{requested}" "LinkedIn" company',
    ]
    if "&" in requested:
        ampersand_expanded = " ".join(requested.replace("&", " and ").split()).strip()
        compact = re.sub(r"[^a-z0-9]+", "", ampersand_expanded.lower())
        if ampersand_expanded and ampersand_expanded.lower() != requested.lower():
            queries.append(f'site:linkedin.com/company "{ampersand_expanded}" LinkedIn')
        if compact and compact not in {re.sub(r"[^a-z0-9]+", "", requested.lower()), ""}:
            queries.append(f'site:linkedin.com/company "{compact}" LinkedIn')
    deduped: list[str] = []
    seen: set[str] = set()
    for item in queries:
        normalized = " ".join(item.split()).strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


def _company_identity_candidate_from_search_result(result: dict[str, Any]) -> dict[str, Any] | None:
    url = str(result.get("url") or "").strip()
    match = re.search(r"linkedin\.com/company/([^/?#]+)", url, flags=re.IGNORECASE)
    if not match:
        return None
    linkedin_slug = str(match.group(1) or "").strip()
    if not linkedin_slug:
        return None
    title = " ".join(str(result.get("title") or "").split()).strip()
    label = _clean_company_result_title(title)
    if not label:
        return None
    return {
        "label": label,
        "linkedin_slug": linkedin_slug,
        "linkedin_company_url": f"https://www.linkedin.com/company/{linkedin_slug}/",
        "domain_hint": _extract_domain_hint(str(result.get("snippet") or "").strip()),
    }


def _clean_company_result_title(title: str) -> str:
    candidate = str(title or "").strip()
    for token in [" | LinkedIn", " - LinkedIn", "| LinkedIn", "- LinkedIn"]:
        if token in candidate:
            candidate = candidate.split(token, 1)[0].strip()
            break
    candidate = re.sub(r"^\s*LinkedIn:\s*", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"^\s*Overview of\s+", "", candidate, flags=re.IGNORECASE)
    candidate = " ".join(candidate.split()).strip(" -|")
    if not candidate:
        return ""
    if re.search(r"\b(job|jobs|hiring|careers|employee|employees)\b", candidate, flags=re.IGNORECASE):
        return ""
    return candidate


def _extract_domain_hint(snippet: str) -> str:
    match = re.search(r"\b([a-z0-9][a-z0-9-]*(?:\.[a-z0-9-]+)+)\b", str(snippet or "").lower())
    if not match:
        return ""
    domain = str(match.group(1) or "").strip().rstrip(".,)")
    if domain.endswith("linkedin.com"):
        return ""
    return domain


def _build_investor_firm_plan(
    target_company: str, candidates: list[Candidate], job_request: JobRequest
) -> dict[str, Any]:
    intent_view = resolve_request_intent_view(job_request)
    grouped: dict[str, list[Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.organization or "Unknown Firm", []).append(candidate)
    firms: list[dict[str, Any]] = []
    for organization, members in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0].lower())):
        direct_count = sum(
            1 for member in members if _investment_involvement_label(member.investment_involvement) == "yes"
        )
        possible_count = sum(
            1 for member in members if _investment_involvement_label(member.investment_involvement) == "possible"
        )
        if direct_count > 0 or len(members) >= 8:
            tier = "tier_1"
        elif possible_count > 0 or len(members) >= 4:
            tier = "tier_2"
        else:
            tier = "tier_3"
        firms.append(
            {
                "organization": organization,
                "tier": tier,
                "member_count": len(members),
                "direct_investment_member_count": direct_count,
                "possible_investment_member_count": possible_count,
                "sample_members": [member.display_name for member in members[:5]],
                "recommended_channels": _recommended_investor_channels(tier),
            }
        )
    return {
        "target_company": target_company,
        "strategy_type": "investor_firm_roster",
        "target_population": (
            f"Full investor-firm roster for institutions connected to {target_company}, followed by later filtering "
            "for role, involvement, and other sourcing criteria."
        ),
        "keywords": list(intent_view.get("keywords") or []),
        "firms": firms,
        "tier_summary": {
            "tier_1": sum(1 for firm in firms if firm["tier"] == "tier_1"),
            "tier_2": sum(1 for firm in firms if firm["tier"] == "tier_2"),
            "tier_3": sum(1 for firm in firms if firm["tier"] == "tier_3"),
        },
    }


def _recommended_investor_channels(tier: str) -> list[str]:
    if tier == "tier_1":
        return ["internal_investor_asset", "linkedin_company_roster", "targeted_web_search"]
    if tier == "tier_2":
        return ["internal_investor_asset", "targeted_web_search", "linkedin_people_search"]
    return ["internal_investor_asset", "targeted_web_search"]


def _investment_involvement_label(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized.startswith("⭐ 是") or normalized.startswith("是"):
        return "yes"
    if normalized.startswith("可能"):
        return "possible"
    if normalized.startswith("否"):
        return "no"
    return "unknown"
