from __future__ import annotations

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from html import unescape
from pathlib import Path
from typing import Any, Callable
from urllib import error, parse, request

from .agent_runtime import AgentRuntimeCoordinator
from .asset_logger import AssetLogger
from .company_shard_planning import resolve_segmented_roster_completion
from .connectors import CompanyIdentity, RapidApiAccount, search_people_accounts
from .domain import Candidate, EvidenceRecord, JobRequest, format_display_name, make_evidence_id, normalize_name_token
from .durable_runtime import (
    LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
    DurableRuntimeWriter,
    legacy_job_operation_id,
    legacy_job_workflow_run_id,
    linkedin_discovery_query_run_idempotency_key,
)
from .harvest_connectors import (
    HarvestProfileSearchConnector,
    _harvest_profile_search_request_lane,
    harvest_connector_available,
)
from .model_provider import DeterministicModelClient, ModelClient
from .query_signal_knowledge import (
    canonicalize_scope_signal_label,
    canonicalize_thematic_signal_label,
    thematic_signal_search_query_aliases,
)
from .request_normalization import (
    build_effective_job_request,
    build_effective_request_payload,
    resolve_request_intent_view,
)
from .runtime_environment import (
    assert_live_provider_access_allowed,
    current_runtime_environment,
    external_provider_mode,
    infer_runtime_dir_from_path,
)
from .runtime_tuning import (
    apply_runtime_timing_overrides_to_search_state,
    resolve_runtime_timing_overrides,
    resolved_harvest_people_search_global_inflight,
    resolved_lane_fetch_cooldown_seconds,
    resolved_lane_ready_cooldown_seconds,
    resolved_provider_people_search_parallel_queries,
    runtime_inflight_slot,
)
from .search_provider import (
    BaseSearchProvider,
    DuckDuckGoHtmlSearchProvider,
    SearchResponse,
    parse_duckduckgo_html_results,
    search_response_from_record,
    search_response_to_record,
)
from .web_fetch import fetch_search_results_html
from .worker_daemon import AutonomousWorkerDaemon
from .workflow_event_response import (
    SEARCH_SEED_DISCOVERY_QUERY_ITEM_KIND,
    SEARCH_SEED_DISCOVERY_RECOVERY_KIND,
)

SearchSeedIncrementalResultCallback = Callable[[dict[str, Any]], None]

PROVIDER_SEARCH_RETRY_ITEM_KIND = "provider_search_retry"
PROVIDER_RETRY_TYPE_HARVEST_ZERO_RESULT = "harvest_people_search_zero_result_retry"

# WS1 Step 4b-B (ruling RATIFIED 2026-07-23, B-then-A): a scoped request's
# keyword shards execute as first-class shards of the paid people-search lane.
# The persisted plan file is the canonical expected-shard contract and the
# completion routes through resolve_segmented_roster_completion — the same
# honesty contract the segmented company-roster lane uses.
SCOPED_KEYWORD_UNION_SHARD_PLAN_FILENAME = "scoped_keyword_union_shard_plan.json"
SCOPED_KEYWORD_UNION_COMPLETED_STOP_REASON = "completed_keyword_union"
SCOPED_KEYWORD_UNION_PARTIAL_STOP_REASON = "partial_keyword_union"
# Dispatch statuses that keep a keyword shard in the expected-coverage set.
# Suppressed shards (generic-only text or company-identity echoes) are
# recorded in the plan for audit but are structurally unqueryable, so they
# never hold completion hostage.
SCOPED_KEYWORD_UNION_EXPECTED_DISPATCH_STATUSES = frozenset(
    {"dispatched", "merged_duplicate_query", "not_dispatched_query_budget"}
)
# Provider-lane query summary states meaning this query's coverage is NOT
# exhaustively fetched yet (retry pending, provider-side incomplete, degraded
# page coverage, or still queued).
_SCOPED_KEYWORD_UNION_TRUNCATED_QUERY_STATUSES = frozenset(
    {"retry_wait", "incomplete", "degraded", "queued", "skipped_degraded"}
)


def provider_query_summary_is_truncated(summary: dict[str, Any]) -> bool:
    """True when one paid people-search query summary shows non-exhaustive coverage."""

    payload = dict(summary or {})
    status = str(payload.get("status") or "").strip().lower()
    if status in _SCOPED_KEYWORD_UNION_TRUNCATED_QUERY_STATUSES:
        return True
    return bool(
        payload.get("provider_search_incomplete")
        or payload.get("provider_search_retryable")
        or payload.get("provider_search_degraded")
    )


def resolve_scoped_keyword_union_completion(
    *,
    dispatch: dict[str, Any],
    query_summaries: list[dict[str, Any]],
) -> dict[str, Any]:
    """Honest completion for the scoped keyword-union seed roster (Step 4b-B).

    Routes through ``resolve_segmented_roster_completion``: every expected
    keyword shard must be present and untruncated or the union stays
    ``partial`` — a query cut by the provider query budget, an unrun provider
    lane, or a retry-pending/degraded query is never reported as complete
    coverage.  Overlap-pruned queries stay covered: their summaries carry
    ``skipped_high_overlap`` (a coverage decision with probe evidence), which
    is not a truncated state.
    """

    summaries_by_query: dict[str, dict[str, Any]] = {}
    for item in list(query_summaries or []):
        if not isinstance(item, dict):
            continue
        query_text = str(item.get("query") or "").strip()
        if query_text and query_text not in summaries_by_query:
            summaries_by_query[query_text] = dict(item)
    shard_summaries: list[dict[str, Any]] = []
    for shard in list(dispatch.get("shards") or []):
        record = dict(shard or {})
        shard_id = str(record.get("shard_id") or "").strip()
        if not shard_id or str(record.get("dispatch_status") or "") not in SCOPED_KEYWORD_UNION_EXPECTED_DISPATCH_STATUSES:
            continue
        summary = summaries_by_query.get(str(record.get("provider_query") or "").strip())
        if summary is None:
            # Missing shard: the resolver reports it in missing_shard_ids.
            continue
        shard_summaries.append(
            {
                "shard_id": shard_id,
                "partial_result": provider_query_summary_is_truncated(summary),
                "query_status": str(summary.get("status") or ""),
                "seed_entry_count": int(summary.get("seed_entry_count") or 0),
            }
        )
    return resolve_segmented_roster_completion(
        expected_shard_ids=[str(item) for item in list(dispatch.get("expected_shard_ids") or [])],
        shard_summaries=shard_summaries,
        completed_stop_reason=SCOPED_KEYWORD_UNION_COMPLETED_STOP_REASON,
        partial_stop_reason=SCOPED_KEYWORD_UNION_PARTIAL_STOP_REASON,
    )


def _provider_people_search_max_query_count(cost_policy: dict[str, Any] | None) -> int:
    try:
        max_query_count = int(dict(cost_policy or {}).get("provider_people_search_max_queries") or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, max_query_count)


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _external_provider_mode() -> str:
    return external_provider_mode()


def _lane_ready_poll_min_interval_seconds() -> int:
    return max(
        0,
        _env_int(
            "SEED_DISCOVERY_READY_POLL_MIN_INTERVAL_SECONDS",
            _env_int("WEB_SEARCH_READY_POLL_MIN_INTERVAL_SECONDS", 15),
        ),
    )


def _lane_fetch_min_interval_seconds() -> int:
    return max(
        0,
        _env_int(
            "SEED_DISCOVERY_FETCH_MIN_INTERVAL_SECONDS",
            _env_int("WEB_SEARCH_FETCH_MIN_INTERVAL_SECONDS", 15),
        ),
    )


def _provider_people_search_zero_result_retry_attempts(cost_policy: dict[str, Any] | None) -> int:
    raw_value = dict(cost_policy or {}).get("provider_people_search_zero_result_retry_attempts")
    if raw_value is None:
        raw_value = os.getenv("HARVEST_PROFILE_SEARCH_ZERO_RESULT_RETRY_ATTEMPTS", "2")
    try:
        return max(0, min(int(str(raw_value).strip() or "0"), 5))
    except (TypeError, ValueError):
        return 2


def _provider_people_search_zero_result_retry_backoff_seconds(cost_policy: dict[str, Any] | None) -> float:
    raw_value = dict(cost_policy or {}).get("provider_people_search_zero_result_retry_backoff_seconds")
    if raw_value is None:
        raw_value = os.getenv("HARVEST_PROFILE_SEARCH_ZERO_RESULT_RETRY_BACKOFF_SECONDS", "0")
    try:
        return max(0.0, min(float(str(raw_value).strip() or "0"), 30.0))
    except (TypeError, ValueError):
        return 0.0


def _runtime_timing_overrides_from_request_payload(request_payload: dict[str, Any] | None) -> dict[str, Any]:
    if not request_payload:
        return {}
    execution_preferences = dict(JobRequest.from_payload(dict(request_payload or {})).execution_preferences or {})
    return resolve_runtime_timing_overrides(execution_preferences)


@dataclass(slots=True)
class SearchSeedSnapshot:
    snapshot_id: str
    target_company: str
    company_identity: CompanyIdentity
    snapshot_dir: Path
    entries: list[dict[str, Any]]
    query_summaries: list[dict[str, Any]]
    accounts_used: list[str]
    errors: list[str]
    stop_reason: str
    summary_path: Path
    entries_path: Path | None = None
    summary_payload: dict[str, Any] = field(default_factory=dict)
    lane_payloads: dict[str, dict[str, Any]] = field(default_factory=dict)
    lane_entries: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        lane_summary_paths = {
            str(lane).strip(): str(dict(payload or {}).get("summary_path") or "")
            for lane, payload in dict(self.lane_payloads or {}).items()
            if str(lane).strip()
        }
        return {
            "snapshot_id": self.snapshot_id,
            "target_company": self.target_company,
            "company_identity": self.company_identity.to_record(),
            "snapshot_dir": str(self.snapshot_dir),
            "entry_count": len(self.entries),
            "query_count": len(self.query_summaries),
            "accounts_used": self.accounts_used,
            "errors": self.errors,
            "stop_reason": self.stop_reason,
            "summary_path": str(self.summary_path),
            "entries_path": str(self.entries_path) if isinstance(self.entries_path, Path) else "",
            "lane_keys": sorted({*lane_summary_paths.keys(), *[str(key).strip() for key in self.lane_entries.keys()]}),
            "lane_summary_paths": lane_summary_paths,
            # WS1 Step 4b-B: the scoped keyword-union shard contract (persisted
            # plan path + honest completion) rides on the execution payload.
            **(
                {"scoped_keyword_union": dict(dict(self.summary_payload or {}).get("scoped_keyword_union") or {})}
                if dict(self.summary_payload or {}).get("scoped_keyword_union")
                else {}
            ),
        }


def normalize_search_seed_employment_scope(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "former":
        return "former"
    if normalized == "current":
        return "current"
    return "all"


def _copy_search_seed_filter_hints(payload: dict[str, Any] | None) -> dict[str, Any]:
    copied: dict[str, Any] = {}
    for key, value in dict(payload or {}).items():
        if isinstance(value, list):
            copied[str(key)] = [item for item in value]
        else:
            copied[str(key)] = value
    return copied


def _annotate_search_seed_query_summaries(
    query_summaries: list[dict[str, Any]],
    *,
    employment_status: str,
    filter_hints: dict[str, Any] | None,
    strategy_type: str,
) -> list[dict[str, Any]]:
    employment_scope = normalize_search_seed_employment_scope(employment_status)
    copied_filter_hints = _copy_search_seed_filter_hints(filter_hints)
    annotated: list[dict[str, Any]] = []
    for item in list(query_summaries or []):
        if not isinstance(item, dict):
            continue
        annotated.append(
            {
                **dict(item),
                "lane": str(item.get("lane") or "profile_search"),
                "employment_scope": str(item.get("employment_scope") or employment_scope),
                "employment_status": str(item.get("employment_status") or employment_scope),
                "strategy_type": str(item.get("strategy_type") or strategy_type),
                "filter_hints": _copy_search_seed_filter_hints(dict(item.get("filter_hints") or copied_filter_hints)),
            }
        )
    return annotated


def _count_incomplete_provider_query_summaries(query_summaries: list[dict[str, Any]]) -> int:
    count = 0
    for item in list(query_summaries or []):
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "").strip().lower()
        if (
            status in {"incomplete", "retry_wait"}
            or bool(item.get("provider_search_incomplete"))
            or bool(item.get("provider_search_retryable"))
        ):
            count += 1
    return count


def _safe_nonnegative_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return max(0, int(default or 0))


def _utc_timestamp_after_seconds(seconds: int | float) -> str:
    try:
        delay_seconds = max(1, int(float(seconds or 0)))
    except (TypeError, ValueError):
        delay_seconds = 30
    return (
        datetime.now(timezone.utc).replace(microsecond=0)
        + timedelta(seconds=delay_seconds)
    ).strftime("%Y-%m-%d %H:%M:%S")


def _provider_retry_item_key(payload: dict[str, Any]) -> str:
    signature_payload = {
        "provider_retry_type": str(payload.get("provider_retry_type") or ""),
        "provider": str(payload.get("provider") or ""),
        "target_company": str(payload.get("target_company") or ""),
        "snapshot_id": str(payload.get("snapshot_id") or ""),
        "employment_status": str(payload.get("employment_status") or ""),
        "query": str(payload.get("query") or ""),
        "effective_query_text": str(payload.get("effective_query_text") or ""),
        "incomplete_reason": str(payload.get("incomplete_reason") or ""),
    }
    return sha1(json.dumps(signature_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:24]


def _dedupe_provider_retry_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in list(items or []):
        if not isinstance(item, dict):
            continue
        key = str(item.get("item_key") or _provider_retry_item_key(item)).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append({**dict(item), "item_key": key})
    return deduped


def _search_seed_discovery_query_item_id(
    *,
    job_id: str,
    snapshot_id: str,
    query_spec: dict[str, Any],
    index: int,
    employment_status: str,
) -> str:
    payload = {
        "job_id": str(job_id or "").strip(),
        "snapshot_id": str(snapshot_id or "").strip(),
        "bundle_id": str(query_spec.get("bundle_id") or "").strip(),
        "index": int(index or 0),
        "query": str(query_spec.get("query") or "").strip(),
        "execution_mode": str(query_spec.get("execution_mode") or "").strip(),
        "employment_status": normalize_search_seed_employment_scope(employment_status),
    }
    return "jdisc_" + sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:24]


def _record_search_seed_discovery_query_item(
    *,
    worker_runtime: AgentRuntimeCoordinator | None,
    item_id: str,
    job_id: str,
    identity: CompanyIdentity,
    snapshot_id: str,
    index: int,
    query_spec: dict[str, Any],
    employment_status: str,
    status: str,
    phase: str,
    reason: str,
    worker_id: int = 0,
    max_attempts: int = 5,
    not_before_at: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    store = getattr(worker_runtime, "store", None)
    if store is None:
        return {}
    query_text = str(query_spec.get("query") or "").strip()
    source_worker_ids = [int(worker_id)] if int(worker_id or 0) > 0 else []
    metadata_payload = dict(metadata or {})
    if isinstance(metadata_payload.get("summary"), dict) and not isinstance(
        metadata_payload.get("query_summary"),
        dict,
    ):
        # `query_summary` is the canonical diagnostics contract. Keep `summary`
        # for artifact/backward compatibility, but do not make service metrics
        # depend on that older provider-local name.
        metadata_payload["query_summary"] = dict(metadata_payload["summary"])
    normalized_job_id = str(job_id or "").strip()
    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_status = str(status or "queued").strip().lower() or "queued"
    normalized_phase = str(phase or status or "queued").strip().lower() or "queued"
    target_company = str(identity.canonical_name or identity.requested_name or "").strip()
    materialization_metadata = {
        "schema_version": 1,
        "queue_contract": "search_seed_discovery_query",
        "query": query_text,
        "effective_query_text": query_text,
        "query_signature": _search_query_signature(query_text),
        "bundle_id": str(query_spec.get("bundle_id") or "").strip(),
        "source_family": str(query_spec.get("source_family") or "").strip(),
        "execution_mode": str(query_spec.get("execution_mode") or "").strip(),
        "employment_status": normalize_search_seed_employment_scope(employment_status),
        "index": int(index or 0),
        "worker_id": int(worker_id or 0),
        "command_payload_storage": "workflow_commands",
        "write_owner": "linkedin_acquisition_owner",
        **metadata_payload,
    }
    synthetic_row = {
        "item_id": item_id,
        "job_id": normalized_job_id,
        "target_company": target_company,
        "snapshot_id": normalized_snapshot_id,
        "item_kind": SEARCH_SEED_DISCOVERY_QUERY_ITEM_KIND,
        "source": "search_seed_discovery",
        "reason": str(reason or "search_seed_discovery_query").strip(),
        "status": normalized_status,
        "phase": normalized_phase,
        "priority": -30,
        "source_worker_ids": source_worker_ids,
        "idempotency_key": item_id,
        "max_attempts": max(1, int(max_attempts or 5)),
        "not_before_at": str(not_before_at or "").strip(),
        "metadata": materialization_metadata,
    }
    if not normalized_job_id or not normalized_snapshot_id or not item_id:
        return synthetic_row
    runtime_writer = DurableRuntimeWriter(store)
    workflow_run_id = legacy_job_workflow_run_id(normalized_job_id)
    operation_id = legacy_job_operation_id(normalized_job_id)
    terminal_statuses = {"completed", "exhausted", "failed", "skipped", "interrupted"}
    if normalized_status in {"failed_retryable"} or normalized_phase == "retry_wait":
        command_idempotency_key = linkedin_discovery_query_run_idempotency_key(
            job_id=normalized_job_id,
            snapshot_id=normalized_snapshot_id,
            item_id=item_id,
            query=query_text,
            employment_status=normalize_search_seed_employment_scope(employment_status),
            run_scope="search_seed_discovery_retry",
        )
        if command_idempotency_key:
            try:
                runtime_writer.append_event_and_reduce(
                    workflow_run_id=workflow_run_id,
                    operation_id=operation_id,
                    event_family="workflow_event",
                    event_type="WorkflowStarted",
                    idempotency_key=f"{workflow_run_id}:search_seed_discovery_started",
                    actor="search_seed_discovery_planner",
                    source="search_seed_discovery",
                    payload={
                        "workflow_type": "linkedin_acquisition",
                        "stage_key": "search_seed_discovery",
                        "job_id": normalized_job_id,
                        "snapshot_id": normalized_snapshot_id,
                        "migration_phase": "W6_search_seed_discovery_query_run",
                    },
                )
                apply_result = runtime_writer.append_event_and_reduce(
                    workflow_run_id=workflow_run_id,
                    operation_id=operation_id,
                    event_family="workflow_event",
                    event_type="CommandPlanRequested",
                    idempotency_key=f"{command_idempotency_key}:plan",
                    actor="search_seed_discovery_planner",
                    source="search_seed_discovery",
                    payload={
                        "workflow_type": "linkedin_acquisition",
                        "stage_key": "search_seed_discovery",
                        "command_type": LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
                        "idempotency_key": command_idempotency_key,
                        "payload": {
                            "job_id": normalized_job_id,
                            "target_company": target_company,
                            "snapshot_id": normalized_snapshot_id,
                            "item_id": item_id,
                            "item_kind": SEARCH_SEED_DISCOVERY_QUERY_ITEM_KIND,
                            "source": "search_seed_discovery",
                            "reason": str(reason or "search_seed_discovery_query").strip(),
                            "query": query_text,
                            "employment_status": normalize_search_seed_employment_scope(employment_status),
                            "source_worker_ids": source_worker_ids,
                            "materialization_metadata": materialization_metadata,
                            "migration_phase": "W6_search_seed_discovery_query_run",
                        },
                        "not_before_at": str(not_before_at or "").strip(),
                        "max_attempts": max(1, int(max_attempts or 5)),
                        "retry_policy": {
                            "kind": "search_seed_discovery_query_run",
                            "retry_delay_seconds": 30,
                        },
                    },
                )
                for command in list(getattr(apply_result, "commands", ()) or ()):
                    command_payload = dict(command or {})
                    if str(command_payload.get("idempotency_key") or "") == command_idempotency_key:
                        synthetic_row["workflow_command"] = command_payload
                        break
            except Exception:
                pass
        return synthetic_row
    event_type = "CompletionProofRecorded" if normalized_status in terminal_statuses else "DiscoveryQueryStateRecorded"
    payload = {
        "workflow_type": "linkedin_acquisition",
        "stage_key": "search_seed_discovery",
        "proof_key": (
            f"stage1_lane:scoped_search:{normalized_snapshot_id}:{item_id}"
            if event_type == "CompletionProofRecorded"
            else ""
        ),
        "status": normalized_status,
        "phase": normalized_phase,
        "job_id": normalized_job_id,
        "snapshot_id": normalized_snapshot_id,
        "item_id": item_id,
        "item_kind": SEARCH_SEED_DISCOVERY_QUERY_ITEM_KIND,
        "source_worker_ids": source_worker_ids,
        "materialization_metadata": materialization_metadata,
        "migration_phase": "W6_search_seed_discovery_query_run",
    }
    try:
        runtime_writer.append_event_and_reduce(
            workflow_run_id=workflow_run_id,
            operation_id=operation_id,
            event_family="workflow_event",
            event_type=event_type,
            idempotency_key=f"{normalized_job_id}:{item_id}:{normalized_status}:{normalized_phase}",
            actor="search_seed_discovery",
            source="search_seed_discovery",
            payload=payload,
        )
    except Exception:
        pass
    return synthetic_row


def _provider_retry_items_for_query_summary(
    summary: dict[str, Any],
    *,
    target_company: str,
    snapshot_id: str,
) -> list[dict[str, Any]]:
    query_summary = dict(summary or {})
    zero_retry = dict(query_summary.get("zero_result_retry") or {})
    result_retry = dict(zero_retry.get("result") or zero_retry)
    if bool(query_summary.get("zero_result_accepted")):
        return []
    incomplete_reason = str(query_summary.get("incomplete_reason") or "").strip()
    provider_search_incomplete = bool(query_summary.get("provider_search_incomplete")) or (
        str(query_summary.get("status") or "").strip().lower() == "incomplete"
    )
    exhausted = bool(result_retry.get("exhausted")) or (
        provider_search_incomplete and incomplete_reason == "provider_zero_results_after_retry"
    )
    if not exhausted:
        return []
    owner_item_id = str(
        query_summary.get("discovery_query_item_id")
        or query_summary.get("search_seed_discovery_query_item_id")
        or ""
    ).strip()
    retry_count = _safe_nonnegative_int(result_retry.get("retry_count"))
    configured_retry_attempts = _safe_nonnegative_int(result_retry.get("attempts"), retry_count)
    provider_attempt_count = max(1, retry_count + 1)
    employment_status = normalize_search_seed_employment_scope(
        query_summary.get("employment_status") or query_summary.get("employment_scope")
    )
    item = {
        "kind": "provider_retry_item",
        "item_kind": PROVIDER_SEARCH_RETRY_ITEM_KIND,
        "provider_retry_type": PROVIDER_RETRY_TYPE_HARVEST_ZERO_RESULT,
        "provider": "harvest_profile_search",
        "scope": "search_seed_discovery",
        "owner": "search_seed_discovery_query" if owner_item_id else "query_summary",
        "owner_item_id": owner_item_id,
        "search_seed_discovery_query_item_id": owner_item_id,
        "status": "exhausted",
        "phase": "terminal",
        "queue_status": "failed",
        "target_company": str(target_company or "").strip(),
        "snapshot_id": str(snapshot_id or "").strip(),
        "query": str(query_summary.get("query") or "").strip(),
        "effective_query_text": str(query_summary.get("effective_query_text") or "").strip(),
        "query_signature": _search_query_signature(
            str(query_summary.get("effective_query_text") or query_summary.get("query") or "")
        ),
        "employment_status": employment_status,
        "employment_scope": employment_status,
        "strategy_type": str(query_summary.get("strategy_type") or "").strip(),
        "filter_hints": _copy_search_seed_filter_hints(dict(query_summary.get("filter_hints") or {})),
        "raw_path": str(query_summary.get("raw_path") or "").strip(),
        "incomplete_reason": incomplete_reason or "provider_zero_results_after_retry",
        "query_summary_status": str(query_summary.get("status") or "").strip(),
        "configured_retry_attempts": configured_retry_attempts,
        "retry_count": retry_count,
        "provider_attempt_count": provider_attempt_count,
        "max_attempts": max(provider_attempt_count, configured_retry_attempts + 1),
        "exhausted": True,
        "zero_result_retry": zero_retry,
    }
    item["item_key"] = _provider_retry_item_key(item)
    return [item]


def _attach_provider_retry_items_to_query_summaries(
    query_summaries: list[dict[str, Any]],
    *,
    target_company: str,
    snapshot_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    annotated: list[dict[str, Any]] = []
    provider_retry_items: list[dict[str, Any]] = []
    for summary in list(query_summaries or []):
        if not isinstance(summary, dict):
            continue
        copied = dict(summary)
        query_retry_items = _provider_retry_items_for_query_summary(
            copied,
            target_company=target_company,
            snapshot_id=snapshot_id,
        )
        if query_retry_items:
            copied["provider_retry_items"] = query_retry_items
            provider_retry_items.extend(query_retry_items)
        annotated.append(copied)
    return annotated, _dedupe_provider_retry_items(provider_retry_items)


def collect_search_seed_provider_retry_items(snapshot: SearchSeedSnapshot | None) -> list[dict[str, Any]]:
    if not isinstance(snapshot, SearchSeedSnapshot):
        return []
    items: list[dict[str, Any]] = []
    summary_payload = dict(snapshot.summary_payload or {})
    items.extend([dict(item) for item in list(summary_payload.get("provider_retry_items") or []) if isinstance(item, dict)])
    for lane_payload in dict(snapshot.lane_payloads or {}).values():
        lane_payload = dict(lane_payload or {})
        items.extend([dict(item) for item in list(lane_payload.get("provider_retry_items") or []) if isinstance(item, dict)])
        for query_summary in list(lane_payload.get("query_summaries") or []):
            if isinstance(query_summary, dict):
                items.extend(
                    [
                        dict(item)
                        for item in list(query_summary.get("provider_retry_items") or [])
                        if isinstance(item, dict)
                    ]
                )
    for query_summary in list(snapshot.query_summaries or []):
        if isinstance(query_summary, dict):
            items.extend(
                [
                    dict(item)
                    for item in list(query_summary.get("provider_retry_items") or [])
                    if isinstance(item, dict)
                ]
            )
    return _dedupe_provider_retry_items(items)


class SearchSeedAcquirer:
    def __init__(
        self,
        accounts: list[RapidApiAccount],
        model_client: ModelClient | None = None,
        harvest_search_connector: HarvestProfileSearchConnector | None = None,
        search_provider: BaseSearchProvider | None = None,
    ) -> None:
        self.accounts = search_people_accounts(accounts)
        self._exhausted_account_ids: set[str] = set()
        self.model_client = model_client or DeterministicModelClient()
        self.harvest_search_connector = harvest_search_connector
        self.search_provider = search_provider or DuckDuckGoHtmlSearchProvider()

    def _provider_runtime_dir(self, runtime_dir: str | Path | None) -> Path | None:
        if runtime_dir is None or not str(runtime_dir).strip():
            return None
        return infer_runtime_dir_from_path(runtime_dir) or Path(runtime_dir).expanduser()

    def _rapidapi_people_search_enabled(
        self,
        *,
        runtime_dir: str | Path | None = None,
        provider_mode: str | None = None,
        runtime_environment: str | None = None,
    ) -> bool:
        if not self.accounts:
            return False
        scoped_runtime_dir = self._provider_runtime_dir(runtime_dir)
        if scoped_runtime_dir is None and provider_mode is None and runtime_environment is None:
            return _external_provider_mode() == "live"
        env = current_runtime_environment(
            runtime_dir=scoped_runtime_dir,
            provider_mode=provider_mode,
            runtime_environment=runtime_environment,
        )
        return env.provider_mode == "live"

    def _harvest_people_search_enabled(self) -> bool:
        return bool(
            self.harvest_search_connector
            and harvest_connector_available(self.harvest_search_connector.settings)
        )

    @staticmethod
    def scoped_seed_pool_admission_block(
        *,
        strategy_type: str,
        employment_status: str,
        policy: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Fail-closed admission gate for the scoped keyword seed-pool lane.

        WS1 Step 4c (pgLegacy deletion, 2026-07-23): every live scoped plan
        mints the keyword-union shard policy at plan time (Step 4a — a scoped
        strategy always compiles at least one seed query, so the mint is
        unconditional), and the live-schema carrier census was zero at
        deletion time (all jobs/workflows terminal, zero pending review
        sessions; the queued commands were post-acquisition projections
        only).  A scoped CURRENT-member seed-pool task without the
        planner-minted policy is a legacy/malformed carrier and fails closed:
        proceeding would run the paid people-search lane without the
        persisted shard plan / honest completion contract (the retired
        pre-4b-B silent pass-through), and plain deletion would have widened
        into exactly that ungoverned dispatch.  The former companion pass
        keeps its own former shard-plan contract (employment_status
        ``former`` is exempt); the explicit-cohort lane never reaches this
        gate.

        Returns ``{}`` to admit the task, or blocked ``AcquisitionExecution``
        kwargs (status/detail/payload) mirroring the 9544b69 roster-policy
        fail-closed precedent.
        """

        if (
            str(strategy_type or "").strip() != "scoped_search_roster"
            or str(employment_status or "").strip().lower() == "former"
            or dict(policy or {})
        ):
            return {}
        return {
            "status": "blocked",
            "detail": (
                "Scoped search-seed acquisition requires the planner-minted "
                "scoped_keyword_union_shard_policy; policy-less legacy tasks are "
                "retired (WS1 Step 4c pgLegacy deletion, 2026-07-23)."
            ),
            "payload": {
                "reason": "scoped_keyword_union_shard_policy_missing",
                "strategy_type": "scoped_search_roster",
            },
        }

    def _deduped_provider_people_search_query_texts(
        self,
        *,
        identity: CompanyIdentity,
        filter_hints: dict[str, list[str]],
        queries: list[str],
        max_query_count: int,
    ) -> list[str]:
        """Signature-level dedupe + query budget for the paid people-search lane.

        Single source for BOTH the live dispatch loop
        (``_provider_people_search_fallback``) and the scoped keyword-union
        shard plan (WS1 Step 4b-B): the persisted plan must pre-register
        exactly the query list the dispatch loop will run.
        """

        deduped: list[str] = []
        seen: set[str] = set()
        harvest_enabled = self._harvest_people_search_enabled()
        for item in queries:
            key = str(item or "")
            if harvest_enabled:
                effective_key = _normalize_harvest_query_text(
                    query_text=key,
                    filter_hints=filter_hints,
                    identity=identity,
                )
                signature = _search_query_signature(effective_key) or "__empty__"
            else:
                signature = _search_query_signature(key) or "__empty__"
            if signature in seen:
                continue
            seen.add(signature)
            deduped.append(key)
        if max_query_count > 0:
            deduped = deduped[:max_query_count]
        return deduped

    def resolve_scoped_keyword_union_shard_dispatch(
        self,
        *,
        policy: dict[str, Any],
        identity: CompanyIdentity,
        filter_hints: dict[str, list[str]],
        search_seed_queries: list[str],
        cost_policy: dict[str, Any],
    ) -> dict[str, Any]:
        """Map planner-minted keyword shards onto the paid people-search dispatch.

        WS1 Step 4b-B: shard ids come verbatim from the planner's
        ``scoped_keyword_union_shard_policy`` rules (the planner is the single
        writer — ids are never re-normalized here).  The provider query list is
        resolved through the SAME pipeline the dispatch loop uses
        (``_resolve_provider_people_search_queries`` + signature dedupe +
        query budget), so per-keyword provider payloads stay byte-compatible
        with the pre-4b seed-pool path and the mapping cannot drift from the
        real dispatch.  Shards whose query survives resolution are
        ``dispatched`` (or ``merged_duplicate_query`` when two keywords
        canonicalize into one query family); shards cut by the query budget
        stay expected (``not_dispatched_query_budget``) so completion reports
        them as missing coverage instead of silently dropping the keyword;
        structurally unqueryable shards (generic-only text, company-identity
        echoes) are recorded as suppressed for audit and excluded from the
        expected set.
        """

        paid_queries = _resolve_provider_people_search_queries(
            identity=identity,
            filter_hints=filter_hints,
            search_seed_queries=list(search_seed_queries or []),
        )
        dispatched_query_texts = self._deduped_provider_people_search_query_texts(
            identity=identity,
            filter_hints=filter_hints,
            queries=paid_queries,
            max_query_count=_provider_people_search_max_query_count(cost_policy),
        )
        query_by_family: dict[str, str] = {}
        for query_text in dispatched_query_texts:
            family = _provider_query_family_key(query_text)
            if family and family not in query_by_family:
                query_by_family[family] = query_text
        shards: list[dict[str, Any]] = []
        claimed_queries: set[str] = set()
        for rule in list(policy.get("keyword_shards") or []):
            record = dict(rule or {})
            shard_id = str(record.get("rule_id") or "").strip()
            include_patch = dict(record.get("include_patch") or {})
            keywords = [
                str(item or "").strip()
                for item in list(include_patch.get("keywords") or [])
                if str(item or "").strip()
            ]
            keyword = keywords[0] if keywords else str(record.get("title") or "").strip()
            if not shard_id or not keyword:
                continue
            cleaned = _clean_provider_query_text(keyword)
            normalized = _clean_provider_query_text(
                _normalize_harvest_query_text(
                    query_text=keyword,
                    filter_hints=filter_hints,
                    identity=identity,
                )
            )
            provider_query = ""
            for candidate in (cleaned, normalized):
                family = _provider_query_family_key(candidate) if candidate else ""
                if family and family in query_by_family:
                    provider_query = query_by_family[family]
                    break
            if provider_query:
                dispatch_status = "merged_duplicate_query" if provider_query in claimed_queries else "dispatched"
                claimed_queries.add(provider_query)
            elif not cleaned and not normalized:
                dispatch_status = "suppressed_generic_terms"
            elif _provider_query_matches_company_identity(
                cleaned or keyword, identity=identity
            ) or _provider_query_matches_company_identity(normalized or keyword, identity=identity):
                dispatch_status = "suppressed_company_identity"
            else:
                dispatch_status = "not_dispatched_query_budget"
            shards.append(
                {
                    "shard_id": shard_id,
                    "keyword": keyword,
                    "provider_query": provider_query,
                    "dispatch_status": dispatch_status,
                }
            )
        expected_shard_ids = [
            str(item.get("shard_id") or "")
            for item in shards
            if str(item.get("dispatch_status") or "") in SCOPED_KEYWORD_UNION_EXPECTED_DISPATCH_STATUSES
        ]
        return {
            "strategy_id": str(policy.get("strategy_id") or ""),
            "mode": str(policy.get("mode") or ""),
            "shards": shards,
            "expected_shard_ids": expected_shard_ids,
            "unsharded_query_texts": [item for item in dispatched_query_texts if item not in claimed_queries],
        }

    def refresh_background_search_workers(self, workers: list[dict[str, Any]]) -> dict[str, Any]:
        grouped_specs: dict[Path, list[dict[str, Any]]] = {}
        discovery_dirs: dict[Path, Path] = {}
        errors: list[str] = []
        worker_updates: dict[int, dict[str, Any]] = {}

        for worker in workers:
            lane_id = str(worker.get("lane_id") or "").strip()
            if lane_id not in {"search_planner", "public_media_specialist"}:
                continue
            metadata = dict(worker.get("metadata") or {})
            input_payload = dict(worker.get("input") or {})
            checkpoint = dict(worker.get("checkpoint") or {})
            worker_key = str(worker.get("worker_key") or "").strip()
            if not worker_key:
                continue

            discovery_dir_text = str(metadata.get("discovery_dir") or "").strip()
            snapshot_dir_text = str(metadata.get("snapshot_dir") or "").strip()
            if discovery_dir_text:
                discovery_dir = Path(discovery_dir_text).expanduser()
            elif snapshot_dir_text:
                discovery_dir = Path(snapshot_dir_text).expanduser() / "search_seed_discovery"
            else:
                continue
            snapshot_dir = (
                Path(snapshot_dir_text).expanduser()
                if snapshot_dir_text
                else discovery_dir.parent
            )
            if not discovery_dir.exists():
                continue

            grouped_specs.setdefault(discovery_dir, []).append(
                {
                    "index": int(metadata.get("index") or input_payload.get("index") or 0),
                    "query_spec": dict(input_payload.get("query_spec") or {}),
                    "lane_id": lane_id,
                    "worker_key": worker_key,
                    "prefetched_search_state": dict(checkpoint.get("search_state") or {}),
                    "prefetched_search_artifact_paths": dict(checkpoint.get("search_artifact_paths") or {}),
                    "prefetched_search_raw_path": str(checkpoint.get("raw_path") or ""),
                    "prefetched_search_manifest_path": str(checkpoint.get("search_manifest_path") or ""),
                    "prefetched_search_manifest_key": str(checkpoint.get("search_manifest_key") or worker_key),
                    "runtime_timing_overrides": dict(metadata.get("runtime_timing_overrides") or {})
                    or _runtime_timing_overrides_from_request_payload(dict(metadata.get("request_payload") or {})),
                    "worker_id": int(worker.get("worker_id") or 0),
                }
            )
            discovery_dirs[discovery_dir] = snapshot_dir

        for discovery_dir, pending_specs in grouped_specs.items():
            logger = AssetLogger(discovery_dirs[discovery_dir])
            errors.extend(
                _prepare_batched_search_seed_queries(
                    search_provider=self.search_provider,
                    logger=logger,
                    discovery_dir=discovery_dir,
                    pending_specs=pending_specs,
                    result_limit=10,
                )
            )
            manifest_path = discovery_dir / "web_search_batch_manifest.json"
            manifest_entries = _load_search_batch_manifest_entries(manifest_path)
            for spec in pending_specs:
                task_key = str(spec.get("worker_key") or "").strip()
                entry = dict(manifest_entries.get(task_key) or {})
                if not entry:
                    continue
                worker_id = int(spec.get("worker_id") or 0)
                if worker_id <= 0:
                    continue
                worker_updates[worker_id] = {
                    "search_state": dict(entry.get("search_state") or {}),
                    "search_artifact_paths": {
                        str(key): str(value)
                        for key, value in dict(entry.get("artifact_paths") or {}).items()
                        if str(key).strip() and str(value).strip()
                    },
                    "raw_path": str(entry.get("raw_path") or ""),
                    "search_manifest_path": str(manifest_path),
                    "search_manifest_key": task_key,
                }

        return {
            "errors": errors,
            "worker_updates": worker_updates,
        }

    def discover(
        self,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        *,
        asset_logger: AssetLogger | None = None,
        search_seed_queries: list[str],
        query_bundles: list[dict[str, Any]] | None = None,
        filter_hints: dict[str, list[str]],
        cost_policy: dict[str, Any],
        employment_status: str,
        worker_runtime: AgentRuntimeCoordinator | None = None,
        job_id: str = "",
        request_payload: dict[str, Any] | None = None,
        plan_payload: dict[str, Any] | None = None,
        runtime_mode: str = "workflow",
        intent_view: dict[str, Any] | None = None,
        delta_execution_plan: dict[str, Any] | None = None,
        lane_context: dict[str, Any] | None = None,
        scoped_keyword_union_shard_policy: dict[str, Any] | None = None,
        on_incremental_query_result: SearchSeedIncrementalResultCallback | None = None,
    ) -> SearchSeedSnapshot:
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        resolved_intent_view = dict(
            intent_view
            or resolve_request_intent_view(request_payload or {})
            or {}
        )
        effective_request, _ = build_effective_job_request(
            request_payload or {"target_company": identity.canonical_name},
            intent_view=resolved_intent_view,
        )
        effective_request_payload = effective_request.to_record()
        resolved_filter_hints = _resolve_discovery_filter_hints(
            filter_hints=filter_hints,
            intent_view=resolved_intent_view,
        )
        effective_filter_hints = _normalize_harvest_company_filters(identity, resolved_filter_hints)
        resolved_search_seed_queries = _resolve_discovery_search_seed_queries(
            search_seed_queries=search_seed_queries,
            query_bundles=query_bundles or [],
            intent_view=resolved_intent_view,
        )
        effective_search_seed_queries = _resolve_effective_search_seed_queries(
            search_seed_queries=resolved_search_seed_queries,
            delta_execution_plan=delta_execution_plan,
        )
        resolved_query_bundles = _resolve_discovery_query_bundles(
            query_bundles=query_bundles or [],
            intent_view=resolved_intent_view,
        )
        effective_query_bundles = _filter_query_bundles_for_delta(
            resolved_query_bundles,
            allowed_queries=effective_search_seed_queries,
        )
        provider_people_search_mode = str(cost_policy.get("provider_people_search_mode") or "fallback_only").strip().lower()
        provider_search_only = provider_people_search_mode in {"primary_only", "provider_only", "harvest_only"}
        provider_search_primary = provider_people_search_mode in {"primary", "always", "primary_only", "provider_only", "harvest_only"}
        allow_web_seed_fallback = _stage1_web_seed_fallback_enabled(
            cost_policy=cost_policy,
            intent_view=resolved_intent_view,
        )

        entries: list[dict[str, Any]] = []
        query_summaries: list[dict[str, Any]] = []
        accounts_used: list[str] = []
        errors: list[str] = []
        stop_reason = "completed"

        compiled_queries = _compile_query_specs(effective_search_seed_queries, effective_query_bundles)
        web_result_target = int(cost_policy.get("provider_people_search_min_expected_results", 10) or 10)
        parallel_limit = max(1, min(int(cost_policy.get("parallel_search_workers", 3) or 3), len(compiled_queries) or 1))
        result_limit = max(1, min(int(cost_policy.get("public_media_results_per_query", 10) or 10), 25))
        runtime_timing_overrides = _runtime_timing_overrides_from_request_payload(effective_request_payload)
        worker_results: list[dict[str, Any]] = []

        def _emit_incremental_query_result(result: dict[str, Any]) -> None:
            if on_incremental_query_result is None:
                return
            incremental_entries = list(result.get("entries") or [])
            if not incremental_entries:
                return
            summary = dict(result.get("summary") or {})
            try:
                on_incremental_query_result(
                    {
                        "index": int(result.get("index") or 0),
                        "entries": incremental_entries,
                        "summary": summary,
                        "query": str(summary.get("query") or ""),
                        "bundle_id": str(summary.get("bundle_id") or ""),
                        "source_family": str(summary.get("source_family") or ""),
                        "execution_mode": str(summary.get("execution_mode") or ""),
                        "mode": str(summary.get("mode") or ""),
                        "employment_status": employment_status,
                        "raw_path": str(summary.get("raw_path") or ""),
                    }
                )
            except Exception as exc:
                errors.append(f"incremental_query_result:{str(exc)[:160]}")

        pending_specs = [] if provider_search_only or not allow_web_seed_fallback else [
            {
                "index": index,
                "query_spec": query_spec,
                "lane_id": (
                    "public_media_specialist"
                    if query_spec["source_family"] in {"public_interviews", "publication_and_blog"}
                    else "search_planner"
                ),
                "worker_key": _search_seed_worker_key(
                    query_spec["bundle_id"],
                    index,
                    employment_status,
                    query_text=str(query_spec.get("query") or ""),
                ),
                "label": str(query_spec.get("query") or ""),
                "runtime_timing_overrides": dict(runtime_timing_overrides),
            }
            for index, query_spec in enumerate(compiled_queries, start=1)
            if query_spec["execution_mode"] != "paid_fallback"
        ]
        web_seed_fallback_suppressed = bool(
            not allow_web_seed_fallback
            and not provider_search_only
            and [
                item
                for item in compiled_queries
                if str(item.get("execution_mode") or "") != "paid_fallback"
            ]
        )
        daemon_summary = {
            "results": [],
            "retried": [],
            "backlog": [],
            "lane_budget_used": {},
            "lane_budget_caps": {},
            "cycles": 0,
            "daemon_events": [],
        }
        if worker_runtime is not None and job_id and pending_specs:
            errors.extend(
                _prepare_batched_search_seed_queries(
                    search_provider=self.search_provider,
                    logger=logger,
                    discovery_dir=discovery_dir,
                    pending_specs=pending_specs,
                    result_limit=result_limit,
                )
            )
        if pending_specs:
            if worker_runtime is not None and job_id:
                daemon = AutonomousWorkerDaemon.from_plan(
                    plan_payload={"acquisition_strategy": {"cost_policy": cost_policy}},
                    existing_workers=worker_runtime.list_workers(job_id=job_id),
                    total_limit=parallel_limit,
                )
                daemon_summary = daemon.run(
                    pending_specs,
                    executor=lambda spec: self._execute_query_spec(
                        index=int(spec["index"]),
                        query_spec=dict(spec["query_spec"]),
                        identity=identity,
                        discovery_dir=discovery_dir,
                        logger=logger,
                        employment_status=employment_status,
                        worker_runtime=worker_runtime,
                        job_id=job_id,
                        request_payload=effective_request_payload,
                        plan_payload=plan_payload or {},
                        runtime_mode=runtime_mode,
                        result_limit=result_limit,
                        prefetched_search_state=dict(spec.get("prefetched_search_state") or {}),
                        prefetched_search_artifact_paths=dict(spec.get("prefetched_search_artifact_paths") or {}),
                        prefetched_search_raw_path=str(spec.get("prefetched_search_raw_path") or ""),
                        prefetched_search_manifest_path=str(spec.get("prefetched_search_manifest_path") or ""),
                        prefetched_search_manifest_key=str(spec.get("prefetched_search_manifest_key") or ""),
                    ),
                    result_callback=_emit_incremental_query_result,
                )
                worker_results.extend(list(daemon_summary.get("results") or []))
            else:
                with ThreadPoolExecutor(max_workers=max(1, min(parallel_limit, len(pending_specs)))) as executor:
                    future_to_spec = {
                        executor.submit(
                            self._execute_query_spec,
                            index=int(spec["index"]),
                            query_spec=dict(spec["query_spec"]),
                            identity=identity,
                            discovery_dir=discovery_dir,
                            logger=logger,
                            employment_status=employment_status,
                            worker_runtime=worker_runtime,
                            job_id=job_id,
                            request_payload=effective_request_payload,
                            plan_payload=plan_payload or {},
                            runtime_mode=runtime_mode,
                            result_limit=result_limit,
                            prefetched_search_state=dict(spec.get("prefetched_search_state") or {}),
                            prefetched_search_artifact_paths=dict(spec.get("prefetched_search_artifact_paths") or {}),
                            prefetched_search_raw_path=str(spec.get("prefetched_search_raw_path") or ""),
                            prefetched_search_manifest_path=str(spec.get("prefetched_search_manifest_path") or ""),
                            prefetched_search_manifest_key=str(spec.get("prefetched_search_manifest_key") or ""),
                        ): spec
                        for spec in pending_specs
                    }
                    for future in as_completed(future_to_spec):
                        result = future.result()
                        worker_results.append(result)
                        _emit_incremental_query_result(result)

        worker_results.sort(key=lambda item: int(item.get("index") or 0))
        for result in worker_results:
            entries.extend(list(result.get("entries") or []))
            query_summaries.append(dict(result.get("summary") or {}))
            errors.extend(list(result.get("errors") or []))

        queued_query_count = len([item for item in query_summaries if str(item.get("status") or "") == "queued"])
        queued_background_search = queued_query_count > 0
        if queued_background_search:
            stop_reason = "queued_background_search"

        entries = _dedupe_seed_entries(entries)
        paid_queries = _resolve_provider_people_search_queries(
            identity=identity,
            filter_hints=effective_filter_hints,
            search_seed_queries=[item["query"] for item in compiled_queries if item["execution_mode"] == "paid_fallback"]
            or list(effective_search_seed_queries),
        )
        provider_available = bool(
            self._rapidapi_people_search_enabled(runtime_dir=snapshot_dir) or self._harvest_people_search_enabled()
        )
        should_run_provider_people_search = False
        if provider_search_primary:
            should_run_provider_people_search = True
        elif provider_available and len(entries) < web_result_target and provider_people_search_mode == "fallback_only":
            should_run_provider_people_search = True
        scoped_keyword_union_policy = dict(scoped_keyword_union_shard_policy or {})
        scoped_keyword_union_dispatch: dict[str, Any] = {}
        scoped_keyword_union_plan_path = discovery_dir / SCOPED_KEYWORD_UNION_SHARD_PLAN_FILENAME
        provider_query_summaries: list[dict[str, Any]] = []
        scoped_provider_entries: list[dict[str, Any]] = []
        if scoped_keyword_union_policy:
            # WS1 Step 4b-B (ruling RATIFIED 2026-07-23, B-then-A): keyword
            # shards are first-class shards of the paid people-search lane.
            # The expected-shard set is derived from the planner-minted policy
            # (shard ids recorded verbatim — the planner is the single writer)
            # against the exact query list the dispatch loop will run, and is
            # persisted BEFORE dispatch so recovery and completion fail closed
            # against the full plan, not whatever queries happen to finish.
            scoped_keyword_union_dispatch = self.resolve_scoped_keyword_union_shard_dispatch(
                policy=scoped_keyword_union_policy,
                identity=identity,
                filter_hints=effective_filter_hints,
                search_seed_queries=list(paid_queries),
                cost_policy=cost_policy,
            )
            logger.write_json(
                scoped_keyword_union_plan_path,
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": identity.canonical_name,
                    **scoped_keyword_union_dispatch,
                    "provider_lane_planned": bool(
                        should_run_provider_people_search and self._harvest_people_search_enabled()
                    ),
                },
                asset_type="scoped_keyword_union_shard_plan",
                source_kind="search_seed_discovery",
                is_raw_asset=False,
                model_safe=True,
            )
        if should_run_provider_people_search:
            needed = max(web_result_target - len(entries), 0)
            provider_limit = max(needed, web_result_target)
            provider_entries, provider_summaries, provider_errors, provider_accounts = self._provider_people_search_fallback(
                identity=identity,
                discovery_dir=discovery_dir,
                asset_logger=logger,
                search_seed_queries=paid_queries,
                filter_hints=effective_filter_hints,
                employment_status=employment_status,
                limit=provider_limit,
                cost_policy=cost_policy,
                runtime_timing_overrides=runtime_timing_overrides,
                on_incremental_query_result=on_incremental_query_result,
                worker_runtime=worker_runtime,
                job_id=job_id,
                request_payload=effective_request_payload,
                plan_payload=plan_payload or {},
                runtime_mode=runtime_mode,
                snapshot_id=snapshot_dir.name,
            )
            scoped_provider_entries = [dict(item) for item in provider_entries if isinstance(item, dict)]
            entries.extend(provider_entries)
            entries = _dedupe_seed_entries(entries)
            query_summaries.extend(provider_summaries)
            provider_query_summaries = [dict(item) for item in provider_summaries if isinstance(item, dict)]
            errors.extend(provider_errors)
            accounts_used.extend(provider_accounts)
            if provider_entries and not queued_background_search:
                stop_reason = "provider_people_search_primary" if provider_search_primary else "provider_people_search_fallback"

        scoped_keyword_union_block: dict[str, Any] = {}
        if scoped_keyword_union_policy:
            # Honest completion + union-dedupe provenance (Step 4b-B): every
            # duplicate person found by multiple keyword shards keeps the FULL
            # set of contributing shard ids on the surviving deduped entry.
            scoped_keyword_union_completion = resolve_scoped_keyword_union_completion(
                dispatch=scoped_keyword_union_dispatch,
                query_summaries=provider_query_summaries,
            )
            shard_ids_by_query: dict[str, list[str]] = {}
            for shard in list(scoped_keyword_union_dispatch.get("shards") or []):
                provider_query = str(dict(shard or {}).get("provider_query") or "").strip()
                shard_id = str(dict(shard or {}).get("shard_id") or "").strip()
                if provider_query and shard_id:
                    shard_ids_by_query.setdefault(provider_query, []).append(shard_id)
            provenance_by_seed_key: dict[str, list[str]] = {}
            for provider_entry in scoped_provider_entries:
                seed_key = str(provider_entry.get("seed_key") or "").strip()
                source_query = str(provider_entry.get("source_query") or "").strip()
                if not seed_key:
                    continue
                for shard_id in shard_ids_by_query.get(source_query, []):
                    bucket = provenance_by_seed_key.setdefault(seed_key, [])
                    if shard_id not in bucket:
                        bucket.append(shard_id)
            if provenance_by_seed_key:
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    shard_ids = provenance_by_seed_key.get(str(entry.get("seed_key") or "").strip())
                    if not shard_ids:
                        continue
                    entry_metadata = dict(entry.get("metadata") or {})
                    entry_metadata["scoped_keyword_union_shard_ids"] = list(shard_ids)
                    entry["metadata"] = entry_metadata
            scoped_keyword_union_block = {
                "strategy_id": str(scoped_keyword_union_dispatch.get("strategy_id") or ""),
                "mode": str(scoped_keyword_union_dispatch.get("mode") or ""),
                "plan_path": str(scoped_keyword_union_plan_path),
                "shards": list(scoped_keyword_union_dispatch.get("shards") or []),
                "expected_shard_ids": list(scoped_keyword_union_dispatch.get("expected_shard_ids") or []),
                "unsharded_query_texts": list(scoped_keyword_union_dispatch.get("unsharded_query_texts") or []),
                "completion": dict(scoped_keyword_union_completion),
            }
            logger.write_json(
                scoped_keyword_union_plan_path,
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": identity.canonical_name,
                    **scoped_keyword_union_dispatch,
                    "provider_lane_planned": bool(
                        should_run_provider_people_search and self._harvest_people_search_enabled()
                    ),
                    "completion": dict(scoped_keyword_union_completion),
                },
                asset_type="scoped_keyword_union_shard_plan",
                source_kind="search_seed_discovery",
                is_raw_asset=False,
                model_safe=True,
            )

        strategy_type = str(dict(lane_context or {}).get("strategy_type") or "").strip()
        query_summaries = _annotate_search_seed_query_summaries(
            query_summaries,
            employment_status=employment_status,
            filter_hints=effective_filter_hints,
            strategy_type=strategy_type,
        )
        query_summaries, provider_retry_items = _attach_provider_retry_items_to_query_summaries(
            query_summaries,
            target_company=identity.canonical_name,
            snapshot_id=snapshot_dir.name,
        )
        incomplete_provider_query_count = _count_incomplete_provider_query_summaries(query_summaries)
        provider_retry_exhausted_count = len(
            [
                item
                for item in list(provider_retry_items or [])
                if str(item.get("status") or "").strip().lower() == "exhausted"
            ]
        )
        if incomplete_provider_query_count > 0 and not queued_background_search:
            stop_reason = "provider_people_search_incomplete"
        entries_path = discovery_dir / "entries.json"
        logger.write_json(
            entries_path,
            entries,
            asset_type="search_seed_entries",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        employment_scope = normalize_search_seed_employment_scope(employment_status)
        lane_summary_path = discovery_dir / employment_scope / "summary.json"
        lane_entries_path = discovery_dir / employment_scope / "entries.json"
        lane_payload = {
            "snapshot_id": snapshot_dir.name,
            "target_company": identity.canonical_name,
            "company_identity": identity.to_record(),
            "lane": "profile_search",
            "employment_scope": employment_scope,
            "employment_status": employment_scope,
            "strategy_type": strategy_type,
            "entry_count": len(entries),
            "search_seed_queries": list(effective_search_seed_queries),
            "requested_search_seed_queries": list(resolved_search_seed_queries),
            "effective_query_bundles": list(effective_query_bundles),
            "requested_filter_hints": _copy_search_seed_filter_hints(resolved_filter_hints),
            "effective_filter_hints": _copy_search_seed_filter_hints(effective_filter_hints),
            "query_summaries": list(query_summaries),
            "accounts_used": list(accounts_used),
            "errors": list(errors),
            "stop_reason": stop_reason,
            "incomplete_provider_query_count": incomplete_provider_query_count,
            "provider_retry_items": list(provider_retry_items),
            "provider_retry_item_count": len(provider_retry_items),
            "provider_retry_exhausted_count": provider_retry_exhausted_count,
            "queued_query_count": queued_query_count,
            "cost_policy": dict(cost_policy),
            "web_seed_fallback_enabled": allow_web_seed_fallback,
            "web_seed_fallback_suppressed": web_seed_fallback_suppressed,
            "intent_view": dict(resolved_intent_view),
            "delta_execution_plan": dict(delta_execution_plan or {}),
            "lane_context": dict(lane_context or {}),
            "summary_path": str(lane_summary_path),
            "entries_path": str(lane_entries_path),
            **({"scoped_keyword_union": dict(scoped_keyword_union_block)} if scoped_keyword_union_block else {}),
            "worker_daemon": {
                "cycles": int(daemon_summary.get("cycles") or 0),
                "retried": list(daemon_summary.get("retried") or []),
                "backlog_count": len(list(daemon_summary.get("backlog") or [])),
                "lane_budget_used": dict(daemon_summary.get("lane_budget_used") or {}),
                "lane_budget_caps": dict(daemon_summary.get("lane_budget_caps") or {}),
            },
        }
        summary_path = discovery_dir / "summary.json"
        summary_payload = {
            "snapshot_id": snapshot_dir.name,
            "target_company": identity.canonical_name,
            "company_identity": identity.to_record(),
            "entry_count": len(entries),
            "search_seed_queries": effective_search_seed_queries,
            "requested_search_seed_queries": resolved_search_seed_queries,
            "effective_query_bundles": effective_query_bundles,
            "requested_filter_hints": resolved_filter_hints,
            "effective_filter_hints": effective_filter_hints,
            "query_summaries": query_summaries,
            "accounts_used": accounts_used,
            "errors": errors,
            "stop_reason": stop_reason,
            "incomplete_provider_query_count": incomplete_provider_query_count,
            "provider_retry_items": provider_retry_items,
            "provider_retry_item_count": len(provider_retry_items),
            "provider_retry_exhausted_count": provider_retry_exhausted_count,
            "queued_query_count": queued_query_count,
            "cost_policy": cost_policy,
            "web_seed_fallback_enabled": allow_web_seed_fallback,
            "web_seed_fallback_suppressed": web_seed_fallback_suppressed,
            "intent_view": resolved_intent_view,
            "delta_execution_plan": dict(delta_execution_plan or {}),
            "lane_context": dict(lane_context or {}),
            **({"scoped_keyword_union": dict(scoped_keyword_union_block)} if scoped_keyword_union_block else {}),
            "lane_summaries": {
                employment_scope: {
                    "lane": "profile_search",
                    "employment_scope": employment_scope,
                    "strategy_type": strategy_type,
                    "summary_path": str(lane_summary_path),
                    "entries_path": str(lane_entries_path),
                    "entry_count": len(entries),
                    "query_count": len(query_summaries),
                }
            },
            "worker_daemon": {
                "cycles": int(daemon_summary.get("cycles") or 0),
                "retried": list(daemon_summary.get("retried") or []),
                "backlog_count": len(list(daemon_summary.get("backlog") or [])),
                "lane_budget_used": dict(daemon_summary.get("lane_budget_used") or {}),
                "lane_budget_caps": dict(daemon_summary.get("lane_budget_caps") or {}),
            },
        }
        logger.write_json(
            summary_path,
            summary_payload,
            asset_type="search_seed_summary",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        return SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=entries,
            query_summaries=query_summaries,
            accounts_used=accounts_used,
            errors=errors,
            stop_reason=stop_reason,
            summary_path=summary_path,
            entries_path=entries_path,
            summary_payload=summary_payload,
            lane_payloads={employment_scope: lane_payload},
            lane_entries={employment_scope: list(entries)},
        )

    def _execute_query_spec(
        self,
        *,
        index: int,
        query_spec: dict[str, str],
        identity: CompanyIdentity,
        discovery_dir: Path,
        logger: AssetLogger,
        employment_status: str,
        worker_runtime: AgentRuntimeCoordinator | None,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        result_limit: int,
        prefetched_search_state: dict[str, Any] | None = None,
        prefetched_search_artifact_paths: dict[str, str] | None = None,
        prefetched_search_raw_path: str = "",
        prefetched_search_manifest_path: str = "",
        prefetched_search_manifest_key: str = "",
    ) -> dict[str, Any]:
        query_text = str(query_spec["query"] or "").strip()
        snapshot_id = str(discovery_dir.parent.name if isinstance(discovery_dir, Path) else "").strip()
        query_item_id = _search_seed_discovery_query_item_id(
            job_id=job_id,
            snapshot_id=snapshot_id,
            query_spec=query_spec,
            index=index,
            employment_status=employment_status,
        )

        def record_query_item(
            *,
            status: str,
            phase: str,
            reason: str,
            worker_id: int = 0,
            metadata: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            return _record_search_seed_discovery_query_item(
                worker_runtime=worker_runtime,
                item_id=query_item_id,
                job_id=job_id,
                identity=identity,
                snapshot_id=snapshot_id,
                index=index,
                query_spec=query_spec,
                employment_status=employment_status,
                status=status,
                phase=phase,
                reason=reason,
                worker_id=worker_id,
                metadata=metadata,
            )

        effective_request_payload = build_effective_request_payload(request_payload or {}) if request_payload else {}
        runtime_timing_overrides = resolve_runtime_timing_overrides(
            dict(effective_request_payload.get("execution_preferences") or {})
        )
        raw_search_path = discovery_dir / f"web_query_{index:02d}.html"
        lane_id = "public_media_specialist" if query_spec["source_family"] in {"public_interviews", "publication_and_blog"} else "search_planner"
        worker_handle = None
        checkpoint: dict[str, Any] = {}
        interrupted = False
        if worker_runtime is not None and job_id:
            record_query_item(
                status="queued",
                phase="queued",
                reason="initial_query",
                metadata={"snapshot_dir": str(discovery_dir.parent), "discovery_dir": str(discovery_dir)},
            )
            worker_handle = worker_runtime.begin_worker(
                job_id=job_id,
                request=JobRequest.from_payload(effective_request_payload),
                plan_payload=plan_payload,
                runtime_mode=runtime_mode,
                lane_id=lane_id,
                worker_key=_search_seed_worker_key(
                    query_spec["bundle_id"],
                    index,
                    employment_status,
                    query_text=query_text,
                ),
                stage="acquiring",
                span_name=f"search_bundle:{query_spec['bundle_id']}",
                budget_payload={"max_results": 10, "execution_mode": query_spec["execution_mode"], "query": query_text},
                input_payload={"query_spec": query_spec, "query": query_text, "index": index},
                metadata={
                    "recovery_kind": SEARCH_SEED_DISCOVERY_RECOVERY_KIND,
                    "index": index,
                    "identity": identity.to_record(),
                    "snapshot_dir": str(discovery_dir.parent),
                    "discovery_dir": str(discovery_dir),
                    "employment_status": employment_status,
                    "request_payload": effective_request_payload,
                    "plan_payload": plan_payload,
                    "runtime_mode": runtime_mode,
                    "result_limit": result_limit,
                    "runtime_timing_overrides": dict(runtime_timing_overrides),
                },
                handoff_from_lane="search_planner" if lane_id == "public_media_specialist" else "triage_planner",
            )
            record_query_item(
                status="running",
                phase="provider_owned",
                reason="worker_envelope_created",
                worker_id=int(worker_handle.worker_id or 0),
                metadata={
                    "snapshot_dir": str(discovery_dir.parent),
                    "discovery_dir": str(discovery_dir),
                    "lane_id": lane_id,
                    "worker_key": _search_seed_worker_key(
                        query_spec["bundle_id"],
                        index,
                        employment_status,
                        query_text=query_text,
                    ),
                },
            )
            existing = worker_runtime.get_worker(worker_handle.worker_id) or {}
            checkpoint = dict(existing.get("checkpoint") or {})
            output_payload = dict(existing.get("output") or {})
            if str(existing.get("status") or "") == "completed" and output_payload:
                summary = dict(output_payload.get("summary") or {})
                entries = list(output_payload.get("entries") or [])
                errors = list(output_payload.get("errors") or [])
                worker_runtime.complete_worker(
                    worker_handle,
                    status="completed",
                    checkpoint_payload=checkpoint,
                    output_payload=output_payload,
                    handoff_to_lane="acquisition_specialist" if lane_id == "search_planner" else "exploration_specialist",
                )
                record_query_item(
                    status="completed",
                    phase="completed",
                    reason="reused_completed_worker_output",
                    worker_id=int(worker_handle.worker_id or 0),
                    metadata={
                        "summary": summary,
                        "entry_count": len(entries),
                        "errors": errors,
                        "worker_status": "completed",
                    },
                )
                return {
                    "index": index,
                    "entries": entries,
                    "summary": summary,
                    "errors": errors,
                    "worker_status": "completed",
                    "daemon_action": "reused_output",
                }
            checkpoint, prefetched_applied = _merge_prefetched_search_checkpoint(
                checkpoint=checkpoint,
                prefetched_search_state=dict(prefetched_search_state or {}),
                prefetched_search_artifact_paths=dict(prefetched_search_artifact_paths or {}),
                prefetched_search_raw_path=str(prefetched_search_raw_path or "").strip(),
                manifest_path=Path(prefetched_search_manifest_path).expanduser()
                if str(prefetched_search_manifest_path or "").strip()
                else None,
                manifest_key=str(prefetched_search_manifest_key or "").strip(),
            )
            if prefetched_applied:
                worker_runtime.checkpoint_worker(
                    worker_handle,
                    checkpoint_payload=checkpoint,
                    output_payload={
                        "search_state": dict(checkpoint.get("search_state") or {}),
                        "search_artifact_paths": dict(checkpoint.get("search_artifact_paths") or {}),
                    },
                )
        try:
            if worker_handle and worker_runtime.should_interrupt_worker(worker_handle):
                summary = _interrupted_query_summary(index, query_spec, query_text, raw_search_path)
                worker_runtime.complete_worker(
                    worker_handle,
                    status="interrupted",
                    checkpoint_payload=checkpoint,
                    output_payload={"summary": summary, "entries": [], "errors": []},
                    handoff_to_lane="review_specialist",
                )
                record_query_item(
                    status="failed",
                    phase="interrupted",
                    reason="worker_interrupted",
                    worker_id=int(worker_handle.worker_id or 0),
                    metadata={"summary": summary, "worker_status": "interrupted"},
                )
                return {"index": index, "entries": [], "summary": summary, "errors": [], "worker_status": "interrupted"}

            cached_search_path = Path(str(checkpoint.get("raw_path") or "")) if checkpoint.get("raw_path") else None
            search_response: SearchResponse | None = None
            manifest_path = (
                Path(str(checkpoint.get("search_manifest_path") or "")).expanduser()
                if str(checkpoint.get("search_manifest_path") or "").strip()
                else None
            )
            manifest_key = str(checkpoint.get("search_manifest_key") or "").strip()
            if cached_search_path and cached_search_path.exists():
                parsed_results, search_response = _load_cached_search_response(cached_search_path, query_text)
                raw_search_path = cached_search_path
                manifest_search_state = _mark_search_state_as_worker_fetched(
                    dict(checkpoint.get("search_state") or {}),
                    query_text=query_text,
                )
                if manifest_search_state:
                    checkpoint["search_state"] = manifest_search_state
                    _update_search_batch_manifest_entry(
                        logger=logger,
                        manifest_path=manifest_path,
                        task_key=manifest_key,
                        search_state=manifest_search_state,
                        artifact_paths=dict(checkpoint.get("search_artifact_paths") or {}),
                        raw_path=str(cached_search_path),
                    )
            else:
                if worker_handle:
                    search_checkpoint = apply_runtime_timing_overrides_to_search_state(
                        dict(checkpoint.get("search_state") or {}),
                        runtime_timing_overrides=runtime_timing_overrides,
                    )
                    search_execution = self.search_provider.execute_with_checkpoint(
                        query_text,
                        max_results=result_limit,
                        checkpoint=search_checkpoint,
                    )
                    artifact_paths = dict(checkpoint.get("search_artifact_paths") or {})
                    for artifact in list(search_execution.artifacts or []):
                        artifact_path = _write_search_execution_artifact(
                            logger=logger,
                            artifact=artifact,
                            default_path=raw_search_path,
                            asset_type="web_search_queue_payload",
                            source_kind="search_seed_discovery",
                            metadata={"query": query_text, "provider_name": search_execution.provider_name},
                        )
                        artifact_paths[str(artifact.label)] = str(artifact_path)
                    checkpoint = {
                        **checkpoint,
                        "provider_name": search_execution.provider_name,
                        "search_state": dict(search_execution.checkpoint or {}),
                        "search_artifact_paths": artifact_paths,
                    }
                    if search_execution.pending:
                        summary = {
                            "query": query_text,
                            "bundle_id": query_spec["bundle_id"],
                            "source_family": query_spec["source_family"],
                            "execution_mode": query_spec["execution_mode"],
                            "mode": "web_search",
                            "status": "queued",
                            "raw_path": "",
                            "provider_name": search_execution.provider_name,
                            "result_count": 0,
                            "linkedin_result_count": 0,
                            "seed_entry_count": 0,
                            "search_state": dict(search_execution.checkpoint or {}),
                            "search_artifact_paths": artifact_paths,
                            "message": str(search_execution.message or ""),
                        }
                        worker_runtime.complete_worker(
                            worker_handle,
                            status="queued",
                            checkpoint_payload={
                                **checkpoint,
                                "stage": "waiting_remote_search",
                                "recovery_kind": SEARCH_SEED_DISCOVERY_RECOVERY_KIND,
                            },
                            output_payload={"summary": summary, "entries": [], "errors": [], "search_state": dict(search_execution.checkpoint or {})},
                        )
                        record_query_item(
                            status="running",
                            phase="provider_owned",
                            reason="provider_pending",
                            worker_id=int(worker_handle.worker_id or 0),
                            metadata={
                                "summary": summary,
                                "search_state": dict(search_execution.checkpoint or {}),
                                "search_artifact_paths": artifact_paths,
                                "provider_name": search_execution.provider_name,
                                "worker_status": "queued",
                            },
                        )
                        return {
                            "index": index,
                            "entries": [],
                            "summary": summary,
                            "errors": [],
                            "worker_status": "queued",
                        }
                    search_response = search_execution.response
                    if search_response is None:
                        raise RuntimeError("Search provider returned no response.")
                    raw_search_path = _write_search_response_raw_asset(
                        logger=logger,
                        response=search_response,
                        default_path=raw_search_path,
                        asset_type="web_search_payload",
                        source_kind="search_seed_discovery",
                        metadata={"query": query_text, "provider_name": search_response.provider_name},
                    )
                    manifest_search_state = _mark_search_state_as_worker_fetched(
                        dict(checkpoint.get("search_state") or {}),
                        fallback_state=dict(search_execution.checkpoint or {}),
                        query_text=query_text,
                    )
                    checkpoint = {
                        **checkpoint,
                        "stage": "fetched_search_results",
                        "raw_path": str(raw_search_path),
                        "provider_name": search_response.provider_name,
                        "search_state": manifest_search_state,
                    }
                    if manifest_search_state:
                        _update_search_batch_manifest_entry(
                            logger=logger,
                            manifest_path=manifest_path,
                            task_key=manifest_key,
                            search_state=manifest_search_state,
                            artifact_paths=dict(checkpoint.get("search_artifact_paths") or {}),
                            raw_path=str(raw_search_path),
                        )
                    worker_runtime.checkpoint_worker(
                        worker_handle,
                        checkpoint_payload=checkpoint,
                        output_payload={"raw_path": str(raw_search_path), "provider_name": search_response.provider_name},
                    )
                else:
                    search_response = self.search_provider.search(query_text, max_results=result_limit)
                    raw_search_path = _write_search_response_raw_asset(
                        logger=logger,
                        response=search_response,
                        default_path=raw_search_path,
                        asset_type="web_search_payload",
                        source_kind="search_seed_discovery",
                        metadata={"query": query_text, "provider_name": search_response.provider_name},
                    )
                    checkpoint = {
                        **checkpoint,
                        "stage": "fetched_search_results",
                        "raw_path": str(raw_search_path),
                        "provider_name": search_response.provider_name,
                    }
                    if worker_handle:
                        worker_runtime.checkpoint_worker(
                            worker_handle,
                            checkpoint_payload=checkpoint,
                            output_payload={"raw_path": str(raw_search_path), "provider_name": search_response.provider_name},
                        )

            if search_response is None:
                raise RuntimeError("Search provider returned no response.")

            parsed_results = [item.to_record() for item in search_response.results]
            public_media_analysis = {}
            if query_spec["source_family"] in {"public_interviews", "publication_and_blog"}:
                public_media_analysis, checkpoint = self._analyze_public_media_results(
                    index=index,
                    query_spec=query_spec,
                    identity=identity,
                    discovery_dir=discovery_dir,
                    logger=logger,
                    parsed_results=parsed_results,
                    checkpoint=checkpoint,
                    worker_runtime=worker_runtime,
                    worker_handle=worker_handle,
                    result_limit=result_limit,
                )
                checkpoint = {**checkpoint, "stage": "public_media_analyzed"}
                if worker_handle and worker_runtime and worker_runtime.should_interrupt_worker(worker_handle):
                    interrupted = True

            linkedin_results = [item for item in parsed_results if "linkedin.com/in/" in item.get("url", "")]
            query_entries = []
            for item in linkedin_results[:result_limit]:
                seed_entry = _seed_entry_from_web_result(
                    item,
                    identity,
                    query_text,
                    employment_status,
                    source_family=query_spec["source_family"],
                )
                if seed_entry is not None:
                    query_entries.append(seed_entry)
            if query_spec["source_family"] in {"public_interviews", "publication_and_blog"}:
                for item in parsed_results[:result_limit]:
                    analysis = public_media_analysis.get(str(item.get("url") or "").strip(), {})
                    if str(analysis.get("confidence_label") or "low") == "low":
                        continue
                    for seed_entry in _lead_entries_from_public_result(
                        item,
                        identity,
                        query_text,
                        employment_status,
                        analysis=analysis,
                        source_family=query_spec["source_family"],
                    ):
                        query_entries.append(seed_entry)

            summary = {
                "query": query_text,
                "bundle_id": query_spec["bundle_id"],
                "source_family": query_spec["source_family"],
                "execution_mode": query_spec["execution_mode"],
                "mode": "web_search",
                "status": "interrupted" if interrupted else "completed",
                "raw_path": str(raw_search_path),
                "provider_name": search_response.provider_name,
                "result_count": len(parsed_results),
                "linkedin_result_count": len(linkedin_results),
                "seed_entry_count": len(query_entries),
            }
            if worker_handle:
                worker_runtime.complete_worker(
                    worker_handle,
                    status="interrupted" if interrupted else "completed",
                    checkpoint_payload={
                        **checkpoint,
                        "stage": "interrupted" if interrupted else "completed",
                        "recovery_kind": SEARCH_SEED_DISCOVERY_RECOVERY_KIND,
                        "result_count": len(parsed_results),
                    },
                    output_payload={"summary": summary, "entries": query_entries, "errors": [], "seed_entry_count": len(query_entries)},
                    handoff_to_lane=(
                        "review_specialist"
                        if interrupted
                        else "acquisition_specialist"
                        if lane_id == "search_planner"
                        else "exploration_specialist"
                    ),
                )
                record_query_item(
                    status="failed" if interrupted else "completed",
                    phase="interrupted" if interrupted else "completed",
                    reason="worker_interrupted" if interrupted else "provider_result_persisted",
                    worker_id=int(worker_handle.worker_id or 0),
                    metadata={
                        "summary": summary,
                        "entry_count": len(query_entries),
                        "raw_path": str(raw_search_path),
                        "provider_name": search_response.provider_name,
                        "worker_status": "interrupted" if interrupted else "completed",
                    },
                )
            return {
                "index": index,
                "entries": query_entries,
                "summary": summary,
                "errors": [],
                "worker_status": "interrupted" if interrupted else "completed",
            }
        except Exception as exc:
            error_text = f"web_search:{query_text}:{str(exc)[:120]}"
            summary = {
                "query": query_text,
                "bundle_id": query_spec["bundle_id"],
                "source_family": query_spec["source_family"],
                "execution_mode": query_spec["execution_mode"],
                "mode": "web_search",
                "status": "failed",
                "raw_path": str(raw_search_path),
                "result_count": 0,
                "linkedin_result_count": 0,
                "seed_entry_count": 0,
                "error": str(exc),
            }
            if worker_handle:
                worker_runtime.complete_worker(
                    worker_handle,
                    status="failed",
                    checkpoint_payload={
                        **checkpoint,
                        "stage": "failed",
                        "recovery_kind": SEARCH_SEED_DISCOVERY_RECOVERY_KIND,
                    },
                    output_payload={"error": str(exc), "summary": summary, "entries": [], "errors": [error_text]},
                    handoff_to_lane="review_specialist",
                )
                record_query_item(
                    status="failed",
                    phase="terminal",
                    reason="provider_execution_failed",
                    worker_id=int(worker_handle.worker_id or 0),
                    metadata={"summary": summary, "error": str(exc), "worker_status": "failed"},
                )
            return {"index": index, "entries": [], "summary": summary, "errors": [error_text], "worker_status": "failed"}

    def _analyze_public_media_results(
        self,
        *,
        index: int,
        query_spec: dict[str, str],
        identity: CompanyIdentity,
        discovery_dir: Path,
        logger: AssetLogger,
        parsed_results: list[dict[str, str]],
        checkpoint: dict[str, Any],
        worker_runtime: AgentRuntimeCoordinator | None,
        worker_handle,
        result_limit: int,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        public_media_payloads = [
            {
                "title": str(item.get("title") or "").strip(),
                "snippet": str(item.get("snippet") or "").strip(),
                "url": str(item.get("url") or "").strip(),
                "source_family": query_spec["source_family"],
            }
            for item in parsed_results[:result_limit]
        ]
        results_path = discovery_dir / f"public_media_results_{index:02d}.json"
        analysis_path = discovery_dir / f"public_media_analysis_{index:02d}.json"
        logger.write_json(
            results_path,
            {"query": query_spec["query"], "records": public_media_payloads},
            asset_type="public_media_results",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        analyzed_records = []
        analysis_map: dict[str, dict[str, Any]] = {
            str(key): dict(value)
            for key, value in dict(checkpoint.get("public_media_analysis") or {}).items()
            if str(key).strip()
        }
        completed_urls = set(checkpoint.get("completed_urls") or [])
        for record in public_media_payloads:
            if worker_handle and worker_runtime and worker_runtime.should_interrupt_worker(worker_handle):
                break
            if record["url"] in completed_urls and record["url"] in analysis_map:
                analyzed_records.append({**record, "analysis": analysis_map[record["url"]]})
                continue
            analysis = self.model_client.analyze_page_asset(
                {
                    "target_company": identity.canonical_name,
                    "title": record["title"],
                    "description": record["snippet"],
                    "url": record["url"],
                    "text_excerpt": f"{record['title']} {record['snippet']}".strip(),
                    "extracted_links": {"linkedin_urls": [], "personal_urls": [], "x_urls": [], "github_urls": [], "resume_urls": []},
                }
            )
            analyzed_records.append({**record, "analysis": analysis})
            analysis_map[record["url"]] = analysis
            completed_urls.add(record["url"])
            if worker_handle and worker_runtime:
                checkpoint = {
                    **checkpoint,
                    "stage": "public_media_analysis",
                    "completed_urls": sorted(completed_urls),
                    "public_media_analysis": analysis_map,
                }
                worker_runtime.checkpoint_worker(
                    worker_handle,
                    checkpoint_payload=checkpoint,
                    output_payload={"analyzed_count": len(analyzed_records)},
                )
        logger.write_json(
            analysis_path,
            {"query": query_spec["query"], "records": analyzed_records},
            asset_type="public_media_analysis",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        checkpoint = {
            **checkpoint,
            "stage": "public_media_analyzed",
            "completed_urls": sorted(completed_urls),
            "public_media_analysis": analysis_map,
        }
        return analysis_map, checkpoint

    def _provider_people_search_fallback(
        self,
        *,
        identity: CompanyIdentity,
        discovery_dir: Path,
        asset_logger: AssetLogger,
        search_seed_queries: list[str],
        filter_hints: dict[str, list[str]],
        employment_status: str,
        limit: int,
        cost_policy: dict[str, Any],
        runtime_timing_overrides: dict[str, Any] | None = None,
        on_incremental_query_result: SearchSeedIncrementalResultCallback | None = None,
        worker_runtime: AgentRuntimeCoordinator | None = None,
        job_id: str = "",
        request_payload: dict[str, Any] | None = None,
        plan_payload: dict[str, Any] | None = None,
        runtime_mode: str = "",
        snapshot_id: str = "",
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], list[str]]:
        entries: list[dict[str, Any]] = []
        query_summaries: list[dict[str, Any]] = []
        errors: list[str] = []
        accounts_used: list[str] = []
        runtime_timing_overrides = dict(runtime_timing_overrides or {})
        page_count = int(cost_policy.get("provider_people_search_pages", 2 if employment_status == "former" else 1) or 1)
        query_strategy = str(cost_policy.get("provider_people_search_query_strategy") or "all_queries_union").strip().lower()
        stop_after_first_hit = query_strategy in {"first_hit", "first_nonempty", "first_non_empty", "first_match"}
        zero_result_retry_attempts = _provider_people_search_zero_result_retry_attempts(cost_policy)
        zero_result_retry_backoff_seconds = _provider_people_search_zero_result_retry_backoff_seconds(cost_policy)
        effective_snapshot_id = str(snapshot_id or discovery_dir.parent.name).strip()
        effective_job_id = str(job_id or "").strip()
        max_query_attempts = max(1, zero_result_retry_attempts + 1)

        def _provider_query_spec(summary_query: str) -> dict[str, str]:
            return {
                "query": str(summary_query or "").strip(),
                "bundle_id": "harvest_people_search",
                "source_family": "linkedin_people_search",
                "execution_mode": "paid_fallback",
            }

        def _record_provider_query_item(
            *,
            index: int,
            summary_query: str,
            effective_query_text: str,
            status: str,
            phase: str,
            reason: str,
            not_before_at: str = "",
            metadata: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            if not effective_job_id:
                return {}
            query_spec = _provider_query_spec(summary_query)
            item_id = _search_seed_discovery_query_item_id(
                job_id=effective_job_id,
                snapshot_id=effective_snapshot_id,
                query_spec=query_spec,
                index=index,
                employment_status=employment_status,
            )
            return _record_search_seed_discovery_query_item(
                worker_runtime=worker_runtime,
                item_id=item_id,
                job_id=effective_job_id,
                identity=identity,
                snapshot_id=effective_snapshot_id,
                index=index,
                query_spec=query_spec,
                employment_status=employment_status,
                status=status,
                phase=phase,
                reason=reason,
                max_attempts=max_query_attempts,
                not_before_at=not_before_at,
                metadata={
                    "provider": "harvest_profile_search",
                    "provider_name": "harvest_profile_search",
                    "provider_query_kind": "harvest_people_search",
                    "effective_query_text": str(effective_query_text or summary_query or "").strip(),
                    "query_signature": _search_query_signature(
                        str(effective_query_text or summary_query or "")
                    ),
                    "filter_hints": _copy_search_seed_filter_hints(filter_hints),
                    "cost_policy": dict(cost_policy or {}),
                    "identity": identity.to_record(),
                    "snapshot_dir": str(discovery_dir.parent),
                    "discovery_dir": str(discovery_dir),
                    "request_payload": dict(request_payload or {}),
                    "plan_payload": dict(plan_payload or {}),
                    "runtime_mode": str(runtime_mode or ""),
                    "limit": max(1, int(limit or 25)),
                    "page_count": max(1, int(page_count or 1)),
                    **dict(metadata or {}),
                },
            )

        def _retryable_provider_failure_payload(
            *,
            index: int,
            summary_query: str,
            query_text: str,
            error: Exception,
        ) -> dict[str, Any]:
            retry_delay_seconds = dict(cost_policy or {}).get("provider_people_search_retry_delay_seconds", 30)
            not_before_at = _utc_timestamp_after_seconds(retry_delay_seconds)
            effective_query_text = _normalize_harvest_query_text(
                query_text=query_text,
                filter_hints=filter_hints,
                identity=identity,
            )
            item = _record_provider_query_item(
                index=index,
                summary_query=summary_query,
                effective_query_text=effective_query_text,
                status="failed_retryable",
                phase="retry_wait",
                reason="retryable_provider_failure",
                not_before_at=not_before_at,
                metadata={
                    "error": str(error),
                    "last_error": str(error),
                    "retry_not_before_at": not_before_at,
                },
            )
            query_summary = {
                "query": summary_query,
                "effective_query_text": effective_query_text,
                "mode": "harvest_profile_search",
                "status": "retry_wait",
                "raw_path": "",
                "account_id": "harvest_profile_search",
                "seed_entry_count": 0,
                "provider_search_incomplete": True,
                "provider_search_retryable": True,
                "incomplete_reason": "provider_retry_wait",
                "retry_not_before_at": not_before_at,
                "error": str(error),
            }
            if item:
                query_summary["discovery_query_item_id"] = str(item.get("item_id") or "")
                query_summary["search_seed_discovery_query_item_id"] = str(item.get("item_id") or "")
                query_summary["discovery_query_item_status"] = str(item.get("status") or "")
            return {"query_entries": [], "query_summary": query_summary, "account_used": ""}
        max_query_count = _provider_people_search_max_query_count(cost_policy)
        paid_queries = _resolve_provider_people_search_queries(
            identity=identity,
            filter_hints=filter_hints,
            search_seed_queries=search_seed_queries,
        )
        scoped_paid_queries = [
            query_text
            for query_text in paid_queries
            if not _provider_query_matches_company_identity(query_text, identity=identity)
        ]
        former_past_company_lane = (
            employment_status == "former"
            and bool(list(filter_hints.get("past_companies") or []))
            and not bool(cost_policy.get("former_keyword_queries_only"))
        )
        broad_former_past_company_allowed = former_past_company_lane and bool(
            cost_policy.get("former_broad_past_company_only")
        )
        if (
            former_past_company_lane
            and not broad_former_past_company_allowed
            and not scoped_paid_queries
            and self._harvest_people_search_enabled()
        ):
            skipped_item = _record_provider_query_item(
                index=1,
                summary_query="__past_company_only__",
                effective_query_text="",
                status="skipped",
                phase="not_dispatched",
                reason="former_broad_past_company_requires_explicit_strategy",
                metadata={
                    "provider_query_status": "skipped",
                    "skipped_reason": "former_broad_past_company_requires_explicit_strategy",
                    "resolved_provider_queries": list(paid_queries),
                    "search_seed_queries": [str(item or "").strip() for item in list(search_seed_queries or []) if str(item or "").strip()],
                },
            )
            skipped_summary = {
                "query": "__past_company_only__",
                "effective_query_text": "",
                "mode": "harvest_profile_search",
                "status": "skipped_degraded",
                "raw_path": "",
                "account_id": "harvest_profile_search",
                "seed_entry_count": 0,
                "provider_search_degraded": True,
                "degraded_reason": "former_broad_past_company_requires_explicit_strategy",
                "skipped_reason": "company_only_or_missing_scoped_provider_query",
                "resolved_provider_queries": list(paid_queries),
            }
            if skipped_item:
                skipped_summary["discovery_query_item_id"] = str(skipped_item.get("item_id") or "")
                skipped_summary["search_seed_discovery_query_item_id"] = str(skipped_item.get("item_id") or "")
                skipped_summary["discovery_query_item_status"] = str(skipped_item.get("status") or "")
            query_summaries.append(skipped_summary)
            paid_queries = []
        if (
            broad_former_past_company_allowed
        ):
            paid_queries = [""]
        # Single-sourced with the scoped keyword-union shard plan (Step 4b-B):
        # the persisted plan pre-registers exactly this deduped/budgeted list.
        deduped_queries = self._deduped_provider_people_search_query_texts(
            identity=identity,
            filter_hints=filter_hints,
            queries=paid_queries,
            max_query_count=max_query_count,
        )

        def _emit_incremental_provider_result(
            *,
            index: int,
            query_entries: list[dict[str, Any]],
            query_summary: dict[str, Any],
        ) -> None:
            if on_incremental_query_result is None or not query_entries:
                return
            try:
                on_incremental_query_result(
                    {
                        "index": index,
                        "entries": list(query_entries),
                        "summary": dict(query_summary),
                        "query": str(query_summary.get("query") or ""),
                        "bundle_id": "",
                        "source_family": "linkedin_people_search",
                        "execution_mode": "paid_fallback",
                        "mode": str(query_summary.get("mode") or ""),
                        "employment_status": employment_status,
                        "raw_path": str(query_summary.get("raw_path") or ""),
                    }
                )
            except Exception as exc:
                errors.append(f"incremental_query_result:{str(exc)[:160]}")

        precomputed_harvest_plans: dict[str, dict[str, Any]] = {}
        if (
            deduped_queries
            and self._harvest_people_search_enabled()
            and not stop_after_first_hit
            and bool(cost_policy.get("provider_people_search_overlap_pruning", True))
        ):
            try:
                overlap_threshold = float(cost_policy.get("provider_people_search_overlap_threshold") or 0.9)
            except (TypeError, ValueError):
                overlap_threshold = 0.9
            overlap_threshold = max(0.0, min(1.0, overlap_threshold))
            try:
                min_probe_rows = int(cost_policy.get("provider_people_search_overlap_min_probe_rows") or 10)
            except (TypeError, ValueError):
                min_probe_rows = 10
            min_probe_rows = max(1, min_probe_rows)
            kept_queries: list[str] = []
            kept_probe_sets: list[set[str]] = []
            for query_text in deduped_queries:
                summary_query = query_text or "__past_company_only__"
                effective_query_text = _normalize_harvest_query_text(
                    query_text=query_text,
                    filter_hints=filter_hints,
                    identity=identity,
                )
                probe_plan = self._resolve_harvest_search_execution_plan(
                    query_text=effective_query_text,
                    filter_hints=filter_hints,
                    employment_status=employment_status,
                    discovery_dir=discovery_dir,
                    asset_logger=asset_logger,
                    requested_limit=max(26, int(limit or 25)),
                    requested_pages=max(2, int(page_count or 1)),
                    allow_shared_provider_cache=bool(cost_policy.get("allow_shared_provider_cache", True)),
                    zero_result_retry_attempts=zero_result_retry_attempts,
                    zero_result_retry_backoff_seconds=zero_result_retry_backoff_seconds,
                )
                probe_plan = dict(probe_plan or {})
                probe_plan["effective_query_text"] = effective_query_text
                precomputed_harvest_plans[query_text] = probe_plan
                probe_profiles = _extract_harvest_profile_urls_from_result(dict(probe_plan.get("initial_result") or {}))
                max_overlap = 0.0
                overlap_with_query = ""
                if probe_profiles and len(probe_profiles) >= min_probe_rows:
                    for kept_query, kept_profiles in zip(kept_queries, kept_probe_sets):
                        if not kept_profiles:
                            continue
                        overlap = _jaccard_overlap_ratio(probe_profiles, kept_profiles)
                        if overlap > max_overlap:
                            max_overlap = overlap
                            overlap_with_query = kept_query or "__past_company_only__"
                if max_overlap >= overlap_threshold and overlap_with_query:
                    query_summaries.append(
                        {
                            "query": summary_query,
                            "effective_query_text": effective_query_text,
                            "mode": "harvest_profile_search",
                            "status": "skipped_high_overlap",
                            "overlap_ratio": round(max_overlap, 4),
                            "overlap_with_query": overlap_with_query,
                            "probe_profile_count": len(probe_profiles),
                            "probe": _harvest_search_plan_summary(probe_plan),
                        }
                    )
                    continue
                kept_queries.append(query_text)
                kept_probe_sets.append(probe_profiles)
            deduped_queries = kept_queries

        def _run_harvest_query(
            index: int,
            summary_query: str,
            query_text: str,
            *,
            precomputed_plan: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            if not self._harvest_people_search_enabled():
                return {"query_entries": [], "query_summary": None, "account_used": ""}
            harvest_plan = dict(precomputed_plan or {})
            effective_query_text = str(harvest_plan.get("effective_query_text") or "").strip()
            if not effective_query_text:
                effective_query_text = _normalize_harvest_query_text(
                    query_text=query_text,
                    filter_hints=filter_hints,
                    identity=identity,
                )
            queued_item = _record_provider_query_item(
                index=index,
                summary_query=summary_query,
                effective_query_text=effective_query_text,
                status="queued",
                phase="queued",
                reason="initial_query",
                metadata={"provider_query_status": "queued"},
            )
            _record_provider_query_item(
                index=index,
                summary_query=summary_query,
                effective_query_text=effective_query_text,
                status="running",
                phase="dispatch_claimed",
                reason="provider_dispatch_claimed",
                metadata={
                    "provider_query_status": "dispatch_claimed",
                    "queued_item_id": str(queued_item.get("item_id") or ""),
                },
            )
            if not harvest_plan:
                harvest_plan = self._resolve_harvest_search_execution_plan(
                    query_text=effective_query_text,
                    filter_hints=filter_hints,
                    employment_status=employment_status,
                    discovery_dir=discovery_dir,
                    asset_logger=asset_logger,
                    requested_limit=limit,
                    requested_pages=page_count,
                    allow_shared_provider_cache=bool(cost_policy.get("allow_shared_provider_cache", True)),
                    runtime_timing_overrides=runtime_timing_overrides,
                    zero_result_retry_attempts=zero_result_retry_attempts,
                    zero_result_retry_backoff_seconds=zero_result_retry_backoff_seconds,
                )
            harvest_result = harvest_plan.get("initial_result")
            probe_result = harvest_plan.get("probe_result")
            used_probe_fallback = False
            used_chunked_scale_fallback = False
            if harvest_result is None:
                harvest_result = self._search_harvest_profiles_with_budget(
                    query_text=effective_query_text,
                    filter_hints=filter_hints,
                    employment_status=employment_status,
                    discovery_dir=discovery_dir,
                    asset_logger=asset_logger,
                    limit=int(harvest_plan.get("effective_limit") or limit),
                    pages=int(harvest_plan.get("effective_pages") or page_count),
                    start_page=1,
                    allow_shared_provider_cache=bool(cost_policy.get("allow_shared_provider_cache", True)),
                    auto_probe=False,
                    runtime_timing_overrides=runtime_timing_overrides,
                    zero_result_retry_attempts=zero_result_retry_attempts,
                    zero_result_retry_backoff_seconds=zero_result_retry_backoff_seconds,
                )
            if harvest_result is None:
                if isinstance(probe_result, dict) and list(probe_result.get("rows") or []):
                    harvest_result = probe_result
                    used_probe_fallback = True
                else:
                    return {"query_entries": [], "query_summary": None, "account_used": ""}

            rows = list(harvest_result.get("rows") or [])
            if not rows and isinstance(probe_result, dict) and list(probe_result.get("rows") or []):
                chunked_result = self._search_harvest_profiles_with_page_chunks(
                    query_text=effective_query_text,
                    filter_hints=filter_hints,
                    employment_status=employment_status,
                    discovery_dir=discovery_dir,
                    asset_logger=asset_logger,
                    harvest_plan=harvest_plan,
                    probe_result=probe_result,
                    cost_policy=cost_policy,
                    allow_shared_provider_cache=bool(cost_policy.get("allow_shared_provider_cache", True)),
                    runtime_timing_overrides=runtime_timing_overrides,
                    zero_result_retry_attempts=zero_result_retry_attempts,
                    zero_result_retry_backoff_seconds=zero_result_retry_backoff_seconds,
                )
                harvest_result = chunked_result or probe_result
                rows = list(harvest_result.get("rows") or [])
                used_probe_fallback = True
                used_chunked_scale_fallback = bool(chunked_result)
            chunked_scale_fallback = dict(harvest_result.get("chunked_scale_fallback") or {})
            chunked_scale_incomplete = bool(chunked_scale_fallback.get("incomplete"))
            effective_limit = max(1, int(harvest_plan.get("effective_limit") or limit))
            query_entries: list[dict[str, Any]] = []
            for row in rows[:effective_limit]:
                entry = {
                    "seed_key": _seed_key(str(row.get("full_name") or ""), str(row.get("profile_url") or summary_query)),
                    "full_name": str(row.get("full_name") or "").strip(),
                    "headline": str(row.get("headline") or "").strip(),
                    "location": str(row.get("location") or "").strip(),
                    "source_type": "harvest_profile_search",
                    "source_query": summary_query,
                    "profile_url": str(row.get("profile_url") or "").strip(),
                    "slug": str(row.get("username") or "").strip() or extract_linkedin_slug(str(row.get("profile_url") or "")),
                    "employment_status": employment_status,
                    "target_company": identity.canonical_name,
                    "metadata": {
                        "provider_account_id": "harvest_profile_search",
                        "current_company": str(row.get("current_company") or "").strip(),
                        "scope_keywords": list(filter_hints.get("scope_keywords") or []),
                    },
                }
                if entry["full_name"]:
                    query_entries.append(entry)
            query_summary = {
                "query": summary_query,
                "effective_query_text": effective_query_text,
                "mode": "harvest_profile_search",
                "raw_path": str(harvest_result.get("raw_path") or ""),
                "account_id": "harvest_profile_search",
                "requested_limit": max(1, int(limit or 25)),
                "requested_pages": max(1, int(page_count or 1)),
                "effective_limit": effective_limit,
                "effective_pages": max(1, int(harvest_plan.get("effective_pages") or page_count)),
                "pagination": dict(harvest_result.get("pagination") or {}),
                "probe": _harvest_search_plan_summary(harvest_plan),
                "seed_entry_count": len(query_entries),
            }
            zero_retry_summaries = _collect_harvest_zero_result_retry_summaries(
                harvest_plan,
                harvest_result,
            )
            if zero_retry_summaries:
                query_summary["zero_result_retry"] = zero_retry_summaries
            if used_probe_fallback:
                if used_chunked_scale_fallback and chunked_scale_incomplete:
                    query_summary["result_source"] = "chunked_scale_partial_fallback"
                elif used_chunked_scale_fallback:
                    query_summary["result_source"] = "chunked_scale_fallback"
                else:
                    query_summary["result_source"] = "probe_fallback"
                query_summary["fallback_reason"] = "scaled_harvest_profile_search_returned_no_rows"
                if (
                    not used_chunked_scale_fallback
                    or chunked_scale_incomplete
                    or bool(chunked_scale_fallback.get("coverage_degraded"))
                ):
                    query_summary["status"] = "degraded"
                    query_summary["provider_search_degraded"] = True
                    query_summary["degraded_reason"] = (
                        "provider_reported_variable_or_unreliable_page_coverage_after_probe"
                    )
            if (
                not query_entries
                and _harvest_result_zero_retry_exhausted(harvest_result)
                and not used_probe_fallback
            ):
                if bool(dict(cost_policy or {}).get("provider_people_search_accept_zero_results")):
                    query_summary["status"] = "completed"
                    query_summary["zero_result_accepted"] = True
                    query_summary["zero_result_reason"] = "accepted_scoped_lane_zero_result_after_retry"
                else:
                    query_summary["status"] = "incomplete"
                    query_summary["provider_search_incomplete"] = True
                    query_summary["incomplete_reason"] = "provider_zero_results_after_retry"
            if used_chunked_scale_fallback:
                query_summary["chunked_scale_fallback"] = chunked_scale_fallback
            item_status = "completed"
            item_phase = "completed"
            item_reason = "provider_result_persisted"
            if str(query_summary.get("incomplete_reason") or "") == "provider_zero_results_after_retry":
                item_status = "exhausted"
                item_phase = "exhausted"
                item_reason = "provider_zero_result_retry_exhausted"
            terminal_item = _record_provider_query_item(
                index=index,
                summary_query=summary_query,
                effective_query_text=effective_query_text,
                status=item_status,
                phase=item_phase,
                reason=item_reason,
                metadata={
                    "summary": query_summary,
                    "entry_count": len(query_entries),
                    "raw_path": str(query_summary.get("raw_path") or ""),
                    "provider_query_status": item_status,
                    "linked_provider_search_retry_required": item_status == "exhausted",
                },
            )
            if terminal_item:
                query_summary["discovery_query_item_id"] = str(terminal_item.get("item_id") or "")
                query_summary["search_seed_discovery_query_item_id"] = str(terminal_item.get("item_id") or "")
                query_summary["discovery_query_item_status"] = str(terminal_item.get("status") or "")
            return {
                "query_entries": query_entries,
                "query_summary": query_summary,
                "account_used": "harvest_profile_search" if query_entries else "",
            }

        parallel_harvest_results: dict[int, dict[str, Any]] = {}
        emitted_incremental_indexes: set[int] = set()
        if (
            deduped_queries
            and not stop_after_first_hit
            and self._harvest_people_search_enabled()
        ):
            parallel_query_workers = resolved_provider_people_search_parallel_queries(
                runtime_timing_overrides,
                cost_policy=cost_policy,
                query_count=len(deduped_queries),
                default=4,
            )
            with ThreadPoolExecutor(max_workers=parallel_query_workers) as executor:
                future_to_query = {
                    executor.submit(
                        _run_harvest_query,
                        index,
                        query_text or "__past_company_only__",
                        query_text,
                        precomputed_plan=dict(precomputed_harvest_plans.get(query_text) or {}),
                    ): (index, query_text)
                    for index, query_text in enumerate(deduped_queries, start=1)
                }
                for future in as_completed(future_to_query):
                    index, _query_text = future_to_query[future]
                    try:
                        harvest_payload = dict(future.result() or {})
                    except Exception as exc:
                        harvest_payload = _retryable_provider_failure_payload(
                            index=index,
                            summary_query=_query_text or "__past_company_only__",
                            query_text=_query_text,
                            error=exc,
                        )
                    parallel_harvest_results[index] = harvest_payload
                    query_entries = list(harvest_payload.get("query_entries") or [])
                    query_summary = dict(harvest_payload.get("query_summary") or {})
                    if query_entries and query_summary:
                        _emit_incremental_provider_result(
                            index=index,
                            query_entries=query_entries,
                            query_summary=query_summary,
                        )
                        emitted_incremental_indexes.add(index)

        for index, query_text in enumerate(deduped_queries, start=1):
            summary_query = query_text or "__past_company_only__"
            if self._harvest_people_search_enabled():
                if not stop_after_first_hit and parallel_harvest_results:
                    harvest_payload = dict(parallel_harvest_results.get(index) or {})
                else:
                    try:
                        harvest_payload = _run_harvest_query(
                            index,
                            summary_query,
                            query_text,
                            precomputed_plan=dict(precomputed_harvest_plans.get(query_text) or {}),
                        )
                    except Exception as exc:
                        harvest_payload = _retryable_provider_failure_payload(
                            index=index,
                            summary_query=summary_query,
                            query_text=query_text,
                            error=exc,
                        )
                query_entries = list(harvest_payload.get("query_entries") or [])
                query_summary = dict(harvest_payload.get("query_summary") or {})
                account_used = str(harvest_payload.get("account_used") or "").strip()
                if query_entries:
                    entries.extend(query_entries)
                if query_summary:
                    query_summaries.append(query_summary)
                if account_used and account_used not in accounts_used:
                    accounts_used.append(account_used)
                if query_entries and query_summary and index not in emitted_incremental_indexes:
                    _emit_incremental_provider_result(
                        index=index,
                        query_entries=query_entries,
                        query_summary=query_summary,
                    )
                if query_entries and stop_after_first_hit:
                    break
            if not self._rapidapi_people_search_enabled(runtime_dir=discovery_dir):
                continue
            if not query_text:
                continue
            payload, account, provider_errors = self._search_people(
                query_text,
                limit=min(limit, 25),
                runtime_dir=discovery_dir,
            )
            errors.extend(provider_errors)
            raw_path = discovery_dir / f"provider_query_{index:02d}.json"
            if payload is None or account is None:
                query_summaries.append(
                    {
                        "query": summary_query,
                        "mode": "provider_people_search",
                        "raw_path": str(raw_path),
                        "seed_entry_count": 0,
                    }
                )
                continue
            asset_logger.write_json(
                raw_path,
                payload,
                asset_type="provider_people_search_payload",
                source_kind="search_seed_discovery",
                is_raw_asset=True,
                model_safe=False,
                metadata={"query": query_text, "account_id": account.account_id},
            )
            if account.account_id not in accounts_used:
                accounts_used.append(account.account_id)
            rows = extract_people_search_rows(payload)
            query_entries: list[dict[str, Any]] = []
            for row in rows[:limit]:
                entry = {
                    "seed_key": _seed_key(str(row.get("full_name") or ""), str(row.get("profile_url") or row.get("urn") or query_text)),
                    "full_name": str(row.get("full_name") or "").strip(),
                    "headline": str(row.get("headline") or "").strip(),
                    "location": str(row.get("location") or "").strip(),
                    "source_type": "provider_people_search",
                    "source_query": query_text,
                    "profile_url": str(row.get("profile_url") or "").strip(),
                    "slug": str(row.get("username") or "").strip() or extract_linkedin_slug(str(row.get("profile_url") or "")),
                    "employment_status": employment_status,
                    "target_company": identity.canonical_name,
                    "metadata": {
                        "provider_account_id": account.account_id,
                        "urn": str(row.get("urn") or "").strip(),
                        "scope_keywords": list(filter_hints.get("scope_keywords") or []),
                    },
                }
                if entry["full_name"]:
                    query_entries.append(entry)
                    entries.append(entry)
            query_summaries.append(
                {
                    "query": summary_query,
                    "mode": "provider_people_search",
                    "raw_path": str(raw_path),
                    "account_id": account.account_id,
                    "seed_entry_count": len(query_entries),
                }
            )
            if query_entries:
                _emit_incremental_provider_result(
                    index=index,
                    query_entries=query_entries,
                    query_summary=dict(query_summaries[-1] or {}),
                )
            if query_entries and stop_after_first_hit:
                break
        return entries, query_summaries, errors, accounts_used

    def _resolve_harvest_search_execution_plan(
        self,
        *,
        query_text: str,
        filter_hints: dict[str, list[str]],
        employment_status: str,
        discovery_dir: Path,
        asset_logger: AssetLogger | None,
        requested_limit: int,
        requested_pages: int,
        allow_shared_provider_cache: bool,
        runtime_timing_overrides: dict[str, Any] | None = None,
        zero_result_retry_attempts: int = 0,
        zero_result_retry_backoff_seconds: float = 0.0,
    ) -> dict[str, Any]:
        plan = {
            "probe_performed": False,
            "probe_query_text": str(query_text or "").strip(),
            "probe_limit": 25,
            "probe_pages": 1,
            "requested_limit": max(1, int(requested_limit or 25)),
            "requested_pages": max(1, int(requested_pages or 1)),
            "effective_limit": max(1, int(requested_limit or 25)),
            "effective_pages": max(1, int(requested_pages or 1)),
            "provider_total_count": 0,
            "provider_total_pages": 0,
            "probe_returned_count": 0,
            "probe_raw_path": "",
            "initial_result": None,
            "probe_result": None,
        }
        if self.harvest_search_connector is None or not harvest_connector_available(
            self.harvest_search_connector.settings
        ):
            return plan
        company_scoped_search = bool(
            list(filter_hints.get("past_companies") or [])
            or list(filter_hints.get("current_companies") or [])
        )
        if not company_scoped_search:
            return plan
        requested_limit_value = int(plan["requested_limit"])
        requested_pages_value = int(plan["requested_pages"])
        former_past_company_scan = (
            str(employment_status or "").strip().lower() == "former"
            and bool(list(filter_hints.get("past_companies") or []))
            and not str(query_text or "").strip()
        )
        should_probe = requested_limit_value > 25 or requested_pages_value > 1 or former_past_company_scan
        if not should_probe:
            return plan
        probe_result = self._search_harvest_profiles_with_budget(
            query_text=query_text,
            filter_hints=filter_hints,
            employment_status=employment_status,
            discovery_dir=discovery_dir,
            asset_logger=asset_logger,
            limit=25,
            pages=1,
            allow_shared_provider_cache=allow_shared_provider_cache,
            auto_probe=False,
            runtime_timing_overrides=runtime_timing_overrides,
            zero_result_retry_attempts=zero_result_retry_attempts,
            zero_result_retry_backoff_seconds=zero_result_retry_backoff_seconds,
        )
        plan["probe_performed"] = True
        if probe_result is None:
            return plan
        plan["probe_result"] = probe_result
        pagination = dict(probe_result.get("pagination") or {})
        total_count = max(0, int(pagination.get("total_elements") or 0))
        total_pages = max(0, int(pagination.get("total_pages") or 0))
        probe_returned_count = max(0, int(pagination.get("returned_count") or len(list(probe_result.get("rows") or []))))
        plan["provider_total_count"] = total_count
        plan["provider_total_pages"] = total_pages
        plan["probe_returned_count"] = probe_returned_count
        plan["probe_raw_path"] = str(probe_result.get("raw_path") or "")
        probe_zero_retry = dict(probe_result.get("zero_result_retry") or {})
        if probe_zero_retry:
            plan["probe_zero_result_retry"] = probe_zero_retry
        if total_count > 0:
            effective_pages = max(1, total_pages or ((total_count + 24) // 25))
            plan["effective_limit"] = total_count
            plan["effective_pages"] = effective_pages
            if effective_pages == 1 and probe_returned_count >= total_count:
                plan["initial_result"] = probe_result
            return plan
        plan["initial_result"] = probe_result
        return plan

    def _search_harvest_profiles_with_budget(
        self,
        *,
        query_text: str,
        filter_hints: dict[str, list[str]],
        employment_status: str,
        discovery_dir: Path,
        asset_logger: AssetLogger | None,
        limit: int,
        pages: int,
        start_page: int = 1,
        allow_shared_provider_cache: bool,
        auto_probe: bool,
        runtime_timing_overrides: dict[str, Any] | None = None,
        zero_result_retry_attempts: int = 0,
        zero_result_retry_backoff_seconds: float = 0.0,
    ) -> dict[str, Any] | None:
        if self.harvest_search_connector is None:
            return None
        budget = resolved_harvest_people_search_global_inflight(runtime_timing_overrides)
        request_lane = _harvest_profile_search_request_lane(
            discovery_dir=discovery_dir,
            query_text=query_text,
            filter_hints=filter_hints,
            employment_status=employment_status,
            limit=limit,
            pages=pages,
            start_page=start_page,
            allow_shared_provider_cache=allow_shared_provider_cache,
            auto_probe=auto_probe,
            zero_result_retry_attempts=zero_result_retry_attempts,
            zero_result_retry_backoff_seconds=zero_result_retry_backoff_seconds,
        )
        with runtime_inflight_slot(
            "harvest_people_search",
            budget=budget,
            metadata={
                "query_text": str(query_text or "").strip(),
                "employment_status": str(employment_status or "").strip(),
                "start_page": max(1, int(start_page or 1)),
            },
        ), runtime_inflight_slot(
            request_lane,
            budget=1,
            metadata={
                "query_text": str(query_text or "").strip(),
                "employment_status": str(employment_status or "").strip(),
                "start_page": max(1, int(start_page or 1)),
            },
        ):
            return self.harvest_search_connector.search_profiles(
                query_text=query_text,
                filter_hints=filter_hints,
                employment_status=employment_status,
                discovery_dir=discovery_dir,
                asset_logger=asset_logger,
                limit=limit,
                pages=pages,
                start_page=start_page,
                allow_shared_provider_cache=allow_shared_provider_cache,
                auto_probe=auto_probe,
                runtime_timing_overrides=runtime_timing_overrides,
                zero_result_retry_attempts=zero_result_retry_attempts,
                zero_result_retry_backoff_seconds=zero_result_retry_backoff_seconds,
            )

    def _search_harvest_profiles_with_page_chunks(
        self,
        *,
        query_text: str,
        filter_hints: dict[str, list[str]],
        employment_status: str,
        discovery_dir: Path,
        asset_logger: AssetLogger | None,
        harvest_plan: dict[str, Any],
        probe_result: dict[str, Any],
        cost_policy: dict[str, Any],
        allow_shared_provider_cache: bool,
        runtime_timing_overrides: dict[str, Any] | None = None,
        zero_result_retry_attempts: int = 0,
        zero_result_retry_backoff_seconds: float = 0.0,
    ) -> dict[str, Any] | None:
        probe_rows = list(probe_result.get("rows") or [])
        if not probe_rows:
            return None
        total_pages = max(0, int(harvest_plan.get("provider_total_pages") or 0))
        total_count = max(0, int(harvest_plan.get("provider_total_count") or 0))
        if total_pages <= 1 and total_count <= len(probe_rows):
            return None
        try:
            chunk_pages = int(cost_policy.get("provider_people_search_scale_chunk_pages") or 5)
        except (TypeError, ValueError):
            chunk_pages = 5
        chunk_pages = max(1, min(chunk_pages, 10))
        effective_limit = max(1, int(harvest_plan.get("effective_limit") or total_count or len(probe_rows)))
        effective_pages = max(1, int(harvest_plan.get("effective_pages") or total_pages or 1))
        target_pages = max(1, min(effective_pages, total_pages or effective_pages))
        combined_rows = list(probe_rows)
        chunk_paths: list[str] = []
        zero_result_retry_events: list[dict[str, Any]] = []
        chunk_count = 0
        single_page_retry_count = 0
        empty_page_ranges: list[dict[str, int]] = []
        for start_page in range(2, target_pages + 1, chunk_pages):
            if len(combined_rows) >= effective_limit:
                break
            pages = min(chunk_pages, target_pages - start_page + 1)
            result = self._search_harvest_profiles_with_budget(
                query_text=query_text,
                filter_hints=filter_hints,
                employment_status=employment_status,
                discovery_dir=discovery_dir,
                asset_logger=asset_logger,
                limit=pages * 25,
                pages=pages,
                start_page=start_page,
                allow_shared_provider_cache=allow_shared_provider_cache,
                auto_probe=False,
                runtime_timing_overrides=runtime_timing_overrides,
                zero_result_retry_attempts=zero_result_retry_attempts,
                zero_result_retry_backoff_seconds=zero_result_retry_backoff_seconds,
            )
            if result is None:
                empty_page_ranges.append({"start_page": start_page, "pages": pages})
                continue
            chunk_count += 1
            raw_path = str(result.get("raw_path") or "").strip()
            if raw_path:
                chunk_paths.append(raw_path)
            zero_retry = dict(result.get("zero_result_retry") or {})
            if zero_retry:
                zero_result_retry_events.append(
                    {"start_page": start_page, "pages": pages, **zero_retry}
                )
            result_rows = list(result.get("rows") or [])
            if not result_rows and pages > 1:
                recovered_single_page_rows = 0
                for page_number in range(start_page, start_page + pages):
                    if len(combined_rows) >= effective_limit:
                        break
                    single_result = self._search_harvest_profiles_with_budget(
                        query_text=query_text,
                        filter_hints=filter_hints,
                        employment_status=employment_status,
                        discovery_dir=discovery_dir,
                        asset_logger=asset_logger,
                        limit=25,
                        pages=1,
                        start_page=page_number,
                        allow_shared_provider_cache=allow_shared_provider_cache,
                        auto_probe=False,
                        runtime_timing_overrides=runtime_timing_overrides,
                        zero_result_retry_attempts=zero_result_retry_attempts,
                        zero_result_retry_backoff_seconds=zero_result_retry_backoff_seconds,
                    )
                    single_page_retry_count += 1
                    if single_result is None:
                        empty_page_ranges.append({"start_page": page_number, "pages": 1})
                        continue
                    single_raw_path = str(single_result.get("raw_path") or "").strip()
                    if single_raw_path:
                        chunk_paths.append(single_raw_path)
                    single_zero_retry = dict(single_result.get("zero_result_retry") or {})
                    if single_zero_retry:
                        zero_result_retry_events.append(
                            {"start_page": page_number, "pages": 1, **single_zero_retry}
                        )
                    single_rows = list(single_result.get("rows") or [])
                    if not single_rows:
                        empty_page_ranges.append({"start_page": page_number, "pages": 1})
                        continue
                    recovered_single_page_rows += len(single_rows)
                    combined_rows.extend(single_rows)
                if recovered_single_page_rows <= 0:
                    empty_page_ranges.append({"start_page": start_page, "pages": pages})
                continue
            if not result_rows:
                empty_page_ranges.append({"start_page": start_page, "pages": pages})
                continue
            combined_rows.extend(result_rows)
        if len(combined_rows) <= len(probe_rows):
            return None
        pagination = dict(probe_result.get("pagination") or {})
        if total_count:
            pagination["total_elements"] = total_count
        if total_pages:
            pagination["total_pages"] = total_pages
        returned_count = len(combined_rows[:effective_limit])
        expected_count = min(effective_limit, total_count or effective_limit)
        coverage_degraded = bool(empty_page_ranges) or (expected_count > 0 and returned_count < expected_count)
        pagination["returned_count"] = returned_count
        return {
            "raw_path": str(probe_result.get("raw_path") or ""),
            "account_id": "harvest_profile_search",
            "rows": combined_rows[:effective_limit],
            "pagination": pagination,
            "payload": {
                "probe_payload": probe_result.get("payload"),
                "chunk_raw_paths": chunk_paths,
            },
            "chunked_scale_fallback": {
                "chunk_count": chunk_count,
                "chunk_pages": chunk_pages,
                "single_page_retry_count": single_page_retry_count,
                "chunk_raw_paths": chunk_paths,
                "provider_total_count": total_count,
                "provider_total_pages": total_pages,
                "expected_count": expected_count,
                "returned_count": returned_count,
                "empty_page_ranges": empty_page_ranges,
                "coverage_degraded": coverage_degraded,
                "incomplete": coverage_degraded,
                "zero_result_retry_events": zero_result_retry_events,
            },
        }

    def _search_people(
        self,
        query_text: str,
        *,
        limit: int,
        runtime_dir: str | Path | None = None,
        provider_mode: str | None = None,
        runtime_environment: str | None = None,
    ) -> tuple[dict[str, Any] | None, RapidApiAccount | None, list[str]]:
        scoped_runtime_dir = self._provider_runtime_dir(runtime_dir)
        effective_provider_mode = provider_mode
        if scoped_runtime_dir is None and effective_provider_mode is None:
            effective_provider_mode = _external_provider_mode()
        if not self._rapidapi_people_search_enabled(
            runtime_dir=scoped_runtime_dir,
            provider_mode=effective_provider_mode,
            runtime_environment=runtime_environment,
        ):
            return None, None, []
        errors_seen: list[str] = []
        for account in self.accounts:
            if account.account_id in self._exhausted_account_ids:
                continue
            url = _build_people_search_url(account, query_text, limit=limit)
            assert_live_provider_access_allowed(
                provider_name="rapidapi_linkedin",
                operation="people_search",
                provider_mode=effective_provider_mode,
                runtime_dir=scoped_runtime_dir,
                runtime_environment=runtime_environment,
                payload={"query": query_text, "limit": limit, "host": account.host},
            )
            headers = {"x-rapidapi-host": account.host, "x-rapidapi-key": account.api_key, "User-Agent": "Mozilla/5.0"}
            http_request = request.Request(url, headers=headers, method="GET")
            try:
                with request.urlopen(http_request, timeout=30) as response:
                    return json.loads(response.read().decode("utf-8")), account, errors_seen
            except error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="ignore")
                if exc.code == 429:
                    self._exhausted_account_ids.add(account.account_id)
                errors_seen.append(f"provider_people_search:{account.account_id}:{exc.code}:{detail[:120]}")
            except Exception as exc:
                errors_seen.append(f"provider_people_search:{account.account_id}:{str(exc)[:120]}")
        return None, None, errors_seen


def _prepare_batched_search_seed_queries(
    *,
    search_provider: BaseSearchProvider,
    logger: AssetLogger,
    discovery_dir: Path,
    pending_specs: list[dict[str, Any]],
    result_limit: int,
) -> list[str]:
    manifest_path = discovery_dir / "web_search_batch_manifest.json"
    manifest_entries = _load_search_batch_manifest_entries(manifest_path)
    pending_by_key = {
        str(spec.get("worker_key") or "").strip(): spec
        for spec in pending_specs
        if str(spec.get("worker_key") or "").strip()
    }
    for task_key, spec in pending_by_key.items():
        entry = dict(manifest_entries.get(task_key) or {})
        search_state = dict(entry.get("search_state") or {})
        if str(search_state.get("task_id") or "").strip():
            spec["prefetched_search_state"] = search_state
            spec["prefetched_search_artifact_paths"] = dict(entry.get("artifact_paths") or {})
            spec["prefetched_search_raw_path"] = str(entry.get("raw_path") or "")
            spec["prefetched_search_manifest_path"] = str(manifest_path)
            spec["prefetched_search_manifest_key"] = task_key

    unresolved_requests: list[dict[str, Any]] = []
    for task_key, spec in pending_by_key.items():
        if dict(spec.get("prefetched_search_state") or {}).get("task_id"):
            continue
        query_text = str(dict(spec.get("query_spec") or {}).get("query") or "").strip()
        if not query_text:
            continue
        unresolved_requests.append(
            {
                "task_key": task_key,
                "query_text": query_text,
                "max_results": result_limit,
                "runtime_timing_overrides": dict(spec.get("runtime_timing_overrides") or {}),
            }
        )
    provider_name = str(
        dict(next(iter(manifest_entries.values()), {})).get("search_state", {}).get("provider_name")
        or getattr(search_provider, "provider_name", "")
        or ""
    ).strip()
    artifact_paths: dict[str, str] = {}
    batch_message = ""
    submitted_query_count = 0

    def _write_manifest_snapshot() -> None:
        if not manifest_entries and not artifact_paths:
            return
        root_artifact_paths = {
            str(key): str(value)
            for key, value in dict(artifact_paths).items()
            if str(key).strip() and str(value).strip()
        }
        for entry in manifest_entries.values():
            for key, value in dict(entry.get("artifact_paths") or {}).items():
                normalized_key = str(key or "").strip()
                normalized_value = str(value or "").strip()
                if normalized_key and normalized_value and normalized_key not in root_artifact_paths:
                    root_artifact_paths[normalized_key] = normalized_value
        logger.write_json(
            manifest_path,
            {
                "provider_name": provider_name,
                "submitted_query_count": submitted_query_count,
                "artifact_paths": root_artifact_paths,
                "entries": [manifest_entries[key] for key in sorted(manifest_entries)],
                "message": batch_message,
                "updated_at": _batch_lane_timestamp(),
            },
            asset_type="web_search_batch_manifest",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=False,
        )

    if unresolved_requests:
        submit_batch = getattr(search_provider, "submit_batch_queries", None)
        if not callable(submit_batch):
            return []

        try:
            batch_result = submit_batch(unresolved_requests)
        except Exception as exc:
            return [f"web_search_batch_submit:{str(exc)[:160]}"]
        if batch_result is None:
            return []

        provider_name = str(getattr(batch_result, "provider_name", "") or provider_name).strip()
        batch_message = str(getattr(batch_result, "message", "") or "")
        submitted_query_count = len(batch_result.tasks or [])
        artifact_paths = {}
        for artifact in list(batch_result.artifacts or []):
            artifact_label = str(getattr(artifact, "label", "artifact") or "artifact")
            default_path = discovery_dir / f"web_search_{artifact_label}.json"
            artifact_path = _write_search_execution_artifact(
                logger=logger,
                artifact=artifact,
                default_path=default_path,
                asset_type="web_search_batch_queue_payload",
                source_kind="search_seed_discovery",
                metadata={"provider_name": provider_name, "submitted_query_count": len(unresolved_requests)},
            )
            artifact_paths[artifact_label] = str(artifact_path)

        for task in list(batch_result.tasks or []):
            task_key = str(getattr(task, "task_key", "") or "").strip()
            if not task_key:
                continue
            entry = {
                "task_key": task_key,
                "query": str(getattr(task, "query_text", "") or "").strip(),
                "search_state": dict(getattr(task, "checkpoint", {}) or {}),
                "artifact_paths": {},
                "metadata": dict(getattr(task, "metadata", {}) or {}),
            }
            artifact_label = str(entry["metadata"].get("artifact_label") or "").strip()
            if artifact_label and artifact_label in artifact_paths:
                entry["artifact_paths"] = {artifact_label: artifact_paths[artifact_label]}
            manifest_entries[task_key] = entry
            spec = pending_by_key.get(task_key)
            if spec is not None:
                spec["prefetched_search_state"] = dict(entry["search_state"] or {})
                spec["prefetched_search_artifact_paths"] = dict(entry["artifact_paths"] or {})
                spec["prefetched_search_manifest_path"] = str(manifest_path)
                spec["prefetched_search_manifest_key"] = task_key
        _write_manifest_snapshot()

    ready_errors = _refresh_batched_search_seed_ready_cache(
        search_provider=search_provider,
        logger=logger,
        discovery_dir=discovery_dir,
        manifest_path=manifest_path,
        manifest_entries=manifest_entries,
        pending_by_key=pending_by_key,
    )
    fetch_errors = _fetch_batched_search_seed_ready_results(
        search_provider=search_provider,
        logger=logger,
        discovery_dir=discovery_dir,
        manifest_path=manifest_path,
        manifest_entries=manifest_entries,
        pending_by_key=pending_by_key,
    )

    if not manifest_entries and not artifact_paths:
        return [*ready_errors, *fetch_errors]
    _write_manifest_snapshot()
    return [*ready_errors, *fetch_errors]


def _refresh_batched_search_seed_ready_cache(
    *,
    search_provider: BaseSearchProvider,
    logger: AssetLogger,
    discovery_dir: Path,
    manifest_path: Path,
    manifest_entries: dict[str, dict[str, Any]],
    pending_by_key: dict[str, dict[str, Any]],
) -> list[str]:
    poll_ready = getattr(search_provider, "poll_ready_batch", None)
    if not callable(poll_ready):
        return []

    poll_specs: list[dict[str, Any]] = []
    attempted_at = _batch_lane_timestamp()
    for task_key, spec in pending_by_key.items():
        entry = dict(manifest_entries.get(task_key) or {})
        search_state = dict(entry.get("search_state") or {})
        task_id = str(search_state.get("task_id") or "").strip()
        if not task_id:
            continue
        if str(search_state.get("status") or "").strip() in {"completed", "fetched_cached", "ready_cached"}:
            continue
        ready_poll_min_interval_seconds = resolved_lane_ready_cooldown_seconds(
            search_state,
            default=_lane_ready_poll_min_interval_seconds(),
        )
        if _timestamp_within_seconds(str(search_state.get("ready_attempted_at") or ""), ready_poll_min_interval_seconds):
            continue
        search_state["ready_attempted_at"] = attempted_at
        entry["search_state"] = search_state
        manifest_entries[task_key] = entry
        poll_specs.append(
            {
                "task_key": task_key,
                "query_text": str(entry.get("query") or dict(spec.get("query_spec") or {}).get("query") or "").strip(),
                "checkpoint": search_state,
            }
        )
    if not poll_specs:
        return []

    try:
        ready_result = poll_ready(poll_specs)
    except Exception as exc:
        return [f"web_search_ready_poll:{str(exc)[:160]}"]
    if ready_result is None:
        return []

    poll_token = _batch_lane_timestamp(compact=True)
    poll_artifact_paths: dict[str, str] = {}
    for artifact in list(ready_result.artifacts or []):
        artifact_label = str(getattr(artifact, "label", "artifact") or "artifact")
        artifact_key = f"{artifact_label}_{poll_token}"
        default_path = discovery_dir / f"web_search_{artifact_key}.json"
        artifact_path = _write_search_execution_artifact(
            logger=logger,
            artifact=artifact,
            default_path=default_path,
            asset_type="web_search_ready_poll_payload",
            source_kind="search_seed_discovery",
            metadata={"provider_name": ready_result.provider_name, "poll_token": poll_token},
        )
        poll_artifact_paths[artifact_key] = str(artifact_path)

    for task in list(ready_result.tasks or []):
        task_key = str(getattr(task, "task_key", "") or "").strip()
        if not task_key:
            continue
        entry = dict(manifest_entries.get(task_key) or {})
        search_state = dict(getattr(task, "checkpoint", {}) or {})
        search_state["ready_attempted_at"] = attempted_at
        search_state["ready_poll_token"] = poll_token
        search_state["ready_checked_at"] = _batch_lane_timestamp()
        search_state["ready_poll_source"] = "lane_batch"
        search_state["ready_poll_label"] = "tasks_ready_batch"
        entry["search_state"] = search_state
        entry_artifact_paths = {
            str(key): str(value)
            for key, value in dict(entry.get("artifact_paths") or {}).items()
            if str(key).strip() and str(value).strip()
        }
        entry_artifact_paths.update(poll_artifact_paths)
        entry["artifact_paths"] = entry_artifact_paths
        manifest_entries[task_key] = entry
        spec = pending_by_key.get(task_key)
        if spec is not None:
            spec["prefetched_search_state"] = search_state
            spec["prefetched_search_artifact_paths"] = dict(entry_artifact_paths)
            spec["prefetched_search_manifest_path"] = str(manifest_path)
            spec["prefetched_search_manifest_key"] = task_key
    return []


def _fetch_batched_search_seed_ready_results(
    *,
    search_provider: BaseSearchProvider,
    logger: AssetLogger,
    discovery_dir: Path,
    manifest_path: Path,
    manifest_entries: dict[str, dict[str, Any]],
    pending_by_key: dict[str, dict[str, Any]],
) -> list[str]:
    fetch_ready = getattr(search_provider, "fetch_ready_batch", None)
    if not callable(fetch_ready):
        return []

    fetch_specs: list[dict[str, Any]] = []
    attempted_at = _batch_lane_timestamp()
    for task_key, spec in pending_by_key.items():
        entry = dict(manifest_entries.get(task_key) or {})
        search_state = dict(entry.get("search_state") or {})
        if str(search_state.get("status") or "").strip() != "ready_cached":
            continue
        raw_path = str(entry.get("raw_path") or "").strip()
        if raw_path and Path(raw_path).exists():
            continue
        fetch_min_interval_seconds = resolved_lane_fetch_cooldown_seconds(
            search_state,
            default=_lane_fetch_min_interval_seconds(),
        )
        if _timestamp_within_seconds(str(search_state.get("fetch_attempted_at") or ""), fetch_min_interval_seconds):
            continue
        search_state["fetch_attempted_at"] = attempted_at
        search_state["lane_fetch_cooldown_seconds"] = fetch_min_interval_seconds
        entry["search_state"] = search_state
        manifest_entries[task_key] = entry
        fetch_specs.append(
            {
                "task_key": task_key,
                "query_text": str(entry.get("query") or dict(spec.get("query_spec") or {}).get("query") or "").strip(),
                "checkpoint": search_state,
            }
        )
    if not fetch_specs:
        return []

    try:
        fetch_result = fetch_ready(fetch_specs)
    except Exception as exc:
        return [f"web_search_task_get:{str(exc)[:160]}"]
    if fetch_result is None:
        return []

    fetch_token = _batch_lane_timestamp(compact=True)
    fetch_artifact_paths: dict[str, str] = {}
    for artifact in list(fetch_result.artifacts or []):
        artifact_label = str(getattr(artifact, "label", "artifact") or "artifact")
        artifact_key = f"{artifact_label}_{fetch_token}"
        default_path = discovery_dir / f"web_search_{artifact_key}.json"
        artifact_path = _write_search_execution_artifact(
            logger=logger,
            artifact=artifact,
            default_path=default_path,
            asset_type="web_search_task_get_payload",
            source_kind="search_seed_discovery",
            metadata={"provider_name": fetch_result.provider_name, "fetch_token": fetch_token},
        )
        fetch_artifact_paths[artifact_key] = str(artifact_path)

    for task in list(fetch_result.tasks or []):
        task_key = str(getattr(task, "task_key", "") or "").strip()
        if not task_key:
            continue
        spec = pending_by_key.get(task_key)
        entry = dict(manifest_entries.get(task_key) or {})
        response = getattr(task, "response", None)
        if response is None or spec is None:
            continue
        index = int(spec.get("index") or 0)
        default_path = discovery_dir / f"web_query_{index:02d}.html"
        raw_path = _write_search_response_raw_asset(
            logger=logger,
            response=response,
            default_path=default_path,
            asset_type="web_search_payload",
            source_kind="search_seed_discovery",
            metadata={"query": str(entry.get("query") or ""), "provider_name": response.provider_name},
        )
        search_state = dict(getattr(task, "checkpoint", {}) or {})
        search_state["fetch_attempted_at"] = attempted_at
        search_state["fetched_at"] = _batch_lane_timestamp()
        search_state["fetch_token"] = fetch_token
        search_state["lane_fetch_cooldown_seconds"] = fetch_min_interval_seconds
        entry["search_state"] = search_state
        entry["raw_path"] = str(raw_path)
        entry_artifact_paths = {
            str(key): str(value)
            for key, value in dict(entry.get("artifact_paths") or {}).items()
            if str(key).strip() and str(value).strip()
        }
        entry_artifact_paths.update(fetch_artifact_paths)
        entry["artifact_paths"] = entry_artifact_paths
        manifest_entries[task_key] = entry
        spec["prefetched_search_state"] = search_state
        spec["prefetched_search_artifact_paths"] = dict(entry_artifact_paths)
        spec["prefetched_search_raw_path"] = str(raw_path)
        spec["prefetched_search_manifest_path"] = str(manifest_path)
        spec["prefetched_search_manifest_key"] = task_key
    return []


def _load_search_batch_manifest_entries(manifest_path: Path) -> dict[str, dict[str, Any]]:
    if not manifest_path.exists():
        return {}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    entries: dict[str, dict[str, Any]] = {}
    for item in list(payload.get("entries") or []):
        task_key = str((item or {}).get("task_key") or "").strip()
        if task_key:
            entries[task_key] = dict(item or {})
    return entries


def _update_search_batch_manifest_entry(
    *,
    logger: AssetLogger,
    manifest_path: Path | None,
    task_key: str,
    search_state: dict[str, Any],
    artifact_paths: dict[str, str],
    raw_path: str,
) -> None:
    if manifest_path is None or not str(task_key or "").strip():
        return
    payload: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
    entries = _load_search_batch_manifest_entries(manifest_path)
    entry = dict(entries.get(task_key) or {})
    if not entry:
        return
    entry["search_state"] = dict(search_state or {})
    merged_artifact_paths = {
        str(key): str(value)
        for key, value in dict(entry.get("artifact_paths") or {}).items()
        if str(key).strip() and str(value).strip()
    }
    for key, value in dict(artifact_paths or {}).items():
        normalized_key = str(key or "").strip()
        normalized_value = str(value or "").strip()
        if normalized_key and normalized_value and normalized_key not in merged_artifact_paths:
            merged_artifact_paths[normalized_key] = normalized_value
    if merged_artifact_paths:
        entry["artifact_paths"] = merged_artifact_paths
    normalized_raw_path = str(raw_path or "").strip()
    if normalized_raw_path:
        entry["raw_path"] = normalized_raw_path
    entries[task_key] = entry

    root_artifact_paths = {
        str(key): str(value)
        for key, value in dict(payload.get("artifact_paths") or {}).items()
        if str(key).strip() and str(value).strip()
    }
    for key, value in merged_artifact_paths.items():
        if key not in root_artifact_paths:
            root_artifact_paths[key] = value
    logger.write_json(
        manifest_path,
        {
            "provider_name": str(payload.get("provider_name") or ""),
            "submitted_query_count": int(payload.get("submitted_query_count") or 0),
            "artifact_paths": root_artifact_paths,
            "entries": [entries[key] for key in sorted(entries)],
            "message": str(payload.get("message") or ""),
            "updated_at": _batch_lane_timestamp(),
        },
        asset_type="web_search_batch_manifest",
        source_kind="search_seed_discovery",
        is_raw_asset=False,
        model_safe=False,
    )


def _mark_search_state_as_worker_fetched(
    existing_state: dict[str, Any],
    *,
    fallback_state: dict[str, Any] | None = None,
    query_text: str,
) -> dict[str, Any]:
    normalized_existing = dict(existing_state or {})
    normalized_fallback = dict(fallback_state or {})
    task_id = str(normalized_existing.get("task_id") or normalized_fallback.get("task_id") or "").strip()
    if not task_id:
        return {}
    current_timestamp = _batch_lane_timestamp()
    fetch_token = str(normalized_existing.get("fetch_token") or normalized_fallback.get("fetch_token") or "").strip()
    if not fetch_token:
        fetch_token = f"worker_direct_{_batch_lane_timestamp(compact=True)}"
    return {
        **normalized_fallback,
        **normalized_existing,
        "task_id": task_id,
        "query_text": str(normalized_existing.get("query_text") or normalized_fallback.get("query_text") or query_text),
        "status": "fetched_cached",
        "fetch_attempted_at": str(
            normalized_existing.get("fetch_attempted_at")
            or normalized_fallback.get("fetch_attempted_at")
            or current_timestamp
        ),
        "fetched_at": str(
            normalized_existing.get("fetched_at")
            or normalized_fallback.get("fetched_at")
            or current_timestamp
        ),
        "fetch_token": fetch_token,
    }


def _merge_prefetched_search_checkpoint(
    *,
    checkpoint: dict[str, Any],
    prefetched_search_state: dict[str, Any],
    prefetched_search_artifact_paths: dict[str, str],
    prefetched_search_raw_path: str,
    manifest_path: Path | None,
    manifest_key: str,
) -> tuple[dict[str, Any], bool]:
    updated = dict(checkpoint or {})
    existing_state = dict(updated.get("search_state") or {})

    recovered_entry: dict[str, Any] = {}
    if not prefetched_search_state and manifest_path is not None and manifest_path.exists() and manifest_key:
        recovered_entry = dict(_load_search_batch_manifest_entries(manifest_path).get(manifest_key) or {})
        prefetched_search_state = dict(recovered_entry.get("search_state") or {})
        prefetched_search_artifact_paths = {
            str(key): str(value)
            for key, value in dict(recovered_entry.get("artifact_paths") or {}).items()
            if str(key).strip() and str(value).strip()
        }
        prefetched_search_raw_path = str(recovered_entry.get("raw_path") or prefetched_search_raw_path or "").strip()

    if not str(dict(prefetched_search_state or {}).get("task_id") or "").strip():
        return updated, False

    existing_task_id = str(existing_state.get("task_id") or "").strip()
    prefetched_task_id = str(dict(prefetched_search_state or {}).get("task_id") or "").strip()
    should_replace = False
    if not existing_task_id:
        should_replace = True
    elif existing_task_id == prefetched_task_id:
        existing_poll = str(existing_state.get("ready_poll_token") or "").strip()
        prefetched_poll = str(dict(prefetched_search_state or {}).get("ready_poll_token") or "").strip()
        existing_status = str(existing_state.get("status") or "").strip()
        prefetched_status = str(dict(prefetched_search_state or {}).get("status") or "").strip()
        existing_raw_path = str(updated.get("raw_path") or "").strip()
        prefetched_raw_path = str(prefetched_search_raw_path or "").strip()
        should_replace = (
            prefetched_poll != existing_poll
            or prefetched_status != existing_status
            or (prefetched_raw_path and prefetched_raw_path != existing_raw_path)
        )
    if not should_replace:
        return updated, False

    merged_artifact_paths = {
        str(key): str(value)
        for key, value in dict(updated.get("search_artifact_paths") or {}).items()
        if str(key).strip() and str(value).strip()
    }
    for key, value in dict(prefetched_search_artifact_paths or {}).items():
        normalized_key = str(key or "").strip()
        normalized_value = str(value or "").strip()
        if normalized_key and normalized_value and normalized_key not in merged_artifact_paths:
            merged_artifact_paths[normalized_key] = normalized_value
    updated["search_state"] = dict(prefetched_search_state or {})
    if merged_artifact_paths:
        updated["search_artifact_paths"] = merged_artifact_paths
    normalized_raw_path = str(prefetched_search_raw_path or "").strip()
    if normalized_raw_path:
        current_raw_path = str(updated.get("raw_path") or "").strip()
        if not current_raw_path or not Path(current_raw_path).exists():
            updated["raw_path"] = normalized_raw_path
    if manifest_path is not None and str(manifest_key or "").strip():
        updated["search_manifest_path"] = str(manifest_path)
        updated["search_manifest_key"] = str(manifest_key)
    return updated, True


def _batch_lane_timestamp(*, compact: bool = False) -> str:
    current = datetime.now(timezone.utc)
    if compact:
        return current.strftime("%Y%m%dT%H%M%SZ")
    return current.isoformat()


def _timestamp_within_seconds(value: str, seconds: int) -> bool:
    normalized = str(value or "").strip()
    if not normalized:
        return False
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds() < max(0, int(seconds or 0))


def build_candidates_from_seed_entries(
    *,
    company_identity: CompanyIdentity,
    target_company: str,
    entries: list[dict[str, Any]],
    source_path: str,
    dataset_name: str = "",
) -> tuple[list[Candidate], list[EvidenceRecord]]:
    normalized_source_path = str(source_path or "").strip()
    effective_dataset_name = str(dataset_name or "").strip() or f"{company_identity.company_key}_search_seed_candidates"
    candidates: list[Candidate] = []
    evidence_items: list[EvidenceRecord] = []
    for row in list(entries or []):
        full_name = str(row.get("full_name") or "").strip()
        if not full_name:
            continue
        seed_reference = str(row.get("profile_url") or row.get("slug") or row.get("source_query") or normalized_source_path).strip()
        candidate_id = sha1(
            "|".join([normalize_name_token(target_company), normalize_name_token(full_name), seed_reference]).encode("utf-8")
        ).hexdigest()[:16]
        profile_url = str(row.get("profile_url") or "").strip()
        slug = str(row.get("slug") or "").strip()
        candidate = Candidate(
            candidate_id=candidate_id,
            name_en=full_name,
            display_name=format_display_name(full_name, ""),
            category=_seed_candidate_category(row),
            target_company=target_company,
            organization=target_company,
            employment_status=str(row.get("employment_status") or "current"),
            role=str(row.get("headline") or "").strip(),
            team="",
            focus_areas=str(row.get("headline") or "").strip(),
            notes=_build_seed_notes(row),
            linkedin_url=profile_url,
            source_dataset=effective_dataset_name,
            source_path=normalized_source_path,
            metadata={
                "seed_slug": slug,
                "seed_query": str(row.get("source_query") or ""),
                "seed_source_type": str(row.get("source_type") or ""),
                "seed_location": str(row.get("location") or ""),
                **dict(row.get("metadata") or {}),
            },
        )
        candidates.append(candidate)
        evidence_items.append(
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    candidate_id,
                    effective_dataset_name,
                    candidate.role or "Search seed",
                    profile_url or normalized_source_path,
                ),
                candidate_id=candidate_id,
                source_type=str(row.get("source_type") or "search_seed"),
                title=candidate.role or "Search seed",
                url=profile_url,
                summary=f"{full_name} was discovered as a search-seed candidate for {target_company}.",
                source_dataset=effective_dataset_name,
                source_path=normalized_source_path,
                metadata={"source_query": str(row.get("source_query") or ""), "slug": slug},
            )
        )
    return candidates, evidence_items


def build_candidates_from_seed_snapshot(snapshot: SearchSeedSnapshot) -> tuple[list[Candidate], list[EvidenceRecord]]:
    return build_candidates_from_seed_entries(
        company_identity=snapshot.company_identity,
        target_company=snapshot.target_company,
        entries=list(snapshot.entries or []),
        source_path=str(snapshot.summary_path),
    )


def extract_web_search_results(html_text: str) -> list[dict[str, str]]:
    return [item.to_record() for item in parse_duckduckgo_html_results(html_text)]


def infer_name_from_result_title(title: str, identity: CompanyIdentity) -> str:
    candidate = title
    for token in [" - LinkedIn", "| LinkedIn", " | LinkedIn", " - ", " | "]:
        if token in candidate:
            candidate = candidate.split(token, 1)[0]
            break
    candidate = candidate.replace(identity.canonical_name, " ")
    for alias in identity.aliases:
        candidate = candidate.replace(alias, " ")
    candidate = re.sub(r"\bLinkedIn\b", " ", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\s*[-|]+\s*$", " ", candidate).strip()
    candidate = " ".join(candidate.split())
    if len(candidate.split()) < 2:
        return ""
    return candidate


def extract_linkedin_slug(url: str) -> str:
    match = re.search(r"linkedin\.com/in/([^/?#]+)", url)
    if not match:
        return ""
    return match.group(1).strip()


def extract_people_search_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    container = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    rows = list((container or {}).get("data") or [])
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        profile_url = str(row.get("profileUrl") or row.get("profile_url") or "").strip()
        normalized.append(
            {
                "urn": str(row.get("urn") or "").strip(),
                "full_name": str(row.get("fullName") or row.get("full_name") or "").strip(),
                "headline": str(row.get("headline") or "").strip(),
                "location": str(row.get("location") or "").strip(),
                "profile_url": profile_url,
                "username": extract_linkedin_slug(profile_url),
            }
        )
    return normalized


def _fetch_duckduckgo_html(query_text: str) -> str:
    return fetch_search_results_html(query_text, timeout=30).text


def _write_search_response_raw_asset(
    *,
    logger: AssetLogger,
    response: SearchResponse,
    default_path: Path,
    asset_type: str,
    source_kind: str,
    metadata: dict[str, Any] | None = None,
) -> Path:
    record = search_response_to_record(response)
    payload_metadata = {
        "provider_name": response.provider_name,
        "query_text": response.query_text,
        **dict(metadata or {}),
    }
    if response.raw_format == "json":
        raw_path = default_path.with_suffix(".json")
        logger.write_json(
            raw_path,
            record,
            asset_type=asset_type,
            source_kind=source_kind,
            is_raw_asset=True,
            model_safe=False,
            metadata=payload_metadata,
        )
        return raw_path

    raw_path = default_path.with_suffix(".html")
    logger.write_text(
        raw_path,
        str(response.raw_payload or ""),
        asset_type=asset_type,
        source_kind=source_kind,
        content_type=response.content_type or "text/html",
        is_raw_asset=True,
        model_safe=False,
        metadata=payload_metadata,
    )
    return raw_path


def _write_search_execution_artifact(
    *,
    logger: AssetLogger,
    artifact,
    default_path: Path,
    asset_type: str,
    source_kind: str,
    metadata: dict[str, Any] | None = None,
) -> Path:
    raw_path = default_path.with_name(f"{default_path.stem}_{str(getattr(artifact, 'label', 'artifact') or 'artifact')}.json")
    logger.write_json(
        raw_path,
        getattr(artifact, "payload", {}),
        asset_type=asset_type,
        source_kind=source_kind,
        is_raw_asset=True,
        model_safe=False,
        metadata={
            **dict(metadata or {}),
            **dict(getattr(artifact, "metadata", {}) or {}),
        },
    )
    return raw_path


def _load_cached_search_response(path: Path, query_text: str) -> tuple[list[dict[str, str]], SearchResponse]:
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text())
        response = search_response_from_record(payload, fallback_query_text=query_text)
    else:
        html_text = path.read_text()
        response = SearchResponse(
            provider_name="duckduckgo_html",
            query_text=query_text,
            results=parse_duckduckgo_html_results(html_text),
            raw_payload=html_text,
            raw_format="html",
            final_url="",
            content_type="text/html",
        )
    return [item.to_record() for item in response.results], response


def _seed_entry_from_web_result(
    result: dict[str, str],
    identity: CompanyIdentity,
    query_text: str,
    employment_status: str,
    *,
    source_family: str = "public_web_search",
) -> dict[str, Any] | None:
    url = str(result.get("url") or "").strip()
    slug = extract_linkedin_slug(url)
    if not slug:
        return None
    full_name = infer_name_from_result_title(str(result.get("title") or ""), identity)
    if not full_name:
        return None
    return {
        "seed_key": _seed_key(full_name, url),
        "full_name": full_name,
        "headline": "",
        "location": "",
        "source_type": "web_linkedin_url_search",
        "source_query": query_text,
        "profile_url": url,
        "slug": slug,
        "employment_status": employment_status,
        "target_company": identity.canonical_name,
        "metadata": {"title": str(result.get("title") or "").strip(), "source_family": source_family},
    }


def _lead_entries_from_public_result(
    result: dict[str, str],
    identity: CompanyIdentity,
    query_text: str,
    employment_status: str,
    *,
    analysis: dict[str, Any] | None = None,
    source_family: str,
) -> list[dict[str, Any]]:
    url = str(result.get("url") or "").strip()
    if not url or "linkedin.com/in/" in url:
        return []
    title = str(result.get("title") or "").strip()
    if not title:
        return []
    relation = str((analysis or {}).get("target_company_relation") or "").strip().lower()
    confidence_label = str((analysis or {}).get("confidence_label") or "").strip().lower()
    if relation != "explicit" or confidence_label not in {"high", "medium"}:
        return []
    names = infer_public_names_from_result_title(title, identity)
    entries: list[dict[str, Any]] = []
    for name in names[:2]:
        if not _looks_like_person_name(name, identity):
            continue
        entries.append(
            {
                "seed_key": _seed_key(name, url),
                "full_name": name,
                "headline": title,
                "location": "",
                "source_type": "public_media_lead",
                "source_query": query_text,
                "profile_url": "",
                "slug": "",
                "employment_status": employment_status if employment_status in {"former", "current"} else "unknown",
                "target_company": identity.canonical_name,
                "metadata": {
                    "title": title,
                    "source_family": source_family,
                    "lead_url": url,
                },
            }
        )
    return entries


def _seed_key(name: str, reference: str) -> str:
    payload = "|".join([normalize_name_token(name), reference.strip().lower()])
    return sha1(payload.encode("utf-8")).hexdigest()[:16]


def _dedupe_seed_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in entries:
        key = str(entry.get("seed_key") or _seed_key(str(entry.get("full_name") or ""), str(entry.get("profile_url") or entry.get("source_query") or "")))
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped


def _normalize_harvest_company_filters(identity: CompanyIdentity, filter_hints: dict[str, list[str]]) -> dict[str, list[str]]:
    normalized = {
        str(key): [str(item).strip() for item in list(values or []) if str(item).strip()]
        for key, values in dict(filter_hints or {}).items()
    }
    company_url = str(identity.linkedin_company_url or "").strip()
    if not company_url:
        for key, values in list(normalized.items()):
            normalized[key] = _dedupe_filter_values(values, company_filter=key in _HARVEST_COMPANY_FILTER_KEYS)
        return normalized

    target_tokens = {
        _normalize_company_filter_token(identity.canonical_name),
        _normalize_company_filter_token(identity.requested_name),
        _normalize_company_filter_token(identity.company_key),
        _normalize_company_filter_token(identity.linkedin_slug),
        _normalize_company_filter_token(company_url),
    }
    target_tokens.discard("")
    if not target_tokens:
        for key, values in list(normalized.items()):
            normalized[key] = _dedupe_filter_values(values, company_filter=key in _HARVEST_COMPANY_FILTER_KEYS)
        return normalized

    for key in _HARVEST_COMPANY_FILTER_KEYS:
        values = list(normalized.get(key) or [])
        if not values:
            continue
        rewritten_values = [
            company_url if _normalize_company_filter_token(value) in target_tokens else value
            for value in values
        ]
        normalized[key] = _dedupe_filter_values(rewritten_values, company_filter=True)
    for key, values in list(normalized.items()):
        if key in _HARVEST_COMPANY_FILTER_KEYS:
            continue
        normalized[key] = _dedupe_filter_values(values, company_filter=False)
    return normalized


def _normalize_harvest_query_text(
    *,
    query_text: str,
    filter_hints: dict[str, list[str]],
    identity: CompanyIdentity,
) -> str:
    normalized_query = " ".join(str(query_text or "").split()).strip()
    if not normalized_query:
        return ""

    stripped_query = normalized_query
    for token in sorted(_harvest_blocked_query_tokens(filter_hints=filter_hints, identity=identity), key=len, reverse=True):
        stripped_query = re.sub(re.escape(token), " ", stripped_query, flags=re.IGNORECASE)
    stripped_query = re.sub(
        r"\b(linkedin|employee|employees|former|current|member|members|team|teams)\b",
        " ",
        stripped_query,
        flags=re.IGNORECASE,
    )
    stripped_query = " ".join(stripped_query.split())
    if stripped_query:
        return _canonicalize_provider_query_alias(stripped_query)

    keyword_fallback = _harvest_keyword_fallback_query(filter_hints)
    if keyword_fallback:
        return keyword_fallback
    return ""


def _harvest_keyword_fallback_query(filter_hints: dict[str, list[str]]) -> str:
    keyword_values: list[str] = []
    seen: set[str] = set()
    for item in list(filter_hints.get("keywords") or []):
        cleaned = _clean_provider_query_text(str(item or ""))
        if not cleaned:
            continue
        signature = _provider_query_family_key(cleaned)
        if signature in seen:
            continue
        seen.add(signature)
        keyword_values.append(cleaned)
    if not keyword_values:
        return ""
    return _canonicalize_provider_query_alias(" ".join(keyword_values[:2]))


def _canonicalize_provider_query_alias(value: str) -> str:
    """Map alias / hyphen-underscore variants of a provider query to the same canonical form.

    Two semantically-equivalent variants that already collapse at the dedupe-signature step
    (`_search_query_signature`) would still reach the provider as different raw query strings
    if we did not canonicalize here. That wastes per-variant provider response caching and
    makes query-attribution noisy in summaries. Mirror the alias resolution
    `_clean_provider_query_text` already applies on the keyword path so all entry points
    converge on the same provider-facing form.
    """

    normalized = " ".join(str(value or "").split()).strip()
    if not normalized:
        return ""
    alias = _PROVIDER_QUERY_CANONICAL_ALIASES.get(normalized.lower())
    if alias:
        return alias
    # Hyphen / underscore variants (`Reasoning-Model` vs `Reasoning_Model`) need explicit
    # collapse before the alias lookup — the alias table is keyed on space-separated forms.
    space_normalized = " ".join(re.sub(r"[\-_]+", " ", normalized).split()).strip()
    if space_normalized and space_normalized.lower() != normalized.lower():
        space_alias = _PROVIDER_QUERY_CANONICAL_ALIASES.get(space_normalized.lower())
        if space_alias:
            return space_alias
    thematic_aliases = thematic_signal_search_query_aliases(normalized)
    if thematic_aliases:
        return thematic_aliases[0]
    thematic = canonicalize_thematic_signal_label(normalized)
    if thematic and thematic != normalized:
        return thematic
    canonical = canonicalize_scope_signal_label(normalized)
    return canonical or normalized


_GENERIC_PROVIDER_QUERY_TERMS = {
    "research",
    "researcher",
    "researchers",
    "employee",
    "employees",
    "member",
    "members",
    "team",
    "teams",
    "current",
    "former",
    "people",
    "person",
}

_PROVIDER_QUERY_CANONICAL_ALIASES = {
    "reasoning model": "Reasoning",
    "reasoning models": "Reasoning",
    "chain of thought": "Chain-of-thought",
    "chain-of-thought": "Chain-of-thought",
    "inference time compute": "Inference-time compute",
    "inference-time compute": "Inference-time compute",
    "post train": "Post-train",
    "post-training": "Post-train",
    "pre train": "Pre-train",
    "pre-training": "Pre-train",
    "vision language": "Vision-language",
    "vision-language": "Vision-language",
    "multimodality": "Multimodal",
    "video-generation": "Video generation",
    "infrastructure": "Infra",
}


def _resolve_provider_people_search_queries(
    *,
    identity: CompanyIdentity,
    filter_hints: dict[str, list[str]],
    search_seed_queries: list[str],
) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()

    def _add(value: str) -> None:
        cleaned = _clean_provider_query_text(value)
        if not cleaned:
            return
        signature = _provider_query_family_key(cleaned)
        if signature in seen:
            return
        seen.add(signature)
        queries.append(cleaned)

    for value in list(filter_hints.get("keywords") or []):
        _add(str(value or ""))
    for value in list(filter_hints.get("scope_keywords") or []):
        scope_value = str(value or "")
        if _provider_query_matches_company_identity(scope_value, identity=identity):
            continue
        _add(scope_value)
    for value in list(search_seed_queries or []):
        normalized = _normalize_harvest_query_text(
            query_text=str(value or ""),
            filter_hints=filter_hints,
            identity=identity,
        )
        if _provider_query_matches_company_identity(normalized, identity=identity):
            continue
        _add(normalized)
    return queries


def _clean_provider_query_text(value: str) -> str:
    normalized = " ".join(str(value or "").split()).strip()
    if not normalized:
        return ""
    stripped = re.sub(
        r"\b(" + "|".join(re.escape(token) for token in sorted(_GENERIC_PROVIDER_QUERY_TERMS)) + r")\b",
        " ",
        normalized,
        flags=re.IGNORECASE,
    )
    stripped = " ".join(stripped.split())
    if not stripped:
        return ""
    alias = _PROVIDER_QUERY_CANONICAL_ALIASES.get(stripped.lower())
    if alias:
        return alias
    thematic_aliases = thematic_signal_search_query_aliases(stripped)
    if thematic_aliases:
        return thematic_aliases[0]
    thematic = canonicalize_thematic_signal_label(stripped)
    if thematic and thematic != stripped:
        return thematic
    canonical = canonicalize_scope_signal_label(stripped)
    return canonical or stripped


def _provider_query_family_key(value: str) -> str:
    normalized = " ".join(str(value or "").split()).strip()
    if not normalized:
        return ""
    thematic = canonicalize_thematic_signal_label(normalized)
    scoped = canonicalize_scope_signal_label(normalized)
    canonical = _PROVIDER_QUERY_CANONICAL_ALIASES.get(normalized.lower(), thematic or scoped or normalized)
    return _search_query_signature(canonical) or canonical.lower()


def _provider_query_matches_company_identity(value: str, *, identity: CompanyIdentity) -> bool:
    normalized = _normalize_company_filter_token(value)
    if not normalized:
        return False
    company_url = str(identity.linkedin_company_url or "").strip()
    target_tokens = {
        _normalize_company_filter_token(identity.canonical_name),
        _normalize_company_filter_token(identity.requested_name),
        _normalize_company_filter_token(identity.company_key),
        _normalize_company_filter_token(identity.linkedin_slug),
        _normalize_company_filter_token(company_url),
    }
    target_tokens.discard("")
    return normalized in target_tokens


def _harvest_blocked_query_tokens(*, filter_hints: dict[str, list[str]], identity: CompanyIdentity) -> list[str]:
    blocked: list[str] = []
    seen: set[str] = set()

    def _add(value: str) -> None:
        token = " ".join(str(value or "").split()).strip()
        if not token:
            return
        lowered = token.lower()
        if lowered in seen:
            return
        seen.add(lowered)
        blocked.append(token)

    _add(identity.canonical_name)
    _add(identity.requested_name)
    _add(identity.linkedin_slug)
    for alias in list(identity.aliases or []):
        _add(str(alias or ""))
    for key in ["current_companies", "past_companies", "exclude_current_companies", "exclude_past_companies"]:
        for value in list(filter_hints.get(key) or []):
            _add(_company_filter_search_label(str(value or "")))
    for value in list(filter_hints.get("job_titles") or []):
        _add(str(value or ""))
    return blocked


def _company_filter_search_label(value: str) -> str:
    raw = unescape(str(value or "")).strip()
    if not raw:
        return ""
    match = re.search(r"linkedin\.com/company/([^/?#]+)", raw, flags=re.IGNORECASE)
    if match:
        raw = str(match.group(1) or "").strip()
    raw = raw.replace("-", " ").replace("_", " ")
    return " ".join(raw.split())


def _normalize_company_filter_token(value: str) -> str:
    raw = unescape(str(value or "")).strip().lower()
    if not raw:
        return ""
    match = re.search(r"linkedin\.com/company/([^/?#]+)", raw)
    if match:
        return re.sub(r"[^a-z0-9]+", "", str(match.group(1) or "").lower())
    return re.sub(r"[^a-z0-9]+", "", raw)


_HARVEST_COMPANY_FILTER_KEYS = {
    "current_companies",
    "past_companies",
    "exclude_current_companies",
    "exclude_past_companies",
}


def _dedupe_filter_values(values: list[str], *, company_filter: bool) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized:
            continue
        key = _normalize_company_filter_token(normalized) if company_filter else " ".join(normalized.lower().split())
        if not key:
            key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


def _harvest_search_plan_summary(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in dict(plan or {}).items()
        if str(key) not in {"initial_result", "probe_result"}
    }


def _harvest_result_zero_retry_exhausted(result: dict[str, Any] | None) -> bool:
    return bool(dict(dict(result or {}).get("zero_result_retry") or {}).get("exhausted"))


def _collect_harvest_zero_result_retry_summaries(
    harvest_plan: dict[str, Any],
    harvest_result: dict[str, Any],
) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    probe_retry = dict(dict(harvest_plan or {}).get("probe_zero_result_retry") or {})
    if probe_retry:
        summary["probe"] = probe_retry
    result_retry = dict(dict(harvest_result or {}).get("zero_result_retry") or {})
    if result_retry:
        summary["result"] = result_retry
    chunk_retry_events = list(
        dict(dict(harvest_result or {}).get("chunked_scale_fallback") or {}).get("zero_result_retry_events") or []
    )
    if chunk_retry_events:
        summary["page_chunks"] = chunk_retry_events
    return summary


def _search_query_signature(value: str) -> str:
    normalized = " ".join(str(value or "").lower().split()).strip()
    if not normalized:
        return ""
    compact = re.sub(r"[\s\-_]+", "", normalized)
    alnum = re.sub(r"[^0-9a-z]+", "", compact)
    return alnum or compact


def _extract_harvest_profile_urls_from_result(result: dict[str, Any]) -> set[str]:
    rows = list(dict(result or {}).get("rows") or [])
    urls: set[str] = set()
    for row in rows:
        url = str(dict(row or {}).get("profile_url") or "").strip().lower()
        if not url:
            continue
        urls.add(url.rstrip("/"))
    return urls


def _jaccard_overlap_ratio(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    union = left.union(right)
    if not union:
        return 0.0
    return float(len(left.intersection(right)) / len(union))


def _compile_query_specs(search_seed_queries: list[str], query_bundles: list[dict[str, Any]]) -> list[dict[str, str]]:
    specs: list[dict[str, str]] = []
    for query in search_seed_queries:
        normalized = " ".join(str(query or "").split()).strip()
        if normalized:
            specs.append(
                {
                    "bundle_id": "seed_queries",
                    "source_family": "public_web_search",
                    "execution_mode": "low_cost_web_search",
                    "query": normalized,
                }
            )
    for bundle in query_bundles:
        if not isinstance(bundle, dict):
            continue
        for query in bundle.get("queries") or []:
            normalized = " ".join(str(query or "").split()).strip()
            if not normalized:
                continue
            specs.append(
                {
                    "bundle_id": str(bundle.get("bundle_id") or "bundle"),
                    "source_family": str(bundle.get("source_family") or "public_web_search"),
                    "execution_mode": str(bundle.get("execution_mode") or "low_cost_web_search"),
                    "query": normalized,
                }
            )
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in specs:
        query_signature = _search_query_signature(item["query"]) or item["query"].lower()
        key = (item["execution_mode"], query_signature)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _stage1_web_seed_fallback_enabled(
    *,
    cost_policy: dict[str, Any] | None,
    intent_view: dict[str, Any] | None,
) -> bool:
    policy = dict(cost_policy or {})
    execution_preferences = dict(dict(intent_view or {}).get("execution_preferences") or {})
    return bool(
        policy.get("allow_stage1_web_seed_fallback")
        or policy.get("allow_public_web_seed_fallback")
        or execution_preferences.get("allow_stage1_web_seed_fallback")
        or execution_preferences.get("allow_public_web_seed_fallback")
    )


def _search_seed_worker_key(bundle_id: str, index: int, employment_status: str, *, query_text: str = "") -> str:
    scope = normalize_search_seed_employment_scope(employment_status)
    query_signature = _search_query_signature(query_text)
    if query_signature:
        identity_payload = "|".join(
            [
                str(scope or "all"),
                str(bundle_id or "bundle"),
                query_signature,
            ]
        )
        query_suffix = "q_" + sha1(identity_payload.encode("utf-8")).hexdigest()[:16]
    else:
        # Migration/test compatibility only. Normal call sites pass query_text
        # so provider work identity is independent from query list order.
        query_suffix = f"{int(index):02d}"
    if scope and scope != "all":
        return f"{scope}::{bundle_id}::{query_suffix}"
    return f"{bundle_id}::{query_suffix}"


def _resolve_effective_search_seed_queries(
    *,
    search_seed_queries: list[str],
    delta_execution_plan: dict[str, Any] | None,
) -> list[str]:
    delta_queries = [
        " ".join(str(item or "").split()).strip()
        for item in list(dict(delta_execution_plan or {}).get("missing_profile_search_queries") or [])
        if " ".join(str(item or "").split()).strip()
    ]
    source = delta_queries or list(search_seed_queries or [])
    deduped: list[str] = []
    seen: set[str] = set()
    for item in source:
        normalized = " ".join(str(item or "").split()).strip()
        if not normalized:
            continue
        signature = _search_query_signature(normalized) or normalized.lower()
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(normalized)
    return deduped


def _resolve_discovery_filter_hints(
    *,
    filter_hints: dict[str, list[str]] | None,
    intent_view: dict[str, Any] | None,
) -> dict[str, list[str]]:
    explicit = {
        str(key): [str(item).strip() for item in list(values or []) if str(item).strip()]
        for key, values in dict(filter_hints or {}).items()
        if str(key).strip()
    }
    if explicit:
        return explicit
    return {
        str(key): [str(item).strip() for item in list(values or []) if str(item).strip()]
        for key, values in dict(dict(intent_view or {}).get("filter_hints") or {}).items()
        if str(key).strip()
    }


def _resolve_discovery_query_bundles(
    *,
    query_bundles: list[dict[str, Any]] | None,
    intent_view: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    explicit = [dict(item) for item in list(query_bundles or []) if isinstance(item, dict)]
    if explicit:
        return explicit
    return [dict(item) for item in list(dict(intent_view or {}).get("search_query_bundles") or []) if isinstance(item, dict)]


def _resolve_discovery_search_seed_queries(
    *,
    search_seed_queries: list[str] | None,
    query_bundles: list[dict[str, Any]] | None,
    intent_view: dict[str, Any] | None,
) -> list[str]:
    explicit_queries = [
        " ".join(str(item or "").split()).strip()
        for item in list(search_seed_queries or [])
        if " ".join(str(item or "").split()).strip()
    ]
    if explicit_queries:
        return explicit_queries
    bundled_queries: list[str] = []
    for bundle in list(query_bundles or []):
        if not isinstance(bundle, dict):
            continue
        for query in list(bundle.get("queries") or []):
            normalized = " ".join(str(query or "").split()).strip()
            if normalized:
                bundled_queries.append(normalized)
    if bundled_queries:
        return bundled_queries
    return [
        " ".join(str(item or "").split()).strip()
        for item in list(dict(intent_view or {}).get("search_seed_queries") or [])
        if " ".join(str(item or "").split()).strip()
    ]


def _filter_query_bundles_for_delta(
    query_bundles: list[dict[str, Any]],
    *,
    allowed_queries: list[str],
) -> list[dict[str, Any]]:
    allowed_signatures = {
        _search_query_signature(query) or " ".join(str(query or "").lower().split())
        for query in list(allowed_queries or [])
        if str(query or "").strip()
    }
    if not allowed_signatures:
        return [dict(bundle or {}) for bundle in list(query_bundles or []) if isinstance(bundle, dict)]
    filtered: list[dict[str, Any]] = []
    for bundle in list(query_bundles or []):
        if not isinstance(bundle, dict):
            continue
        queries = []
        for query in list(bundle.get("queries") or []):
            normalized = " ".join(str(query or "").split()).strip()
            if not normalized:
                continue
            signature = _search_query_signature(normalized) or normalized.lower()
            if signature in allowed_signatures:
                queries.append(normalized)
        if not queries:
            continue
        filtered.append(
            {
                **dict(bundle),
                "queries": queries,
            }
        )
    return filtered


def infer_public_names_from_result_title(title: str, identity: CompanyIdentity) -> list[str]:
    cleaned = " " + title + " "
    cleaned = cleaned.replace(identity.canonical_name, " ")
    for alias in identity.aliases:
        cleaned = cleaned.replace(alias, " ")
    cleaned = re.sub(r"\b(YouTube|Podcast|Interview|with|on|about|episode|official|blog|research|engineering)\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = " ".join(cleaned.split())
    candidates = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}\b", cleaned)
    blocked = {identity.canonical_name.lower(), "Google DeepMind".lower()}
    results: list[str] = []
    for item in candidates:
        normalized = " ".join(item.split()).strip()
        if not normalized or normalized.lower() in blocked:
            continue
        if not _looks_like_person_name(normalized, identity):
            continue
        if normalized not in results:
            results.append(normalized)
    return results[:3]


def _looks_like_person_name(value: str, identity: CompanyIdentity | None = None) -> bool:
    tokens = [token for token in str(value or "").split() if token]
    if len(tokens) < 2 or len(tokens) > 3:
        return False
    blocked_tokens = {
        "acknowledgment",
        "acknowledgments",
        "acknowledgement",
        "acknowledgements",
        "author",
        "authors",
        "authorship",
        "biomedical",
        "blog",
        "build",
        "conference",
        "contributor",
        "contributors",
        "engineering",
        "episode",
        "guidelines",
        "human",
        "interview",
        "launch",
        "launches",
        "nature",
        "official",
        "podcast",
        "research",
        "roadmap",
        "section",
        "team",
        "with",
        "your",
    }
    company_tokens = {
        normalize_name_token(str(identity.canonical_name or "")),
        normalize_name_token(str(identity.requested_name or "")),
    } if identity is not None else set()
    company_tokens.discard("")
    for token in tokens:
        normalized = normalize_name_token(token)
        if not normalized:
            return False
        if normalized in blocked_tokens:
            return False
        if normalized in company_tokens:
            return False
        if not re.fullmatch(r"[^\W\d_]+(?:[-'][^\W\d_]+)*", token, flags=re.UNICODE):
            return False
    return True


def _seed_candidate_category(row: dict[str, Any]) -> str:
    source_type = str(row.get("source_type") or "").strip()
    if source_type == "public_media_lead" and not str(row.get("profile_url") or "").strip():
        return "lead"
    return "former_employee" if row.get("employment_status") == "former" else "employee"


def _interrupted_query_summary(index: int, query_spec: dict[str, str], query_text: str, html_path: Path) -> dict[str, Any]:
    return {
        "query": query_text,
        "bundle_id": query_spec.get("bundle_id", ""),
        "source_family": query_spec.get("source_family", ""),
        "execution_mode": query_spec.get("execution_mode", ""),
        "mode": "web_search",
        "raw_path": str(html_path),
        "result_count": 0,
        "linkedin_result_count": 0,
        "seed_entry_count": 0,
        "status": "interrupted",
        "index": index,
    }


def _build_people_search_url(account: RapidApiAccount, query_text: str, *, limit: int) -> str:
    base = account.base_url.rstrip("/")
    if "z-real-time-linkedin-scraper-api1" in account.host:
        endpoint = base[:-len("/api/search/people")] if base.endswith("/api/search/people") else base
        return endpoint + "/api/search/people?" + parse.urlencode({"keywords": query_text, "limit": limit})
    endpoint_path = str(account.endpoint_search or "/api/search/people").split("?", 1)[0]
    return base + endpoint_path + "?" + parse.urlencode({"keywords": query_text, "limit": limit})


def _build_seed_notes(row: dict[str, Any]) -> str:
    parts = ["Discovered from low-cost search seed acquisition."]
    if row.get("source_query"):
        parts.append(f"Query: {row['source_query']}.")
    if row.get("source_type"):
        parts.append(f"Source: {row['source_type']}.")
    if row.get("location"):
        parts.append(f"Location: {row['location']}.")
    return " ".join(parts)
