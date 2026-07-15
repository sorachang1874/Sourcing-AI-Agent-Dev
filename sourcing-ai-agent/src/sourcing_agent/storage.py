from __future__ import annotations

import ast
import json
import os
import re
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any
from uuid import uuid4

from .asset_governance import (
    build_canonical_asset_replacement_plan,
    default_asset_pointer_history_id,
    default_asset_pointer_key,
    normalize_default_asset_pointer_payload,
)
from .company_registry import normalize_company_key, resolve_company_alias_key
from .control_plane_job_progress import (
    build_job_progress_event_summary as _cp_build_job_progress_event_summary,
)
from .control_plane_job_progress import (
    merge_progress_event_metrics as _cp_merge_progress_event_metrics,
)
from .control_plane_live_postgres import (
    LiveControlPlanePostgresAdapter,
    resolve_control_plane_postgres_live_mode,
)
from .control_plane_repository import _validate_bulk_upsert_rows_call_contract
from .control_plane_serde import json_safe_payload as _control_plane_json_safe_payload
from .control_plane_time import (
    is_sqlite_timestamp_expired as _is_sqlite_timestamp_expired,
)
from .control_plane_time import (
    parse_sqlite_timestamp as _parse_sqlite_timestamp,
)
from .domain import Candidate, EvidenceRecord, JobRequest, normalize_candidate
from .linkedin_url_normalization import (
    normalize_linkedin_profile_url_key as _normalize_linkedin_profile_url_key,
)
from .local_postgres import resolve_control_plane_postgres_dsn
from .person_identity import (
    resolve_candidate_identity_key as _resolve_candidate_identity_key,
)
from .person_identity import resolve_person_identity_key as _resolve_person_identity_key
from .person_identity import resolve_profile_url_key as _resolve_profile_url_key
from .public_web_signal_identity import public_web_signal_id_for_identity
from .repositories import ControlPlaneRepositories
from .repositories import crm_core as _crm_core_repo
from .repositories import person_company_assets as _person_company_assets_repo
from .repositories import public_web as _public_web_repo
from .repositories import workflow_runtime as _workflow_runtime_repo
from .request_matching import (
    MATCH_THRESHOLD,
    build_request_family_match_explanation,
    matching_request_signature,
    request_family_score,
    request_family_signature,
    request_signature,
)
from .request_matching import matching_bundle_payload as _matching_bundle_payload
from .worker_scheduler import effective_worker_status, wait_stage

_RESULT_VIEW_SERVING_ARTIFACT_FILENAMES = (
    "manifest.json",
    "artifact_summary.json",
    "snapshot_manifest.json",
    "materialized_candidate_documents.json",
)


def _normalize_job_result_view_source_path(source_path: str) -> str:
    """Persist result views against a concrete serving artifact, not a snapshot directory."""

    normalized_source_path = str(source_path or "").strip()
    if not normalized_source_path:
        return ""
    path = Path(normalized_source_path).expanduser()
    if not path.is_dir():
        return normalized_source_path
    normalized_artifacts = path / "normalized_artifacts"
    for filename in _RESULT_VIEW_SERVING_ARTIFACT_FILENAMES:
        candidate = normalized_artifacts / filename
        if candidate.exists() and candidate.is_file():
            return str(candidate)
    return normalized_source_path


_TERMINAL_JOB_STATUSES = {"completed", "failed"}
_CONTROL_PLANE_POSTGRES_NATIVE_READ_METHODS = {
    "get_agent_trace_span",
    "list_agent_trace_spans",
    "list_recoverable_agent_workers",
    "get_workflow_job_lease",
    "get_agent_worker",
    "list_agent_workers",
    "list_agent_workers_by_remote_provider_identifiers",
    "list_latest_target_candidate_public_web_runs_by_record_ids",
    "list_latest_crm_public_web_runs_by_record_ids",
    "list_latest_company_public_web_asset_runs_by_company_keys",
}
_CONTROL_PLANE_POSTGRES_NATIVE_TABLES = {
    "save_job_row": "jobs",
    "update_job_row_if_owned": "jobs",
    "apply_owned_crm_record_update": "crm_records",
    "upsert_crm_public_web_promotion_if_owned": "crm_public_web_promotions",
    "append_job_event": "job_events",
    "create_agent_runtime_session_row": "agent_runtime_sessions",
    "update_agent_runtime_session_status": "agent_runtime_sessions",
    "create_agent_trace_span": "agent_trace_spans",
    "update_agent_trace_span": "agent_trace_spans",
    "create_or_resume_agent_worker": "agent_worker_runs",
    "mark_agent_worker_running": "agent_worker_runs",
    "checkpoint_agent_worker": "agent_worker_runs",
    "complete_agent_worker": "agent_worker_runs",
    "retire_agent_workers": "agent_worker_runs",
    "claim_agent_worker": "agent_worker_runs",
    "renew_agent_worker_lease": "agent_worker_runs",
    "release_agent_worker_lease": "agent_worker_runs",
    "request_interrupt_agent_worker": "agent_worker_runs",
    "clear_interrupt_agent_worker": "agent_worker_runs",
    "list_agent_workers_by_remote_provider_identifiers": "agent_worker_runs",
    "list_latest_company_public_web_asset_runs_by_company_keys": "company_public_web_asset_runs",
    "acquire_workflow_job_lease": "workflow_job_leases",
    "get_workflow_job_lease": "workflow_job_leases",
    "renew_workflow_job_lease": "workflow_job_leases",
    "release_workflow_job_lease": "workflow_job_leases",
    "supersede_workflow_runtime_state": "agent_worker_runs",
    "upsert_workflow_command": "workflow_commands",
    "update_workflow_command_payload": "workflow_commands",
    "claim_workflow_command": "workflow_commands",
    "mark_workflow_command_running": "workflow_commands",
    "mark_workflow_command_succeeded": "workflow_commands",
    "mark_workflow_command_failed": "workflow_commands",
    "mark_workflow_command_partial_progress": "workflow_commands",
    "mark_workflow_command_waiting_prerequisite": "workflow_commands",
    "cancel_workflow_command": "workflow_commands",
    "retry_workflow_command": "workflow_commands",
    "resume_workflow_command": "workflow_commands",
    "claim_job_materialization_item": "job_materialization_items",
    "mark_job_materialization_item_completed": "job_materialization_items",
    "mark_job_materialization_item_failed": "job_materialization_items",
    "mark_job_materialization_item_waiting_prerequisite": "job_materialization_items",
    "reawaken_waiting_prerequisite_job_materialization_items": "job_materialization_items",
    "list_latest_target_candidate_public_web_runs_by_record_ids": "target_candidate_public_web_runs",
    "list_latest_crm_public_web_runs_by_record_ids": "crm_public_web_runs",
    "upsert_raw_profile_index": "raw_profile_index",
    "upsert_candidate_evidence_index": "candidate_evidence_index",
}


def _env_flag_enabled(value: Any, *, default: bool) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return bool(default)
    return normalized not in {"0", "false", "no", "off"}


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _strict_legacy_materialization_write_gate_enabled() -> bool:
    return _env_flag_enabled(
        os.getenv("SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES"),
        default=False,
    )


_DURABLE_RUNTIME_TABLES = {
    "workflow_events",
    "workflow_current_state",
    "workflow_commands",
    "workflow_recovery_intents",
    "runtime_outbox",
    "agent_actions",
    "operation_runs",
    "acquisition_runs",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    "acquisition_discovery_lanes",
    "operation_events",
    "crm_tasks",
    "company_assets",
    "company_evidence",
    "company_assertions",
}


def _legacy_materialization_write_is_migration(metadata: dict[str, Any] | None, source: str = "") -> bool:
    payload = dict(metadata or {})
    source_text = str(source or "").strip().lower()
    owner = str(payload.get("owner") or payload.get("write_owner") or "").strip().lower()
    bridge_kind = str(payload.get("migration_bridge") or payload.get("bridge_kind") or "").strip().lower()
    return bool(
        payload.get("migration_adapter")
        or payload.get("legacy_migration_adapter")
        or bridge_kind in {"job_materialization_items_adapter", "legacy_materialization_adapter"}
        or owner in {"durable_runtime_migration_adapter", "legacy_materialization_adapter"}
        or source_text in {"durable_runtime_migration_adapter", "legacy_materialization_adapter"}
    )


# Extracted to control_plane_serde so the typed repository write path applies the SAME json-safe
# coercion (re-imported under the legacy private name to keep all internal call sites unchanged).
_json_safe_payload = _control_plane_json_safe_payload


def _workflow_command_causality_columns_from_payload(
    payload: Any,
    *,
    workflow_run_id: str,
    operation_id: str,
    command_type: str,
    owner: str,
    idempotency_key: str,
) -> dict[str, Any]:
    payload_dict = dict(payload or {}) if isinstance(payload, dict) else {}
    causality = dict(payload_dict.get("causality") or {})
    return {
        "stage_id": str(causality.get("stage_id") or payload_dict.get("stage_id") or "").strip(),
        "causal_group_id": str(causality.get("causal_group_id") or payload_dict.get("causal_group_id") or "").strip(),
        "parent_command_id": str(causality.get("parent_command_id") or "").strip(),
        "source_event_id": str(causality.get("source_event_id") or "").strip(),
        "source_event_type": str(causality.get("source_event_type") or "").strip(),
        "input_artifact_refs_json": json.dumps(
            _json_safe_payload(list(causality.get("input_artifact_refs") or [])),
            ensure_ascii=False,
        ),
        "output_artifact_refs_json": json.dumps(
            _json_safe_payload(list(causality.get("output_artifact_refs") or [])),
            ensure_ascii=False,
        ),
        "produced_entity_counts_json": json.dumps(
            _json_safe_payload(dict(causality.get("produced_entity_counts") or {})),
            ensure_ascii=False,
        ),
        "no_op_reason": str(causality.get("no_op_reason") or "").strip(),
        "readiness_effect": str(causality.get("readiness_effect") or "").strip(),
        "downstream_command_ids_json": json.dumps(
            _json_safe_payload(list(causality.get("downstream_command_ids") or [])),
            ensure_ascii=False,
        ),
        "causality_schema_version": str(causality.get("schema_version") or "command_causality_v1").strip()
        or "command_causality_v1",
    }


def _dedupe_ordered_texts(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    ordered: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _safe_int_list(values: Any) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    for value in list(values or []):
        try:
            item = int(value or 0)
        except (TypeError, ValueError):
            continue
        if item <= 0 or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _normalize_textual_value(value: Any) -> str:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return bytes(value).decode("utf-8", errors="replace")
    text = str(value or "").strip()
    if not text:
        return ""
    if (text.startswith("b'") and text.endswith("'")) or (text.startswith('b"') and text.endswith('"')):
        try:
            parsed = ast.literal_eval(text)
        except (SyntaxError, ValueError):
            return text
        if isinstance(parsed, memoryview):
            parsed = parsed.tobytes()
        if isinstance(parsed, (bytes, bytearray)):
            try:
                return bytes(parsed).decode("utf-8")
            except UnicodeDecodeError:
                return bytes(parsed).decode("utf-8", errors="replace")
    return text


def _normalize_search_index_text(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", " ", str(value or "").lower())).strip()


def _normalize_search_index_terms(value: Any) -> list[str]:
    raw_items = value if isinstance(value, (list, tuple, set)) else [value]
    normalized_terms: list[str] = []
    seen_terms: set[str] = set()
    for raw_item in raw_items:
        if isinstance(raw_item, (list, tuple, set)):
            nested_items = list(raw_item)
        else:
            nested_items = str(raw_item or "").split(",")
        for item in nested_items:
            normalized = _normalize_search_index_text(item)
            if not normalized or normalized in seen_terms:
                continue
            seen_terms.add(normalized)
            normalized_terms.append(normalized)
    return normalized_terms


def _normalized_payload_text(
    payload: dict[str, Any] | None,
    key: str,
    *,
    default: str = "",
) -> str:
    normalized = _normalize_textual_value((payload or {}).get(key))
    if normalized:
        return normalized
    return default


def _normalized_company_scope(target_company: str, company_key: str = "") -> tuple[str, str]:
    normalized_target_company = str(target_company or "").strip()
    normalized_company_key = resolve_company_alias_key(str(company_key or "").strip() or normalized_target_company)
    return normalized_target_company, normalized_company_key


def _company_scope_predicate(
    normalized_target_company: str,
    normalized_company_key: str,
    *,
    target_column: str = "target_company",
    company_key_column: str = "company_key",
    placeholder: str = "?",
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if normalized_target_company:
        clauses.append(f"{target_column} = {placeholder}")
        params.append(normalized_target_company)
    if normalized_company_key:
        clauses.append(f"{company_key_column} = {placeholder}")
        params.append(normalized_company_key)
    if not clauses:
        return "1 = 0", []
    if len(clauses) == 1:
        return clauses[0], params
    return f"({' OR '.join(clauses)})", params


def _company_identity_lookup_predicate(
    normalized_target_company: str,
    normalized_company_key: str,
    *,
    target_column: str = "target_company",
    company_key_column: str = "company_key",
    placeholder: str = "?",
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if normalized_target_company:
        clauses.append(f"lower({target_column}) = lower({placeholder})")
        params.append(normalized_target_company)
    if normalized_company_key:
        clauses.append(f"{company_key_column} = {placeholder}")
        params.append(normalized_company_key)
    if not clauses:
        return "1 = 0", []
    if len(clauses) == 1:
        return clauses[0], params
    return f"({' OR '.join(clauses)})", params


def _company_display_name_preference_score(
    target_company: str,
    *,
    company_key: str,
) -> tuple[int, int, int]:
    normalized_target = normalize_company_key(target_company)
    normalized_company_key = normalize_company_key(company_key)
    display_like_name = 1 if target_company and normalized_target != normalized_company_key else 0
    has_visual_spacing = 1 if any(not character.isalnum() for character in target_company) else 0
    return (display_like_name, has_visual_spacing, len(target_company))


def _resolve_storage_target_company(
    *,
    requested_target_company: str,
    company_key: str,
    existing_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    id_column: str = "",
) -> str:
    normalized_requested = str(requested_target_company or "").strip()
    normalized_company_key = str(company_key or "").strip()
    best_target_company = normalized_requested or normalized_company_key
    best_sort_key = (
        _company_display_name_preference_score(best_target_company, company_key=normalized_company_key),
        0,
        "",
        0,
    )
    for row in [dict(row) for row in list(existing_rows or []) if isinstance(row, dict)]:
        candidate_target_company = str(row.get("target_company") or "").strip()
        if not candidate_target_company:
            continue
        candidate_sort_key = (
            _company_display_name_preference_score(
                candidate_target_company,
                company_key=normalized_company_key,
            ),
            int(bool(row.get("authoritative"))),
            str(row.get("updated_at") or ""),
            int(row.get(id_column or "") or 0),
        )
        if candidate_sort_key > best_sort_key:
            best_target_company = candidate_target_company
            best_sort_key = candidate_sort_key
    if best_target_company:
        return best_target_company
    return normalized_requested or normalized_company_key


def _merge_progress_event_metrics(
    metrics: dict[str, Any] | None,
    *,
    event_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    return _cp_merge_progress_event_metrics(metrics, event_payload=event_payload)


def _build_job_progress_event_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    return _cp_build_job_progress_event_summary(events)


def _normalize_member_token(value: Any) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def _normalize_employment_scope(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"current", "former", "all"}:
        return normalized
    if normalized in {"past", "previous", "ex"}:
        return "former"
    if normalized:
        return normalized
    return ""


def _build_asset_membership_row(
    *,
    target_company: str,
    snapshot_id: str,
    asset_view: str,
    artifact_kind: str,
    artifact_key: str,
    candidate_record: dict[str, Any],
    lane: str = "",
    employment_scope: str = "",
) -> dict[str, Any]:
    record = dict(candidate_record or {})
    candidate_id = str(record.get("candidate_id") or "").strip()
    linkedin_url = str(record.get("linkedin_url") or "").strip()
    profile_url_key = _normalize_linkedin_profile_url_key(linkedin_url)
    if profile_url_key:
        member_key = profile_url_key
        member_key_kind = "linkedin_profile_url"
    elif candidate_id:
        member_key = f"candidate:{candidate_id.lower()}"
        member_key_kind = "candidate_id"
    else:
        fallback_payload = "|".join(
            [
                _normalize_member_token(record.get("name_en") or record.get("display_name")),
                _normalize_member_token(record.get("target_company") or target_company),
                _normalize_member_token(record.get("organization")),
                _normalize_member_token(record.get("role")),
            ]
        )
        member_key = f"fallback:{sha1(fallback_payload.encode('utf-8')).hexdigest()[:24]}"
        member_key_kind = "candidate_fallback"
    normalized_scope = _normalize_employment_scope(employment_scope) or _normalize_employment_scope(
        record.get("employment_status")
    )
    metadata = {
        "display_name": str(record.get("display_name") or record.get("name_en") or "").strip(),
        "category": str(record.get("category") or "").strip(),
        "employment_status": str(record.get("employment_status") or "").strip(),
        "source_dataset": str(record.get("source_dataset") or "").strip(),
        "source_path": str(record.get("source_path") or "").strip(),
        "organization": str(record.get("organization") or "").strip(),
        "role": str(record.get("role") or "").strip(),
    }
    return {
        "target_company": str(target_company or record.get("target_company") or "").strip(),
        "snapshot_id": str(snapshot_id or "").strip(),
        "asset_view": str(asset_view or "canonical_merged").strip() or "canonical_merged",
        "artifact_kind": str(artifact_kind or "").strip(),
        "artifact_key": str(artifact_key or "").strip(),
        "lane": str(lane or "").strip(),
        "employment_scope": normalized_scope,
        "member_key": member_key,
        "member_key_kind": member_key_kind,
        "candidate_id": candidate_id,
        "profile_url_key": profile_url_key,
        "metadata": metadata,
    }


def _normalize_asset_materialization_members(
    members: list[dict[str, Any]] | None,
    *,
    target_company: str,
    snapshot_id: str,
    asset_view: str,
    artifact_kind: str,
    artifact_key: str,
) -> list[dict[str, Any]]:
    normalized_target_company = str(target_company or "").strip()
    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
    normalized_artifact_kind = str(artifact_kind or "").strip()
    normalized_artifact_key = str(artifact_key or "").strip()
    normalized_members: list[dict[str, Any]] = []
    seen_member_keys: set[str] = set()
    for item in list(members or []):
        if not isinstance(item, dict):
            continue
        member_key = str(item.get("member_key") or "").strip()
        if not member_key or member_key in seen_member_keys:
            continue
        seen_member_keys.add(member_key)
        normalized_members.append(
            {
                "target_company": str(item.get("target_company") or normalized_target_company).strip(),
                "snapshot_id": str(item.get("snapshot_id") or normalized_snapshot_id).strip(),
                "asset_view": str(item.get("asset_view") or normalized_asset_view).strip() or normalized_asset_view,
                "artifact_kind": str(item.get("artifact_kind") or normalized_artifact_kind).strip(),
                "artifact_key": str(item.get("artifact_key") or normalized_artifact_key).strip(),
                "lane": str(item.get("lane") or "").strip(),
                "employment_scope": _normalize_employment_scope(item.get("employment_scope")),
                "member_key": member_key,
                "member_key_kind": str(item.get("member_key_kind") or "candidate_fallback").strip()
                or "candidate_fallback",
                "candidate_id": str(item.get("candidate_id") or "").strip(),
                "profile_url_key": str(item.get("profile_url_key") or "").strip(),
                "metadata": dict(item.get("metadata") or {}),
            }
        )
    normalized_members.sort(key=lambda item: item["member_key"])
    return normalized_members


def _asset_materialization_member_projection(members: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [
        {
            "member_key": str(item.get("member_key") or "").strip(),
            "member_key_kind": str(item.get("member_key_kind") or "").strip(),
            "employment_scope": _normalize_employment_scope(item.get("employment_scope")),
            "lane": str(item.get("lane") or "").strip(),
            "candidate_id": str(item.get("candidate_id") or "").strip(),
            "profile_url_key": str(item.get("profile_url_key") or "").strip(),
        }
        for item in list(members or [])
        if isinstance(item, dict) and str(item.get("member_key") or "").strip()
    ]


def _asset_materialization_member_signature(members: list[dict[str, Any]] | None) -> str:
    return sha1(
        json.dumps(
            _asset_materialization_member_projection(members),
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]


def _asset_membership_summary_from_rows(rows: list[dict[str, Any]] | None) -> dict[str, Any]:
    employment_scope_counts: dict[str, int] = {}
    lane_counts: dict[str, int] = {}
    member_key_kind_counts: dict[str, int] = {}
    total = 0
    for row in list(rows or []):
        member_key = str(dict(row or {}).get("member_key") or "").strip()
        if not member_key:
            continue
        total += 1
        scope = str(dict(row or {}).get("employment_scope") or "").strip()
        lane = str(dict(row or {}).get("lane") or "").strip()
        kind = str(dict(row or {}).get("member_key_kind") or "").strip()
        if scope:
            employment_scope_counts[scope] = employment_scope_counts.get(scope, 0) + 1
        if lane:
            lane_counts[lane] = lane_counts.get(lane, 0) + 1
        if kind:
            member_key_kind_counts[kind] = member_key_kind_counts.get(kind, 0) + 1
    return {
        "member_count": total,
        "employment_scope_counts": employment_scope_counts,
        "lane_counts": lane_counts,
        "member_key_kind_counts": member_key_kind_counts,
    }


class ControlPlaneStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._profile_prefetch_scheduler_lock_guard = threading.Lock()
        self._profile_prefetch_scheduler_locks: dict[str, threading.RLock] = {}
        self._board_visible_patch_publication_lock_guard = threading.Lock()
        self._board_visible_patch_publication_locks: dict[str, threading.RLock] = {}
        self._legacy_target_public_web_migration_write_depth = 0
        self._legacy_target_public_web_migration_write_reason = ""
        self._bootstrap_candidate_store_loaded = False
        runtime_dir = self.db_path.parent
        control_plane_postgres_dsn = resolve_control_plane_postgres_dsn(runtime_dir)
        control_plane_postgres_mode = resolve_control_plane_postgres_live_mode(
            os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE")
            or ("postgres_only" if control_plane_postgres_dsn else "disabled")
        )
        # Track B B3: SQLite-authoritative control-plane storage is no longer supported. Every
        # ControlPlaneStore — serving, CLI, local-dev, scripted, tests — requires a resolved
        # Postgres DSN and postgres_only live mode, so PostgreSQL is the sole authoritative
        # backend and should_prefer_read / should_skip_sqlite_fallback are always True.
        if not control_plane_postgres_dsn:
            raise RuntimeError(
                "ControlPlaneStore requires a resolved control-plane Postgres DSN "
                "(set SOURCING_CONTROL_PLANE_POSTGRES_DSN, provide a .local-postgres.env, or run "
                "`make local-pg-up`). SQLite-authoritative control-plane storage is no longer supported."
            )
        if control_plane_postgres_mode != "postgres_only":
            raise RuntimeError(
                "ControlPlaneStore requires SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only "
                f"(resolved mode={control_plane_postgres_mode!r}). The 'disabled', 'mirror', and "
                "'prefer_postgres' control-plane modes are no longer supported."
            )
        self._control_plane_postgres = LiveControlPlanePostgresAdapter(
            runtime_dir=runtime_dir,
            dsn=control_plane_postgres_dsn,
            mode=control_plane_postgres_mode,
        )
        # Track B B4.3f (shadow deleted): the SQLite compatibility connection is gone. Every
        # read/write routes to PG; the PG schema is created by the versioned migration runner via
        # the adapter's ensure_bootstrapped(), and the legacy target-public-web migration tables
        # by the adapter's native DDL inside the migration table context.
        # Track B ②: per-domain repositories are the public data API; migrated domains live here
        # and their former facade methods are deleted from this class (no dual-track).
        self.repos = ControlPlaneRepositories(self._control_plane_postgres, job_lookup=self.get_job)

    def control_plane_postgres_live_mode(self) -> str:
        return str(getattr(self._control_plane_postgres, "mode", "disabled") or "disabled")

    def control_plane_postgres_is_postgres_only(self) -> bool:
        return self.control_plane_postgres_live_mode() == "postgres_only"

    # B4.3f: the SQLite compatibility shadow is deleted. These accessors survive as inert
    # status labels for CLI/status surfaces; nothing may treat the return values as a
    # connectable SQLite target.
    def compatibility_shadow_backend(self) -> str:
        return "retired"

    def compatibility_shadow_connect_target(self) -> str:
        return ""

    def compatibility_shadow_seed_path(self) -> str:
        return str(self.db_path)

    def compatibility_shadow_is_ephemeral(self) -> bool:
        return True

    def close(self) -> None:
        """Dispose persistent control-plane handles (idempotent teardown hook).

        Closes the live Postgres adapter's connection pool when present. Safe
        to call repeatedly; multi-runtime processes (test harnesses, scripted
        runtimes) must call this so adapter pools do not accumulate per runtime.
        """

        adapter = getattr(self, "_control_plane_postgres", None)
        adapter_close = getattr(adapter, "close", None)
        if callable(adapter_close):
            try:
                adapter_close()
            except Exception:
                pass

    @contextmanager
    def legacy_target_public_web_migration_write_context(self, reason: str = ""):
        """Allow reviewed migration/cold-backup code to seed retired Public Web rows.

        Normal runtime code must not write `target_candidate_public_web_*` rows.
        This context keeps historical migration tests explicit while letting the
        storage methods fail closed for accidental normal-path calls.
        """

        with self.legacy_target_public_web_migration_read_context(reason or "legacy_migration_write"):
            previous_reason = self._legacy_target_public_web_migration_write_reason
            self._legacy_target_public_web_migration_write_depth += 1
            self._legacy_target_public_web_migration_write_reason = str(reason or "").strip() or "legacy_migration"
            try:
                yield
            finally:
                self._legacy_target_public_web_migration_write_depth = max(
                    0,
                    self._legacy_target_public_web_migration_write_depth - 1,
                )
                self._legacy_target_public_web_migration_write_reason = (
                    previous_reason if self._legacy_target_public_web_migration_write_depth else ""
                )

    @contextmanager
    def legacy_target_public_web_migration_read_context(self, reason: str = ""):
        context_factory = getattr(
            self._control_plane_postgres,
            "legacy_target_public_web_migration_table_context",
            None,
        )
        if callable(context_factory):
            with context_factory(str(reason or "").strip() or "legacy_migration_read"):
                yield
            return
        yield

    def _require_legacy_target_public_web_migration_write(self, table_name: str) -> None:
        if int(getattr(self, "_legacy_target_public_web_migration_write_depth", 0) or 0) > 0:
            prepare_schema = getattr(
                self._control_plane_postgres,
                "ensure_legacy_target_public_web_migration_write_schema",
                None,
            )
            if callable(prepare_schema):
                prepare_schema(table_name)
            return
        raise RuntimeError(
            "legacy_target_candidate_public_web_write_retired: "
            f"{table_name} is migration-only after W7e; normal Public Web code must use crm_public_web_* "
            "storage and workflow_commands. Seed historical rows through legacy_public_web_storage.seed_* "
            "or a reviewed migration write context."
        )

    # Legacy aliases kept until the last sqlite_* debug/test call sites are removed.
    def sqlite_shadow_backend(self) -> str:
        return self.compatibility_shadow_backend()

    def sqlite_shadow_connect_target(self) -> str:
        return self.compatibility_shadow_connect_target()

    def sqlite_shadow_is_ephemeral(self) -> bool:
        return self.compatibility_shadow_is_ephemeral()

    def bootstrap_candidate_store_enabled(self) -> bool:
        return self._bootstrap_candidate_store_loaded or _env_flag_enabled(
            os.getenv("SOURCING_ENABLE_BOOTSTRAP_CANDIDATE_STORE"),
            default=False,
        )

    def bootstrap_candidate_store_loaded(self) -> bool:
        return bool(self._bootstrap_candidate_store_loaded)

    def candidate_documents_fallback_enabled(self) -> bool:
        return _env_flag_enabled(
            os.getenv("SOURCING_ENABLE_LEGACY_CANDIDATE_DOCUMENTS_FALLBACK"),
            default=False,
        )

    def _control_plane_postgres_should_prefer_read(self, table_name: str) -> bool:
        return bool(self._control_plane_postgres.should_prefer_read(table_name))

    def _control_plane_postgres_is_authoritative(self, table_name: str) -> bool:
        predicate = getattr(self._control_plane_postgres, "is_authoritative", None)
        if not callable(predicate):
            return False
        try:
            return bool(predicate(table_name))
        except Exception:
            return False

    def _control_plane_postgres_should_skip_sqlite_fallback(self, table_name: str) -> bool:
        return bool(
            self._control_plane_postgres_should_prefer_read(table_name)
            and self._control_plane_postgres_is_authoritative(table_name)
        )

    def _require_postgres_for_durable_runtime(self, table_name: str) -> None:
        normalized_table = str(table_name or "").strip()
        if normalized_table not in _DURABLE_RUNTIME_TABLES:
            return
        if self.control_plane_postgres_is_postgres_only():
            return
        raise RuntimeError(
            f"{normalized_table} is PG-only durable runtime storage. "
            "Set SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only with a resolved Postgres DSN; "
            "SQLite durable runtime execution is not a normal path."
        )

    @contextmanager
    def profile_prefetch_scheduler_lock(self, *, source_job: str, snapshot_dir: str) -> Any:
        """Serialize profile-prefetch replan/claim for one job snapshot.

        Production PG uses a transaction-scoped try-advisory lock. A busy PG
        lock is a cooperative yield signal for the caller, not permission to
        block a recovery tick. SQLite/local compatibility uses an in-process
        reentrant lock; that path is sufficient for unit tests but is not a
        distributed deployment contract.
        """

        normalized_source_job = str(source_job or "").strip()
        normalized_snapshot_dir = str(snapshot_dir or "").strip()
        if not normalized_source_job or not normalized_snapshot_dir:
            yield
            return
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry"):
            lock_context = getattr(self._control_plane_postgres, "profile_prefetch_scheduler_lock", None)
            if callable(lock_context):
                try:
                    with lock_context(
                        source_job=normalized_source_job,
                        snapshot_dir=normalized_snapshot_dir,
                    ) as lock_evidence:
                        yield dict(
                            lock_evidence
                            or {
                                "kind": "pg_try_advisory_xact_lock",
                                "lock_kind": "pg_try_advisory_xact_lock",
                                "distributed": True,
                                "acquired": True,
                                "busy": False,
                                "source": "control_plane_live_postgres",
                            }
                        )
                    return
                except Exception as exc:
                    if self._control_plane_postgres_should_skip_sqlite_fallback("linkedin_profile_registry"):
                        self._raise_control_plane_postgres_write_failure(
                            table_name="linkedin_profile_registry",
                            method_name="profile_prefetch_scheduler_lock",
                            reason=f"{type(exc).__name__}: {exc}",
                            error=exc,
                        )
        lock_key = f"{normalized_source_job}\n{normalized_snapshot_dir}"
        with self._profile_prefetch_scheduler_lock_guard:
            lock = self._profile_prefetch_scheduler_locks.get(lock_key)
            if lock is None:
                lock = threading.RLock()
                self._profile_prefetch_scheduler_locks[lock_key] = lock
        with lock:
            yield {
                "kind": "in_process_rlock",
                "lock_kind": "in_process_rlock",
                "distributed": False,
                "lock_key": lock_key,
                "source": "sqlite_compatibility",
                "scope": "source_job_snapshot_dir",
            }

    @contextmanager
    def board_visible_patch_publication_lock(self, *, job_id: str, snapshot_id: str) -> Any:
        """Serialize board-visible patch sequence/cumulative publication.

        `board_visible_delta_apply` items may be claimed independently, but patch
        publication reads and rewrites shared per-job/snapshot sequence and
        cumulative candidate ids. Production PG therefore uses a transaction-scoped
        advisory lock; SQLite/local compatibility uses an in-process reentrant lock.
        """

        normalized_job_id = str(job_id or "").strip()
        normalized_snapshot_id = str(snapshot_id or "").strip()
        if not normalized_job_id or not normalized_snapshot_id:
            yield
            return
        if self._control_plane_postgres_should_prefer_read("job_board_visible_patches"):
            lock_context = getattr(self._control_plane_postgres, "board_visible_patch_publication_lock", None)
            if callable(lock_context):
                try:
                    with lock_context(
                        job_id=normalized_job_id,
                        snapshot_id=normalized_snapshot_id,
                    ) as lock_evidence:
                        yield dict(
                            lock_evidence
                            or {
                                "kind": "pg_advisory_xact_lock",
                                "lock_kind": "pg_advisory_xact_lock",
                                "distributed": True,
                                "source": "control_plane_live_postgres",
                            }
                        )
                    return
                except Exception as exc:
                    if self._control_plane_postgres_should_skip_sqlite_fallback("job_board_visible_patches"):
                        self._raise_control_plane_postgres_write_failure(
                            table_name="job_board_visible_patches",
                            method_name="board_visible_patch_publication_lock",
                            reason=f"{type(exc).__name__}: {exc}",
                            error=exc,
                        )
        lock_key = f"{normalized_job_id}\n{normalized_snapshot_id}"
        with self._board_visible_patch_publication_lock_guard:
            lock = self._board_visible_patch_publication_locks.get(lock_key)
            if lock is None:
                lock = threading.RLock()
                self._board_visible_patch_publication_locks[lock_key] = lock
        with lock:
            yield {
                "kind": "in_process_rlock",
                "lock_kind": "in_process_rlock",
                "distributed": False,
                "lock_key": lock_key,
                "source": "sqlite_compatibility",
                "scope": "job_snapshot_board_visible_publication",
            }

    def _control_plane_postgres_native_table_name(self, method_name: str, kwargs: dict[str, Any]) -> str:
        explicit_table_name = str(kwargs.get("table_name") or "").strip()
        if explicit_table_name:
            return explicit_table_name
        return str(_CONTROL_PLANE_POSTGRES_NATIVE_TABLES.get(str(method_name or "").strip()) or "").strip()

    def _raise_control_plane_postgres_write_failure(
        self,
        *,
        table_name: str,
        method_name: str,
        reason: str,
        error: Exception | None = None,
    ) -> None:
        message = f"Postgres authoritative write failed for {table_name} via {method_name}: {reason}"
        if error is not None:
            raise RuntimeError(message) from error
        raise RuntimeError(message)

    def _raise_control_plane_postgres_read_failure(
        self,
        *,
        table_name: str,
        method_name: str,
        reason: str,
        error: Exception | None = None,
    ) -> None:
        message = f"Postgres authoritative read failed for {table_name} via {method_name}: {reason}"
        if error is not None:
            raise RuntimeError(message) from error
        raise RuntimeError(message)

    def _call_control_plane_postgres_native(self, method_name: str, /, *args: Any, **kwargs: Any) -> Any:
        normalized_method_name = str(method_name or "").strip()
        _validate_bulk_upsert_rows_call_contract(
            normalized_method_name,
            positional_args=args,
            keyword_args=kwargs,
        )
        table_name = self._control_plane_postgres_native_table_name(normalized_method_name, kwargs)
        strict_no_fallback = bool(table_name and self._control_plane_postgres_should_skip_sqlite_fallback(table_name))
        native_read = normalized_method_name in _CONTROL_PLANE_POSTGRES_NATIVE_READ_METHODS
        method = getattr(self._control_plane_postgres, method_name, None)
        if method is None:
            if strict_no_fallback:
                if native_read:
                    self._raise_control_plane_postgres_read_failure(
                        table_name=table_name,
                        method_name=normalized_method_name,
                        reason="native reader is unavailable",
                    )
                else:
                    self._raise_control_plane_postgres_write_failure(
                        table_name=table_name,
                        method_name=normalized_method_name,
                        reason="native writer is unavailable",
                    )
            return None
        try:
            return method(*args, **kwargs)
        except Exception as exc:
            if strict_no_fallback:
                if native_read:
                    self._raise_control_plane_postgres_read_failure(
                        table_name=table_name,
                        method_name=normalized_method_name,
                        reason=f"{type(exc).__name__}: {exc}",
                        error=exc,
                    )
                else:
                    self._raise_control_plane_postgres_write_failure(
                        table_name=table_name,
                        method_name=normalized_method_name,
                        reason=f"{type(exc).__name__}: {exc}",
                        error=exc,
                    )
            return None

    def workflow_job_coordination_uses_postgres(self) -> bool:
        return bool(
            self._control_plane_postgres_should_prefer_read("workflow_job_leases")
            and callable(getattr(self._control_plane_postgres, "acquire_workflow_job_lease", None))
        )

    def _write_control_plane_row_to_postgres(self, table_name: str, row: dict[str, Any] | None) -> bool:
        if row is None or not self._control_plane_postgres_should_prefer_read(table_name):
            return False
        try:
            self._control_plane_postgres.upsert_row(table_name, dict(row))
        except Exception as exc:
            if self._control_plane_postgres_should_skip_sqlite_fallback(table_name):
                self._raise_control_plane_postgres_write_failure(
                    table_name=table_name,
                    method_name="upsert_row",
                    reason=f"{type(exc).__name__}: {exc}",
                    error=exc,
                )
            return False
        return True

    def _select_control_plane_job_rows(
        self,
        *,
        where_sql: str = "",
        params: list[Any] | tuple[Any, ...] = (),
        order_by_sql: str = "updated_at DESC, created_at DESC",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return self._select_control_plane_rows(
            "jobs",
            row_builder=self._job_from_row,
            where_sql=where_sql,
            params=params,
            order_by_sql=order_by_sql,
            limit=limit,
        )

    def _select_control_plane_rows(
        self,
        table_name: str,
        *,
        row_builder: Any,
        where_sql: str = "",
        params: list[Any] | tuple[Any, ...] = (),
        order_by_sql: str = "",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if not self._control_plane_postgres_should_prefer_read(table_name):
            return []
        try:
            rows = self._control_plane_postgres.select_many(
                table_name,
                where_sql=where_sql,
                params=list(params),
                order_by_sql=order_by_sql,
                limit=limit,
                offset=offset,
            )
        except Exception as exc:
            if self._control_plane_postgres_should_skip_sqlite_fallback(table_name):
                self._raise_control_plane_postgres_read_failure(
                    table_name=table_name,
                    method_name="select_many",
                    reason=f"{type(exc).__name__}: {exc}",
                    error=exc,
                )
            return []
        if not rows:
            return []
        return [row_builder(row) for row in rows]

    def _select_control_plane_row(
        self,
        table_name: str,
        *,
        row_builder: Any,
        where_sql: str,
        params: list[Any] | tuple[Any, ...],
        order_by_sql: str = "",
    ) -> dict[str, Any] | None:
        if not self._control_plane_postgres_should_prefer_read(table_name):
            return None
        try:
            row = self._control_plane_postgres.select_one(
                table_name,
                where_sql=where_sql,
                params=list(params),
                order_by_sql=order_by_sql,
            )
        except Exception as exc:
            if self._control_plane_postgres_should_skip_sqlite_fallback(table_name):
                self._raise_control_plane_postgres_read_failure(
                    table_name=table_name,
                    method_name="select_one",
                    reason=f"{type(exc).__name__}: {exc}",
                    error=exc,
                )
            return None
        if row is None:
            return None
        return row_builder(row)

    def replace_bootstrap_data(self, candidates: list[Candidate], evidence: list[EvidenceRecord]) -> None:
        if self._replace_candidates_and_evidence_in_postgres(
            current_candidate_rows=self._select_postgres_candidate_rows(limit=0),
            candidates=candidates,
            evidence=evidence,
        ):
            self._bootstrap_candidate_store_loaded = True
            return
        raise RuntimeError(
            "postgres-only invariant violated for candidates/evidence in replace_bootstrap_data: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def replace_company_data(
        self, target_company: str, candidates: list[Candidate], evidence: list[EvidenceRecord]
    ) -> None:
        if self._replace_candidates_and_evidence_in_postgres(
            current_candidate_rows=self._select_postgres_candidate_rows(
                where_sql="lower(target_company) = lower(%s)",
                params=[target_company],
                limit=0,
            ),
            candidates=candidates,
            evidence=evidence,
        ):
            return
        raise RuntimeError(
            "postgres-only invariant violated for candidates/evidence in replace_company_data: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def replace_company_candidate_data(
        self,
        target_company: str,
        candidate_ids: list[str] | tuple[str, ...],
        candidates: list[Candidate],
        evidence: list[EvidenceRecord],
    ) -> None:
        normalized_candidate_ids = _dedupe_preserve_order(
            [
                str(candidate_id or "").strip()
                for candidate_id in list(candidate_ids or [])
                if str(candidate_id or "").strip()
            ]
        )
        if not normalized_candidate_ids:
            return
        filtered_candidates = [
            candidate
            for candidate in list(candidates or [])
            if str(candidate.candidate_id or "").strip() in set(normalized_candidate_ids)
        ]
        filtered_candidate_ids = {
            str(candidate.candidate_id or "").strip()
            for candidate in filtered_candidates
            if str(candidate.candidate_id or "").strip()
        }
        filtered_evidence = [
            item for item in list(evidence or []) if str(item.candidate_id or "").strip() in filtered_candidate_ids
        ]
        if self._replace_candidates_and_evidence_in_postgres(
            current_candidate_rows=self._select_postgres_candidate_rows(
                where_sql="lower(target_company) = lower(%s) AND candidate_id = ANY(%s)",
                params=[target_company, normalized_candidate_ids],
                limit=0,
            ),
            candidates=filtered_candidates,
            evidence=filtered_evidence,
            conflict_candidate_ids=normalized_candidate_ids,
        ):
            return
        raise RuntimeError(
            "postgres-only invariant violated for candidates in replace_company_candidate_data: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def replace_company_category_data(
        self,
        target_company: str,
        category: str,
        candidates: list[Candidate],
        evidence: list[EvidenceRecord],
    ) -> None:
        incoming_candidate_ids = _dedupe_preserve_order(
            [str(candidate.candidate_id or "").strip() for candidate in list(candidates or [])]
        )
        if self._replace_candidates_and_evidence_in_postgres(
            current_candidate_rows=self._select_postgres_candidate_rows(
                where_sql="lower(target_company) = lower(%s) AND lower(category) = lower(%s)",
                params=[target_company, category],
                limit=0,
            ),
            candidates=candidates,
            evidence=evidence,
            conflict_candidate_ids=incoming_candidate_ids,
        ):
            return
        raise RuntimeError(
            "postgres-only invariant violated for candidates in replace_company_category_data: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def _replace_candidates_and_evidence_in_postgres(
        self,
        *,
        current_candidate_rows: list[dict[str, Any]],
        candidates: list[Candidate],
        evidence: list[EvidenceRecord],
        conflict_candidate_ids: list[str] | None = None,
    ) -> bool:
        if not (
            self._control_plane_postgres_should_prefer_read("candidates")
            and self._control_plane_postgres_should_prefer_read("evidence")
        ):
            return False
        current_candidate_ids = [
            str(row.get("candidate_id") or "").strip()
            for row in list(current_candidate_rows or [])
            if str(row.get("candidate_id") or "").strip()
        ]
        delete_candidate_ids = _dedupe_preserve_order(
            [
                *current_candidate_ids,
                *[
                    str(candidate_id or "").strip()
                    for candidate_id in list(conflict_candidate_ids or [])
                    if str(candidate_id or "").strip()
                ],
            ]
        )
        candidate_payloads = self._dedupe_candidate_payloads(candidates)
        evidence_payloads = self._dedupe_evidence_payloads(evidence)
        try:
            if delete_candidate_ids:
                placeholders = ", ".join("%s" for _ in delete_candidate_ids)
                self._call_control_plane_postgres_native(
                    "delete_rows",
                    table_name="evidence",
                    where_sql=f"candidate_id IN ({placeholders})",
                    params=delete_candidate_ids,
                )
                self._call_control_plane_postgres_native(
                    "delete_rows",
                    table_name="candidates",
                    where_sql=f"candidate_id IN ({placeholders})",
                    params=delete_candidate_ids,
                )
            if candidate_payloads:
                self._control_plane_postgres.bulk_upsert_rows("candidates", candidate_payloads)
            if evidence_payloads:
                self._control_plane_postgres.bulk_upsert_rows("evidence", evidence_payloads)
        except Exception:
            if self._control_plane_postgres_should_skip_sqlite_fallback(
                "candidates"
            ) or self._control_plane_postgres_should_skip_sqlite_fallback("evidence"):
                raise
            return False
        return True

    def _select_postgres_candidate_rows(
        self,
        *,
        where_sql: str = "",
        params: list[Any] | tuple[Any, ...] = (),
        order_by_sql: str = "",
        limit: int = 0,
    ) -> list[dict[str, Any]]:
        if not self._control_plane_postgres_should_prefer_read("candidates"):
            return []
        try:
            rows = self._control_plane_postgres.select_many(
                "candidates",
                where_sql=where_sql,
                params=list(params),
                order_by_sql=order_by_sql,
                limit=limit,
            )
        except Exception:
            return []
        return [dict(row) for row in list(rows or []) if isinstance(row, dict)]

    def _select_postgres_evidence_rows(
        self,
        *,
        where_sql: str = "",
        params: list[Any] | tuple[Any, ...] = (),
        order_by_sql: str = "",
        limit: int = 0,
    ) -> list[dict[str, Any]]:
        if not self._control_plane_postgres_should_prefer_read("evidence"):
            return []
        try:
            rows = self._control_plane_postgres.select_many(
                "evidence",
                where_sql=where_sql,
                params=list(params),
                order_by_sql=order_by_sql,
                limit=limit,
            )
        except Exception:
            return []
        return [dict(row) for row in list(rows or []) if isinstance(row, dict)]

    def _select_postgres_job_result_rows(
        self,
        *,
        where_sql: str = "",
        params: list[Any] | tuple[Any, ...] = (),
        limit: int = 0,
    ) -> list[dict[str, Any]]:
        if not self._control_plane_postgres_should_prefer_read("job_results"):
            return []
        try:
            rows = self._control_plane_postgres.select_many(
                "job_results",
                where_sql=where_sql,
                params=list(params),
                order_by_sql="",
                limit=limit,
            )
        except Exception:
            return []
        return sorted(
            [dict(row) for row in list(rows or []) if isinstance(row, dict)],
            key=lambda row: (int(row.get("rank_index") or 0), str(row.get("candidate_id") or "")),
        )

    def candidate_count(self) -> int:
        postgres_rows = self._select_postgres_candidate_rows(limit=0)
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("candidates"):
            return len(postgres_rows)
        raise RuntimeError(
            "postgres-only invariant violated for candidates in candidate_count: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def candidate_count_for_company(self, target_company: str) -> int:
        postgres_rows = self._select_postgres_candidate_rows(
            where_sql="lower(target_company) = lower(%s)",
            params=[target_company],
            limit=0,
        )
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("candidates"):
            return len(postgres_rows)
        raise RuntimeError(
            "postgres-only invariant violated for candidates in candidate_count_for_company: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_candidates(self) -> list[Candidate]:
        postgres_rows = self._select_postgres_candidate_rows(limit=0)
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("candidates"):
            return sorted(
                [self._candidate_from_row(row) for row in postgres_rows],
                key=lambda candidate: str(candidate.name_en or "").lower(),
            )
        raise RuntimeError(
            "postgres-only invariant violated for candidates in list_candidates: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_candidates_for_company(self, target_company: str) -> list[Candidate]:
        postgres_rows = self._select_postgres_candidate_rows(
            where_sql="lower(target_company) = lower(%s)",
            params=[target_company],
            limit=0,
        )
        return sorted(
            [self._candidate_from_row(row) for row in postgres_rows],
            key=lambda candidate: str(candidate.name_en or "").lower(),
        )

    def find_candidate_by_linkedin_url(self, linkedin_url: str) -> Candidate | None:
        normalized_key = _normalize_linkedin_profile_url_key(linkedin_url)
        if not normalized_key:
            return None
        lookup_values = _dedupe_preserve_order(
            [
                str(linkedin_url or "").strip().lower(),
                normalized_key,
                f"{normalized_key}/",
            ]
        )
        postgres_rows = self._select_postgres_candidate_rows(
            where_sql="lower(linkedin_url) IN (" + ", ".join(["%s"] * len(lookup_values)) + ")",
            params=lookup_values,
            limit=0,
        )
        candidates: list[Candidate] = []
        if postgres_rows:
            candidates = [self._candidate_from_row(row) for row in postgres_rows]
        if not candidates:
            return None
        return max(candidates, key=_candidate_richness_score_for_store_match)

    def get_candidate(self, candidate_id: str) -> Candidate | None:
        normalized_candidate_id = str(candidate_id or "").strip()
        if not normalized_candidate_id:
            return None
        postgres_row = self._select_control_plane_row(
            "candidates",
            row_builder=self._candidate_from_row,
            where_sql="candidate_id = %s",
            params=[normalized_candidate_id],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def find_candidate_by_name(self, *, target_company: str, name_en: str) -> Candidate | None:
        normalized_name = str(name_en or "").strip()
        normalized_company = str(target_company or "").strip()
        if not normalized_name:
            return None
        postgres_row = self._select_control_plane_row(
            "candidates",
            row_builder=self._candidate_from_row,
            where_sql="lower(target_company) = lower(%s) AND lower(name_en) = lower(%s)",
            params=[normalized_company, normalized_name],
        )
        if postgres_row is not None:
            return postgres_row
        if self._control_plane_postgres_should_skip_sqlite_fallback("candidates"):
            return None
        raise RuntimeError(
            "postgres-only invariant violated for candidates in find_candidate_by_name: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def upsert_candidate(self, candidate: Candidate) -> Candidate:
        payload = self._candidate_payload(candidate)
        if self._write_control_plane_row_to_postgres("candidates", payload):
            return self.get_candidate(candidate.candidate_id) or candidate
        self._raise_control_plane_postgres_write_failure(
            table_name="candidates",
            method_name="upsert_candidate",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def list_evidence(self, candidate_id: str) -> list[dict[str, Any]]:
        normalized_candidate_id = str(candidate_id or "").strip()
        postgres_rows = self._select_postgres_evidence_rows(
            where_sql="candidate_id = %s",
            params=[normalized_candidate_id],
            limit=0,
        )
        return sorted(
            [self._evidence_payload_from_row(row, include_candidate_id=False) for row in postgres_rows],
            key=lambda row: (str(row.get("title") or ""), str(row.get("evidence_id") or "")),
        )

    def list_evidence_for_company(self, target_company: str) -> list[dict[str, Any]]:
        company_candidates = self.list_candidates_for_company(target_company)
        candidate_ids = [
            candidate.candidate_id for candidate in company_candidates if str(candidate.candidate_id or "").strip()
        ]
        postgres_rows: list[dict[str, Any]] = []
        if candidate_ids:
            placeholders = ", ".join("%s" for _ in candidate_ids)
            postgres_rows = self._select_postgres_evidence_rows(
                where_sql=f"candidate_id IN ({placeholders})",
                params=candidate_ids,
                limit=0,
            )
        if (
            postgres_rows
            or (not candidate_ids and self._control_plane_postgres_should_skip_sqlite_fallback("evidence"))
            or self._control_plane_postgres_should_skip_sqlite_fallback("candidates")
        ):
            return sorted(
                [self._evidence_payload_from_row(row, include_candidate_id=True) for row in postgres_rows],
                key=lambda row: (str(row.get("candidate_id") or ""), str(row.get("title") or "")),
            )
        raise RuntimeError(
            "postgres-only invariant violated for evidence in list_evidence_for_company: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def upsert_evidence_records(self, evidence: list[EvidenceRecord]) -> list[dict[str, Any]]:
        if not evidence:
            return []
        evidence_payloads = [self._evidence_payload(item) for item in evidence]
        if all(self._write_control_plane_row_to_postgres("evidence", payload) for payload in evidence_payloads):
            return self.list_evidence(evidence[0].candidate_id)
        self._raise_control_plane_postgres_write_failure(
            table_name="evidence",
            method_name="upsert_evidence_records",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    @staticmethod
    def _job_storage_row(
        *,
        job_id: str,
        job_type: str,
        status: str,
        stage: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any] | None = None,
        execution_bundle_payload: dict[str, Any] | None = None,
        summary_payload: dict[str, Any] | None = None,
        artifact_path: str = "",
        requester_id: str = "",
        tenant_id: str = "",
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        execution_bundle_value = dict(execution_bundle_payload or {})
        matching_bundle = _matching_bundle_payload(
            request_payload,
            execution_bundle_payload=execution_bundle_value,
        )
        return {
            "job_id": str(job_id or "").strip(),
            "job_type": str(job_type or "").strip(),
            "status": str(status or "").strip(),
            "stage": str(stage or "").strip(),
            "request_json": json.dumps(_json_safe_payload(request_payload), ensure_ascii=False),
            "plan_json": json.dumps(_json_safe_payload(plan_payload or {}), ensure_ascii=False),
            "execution_bundle_json": json.dumps(_json_safe_payload(execution_bundle_value), ensure_ascii=False),
            "matching_request_json": json.dumps(_json_safe_payload(matching_bundle), ensure_ascii=False),
            "summary_json": json.dumps(_json_safe_payload(summary_payload or {}), ensure_ascii=False),
            "artifact_path": str(artifact_path or ""),
            "request_signature": request_signature(request_payload),
            "request_family_signature": request_family_signature(request_payload),
            "matching_request_signature": str(matching_bundle.get("matching_request_signature") or ""),
            "matching_request_family_signature": str(matching_bundle.get("matching_request_family_signature") or ""),
            "requester_id": str(requester_id or "").strip(),
            "tenant_id": str(tenant_id or "").strip(),
            "idempotency_key": str(idempotency_key or "").strip(),
        }

    def save_job(
        self,
        job_id: str,
        job_type: str,
        status: str,
        stage: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any] | None = None,
        execution_bundle_payload: dict[str, Any] | None = None,
        summary_payload: dict[str, Any] | None = None,
        artifact_path: str = "",
        requester_id: str = "",
        tenant_id: str = "",
        idempotency_key: str = "",
    ) -> None:
        row_payload = self._job_storage_row(
            job_id=job_id,
            job_type=job_type,
            status=status,
            stage=stage,
            request_payload=request_payload,
            plan_payload=plan_payload,
            execution_bundle_payload=execution_bundle_payload,
            summary_payload=summary_payload,
            artifact_path=artifact_path,
            requester_id=requester_id,
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
        )
        if self._control_plane_postgres_should_prefer_read("jobs"):
            row = self._call_control_plane_postgres_native(
                "save_job_row",
                row=row_payload,
                protect_terminal_statuses=True,
                terminal_statuses=sorted(_TERMINAL_JOB_STATUSES),
            )
            if row is not None:
                return
            if self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
                # Track B B4.1b: PG is the sole authoritative store. save_job_row returns None ONLY when
                # its terminal-status protection skips a terminal->non-terminal downgrade (a deliberate
                # no-op; real PG errors raise via strict-no-fallback). The write is complete either way —
                # never fall through to the dead SQLite shadow tail below (which would write the row to a
                # never-read shadow, and would break once init_schema stops creating shadow tables).
                return
        raise RuntimeError(
            "postgres-only invariant violated for jobs in save_job: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def save_job_if_owned(
        self,
        *,
        expected_requester_id: str,
        expected_tenant_id: str,
        job_id: str,
        job_type: str,
        status: str,
        stage: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any] | None = None,
        execution_bundle_payload: dict[str, Any] | None = None,
        summary_payload: dict[str, Any] | None = None,
        artifact_path: str = "",
        requester_id: str = "",
        tenant_id: str = "",
        idempotency_key: str = "",
        expected_job_type: str = "",
        expected_statuses: tuple[str, ...] = (),
        expected_stage: str = "",
        forbidden_statuses: tuple[str, ...] = (),
        expected_summary_fields: dict[str, Any] | None = None,
        forbidden_summary_values: dict[str, tuple[str, ...]] | None = None,
    ) -> dict[str, Any]:
        """Atomically update an existing job under exact owner and state guards."""

        normalized_requester = str(expected_requester_id or "").strip()
        normalized_tenant = str(expected_tenant_id or "").strip()
        if not normalized_requester or not normalized_tenant:
            return {"status": "owner_miss"}
        row_payload = self._job_storage_row(
            job_id=job_id,
            job_type=job_type,
            status=status,
            stage=stage,
            request_payload=request_payload,
            plan_payload=plan_payload,
            execution_bundle_payload=execution_bundle_payload,
            summary_payload=summary_payload,
            artifact_path=artifact_path,
            requester_id=requester_id,
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
        )
        result = self._call_control_plane_postgres_native(
            "update_job_row_if_owned",
            row=row_payload,
            expected_requester_id=normalized_requester,
            expected_tenant_id=normalized_tenant,
            expected_job_type=str(expected_job_type or "").strip(),
            expected_statuses=list(expected_statuses),
            expected_stage=str(expected_stage or "").strip(),
            forbidden_statuses=list(forbidden_statuses),
            expected_summary_fields=dict(expected_summary_fields or {}),
            forbidden_summary_values={
                str(field): list(values) for field, values in dict(forbidden_summary_values or {}).items()
            },
        )
        if not isinstance(result, dict) or str(result.get("status") or "") not in {
            "applied",
            "owner_miss",
            "state_conflict",
        }:
            self._raise_control_plane_postgres_write_failure(
                table_name="jobs",
                method_name="save_job_if_owned",
                reason="postgres-only: typed owner/state CAS returned no valid confirmation",
            )
        response = dict(result)
        raw_row = response.pop("row", None)
        if isinstance(raw_row, dict) and raw_row:
            response["job"] = self._job_from_row(raw_row)
        return response

    def append_job_event(
        self,
        job_id: str,
        stage: str,
        status: str,
        detail: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        payload_dict = _json_safe_payload(dict(payload or {}))
        if self._control_plane_postgres_should_prefer_read("job_events"):
            result = self._call_control_plane_postgres_native(
                "append_job_event",
                job_id=job_id,
                stage=stage,
                status=status,
                detail=detail,
                payload_dict=payload_dict,
            )
            if result is not None:
                return
            if self._control_plane_postgres_should_skip_sqlite_fallback("job_events"):
                # Track B B4.1b: PG is authoritative. The native append_job_event uses a plain
                # sequence-id INSERT...RETURNING (no ON CONFLICT) — it returns the row on success and only
                # raises on error; the sole non-exception None is the deliberate empty/blank job_id
                # rejection (a no-op). Return (void) rather than fall through to the dead SQLite shadow.
                return
        raise RuntimeError(
            "postgres-only invariant violated for job_events in append_job_event: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_job_progress_event_summary(self, job_id: str, *, hydrate_if_missing: bool = True) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("job_progress_event_summaries"):
            row = self._control_plane_postgres.select_one(
                "job_progress_event_summaries",
                where_sql="job_id = %s",
                params=[normalized_job_id],
            )
            if row is not None:
                return self._job_progress_event_summary_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("job_progress_event_summaries"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for job_progress_event_summaries in get_job_progress_event_summary: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def replace_job_results(self, job_id: str, results: list[dict[str, Any]]) -> None:
        normalized_job_id = str(job_id or "").strip()
        row_payloads = [
            {
                "job_id": normalized_job_id,
                "candidate_id": str(result.get("candidate_id") or "").strip(),
                "rank_index": int(result.get("rank") or 0),
                "score": result.get("score"),
                "confidence_label": result.get("confidence_label", ""),
                "confidence_score": result.get("confidence_score", 0.0),
                "confidence_reason": result.get("confidence_reason", ""),
                "explanation": result.get("explanation", ""),
                "matched_fields_json": json.dumps(result.get("matched_fields", []), ensure_ascii=False),
            }
            for result in results
            if str(result.get("candidate_id") or "").strip()
        ]
        if self._control_plane_postgres_should_prefer_read("job_results"):
            self._call_control_plane_postgres_native(
                "delete_rows",
                table_name="job_results",
                where_sql="job_id = %s",
                params=[normalized_job_id],
            )
            if all(self._write_control_plane_row_to_postgres("job_results", payload) for payload in row_payloads):
                if self._control_plane_postgres_should_skip_sqlite_fallback("job_results"):
                    return
        raise RuntimeError(
            "postgres-only invariant violated for job_results in replace_job_results: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return None
        if self._control_plane_postgres_should_prefer_read("jobs"):
            row = self._control_plane_postgres.select_one(
                "jobs",
                where_sql="job_id = %s",
                params=[normalized_job_id],
            )
            if row is not None:
                return self._job_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
                return None
        raise RuntimeError(
            "postgres-only invariant violated for jobs in get_job: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def upsert_job_result_view(
        self,
        *,
        job_id: str,
        target_company: str,
        source_kind: str,
        view_kind: str,
        snapshot_id: str = "",
        asset_view: str = "canonical_merged",
        source_path: str = "",
        authoritative_snapshot_id: str = "",
        materialization_generation_key: str = "",
        request_signature_value: str = "",
        summary: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {}
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        normalized_source_kind = str(source_kind or "").strip()
        normalized_view_kind = str(view_kind or "").strip()
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        normalized_source_path = _normalize_job_result_view_source_path(source_path)
        normalized_authoritative_snapshot_id = str(authoritative_snapshot_id or "").strip()
        normalized_generation_key = str(materialization_generation_key or "").strip()
        normalized_request_signature = str(request_signature_value or "").strip()
        summary_json = json.dumps(_json_safe_payload(summary or {}), ensure_ascii=False)
        metadata_json = json.dumps(_json_safe_payload(metadata or {}), ensure_ascii=False)
        view_seed = "|".join(
            [
                normalized_job_id,
                normalized_company_key,
                normalized_view_kind,
                normalized_source_kind,
                normalized_snapshot_id,
                normalized_asset_view,
            ]
        )
        existing = self.get_job_result_view(job_id=normalized_job_id)
        now = _utc_now_timestamp()
        view_id = (
            str((existing or {}).get("view_id") or "").strip()
            or f"jrv_{sha1(view_seed.encode('utf-8')).hexdigest()[:16]}"
        )
        row_payload = {
            "view_id": view_id,
            "job_id": normalized_job_id,
            "target_company": normalized_target_company,
            "company_key": normalized_company_key,
            "source_kind": normalized_source_kind,
            "view_kind": normalized_view_kind,
            "snapshot_id": normalized_snapshot_id,
            "asset_view": normalized_asset_view,
            "source_path": normalized_source_path,
            "authoritative_snapshot_id": normalized_authoritative_snapshot_id,
            "materialization_generation_key": normalized_generation_key,
            "request_signature": normalized_request_signature,
            "summary_json": summary_json,
            "metadata_json": metadata_json,
            "created_at": str((existing or {}).get("created_at") or "").strip() or now,
            "updated_at": now,
        }
        if self._write_control_plane_row_to_postgres("job_result_views", row_payload):
            return (
                self.get_job_result_view(job_id=normalized_job_id) or self._job_result_view_from_row(row_payload) or {}
            )
        self._raise_control_plane_postgres_write_failure(
            table_name="job_result_views",
            method_name="upsert_job_result_view",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_job_result_view(
        self,
        *,
        job_id: str = "",
        view_id: str = "",
    ) -> dict[str, Any] | None:
        normalized_job_id = str(job_id or "").strip()
        normalized_view_id = str(view_id or "").strip()
        if not normalized_job_id and not normalized_view_id:
            return None
        if self._control_plane_postgres_should_prefer_read("job_result_views"):
            clauses: list[str] = []
            params: list[Any] = []
            if normalized_job_id:
                clauses.append("job_id = %s")
                params.append(normalized_job_id)
            if normalized_view_id:
                clauses.append("view_id = %s")
                params.append(normalized_view_id)
            row = self._control_plane_postgres.select_one(
                "job_result_views",
                where_sql=" OR ".join(clauses),
                params=params,
                order_by_sql="updated_at DESC",
            )
            if row is not None:
                return self._job_result_view_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("job_result_views"):
                return None
        raise RuntimeError(
            "postgres-only invariant violated for job_result_views in get_job_result_view: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    # ------------------------------------------------------------------
    # Canonical job_result_lifecycle persistence (rebuild slice 1)
    # ------------------------------------------------------------------

    def upsert_job_board_visible_patch(
        self,
        *,
        patch_id: str,
        job_id: str,
        target_company: str = "",
        snapshot_id: str = "",
        baseline_snapshot_id: str = "",
        asset_view: str = "canonical_merged",
        patch_kind: str = "partial_delta_board_visible_patch",
        patch_phase: str = "board_visible_delta_applied",
        source: str = "",
        reason: str = "",
        sequence_index: int | None = None,
        candidate_ids: list[str] | tuple[str, ...] | None = None,
        cumulative_candidate_ids: list[str] | tuple[str, ...] | None = None,
        candidate_count: int | None = None,
        cumulative_candidate_count: int | None = None,
        served_candidate_count: int = 0,
        result_view_id: str = "",
        serving_projection_id: str = "",
        serving_projection_phase: str = "",
        overlay_path: str = "",
        published_at: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_patch_id = str(patch_id or "").strip()
        normalized_job_id = str(job_id or "").strip()
        if not normalized_patch_id or not normalized_job_id:
            return {}
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_baseline_snapshot_id = str(baseline_snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        normalized_candidate_ids = _dedupe_ordered_texts(candidate_ids or [])
        normalized_cumulative_candidate_ids = _dedupe_ordered_texts(cumulative_candidate_ids or [])
        resolved_candidate_count = (
            max(0, int(candidate_count or 0)) if candidate_count is not None else len(normalized_candidate_ids)
        )
        resolved_cumulative_count = (
            max(0, int(cumulative_candidate_count or 0))
            if cumulative_candidate_count is not None
            else len(normalized_cumulative_candidate_ids)
        )
        existing = self.get_job_board_visible_patch(normalized_patch_id) or {}
        if sequence_index is None:
            sequence_index = int(existing.get("sequence_index") or 0)
            if sequence_index <= 0:
                sequence_index = self.next_job_board_visible_patch_sequence(
                    job_id=normalized_job_id,
                    snapshot_id=normalized_snapshot_id,
                )
        now = _utc_now_timestamp()
        row_payload = {
            "patch_id": normalized_patch_id,
            "job_id": normalized_job_id,
            "target_company": normalized_target_company,
            "company_key": normalized_company_key,
            "snapshot_id": normalized_snapshot_id,
            "baseline_snapshot_id": normalized_baseline_snapshot_id,
            "asset_view": normalized_asset_view,
            "patch_kind": str(patch_kind or "partial_delta_board_visible_patch").strip()
            or "partial_delta_board_visible_patch",
            "patch_phase": str(patch_phase or "board_visible_delta_applied").strip() or "board_visible_delta_applied",
            "source": str(source or "").strip(),
            "reason": str(reason or "").strip(),
            "sequence_index": max(1, int(sequence_index or 1)),
            "candidate_count": resolved_candidate_count,
            "cumulative_candidate_count": resolved_cumulative_count,
            "served_candidate_count": max(0, int(served_candidate_count or 0)),
            "result_view_id": str(result_view_id or "").strip(),
            "serving_projection_id": str(serving_projection_id or "").strip(),
            "serving_projection_phase": str(serving_projection_phase or "").strip(),
            "overlay_path": str(overlay_path or "").strip(),
            "candidate_ids_json": json.dumps(_json_safe_payload(normalized_candidate_ids), ensure_ascii=False),
            "cumulative_candidate_ids_json": json.dumps(
                _json_safe_payload(normalized_cumulative_candidate_ids),
                ensure_ascii=False,
            ),
            "metadata_json": json.dumps(_json_safe_payload(metadata or {}), ensure_ascii=False),
            "published_at": str(published_at or "").strip() or now,
            "created_at": str(existing.get("created_at") or "").strip() or now,
            "updated_at": now,
        }
        if self._write_control_plane_row_to_postgres("job_board_visible_patches", row_payload):
            return self.get_job_board_visible_patch(normalized_patch_id) or self._job_board_visible_patch_from_row(
                row_payload
            )
        self._raise_control_plane_postgres_write_failure(
            table_name="job_board_visible_patches",
            method_name="upsert_job_board_visible_patch",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_job_board_visible_patch(self, patch_id: str) -> dict[str, Any]:
        normalized_patch_id = str(patch_id or "").strip()
        if not normalized_patch_id:
            return {}
        postgres_row = self._select_control_plane_row(
            "job_board_visible_patches",
            row_builder=self._job_board_visible_patch_from_row,
            where_sql="patch_id = %s",
            params=[normalized_patch_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_job_board_visible_patches(
        self,
        *,
        job_id: str,
        snapshot_id: str = "",
        limit: int = 0,
    ) -> list[dict[str, Any]]:
        normalized_job_id = str(job_id or "").strip()
        normalized_snapshot_id = str(snapshot_id or "").strip()
        if not normalized_job_id:
            return []
        clauses = ["job_id = ?"]
        params: list[Any] = [normalized_job_id]
        postgres_clauses = ["job_id = %s"]
        postgres_params: list[Any] = [normalized_job_id]
        if normalized_snapshot_id:
            clauses.append("snapshot_id = ?")
            params.append(normalized_snapshot_id)
            postgres_clauses.append("snapshot_id = %s")
            postgres_params.append(normalized_snapshot_id)
        postgres_rows = self._select_control_plane_rows(
            "job_board_visible_patches",
            row_builder=self._job_board_visible_patch_from_row,
            where_sql=" AND ".join(postgres_clauses),
            params=postgres_params,
            order_by_sql="sequence_index ASC, published_at ASC, created_at ASC",
            limit=max(0, int(limit or 0)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def next_job_board_visible_patch_sequence(self, *, job_id: str, snapshot_id: str = "") -> int:
        patches = self.list_job_board_visible_patches(job_id=job_id, snapshot_id=snapshot_id, limit=0)
        if not patches:
            return 1
        return max(int(patch.get("sequence_index") or 0) for patch in patches) + 1

    def _job_board_visible_patch_from_row(
        self,
        row: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if row is None:
            return {}

        def getter(key: str, default: Any = None) -> Any:
            if isinstance(row, dict):
                return row.get(key, default)
            try:
                return row[key]
            except (IndexError, KeyError):
                return default

        def load_json_list(key: str) -> list[str]:
            try:
                payload = json.loads(getter(key) or "[]")
            except (json.JSONDecodeError, TypeError):
                payload = []
            return _dedupe_ordered_texts(payload if isinstance(payload, list) else [])

        try:
            metadata_payload = json.loads(getter("metadata_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            metadata_payload = {}
        return {
            "patch_id": str(getter("patch_id") or ""),
            "job_id": str(getter("job_id") or ""),
            "target_company": str(getter("target_company") or ""),
            "company_key": str(getter("company_key") or ""),
            "snapshot_id": str(getter("snapshot_id") or ""),
            "baseline_snapshot_id": str(getter("baseline_snapshot_id") or ""),
            "asset_view": str(getter("asset_view") or "canonical_merged"),
            "patch_kind": str(getter("patch_kind") or ""),
            "patch_phase": str(getter("patch_phase") or ""),
            "source": str(getter("source") or ""),
            "reason": str(getter("reason") or ""),
            "sequence_index": int(getter("sequence_index") or 0),
            "candidate_count": int(getter("candidate_count") or 0),
            "cumulative_candidate_count": int(getter("cumulative_candidate_count") or 0),
            "served_candidate_count": int(getter("served_candidate_count") or 0),
            "result_view_id": str(getter("result_view_id") or ""),
            "serving_projection_id": str(getter("serving_projection_id") or ""),
            "serving_projection_phase": str(getter("serving_projection_phase") or ""),
            "overlay_path": str(getter("overlay_path") or ""),
            "candidate_ids": load_json_list("candidate_ids_json"),
            "cumulative_candidate_ids": load_json_list("cumulative_candidate_ids_json"),
            "metadata": metadata_payload if isinstance(metadata_payload, dict) else {},
            "published_at": str(getter("published_at") or ""),
            "created_at": getter("created_at"),
            "updated_at": getter("updated_at"),
        }

    def upsert_job_materialization_item(
        self,
        *,
        item_id: str,
        job_id: str,
        target_company: str = "",
        snapshot_id: str = "",
        baseline_snapshot_id: str = "",
        asset_view: str = "canonical_merged",
        item_kind: str = "board_visible_delta_apply",
        source: str = "",
        reason: str = "",
        status: str = "queued",
        phase: str = "queued",
        priority: int = 0,
        candidate_ids: list[str] | tuple[str, ...] | None = None,
        source_worker_ids: list[int] | tuple[int, ...] | None = None,
        idempotency_key: str = "",
        max_attempts: int = 5,
        serving_projection_id: str = "",
        not_before_at: str = "",
        last_error: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_item_id = str(item_id or "").strip()
        normalized_job_id = str(job_id or "").strip()
        if not normalized_item_id or not normalized_job_id:
            return {}
        existing = self.get_job_materialization_item(normalized_item_id) or {}
        existing_status = str(existing.get("status") or "").strip().lower()
        requested_status = str(status or "queued").strip().lower() or "queued"
        terminal_statuses = {"completed", "failed", "cancelled", "canceled", "superseded", "exhausted"}
        if existing_status in terminal_statuses and requested_status in {"queued", "deferred", "failed_retryable"}:
            return existing
        if existing_status == "running" and requested_status in {"queued", "deferred", "failed_retryable"}:
            return existing
        provided_metadata = dict(metadata or {})
        migration_adapter_write = _legacy_materialization_write_is_migration(provided_metadata, source=source)
        if _strict_legacy_materialization_write_gate_enabled() and not migration_adapter_write:
            raise RuntimeError(
                "normal-path job_materialization_items writes are blocked by "
                "SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES; "
                "write a typed workflow_command or mark the write as an explicit migration adapter."
            )
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        normalized_candidate_ids = _dedupe_ordered_texts(candidate_ids or existing.get("candidate_ids") or [])
        normalized_worker_ids: list[int] = []
        seen_worker_ids: set[int] = set()
        for value in list(source_worker_ids or existing.get("source_worker_ids") or []):
            try:
                worker_id = int(value or 0)
            except (TypeError, ValueError):
                continue
            if worker_id <= 0 or worker_id in seen_worker_ids:
                continue
            seen_worker_ids.add(worker_id)
            normalized_worker_ids.append(worker_id)
        now = _utc_now_timestamp()
        row_payload = {
            "item_id": normalized_item_id,
            "job_id": normalized_job_id,
            "target_company": normalized_target_company or str(existing.get("target_company") or ""),
            "company_key": normalized_company_key or str(existing.get("company_key") or ""),
            "snapshot_id": str(snapshot_id or existing.get("snapshot_id") or "").strip(),
            "baseline_snapshot_id": str(baseline_snapshot_id or existing.get("baseline_snapshot_id") or "").strip(),
            "asset_view": str(asset_view or existing.get("asset_view") or "canonical_merged").strip()
            or "canonical_merged",
            "item_kind": str(item_kind or existing.get("item_kind") or "board_visible_delta_apply").strip()
            or "board_visible_delta_apply",
            "source": str(source or existing.get("source") or "").strip(),
            "reason": str(reason or existing.get("reason") or "").strip(),
            "status": requested_status,
            "phase": str(phase or existing.get("phase") or requested_status or "queued").strip() or "queued",
            "priority": int(priority if priority is not None else existing.get("priority") or 0),
            "attempt_count": int(existing.get("attempt_count") or 0),
            "max_attempts": max(1, int(max_attempts or existing.get("max_attempts") or 5)),
            "candidate_count": len(normalized_candidate_ids),
            "candidate_ids_json": json.dumps(_json_safe_payload(normalized_candidate_ids), ensure_ascii=False),
            "source_worker_ids_json": json.dumps(_json_safe_payload(normalized_worker_ids), ensure_ascii=False),
            "idempotency_key": str(idempotency_key or existing.get("idempotency_key") or normalized_item_id).strip(),
            "result_patch_id": str(existing.get("result_patch_id") or "").strip(),
            "result_view_id": str(existing.get("result_view_id") or "").strip(),
            "serving_projection_id": str(serving_projection_id or existing.get("serving_projection_id") or "").strip(),
            "lease_owner": str(existing.get("lease_owner") or "").strip(),
            "lease_expires_at": str(existing.get("lease_expires_at") or "").strip(),
            "not_before_at": str(not_before_at or existing.get("not_before_at") or "").strip(),
            "last_error": (
                str(last_error or "").strip()
                if last_error is not None
                else str(existing.get("last_error") or "").strip()
            ),
            "metadata_json": json.dumps(
                _json_safe_payload(
                    {
                        **dict(existing.get("metadata") or {}),
                        **provided_metadata,
                        "legacy_materialization_write_contract": {
                            "table": "job_materialization_items",
                            "normal_path": not migration_adapter_write,
                            "migration_adapter": migration_adapter_write,
                            "target_runtime_table": "workflow_commands",
                            "retirement_phase": "Phase W4",
                        },
                    }
                ),
                ensure_ascii=False,
            ),
            "completed_at": str(existing.get("completed_at") or "").strip(),
            "created_at": str(existing.get("created_at") or "").strip() or now,
            "updated_at": now,
        }
        if self._write_control_plane_row_to_postgres("job_materialization_items", row_payload):
            return (
                self.get_job_materialization_item(normalized_item_id)
                or self._job_materialization_item_from_row(row_payload)
                or {}
            )
        self._raise_control_plane_postgres_write_failure(
            table_name="job_materialization_items",
            method_name="upsert_job_materialization_item",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_job_materialization_item(self, item_id: str) -> dict[str, Any]:
        normalized_item_id = str(item_id or "").strip()
        if not normalized_item_id:
            return {}
        postgres_row = self._select_control_plane_row(
            "job_materialization_items",
            row_builder=self._job_materialization_item_from_row,
            where_sql="item_id = %s",
            params=[normalized_item_id],
        )
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # fallback below is dead and removed.
        if postgres_row is None:
            return {}
        return postgres_row

    def list_job_materialization_items(
        self,
        *,
        job_id: str = "",
        item_kind: str = "",
        statuses: list[str] | tuple[str, ...] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # fallback below is dead and removed, along with the now-unused SQLite-`?`-placeholder clause/param
        # builders (only the postgres %s variants remain).
        postgres_clauses: list[str] = []
        postgres_params: list[Any] = []
        normalized_job_id = str(job_id or "").strip()
        if normalized_job_id:
            postgres_clauses.append("job_id = %s")
            postgres_params.append(normalized_job_id)
        normalized_kind = str(item_kind or "").strip()
        if normalized_kind:
            postgres_clauses.append("item_kind = %s")
            postgres_params.append(normalized_kind)
        normalized_statuses = [
            str(status or "").strip().lower() for status in list(statuses or []) if str(status or "").strip()
        ]
        if normalized_statuses:
            postgres_placeholders = ", ".join(["%s"] * len(normalized_statuses))
            postgres_clauses.append(f"status IN ({postgres_placeholders})")
            postgres_params.extend(normalized_statuses)
        postgres_rows = self._select_control_plane_rows(
            "job_materialization_items",
            row_builder=self._job_materialization_item_from_row,
            where_sql=" AND ".join(postgres_clauses),
            params=postgres_params,
            order_by_sql="priority DESC, updated_at ASC, created_at ASC",
            limit=max(0, int(limit or 0)),
        )
        if not postgres_rows:
            return []
        return postgres_rows

    def list_ready_job_materialization_items(
        self,
        *,
        job_id: str = "",
        item_kind: str = "board_visible_delta_apply",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        # `waiting_prerequisite` rows are also returned once their bounded
        # not_before_at falls due. Expired `running` rows are returned as well:
        # the lease is the durable ownership boundary, so a crashed daemon must
        # not strand local-apply/board-visible work in applying forever.
        statuses = ["queued", "deferred", "failed_retryable", "waiting_prerequisite", "running"]
        normalized_job_id = str(job_id or "").strip()
        normalized_kind = str(item_kind or "board_visible_delta_apply").strip() or "board_visible_delta_apply"
        now = _utc_now_timestamp()
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # fallback below is dead and removed, along with the now-unused SQLite clause/param builders
        # (only the postgres %s variants remain; the SQLite `datetime(...)` wrappers were dialect-only).
        postgres_clauses = [
            "item_kind = %s",
            "status IN (%s, %s, %s, %s, %s)",
            "(not_before_at = '' OR not_before_at <= %s)",
            "(lease_expires_at = '' OR lease_expires_at <= %s)",
        ]
        postgres_params: list[Any] = [normalized_kind, *statuses, now, now]
        if normalized_job_id:
            postgres_clauses.append("job_id = %s")
            postgres_params.append(normalized_job_id)
        postgres_rows = self._select_control_plane_rows(
            "job_materialization_items",
            row_builder=self._job_materialization_item_from_row,
            where_sql=" AND ".join(postgres_clauses),
            params=postgres_params,
            order_by_sql="priority DESC, updated_at ASC, created_at ASC",
            limit=max(1, int(limit or 100)),
        )
        if not postgres_rows:
            return []
        return postgres_rows

    def claim_job_materialization_item(
        self,
        item_id: str,
        *,
        lease_owner: str,
        lease_seconds: int = 300,
    ) -> dict[str, Any]:
        normalized_item_id = str(item_id or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        if not normalized_item_id or not normalized_owner:
            return {}
        if self._control_plane_postgres_should_prefer_read("job_materialization_items"):
            row = self._call_control_plane_postgres_native(
                "claim_job_materialization_item",
                normalized_item_id,
                lease_owner=normalized_owner,
                lease_seconds=max(1, int(lease_seconds or 300)),
            )
            if row is not None:
                return self._job_materialization_item_from_row(row) or {}
            if self._control_plane_postgres_should_skip_sqlite_fallback("job_materialization_items"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for job_materialization_items in claim_job_materialization_item: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def mark_job_materialization_item_completed(
        self,
        item_id: str,
        *,
        result_patch_id: str = "",
        result_view_id: str = "",
        serving_projection_id: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_item_id = str(item_id or "").strip()
        if not normalized_item_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("job_materialization_items"):
            row = self._call_control_plane_postgres_native(
                "mark_job_materialization_item_completed",
                normalized_item_id,
                result_patch_id=result_patch_id,
                result_view_id=result_view_id,
                serving_projection_id=serving_projection_id,
                metadata=metadata or {},
            )
            if row is not None:
                return self._job_materialization_item_from_row(row) or {}
            if self._control_plane_postgres_should_skip_sqlite_fallback("job_materialization_items"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for job_materialization_items in mark_job_materialization_item_completed: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def mark_job_materialization_item_failed(
        self,
        item_id: str,
        *,
        error_text: str,
        retryable: bool = True,
        retry_delay_seconds: int = 30,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_item_id = str(item_id or "").strip()
        if not normalized_item_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("job_materialization_items"):
            row = self._call_control_plane_postgres_native(
                "mark_job_materialization_item_failed",
                normalized_item_id,
                error_text=error_text,
                retryable=retryable,
                retry_delay_seconds=retry_delay_seconds,
                metadata=metadata or {},
            )
            if row is not None:
                return self._job_materialization_item_from_row(row) or {}
            if self._control_plane_postgres_should_skip_sqlite_fallback("job_materialization_items"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for job_materialization_items in mark_job_materialization_item_failed: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def mark_job_materialization_item_waiting_prerequisite(
        self,
        item_id: str,
        *,
        retry_delay_seconds: int = 8,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Mark an item as waiting for a missing prerequisite (e.g.,
        candidate_documents.json not yet written). Sets a bounded
        `not_before_at` as a fallback safety net; the primary recovery
        path is the prerequisite-writer event, which calls
        `reawaken_waiting_prerequisite_job_materialization_items` to
        clear `not_before_at` immediately.

        This MUST NOT increment attempt_count toward terminal failure
        budget. It rolls back the +1 added by the previous
        `claim_job_materialization_item` call so the soft-retry cycle
        does not exhaust attempts on a missing prerequisite. It also
        does NOT write `last_error` text — waiting is not a failure.
        """

        normalized_item_id = str(item_id or "").strip()
        if not normalized_item_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("job_materialization_items"):
            row = self._call_control_plane_postgres_native(
                "mark_job_materialization_item_waiting_prerequisite",
                normalized_item_id,
                retry_delay_seconds=retry_delay_seconds,
                metadata=metadata or {},
            )
            if row is not None:
                return self._job_materialization_item_from_row(row) or {}
            if self._control_plane_postgres_should_skip_sqlite_fallback("job_materialization_items"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for job_materialization_items in mark_job_materialization_item_waiting_prerequisite: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def mark_job_materialization_item_partial_progress(
        self,
        item_id: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Release a running durable item after successful partial progress.

        Partial local-apply chunks are not failures and must not burn retry
        attempts. The next service tick can immediately reclaim the same item
        and continue from metadata/worker-marker progress.
        """

        normalized_item_id = str(item_id or "").strip()
        if not normalized_item_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("job_materialization_items"):
            row = self._call_control_plane_postgres_native(
                "mark_job_materialization_item_partial_progress",
                normalized_item_id,
                metadata=metadata or {},
            )
            if row is not None:
                return self._job_materialization_item_from_row(row) or {}
            if self._control_plane_postgres_should_skip_sqlite_fallback("job_materialization_items"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for job_materialization_items in mark_job_materialization_item_partial_progress: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def reawaken_waiting_prerequisite_job_materialization_items(
        self,
        *,
        job_id: str,
        snapshot_id: str,
        item_kind: str = "local_apply_closure",
        source: str = "candidate_documents_prerequisite_ready",
    ) -> int:
        """Reawaken `waiting_prerequisite` items the moment the
        prerequisite writer event fires. Clears `not_before_at` so the
        next service-loop tick claims the item immediately, without
        waiting for the bounded fallback timer.

        Returns the number of rows reawakened.
        """

        normalized_job_id = str(job_id or "").strip()
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_kind = str(item_kind or "local_apply_closure").strip() or "local_apply_closure"
        normalized_source = str(source or "candidate_documents_prerequisite_ready").strip()
        if not normalized_job_id or not normalized_snapshot_id:
            return 0
        if self._control_plane_postgres_should_prefer_read("job_materialization_items"):
            count = self._call_control_plane_postgres_native(
                "reawaken_waiting_prerequisite_job_materialization_items",
                job_id=normalized_job_id,
                snapshot_id=normalized_snapshot_id,
                item_kind=normalized_kind,
                source=normalized_source,
            )
            if count is not None:
                try:
                    return max(0, int(count))
                except (TypeError, ValueError):
                    return 0
            if self._control_plane_postgres_should_skip_sqlite_fallback("job_materialization_items"):
                return 0
        raise RuntimeError(
            "postgres-only invariant violated for job_materialization_items in reawaken_waiting_prerequisite_job_materialization_items: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def reawaken_waiting_prerequisite_workflow_commands(
        self,
        *,
        workflow_run_id: str,
        snapshot_id: str,
        command_type: str,
        source: str = "candidate_documents_prerequisite_ready",
    ) -> int:
        """Clear the bounded fallback delay for typed prerequisite-wait commands."""

        normalized_run_id = str(workflow_run_id or "").strip()
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_command_type = str(command_type or "").strip()
        normalized_source = str(source or "candidate_documents_prerequisite_ready").strip()
        if not normalized_run_id or not normalized_snapshot_id or not normalized_command_type:
            return 0
        if self._control_plane_postgres_should_prefer_read("workflow_commands"):
            count = self._call_control_plane_postgres_native(
                "reawaken_waiting_prerequisite_workflow_commands",
                workflow_run_id=normalized_run_id,
                snapshot_id=normalized_snapshot_id,
                command_type=normalized_command_type,
                source=normalized_source,
            )
            if count is not None:
                try:
                    return max(0, int(count))
                except (TypeError, ValueError):
                    return 0
            if self._control_plane_postgres_should_skip_sqlite_fallback("workflow_commands"):
                return 0
        raise RuntimeError(
            "postgres-only invariant violated for workflow_commands in reawaken_waiting_prerequisite_workflow_commands: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def _job_materialization_item_from_row(
        self,
        row: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if row is None:
            return {}

        def getter(key: str, default: Any = None) -> Any:
            if isinstance(row, dict):
                return row.get(key, default)
            try:
                return row[key]
            except (IndexError, KeyError):
                return default

        def load_json_list(key: str) -> list[Any]:
            try:
                payload = json.loads(getter(key) or "[]")
            except (json.JSONDecodeError, TypeError):
                payload = []
            return list(payload) if isinstance(payload, list) else []

        try:
            metadata_payload = json.loads(getter("metadata_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            metadata_payload = {}
        return {
            "item_id": str(getter("item_id") or ""),
            "job_id": str(getter("job_id") or ""),
            "target_company": str(getter("target_company") or ""),
            "company_key": str(getter("company_key") or ""),
            "snapshot_id": str(getter("snapshot_id") or ""),
            "baseline_snapshot_id": str(getter("baseline_snapshot_id") or ""),
            "asset_view": str(getter("asset_view") or "canonical_merged"),
            "item_kind": str(getter("item_kind") or ""),
            "source": str(getter("source") or ""),
            "reason": str(getter("reason") or ""),
            "status": str(getter("status") or ""),
            "phase": str(getter("phase") or ""),
            "priority": int(getter("priority") or 0),
            "attempt_count": int(getter("attempt_count") or 0),
            "max_attempts": int(getter("max_attempts") or 0),
            "candidate_count": int(getter("candidate_count") or 0),
            "candidate_ids": _dedupe_ordered_texts(load_json_list("candidate_ids_json")),
            "source_worker_ids": _safe_int_list(load_json_list("source_worker_ids_json")),
            "idempotency_key": str(getter("idempotency_key") or ""),
            "result_patch_id": str(getter("result_patch_id") or ""),
            "result_view_id": str(getter("result_view_id") or ""),
            "serving_projection_id": str(getter("serving_projection_id") or ""),
            "lease_owner": str(getter("lease_owner") or ""),
            "lease_expires_at": str(getter("lease_expires_at") or ""),
            "not_before_at": str(getter("not_before_at") or ""),
            "last_error": str(getter("last_error") or ""),
            "metadata": metadata_payload if isinstance(metadata_payload, dict) else {},
            "completed_at": str(getter("completed_at") or ""),
            "created_at": getter("created_at"),
            "updated_at": getter("updated_at"),
        }

    JOB_RESULT_LIFECYCLE_INT_FIELDS: tuple[str, ...] = (
        "baseline_candidate_count",
        "expected_candidate_count",
        "served_candidate_count",
        "delta_profile_required_count",
        "delta_profile_fetched_count",
        "delta_profile_applied_count",
        "delta_profile_materialized_count",
        "delta_profile_board_visible_count",
        "stage1_current_search_returned_count",
        "stage1_former_search_returned_count",
        "stage1_all_search_returned_count",
        "stage1_deduped_candidate_count",
        "stage1_deduped_profile_url_count",
        "stage1_profile_fetch_required_count",
        "stage1_profile_fetched_count",
        "last_event_id",
    )
    JOB_RESULT_LIFECYCLE_DELTA_MONOTONIC_INT_FIELDS: tuple[str, ...] = (
        "delta_profile_required_count",
        "delta_profile_fetched_count",
        "delta_profile_applied_count",
        "delta_profile_materialized_count",
        "delta_profile_board_visible_count",
    )
    JOB_RESULT_LIFECYCLE_STAGE1_MONOTONIC_INT_FIELDS: tuple[str, ...] = (
        "stage1_current_search_returned_count",
        "stage1_former_search_returned_count",
        "stage1_all_search_returned_count",
        "stage1_deduped_candidate_count",
        "stage1_deduped_profile_url_count",
        "stage1_profile_fetch_required_count",
        "stage1_profile_fetched_count",
    )
    JOB_RESULT_LIFECYCLE_TEXT_FIELDS: tuple[str, ...] = (
        "view_id",
        "company_key",
        "target_company",
        "workflow_kind",
        "phase",
        "phase_status",
        "state",
        "baseline_snapshot_id",
        "current_snapshot_id",
        "served_snapshot_id",
        "served_generation_key",
        "serving_projection_id",
        "serving_projection_phase",
        "delta_profile_progress_reason",
        "background_snapshot_materialization_status",
        "outreach_layering_status",
        "source_validation_status",
        "projection_source_snapshot_id",
    )

    def get_job_result_lifecycle(self, job_id: str) -> dict[str, Any] | None:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return None
        if self._control_plane_postgres_should_prefer_read("job_result_lifecycle"):
            row = self._control_plane_postgres.select_one(
                "job_result_lifecycle",
                where_sql="job_id = %s",
                params=[normalized_job_id],
            )
            if row is not None:
                return self._job_result_lifecycle_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("job_result_lifecycle"):
                return None
        raise RuntimeError(
            "postgres-only invariant violated for job_result_lifecycle in get_job_result_lifecycle: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def upsert_job_result_lifecycle(
        self,
        *,
        job_id: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        """Patch-style upsert. Only provided fields are written; existing values are
        preserved. Callers must keep writes monotonic where the schema requires it
        (the canonical writer enforces phase progression and projection-source
        validation; this layer is intentionally low-level)."""

        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {}
        existing = self.get_job_result_lifecycle(normalized_job_id) or {}
        input_fields = dict(fields or {})
        merged: dict[str, Any] = {**existing, **input_fields}
        merged["job_id"] = normalized_job_id
        # delta_profile_progress_applicable is stored as INTEGER (0/1)
        applicable_value = merged.get("delta_profile_progress_applicable")
        if isinstance(applicable_value, bool):
            applicable_int = 1 if applicable_value else 0
        elif applicable_value is None:
            applicable_int = 1
        else:
            try:
                applicable_int = 1 if int(applicable_value) else 0
            except (TypeError, ValueError):
                applicable_int = 1
        if applicable_int:
            for field in self.JOB_RESULT_LIFECYCLE_DELTA_MONOTONIC_INT_FIELDS:
                if field not in input_fields:
                    continue
                try:
                    incoming_value = int(input_fields.get(field) or 0)
                except (TypeError, ValueError):
                    incoming_value = 0
                try:
                    existing_value = int(existing.get(field) or 0)
                except (TypeError, ValueError):
                    existing_value = 0
                merged[field] = max(existing_value, incoming_value)
        existing_metadata_payload = existing.get("metadata") or existing.get("metadata_json") or {}
        if isinstance(existing_metadata_payload, str):
            try:
                existing_metadata_payload = json.loads(existing_metadata_payload or "{}")
            except json.JSONDecodeError:
                existing_metadata_payload = {}
        if not isinstance(existing_metadata_payload, dict):
            existing_metadata_payload = {}
        incoming_metadata_payload = input_fields.get("metadata") or input_fields.get("metadata_json") or {}
        if isinstance(incoming_metadata_payload, str):
            try:
                incoming_metadata_payload = json.loads(incoming_metadata_payload or "{}")
            except json.JSONDecodeError:
                incoming_metadata_payload = {}
        if not isinstance(incoming_metadata_payload, dict):
            incoming_metadata_payload = {}

        for field in self.JOB_RESULT_LIFECYCLE_STAGE1_MONOTONIC_INT_FIELDS:
            if field not in input_fields:
                continue
            try:
                incoming_value = int(input_fields.get(field) or 0)
            except (TypeError, ValueError):
                incoming_value = 0
            try:
                existing_value = int(existing.get(field) or 0)
            except (TypeError, ValueError):
                existing_value = 0
            merged[field] = max(existing_value, incoming_value)

        protected_current_phases = {
            "current_snapshot_row_shell_overlay",
            "current_snapshot_serving",
            "current_serving",
        }
        incoming_partial_phases = {
            "partial_delta_overlay",
            "partial_delta_board_visible_overlay",
            "partial_current_snapshot_overlay",
        }
        existing_projection_phase = str(existing.get("serving_projection_phase") or "").strip()
        incoming_projection_phase = str(input_fields.get("serving_projection_phase") or "").strip()
        existing_current_snapshot_id = str(existing.get("current_snapshot_id") or "").strip()
        incoming_current_snapshot_id = str(
            input_fields.get("current_snapshot_id") or merged.get("current_snapshot_id") or ""
        ).strip()
        stale_partial_after_row_shell = bool(
            existing_projection_phase in protected_current_phases
            and incoming_projection_phase in incoming_partial_phases
            and existing_current_snapshot_id
            and incoming_current_snapshot_id == existing_current_snapshot_id
        )
        if stale_partial_after_row_shell:
            for field in (
                "phase",
                "state",
                "served_snapshot_id",
                "serving_projection_id",
                "serving_projection_phase",
                "background_snapshot_materialization_status",
            ):
                if existing.get(field) not in (None, ""):
                    merged[field] = existing.get(field)
            if applicable_int:
                for field in ("served_candidate_count", "expected_candidate_count"):
                    try:
                        merged[field] = max(int(existing.get(field) or 0), int(merged.get(field) or 0))
                    except (TypeError, ValueError):
                        merged[field] = existing.get(field) or merged.get(field) or 0
            else:
                try:
                    canonical_served = max(
                        int(existing.get("served_candidate_count") or 0), int(merged.get("served_candidate_count") or 0)
                    )
                except (TypeError, ValueError):
                    canonical_served = (
                        int(existing.get("served_candidate_count") or 0)
                        if existing.get("served_candidate_count")
                        else 0
                    )
                merged["served_candidate_count"] = canonical_served
                if canonical_served > 0:
                    # Non-delta projections serve a deduped canonical row set.
                    # Partial patch writes must not preserve a larger raw lane
                    # denominator after row-shell publication.
                    merged["expected_candidate_count"] = canonical_served
                else:
                    try:
                        merged["expected_candidate_count"] = max(
                            int(existing.get("expected_candidate_count") or 0),
                            int(merged.get("expected_candidate_count") or 0),
                        )
                    except (TypeError, ValueError):
                        merged["expected_candidate_count"] = (
                            existing.get("expected_candidate_count") or merged.get("expected_candidate_count") or 0
                        )
            if existing_metadata_payload:
                merged_metadata_payload = {
                    **incoming_metadata_payload,
                    **existing_metadata_payload,
                }
                merged["metadata"] = merged_metadata_payload
                incoming_metadata_payload = merged_metadata_payload

        if bool(existing_metadata_payload.get("delta_profile_denominator_promoted")) or bool(
            incoming_metadata_payload.get("delta_profile_denominator_promoted")
        ):
            promoted_metadata_payload = {
                **existing_metadata_payload,
                **incoming_metadata_payload,
                "delta_profile_denominator_promoted": True,
            }
            if not str(promoted_metadata_payload.get("stage1_terminal_promoted_at") or "").strip():
                promoted_at = str(existing_metadata_payload.get("stage1_terminal_promoted_at") or "").strip()
                if promoted_at:
                    promoted_metadata_payload["stage1_terminal_promoted_at"] = promoted_at
            merged["metadata"] = promoted_metadata_payload

        if not applicable_int:
            current_snapshot_id = str(merged.get("current_snapshot_id") or "").strip()
            served_snapshot_id = str(merged.get("served_snapshot_id") or "").strip()
            serving_projection_phase = str(merged.get("serving_projection_phase") or "").strip()
            try:
                canonical_served_count = int(merged.get("served_candidate_count") or 0)
            except (TypeError, ValueError):
                canonical_served_count = 0
            if (
                canonical_served_count > 0
                and current_snapshot_id
                and served_snapshot_id == current_snapshot_id
                and serving_projection_phase in protected_current_phases
            ):
                metadata_payload_for_clamp = merged.get("metadata") or merged.get("metadata_json") or {}
                if isinstance(metadata_payload_for_clamp, str):
                    try:
                        metadata_payload_for_clamp = json.loads(metadata_payload_for_clamp or "{}")
                    except json.JSONDecodeError:
                        metadata_payload_for_clamp = {}
                if not isinstance(metadata_payload_for_clamp, dict):
                    metadata_payload_for_clamp = {}
                stage1_public_candidate_count = max(
                    int(merged.get("stage1_profile_fetch_required_count") or 0),
                    int(merged.get("stage1_deduped_candidate_count") or 0),
                    int(merged.get("stage1_deduped_profile_url_count") or 0),
                )
                denominator_promoted = bool(metadata_payload_for_clamp.get("delta_profile_denominator_promoted"))
                if denominator_promoted and stage1_public_candidate_count > 0:
                    merged["expected_candidate_count"] = max(
                        canonical_served_count,
                        stage1_public_candidate_count,
                    )
                    metadata_payload_for_clamp.setdefault(
                        "non_delta_expected_source",
                        "stage1_public_candidate_count",
                    )
                else:
                    # Track B diagnosis fix: floor expected on a TRUSTED current-snapshot population
                    # signal, never on the raw incoming expected_candidate_count. The orchestrator stamps
                    # metadata["current_snapshot_population"] only when it has freshly counted the current
                    # snapshot (the already-served reuse path), so an honest population (e.g. 297) survives
                    # the clamp while a stale raw URL-lane denominator — which never carries this signal —
                    # is still suppressed down to the served count.
                    trusted_current_snapshot_population = int(
                        metadata_payload_for_clamp.get("current_snapshot_population") or 0
                    )
                    if trusted_current_snapshot_population > canonical_served_count:
                        merged["expected_candidate_count"] = trusted_current_snapshot_population
                        metadata_payload_for_clamp.setdefault(
                            "non_delta_expected_source",
                            "current_snapshot_population",
                        )
                    else:
                        merged["expected_candidate_count"] = canonical_served_count
                        metadata_payload_for_clamp.setdefault(
                            "non_delta_expected_source",
                            "canonical_served_candidate_count",
                        )
                metadata_payload_for_clamp.setdefault(
                    "non_delta_canonical_served_candidate_count",
                    canonical_served_count,
                )
                merged["metadata"] = metadata_payload_for_clamp

        existing_layering_status = str(existing.get("outreach_layering_status") or "").strip().lower()
        incoming_layering_status = str(input_fields.get("outreach_layering_status") or "").strip().lower()
        terminal_layering_statuses = {"completed", "failed"}
        nonterminal_layering_statuses = {"", "scheduled", "running", "deferred", "pending"}
        if (
            existing_layering_status in terminal_layering_statuses
            and incoming_layering_status in nonterminal_layering_statuses
        ):
            existing_layering_snapshot = str(
                existing.get("current_snapshot_id") or existing.get("projection_source_snapshot_id") or ""
            ).strip()
            incoming_layering_snapshot = str(
                input_fields.get("current_snapshot_id")
                or input_fields.get("projection_source_snapshot_id")
                or merged.get("current_snapshot_id")
                or merged.get("projection_source_snapshot_id")
                or ""
            ).strip()
            if not incoming_layering_snapshot or incoming_layering_snapshot == existing_layering_snapshot:
                merged["outreach_layering_status"] = existing_layering_status
                metadata_payload_for_layering = merged.get("metadata") or merged.get("metadata_json") or {}
                if isinstance(metadata_payload_for_layering, str):
                    try:
                        metadata_payload_for_layering = json.loads(metadata_payload_for_layering or "{}")
                    except json.JSONDecodeError:
                        metadata_payload_for_layering = {}
                if not isinstance(metadata_payload_for_layering, dict):
                    metadata_payload_for_layering = {}
                metadata_payload_for_layering["outreach_layering_status_preserved"] = {
                    "source": "job_result_lifecycle_writer",
                    "reason": "terminal_layering_status_is_monotonic",
                    "existing_status": existing_layering_status,
                    "incoming_status": incoming_layering_status,
                    "snapshot_id": existing_layering_snapshot or incoming_layering_snapshot,
                }
                merged["metadata"] = metadata_payload_for_layering

        metadata_payload = merged.get("metadata") or merged.get("metadata_json") or {}
        if isinstance(metadata_payload, str):
            try:
                metadata_payload = json.loads(metadata_payload or "{}")
            except json.JSONDecodeError:
                metadata_payload = {}
        metadata_json = json.dumps(_json_safe_payload(metadata_payload or {}), ensure_ascii=False)
        text_payload = {field: str(merged.get(field) or "").strip() for field in self.JOB_RESULT_LIFECYCLE_TEXT_FIELDS}
        if not text_payload.get("phase"):
            text_payload["phase"] = "planning"
        if not text_payload.get("source_validation_status"):
            text_payload["source_validation_status"] = "validated"
        int_payload: dict[str, int] = {}
        for field in self.JOB_RESULT_LIFECYCLE_INT_FIELDS:
            try:
                int_payload[field] = int(merged.get(field) or 0)
            except (TypeError, ValueError):
                int_payload[field] = 0
        now = _utc_now_timestamp()
        created_at = str(existing.get("created_at") or "").strip() or now
        row_payload: dict[str, Any] = {
            "job_id": normalized_job_id,
            "delta_profile_progress_applicable": applicable_int,
            "metadata_json": metadata_json,
            "created_at": created_at,
            "updated_at": now,
            **text_payload,
            **int_payload,
        }
        if self._write_control_plane_row_to_postgres("job_result_lifecycle", row_payload):
            return (
                self.get_job_result_lifecycle(normalized_job_id)
                or self._job_result_lifecycle_from_row(row_payload)
                or {}
            )
        self._raise_control_plane_postgres_write_failure(
            table_name="job_result_lifecycle",
            method_name="upsert_job_result_lifecycle",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def _job_result_lifecycle_from_row(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        if isinstance(row, dict):

            def getter(key: str, default: Any = None) -> Any:
                return row.get(key, default)
        else:

            def getter(key: str, default: Any = None) -> Any:
                try:
                    return row[key]
                except (IndexError, KeyError):
                    return default

        metadata_payload: dict[str, Any] = {}
        try:
            metadata_payload = json.loads(getter("metadata_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            metadata_payload = {}
        applicable_raw = getter("delta_profile_progress_applicable")
        try:
            applicable_bool = bool(int(applicable_raw if applicable_raw is not None else 1))
        except (TypeError, ValueError):
            applicable_bool = True
        result: dict[str, Any] = {
            "job_id": str(getter("job_id") or ""),
            "delta_profile_progress_applicable": applicable_bool,
            "metadata": metadata_payload,
            "created_at": getter("created_at"),
            "updated_at": getter("updated_at"),
        }
        for field in self.JOB_RESULT_LIFECYCLE_TEXT_FIELDS:
            result[field] = str(getter(field) or "")
        for field in self.JOB_RESULT_LIFECYCLE_INT_FIELDS:
            try:
                result[field] = int(getter(field) or 0)
            except (TypeError, ValueError):
                result[field] = 0
        return result

    def list_stale_workflow_jobs_in_acquiring(
        self,
        *,
        statuses: list[str] | None = None,
        stale_after_seconds: int = 60,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_statuses = [
            str(item or "").strip().lower()
            for item in list(statuses or ["running", "blocked"])
            if str(item or "").strip()
        ]
        if not normalized_statuses:
            normalized_statuses = ["running", "blocked"]
        placeholders = ",".join("?" for _ in normalized_statuses)
        clauses = [
            "job_type = 'workflow'",
            "stage = 'acquiring'",
            f"lower(status) IN ({placeholders})",
        ]
        params: list[Any] = list(normalized_statuses)
        normalized_stale_after = int(stale_after_seconds or 0)
        if normalized_stale_after > 0:
            clauses.append("datetime(updated_at) <= datetime('now', ?)")
            params.append(f"-{normalized_stale_after} seconds")
        f"SELECT * FROM jobs WHERE {' AND '.join(clauses)} ORDER BY updated_at ASC, created_at ASC LIMIT ?"
        params.append(max(1, int(limit or 100)))
        postgres_jobs = self._select_control_plane_job_rows(
            where_sql=" AND ".join(
                [
                    "job_type = %s",
                    "stage = %s",
                    f"lower(status) IN ({', '.join('%s' for _ in normalized_statuses)})",
                    *(["updated_at <= %s"] if normalized_stale_after > 0 else []),
                ]
            ),
            params=[
                "workflow",
                "acquiring",
                *normalized_statuses,
                *([datetime.now(timezone.utc).isoformat(timespec="seconds")] if normalized_stale_after > 0 else []),
            ],
            order_by_sql="updated_at ASC, created_at ASC",
            limit=max(1, int(limit or 100)),
        )
        if postgres_jobs:
            cutoff = None
            if normalized_stale_after > 0:
                cutoff = datetime.now(timezone.utc).timestamp() - normalized_stale_after
            filtered_jobs: list[dict[str, Any]] = []
            for job in postgres_jobs:
                if cutoff is not None:
                    updated_at = str(job.get("updated_at") or "")
                    try:
                        updated_epoch = datetime.fromisoformat(updated_at.replace("Z", "+00:00")).timestamp()
                    except ValueError:
                        updated_epoch = float("inf")
                    if updated_epoch > cutoff:
                        continue
                filtered_jobs.append(job)
            if filtered_jobs:
                return filtered_jobs[: max(1, int(limit or 100))]
        return []

    def list_stale_workflow_jobs_in_queue(
        self,
        *,
        stale_after_seconds: int = 60,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses = [
            "job_type = 'workflow'",
            "lower(status) = 'queued'",
            "stage = 'planning'",
        ]
        params: list[Any] = []
        normalized_stale_after = int(stale_after_seconds or 0)
        if normalized_stale_after > 0:
            clauses.append("datetime(updated_at) <= datetime('now', ?)")
            params.append(f"-{normalized_stale_after} seconds")
        f"SELECT * FROM jobs WHERE {' AND '.join(clauses)} ORDER BY updated_at ASC, created_at ASC LIMIT ?"
        params.append(max(1, int(limit or 100)))
        postgres_jobs = self._select_control_plane_job_rows(
            where_sql=" AND ".join(
                [
                    "job_type = %s",
                    "lower(status) = %s",
                    "stage = %s",
                    *(["updated_at <= %s"] if normalized_stale_after > 0 else []),
                ]
            ),
            params=[
                "workflow",
                "queued",
                "planning",
                *([datetime.now(timezone.utc).isoformat(timespec="seconds")] if normalized_stale_after > 0 else []),
            ],
            order_by_sql="updated_at ASC, created_at ASC",
            limit=max(1, int(limit or 100)),
        )
        if postgres_jobs:
            cutoff = None
            if normalized_stale_after > 0:
                cutoff = datetime.now(timezone.utc).timestamp() - normalized_stale_after
            filtered_jobs: list[dict[str, Any]] = []
            for job in postgres_jobs:
                if cutoff is not None:
                    updated_at = str(job.get("updated_at") or "")
                    try:
                        updated_epoch = datetime.fromisoformat(updated_at.replace("Z", "+00:00")).timestamp()
                    except ValueError:
                        updated_epoch = float("inf")
                    if updated_epoch > cutoff:
                        continue
                filtered_jobs.append(job)
            if filtered_jobs:
                return filtered_jobs[: max(1, int(limit or 100))]
        return []

    def _job_result_record_from_row(
        self,
        row: Any,
        *,
        include_evidence: bool,
    ) -> dict[str, Any]:
        metadata = json.loads(row["metadata_json"] or "{}")
        return {
            "candidate_id": row["candidate_id"],
            "display_name": row["display_name"],
            "name_en": row["name_en"],
            "name_zh": row["name_zh"],
            "category": row["category"],
            "organization": row["organization"],
            "role": row["role"],
            "team": row["team"],
            "employment_status": row["employment_status"],
            "focus_areas": row["focus_areas"],
            "education": row["education"],
            "work_history": row["work_history"],
            "notes": row["notes"],
            "linkedin_url": row["linkedin_url"],
            "media_url": row["media_url"],
            "source_dataset": row["source_dataset"],
            "source_path": row["source_path"],
            "metadata": metadata,
            "rank": row["rank_index"],
            "score": row["score"],
            "confidence_label": row["confidence_label"],
            "confidence_score": row["confidence_score"],
            "confidence_reason": row["confidence_reason"],
            "explanation": row["explanation"],
            "matched_fields": json.loads(row["matched_fields_json"] or "[]"),
            "outreach_layer": int(metadata.get("outreach_layer") or 0),
            "outreach_layer_key": str(metadata.get("outreach_layer_key") or ""),
            "outreach_layer_source": str(metadata.get("outreach_layer_source") or ""),
            "evidence": self.list_evidence(str(row["candidate_id"] or ""))[:3] if include_evidence else [],
        }

    def _job_result_records_from_rows(
        self,
        rows: list[Any],
        *,
        include_evidence: bool,
    ) -> list[dict[str, Any]]:
        return [self._job_result_record_from_row(row, include_evidence=include_evidence) for row in rows]

    def _job_result_records_from_postgres_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        include_evidence: bool,
    ) -> list[dict[str, Any]]:
        if not rows:
            return []
        candidate_ids = [
            str(row.get("candidate_id") or "").strip() for row in rows if str(row.get("candidate_id") or "").strip()
        ]
        candidate_rows = []
        if candidate_ids:
            placeholders = ", ".join("%s" for _ in candidate_ids)
            candidate_rows = self._select_postgres_candidate_rows(
                where_sql=f"candidate_id IN ({placeholders})",
                params=candidate_ids,
                limit=0,
            )
        candidate_lookup = {
            str(candidate_row.get("candidate_id") or "").strip(): candidate_row
            for candidate_row in candidate_rows
            if str(candidate_row.get("candidate_id") or "").strip()
        }
        joined_rows: list[dict[str, Any]] = []
        for row in rows:
            candidate_row = candidate_lookup.get(str(row.get("candidate_id") or "").strip())
            if candidate_row is None:
                continue
            joined_rows.append({**dict(row), **dict(candidate_row)})
        return self._job_result_records_from_rows(joined_rows, include_evidence=include_evidence)

    def get_job_results(self, job_id: str, *, include_evidence: bool = True) -> list[dict[str, Any]]:
        postgres_rows = self._select_postgres_job_result_rows(
            where_sql="job_id = %s",
            params=[job_id],
            limit=0,
        )
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # JOIN fallback below is dead and removed (should_skip_sqlite_fallback is always true).
        if not postgres_rows:
            return []
        return self._job_result_records_from_postgres_rows(postgres_rows, include_evidence=include_evidence)

    def get_job_results_page(
        self,
        job_id: str,
        *,
        offset: int = 0,
        limit: int = 100,
        include_evidence: bool = True,
    ) -> list[dict[str, Any]]:
        normalized_offset = max(int(offset or 0), 0)
        normalized_limit = max(int(limit or 0), 1)
        postgres_rows = self._select_postgres_job_result_rows(
            where_sql="job_id = %s",
            params=[job_id],
            limit=0,
        )
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # JOIN fallback below is dead and removed (should_skip_sqlite_fallback is always true).
        if not postgres_rows:
            return []
        selected_rows = postgres_rows[normalized_offset : normalized_offset + normalized_limit]
        return self._job_result_records_from_postgres_rows(selected_rows, include_evidence=include_evidence)

    def get_job_results_for_candidates(
        self,
        job_id: str,
        candidate_ids: list[str],
        *,
        include_evidence: bool = True,
    ) -> list[dict[str, Any]]:
        normalized_candidate_ids = [str(item or "").strip() for item in candidate_ids if str(item or "").strip()]
        if not normalized_candidate_ids:
            return []
        postgres_where = "job_id = %s"
        postgres_params: list[Any] = [job_id]
        if normalized_candidate_ids:
            placeholders = ", ".join("%s" for _ in normalized_candidate_ids)
            postgres_where = f"{postgres_where} AND candidate_id IN ({placeholders})"
            postgres_params.extend(normalized_candidate_ids)
        postgres_rows = self._select_postgres_job_result_rows(
            where_sql=postgres_where,
            params=postgres_params,
            limit=0,
        )
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # JOIN fallback below (and its now-unused SQLite-`?` placeholders builder) is dead and removed.
        if not postgres_rows:
            return []
        return self._job_result_records_from_postgres_rows(postgres_rows, include_evidence=include_evidence)

    def count_job_results(self, job_id: str) -> int:
        postgres_rows = self._select_postgres_job_result_rows(
            where_sql="job_id = %s",
            params=[job_id],
            limit=0,
        )
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # COUNT(*) fallback below is dead and removed (should_skip_sqlite_fallback is always true).
        return len(postgres_rows)

    def create_plan_review_session(
        self,
        *,
        target_company: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        gate_payload: dict[str, Any],
        execution_bundle_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        status = "pending" if bool(gate_payload.get("required_before_execution")) else "ready"
        execution_bundle_value = dict(execution_bundle_payload or {})
        matching_bundle = _matching_bundle_payload(
            request_payload,
            execution_bundle_payload=execution_bundle_value,
        )
        if self._control_plane_postgres_should_prefer_read("plan_review_sessions"):
            now = _utc_now_timestamp()
            row = self._call_control_plane_postgres_native(
                "insert_row_with_generated_id",
                table_name="plan_review_sessions",
                row={
                    "target_company": target_company,
                    "request_signature": request_signature(request_payload),
                    "request_family_signature": request_family_signature(request_payload),
                    "matching_request_signature": str(matching_bundle.get("matching_request_signature") or ""),
                    "matching_request_family_signature": str(
                        matching_bundle.get("matching_request_family_signature") or ""
                    ),
                    "status": status,
                    "risk_level": str(gate_payload.get("risk_level") or "medium"),
                    "required_before_execution": 1 if bool(gate_payload.get("required_before_execution")) else 0,
                    "request_json": json.dumps(request_payload, ensure_ascii=False),
                    "plan_json": json.dumps(plan_payload, ensure_ascii=False),
                    "gate_json": json.dumps(gate_payload, ensure_ascii=False),
                    "execution_bundle_json": json.dumps(_json_safe_payload(execution_bundle_value), ensure_ascii=False),
                    "matching_request_json": json.dumps(_json_safe_payload(matching_bundle), ensure_ascii=False),
                    "decision_json": json.dumps({}, ensure_ascii=False),
                    "reviewer": "",
                    "review_notes": "",
                    "approved_at": "",
                    "created_at": now,
                    "updated_at": now,
                },
            )
            if row is not None:
                return self._plan_review_session_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("plan_review_sessions"):
                # Track B B4.1b: PG is authoritative. insert_row_with_generated_id uses a plain
                # sequence-id INSERT...RETURNING (no ON CONFLICT) — it returns the inserted row on
                # success and only raises on error, so a None here is unreachable for valid input. Fail
                # closed rather than fall through to the dead SQLite shadow.
                self._raise_control_plane_postgres_write_failure(
                    table_name="plan_review_sessions",
                    method_name="insert_row_with_generated_id",
                    reason="native insert returned no row under postgres_only",
                )
        raise RuntimeError(
            "postgres-only invariant violated for plan_review_sessions in create_plan_review_session: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_plan_review_session(self, review_id: int) -> dict[str, Any] | None:
        if review_id <= 0:
            return None
        if self._control_plane_postgres_should_prefer_read("plan_review_sessions"):
            row = self._control_plane_postgres.select_one(
                "plan_review_sessions",
                where_sql="review_id = %s",
                params=[review_id],
            )
            if row is not None:
                return self._plan_review_session_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("plan_review_sessions"):
                return None
        raise RuntimeError(
            "postgres-only invariant violated for plan_review_sessions in get_plan_review_session: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_plan_review_sessions(
        self,
        *,
        target_company: str = "",
        status: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if self._control_plane_postgres_should_prefer_read("plan_review_sessions"):
            clauses: list[str] = []
            params: list[Any] = []
            if target_company:
                clauses.append("lower(target_company) = lower(%s)")
                params.append(target_company)
            if status:
                clauses.append("status = %s")
                params.append(status)
            rows = self._control_plane_postgres.select_many(
                "plan_review_sessions",
                where_sql=" AND ".join(clauses),
                params=params,
                order_by_sql="updated_at DESC, review_id DESC",
                limit=limit,
            )
            if rows:
                return [self._plan_review_session_from_row(row) for row in rows]
            if self._control_plane_postgres_should_skip_sqlite_fallback("plan_review_sessions"):
                return []
        raise RuntimeError(
            "postgres-only invariant violated for plan_review_sessions in list_plan_review_sessions: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def review_plan_session(
        self,
        *,
        review_id: int,
        status: str,
        reviewer: str = "",
        notes: str = "",
        decision_payload: dict[str, Any] | None = None,
        request_payload: dict[str, Any] | None = None,
        plan_payload: dict[str, Any] | None = None,
        execution_bundle_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        existing = self._select_control_plane_row(
            "plan_review_sessions",
            row_builder=self._plan_review_session_from_row,
            where_sql="review_id = %s",
            params=[review_id],
        )
        if existing is None:
            existing = self.get_plan_review_session(review_id)
        if existing is None:
            return None
        final_request_payload = request_payload if request_payload is not None else dict(existing.get("request") or {})
        final_plan_payload = plan_payload if plan_payload is not None else dict(existing.get("plan") or {})
        final_target_company = str(
            final_request_payload.get("target_company") or existing.get("target_company") or ""
        ).strip()
        execution_bundle_value = dict(execution_bundle_payload or {})
        execution_bundle_json = json.dumps(_json_safe_payload(execution_bundle_value), ensure_ascii=False)
        matching_bundle = _matching_bundle_payload(
            final_request_payload,
            execution_bundle_payload=execution_bundle_value,
        )
        if self._control_plane_postgres_should_prefer_read("plan_review_sessions"):
            now = _utc_now_timestamp()
            row = self._call_control_plane_postgres_native(
                "update_row_returning",
                table_name="plan_review_sessions",
                id_column="review_id",
                id_value=review_id,
                row={
                    "target_company": final_target_company,
                    "request_signature": request_signature(final_request_payload),
                    "request_family_signature": request_family_signature(final_request_payload),
                    "matching_request_signature": str(matching_bundle.get("matching_request_signature") or ""),
                    "matching_request_family_signature": str(
                        matching_bundle.get("matching_request_family_signature") or ""
                    ),
                    "status": status,
                    "reviewer": reviewer,
                    "review_notes": notes,
                    "decision_json": json.dumps(decision_payload or {}, ensure_ascii=False),
                    "request_json": json.dumps(final_request_payload, ensure_ascii=False),
                    "plan_json": json.dumps(final_plan_payload, ensure_ascii=False),
                    "execution_bundle_json": (
                        execution_bundle_json
                        if execution_bundle_json != "{}"
                        else json.dumps(existing.get("execution_bundle") or {}, ensure_ascii=False)
                    ),
                    "matching_request_json": json.dumps(_json_safe_payload(matching_bundle), ensure_ascii=False),
                    "approved_at": now if status == "approved" else str(existing.get("approved_at") or ""),
                    "updated_at": now,
                },
            )
            if row is not None:
                return self._plan_review_session_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("plan_review_sessions"):
                # Track B B4.1b: PG is authoritative. update_row_returning returns None only when the
                # UPDATE matched no row — but `existing` was loaded before this call, so a None here means
                # the session vanished mid-method (a race). Fail closed rather than fall through to the
                # dead SQLite shadow.
                self._raise_control_plane_postgres_write_failure(
                    table_name="plan_review_sessions",
                    method_name="update_row_returning",
                    reason=f"native update returned no row for review_id={review_id} after the session was read",
                )
        raise RuntimeError(
            "postgres-only invariant violated for plan_review_sessions in review_plan_session: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_candidate_review_records(
        self,
        *,
        job_id: str = "",
        history_id: str = "",
        candidate_id: str = "",
        status: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if job_id:
            clauses.append("job_id = ?")
            params.append(job_id)
        if history_id:
            clauses.append("history_id = ?")
            params.append(history_id)
        if candidate_id:
            clauses.append("candidate_id = ?")
            params.append(candidate_id)
        if status:
            clauses.append("status = ?")
            params.append(_normalize_candidate_review_status(status))
        postgres_rows = self._select_control_plane_rows(
            "candidate_review_registry",
            row_builder=self._candidate_review_record_from_row,
            where_sql=" AND ".join(
                [
                    *(["job_id = %s"] if job_id else []),
                    *(["history_id = %s"] if history_id else []),
                    *(["candidate_id = %s"] if candidate_id else []),
                    *(["status = %s"] if status else []),
                ]
            ),
            params=[
                *([job_id] if job_id else []),
                *([history_id] if history_id else []),
                *([candidate_id] if candidate_id else []),
                *([_normalize_candidate_review_status(status)] if status else []),
            ],
            order_by_sql="updated_at DESC, added_at DESC, record_id DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_candidate_review_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = _normalize_candidate_review_record_payload(payload)
        existing = self.get_candidate_review_record(normalized["record_id"])
        now = _utc_now_timestamp()
        row_payload = {
            "record_id": normalized["record_id"],
            "job_id": normalized["job_id"],
            "history_id": normalized["history_id"],
            "candidate_id": normalized["candidate_id"],
            "candidate_name": normalized["candidate_name"],
            "headline": normalized["headline"],
            "current_company": normalized["current_company"],
            "avatar_url": normalized["avatar_url"],
            "linkedin_url": normalized["linkedin_url"],
            "primary_email": normalized["primary_email"],
            "status": normalized["status"],
            "comment": normalized["comment"],
            "source": normalized["source"],
            "metadata_json": json.dumps(normalized["metadata"], ensure_ascii=False),
            "added_at": str((existing or {}).get("added_at") or normalized.get("added_at") or "").strip() or now,
            "updated_at": now,
        }
        if self._write_control_plane_row_to_postgres("candidate_review_registry", row_payload):
            return (
                self.get_candidate_review_record(normalized["record_id"])
                or self._candidate_review_record_from_row(row_payload)
                or normalized
            )
        self._raise_control_plane_postgres_write_failure(
            table_name="candidate_review_registry",
            method_name="upsert_candidate_review_record",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_candidate_review_record(self, record_id: str) -> dict[str, Any] | None:
        normalized_record_id = str(record_id or "").strip()
        if not normalized_record_id:
            return None
        postgres_row = self._select_control_plane_row(
            "candidate_review_registry",
            row_builder=self._candidate_review_record_from_row,
            where_sql="record_id = %s",
            params=[normalized_record_id],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_target_candidates(
        self,
        *,
        job_id: str = "",
        history_id: str = "",
        candidate_id: str = "",
        source_projection_id: str = "",
        follow_up_status: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if job_id:
            clauses.append("job_id = ?")
            params.append(job_id)
        if history_id:
            clauses.append("history_id = ?")
            params.append(history_id)
        if candidate_id:
            clauses.append("candidate_id = ?")
            params.append(candidate_id)
        if source_projection_id:
            clauses.append("source_projection_id = ?")
            params.append(source_projection_id)
        if follow_up_status:
            clauses.append("follow_up_status = ?")
            params.append(_normalize_target_candidate_follow_up_status(follow_up_status))
        postgres_rows = self._select_control_plane_rows(
            "target_candidates",
            row_builder=self._target_candidate_from_row,
            where_sql=" AND ".join(
                [
                    *(["job_id = %s"] if job_id else []),
                    *(["history_id = %s"] if history_id else []),
                    *(["candidate_id = %s"] if candidate_id else []),
                    *(["source_projection_id = %s"] if source_projection_id else []),
                    *(["follow_up_status = %s"] if follow_up_status else []),
                ]
            ),
            params=[
                *([job_id] if job_id else []),
                *([history_id] if history_id else []),
                *([candidate_id] if candidate_id else []),
                *([source_projection_id] if source_projection_id else []),
                *([_normalize_target_candidate_follow_up_status(follow_up_status)] if follow_up_status else []),
            ],
            order_by_sql="updated_at DESC, added_at DESC, record_id DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_target_candidate(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = _normalize_target_candidate_payload(payload)
        existing = self.get_target_candidate(normalized["record_id"])
        now = _utc_now_timestamp()
        row_payload = {
            "record_id": normalized["record_id"],
            "candidate_id": normalized["candidate_id"],
            "history_id": normalized["history_id"],
            "job_id": normalized["job_id"],
            "candidate_name": normalized["candidate_name"],
            "headline": normalized["headline"],
            "current_company": normalized["current_company"],
            "avatar_url": normalized["avatar_url"],
            "linkedin_url": normalized["linkedin_url"],
            "primary_email": normalized["primary_email"],
            "person_identity_key": normalized["person_identity_key"],
            "candidate_identity_key": normalized["candidate_identity_key"],
            "source_projection_id": normalized["source_projection_id"],
            "source_run_id": normalized["source_run_id"],
            "source_collection_id": normalized["source_collection_id"],
            "source_reason": normalized["source_reason"],
            "follow_up_status": normalized["follow_up_status"],
            "quality_score": normalized["quality_score"],
            "comment": normalized["comment"],
            "metadata_json": json.dumps(normalized["metadata"], ensure_ascii=False),
            "added_at": str((existing or {}).get("added_at") or normalized.get("added_at") or "").strip() or now,
            "updated_at": now,
        }
        if self._write_control_plane_row_to_postgres("target_candidates", row_payload):
            return (
                self.get_target_candidate(normalized["record_id"])
                or self._target_candidate_from_row(row_payload)
                or normalized
            )
        self._raise_control_plane_postgres_write_failure(
            table_name="target_candidates",
            method_name="upsert_target_candidate",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_target_candidate(self, record_id: str) -> dict[str, Any] | None:
        normalized_record_id = str(record_id or "").strip()
        if not normalized_record_id:
            return None
        postgres_row = self._select_control_plane_row(
            "target_candidates",
            row_builder=self._target_candidate_from_row,
            where_sql="record_id = %s",
            params=[normalized_record_id],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def _upsert_simple_control_plane_row(
        self,
        table_name: str,
        *,
        id_column: str,
        row_payload: dict[str, Any],
        row_builder: Any,
    ) -> dict[str, Any]:
        row_id = str(row_payload.get(id_column) or "").strip()
        if not row_id:
            return {}
        if self._write_control_plane_row_to_postgres(table_name, row_payload):
            postgres_row = self._select_control_plane_row(
                table_name,
                row_builder=row_builder,
                where_sql=f"{id_column} = %s",
                params=[row_id],
            )
            return postgres_row or row_builder(row_payload)
        # postgres-only (B3.0): _write_control_plane_row_to_postgres returns True or raises; reaching here
        # means the authoritative write returned no confirmation, which is forbidden. The legacy SQLite
        # upsert + mirror tail is retired (B4).
        self._raise_control_plane_postgres_write_failure(
            table_name=table_name,
            method_name="_upsert_simple_control_plane_row",
            reason="postgres-only: authoritative upsert returned no confirmation; SQLite fallback retired (B4)",
        )

    def upsert_person_asset(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        asset_id = str(normalized.get("asset_id") or normalized.get("id") or f"pa_{uuid4().hex}").strip()
        profile_url_key = _resolve_profile_url_key(
            normalized.get("profile_url_key"),
            normalized.get("linkedin_url"),
            normalized.get("source_url"),
        )
        person_identity_key = _resolve_person_identity_key(
            person_identity_key=str(normalized.get("person_identity_key") or ""),
            profile_url_key=profile_url_key,
            linkedin_url=str(normalized.get("linkedin_url") or ""),
            candidate_id=str(normalized.get("candidate_id") or ""),
        )
        now = _utc_now_timestamp()
        existing = self.get_person_asset(asset_id)
        row_payload = _person_company_assets_repo.PERSON_ASSETS.to_columns(
            {
                **normalized,
                "asset_id": asset_id,
                "person_identity_key": person_identity_key,
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        return self._upsert_simple_control_plane_row(
            "person_assets",
            id_column="asset_id",
            row_payload=row_payload,
            row_builder=self._person_asset_from_row,
        )

    def get_person_asset(self, asset_id: str) -> dict[str, Any]:
        normalized_asset_id = str(asset_id or "").strip()
        if not normalized_asset_id:
            return {}
        postgres_row = self._select_control_plane_row(
            "person_assets",
            row_builder=self._person_asset_from_row,
            where_sql="asset_id = %s",
            params=[normalized_asset_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_person_assets(
        self,
        *,
        person_identity_key: str = "",
        asset_type: str = "",
        source_projection_id: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if person_identity_key:
            clauses.append("person_identity_key = ?")
            params.append(str(person_identity_key).strip())
        if asset_type:
            clauses.append("asset_type = ?")
            params.append(str(asset_type).strip())
        if source_projection_id:
            clauses.append("source_projection_id = ?")
            params.append(str(source_projection_id).strip())
        where_sqlite = " AND ".join(clauses)
        postgres_rows = self._select_control_plane_rows(
            "person_assets",
            row_builder=self._person_asset_from_row,
            where_sql=where_sqlite.replace("?", "%s"),
            params=params,
            order_by_sql="updated_at DESC, asset_id DESC",
            limit=max(1, int(limit or 100)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def list_person_assets_for_person_keys(
        self,
        person_identity_keys: list[str] | tuple[str, ...],
        *,
        asset_type: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        keys = [str(item or "").strip() for item in person_identity_keys if str(item or "").strip()]
        if not keys:
            return []
        deduped_keys = list(dict.fromkeys(keys))
        placeholders = ", ".join(["?"] * len(deduped_keys))
        clauses = [f"person_identity_key IN ({placeholders})"]
        params: list[Any] = list(deduped_keys)
        if asset_type:
            clauses.append("asset_type = ?")
            params.append(str(asset_type).strip())
        where_sqlite = " AND ".join(clauses)
        normalized_limit = max(1, int(limit or 1000))
        postgres_rows = self._select_control_plane_rows(
            "person_assets",
            row_builder=self._person_asset_from_row,
            where_sql=where_sqlite.replace("?", "%s"),
            params=params,
            order_by_sql="person_identity_key ASC, updated_at DESC, asset_id DESC",
            limit=normalized_limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_person_evidence(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        evidence_id = str(normalized.get("evidence_id") or normalized.get("id") or f"pe_{uuid4().hex}").strip()
        now = _utc_now_timestamp()
        existing = self.get_person_evidence(evidence_id)
        row_payload = _person_company_assets_repo.PERSON_EVIDENCE.to_columns(
            {
                **normalized,
                "evidence_id": evidence_id,
                "normalized_value": normalized.get("normalized_value") or normalized.get("value"),
                "artifact_refs": _normalize_json_object_payload(
                    normalized.get("artifact_refs") or normalized.get("artifact_refs_json")
                ),
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        return self._upsert_simple_control_plane_row(
            "person_evidence",
            id_column="evidence_id",
            row_payload=row_payload,
            row_builder=self._person_evidence_from_row,
        )

    def get_person_evidence(self, evidence_id: str) -> dict[str, Any]:
        normalized_evidence_id = str(evidence_id or "").strip()
        if not normalized_evidence_id:
            return {}
        postgres_row = self._select_control_plane_row(
            "person_evidence",
            row_builder=self._person_evidence_from_row,
            where_sql="evidence_id = %s",
            params=[normalized_evidence_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_person_evidence(
        self,
        *,
        person_identity_key: str = "",
        asset_id: str = "",
        evidence_type: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if person_identity_key:
            clauses.append("person_identity_key = ?")
            params.append(str(person_identity_key).strip())
        if asset_id:
            clauses.append("asset_id = ?")
            params.append(str(asset_id).strip())
        if evidence_type:
            clauses.append("evidence_type = ?")
            params.append(str(evidence_type).strip())
        where_sqlite = " AND ".join(clauses)
        postgres_rows = self._select_control_plane_rows(
            "person_evidence",
            row_builder=self._person_evidence_from_row,
            where_sql=where_sqlite.replace("?", "%s"),
            params=params,
            order_by_sql="updated_at DESC, evidence_id DESC",
            limit=max(1, int(limit or 100)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_person_assertion(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        assertion_id = str(normalized.get("assertion_id") or normalized.get("id") or f"pass_{uuid4().hex}").strip()
        now = _utc_now_timestamp()
        existing = self.get_person_assertion(assertion_id)
        row_payload = _person_company_assets_repo.PERSON_ASSERTIONS.to_columns(
            {
                **normalized,
                "assertion_id": assertion_id,
                "normalized_value": normalized.get("normalized_value") or normalized.get("value"),
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        return self._upsert_simple_control_plane_row(
            "person_assertions",
            id_column="assertion_id",
            row_payload=row_payload,
            row_builder=self._person_assertion_from_row,
        )

    def get_person_assertion(self, assertion_id: str) -> dict[str, Any]:
        normalized_assertion_id = str(assertion_id or "").strip()
        if not normalized_assertion_id:
            return {}
        postgres_row = self._select_control_plane_row(
            "person_assertions",
            row_builder=self._person_assertion_from_row,
            where_sql="assertion_id = %s",
            params=[normalized_assertion_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_person_assertions(
        self,
        *,
        person_identity_key: str = "",
        assertion_type: str = "",
        verification_status: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if person_identity_key:
            clauses.append("person_identity_key = ?")
            params.append(str(person_identity_key).strip())
        if assertion_type:
            clauses.append("assertion_type = ?")
            params.append(str(assertion_type).strip())
        if verification_status:
            clauses.append("verification_status = ?")
            params.append(str(verification_status).strip())
        where_sqlite = " AND ".join(clauses)
        postgres_rows = self._select_control_plane_rows(
            "person_assertions",
            row_builder=self._person_assertion_from_row,
            where_sql=where_sqlite.replace("?", "%s"),
            params=params,
            order_by_sql="updated_at DESC, assertion_id DESC",
            limit=max(1, int(limit or 100)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_company_asset(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("company_assets")
        normalized = dict(payload or {})
        asset_id = str(normalized.get("asset_id") or normalized.get("id") or f"ca_{uuid4().hex}").strip()
        target_company = str(normalized.get("target_company") or normalized.get("company") or "").strip()
        company_key = str(normalized.get("company_key") or "").strip() or resolve_company_alias_key(target_company)
        now = _utc_now_timestamp()
        existing = self.get_company_asset(asset_id)
        row_payload = _person_company_assets_repo.COMPANY_ASSETS.to_columns(
            {
                **normalized,
                "asset_id": asset_id,
                "workspace_id": str(normalized.get("workspace_id") or "default").strip() or "default",
                "company_key": company_key,
                "target_company": target_company,
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        return self._upsert_simple_control_plane_row(
            "company_assets",
            id_column="asset_id",
            row_payload=row_payload,
            row_builder=self._company_asset_from_row,
        )

    def get_company_asset(self, asset_id: str) -> dict[str, Any]:
        normalized_asset_id = str(asset_id or "").strip()
        if not normalized_asset_id or not self._control_plane_postgres_should_prefer_read("company_assets"):
            return {}
        postgres_row = self._select_control_plane_row(
            "company_assets",
            row_builder=self._company_asset_from_row,
            where_sql="asset_id = %s",
            params=[normalized_asset_id],
        )
        return postgres_row or {}

    def list_company_assets(
        self,
        *,
        workspace_id: str = "default",
        company_key: str = "",
        target_company: str = "",
        asset_type: str = "",
        status: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if not self._control_plane_postgres_should_prefer_read("company_assets"):
            return []
        normalized_company_key = str(company_key or "").strip() or resolve_company_alias_key(target_company)
        clauses: list[str] = []
        params: list[Any] = []
        if workspace_id:
            clauses.append("workspace_id = %s")
            params.append(str(workspace_id).strip())
        if normalized_company_key:
            clauses.append("company_key = %s")
            params.append(normalized_company_key)
        if asset_type:
            clauses.append("asset_type = %s")
            params.append(str(asset_type).strip())
        if status:
            clauses.append("status = %s")
            params.append(str(status).strip())
        return self._select_control_plane_rows(
            "company_assets",
            row_builder=self._company_asset_from_row,
            where_sql=" AND ".join(clauses),
            params=params,
            order_by_sql="updated_at DESC, asset_id DESC",
            limit=max(1, int(limit or 100)),
        )

    def upsert_company_evidence(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("company_evidence")
        normalized = dict(payload or {})
        evidence_id = str(normalized.get("evidence_id") or normalized.get("id") or f"ce_{uuid4().hex}").strip()
        target_company = str(normalized.get("target_company") or normalized.get("company") or "").strip()
        company_key = str(normalized.get("company_key") or "").strip() or resolve_company_alias_key(target_company)
        now = _utc_now_timestamp()
        existing = self.get_company_evidence(evidence_id)
        row_payload = _person_company_assets_repo.COMPANY_EVIDENCE.to_columns(
            {
                **normalized,
                "evidence_id": evidence_id,
                "workspace_id": str(normalized.get("workspace_id") or "default").strip() or "default",
                "company_key": company_key,
                "target_company": target_company,
                "normalized_value": normalized.get("normalized_value") or normalized.get("value"),
                "artifact_refs": _normalize_json_object_payload(
                    normalized.get("artifact_refs") or normalized.get("artifact_refs_json")
                ),
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        return self._upsert_simple_control_plane_row(
            "company_evidence",
            id_column="evidence_id",
            row_payload=row_payload,
            row_builder=self._company_evidence_from_row,
        )

    def get_company_evidence(self, evidence_id: str) -> dict[str, Any]:
        normalized_evidence_id = str(evidence_id or "").strip()
        if not normalized_evidence_id or not self._control_plane_postgres_should_prefer_read("company_evidence"):
            return {}
        postgres_row = self._select_control_plane_row(
            "company_evidence",
            row_builder=self._company_evidence_from_row,
            where_sql="evidence_id = %s",
            params=[normalized_evidence_id],
        )
        return postgres_row or {}

    def list_company_evidence(
        self,
        *,
        workspace_id: str = "default",
        company_key: str = "",
        target_company: str = "",
        asset_id: str = "",
        evidence_type: str = "",
        status: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if not self._control_plane_postgres_should_prefer_read("company_evidence"):
            return []
        normalized_company_key = str(company_key or "").strip() or resolve_company_alias_key(target_company)
        clauses: list[str] = []
        params: list[Any] = []
        if workspace_id:
            clauses.append("workspace_id = %s")
            params.append(str(workspace_id).strip())
        if normalized_company_key:
            clauses.append("company_key = %s")
            params.append(normalized_company_key)
        if asset_id:
            clauses.append("asset_id = %s")
            params.append(str(asset_id).strip())
        if evidence_type:
            clauses.append("evidence_type = %s")
            params.append(str(evidence_type).strip())
        if status:
            clauses.append("status = %s")
            params.append(str(status).strip())
        return self._select_control_plane_rows(
            "company_evidence",
            row_builder=self._company_evidence_from_row,
            where_sql=" AND ".join(clauses),
            params=params,
            order_by_sql="updated_at DESC, evidence_id DESC",
            limit=max(1, int(limit or 100)),
        )

    def upsert_company_assertion(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("company_assertions")
        normalized = dict(payload or {})
        assertion_id = str(normalized.get("assertion_id") or normalized.get("id") or f"cass_{uuid4().hex}").strip()
        target_company = str(normalized.get("target_company") or normalized.get("company") or "").strip()
        company_key = str(normalized.get("company_key") or "").strip() or resolve_company_alias_key(target_company)
        now = _utc_now_timestamp()
        existing = self.get_company_assertion(assertion_id)
        row_payload = _person_company_assets_repo.COMPANY_ASSERTIONS.to_columns(
            {
                **normalized,
                "assertion_id": assertion_id,
                "workspace_id": str(normalized.get("workspace_id") or "default").strip() or "default",
                "company_key": company_key,
                "target_company": target_company,
                "normalized_value": normalized.get("normalized_value") or normalized.get("value"),
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        return self._upsert_simple_control_plane_row(
            "company_assertions",
            id_column="assertion_id",
            row_payload=row_payload,
            row_builder=self._company_assertion_from_row,
        )

    def get_company_assertion(self, assertion_id: str) -> dict[str, Any]:
        normalized_assertion_id = str(assertion_id or "").strip()
        if not normalized_assertion_id or not self._control_plane_postgres_should_prefer_read("company_assertions"):
            return {}
        postgres_row = self._select_control_plane_row(
            "company_assertions",
            row_builder=self._company_assertion_from_row,
            where_sql="assertion_id = %s",
            params=[normalized_assertion_id],
        )
        return postgres_row or {}

    def list_company_assertions(
        self,
        *,
        workspace_id: str = "default",
        company_key: str = "",
        target_company: str = "",
        assertion_type: str = "",
        verification_status: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if not self._control_plane_postgres_should_prefer_read("company_assertions"):
            return []
        normalized_company_key = str(company_key or "").strip() or resolve_company_alias_key(target_company)
        clauses: list[str] = []
        params: list[Any] = []
        if workspace_id:
            clauses.append("workspace_id = %s")
            params.append(str(workspace_id).strip())
        if normalized_company_key:
            clauses.append("company_key = %s")
            params.append(normalized_company_key)
        if assertion_type:
            clauses.append("assertion_type = %s")
            params.append(str(assertion_type).strip())
        if verification_status:
            clauses.append("verification_status = %s")
            params.append(str(verification_status).strip())
        return self._select_control_plane_rows(
            "company_assertions",
            row_builder=self._company_assertion_from_row,
            where_sql=" AND ".join(clauses),
            params=params,
            order_by_sql="updated_at DESC, assertion_id DESC",
            limit=max(1, int(limit or 100)),
        )

    def upsert_raw_profile_index(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        person_identity_key = str(normalized.get("person_identity_key") or "").strip()
        if not person_identity_key:
            return {}
        now = _utc_now_timestamp()
        existing = self.get_raw_profile_index(person_identity_key)
        raw_terms = _normalize_search_index_terms(normalized.get("raw_profile_terms"))
        indexed_text = _normalize_search_index_text(" ".join([str(normalized.get("indexed_text") or ""), *raw_terms]))
        row_payload = {
            "person_identity_key": person_identity_key,
            "indexed_text": indexed_text,
            "raw_profile_terms_json": json.dumps(raw_terms, ensure_ascii=False),
            "source_asset_ids_json": json.dumps(
                _loads_json_list(normalized.get("source_asset_ids_json"), default=[])
                if "source_asset_ids_json" in normalized
                else [
                    str(item or "").strip()
                    for item in list(normalized.get("source_asset_ids") or [])
                    if str(item or "").strip()
                ],
                ensure_ascii=False,
            ),
            "indexed_field_sources_json": json.dumps(
                _normalize_json_object_payload(
                    normalized.get("indexed_field_sources") or normalized.get("indexed_field_sources_json")
                ),
                ensure_ascii=False,
            ),
            "raw_profile_index_watermark": str(normalized.get("raw_profile_index_watermark") or "").strip(),
            "profile_fetched_at": str(normalized.get("profile_fetched_at") or "").strip(),
            "profile_indexed_at": str(normalized.get("profile_indexed_at") or "").strip(),
            "metadata_json": json.dumps(
                _normalize_json_object_payload(normalized.get("metadata") or normalized.get("metadata_json")),
                ensure_ascii=False,
            ),
            "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
            "updated_at": now,
        }
        return self._upsert_simple_control_plane_row(
            "raw_profile_index",
            id_column="person_identity_key",
            row_payload=row_payload,
            row_builder=self._raw_profile_index_from_row,
        )

    def get_raw_profile_index(self, person_identity_key: str) -> dict[str, Any]:
        normalized_person_key = str(person_identity_key or "").strip()
        if not normalized_person_key:
            return {}
        postgres_row = self._select_control_plane_row(
            "raw_profile_index",
            row_builder=self._raw_profile_index_from_row,
            where_sql="person_identity_key = %s",
            params=[normalized_person_key],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_raw_profile_indexes(
        self,
        person_identity_keys: list[str] | tuple[str, ...],
    ) -> dict[str, dict[str, Any]]:
        normalized_keys = _dedupe_ordered_texts(person_identity_keys)
        if not normalized_keys:
            return {}
        placeholders = ", ".join(["%s"] * len(normalized_keys))
        postgres_rows = self._select_control_plane_rows(
            "raw_profile_index",
            row_builder=self._raw_profile_index_from_row,
            where_sql=f"person_identity_key IN ({placeholders})",
            params=normalized_keys,
            order_by_sql="person_identity_key ASC",
            limit=len(normalized_keys),
        )
        if postgres_rows:
            return {
                str(row.get("person_identity_key") or "").strip(): row
                for row in postgres_rows
                if str(row.get("person_identity_key") or "").strip()
            }
        return {}

    def upsert_candidate_evidence_index(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        person_identity_key = str(normalized.get("person_identity_key") or "").strip()
        if not person_identity_key:
            return {}
        now = _utc_now_timestamp()
        existing = self.get_candidate_evidence_index(person_identity_key)
        evidence_terms = _normalize_search_index_terms(normalized.get("evidence_terms"))
        assertion_terms = _normalize_search_index_terms(normalized.get("assertion_terms"))
        indexed_text = _normalize_search_index_text(
            " ".join([str(normalized.get("indexed_text") or ""), *evidence_terms, *assertion_terms])
        )
        row_payload = {
            "person_identity_key": person_identity_key,
            "indexed_text": indexed_text,
            "evidence_terms_json": json.dumps(evidence_terms, ensure_ascii=False),
            "assertion_terms_json": json.dumps(assertion_terms, ensure_ascii=False),
            "source_evidence_ids_json": json.dumps(
                _loads_json_list(normalized.get("source_evidence_ids_json"), default=[])
                if "source_evidence_ids_json" in normalized
                else [
                    str(item or "").strip()
                    for item in list(normalized.get("source_evidence_ids") or [])
                    if str(item or "").strip()
                ],
                ensure_ascii=False,
            ),
            "source_assertion_ids_json": json.dumps(
                _loads_json_list(normalized.get("source_assertion_ids_json"), default=[])
                if "source_assertion_ids_json" in normalized
                else [
                    str(item or "").strip()
                    for item in list(normalized.get("source_assertion_ids") or [])
                    if str(item or "").strip()
                ],
                ensure_ascii=False,
            ),
            "indexed_field_sources_json": json.dumps(
                _normalize_json_object_payload(
                    normalized.get("indexed_field_sources") or normalized.get("indexed_field_sources_json")
                ),
                ensure_ascii=False,
            ),
            "evidence_index_watermark": str(normalized.get("evidence_index_watermark") or "").strip(),
            "evidence_indexed_at": str(normalized.get("evidence_indexed_at") or "").strip(),
            "metadata_json": json.dumps(
                _normalize_json_object_payload(normalized.get("metadata") or normalized.get("metadata_json")),
                ensure_ascii=False,
            ),
            "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
            "updated_at": now,
        }
        return self._upsert_simple_control_plane_row(
            "candidate_evidence_index",
            id_column="person_identity_key",
            row_payload=row_payload,
            row_builder=self._candidate_evidence_index_from_row,
        )

    def get_candidate_evidence_index(self, person_identity_key: str) -> dict[str, Any]:
        normalized_person_key = str(person_identity_key or "").strip()
        if not normalized_person_key:
            return {}
        postgres_row = self._select_control_plane_row(
            "candidate_evidence_index",
            row_builder=self._candidate_evidence_index_from_row,
            where_sql="person_identity_key = %s",
            params=[normalized_person_key],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_candidate_evidence_indexes(
        self,
        person_identity_keys: list[str] | tuple[str, ...],
    ) -> dict[str, dict[str, Any]]:
        normalized_keys = _dedupe_ordered_texts(person_identity_keys)
        if not normalized_keys:
            return {}
        placeholders = ", ".join(["%s"] * len(normalized_keys))
        postgres_rows = self._select_control_plane_rows(
            "candidate_evidence_index",
            row_builder=self._candidate_evidence_index_from_row,
            where_sql=f"person_identity_key IN ({placeholders})",
            params=normalized_keys,
            order_by_sql="person_identity_key ASC",
            limit=len(normalized_keys),
        )
        if postgres_rows:
            return {
                str(row.get("person_identity_key") or "").strip(): row
                for row in postgres_rows
                if str(row.get("person_identity_key") or "").strip()
            }
        return {}

    def apply_projection_crm_selection(
        self,
        *,
        projection_id: str,
        expected_membership_revision: str,
        expected_source_candidate_count: int,
        candidate_identity_keys: list[str] | tuple[str, ...],
        workspace_id: str,
        selection_idempotency_keys: dict[str, str],
        payload_builder: Any,
    ) -> dict[str, Any]:
        """Temporary Store facade for the fixed projection-to-CRM PG UoW.

        CRM repository migration removes this facade; callers receive domain
        rows while the adapter owns the transaction and lock ordering.
        """

        if not callable(payload_builder):
            raise TypeError("payload_builder must be callable")

        def build_storage_payload(**raw_payload: Any) -> dict[str, Any]:
            projection_row = dict(raw_payload.get("projection_row") or {})
            member_rows_by_key = {
                str(key or "").strip(): self.repos.serving_projection._member_from_row(row)  # noqa: SLF001
                for key, row in dict(raw_payload.get("member_rows_by_key") or {}).items()
            }
            existing_records_by_person = {
                str(key or "").strip(): self._crm_record_from_row(row)
                for key, row in dict(raw_payload.get("existing_records_by_person") or {}).items()
            }
            existing_engagements_by_id = {
                str(key or "").strip(): self._crm_engagement_from_row(row)
                for key, row in dict(raw_payload.get("existing_engagements_by_id") or {}).items()
            }
            existing_events_by_idempotency = {
                str(key or "").strip(): self._crm_event_from_row(row)
                for key, row in dict(raw_payload.get("existing_events_by_idempotency") or {}).items()
            }
            built = payload_builder(
                projection=self.repos.serving_projection._projection_from_row(projection_row),  # noqa: SLF001
                members_by_candidate_key=member_rows_by_key,
                existing_records_by_person=existing_records_by_person,
                existing_engagements_by_id=existing_engagements_by_id,
                existing_events_by_idempotency=existing_events_by_idempotency,
                selection_now=str(raw_payload.get("selection_now") or ""),
                source_candidate_count=int(raw_payload.get("source_candidate_count") or 0),
            )
            if not isinstance(built, dict):
                raise TypeError("projection CRM domain payload_builder must return a dict")
            record_rows = [
                _crm_core_repo.CRM_RECORDS.to_columns(
                    {
                        **dict(row or {}),
                        "metadata": _normalize_json_object_payload(
                            dict(row or {}).get("metadata") or dict(row or {}).get("metadata_json")
                        ),
                    }
                )
                for row in list(built.get("record_rows") or [])
            ]
            engagement_rows = []
            for raw_row in list(built.get("engagement_rows") or []):
                row = dict(raw_row or {})
                engagement_rows.append(
                    {
                        "engagement_id": str(row.get("engagement_id") or "").strip(),
                        "crm_record_id": str(row.get("crm_record_id") or "").strip(),
                        "pipeline_id": str(row.get("pipeline_id") or "default_sourcing").strip() or "default_sourcing",
                        "stage": str(row.get("stage") or "new").strip() or "new",
                        "stage_category": str(
                            row.get("stage_category") or _crm_stage_category(row.get("stage"))
                        ).strip()
                        or "open",
                        "priority": str(row.get("priority") or "normal").strip() or "normal",
                        "quality_score": row.get("quality_score"),
                        "next_action_at": str(row.get("next_action_at") or "").strip(),
                        "last_contacted_at": str(row.get("last_contacted_at") or "").strip(),
                        "source_projection_id": str(row.get("source_projection_id") or "").strip(),
                        "source_run_id": str(row.get("source_run_id") or "").strip(),
                        "source_selection_reason": str(row.get("source_selection_reason") or "").strip(),
                        "created_by_actor": str(row.get("created_by_actor") or "").strip(),
                        "metadata_json": json.dumps(
                            _normalize_json_object_payload(row.get("metadata") or row.get("metadata_json")),
                            ensure_ascii=False,
                        ),
                        "created_at": str(row.get("created_at") or "").strip(),
                        "updated_at": str(row.get("updated_at") or "").strip(),
                    }
                )
            event_rows = [
                _crm_core_repo.CRM_EVENTS.to_columns(
                    {
                        **dict(row or {}),
                        "payload": _normalize_json_object_payload(
                            dict(row or {}).get("payload") or dict(row or {}).get("payload_json")
                        ),
                        "metadata": _normalize_json_object_payload(
                            dict(row or {}).get("metadata") or dict(row or {}).get("metadata_json")
                        ),
                    }
                )
                for row in list(built.get("event_rows") or [])
            ]
            return {
                **built,
                "record_rows": record_rows,
                "engagement_rows": engagement_rows,
                "event_rows": event_rows,
            }

        result = self._call_control_plane_postgres_native(
            "apply_projection_crm_selection",
            table_name="crm_records",
            projection_id=projection_id,
            expected_membership_revision=expected_membership_revision,
            expected_source_candidate_count=expected_source_candidate_count,
            candidate_identity_keys=list(candidate_identity_keys or []),
            workspace_id=workspace_id,
            selection_idempotency_keys=dict(selection_idempotency_keys or {}),
            payload_builder=build_storage_payload,
        )
        if not isinstance(result, dict):
            self._raise_control_plane_postgres_write_failure(
                table_name="crm_records",
                method_name="apply_projection_crm_selection",
                reason="postgres-only: projection CRM UoW returned no confirmation",
            )
        return {
            **result,
            "projection": self.repos.serving_projection._projection_from_row(  # noqa: SLF001
                result.get("projection_row")
            ),
            "members": [
                self.repos.serving_projection._member_from_row(row)  # noqa: SLF001
                for row in list(result.get("member_rows") or [])
            ],
            "crm_records": [self._crm_record_from_row(row) for row in list(result.get("record_rows") or [])],
            "crm_engagements": [
                self._crm_engagement_from_row(row) for row in list(result.get("engagement_rows") or [])
            ],
            "crm_events": [self._crm_event_from_row(row) for row in list(result.get("event_rows") or [])],
        }

    def upsert_crm_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        workspace_id = str(normalized.get("workspace_id") or "default").strip() or "default"
        person_identity_key = str(normalized.get("person_identity_key") or "").strip()
        if not person_identity_key:
            return {}
        existing = self.get_crm_record_by_person_identity(
            person_identity_key,
            workspace_id=workspace_id,
        )
        crm_record_id = str(
            normalized.get("crm_record_id")
            or normalized.get("id")
            or (existing or {}).get("crm_record_id")
            or f"crmrec_{uuid4().hex}"
        ).strip()
        now = _utc_now_timestamp()
        crm_version = (
            int((existing or {}).get("crm_version") or normalized.get("crm_version") or 0) + 1
            if existing
            else int(normalized.get("crm_version") or 1)
        )
        row_payload = _crm_core_repo.CRM_RECORDS.to_columns(
            {
                **normalized,
                "crm_record_id": crm_record_id,
                "workspace_id": workspace_id,
                "person_identity_key": person_identity_key,
                "crm_version": crm_version,
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        return self._upsert_simple_control_plane_row(
            "crm_records",
            id_column="crm_record_id",
            row_payload=row_payload,
            row_builder=self._crm_record_from_row,
        )

    def apply_owned_crm_record_update(
        self,
        *,
        record_payload: dict[str, Any],
        engagement_payload: dict[str, Any],
        event_payload: dict[str, Any],
        expected_workspace_id: str,
        expected_owner_user_id: str,
    ) -> dict[str, Any]:
        """Commit an authenticated CRM edit under the canonical record row lock."""

        normalized_record = dict(record_payload or {})
        normalized_engagement = dict(engagement_payload or {})
        normalized_event = dict(event_payload or {})
        crm_record_id = str(normalized_record.get("crm_record_id") or normalized_record.get("id") or "").strip()
        person_identity_key = str(normalized_record.get("person_identity_key") or "").strip()
        normalized_workspace = str(expected_workspace_id or "").strip()
        normalized_owner = str(expected_owner_user_id or "").strip()
        if not crm_record_id or not person_identity_key or not normalized_workspace or not normalized_owner:
            return {"status": "not_found", "reason": "crm_record_not_found"}

        now = _utc_now_timestamp()
        expected_crm_version = int(normalized_record.get("crm_version") or 0)
        engagement_id = str(
            normalized_engagement.get("engagement_id") or normalized_engagement.get("id") or f"crmeng_{uuid4().hex}"
        ).strip()
        event_id = str(
            normalized_event.get("event_id") or normalized_event.get("id") or f"crmevt_{uuid4().hex}"
        ).strip()
        record_row = _crm_core_repo.CRM_RECORDS.to_columns(
            {
                **normalized_record,
                "crm_record_id": crm_record_id,
                "workspace_id": normalized_workspace,
                "person_identity_key": person_identity_key,
                "current_engagement_id": engagement_id,
                "crm_version": expected_crm_version + 1,
                "metadata": _normalize_json_object_payload(
                    normalized_record.get("metadata") or normalized_record.get("metadata_json")
                ),
                "created_at": str(normalized_record.get("created_at") or now),
                "updated_at": now,
            }
        )
        engagement_row = {
            "engagement_id": engagement_id,
            "crm_record_id": crm_record_id,
            "pipeline_id": str(normalized_engagement.get("pipeline_id") or "default_sourcing").strip()
            or "default_sourcing",
            "stage": str(normalized_engagement.get("stage") or "new").strip() or "new",
            "stage_category": str(
                normalized_engagement.get("stage_category") or _crm_stage_category(normalized_engagement.get("stage"))
            ).strip()
            or "open",
            "priority": str(normalized_engagement.get("priority") or "normal").strip() or "normal",
            "quality_score": normalized_engagement.get("quality_score"),
            "next_action_at": str(normalized_engagement.get("next_action_at") or "").strip(),
            "last_contacted_at": str(normalized_engagement.get("last_contacted_at") or "").strip(),
            "source_projection_id": str(normalized_engagement.get("source_projection_id") or "").strip(),
            "source_run_id": str(normalized_engagement.get("source_run_id") or "").strip(),
            "source_selection_reason": str(normalized_engagement.get("source_selection_reason") or "").strip(),
            "created_by_actor": str(normalized_engagement.get("created_by_actor") or "").strip(),
            "metadata_json": json.dumps(
                _normalize_json_object_payload(
                    normalized_engagement.get("metadata") or normalized_engagement.get("metadata_json")
                ),
                ensure_ascii=False,
            ),
            "created_at": str(normalized_engagement.get("created_at") or now),
            "updated_at": now,
        }
        event_row = _crm_core_repo.CRM_EVENTS.to_columns(
            {
                **normalized_event,
                "event_id": event_id,
                "workspace_id": normalized_workspace,
                "crm_record_id": crm_record_id,
                "engagement_id": engagement_id,
                "person_identity_key": person_identity_key,
                "payload": _normalize_json_object_payload(
                    normalized_event.get("payload") or normalized_event.get("payload_json")
                ),
                "metadata": _normalize_json_object_payload(
                    normalized_event.get("metadata") or normalized_event.get("metadata_json")
                ),
                "occurred_at": str(normalized_event.get("occurred_at") or now).strip(),
                "created_at": str(normalized_event.get("created_at") or now),
            }
        )
        result = self._call_control_plane_postgres_native(
            "apply_owned_crm_record_update",
            crm_record_id=crm_record_id,
            expected_workspace_id=normalized_workspace,
            expected_owner_user_id=normalized_owner,
            expected_crm_version=expected_crm_version,
            record_row=record_row,
            engagement_row=engagement_row,
            event_row=event_row,
        )
        if not isinstance(result, dict):
            self._raise_control_plane_postgres_write_failure(
                table_name="crm_records",
                method_name="apply_owned_crm_record_update",
                reason="postgres-only: owned CRM update UoW returned no confirmation",
            )
        if result.get("status") != "applied":
            return dict(result)
        return {
            "status": "applied",
            "crm_record": self._crm_record_from_row(dict(result.get("record_row") or {})),
            "crm_engagement": self._crm_engagement_from_row(dict(result.get("engagement_row") or {})),
            "crm_event": self._crm_event_from_row(dict(result.get("event_row") or {})),
        }

    def get_crm_record(self, crm_record_id: str) -> dict[str, Any]:
        normalized_record_id = str(crm_record_id or "").strip()
        if not normalized_record_id:
            return {}
        postgres_row = self._select_control_plane_row(
            "crm_records",
            row_builder=self._crm_record_from_row,
            where_sql="crm_record_id = %s",
            params=[normalized_record_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def get_crm_record_by_person_identity(
        self,
        person_identity_key: str,
        *,
        workspace_id: str = "default",
    ) -> dict[str, Any]:
        normalized_person_key = str(person_identity_key or "").strip()
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        if not normalized_person_key:
            return {}
        postgres_row = self._select_control_plane_row(
            "crm_records",
            row_builder=self._crm_record_from_row,
            where_sql="workspace_id = %s AND person_identity_key = %s",
            params=[normalized_workspace_id, normalized_person_key],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_crm_records_by_person_identity_keys(
        self,
        person_identity_keys: list[str] | tuple[str, ...],
        *,
        workspace_id: str = "default",
    ) -> dict[str, dict[str, Any]]:
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        keys = _dedupe_preserve_order(
            [str(key or "").strip() for key in list(person_identity_keys or []) if str(key or "").strip()]
        )
        if not keys:
            return {}
        ", ".join("?" for _ in keys)
        postgres_rows = self._select_control_plane_rows(
            "crm_records",
            row_builder=self._crm_record_from_row,
            where_sql=f"workspace_id = %s AND person_identity_key IN ({', '.join('%s' for _ in keys)})",
            params=[normalized_workspace_id, *keys],
            order_by_sql="updated_at DESC",
            limit=0,
        )
        if postgres_rows:
            return {
                str(row.get("person_identity_key") or "").strip(): row
                for row in postgres_rows
                if str(row.get("person_identity_key") or "").strip()
            }
        return {}

    def list_crm_records(
        self,
        *,
        source_projection_id: str = "",
        source_collection_id: str = "",
        workspace_id: str = "default",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses = ["workspace_id = ?"]
        params: list[Any] = [str(workspace_id or "default").strip() or "default"]
        if source_projection_id:
            clauses.append("source_projection_id = ?")
            params.append(str(source_projection_id).strip())
        if source_collection_id:
            clauses.append("source_collection_id = ?")
            params.append(str(source_collection_id).strip())
        where_sqlite = " AND ".join(clauses)
        postgres_rows = self._select_control_plane_rows(
            "crm_records",
            row_builder=self._crm_record_from_row,
            where_sql=where_sqlite.replace("?", "%s"),
            params=params,
            order_by_sql="updated_at DESC, crm_record_id DESC",
            limit=max(1, int(limit or 100)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_crm_engagement(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        engagement_id = str(normalized.get("engagement_id") or normalized.get("id") or f"crmeng_{uuid4().hex}").strip()
        now = _utc_now_timestamp()
        existing = self.get_crm_engagement(engagement_id)
        row_payload = {
            "engagement_id": engagement_id,
            "crm_record_id": str(normalized.get("crm_record_id") or "").strip(),
            "pipeline_id": str(normalized.get("pipeline_id") or "default_sourcing").strip() or "default_sourcing",
            "stage": str(normalized.get("stage") or "new").strip() or "new",
            "stage_category": str(
                normalized.get("stage_category") or _crm_stage_category(normalized.get("stage"))
            ).strip()
            or "open",
            "priority": str(normalized.get("priority") or "normal").strip() or "normal",
            "quality_score": normalized.get("quality_score"),
            "next_action_at": str(normalized.get("next_action_at") or "").strip(),
            "last_contacted_at": str(normalized.get("last_contacted_at") or "").strip(),
            "source_projection_id": str(normalized.get("source_projection_id") or "").strip(),
            "source_run_id": str(normalized.get("source_run_id") or "").strip(),
            "source_selection_reason": str(normalized.get("source_selection_reason") or "").strip(),
            "created_by_actor": str(normalized.get("created_by_actor") or "").strip(),
            "metadata_json": json.dumps(
                _normalize_json_object_payload(normalized.get("metadata") or normalized.get("metadata_json")),
                ensure_ascii=False,
            ),
            "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
            "updated_at": now,
        }
        return self._upsert_simple_control_plane_row(
            "crm_engagements",
            id_column="engagement_id",
            row_payload=row_payload,
            row_builder=self._crm_engagement_from_row,
        )

    def get_crm_engagement(self, engagement_id: str) -> dict[str, Any]:
        normalized_engagement_id = str(engagement_id or "").strip()
        if not normalized_engagement_id:
            return {}
        postgres_row = self._select_control_plane_row(
            "crm_engagements",
            row_builder=self._crm_engagement_from_row,
            where_sql="engagement_id = %s",
            params=[normalized_engagement_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_crm_engagements(self, *, crm_record_id: str, limit: int = 50) -> list[dict[str, Any]]:
        normalized_record_id = str(crm_record_id or "").strip()
        if not normalized_record_id:
            return []
        postgres_rows = self._select_control_plane_rows(
            "crm_engagements",
            row_builder=self._crm_engagement_from_row,
            where_sql="crm_record_id = %s",
            params=[normalized_record_id],
            order_by_sql="updated_at DESC, engagement_id DESC",
            limit=max(1, int(limit or 50)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_crm_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("crm_tasks")
        normalized = dict(payload or {})
        workspace_id = str(normalized.get("workspace_id") or "default").strip() or "default"
        idempotency_key = str(normalized.get("idempotency_key") or "").strip()
        if idempotency_key:
            existing_by_key = self.get_crm_task_by_idempotency(idempotency_key, workspace_id=workspace_id)
            if existing_by_key:
                normalized["task_id"] = existing_by_key.get("task_id")
                normalized.setdefault("created_at", existing_by_key.get("created_at"))
        task_id = str(normalized.get("task_id") or normalized.get("id") or f"crmtask_{uuid4().hex}").strip()
        if not task_id:
            return {}
        existing = self.get_crm_task(task_id)
        now = _utc_now_timestamp()
        row_payload = _crm_core_repo.CRM_TASKS.to_columns(
            {
                **normalized,
                "task_id": task_id,
                "workspace_id": workspace_id,
                "idempotency_key": idempotency_key,
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        return self._upsert_simple_control_plane_row(
            "crm_tasks",
            id_column="task_id",
            row_payload=row_payload,
            row_builder=self._crm_task_from_row,
        )

    def get_crm_task(self, task_id: str) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("crm_tasks")
        normalized_task_id = str(task_id or "").strip()
        if not normalized_task_id:
            return {}
        postgres_row = self._select_control_plane_row(
            "crm_tasks",
            row_builder=self._crm_task_from_row,
            where_sql="task_id = %s",
            params=[normalized_task_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def get_crm_task_by_idempotency(self, idempotency_key: str, *, workspace_id: str = "default") -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("crm_tasks")
        normalized_key = str(idempotency_key or "").strip()
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        if not normalized_key:
            return {}
        postgres_row = self._select_control_plane_row(
            "crm_tasks",
            row_builder=self._crm_task_from_row,
            where_sql="workspace_id = %s AND idempotency_key = %s",
            params=[normalized_workspace_id, normalized_key],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_crm_tasks(
        self,
        *,
        workspace_id: str = "default",
        crm_record_id: str = "",
        status: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        self._require_postgres_for_durable_runtime("crm_tasks")
        clauses = ["workspace_id = ?"]
        params: list[Any] = [str(workspace_id or "default").strip() or "default"]
        normalized_record_id = str(crm_record_id or "").strip()
        normalized_status = str(status or "").strip()
        if normalized_record_id:
            clauses.append("crm_record_id = ?")
            params.append(normalized_record_id)
        if normalized_status:
            clauses.append("status = ?")
            params.append(normalized_status)
        where_sqlite = " AND ".join(clauses)
        postgres_rows = self._select_control_plane_rows(
            "crm_tasks",
            row_builder=self._crm_task_from_row,
            where_sql=where_sqlite.replace("?", "%s"),
            params=params,
            order_by_sql="due_at ASC, updated_at DESC, task_id DESC",
            limit=max(1, int(limit or 100)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def append_crm_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        workspace_id = str(normalized.get("workspace_id") or "default").strip() or "default"
        idempotency_key = str(normalized.get("idempotency_key") or "").strip()
        if idempotency_key:
            existing = self.get_crm_event_by_idempotency(idempotency_key, workspace_id=workspace_id)
            if existing:
                return existing
        event_id = str(normalized.get("event_id") or normalized.get("id") or f"crmevt_{uuid4().hex}").strip()
        now = _utc_now_timestamp()
        row_payload = _crm_core_repo.CRM_EVENTS.to_columns(
            {
                **normalized,
                "event_id": event_id,
                "workspace_id": workspace_id,
                "idempotency_key": idempotency_key,
                "payload": _normalize_json_object_payload(normalized.get("payload") or normalized.get("payload_json")),
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "occurred_at": str(normalized.get("occurred_at") or now).strip(),
                "created_at": now,
            }
        )
        return self._upsert_simple_control_plane_row(
            "crm_events",
            id_column="event_id",
            row_payload=row_payload,
            row_builder=self._crm_event_from_row,
        )

    def get_crm_event_by_idempotency(self, idempotency_key: str, *, workspace_id: str = "default") -> dict[str, Any]:
        normalized_key = str(idempotency_key or "").strip()
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        if not normalized_key:
            return {}
        postgres_row = self._select_control_plane_row(
            "crm_events",
            row_builder=self._crm_event_from_row,
            where_sql="workspace_id = %s AND idempotency_key = %s",
            params=[normalized_workspace_id, normalized_key],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def upsert_target_candidate_public_web_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_legacy_target_public_web_migration_write("target_candidate_public_web_batches")
        normalized = _normalize_target_candidate_public_web_batch_payload(payload)
        existing = self.get_target_candidate_public_web_batch(batch_id=normalized["batch_id"])
        now = _utc_now_timestamp()
        created_at = str((existing or {}).get("created_at") or normalized.get("created_at") or "").strip() or now
        row_payload = _public_web_repo.TARGET_CANDIDATE_PUBLIC_WEB_BATCHES.to_columns(
            {**normalized, "created_at": created_at, "updated_at": now}
        )
        if self._write_control_plane_row_to_postgres("target_candidate_public_web_batches", row_payload):
            return self.get_target_candidate_public_web_batch(
                batch_id=normalized["batch_id"]
            ) or self._target_candidate_public_web_batch_from_row(row_payload)
        self._raise_control_plane_postgres_write_failure(
            table_name="target_candidate_public_web_batches",
            method_name="upsert_target_candidate_public_web_batch",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_target_candidate_public_web_batch(
        self,
        *,
        batch_id: str = "",
        idempotency_key: str = "",
    ) -> dict[str, Any] | None:
        normalized_batch_id = str(batch_id or "").strip()
        normalized_idempotency_key = str(idempotency_key or "").strip()
        if not normalized_batch_id and not normalized_idempotency_key:
            return None
        where_sql = "batch_id = %s" if normalized_batch_id else "idempotency_key = %s"
        value = normalized_batch_id or normalized_idempotency_key
        postgres_row = self._select_control_plane_row(
            "target_candidate_public_web_batches",
            row_builder=self._target_candidate_public_web_batch_from_row,
            where_sql=where_sql,
            params=[value],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_target_candidate_public_web_batches(
        self,
        *,
        status: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_status = _normalize_target_candidate_public_web_status(status, default="")
        postgres_rows = self._select_control_plane_rows(
            "target_candidate_public_web_batches",
            row_builder=self._target_candidate_public_web_batch_from_row,
            where_sql="status = %s" if normalized_status else "",
            params=[normalized_status] if normalized_status else [],
            order_by_sql="updated_at DESC, created_at DESC, batch_id DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_target_candidate_public_web_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_legacy_target_public_web_migration_write("target_candidate_public_web_runs")
        normalized = _normalize_target_candidate_public_web_run_payload(payload)
        existing = self.get_target_candidate_public_web_run(run_id=normalized["run_id"])
        now = _utc_now_timestamp()
        row_payload = _target_candidate_public_web_run_row_payload(normalized, existing=existing, now=now)
        if self._write_control_plane_row_to_postgres("target_candidate_public_web_runs", row_payload):
            return self.get_target_candidate_public_web_run(
                run_id=normalized["run_id"]
            ) or self._target_candidate_public_web_run_from_row(row_payload)
        self._raise_control_plane_postgres_write_failure(
            table_name="target_candidate_public_web_runs",
            method_name="upsert_target_candidate_public_web_run",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def update_target_candidate_public_web_run(self, run_id: str, patch: dict[str, Any]) -> dict[str, Any] | None:
        self._require_legacy_target_public_web_migration_write("target_candidate_public_web_runs")
        existing = self.get_target_candidate_public_web_run(run_id=run_id)
        if existing is None:
            return None
        merged = {
            **existing,
            **dict(patch or {}),
            "run_id": str(run_id or "").strip(),
            "source_families": patch.get("source_families", existing.get("source_families"))
            if patch
            else existing.get("source_families"),
            "options": patch.get("options", existing.get("options")) if patch else existing.get("options"),
            "query_manifest": patch.get("query_manifest", existing.get("query_manifest"))
            if patch
            else existing.get("query_manifest"),
            "search_checkpoint": patch.get("search_checkpoint", existing.get("search_checkpoint"))
            if patch
            else existing.get("search_checkpoint"),
            "fetch_checkpoint": patch.get("fetch_checkpoint", existing.get("fetch_checkpoint"))
            if patch
            else existing.get("fetch_checkpoint"),
            "analysis_checkpoint": patch.get("analysis_checkpoint", existing.get("analysis_checkpoint"))
            if patch
            else existing.get("analysis_checkpoint"),
            "summary": patch.get("summary", existing.get("summary")) if patch else existing.get("summary"),
        }
        return self.upsert_target_candidate_public_web_run(merged)

    def get_target_candidate_public_web_run(
        self,
        *,
        run_id: str = "",
        idempotency_key: str = "",
    ) -> dict[str, Any] | None:
        normalized_run_id = str(run_id or "").strip()
        normalized_idempotency_key = str(idempotency_key or "").strip()
        if not normalized_run_id and not normalized_idempotency_key:
            return None
        where_sql = "run_id = %s" if normalized_run_id else "idempotency_key = %s"
        value = normalized_run_id or normalized_idempotency_key
        postgres_row = self._select_control_plane_row(
            "target_candidate_public_web_runs",
            row_builder=self._target_candidate_public_web_run_from_row,
            where_sql=where_sql,
            params=[value],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_target_candidate_public_web_runs(
        self,
        *,
        batch_id: str = "",
        record_id: str = "",
        status: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if batch_id:
            clauses.append("batch_id = ?")
            params.append(str(batch_id or "").strip())
        if record_id:
            clauses.append("record_id = ?")
            params.append(str(record_id or "").strip())
        normalized_status = _normalize_target_candidate_public_web_status(status, default="")
        if normalized_status:
            clauses.append("status = ?")
            params.append(normalized_status)
        postgres_rows = self._select_control_plane_rows(
            "target_candidate_public_web_runs",
            row_builder=self._target_candidate_public_web_run_from_row,
            where_sql=" AND ".join(
                [
                    *(["batch_id = %s"] if batch_id else []),
                    *(["record_id = %s"] if record_id else []),
                    *(["status = %s"] if normalized_status else []),
                ]
            ),
            params=[
                *([str(batch_id or "").strip()] if batch_id else []),
                *([str(record_id or "").strip()] if record_id else []),
                *([normalized_status] if normalized_status else []),
            ],
            order_by_sql="updated_at DESC, created_at DESC, run_id DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def list_latest_target_candidate_public_web_runs_by_record_ids(
        self,
        record_ids: list[str] | tuple[str, ...],
        *,
        status: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        normalized_record_ids = _normalize_public_web_string_list(record_ids)[: max(1, int(limit or 1000))]
        if not normalized_record_ids:
            return []
        normalized_status = _normalize_target_candidate_public_web_status(status, default="")
        native_rows = self._call_control_plane_postgres_native(
            "list_latest_target_candidate_public_web_runs_by_record_ids",
            normalized_record_ids,
            status=normalized_status,
            limit=max(1, int(limit or 1000)),
        )
        if native_rows:
            return [
                self._target_candidate_public_web_run_from_row(row)
                for row in list(native_rows or [])
                if isinstance(row, dict)
            ]
        return []

    def upsert_crm_public_web_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = _normalize_crm_public_web_batch_payload(payload)
        existing = self.get_crm_public_web_batch(batch_id=normalized["batch_id"])
        now = _utc_now_timestamp()
        row_payload = _crm_public_web_batch_row_payload(normalized, existing=existing, now=now)
        if self._write_control_plane_row_to_postgres("crm_public_web_batches", row_payload):
            return self.get_crm_public_web_batch(
                batch_id=normalized["batch_id"]
            ) or self._crm_public_web_batch_from_row(row_payload)
        self._raise_control_plane_postgres_write_failure(
            table_name="crm_public_web_batches",
            method_name="upsert_crm_public_web_batch",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_crm_public_web_batch(
        self,
        *,
        batch_id: str = "",
        idempotency_key: str = "",
    ) -> dict[str, Any] | None:
        normalized_batch_id = str(batch_id or "").strip()
        normalized_idempotency_key = str(idempotency_key or "").strip()
        if not normalized_batch_id and not normalized_idempotency_key:
            return None
        where_sql = "batch_id = %s" if normalized_batch_id else "idempotency_key = %s"
        value = normalized_batch_id or normalized_idempotency_key
        postgres_row = self._select_control_plane_row(
            "crm_public_web_batches",
            row_builder=self._crm_public_web_batch_from_row,
            where_sql=where_sql,
            params=[value],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_crm_public_web_batches(
        self,
        *,
        status: str = "",
        workspace_id: str = "default",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_status = _normalize_target_candidate_public_web_status(status, default="")
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        clauses_pg = ["workspace_id = %s"]
        clauses_sqlite = ["workspace_id = ?"]
        params: list[Any] = [normalized_workspace_id]
        if normalized_status:
            clauses_pg.append("status = %s")
            clauses_sqlite.append("status = ?")
            params.append(normalized_status)
        postgres_rows = self._select_control_plane_rows(
            "crm_public_web_batches",
            row_builder=self._crm_public_web_batch_from_row,
            where_sql=" AND ".join(clauses_pg),
            params=params,
            order_by_sql="updated_at DESC, created_at DESC, batch_id DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_crm_public_web_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = _normalize_crm_public_web_run_payload(payload)
        existing = self.get_crm_public_web_run(run_id=normalized["run_id"])
        now = _utc_now_timestamp()
        row_payload = _crm_public_web_run_row_payload(normalized, existing=existing, now=now)
        if self._write_control_plane_row_to_postgres("crm_public_web_runs", row_payload):
            return self.get_crm_public_web_run(run_id=normalized["run_id"]) or self._crm_public_web_run_from_row(
                row_payload
            )
        self._raise_control_plane_postgres_write_failure(
            table_name="crm_public_web_runs",
            method_name="upsert_crm_public_web_run",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def update_crm_public_web_run(self, run_id: str, patch: dict[str, Any]) -> dict[str, Any] | None:
        existing = self.get_crm_public_web_run(run_id=run_id)
        if existing is None:
            return None
        merged = {
            **existing,
            **dict(patch or {}),
            "run_id": str(run_id or "").strip(),
            "crm_record_id": str(
                (patch or {}).get("crm_record_id") or existing.get("crm_record_id") or existing.get("record_id") or ""
            ),
            "source_families": patch.get("source_families", existing.get("source_families"))
            if patch
            else existing.get("source_families"),
            "options": patch.get("options", existing.get("options")) if patch else existing.get("options"),
            "query_manifest": patch.get("query_manifest", existing.get("query_manifest"))
            if patch
            else existing.get("query_manifest"),
            "search_checkpoint": patch.get("search_checkpoint", existing.get("search_checkpoint"))
            if patch
            else existing.get("search_checkpoint"),
            "fetch_checkpoint": patch.get("fetch_checkpoint", existing.get("fetch_checkpoint"))
            if patch
            else existing.get("fetch_checkpoint"),
            "analysis_checkpoint": patch.get("analysis_checkpoint", existing.get("analysis_checkpoint"))
            if patch
            else existing.get("analysis_checkpoint"),
            "summary": patch.get("summary", existing.get("summary")) if patch else existing.get("summary"),
        }
        return self.upsert_crm_public_web_run(merged)

    def get_crm_public_web_run(
        self,
        *,
        run_id: str = "",
        idempotency_key: str = "",
    ) -> dict[str, Any] | None:
        normalized_run_id = str(run_id or "").strip()
        normalized_idempotency_key = str(idempotency_key or "").strip()
        if not normalized_run_id and not normalized_idempotency_key:
            return None
        where_sql = "run_id = %s" if normalized_run_id else "idempotency_key = %s"
        value = normalized_run_id or normalized_idempotency_key
        postgres_row = self._select_control_plane_row(
            "crm_public_web_runs",
            row_builder=self._crm_public_web_run_from_row,
            where_sql=where_sql,
            params=[value],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_crm_public_web_runs(
        self,
        *,
        batch_id: str = "",
        crm_record_id: str = "",
        workspace_id: str = "default",
        status: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        clauses_sqlite: list[str] = []
        clauses_pg: list[str] = []
        params: list[Any] = []
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        if batch_id:
            clauses_sqlite.append("batch_id = ?")
            clauses_pg.append("batch_id = %s")
            params.append(str(batch_id or "").strip())
            clauses_sqlite.append("workspace_id = ?")
            clauses_pg.append("workspace_id = %s")
            params.append(normalized_workspace_id)
        if crm_record_id:
            clauses_sqlite.append("workspace_id = ?")
            clauses_pg.append("workspace_id = %s")
            params.append(normalized_workspace_id)
            clauses_sqlite.append("crm_record_id = ?")
            clauses_pg.append("crm_record_id = %s")
            params.append(str(crm_record_id or "").strip())
        elif not batch_id:
            clauses_sqlite.append("workspace_id = ?")
            clauses_pg.append("workspace_id = %s")
            params.append(normalized_workspace_id)
        normalized_status = _normalize_target_candidate_public_web_status(status, default="")
        if normalized_status:
            clauses_sqlite.append("status = ?")
            clauses_pg.append("status = %s")
            params.append(normalized_status)
        order_by_sql = (
            "created_at DESC, run_id DESC"
            if crm_record_id and not batch_id
            else "updated_at DESC, created_at DESC, run_id DESC"
        )
        postgres_rows = self._select_control_plane_rows(
            "crm_public_web_runs",
            row_builder=self._crm_public_web_run_from_row,
            where_sql=" AND ".join(clauses_pg),
            params=params,
            order_by_sql=order_by_sql,
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def list_latest_crm_public_web_runs_by_record_ids(
        self,
        crm_record_ids: list[str] | tuple[str, ...],
        *,
        workspace_id: str = "default",
        status: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        normalized_record_ids = _normalize_public_web_string_list(crm_record_ids)[: max(1, int(limit or 1000))]
        if not normalized_record_ids:
            return []
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        normalized_status = _normalize_target_candidate_public_web_status(status, default="")
        native_rows = self._call_control_plane_postgres_native(
            "list_latest_crm_public_web_runs_by_record_ids",
            normalized_record_ids,
            workspace_id=normalized_workspace_id,
            status=normalized_status,
            limit=max(1, int(limit or 1000)),
        )
        if native_rows:
            latest_by_record: dict[str, dict[str, Any]] = {}
            for row in list(native_rows or []):
                if not isinstance(row, dict):
                    continue
                parsed = self._crm_public_web_run_from_row(row)
                record_id = str(parsed.get("crm_record_id") or parsed.get("record_id") or "").strip()
                if record_id and record_id not in latest_by_record:
                    latest_by_record[record_id] = parsed
            return list(latest_by_record.values())[: max(1, int(limit or 1000))]
        return []

    def upsert_company_public_web_asset_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = _normalize_company_public_web_asset_run_payload(payload)
        existing = self.get_company_public_web_asset_run(run_id=normalized["run_id"])
        now = _utc_now_timestamp()
        row_payload = _company_public_web_asset_run_row_payload(normalized, existing=existing, now=now)
        if self._write_control_plane_row_to_postgres("company_public_web_asset_runs", row_payload):
            return self.get_company_public_web_asset_run(
                run_id=normalized["run_id"]
            ) or self._company_public_web_asset_run_from_row(row_payload)
        self._raise_control_plane_postgres_write_failure(
            table_name="company_public_web_asset_runs",
            method_name="upsert_company_public_web_asset_run",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_company_public_web_asset_run(
        self,
        *,
        run_id: str = "",
        idempotency_key: str = "",
    ) -> dict[str, Any] | None:
        normalized_run_id = str(run_id or "").strip()
        normalized_idempotency_key = str(idempotency_key or "").strip()
        if not normalized_run_id and not normalized_idempotency_key:
            return None
        where_sql = "run_id = %s" if normalized_run_id else "idempotency_key = %s"
        value = normalized_run_id or normalized_idempotency_key
        postgres_row = self._select_control_plane_row(
            "company_public_web_asset_runs",
            row_builder=self._company_public_web_asset_run_from_row,
            where_sql=where_sql,
            params=[value],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_company_public_web_asset_runs(
        self,
        *,
        target_company: str = "",
        company_key: str = "",
        status: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company, company_key)
        normalized_status = _normalize_target_candidate_public_web_status(status, default="")
        clauses_sqlite: list[str] = []
        clauses_pg: list[str] = []
        params: list[Any] = []
        if normalized_target_company or normalized_company_key:
            clause_sqlite, clause_params = _company_identity_lookup_predicate(
                normalized_target_company,
                normalized_company_key,
            )
            clause_pg, _ = _company_identity_lookup_predicate(
                normalized_target_company,
                normalized_company_key,
                placeholder="%s",
            )
            clauses_sqlite.append(clause_sqlite)
            clauses_pg.append(clause_pg)
            params.extend(clause_params)
        if normalized_status:
            clauses_sqlite.append("status = ?")
            clauses_pg.append("status = %s")
            params.append(normalized_status)
        postgres_rows = self._select_control_plane_rows(
            "company_public_web_asset_runs",
            row_builder=self._company_public_web_asset_run_from_row,
            where_sql=" AND ".join(clauses_pg),
            params=params,
            order_by_sql="updated_at DESC, created_at DESC, run_id DESC",
            limit=max(1, int(limit or 100)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def list_latest_company_public_web_asset_runs_by_company_keys(
        self,
        company_keys: list[str] | tuple[str, ...],
        *,
        status: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        normalized_company_keys = _normalize_public_web_string_list(company_keys)[: max(1, int(limit or 1000))]
        if not normalized_company_keys:
            return []
        normalized_status = _normalize_target_candidate_public_web_status(status, default="")
        native_rows = self._call_control_plane_postgres_native(
            "list_latest_company_public_web_asset_runs_by_company_keys",
            normalized_company_keys,
            status=normalized_status,
            limit=max(1, int(limit or 1000)),
        )
        if native_rows:
            return [
                self._company_public_web_asset_run_from_row(row)
                for row in list(native_rows or [])
                if isinstance(row, dict)
            ]
        return []

    def upsert_company_public_web_asset(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = _normalize_company_public_web_asset_payload(payload)
        existing = self.get_company_public_web_asset(asset_id=normalized["asset_id"])
        now = _utc_now_timestamp()
        row_payload = _company_public_web_asset_row_payload(normalized, existing=existing, now=now)
        if self._write_control_plane_row_to_postgres("company_public_web_assets", row_payload):
            return self.get_company_public_web_asset(
                asset_id=normalized["asset_id"]
            ) or self._company_public_web_asset_from_row(row_payload)
        self._raise_control_plane_postgres_write_failure(
            table_name="company_public_web_assets",
            method_name="upsert_company_public_web_asset",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_company_public_web_asset(
        self,
        *,
        asset_id: str = "",
        company_key: str = "",
        normalized_url_key: str = "",
    ) -> dict[str, Any] | None:
        normalized_asset_id = str(asset_id or "").strip()
        normalized_company_key = str(company_key or "").strip()
        normalized_url = str(normalized_url_key or "").strip()
        params: list[Any]
        if normalized_asset_id:
            where_sql, _where_sqlite, params = "asset_id = %s", "asset_id = ?", [normalized_asset_id]
        elif normalized_company_key and normalized_url:
            where_sql, _where_sqlite, params = (
                "company_key = %s AND normalized_url_key = %s",
                "company_key = ? AND normalized_url_key = ?",
                [normalized_company_key, normalized_url],
            )
        elif normalized_url:
            where_sql, _where_sqlite, params = "normalized_url_key = %s", "normalized_url_key = ?", [normalized_url]
        else:
            return None
        postgres_row = self._select_control_plane_row(
            "company_public_web_assets",
            row_builder=self._company_public_web_asset_from_row,
            where_sql=where_sql,
            params=params,
            order_by_sql="updated_at DESC",
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_company_public_web_assets(
        self,
        *,
        target_company: str = "",
        company_key: str = "",
        source_family: str = "",
        status: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company, company_key)
        clauses_sqlite: list[str] = []
        clauses_pg: list[str] = []
        params: list[Any] = []
        if normalized_target_company or normalized_company_key:
            clause_sqlite, clause_params = _company_identity_lookup_predicate(
                normalized_target_company,
                normalized_company_key,
            )
            clause_pg, _ = _company_identity_lookup_predicate(
                normalized_target_company,
                normalized_company_key,
                placeholder="%s",
            )
            clauses_sqlite.append(clause_sqlite)
            clauses_pg.append(clause_pg)
            params.extend(clause_params)
        normalized_source_family = str(source_family or "").strip()
        if normalized_source_family:
            clauses_sqlite.append("source_family = ?")
            clauses_pg.append("source_family = %s")
            params.append(normalized_source_family)
        normalized_status = str(status or "").strip().lower()
        if normalized_status:
            clauses_sqlite.append("status = ?")
            clauses_pg.append("status = %s")
            params.append(normalized_status)
        postgres_rows = self._select_control_plane_rows(
            "company_public_web_assets",
            row_builder=self._company_public_web_asset_from_row,
            where_sql=" AND ".join(clauses_pg),
            params=params,
            order_by_sql="updated_at DESC, created_at DESC, asset_id DESC",
            limit=max(1, int(limit or 500)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_person_public_web_asset(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = _normalize_person_public_web_asset_payload(payload)
        existing = self.get_person_public_web_asset(asset_id=normalized["asset_id"])
        now = _utc_now_timestamp()
        created_at = str((existing or {}).get("created_at") or normalized.get("created_at") or "").strip() or now
        row_payload = _public_web_repo.PERSON_PUBLIC_WEB_ASSETS.to_columns(
            {**normalized, "created_at": created_at, "updated_at": now}
        )
        if self._write_control_plane_row_to_postgres("person_public_web_assets", row_payload):
            return self.get_person_public_web_asset(
                asset_id=normalized["asset_id"]
            ) or self._person_public_web_asset_from_row(row_payload)
        self._raise_control_plane_postgres_write_failure(
            table_name="person_public_web_assets",
            method_name="upsert_person_public_web_asset",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_person_public_web_asset(
        self,
        *,
        asset_id: str = "",
        person_identity_key: str = "",
        linkedin_url_key: str = "",
    ) -> dict[str, Any] | None:
        normalized_asset_id = str(asset_id or "").strip()
        normalized_person_identity_key = str(person_identity_key or "").strip()
        normalized_linkedin_url_key = str(linkedin_url_key or "").strip()
        if normalized_asset_id:
            where_sql, _where_sqlite, value = "asset_id = %s", "asset_id = ?", normalized_asset_id
        elif normalized_person_identity_key:
            where_sql, _where_sqlite, value = (
                "person_identity_key = %s",
                "person_identity_key = ?",
                normalized_person_identity_key,
            )
        elif normalized_linkedin_url_key:
            where_sql, _where_sqlite, value = (
                "linkedin_url_key = %s",
                "linkedin_url_key = ?",
                normalized_linkedin_url_key,
            )
        else:
            return None
        postgres_row = self._select_control_plane_row(
            "person_public_web_assets",
            row_builder=self._person_public_web_asset_from_row,
            where_sql=where_sql,
            params=[value],
            order_by_sql="updated_at DESC",
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_person_public_web_assets(
        self,
        *,
        linkedin_url_key: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        normalized_linkedin_url_key = str(linkedin_url_key or "").strip()
        postgres_rows = self._select_control_plane_rows(
            "person_public_web_assets",
            row_builder=self._person_public_web_asset_from_row,
            where_sql="linkedin_url_key = %s" if normalized_linkedin_url_key else "",
            params=[normalized_linkedin_url_key] if normalized_linkedin_url_key else [],
            order_by_sql="updated_at DESC, created_at DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_person_public_web_signal(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = _normalize_person_public_web_signal_payload(payload)
        existing = self.get_person_public_web_signal(signal_id=normalized["signal_id"])
        now = _utc_now_timestamp()
        row_payload = _person_public_web_signal_row_payload(normalized, existing=existing, now=now)
        if self._write_control_plane_row_to_postgres("person_public_web_signals", row_payload):
            return self.get_person_public_web_signal(
                signal_id=normalized["signal_id"]
            ) or self._person_public_web_signal_from_row(row_payload)
        self._raise_control_plane_postgres_write_failure(
            table_name="person_public_web_signals",
            method_name="upsert_person_public_web_signal",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def replace_person_public_web_signals_for_run(
        self,
        *,
        run_id: str,
        signals: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> int:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return 0
        now = _utc_now_timestamp()
        row_payloads = [
            _person_public_web_signal_row_payload(
                _normalize_person_public_web_signal_payload({**dict(signal), "run_id": normalized_run_id}),
                existing=None,
                now=now,
            )
            for signal in list(signals or [])
            if isinstance(signal, dict)
        ]
        if self._control_plane_postgres_should_prefer_read("person_public_web_signals"):
            try:
                self._control_plane_postgres.delete_rows(
                    table_name="person_public_web_signals",
                    where_sql="run_id = %s",
                    params=[normalized_run_id],
                )
                if row_payloads:
                    self._control_plane_postgres.bulk_upsert_rows("person_public_web_signals", row_payloads)
                if self._control_plane_postgres_should_skip_sqlite_fallback("person_public_web_signals"):
                    return len(row_payloads)
            except Exception as exc:
                if self._control_plane_postgres_should_skip_sqlite_fallback("person_public_web_signals"):
                    self._raise_control_plane_postgres_write_failure(
                        table_name="person_public_web_signals",
                        method_name="replace_person_public_web_signals_for_run",
                        reason=f"{type(exc).__name__}: {exc}",
                        error=exc,
                    )
        raise RuntimeError(
            "postgres-only invariant violated for person_public_web_signals in replace_person_public_web_signals_for_run: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_person_public_web_signal(self, *, signal_id: str = "") -> dict[str, Any] | None:
        normalized_signal_id = str(signal_id or "").strip()
        if not normalized_signal_id:
            return None
        postgres_row = self._select_control_plane_row(
            "person_public_web_signals",
            row_builder=self._person_public_web_signal_from_row,
            where_sql="signal_id = %s",
            params=[normalized_signal_id],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_person_public_web_signals(
        self,
        *,
        run_id: str = "",
        asset_id: str = "",
        person_identity_key: str = "",
        record_id: str = "",
        signal_kind: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        clauses_sqlite: list[str] = []
        clauses_pg: list[str] = []
        params: list[Any] = []
        for column_name, raw_value in (
            ("run_id", run_id),
            ("asset_id", asset_id),
            ("person_identity_key", person_identity_key),
            ("record_id", record_id),
            ("signal_kind", signal_kind),
        ):
            value = str(raw_value or "").strip()
            if not value:
                continue
            clauses_sqlite.append(f"{column_name} = ?")
            clauses_pg.append(f"{column_name} = %s")
            params.append(value)
        postgres_rows = self._select_control_plane_rows(
            "person_public_web_signals",
            row_builder=self._person_public_web_signal_from_row,
            where_sql=" AND ".join(clauses_pg),
            params=params,
            order_by_sql=(
                "updated_at DESC, "
                "CASE signal_kind WHEN 'email_candidate' THEN 0 WHEN 'profile_link' THEN 1 ELSE 2 END, "
                "confidence_score DESC, signal_id DESC"
            ),
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_target_candidate_public_web_promotion(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_legacy_target_public_web_migration_write("target_candidate_public_web_promotions")
        normalized = _normalize_target_candidate_public_web_promotion_payload(payload)
        existing = self.get_target_candidate_public_web_promotion(normalized["promotion_id"])
        now = _utc_now_timestamp()
        row_payload = _target_candidate_public_web_promotion_row_payload(normalized, existing=existing, now=now)
        if self._write_control_plane_row_to_postgres("target_candidate_public_web_promotions", row_payload):
            return self.get_target_candidate_public_web_promotion(
                normalized["promotion_id"]
            ) or self._target_candidate_public_web_promotion_from_row(row_payload)
        self._raise_control_plane_postgres_write_failure(
            table_name="target_candidate_public_web_promotions",
            method_name="upsert_target_candidate_public_web_promotion",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_target_candidate_public_web_promotion(self, promotion_id: str) -> dict[str, Any] | None:
        normalized_promotion_id = str(promotion_id or "").strip()
        if not normalized_promotion_id:
            return None
        postgres_row = self._select_control_plane_row(
            "target_candidate_public_web_promotions",
            row_builder=self._target_candidate_public_web_promotion_from_row,
            where_sql="promotion_id = %s",
            params=[normalized_promotion_id],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_target_candidate_public_web_promotions(
        self,
        *,
        record_id: str = "",
        signal_id: str = "",
        run_id: str = "",
        action: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        clauses_sqlite: list[str] = []
        clauses_pg: list[str] = []
        params: list[Any] = []
        for column_name, raw_value in (
            ("record_id", record_id),
            ("signal_id", signal_id),
            ("run_id", run_id),
        ):
            value = str(raw_value or "").strip()
            if not value:
                continue
            clauses_sqlite.append(f"{column_name} = ?")
            clauses_pg.append(f"{column_name} = %s")
            params.append(value)
        normalized_action = _normalize_target_candidate_public_web_promotion_action(action, default="")
        if normalized_action:
            clauses_sqlite.append("action = ?")
            clauses_pg.append("action = %s")
            params.append(normalized_action)
        postgres_rows = self._select_control_plane_rows(
            "target_candidate_public_web_promotions",
            row_builder=self._target_candidate_public_web_promotion_from_row,
            where_sql=" AND ".join(clauses_pg),
            params=params,
            order_by_sql="updated_at DESC, created_at DESC, promotion_id DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_crm_public_web_promotion(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = _normalize_crm_public_web_promotion_payload(payload)
        existing = self.get_crm_public_web_promotion(normalized["promotion_id"])
        now = _utc_now_timestamp()
        row_payload = _crm_public_web_promotion_row_payload(normalized, existing=existing, now=now)
        if self._write_control_plane_row_to_postgres("crm_public_web_promotions", row_payload):
            return self.get_crm_public_web_promotion(
                normalized["promotion_id"]
            ) or self._crm_public_web_promotion_from_row(row_payload)
        self._raise_control_plane_postgres_write_failure(
            table_name="crm_public_web_promotions",
            method_name="upsert_crm_public_web_promotion",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def upsert_crm_public_web_promotion_if_owned(
        self,
        payload: dict[str, Any],
        *,
        crm_record_id: str,
        expected_workspace_id: str,
        expected_owner_user_id: str,
    ) -> dict[str, Any]:
        normalized = _normalize_crm_public_web_promotion_payload(payload)
        row_payload = _crm_public_web_promotion_row_payload(
            normalized,
            # The authoritative existing-row read belongs inside the PG owner
            # transaction. Reading it here would reopen the promotion-id race
            # that this method is intended to close.
            existing=None,
            now=_utc_now_timestamp(),
        )
        result = self._call_control_plane_postgres_native(
            "upsert_crm_public_web_promotion_if_owned",
            row=row_payload,
            crm_record_id=str(crm_record_id or "").strip(),
            expected_workspace_id=str(expected_workspace_id or "").strip(),
            expected_owner_user_id=str(expected_owner_user_id or "").strip(),
        )
        if not isinstance(result, dict):
            self._raise_control_plane_postgres_write_failure(
                table_name="crm_public_web_promotions",
                method_name="upsert_crm_public_web_promotion_if_owned",
                reason="postgres-only: owner-fenced CRM promotion returned no confirmation",
            )
        if result.get("status") != "applied":
            return dict(result)
        return self._crm_public_web_promotion_from_row(dict(result.get("row") or {}))

    def get_crm_public_web_promotion(self, promotion_id: str) -> dict[str, Any] | None:
        normalized_promotion_id = str(promotion_id or "").strip()
        if not normalized_promotion_id:
            return None
        postgres_row = self._select_control_plane_row(
            "crm_public_web_promotions",
            row_builder=self._crm_public_web_promotion_from_row,
            where_sql="promotion_id = %s",
            params=[normalized_promotion_id],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_crm_public_web_promotions(
        self,
        *,
        crm_record_id: str = "",
        workspace_id: str = "default",
        signal_id: str = "",
        run_id: str = "",
        action: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        clauses_sqlite: list[str] = []
        clauses_pg: list[str] = []
        params: list[Any] = []
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        if crm_record_id:
            clauses_sqlite.append("workspace_id = ?")
            clauses_pg.append("workspace_id = %s")
            params.append(normalized_workspace_id)
        for column_name, raw_value in (
            ("crm_record_id", crm_record_id),
            ("signal_id", signal_id),
            ("run_id", run_id),
        ):
            value = str(raw_value or "").strip()
            if not value:
                continue
            clauses_sqlite.append(f"{column_name} = ?")
            clauses_pg.append(f"{column_name} = %s")
            params.append(value)
        normalized_action = _normalize_target_candidate_public_web_promotion_action(action, default="")
        if normalized_action:
            clauses_sqlite.append("action = ?")
            clauses_pg.append("action = %s")
            params.append(normalized_action)
        postgres_rows = self._select_control_plane_rows(
            "crm_public_web_promotions",
            row_builder=self._crm_public_web_promotion_from_row,
            where_sql=" AND ".join(clauses_pg),
            params=params,
            order_by_sql="updated_at DESC, created_at DESC, promotion_id DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def get_asset_default_pointer(
        self,
        *,
        pointer_key: str = "",
        company_key: str = "",
        scope_kind: str = "company",
        scope_key: str = "",
        asset_kind: str = "company_asset",
    ) -> dict[str, Any] | None:
        normalized_pointer_key = str(pointer_key or "").strip()
        if not normalized_pointer_key:
            normalized_company_key = normalize_company_key(company_key)
            if not normalized_company_key:
                return None
            normalized_pointer_key = default_asset_pointer_key(
                company_key=normalized_company_key,
                scope_kind=scope_kind,
                scope_key=scope_key or normalized_company_key,
                asset_kind=asset_kind,
            )
        postgres_row = self._select_control_plane_row(
            "asset_default_pointers",
            row_builder=self._asset_default_pointer_from_row,
            where_sql="pointer_key = %s",
            params=[normalized_pointer_key],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_asset_default_pointers(
        self,
        *,
        company_key: str = "",
        scope_kind: str = "",
        asset_kind: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        normalized_company_key = normalize_company_key(company_key) if company_key else ""
        if normalized_company_key:
            clauses.append("company_key = ?")
            params.append(normalized_company_key)
        if scope_kind:
            clauses.append("scope_kind = ?")
            params.append(str(scope_kind or "").strip().lower())
        if asset_kind:
            clauses.append("asset_kind = ?")
            params.append(str(asset_kind or "").strip().lower())
        postgres_rows = self._select_control_plane_rows(
            "asset_default_pointers",
            row_builder=self._asset_default_pointer_from_row,
            where_sql=" AND ".join(
                [
                    *(["company_key = %s"] if normalized_company_key else []),
                    *(["scope_kind = %s"] if scope_kind else []),
                    *(["asset_kind = %s"] if asset_kind else []),
                ]
            ),
            params=[
                *([normalized_company_key] if normalized_company_key else []),
                *([str(scope_kind or "").strip().lower()] if scope_kind else []),
                *([str(asset_kind or "").strip().lower()] if asset_kind else []),
            ],
            order_by_sql="updated_at DESC, pointer_key ASC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def list_asset_default_pointer_history(
        self,
        *,
        pointer_key: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_pointer_key = str(pointer_key or "").strip()
        if not normalized_pointer_key:
            return []
        postgres_rows = self._select_control_plane_rows(
            "asset_default_pointer_history",
            row_builder=self._asset_default_pointer_history_from_row,
            where_sql="pointer_key = %s",
            params=[normalized_pointer_key],
            order_by_sql="occurred_at DESC, created_at DESC, history_id DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def promote_asset_default_pointer(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize_default_asset_pointer_payload(payload)
        if not normalized["company_key"]:
            return {"status": "rejected", "reason": "missing_company_key"}
        current_pointer = self.get_asset_default_pointer(pointer_key=normalized["pointer_key"])
        plan = build_canonical_asset_replacement_plan(
            current_pointer=current_pointer,
            company_key=normalized["company_key"],
            snapshot_id=normalized["snapshot_id"],
            scope_kind=normalized["scope_kind"],
            scope_key=normalized["scope_key"],
            asset_kind=normalized["asset_kind"],
            lifecycle_status=normalized["lifecycle_status"],
            coverage_proof=normalized["coverage_proof"],
            promoted_by_job_id=normalized["promoted_by_job_id"],
        )
        if str(plan.get("status") or "") != "promoted":
            return plan
        default_pointer = {
            **dict(plan.get("default_pointer") or {}),
            "metadata": normalized["metadata"],
        }
        stored_pointer = self._upsert_asset_default_pointer(default_pointer)
        occurred_at = str(default_pointer.get("promoted_at") or _utc_now_timestamp())
        self._append_asset_default_pointer_history(
            pointer=stored_pointer,
            event_type="promoted",
            occurred_at=occurred_at,
            payload={
                "replacement_plan": plan,
                "metadata": normalized["metadata"],
            },
        )
        superseded_pointer = dict(plan.get("superseded_pointer") or {})
        if superseded_pointer:
            self._append_asset_default_pointer_history(
                pointer=superseded_pointer,
                event_type="superseded",
                occurred_at=occurred_at,
                payload={
                    "superseded_by_snapshot_id": str(default_pointer.get("snapshot_id") or ""),
                    "replacement_plan": plan,
                },
            )
        return {
            **plan,
            "default_pointer": stored_pointer,
            "history": self.list_asset_default_pointer_history(pointer_key=normalized["pointer_key"], limit=20),
        }

    def _upsert_asset_default_pointer(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize_default_asset_pointer_payload(payload)
        now = _utc_now_timestamp()
        row_payload = {
            "pointer_key": normalized["pointer_key"],
            "company_key": normalized["company_key"],
            "scope_kind": normalized["scope_kind"],
            "scope_key": normalized["scope_key"],
            "asset_kind": normalized["asset_kind"],
            "snapshot_id": normalized["snapshot_id"],
            "lifecycle_status": normalized["lifecycle_status"],
            "coverage_proof_json": json.dumps(_json_safe_payload(normalized["coverage_proof"]), ensure_ascii=False),
            "previous_snapshot_id": normalized["previous_snapshot_id"],
            "promoted_by_job_id": normalized["promoted_by_job_id"],
            "promoted_at": normalized["promoted_at"] or now,
            "metadata_json": json.dumps(_json_safe_payload(normalized["metadata"]), ensure_ascii=False),
            "created_at": str(
                (self.get_asset_default_pointer(pointer_key=normalized["pointer_key"]) or {}).get("created_at") or now
            ),
            "updated_at": now,
        }
        if self._write_control_plane_row_to_postgres("asset_default_pointers", row_payload):
            return self.get_asset_default_pointer(
                pointer_key=normalized["pointer_key"]
            ) or self._asset_default_pointer_from_row(row_payload)
        self._raise_control_plane_postgres_write_failure(
            table_name="asset_default_pointers",
            method_name="_upsert_asset_default_pointer",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def _append_asset_default_pointer_history(
        self,
        *,
        pointer: dict[str, Any],
        event_type: str,
        occurred_at: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_pointer = normalize_default_asset_pointer_payload(pointer)
        normalized_event_type = str(event_type or "").strip().lower() or "updated"
        normalized_occurred_at = str(occurred_at or "").strip() or _utc_now_timestamp()
        row_payload = {
            "history_id": default_asset_pointer_history_id(
                pointer_key=normalized_pointer["pointer_key"],
                snapshot_id=normalized_pointer["snapshot_id"],
                event_type=normalized_event_type,
                occurred_at=normalized_occurred_at,
            ),
            "pointer_key": normalized_pointer["pointer_key"],
            "company_key": normalized_pointer["company_key"],
            "scope_kind": normalized_pointer["scope_kind"],
            "scope_key": normalized_pointer["scope_key"],
            "asset_kind": normalized_pointer["asset_kind"],
            "snapshot_id": normalized_pointer["snapshot_id"],
            "lifecycle_status": normalized_pointer["lifecycle_status"],
            "event_type": normalized_event_type,
            "payload_json": json.dumps(_json_safe_payload(payload), ensure_ascii=False),
            "occurred_at": normalized_occurred_at,
            "created_at": _utc_now_timestamp(),
        }
        if self._write_control_plane_row_to_postgres("asset_default_pointer_history", row_payload):
            return self._asset_default_pointer_history_from_row(row_payload)
        self._raise_control_plane_postgres_write_failure(
            table_name="asset_default_pointer_history",
            method_name="_append_asset_default_pointer_history",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_frontend_history_link(self, history_id: str) -> dict[str, Any] | None:
        normalized_history_id = str(history_id or "").strip()
        if not normalized_history_id:
            return None
        postgres_row = self._select_control_plane_row(
            "frontend_history_links",
            row_builder=self._frontend_history_link_from_row,
            where_sql="history_id = %s",
            params=[normalized_history_id],
        )
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # fallback below is dead and removed.
        if postgres_row is None:
            return None
        return postgres_row

    def list_frontend_history_links(
        self,
        *,
        limit: int = 24,
    ) -> list[dict[str, Any]]:
        postgres_rows = self._select_control_plane_rows(
            "frontend_history_links",
            row_builder=self._frontend_history_link_from_row,
            order_by_sql="updated_at DESC, created_at DESC, history_id DESC",
            limit=limit,
        )
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # fallback below is dead and removed.
        if not postgres_rows:
            return []
        return postgres_rows

    def list_frontend_history_links_for_job(
        self,
        job_id: str,
        *,
        limit: int = 24,
    ) -> list[dict[str, Any]]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return []
        postgres_rows = self._select_control_plane_rows(
            "frontend_history_links",
            row_builder=self._frontend_history_link_from_row,
            where_sql="job_id = %s",
            params=[normalized_job_id],
            order_by_sql="updated_at DESC, created_at DESC, history_id DESC",
            limit=limit,
        )
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # fallback below is dead and removed.
        if not postgres_rows:
            return []
        return postgres_rows

    def list_frontend_history_links_for_review(
        self,
        review_id: int,
        *,
        limit: int = 24,
    ) -> list[dict[str, Any]]:
        normalized_review_id = int(review_id or 0)
        if normalized_review_id <= 0:
            return []
        postgres_rows = self._select_control_plane_rows(
            "frontend_history_links",
            row_builder=self._frontend_history_link_from_row,
            where_sql="review_id = %s",
            params=[normalized_review_id],
            order_by_sql="updated_at DESC, created_at DESC, history_id DESC",
            limit=limit,
        )
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # fallback below is dead and removed.
        if not postgres_rows:
            return []
        return postgres_rows

    def upsert_frontend_history_link(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = _normalize_frontend_history_link_payload(payload)
        if not normalized["history_id"]:
            return normalized
        existing = self.get_frontend_history_link(normalized["history_id"])
        merged_metadata = dict(existing.get("metadata") or {}) if existing else {}
        merged_metadata.update(dict(normalized.get("metadata") or {}))
        request_payload = dict(existing.get("request") or {}) if existing else {}
        if normalized["request"]:
            request_payload = dict(normalized["request"])
        plan_payload = dict(existing.get("plan") or {}) if existing else {}
        if normalized["plan"]:
            plan_payload = dict(normalized["plan"])
        now = _utc_now_timestamp()
        row_payload = {
            "history_id": normalized["history_id"],
            "query_text": normalized["query_text"] or str((existing or {}).get("query_text") or ""),
            "target_company": normalized["target_company"] or str((existing or {}).get("target_company") or ""),
            "review_id": normalized["review_id"]
            if int(normalized["review_id"] or 0) > 0
            else int((existing or {}).get("review_id") or 0),
            "job_id": normalized["job_id"] or str((existing or {}).get("job_id") or ""),
            "phase": normalized["phase"] or str((existing or {}).get("phase") or ""),
            "request_json": json.dumps(_json_safe_payload(request_payload), ensure_ascii=False),
            "plan_json": json.dumps(_json_safe_payload(plan_payload), ensure_ascii=False),
            "metadata_json": json.dumps(_json_safe_payload(merged_metadata), ensure_ascii=False),
            "created_at": (
                str((existing or {}).get("created_at") or "").strip()
                or str(normalized.get("created_at") or "").strip()
                or now
            ),
            "updated_at": now,
        }
        if self._write_control_plane_row_to_postgres("frontend_history_links", row_payload):
            return (
                self.get_frontend_history_link(normalized["history_id"])
                or self._frontend_history_link_from_row(row_payload)
                or normalized
            )
        self._raise_control_plane_postgres_write_failure(
            table_name="frontend_history_links",
            method_name="upsert_frontend_history_link",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def delete_frontend_history_link(self, history_id: str) -> bool:
        normalized_history_id = str(history_id or "").strip()
        if not normalized_history_id:
            return False
        if self._control_plane_postgres_should_prefer_read("frontend_history_links"):
            try:
                deleted_count = self._control_plane_postgres.delete_rows(
                    table_name="frontend_history_links",
                    where_sql="history_id = %s",
                    params=[normalized_history_id],
                )
            except Exception as exc:
                if self._control_plane_postgres_should_skip_sqlite_fallback("frontend_history_links"):
                    self._raise_control_plane_postgres_write_failure(
                        table_name="frontend_history_links",
                        method_name="delete_rows",
                        reason=f"{type(exc).__name__}: {exc}",
                        error=exc,
                    )
                deleted_count = 0
            if deleted_count or self._control_plane_postgres_should_skip_sqlite_fallback("frontend_history_links"):
                return bool(deleted_count)
        raise RuntimeError(
            "postgres-only invariant violated for frontend_history_links in delete_frontend_history_link: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def resolve_frontend_history_link(self, history_id: str) -> dict[str, Any] | None:
        normalized_history_id = str(history_id or "").strip()
        if not normalized_history_id:
            return None
        direct = self.get_frontend_history_link(normalized_history_id)
        source = "frontend_history_links" if direct else ""
        fallback_candidate: dict[str, Any] | None = None
        fallback_target: dict[str, Any] | None = None
        direct_request_payload = dict((direct or {}).get("request") or {})
        direct_plan_payload = dict((direct or {}).get("plan") or {})
        direct_phase = str((direct or {}).get("phase") or "").strip().lower()
        direct_review_id = int((direct or {}).get("review_id") or 0)
        direct_has_recovery_context = bool(
            str((direct or {}).get("job_id") or "").strip()
            or direct_review_id > 0
            or direct_phase == "plan"
            or direct_request_payload
            or direct_plan_payload
        )
        if not direct_has_recovery_context:
            candidate_records = self.list_candidate_review_records(history_id=normalized_history_id, limit=500)
            target_records = self.list_target_candidates(history_id=normalized_history_id, limit=500)
            candidate_row = next(
                (record for record in candidate_records if str(record.get("job_id") or "").strip()),
                None,
            )
            target_row = next(
                (record for record in target_records if str(record.get("job_id") or "").strip()),
                None,
            )
            if candidate_row is not None:
                fallback_candidate = {
                    "job_id": str(candidate_row.get("job_id") or "").strip(),
                    "added_at": str(candidate_row.get("added_at") or ""),
                    "updated_at": str(candidate_row.get("updated_at") or ""),
                    "source": "candidate_review_registry",
                }
            if target_row is not None:
                fallback_target = {
                    "job_id": str(target_row.get("job_id") or "").strip(),
                    "added_at": str(target_row.get("added_at") or ""),
                    "updated_at": str(target_row.get("updated_at") or ""),
                    "source": "target_candidates",
                }
        fallback = fallback_candidate
        if fallback_target and (
            fallback is None or str(fallback_target.get("updated_at") or "") >= str(fallback.get("updated_at") or "")
        ):
            fallback = fallback_target
        if direct is None and fallback is None:
            return None
        resolved = dict(direct or {})
        if fallback and not str(resolved.get("job_id") or "").strip():
            resolved["job_id"] = str(fallback.get("job_id") or "")
            resolved["updated_at"] = str(fallback.get("updated_at") or resolved.get("updated_at") or "")
            resolved["created_at"] = str(resolved.get("created_at") or fallback.get("added_at") or "")
            source = str(fallback.get("source") or source)
        resolved["history_id"] = normalized_history_id
        resolved["source"] = source or "frontend_history_links"
        return resolved

    def create_agent_runtime_session(
        self,
        *,
        job_id: str,
        target_company: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        lanes: list[dict[str, Any]],
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        canonical_request = JobRequest.from_payload(request_payload).to_record()
        if self._control_plane_postgres_should_prefer_read("agent_runtime_sessions"):
            row = self._call_control_plane_postgres_native(
                "create_agent_runtime_session_row",
                row={
                    "job_id": job_id,
                    "target_company": target_company,
                    "request_signature": request_signature(canonical_request),
                    "request_family_signature": request_family_signature(canonical_request),
                    "runtime_mode": runtime_mode,
                    "status": "running",
                    "lanes_json": json.dumps(lanes, ensure_ascii=False),
                    "metadata_json": json.dumps(metadata or {"plan": plan_payload}, ensure_ascii=False),
                },
            )
            if row is not None:
                return self._agent_runtime_session_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("agent_runtime_sessions"):
                self._raise_control_plane_postgres_write_failure(
                    table_name="agent_runtime_sessions",
                    method_name="create_agent_runtime_session_row",
                    reason="native writer returned no row",
                )
        raise RuntimeError(
            "postgres-only invariant violated for agent_runtime_sessions in create_agent_runtime_session: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_agent_runtime_session(self, *, job_id: str = "", session_id: int = 0) -> dict[str, Any] | None:
        if session_id > 0:
            postgres_row = self._select_control_plane_row(
                "agent_runtime_sessions",
                row_builder=self._agent_runtime_session_from_row,
                where_sql="session_id = %s",
                params=[session_id],
            )
            if postgres_row is not None:
                return postgres_row
            if self._control_plane_postgres_should_skip_sqlite_fallback("agent_runtime_sessions"):
                return None
        elif job_id:
            postgres_row = self._select_control_plane_row(
                "agent_runtime_sessions",
                row_builder=self._agent_runtime_session_from_row,
                where_sql="job_id = %s",
                params=[job_id],
            )
            if postgres_row is not None:
                return postgres_row
            if self._control_plane_postgres_should_skip_sqlite_fallback("agent_runtime_sessions"):
                return None
        if session_id > 0 or job_id:
            raise RuntimeError(
                "postgres-only invariant violated for agent_runtime_sessions in get_agent_runtime_session: should_prefer_read "
                "returned False; legacy SQLite tail retired (B4)"
            )
        return None

    def create_agent_trace_span(
        self,
        *,
        session_id: int,
        job_id: str,
        lane_id: str,
        span_name: str,
        stage: str,
        parent_span_id: int = 0,
        handoff_from_lane: str = "",
        handoff_to_lane: str = "",
        input_payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._control_plane_postgres_should_prefer_read("agent_trace_spans"):
            row = self._call_control_plane_postgres_native(
                "create_agent_trace_span",
                session_id=session_id,
                job_id=job_id,
                lane_id=lane_id,
                span_name=span_name,
                stage=stage,
                parent_span_id=parent_span_id,
                handoff_from_lane=handoff_from_lane,
                handoff_to_lane=handoff_to_lane,
                input_payload=input_payload or {},
                metadata=metadata or {},
            )
            if row is None:
                raise RuntimeError(f"Failed to create agent trace span for {job_id}:{lane_id}:{span_name}")
            return self._agent_trace_span_from_row(row)
        raise RuntimeError(
            "postgres-only invariant violated for agent_trace_spans in create_agent_trace_span: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def update_agent_runtime_session_status(self, job_id: str, status: str) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_runtime_sessions"):
            row = self._call_control_plane_postgres_native(
                "update_agent_runtime_session_status",
                str(job_id),
                str(status),
            )
            if row is not None:
                return self._agent_runtime_session_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("agent_runtime_sessions"):
                if self.get_agent_runtime_session(job_id=job_id) is None:
                    # Parity with the SQLite fallback below: updating a session
                    # that does not exist (legacy/recovered jobs) is a no-op,
                    # not a write failure.
                    return None
                self._raise_control_plane_postgres_write_failure(
                    table_name="agent_runtime_sessions",
                    method_name="update_agent_runtime_session_status",
                    reason="native writer returned no row",
                )
        raise RuntimeError(
            "postgres-only invariant violated for agent_runtime_sessions in update_agent_runtime_session_status: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def complete_agent_trace_span(
        self,
        span_id: int,
        *,
        status: str,
        output_payload: dict[str, Any] | None = None,
        handoff_to_lane: str = "",
    ) -> dict[str, Any]:
        if self._control_plane_postgres_should_prefer_read("agent_trace_spans"):
            row = self._call_control_plane_postgres_native(
                "update_agent_trace_span",
                span_id,
                status=status,
                output_payload=output_payload or {},
                handoff_to_lane=handoff_to_lane,
            )
            if row is None:
                raise RuntimeError(f"Failed to complete agent trace span {span_id}")
            return self._agent_trace_span_from_row(row)
        raise RuntimeError(
            "postgres-only invariant violated for agent_trace_spans in complete_agent_trace_span: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_agent_trace_span(self, span_id: int) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_trace_spans"):
            row = self._call_control_plane_postgres_native("get_agent_trace_span", span_id)
            return self._agent_trace_span_from_row(row) if row is not None else None
        raise RuntimeError(
            "postgres-only invariant violated for agent_trace_spans in get_agent_trace_span: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_agent_trace_spans(self, *, job_id: str = "", session_id: int = 0) -> list[dict[str, Any]]:
        if self._control_plane_postgres_should_prefer_read("agent_trace_spans"):
            rows = self._call_control_plane_postgres_native(
                "list_agent_trace_spans",
                job_id=job_id,
                session_id=session_id,
            )
            return [self._agent_trace_span_from_row(row) for row in list(rows or [])]
        raise RuntimeError(
            "postgres-only invariant violated for agent_trace_spans in list_agent_trace_spans: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def create_or_resume_agent_worker(
        self,
        *,
        session_id: int,
        job_id: str,
        span_id: int,
        lane_id: str,
        worker_key: str,
        budget_payload: dict[str, Any] | None = None,
        input_payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native(
                "create_or_resume_agent_worker",
                session_id=session_id,
                job_id=job_id,
                span_id=span_id,
                lane_id=lane_id,
                worker_key=worker_key,
                budget_payload=budget_payload or {},
                input_payload=input_payload or {},
                metadata=metadata or {},
            )
            if row is None:
                raise RuntimeError(f"Failed to create agent worker {job_id}:{lane_id}:{worker_key}")
            return self._agent_worker_from_row(row)
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in create_or_resume_agent_worker: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def mark_agent_worker_running(self, worker_id: int) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native("mark_agent_worker_running", worker_id)
            return self._agent_worker_from_row(row) if row is not None else None
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in mark_agent_worker_running: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def checkpoint_agent_worker(
        self,
        worker_id: int,
        *,
        checkpoint_payload: dict[str, Any] | None = None,
        output_payload: dict[str, Any] | None = None,
        status: str = "running",
    ) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native(
                "checkpoint_agent_worker",
                worker_id,
                checkpoint_payload=checkpoint_payload or {},
                output_payload=output_payload or {},
                status=status,
            )
            return self._agent_worker_from_row(row) if row is not None else None
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in checkpoint_agent_worker: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def complete_agent_worker(
        self,
        worker_id: int,
        *,
        status: str,
        checkpoint_payload: dict[str, Any] | None = None,
        output_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native(
                "complete_agent_worker",
                worker_id,
                status=status,
                checkpoint_payload=checkpoint_payload or {},
                output_payload=output_payload or {},
            )
            return self._agent_worker_from_row(row) if row is not None else None
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in complete_agent_worker: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_recoverable_agent_workers(
        self,
        *,
        limit: int = 100,
        stale_after_seconds: int = 300,
        lane_id: str = "",
        job_id: str = "",
    ) -> list[dict[str, Any]]:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            rows = self._call_control_plane_postgres_native(
                "list_recoverable_agent_workers",
                limit=limit,
                stale_after_seconds=stale_after_seconds,
                lane_id=lane_id,
                job_id=job_id,
            )
            return [self._agent_worker_from_row(row) for row in list(rows or [])]
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in list_recoverable_agent_workers: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def retire_agent_workers(
        self,
        *,
        worker_ids: list[int],
        status: str = "cancelled",
        reason: str = "",
        cleanup_metadata: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            rows = self._call_control_plane_postgres_native(
                "retire_agent_workers",
                worker_ids=worker_ids,
                status=status,
                reason=reason,
                cleanup_metadata=cleanup_metadata or {},
            )
            return [self._agent_worker_from_row(row) for row in list(rows or [])]
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in retire_agent_workers: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def claim_agent_worker(
        self,
        worker_id: int,
        *,
        lease_owner: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native(
                "claim_agent_worker",
                worker_id,
                lease_owner=lease_owner,
                lease_seconds=lease_seconds,
            )
            return self._agent_worker_from_row(row) if row is not None else None
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in claim_agent_worker: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def acquire_workflow_job_lease(
        self,
        job_id: str,
        *,
        lease_owner: str,
        lease_seconds: int = 900,
        lease_token: str = "",
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        if not normalized_job_id or not normalized_owner:
            return {
                "job_id": normalized_job_id,
                "lease_owner": "",
                "lease_token": "",
                "lease_expires_at": "",
                "expired": True,
                "acquired": False,
            }
        normalized_token = (
            str(lease_token or "").strip()
            or sha1(f"{normalized_job_id}:{normalized_owner}".encode("utf-8")).hexdigest()
        )
        if self._control_plane_postgres_should_prefer_read("workflow_job_leases"):
            row = self._call_control_plane_postgres_native(
                "acquire_workflow_job_lease",
                normalized_job_id,
                lease_owner=normalized_owner,
                lease_seconds=lease_seconds,
                lease_token=normalized_token,
            )
            payload = self._workflow_job_lease_from_row(row)
            return {
                **payload,
                "acquired": bool(
                    payload
                    and str(payload.get("lease_owner") or "") == normalized_owner
                    and str(payload.get("lease_token") or "") == normalized_token
                ),
            }
        raise RuntimeError(
            "postgres-only invariant violated for workflow_job_leases in acquire_workflow_job_lease: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_workflow_job_lease(self, job_id: str) -> dict[str, Any] | None:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return None
        if self._control_plane_postgres_should_prefer_read("workflow_job_leases"):
            row = self._call_control_plane_postgres_native("get_workflow_job_lease", normalized_job_id)
            return self._workflow_job_lease_from_row(row) if row is not None else None
        raise RuntimeError(
            "postgres-only invariant violated for workflow_job_leases in get_workflow_job_lease: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def renew_workflow_job_lease(
        self,
        job_id: str,
        *,
        lease_owner: str,
        lease_seconds: int,
        lease_token: str = "",
    ) -> dict[str, Any] | None:
        normalized_job_id = str(job_id or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        normalized_token = str(lease_token or "").strip()
        if not normalized_job_id or not normalized_owner:
            return None
        if self._control_plane_postgres_should_prefer_read("workflow_job_leases"):
            row = self._call_control_plane_postgres_native(
                "renew_workflow_job_lease",
                normalized_job_id,
                lease_owner=normalized_owner,
                lease_seconds=lease_seconds,
                lease_token=normalized_token,
            )
            if row is not None:
                return self._workflow_job_lease_from_row(row)
            return None
        raise RuntimeError(
            "postgres-only invariant violated for workflow_job_leases in renew_workflow_job_lease: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def release_workflow_job_lease(
        self,
        job_id: str,
        *,
        lease_owner: str = "",
        lease_token: str = "",
    ) -> None:
        normalized_job_id = str(job_id or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        normalized_token = str(lease_token or "").strip()
        if not normalized_job_id:
            return
        if self._control_plane_postgres_should_prefer_read("workflow_job_leases"):
            self._call_control_plane_postgres_native(
                "release_workflow_job_lease",
                normalized_job_id,
                lease_owner=normalized_owner,
                lease_token=normalized_token,
            )
            return
        raise RuntimeError(
            "postgres-only invariant violated for workflow_job_leases in release_workflow_job_lease: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def upsert_workflow_command(
        self,
        *,
        workflow_run_id: str,
        command_type: str,
        owner: str,
        idempotency_key: str,
        command_id: str = "",
        operation_id: str = "",
        payload: dict[str, Any] | None = None,
        artifact_refs: list[Any] | tuple[Any, ...] | None = None,
        not_before_at: str = "",
        max_attempts: int = 5,
        retry_policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_run_id = str(workflow_run_id or "").strip()
        normalized_type = str(command_type or "").strip()
        normalized_owner = str(owner or "").strip()
        normalized_idempotency = str(idempotency_key or "").strip()
        if not normalized_run_id or not normalized_type or not normalized_owner or not normalized_idempotency:
            return {}
        now = _utc_now_timestamp()
        normalized_command_id = str(command_id or "").strip() or (
            "cmd_" + sha1(f"{normalized_run_id}:{normalized_idempotency}".encode("utf-8")).hexdigest()[:24]
        )
        safe_payload = _json_safe_payload(payload or {})
        causality_columns = _workflow_command_causality_columns_from_payload(
            safe_payload,
            workflow_run_id=normalized_run_id,
            operation_id=str(operation_id or "").strip(),
            command_type=normalized_type,
            owner=normalized_owner,
            idempotency_key=normalized_idempotency,
        )
        row_payload = {
            "command_id": normalized_command_id,
            "workflow_run_id": normalized_run_id,
            "operation_id": str(operation_id or "").strip(),
            "command_type": normalized_type,
            "owner": normalized_owner,
            **causality_columns,
            "status": "queued",
            "idempotency_key": normalized_idempotency,
            "payload_json": json.dumps(safe_payload, ensure_ascii=False),
            "artifact_refs_json": json.dumps(_json_safe_payload(list(artifact_refs or [])), ensure_ascii=False),
            "not_before_at": str(not_before_at or "").strip(),
            "attempt": 0,
            "max_attempts": max(1, int(max_attempts or 5)),
            "retry_policy_json": json.dumps(_json_safe_payload(retry_policy or {}), ensure_ascii=False),
            "lease_owner": "",
            "lease_expires_at": "",
            "heartbeat_at": "",
            "last_error": "",
            "result_json": "{}",
            "schema_version": "workflow_command_v1",
            "created_at": now,
            "updated_at": now,
        }
        if self._control_plane_postgres_should_prefer_read("workflow_commands"):
            row = self._call_control_plane_postgres_native("upsert_workflow_command", row_payload)
            if row is not None:
                return self._workflow_command_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("workflow_commands"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for workflow_commands in upsert_workflow_command: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_workflow_command(self, command_id: str) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return {}
        postgres_row = self._select_control_plane_row(
            "workflow_commands",
            row_builder=self._workflow_command_from_row,
            where_sql="command_id = %s",
            params=[normalized_command_id],
        )
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # fallback below is dead and removed.
        if postgres_row is None:
            return {}
        return postgres_row

    def list_workflow_commands_by_ids(
        self,
        command_ids: list[str] | tuple[str, ...],
    ) -> list[dict[str, Any]]:
        """Fetch one bounded public page of WorkflowCommand identities with one authoritative query."""

        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_ids = _workflow_runtime_repo.normalize_workflow_evidence_batch_ids(
            command_ids,
            identity_name="command_ids",
        )
        if not normalized_ids:
            return []
        rows = self._select_control_plane_rows(
            "workflow_commands",
            row_builder=self._workflow_command_from_row,
            where_sql="command_id IN (" + ", ".join(["%s"] * len(normalized_ids)) + ")",
            params=normalized_ids,
            limit=len(normalized_ids),
        )
        requested_ids = set(normalized_ids)
        rows_by_id = {
            str(row.get("command_id") or "").strip(): row
            for row in rows
            if str(row.get("command_id") or "").strip() in requested_ids
        }
        return [rows_by_id[command_id] for command_id in normalized_ids if command_id in rows_by_id]

    def update_workflow_command_payload(
        self,
        command_id: str,
        *,
        payload: dict[str, Any] | None = None,
        artifact_refs: list[Any] | tuple[Any, ...] | None = None,
        not_before_at: str | None = None,
        result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return {}
        existing = self.get_workflow_command(normalized_command_id)
        if not existing:
            return {}
        next_payload = dict(payload if payload is not None else existing.get("payload") or {})
        next_artifact_refs = list(
            artifact_refs if artifact_refs is not None else list(existing.get("artifact_refs") or [])
        )
        next_not_before = (
            str(not_before_at or "").strip()
            if not_before_at is not None
            else str(existing.get("not_before_at") or "").strip()
        )
        next_result = dict(result if result is not None else existing.get("result") or {})
        if self._control_plane_postgres_should_prefer_read("workflow_commands"):
            row = self._call_control_plane_postgres_native(
                "update_workflow_command_payload",
                normalized_command_id,
                payload=next_payload,
                artifact_refs=next_artifact_refs,
                not_before_at=next_not_before,
                result=next_result,
            )
            if row is not None:
                return self._workflow_command_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("workflow_commands"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for workflow_commands in update_workflow_command_payload: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_workflow_commands(
        self,
        *,
        workflow_run_id: str = "",
        operation_id: str = "",
        owner: str = "",
        statuses: list[str] | tuple[str, ...] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # fallback (and its now-unused SQLite-`?` clause/param builders) is dead and removed; only the
        # postgres %s variants remain.
        pg_clauses: list[str] = []
        pg_params: list[Any] = []
        normalized_run_id = str(workflow_run_id or "").strip()
        if normalized_run_id:
            pg_clauses.append("workflow_run_id = %s")
            pg_params.append(normalized_run_id)
        normalized_operation_id = str(operation_id or "").strip()
        if normalized_operation_id:
            pg_clauses.append("operation_id = %s")
            pg_params.append(normalized_operation_id)
        normalized_owner = str(owner or "").strip()
        if normalized_owner:
            pg_clauses.append("owner = %s")
            pg_params.append(normalized_owner)
        normalized_statuses = [
            str(status or "").strip() for status in list(statuses or []) if str(status or "").strip()
        ]
        if normalized_statuses:
            pg_placeholders = ", ".join(["%s"] * len(normalized_statuses))
            pg_clauses.append(f"status IN ({pg_placeholders})")
            pg_params.extend(normalized_statuses)
        pg_rows = self._select_control_plane_rows(
            "workflow_commands",
            row_builder=self._workflow_command_from_row,
            where_sql=" AND ".join(pg_clauses),
            params=pg_params,
            order_by_sql="updated_at ASC, created_at ASC",
            limit=max(0, int(limit or 0)),
        )
        if not pg_rows:
            return []
        return pg_rows

    def list_ready_workflow_commands(
        self,
        *,
        workflow_run_id: str = "",
        owner: str = "",
        command_type: str = "",
        limit: int = 100,
        reclaim_claimed: bool = False,
    ) -> list[dict[str, Any]]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_run_id = str(workflow_run_id or "").strip()
        normalized_owner = str(owner or "").strip()
        normalized_type = str(command_type or "").strip()
        now = _utc_now_timestamp()
        # reclaim_claimed (opt-in) additionally surfaces an expired-lease 'claimed'
        # row — a worker that crashed between claim_workflow_command and
        # mark_workflow_command_running. The trailing lease-expiry clause still
        # excludes still-active claims (mirroring how 'running' is reclaimed only
        # once its lease lapses). SCOPED to idempotent export commands until the
        # general ownership-fencing hardening (docs/DURABLE_COMMAND_OWNERSHIP_FENCING.md)
        # makes reclaim universally safe against a stalled original claimant
        # resuming; non-export callers keep the original queued/retry_wait/running set.
        ready_status_in = (
            "status IN ('queued', 'retry_wait', 'running', 'claimed')"
            if reclaim_claimed
            else "status IN ('queued', 'retry_wait', 'running')"
        )
        # Track B B3.2: PG is the sole authoritative control-plane store under postgres_only; the SQLite
        # fallback (and its now-unused SQLite-`?` clause/param builders, incl. the datetime() wrappers) is
        # dead and removed; only the postgres %s variants remain. ready_status_in is dialect-neutral.
        pg_clauses = [
            ready_status_in,
            "(not_before_at = '' OR not_before_at <= %s)",
            "(lease_expires_at = '' OR lease_expires_at <= %s)",
        ]
        pg_params: list[Any] = [now, now]
        if normalized_run_id:
            pg_clauses.append("workflow_run_id = %s")
            pg_params.append(normalized_run_id)
        if normalized_owner:
            pg_clauses.append("owner = %s")
            pg_params.append(normalized_owner)
        if normalized_type:
            pg_clauses.append("command_type = %s")
            pg_params.append(normalized_type)
        pg_rows = self._select_control_plane_rows(
            "workflow_commands",
            row_builder=self._workflow_command_from_row,
            where_sql=" AND ".join(pg_clauses),
            params=pg_params,
            order_by_sql="updated_at ASC, created_at ASC",
            limit=max(1, int(limit or 100)),
        )
        if not pg_rows:
            return []
        return pg_rows

    def claim_workflow_command(
        self,
        command_id: str,
        *,
        lease_owner: str,
        lease_seconds: int = 300,
        reclaim_claimed: bool = False,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_command_id = str(command_id or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        if not normalized_command_id or not normalized_owner:
            return {}
        if self._control_plane_postgres_should_prefer_read("workflow_commands"):
            row = self._call_control_plane_postgres_native(
                "claim_workflow_command",
                normalized_command_id,
                lease_owner=normalized_owner,
                lease_seconds=max(1, int(lease_seconds or 300)),
                reclaim_claimed=bool(reclaim_claimed),
            )
            return self._workflow_command_from_row(row) if row is not None else {}
        raise RuntimeError(
            "postgres-only invariant violated for workflow_commands in claim_workflow_command: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def mark_workflow_command_running(self, command_id: str, *, lease_owner: str = "") -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("workflow_commands"):
            row = self._call_control_plane_postgres_native(
                "mark_workflow_command_running",
                normalized_command_id,
                lease_owner=lease_owner,
            )
            return self._workflow_command_from_row(row) if row is not None else {}
        raise RuntimeError(
            "postgres-only invariant violated for workflow_commands in mark_workflow_command_running: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def mark_workflow_command_succeeded(
        self,
        command_id: str,
        *,
        result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("workflow_commands"):
            row = self._call_control_plane_postgres_native(
                "mark_workflow_command_succeeded",
                normalized_command_id,
                result=result or {},
            )
            return self._workflow_command_from_row(row) if row is not None else {}
        raise RuntimeError(
            "postgres-only invariant violated for workflow_commands in mark_workflow_command_succeeded: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def mark_workflow_command_failed(
        self,
        command_id: str,
        *,
        error_text: str,
        retryable: bool = True,
        retry_delay_seconds: int = 30,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("workflow_commands"):
            row = self._call_control_plane_postgres_native(
                "mark_workflow_command_failed",
                normalized_command_id,
                error_text=error_text,
                retryable=retryable,
                retry_delay_seconds=retry_delay_seconds,
            )
            return self._workflow_command_from_row(row) if row is not None else {}
        raise RuntimeError(
            "postgres-only invariant violated for workflow_commands in mark_workflow_command_failed: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def mark_workflow_command_partial_progress(
        self,
        command_id: str,
        *,
        result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("workflow_commands"):
            row = self._call_control_plane_postgres_native(
                "mark_workflow_command_partial_progress",
                normalized_command_id,
                result=result or {},
            )
            return self._workflow_command_from_row(row) if row is not None else {}
        raise RuntimeError(
            "postgres-only invariant violated for workflow_commands in mark_workflow_command_partial_progress: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def mark_workflow_command_waiting_prerequisite(
        self,
        command_id: str,
        *,
        retry_delay_seconds: int = 8,
        result: dict[str, Any] | None = None,
        from_statuses: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("workflow_commands"):
            row = self._call_control_plane_postgres_native(
                "mark_workflow_command_waiting_prerequisite",
                normalized_command_id,
                retry_delay_seconds=retry_delay_seconds,
                result=result or {},
                from_statuses=list(from_statuses or []),
            )
            return self._workflow_command_from_row(row) if row is not None else {}
        raise RuntimeError(
            "postgres-only invariant violated for workflow_commands in mark_workflow_command_waiting_prerequisite: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def cancel_workflow_command(
        self,
        command_id: str,
        *,
        reason: str = "",
        actor: str = "",
        result: dict[str, Any] | None = None,
        from_statuses: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        """Cancel a command that has not started owner work.

        Running commands may have external side effects in progress; those must
        be cancelled through owner-specific APIs. The generic command surface is
        intentionally safe for Agent callers and only cancels queued/waiting
        work.
        """

        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("workflow_commands"):
            row = self._call_control_plane_postgres_native(
                "cancel_workflow_command",
                normalized_command_id,
                reason=reason,
                actor=actor,
                result=result or {},
                from_statuses=list(from_statuses or []),
            )
            return self._workflow_command_from_row(row) if row is not None else {}
        raise RuntimeError(
            "postgres-only invariant violated for workflow_commands in cancel_workflow_command: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def retry_workflow_command(
        self,
        command_id: str,
        *,
        reason: str = "",
        actor: str = "",
        result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("workflow_commands"):
            row = self._call_control_plane_postgres_native(
                "retry_workflow_command",
                normalized_command_id,
                reason=reason,
                actor=actor,
                result=result or {},
            )
            return self._workflow_command_from_row(row) if row is not None else {}
        raise RuntimeError(
            "postgres-only invariant violated for workflow_commands in retry_workflow_command: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def resume_workflow_command(
        self,
        command_id: str,
        *,
        reason: str = "",
        actor: str = "",
        result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require_postgres_for_durable_runtime("workflow_commands")
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("workflow_commands"):
            row = self._call_control_plane_postgres_native(
                "resume_workflow_command",
                normalized_command_id,
                reason=reason,
                actor=actor,
                result=result or {},
            )
            return self._workflow_command_from_row(row) if row is not None else {}
        raise RuntimeError(
            "postgres-only invariant violated for workflow_commands in resume_workflow_command: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def renew_agent_worker_lease(
        self,
        worker_id: int,
        *,
        lease_owner: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native(
                "renew_agent_worker_lease",
                worker_id,
                lease_owner=lease_owner,
                lease_seconds=lease_seconds,
            )
            return self._agent_worker_from_row(row) if row is not None else None
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in renew_agent_worker_lease: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def release_agent_worker_lease(
        self,
        worker_id: int,
        *,
        lease_owner: str = "",
        error_text: str = "",
    ) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native(
                "release_agent_worker_lease",
                worker_id,
                lease_owner=lease_owner,
                error_text=error_text,
            )
            return self._agent_worker_from_row(row) if row is not None else None
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in release_agent_worker_lease: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def request_interrupt_agent_worker(self, worker_id: int) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native("request_interrupt_agent_worker", worker_id)
            return self._agent_worker_from_row(row) if row is not None else None
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in request_interrupt_agent_worker: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def clear_interrupt_agent_worker(self, worker_id: int) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native("clear_interrupt_agent_worker", worker_id)
            return self._agent_worker_from_row(row) if row is not None else None
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in clear_interrupt_agent_worker: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_agent_worker(
        self,
        *,
        worker_id: int = 0,
        job_id: str = "",
        lane_id: str = "",
        worker_key: str = "",
    ) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native(
                "get_agent_worker",
                worker_id=worker_id,
                job_id=job_id,
                lane_id=lane_id,
                worker_key=worker_key,
            )
            return self._agent_worker_from_row(row) if row is not None else None
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in get_agent_worker: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_agent_workers(self, *, job_id: str = "", session_id: int = 0, lane_id: str = "") -> list[dict[str, Any]]:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            rows = self._call_control_plane_postgres_native(
                "list_agent_workers",
                job_id=job_id,
                session_id=session_id,
                lane_id=lane_id,
            )
            return [self._agent_worker_from_row(row) for row in list(rows or [])]
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in list_agent_workers: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_agent_workers_by_remote_provider_identifiers(
        self,
        *,
        run_id: str = "",
        dataset_id: str = "",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        normalized_run_id = str(run_id or "").strip()
        normalized_dataset_id = str(dataset_id or "").strip()
        if not normalized_run_id and not normalized_dataset_id:
            return []
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            rows = self._call_control_plane_postgres_native(
                "list_agent_workers_by_remote_provider_identifiers",
                run_id=normalized_run_id,
                dataset_id=normalized_dataset_id,
                limit=limit,
            )
            return [self._agent_worker_from_row(row) for row in list(rows or [])]
        raise RuntimeError(
            "postgres-only invariant violated for agent_worker_runs in list_agent_workers_by_remote_provider_identifiers: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_job_events(
        self,
        job_id: str,
        *,
        stage: str = "",
        limit: int = 0,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        postgres_rows = self._select_control_plane_rows(
            "job_events",
            row_builder=self._job_event_from_row,
            where_sql=" AND ".join(
                [
                    "job_id = %s",
                    *(["stage = %s"] if stage else []),
                ]
            ),
            params=[job_id, *([stage] if stage else [])],
            order_by_sql=f"event_id {'DESC' if descending else 'ASC'}",
            limit=int(limit or 0),
        )
        if postgres_rows:
            return postgres_rows
        if self._control_plane_postgres_should_skip_sqlite_fallback("job_events"):
            return []
        raise RuntimeError(
            "postgres-only invariant violated for job_events in list_job_events: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_jobs(
        self,
        *,
        job_type: str = "",
        statuses: list[str] | None = None,
        stages: list[str] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        pg_clauses: list[str] = []
        pg_params: list[Any] = []
        if job_type:
            pg_clauses.append("job_type = %s")
            pg_params.append(job_type)
        normalized_statuses = [
            str(item or "").strip().lower() for item in list(statuses or []) if str(item or "").strip()
        ]
        if normalized_statuses:
            placeholders = ", ".join("%s" for _ in normalized_statuses)
            pg_clauses.append(f"lower(status) IN ({placeholders})")
            pg_params.extend(normalized_statuses)
        normalized_stages = [str(item or "").strip().lower() for item in list(stages or []) if str(item or "").strip()]
        if normalized_stages:
            placeholders = ", ".join("%s" for _ in normalized_stages)
            pg_clauses.append(f"lower(stage) IN ({placeholders})")
            pg_params.extend(normalized_stages)
        postgres_jobs = self._select_control_plane_job_rows(
            where_sql=" AND ".join(pg_clauses),
            params=pg_params,
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(1, int(limit or 100)),
        )
        if postgres_jobs:
            return postgres_jobs
        return []

    def summarize_jobs(
        self,
        *,
        job_type: str = "",
        statuses: list[str] | None = None,
    ) -> dict[str, Any]:
        clauses: list[str] = []
        params: list[Any] = []
        if job_type:
            clauses.append("job_type = ?")
            params.append(job_type)
        normalized_statuses = [
            str(item or "").strip().lower() for item in list(statuses or []) if str(item or "").strip()
        ]
        postgres_jobs = self._select_control_plane_job_rows(
            where_sql=" AND ".join(
                [
                    *(["job_type = %s"] if job_type else []),
                    *(
                        [f"lower(status) IN ({', '.join('%s' for _ in normalized_statuses)})"]
                        if normalized_statuses
                        else []
                    ),
                ]
            ),
            params=[
                *([job_type] if job_type else []),
                *normalized_statuses,
            ],
            order_by_sql="updated_at DESC, created_at DESC",
            limit=5000,
        )
        if postgres_jobs:
            by_status: dict[str, int] = {}
            by_stage: dict[str, int] = {}
            by_status_stage: dict[str, int] = {}
            total = 0
            for job in postgres_jobs:
                status_value = str(job.get("status") or "").strip().lower()
                stage_value = str(job.get("stage") or "").strip().lower()
                total += 1
                if status_value:
                    by_status[status_value] = int(by_status.get(status_value) or 0) + 1
                if stage_value:
                    by_stage[stage_value] = int(by_stage.get(stage_value) or 0) + 1
                key = f"{status_value}:{stage_value}".strip(":")
                if key:
                    by_status_stage[key] = int(by_status_stage.get(key) or 0) + 1
            return {
                "total": total,
                "by_status": by_status,
                "by_stage": by_stage,
                "by_status_stage": by_status_stage,
            }
        return {"total": 0, "by_status": {}, "by_stage": {}, "by_status_stage": {}}

    def find_latest_completed_job(
        self,
        *,
        target_company: str,
        exclude_job_id: str = "",
        job_types: list[str] | None = None,
        limit: int = 200,
        requester_id: str = "",
        tenant_id: str = "",
    ) -> dict[str, Any] | None:
        normalized_requester_id = str(requester_id or "").strip()
        normalized_tenant_id = str(tenant_id or "").strip()
        if bool(normalized_requester_id) != bool(normalized_tenant_id):
            return None
        owner_clauses = ["requester_id = %s", "tenant_id = %s"] if normalized_requester_id else []
        postgres_jobs = self._select_control_plane_job_rows(
            where_sql=" AND ".join(["status = %s", *owner_clauses]),
            params=["completed", *([normalized_requester_id, normalized_tenant_id] if owner_clauses else [])],
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(1, int(limit or 200)),
        )
        if postgres_jobs:
            rows = postgres_jobs
        elif self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
            return None
        else:
            raise RuntimeError(
                "postgres-only invariant violated for jobs in find_latest_completed_job: should_prefer_read "
                "returned False; legacy SQLite tail retired (B4)"
            )
        allowed = set(job_types or ["retrieval", "workflow", "retrieval_rerun"])
        for row in rows:
            job_id = str(row.get("job_id") or "")
            if exclude_job_id and job_id == exclude_job_id:
                continue
            if allowed and str(row.get("job_type") or "") not in allowed:
                continue
            request_payload = dict(row.get("request") or {})
            if str(request_payload.get("target_company") or "").strip().lower() != target_company.strip().lower():
                continue
            return row
        return None

    def find_best_completed_job_match(
        self,
        *,
        target_company: str,
        request_payload: dict[str, Any],
        exclude_job_id: str = "",
        job_types: list[str] | None = None,
        limit: int = 200,
        requester_id: str = "",
        tenant_id: str = "",
    ) -> dict[str, Any] | None:
        normalized_requester_id = str(requester_id or "").strip()
        normalized_tenant_id = str(tenant_id or "").strip()
        if bool(normalized_requester_id) != bool(normalized_tenant_id):
            return None
        owner_clauses = ["requester_id = %s", "tenant_id = %s"] if normalized_requester_id else []
        postgres_jobs = self._select_control_plane_job_rows(
            where_sql=" AND ".join(["status = %s", *owner_clauses]),
            params=["completed", *([normalized_requester_id, normalized_tenant_id] if owner_clauses else [])],
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(1, int(limit or 200)),
        )
        if postgres_jobs:
            rows = postgres_jobs
        elif self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
            return None
        else:
            raise RuntimeError(
                "postgres-only invariant violated for jobs in find_best_completed_job_match: should_prefer_read "
                "returned False; legacy SQLite tail retired (B4)"
            )
        allowed = set(job_types or ["retrieval", "workflow", "retrieval_rerun"])
        request_matching = _matching_bundle_payload(request_payload)
        best_job: dict[str, Any] | None = None
        best_match: dict[str, Any] | None = None
        best_sort_key: tuple[float, str, str] | None = None
        for row in rows:
            job_id = str(row.get("job_id") or "")
            if exclude_job_id and job_id == exclude_job_id:
                continue
            if allowed and str(row.get("job_type") or "") not in allowed:
                continue
            candidate_request = dict(row.get("request") or {})
            if str(candidate_request.get("target_company") or "").strip().lower() != target_company.strip().lower():
                continue
            job_payload = row
            match = request_family_score(
                request_payload,
                candidate_request,
                left_bundle=request_matching,
                right_bundle=dict(job_payload.get("request_matching") or {}),
            )
            sort_key = _job_match_sort_key(match, row)
            if best_sort_key is None or sort_key > best_sort_key:
                best_job = job_payload
                best_match = match
                best_sort_key = sort_key

        if best_job is None or best_match is None:
            return None
        if float(best_match.get("score") or 0.0) < MATCH_THRESHOLD:
            fallback = self.find_latest_completed_job(
                target_company=target_company,
                exclude_job_id=exclude_job_id,
                job_types=job_types,
                limit=limit,
                requester_id=normalized_requester_id,
                tenant_id=normalized_tenant_id,
            )
            if fallback is None:
                return None
            fallback_match = request_family_score(
                request_payload,
                fallback.get("request") or {},
                left_bundle=request_matching,
                right_bundle=dict(fallback.get("request_matching") or {}),
            )
            fallback["baseline_match"] = {
                "selected_via": "latest_company_fallback",
                "family_score": float(fallback_match.get("score") or 0.0),
                "exact_request_match": bool(fallback_match.get("exact_request_match")),
                "exact_family_match": bool(fallback_match.get("exact_family_match")),
                "request_signature": str(request_matching.get("matching_request_signature") or ""),
                "request_family_signature": str(request_matching.get("matching_request_family_signature") or ""),
                "matched_request_signature": str(fallback.get("matching_request_signature") or ""),
                "matched_request_family_signature": str(fallback.get("matching_request_family_signature") or ""),
                "reasons": list(fallback_match.get("reasons") or []),
                "request_family_match_explanation": build_request_family_match_explanation(
                    request_payload,
                    fallback.get("request") or {},
                    match=fallback_match,
                    selection_mode="latest_company_fallback",
                ),
            }
            return fallback

        best_job["baseline_match"] = {
            "selected_via": "request_family_score",
            "family_score": float(best_match.get("score") or 0.0),
            "exact_request_match": bool(best_match.get("exact_request_match")),
            "exact_family_match": bool(best_match.get("exact_family_match")),
            "request_signature": str(request_matching.get("matching_request_signature") or ""),
            "request_family_signature": str(request_matching.get("matching_request_family_signature") or ""),
            "matched_request_signature": str(best_job.get("matching_request_signature") or ""),
            "matched_request_family_signature": str(best_job.get("matching_request_family_signature") or ""),
            "reasons": list(best_match.get("reasons") or []),
            "request_family_match_explanation": build_request_family_match_explanation(
                request_payload,
                best_job.get("request") or {},
                match=best_match,
                selection_mode="request_family_score",
            ),
        }
        return best_job

    def find_latest_job_by_request_signature(
        self,
        *,
        request_signature_value: str,
        target_company: str = "",
        statuses: list[str] | None = None,
        requester_id: str = "",
        tenant_id: str = "",
        scope: str = "auto",
        exclude_job_id: str = "",
        limit: int = 200,
    ) -> dict[str, Any] | None:
        signature = str(request_signature_value or "").strip()
        if not signature:
            return None
        normalized_statuses = [str(item or "").strip() for item in list(statuses or []) if str(item or "").strip()]
        # Track B B3.2: PG is the sole authoritative control-plane store; the dead SQLite fallback
        # branch is removed (should_prefer_read / should_skip_sqlite_fallback are always true).
        postgres_rows = self._select_control_plane_job_rows(
            where_sql=" AND ".join(
                [
                    "(matching_request_signature = %s OR (coalesce(matching_request_signature, '') = '' AND request_signature = %s))",
                    *([f"status IN ({', '.join('%s' for _ in normalized_statuses)})"] if normalized_statuses else []),
                ]
            ),
            params=[signature, signature, *normalized_statuses],
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(1, int(limit or 200)),
        )
        if not postgres_rows:
            return None
        rows = postgres_rows
        scope_mode = _normalize_dispatch_scope(scope, requester_id=requester_id, tenant_id=tenant_id)
        normalized_target_company = str(target_company or "").strip().lower()
        for row in rows:
            payload = row
            job_id = str(payload.get("job_id") or "")
            if exclude_job_id and job_id == exclude_job_id:
                continue
            request_payload = dict(payload.get("request") or {})
            if (
                normalized_target_company
                and str(request_payload.get("target_company") or "").strip().lower() != normalized_target_company
            ):
                continue
            if not _job_matches_dispatch_scope(
                payload,
                scope=scope_mode,
                requester_id=str(requester_id or "").strip(),
                tenant_id=str(tenant_id or "").strip(),
            ):
                continue
            return payload
        return None

    def find_latest_job_by_request_family_signature(
        self,
        *,
        request_family_signature_value: str,
        target_company: str = "",
        statuses: list[str] | None = None,
        requester_id: str = "",
        tenant_id: str = "",
        scope: str = "auto",
        exclude_job_id: str = "",
        limit: int = 200,
    ) -> dict[str, Any] | None:
        family_signature = str(request_family_signature_value or "").strip()
        if not family_signature:
            return None
        normalized_statuses = [str(item or "").strip() for item in list(statuses or []) if str(item or "").strip()]
        # Track B B3.2: PG is the sole authoritative control-plane store (postgres_only is required at
        # construction), so should_prefer_read / should_skip_sqlite_fallback are always true and the
        # SQLite fallback branch here is dead. Removed; the PG read is authoritative.
        postgres_rows = self._select_control_plane_job_rows(
            where_sql=" AND ".join(
                [
                    "("
                    "matching_request_family_signature = %s "
                    "OR (coalesce(matching_request_family_signature, '') = '' AND request_family_signature = %s)"
                    ")",
                    *([f"status IN ({', '.join('%s' for _ in normalized_statuses)})"] if normalized_statuses else []),
                ]
            ),
            params=[family_signature, family_signature, *normalized_statuses],
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(1, int(limit or 200)),
        )
        if not postgres_rows:
            return None
        rows = postgres_rows
        scope_mode = _normalize_dispatch_scope(scope, requester_id=requester_id, tenant_id=tenant_id)
        normalized_target_company = str(target_company or "").strip().lower()
        for row in rows:
            payload = row
            job_id = str(payload.get("job_id") or "")
            if exclude_job_id and job_id == exclude_job_id:
                continue
            request_payload = dict(payload.get("request") or {})
            if (
                normalized_target_company
                and str(request_payload.get("target_company") or "").strip().lower() != normalized_target_company
            ):
                continue
            if not _job_matches_dispatch_scope(
                payload,
                scope=scope_mode,
                requester_id=str(requester_id or "").strip(),
                tenant_id=str(tenant_id or "").strip(),
            ):
                continue
            return payload
        return None

    def list_jobs_by_request_signature(
        self,
        *,
        request_signature_value: str,
        target_company: str = "",
        statuses: list[str] | None = None,
        requester_id: str = "",
        tenant_id: str = "",
        scope: str = "auto",
        exclude_job_id: str = "",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        signature = str(request_signature_value or "").strip()
        if not signature:
            return []
        normalized_statuses = [str(item or "").strip() for item in list(statuses or []) if str(item or "").strip()]
        # Track B B3.2: PG is the sole authoritative control-plane store; dead SQLite fallback removed.
        postgres_rows = self._select_control_plane_job_rows(
            where_sql=" AND ".join(
                [
                    "(matching_request_signature = %s OR (coalesce(matching_request_signature, '') = '' AND request_signature = %s))",
                    *([f"status IN ({', '.join('%s' for _ in normalized_statuses)})"] if normalized_statuses else []),
                ]
            ),
            params=[signature, signature, *normalized_statuses],
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(1, int(limit or 200)),
        )
        if not postgres_rows:
            return []
        rows = postgres_rows
        scope_mode = _normalize_dispatch_scope(scope, requester_id=requester_id, tenant_id=tenant_id)
        normalized_target_company = str(target_company or "").strip().lower()
        results: list[dict[str, Any]] = []
        for row in rows:
            payload = row
            job_id = str(payload.get("job_id") or "")
            if exclude_job_id and job_id == exclude_job_id:
                continue
            request_payload = dict(payload.get("request") or {})
            if (
                normalized_target_company
                and str(request_payload.get("target_company") or "").strip().lower() != normalized_target_company
            ):
                continue
            if not _job_matches_dispatch_scope(
                payload,
                scope=scope_mode,
                requester_id=str(requester_id or "").strip(),
                tenant_id=str(tenant_id or "").strip(),
            ):
                continue
            results.append(payload)
        return results

    def supersede_workflow_job(
        self,
        *,
        job_id: str,
        replacement_job_id: str = "",
        reason: str = "",
    ) -> dict[str, Any] | None:
        existing = self.get_job(job_id)
        if existing is None:
            return None
        if str(existing.get("job_type") or "") != "workflow":
            return None

        normalized_reason = str(reason or "").strip() or "Superseded by a newer workflow run."
        normalized_replacement_job_id = str(replacement_job_id or "").strip()
        summary = dict(existing.get("summary") or {})
        summary["message"] = normalized_reason
        summary["superseded_reason"] = normalized_reason
        summary["superseded_at"] = datetime.now(timezone.utc).isoformat()
        if normalized_replacement_job_id:
            summary["superseded_by_job_id"] = normalized_replacement_job_id

        superseded_worker_count = 0
        superseded_trace_count = 0
        self.save_job(
            job_id=str(job_id),
            job_type="workflow",
            status="superseded",
            stage="completed",
            request_payload=dict(existing.get("request") or {}),
            plan_payload=dict(existing.get("plan") or {}),
            execution_bundle_payload=dict(existing.get("execution_bundle") or {}),
            summary_payload=summary,
            artifact_path=str(existing.get("artifact_path") or ""),
            requester_id=str(existing.get("requester_id") or ""),
            tenant_id=str(existing.get("tenant_id") or ""),
            idempotency_key=str(existing.get("idempotency_key") or ""),
        )
        self.update_agent_runtime_session_status(str(job_id), "superseded")
        if (
            self._control_plane_postgres_should_prefer_read("agent_worker_runs")
            and self._control_plane_postgres_should_prefer_read("agent_trace_spans")
            and self._control_plane_postgres_should_prefer_read("workflow_job_leases")
        ):
            runtime_state = (
                self._call_control_plane_postgres_native(
                    "supersede_workflow_runtime_state",
                    str(job_id),
                )
                or {}
            )
            superseded_worker_count = int(runtime_state.get("superseded_worker_count") or 0)
            superseded_trace_count = int(runtime_state.get("superseded_trace_count") or 0)
        else:
            raise RuntimeError(
                "postgres-only invariant violated for agent_worker_runs in supersede_workflow_job: should_prefer_read "
                "returned False; legacy SQLite tail retired (B4)"
            )
        refreshed = self.get_job(job_id)
        if refreshed is None:
            return None
        return {
            "job": refreshed,
            "superseded_worker_count": superseded_worker_count,
            "superseded_trace_count": superseded_trace_count,
        }

    def find_latest_job_by_idempotency_key(
        self,
        *,
        idempotency_key: str,
        target_company: str = "",
        statuses: list[str] | None = None,
        requester_id: str = "",
        tenant_id: str = "",
        scope: str = "auto",
        limit: int = 200,
    ) -> dict[str, Any] | None:
        normalized_idempotency_key = str(idempotency_key or "").strip()
        if not normalized_idempotency_key:
            return None
        normalized_statuses = [str(item or "").strip() for item in list(statuses or []) if str(item or "").strip()]
        # PG-authoritative idempotency-dedup probe. B2 added the PG read to fix duplicate-job dedup
        # (the SQLite shadow is empty in postgres_only); B3.2 removed the now-dead SQLite fallback.
        postgres_rows = self._select_control_plane_job_rows(
            where_sql=" AND ".join(
                [
                    "idempotency_key = %s",
                    *([f"status IN ({', '.join('%s' for _ in normalized_statuses)})"] if normalized_statuses else []),
                ]
            ),
            params=[normalized_idempotency_key, *normalized_statuses],
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(1, int(limit or 200)),
        )
        if not postgres_rows:
            return None
        rows = postgres_rows
        scope_mode = _normalize_dispatch_scope(scope, requester_id=requester_id, tenant_id=tenant_id)
        normalized_target_company = str(target_company or "").strip().lower()
        for row in rows:
            payload = row
            request_payload = dict(payload.get("request") or {})
            if (
                normalized_target_company
                and str(request_payload.get("target_company") or "").strip().lower() != normalized_target_company
            ):
                continue
            if not _job_matches_dispatch_scope(
                payload,
                scope=scope_mode,
                requester_id=str(requester_id or "").strip(),
                tenant_id=str(tenant_id or "").strip(),
            ):
                continue
            return payload
        return None

    def record_query_dispatch(
        self,
        *,
        target_company: str,
        request_payload: dict[str, Any],
        strategy: str,
        status: str,
        source_job_id: str = "",
        created_job_id: str = "",
        requester_id: str = "",
        tenant_id: str = "",
        idempotency_key: str = "",
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_target_company = str(target_company or "").strip()
        requester_id_value = str(requester_id or "").strip()
        tenant_id_value = str(tenant_id or "").strip()
        idempotency_key_value = str(idempotency_key or "").strip()
        matching_bundle = _matching_bundle_payload(request_payload)
        dispatch_payload = dict(payload or {})
        dispatch_payload.setdefault("request", _json_safe_payload(request_payload))
        dispatch_payload.setdefault("request_matching", _json_safe_payload(matching_bundle))
        request_sig = request_signature(request_payload)
        request_family_sig = request_family_signature(request_payload)
        if self._control_plane_postgres_should_prefer_read("query_dispatches"):
            now = _utc_now_timestamp()
            row = self._call_control_plane_postgres_native(
                "insert_row_with_generated_id",
                table_name="query_dispatches",
                row={
                    "target_company": normalized_target_company,
                    "request_signature": request_sig,
                    "request_family_signature": request_family_sig,
                    "matching_request_signature": str(matching_bundle.get("matching_request_signature") or ""),
                    "matching_request_family_signature": str(
                        matching_bundle.get("matching_request_family_signature") or ""
                    ),
                    "requester_id": requester_id_value,
                    "tenant_id": tenant_id_value,
                    "idempotency_key": idempotency_key_value,
                    "strategy": str(strategy or "").strip(),
                    "status": str(status or "").strip(),
                    "source_job_id": str(source_job_id or "").strip(),
                    "created_job_id": str(created_job_id or "").strip(),
                    "matching_request_json": json.dumps(_json_safe_payload(matching_bundle), ensure_ascii=False),
                    "payload_json": json.dumps(_json_safe_payload(dispatch_payload), ensure_ascii=False),
                    "created_at": now,
                    "updated_at": now,
                },
            )
            if row is not None:
                return self._query_dispatch_from_row(row)
            # postgres-only (B4): insert_row_with_generated_id returns the inserted row or raises via
            # strict-no-fallback; a non-exception None means the authoritative insert returned no
            # confirmation, which is forbidden. The legacy SQLite tail below was retired (B4).
            self._raise_control_plane_postgres_write_failure(
                table_name="query_dispatches",
                method_name="record_query_dispatch",
                reason="postgres-only: authoritative insert returned no confirmation; SQLite fallback retired (B4)",
            )
        raise RuntimeError(
            "postgres-only invariant violated for query_dispatches in record_query_dispatch: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_query_dispatch(self, dispatch_id: int) -> dict[str, Any] | None:
        if dispatch_id <= 0:
            return None
        postgres_row = self._select_control_plane_row(
            "query_dispatches",
            row_builder=self._query_dispatch_from_row,
            where_sql="dispatch_id = %s",
            params=[dispatch_id],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_query_dispatches(
        self,
        *,
        target_company: str = "",
        requester_id: str = "",
        tenant_id: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            clauses.append("lower(target_company) = lower(?)")
            params.append(target_company)
        if requester_id:
            clauses.append("requester_id = ?")
            params.append(requester_id)
        if tenant_id:
            clauses.append("tenant_id = ?")
            params.append(tenant_id)
        postgres_rows = self._select_control_plane_rows(
            "query_dispatches",
            row_builder=self._query_dispatch_from_row,
            where_sql=" AND ".join(
                [
                    *(["lower(target_company) = lower(%s)"] if target_company else []),
                    *(["requester_id = %s"] if requester_id else []),
                    *(["tenant_id = %s"] if tenant_id else []),
                ]
            ),
            params=[
                *([target_company] if target_company else []),
                *([requester_id] if requester_id else []),
                *([tenant_id] if tenant_id else []),
            ],
            order_by_sql="updated_at DESC, dispatch_id DESC",
            limit=max(1, int(limit or 100)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_organization_asset_registry(
        self,
        payload: dict[str, Any],
        *,
        authoritative: bool = False,
    ) -> dict[str, Any]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(
            _normalized_payload_text(payload, "target_company"),
            _normalized_payload_text(payload, "company_key"),
        )
        snapshot_id = _normalized_payload_text(payload, "snapshot_id")
        asset_view = _normalized_payload_text(payload, "asset_view", default="canonical_merged") or "canonical_merged"
        if not normalized_target_company or not snapshot_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("organization_asset_registry"):
            postgres_company_lookup_clause, postgres_company_lookup_params = _company_identity_lookup_predicate(
                normalized_target_company,
                normalized_company_key,
                placeholder="%s",
            )
            existing_rows = self._select_control_plane_rows(
                "organization_asset_registry",
                row_builder=self._organization_asset_registry_from_row,
                where_sql=f"{postgres_company_lookup_clause} AND asset_view = %s",
                params=[*postgres_company_lookup_params, asset_view],
                order_by_sql="authoritative DESC, updated_at DESC, registry_id DESC",
                limit=0,
            )
            existing_row = next(
                (row for row in existing_rows if str(row.get("snapshot_id") or "").strip() == snapshot_id),
                None,
            )
            storage_target_company = _resolve_storage_target_company(
                requested_target_company=normalized_target_company,
                company_key=normalized_company_key,
                existing_rows=existing_rows,
                id_column="registry_id",
            )
            if authoritative:
                for companion_row in existing_rows:
                    if not bool(companion_row.get("authoritative")):
                        continue
                    self._call_control_plane_postgres_native(
                        "update_row_returning",
                        table_name="organization_asset_registry",
                        id_column="registry_id",
                        id_value=int(companion_row.get("registry_id") or 0),
                        row={
                            "authoritative": 0,
                            "updated_at": _utc_now_timestamp(),
                        },
                    )
            now = _utc_now_timestamp()
            row_payload = {
                "target_company": storage_target_company,
                "company_key": normalized_company_key,
                "snapshot_id": snapshot_id,
                "asset_view": asset_view,
                "status": _normalized_payload_text(payload, "status", default="ready") or "ready",
                "authoritative": (
                    1
                    if authoritative
                    or bool(payload.get("authoritative"))
                    or bool((existing_row or {}).get("authoritative"))
                    else 0
                ),
                "candidate_count": int(payload.get("candidate_count") or 0),
                "evidence_count": int(payload.get("evidence_count") or 0),
                "profile_detail_count": int(payload.get("profile_detail_count") or 0),
                "explicit_profile_capture_count": int(payload.get("explicit_profile_capture_count") or 0),
                "missing_linkedin_count": int(payload.get("missing_linkedin_count") or 0),
                "manual_review_backlog_count": int(payload.get("manual_review_backlog_count") or 0),
                "profile_completion_backlog_count": int(payload.get("profile_completion_backlog_count") or 0),
                "source_snapshot_count": int(payload.get("source_snapshot_count") or 0),
                "completeness_score": float(payload.get("completeness_score") or 0.0),
                "completeness_band": _normalized_payload_text(payload, "completeness_band", default="low") or "low",
                "current_lane_coverage_json": json.dumps(
                    _json_safe_payload(payload.get("current_lane_coverage") or {}),
                    ensure_ascii=False,
                ),
                "former_lane_coverage_json": json.dumps(
                    _json_safe_payload(payload.get("former_lane_coverage") or {}),
                    ensure_ascii=False,
                ),
                "current_lane_effective_candidate_count": int(
                    payload.get("current_lane_effective_candidate_count") or 0
                ),
                "former_lane_effective_candidate_count": int(payload.get("former_lane_effective_candidate_count") or 0),
                "current_lane_effective_ready": 1 if bool(payload.get("current_lane_effective_ready")) else 0,
                "former_lane_effective_ready": 1 if bool(payload.get("former_lane_effective_ready")) else 0,
                "source_snapshot_selection_json": json.dumps(
                    _json_safe_payload(payload.get("source_snapshot_selection") or {}),
                    ensure_ascii=False,
                ),
                "selected_snapshot_ids_json": json.dumps(
                    _json_safe_payload(payload.get("selected_snapshot_ids") or []),
                    ensure_ascii=False,
                ),
                "source_path": _normalized_payload_text(payload, "source_path"),
                "source_job_id": _normalized_payload_text(payload, "source_job_id"),
                "materialization_generation_key": _normalized_payload_text(payload, "materialization_generation_key"),
                "materialization_generation_sequence": int(payload.get("materialization_generation_sequence") or 0),
                "materialization_watermark": _normalized_payload_text(payload, "materialization_watermark"),
                "summary_json": json.dumps(_json_safe_payload(payload.get("summary") or {}), ensure_ascii=False),
                "created_at": _normalize_textual_value((existing_row or {}).get("created_at") or now),
                "updated_at": now,
            }
            if existing_row and str(existing_row.get("target_company") or "").strip() != storage_target_company:
                row = self._call_control_plane_postgres_native(
                    "update_row_returning",
                    table_name="organization_asset_registry",
                    id_column="registry_id",
                    id_value=int(existing_row.get("registry_id") or 0),
                    row=row_payload,
                )
            else:
                row = self._call_control_plane_postgres_native(
                    "upsert_row_with_generated_id",
                    table_name="organization_asset_registry",
                    row={
                        "registry_id": int(existing_row.get("registry_id") or 0) if existing_row else None,
                        **row_payload,
                    },
                    conflict_columns=["target_company", "snapshot_id", "asset_view"],
                    update_columns=[
                        "company_key",
                        "status",
                        "authoritative",
                        "candidate_count",
                        "evidence_count",
                        "profile_detail_count",
                        "explicit_profile_capture_count",
                        "missing_linkedin_count",
                        "manual_review_backlog_count",
                        "profile_completion_backlog_count",
                        "source_snapshot_count",
                        "completeness_score",
                        "completeness_band",
                        "current_lane_coverage_json",
                        "former_lane_coverage_json",
                        "current_lane_effective_candidate_count",
                        "former_lane_effective_candidate_count",
                        "current_lane_effective_ready",
                        "former_lane_effective_ready",
                        "source_snapshot_selection_json",
                        "selected_snapshot_ids_json",
                        "source_path",
                        "source_job_id",
                        "materialization_generation_key",
                        "materialization_generation_sequence",
                        "materialization_watermark",
                        "summary_json",
                        "updated_at",
                    ],
                )
            if row is not None:
                return self._organization_asset_registry_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("organization_asset_registry"):
                # Track B B4.1b: PG is authoritative — fail closed rather than fall through to the dead
                # SQLite shadow.
                self._raise_control_plane_postgres_write_failure(
                    table_name="organization_asset_registry",
                    method_name="upsert_organization_asset_registry",
                    reason="native upsert returned no row under postgres_only",
                )
        raise RuntimeError(
            "postgres-only invariant violated for organization_asset_registry in upsert_organization_asset_registry: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def upsert_organization_execution_profile(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(
            _normalized_payload_text(payload, "target_company"),
            _normalized_payload_text(payload, "company_key"),
        )
        asset_view = _normalized_payload_text(payload, "asset_view", default="canonical_merged") or "canonical_merged"
        if not normalized_target_company:
            return {}
        if self._control_plane_postgres_should_prefer_read("organization_execution_profiles"):
            postgres_company_lookup_clause, postgres_company_lookup_params = _company_identity_lookup_predicate(
                normalized_target_company,
                normalized_company_key,
                placeholder="%s",
            )
            existing_rows = self._select_control_plane_rows(
                "organization_execution_profiles",
                row_builder=self._organization_execution_profile_from_row,
                where_sql=f"{postgres_company_lookup_clause} AND asset_view = %s",
                params=[*postgres_company_lookup_params, asset_view],
                order_by_sql="updated_at DESC, profile_id DESC",
                limit=0,
            )
            existing_row = existing_rows[0] if existing_rows else None
            storage_target_company = _resolve_storage_target_company(
                requested_target_company=normalized_target_company,
                company_key=normalized_company_key,
                existing_rows=existing_rows,
                id_column="profile_id",
            )
            now = _utc_now_timestamp()
            row_payload = {
                "target_company": storage_target_company,
                "company_key": normalized_company_key,
                "asset_view": asset_view,
                "source_registry_id": int(payload.get("source_registry_id") or 0),
                "source_snapshot_id": _normalized_payload_text(payload, "source_snapshot_id"),
                "source_job_id": _normalized_payload_text(payload, "source_job_id"),
                "source_generation_key": _normalized_payload_text(payload, "source_generation_key"),
                "source_generation_sequence": int(payload.get("source_generation_sequence") or 0),
                "source_generation_watermark": _normalized_payload_text(payload, "source_generation_watermark"),
                "status": _normalized_payload_text(payload, "status", default="ready") or "ready",
                "org_scale_band": _normalized_payload_text(payload, "org_scale_band", default="unknown") or "unknown",
                "default_acquisition_mode": _normalized_payload_text(
                    payload,
                    "default_acquisition_mode",
                    default="full_company_roster",
                )
                or "full_company_roster",
                "prefer_delta_from_baseline": 1 if bool(payload.get("prefer_delta_from_baseline")) else 0,
                "current_lane_default": _normalized_payload_text(
                    payload,
                    "current_lane_default",
                    default="live_acquisition",
                )
                or "live_acquisition",
                "former_lane_default": _normalized_payload_text(
                    payload,
                    "former_lane_default",
                    default="live_acquisition",
                )
                or "live_acquisition",
                "baseline_candidate_count": int(payload.get("baseline_candidate_count") or 0),
                "current_lane_effective_candidate_count": int(
                    payload.get("current_lane_effective_candidate_count") or 0
                ),
                "former_lane_effective_candidate_count": int(payload.get("former_lane_effective_candidate_count") or 0),
                "completeness_score": float(payload.get("completeness_score") or 0.0),
                "completeness_band": _normalized_payload_text(payload, "completeness_band", default="low") or "low",
                "profile_detail_ratio": float(payload.get("profile_detail_ratio") or 0.0),
                "company_employee_shard_count": int(payload.get("company_employee_shard_count") or 0),
                "current_profile_search_shard_count": int(payload.get("current_profile_search_shard_count") or 0),
                "former_profile_search_shard_count": int(payload.get("former_profile_search_shard_count") or 0),
                "company_employee_cap_hit_count": int(payload.get("company_employee_cap_hit_count") or 0),
                "profile_search_cap_hit_count": int(payload.get("profile_search_cap_hit_count") or 0),
                "reason_codes_json": json.dumps(
                    _json_safe_payload(payload.get("reason_codes") or []), ensure_ascii=False
                ),
                "explanation_json": json.dumps(
                    _json_safe_payload(payload.get("explanation") or {}),
                    ensure_ascii=False,
                ),
                "summary_json": json.dumps(_json_safe_payload(payload.get("summary") or {}), ensure_ascii=False),
                "created_at": _normalize_textual_value((existing_row or {}).get("created_at") or now),
                "updated_at": now,
            }
            if existing_row and str(existing_row.get("target_company") or "").strip() != storage_target_company:
                row = self._call_control_plane_postgres_native(
                    "update_row_returning",
                    table_name="organization_execution_profiles",
                    id_column="profile_id",
                    id_value=int(existing_row.get("profile_id") or 0),
                    row=row_payload,
                )
            else:
                row = self._call_control_plane_postgres_native(
                    "upsert_row_with_generated_id",
                    table_name="organization_execution_profiles",
                    row={
                        "profile_id": int(existing_row.get("profile_id") or 0) if existing_row else None,
                        **row_payload,
                    },
                    conflict_columns=["target_company", "asset_view"],
                    update_columns=[
                        "company_key",
                        "source_registry_id",
                        "source_snapshot_id",
                        "source_job_id",
                        "source_generation_key",
                        "source_generation_sequence",
                        "source_generation_watermark",
                        "status",
                        "org_scale_band",
                        "default_acquisition_mode",
                        "prefer_delta_from_baseline",
                        "current_lane_default",
                        "former_lane_default",
                        "baseline_candidate_count",
                        "current_lane_effective_candidate_count",
                        "former_lane_effective_candidate_count",
                        "completeness_score",
                        "completeness_band",
                        "profile_detail_ratio",
                        "company_employee_shard_count",
                        "current_profile_search_shard_count",
                        "former_profile_search_shard_count",
                        "company_employee_cap_hit_count",
                        "profile_search_cap_hit_count",
                        "reason_codes_json",
                        "explanation_json",
                        "summary_json",
                        "updated_at",
                    ],
                )
            if row is not None:
                return self._organization_execution_profile_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("organization_execution_profiles"):
                # Track B B4.1b: PG is authoritative — fail closed rather than fall through to the dead
                # SQLite shadow.
                self._raise_control_plane_postgres_write_failure(
                    table_name="organization_execution_profiles",
                    method_name="upsert_organization_execution_profile",
                    reason="native upsert returned no row under postgres_only",
                )
        raise RuntimeError(
            "postgres-only invariant violated for organization_execution_profiles in upsert_organization_execution_profile: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_organization_execution_profile(
        self,
        *,
        target_company: str,
        asset_view: str = "canonical_merged",
    ) -> dict[str, Any]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        if not normalized_target_company:
            return {}
        if self._control_plane_postgres_should_prefer_read("organization_execution_profiles"):
            postgres_company_lookup_clause, postgres_company_lookup_params = _company_identity_lookup_predicate(
                normalized_target_company,
                normalized_company_key,
                placeholder="%s",
            )
            row = self._control_plane_postgres.select_one(
                "organization_execution_profiles",
                where_sql=f"{postgres_company_lookup_clause} AND asset_view = %s",
                params=[
                    *postgres_company_lookup_params,
                    str(asset_view or "canonical_merged").strip() or "canonical_merged",
                ],
                order_by_sql="updated_at DESC, profile_id DESC",
            )
            if row is not None:
                return self._organization_execution_profile_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("organization_execution_profiles"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for organization_execution_profiles in get_organization_execution_profile: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_organization_execution_profiles(
        self,
        *,
        target_company: str = "",
        asset_view: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if self._control_plane_postgres_should_prefer_read("organization_execution_profiles"):
            clauses: list[str] = []
            params: list[Any] = []
            if target_company:
                normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
                company_lookup_clause, company_lookup_params = _company_identity_lookup_predicate(
                    normalized_target_company,
                    normalized_company_key,
                    placeholder="%s",
                )
                clauses.append(company_lookup_clause)
                params.extend(company_lookup_params)
            if asset_view:
                clauses.append("asset_view = %s")
                params.append(asset_view)
            rows = self._control_plane_postgres.select_many(
                "organization_execution_profiles",
                where_sql=" AND ".join(clauses),
                params=params,
                order_by_sql="updated_at DESC, profile_id DESC",
                limit=limit,
            )
            if rows:
                return [self._organization_execution_profile_from_row(row) for row in rows]
            if self._control_plane_postgres_should_skip_sqlite_fallback("organization_execution_profiles"):
                return []
        raise RuntimeError(
            "postgres-only invariant violated for organization_execution_profiles in list_organization_execution_profiles: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_authoritative_organization_asset_registry(
        self,
        *,
        target_company: str,
        asset_view: str = "canonical_merged",
    ) -> dict[str, Any]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        if not normalized_target_company:
            return {}
        if self._control_plane_postgres_should_prefer_read("organization_asset_registry"):
            postgres_company_lookup_clause, postgres_company_lookup_params = _company_identity_lookup_predicate(
                normalized_target_company,
                normalized_company_key,
                placeholder="%s",
            )
            row = self._control_plane_postgres.select_one(
                "organization_asset_registry",
                where_sql=f"{postgres_company_lookup_clause} AND asset_view = %s",
                params=[
                    *postgres_company_lookup_params,
                    str(asset_view or "canonical_merged").strip() or "canonical_merged",
                ],
                order_by_sql="authoritative DESC, updated_at DESC, registry_id DESC",
            )
            if row is not None:
                return self._organization_asset_registry_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("organization_asset_registry"):
                return {}
        raise RuntimeError(
            "postgres-only invariant violated for organization_asset_registry in get_authoritative_organization_asset_registry: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_organization_asset_registry(
        self,
        *,
        target_company: str = "",
        asset_view: str = "",
        authoritative_only: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if self._control_plane_postgres_should_prefer_read("organization_asset_registry"):
            clauses: list[str] = []
            params: list[Any] = []
            if target_company:
                normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
                company_lookup_clause, company_lookup_params = _company_identity_lookup_predicate(
                    normalized_target_company,
                    normalized_company_key,
                    placeholder="%s",
                )
                clauses.append(company_lookup_clause)
                params.extend(company_lookup_params)
            if asset_view:
                clauses.append("asset_view = %s")
                params.append(asset_view)
            if authoritative_only:
                clauses.append("authoritative = 1")
            rows = self._control_plane_postgres.select_many(
                "organization_asset_registry",
                where_sql=" AND ".join(clauses),
                params=params,
                order_by_sql="authoritative DESC, updated_at DESC, registry_id DESC",
                limit=limit,
            )
            if rows:
                return [self._organization_asset_registry_from_row(row) for row in rows]
            if self._control_plane_postgres_should_skip_sqlite_fallback("organization_asset_registry"):
                return []
        raise RuntimeError(
            "postgres-only invariant violated for organization_asset_registry in list_organization_asset_registry: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def canonicalize_organization_asset_registry_target_company(
        self,
        *,
        target_company: str,
        company_key: str = "",
        asset_view: str = "",
    ) -> dict[str, Any]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company, company_key)
        normalized_asset_view = str(asset_view or "").strip()
        if not normalized_target_company:
            return {"updated_rows": 0, "deleted_rows": 0, "merged_groups": 0}

        def _postgres_where_clause() -> tuple[str, list[Any]]:
            clauses = ["(lower(target_company) = lower(%s)"]
            params: list[Any] = [normalized_target_company]
            if normalized_company_key:
                clauses.append(" OR company_key = %s")
                params.append(normalized_company_key)
            clauses.append(")")
            if normalized_asset_view:
                clauses.append("AND asset_view = %s")
                params.append(normalized_asset_view)
            return " ".join(clauses), params

        def _row_sort_key(row: dict[str, Any]) -> tuple[int, int, str, int]:
            return (
                1 if bool(row["authoritative"]) else 0,
                int(row["candidate_count"] or 0),
                str(row["updated_at"] or ""),
                int(row["registry_id"] or 0),
            )

        def _apply_registry_canonicalization(rows: list[dict[str, Any]], *, use_postgres: bool) -> dict[str, Any]:
            updated_rows = 0
            deleted_rows = 0
            merged_groups = 0
            grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for row in rows:
                key = (str(row["snapshot_id"] or ""), str(row["asset_view"] or "canonical_merged"))
                grouped.setdefault(key, []).append(row)
            for group_rows in grouped.values():
                if not group_rows:
                    continue
                keeper = max(
                    group_rows,
                    key=lambda candidate_row: (
                        1 if str(candidate_row["target_company"] or "") == normalized_target_company else 0,
                        *_row_sort_key(candidate_row),
                    ),
                )
                keeper_id = int(keeper["registry_id"] or 0)
                authoritative = any(bool(candidate_row["authoritative"]) for candidate_row in group_rows)
                keeper_needs_update = str(
                    keeper["target_company"] or ""
                ) != normalized_target_company or authoritative != bool(keeper["authoritative"])
                if keeper_needs_update and keeper_id > 0:
                    if use_postgres:
                        updated_row = self._call_control_plane_postgres_native(
                            "update_row_returning",
                            table_name="organization_asset_registry",
                            id_column="registry_id",
                            id_value=keeper_id,
                            row={
                                "target_company": normalized_target_company,
                                "authoritative": 1 if authoritative else 0,
                                "updated_at": _utc_now_timestamp(),
                            },
                        )
                        if updated_row is not None:
                            updated_rows += 1
                extra_ids = [
                    int(candidate_row["registry_id"] or 0)
                    for candidate_row in group_rows
                    if int(candidate_row["registry_id"] or 0) != keeper_id
                ]
                extra_ids = [registry_id for registry_id in extra_ids if registry_id > 0]
                if extra_ids:
                    if use_postgres:
                        placeholders = ", ".join("%s" for _ in extra_ids)
                        deleted_rows += int(
                            self._call_control_plane_postgres_native(
                                "delete_rows",
                                table_name="organization_asset_registry",
                                where_sql=f"registry_id IN ({placeholders})",
                                params=extra_ids,
                            )
                            or 0
                        )
                    merged_groups += 1
            if normalized_asset_view and any(bool(row["authoritative"]) for row in rows):
                if use_postgres:
                    refreshed_where_clause = "lower(target_company) = lower(%s) AND asset_view = %s"
                    refreshed_rows = self._select_control_plane_rows(
                        "organization_asset_registry",
                        row_builder=self._organization_asset_registry_from_row,
                        where_sql=refreshed_where_clause,
                        params=[normalized_target_company, normalized_asset_view],
                        order_by_sql="updated_at DESC, registry_id DESC",
                        limit=0,
                    )
                    authoritative_rows = [row for row in refreshed_rows if bool(row.get("authoritative"))]
                    authoritative_id = int(authoritative_rows[0]["registry_id"] or 0) if authoritative_rows else 0
                    if authoritative_id > 0:
                        for row in refreshed_rows:
                            registry_id = int(row.get("registry_id") or 0)
                            desired_authoritative = registry_id == authoritative_id
                            if registry_id <= 0 or bool(row.get("authoritative")) == desired_authoritative:
                                continue
                            updated_row = self._call_control_plane_postgres_native(
                                "update_row_returning",
                                table_name="organization_asset_registry",
                                id_column="registry_id",
                                id_value=registry_id,
                                row={
                                    "authoritative": 1 if desired_authoritative else 0,
                                    "updated_at": _utc_now_timestamp(),
                                },
                            )
                            if updated_row is not None:
                                updated_rows += 1
            return {
                "target_company": normalized_target_company,
                "asset_view": normalized_asset_view or "",
                "updated_rows": updated_rows,
                "deleted_rows": deleted_rows,
                "merged_groups": merged_groups,
            }

        if self._control_plane_postgres_should_prefer_read("organization_asset_registry"):
            postgres_where_clause, postgres_params = _postgres_where_clause()
            postgres_rows = self._select_control_plane_rows(
                "organization_asset_registry",
                row_builder=self._organization_asset_registry_from_row,
                where_sql=postgres_where_clause,
                params=postgres_params,
                order_by_sql="snapshot_id ASC, asset_view ASC, registry_id DESC",
                limit=0,
            )
            if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("organization_asset_registry"):
                return _apply_registry_canonicalization(postgres_rows, use_postgres=True)
        raise RuntimeError(
            "postgres-only invariant violated for organization_asset_registry in canonicalize_organization_asset_registry_target_company: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def upsert_acquisition_shard_registry(self, payload: dict[str, Any]) -> dict[str, Any]:
        shard_key = _normalized_payload_text(payload, "shard_key")
        target_company = _normalized_payload_text(payload, "target_company")
        snapshot_id = _normalized_payload_text(payload, "snapshot_id")
        if not shard_key or not target_company or not snapshot_id:
            return {}
        status = _normalized_payload_text(payload, "status", default="completed") or "completed"
        completed_at = (
            datetime.now(timezone.utc).isoformat(timespec="seconds") if status.startswith("completed") else ""
        )
        existing = self._select_control_plane_row(
            "acquisition_shard_registry",
            row_builder=self._acquisition_shard_registry_from_row,
            where_sql="shard_key = %s",
            params=[shard_key],
        )
        now = _utc_now_timestamp()
        row_payload = {
            "shard_key": shard_key,
            "target_company": target_company,
            "company_key": _normalized_payload_text(payload, "company_key"),
            "snapshot_id": snapshot_id,
            "asset_view": _normalized_payload_text(payload, "asset_view", default="canonical_merged")
            or "canonical_merged",
            "lane": _normalized_payload_text(payload, "lane"),
            "status": status,
            "employment_scope": _normalized_payload_text(payload, "employment_scope", default="all") or "all",
            "strategy_type": _normalized_payload_text(payload, "strategy_type"),
            "shard_id": _normalized_payload_text(payload, "shard_id"),
            "shard_title": _normalized_payload_text(payload, "shard_title"),
            "search_query": _normalized_payload_text(payload, "search_query"),
            "query_signature": _normalized_payload_text(payload, "query_signature"),
            "company_scope_json": json.dumps(
                _json_safe_payload(payload.get("company_scope") or []), ensure_ascii=False
            ),
            "locations_json": json.dumps(_json_safe_payload(payload.get("locations") or []), ensure_ascii=False),
            "function_ids_json": json.dumps(_json_safe_payload(payload.get("function_ids") or []), ensure_ascii=False),
            "result_count": int(payload.get("result_count") or 0),
            "estimated_total_count": int(payload.get("estimated_total_count") or 0),
            "provider_cap_hit": 1 if bool(payload.get("provider_cap_hit")) else 0,
            "source_path": _normalized_payload_text(payload, "source_path"),
            "source_job_id": _normalized_payload_text(payload, "source_job_id"),
            "materialization_generation_key": _normalized_payload_text(payload, "materialization_generation_key"),
            "materialization_generation_sequence": int(payload.get("materialization_generation_sequence") or 0),
            "materialization_watermark": _normalized_payload_text(payload, "materialization_watermark"),
            "metadata_json": json.dumps(_json_safe_payload(payload.get("metadata") or {}), ensure_ascii=False),
            "first_seen_at": _normalized_payload_text(
                payload,
                "first_seen_at",
                default=_normalize_textual_value((existing or {}).get("first_seen_at") or now),
            )
            or now,
            "last_completed_at": completed_at
            or _normalize_textual_value((existing or {}).get("last_completed_at") or ""),
            "created_at": _normalize_textual_value((existing or {}).get("created_at") or now) or now,
            "updated_at": now,
        }
        if self._write_control_plane_row_to_postgres("acquisition_shard_registry", row_payload):
            return self._select_control_plane_row(
                "acquisition_shard_registry",
                row_builder=self._acquisition_shard_registry_from_row,
                where_sql="shard_key = %s",
                params=[shard_key],
            ) or self._acquisition_shard_registry_from_row(row_payload)
        # Track B B4.1b: PG is authoritative. _write_control_plane_row_to_postgres returns True or
        # raises under postgres_only, so this is a fail-closed assertion — never the dead SQLite tail.
        self._raise_control_plane_postgres_write_failure(
            table_name="acquisition_shard_registry",
            method_name="upsert_acquisition_shard_registry",
            reason="native write did not confirm under postgres_only",
        )

    def list_acquisition_shard_registry(
        self,
        *,
        target_company: str = "",
        snapshot_ids: list[str] | None = None,
        lane: str = "",
        employment_scope: str = "",
        statuses: list[str] | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if self._control_plane_postgres_should_prefer_read("acquisition_shard_registry"):
            clauses: list[str] = []
            params: list[Any] = []
            if target_company:
                clauses.append("lower(target_company) = lower(%s)")
                params.append(target_company)
            normalized_snapshot_ids = [str(item).strip() for item in list(snapshot_ids or []) if str(item).strip()]
            if normalized_snapshot_ids:
                placeholders = ", ".join(["%s"] * len(normalized_snapshot_ids))
                clauses.append(f"snapshot_id IN ({placeholders})")
                params.extend(normalized_snapshot_ids)
            if lane:
                clauses.append("lane = %s")
                params.append(lane)
            if employment_scope:
                clauses.append("employment_scope = %s")
                params.append(employment_scope)
            normalized_statuses = [str(item).strip() for item in list(statuses or []) if str(item).strip()]
            if normalized_statuses:
                placeholders = ", ".join(["%s"] * len(normalized_statuses))
                clauses.append(f"status IN ({placeholders})")
                params.extend(normalized_statuses)
            rows = self._control_plane_postgres.select_many(
                "acquisition_shard_registry",
                where_sql=" AND ".join(clauses),
                params=params,
                order_by_sql="updated_at DESC, shard_key DESC",
                limit=limit,
            )
            if rows:
                return [self._acquisition_shard_registry_from_row(row) for row in rows]
            if self._control_plane_postgres_should_skip_sqlite_fallback("acquisition_shard_registry"):
                return []
        raise RuntimeError(
            "postgres-only invariant violated for acquisition_shard_registry in list_acquisition_shard_registry: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def _asset_membership_index_from_row(self, row: Any) -> dict[str, Any]:
        if row is None:
            return {}
        row_keys = set(row.keys()) if hasattr(row, "keys") else set(dict(row).keys())
        raw_metadata = row["metadata_json"] if "metadata_json" in row_keys else dict(row).get("metadata_json")
        try:
            metadata_payload = dict(json.loads(raw_metadata or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            metadata_payload = {}
        return {
            "generation_key": str(
                row["generation_key"] if "generation_key" in row_keys else dict(row).get("generation_key") or ""
            ),
            "target_company": str(
                row["target_company"] if "target_company" in row_keys else dict(row).get("target_company") or ""
            ),
            "snapshot_id": str(row["snapshot_id"] if "snapshot_id" in row_keys else dict(row).get("snapshot_id") or ""),
            "asset_view": str(row["asset_view"] if "asset_view" in row_keys else dict(row).get("asset_view") or ""),
            "artifact_kind": str(
                row["artifact_kind"] if "artifact_kind" in row_keys else dict(row).get("artifact_kind") or ""
            ),
            "artifact_key": str(
                row["artifact_key"] if "artifact_key" in row_keys else dict(row).get("artifact_key") or ""
            ),
            "lane": str(row["lane"] if "lane" in row_keys else dict(row).get("lane") or ""),
            "employment_scope": str(
                row["employment_scope"] if "employment_scope" in row_keys else dict(row).get("employment_scope") or ""
            ),
            "member_key": str(row["member_key"] if "member_key" in row_keys else dict(row).get("member_key") or ""),
            "member_key_kind": str(
                row["member_key_kind"] if "member_key_kind" in row_keys else dict(row).get("member_key_kind") or ""
            ),
            "candidate_id": str(
                row["candidate_id"] if "candidate_id" in row_keys else dict(row).get("candidate_id") or ""
            ),
            "profile_url_key": str(
                row["profile_url_key"] if "profile_url_key" in row_keys else dict(row).get("profile_url_key") or ""
            ),
            "metadata": metadata_payload,
            "created_at": str(row["created_at"] if "created_at" in row_keys else dict(row).get("created_at") or ""),
            "updated_at": str(row["updated_at"] if "updated_at" in row_keys else dict(row).get("updated_at") or ""),
        }

    def _select_asset_membership_rows_for_generation(self, generation_key: str) -> list[dict[str, Any]]:
        normalized_generation_key = str(generation_key or "").strip()
        if not normalized_generation_key:
            return []
        postgres_rows = self._select_control_plane_rows(
            "asset_membership_index",
            row_builder=self._asset_membership_index_from_row,
            where_sql="generation_key = %s",
            params=[normalized_generation_key],
            order_by_sql="member_key ASC",
            limit=0,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def _get_asset_materialization_generation_by_key(self, generation_key: str) -> dict[str, Any]:
        normalized_generation_key = str(generation_key or "").strip()
        if not normalized_generation_key:
            return {}
        postgres_row = self._select_control_plane_row(
            "asset_materialization_generations",
            row_builder=self._asset_materialization_generation_from_row,
            where_sql="generation_key = %s",
            params=[normalized_generation_key],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def _resolve_asset_membership_rows_for_generation(
        self,
        generation_key: str,
        *,
        _visited: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        normalized_generation_key = str(generation_key or "").strip()
        if not normalized_generation_key:
            return []
        visited = set(_visited or set())
        if normalized_generation_key in visited:
            return []
        visited.add(normalized_generation_key)
        generation = self._get_asset_materialization_generation_by_key(normalized_generation_key)
        if not generation:
            return self._select_asset_membership_rows_for_generation(normalized_generation_key)
        patch_metadata = dict(dict(generation.get("metadata") or {}).get("generation_patch") or {})
        if str(patch_metadata.get("mode") or "").strip() != "generation_patch":
            return self._select_asset_membership_rows_for_generation(normalized_generation_key)
        base_generation_key = str(patch_metadata.get("base_generation_key") or "").strip()
        resolved_rows = {
            str(row.get("member_key") or "").strip(): row
            for row in self._resolve_asset_membership_rows_for_generation(base_generation_key, _visited=visited)
            if str(row.get("member_key") or "").strip()
        }
        removed_member_keys = {
            str(item or "").strip()
            for item in list(patch_metadata.get("removed_member_keys") or [])
            if str(item or "").strip()
        }
        for member_key in removed_member_keys:
            resolved_rows.pop(member_key, None)
        for row in self._select_asset_membership_rows_for_generation(normalized_generation_key):
            member_key = str(row.get("member_key") or "").strip()
            if not member_key:
                continue
            resolved_rows[member_key] = row
        return [resolved_rows[member_key] for member_key in sorted(resolved_rows)]

    def _write_asset_membership_rows_to_postgres(
        self,
        *,
        generation_key: str,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        artifact_kind: str,
        artifact_key: str,
        members: list[dict[str, Any]],
        created_at: str,
    ) -> None:
        payload_rows = [
            {
                "generation_key": generation_key,
                "target_company": target_company,
                "snapshot_id": snapshot_id,
                "asset_view": asset_view,
                "artifact_kind": artifact_kind,
                "artifact_key": artifact_key,
                "lane": str(item.get("lane") or "").strip(),
                "employment_scope": _normalize_employment_scope(item.get("employment_scope")),
                "member_key": str(item.get("member_key") or "").strip(),
                "member_key_kind": str(item.get("member_key_kind") or "candidate_fallback").strip()
                or "candidate_fallback",
                "candidate_id": str(item.get("candidate_id") or "").strip(),
                "profile_url_key": str(item.get("profile_url_key") or "").strip(),
                "metadata_json": json.dumps(_json_safe_payload(item.get("metadata") or {}), ensure_ascii=False),
                "created_at": created_at,
                "updated_at": created_at,
            }
            for item in list(members or [])
            if str(item.get("member_key") or "").strip()
        ]
        if payload_rows:
            self._call_control_plane_postgres_native(
                "bulk_upsert_rows",
                table_name="asset_membership_index",
                rows=payload_rows,
            )

    def register_asset_materialization(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        artifact_kind: str,
        artifact_key: str,
        source_path: str = "",
        summary: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        members: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        normalized_artifact_kind = str(artifact_kind or "").strip()
        normalized_artifact_key = str(artifact_key or "").strip()
        if not normalized_target_company or not normalized_snapshot_id or not normalized_artifact_kind:
            return {}
        summary_payload = _json_safe_payload(dict(summary or {}))
        metadata_payload = _json_safe_payload(dict(metadata or {}))
        normalized_members = _normalize_asset_materialization_members(
            members,
            target_company=normalized_target_company,
            snapshot_id=normalized_snapshot_id,
            asset_view=normalized_asset_view,
            artifact_kind=normalized_artifact_kind,
            artifact_key=normalized_artifact_key,
        )
        payload_signature = sha1(
            json.dumps(summary_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:24]
        member_signature = _asset_materialization_member_signature(normalized_members)
        generation_key = sha1(
            "|".join(
                [
                    normalized_company_key or normalized_target_company.lower(),
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_artifact_kind,
                    normalized_artifact_key,
                    payload_signature,
                    member_signature,
                ]
            ).encode("utf-8")
        ).hexdigest()[:32]
        if self._control_plane_postgres_should_prefer_read(
            "asset_materialization_generations"
        ) and self._control_plane_postgres_should_prefer_read("asset_membership_index"):
            postgres_company_scope_clause, postgres_company_scope_params = _company_scope_predicate(
                normalized_target_company,
                normalized_company_key,
                placeholder="%s",
            )
            existing = self._select_control_plane_row(
                "asset_materialization_generations",
                row_builder=self._asset_materialization_generation_from_row,
                where_sql=(
                    f"{postgres_company_scope_clause} AND snapshot_id = %s AND asset_view = %s "
                    "AND artifact_kind = %s AND artifact_key = %s"
                ),
                params=[
                    *postgres_company_scope_params,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_artifact_kind,
                    normalized_artifact_key,
                ],
            )
            if existing:
                normalized_target_company = str(existing.get("target_company") or normalized_target_company).strip()
            previous_generation_key = str((existing or {}).get("generation_key") or "").strip()
            previous_sequence = int((existing or {}).get("generation_sequence") or 0)
            generation_sequence = (
                previous_sequence if previous_generation_key == generation_key else previous_sequence + 1
            )
            if generation_sequence <= 0:
                generation_sequence = 1
            now = _utc_now_timestamp()
            row_payload = {
                "target_company": normalized_target_company,
                "company_key": normalized_company_key,
                "snapshot_id": normalized_snapshot_id,
                "asset_view": normalized_asset_view,
                "artifact_kind": normalized_artifact_kind,
                "artifact_key": normalized_artifact_key,
                "generation_key": generation_key,
                "generation_sequence": generation_sequence,
                "source_path": str(source_path or "").strip(),
                "payload_signature": payload_signature,
                "member_signature": member_signature,
                "member_count": len(normalized_members),
                "summary_json": json.dumps(summary_payload, ensure_ascii=False),
                "metadata_json": json.dumps(metadata_payload, ensure_ascii=False),
                "created_at": str((existing or {}).get("created_at") or now),
                "updated_at": now,
            }
            if self._write_control_plane_row_to_postgres("asset_materialization_generations", row_payload):
                if previous_generation_key and previous_generation_key != generation_key:
                    self._call_control_plane_postgres_native(
                        "delete_rows",
                        table_name="asset_membership_index",
                        where_sql="generation_key = %s",
                        params=[previous_generation_key],
                    )
                self._call_control_plane_postgres_native(
                    "delete_rows",
                    table_name="asset_membership_index",
                    where_sql="generation_key = %s",
                    params=[generation_key],
                )
                self._write_asset_membership_rows_to_postgres(
                    generation_key=generation_key,
                    target_company=normalized_target_company,
                    snapshot_id=normalized_snapshot_id,
                    asset_view=normalized_asset_view,
                    artifact_kind=normalized_artifact_kind,
                    artifact_key=normalized_artifact_key,
                    members=normalized_members,
                    created_at=now,
                )
                return self.get_asset_materialization_generation(
                    target_company=normalized_target_company,
                    snapshot_id=normalized_snapshot_id,
                    asset_view=normalized_asset_view,
                    artifact_kind=normalized_artifact_kind,
                    artifact_key=normalized_artifact_key,
                )
            # postgres-only (B4): _write_control_plane_row_to_postgres returns True or raises; reaching
            # here means the authoritative write returned no confirmation, which is forbidden. The
            # legacy SQLite generation/membership tail below was retired (B4).
            self._raise_control_plane_postgres_write_failure(
                table_name="asset_materialization_generations",
                method_name="register_asset_materialization",
                reason="postgres-only: authoritative upsert returned no confirmation; SQLite fallback retired (B4)",
            )
        raise RuntimeError(
            "postgres-only invariant violated for asset_materialization_generations in register_asset_materialization: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def patch_asset_materialization(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        artifact_kind: str,
        artifact_key: str,
        base_generation_key: str,
        source_path: str = "",
        summary: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        members: list[dict[str, Any]] | None = None,
        removed_member_keys: list[str] | None = None,
    ) -> dict[str, Any]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        normalized_artifact_kind = str(artifact_kind or "").strip()
        normalized_artifact_key = str(artifact_key or "").strip()
        normalized_base_generation_key = str(base_generation_key or "").strip()
        if (
            not normalized_target_company
            or not normalized_snapshot_id
            or not normalized_artifact_kind
            or not normalized_base_generation_key
        ):
            return {}

        base_generation = self._get_asset_materialization_generation_by_key(normalized_base_generation_key)
        if not base_generation:
            return {}

        summary_payload = _json_safe_payload(dict(summary or {}))
        metadata_payload = _json_safe_payload(dict(metadata or {}))
        normalized_members = _normalize_asset_materialization_members(
            members,
            target_company=normalized_target_company,
            snapshot_id=normalized_snapshot_id,
            asset_view=normalized_asset_view,
            artifact_kind=normalized_artifact_kind,
            artifact_key=normalized_artifact_key,
        )
        normalized_removed_member_keys = sorted(
            {str(item or "").strip() for item in list(removed_member_keys or []) if str(item or "").strip()}
        )
        effective_rows = {
            str(row.get("member_key") or "").strip(): row
            for row in self._resolve_asset_membership_rows_for_generation(normalized_base_generation_key)
            if str(row.get("member_key") or "").strip()
        }
        for member_key in normalized_removed_member_keys:
            effective_rows.pop(member_key, None)
        for item in normalized_members:
            member_key = str(item.get("member_key") or "").strip()
            if not member_key:
                continue
            effective_rows[member_key] = {
                "generation_key": "",
                "target_company": normalized_target_company,
                "snapshot_id": normalized_snapshot_id,
                "asset_view": normalized_asset_view,
                "artifact_kind": normalized_artifact_kind,
                "artifact_key": normalized_artifact_key,
                "lane": str(item.get("lane") or "").strip(),
                "employment_scope": _normalize_employment_scope(item.get("employment_scope")),
                "member_key": member_key,
                "member_key_kind": str(item.get("member_key_kind") or "candidate_fallback").strip()
                or "candidate_fallback",
                "candidate_id": str(item.get("candidate_id") or "").strip(),
                "profile_url_key": str(item.get("profile_url_key") or "").strip(),
                "metadata": dict(item.get("metadata") or {}),
            }
        payload_signature = sha1(
            json.dumps(summary_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:24]
        patch_signature = sha1(
            json.dumps(
                {
                    "base_generation_key": normalized_base_generation_key,
                    "patch_members": _asset_materialization_member_projection(normalized_members),
                    "removed_member_keys": normalized_removed_member_keys,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:24]
        generation_key = sha1(
            "|".join(
                [
                    normalized_company_key or normalized_target_company.lower(),
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_artifact_kind,
                    normalized_artifact_key,
                    payload_signature,
                    normalized_base_generation_key,
                    patch_signature,
                ]
            ).encode("utf-8")
        ).hexdigest()[:32]
        patch_metadata = dict(dict(metadata_payload.get("generation_patch") or {}))
        patch_metadata.update(
            {
                "mode": "generation_patch",
                "base_generation_key": normalized_base_generation_key,
                "base_generation_sequence": int(base_generation.get("generation_sequence") or 0),
                "base_generation_watermark": str(base_generation.get("generation_watermark") or "").strip(),
                "patch_member_count": len(normalized_members),
                "removed_member_keys": normalized_removed_member_keys,
                "effective_member_count": len(effective_rows),
            }
        )
        metadata_payload["generation_patch"] = patch_metadata

        if self._control_plane_postgres_should_prefer_read(
            "asset_materialization_generations"
        ) and self._control_plane_postgres_should_prefer_read("asset_membership_index"):
            postgres_company_scope_clause, postgres_company_scope_params = _company_scope_predicate(
                normalized_target_company,
                normalized_company_key,
                placeholder="%s",
            )
            existing = self._select_control_plane_row(
                "asset_materialization_generations",
                row_builder=self._asset_materialization_generation_from_row,
                where_sql=(
                    f"{postgres_company_scope_clause} AND snapshot_id = %s AND asset_view = %s "
                    "AND artifact_kind = %s AND artifact_key = %s"
                ),
                params=[
                    *postgres_company_scope_params,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_artifact_kind,
                    normalized_artifact_key,
                ],
            )
            if existing:
                normalized_target_company = str(existing.get("target_company") or normalized_target_company).strip()
            previous_generation_key = str((existing or {}).get("generation_key") or "").strip()
            previous_sequence = int((existing or {}).get("generation_sequence") or 0)
            generation_sequence = (
                previous_sequence if previous_generation_key == generation_key else previous_sequence + 1
            )
            if generation_sequence <= 0:
                generation_sequence = 1
            now = _utc_now_timestamp()
            row_payload = {
                "target_company": normalized_target_company,
                "company_key": normalized_company_key,
                "snapshot_id": normalized_snapshot_id,
                "asset_view": normalized_asset_view,
                "artifact_kind": normalized_artifact_kind,
                "artifact_key": normalized_artifact_key,
                "generation_key": generation_key,
                "generation_sequence": generation_sequence,
                "source_path": str(source_path or "").strip(),
                "payload_signature": payload_signature,
                "member_signature": patch_signature,
                "member_count": len(effective_rows),
                "summary_json": json.dumps(summary_payload, ensure_ascii=False),
                "metadata_json": json.dumps(metadata_payload, ensure_ascii=False),
                "created_at": str((existing or {}).get("created_at") or now),
                "updated_at": now,
            }
            if self._write_control_plane_row_to_postgres("asset_materialization_generations", row_payload):
                if (
                    previous_generation_key
                    and previous_generation_key != generation_key
                    and previous_generation_key != normalized_base_generation_key
                ):
                    self._call_control_plane_postgres_native(
                        "delete_rows",
                        table_name="asset_membership_index",
                        where_sql="generation_key = %s",
                        params=[previous_generation_key],
                    )
                self._call_control_plane_postgres_native(
                    "delete_rows",
                    table_name="asset_membership_index",
                    where_sql="generation_key = %s",
                    params=[generation_key],
                )
                self._write_asset_membership_rows_to_postgres(
                    generation_key=generation_key,
                    target_company=normalized_target_company,
                    snapshot_id=normalized_snapshot_id,
                    asset_view=normalized_asset_view,
                    artifact_kind=normalized_artifact_kind,
                    artifact_key=normalized_artifact_key,
                    members=normalized_members,
                    created_at=now,
                )
                return self.get_asset_materialization_generation(
                    target_company=normalized_target_company,
                    snapshot_id=normalized_snapshot_id,
                    asset_view=normalized_asset_view,
                    artifact_kind=normalized_artifact_kind,
                    artifact_key=normalized_artifact_key,
                )
            # postgres-only (B4): _write_control_plane_row_to_postgres returns True or raises; reaching
            # here means the authoritative write returned no confirmation, which is forbidden. The
            # legacy SQLite generation/membership tail below was retired (B4).
            self._raise_control_plane_postgres_write_failure(
                table_name="asset_materialization_generations",
                method_name="patch_asset_materialization",
                reason="postgres-only: authoritative upsert returned no confirmation; SQLite fallback retired (B4)",
            )
        raise RuntimeError(
            "postgres-only invariant violated for asset_materialization_generations in patch_asset_materialization: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_asset_materialization_generation(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        artifact_kind: str,
        artifact_key: str,
    ) -> dict[str, Any]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        normalized_artifact_kind = str(artifact_kind or "").strip()
        normalized_artifact_key = str(artifact_key or "").strip()
        if not normalized_target_company or not normalized_snapshot_id or not normalized_artifact_kind:
            return {}
        postgres_company_scope_clause, postgres_company_scope_params = _company_scope_predicate(
            normalized_target_company,
            normalized_company_key,
            placeholder="%s",
        )
        postgres_row = self._select_control_plane_row(
            "asset_materialization_generations",
            row_builder=self._asset_materialization_generation_from_row,
            where_sql=(
                f"{postgres_company_scope_clause} AND snapshot_id = %s AND asset_view = %s "
                "AND artifact_kind = %s AND artifact_key = %s"
            ),
            params=[
                *postgres_company_scope_params,
                normalized_snapshot_id,
                normalized_asset_view,
                normalized_artifact_kind,
                normalized_artifact_key,
            ],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def summarize_asset_membership_index(
        self,
        *,
        generation_key: str = "",
        target_company: str = "",
        snapshot_id: str = "",
        asset_view: str = "",
        artifact_kind: str = "",
        artifact_key: str = "",
    ) -> dict[str, Any]:
        normalized_generation_key = str(generation_key or "").strip()
        if normalized_generation_key:
            return _asset_membership_summary_from_rows(
                self._resolve_asset_membership_rows_for_generation(normalized_generation_key)
            )
        if target_company and snapshot_id and artifact_kind:
            generation = self.get_asset_materialization_generation(
                target_company=target_company,
                snapshot_id=snapshot_id,
                asset_view=asset_view or "canonical_merged",
                artifact_kind=artifact_kind,
                artifact_key=artifact_key,
            )
            if generation:
                return _asset_membership_summary_from_rows(
                    self._resolve_asset_membership_rows_for_generation(
                        str(generation.get("generation_key") or "").strip()
                    )
                )
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            clauses.append("target_company = ?")
            params.append(str(target_company or "").strip())
        if snapshot_id:
            clauses.append("snapshot_id = ?")
            params.append(str(snapshot_id or "").strip())
        if asset_view:
            clauses.append("asset_view = ?")
            params.append(str(asset_view or "").strip())
        if artifact_kind:
            clauses.append("artifact_kind = ?")
            params.append(str(artifact_kind or "").strip())
        if artifact_key:
            clauses.append("artifact_key = ?")
            params.append(str(artifact_key or "").strip())
        if self._control_plane_postgres_should_prefer_read("asset_membership_index"):
            postgres_rows = self._select_control_plane_rows(
                "asset_membership_index",
                row_builder=self._asset_membership_index_from_row,
                where_sql=" AND ".join(clause.replace("?", "%s") for clause in clauses),
                params=params,
                limit=0,
            )
            if postgres_rows:
                return _asset_membership_summary_from_rows(postgres_rows)
            if self._control_plane_postgres_should_skip_sqlite_fallback("asset_membership_index"):
                return {
                    "member_count": 0,
                    "employment_scope_counts": {},
                    "lane_counts": {},
                    "member_key_kind_counts": {},
                }
        raise RuntimeError(
            "postgres-only invariant violated for asset_membership_index in summarize_asset_membership_index: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def upsert_candidate_materialization_state(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        candidate_id: str,
        fingerprint: str,
        shard_path: str = "",
        list_page: int = 0,
        dirty_reason: str = "",
        materialized_at: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        normalized_candidate_id = str(candidate_id or "").strip()
        normalized_fingerprint = str(fingerprint or "").strip()
        if not normalized_target_company or not normalized_snapshot_id or not normalized_candidate_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("candidate_materialization_state"):
            existing = self._select_control_plane_row(
                "candidate_materialization_state",
                row_builder=self._candidate_materialization_state_from_row,
                where_sql=(
                    f"{_company_scope_predicate(normalized_target_company, normalized_company_key, placeholder='%s')[0]}"
                    " AND snapshot_id = %s AND asset_view = %s AND candidate_id = %s"
                ),
                params=[
                    *_company_scope_predicate(
                        normalized_target_company,
                        normalized_company_key,
                        placeholder="%s",
                    )[1],
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_candidate_id,
                ],
            )
            if existing:
                normalized_target_company = str(existing.get("target_company") or normalized_target_company).strip()
            row_payload = {
                "target_company": normalized_target_company,
                "company_key": normalized_company_key,
                "snapshot_id": normalized_snapshot_id,
                "asset_view": normalized_asset_view,
                "candidate_id": normalized_candidate_id,
                "fingerprint": normalized_fingerprint,
                "shard_path": str(shard_path or "").strip(),
                "list_page": max(0, int(list_page or 0)),
                "dirty_reason": str(dirty_reason or "").strip(),
                "materialized_at": str(materialized_at or datetime.now(timezone.utc).isoformat()),
                "metadata_json": json.dumps(_json_safe_payload(metadata or {}), ensure_ascii=False),
                "created_at": str((existing or {}).get("created_at") or _utc_now_timestamp()),
                "updated_at": _utc_now_timestamp(),
            }
            if self._write_control_plane_row_to_postgres("candidate_materialization_state", row_payload):
                return self.get_candidate_materialization_state(
                    target_company=normalized_target_company,
                    snapshot_id=normalized_snapshot_id,
                    asset_view=normalized_asset_view,
                    candidate_id=normalized_candidate_id,
                )
            # postgres-only (B4): _write_control_plane_row_to_postgres returns True or raises; reaching
            # here means the authoritative write returned no confirmation, which is forbidden. The
            # legacy SQLite upsert/read-back tail below was retired (B4).
            self._raise_control_plane_postgres_write_failure(
                table_name="candidate_materialization_state",
                method_name="upsert_candidate_materialization_state",
                reason="postgres-only: authoritative upsert returned no confirmation; SQLite fallback retired (B4)",
            )
        raise RuntimeError(
            "postgres-only invariant violated for candidate_materialization_state in upsert_candidate_materialization_state: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def bulk_upsert_candidate_materialization_states(
        self,
        *,
        states: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> int:
        grouped_states: dict[tuple[str, str, str, str], dict[str, dict[str, Any]]] = {}
        for state in list(states or []):
            if not isinstance(state, dict):
                continue
            normalized_target_company, normalized_company_key = _normalized_company_scope(
                str(state.get("target_company") or ""),
                str(state.get("company_key") or ""),
            )
            normalized_snapshot_id = str(state.get("snapshot_id") or "").strip()
            normalized_asset_view = str(state.get("asset_view") or "canonical_merged").strip() or "canonical_merged"
            normalized_candidate_id = str(state.get("candidate_id") or "").strip()
            if not normalized_target_company or not normalized_snapshot_id or not normalized_candidate_id:
                continue
            normalized_group_key = (
                normalized_target_company,
                normalized_company_key,
                normalized_snapshot_id,
                normalized_asset_view,
            )
            grouped_states.setdefault(normalized_group_key, {})[normalized_candidate_id] = {
                "target_company": normalized_target_company,
                "company_key": normalized_company_key,
                "snapshot_id": normalized_snapshot_id,
                "asset_view": normalized_asset_view,
                "candidate_id": normalized_candidate_id,
                "fingerprint": str(state.get("fingerprint") or "").strip(),
                "shard_path": str(state.get("shard_path") or "").strip(),
                "list_page": max(0, int(state.get("list_page") or 0)),
                "dirty_reason": str(state.get("dirty_reason") or "").strip(),
                "materialized_at": str(state.get("materialized_at") or datetime.now(timezone.utc).isoformat()),
                "metadata_json": json.dumps(_json_safe_payload(state.get("metadata") or {}), ensure_ascii=False),
            }
        if not grouped_states:
            return 0

        if self._control_plane_postgres_should_prefer_read("candidate_materialization_state"):
            payload_rows: list[dict[str, Any]] = []
            for (
                normalized_target_company,
                normalized_company_key,
                normalized_snapshot_id,
                normalized_asset_view,
            ), grouped_rows in grouped_states.items():
                postgres_company_scope_clause, postgres_company_scope_params = _company_scope_predicate(
                    normalized_target_company,
                    normalized_company_key,
                    placeholder="%s",
                )
                existing_rows = self._select_control_plane_rows(
                    "candidate_materialization_state",
                    row_builder=self._candidate_materialization_state_from_row,
                    where_sql=f"{postgres_company_scope_clause} AND snapshot_id = %s AND asset_view = %s",
                    params=[
                        *postgres_company_scope_params,
                        normalized_snapshot_id,
                        normalized_asset_view,
                    ],
                    order_by_sql="candidate_id ASC",
                    limit=0,
                )
                existing_by_candidate_id = {
                    str(item.get("candidate_id") or "").strip(): item
                    for item in existing_rows
                    if str(item.get("candidate_id") or "").strip()
                }
                for candidate_id, grouped_row in grouped_rows.items():
                    existing = dict(existing_by_candidate_id.get(candidate_id) or {})
                    now = _utc_now_timestamp()
                    payload_rows.append(
                        {
                            "target_company": str(existing.get("target_company") or normalized_target_company).strip(),
                            "company_key": normalized_company_key,
                            "snapshot_id": normalized_snapshot_id,
                            "asset_view": normalized_asset_view,
                            "candidate_id": candidate_id,
                            "fingerprint": str(grouped_row.get("fingerprint") or "").strip(),
                            "shard_path": str(grouped_row.get("shard_path") or "").strip(),
                            "list_page": max(0, int(grouped_row.get("list_page") or 0)),
                            "dirty_reason": str(grouped_row.get("dirty_reason") or "").strip(),
                            "materialized_at": str(
                                grouped_row.get("materialized_at") or datetime.now(timezone.utc).isoformat()
                            ),
                            "metadata_json": str(grouped_row.get("metadata_json") or "{}"),
                            "created_at": str(existing.get("created_at") or now),
                            "updated_at": now,
                        }
                    )
            if payload_rows:
                return int(
                    self._call_control_plane_postgres_native(
                        "bulk_upsert_rows",
                        table_name="candidate_materialization_state",
                        rows=payload_rows,
                    )
                    or 0
                )
            return 0
        raise RuntimeError(
            "postgres-only invariant violated for candidate_materialization_state in bulk_upsert_candidate_materialization_states: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def replace_candidate_materialization_state_scope(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        states: list[dict[str, Any]] | tuple[dict[str, Any], ...],
        existing_states_by_candidate_id: dict[str, dict[str, Any]] | None = None,
    ) -> int:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        if not normalized_target_company or not normalized_snapshot_id:
            return 0

        grouped_rows: dict[str, dict[str, Any]] = {}
        for state in list(states or []):
            if not isinstance(state, dict):
                continue
            normalized_candidate_id = str(state.get("candidate_id") or "").strip()
            if not normalized_candidate_id:
                continue
            grouped_rows[normalized_candidate_id] = {
                "fingerprint": str(state.get("fingerprint") or "").strip(),
                "shard_path": str(state.get("shard_path") or "").strip(),
                "list_page": max(0, int(state.get("list_page") or 0)),
                "dirty_reason": str(state.get("dirty_reason") or "").strip(),
                "materialized_at": str(state.get("materialized_at") or datetime.now(timezone.utc).isoformat()),
                "metadata_json": json.dumps(_json_safe_payload(state.get("metadata") or {}), ensure_ascii=False),
            }

        normalized_existing_states = {
            str(candidate_id or "").strip(): dict(state or {})
            for candidate_id, state in dict(existing_states_by_candidate_id or {}).items()
            if str(candidate_id or "").strip()
        }
        if not normalized_existing_states:
            if self._control_plane_postgres_should_prefer_read("candidate_materialization_state"):
                postgres_company_scope_clause, postgres_company_scope_params = _company_scope_predicate(
                    normalized_target_company,
                    normalized_company_key,
                    placeholder="%s",
                )
                existing_rows = self._select_control_plane_rows(
                    "candidate_materialization_state",
                    row_builder=self._candidate_materialization_state_from_row,
                    where_sql=f"{postgres_company_scope_clause} AND snapshot_id = %s AND asset_view = %s",
                    params=[
                        *postgres_company_scope_params,
                        normalized_snapshot_id,
                        normalized_asset_view,
                    ],
                    order_by_sql="candidate_id ASC",
                    limit=0,
                )
            else:
                raise RuntimeError(
                    "postgres-only invariant violated for candidate_materialization_state in "
                    "replace_candidate_materialization_state_scope: should_prefer_read "
                    "returned False; legacy SQLite tail retired (B4)"
                )
            normalized_existing_states = {
                str(item.get("candidate_id") or "").strip(): item
                for item in existing_rows
                if str(item.get("candidate_id") or "").strip()
            }
        now = _utc_now_timestamp()

        if self._control_plane_postgres_should_prefer_read("candidate_materialization_state"):
            payload_rows = []
            for candidate_id, grouped_row in grouped_rows.items():
                existing = dict(normalized_existing_states.get(candidate_id) or {})
                payload_rows.append(
                    {
                        "target_company": str(existing.get("target_company") or normalized_target_company).strip(),
                        "company_key": normalized_company_key,
                        "snapshot_id": normalized_snapshot_id,
                        "asset_view": normalized_asset_view,
                        "candidate_id": candidate_id,
                        "fingerprint": str(grouped_row.get("fingerprint") or "").strip(),
                        "shard_path": str(grouped_row.get("shard_path") or "").strip(),
                        "list_page": max(0, int(grouped_row.get("list_page") or 0)),
                        "dirty_reason": str(grouped_row.get("dirty_reason") or "").strip(),
                        "materialized_at": str(
                            grouped_row.get("materialized_at") or datetime.now(timezone.utc).isoformat()
                        ),
                        "metadata_json": str(grouped_row.get("metadata_json") or "{}"),
                        "created_at": str(existing.get("created_at") or now),
                        "updated_at": now,
                    }
                )
            return int(
                self._call_control_plane_postgres_native(
                    "replace_candidate_materialization_state_scope",
                    table_name="candidate_materialization_state",
                    target_company=normalized_target_company,
                    company_key=normalized_company_key,
                    snapshot_id=normalized_snapshot_id,
                    asset_view=normalized_asset_view,
                    rows=payload_rows,
                )
                or 0
            )
        raise RuntimeError(
            "postgres-only invariant violated for candidate_materialization_state in replace_candidate_materialization_state_scope: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_candidate_materialization_state(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        candidate_id: str,
    ) -> dict[str, Any]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        normalized_candidate_id = str(candidate_id or "").strip()
        if not normalized_target_company or not normalized_snapshot_id or not normalized_candidate_id:
            return {}
        postgres_company_scope_clause, postgres_company_scope_params = _company_scope_predicate(
            normalized_target_company,
            normalized_company_key,
            placeholder="%s",
        )
        postgres_row = self._select_control_plane_row(
            "candidate_materialization_state",
            row_builder=self._candidate_materialization_state_from_row,
            where_sql=(
                f"{postgres_company_scope_clause} AND snapshot_id = %s AND asset_view = %s AND candidate_id = %s"
            ),
            params=[
                *postgres_company_scope_params,
                normalized_snapshot_id,
                normalized_asset_view,
                normalized_candidate_id,
            ],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_candidate_materialization_states(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
    ) -> list[dict[str, Any]]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        if not normalized_target_company or not normalized_snapshot_id:
            return []
        postgres_company_scope_clause, postgres_company_scope_params = _company_scope_predicate(
            normalized_target_company,
            normalized_company_key,
            placeholder="%s",
        )
        postgres_rows = self._select_control_plane_rows(
            "candidate_materialization_state",
            row_builder=self._candidate_materialization_state_from_row,
            where_sql=f"{postgres_company_scope_clause} AND snapshot_id = %s AND asset_view = %s",
            params=[
                *postgres_company_scope_params,
                normalized_snapshot_id,
                normalized_asset_view,
            ],
            order_by_sql="list_page ASC, candidate_id ASC",
            limit=0,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def prune_candidate_materialization_states(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        active_candidate_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        active_ids = sorted(
            {str(item or "").strip() for item in list(active_candidate_ids or []) if str(item or "").strip()}
        )
        if not normalized_target_company or not normalized_snapshot_id:
            return []
        if self._control_plane_postgres_should_prefer_read("candidate_materialization_state"):
            clauses = []
            params: list[Any] = []
            company_scope_clause, company_scope_params = _company_scope_predicate(
                normalized_target_company,
                normalized_company_key,
                placeholder="%s",
            )
            clauses.extend([company_scope_clause, "snapshot_id = %s", "asset_view = %s"])
            params.extend([*company_scope_params, normalized_snapshot_id, normalized_asset_view])
            if active_ids:
                placeholders = ",".join("%s" for _ in active_ids)
                clauses.append(f"candidate_id NOT IN ({placeholders})")
                params.extend(active_ids)
            rows = self._select_control_plane_rows(
                "candidate_materialization_state",
                row_builder=self._candidate_materialization_state_from_row,
                where_sql=" AND ".join(clauses),
                params=params,
                order_by_sql="candidate_id ASC",
                limit=0,
            )
            if rows:
                self._call_control_plane_postgres_native(
                    "delete_rows",
                    table_name="candidate_materialization_state",
                    where_sql=" AND ".join(clauses),
                    params=params,
                )
            return rows
        raise RuntimeError(
            "postgres-only invariant violated for candidate_materialization_state in prune_candidate_materialization_states: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def start_snapshot_materialization_run(
        self,
        *,
        run_id: str,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_run_id = str(run_id or "").strip()
        normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
        normalized_snapshot_id = str(snapshot_id or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        if not normalized_run_id or not normalized_target_company or not normalized_snapshot_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("snapshot_materialization_runs"):
            existing = self.get_snapshot_materialization_run(normalized_run_id)
            row_payload = {
                "run_id": normalized_run_id,
                "target_company": normalized_target_company,
                "company_key": normalized_company_key,
                "snapshot_id": normalized_snapshot_id,
                "asset_view": normalized_asset_view,
                "status": "running",
                "dirty_candidate_count": 0,
                "completed_candidate_count": 0,
                "reused_candidate_count": 0,
                "summary_json": json.dumps(_json_safe_payload(summary or {}), ensure_ascii=False),
                "started_at": str((existing or {}).get("started_at") or _utc_now_timestamp()),
                "completed_at": "",
                "created_at": str((existing or {}).get("created_at") or _utc_now_timestamp()),
                "updated_at": _utc_now_timestamp(),
            }
            if self._write_control_plane_row_to_postgres("snapshot_materialization_runs", row_payload):
                return self.get_snapshot_materialization_run(normalized_run_id)
            # postgres-only (B4): _write_control_plane_row_to_postgres returns True or raises; reaching
            # here means the authoritative write returned no confirmation, which is forbidden. The
            # legacy SQLite upsert/read-back tail below was retired (B4).
            self._raise_control_plane_postgres_write_failure(
                table_name="snapshot_materialization_runs",
                method_name="start_snapshot_materialization_run",
                reason="postgres-only: authoritative upsert returned no confirmation; SQLite fallback retired (B4)",
            )
        raise RuntimeError(
            "postgres-only invariant violated for snapshot_materialization_runs in start_snapshot_materialization_run: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def complete_snapshot_materialization_run(
        self,
        *,
        run_id: str,
        status: str,
        dirty_candidate_count: int = 0,
        completed_candidate_count: int = 0,
        reused_candidate_count: int = 0,
        summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return {}
        if self._control_plane_postgres_should_prefer_read("snapshot_materialization_runs"):
            existing = self.get_snapshot_materialization_run(normalized_run_id)
            if not existing:
                return {}
            row_payload = {
                "run_id": normalized_run_id,
                "target_company": str(existing.get("target_company") or ""),
                "company_key": str(existing.get("company_key") or ""),
                "snapshot_id": str(existing.get("snapshot_id") or ""),
                "asset_view": str(existing.get("asset_view") or "canonical_merged"),
                "status": str(status or "completed").strip() or "completed",
                "dirty_candidate_count": max(0, int(dirty_candidate_count or 0)),
                "completed_candidate_count": max(0, int(completed_candidate_count or 0)),
                "reused_candidate_count": max(0, int(reused_candidate_count or 0)),
                "summary_json": json.dumps(_json_safe_payload(summary or {}), ensure_ascii=False),
                "started_at": str(existing.get("started_at") or ""),
                "completed_at": _utc_now_timestamp(),
                "created_at": str(existing.get("created_at") or _utc_now_timestamp()),
                "updated_at": _utc_now_timestamp(),
            }
            if self._write_control_plane_row_to_postgres("snapshot_materialization_runs", row_payload):
                return self.get_snapshot_materialization_run(normalized_run_id)
            # postgres-only (B4): _write_control_plane_row_to_postgres returns True or raises; reaching
            # here means the authoritative write returned no confirmation, which is forbidden. The
            # legacy SQLite update/read-back tail below was retired (B4).
            self._raise_control_plane_postgres_write_failure(
                table_name="snapshot_materialization_runs",
                method_name="complete_snapshot_materialization_run",
                reason="postgres-only: authoritative upsert returned no confirmation; SQLite fallback retired (B4)",
            )
        raise RuntimeError(
            "postgres-only invariant violated for snapshot_materialization_runs in complete_snapshot_materialization_run: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_snapshot_materialization_run(self, run_id: str) -> dict[str, Any]:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return {}
        postgres_row = self._select_control_plane_row(
            "snapshot_materialization_runs",
            row_builder=self._snapshot_materialization_run_from_row,
            where_sql="run_id = %s",
            params=[normalized_run_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def compare_asset_membership_generations(
        self,
        *,
        primary_generation_key: str,
        secondary_generation_key: str,
        primary_employment_scope: str = "",
        secondary_employment_scope: str = "",
        primary_lane: str = "",
        secondary_lane: str = "",
    ) -> dict[str, Any]:
        normalized_primary_generation_key = str(primary_generation_key or "").strip()
        normalized_secondary_generation_key = str(secondary_generation_key or "").strip()
        if not normalized_primary_generation_key or not normalized_secondary_generation_key:
            return {}
        normalized_primary_scope = _normalize_employment_scope(primary_employment_scope)
        normalized_secondary_scope = _normalize_employment_scope(secondary_employment_scope)
        normalized_primary_lane = str(primary_lane or "").strip()
        normalized_secondary_lane = str(secondary_lane or "").strip()

        def _member_keys_for_generation(
            *,
            generation_key: str,
            employment_scope: str,
            lane: str,
        ) -> set[str]:
            rows = self._resolve_asset_membership_rows_for_generation(generation_key)
            member_keys: set[str] = set()
            for row in rows:
                member_key = str(row.get("member_key") or "").strip()
                if not member_key:
                    continue
                if employment_scope and str(row.get("employment_scope") or "").strip() != employment_scope:
                    continue
                if lane and str(row.get("lane") or "").strip() != lane:
                    continue
                member_keys.add(member_key)
            return member_keys

        primary_member_keys = _member_keys_for_generation(
            generation_key=normalized_primary_generation_key,
            employment_scope=normalized_primary_scope,
            lane=normalized_primary_lane,
        )
        secondary_member_keys = _member_keys_for_generation(
            generation_key=normalized_secondary_generation_key,
            employment_scope=normalized_secondary_scope,
            lane=normalized_secondary_lane,
        )
        overlap_count = len(primary_member_keys & secondary_member_keys)
        primary_count = len(primary_member_keys)
        secondary_count = len(secondary_member_keys)

        primary_only_count = max(primary_count - overlap_count, 0)
        secondary_only_count = max(secondary_count - overlap_count, 0)
        primary_overlap_ratio = round(overlap_count / primary_count, 4) if primary_count > 0 else 0.0
        secondary_overlap_ratio = round(overlap_count / secondary_count, 4) if secondary_count > 0 else 0.0
        primary_subsumes_secondary = bool(secondary_count > 0 and overlap_count >= secondary_count)
        secondary_subsumes_primary = bool(primary_count > 0 and overlap_count >= primary_count)
        return {
            "comparison_available": True,
            "primary_generation_key": normalized_primary_generation_key,
            "secondary_generation_key": normalized_secondary_generation_key,
            "primary_employment_scope": normalized_primary_scope,
            "secondary_employment_scope": normalized_secondary_scope,
            "primary_lane": normalized_primary_lane,
            "secondary_lane": normalized_secondary_lane,
            "primary_member_count": primary_count,
            "secondary_member_count": secondary_count,
            "overlap_member_count": overlap_count,
            "primary_only_member_count": primary_only_count,
            "secondary_only_member_count": secondary_only_count,
            "primary_overlap_ratio": primary_overlap_ratio,
            "secondary_overlap_ratio": secondary_overlap_ratio,
            "primary_subsumes_secondary": primary_subsumes_secondary,
            "secondary_subsumes_primary": secondary_subsumes_primary,
        }

    def record_cloud_asset_operation(
        self,
        *,
        operation_type: str,
        bundle_kind: str,
        bundle_id: str,
        status: str,
        sync_run_id: str = "",
        manifest_path: str = "",
        target_runtime_dir: str = "",
        target_db_path: str = "",
        scoped_companies: list[str] | None = None,
        scoped_snapshot_id: str = "",
        summary: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._control_plane_postgres_should_prefer_read("cloud_asset_operation_ledger"):
            now = _utc_now_timestamp()
            row = self._call_control_plane_postgres_native(
                "insert_row_with_generated_id",
                table_name="cloud_asset_operation_ledger",
                row={
                    "operation_type": str(operation_type or "").strip(),
                    "bundle_kind": str(bundle_kind or "").strip(),
                    "bundle_id": str(bundle_id or "").strip(),
                    "sync_run_id": str(sync_run_id or "").strip(),
                    "status": str(status or "").strip(),
                    "manifest_path": str(manifest_path or "").strip(),
                    "target_runtime_dir": str(target_runtime_dir or "").strip(),
                    "target_db_path": str(target_db_path or "").strip(),
                    "scoped_companies_json": json.dumps(_json_safe_payload(scoped_companies or []), ensure_ascii=False),
                    "scoped_snapshot_id": str(scoped_snapshot_id or "").strip(),
                    "summary_json": json.dumps(_json_safe_payload(summary or {}), ensure_ascii=False),
                    "metadata_json": json.dumps(_json_safe_payload(metadata or {}), ensure_ascii=False),
                    "created_at": now,
                    "updated_at": now,
                },
            )
            if row is not None:
                return self._cloud_asset_operation_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("cloud_asset_operation_ledger"):
                # Track B B4.1b: PG is authoritative. The native insert is a plain sequence-id
                # INSERT...RETURNING (no ON CONFLICT) — a None is unreachable for valid input; fail closed
                # rather than fall through to the dead SQLite shadow.
                self._raise_control_plane_postgres_write_failure(
                    table_name="cloud_asset_operation_ledger",
                    method_name="insert_row_with_generated_id",
                    reason="native insert returned no row under postgres_only",
                )
        raise RuntimeError(
            "postgres-only invariant violated for cloud_asset_operation_ledger in record_cloud_asset_operation: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_cloud_asset_operations(
        self,
        *,
        operation_types: list[str] | None = None,
        bundle_kind: str = "",
        status: str = "",
        scoped_company: str = "",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        normalized_limit = max(1, int(limit or 20))
        if self._control_plane_postgres_should_prefer_read("cloud_asset_operation_ledger"):
            clauses: list[str] = []
            params: list[Any] = []
            normalized_operation_types = [
                str(item or "").strip() for item in list(operation_types or []) if str(item or "").strip()
            ]
            if normalized_operation_types:
                placeholders = ", ".join(["%s"] * len(normalized_operation_types))
                clauses.append(f"operation_type IN ({placeholders})")
                params.extend(normalized_operation_types)
            if bundle_kind:
                clauses.append("bundle_kind = %s")
                params.append(str(bundle_kind or "").strip())
            if status:
                clauses.append("status = %s")
                params.append(str(status or "").strip())
            fetch_limit = normalized_limit if not scoped_company else max(normalized_limit * 5, normalized_limit)
            rows = self._control_plane_postgres.select_many(
                "cloud_asset_operation_ledger",
                where_sql=" AND ".join(clauses),
                params=params,
                order_by_sql="ledger_id DESC",
                limit=fetch_limit,
            )
            if rows:
                entries = [self._cloud_asset_operation_from_row(row) for row in rows]
                if scoped_company:
                    normalized_company = str(scoped_company or "").strip().lower()
                    entries = [
                        entry
                        for entry in entries
                        if normalized_company
                        in {
                            str(item or "").strip().lower()
                            for item in list(entry.get("scoped_companies") or [])
                            if str(item or "").strip()
                        }
                    ]
                return entries[:normalized_limit]
            if self._control_plane_postgres_should_skip_sqlite_fallback("cloud_asset_operation_ledger"):
                return []
        raise RuntimeError(
            "postgres-only invariant violated for cloud_asset_operation_ledger in list_cloud_asset_operations: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def acquire_runtime_provider_limiter_slot(
        self,
        limiter_key: str,
        *,
        lease_owner: str,
        budget: int,
        lease_seconds: int = 7200,
        lease_token: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_key = str(limiter_key or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        normalized_budget = max(1, int(budget or 1))
        if not normalized_key or not normalized_owner:
            return {
                "acquired": False,
                "limiter_key": normalized_key,
                "lease_owner": normalized_owner,
                "lease_token": "",
                "active_count": 0,
                "budget": normalized_budget,
                "db_limiter_enabled": True,
            }
        normalized_token = str(lease_token or "").strip() or f"lease_{uuid4().hex}"
        ttl_seconds = max(5, int(lease_seconds or 0))
        metadata_payload = dict(metadata or {})
        if self._control_plane_postgres_should_prefer_read("runtime_provider_limiter_leases"):
            native_acquire = getattr(self._control_plane_postgres, "acquire_runtime_provider_limiter_slot", None)
            if callable(native_acquire):
                try:
                    native_payload = native_acquire(
                        normalized_key,
                        lease_owner=normalized_owner,
                        budget=normalized_budget,
                        lease_seconds=ttl_seconds,
                        lease_token=normalized_token,
                        metadata=metadata_payload,
                    )
                except Exception as exc:
                    if self._control_plane_postgres_should_skip_sqlite_fallback("runtime_provider_limiter_leases"):
                        self._raise_control_plane_postgres_write_failure(
                            table_name="runtime_provider_limiter_leases",
                            method_name="acquire_runtime_provider_limiter_slot",
                            reason=f"{type(exc).__name__}: {exc}",
                            error=exc,
                        )
                    native_payload = None
                if native_payload is not None:
                    return {
                        **dict(native_payload),
                        "acquired": bool(dict(native_payload).get("acquired")),
                        "limiter_key": normalized_key,
                        "lease_owner": normalized_owner,
                        "lease_token": normalized_token,
                        "budget": normalized_budget,
                        "db_limiter_enabled": True,
                    }
            if self._control_plane_postgres_should_skip_sqlite_fallback("runtime_provider_limiter_leases"):
                return {
                    "acquired": False,
                    "limiter_key": normalized_key,
                    "lease_owner": normalized_owner,
                    "lease_token": normalized_token,
                    "active_count": normalized_budget,
                    "budget": normalized_budget,
                    "db_limiter_enabled": True,
                    "reason": "postgres_native_limiter_unavailable",
                }
        raise RuntimeError(
            "postgres-only invariant violated for runtime_provider_limiter_leases in acquire_runtime_provider_limiter_slot: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_runtime_provider_limiter_status(
        self,
        limiter_key: str,
        *,
        budget: int,
    ) -> dict[str, Any]:
        normalized_key = str(limiter_key or "").strip()
        normalized_budget = max(1, int(budget or 1))
        if not normalized_key:
            return {
                "limiter_key": normalized_key,
                "active_count": 0,
                "budget": normalized_budget,
                "available_count": normalized_budget,
                "available": True,
                "db_limiter_enabled": True,
                "reason": "limiter_key_missing",
            }
        if self._control_plane_postgres_should_prefer_read("runtime_provider_limiter_leases"):
            native_status = getattr(
                self._control_plane_postgres,
                "get_runtime_provider_limiter_status",
                None,
            )
            if callable(native_status):
                try:
                    native_payload = native_status(normalized_key, budget=normalized_budget)
                except Exception as exc:
                    if self._control_plane_postgres_should_skip_sqlite_fallback("runtime_provider_limiter_leases"):
                        self._raise_control_plane_postgres_write_failure(
                            table_name="runtime_provider_limiter_leases",
                            method_name="get_runtime_provider_limiter_status",
                            reason=f"{type(exc).__name__}: {exc}",
                            error=exc,
                        )
                    native_payload = None
                if native_payload is not None:
                    payload = dict(native_payload)
                    active_count = max(0, int(payload.get("active_count") or 0))
                    available_count = max(0, normalized_budget - active_count)
                    return {
                        **payload,
                        "limiter_key": normalized_key,
                        "active_count": active_count,
                        "budget": normalized_budget,
                        "available_count": available_count,
                        "available": available_count > 0,
                        "db_limiter_enabled": True,
                    }
            if self._control_plane_postgres_should_skip_sqlite_fallback("runtime_provider_limiter_leases"):
                return {
                    "limiter_key": normalized_key,
                    "active_count": normalized_budget,
                    "budget": normalized_budget,
                    "available_count": 0,
                    "available": False,
                    "db_limiter_enabled": True,
                    "reason": "postgres_native_limiter_status_unavailable",
                }
        raise RuntimeError(
            "postgres-only invariant violated for runtime_provider_limiter_leases in get_runtime_provider_limiter_status: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def release_runtime_provider_limiter_slot(
        self,
        lease_token: str,
        *,
        limiter_key: str = "",
        lease_owner: str = "",
    ) -> bool:
        normalized_token = str(lease_token or "").strip()
        if not normalized_token:
            return False
        normalized_key = str(limiter_key or "").strip()
        normalized_owner = str(lease_owner or "").strip()
        if self._control_plane_postgres_should_prefer_read("runtime_provider_limiter_leases"):
            native_release = getattr(self._control_plane_postgres, "release_runtime_provider_limiter_slot", None)
            if callable(native_release):
                try:
                    deleted = native_release(
                        normalized_token,
                        limiter_key=normalized_key,
                        lease_owner=normalized_owner,
                    )
                except Exception as exc:
                    if self._control_plane_postgres_should_skip_sqlite_fallback("runtime_provider_limiter_leases"):
                        self._raise_control_plane_postgres_write_failure(
                            table_name="runtime_provider_limiter_leases",
                            method_name="release_runtime_provider_limiter_slot",
                            reason=f"{type(exc).__name__}: {exc}",
                            error=exc,
                        )
                    deleted = False
                if deleted or self._control_plane_postgres_should_skip_sqlite_fallback(
                    "runtime_provider_limiter_leases"
                ):
                    return bool(deleted)
        raise RuntimeError(
            "postgres-only invariant violated for runtime_provider_limiter_leases in release_runtime_provider_limiter_slot: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def find_pending_plan_review_session(
        self,
        *,
        target_company: str,
        request_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        target = str(target_company or "").strip()
        if not target:
            return None
        request_sig = matching_request_signature(request_payload)
        # PG-authoritative read (Track B B2): the SQLite plan_review_sessions shadow is empty
        # in postgres_only, so this pending-session dedup probe must read PG. Reading the shadow
        # returned None and silently defeated dedup (duplicate pending plan-review sessions).
        postgres_row = self._select_control_plane_row(
            "plan_review_sessions",
            row_builder=self._plan_review_session_from_row,
            where_sql=(
                "lower(target_company) = lower(%s) AND status = 'pending' AND ("
                "matching_request_signature = %s "
                "OR (coalesce(matching_request_signature, '') = '' AND request_signature = %s))"
            ),
            params=[target, request_sig, request_sig],
            order_by_sql="updated_at DESC, review_id DESC",
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def _job_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        request_payload = {}
        plan_payload = {}
        execution_bundle_payload = {}
        matching_request_payload = {}
        summary_payload = {}
        try:
            request_payload = json.loads(row["request_json"] or "{}")
        except json.JSONDecodeError:
            request_payload = {}
        try:
            plan_payload = json.loads(row["plan_json"] or "{}")
        except json.JSONDecodeError:
            plan_payload = {}
        try:
            execution_bundle_payload = json.loads(row["execution_bundle_json"] or "{}")
        except json.JSONDecodeError:
            execution_bundle_payload = {}
        try:
            matching_request_payload = json.loads(row["matching_request_json"] or "{}")
        except json.JSONDecodeError:
            matching_request_payload = {}
        try:
            summary_payload = json.loads(row["summary_json"] or "{}")
        except json.JSONDecodeError:
            summary_payload = {}
        return {
            "job_id": row["job_id"],
            "job_type": row["job_type"],
            "status": row["status"],
            "stage": row["stage"],
            "request": request_payload,
            "plan": plan_payload,
            "execution_bundle": execution_bundle_payload,
            "request_matching": matching_request_payload,
            "summary": summary_payload,
            "artifact_path": row["artifact_path"],
            "request_signature": str(row["request_signature"] or ""),
            "request_family_signature": str(row["request_family_signature"] or ""),
            "matching_request_signature": str(row["matching_request_signature"] or ""),
            "matching_request_family_signature": str(row["matching_request_family_signature"] or ""),
            "requester_id": str(row["requester_id"] or ""),
            "tenant_id": str(row["tenant_id"] or ""),
            "idempotency_key": str(row["idempotency_key"] or ""),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _job_result_view_from_row(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        summary_payload = {}
        metadata_payload = {}
        try:
            summary_payload = json.loads(row["summary_json"] or "{}")
        except json.JSONDecodeError:
            summary_payload = {}
        try:
            metadata_payload = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata_payload = {}
        return {
            "view_id": str(row["view_id"] or ""),
            "job_id": str(row["job_id"] or ""),
            "target_company": str(row["target_company"] or ""),
            "company_key": str(row["company_key"] or ""),
            "source_kind": str(row["source_kind"] or ""),
            "view_kind": str(row["view_kind"] or ""),
            "snapshot_id": str(row["snapshot_id"] or ""),
            "asset_view": str(row["asset_view"] or ""),
            "source_path": str(row["source_path"] or ""),
            "authoritative_snapshot_id": str(row["authoritative_snapshot_id"] or ""),
            "materialization_generation_key": str(row["materialization_generation_key"] or ""),
            "request_signature": str(row["request_signature"] or ""),
            "summary": summary_payload,
            "metadata": metadata_payload,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _query_dispatch_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = {}
        matching_request_payload = {}
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except json.JSONDecodeError:
            payload = {}
        try:
            matching_request_payload = json.loads(row["matching_request_json"] or "{}")
        except json.JSONDecodeError:
            matching_request_payload = {}
        return {
            "dispatch_id": int(row["dispatch_id"] or 0),
            "target_company": str(row["target_company"] or ""),
            "request_signature": str(row["request_signature"] or ""),
            "request_family_signature": str(row["request_family_signature"] or ""),
            "matching_request_signature": str(row["matching_request_signature"] or ""),
            "matching_request_family_signature": str(row["matching_request_family_signature"] or ""),
            "requester_id": str(row["requester_id"] or ""),
            "tenant_id": str(row["tenant_id"] or ""),
            "idempotency_key": str(row["idempotency_key"] or ""),
            "strategy": str(row["strategy"] or ""),
            "status": str(row["status"] or ""),
            "source_job_id": str(row["source_job_id"] or ""),
            "created_job_id": str(row["created_job_id"] or ""),
            "request_matching": matching_request_payload,
            "payload": payload,
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _job_progress_event_summary_from_row(self, row: dict[str, Any] | None) -> dict[str, Any]:
        if row is None:
            return {}
        latest_event = {}
        stage_sequence = []
        stage_stats = {}
        latest_metrics = {}
        try:
            latest_event = dict(json.loads(row["latest_event_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            latest_event = {}
        try:
            stage_sequence = list(json.loads(row["stage_sequence_json"] or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            stage_sequence = []
        try:
            stage_stats = dict(json.loads(row["stage_stats_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            stage_stats = {}
        try:
            latest_metrics = dict(json.loads(row["latest_metrics_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            latest_metrics = {}
        return {
            "job_id": str(row["job_id"] or ""),
            "event_count": int(row["event_count"] or 0),
            "latest_event": latest_event,
            "stage_sequence": [str(item).strip() for item in stage_sequence if str(item).strip()],
            "stage_stats": stage_stats,
            "latest_metrics": latest_metrics,
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _job_event_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        try:
            payload = dict(json.loads(row["payload_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        return {
            "event_id": int(row["event_id"] or 0),
            "job_id": str(row["job_id"] or ""),
            "stage": str(row["stage"] or ""),
            "status": str(row["status"] or ""),
            "detail": str(row["detail"] or ""),
            "payload": payload,
            "created_at": str(row["created_at"] or ""),
        }

    def _asset_materialization_generation_from_row(self, row: dict[str, Any] | None) -> dict[str, Any]:
        if row is None:
            return {}
        summary = {}
        metadata = {}
        try:
            summary = dict(json.loads(row["summary_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            summary = {}
        try:
            metadata = dict(json.loads(row["metadata_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            metadata = {}
        generation_key = str(row["generation_key"] or "")
        generation_sequence = int(row["generation_sequence"] or 0)
        generation_watermark = (
            f"{generation_sequence}:{generation_key[:12]}" if generation_key and generation_sequence > 0 else ""
        )
        return {
            "target_company": str(row["target_company"] or ""),
            "company_key": str(row["company_key"] or ""),
            "snapshot_id": str(row["snapshot_id"] or ""),
            "asset_view": str(row["asset_view"] or ""),
            "artifact_kind": str(row["artifact_kind"] or ""),
            "artifact_key": str(row["artifact_key"] or ""),
            "generation_key": generation_key,
            "generation_sequence": generation_sequence,
            "generation_watermark": generation_watermark,
            "source_path": str(row["source_path"] or ""),
            "payload_signature": str(row["payload_signature"] or ""),
            "member_signature": str(row["member_signature"] or ""),
            "member_count": int(row["member_count"] or 0),
            "summary": summary,
            "metadata": metadata,
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _candidate_materialization_state_from_row(self, row: dict[str, Any] | None) -> dict[str, Any]:
        if row is None:
            return {}
        metadata = {}
        try:
            metadata = dict(json.loads(row["metadata_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            metadata = {}
        return {
            "target_company": str(row["target_company"] or ""),
            "company_key": str(row["company_key"] or ""),
            "snapshot_id": str(row["snapshot_id"] or ""),
            "asset_view": str(row["asset_view"] or ""),
            "candidate_id": str(row["candidate_id"] or ""),
            "fingerprint": str(row["fingerprint"] or ""),
            "shard_path": str(row["shard_path"] or ""),
            "list_page": int(row["list_page"] or 0),
            "dirty_reason": str(row["dirty_reason"] or ""),
            "materialized_at": str(row["materialized_at"] or ""),
            "metadata": metadata,
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _snapshot_materialization_run_from_row(self, row: dict[str, Any] | None) -> dict[str, Any]:
        if row is None:
            return {}
        summary = {}
        try:
            summary = dict(json.loads(row["summary_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            summary = {}
        return {
            "run_id": str(row["run_id"] or ""),
            "target_company": str(row["target_company"] or ""),
            "company_key": str(row["company_key"] or ""),
            "snapshot_id": str(row["snapshot_id"] or ""),
            "asset_view": str(row["asset_view"] or ""),
            "status": str(row["status"] or ""),
            "dirty_candidate_count": int(row["dirty_candidate_count"] or 0),
            "completed_candidate_count": int(row["completed_candidate_count"] or 0),
            "reused_candidate_count": int(row["reused_candidate_count"] or 0),
            "summary": summary,
            "started_at": str(row["started_at"] or ""),
            "completed_at": str(row["completed_at"] or ""),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _cloud_asset_operation_from_row(self, row: dict[str, Any] | None) -> dict[str, Any]:
        if row is None:
            return {}
        scoped_companies = []
        summary = {}
        metadata = {}
        try:
            scoped_companies = list(json.loads(row["scoped_companies_json"] or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            scoped_companies = []
        try:
            summary = dict(json.loads(row["summary_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            summary = {}
        try:
            metadata = dict(json.loads(row["metadata_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            metadata = {}
        return {
            "ledger_id": int(row["ledger_id"] or 0),
            "operation_type": str(row["operation_type"] or ""),
            "bundle_kind": str(row["bundle_kind"] or ""),
            "bundle_id": str(row["bundle_id"] or ""),
            "sync_run_id": str(row["sync_run_id"] or ""),
            "status": str(row["status"] or ""),
            "manifest_path": str(row["manifest_path"] or ""),
            "target_runtime_dir": str(row["target_runtime_dir"] or ""),
            "target_db_path": str(row["target_db_path"] or ""),
            "scoped_companies": [str(item).strip() for item in scoped_companies if str(item).strip()],
            "scoped_snapshot_id": str(row["scoped_snapshot_id"] or ""),
            "summary": summary,
            "metadata": metadata,
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _organization_execution_profile_from_row(self, row: dict[str, Any] | None) -> dict[str, Any]:
        if row is None:
            return {}
        reason_codes = []
        explanation = {}
        summary = {}
        try:
            reason_codes = list(json.loads(row["reason_codes_json"] or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            reason_codes = []
        try:
            explanation = dict(json.loads(row["explanation_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            explanation = {}
        try:
            summary = dict(json.loads(row["summary_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            summary = {}
        return {
            "profile_id": int(row["profile_id"] or 0),
            "target_company": _normalize_textual_value(row["target_company"]),
            "company_key": _normalize_textual_value(row["company_key"]),
            "asset_view": _normalize_textual_value(row["asset_view"]),
            "source_registry_id": int(row["source_registry_id"] or 0),
            "source_snapshot_id": _normalize_textual_value(row["source_snapshot_id"]),
            "source_job_id": _normalize_textual_value(row["source_job_id"]),
            "source_generation_key": _normalize_textual_value(row["source_generation_key"]),
            "source_generation_sequence": int(row["source_generation_sequence"] or 0),
            "source_generation_watermark": _normalize_textual_value(row["source_generation_watermark"]),
            "status": _normalize_textual_value(row["status"]),
            "org_scale_band": _normalize_textual_value(row["org_scale_band"]),
            "default_acquisition_mode": _normalize_textual_value(row["default_acquisition_mode"]),
            "prefer_delta_from_baseline": bool(row["prefer_delta_from_baseline"]),
            "current_lane_default": _normalize_textual_value(row["current_lane_default"]),
            "former_lane_default": _normalize_textual_value(row["former_lane_default"]),
            "baseline_candidate_count": int(row["baseline_candidate_count"] or 0),
            "current_lane_effective_candidate_count": int(row["current_lane_effective_candidate_count"] or 0),
            "former_lane_effective_candidate_count": int(row["former_lane_effective_candidate_count"] or 0),
            "completeness_score": float(row["completeness_score"] or 0.0),
            "completeness_band": _normalize_textual_value(row["completeness_band"]),
            "profile_detail_ratio": float(row["profile_detail_ratio"] or 0.0),
            "company_employee_shard_count": int(row["company_employee_shard_count"] or 0),
            "current_profile_search_shard_count": int(row["current_profile_search_shard_count"] or 0),
            "former_profile_search_shard_count": int(row["former_profile_search_shard_count"] or 0),
            "company_employee_cap_hit_count": int(row["company_employee_cap_hit_count"] or 0),
            "profile_search_cap_hit_count": int(row["profile_search_cap_hit_count"] or 0),
            "reason_codes": [_normalize_textual_value(item) for item in reason_codes if _normalize_textual_value(item)],
            "explanation": explanation,
            "summary": summary,
            "created_at": _normalize_textual_value(row["created_at"]),
            "updated_at": _normalize_textual_value(row["updated_at"]),
        }

    def _organization_asset_registry_from_row(self, row: dict[str, Any] | None) -> dict[str, Any]:
        if row is None:
            return {}
        source_snapshot_selection = {}
        selected_snapshot_ids = []
        summary = {}
        current_lane_coverage = {}
        former_lane_coverage = {}
        try:
            source_snapshot_selection = dict(json.loads(row["source_snapshot_selection_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            source_snapshot_selection = {}
        try:
            selected_snapshot_ids = list(json.loads(row["selected_snapshot_ids_json"] or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            selected_snapshot_ids = []
        try:
            summary = dict(json.loads(row["summary_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            summary = {}
        try:
            current_lane_coverage = dict(json.loads(row["current_lane_coverage_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            current_lane_coverage = {}
        try:
            former_lane_coverage = dict(json.loads(row["former_lane_coverage_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            former_lane_coverage = {}
        return {
            "registry_id": int(row["registry_id"] or 0),
            "target_company": _normalize_textual_value(row["target_company"]),
            "company_key": _normalize_textual_value(row["company_key"]),
            "snapshot_id": _normalize_textual_value(row["snapshot_id"]),
            "asset_view": _normalize_textual_value(row["asset_view"]),
            "status": _normalize_textual_value(row["status"]),
            "authoritative": bool(row["authoritative"]),
            "candidate_count": int(row["candidate_count"] or 0),
            "evidence_count": int(row["evidence_count"] or 0),
            "profile_detail_count": int(row["profile_detail_count"] or 0),
            "explicit_profile_capture_count": int(row["explicit_profile_capture_count"] or 0),
            "missing_linkedin_count": int(row["missing_linkedin_count"] or 0),
            "manual_review_backlog_count": int(row["manual_review_backlog_count"] or 0),
            "profile_completion_backlog_count": int(row["profile_completion_backlog_count"] or 0),
            "source_snapshot_count": int(row["source_snapshot_count"] or 0),
            "completeness_score": float(row["completeness_score"] or 0.0),
            "completeness_band": _normalize_textual_value(row["completeness_band"]),
            "current_lane_coverage": current_lane_coverage,
            "former_lane_coverage": former_lane_coverage,
            "current_lane_effective_candidate_count": int(row["current_lane_effective_candidate_count"] or 0),
            "former_lane_effective_candidate_count": int(row["former_lane_effective_candidate_count"] or 0),
            "current_lane_effective_ready": bool(row["current_lane_effective_ready"]),
            "former_lane_effective_ready": bool(row["former_lane_effective_ready"]),
            "source_snapshot_selection": source_snapshot_selection,
            "selected_snapshot_ids": [
                _normalize_textual_value(item) for item in selected_snapshot_ids if _normalize_textual_value(item)
            ],
            "source_path": _normalize_textual_value(row["source_path"]),
            "source_job_id": _normalize_textual_value(row["source_job_id"]),
            "materialization_generation_key": _normalize_textual_value(row["materialization_generation_key"]),
            "materialization_generation_sequence": int(row["materialization_generation_sequence"] or 0),
            "materialization_watermark": _normalize_textual_value(row["materialization_watermark"]),
            "summary": summary,
            "created_at": _normalize_textual_value(row["created_at"]),
            "updated_at": _normalize_textual_value(row["updated_at"]),
        }

    def _acquisition_shard_registry_from_row(self, row: dict[str, Any] | None) -> dict[str, Any]:
        if row is None:
            return {}
        company_scope = []
        locations = []
        function_ids = []
        metadata = {}
        try:
            company_scope = list(json.loads(row["company_scope_json"] or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            company_scope = []
        try:
            locations = list(json.loads(row["locations_json"] or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            locations = []
        try:
            function_ids = list(json.loads(row["function_ids_json"] or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            function_ids = []
        try:
            metadata = dict(json.loads(row["metadata_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            metadata = {}
        return {
            "shard_key": _normalize_textual_value(row["shard_key"]),
            "target_company": _normalize_textual_value(row["target_company"]),
            "company_key": _normalize_textual_value(row["company_key"]),
            "snapshot_id": _normalize_textual_value(row["snapshot_id"]),
            "asset_view": _normalize_textual_value(row["asset_view"]),
            "lane": _normalize_textual_value(row["lane"]),
            "status": _normalize_textual_value(row["status"]),
            "employment_scope": _normalize_textual_value(row["employment_scope"]),
            "strategy_type": _normalize_textual_value(row["strategy_type"]),
            "shard_id": _normalize_textual_value(row["shard_id"]),
            "shard_title": _normalize_textual_value(row["shard_title"]),
            "search_query": _normalize_textual_value(row["search_query"]),
            "query_signature": _normalize_textual_value(row["query_signature"]),
            "company_scope": [
                _normalize_textual_value(item) for item in company_scope if _normalize_textual_value(item)
            ],
            "locations": [_normalize_textual_value(item) for item in locations if _normalize_textual_value(item)],
            "function_ids": [_normalize_textual_value(item) for item in function_ids if _normalize_textual_value(item)],
            "result_count": int(row["result_count"] or 0),
            "estimated_total_count": int(row["estimated_total_count"] or 0),
            "provider_cap_hit": bool(row["provider_cap_hit"]),
            "source_path": _normalize_textual_value(row["source_path"]),
            "source_job_id": _normalize_textual_value(row["source_job_id"]),
            "materialization_generation_key": _normalize_textual_value(row["materialization_generation_key"]),
            "materialization_generation_sequence": int(row["materialization_generation_sequence"] or 0),
            "materialization_watermark": _normalize_textual_value(row["materialization_watermark"]),
            "metadata": metadata,
            "first_seen_at": _normalize_textual_value(row["first_seen_at"]),
            "last_completed_at": _normalize_textual_value(row["last_completed_at"]),
            "created_at": _normalize_textual_value(row["created_at"]),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _runtime_provider_limiter_lease_from_row(self, row: Any) -> dict[str, Any]:
        if row is None:
            return {}
        lease_expires_at = str(row["lease_expires_at"] or "")
        metadata: dict[str, Any] = {}
        try:
            metadata = dict(json.loads(row["metadata_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            metadata = {}
        return {
            "lease_token": str(row["lease_token"] or ""),
            "limiter_key": str(row["limiter_key"] or ""),
            "lease_owner": str(row["lease_owner"] or ""),
            "lease_expires_at": lease_expires_at,
            "metadata": metadata,
            "expired": _is_sqlite_timestamp_expired(lease_expires_at),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _workflow_job_lease_from_row(self, row: dict[str, Any] | None) -> dict[str, Any]:
        if row is None:
            return {}
        lease_expires_at = str(row["lease_expires_at"] or "")
        return {
            "job_id": str(row["job_id"] or ""),
            "lease_owner": str(row["lease_owner"] or ""),
            "lease_token": str(row["lease_token"] or ""),
            "lease_expires_at": lease_expires_at,
            "expired": _is_sqlite_timestamp_expired(lease_expires_at),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _workflow_command_from_row(self, row: Any) -> dict[str, Any]:
        return _workflow_runtime_repo.WORKFLOW_COMMANDS.from_row(row)

    def _plan_review_session_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        execution_bundle = {}
        matching_request = {}
        try:
            execution_bundle = json.loads(row["execution_bundle_json"] or "{}")
        except json.JSONDecodeError:
            execution_bundle = {}
        try:
            matching_request = json.loads(row["matching_request_json"] or "{}")
        except json.JSONDecodeError:
            matching_request = {}
        return {
            "review_id": row["review_id"],
            "target_company": _normalize_textual_value(row["target_company"]),
            "request_signature": _normalize_textual_value(row["request_signature"]),
            "request_family_signature": _normalize_textual_value(row["request_family_signature"]),
            "matching_request_signature": _normalize_textual_value(row["matching_request_signature"]),
            "matching_request_family_signature": _normalize_textual_value(row["matching_request_family_signature"]),
            "status": _normalize_textual_value(row["status"]),
            "risk_level": _normalize_textual_value(row["risk_level"]),
            "required_before_execution": bool(row["required_before_execution"]),
            "request": json.loads(row["request_json"] or "{}"),
            "plan": json.loads(row["plan_json"] or "{}"),
            "gate": json.loads(row["gate_json"] or "{}"),
            "execution_bundle": execution_bundle,
            "request_matching": matching_request,
            "decision": json.loads(row["decision_json"] or "{}"),
            "reviewer": _normalize_textual_value(row["reviewer"]),
            "review_notes": _normalize_textual_value(row["review_notes"]),
            "approved_at": _normalize_textual_value(row["approved_at"]),
            "created_at": _normalize_textual_value(row["created_at"]),
            "updated_at": _normalize_textual_value(row["updated_at"]),
        }

    def _candidate_review_record_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": row["record_id"],
            "job_id": row["job_id"],
            "history_id": row["history_id"],
            "candidate_id": row["candidate_id"],
            "candidate_name": row["candidate_name"],
            "headline": row["headline"] or "",
            "current_company": row["current_company"] or "",
            "avatar_url": row["avatar_url"] or "",
            "linkedin_url": row["linkedin_url"] or "",
            "primary_email": row["primary_email"] or "",
            "status": row["status"],
            "comment": row["comment"] or "",
            "source": row["source"] or "manual_review",
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "added_at": row["added_at"],
            "updated_at": row["updated_at"],
        }

    def _target_candidate_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        quality_score = row["quality_score"]
        return {
            "id": row["record_id"],
            "candidate_id": row["candidate_id"],
            "history_id": row["history_id"],
            "job_id": row["job_id"],
            "candidate_name": row["candidate_name"],
            "headline": row["headline"] or "",
            "current_company": row["current_company"] or "",
            "avatar_url": row["avatar_url"] or "",
            "linkedin_url": row["linkedin_url"] or "",
            "primary_email": row["primary_email"] or "",
            "person_identity_key": str(_row_value(row, "person_identity_key") or ""),
            "candidate_identity_key": str(_row_value(row, "candidate_identity_key") or ""),
            "source_projection_id": str(_row_value(row, "source_projection_id") or ""),
            "source_run_id": str(_row_value(row, "source_run_id") or ""),
            "source_collection_id": str(_row_value(row, "source_collection_id") or ""),
            "source_reason": str(_row_value(row, "source_reason") or ""),
            "follow_up_status": row["follow_up_status"] or "pending_outreach",
            "quality_score": float(quality_score) if quality_score is not None else None,
            "comment": row["comment"] or "",
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "added_at": row["added_at"],
            "updated_at": row["updated_at"],
        }

    def _target_candidate_public_web_batch_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _public_web_repo.TARGET_CANDIDATE_PUBLIC_WEB_BATCHES.from_row(row)

    def _target_candidate_public_web_run_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _public_web_repo.TARGET_CANDIDATE_PUBLIC_WEB_RUNS.from_row(row)

    def _crm_public_web_batch_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _public_web_repo.CRM_PUBLIC_WEB_BATCHES.from_row(row)

    def _crm_public_web_run_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _public_web_repo.CRM_PUBLIC_WEB_RUNS.from_row(row)

    def _person_public_web_asset_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _public_web_repo.PERSON_PUBLIC_WEB_ASSETS.from_row(row)

    def _person_public_web_signal_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _public_web_repo.PERSON_PUBLIC_WEB_SIGNALS.from_row(row)

    def _person_asset_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _person_company_assets_repo.PERSON_ASSETS.from_row(row)

    def _person_evidence_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _person_company_assets_repo.PERSON_EVIDENCE.from_row(row)

    def _person_assertion_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _person_company_assets_repo.PERSON_ASSERTIONS.from_row(row)

    def _company_asset_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _person_company_assets_repo.COMPANY_ASSETS.from_row(row)

    def _company_evidence_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _person_company_assets_repo.COMPANY_EVIDENCE.from_row(row)

    def _company_assertion_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _person_company_assets_repo.COMPANY_ASSERTIONS.from_row(row)

    def _raw_profile_index_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _person_company_assets_repo.RAW_PROFILE_INDEX.from_row(row)

    def _candidate_evidence_index_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _person_company_assets_repo.CANDIDATE_EVIDENCE_INDEX.from_row(row)

    def _crm_record_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _crm_core_repo.CRM_RECORDS.from_row(row)

    def _crm_engagement_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        if row is None:
            return {}
        quality_score = _row_value(row, "quality_score")
        return {
            "engagement_id": str(_row_value(row, "engagement_id") or ""),
            "crm_record_id": str(_row_value(row, "crm_record_id") or ""),
            "pipeline_id": str(_row_value(row, "pipeline_id") or "default_sourcing"),
            "stage": str(_row_value(row, "stage") or "new"),
            "stage_category": str(_row_value(row, "stage_category") or "open"),
            "priority": str(_row_value(row, "priority") or "normal"),
            "quality_score": float(quality_score) if quality_score not in {None, ""} else None,
            "next_action_at": str(_row_value(row, "next_action_at") or ""),
            "last_contacted_at": str(_row_value(row, "last_contacted_at") or ""),
            "source_projection_id": str(_row_value(row, "source_projection_id") or ""),
            "source_run_id": str(_row_value(row, "source_run_id") or ""),
            "source_selection_reason": str(_row_value(row, "source_selection_reason") or ""),
            "created_by_actor": str(_row_value(row, "created_by_actor") or ""),
            "metadata": _loads_json_dict(_row_value(row, "metadata_json")),
            "created_at": str(_row_value(row, "created_at") or ""),
            "updated_at": str(_row_value(row, "updated_at") or ""),
        }

    def _crm_event_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _crm_core_repo.CRM_EVENTS.from_row(row)

    def _crm_task_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _crm_core_repo.CRM_TASKS.from_row(row)

    def _target_candidate_public_web_promotion_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _public_web_repo.TARGET_CANDIDATE_PUBLIC_WEB_PROMOTIONS.from_row(row)

    def _crm_public_web_promotion_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _public_web_repo.CRM_PUBLIC_WEB_PROMOTIONS.from_row(row)

    def _company_public_web_asset_run_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _public_web_repo.COMPANY_PUBLIC_WEB_ASSET_RUNS.from_row(row)

    def _company_public_web_asset_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _public_web_repo.COMPANY_PUBLIC_WEB_ASSETS.from_row(row)

    def _asset_default_pointer_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "pointer_key": str(row["pointer_key"] or ""),
            "company_key": str(row["company_key"] or ""),
            "scope_kind": str(row["scope_kind"] or "company"),
            "scope_key": str(row["scope_key"] or ""),
            "asset_kind": str(row["asset_kind"] or "company_asset"),
            "snapshot_id": str(row["snapshot_id"] or ""),
            "lifecycle_status": str(row["lifecycle_status"] or "canonical"),
            "coverage_proof": json.loads(row["coverage_proof_json"] or "{}"),
            "previous_snapshot_id": str(row["previous_snapshot_id"] or ""),
            "promoted_by_job_id": str(row["promoted_by_job_id"] or ""),
            "promoted_at": str(row["promoted_at"] or ""),
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _asset_default_pointer_history_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "history_id": str(row["history_id"] or ""),
            "pointer_key": str(row["pointer_key"] or ""),
            "company_key": str(row["company_key"] or ""),
            "scope_kind": str(row["scope_kind"] or "company"),
            "scope_key": str(row["scope_key"] or ""),
            "asset_kind": str(row["asset_kind"] or "company_asset"),
            "snapshot_id": str(row["snapshot_id"] or ""),
            "lifecycle_status": str(row["lifecycle_status"] or ""),
            "event_type": str(row["event_type"] or ""),
            "payload": json.loads(row["payload_json"] or "{}"),
            "occurred_at": str(row["occurred_at"] or ""),
            "created_at": str(row["created_at"] or ""),
        }

    def _frontend_history_link_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "history_id": str(row["history_id"] or ""),
            "query_text": str(row["query_text"] or ""),
            "target_company": str(row["target_company"] or ""),
            "review_id": int(row["review_id"] or 0),
            "job_id": str(row["job_id"] or ""),
            "phase": str(row["phase"] or ""),
            "request": json.loads(row["request_json"] or "{}"),
            "plan": json.loads(row["plan_json"] or "{}"),
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _agent_runtime_session_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "session_id": row["session_id"],
            "job_id": row["job_id"],
            "target_company": row["target_company"],
            "request_signature": row["request_signature"],
            "request_family_signature": row["request_family_signature"],
            "runtime_mode": row["runtime_mode"],
            "status": row["status"],
            "lanes": json.loads(row["lanes_json"] or "[]"),
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _agent_trace_span_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "span_id": row["span_id"],
            "session_id": row["session_id"],
            "job_id": row["job_id"],
            "parent_span_id": row["parent_span_id"],
            "lane_id": row["lane_id"],
            "handoff_from_lane": row["handoff_from_lane"],
            "handoff_to_lane": row["handoff_to_lane"],
            "span_name": row["span_name"],
            "stage": row["stage"],
            "status": row["status"],
            "input": json.loads(row["input_json"] or "{}"),
            "output": json.loads(row["output_json"] or "{}"),
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "created_at": row["created_at"],
        }

    def _agent_worker_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        worker = {
            "worker_id": row["worker_id"],
            "session_id": row["session_id"],
            "job_id": row["job_id"],
            "span_id": row["span_id"],
            "lane_id": row["lane_id"],
            "worker_key": row["worker_key"],
            "status": row["status"],
            "interrupt_requested": bool(row["interrupt_requested"]),
            "budget": json.loads(row["budget_json"] or "{}"),
            "checkpoint": json.loads(row["checkpoint_json"] or "{}"),
            "input": json.loads(row["input_json"] or "{}"),
            "output": json.loads(row["output_json"] or "{}"),
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "lease_owner": row["lease_owner"] or "",
            "lease_expires_at": row["lease_expires_at"] or "",
            "attempt_count": int(row["attempt_count"] or 0),
            "last_error": row["last_error"] or "",
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        worker["wait_stage"] = wait_stage(worker)
        worker["effective_status"] = effective_worker_status(worker)
        return worker

    def _candidate_payload(self, candidate: Candidate) -> dict[str, Any]:
        payload = normalize_candidate(candidate).to_record()
        payload["metadata_json"] = json.dumps(payload.pop("metadata"), ensure_ascii=False)
        return payload

    def _evidence_payload(self, evidence: EvidenceRecord) -> dict[str, Any]:
        payload = evidence.to_record()
        payload["metadata_json"] = json.dumps(payload.pop("metadata"), ensure_ascii=False)
        return payload

    def _dedupe_candidate_payloads(self, candidates: list[Candidate]) -> list[dict[str, Any]]:
        deduped: dict[str, dict[str, Any]] = {}
        ordered_keys: list[str] = []
        for candidate in list(candidates or []):
            payload = self._candidate_payload(candidate)
            candidate_id = str(payload.get("candidate_id") or "").strip()
            dedupe_key = candidate_id or "|".join(
                [
                    "blank",
                    str(payload.get("linkedin_url") or "").strip().lower(),
                    str(payload.get("display_name") or payload.get("name_en") or "").strip().lower(),
                    str(payload.get("target_company") or "").strip().lower(),
                ]
            )
            if dedupe_key not in deduped:
                ordered_keys.append(dedupe_key)
            deduped[dedupe_key] = payload
        return [deduped[key] for key in ordered_keys]

    def _dedupe_evidence_payloads(self, evidence: list[EvidenceRecord]) -> list[dict[str, Any]]:
        deduped: dict[str, dict[str, Any]] = {}
        ordered_keys: list[str] = []
        for item in list(evidence or []):
            payload = self._evidence_payload(item)
            evidence_id = str(payload.get("evidence_id") or "").strip()
            dedupe_key = evidence_id or "|".join(
                [
                    str(payload.get("candidate_id") or "").strip(),
                    str(payload.get("source_type") or "").strip().lower(),
                    str(payload.get("title") or "").strip().lower(),
                    str(payload.get("url") or payload.get("source_path") or "").strip().lower(),
                ]
            )
            if dedupe_key not in deduped:
                ordered_keys.append(dedupe_key)
            deduped[dedupe_key] = payload
        return [deduped[key] for key in ordered_keys]

    def _evidence_payload_from_row(self, row: Any, *, include_candidate_id: bool) -> dict[str, Any]:
        payload = {
            "evidence_id": row["evidence_id"],
            "source_type": row["source_type"],
            "title": row["title"],
            "url": row["url"],
            "summary": row["summary"],
            "source_dataset": row["source_dataset"],
            "source_path": row["source_path"],
            "metadata": json.loads(row["metadata_json"] or "{}"),
        }
        if include_candidate_id:
            payload["candidate_id"] = row["candidate_id"]
        return payload

    def _candidate_from_row(self, row: Any) -> Candidate:
        return normalize_candidate(
            Candidate(
                candidate_id=row["candidate_id"],
                name_en=row["name_en"],
                name_zh=row["name_zh"],
                display_name=row["display_name"],
                category=row["category"],
                target_company=row["target_company"],
                organization=row["organization"],
                employment_status=row["employment_status"],
                role=row["role"],
                team=row["team"],
                joined_at=row["joined_at"],
                left_at=row["left_at"],
                current_destination=row["current_destination"],
                ethnicity_background=row["ethnicity_background"],
                investment_involvement=row["investment_involvement"],
                focus_areas=row["focus_areas"],
                education=row["education"],
                work_history=row["work_history"],
                notes=row["notes"],
                linkedin_url=row["linkedin_url"],
                media_url=row["media_url"],
                source_dataset=row["source_dataset"],
                source_path=row["source_path"],
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
        )


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _candidate_richness_score_for_store_match(candidate: Candidate) -> int:
    score = 0
    for value in (
        candidate.linkedin_url,
        candidate.role,
        candidate.team,
        candidate.focus_areas,
        candidate.education,
        candidate.work_history,
        candidate.notes,
        candidate.source_path,
    ):
        if str(value or "").strip():
            score += 1
    metadata = dict(candidate.metadata or {})
    for key in ("headline", "summary", "languages", "skills", "public_identifier", "profile_capture_source_path"):
        value = metadata.get(key)
        if isinstance(value, list):
            if value:
                score += 1
        elif str(value or "").strip():
            score += 1
    return score


_CANDIDATE_REVIEW_STATUSES = {
    "no_review_needed",
    "needs_review",
    "needs_profile_completion",
    "low_profile_richness",
    "verified_keep",
    "verified_exclude",
}

_CANDIDATE_REVIEW_SOURCES = {
    "manual_add",
    "backend_override",
    "manual_review",
}

_TARGET_CANDIDATE_FOLLOW_UP_STATUSES = {
    "pending_outreach",
    "contacted_waiting",
    "rejected",
    "accepted",
    "interview_completed",
}

_FRONTEND_HISTORY_PHASES = {
    "idle",
    "plan",
    "running",
    "results",
}


def _fallback_record_token(*values: Any) -> str:
    raw = "|".join(str(value or "").strip().lower() for value in values if str(value or "").strip())
    if not raw:
        raw = "unknown"
    return sha1(raw.encode("utf-8")).hexdigest()[:16]


def _normalize_candidate_review_status(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _CANDIDATE_REVIEW_STATUSES:
        return normalized
    return "needs_review"


def _normalize_candidate_review_source(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _CANDIDATE_REVIEW_SOURCES:
        return normalized
    return "manual_review"


def _build_candidate_review_record_id(payload: dict[str, Any]) -> str:
    explicit = str(payload.get("id") or payload.get("record_id") or "").strip()
    if explicit:
        return explicit
    job_id = str(payload.get("job_id") or "").strip() or "no-job"
    candidate_id = str(payload.get("candidate_id") or "").strip()
    if candidate_id:
        return f"{job_id}::{candidate_id}"
    linkedin_key = _normalize_linkedin_profile_url_key(str(payload.get("linkedin_url") or ""))
    if linkedin_key:
        return f"{job_id}::{linkedin_key}"
    return f"{job_id}::candidate::{_fallback_record_token(payload.get('candidate_name'), payload.get('headline'), payload.get('current_company'))}"


def _normalize_candidate_review_record_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    return {
        "record_id": _build_candidate_review_record_id(normalized),
        "job_id": str(normalized.get("job_id") or "").strip(),
        "history_id": str(normalized.get("history_id") or "").strip(),
        "candidate_id": str(normalized.get("candidate_id") or "").strip(),
        "candidate_name": str(normalized.get("candidate_name") or "").strip(),
        "headline": str(normalized.get("headline") or "").strip(),
        "current_company": str(normalized.get("current_company") or "").strip(),
        "avatar_url": str(normalized.get("avatar_url") or "").strip(),
        "linkedin_url": str(normalized.get("linkedin_url") or "").strip(),
        "primary_email": str(normalized.get("primary_email") or "").strip(),
        "status": _normalize_candidate_review_status(normalized.get("status")),
        "comment": str(normalized.get("comment") or "").strip(),
        "source": _normalize_candidate_review_source(normalized.get("source")),
        "metadata": dict(normalized.get("metadata") or {}),
        "added_at": str(normalized.get("added_at") or "").strip(),
    }


def _build_target_candidate_record_id(payload: dict[str, Any]) -> str:
    explicit = str(payload.get("id") or payload.get("record_id") or "").strip()
    if explicit:
        return explicit
    linkedin_key = _normalize_linkedin_profile_url_key(str(payload.get("linkedin_url") or ""))
    if linkedin_key:
        return linkedin_key
    candidate_id = str(payload.get("candidate_id") or "").strip()
    if candidate_id:
        return f"candidate::{candidate_id}"
    return f"target::{_fallback_record_token(payload.get('candidate_name'), payload.get('headline'), payload.get('current_company'))}"


def _normalize_target_candidate_follow_up_status(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _TARGET_CANDIDATE_FOLLOW_UP_STATUSES:
        return normalized
    return "pending_outreach"


_CRM_STAGE_CATEGORIES = {
    "new": "open",
    "researching": "open",
    "outreach_ready": "open",
    "contacted_waiting": "waiting",
    "responded": "open",
    "interview_completed": "terminal_success",
    "accepted": "terminal_success",
    "rejected": "terminal_loss",
    "do_not_contact": "blocked",
    "archived": "archived",
}


def _crm_stage_category(value: Any) -> str:
    normalized = str(value or "new").strip().lower() or "new"
    return _CRM_STAGE_CATEGORIES.get(normalized, "open")


def _normalize_target_candidate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    quality_score = normalized.get("quality_score")
    parsed_quality_score: float | None = None
    if quality_score not in {None, ""}:
        try:
            parsed_quality_score = float(quality_score)
        except (TypeError, ValueError):
            parsed_quality_score = None
    profile_url_key = _resolve_profile_url_key(
        normalized.get("profile_url_key"),
        normalized.get("linkedin_url"),
    )
    person_identity_key = _resolve_person_identity_key(
        person_identity_key=str(normalized.get("person_identity_key") or ""),
        profile_url_key=profile_url_key,
        linkedin_url=str(normalized.get("linkedin_url") or ""),
        candidate_identity_key=str(normalized.get("candidate_identity_key") or ""),
        candidate_id=str(normalized.get("candidate_id") or ""),
    )
    candidate_identity_key = _resolve_candidate_identity_key(
        candidate_identity_key=str(normalized.get("candidate_identity_key") or ""),
        person_identity_key=person_identity_key,
        profile_url_key=profile_url_key,
        linkedin_url=str(normalized.get("linkedin_url") or ""),
        candidate_id=str(normalized.get("candidate_id") or ""),
    )
    return {
        "record_id": _build_target_candidate_record_id(normalized),
        "candidate_id": str(normalized.get("candidate_id") or "").strip(),
        "history_id": str(normalized.get("history_id") or "").strip(),
        "job_id": str(normalized.get("job_id") or "").strip(),
        "candidate_name": str(normalized.get("candidate_name") or "").strip(),
        "headline": str(normalized.get("headline") or "").strip(),
        "current_company": str(normalized.get("current_company") or "").strip(),
        "avatar_url": str(normalized.get("avatar_url") or "").strip(),
        "linkedin_url": str(normalized.get("linkedin_url") or "").strip(),
        "primary_email": str(normalized.get("primary_email") or "").strip(),
        "person_identity_key": person_identity_key,
        "candidate_identity_key": candidate_identity_key,
        "source_projection_id": str(normalized.get("source_projection_id") or "").strip(),
        "source_run_id": str(normalized.get("source_run_id") or "").strip(),
        "source_collection_id": str(normalized.get("source_collection_id") or "").strip(),
        "source_reason": str(normalized.get("source_reason") or "").strip(),
        "follow_up_status": _normalize_target_candidate_follow_up_status(normalized.get("follow_up_status")),
        "quality_score": parsed_quality_score,
        "comment": str(normalized.get("comment") or "").strip(),
        "metadata": dict(normalized.get("metadata") or {}),
        "added_at": str(normalized.get("added_at") or "").strip(),
    }


def _normalize_frontend_history_phase(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _FRONTEND_HISTORY_PHASES:
        return normalized
    return ""


def _normalize_frontend_history_link_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    review_id_value = normalized.get("review_id")
    try:
        review_id = int(review_id_value or 0)
    except (TypeError, ValueError):
        review_id = 0
    return {
        "history_id": str(normalized.get("history_id") or normalized.get("id") or "").strip(),
        "query_text": str(normalized.get("query_text") or normalized.get("raw_user_request") or "").strip(),
        "target_company": str(normalized.get("target_company") or "").strip(),
        "review_id": max(0, review_id),
        "job_id": str(normalized.get("job_id") or "").strip(),
        "phase": _normalize_frontend_history_phase(normalized.get("phase")),
        "request": dict(normalized.get("request") or {}),
        "plan": dict(normalized.get("plan") or {}),
        "metadata": dict(normalized.get("metadata") or {}),
        "created_at": str(normalized.get("created_at") or "").strip(),
    }


_TARGET_CANDIDATE_PUBLIC_WEB_STATUSES = {
    "queued",
    "search_submitted",
    "searching",
    "entry_links_ready",
    "fetching",
    "documents_fetched",
    "analyzing",
    "adjudication_completed",
    "analysis_completed",
    "completed",
    "completed_with_errors",
    "needs_review",
    "failed",
    "cancelled",
}
_TARGET_CANDIDATE_PUBLIC_WEB_TERMINAL_STATUSES = {
    "completed",
    "completed_with_errors",
    "needs_review",
    "failed",
    "cancelled",
}


def _loads_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _loads_json_list(value: Any, *, default: list[Any] | None = None) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return list(default or [])
    return list(parsed) if isinstance(parsed, list) else list(default or [])


def _normalize_json_object_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(_json_safe_payload(value))
    return _loads_json_dict(value)


def _normalize_public_web_string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items = [item.strip() for item in value.split(",")]
    else:
        raw_items = list(value or []) if isinstance(value, (list, tuple, set)) else []
    seen: set[str] = set()
    items: list[str] = []
    for raw_item in raw_items:
        item = str(raw_item or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        items.append(item)
    return items


def _normalize_target_candidate_public_web_status(value: Any, *, default: str = "queued") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _TARGET_CANDIDATE_PUBLIC_WEB_STATUSES:
        return normalized
    return default


def _public_web_hash_token(*parts: Any) -> str:
    return sha1("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[:16]


def _normalize_target_candidate_public_web_batch_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    requested_record_ids = _normalize_public_web_string_list(
        normalized.get("requested_record_ids") or normalized.get("record_ids")
    )
    source_families = _normalize_public_web_string_list(normalized.get("source_families"))
    options = dict(normalized.get("options") or {})
    run_ids = _normalize_public_web_string_list(normalized.get("run_ids"))
    idempotency_key = str(normalized.get("idempotency_key") or "").strip()
    if not idempotency_key:
        idempotency_key = "target-candidate-public-web-batch:" + _public_web_hash_token(
            ",".join(sorted(requested_record_ids)),
            ",".join(sorted(source_families)),
            json.dumps(_json_safe_payload(options), sort_keys=True, ensure_ascii=False),
            str(bool(normalized.get("force_refresh"))),
        )
    batch_id = str(normalized.get("batch_id") or normalized.get("id") or "").strip()
    if not batch_id:
        batch_id = f"tc-public-web-batch-{_public_web_hash_token(idempotency_key)}"
    status = _normalize_target_candidate_public_web_status(normalized.get("status"))
    completed_at = str(normalized.get("completed_at") or "").strip()
    if status in _TARGET_CANDIDATE_PUBLIC_WEB_TERMINAL_STATUSES and not completed_at:
        completed_at = _utc_now_timestamp()
    return {
        "batch_id": batch_id,
        "idempotency_key": idempotency_key,
        "status": status,
        "requested_record_ids": requested_record_ids,
        "source_families": source_families,
        "options": options,
        "run_ids": run_ids,
        "summary": dict(normalized.get("summary") or {}),
        "metadata": dict(normalized.get("metadata") or {}),
        "requested_by": str(normalized.get("requested_by") or "").strip(),
        "force_refresh": bool(normalized.get("force_refresh")),
        "started_at": str(normalized.get("started_at") or "").strip(),
        "completed_at": completed_at,
        "created_at": str(normalized.get("created_at") or "").strip(),
    }


def _normalize_crm_public_web_batch_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    requested_record_ids = _normalize_public_web_string_list(
        normalized.get("requested_crm_record_ids")
        or normalized.get("crm_record_ids")
        or normalized.get("requested_record_ids")
        or normalized.get("record_ids")
    )
    source_families = _normalize_public_web_string_list(normalized.get("source_families"))
    options = dict(normalized.get("options") or {})
    run_ids = _normalize_public_web_string_list(normalized.get("run_ids"))
    idempotency_key = str(normalized.get("idempotency_key") or "").strip()
    if not idempotency_key:
        idempotency_key = "crm-public-web-batch:" + _public_web_hash_token(
            str(normalized.get("workspace_id") or "default").strip() or "default",
            ",".join(sorted(requested_record_ids)),
            ",".join(sorted(source_families)),
            json.dumps(_json_safe_payload(options), sort_keys=True, ensure_ascii=False),
            str(bool(normalized.get("force_refresh"))),
        )
    batch_id = str(normalized.get("batch_id") or normalized.get("id") or "").strip()
    if not batch_id:
        batch_id = f"crm-public-web-batch-{_public_web_hash_token(idempotency_key)}"
    status = _normalize_target_candidate_public_web_status(normalized.get("status"))
    completed_at = str(normalized.get("completed_at") or "").strip()
    if status in _TARGET_CANDIDATE_PUBLIC_WEB_TERMINAL_STATUSES and not completed_at:
        completed_at = _utc_now_timestamp()
    return {
        "batch_id": batch_id,
        "idempotency_key": idempotency_key,
        "workspace_id": str(normalized.get("workspace_id") or "default").strip() or "default",
        "status": status,
        "requested_crm_record_ids": requested_record_ids,
        "source_families": source_families,
        "options": options,
        "run_ids": run_ids,
        "summary": dict(normalized.get("summary") or {}),
        "metadata": dict(normalized.get("metadata") or {}),
        "requested_by": str(normalized.get("requested_by") or "").strip(),
        "force_refresh": bool(normalized.get("force_refresh")),
        "execution_backend": str(normalized.get("execution_backend") or "crm_public_web_v1").strip()
        or "crm_public_web_v1",
        "source_target_batch_id": str(normalized.get("source_target_batch_id") or "").strip(),
        "started_at": str(normalized.get("started_at") or "").strip(),
        "completed_at": completed_at,
        "created_at": str(normalized.get("created_at") or "").strip(),
    }


def _normalize_target_candidate_public_web_run_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    record_id = str(normalized.get("record_id") or normalized.get("id") or "").strip()
    linkedin_url = str(normalized.get("linkedin_url") or "").strip()
    linkedin_url_key = str(normalized.get("linkedin_url_key") or "").strip() or _normalize_linkedin_profile_url_key(
        linkedin_url
    )
    source_families = _normalize_public_web_string_list(normalized.get("source_families"))
    options = dict(normalized.get("options") or {})
    person_identity_key = str(normalized.get("person_identity_key") or "").strip()
    if not person_identity_key:
        person_identity_key = f"linkedin:{linkedin_url_key}" if linkedin_url_key else f"target_candidate:{record_id}"
    idempotency_key = str(normalized.get("idempotency_key") or "").strip()
    if not idempotency_key:
        idempotency_key = "target-candidate-public-web-run:" + _public_web_hash_token(
            record_id,
            person_identity_key,
            ",".join(sorted(source_families)),
            json.dumps(_json_safe_payload(options), sort_keys=True, ensure_ascii=False),
        )
    run_id = (
        str(normalized.get("run_id") or "").strip() or f"tc-public-web-run-{_public_web_hash_token(idempotency_key)}"
    )
    status = _normalize_target_candidate_public_web_status(normalized.get("status"))
    phase = str(normalized.get("phase") or status or "queued").strip().lower()
    started_at = str(normalized.get("started_at") or "").strip()
    if status != "queued" and not started_at:
        started_at = _utc_now_timestamp()
    completed_at = str(normalized.get("completed_at") or "").strip()
    if status in _TARGET_CANDIDATE_PUBLIC_WEB_TERMINAL_STATUSES and not completed_at:
        completed_at = _utc_now_timestamp()
    attempt_value = normalized.get("attempt_count")
    try:
        attempt_count = max(0, int(attempt_value or 0))
    except (TypeError, ValueError):
        attempt_count = 0
    return {
        "run_id": run_id,
        "batch_id": str(normalized.get("batch_id") or "").strip(),
        "record_id": record_id,
        "candidate_id": str(normalized.get("candidate_id") or "").strip(),
        "candidate_name": str(normalized.get("candidate_name") or "").strip(),
        "current_company": str(normalized.get("current_company") or "").strip(),
        "linkedin_url": linkedin_url,
        "linkedin_url_key": linkedin_url_key,
        "person_identity_key": person_identity_key,
        "idempotency_key": idempotency_key,
        "status": status,
        "phase": phase,
        "source_families": source_families,
        "options": options,
        "query_manifest": _loads_json_list(normalized.get("query_manifest"), default=[]),
        "search_checkpoint": dict(normalized.get("search_checkpoint") or {}),
        "fetch_checkpoint": dict(normalized.get("fetch_checkpoint") or {}),
        "analysis_checkpoint": dict(normalized.get("analysis_checkpoint") or {}),
        "summary": dict(normalized.get("summary") or {}),
        "artifact_root": str(normalized.get("artifact_root") or "").strip(),
        "worker_key": str(normalized.get("worker_key") or "").strip() or f"public_web_run::{run_id}",
        "lease_owner": str(normalized.get("lease_owner") or "").strip(),
        "lease_expires_at": str(normalized.get("lease_expires_at") or "").strip(),
        "attempt_count": attempt_count,
        "last_error": str(normalized.get("last_error") or "").strip(),
        "started_at": started_at,
        "completed_at": completed_at,
        "created_at": str(normalized.get("created_at") or "").strip(),
    }


def _normalize_crm_public_web_run_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    crm_record_id = str(
        normalized.get("crm_record_id") or normalized.get("record_id") or normalized.get("id") or ""
    ).strip()
    linkedin_url = str(normalized.get("linkedin_url") or "").strip()
    linkedin_url_key = str(normalized.get("linkedin_url_key") or "").strip() or _normalize_linkedin_profile_url_key(
        linkedin_url
    )
    source_families = _normalize_public_web_string_list(normalized.get("source_families"))
    options = dict(normalized.get("options") or {})
    person_identity_key = str(normalized.get("person_identity_key") or "").strip()
    if not person_identity_key:
        person_identity_key = f"linkedin:{linkedin_url_key}" if linkedin_url_key else f"crm_record:{crm_record_id}"
    idempotency_key = str(normalized.get("idempotency_key") or "").strip()
    if not idempotency_key:
        idempotency_key = "crm-public-web-run:" + _public_web_hash_token(
            str(normalized.get("workspace_id") or "default").strip() or "default",
            crm_record_id,
            person_identity_key,
            ",".join(sorted(source_families)),
            json.dumps(_json_safe_payload(options), sort_keys=True, ensure_ascii=False),
        )
    run_id = (
        str(normalized.get("run_id") or "").strip() or f"crm-public-web-run-{_public_web_hash_token(idempotency_key)}"
    )
    status = _normalize_target_candidate_public_web_status(normalized.get("status"))
    phase = str(normalized.get("phase") or status or "queued").strip().lower()
    started_at = str(normalized.get("started_at") or "").strip()
    if status != "queued" and not started_at:
        started_at = _utc_now_timestamp()
    completed_at = str(normalized.get("completed_at") or "").strip()
    if status in _TARGET_CANDIDATE_PUBLIC_WEB_TERMINAL_STATUSES and not completed_at:
        completed_at = _utc_now_timestamp()
    attempt_value = normalized.get("attempt_count")
    try:
        attempt_count = max(0, int(attempt_value or 0))
    except (TypeError, ValueError):
        attempt_count = 0
    return {
        "run_id": run_id,
        "batch_id": str(normalized.get("batch_id") or "").strip(),
        "crm_record_id": crm_record_id,
        "workspace_id": str(normalized.get("workspace_id") or "default").strip() or "default",
        "candidate_id": str(normalized.get("candidate_id") or "").strip(),
        "candidate_name": str(normalized.get("candidate_name") or "").strip(),
        "current_company": str(normalized.get("current_company") or "").strip(),
        "linkedin_url": linkedin_url,
        "linkedin_url_key": linkedin_url_key,
        "person_identity_key": person_identity_key,
        "idempotency_key": idempotency_key,
        "status": status,
        "phase": phase,
        "source_families": source_families,
        "options": options,
        "query_manifest": _loads_json_list(normalized.get("query_manifest"), default=[]),
        "search_checkpoint": dict(normalized.get("search_checkpoint") or {}),
        "fetch_checkpoint": dict(normalized.get("fetch_checkpoint") or {}),
        "analysis_checkpoint": dict(normalized.get("analysis_checkpoint") or {}),
        "summary": dict(normalized.get("summary") or {}),
        "artifact_root": str(normalized.get("artifact_root") or "").strip(),
        "worker_key": str(normalized.get("worker_key") or "").strip() or f"public_web_run::{run_id}",
        "lease_owner": str(normalized.get("lease_owner") or "").strip(),
        "lease_expires_at": str(normalized.get("lease_expires_at") or "").strip(),
        "attempt_count": attempt_count,
        "last_error": str(normalized.get("last_error") or "").strip(),
        "execution_backend": str(normalized.get("execution_backend") or "crm_public_web_v1").strip()
        or "crm_public_web_v1",
        "source_target_run_id": str(normalized.get("source_target_run_id") or "").strip(),
        "started_at": started_at,
        "completed_at": completed_at,
        "created_at": str(normalized.get("created_at") or "").strip(),
    }


def _normalize_company_public_web_asset_run_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    target_company = str(normalized.get("target_company") or normalized.get("company") or "").strip()
    company_key = str(normalized.get("company_key") or "").strip() or resolve_company_alias_key(target_company)
    source_families = _normalize_public_web_string_list(normalized.get("source_families"))
    seed_urls = _normalize_public_web_string_list(normalized.get("seed_urls") or normalized.get("urls"))
    options = dict(normalized.get("options") or {})
    force_refresh = bool(normalized.get("force_refresh"))
    idempotency_key = str(normalized.get("idempotency_key") or "").strip()
    if not idempotency_key:
        idempotency_key = "company-public-web-run:" + _public_web_hash_token(
            company_key,
            target_company,
            ",".join(sorted(source_families)),
            ",".join(sorted(seed_urls)),
            json.dumps(_json_safe_payload(options), sort_keys=True, ensure_ascii=False),
            str(force_refresh),
            str(normalized.get("refresh_nonce") or normalized.get("nonce") or "") if force_refresh else "",
        )
    run_id = (
        str(normalized.get("run_id") or "").strip()
        or f"company-public-web-run-{_public_web_hash_token(idempotency_key)}"
    )
    status = _normalize_target_candidate_public_web_status(normalized.get("status"))
    phase = str(normalized.get("phase") or status or "queued").strip().lower()
    started_at = str(normalized.get("started_at") or "").strip()
    if status != "queued" and not started_at:
        started_at = _utc_now_timestamp()
    completed_at = str(normalized.get("completed_at") or "").strip()
    if status in _TARGET_CANDIDATE_PUBLIC_WEB_TERMINAL_STATUSES and not completed_at:
        completed_at = _utc_now_timestamp()
    return {
        "run_id": run_id,
        "target_company": target_company,
        "company_key": company_key,
        "idempotency_key": idempotency_key,
        "status": status,
        "phase": phase,
        "source_families": source_families,
        "seed_urls": seed_urls,
        "options": options,
        "discovered_assets": _loads_json_list(normalized.get("discovered_assets"), default=[]),
        "summary": dict(normalized.get("summary") or {}),
        "artifact_root": str(normalized.get("artifact_root") or "").strip(),
        "requested_by": str(normalized.get("requested_by") or normalized.get("user_id") or "").strip(),
        "force_refresh": force_refresh,
        "started_at": started_at,
        "completed_at": completed_at,
        "last_error": str(normalized.get("last_error") or "").strip(),
        "metadata": dict(normalized.get("metadata") or {}),
        "created_at": str(normalized.get("created_at") or "").strip(),
    }


def _company_public_web_asset_run_row_payload(
    normalized: dict[str, Any],
    *,
    existing: dict[str, Any] | None,
    now: str,
) -> dict[str, Any]:
    created_at = str((existing or {}).get("created_at") or normalized.get("created_at") or "").strip() or now
    return _public_web_repo.COMPANY_PUBLIC_WEB_ASSET_RUNS.to_columns(
        {**normalized, "created_at": created_at, "updated_at": now}
    )


def _crm_public_web_batch_row_payload(
    normalized: dict[str, Any],
    *,
    existing: dict[str, Any] | None,
    now: str,
) -> dict[str, Any]:
    created_at = str((existing or {}).get("created_at") or normalized.get("created_at") or "").strip() or now
    return _public_web_repo.CRM_PUBLIC_WEB_BATCHES.to_columns(
        {**normalized, "created_at": created_at, "updated_at": now}
    )


def _crm_public_web_run_row_payload(
    normalized: dict[str, Any],
    *,
    existing: dict[str, Any] | None,
    now: str,
) -> dict[str, Any]:
    created_at = str((existing or {}).get("created_at") or normalized.get("created_at") or "").strip() or now
    return _public_web_repo.CRM_PUBLIC_WEB_RUNS.to_columns({**normalized, "created_at": created_at, "updated_at": now})


def _normalize_company_public_web_asset_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    target_company = str(normalized.get("target_company") or normalized.get("company") or "").strip()
    company_key = str(normalized.get("company_key") or "").strip() or resolve_company_alias_key(target_company)
    source_family = str(normalized.get("source_family") or "").strip() or "company_homepage"
    url = str(normalized.get("url") or normalized.get("source_url") or "").strip()
    normalized_url_key = str(normalized.get("normalized_url_key") or "").strip() or _normalize_public_url_key(url)
    latest_run_id = str(normalized.get("latest_run_id") or normalized.get("run_id") or "").strip()
    asset_kind = str(normalized.get("asset_kind") or "").strip() or "company_public_web_asset"
    asset_id = str(normalized.get("asset_id") or "").strip()
    if not asset_id:
        asset_id = "company-public-web-asset-" + _public_web_hash_token(company_key, source_family, normalized_url_key)
    source_run_ids = _normalize_public_web_string_list(normalized.get("source_run_ids"))
    if latest_run_id and latest_run_id not in source_run_ids:
        source_run_ids.append(latest_run_id)
    return {
        "asset_id": asset_id,
        "company_key": company_key,
        "target_company": target_company,
        "latest_run_id": latest_run_id,
        "source_family": source_family,
        "asset_kind": asset_kind,
        "title": str(normalized.get("title") or "").strip(),
        "url": url,
        "normalized_url_key": normalized_url_key,
        "summary": str(normalized.get("summary") or "").strip(),
        "model_safe_payload": dict(normalized.get("model_safe_payload") or {}),
        "source_run_ids": source_run_ids,
        "artifact_refs": dict(normalized.get("artifact_refs") or {}),
        "status": str(normalized.get("status") or "active").strip().lower() or "active",
        "metadata": dict(normalized.get("metadata") or {}),
        "created_at": str(normalized.get("created_at") or "").strip(),
    }


def _company_public_web_asset_row_payload(
    normalized: dict[str, Any],
    *,
    existing: dict[str, Any] | None,
    now: str,
) -> dict[str, Any]:
    merged_source_run_ids = _normalize_public_web_string_list(
        [*list((existing or {}).get("source_run_ids") or []), *list(normalized["source_run_ids"] or [])]
    )
    created_at = str((existing or {}).get("created_at") or normalized.get("created_at") or "").strip() or now
    return _public_web_repo.COMPANY_PUBLIC_WEB_ASSETS.to_columns(
        {**normalized, "source_run_ids": merged_source_run_ids, "created_at": created_at, "updated_at": now}
    )


def _normalize_public_url_key(url: Any) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    return sha1(text.lower().rstrip("/").encode("utf-8")).hexdigest()[:24]


def _target_candidate_public_web_run_row_payload(
    normalized: dict[str, Any],
    *,
    existing: dict[str, Any] | None,
    now: str,
) -> dict[str, Any]:
    created_at = str((existing or {}).get("created_at") or normalized.get("created_at") or "").strip() or now
    return _public_web_repo.TARGET_CANDIDATE_PUBLIC_WEB_RUNS.to_columns(
        {**normalized, "created_at": created_at, "updated_at": now}
    )


def _normalize_person_public_web_asset_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    linkedin_url_key = str(normalized.get("linkedin_url_key") or "").strip()
    person_identity_key = str(normalized.get("person_identity_key") or "").strip()
    if not person_identity_key:
        person_identity_key = f"linkedin:{linkedin_url_key}" if linkedin_url_key else ""
    asset_id = str(normalized.get("asset_id") or "").strip()
    if not asset_id:
        asset_id = f"person-public-web-{_public_web_hash_token(person_identity_key)}"
    source_run_ids = _normalize_public_web_string_list(normalized.get("source_run_ids"))
    latest_run_id = str(normalized.get("latest_run_id") or "").strip()
    if latest_run_id and latest_run_id not in source_run_ids:
        source_run_ids.append(latest_run_id)
    return {
        "asset_id": asset_id,
        "person_identity_key": person_identity_key,
        "linkedin_url_key": linkedin_url_key,
        "latest_run_id": latest_run_id,
        "target_candidate_record_id": str(normalized.get("target_candidate_record_id") or "").strip(),
        "candidate_name": str(normalized.get("candidate_name") or "").strip(),
        "current_company": str(normalized.get("current_company") or "").strip(),
        "status": _normalize_target_candidate_public_web_status(normalized.get("status"), default="completed"),
        "summary": dict(normalized.get("summary") or {}),
        "signals": dict(normalized.get("signals") or {}),
        "source_run_ids": source_run_ids,
        "artifact_root": str(normalized.get("artifact_root") or "").strip(),
        "metadata": dict(normalized.get("metadata") or {}),
        "created_at": str(normalized.get("created_at") or "").strip(),
    }


def _normalize_person_public_web_signal_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    run_id = str(normalized.get("run_id") or "").strip()
    person_identity_key = str(normalized.get("person_identity_key") or "").strip()
    linkedin_url_key = str(normalized.get("linkedin_url_key") or "").strip()
    asset_id = str(normalized.get("asset_id") or "").strip()
    if not asset_id and person_identity_key.startswith("linkedin:"):
        asset_id = f"person-public-web-{_public_web_hash_token(person_identity_key)}"
    signal_kind = str(normalized.get("signal_kind") or normalized.get("kind") or "").strip()
    signal_type = str(normalized.get("signal_type") or normalized.get("type") or "").strip()
    value = str(normalized.get("value") or "").strip()
    normalized_value = str(normalized.get("normalized_value") or value).strip()
    url = str(normalized.get("url") or "").strip()
    source_url = str(normalized.get("source_url") or url).strip()
    signal_id = str(normalized.get("signal_id") or "").strip()
    if not signal_id:
        signal_id = public_web_signal_id_for_identity(
            person_identity_key=person_identity_key,
            record_id=str(normalized.get("record_id") or "").strip(),
            signal_kind=signal_kind,
            signal_type=signal_type,
            normalized_value=normalized_value,
            value=value,
            url=url,
            source_url=source_url,
        )
    return {
        "signal_id": signal_id,
        "run_id": run_id,
        "asset_id": asset_id,
        "person_identity_key": person_identity_key,
        "record_id": str(normalized.get("record_id") or "").strip(),
        "candidate_id": str(normalized.get("candidate_id") or "").strip(),
        "candidate_name": str(normalized.get("candidate_name") or "").strip(),
        "current_company": str(normalized.get("current_company") or "").strip(),
        "linkedin_url_key": linkedin_url_key,
        "signal_kind": signal_kind,
        "signal_type": signal_type,
        "email_type": str(normalized.get("email_type") or "").strip(),
        "value": value,
        "normalized_value": normalized_value,
        "url": url,
        "source_url": source_url,
        "source_domain": str(normalized.get("source_domain") or "").strip(),
        "source_family": str(normalized.get("source_family") or "").strip(),
        "source_title": str(normalized.get("source_title") or "").strip(),
        "confidence_label": str(normalized.get("confidence_label") or "").strip(),
        "confidence_score": _coerce_public_web_float(normalized.get("confidence_score")),
        "identity_match_label": str(normalized.get("identity_match_label") or "").strip(),
        "identity_match_score": _coerce_public_web_float(normalized.get("identity_match_score")),
        "publishable": bool(normalized.get("publishable")),
        "promotion_status": str(normalized.get("promotion_status") or "").strip(),
        "suppression_reason": str(normalized.get("suppression_reason") or "").strip(),
        "evidence_excerpt": str(normalized.get("evidence_excerpt") or "").strip(),
        "artifact_refs": dict(normalized.get("artifact_refs") or {}),
        "model_provider": str(normalized.get("model_provider") or "").strip(),
        "model_version": str(normalized.get("model_version") or "").strip(),
        "metadata": dict(normalized.get("metadata") or {}),
        "created_at": str(normalized.get("created_at") or "").strip(),
    }


def _person_public_web_signal_row_payload(
    normalized: dict[str, Any],
    *,
    existing: dict[str, Any] | None,
    now: str,
) -> dict[str, Any]:
    created_at = str((existing or {}).get("created_at") or normalized.get("created_at") or "").strip() or now
    return _public_web_repo.PERSON_PUBLIC_WEB_SIGNALS.to_columns(
        {**normalized, "created_at": created_at, "updated_at": now}
    )


def _normalize_target_candidate_public_web_promotion_action(value: Any, *, default: str = "promote") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"promote", "reject"}:
        return normalized
    return default


def _normalize_target_candidate_public_web_promotion_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    action = _normalize_target_candidate_public_web_promotion_action(normalized.get("action"))
    promotion_status = str(normalized.get("promotion_status") or "").strip()
    if not promotion_status:
        promotion_status = "manually_promoted" if action == "promote" else "manually_rejected"
    signal_id = str(normalized.get("signal_id") or "").strip()
    record_id = str(normalized.get("record_id") or "").strip()
    normalized_value = str(
        normalized.get("normalized_value") or normalized.get("value") or normalized.get("url") or ""
    ).strip()
    promotion_id = str(normalized.get("promotion_id") or normalized.get("id") or "").strip()
    if not promotion_id:
        promotion_id = "target-candidate-public-web-promotion-" + _public_web_hash_token(
            record_id,
            signal_id,
            action,
            normalized_value,
            normalized.get("created_at") or _utc_now_timestamp(),
        )
    link_shape_warnings = _normalize_public_web_string_list(normalized.get("link_shape_warnings"))
    metadata = dict(normalized.get("metadata") or {})
    return {
        "promotion_id": promotion_id,
        "signal_id": signal_id,
        "run_id": str(normalized.get("run_id") or "").strip(),
        "asset_id": str(normalized.get("asset_id") or "").strip(),
        "person_identity_key": str(normalized.get("person_identity_key") or "").strip(),
        "record_id": record_id,
        "candidate_id": str(normalized.get("candidate_id") or "").strip(),
        "candidate_name": str(normalized.get("candidate_name") or "").strip(),
        "current_company": str(normalized.get("current_company") or "").strip(),
        "linkedin_url_key": str(normalized.get("linkedin_url_key") or "").strip(),
        "signal_kind": str(normalized.get("signal_kind") or "").strip(),
        "signal_type": str(normalized.get("signal_type") or "").strip(),
        "email_type": str(normalized.get("email_type") or "").strip(),
        "value": str(normalized.get("value") or "").strip(),
        "normalized_value": normalized_value,
        "url": str(normalized.get("url") or "").strip(),
        "source_url": str(normalized.get("source_url") or "").strip(),
        "source_domain": str(normalized.get("source_domain") or "").strip(),
        "source_family": str(normalized.get("source_family") or "").strip(),
        "source_title": str(normalized.get("source_title") or "").strip(),
        "confidence_label": str(normalized.get("confidence_label") or "").strip(),
        "confidence_score": _coerce_public_web_float(normalized.get("confidence_score")),
        "identity_match_label": str(normalized.get("identity_match_label") or "").strip(),
        "identity_match_score": _coerce_public_web_float(normalized.get("identity_match_score")),
        "publishable": bool(normalized.get("publishable")),
        "clean_profile_link": bool(normalized.get("clean_profile_link")),
        "link_shape_warnings": link_shape_warnings,
        "action": action,
        "promotion_status": promotion_status,
        "promoted_field": str(normalized.get("promoted_field") or "").strip(),
        "previous_value": str(normalized.get("previous_value") or "").strip(),
        "new_value": str(normalized.get("new_value") or normalized_value).strip(),
        "operator": str(normalized.get("operator") or "operator").strip() or "operator",
        "note": str(normalized.get("note") or "").strip(),
        "evidence_excerpt": str(normalized.get("evidence_excerpt") or "").strip(),
        "metadata": metadata,
        "created_at": str(normalized.get("created_at") or "").strip(),
    }


def _normalize_crm_public_web_promotion_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    action = _normalize_target_candidate_public_web_promotion_action(normalized.get("action"))
    promotion_status = str(normalized.get("promotion_status") or "").strip()
    if not promotion_status:
        promotion_status = "manually_promoted" if action == "promote" else "manually_rejected"
    signal_id = str(normalized.get("signal_id") or "").strip()
    crm_record_id = str(normalized.get("crm_record_id") or normalized.get("record_id") or "").strip()
    normalized_value = str(
        normalized.get("normalized_value") or normalized.get("value") or normalized.get("url") or ""
    ).strip()
    promotion_id = str(normalized.get("promotion_id") or normalized.get("id") or "").strip()
    if not promotion_id:
        promotion_id = "crm-public-web-promotion-" + _public_web_hash_token(
            str(normalized.get("workspace_id") or "default").strip() or "default",
            crm_record_id,
            signal_id,
            action,
            normalized_value,
            normalized.get("created_at") or _utc_now_timestamp(),
        )
    link_shape_warnings = _normalize_public_web_string_list(normalized.get("link_shape_warnings"))
    metadata = dict(normalized.get("metadata") or {})
    return {
        "promotion_id": promotion_id,
        "signal_id": signal_id,
        "run_id": str(normalized.get("run_id") or "").strip(),
        "asset_id": str(normalized.get("asset_id") or "").strip(),
        "person_identity_key": str(normalized.get("person_identity_key") or "").strip(),
        "crm_record_id": crm_record_id,
        "workspace_id": str(normalized.get("workspace_id") or "default").strip() or "default",
        "candidate_id": str(normalized.get("candidate_id") or "").strip(),
        "candidate_name": str(normalized.get("candidate_name") or "").strip(),
        "current_company": str(normalized.get("current_company") or "").strip(),
        "linkedin_url_key": str(normalized.get("linkedin_url_key") or "").strip(),
        "signal_kind": str(normalized.get("signal_kind") or "").strip(),
        "signal_type": str(normalized.get("signal_type") or "").strip(),
        "email_type": str(normalized.get("email_type") or "").strip(),
        "value": str(normalized.get("value") or "").strip(),
        "normalized_value": normalized_value,
        "url": str(normalized.get("url") or "").strip(),
        "source_url": str(normalized.get("source_url") or "").strip(),
        "source_domain": str(normalized.get("source_domain") or "").strip(),
        "source_family": str(normalized.get("source_family") or "").strip(),
        "source_title": str(normalized.get("source_title") or "").strip(),
        "confidence_label": str(normalized.get("confidence_label") or "").strip(),
        "confidence_score": _coerce_public_web_float(normalized.get("confidence_score")),
        "identity_match_label": str(normalized.get("identity_match_label") or "").strip(),
        "identity_match_score": _coerce_public_web_float(normalized.get("identity_match_score")),
        "publishable": bool(normalized.get("publishable")),
        "clean_profile_link": bool(normalized.get("clean_profile_link")),
        "link_shape_warnings": link_shape_warnings,
        "action": action,
        "promotion_status": promotion_status,
        "promoted_field": str(normalized.get("promoted_field") or "").strip(),
        "previous_value": str(normalized.get("previous_value") or "").strip(),
        "new_value": str(normalized.get("new_value") or normalized_value).strip(),
        "operator": str(normalized.get("operator") or "operator").strip() or "operator",
        "note": str(normalized.get("note") or "").strip(),
        "evidence_excerpt": str(normalized.get("evidence_excerpt") or "").strip(),
        "execution_backend": str(normalized.get("execution_backend") or "crm_public_web_v1").strip()
        or "crm_public_web_v1",
        "source_target_promotion_id": str(normalized.get("source_target_promotion_id") or "").strip(),
        "metadata": metadata,
        "created_at": str(normalized.get("created_at") or "").strip(),
    }


def _target_candidate_public_web_promotion_row_payload(
    normalized: dict[str, Any],
    *,
    existing: dict[str, Any] | None,
    now: str,
) -> dict[str, Any]:
    created_at = str((existing or {}).get("created_at") or normalized.get("created_at") or "").strip() or now
    return _public_web_repo.TARGET_CANDIDATE_PUBLIC_WEB_PROMOTIONS.to_columns(
        {**normalized, "created_at": created_at, "updated_at": now}
    )


def _crm_public_web_promotion_row_payload(
    normalized: dict[str, Any],
    *,
    existing: dict[str, Any] | None,
    now: str,
) -> dict[str, Any]:
    created_at = str((existing or {}).get("created_at") or normalized.get("created_at") or "").strip() or now
    return _public_web_repo.CRM_PUBLIC_WEB_PROMOTIONS.to_columns(
        {**normalized, "created_at": created_at, "updated_at": now}
    )


def _coerce_public_web_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _row_value(row: Any, key: str, default: Any = "") -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except Exception:
        return default


def _utc_now_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _milliseconds_between(start_value: str, end_value: str) -> int | None:
    start = _parse_sqlite_timestamp(start_value)
    end = _parse_sqlite_timestamp(end_value)
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds() * 1000))


def _normalize_dispatch_scope(scope: str, *, requester_id: str = "", tenant_id: str = "") -> str:
    normalized = str(scope or "").strip().lower()
    if normalized in {"global", "tenant", "requester"}:
        return normalized
    if str(tenant_id or "").strip():
        return "tenant"
    if str(requester_id or "").strip():
        return "requester"
    return "global"


def _job_matches_dispatch_scope(
    job_payload: dict[str, Any],
    *,
    scope: str,
    requester_id: str = "",
    tenant_id: str = "",
) -> bool:
    normalized_scope = _normalize_dispatch_scope(scope, requester_id=requester_id, tenant_id=tenant_id)
    job_requester_id = str(job_payload.get("requester_id") or "").strip()
    job_tenant_id = str(job_payload.get("tenant_id") or "").strip()
    if normalized_scope == "global":
        return True
    if normalized_scope == "tenant":
        normalized_tenant_id = str(tenant_id or "").strip()
        return bool(normalized_tenant_id and job_tenant_id and normalized_tenant_id == job_tenant_id)
    if normalized_scope == "requester":
        normalized_requester_id = str(requester_id or "").strip()
        return bool(normalized_requester_id and job_requester_id and normalized_requester_id == job_requester_id)
    return False


def _job_match_sort_key(match: dict[str, Any], row: dict[str, Any] | None) -> tuple[float, str, str]:
    created_at = ""
    updated_at = ""
    if row is not None:
        created_at = str(row["created_at"] or "")
        updated_at = str(row["updated_at"] or "")
    return (
        float(match.get("score") or 0.0),
        updated_at,
        created_at,
    )
