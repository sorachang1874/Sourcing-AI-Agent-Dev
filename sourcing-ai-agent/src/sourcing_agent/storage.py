from __future__ import annotations

import ast
import json
import os
import re
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any
from urllib import parse

from .asset_paths import extract_company_snapshot_ref
from .asset_governance import (
    build_canonical_asset_replacement_plan,
    default_asset_pointer_key,
    default_asset_pointer_history_id,
    normalize_default_asset_pointer_payload,
)
from .company_registry import normalize_company_key, resolve_company_alias_key
from .control_plane_job_progress import (
    build_job_progress_event_summary as _cp_build_job_progress_event_summary,
)
from .control_plane_job_progress import (
    merge_progress_event_metrics as _cp_merge_progress_event_metrics,
)
from .control_plane_job_progress import (
    update_job_progress_event_summary as _cp_update_job_progress_event_summary,
)
from .control_plane_live_postgres import (
    LiveControlPlanePostgresAdapter,
    resolve_control_plane_postgres_live_mode,
)
from .domain import Candidate, EvidenceRecord, JobRequest, normalize_candidate
from .local_postgres import resolve_control_plane_postgres_dsn
from .request_matching import (
    MATCH_THRESHOLD,
    build_request_family_match_explanation,
    build_request_matching_bundle,
    matching_request_family_signature,
    matching_request_signature,
    request_family_score,
    request_family_signature,
    request_signature,
)
from .runtime_environment import current_runtime_environment
from .worker_scheduler import effective_worker_status, wait_stage

_TERMINAL_JOB_STATUSES = {"completed", "failed"}
_CONTROL_PLANE_POSTGRES_NATIVE_READ_METHODS = {
    "get_agent_trace_span",
    "list_agent_trace_spans",
    "list_recoverable_agent_workers",
    "get_workflow_job_lease",
    "get_agent_worker",
    "list_agent_workers",
}
_CONTROL_PLANE_POSTGRES_NATIVE_TABLES = {
    "save_job_row": "jobs",
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
    "acquire_workflow_job_lease": "workflow_job_leases",
    "get_workflow_job_lease": "workflow_job_leases",
    "renew_workflow_job_lease": "workflow_job_leases",
    "release_workflow_job_lease": "workflow_job_leases",
    "supersede_workflow_runtime_state": "agent_worker_runs",
}


def _env_flag_enabled(value: Any, *, default: bool) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return bool(default)
    return normalized not in {"0", "false", "no", "off"}


def _control_plane_postgres_required(runtime_dir: Path | None = None) -> bool:
    if _env_flag_enabled(os.getenv("SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES"), default=False):
        return True
    if os.getenv("SOURCING_ALLOW_PRODUCTION_SQLITE_CONTROL_PLANE") == "1":
        return False
    runtime = current_runtime_environment(runtime_dir=runtime_dir)
    return bool(runtime.is_production)


def _resolve_postgres_only_sqlite_backend(
    *,
    db_path: Path,
    control_plane_postgres_mode: str,
) -> tuple[str, str, bool]:
    normalized_mode = str(control_plane_postgres_mode or "").strip().lower()
    configured_backend = str(os.getenv("SOURCING_PG_ONLY_SQLITE_BACKEND") or "").strip().lower()
    if configured_backend == "memory":
        configured_backend = "shared_memory"
    if normalized_mode != "postgres_only" or configured_backend == "disk":
        return "disk", str(db_path), False
    backend = configured_backend or "shared_memory"
    if backend != "shared_memory":
        return "disk", str(db_path), False
    seed = str(db_path.expanduser())
    uri = f"file:sourcing-agent-shadow-{sha1(seed.encode('utf-8')).hexdigest()[:16]}?mode=memory&cache=shared"
    return backend, uri, True


def _json_safe_payload(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return bytes(value).decode("utf-8", errors="replace")
    to_record = getattr(value, "to_record", None)
    if callable(to_record):
        return _json_safe_payload(to_record())
    if isinstance(value, dict):
        return {str(key): _json_safe_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_payload(item) for item in value]
    return value


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


def _matching_bundle_payload(
    request_payload: dict[str, Any],
    *,
    execution_bundle_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    execution_bundle_payload = dict(execution_bundle_payload or {})
    request_matching = dict(execution_bundle_payload.get("request_matching") or {})
    if request_matching:
        matching_request = dict(request_matching.get("matching_request") or {})
        matching_family_request = dict(request_matching.get("matching_family_request") or {})
        if matching_request and matching_family_request:
            request_matching["matching_request"] = matching_request
            request_matching["matching_family_request"] = matching_family_request
            request_matching.setdefault(
                "matching_request_signature",
                matching_request_signature(request_payload),
            )
            request_matching.setdefault(
                "matching_request_family_signature",
                matching_request_family_signature(request_payload),
            )
            return request_matching
    return build_request_matching_bundle(request_payload)


def _request_signature_context(
    request_payload: dict[str, Any],
    *,
    execution_bundle_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_request = dict(request_payload or {})
    if not normalized_request:
        return {
            "request_signature": "",
            "request_family_signature": "",
            "matching_request_signature": "",
            "matching_request_family_signature": "",
            "request_matching": {},
        }
    matching_bundle = _matching_bundle_payload(
        normalized_request,
        execution_bundle_payload=execution_bundle_payload,
    )
    return {
        "request_signature": request_signature(normalized_request),
        "request_family_signature": request_family_signature(normalized_request),
        "matching_request_signature": str(matching_bundle.get("matching_request_signature") or ""),
        "matching_request_family_signature": str(matching_bundle.get("matching_request_family_signature") or ""),
        "request_matching": matching_bundle,
    }


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


class SQLiteStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._bootstrap_candidate_store_loaded = False
        runtime_dir = self.db_path.parent
        control_plane_postgres_dsn = resolve_control_plane_postgres_dsn(runtime_dir)
        control_plane_postgres_mode = resolve_control_plane_postgres_live_mode(
            os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE")
            or ("postgres_only" if control_plane_postgres_dsn else "disabled")
        )
        self._sqlite_backend_mode, self._sqlite_connect_target, self._sqlite_connect_uri = (
            _resolve_postgres_only_sqlite_backend(
                db_path=self.db_path,
                control_plane_postgres_mode=control_plane_postgres_mode,
            )
        )
        if _control_plane_postgres_required(runtime_dir):
            if not control_plane_postgres_dsn:
                raise RuntimeError(
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1 but no control-plane Postgres DSN was resolved."
                )
            if control_plane_postgres_mode != "postgres_only":
                raise RuntimeError(
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1 requires "
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only."
                )
            if self._sqlite_backend_mode == "disk":
                raise RuntimeError(
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1 refuses disk-backed SQLite shadow storage. "
                    "Use the default shared_memory shadow backend instead."
                )
        self._control_plane_postgres = LiveControlPlanePostgresAdapter(
            runtime_dir=runtime_dir,
            sqlite_path=self._sqlite_connect_target,
            dsn=control_plane_postgres_dsn,
            mode=control_plane_postgres_mode,
        )
        self._connection = sqlite3.connect(
            self._sqlite_connect_target,
            check_same_thread=False,
            timeout=60.0,
            uri=self._sqlite_connect_uri,
        )
        self._connection.row_factory = sqlite3.Row
        self._configure_connection()
        self.init_schema()

    def control_plane_postgres_live_mode(self) -> str:
        return str(getattr(self._control_plane_postgres, "mode", "disabled") or "disabled")

    def control_plane_postgres_is_postgres_only(self) -> bool:
        return self.control_plane_postgres_live_mode() == "postgres_only"

    def sqlite_shadow_backend(self) -> str:
        return str(self._sqlite_backend_mode or "disk")

    def sqlite_shadow_connect_target(self) -> str:
        return str(self._sqlite_connect_target or self.db_path)

    def sqlite_shadow_is_ephemeral(self) -> bool:
        return self.sqlite_shadow_backend() != "disk"

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
        message = (
            f"Postgres authoritative write failed for {table_name} via {method_name}: {reason}"
        )
        if error is not None:
            raise RuntimeError(message) from error
        raise RuntimeError(message)

    def _call_control_plane_postgres_native(self, method_name: str, /, *args: Any, **kwargs: Any) -> Any:
        normalized_method_name = str(method_name or "").strip()
        table_name = self._control_plane_postgres_native_table_name(normalized_method_name, kwargs)
        strict_no_fallback = bool(
            table_name
            and normalized_method_name not in _CONTROL_PLANE_POSTGRES_NATIVE_READ_METHODS
            and self._control_plane_postgres_should_skip_sqlite_fallback(table_name)
        )
        method = getattr(self._control_plane_postgres, method_name, None)
        if method is None:
            if strict_no_fallback:
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

    def _mirror_control_plane_row(self, table_name: str, row: sqlite3.Row | dict[str, Any] | None) -> None:
        if row is None or not self._control_plane_postgres.should_mirror(table_name):
            return
        try:
            self._control_plane_postgres.upsert_row(table_name, dict(row))
        except Exception:
            return

    def _replace_control_plane_table_from_sqlite(self, table_name: str) -> None:
        if not self._control_plane_postgres.should_mirror(table_name):
            return
        try:
            self._control_plane_postgres.replace_table_from_sqlite(table_name)
        except Exception:
            return

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
            )
        except Exception:
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
        except Exception:
            return None
        if row is None:
            return None
        return row_builder(row)

    def _configure_connection(self) -> None:
        with self._lock:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=NORMAL")
            self._connection.execute("PRAGMA busy_timeout = 60000")
            self._connection.execute("PRAGMA foreign_keys = ON")

    def init_schema(self) -> None:
        with self._lock, self._connection:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS candidates (
                    candidate_id TEXT PRIMARY KEY,
                    name_en TEXT NOT NULL,
                    name_zh TEXT,
                    display_name TEXT,
                    category TEXT,
                    target_company TEXT,
                    organization TEXT,
                    employment_status TEXT,
                    role TEXT,
                    team TEXT,
                    joined_at TEXT,
                    left_at TEXT,
                    current_destination TEXT,
                    ethnicity_background TEXT,
                    investment_involvement TEXT,
                    focus_areas TEXT,
                    education TEXT,
                    work_history TEXT,
                    notes TEXT,
                    linkedin_url TEXT,
                    media_url TEXT,
                    source_dataset TEXT,
                    source_path TEXT,
                    metadata_json TEXT
                );

                CREATE TABLE IF NOT EXISTS evidence (
                    evidence_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL,
                    source_type TEXT,
                    title TEXT,
                    url TEXT,
                    summary TEXT,
                    source_dataset TEXT,
                    source_path TEXT,
                    metadata_json TEXT
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL DEFAULT 'retrieval',
                    status TEXT NOT NULL,
                    stage TEXT NOT NULL DEFAULT 'pending',
                    request_json TEXT NOT NULL,
                    plan_json TEXT,
                    execution_bundle_json TEXT NOT NULL DEFAULT '{}',
                    matching_request_json TEXT NOT NULL DEFAULT '{}',
                    summary_json TEXT,
                    artifact_path TEXT,
                    request_signature TEXT,
                    request_family_signature TEXT,
                    matching_request_signature TEXT,
                    matching_request_family_signature TEXT,
                    requester_id TEXT,
                    tenant_id TEXT,
                    idempotency_key TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS job_results (
                    job_id TEXT NOT NULL,
                    candidate_id TEXT NOT NULL,
                    rank_index INTEGER NOT NULL,
                    score REAL NOT NULL,
                    confidence_label TEXT,
                    confidence_score REAL,
                    confidence_reason TEXT,
                    explanation TEXT,
                    matched_fields_json TEXT,
                    PRIMARY KEY (job_id, candidate_id)
                );

                CREATE TABLE IF NOT EXISTS job_result_views (
                    view_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL UNIQUE,
                    target_company TEXT NOT NULL DEFAULT '',
                    company_key TEXT,
                    source_kind TEXT NOT NULL DEFAULT '',
                    view_kind TEXT NOT NULL DEFAULT '',
                    snapshot_id TEXT NOT NULL DEFAULT '',
                    asset_view TEXT NOT NULL DEFAULT 'canonical_merged',
                    source_path TEXT NOT NULL DEFAULT '',
                    authoritative_snapshot_id TEXT NOT NULL DEFAULT '',
                    materialization_generation_key TEXT NOT NULL DEFAULT '',
                    request_signature TEXT NOT NULL DEFAULT '',
                    summary_json TEXT NOT NULL DEFAULT '{}',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS job_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    detail TEXT,
                    payload_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS job_progress_event_summaries (
                    job_id TEXT PRIMARY KEY,
                    event_count INTEGER NOT NULL DEFAULT 0,
                    latest_event_json TEXT NOT NULL DEFAULT '{}',
                    stage_sequence_json TEXT NOT NULL DEFAULT '[]',
                    stage_stats_json TEXT NOT NULL DEFAULT '{}',
                    latest_metrics_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS criteria_feedback (
                    feedback_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT,
                    candidate_id TEXT,
                    target_company TEXT,
                    feedback_type TEXT NOT NULL,
                    subject TEXT,
                    value TEXT,
                    reviewer TEXT,
                    notes TEXT,
                    payload_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS criteria_patterns (
                    pattern_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_company TEXT,
                    pattern_type TEXT NOT NULL,
                    subject TEXT,
                    value TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    confidence TEXT NOT NULL DEFAULT 'medium',
                    source_feedback_id INTEGER,
                    metadata_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(target_company, pattern_type, subject, value)
                );

                CREATE TABLE IF NOT EXISTS criteria_versions (
                    version_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_company TEXT,
                    request_signature TEXT NOT NULL,
                    source_kind TEXT NOT NULL DEFAULT 'plan',
                    parent_version_id INTEGER,
                    trigger_feedback_id INTEGER,
                    evolution_stage TEXT NOT NULL DEFAULT 'planned',
                    request_json TEXT NOT NULL,
                    plan_json TEXT NOT NULL,
                    patterns_json TEXT,
                    notes TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS criteria_compiler_runs (
                    compiler_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER,
                    job_id TEXT,
                    trigger_feedback_id INTEGER,
                    provider_name TEXT NOT NULL,
                    compiler_kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    output_json TEXT NOT NULL,
                    notes TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS criteria_result_diffs (
                    diff_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_company TEXT,
                    trigger_feedback_id INTEGER,
                    criteria_version_id INTEGER,
                    baseline_job_id TEXT,
                    rerun_job_id TEXT,
                    summary_json TEXT NOT NULL,
                    diff_json TEXT NOT NULL,
                    artifact_path TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS confidence_policy_runs (
                    policy_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_company TEXT,
                    job_id TEXT,
                    criteria_version_id INTEGER,
                    trigger_feedback_id INTEGER,
                    request_signature TEXT,
                    request_family_signature TEXT,
                    matching_request_signature TEXT,
                    matching_request_family_signature TEXT,
                    scope_kind TEXT NOT NULL DEFAULT 'company',
                    high_threshold REAL NOT NULL,
                    medium_threshold REAL NOT NULL,
                    summary_json TEXT NOT NULL,
                    policy_json TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS confidence_policy_controls (
                    control_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_company TEXT,
                    request_signature TEXT,
                    request_family_signature TEXT,
                    matching_request_signature TEXT,
                    matching_request_family_signature TEXT,
                    scope_kind TEXT NOT NULL DEFAULT 'request_family',
                    control_mode TEXT NOT NULL DEFAULT 'override',
                    status TEXT NOT NULL DEFAULT 'active',
                    high_threshold REAL NOT NULL,
                    medium_threshold REAL NOT NULL,
                    reviewer TEXT,
                    notes TEXT,
                    locked_policy_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS plan_review_sessions (
                    review_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_company TEXT,
                    request_signature TEXT,
                    request_family_signature TEXT,
                    matching_request_signature TEXT,
                    matching_request_family_signature TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    risk_level TEXT NOT NULL DEFAULT 'medium',
                    required_before_execution INTEGER NOT NULL DEFAULT 1,
                    request_json TEXT NOT NULL,
                    plan_json TEXT NOT NULL,
                    gate_json TEXT NOT NULL,
                    execution_bundle_json TEXT NOT NULL DEFAULT '{}',
                    matching_request_json TEXT NOT NULL DEFAULT '{}',
                    decision_json TEXT,
                    reviewer TEXT,
                    review_notes TEXT,
                    approved_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS criteria_pattern_suggestions (
                    suggestion_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_company TEXT,
                    request_signature TEXT,
                    request_family_signature TEXT,
                    matching_request_signature TEXT,
                    matching_request_family_signature TEXT,
                    source_feedback_id INTEGER,
                    source_job_id TEXT,
                    candidate_id TEXT NOT NULL DEFAULT '',
                    pattern_type TEXT NOT NULL,
                    subject TEXT,
                    value TEXT,
                    status TEXT NOT NULL DEFAULT 'suggested',
                    confidence TEXT NOT NULL DEFAULT 'medium',
                    rationale TEXT,
                    evidence_json TEXT,
                    metadata_json TEXT,
                    reviewed_by TEXT,
                    review_notes TEXT,
                    applied_pattern_id INTEGER,
                    reviewed_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(target_company, request_family_signature, candidate_id, pattern_type, subject, value)
                );

                CREATE TABLE IF NOT EXISTS manual_review_items (
                    review_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT,
                    candidate_id TEXT,
                    target_company TEXT,
                    review_type TEXT NOT NULL,
                    priority TEXT NOT NULL DEFAULT 'medium',
                    status TEXT NOT NULL DEFAULT 'open',
                    summary TEXT,
                    candidate_json TEXT NOT NULL,
                    evidence_json TEXT,
                    metadata_json TEXT,
                    reviewed_by TEXT,
                    review_notes TEXT,
                    reviewed_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS candidate_review_registry (
                    record_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL DEFAULT '',
                    history_id TEXT NOT NULL DEFAULT '',
                    candidate_id TEXT NOT NULL DEFAULT '',
                    candidate_name TEXT NOT NULL DEFAULT '',
                    headline TEXT,
                    current_company TEXT,
                    avatar_url TEXT,
                    linkedin_url TEXT,
                    primary_email TEXT,
                    status TEXT NOT NULL DEFAULT 'needs_review',
                    comment TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT 'manual_review',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    added_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS target_candidates (
                    record_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL DEFAULT '',
                    history_id TEXT NOT NULL DEFAULT '',
                    job_id TEXT NOT NULL DEFAULT '',
                    candidate_name TEXT NOT NULL DEFAULT '',
                    headline TEXT,
                    current_company TEXT,
                    avatar_url TEXT,
                    linkedin_url TEXT,
                    primary_email TEXT,
                    follow_up_status TEXT NOT NULL DEFAULT 'pending_outreach',
                    quality_score REAL,
                    comment TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    added_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS asset_default_pointers (
                    pointer_key TEXT PRIMARY KEY,
                    company_key TEXT NOT NULL DEFAULT '',
                    scope_kind TEXT NOT NULL DEFAULT 'company',
                    scope_key TEXT NOT NULL DEFAULT '',
                    asset_kind TEXT NOT NULL DEFAULT 'company_asset',
                    snapshot_id TEXT NOT NULL DEFAULT '',
                    lifecycle_status TEXT NOT NULL DEFAULT 'canonical',
                    coverage_proof_json TEXT NOT NULL DEFAULT '{}',
                    previous_snapshot_id TEXT NOT NULL DEFAULT '',
                    promoted_by_job_id TEXT NOT NULL DEFAULT '',
                    promoted_at TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS asset_default_pointer_history (
                    history_id TEXT PRIMARY KEY,
                    pointer_key TEXT NOT NULL DEFAULT '',
                    company_key TEXT NOT NULL DEFAULT '',
                    scope_kind TEXT NOT NULL DEFAULT 'company',
                    scope_key TEXT NOT NULL DEFAULT '',
                    asset_kind TEXT NOT NULL DEFAULT 'company_asset',
                    snapshot_id TEXT NOT NULL DEFAULT '',
                    lifecycle_status TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    occurred_at TEXT NOT NULL DEFAULT '',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS frontend_history_links (
                    history_id TEXT PRIMARY KEY,
                    query_text TEXT NOT NULL DEFAULT '',
                    target_company TEXT NOT NULL DEFAULT '',
                    review_id INTEGER NOT NULL DEFAULT 0,
                    job_id TEXT NOT NULL DEFAULT '',
                    phase TEXT NOT NULL DEFAULT '',
                    request_json TEXT NOT NULL DEFAULT '{}',
                    plan_json TEXT NOT NULL DEFAULT '{}',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS agent_runtime_sessions (
                    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    target_company TEXT,
                    request_signature TEXT,
                    request_family_signature TEXT,
                    runtime_mode TEXT NOT NULL DEFAULT 'agent_runtime',
                    status TEXT NOT NULL DEFAULT 'running',
                    lanes_json TEXT NOT NULL,
                    metadata_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(job_id)
                );

                CREATE TABLE IF NOT EXISTS agent_trace_spans (
                    span_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    job_id TEXT NOT NULL,
                    parent_span_id INTEGER,
                    lane_id TEXT NOT NULL,
                    handoff_from_lane TEXT,
                    handoff_to_lane TEXT,
                    span_name TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'running',
                    input_json TEXT,
                    output_json TEXT,
                    metadata_json TEXT,
                    started_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    completed_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS agent_worker_runs (
                    worker_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    job_id TEXT NOT NULL,
                    span_id INTEGER,
                    lane_id TEXT NOT NULL,
                    worker_key TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    interrupt_requested INTEGER NOT NULL DEFAULT 0,
                    budget_json TEXT,
                    checkpoint_json TEXT,
                    input_json TEXT,
                    output_json TEXT,
                    metadata_json TEXT,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(job_id, lane_id, worker_key)
                );

                CREATE TABLE IF NOT EXISTS workflow_job_leases (
                    job_id TEXT PRIMARY KEY,
                    lease_owner TEXT NOT NULL,
                    lease_token TEXT NOT NULL,
                    lease_expires_at TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS query_dispatches (
                    dispatch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_company TEXT,
                    request_signature TEXT NOT NULL,
                    request_family_signature TEXT NOT NULL,
                    matching_request_signature TEXT,
                    matching_request_family_signature TEXT,
                    requester_id TEXT,
                    tenant_id TEXT,
                    idempotency_key TEXT,
                    strategy TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source_job_id TEXT,
                    created_job_id TEXT,
                    matching_request_json TEXT NOT NULL DEFAULT '{}',
                    payload_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS linkedin_profile_registry (
                    profile_url_key TEXT PRIMARY KEY,
                    profile_url TEXT NOT NULL,
                    raw_linkedin_url TEXT,
                    sanity_linkedin_url TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    last_run_id TEXT,
                    last_dataset_id TEXT,
                    last_snapshot_dir TEXT,
                    last_raw_path TEXT,
                    first_queued_at TEXT,
                    last_queued_at TEXT,
                    last_fetched_at TEXT,
                    last_failed_at TEXT,
                    source_shards_json TEXT NOT NULL DEFAULT '[]',
                    source_jobs_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS linkedin_profile_registry_aliases (
                    alias_url_key TEXT PRIMARY KEY,
                    profile_url_key TEXT NOT NULL,
                    alias_url TEXT NOT NULL,
                    alias_kind TEXT NOT NULL DEFAULT 'observed',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS linkedin_profile_registry_leases (
                    profile_url_key TEXT PRIMARY KEY,
                    lease_owner TEXT NOT NULL,
                    lease_token TEXT NOT NULL,
                    lease_expires_at TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS linkedin_profile_registry_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_url_key TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_status TEXT,
                    detail TEXT,
                    run_id TEXT,
                    dataset_id TEXT,
                    metadata_json TEXT,
                    duration_ms INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS linkedin_profile_registry_backfill_runs (
                    run_key TEXT PRIMARY KEY,
                    scope_company TEXT,
                    scope_snapshot_id TEXT,
                    checkpoint_json TEXT NOT NULL DEFAULT '{}',
                    summary_json TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL DEFAULT 'running',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS organization_asset_registry (
                    registry_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_company TEXT NOT NULL,
                    company_key TEXT,
                    snapshot_id TEXT NOT NULL,
                    asset_view TEXT NOT NULL DEFAULT 'canonical_merged',
                    status TEXT NOT NULL DEFAULT 'ready',
                    authoritative INTEGER NOT NULL DEFAULT 0,
                    candidate_count INTEGER NOT NULL DEFAULT 0,
                    evidence_count INTEGER NOT NULL DEFAULT 0,
                    profile_detail_count INTEGER NOT NULL DEFAULT 0,
                    explicit_profile_capture_count INTEGER NOT NULL DEFAULT 0,
                    missing_linkedin_count INTEGER NOT NULL DEFAULT 0,
                    manual_review_backlog_count INTEGER NOT NULL DEFAULT 0,
                    profile_completion_backlog_count INTEGER NOT NULL DEFAULT 0,
                    source_snapshot_count INTEGER NOT NULL DEFAULT 0,
                    completeness_score REAL NOT NULL DEFAULT 0,
                    completeness_band TEXT NOT NULL DEFAULT 'low',
                    current_lane_coverage_json TEXT NOT NULL DEFAULT '{}',
                    former_lane_coverage_json TEXT NOT NULL DEFAULT '{}',
                    current_lane_effective_candidate_count INTEGER NOT NULL DEFAULT 0,
                    former_lane_effective_candidate_count INTEGER NOT NULL DEFAULT 0,
                    current_lane_effective_ready INTEGER NOT NULL DEFAULT 0,
                    former_lane_effective_ready INTEGER NOT NULL DEFAULT 0,
                    source_snapshot_selection_json TEXT NOT NULL DEFAULT '{}',
                    selected_snapshot_ids_json TEXT NOT NULL DEFAULT '[]',
                    source_path TEXT,
                    source_job_id TEXT,
                    summary_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(target_company, snapshot_id, asset_view)
                );

                CREATE TABLE IF NOT EXISTS acquisition_shard_registry (
                    shard_key TEXT PRIMARY KEY,
                    target_company TEXT NOT NULL,
                    company_key TEXT,
                    snapshot_id TEXT NOT NULL,
                    asset_view TEXT NOT NULL DEFAULT 'canonical_merged',
                    lane TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'completed',
                    employment_scope TEXT NOT NULL DEFAULT 'all',
                    strategy_type TEXT,
                    shard_id TEXT,
                    shard_title TEXT,
                    search_query TEXT,
                    query_signature TEXT,
                    company_scope_json TEXT NOT NULL DEFAULT '[]',
                    locations_json TEXT NOT NULL DEFAULT '[]',
                    function_ids_json TEXT NOT NULL DEFAULT '[]',
                    result_count INTEGER NOT NULL DEFAULT 0,
                    estimated_total_count INTEGER NOT NULL DEFAULT 0,
                    provider_cap_hit INTEGER NOT NULL DEFAULT 0,
                    source_path TEXT,
                    source_job_id TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    first_seen_at TEXT,
                    last_completed_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS organization_execution_profiles (
                    profile_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_company TEXT NOT NULL,
                    company_key TEXT,
                    asset_view TEXT NOT NULL DEFAULT 'canonical_merged',
                    source_registry_id INTEGER NOT NULL DEFAULT 0,
                    source_snapshot_id TEXT,
                    source_job_id TEXT,
                    source_generation_key TEXT,
                    source_generation_sequence INTEGER NOT NULL DEFAULT 0,
                    source_generation_watermark TEXT,
                    status TEXT NOT NULL DEFAULT 'ready',
                    org_scale_band TEXT NOT NULL DEFAULT 'unknown',
                    default_acquisition_mode TEXT NOT NULL DEFAULT 'full_company_roster',
                    prefer_delta_from_baseline INTEGER NOT NULL DEFAULT 0,
                    current_lane_default TEXT NOT NULL DEFAULT 'live_acquisition',
                    former_lane_default TEXT NOT NULL DEFAULT 'live_acquisition',
                    baseline_candidate_count INTEGER NOT NULL DEFAULT 0,
                    current_lane_effective_candidate_count INTEGER NOT NULL DEFAULT 0,
                    former_lane_effective_candidate_count INTEGER NOT NULL DEFAULT 0,
                    completeness_score REAL NOT NULL DEFAULT 0,
                    completeness_band TEXT NOT NULL DEFAULT 'low',
                    profile_detail_ratio REAL NOT NULL DEFAULT 0,
                    company_employee_shard_count INTEGER NOT NULL DEFAULT 0,
                    current_profile_search_shard_count INTEGER NOT NULL DEFAULT 0,
                    former_profile_search_shard_count INTEGER NOT NULL DEFAULT 0,
                    company_employee_cap_hit_count INTEGER NOT NULL DEFAULT 0,
                    profile_search_cap_hit_count INTEGER NOT NULL DEFAULT 0,
                    reason_codes_json TEXT NOT NULL DEFAULT '[]',
                    explanation_json TEXT NOT NULL DEFAULT '{}',
                    summary_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(target_company, asset_view)
                );

                CREATE TABLE IF NOT EXISTS asset_materialization_generations (
                    target_company TEXT NOT NULL,
                    company_key TEXT,
                    snapshot_id TEXT NOT NULL,
                    asset_view TEXT NOT NULL DEFAULT 'canonical_merged',
                    artifact_kind TEXT NOT NULL,
                    artifact_key TEXT NOT NULL DEFAULT '',
                    generation_key TEXT NOT NULL,
                    generation_sequence INTEGER NOT NULL DEFAULT 1,
                    source_path TEXT NOT NULL DEFAULT '',
                    payload_signature TEXT NOT NULL DEFAULT '',
                    member_signature TEXT NOT NULL DEFAULT '',
                    member_count INTEGER NOT NULL DEFAULT 0,
                    summary_json TEXT NOT NULL DEFAULT '{}',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (target_company, snapshot_id, asset_view, artifact_kind, artifact_key)
                );

                CREATE TABLE IF NOT EXISTS asset_membership_index (
                    generation_key TEXT NOT NULL,
                    target_company TEXT NOT NULL,
                    snapshot_id TEXT NOT NULL,
                    asset_view TEXT NOT NULL DEFAULT 'canonical_merged',
                    artifact_kind TEXT NOT NULL,
                    artifact_key TEXT NOT NULL DEFAULT '',
                    lane TEXT NOT NULL DEFAULT '',
                    employment_scope TEXT NOT NULL DEFAULT '',
                    member_key TEXT NOT NULL,
                    member_key_kind TEXT NOT NULL DEFAULT 'candidate_fallback',
                    candidate_id TEXT NOT NULL DEFAULT '',
                    profile_url_key TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (generation_key, member_key)
                );

                CREATE TABLE IF NOT EXISTS candidate_materialization_state (
                    target_company TEXT NOT NULL,
                    company_key TEXT,
                    snapshot_id TEXT NOT NULL,
                    asset_view TEXT NOT NULL DEFAULT 'canonical_merged',
                    candidate_id TEXT NOT NULL,
                    fingerprint TEXT NOT NULL DEFAULT '',
                    shard_path TEXT NOT NULL DEFAULT '',
                    list_page INTEGER NOT NULL DEFAULT 0,
                    dirty_reason TEXT NOT NULL DEFAULT '',
                    materialized_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (target_company, snapshot_id, asset_view, candidate_id)
                );

                CREATE TABLE IF NOT EXISTS snapshot_materialization_runs (
                    run_id TEXT PRIMARY KEY,
                    target_company TEXT NOT NULL,
                    company_key TEXT,
                    snapshot_id TEXT NOT NULL,
                    asset_view TEXT NOT NULL DEFAULT 'canonical_merged',
                    status TEXT NOT NULL DEFAULT 'running',
                    dirty_candidate_count INTEGER NOT NULL DEFAULT 0,
                    completed_candidate_count INTEGER NOT NULL DEFAULT 0,
                    reused_candidate_count INTEGER NOT NULL DEFAULT 0,
                    summary_json TEXT NOT NULL DEFAULT '{}',
                    started_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    completed_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS cloud_asset_operation_ledger (
                    ledger_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_type TEXT NOT NULL,
                    bundle_kind TEXT NOT NULL,
                    bundle_id TEXT NOT NULL,
                    sync_run_id TEXT,
                    status TEXT NOT NULL,
                    manifest_path TEXT,
                    target_runtime_dir TEXT,
                    target_db_path TEXT,
                    scoped_companies_json TEXT NOT NULL DEFAULT '[]',
                    scoped_snapshot_id TEXT,
                    summary_json TEXT NOT NULL DEFAULT '{}',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_candidates_target_company
                    ON candidates (target_company, category, employment_status);

                CREATE INDEX IF NOT EXISTS idx_evidence_candidate
                    ON evidence (candidate_id);

                CREATE INDEX IF NOT EXISTS idx_job_results_job
                    ON job_results (job_id, rank_index);

                CREATE INDEX IF NOT EXISTS idx_job_events_job
                    ON job_events (job_id, event_id);

                CREATE INDEX IF NOT EXISTS idx_job_progress_event_summaries_updated
                    ON job_progress_event_summaries (updated_at);

                CREATE INDEX IF NOT EXISTS idx_criteria_feedback_company
                    ON criteria_feedback (target_company, feedback_id);

                CREATE INDEX IF NOT EXISTS idx_criteria_patterns_company
                    ON criteria_patterns (target_company, pattern_type, status);

                CREATE INDEX IF NOT EXISTS idx_criteria_versions_company
                    ON criteria_versions (target_company, version_id);

                CREATE INDEX IF NOT EXISTS idx_criteria_compiler_runs_version
                    ON criteria_compiler_runs (version_id, compiler_run_id);

                CREATE INDEX IF NOT EXISTS idx_criteria_result_diffs_company
                    ON criteria_result_diffs (target_company, diff_id);

                CREATE INDEX IF NOT EXISTS idx_confidence_policy_runs_company
                    ON confidence_policy_runs (target_company, policy_run_id);

                CREATE INDEX IF NOT EXISTS idx_confidence_policy_controls_company
                    ON confidence_policy_controls (target_company, status, control_id);

                CREATE INDEX IF NOT EXISTS idx_plan_review_sessions_company
                    ON plan_review_sessions (target_company, status, review_id);

                CREATE INDEX IF NOT EXISTS idx_criteria_pattern_suggestions_company
                    ON criteria_pattern_suggestions (target_company, status, suggestion_id);

                CREATE INDEX IF NOT EXISTS idx_manual_review_items_company
                    ON manual_review_items (target_company, status, review_item_id);

                CREATE INDEX IF NOT EXISTS idx_manual_review_items_job
                    ON manual_review_items (job_id, status, updated_at);

                CREATE INDEX IF NOT EXISTS idx_candidate_review_registry_job
                    ON candidate_review_registry (job_id, status, updated_at);

                CREATE INDEX IF NOT EXISTS idx_candidate_review_registry_history
                    ON candidate_review_registry (history_id, updated_at);

                CREATE INDEX IF NOT EXISTS idx_candidate_review_registry_candidate
                    ON candidate_review_registry (candidate_id, updated_at);

                CREATE INDEX IF NOT EXISTS idx_target_candidates_updated
                    ON target_candidates (updated_at, follow_up_status);

                CREATE INDEX IF NOT EXISTS idx_target_candidates_history
                    ON target_candidates (history_id, updated_at);

                CREATE INDEX IF NOT EXISTS idx_target_candidates_candidate
                    ON target_candidates (candidate_id, updated_at);

                CREATE INDEX IF NOT EXISTS idx_asset_default_pointers_company
                    ON asset_default_pointers (company_key, scope_kind, scope_key, asset_kind);

                CREATE INDEX IF NOT EXISTS idx_asset_default_pointer_history_pointer
                    ON asset_default_pointer_history (pointer_key, occurred_at);

                CREATE INDEX IF NOT EXISTS idx_frontend_history_links_review
                    ON frontend_history_links (review_id, updated_at);

                CREATE INDEX IF NOT EXISTS idx_frontend_history_links_job
                    ON frontend_history_links (job_id, updated_at);

                CREATE INDEX IF NOT EXISTS idx_agent_runtime_sessions_job
                    ON agent_runtime_sessions (job_id, session_id);

                CREATE INDEX IF NOT EXISTS idx_agent_trace_spans_job
                    ON agent_trace_spans (job_id, session_id, span_id);

                CREATE INDEX IF NOT EXISTS idx_agent_worker_runs_job
                    ON agent_worker_runs (job_id, lane_id, worker_id);

                CREATE INDEX IF NOT EXISTS idx_workflow_job_leases_expires
                    ON workflow_job_leases (lease_expires_at);

                CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_status
                    ON linkedin_profile_registry (status, updated_at);

                CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_run
                    ON linkedin_profile_registry (last_run_id, last_dataset_id, updated_at);

                CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_alias_canonical
                    ON linkedin_profile_registry_aliases (profile_url_key, updated_at);

                CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_leases_expires
                    ON linkedin_profile_registry_leases (lease_expires_at);

                CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_event_type
                    ON linkedin_profile_registry_events (event_type, created_at);

                CREATE INDEX IF NOT EXISTS idx_linkedin_profile_registry_event_profile
                    ON linkedin_profile_registry_events (profile_url_key, created_at);

                CREATE INDEX IF NOT EXISTS idx_organization_asset_registry_company
                    ON organization_asset_registry (target_company, asset_view, authoritative, updated_at);

                CREATE INDEX IF NOT EXISTS idx_acquisition_shard_registry_company
                    ON acquisition_shard_registry (target_company, snapshot_id, lane, employment_scope, updated_at);

                CREATE UNIQUE INDEX IF NOT EXISTS idx_asset_materialization_generations_key
                    ON asset_materialization_generations (generation_key);

                CREATE INDEX IF NOT EXISTS idx_asset_materialization_generations_company
                    ON asset_materialization_generations (target_company, snapshot_id, asset_view, artifact_kind, updated_at);

                CREATE INDEX IF NOT EXISTS idx_asset_membership_index_lookup
                    ON asset_membership_index (target_company, snapshot_id, asset_view, artifact_kind, artifact_key, member_key);

                CREATE INDEX IF NOT EXISTS idx_asset_membership_index_generation
                    ON asset_membership_index (generation_key, employment_scope, lane);

                CREATE INDEX IF NOT EXISTS idx_candidate_materialization_state_snapshot
                    ON candidate_materialization_state (target_company, snapshot_id, asset_view, updated_at);

                CREATE INDEX IF NOT EXISTS idx_snapshot_materialization_runs_snapshot
                    ON snapshot_materialization_runs (target_company, snapshot_id, asset_view, created_at);

                CREATE INDEX IF NOT EXISTS idx_job_result_views_company
                    ON job_result_views (target_company, snapshot_id, asset_view, updated_at);

                CREATE INDEX IF NOT EXISTS idx_cloud_asset_operation_ledger_created
                    ON cloud_asset_operation_ledger (operation_type, created_at);

                CREATE INDEX IF NOT EXISTS idx_cloud_asset_operation_ledger_bundle
                    ON cloud_asset_operation_ledger (bundle_kind, bundle_id, created_at);
                """
            )
            self._ensure_column("jobs", "job_type", "TEXT NOT NULL DEFAULT 'retrieval'")
            self._ensure_column("jobs", "stage", "TEXT NOT NULL DEFAULT 'pending'")
            self._ensure_column("jobs", "plan_json", "TEXT")
            self._ensure_column("jobs", "execution_bundle_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("jobs", "matching_request_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("jobs", "request_signature", "TEXT")
            self._ensure_column("jobs", "request_family_signature", "TEXT")
            self._ensure_column("jobs", "matching_request_signature", "TEXT")
            self._ensure_column("jobs", "matching_request_family_signature", "TEXT")
            self._ensure_column("jobs", "requester_id", "TEXT")
            self._ensure_column("jobs", "tenant_id", "TEXT")
            self._ensure_column("jobs", "idempotency_key", "TEXT")
            self._ensure_column("job_results", "confidence_label", "TEXT")
            self._ensure_column("job_results", "confidence_score", "REAL")
            self._ensure_column("job_results", "confidence_reason", "TEXT")
            self._ensure_column("job_result_views", "company_key", "TEXT")
            self._ensure_column("criteria_versions", "parent_version_id", "INTEGER")
            self._ensure_column("criteria_versions", "trigger_feedback_id", "INTEGER")
            self._ensure_column("criteria_versions", "evolution_stage", "TEXT NOT NULL DEFAULT 'planned'")
            self._ensure_column("criteria_compiler_runs", "trigger_feedback_id", "INTEGER")
            self._ensure_column("confidence_policy_runs", "request_signature", "TEXT")
            self._ensure_column("confidence_policy_runs", "request_family_signature", "TEXT")
            self._ensure_column("confidence_policy_runs", "matching_request_signature", "TEXT")
            self._ensure_column("confidence_policy_runs", "matching_request_family_signature", "TEXT")
            self._ensure_column("confidence_policy_runs", "scope_kind", "TEXT NOT NULL DEFAULT 'company'")
            self._ensure_column("confidence_policy_controls", "request_signature", "TEXT")
            self._ensure_column("confidence_policy_controls", "request_family_signature", "TEXT")
            self._ensure_column("confidence_policy_controls", "matching_request_signature", "TEXT")
            self._ensure_column("confidence_policy_controls", "matching_request_family_signature", "TEXT")
            self._ensure_column("confidence_policy_controls", "scope_kind", "TEXT NOT NULL DEFAULT 'request_family'")
            self._ensure_column("confidence_policy_controls", "control_mode", "TEXT NOT NULL DEFAULT 'override'")
            self._ensure_column("confidence_policy_controls", "status", "TEXT NOT NULL DEFAULT 'active'")
            self._ensure_column("confidence_policy_controls", "reviewer", "TEXT")
            self._ensure_column("confidence_policy_controls", "notes", "TEXT")
            self._ensure_column("confidence_policy_controls", "locked_policy_json", "TEXT")
            self._ensure_column("plan_review_sessions", "request_signature", "TEXT")
            self._ensure_column("plan_review_sessions", "request_family_signature", "TEXT")
            self._ensure_column("plan_review_sessions", "matching_request_signature", "TEXT")
            self._ensure_column("plan_review_sessions", "matching_request_family_signature", "TEXT")
            self._ensure_column("plan_review_sessions", "status", "TEXT NOT NULL DEFAULT 'pending'")
            self._ensure_column("plan_review_sessions", "risk_level", "TEXT NOT NULL DEFAULT 'medium'")
            self._ensure_column("plan_review_sessions", "required_before_execution", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column("plan_review_sessions", "decision_json", "TEXT")
            self._ensure_column("plan_review_sessions", "execution_bundle_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("plan_review_sessions", "matching_request_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("plan_review_sessions", "reviewer", "TEXT")
            self._ensure_column("plan_review_sessions", "review_notes", "TEXT")
            self._ensure_column("plan_review_sessions", "approved_at", "TEXT")
            self._ensure_column("criteria_pattern_suggestions", "reviewed_by", "TEXT")
            self._ensure_column("criteria_pattern_suggestions", "review_notes", "TEXT")
            self._ensure_column("criteria_pattern_suggestions", "applied_pattern_id", "INTEGER")
            self._ensure_column("criteria_pattern_suggestions", "reviewed_at", "TEXT")
            self._ensure_column("criteria_pattern_suggestions", "matching_request_signature", "TEXT")
            self._ensure_column("criteria_pattern_suggestions", "matching_request_family_signature", "TEXT")
            self._ensure_column("manual_review_items", "priority", "TEXT NOT NULL DEFAULT 'medium'")
            self._ensure_column("manual_review_items", "status", "TEXT NOT NULL DEFAULT 'open'")
            self._ensure_column("manual_review_items", "metadata_json", "TEXT")
            self._ensure_column("manual_review_items", "reviewed_by", "TEXT")
            self._ensure_column("manual_review_items", "review_notes", "TEXT")
            self._ensure_column("manual_review_items", "reviewed_at", "TEXT")
            self._ensure_column("agent_runtime_sessions", "target_company", "TEXT")
            self._ensure_column("agent_runtime_sessions", "request_signature", "TEXT")
            self._ensure_column("agent_runtime_sessions", "request_family_signature", "TEXT")
            self._ensure_column("agent_runtime_sessions", "runtime_mode", "TEXT NOT NULL DEFAULT 'agent_runtime'")
            self._ensure_column("agent_runtime_sessions", "status", "TEXT NOT NULL DEFAULT 'running'")
            self._ensure_column("agent_runtime_sessions", "metadata_json", "TEXT")
            self._ensure_column("agent_trace_spans", "job_id", "TEXT")
            self._ensure_column("agent_trace_spans", "parent_span_id", "INTEGER")
            self._ensure_column("agent_trace_spans", "handoff_from_lane", "TEXT")
            self._ensure_column("agent_trace_spans", "handoff_to_lane", "TEXT")
            self._ensure_column("agent_trace_spans", "output_json", "TEXT")
            self._ensure_column("agent_trace_spans", "metadata_json", "TEXT")
            self._ensure_column("agent_trace_spans", "completed_at", "TEXT")
            self._ensure_column("agent_worker_runs", "session_id", "INTEGER")
            self._ensure_column("agent_worker_runs", "job_id", "TEXT")
            self._ensure_column("agent_worker_runs", "span_id", "INTEGER")
            self._ensure_column("agent_worker_runs", "lane_id", "TEXT")
            self._ensure_column("agent_worker_runs", "worker_key", "TEXT")
            self._ensure_column("agent_worker_runs", "status", "TEXT NOT NULL DEFAULT 'queued'")
            self._ensure_column("agent_worker_runs", "interrupt_requested", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("agent_worker_runs", "budget_json", "TEXT")
            self._ensure_column("agent_worker_runs", "checkpoint_json", "TEXT")
            self._ensure_column("agent_worker_runs", "input_json", "TEXT")
            self._ensure_column("agent_worker_runs", "output_json", "TEXT")
            self._ensure_column("agent_worker_runs", "metadata_json", "TEXT")
            self._ensure_column("agent_worker_runs", "lease_owner", "TEXT")
            self._ensure_column("agent_worker_runs", "lease_expires_at", "TEXT")
            self._ensure_column("agent_worker_runs", "attempt_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("agent_worker_runs", "last_error", "TEXT")
            self._ensure_column("query_dispatches", "request_signature", "TEXT")
            self._ensure_column("query_dispatches", "request_family_signature", "TEXT")
            self._ensure_column("query_dispatches", "matching_request_signature", "TEXT")
            self._ensure_column("query_dispatches", "matching_request_family_signature", "TEXT")
            self._ensure_column("query_dispatches", "requester_id", "TEXT")
            self._ensure_column("query_dispatches", "tenant_id", "TEXT")
            self._ensure_column("query_dispatches", "idempotency_key", "TEXT")
            self._ensure_column("query_dispatches", "matching_request_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("linkedin_profile_registry", "profile_url", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column("linkedin_profile_registry", "raw_linkedin_url", "TEXT")
            self._ensure_column("linkedin_profile_registry", "sanity_linkedin_url", "TEXT")
            self._ensure_column("linkedin_profile_registry", "status", "TEXT NOT NULL DEFAULT 'queued'")
            self._ensure_column("linkedin_profile_registry", "retry_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("linkedin_profile_registry", "last_error", "TEXT")
            self._ensure_column("linkedin_profile_registry", "last_run_id", "TEXT")
            self._ensure_column("linkedin_profile_registry", "last_dataset_id", "TEXT")
            self._ensure_column("linkedin_profile_registry", "last_snapshot_dir", "TEXT")
            self._ensure_column("linkedin_profile_registry", "last_raw_path", "TEXT")
            self._ensure_column("linkedin_profile_registry", "first_queued_at", "TEXT")
            self._ensure_column("linkedin_profile_registry", "last_queued_at", "TEXT")
            self._ensure_column("linkedin_profile_registry", "last_fetched_at", "TEXT")
            self._ensure_column("linkedin_profile_registry", "last_failed_at", "TEXT")
            self._ensure_column("linkedin_profile_registry", "source_shards_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("linkedin_profile_registry", "source_jobs_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("organization_asset_registry", "company_key", "TEXT")
            self._ensure_column("organization_asset_registry", "status", "TEXT NOT NULL DEFAULT 'ready'")
            self._ensure_column("organization_asset_registry", "authoritative", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "candidate_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "evidence_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "profile_detail_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(
                "organization_asset_registry", "explicit_profile_capture_count", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column("organization_asset_registry", "missing_linkedin_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(
                "organization_asset_registry", "manual_review_backlog_count", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column(
                "organization_asset_registry", "profile_completion_backlog_count", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column("organization_asset_registry", "source_snapshot_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "completeness_score", "REAL NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "completeness_band", "TEXT NOT NULL DEFAULT 'low'")
            self._ensure_column(
                "organization_asset_registry", "current_lane_coverage_json", "TEXT NOT NULL DEFAULT '{}'"
            )
            self._ensure_column(
                "organization_asset_registry", "former_lane_coverage_json", "TEXT NOT NULL DEFAULT '{}'"
            )
            self._ensure_column(
                "organization_asset_registry", "current_lane_effective_candidate_count", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column(
                "organization_asset_registry", "former_lane_effective_candidate_count", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column(
                "organization_asset_registry", "current_lane_effective_ready", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column(
                "organization_asset_registry", "former_lane_effective_ready", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column(
                "organization_asset_registry", "source_snapshot_selection_json", "TEXT NOT NULL DEFAULT '{}'"
            )
            self._ensure_column(
                "organization_asset_registry", "selected_snapshot_ids_json", "TEXT NOT NULL DEFAULT '[]'"
            )
            self._ensure_column("organization_asset_registry", "source_path", "TEXT")
            self._ensure_column("organization_asset_registry", "source_job_id", "TEXT")
            self._ensure_column("organization_asset_registry", "summary_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("organization_asset_registry", "materialization_generation_key", "TEXT")
            self._ensure_column(
                "organization_asset_registry",
                "materialization_generation_sequence",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column("organization_asset_registry", "materialization_watermark", "TEXT")
            self._ensure_column("acquisition_shard_registry", "company_key", "TEXT")
            self._ensure_column("acquisition_shard_registry", "asset_view", "TEXT NOT NULL DEFAULT 'canonical_merged'")
            self._ensure_column("acquisition_shard_registry", "status", "TEXT NOT NULL DEFAULT 'completed'")
            self._ensure_column("acquisition_shard_registry", "employment_scope", "TEXT NOT NULL DEFAULT 'all'")
            self._ensure_column("acquisition_shard_registry", "strategy_type", "TEXT")
            self._ensure_column("acquisition_shard_registry", "shard_id", "TEXT")
            self._ensure_column("acquisition_shard_registry", "shard_title", "TEXT")
            self._ensure_column("acquisition_shard_registry", "search_query", "TEXT")
            self._ensure_column("acquisition_shard_registry", "query_signature", "TEXT")
            self._ensure_column("acquisition_shard_registry", "company_scope_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("acquisition_shard_registry", "locations_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("acquisition_shard_registry", "function_ids_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("acquisition_shard_registry", "result_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("acquisition_shard_registry", "estimated_total_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("acquisition_shard_registry", "provider_cap_hit", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("acquisition_shard_registry", "source_path", "TEXT")
            self._ensure_column("acquisition_shard_registry", "source_job_id", "TEXT")
            self._ensure_column("acquisition_shard_registry", "metadata_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("acquisition_shard_registry", "first_seen_at", "TEXT")
            self._ensure_column("acquisition_shard_registry", "last_completed_at", "TEXT")
            self._ensure_column("acquisition_shard_registry", "materialization_generation_key", "TEXT")
            self._ensure_column(
                "acquisition_shard_registry",
                "materialization_generation_sequence",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column("acquisition_shard_registry", "materialization_watermark", "TEXT")
            self._ensure_column("organization_execution_profiles", "company_key", "TEXT")
            self._ensure_column(
                "organization_execution_profiles", "asset_view", "TEXT NOT NULL DEFAULT 'canonical_merged'"
            )
            self._ensure_column("organization_execution_profiles", "source_registry_id", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_execution_profiles", "source_snapshot_id", "TEXT")
            self._ensure_column("organization_execution_profiles", "source_job_id", "TEXT")
            self._ensure_column("organization_execution_profiles", "source_generation_key", "TEXT")
            self._ensure_column(
                "organization_execution_profiles",
                "source_generation_sequence",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column("organization_execution_profiles", "source_generation_watermark", "TEXT")
            self._ensure_column("organization_execution_profiles", "status", "TEXT NOT NULL DEFAULT 'ready'")
            self._ensure_column("organization_execution_profiles", "org_scale_band", "TEXT NOT NULL DEFAULT 'unknown'")
            self._ensure_column(
                "organization_execution_profiles",
                "default_acquisition_mode",
                "TEXT NOT NULL DEFAULT 'full_company_roster'",
            )
            self._ensure_column(
                "organization_execution_profiles", "prefer_delta_from_baseline", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column(
                "organization_execution_profiles", "current_lane_default", "TEXT NOT NULL DEFAULT 'live_acquisition'"
            )
            self._ensure_column(
                "organization_execution_profiles", "former_lane_default", "TEXT NOT NULL DEFAULT 'live_acquisition'"
            )
            self._ensure_column(
                "organization_execution_profiles", "baseline_candidate_count", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column(
                "organization_execution_profiles",
                "current_lane_effective_candidate_count",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(
                "organization_execution_profiles",
                "former_lane_effective_candidate_count",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column("organization_execution_profiles", "completeness_score", "REAL NOT NULL DEFAULT 0")
            self._ensure_column("organization_execution_profiles", "completeness_band", "TEXT NOT NULL DEFAULT 'low'")
            self._ensure_column("organization_execution_profiles", "profile_detail_ratio", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(
                "organization_execution_profiles", "company_employee_shard_count", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column(
                "organization_execution_profiles",
                "current_profile_search_shard_count",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(
                "organization_execution_profiles",
                "former_profile_search_shard_count",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(
                "organization_execution_profiles",
                "company_employee_cap_hit_count",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(
                "organization_execution_profiles",
                "profile_search_cap_hit_count",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column("organization_execution_profiles", "reason_codes_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("organization_execution_profiles", "explanation_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("organization_execution_profiles", "summary_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("asset_materialization_generations", "company_key", "TEXT")
            self._ensure_column("candidate_materialization_state", "company_key", "TEXT")
            self._ensure_column("snapshot_materialization_runs", "company_key", "TEXT")
            for table_name in (
                "job_result_views",
                "organization_asset_registry",
                "acquisition_shard_registry",
                "organization_execution_profiles",
                "asset_materialization_generations",
                "candidate_materialization_state",
                "snapshot_materialization_runs",
            ):
                self._backfill_company_key_column(table_name)
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_confidence_policy_runs_matching_family
                    ON confidence_policy_runs (target_company, matching_request_family_signature, policy_run_id)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_confidence_policy_controls_matching_family
                    ON confidence_policy_controls (target_company, status, matching_request_family_signature, control_id)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_criteria_pattern_suggestions_matching_family
                    ON criteria_pattern_suggestions (target_company, matching_request_family_signature, suggestion_id)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_jobs_request_signature_status
                ON jobs (request_signature, status, updated_at)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_jobs_request_family_signature_status
                ON jobs (request_family_signature, status, updated_at)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_jobs_matching_request_signature_status
                ON jobs (matching_request_signature, status, updated_at)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_jobs_matching_request_family_signature_status
                ON jobs (matching_request_family_signature, status, updated_at)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_jobs_scope_status
                ON jobs (tenant_id, requester_id, status, updated_at)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_jobs_idempotency_scope
                ON jobs (idempotency_key, tenant_id, requester_id, updated_at)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_query_dispatches_signature
                ON query_dispatches (request_signature, created_at)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_query_dispatches_matching_signature
                ON query_dispatches (matching_request_signature, created_at)
                """
            )
            self._connection.executescript(
                """
                CREATE INDEX IF NOT EXISTS idx_job_result_views_company_key
                    ON job_result_views (company_key, snapshot_id, asset_view, updated_at);
                CREATE INDEX IF NOT EXISTS idx_asset_materialization_generations_company_key
                    ON asset_materialization_generations (company_key, snapshot_id, asset_view, artifact_kind, updated_at);
                CREATE INDEX IF NOT EXISTS idx_candidate_materialization_state_company_key
                    ON candidate_materialization_state (company_key, snapshot_id, asset_view, updated_at);
                CREATE INDEX IF NOT EXISTS idx_snapshot_materialization_runs_company_key
                    ON snapshot_materialization_runs (company_key, snapshot_id, asset_view, created_at);
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_query_dispatches_scope
                ON query_dispatches (tenant_id, requester_id, created_at)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_query_dispatches_idempotency
                ON query_dispatches (idempotency_key, tenant_id, requester_id, created_at)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_organization_asset_registry_company
                ON organization_asset_registry (target_company, asset_view, authoritative, updated_at)
                """
            )
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_acquisition_shard_registry_company
                ON acquisition_shard_registry (target_company, snapshot_id, lane, employment_scope, updated_at)
                """
            )
        self.last_matching_refresh_summary = self.refresh_matching_metadata()

    def refresh_matching_metadata(self) -> dict[str, Any]:
        workflow_counts = self._backfill_workflow_matching_request_metadata()
        policy_counts = self._backfill_confidence_policy_matching_metadata()
        summary = {
            "status": "completed",
            "job_request_signature_count": self._backfill_job_request_signatures(),
            "job_count": int(workflow_counts.get("jobs") or 0),
            "plan_review_count": int(workflow_counts.get("plan_reviews") or 0),
            "dispatch_count": int(workflow_counts.get("dispatches") or 0),
            "feedback_count": self._backfill_feedback_matching_metadata(),
            "confidence_policy_run_count": int(policy_counts.get("runs") or 0),
            "confidence_policy_control_count": int(policy_counts.get("controls") or 0),
            "pattern_suggestion_count": self._backfill_pattern_suggestion_matching_metadata(),
        }
        self.last_matching_refresh_summary = summary
        return summary

    def _backfill_job_request_signatures(self) -> int:
        repaired = 0
        with self._lock, self._connection:
            rows = self._connection.execute(
                """
                SELECT job_id, request_json
                FROM jobs
                WHERE coalesce(request_signature, '') = '' OR coalesce(request_family_signature, '') = ''
                """
            ).fetchall()
            for row in rows:
                try:
                    request_payload = json.loads(row["request_json"] or "{}")
                except json.JSONDecodeError:
                    request_payload = {}
                signature_context = _request_signature_context(request_payload)
                self._connection.execute(
                    """
                    UPDATE jobs
                    SET request_signature = ?, request_family_signature = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = ?
                    """,
                    (
                        str(signature_context.get("request_signature") or ""),
                        str(signature_context.get("request_family_signature") or ""),
                        str(row["job_id"] or ""),
                    ),
                )
                repaired += 1
        return repaired

    def _backfill_workflow_matching_request_metadata(self) -> dict[str, int]:
        counts = {
            "jobs": 0,
            "plan_reviews": 0,
            "dispatches": 0,
        }
        with self._lock, self._connection:
            job_rows = self._connection.execute(
                """
                SELECT job_id, request_json, execution_bundle_json
                FROM jobs
                WHERE coalesce(matching_request_signature, '') = ''
                   OR coalesce(matching_request_family_signature, '') = ''
                   OR coalesce(matching_request_json, '') = ''
                   OR matching_request_json = '{}'
                """
            ).fetchall()
            for row in job_rows:
                try:
                    request_payload = json.loads(row["request_json"] or "{}")
                except json.JSONDecodeError:
                    request_payload = {}
                try:
                    execution_bundle_payload = json.loads(row["execution_bundle_json"] or "{}")
                except json.JSONDecodeError:
                    execution_bundle_payload = {}
                signature_context = _request_signature_context(
                    request_payload,
                    execution_bundle_payload=execution_bundle_payload,
                )
                self._connection.execute(
                    """
                    UPDATE jobs
                    SET matching_request_signature = ?,
                        matching_request_family_signature = ?,
                        matching_request_json = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = ?
                    """,
                    (
                        str(signature_context.get("matching_request_signature") or ""),
                        str(signature_context.get("matching_request_family_signature") or ""),
                        json.dumps(
                            _json_safe_payload(signature_context.get("request_matching") or {}), ensure_ascii=False
                        ),
                        str(row["job_id"] or ""),
                    ),
                )
                counts["jobs"] += 1

            review_rows = self._connection.execute(
                """
                SELECT review_id, request_json, execution_bundle_json
                FROM plan_review_sessions
                WHERE coalesce(matching_request_signature, '') = ''
                   OR coalesce(matching_request_family_signature, '') = ''
                   OR coalesce(matching_request_json, '') = ''
                   OR matching_request_json = '{}'
                """
            ).fetchall()
            for row in review_rows:
                try:
                    request_payload = json.loads(row["request_json"] or "{}")
                except json.JSONDecodeError:
                    request_payload = {}
                try:
                    execution_bundle_payload = json.loads(row["execution_bundle_json"] or "{}")
                except json.JSONDecodeError:
                    execution_bundle_payload = {}
                signature_context = _request_signature_context(
                    request_payload,
                    execution_bundle_payload=execution_bundle_payload,
                )
                self._connection.execute(
                    """
                    UPDATE plan_review_sessions
                    SET matching_request_signature = ?,
                        matching_request_family_signature = ?,
                        matching_request_json = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE review_id = ?
                    """,
                    (
                        str(signature_context.get("matching_request_signature") or ""),
                        str(signature_context.get("matching_request_family_signature") or ""),
                        json.dumps(
                            _json_safe_payload(signature_context.get("request_matching") or {}), ensure_ascii=False
                        ),
                        int(row["review_id"] or 0),
                    ),
                )
                counts["plan_reviews"] += 1

            dispatch_rows = self._connection.execute(
                """
                SELECT dispatch_id, payload_json
                FROM query_dispatches
                WHERE coalesce(matching_request_signature, '') = ''
                   OR coalesce(matching_request_family_signature, '') = ''
                   OR coalesce(matching_request_json, '') = ''
                   OR matching_request_json = '{}'
                """
            ).fetchall()
            for row in dispatch_rows:
                try:
                    dispatch_payload = json.loads(row["payload_json"] or "{}")
                except json.JSONDecodeError:
                    dispatch_payload = {}
                request_payload = dict(dispatch_payload.get("request") or {})
                if not request_payload:
                    continue
                signature_context = _request_signature_context(request_payload)
                self._connection.execute(
                    """
                    UPDATE query_dispatches
                    SET matching_request_signature = ?,
                        matching_request_family_signature = ?,
                        matching_request_json = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE dispatch_id = ?
                    """,
                    (
                        str(signature_context.get("matching_request_signature") or ""),
                        str(signature_context.get("matching_request_family_signature") or ""),
                        json.dumps(
                            _json_safe_payload(signature_context.get("request_matching") or {}), ensure_ascii=False
                        ),
                        int(row["dispatch_id"] or 0),
                    ),
                )
                counts["dispatches"] += 1
        return counts

    def _backfill_feedback_matching_metadata(self) -> int:
        repaired = 0
        with self._lock, self._connection:
            rows = self._connection.execute(
                """
                SELECT feedback_id, job_id, payload_json
                FROM criteria_feedback
                """
            ).fetchall()
            for row in rows:
                try:
                    metadata = json.loads(row["payload_json"] or "{}")
                except json.JSONDecodeError:
                    metadata = {}
                request_payload = (
                    metadata.get("request_payload") if isinstance(metadata.get("request_payload"), dict) else {}
                )
                if not request_payload:
                    job_id = str(row["job_id"] or "").strip()
                    if job_id:
                        job_row = self._connection.execute(
                            "SELECT request_json FROM jobs WHERE job_id = ? LIMIT 1",
                            (job_id,),
                        ).fetchone()
                        if job_row is not None:
                            try:
                                request_payload = json.loads(job_row["request_json"] or "{}")
                            except json.JSONDecodeError:
                                request_payload = {}
                if not request_payload:
                    continue
                existing_matching = (
                    metadata.get("request_matching") if isinstance(metadata.get("request_matching"), dict) else {}
                )
                signature_context = _request_signature_context(
                    request_payload,
                    execution_bundle_payload={"request_matching": existing_matching} if existing_matching else None,
                )
                new_metadata = dict(metadata)
                new_metadata["request_payload"] = request_payload
                new_metadata["request_matching"] = _json_safe_payload(signature_context.get("request_matching") or {})
                new_metadata["request_signature"] = str(signature_context.get("request_signature") or "")
                new_metadata["request_family_signature"] = str(signature_context.get("request_family_signature") or "")
                new_metadata["matching_request_signature"] = str(
                    signature_context.get("matching_request_signature") or ""
                )
                new_metadata["matching_request_family_signature"] = str(
                    signature_context.get("matching_request_family_signature") or ""
                )
                if new_metadata == metadata:
                    continue
                self._connection.execute(
                    """
                    UPDATE criteria_feedback
                    SET payload_json = ?
                    WHERE feedback_id = ?
                    """,
                    (
                        json.dumps(_json_safe_payload(new_metadata), ensure_ascii=False),
                        int(row["feedback_id"] or 0),
                    ),
                )
                repaired += 1
        return repaired

    def _backfill_confidence_policy_matching_metadata(self) -> dict[str, int]:
        counts = {
            "runs": 0,
            "controls": 0,
        }
        with self._lock, self._connection:
            run_rows = self._connection.execute(
                """
                SELECT policy_run_id, job_id, trigger_feedback_id, request_signature, request_family_signature, policy_json
                FROM confidence_policy_runs
                WHERE coalesce(matching_request_signature, '') = ''
                   OR coalesce(matching_request_family_signature, '') = ''
                """
            ).fetchall()
            for row in run_rows:
                request_payload: dict[str, Any] = {}
                job_id = str(row["job_id"] or "").strip()
                if job_id:
                    job_row = self._connection.execute(
                        "SELECT request_json FROM jobs WHERE job_id = ? LIMIT 1",
                        (job_id,),
                    ).fetchone()
                    if job_row is not None:
                        try:
                            request_payload = json.loads(job_row["request_json"] or "{}")
                        except json.JSONDecodeError:
                            request_payload = {}
                if not request_payload and int(row["trigger_feedback_id"] or 0):
                    feedback_row = self._connection.execute(
                        "SELECT payload_json FROM criteria_feedback WHERE feedback_id = ? LIMIT 1",
                        (int(row["trigger_feedback_id"] or 0),),
                    ).fetchone()
                    if feedback_row is not None:
                        try:
                            feedback_metadata = json.loads(feedback_row["payload_json"] or "{}")
                        except json.JSONDecodeError:
                            feedback_metadata = {}
                        if isinstance(feedback_metadata.get("request_payload"), dict):
                            request_payload = dict(feedback_metadata.get("request_payload") or {})
                try:
                    policy_payload = json.loads(row["policy_json"] or "{}")
                except json.JSONDecodeError:
                    policy_payload = {}
                if request_payload:
                    signature_context = _request_signature_context(
                        request_payload,
                        execution_bundle_payload={"request_matching": policy_payload.get("request_matching") or {}}
                        if isinstance(policy_payload.get("request_matching"), dict)
                        else None,
                    )
                    matching_request_sig = str(signature_context.get("matching_request_signature") or "")
                    matching_request_family_sig = str(signature_context.get("matching_request_family_signature") or "")
                else:
                    matching_request_sig = str(row["request_signature"] or "").strip()
                    matching_request_family_sig = str(row["request_family_signature"] or "").strip()
                self._connection.execute(
                    """
                    UPDATE confidence_policy_runs
                    SET matching_request_signature = ?,
                        matching_request_family_signature = ?
                    WHERE policy_run_id = ?
                    """,
                    (
                        matching_request_sig,
                        matching_request_family_sig,
                        int(row["policy_run_id"] or 0),
                    ),
                )
                counts["runs"] += 1

            control_rows = self._connection.execute(
                """
                SELECT control_id, request_signature, request_family_signature, locked_policy_json
                FROM confidence_policy_controls
                WHERE coalesce(matching_request_signature, '') = ''
                   OR coalesce(matching_request_family_signature, '') = ''
                """
            ).fetchall()
            for row in control_rows:
                try:
                    locked_policy = json.loads(row["locked_policy_json"] or "{}")
                except json.JSONDecodeError:
                    locked_policy = {}
                matching_request_sig = str(
                    locked_policy.get("matching_request_signature") or row["request_signature"] or ""
                ).strip()
                matching_request_family_sig = str(
                    locked_policy.get("matching_request_family_signature") or row["request_family_signature"] or ""
                ).strip()
                self._connection.execute(
                    """
                    UPDATE confidence_policy_controls
                    SET matching_request_signature = ?,
                        matching_request_family_signature = ?
                    WHERE control_id = ?
                    """,
                    (
                        matching_request_sig,
                        matching_request_family_sig,
                        int(row["control_id"] or 0),
                    ),
                )
                counts["controls"] += 1
        return counts

    def _backfill_pattern_suggestion_matching_metadata(self) -> int:
        repaired = 0
        with self._lock, self._connection:
            rows = self._connection.execute(
                """
                SELECT suggestion_id, source_feedback_id, request_signature, request_family_signature, metadata_json
                FROM criteria_pattern_suggestions
                WHERE coalesce(matching_request_signature, '') = ''
                   OR coalesce(matching_request_family_signature, '') = ''
                """
            ).fetchall()
            for row in rows:
                try:
                    metadata = json.loads(row["metadata_json"] or "{}")
                except json.JSONDecodeError:
                    metadata = {}
                request_payload = (
                    metadata.get("request_payload") if isinstance(metadata.get("request_payload"), dict) else {}
                )
                if not request_payload and int(row["source_feedback_id"] or 0):
                    feedback_row = self._connection.execute(
                        "SELECT payload_json FROM criteria_feedback WHERE feedback_id = ? LIMIT 1",
                        (int(row["source_feedback_id"] or 0),),
                    ).fetchone()
                    if feedback_row is not None:
                        try:
                            feedback_metadata = json.loads(feedback_row["payload_json"] or "{}")
                        except json.JSONDecodeError:
                            feedback_metadata = {}
                        if isinstance(feedback_metadata.get("request_payload"), dict):
                            request_payload = dict(feedback_metadata.get("request_payload") or {})
                if request_payload:
                    signature_context = _request_signature_context(request_payload)
                    matching_request_sig = str(signature_context.get("matching_request_signature") or "")
                    matching_request_family_sig = str(signature_context.get("matching_request_family_signature") or "")
                else:
                    matching_request_sig = str(row["request_signature"] or "").strip()
                    matching_request_family_sig = str(row["request_family_signature"] or "").strip()
                self._connection.execute(
                    """
                    UPDATE criteria_pattern_suggestions
                    SET matching_request_signature = ?,
                        matching_request_family_signature = ?
                    WHERE suggestion_id = ?
                    """,
                    (
                        matching_request_sig,
                        matching_request_family_sig,
                        int(row["suggestion_id"] or 0),
                    ),
                )
                repaired += 1
        return repaired

    def replace_bootstrap_data(self, candidates: list[Candidate], evidence: list[EvidenceRecord]) -> None:
        if self._replace_candidates_and_evidence_in_postgres(
            current_candidate_rows=self._select_postgres_candidate_rows(limit=0),
            candidates=candidates,
            evidence=evidence,
        ):
            self._bootstrap_candidate_store_loaded = True
            return
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM evidence")
            self._connection.execute("DELETE FROM candidates")
            self._insert_candidates_and_evidence(candidates, evidence)
        self._replace_control_plane_table_from_sqlite("candidates")
        self._replace_control_plane_table_from_sqlite("evidence")
        self._bootstrap_candidate_store_loaded = True

    def replace_company_data(
        self, target_company: str, candidates: list[Candidate], evidence: list[EvidenceRecord]
    ) -> None:
        company_key = target_company.strip().lower()
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
        with self._lock, self._connection:
            rows = self._connection.execute(
                "SELECT candidate_id FROM candidates WHERE lower(target_company) = ?",
                (company_key,),
            ).fetchall()
            candidate_ids = [row["candidate_id"] for row in rows]
            if candidate_ids:
                placeholders = ",".join("?" for _ in candidate_ids)
                self._connection.execute(
                    f"DELETE FROM evidence WHERE candidate_id IN ({placeholders})",
                    candidate_ids,
                )
            self._connection.execute(
                "DELETE FROM candidates WHERE lower(target_company) = ?",
                (company_key,),
            )
            self._insert_candidates_and_evidence(candidates, evidence)
        self._replace_control_plane_table_from_sqlite("candidates")
        self._replace_control_plane_table_from_sqlite("evidence")

    def replace_company_category_data(
        self,
        target_company: str,
        category: str,
        candidates: list[Candidate],
        evidence: list[EvidenceRecord],
    ) -> None:
        company_key = target_company.strip().lower()
        category_key = category.strip().lower()
        if self._replace_candidates_and_evidence_in_postgres(
            current_candidate_rows=self._select_postgres_candidate_rows(
                where_sql="lower(target_company) = lower(%s) AND lower(category) = lower(%s)",
                params=[target_company, category],
                limit=0,
            ),
            candidates=candidates,
            evidence=evidence,
        ):
            return
        with self._lock, self._connection:
            rows = self._connection.execute(
                """
                SELECT candidate_id FROM candidates
                WHERE lower(target_company) = ? AND lower(category) = ?
                """,
                (company_key, category_key),
            ).fetchall()
            candidate_ids = [row["candidate_id"] for row in rows]
            incoming_candidate_ids = _dedupe_preserve_order(
                [str(candidate.candidate_id or "").strip() for candidate in list(candidates or [])]
            )
            incoming_conflict_ids = [candidate_id for candidate_id in incoming_candidate_ids if candidate_id not in candidate_ids]
            if candidate_ids:
                placeholders = ",".join("?" for _ in candidate_ids)
                self._connection.execute(
                    f"DELETE FROM evidence WHERE candidate_id IN ({placeholders})",
                    candidate_ids,
                )
            if incoming_conflict_ids:
                placeholders = ",".join("?" for _ in incoming_conflict_ids)
                self._connection.execute(
                    f"DELETE FROM evidence WHERE candidate_id IN ({placeholders})",
                    incoming_conflict_ids,
                )
                self._connection.execute(
                    f"DELETE FROM candidates WHERE candidate_id IN ({placeholders})",
                    incoming_conflict_ids,
                )
            self._connection.execute(
                "DELETE FROM candidates WHERE lower(target_company) = ? AND lower(category) = ?",
                (company_key, category_key),
            )
            self._insert_candidates_and_evidence(candidates, evidence)
        self._replace_control_plane_table_from_sqlite("candidates")
        self._replace_control_plane_table_from_sqlite("evidence")

    def _replace_candidates_and_evidence_in_postgres(
        self,
        *,
        current_candidate_rows: list[dict[str, Any]],
        candidates: list[Candidate],
        evidence: list[EvidenceRecord],
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
        candidate_payloads = self._dedupe_candidate_payloads(candidates)
        evidence_payloads = self._dedupe_evidence_payloads(evidence)
        try:
            if current_candidate_ids:
                placeholders = ", ".join("%s" for _ in current_candidate_ids)
                self._call_control_plane_postgres_native(
                    "delete_rows",
                    table_name="evidence",
                    where_sql=f"candidate_id IN ({placeholders})",
                    params=current_candidate_ids,
                )
                self._call_control_plane_postgres_native(
                    "delete_rows",
                    table_name="candidates",
                    where_sql=f"candidate_id IN ({placeholders})",
                    params=current_candidate_ids,
                )
            for payload in candidate_payloads:
                self._write_control_plane_row_to_postgres("candidates", payload)
            for payload in evidence_payloads:
                self._write_control_plane_row_to_postgres("evidence", payload)
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
        row = self._connection.execute("SELECT COUNT(*) AS count FROM candidates").fetchone()
        return int(row["count"])

    def candidate_count_for_company(self, target_company: str) -> int:
        postgres_rows = self._select_postgres_candidate_rows(
            where_sql="lower(target_company) = lower(%s)",
            params=[target_company],
            limit=0,
        )
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("candidates"):
            return len(postgres_rows)
        row = self._connection.execute(
            "SELECT COUNT(*) AS count FROM candidates WHERE lower(target_company) = lower(?)",
            (target_company,),
        ).fetchone()
        return int(row["count"])

    def list_candidates(self) -> list[Candidate]:
        postgres_rows = self._select_postgres_candidate_rows(limit=0)
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("candidates"):
            return sorted(
                [self._candidate_from_row(row) for row in postgres_rows],
                key=lambda candidate: str(candidate.name_en or "").lower(),
            )
        rows = self._connection.execute("SELECT * FROM candidates ORDER BY name_en").fetchall()
        return [self._candidate_from_row(row) for row in rows]

    def list_candidates_for_company(self, target_company: str) -> list[Candidate]:
        postgres_rows = self._select_postgres_candidate_rows(
            where_sql="lower(target_company) = lower(%s)",
            params=[target_company],
            limit=0,
        )
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("candidates"):
            return sorted(
                [self._candidate_from_row(row) for row in postgres_rows],
                key=lambda candidate: str(candidate.name_en or "").lower(),
            )
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM candidates WHERE lower(target_company) = lower(?) ORDER BY name_en",
                (target_company,),
            ).fetchall()
        return [self._candidate_from_row(row) for row in rows]

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("candidates"):
            return None
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM candidates WHERE candidate_id = ? LIMIT 1",
                (normalized_candidate_id,),
            ).fetchone()
        if row is None:
            return None
        return self._candidate_from_row(row)

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
        row = self._connection.execute(
            """
            SELECT * FROM candidates
            WHERE lower(target_company) = lower(?) AND lower(name_en) = lower(?)
            ORDER BY candidate_id
            LIMIT 1
            """,
            (normalized_company, normalized_name),
        ).fetchone()
        if row is None:
            return None
        return self._candidate_from_row(row)

    def upsert_candidate(self, candidate: Candidate) -> Candidate:
        payload = self._candidate_payload(candidate)
        if self._write_control_plane_row_to_postgres("candidates", payload):
            return self.get_candidate(candidate.candidate_id) or candidate
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO candidates (
                    candidate_id, name_en, name_zh, display_name, category, target_company,
                    organization, employment_status, role, team, joined_at, left_at,
                    current_destination, ethnicity_background, investment_involvement,
                    focus_areas, education, work_history, notes, linkedin_url, media_url,
                    source_dataset, source_path, metadata_json
                ) VALUES (
                    :candidate_id, :name_en, :name_zh, :display_name, :category, :target_company,
                    :organization, :employment_status, :role, :team, :joined_at, :left_at,
                    :current_destination, :ethnicity_background, :investment_involvement,
                    :focus_areas, :education, :work_history, :notes, :linkedin_url, :media_url,
                    :source_dataset, :source_path, :metadata_json
                )
                ON CONFLICT(candidate_id) DO UPDATE SET
                    name_en = excluded.name_en,
                    name_zh = excluded.name_zh,
                    display_name = excluded.display_name,
                    category = excluded.category,
                    target_company = excluded.target_company,
                    organization = excluded.organization,
                    employment_status = excluded.employment_status,
                    role = excluded.role,
                    team = excluded.team,
                    joined_at = excluded.joined_at,
                    left_at = excluded.left_at,
                    current_destination = excluded.current_destination,
                    ethnicity_background = excluded.ethnicity_background,
                    investment_involvement = excluded.investment_involvement,
                    focus_areas = excluded.focus_areas,
                    education = excluded.education,
                    work_history = excluded.work_history,
                    notes = excluded.notes,
                    linkedin_url = excluded.linkedin_url,
                    media_url = excluded.media_url,
                    source_dataset = excluded.source_dataset,
                    source_path = excluded.source_path,
                    metadata_json = excluded.metadata_json
                """,
                payload,
            )
            row = self._connection.execute(
                "SELECT * FROM candidates WHERE candidate_id = ? LIMIT 1",
                (candidate.candidate_id,),
            ).fetchone()
        self._mirror_control_plane_row("candidates", row)
        return self.get_candidate(candidate.candidate_id) or candidate

    def list_evidence(self, candidate_id: str) -> list[dict[str, Any]]:
        normalized_candidate_id = str(candidate_id or "").strip()
        postgres_rows = self._select_postgres_evidence_rows(
            where_sql="candidate_id = %s",
            params=[normalized_candidate_id],
            limit=0,
        )
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("evidence"):
            return sorted(
                [self._evidence_payload_from_row(row, include_candidate_id=False) for row in postgres_rows],
                key=lambda row: (str(row.get("title") or ""), str(row.get("evidence_id") or "")),
            )
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM evidence WHERE candidate_id = ? ORDER BY title", (normalized_candidate_id,)
            ).fetchall()
        return [self._evidence_payload_from_row(row, include_candidate_id=False) for row in rows]

    def list_evidence_for_company(self, target_company: str) -> list[dict[str, Any]]:
        company_candidates = self.list_candidates_for_company(target_company)
        candidate_ids = [candidate.candidate_id for candidate in company_candidates if str(candidate.candidate_id or "").strip()]
        postgres_rows: list[dict[str, Any]] = []
        if candidate_ids:
            placeholders = ", ".join("%s" for _ in candidate_ids)
            postgres_rows = self._select_postgres_evidence_rows(
                where_sql=f"candidate_id IN ({placeholders})",
                params=candidate_ids,
                limit=0,
            )
        if postgres_rows or (
            not candidate_ids and self._control_plane_postgres_should_skip_sqlite_fallback("evidence")
        ) or self._control_plane_postgres_should_skip_sqlite_fallback("candidates"):
            return sorted(
                [self._evidence_payload_from_row(row, include_candidate_id=True) for row in postgres_rows],
                key=lambda row: (str(row.get("candidate_id") or ""), str(row.get("title") or "")),
            )
        rows = self._connection.execute(
            """
            SELECT e.*
            FROM evidence e
            JOIN candidates c ON c.candidate_id = e.candidate_id
            WHERE lower(c.target_company) = lower(?)
            ORDER BY e.candidate_id, e.title
            """,
            (target_company,),
        ).fetchall()
        return [self._evidence_payload_from_row(row, include_candidate_id=True) for row in rows]

    def upsert_evidence_records(self, evidence: list[EvidenceRecord]) -> list[dict[str, Any]]:
        if not evidence:
            return []
        evidence_payloads = [self._evidence_payload(item) for item in evidence]
        if all(self._write_control_plane_row_to_postgres("evidence", payload) for payload in evidence_payloads):
            return self.list_evidence(evidence[0].candidate_id)
        with self._lock, self._connection:
            self._connection.executemany(
                """
                INSERT INTO evidence (
                    evidence_id, candidate_id, source_type, title, url, summary,
                    source_dataset, source_path, metadata_json
                ) VALUES (
                    :evidence_id, :candidate_id, :source_type, :title, :url, :summary,
                    :source_dataset, :source_path, :metadata_json
                )
                ON CONFLICT(evidence_id) DO UPDATE SET
                    candidate_id = excluded.candidate_id,
                    source_type = excluded.source_type,
                    title = excluded.title,
                    url = excluded.url,
                    summary = excluded.summary,
                    source_dataset = excluded.source_dataset,
                    source_path = excluded.source_path,
                    metadata_json = excluded.metadata_json
                """,
                evidence_payloads,
            )
            rows = self._connection.execute(
                "SELECT * FROM evidence WHERE candidate_id = ? ORDER BY title",
                (evidence[0].candidate_id,),
            ).fetchall()
        for row in rows:
            self._mirror_control_plane_row("evidence", row)
        return self.list_evidence(evidence[0].candidate_id)

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
        summary_json = json.dumps(_json_safe_payload(summary_payload or {}), ensure_ascii=False)
        request_json = json.dumps(_json_safe_payload(request_payload), ensure_ascii=False)
        plan_json = json.dumps(_json_safe_payload(plan_payload or {}), ensure_ascii=False)
        execution_bundle_value = dict(execution_bundle_payload or {})
        execution_bundle_json = json.dumps(_json_safe_payload(execution_bundle_value), ensure_ascii=False)
        matching_bundle = _matching_bundle_payload(
            request_payload,
            execution_bundle_payload=execution_bundle_value,
        )
        matching_request_json = json.dumps(_json_safe_payload(matching_bundle), ensure_ascii=False)
        request_sig = request_signature(request_payload)
        request_family_sig = request_family_signature(request_payload)
        matching_request_sig = str(matching_bundle.get("matching_request_signature") or "")
        matching_request_family_sig = str(matching_bundle.get("matching_request_family_signature") or "")
        requester_id_value = str(requester_id or "").strip()
        tenant_id_value = str(tenant_id or "").strip()
        idempotency_key_value = str(idempotency_key or "").strip()
        if self._control_plane_postgres_should_prefer_read("jobs"):
            row = self._call_control_plane_postgres_native(
                "save_job_row",
                row={
                    "job_id": job_id,
                    "job_type": job_type,
                    "status": status,
                    "stage": stage,
                    "request_json": request_json,
                    "plan_json": plan_json,
                    "execution_bundle_json": execution_bundle_json,
                    "matching_request_json": matching_request_json,
                    "summary_json": summary_json,
                    "artifact_path": artifact_path,
                    "request_signature": request_sig,
                    "request_family_signature": request_family_sig,
                    "matching_request_signature": matching_request_sig,
                    "matching_request_family_signature": matching_request_family_sig,
                    "requester_id": requester_id_value,
                    "tenant_id": tenant_id_value,
                    "idempotency_key": idempotency_key_value,
                },
                protect_terminal_statuses=True,
                terminal_statuses=sorted(_TERMINAL_JOB_STATUSES),
            )
            if row is not None:
                return
        row: sqlite3.Row | None = None
        with self._lock, self._connection:
            existing_job = self._connection.execute(
                "SELECT status FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            existing_status = str(existing_job[0] or "").strip().lower() if existing_job else ""
            incoming_status = str(status or "").strip().lower()
            if existing_status in _TERMINAL_JOB_STATUSES and incoming_status not in _TERMINAL_JOB_STATUSES:
                return
            self._connection.execute(
                """
                INSERT INTO jobs (
                    job_id, job_type, status, stage, request_json, plan_json, execution_bundle_json, matching_request_json,
                    summary_json, artifact_path, request_signature, request_family_signature,
                    matching_request_signature, matching_request_family_signature, requester_id, tenant_id, idempotency_key
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    job_type = excluded.job_type,
                    status = excluded.status,
                    stage = excluded.stage,
                    request_json = excluded.request_json,
                    plan_json = excluded.plan_json,
                    execution_bundle_json = CASE
                        WHEN excluded.execution_bundle_json <> '{}' THEN excluded.execution_bundle_json
                        ELSE jobs.execution_bundle_json
                    END,
                    matching_request_json = CASE
                        WHEN excluded.matching_request_json <> '{}' THEN excluded.matching_request_json
                        ELSE jobs.matching_request_json
                    END,
                    summary_json = excluded.summary_json,
                    artifact_path = excluded.artifact_path,
                    request_signature = excluded.request_signature,
                    request_family_signature = excluded.request_family_signature,
                    matching_request_signature = excluded.matching_request_signature,
                    matching_request_family_signature = excluded.matching_request_family_signature,
                    requester_id = CASE
                        WHEN excluded.requester_id <> '' THEN excluded.requester_id
                        ELSE jobs.requester_id
                    END,
                    tenant_id = CASE
                        WHEN excluded.tenant_id <> '' THEN excluded.tenant_id
                        ELSE jobs.tenant_id
                    END,
                    idempotency_key = CASE
                        WHEN excluded.idempotency_key <> '' THEN excluded.idempotency_key
                        ELSE jobs.idempotency_key
                    END,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    job_id,
                    job_type,
                    status,
                    stage,
                    request_json,
                    plan_json,
                    execution_bundle_json,
                    matching_request_json,
                    summary_json,
                    artifact_path,
                    request_sig,
                    request_family_sig,
                    matching_request_sig,
                    matching_request_family_sig,
                    requester_id_value,
                    tenant_id_value,
                    idempotency_key_value,
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM jobs WHERE job_id = ? LIMIT 1",
                (job_id,),
            ).fetchone()
        self._mirror_control_plane_row("jobs", row)

    def append_job_event(
        self,
        job_id: str,
        stage: str,
        status: str,
        detail: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        payload_dict = _json_safe_payload(dict(payload or {}))
        payload_json = json.dumps(payload_dict, ensure_ascii=False)
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
        event_row: sqlite3.Row | None = None
        summary_row: sqlite3.Row | None = None
        compacted_job_events = False
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO job_events (job_id, stage, status, detail, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, stage, status, detail, payload_json),
            )
            event_id = int(cursor.lastrowid or 0)
            event_row = self._connection.execute(
                """
                SELECT *
                FROM job_events
                WHERE event_id = ?
                LIMIT 1
                """,
                (event_id,),
            ).fetchone()
            self._update_job_progress_event_summary_locked(
                job_id=job_id,
                event={
                    "event_id": event_id,
                    "stage": stage,
                    "status": status,
                    "detail": detail,
                    "payload": payload_dict,
                    "created_at": str(event_row["created_at"] or "") if event_row is not None else "",
                },
            )
            if stage in {"runtime_heartbeat", "runtime_control"}:
                compacted_job_events = self._compact_runtime_job_events_locked(
                    job_id=job_id,
                    stage=stage,
                    payload=payload_dict,
                )
            summary_row = self._connection.execute(
                """
                SELECT *
                FROM job_progress_event_summaries
                WHERE job_id = ?
                LIMIT 1
                """,
                (job_id,),
            ).fetchone()
        self._mirror_control_plane_row("job_events", event_row)
        self._mirror_control_plane_row("job_progress_event_summaries", summary_row)
        if compacted_job_events:
            self._replace_control_plane_table_from_sqlite("job_events")

    def _compact_runtime_job_events_locked(self, *, job_id: str, stage: str, payload: dict[str, Any]) -> bool:
        normalized_job_id = str(job_id or "").strip()
        normalized_stage = str(stage or "").strip()
        if not normalized_job_id or normalized_stage not in {"runtime_heartbeat", "runtime_control"}:
            return False
        grouping_key_name = "source" if normalized_stage == "runtime_heartbeat" else "control"
        keep_latest = 12 if normalized_stage == "runtime_heartbeat" else 8
        grouping_value = str(payload.get(grouping_key_name) or "").strip()
        rows = self._connection.execute(
            """
            SELECT event_id, payload_json
            FROM job_events
            WHERE job_id = ? AND stage = ?
            ORDER BY event_id DESC
            """,
            (normalized_job_id, normalized_stage),
        ).fetchall()
        matched_ids: list[int] = []
        for row in rows:
            try:
                row_payload = dict(json.loads(row["payload_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                row_payload = {}
            row_grouping_value = str(row_payload.get(grouping_key_name) or "").strip()
            if row_grouping_value != grouping_value:
                continue
            matched_ids.append(int(row["event_id"] or 0))
        if len(matched_ids) <= keep_latest:
            return False
        delete_ids = matched_ids[keep_latest:]
        self._connection.executemany(
            "DELETE FROM job_events WHERE event_id = ?",
            [(event_id,) for event_id in delete_ids if event_id > 0],
        )
        self._hydrate_job_progress_event_summary_locked(normalized_job_id)
        return True

    def _upsert_job_progress_event_summary_locked(
        self,
        *,
        job_id: str,
        summary: dict[str, Any],
    ) -> sqlite3.Row | None:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return None
        self._connection.execute(
            """
            INSERT INTO job_progress_event_summaries (
                job_id, event_count, latest_event_json, stage_sequence_json, stage_stats_json, latest_metrics_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                event_count = excluded.event_count,
                latest_event_json = excluded.latest_event_json,
                stage_sequence_json = excluded.stage_sequence_json,
                stage_stats_json = excluded.stage_stats_json,
                latest_metrics_json = excluded.latest_metrics_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                normalized_job_id,
                int(summary.get("event_count") or 0),
                json.dumps(_json_safe_payload(summary.get("latest_event") or {}), ensure_ascii=False),
                json.dumps(_json_safe_payload(summary.get("stage_sequence") or []), ensure_ascii=False),
                json.dumps(_json_safe_payload(summary.get("stage_stats") or {}), ensure_ascii=False),
                json.dumps(_json_safe_payload(summary.get("latest_metrics") or {}), ensure_ascii=False),
            ),
        )
        return self._connection.execute(
            """
            SELECT *
            FROM job_progress_event_summaries
            WHERE job_id = ?
            LIMIT 1
            """,
            (normalized_job_id,),
        ).fetchone()

    def _hydrate_job_progress_event_summary_locked(self, job_id: str) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {}
        rows = self._connection.execute(
            """
            SELECT event_id, stage, status, detail, payload_json, created_at
            FROM job_events
            WHERE job_id = ?
            ORDER BY event_id ASC
            """,
            (normalized_job_id,),
        ).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            try:
                payload = dict(json.loads(row["payload_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            events.append(
                {
                    "event_id": int(row["event_id"] or 0),
                    "stage": str(row["stage"] or ""),
                    "status": str(row["status"] or ""),
                    "detail": str(row["detail"] or ""),
                    "payload": payload,
                    "created_at": str(row["created_at"] or ""),
                }
            )
        summary = _build_job_progress_event_summary(events)
        if rows:
            self._upsert_job_progress_event_summary_locked(job_id=normalized_job_id, summary=summary)
        else:
            self._connection.execute(
                "DELETE FROM job_progress_event_summaries WHERE job_id = ?",
                (normalized_job_id,),
            )
        return {
            "job_id": normalized_job_id,
            **summary,
        }

    def _update_job_progress_event_summary_locked(self, *, job_id: str, event: dict[str, Any]) -> None:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return
        row = self._connection.execute(
            """
            SELECT *
            FROM job_progress_event_summaries
            WHERE job_id = ?
            LIMIT 1
            """,
            (normalized_job_id,),
        ).fetchone()
        if row is None:
            summary = {
                "event_count": 0,
                "latest_event": {},
                "stage_sequence": [],
                "stage_stats": {},
                "latest_metrics": {},
            }
        else:
            summary = {
                "event_count": int(row["event_count"] or 0),
                "latest_event": {},
                "stage_sequence": [],
                "stage_stats": {},
                "latest_metrics": {},
            }
            try:
                summary["latest_event"] = dict(json.loads(row["latest_event_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                summary["latest_event"] = {}
            try:
                summary["stage_sequence"] = list(json.loads(row["stage_sequence_json"] or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                summary["stage_sequence"] = []
            try:
                summary["stage_stats"] = dict(json.loads(row["stage_stats_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                summary["stage_stats"] = {}
            try:
                summary["latest_metrics"] = dict(json.loads(row["latest_metrics_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                summary["latest_metrics"] = {}
        summary = _cp_update_job_progress_event_summary(summary, event=event)
        self._upsert_job_progress_event_summary_locked(job_id=normalized_job_id, summary=summary)

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
        hydrated_summary: dict[str, Any] | None = None
        with self._lock, self._connection:
            row = self._connection.execute(
                """
                SELECT *
                FROM job_progress_event_summaries
                WHERE job_id = ?
                LIMIT 1
                """,
                (normalized_job_id,),
            ).fetchone()
            if row is None and hydrate_if_missing:
                hydrated_summary = self._hydrate_job_progress_event_summary_locked(normalized_job_id)
        if hydrated_summary is not None:
            self._replace_control_plane_table_from_sqlite("job_progress_event_summaries")
            return hydrated_summary
        return self._job_progress_event_summary_from_row(row)

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
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM job_results WHERE job_id = ?", (normalized_job_id,))
            self._connection.executemany(
                """
                INSERT INTO job_results (
                    job_id, candidate_id, rank_index, score, confidence_label, confidence_score, confidence_reason, explanation, matched_fields_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        payload["job_id"],
                        payload["candidate_id"],
                        payload["rank_index"],
                        payload["score"],
                        payload["confidence_label"],
                        payload["confidence_score"],
                        payload["confidence_reason"],
                        payload["explanation"],
                        payload["matched_fields_json"],
                    )
                    for payload in row_payloads
                ],
            )
            rows = self._connection.execute("SELECT * FROM job_results WHERE job_id = ?", (normalized_job_id,)).fetchall()
        for row in rows:
            self._mirror_control_plane_row("job_results", row)

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
        with self._lock:
            row = self._connection.execute("SELECT * FROM jobs WHERE job_id = ?", (normalized_job_id,)).fetchone()
        if row is None:
            return None
        return self._job_from_row(row)

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
        normalized_source_path = str(source_path or "").strip()
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
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO job_result_views (
                    view_id, job_id, target_company, company_key, source_kind, view_kind, snapshot_id, asset_view,
                    source_path, authoritative_snapshot_id, materialization_generation_key, request_signature,
                    summary_json, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    view_id = excluded.view_id,
                    target_company = excluded.target_company,
                    company_key = excluded.company_key,
                    source_kind = excluded.source_kind,
                    view_kind = excluded.view_kind,
                    snapshot_id = excluded.snapshot_id,
                    asset_view = excluded.asset_view,
                    source_path = excluded.source_path,
                    authoritative_snapshot_id = excluded.authoritative_snapshot_id,
                    materialization_generation_key = excluded.materialization_generation_key,
                    request_signature = excluded.request_signature,
                    summary_json = excluded.summary_json,
                    metadata_json = excluded.metadata_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    view_id,
                    normalized_job_id,
                    normalized_target_company,
                    normalized_company_key,
                    normalized_source_kind,
                    normalized_view_kind,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_source_path,
                    normalized_authoritative_snapshot_id,
                    normalized_generation_key,
                    normalized_request_signature,
                    summary_json,
                    metadata_json,
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM job_result_views WHERE job_id = ?",
                (normalized_job_id,),
            ).fetchone()
        self._mirror_control_plane_row("job_result_views", row)
        return self._job_result_view_from_row(row)

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
        clauses: list[str] = []
        params: list[Any] = []
        if normalized_job_id:
            clauses.append("job_id = ?")
            params.append(normalized_job_id)
        if normalized_view_id:
            clauses.append("view_id = ?")
            params.append(normalized_view_id)
        query = f"SELECT * FROM job_result_views WHERE {' OR '.join(clauses)} ORDER BY updated_at DESC LIMIT 1"
        with self._lock:
            row = self._connection.execute(query, tuple(params)).fetchone()
        return self._job_result_view_from_row(row)

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
        query = f"SELECT * FROM jobs WHERE {' AND '.join(clauses)} ORDER BY updated_at ASC, created_at ASC LIMIT ?"
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
        if self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
            return []
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._job_from_row(row) for row in rows]

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
        query = f"SELECT * FROM jobs WHERE {' AND '.join(clauses)} ORDER BY updated_at ASC, created_at ASC LIMIT ?"
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
        if self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
            return []
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._job_from_row(row) for row in rows]

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
        candidate_ids = [str(row.get("candidate_id") or "").strip() for row in rows if str(row.get("candidate_id") or "").strip()]
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
        if postgres_rows:
            postgres_records = self._job_result_records_from_postgres_rows(postgres_rows, include_evidence=include_evidence)
            if (
                postgres_records
                or self._control_plane_postgres_should_skip_sqlite_fallback("job_results")
                or self._control_plane_postgres_should_skip_sqlite_fallback("candidates")
            ):
                return postgres_records
        if self._control_plane_postgres_should_skip_sqlite_fallback("job_results"):
            return []
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT jr.*, c.display_name, c.name_en, c.name_zh, c.category, c.organization,
                       c.role, c.team, c.employment_status, c.focus_areas, c.education,
                       c.work_history, c.notes, c.linkedin_url, c.media_url, c.source_dataset, c.source_path, c.metadata_json
                FROM job_results jr
                JOIN candidates c ON c.candidate_id = jr.candidate_id
                WHERE jr.job_id = ?
                ORDER BY jr.rank_index
                """,
                (job_id,),
            ).fetchall()
        return self._job_result_records_from_rows(rows, include_evidence=include_evidence)

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
        if postgres_rows:
            selected_rows = postgres_rows[normalized_offset : normalized_offset + normalized_limit]
            postgres_records = self._job_result_records_from_postgres_rows(
                selected_rows,
                include_evidence=include_evidence,
            )
            if (
                postgres_records
                or self._control_plane_postgres_should_skip_sqlite_fallback("job_results")
                or self._control_plane_postgres_should_skip_sqlite_fallback("candidates")
            ):
                return postgres_records
        if self._control_plane_postgres_should_skip_sqlite_fallback("job_results"):
            return []
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT jr.*, c.display_name, c.name_en, c.name_zh, c.category, c.organization,
                       c.role, c.team, c.employment_status, c.focus_areas, c.education,
                       c.work_history, c.notes, c.linkedin_url, c.media_url, c.source_dataset, c.source_path, c.metadata_json
                FROM job_results jr
                JOIN candidates c ON c.candidate_id = jr.candidate_id
                WHERE jr.job_id = ?
                ORDER BY jr.rank_index
                LIMIT ? OFFSET ?
                """,
                (job_id, normalized_limit, normalized_offset),
            ).fetchall()
        return self._job_result_records_from_rows(rows, include_evidence=include_evidence)

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
        if postgres_rows:
            postgres_records = self._job_result_records_from_postgres_rows(postgres_rows, include_evidence=include_evidence)
            if (
                postgres_records
                or self._control_plane_postgres_should_skip_sqlite_fallback("job_results")
                or self._control_plane_postgres_should_skip_sqlite_fallback("candidates")
            ):
                return postgres_records
        if self._control_plane_postgres_should_skip_sqlite_fallback("job_results"):
            return []
        placeholders = ", ".join("?" for _ in normalized_candidate_ids)
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT jr.*, c.display_name, c.name_en, c.name_zh, c.category, c.organization,
                       c.role, c.team, c.employment_status, c.focus_areas, c.education,
                       c.work_history, c.notes, c.linkedin_url, c.media_url, c.source_dataset, c.source_path, c.metadata_json
                FROM job_results jr
                JOIN candidates c ON c.candidate_id = jr.candidate_id
                WHERE jr.job_id = ? AND jr.candidate_id IN ({placeholders})
                ORDER BY jr.rank_index
                """,
                (job_id, *normalized_candidate_ids),
            ).fetchall()
        return self._job_result_records_from_rows(rows, include_evidence=include_evidence)

    def count_job_results(self, job_id: str) -> int:
        postgres_rows = self._select_postgres_job_result_rows(
            where_sql="job_id = %s",
            params=[job_id],
            limit=0,
        )
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("job_results"):
            return len(postgres_rows)
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) AS row_count FROM job_results WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return int((row["row_count"] if row is not None else 0) or 0)

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
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO plan_review_sessions (
                    target_company, request_signature, request_family_signature,
                    matching_request_signature, matching_request_family_signature,
                    status, risk_level, required_before_execution,
                    request_json, plan_json, gate_json, execution_bundle_json, matching_request_json, decision_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    target_company,
                    request_signature(request_payload),
                    request_family_signature(request_payload),
                    str(matching_bundle.get("matching_request_signature") or ""),
                    str(matching_bundle.get("matching_request_family_signature") or ""),
                    status,
                    str(gate_payload.get("risk_level") or "medium"),
                    1 if bool(gate_payload.get("required_before_execution")) else 0,
                    json.dumps(request_payload, ensure_ascii=False),
                    json.dumps(plan_payload, ensure_ascii=False),
                    json.dumps(gate_payload, ensure_ascii=False),
                    json.dumps(_json_safe_payload(execution_bundle_value), ensure_ascii=False),
                    json.dumps(_json_safe_payload(matching_bundle), ensure_ascii=False),
                    json.dumps({}, ensure_ascii=False),
                ),
            )
            review_id = int(cursor.lastrowid)
            row = self._connection.execute(
                "SELECT * FROM plan_review_sessions WHERE review_id = ? LIMIT 1",
                (review_id,),
            ).fetchone()
        self._mirror_control_plane_row("plan_review_sessions", row)
        return self._plan_review_session_from_row(row) if row is not None else {}

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
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM plan_review_sessions WHERE review_id = ? LIMIT 1",
                (review_id,),
            ).fetchone()
        if row is None:
            return None
        return self._plan_review_session_from_row(row)

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
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            clauses.append("lower(target_company) = lower(?)")
            params.append(target_company)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection.execute(
            f"""
            SELECT * FROM plan_review_sessions
            {where_clause}
            ORDER BY updated_at DESC, review_id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [self._plan_review_session_from_row(row) for row in rows]

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
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE plan_review_sessions
                SET target_company = ?, request_signature = ?, request_family_signature = ?,
                    matching_request_signature = ?, matching_request_family_signature = ?,
                    status = ?, reviewer = ?, review_notes = ?, decision_json = ?, request_json = ?, plan_json = ?,
                    execution_bundle_json = CASE
                        WHEN ? <> '{}' THEN ?
                        ELSE execution_bundle_json
                    END,
                    matching_request_json = ?,
                    approved_at = CASE WHEN ? = 'approved' THEN CURRENT_TIMESTAMP ELSE approved_at END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE review_id = ?
                """,
                (
                    final_target_company,
                    request_signature(final_request_payload),
                    request_family_signature(final_request_payload),
                    str(matching_bundle.get("matching_request_signature") or ""),
                    str(matching_bundle.get("matching_request_family_signature") or ""),
                    status,
                    reviewer,
                    notes,
                    json.dumps(decision_payload or {}, ensure_ascii=False),
                    json.dumps(final_request_payload, ensure_ascii=False),
                    json.dumps(final_plan_payload, ensure_ascii=False),
                    execution_bundle_json,
                    execution_bundle_json,
                    json.dumps(_json_safe_payload(matching_bundle), ensure_ascii=False),
                    status,
                    review_id,
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM plan_review_sessions WHERE review_id = ? LIMIT 1",
                (review_id,),
            ).fetchone()
        self._mirror_control_plane_row("plan_review_sessions", row)
        return self._plan_review_session_from_row(row) if row is not None else None

    def replace_manual_review_items(self, job_id: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized_items = _prepare_manual_review_items(items)
        scope_keys = {_manual_review_scope_key(item) for item in normalized_items if _manual_review_scope_key(item)}
        legacy_scope_keys = {
            _manual_review_scope_key(item, include_snapshot=False)
            for item in normalized_items
            if _manual_review_scope_key(item, include_snapshot=False)
        }
        if self._control_plane_postgres_should_prefer_read("manual_review_items"):
            self._call_control_plane_postgres_native(
                "delete_rows",
                table_name="manual_review_items",
                where_sql="job_id = %s",
                params=[job_id],
            )
            if legacy_scope_keys:
                existing_rows = self._select_control_plane_rows(
                    "manual_review_items",
                    row_builder=self._manual_review_item_from_row,
                    where_sql="status = %s",
                    params=["open"],
                    order_by_sql="updated_at DESC, review_item_id DESC",
                    limit=0,
                )
                for existing_row in existing_rows:
                    if str(existing_row.get("job_id") or "") == job_id:
                        continue
                    row_scope = _manual_review_scope_key(existing_row)
                    row_legacy_scope = _manual_review_scope_key(existing_row, include_snapshot=False)
                    if row_scope not in scope_keys and row_legacy_scope not in legacy_scope_keys:
                        continue
                    self._call_control_plane_postgres_native(
                        "update_row_returning",
                        table_name="manual_review_items",
                        id_column="review_item_id",
                        id_value=int(existing_row.get("review_item_id") or 0),
                        row={
                            "status": "superseded",
                            "review_notes": _append_review_note(
                                existing_row.get("review_notes"),
                                "Superseded by a newer manual-review queue item.",
                            ),
                            "updated_at": _utc_now_timestamp(),
                        },
                    )
            for item in normalized_items:
                now = _utc_now_timestamp()
                self._call_control_plane_postgres_native(
                    "insert_row_with_generated_id",
                    table_name="manual_review_items",
                    row={
                        "job_id": job_id,
                        "candidate_id": str(item.get("candidate_id") or ""),
                        "target_company": str(item.get("target_company") or ""),
                        "review_type": str(item.get("review_type") or ""),
                        "priority": str(item.get("priority") or "medium"),
                        "status": str(item.get("status") or "open"),
                        "summary": str(item.get("summary") or ""),
                        "candidate_json": json.dumps(item.get("candidate") or {}, ensure_ascii=False),
                        "evidence_json": json.dumps(item.get("evidence") or [], ensure_ascii=False),
                        "metadata_json": json.dumps(item.get("metadata") or {}, ensure_ascii=False),
                        "reviewed_by": "",
                        "review_notes": "",
                        "reviewed_at": "",
                        "created_at": now,
                        "updated_at": now,
                    },
                )
            return self.list_manual_review_items(job_id=job_id, status="", limit=max(len(normalized_items), 1))
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM manual_review_items WHERE job_id = ?", (job_id,))
            if legacy_scope_keys:
                rows = self._connection.execute(
                    """
                    SELECT * FROM manual_review_items
                    WHERE status = 'open'
                    """,
                ).fetchall()
                stale_review_item_ids: list[int] = []
                for row in rows:
                    row_scope = _manual_review_scope_key(_manual_review_item_from_row_payload(row))
                    row_legacy_scope = _manual_review_scope_key(
                        _manual_review_item_from_row_payload(row),
                        include_snapshot=False,
                    )
                    if row["job_id"] == job_id:
                        continue
                    if row_scope in scope_keys or row_legacy_scope in legacy_scope_keys:
                        stale_review_item_ids.append(int(row["review_item_id"]))
                for review_item_id in stale_review_item_ids:
                    self._connection.execute(
                        """
                        UPDATE manual_review_items
                        SET status = 'superseded',
                            review_notes = CASE
                                WHEN trim(coalesce(review_notes, '')) = '' THEN 'Superseded by a newer manual-review queue item.'
                                WHEN instr(review_notes, 'Superseded by a newer manual-review queue item.') > 0 THEN review_notes
                                ELSE review_notes || ' | Superseded by a newer manual-review queue item.'
                            END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE review_item_id = ?
                        """,
                        (review_item_id,),
                    )
            for item in normalized_items:
                self._connection.execute(
                    """
                    INSERT INTO manual_review_items (
                        job_id, candidate_id, target_company, review_type, priority, status, summary,
                        candidate_json, evidence_json, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        str(item.get("candidate_id") or ""),
                        str(item.get("target_company") or ""),
                        str(item.get("review_type") or ""),
                        str(item.get("priority") or "medium"),
                        str(item.get("status") or "open"),
                        str(item.get("summary") or ""),
                        json.dumps(item.get("candidate") or {}, ensure_ascii=False),
                        json.dumps(item.get("evidence") or [], ensure_ascii=False),
                        json.dumps(item.get("metadata") or {}, ensure_ascii=False),
                    ),
                )
        self._replace_control_plane_table_from_sqlite("manual_review_items")
        return self.list_manual_review_items(job_id=job_id, status="", limit=max(len(normalized_items), 1))

    def list_manual_review_items(
        self,
        *,
        target_company: str = "",
        status: str = "open",
        job_id: str = "",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        postgres_rows = self._select_control_plane_rows(
            "manual_review_items",
            row_builder=self._manual_review_item_from_row,
            where_sql=" AND ".join(
                [
                    *(["lower(target_company) = lower(%s)"] if target_company else []),
                    *(["status = %s"] if status else []),
                    *(["job_id = %s"] if job_id else []),
                ]
            ),
            params=[
                *([target_company] if target_company else []),
                *([status] if status else []),
                *([job_id] if job_id else []),
            ],
            order_by_sql="updated_at DESC, review_item_id DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        if self._control_plane_postgres_should_skip_sqlite_fallback("manual_review_items"):
            return []
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            clauses.append("lower(target_company) = lower(?)")
            params.append(target_company)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if job_id:
            clauses.append("job_id = ?")
            params.append(job_id)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT * FROM manual_review_items
                {where_clause}
                ORDER BY updated_at DESC, review_item_id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [self._manual_review_item_from_row(row) for row in rows]

    def count_manual_review_items(
        self,
        *,
        target_company: str = "",
        status: str = "open",
        job_id: str = "",
    ) -> int:
        postgres_rows = self._select_control_plane_rows(
            "manual_review_items",
            row_builder=self._manual_review_item_from_row,
            where_sql=" AND ".join(
                [
                    *(["lower(target_company) = lower(%s)"] if target_company else []),
                    *(["status = %s"] if status else []),
                    *(["job_id = %s"] if job_id else []),
                ]
            ),
            params=[
                *([target_company] if target_company else []),
                *([status] if status else []),
                *([job_id] if job_id else []),
            ],
            order_by_sql="review_item_id DESC",
            limit=0,
        )
        if postgres_rows:
            return len(postgres_rows)
        if self._control_plane_postgres_should_skip_sqlite_fallback("manual_review_items"):
            return 0
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            clauses.append("lower(target_company) = lower(?)")
            params.append(target_company)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if job_id:
            clauses.append("job_id = ?")
            params.append(job_id)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            row = self._connection.execute(
                f"SELECT COUNT(*) AS row_count FROM manual_review_items {where_clause}",
                tuple(params),
            ).fetchone()
        return int((row["row_count"] if row is not None else 0) or 0)

    def cleanup_manual_review_items(
        self,
        *,
        target_company: str = "",
        snapshot_id: str = "",
    ) -> dict[str, Any]:
        if self._control_plane_postgres_should_prefer_read("manual_review_items"):
            rows = self._select_control_plane_rows(
                "manual_review_items",
                row_builder=self._manual_review_item_from_row,
                where_sql=" AND ".join(["lower(target_company) = lower(%s)"] if target_company else []),
                params=[target_company] if target_company else [],
                order_by_sql="updated_at DESC, review_item_id DESC",
                limit=0,
            )
            metadata_updated = 0
            superseded_count = 0
            out_of_scope_count = 0
            latest_open_by_scope: dict[tuple[str, str, str], int] = {}
            for row in rows:
                payload = dict(row)
                metadata = dict(payload.get("metadata") or {})
                inferred_snapshot_id = _infer_manual_review_snapshot_id(payload)
                review_item_id = int(payload.get("review_item_id") or 0)
                if inferred_snapshot_id and str(metadata.get("snapshot_id") or "").strip() != inferred_snapshot_id:
                    metadata["snapshot_id"] = inferred_snapshot_id
                    self._call_control_plane_postgres_native(
                        "update_row_returning",
                        table_name="manual_review_items",
                        id_column="review_item_id",
                        id_value=review_item_id,
                        row={
                            "metadata_json": json.dumps(metadata, ensure_ascii=False),
                            "updated_at": _utc_now_timestamp(),
                        },
                    )
                    payload["metadata"] = metadata
                    metadata_updated += 1

                row_snapshot_id = str(metadata.get("snapshot_id") or "").strip()
                if str(payload.get("status") or "") != "open":
                    continue
                if snapshot_id and row_snapshot_id and row_snapshot_id != snapshot_id:
                    self._call_control_plane_postgres_native(
                        "update_row_returning",
                        table_name="manual_review_items",
                        id_column="review_item_id",
                        id_value=review_item_id,
                        row={
                            "status": "out_of_scope",
                            "review_notes": _append_review_note(
                                payload.get("review_notes"),
                                "Marked out of scope for the active snapshot.",
                            ),
                            "updated_at": _utc_now_timestamp(),
                        },
                    )
                    out_of_scope_count += 1
                    continue

                scope_key = _manual_review_scope_key(payload, include_snapshot=False)
                if not scope_key:
                    continue
                if scope_key in latest_open_by_scope:
                    self._call_control_plane_postgres_native(
                        "update_row_returning",
                        table_name="manual_review_items",
                        id_column="review_item_id",
                        id_value=review_item_id,
                        row={
                            "status": "superseded",
                            "review_notes": _append_review_note(
                                payload.get("review_notes"),
                                "Superseded by a newer manual-review queue item.",
                            ),
                            "updated_at": _utc_now_timestamp(),
                        },
                    )
                    superseded_count += 1
                    continue
                latest_open_by_scope[scope_key] = review_item_id
            return {
                "target_company": target_company,
                "snapshot_id": snapshot_id,
                "metadata_updated_count": metadata_updated,
                "superseded_count": superseded_count,
                "out_of_scope_count": out_of_scope_count,
            }
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            clauses.append("lower(target_company) = lower(?)")
            params.append(target_company)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        metadata_updated = 0
        superseded_count = 0
        out_of_scope_count = 0
        with self._lock, self._connection:
            rows = self._connection.execute(
                f"""
                SELECT * FROM manual_review_items
                {where_clause}
                ORDER BY updated_at DESC, review_item_id DESC
                """,
                params,
            ).fetchall()

            latest_open_by_scope: dict[tuple[str, str, str], int] = {}
            for row in rows:
                payload = _manual_review_item_from_row_payload(row)
                metadata = dict(payload.get("metadata") or {})
                inferred_snapshot_id = _infer_manual_review_snapshot_id(payload)
                if inferred_snapshot_id and str(metadata.get("snapshot_id") or "").strip() != inferred_snapshot_id:
                    metadata["snapshot_id"] = inferred_snapshot_id
                    self._connection.execute(
                        "UPDATE manual_review_items SET metadata_json = ?, updated_at = CURRENT_TIMESTAMP WHERE review_item_id = ?",
                        (json.dumps(metadata, ensure_ascii=False), int(row["review_item_id"])),
                    )
                    payload["metadata"] = metadata
                    metadata_updated += 1

                row_snapshot_id = str(metadata.get("snapshot_id") or "").strip()
                if str(row["status"] or "") != "open":
                    continue
                if snapshot_id and row_snapshot_id and row_snapshot_id != snapshot_id:
                    self._connection.execute(
                        """
                        UPDATE manual_review_items
                        SET status = 'out_of_scope',
                            review_notes = CASE
                                WHEN trim(coalesce(review_notes, '')) = '' THEN 'Marked out of scope for the active snapshot.'
                                WHEN instr(review_notes, 'Marked out of scope for the active snapshot.') > 0 THEN review_notes
                                ELSE review_notes || ' | Marked out of scope for the active snapshot.'
                            END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE review_item_id = ?
                        """,
                        (int(row["review_item_id"]),),
                    )
                    out_of_scope_count += 1
                    continue

                scope_key = _manual_review_scope_key(payload, include_snapshot=False)
                if not scope_key:
                    continue
                if scope_key in latest_open_by_scope:
                    self._connection.execute(
                        """
                        UPDATE manual_review_items
                        SET status = 'superseded',
                            review_notes = CASE
                                WHEN trim(coalesce(review_notes, '')) = '' THEN 'Superseded by a newer manual-review queue item.'
                                WHEN instr(review_notes, 'Superseded by a newer manual-review queue item.') > 0 THEN review_notes
                                ELSE review_notes || ' | Superseded by a newer manual-review queue item.'
                            END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE review_item_id = ?
                        """,
                        (int(row["review_item_id"]),),
                    )
                    superseded_count += 1
                    continue
                latest_open_by_scope[scope_key] = int(row["review_item_id"])
        self._replace_control_plane_table_from_sqlite("manual_review_items")

        return {
            "target_company": target_company,
            "snapshot_id": snapshot_id,
            "metadata_updated_count": metadata_updated,
            "superseded_count": superseded_count,
            "out_of_scope_count": out_of_scope_count,
        }

    def review_manual_review_item(
        self,
        *,
        review_item_id: int,
        action: str,
        reviewer: str = "",
        notes: str = "",
        candidate_payload: dict[str, Any] | None = None,
        evidence_payload: list[dict[str, Any]] | None = None,
        metadata_merge: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        existing = self._select_control_plane_row(
            "manual_review_items",
            row_builder=self._manual_review_item_from_row,
            where_sql="review_item_id = %s",
            params=[review_item_id],
        )
        if existing is None:
            existing = self.get_manual_review_item(review_item_id)
        if existing is None:
            return None
        normalized_action = str(action or "").strip().lower()
        if normalized_action in {"open", "reopen", "pending"}:
            status = "open"
        elif normalized_action in {"resolve", "resolved", "approve", "approved"}:
            status = "resolved"
        elif normalized_action in {"dismiss", "dismissed", "reject", "rejected"}:
            status = "dismissed"
        elif normalized_action in {"escalate", "escalated"}:
            status = "escalated"
        else:
            status = existing.get("status") or "open"
        merged_metadata = dict(existing.get("metadata") or {})
        merged_metadata.update(metadata_merge or {})
        stored_candidate = (
            candidate_payload
            if isinstance(candidate_payload, dict) and candidate_payload
            else existing.get("candidate") or {}
        )
        stored_evidence = (
            evidence_payload
            if isinstance(evidence_payload, list) and evidence_payload
            else existing.get("evidence") or []
        )
        if self._control_plane_postgres_should_prefer_read("manual_review_items"):
            row = self._call_control_plane_postgres_native(
                "update_row_returning",
                table_name="manual_review_items",
                id_column="review_item_id",
                id_value=review_item_id,
                row={
                    "status": status,
                    "summary": str(existing.get("summary") or ""),
                    "candidate_json": json.dumps(stored_candidate, ensure_ascii=False),
                    "evidence_json": json.dumps(stored_evidence, ensure_ascii=False),
                    "metadata_json": json.dumps(merged_metadata, ensure_ascii=False),
                    "reviewed_by": reviewer,
                    "review_notes": notes,
                    "reviewed_at": _utc_now_timestamp(),
                    "updated_at": _utc_now_timestamp(),
                },
            )
            if row is not None:
                return self._manual_review_item_from_row(row)
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE manual_review_items
                SET status = ?, summary = ?, candidate_json = ?, evidence_json = ?, metadata_json = ?,
                    reviewed_by = ?, review_notes = ?, reviewed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE review_item_id = ?
                """,
                (
                    status,
                    str(existing.get("summary") or ""),
                    json.dumps(stored_candidate, ensure_ascii=False),
                    json.dumps(stored_evidence, ensure_ascii=False),
                    json.dumps(merged_metadata, ensure_ascii=False),
                    reviewer,
                    notes,
                    review_item_id,
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM manual_review_items WHERE review_item_id = ? LIMIT 1",
                (review_item_id,),
            ).fetchone()
        self._mirror_control_plane_row("manual_review_items", row)
        return self.get_manual_review_item(review_item_id)

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("candidate_review_registry"):
            return []
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT * FROM candidate_review_registry
                {where_clause}
                ORDER BY updated_at DESC, added_at DESC, record_id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [self._candidate_review_record_from_row(row) for row in rows]

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
        with self._lock, self._connection:
            existing = self._connection.execute(
                "SELECT added_at FROM candidate_review_registry WHERE record_id = ? LIMIT 1",
                (normalized["record_id"],),
            ).fetchone()
            self._connection.execute(
                """
                INSERT INTO candidate_review_registry (
                    record_id, job_id, history_id, candidate_id, candidate_name, headline,
                    current_company, avatar_url, linkedin_url, primary_email, status, comment,
                    source, metadata_json, added_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
                ON CONFLICT(record_id) DO UPDATE SET
                    job_id = excluded.job_id,
                    history_id = excluded.history_id,
                    candidate_id = excluded.candidate_id,
                    candidate_name = excluded.candidate_name,
                    headline = excluded.headline,
                    current_company = excluded.current_company,
                    avatar_url = excluded.avatar_url,
                    linkedin_url = excluded.linkedin_url,
                    primary_email = excluded.primary_email,
                    status = excluded.status,
                    comment = excluded.comment,
                    source = excluded.source,
                    metadata_json = excluded.metadata_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    normalized["record_id"],
                    normalized["job_id"],
                    normalized["history_id"],
                    normalized["candidate_id"],
                    normalized["candidate_name"],
                    normalized["headline"],
                    normalized["current_company"],
                    normalized["avatar_url"],
                    normalized["linkedin_url"],
                    normalized["primary_email"],
                    normalized["status"],
                    normalized["comment"],
                    normalized["source"],
                    json.dumps(normalized["metadata"], ensure_ascii=False),
                    existing["added_at"] if existing is not None else normalized.get("added_at"),
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM candidate_review_registry WHERE record_id = ? LIMIT 1",
                (normalized["record_id"],),
            ).fetchone()
        self._mirror_control_plane_row("candidate_review_registry", row)
        return self.get_candidate_review_record(normalized["record_id"]) or normalized

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("candidate_review_registry"):
            return None
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM candidate_review_registry WHERE record_id = ? LIMIT 1",
                (normalized_record_id,),
            ).fetchone()
        if row is None:
            return None
        return self._candidate_review_record_from_row(row)

    def list_target_candidates(
        self,
        *,
        job_id: str = "",
        history_id: str = "",
        candidate_id: str = "",
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
                    *(["follow_up_status = %s"] if follow_up_status else []),
                ]
            ),
            params=[
                *([job_id] if job_id else []),
                *([history_id] if history_id else []),
                *([candidate_id] if candidate_id else []),
                *([_normalize_target_candidate_follow_up_status(follow_up_status)] if follow_up_status else []),
            ],
            order_by_sql="updated_at DESC, added_at DESC, record_id DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        if self._control_plane_postgres_should_skip_sqlite_fallback("target_candidates"):
            return []
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT * FROM target_candidates
                {where_clause}
                ORDER BY updated_at DESC, added_at DESC, record_id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [self._target_candidate_from_row(row) for row in rows]

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
        with self._lock, self._connection:
            existing = self._connection.execute(
                "SELECT added_at FROM target_candidates WHERE record_id = ? LIMIT 1",
                (normalized["record_id"],),
            ).fetchone()
            self._connection.execute(
                """
                INSERT INTO target_candidates (
                    record_id, candidate_id, history_id, job_id, candidate_name, headline,
                    current_company, avatar_url, linkedin_url, primary_email, follow_up_status,
                    quality_score, comment, metadata_json, added_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
                ON CONFLICT(record_id) DO UPDATE SET
                    candidate_id = excluded.candidate_id,
                    history_id = excluded.history_id,
                    job_id = excluded.job_id,
                    candidate_name = excluded.candidate_name,
                    headline = excluded.headline,
                    current_company = excluded.current_company,
                    avatar_url = excluded.avatar_url,
                    linkedin_url = excluded.linkedin_url,
                    primary_email = excluded.primary_email,
                    follow_up_status = excluded.follow_up_status,
                    quality_score = excluded.quality_score,
                    comment = excluded.comment,
                    metadata_json = excluded.metadata_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    normalized["record_id"],
                    normalized["candidate_id"],
                    normalized["history_id"],
                    normalized["job_id"],
                    normalized["candidate_name"],
                    normalized["headline"],
                    normalized["current_company"],
                    normalized["avatar_url"],
                    normalized["linkedin_url"],
                    normalized["primary_email"],
                    normalized["follow_up_status"],
                    normalized["quality_score"],
                    normalized["comment"],
                    json.dumps(normalized["metadata"], ensure_ascii=False),
                    existing["added_at"] if existing is not None else normalized.get("added_at"),
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM target_candidates WHERE record_id = ? LIMIT 1",
                (normalized["record_id"],),
            ).fetchone()
        self._mirror_control_plane_row("target_candidates", row)
        return self.get_target_candidate(normalized["record_id"]) or normalized

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("target_candidates"):
            return None
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM target_candidates WHERE record_id = ? LIMIT 1",
                (normalized_record_id,),
            ).fetchone()
        if row is None:
            return None
        return self._target_candidate_from_row(row)

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("asset_default_pointers"):
            return None
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM asset_default_pointers WHERE pointer_key = ? LIMIT 1",
                (normalized_pointer_key,),
            ).fetchone()
        if row is None:
            return None
        return self._asset_default_pointer_from_row(row)

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("asset_default_pointers"):
            return []
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT * FROM asset_default_pointers
                {where_clause}
                ORDER BY updated_at DESC, pointer_key ASC
                LIMIT ?
                """,
                (*params, max(1, int(limit or 1))),
            ).fetchall()
        return [self._asset_default_pointer_from_row(row) for row in rows]

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("asset_default_pointer_history"):
            return []
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM asset_default_pointer_history
                WHERE pointer_key = ?
                ORDER BY occurred_at DESC, created_at DESC, history_id DESC
                LIMIT ?
                """,
                (normalized_pointer_key, max(1, int(limit or 1))),
            ).fetchall()
        return [self._asset_default_pointer_history_from_row(row) for row in rows]

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
            "created_at": str((self.get_asset_default_pointer(pointer_key=normalized["pointer_key"]) or {}).get("created_at") or now),
            "updated_at": now,
        }
        if self._write_control_plane_row_to_postgres("asset_default_pointers", row_payload):
            return (
                self.get_asset_default_pointer(pointer_key=normalized["pointer_key"])
                or self._asset_default_pointer_from_row(row_payload)
            )
        with self._lock, self._connection:
            existing = self._connection.execute(
                "SELECT created_at FROM asset_default_pointers WHERE pointer_key = ? LIMIT 1",
                (normalized["pointer_key"],),
            ).fetchone()
            self._connection.execute(
                """
                INSERT INTO asset_default_pointers (
                    pointer_key, company_key, scope_kind, scope_key, asset_kind,
                    snapshot_id, lifecycle_status, coverage_proof_json, previous_snapshot_id,
                    promoted_by_job_id, promoted_at, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
                ON CONFLICT(pointer_key) DO UPDATE SET
                    company_key = excluded.company_key,
                    scope_kind = excluded.scope_kind,
                    scope_key = excluded.scope_key,
                    asset_kind = excluded.asset_kind,
                    snapshot_id = excluded.snapshot_id,
                    lifecycle_status = excluded.lifecycle_status,
                    coverage_proof_json = excluded.coverage_proof_json,
                    previous_snapshot_id = excluded.previous_snapshot_id,
                    promoted_by_job_id = excluded.promoted_by_job_id,
                    promoted_at = excluded.promoted_at,
                    metadata_json = excluded.metadata_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    row_payload["pointer_key"],
                    row_payload["company_key"],
                    row_payload["scope_kind"],
                    row_payload["scope_key"],
                    row_payload["asset_kind"],
                    row_payload["snapshot_id"],
                    row_payload["lifecycle_status"],
                    row_payload["coverage_proof_json"],
                    row_payload["previous_snapshot_id"],
                    row_payload["promoted_by_job_id"],
                    row_payload["promoted_at"],
                    row_payload["metadata_json"],
                    existing["created_at"] if existing is not None else row_payload["created_at"],
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM asset_default_pointers WHERE pointer_key = ? LIMIT 1",
                (normalized["pointer_key"],),
            ).fetchone()
        self._mirror_control_plane_row("asset_default_pointers", row)
        return self.get_asset_default_pointer(pointer_key=normalized["pointer_key"]) or normalized

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
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO asset_default_pointer_history (
                    history_id, pointer_key, company_key, scope_kind, scope_key, asset_kind,
                    snapshot_id, lifecycle_status, event_type, payload_json, occurred_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                ON CONFLICT(history_id) DO UPDATE SET
                    payload_json = excluded.payload_json
                """,
                (
                    row_payload["history_id"],
                    row_payload["pointer_key"],
                    row_payload["company_key"],
                    row_payload["scope_kind"],
                    row_payload["scope_key"],
                    row_payload["asset_kind"],
                    row_payload["snapshot_id"],
                    row_payload["lifecycle_status"],
                    row_payload["event_type"],
                    row_payload["payload_json"],
                    row_payload["occurred_at"],
                    row_payload["created_at"],
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM asset_default_pointer_history WHERE history_id = ? LIMIT 1",
                (row_payload["history_id"],),
            ).fetchone()
        self._mirror_control_plane_row("asset_default_pointer_history", row)
        return self._asset_default_pointer_history_from_row(row)

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
        if postgres_row is not None:
            return postgres_row
        if self._control_plane_postgres_should_skip_sqlite_fallback("frontend_history_links"):
            return None
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM frontend_history_links WHERE history_id = ? LIMIT 1",
                (normalized_history_id,),
            ).fetchone()
        if row is None:
            return None
        return self._frontend_history_link_from_row(row)

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
        if postgres_rows:
            return postgres_rows
        if self._control_plane_postgres_should_skip_sqlite_fallback("frontend_history_links"):
            return []
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM frontend_history_links
                ORDER BY updated_at DESC, created_at DESC, history_id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._frontend_history_link_from_row(row) for row in rows]

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
        if postgres_rows:
            return postgres_rows
        if self._control_plane_postgres_should_skip_sqlite_fallback("frontend_history_links"):
            return []
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM frontend_history_links
                WHERE job_id = ?
                ORDER BY updated_at DESC, created_at DESC, history_id DESC
                LIMIT ?
                """,
                (normalized_job_id, limit),
            ).fetchall()
        return [self._frontend_history_link_from_row(row) for row in rows]

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
        if postgres_rows:
            return postgres_rows
        if self._control_plane_postgres_should_skip_sqlite_fallback("frontend_history_links"):
            return []
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM frontend_history_links
                WHERE review_id = ?
                ORDER BY updated_at DESC, created_at DESC, history_id DESC
                LIMIT ?
                """,
                (normalized_review_id, limit),
            ).fetchall()
        return [self._frontend_history_link_from_row(row) for row in rows]

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
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO frontend_history_links (
                    history_id, query_text, target_company, review_id, job_id, phase,
                    request_json, plan_json, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
                ON CONFLICT(history_id) DO UPDATE SET
                    query_text = CASE
                        WHEN excluded.query_text <> '' THEN excluded.query_text
                        ELSE frontend_history_links.query_text
                    END,
                    target_company = CASE
                        WHEN excluded.target_company <> '' THEN excluded.target_company
                        ELSE frontend_history_links.target_company
                    END,
                    review_id = CASE
                        WHEN excluded.review_id > 0 THEN excluded.review_id
                        ELSE frontend_history_links.review_id
                    END,
                    job_id = CASE
                        WHEN excluded.job_id <> '' THEN excluded.job_id
                        ELSE frontend_history_links.job_id
                    END,
                    phase = CASE
                        WHEN excluded.phase <> '' THEN excluded.phase
                        ELSE frontend_history_links.phase
                    END,
                    request_json = CASE
                        WHEN excluded.request_json <> '{}' THEN excluded.request_json
                        ELSE frontend_history_links.request_json
                    END,
                    plan_json = CASE
                        WHEN excluded.plan_json <> '{}' THEN excluded.plan_json
                        ELSE frontend_history_links.plan_json
                    END,
                    metadata_json = excluded.metadata_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    normalized["history_id"],
                    normalized["query_text"],
                    normalized["target_company"],
                    normalized["review_id"],
                    normalized["job_id"],
                    normalized["phase"],
                    json.dumps(_json_safe_payload(request_payload), ensure_ascii=False),
                    json.dumps(_json_safe_payload(plan_payload), ensure_ascii=False),
                    json.dumps(_json_safe_payload(merged_metadata), ensure_ascii=False),
                    (
                        str((existing or {}).get("created_at") or "").strip()
                        or str(normalized.get("created_at") or "").strip()
                        or None
                    ),
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM frontend_history_links WHERE history_id = ? LIMIT 1",
                (normalized["history_id"],),
            ).fetchone()
        self._mirror_control_plane_row("frontend_history_links", row)
        return self.get_frontend_history_link(normalized["history_id"]) or normalized

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
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "DELETE FROM frontend_history_links WHERE history_id = ?",
                (normalized_history_id,),
            )
        return int(cursor.rowcount or 0) > 0

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

    def get_manual_review_item(self, review_item_id: int) -> dict[str, Any] | None:
        if review_item_id <= 0:
            return None
        postgres_row = self._select_control_plane_row(
            "manual_review_items",
            row_builder=self._manual_review_item_from_row,
            where_sql="review_item_id = %s",
            params=[review_item_id],
        )
        if postgres_row is not None:
            return postgres_row
        if self._control_plane_postgres_should_skip_sqlite_fallback("manual_review_items"):
            return None
        row = self._connection.execute(
            "SELECT * FROM manual_review_items WHERE review_item_id = ? LIMIT 1",
            (review_item_id,),
        ).fetchone()
        if row is None:
            return None
        return self._manual_review_item_from_row(row)

    def merge_manual_review_item_metadata(
        self,
        review_item_id: int,
        metadata_merge: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        existing = self.get_manual_review_item(review_item_id)
        if existing is None:
            return None
        merged_metadata = dict(existing.get("metadata") or {})
        merged_metadata.update(dict(metadata_merge or {}))
        if self._control_plane_postgres_should_prefer_read("manual_review_items"):
            row = self._call_control_plane_postgres_native(
                "update_row_returning",
                table_name="manual_review_items",
                id_column="review_item_id",
                id_value=review_item_id,
                row={
                    "metadata_json": json.dumps(merged_metadata, ensure_ascii=False),
                    "updated_at": _utc_now_timestamp(),
                },
            )
            if row is not None:
                return self._manual_review_item_from_row(row)
            if self._control_plane_postgres_should_skip_sqlite_fallback("manual_review_items"):
                return None
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE manual_review_items
                SET metadata_json = ?, updated_at = CURRENT_TIMESTAMP
                WHERE review_item_id = ?
                """,
                (
                    json.dumps(merged_metadata, ensure_ascii=False),
                    review_item_id,
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM manual_review_items WHERE review_item_id = ? LIMIT 1",
                (review_item_id,),
            ).fetchone()
        self._mirror_control_plane_row("manual_review_items", row)
        return self.get_manual_review_item(review_item_id)

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
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO agent_runtime_sessions (
                    job_id, target_company, request_signature, request_family_signature, runtime_mode, status, lanes_json, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    target_company = excluded.target_company,
                    request_signature = excluded.request_signature,
                    request_family_signature = excluded.request_family_signature,
                    runtime_mode = excluded.runtime_mode,
                    status = excluded.status,
                    lanes_json = excluded.lanes_json,
                    metadata_json = excluded.metadata_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    job_id,
                    target_company,
                    request_signature(canonical_request),
                    request_family_signature(canonical_request),
                    runtime_mode,
                    "running",
                    json.dumps(lanes, ensure_ascii=False),
                    json.dumps(metadata or {"plan": plan_payload}, ensure_ascii=False),
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM agent_runtime_sessions WHERE job_id = ? LIMIT 1",
                (job_id,),
            ).fetchone()
        self._mirror_control_plane_row("agent_runtime_sessions", row)
        session = self.get_agent_runtime_session(job_id=job_id)
        if session is None:
            raise RuntimeError(f"Failed to create agent runtime session for {job_id}")
        return session

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
        with self._lock:
            if session_id > 0:
                row = self._connection.execute(
                    "SELECT * FROM agent_runtime_sessions WHERE session_id = ? LIMIT 1",
                    (session_id,),
                ).fetchone()
            elif job_id:
                row = self._connection.execute(
                    "SELECT * FROM agent_runtime_sessions WHERE job_id = ? LIMIT 1",
                    (job_id,),
                ).fetchone()
            else:
                return None
        if row is None:
            return None
        return self._agent_runtime_session_from_row(row)

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
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO agent_trace_spans (
                    session_id, job_id, parent_span_id, lane_id, handoff_from_lane, handoff_to_lane,
                    span_name, stage, status, input_json, output_json, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    job_id,
                    parent_span_id or None,
                    lane_id,
                    handoff_from_lane,
                    handoff_to_lane,
                    span_name,
                    stage,
                    "running",
                    json.dumps(input_payload or {}, ensure_ascii=False),
                    json.dumps({}, ensure_ascii=False),
                    json.dumps(metadata or {}, ensure_ascii=False),
                ),
            )
            span_id = int(cursor.lastrowid)
        span = self.get_agent_trace_span(span_id)
        if span is None:
            raise RuntimeError(f"Failed to create agent trace span for {job_id}:{lane_id}:{span_name}")
        return span

    def update_agent_runtime_session_status(self, job_id: str, status: str) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_runtime_sessions"):
            row = self._call_control_plane_postgres_native(
                "update_agent_runtime_session_status",
                str(job_id),
                str(status),
            )
            if row is not None:
                return self._agent_runtime_session_from_row(row)
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE agent_runtime_sessions
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE job_id = ?
                """,
                (status, job_id),
            )
            row = self._connection.execute(
                "SELECT * FROM agent_runtime_sessions WHERE job_id = ? LIMIT 1",
                (job_id,),
            ).fetchone()
        self._mirror_control_plane_row("agent_runtime_sessions", row)
        return self.get_agent_runtime_session(job_id=job_id)

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
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE agent_trace_spans
                SET status = ?, output_json = ?, handoff_to_lane = CASE WHEN ? <> '' THEN ? ELSE handoff_to_lane END,
                    completed_at = CURRENT_TIMESTAMP
                WHERE span_id = ?
                """,
                (
                    status,
                    json.dumps(output_payload or {}, ensure_ascii=False),
                    handoff_to_lane,
                    handoff_to_lane,
                    span_id,
                ),
            )
        span = self.get_agent_trace_span(span_id)
        if span is None:
            raise RuntimeError(f"Failed to complete agent trace span {span_id}")
        return span

    def get_agent_trace_span(self, span_id: int) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_trace_spans"):
            row = self._call_control_plane_postgres_native("get_agent_trace_span", span_id)
            return self._agent_trace_span_from_row(row) if row is not None else None
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM agent_trace_spans WHERE span_id = ? LIMIT 1",
                (span_id,),
            ).fetchone()
        if row is None:
            return None
        return self._agent_trace_span_from_row(row)

    def list_agent_trace_spans(self, *, job_id: str = "", session_id: int = 0) -> list[dict[str, Any]]:
        if self._control_plane_postgres_should_prefer_read("agent_trace_spans"):
            rows = self._call_control_plane_postgres_native(
                "list_agent_trace_spans",
                job_id=job_id,
                session_id=session_id,
            )
            return [self._agent_trace_span_from_row(row) for row in list(rows or [])]
        with self._lock:
            if session_id > 0:
                rows = self._connection.execute(
                    "SELECT * FROM agent_trace_spans WHERE session_id = ? ORDER BY span_id",
                    (session_id,),
                ).fetchall()
            elif job_id:
                rows = self._connection.execute(
                    "SELECT * FROM agent_trace_spans WHERE job_id = ? ORDER BY span_id",
                    (job_id,),
                ).fetchall()
            else:
                return []
        return [self._agent_trace_span_from_row(row) for row in rows]

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
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO agent_worker_runs (
                    session_id, job_id, span_id, lane_id, worker_key, status, interrupt_requested,
                    budget_json, checkpoint_json, input_json, output_json, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id, lane_id, worker_key) DO UPDATE SET
                    session_id = excluded.session_id,
                    span_id = excluded.span_id,
                    status = CASE
                        WHEN agent_worker_runs.status = 'completed' THEN agent_worker_runs.status
                        WHEN agent_worker_runs.status = 'interrupted' THEN 'queued'
                        ELSE excluded.status
                    END,
                    budget_json = excluded.budget_json,
                    input_json = excluded.input_json,
                    metadata_json = excluded.metadata_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    session_id,
                    job_id,
                    span_id,
                    lane_id,
                    worker_key,
                    "queued",
                    0,
                    json.dumps(budget_payload or {}, ensure_ascii=False),
                    json.dumps({}, ensure_ascii=False),
                    json.dumps(input_payload or {}, ensure_ascii=False),
                    json.dumps({}, ensure_ascii=False),
                    json.dumps(metadata or {}, ensure_ascii=False),
                ),
            )
        worker = self.get_agent_worker(job_id=job_id, lane_id=lane_id, worker_key=worker_key)
        if worker is None:
            raise RuntimeError(f"Failed to create agent worker {job_id}:{lane_id}:{worker_key}")
        return worker

    def mark_agent_worker_running(self, worker_id: int) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native("mark_agent_worker_running", worker_id)
            return self._agent_worker_from_row(row) if row is not None else None
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE agent_worker_runs
                SET status = CASE WHEN status = 'completed' THEN status ELSE 'running' END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE worker_id = ?
                """,
                (worker_id,),
            )
        return self.get_agent_worker(worker_id=worker_id)

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
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE agent_worker_runs
                SET status = ?, checkpoint_json = ?, output_json = ?, updated_at = CURRENT_TIMESTAMP
                WHERE worker_id = ?
                """,
                (
                    status,
                    json.dumps(checkpoint_payload or {}, ensure_ascii=False),
                    json.dumps(output_payload or {}, ensure_ascii=False),
                    worker_id,
                ),
            )
        return self.get_agent_worker(worker_id=worker_id)

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
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE agent_worker_runs
                SET status = ?, checkpoint_json = ?, output_json = ?, lease_owner = NULL, lease_expires_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE worker_id = ?
                """,
                (
                    status,
                    json.dumps(checkpoint_payload or {}, ensure_ascii=False),
                    json.dumps(output_payload or {}, ensure_ascii=False),
                    worker_id,
                ),
            )
        return self.get_agent_worker(worker_id=worker_id)

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
        clauses = [
            "("
            "(status IN ('queued', 'interrupted', 'failed')) "
            "OR (status = 'running' AND json_extract(checkpoint_json, '$.stage') IN ('waiting_remote_search', 'waiting_remote_harvest')) "
            "OR (status = 'running' AND datetime(updated_at) <= datetime('now', ?))"
            ")",
            "(lease_expires_at IS NULL OR datetime(lease_expires_at) <= datetime('now'))",
        ]
        params: list[Any] = [f"-{max(1, int(stale_after_seconds or 300))} seconds"]
        if job_id:
            clauses.append("job_id = ?")
            params.append(job_id)
        if lane_id:
            clauses.append("lane_id = ?")
            params.append(lane_id)
        query = (
            "SELECT * FROM agent_worker_runs WHERE "
            + " AND ".join(clauses)
            + " ORDER BY updated_at ASC, worker_id ASC LIMIT ?"
        )
        params.append(max(1, int(limit or 100)))
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._agent_worker_from_row(row) for row in rows]

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
        normalized_worker_ids = [int(worker_id) for worker_id in list(worker_ids or []) if int(worker_id or 0) > 0]
        if not normalized_worker_ids:
            return []
        normalized_status = str(status or "cancelled").strip().lower() or "cancelled"
        normalized_reason = str(reason or "").strip()
        cleanup_metadata = dict(cleanup_metadata or {})
        immutable_terminal_statuses = {"completed", "cancelled", "canceled", "superseded"}

        refreshed_ids: list[int] = []
        with self._lock, self._connection:
            for worker_id in normalized_worker_ids:
                row = self._connection.execute(
                    "SELECT * FROM agent_worker_runs WHERE worker_id = ? LIMIT 1",
                    (worker_id,),
                ).fetchone()
                if row is None:
                    continue
                current = self._agent_worker_from_row(row)
                current_status = str(current.get("status") or "").strip().lower()
                if current_status in immutable_terminal_statuses:
                    refreshed_ids.append(worker_id)
                    continue

                metadata = dict(current.get("metadata") or {})
                cleanup_payload = dict(metadata.get("cleanup") or {})
                cleanup_payload.update(
                    {
                        "reason": normalized_reason,
                        "status": normalized_status,
                        "cleaned_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                cleanup_payload.update(cleanup_metadata)
                metadata["cleanup"] = cleanup_payload

                output = dict(current.get("output") or {})
                output["cleanup"] = cleanup_payload

                self._connection.execute(
                    """
                    UPDATE agent_worker_runs
                    SET status = ?,
                        output_json = ?,
                        metadata_json = ?,
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE worker_id = ?
                    """,
                    (
                        normalized_status,
                        json.dumps(output, ensure_ascii=False),
                        json.dumps(metadata, ensure_ascii=False),
                        worker_id,
                    ),
                )
                refreshed_ids.append(worker_id)

        return [
            worker for worker_id in refreshed_ids if (worker := self.get_agent_worker(worker_id=worker_id)) is not None
        ]

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
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE agent_worker_runs
                SET lease_owner = ?, lease_expires_at = datetime('now', ?), attempt_count = attempt_count + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE worker_id = ?
                  AND (lease_expires_at IS NULL OR datetime(lease_expires_at) <= datetime('now'))
                """,
                (
                    lease_owner,
                    f"+{max(1, int(lease_seconds or 60))} seconds",
                    worker_id,
                ),
            )
            changed = self._connection.execute("SELECT changes()").fetchone()[0]
        if not changed:
            return None
        return self.get_agent_worker(worker_id=worker_id)

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
        ttl_seconds = max(5, int(lease_seconds or 0))
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO workflow_job_leases (
                    job_id,
                    lease_owner,
                    lease_token,
                    lease_expires_at
                ) VALUES (?, ?, ?, datetime('now', ?))
                ON CONFLICT(job_id) DO UPDATE SET
                    lease_owner = excluded.lease_owner,
                    lease_token = excluded.lease_token,
                    lease_expires_at = excluded.lease_expires_at,
                    updated_at = CURRENT_TIMESTAMP
                WHERE datetime(workflow_job_leases.lease_expires_at) <= datetime('now')
                   OR workflow_job_leases.lease_owner = excluded.lease_owner
                   OR workflow_job_leases.lease_token = excluded.lease_token
                """,
                (
                    normalized_job_id,
                    normalized_owner,
                    normalized_token,
                    f"+{ttl_seconds} seconds",
                ),
            )
            changed = int(self._connection.execute("SELECT changes()").fetchone()[0] or 0)
            lease_row = self._connection.execute(
                """
                SELECT * FROM workflow_job_leases
                WHERE job_id = ?
                """,
                (normalized_job_id,),
            ).fetchone()
        payload = self._workflow_job_lease_from_row(lease_row)
        return {
            **payload,
            "acquired": bool(
                changed
                and payload
                and str(payload.get("lease_owner") or "") == normalized_owner
                and str(payload.get("lease_token") or "") == normalized_token
            ),
        }

    def get_workflow_job_lease(self, job_id: str) -> dict[str, Any] | None:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return None
        if self._control_plane_postgres_should_prefer_read("workflow_job_leases"):
            row = self._call_control_plane_postgres_native("get_workflow_job_lease", normalized_job_id)
            return self._workflow_job_lease_from_row(row) if row is not None else None
        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM workflow_job_leases
                WHERE job_id = ?
                """,
                (normalized_job_id,),
            ).fetchone()
        if row is None:
            return None
        return self._workflow_job_lease_from_row(row)

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
        clauses = ["job_id = ?", "lease_owner = ?"]
        params: list[Any] = [f"+{max(5, int(lease_seconds or 0))} seconds", normalized_job_id, normalized_owner]
        if normalized_token:
            clauses.append("lease_token = ?")
            params.append(normalized_token)
        with self._lock, self._connection:
            self._connection.execute(
                f"""
                UPDATE workflow_job_leases
                SET lease_expires_at = datetime('now', ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE {" AND ".join(clauses)}
                """,
                tuple(params),
            )
        return self.get_workflow_job_lease(normalized_job_id)

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
        with self._lock, self._connection:
            if normalized_owner and normalized_token:
                self._connection.execute(
                    """
                    DELETE FROM workflow_job_leases
                    WHERE job_id = ? AND lease_owner = ? AND lease_token = ?
                    """,
                    (normalized_job_id, normalized_owner, normalized_token),
                )
                return
            if normalized_owner:
                self._connection.execute(
                    """
                    DELETE FROM workflow_job_leases
                    WHERE job_id = ? AND lease_owner = ?
                    """,
                    (normalized_job_id, normalized_owner),
                )
                return
            if normalized_token:
                self._connection.execute(
                    """
                    DELETE FROM workflow_job_leases
                    WHERE job_id = ? AND lease_token = ?
                    """,
                    (normalized_job_id, normalized_token),
                )
                return
            self._connection.execute(
                """
                DELETE FROM workflow_job_leases
                WHERE job_id = ?
                """,
                (normalized_job_id,),
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
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE agent_worker_runs
                SET lease_expires_at = datetime('now', ?), updated_at = CURRENT_TIMESTAMP
                WHERE worker_id = ? AND lease_owner = ?
                """,
                (
                    f"+{max(1, int(lease_seconds or 60))} seconds",
                    worker_id,
                    lease_owner,
                ),
            )
        return self.get_agent_worker(worker_id=worker_id)

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
        with self._lock, self._connection:
            if lease_owner:
                self._connection.execute(
                    """
                    UPDATE agent_worker_runs
                    SET lease_owner = NULL, lease_expires_at = NULL,
                        last_error = CASE WHEN ? <> '' THEN ? ELSE last_error END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE worker_id = ? AND lease_owner = ?
                    """,
                    (error_text, error_text, worker_id, lease_owner),
                )
            else:
                self._connection.execute(
                    """
                    UPDATE agent_worker_runs
                    SET lease_owner = NULL, lease_expires_at = NULL,
                        last_error = CASE WHEN ? <> '' THEN ? ELSE last_error END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE worker_id = ?
                    """,
                    (error_text, error_text, worker_id),
                )
        return self.get_agent_worker(worker_id=worker_id)

    def request_interrupt_agent_worker(self, worker_id: int) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native("request_interrupt_agent_worker", worker_id)
            return self._agent_worker_from_row(row) if row is not None else None
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE agent_worker_runs
                SET interrupt_requested = 1, updated_at = CURRENT_TIMESTAMP
                WHERE worker_id = ?
                """,
                (worker_id,),
            )
        return self.get_agent_worker(worker_id=worker_id)

    def clear_interrupt_agent_worker(self, worker_id: int) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            row = self._call_control_plane_postgres_native("clear_interrupt_agent_worker", worker_id)
            return self._agent_worker_from_row(row) if row is not None else None
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE agent_worker_runs
                SET interrupt_requested = 0, updated_at = CURRENT_TIMESTAMP
                WHERE worker_id = ?
                """,
                (worker_id,),
            )
        return self.get_agent_worker(worker_id=worker_id)

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
        with self._lock:
            if worker_id > 0:
                row = self._connection.execute(
                    "SELECT * FROM agent_worker_runs WHERE worker_id = ? LIMIT 1",
                    (worker_id,),
                ).fetchone()
            elif job_id and lane_id and worker_key:
                row = self._connection.execute(
                    """
                    SELECT * FROM agent_worker_runs
                    WHERE job_id = ? AND lane_id = ? AND worker_key = ?
                    LIMIT 1
                    """,
                    (job_id, lane_id, worker_key),
                ).fetchone()
            else:
                return None
        if row is None:
            return None
        return self._agent_worker_from_row(row)

    def list_agent_workers(self, *, job_id: str = "", session_id: int = 0, lane_id: str = "") -> list[dict[str, Any]]:
        if self._control_plane_postgres_should_prefer_read("agent_worker_runs"):
            rows = self._call_control_plane_postgres_native(
                "list_agent_workers",
                job_id=job_id,
                session_id=session_id,
                lane_id=lane_id,
            )
            return [self._agent_worker_from_row(row) for row in list(rows or [])]
        clauses: list[str] = []
        params: list[Any] = []
        if job_id:
            clauses.append("job_id = ?")
            params.append(job_id)
        if session_id > 0:
            clauses.append("session_id = ?")
            params.append(session_id)
        if lane_id:
            clauses.append("lane_id = ?")
            params.append(lane_id)
        if not clauses:
            return []
        query = "SELECT * FROM agent_worker_runs WHERE " + " AND ".join(clauses) + " ORDER BY worker_id"
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._agent_worker_from_row(row) for row in rows]

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
        clauses = ["job_id = ?"]
        params: list[Any] = [job_id]
        if stage:
            clauses.append("stage = ?")
            params.append(stage)
        order = "DESC" if descending else "ASC"
        query = f"SELECT * FROM job_events WHERE {' AND '.join(clauses)} ORDER BY event_id {order}"
        normalized_limit = int(limit or 0)
        if normalized_limit > 0:
            query += " LIMIT ?"
            params.append(normalized_limit)
        rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._job_event_from_row(row) for row in rows]

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
            return []
        clauses: list[str] = []
        params: list[Any] = []
        if job_type:
            clauses.append("job_type = ?")
            params.append(job_type)
        normalized_statuses = [
            str(item or "").strip().lower() for item in list(statuses or []) if str(item or "").strip()
        ]
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            clauses.append(f"lower(status) IN ({placeholders})")
            params.extend(normalized_statuses)
        normalized_stages = [str(item or "").strip().lower() for item in list(stages or []) if str(item or "").strip()]
        if normalized_stages:
            placeholders = ",".join("?" for _ in normalized_stages)
            clauses.append(f"lower(stage) IN ({placeholders})")
            params.extend(normalized_stages)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = (
            f"SELECT * FROM jobs {where_clause} ORDER BY datetime(updated_at) DESC, datetime(created_at) DESC LIMIT ?"
        )
        params.append(max(1, int(limit or 100)))
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._job_from_row(row) for row in rows]

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
            return {
                "total": 0,
                "by_status": {},
                "by_stage": {},
                "by_status_stage": {},
            }
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            clauses.append(f"lower(status) IN ({placeholders})")
            params.extend(normalized_statuses)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = (
            "SELECT lower(status) AS status, lower(stage) AS stage, COUNT(*) AS count "
            f"FROM jobs {where_clause} "
            "GROUP BY lower(status), lower(stage)"
        )
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        by_status: dict[str, int] = {}
        by_stage: dict[str, int] = {}
        by_status_stage: dict[str, int] = {}
        total = 0
        for row in rows:
            status_value = str(row["status"] or "").strip()
            stage_value = str(row["stage"] or "").strip()
            count_value = int(row["count"] or 0)
            total += count_value
            if status_value:
                by_status[status_value] = int(by_status.get(status_value) or 0) + count_value
            if stage_value:
                by_stage[stage_value] = int(by_stage.get(stage_value) or 0) + count_value
            key = f"{status_value}:{stage_value}".strip(":")
            if key:
                by_status_stage[key] = int(by_status_stage.get(key) or 0) + count_value
        return {
            "total": total,
            "by_status": by_status,
            "by_stage": by_stage,
            "by_status_stage": by_status_stage,
        }

    def find_latest_completed_job(
        self,
        *,
        target_company: str,
        exclude_job_id: str = "",
        job_types: list[str] | None = None,
        limit: int = 200,
    ) -> dict[str, Any] | None:
        postgres_jobs = self._select_control_plane_job_rows(
            where_sql="status = %s",
            params=["completed"],
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(1, int(limit or 200)),
        )
        if postgres_jobs:
            rows = postgres_jobs
        elif self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
            return None
        else:
            rows = [
                self._job_from_row(row)
                for row in self._connection.execute(
                    """
                    SELECT * FROM jobs
                    WHERE status = 'completed'
                    ORDER BY updated_at DESC, created_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            ]
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
    ) -> dict[str, Any] | None:
        postgres_jobs = self._select_control_plane_job_rows(
            where_sql="status = %s",
            params=["completed"],
            order_by_sql="updated_at DESC, created_at DESC",
            limit=max(1, int(limit or 200)),
        )
        if postgres_jobs:
            rows = postgres_jobs
        elif self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
            return None
        else:
            rows = [
                self._job_from_row(row)
                for row in self._connection.execute(
                    """
                    SELECT * FROM jobs
                    WHERE status = 'completed'
                    ORDER BY updated_at DESC, created_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            ]
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
        params: list[Any] = [signature, signature]
        clauses = [
            "(matching_request_signature = ? OR (coalesce(matching_request_signature, '') = '' AND request_signature = ?))"
        ]
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)
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
        if postgres_rows:
            rows = postgres_rows
        elif self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
            return None
        else:
            rows = [
                self._job_from_row(row)
                for row in self._connection.execute(
                    f"""
                    SELECT * FROM jobs
                    WHERE {" AND ".join(clauses)}
                    ORDER BY updated_at DESC, created_at DESC
                    LIMIT ?
                    """,
                    (*params, max(1, int(limit or 200))),
                ).fetchall()
            ]
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
        params: list[Any] = [family_signature, family_signature]
        clauses = [
            "("
            "matching_request_family_signature = ? "
            "OR (coalesce(matching_request_family_signature, '') = '' AND request_family_signature = ?)"
            ")"
        ]
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)
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
        if postgres_rows:
            rows = postgres_rows
        elif self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
            return None
        else:
            rows = [
                self._job_from_row(row)
                for row in self._connection.execute(
                    f"""
                    SELECT * FROM jobs
                    WHERE {" AND ".join(clauses)}
                    ORDER BY updated_at DESC, created_at DESC
                    LIMIT ?
                    """,
                    (*params, max(1, int(limit or 200))),
                ).fetchall()
            ]
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
        params: list[Any] = [signature, signature]
        clauses = [
            "(matching_request_signature = ? OR (coalesce(matching_request_signature, '') = '' AND request_signature = ?))"
        ]
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)
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
        if postgres_rows:
            rows = postgres_rows
        elif self._control_plane_postgres_should_skip_sqlite_fallback("jobs"):
            return []
        else:
            rows = [
                self._job_from_row(row)
                for row in self._connection.execute(
                    f"""
                    SELECT * FROM jobs
                    WHERE {" AND ".join(clauses)}
                    ORDER BY updated_at DESC, created_at DESC
                    LIMIT ?
                    """,
                    (*params, max(1, int(limit or 200))),
                ).fetchall()
            ]
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
            with self._lock, self._connection:
                worker_cursor = self._connection.execute(
                    """
                    UPDATE agent_worker_runs
                    SET status = 'superseded',
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = ?
                      AND status IN ('queued', 'running', 'interrupted', 'failed')
                    """,
                    (str(job_id),),
                )
                superseded_worker_count = int(worker_cursor.rowcount or 0)
                trace_cursor = self._connection.execute(
                    """
                    UPDATE agent_trace_spans
                    SET status = CASE WHEN status = 'running' THEN 'superseded' ELSE status END
                    WHERE job_id = ?
                    """,
                    (str(job_id),),
                )
                superseded_trace_count = int(trace_cursor.rowcount or 0)
                self._connection.execute(
                    """
                    DELETE FROM workflow_job_leases
                    WHERE job_id = ?
                    """,
                    (str(job_id),),
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
        params: list[Any] = [normalized_idempotency_key]
        clauses = ["idempotency_key = ?"]
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)
        rows = self._connection.execute(
            f"""
            SELECT * FROM jobs
            WHERE {" AND ".join(clauses)}
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?
            """,
            (*params, max(1, int(limit or 200))),
        ).fetchall()
        scope_mode = _normalize_dispatch_scope(scope, requester_id=requester_id, tenant_id=tenant_id)
        normalized_target_company = str(target_company or "").strip().lower()
        for row in rows:
            payload = self._job_from_row(row)
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
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO query_dispatches (
                    target_company,
                    request_signature,
                    request_family_signature,
                    matching_request_signature,
                    matching_request_family_signature,
                    requester_id,
                    tenant_id,
                    idempotency_key,
                    strategy,
                    status,
                    source_job_id,
                    created_job_id,
                    matching_request_json,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_target_company,
                    request_sig,
                    request_family_sig,
                    str(matching_bundle.get("matching_request_signature") or ""),
                    str(matching_bundle.get("matching_request_family_signature") or ""),
                    requester_id_value,
                    tenant_id_value,
                    idempotency_key_value,
                    str(strategy or "").strip(),
                    str(status or "").strip(),
                    str(source_job_id or "").strip(),
                    str(created_job_id or "").strip(),
                    json.dumps(_json_safe_payload(matching_bundle), ensure_ascii=False),
                    json.dumps(_json_safe_payload(dispatch_payload), ensure_ascii=False),
                ),
            )
            dispatch_id = int(cursor.lastrowid)
            row = self._connection.execute(
                "SELECT * FROM query_dispatches WHERE dispatch_id = ? LIMIT 1",
                (dispatch_id,),
            ).fetchone()
        self._mirror_control_plane_row("query_dispatches", row)
        return self.get_query_dispatch(dispatch_id) or {}

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("query_dispatches"):
            return None
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM query_dispatches WHERE dispatch_id = ? LIMIT 1",
                (dispatch_id,),
            ).fetchone()
        if row is None:
            return None
        return self._query_dispatch_from_row(row)

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("query_dispatches"):
            return []
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT * FROM query_dispatches
                {where_clause}
                ORDER BY updated_at DESC, dispatch_id DESC
                LIMIT ?
                """,
                (*params, max(1, int(limit or 100))),
            ).fetchall()
        return [self._query_dispatch_from_row(row) for row in rows]

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
                "former_lane_effective_candidate_count": int(
                    payload.get("former_lane_effective_candidate_count") or 0
                ),
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
        company_lookup_clause, company_lookup_params = _company_identity_lookup_predicate(
            normalized_target_company,
            normalized_company_key,
        )
        with self._lock:
            existing_rows = self._connection.execute(
                """
                SELECT * FROM organization_asset_registry
                WHERE """
                + company_lookup_clause
                + """ AND asset_view = ?
                ORDER BY authoritative DESC, updated_at DESC, registry_id DESC
                """,
                (*company_lookup_params, asset_view),
            ).fetchall()
        existing_snapshot_row = next(
            (
                self._organization_asset_registry_from_row(row)
                for row in existing_rows
                if str(row["snapshot_id"] or "").strip() == snapshot_id
            ),
            {},
        )
        storage_target_company = _resolve_storage_target_company(
            requested_target_company=normalized_target_company,
            company_key=normalized_company_key,
            existing_rows=[self._organization_asset_registry_from_row(row) for row in existing_rows],
            id_column="registry_id",
        )
        if authoritative:
            with self._lock, self._connection:
                self._connection.execute(
                    """
                    UPDATE organization_asset_registry
                    SET authoritative = 0, updated_at = CURRENT_TIMESTAMP
                    WHERE """
                    + company_lookup_clause
                    + """ AND asset_view = ?
                    """,
                    (*company_lookup_params, asset_view),
                )
        with self._lock, self._connection:
            update_payload = (
                storage_target_company,
                normalized_company_key,
                snapshot_id,
                asset_view,
                _normalized_payload_text(payload, "status", default="ready") or "ready",
                1
                if authoritative or bool(payload.get("authoritative")) or bool(existing_snapshot_row.get("authoritative"))
                else 0,
                int(payload.get("candidate_count") or 0),
                int(payload.get("evidence_count") or 0),
                int(payload.get("profile_detail_count") or 0),
                int(payload.get("explicit_profile_capture_count") or 0),
                int(payload.get("missing_linkedin_count") or 0),
                int(payload.get("manual_review_backlog_count") or 0),
                int(payload.get("profile_completion_backlog_count") or 0),
                int(payload.get("source_snapshot_count") or 0),
                float(payload.get("completeness_score") or 0.0),
                _normalized_payload_text(payload, "completeness_band", default="low") or "low",
                json.dumps(_json_safe_payload(payload.get("current_lane_coverage") or {}), ensure_ascii=False),
                json.dumps(_json_safe_payload(payload.get("former_lane_coverage") or {}), ensure_ascii=False),
                int(payload.get("current_lane_effective_candidate_count") or 0),
                int(payload.get("former_lane_effective_candidate_count") or 0),
                1 if bool(payload.get("current_lane_effective_ready")) else 0,
                1 if bool(payload.get("former_lane_effective_ready")) else 0,
                json.dumps(_json_safe_payload(payload.get("source_snapshot_selection") or {}), ensure_ascii=False),
                json.dumps(_json_safe_payload(payload.get("selected_snapshot_ids") or []), ensure_ascii=False),
                _normalized_payload_text(payload, "source_path"),
                _normalized_payload_text(payload, "source_job_id"),
                _normalized_payload_text(payload, "materialization_generation_key"),
                int(payload.get("materialization_generation_sequence") or 0),
                _normalized_payload_text(payload, "materialization_watermark"),
                json.dumps(_json_safe_payload(payload.get("summary") or {}), ensure_ascii=False),
            )
            if existing_snapshot_row and str(existing_snapshot_row.get("target_company") or "").strip() != storage_target_company:
                self._connection.execute(
                    """
                    UPDATE organization_asset_registry
                    SET target_company = ?, company_key = ?, snapshot_id = ?, asset_view = ?, status = ?, authoritative = ?,
                        candidate_count = ?, evidence_count = ?, profile_detail_count = ?, explicit_profile_capture_count = ?,
                        missing_linkedin_count = ?, manual_review_backlog_count = ?, profile_completion_backlog_count = ?,
                        source_snapshot_count = ?, completeness_score = ?, completeness_band = ?,
                        current_lane_coverage_json = ?, former_lane_coverage_json = ?,
                        current_lane_effective_candidate_count = ?, former_lane_effective_candidate_count = ?,
                        current_lane_effective_ready = ?, former_lane_effective_ready = ?,
                        source_snapshot_selection_json = ?, selected_snapshot_ids_json = ?,
                        source_path = ?, source_job_id = ?,
                        materialization_generation_key = ?, materialization_generation_sequence = ?, materialization_watermark = ?,
                        summary_json = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE registry_id = ?
                    """,
                    (*update_payload, int(existing_snapshot_row.get("registry_id") or 0)),
                )
            else:
                self._connection.execute(
                    """
                    INSERT INTO organization_asset_registry (
                        target_company, company_key, snapshot_id, asset_view, status, authoritative,
                        candidate_count, evidence_count, profile_detail_count, explicit_profile_capture_count,
                        missing_linkedin_count, manual_review_backlog_count, profile_completion_backlog_count,
                        source_snapshot_count, completeness_score, completeness_band,
                        current_lane_coverage_json, former_lane_coverage_json,
                        current_lane_effective_candidate_count, former_lane_effective_candidate_count,
                        current_lane_effective_ready, former_lane_effective_ready,
                        source_snapshot_selection_json, selected_snapshot_ids_json,
                        source_path, source_job_id,
                        materialization_generation_key, materialization_generation_sequence, materialization_watermark,
                        summary_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(target_company, snapshot_id, asset_view) DO UPDATE SET
                        company_key = excluded.company_key,
                        status = excluded.status,
                        authoritative = CASE
                            WHEN excluded.authoritative = 1 THEN 1
                            ELSE organization_asset_registry.authoritative
                        END,
                        candidate_count = excluded.candidate_count,
                        evidence_count = excluded.evidence_count,
                        profile_detail_count = excluded.profile_detail_count,
                        explicit_profile_capture_count = excluded.explicit_profile_capture_count,
                        missing_linkedin_count = excluded.missing_linkedin_count,
                        manual_review_backlog_count = excluded.manual_review_backlog_count,
                        profile_completion_backlog_count = excluded.profile_completion_backlog_count,
                        source_snapshot_count = excluded.source_snapshot_count,
                        completeness_score = excluded.completeness_score,
                        completeness_band = excluded.completeness_band,
                        current_lane_coverage_json = excluded.current_lane_coverage_json,
                        former_lane_coverage_json = excluded.former_lane_coverage_json,
                        current_lane_effective_candidate_count = excluded.current_lane_effective_candidate_count,
                        former_lane_effective_candidate_count = excluded.former_lane_effective_candidate_count,
                        current_lane_effective_ready = excluded.current_lane_effective_ready,
                        former_lane_effective_ready = excluded.former_lane_effective_ready,
                        source_snapshot_selection_json = excluded.source_snapshot_selection_json,
                        selected_snapshot_ids_json = excluded.selected_snapshot_ids_json,
                        source_path = excluded.source_path,
                        source_job_id = excluded.source_job_id,
                        materialization_generation_key = excluded.materialization_generation_key,
                        materialization_generation_sequence = excluded.materialization_generation_sequence,
                        materialization_watermark = excluded.materialization_watermark,
                        summary_json = excluded.summary_json,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    update_payload,
                )
            row = self._connection.execute(
                """
                SELECT * FROM organization_asset_registry
                WHERE """
                + company_lookup_clause
                + """ AND snapshot_id = ? AND asset_view = ?
                LIMIT 1
                """,
                (*company_lookup_params, snapshot_id, asset_view),
            ).fetchone()
            companion_rows = self._connection.execute(
                """
                SELECT * FROM organization_asset_registry
                WHERE """
                + company_lookup_clause
                + """ AND asset_view = ?
                """,
                (*company_lookup_params, asset_view),
            ).fetchall()
        for companion_row in companion_rows:
            self._mirror_control_plane_row("organization_asset_registry", companion_row)
        return self._organization_asset_registry_from_row(row)

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
                "former_lane_effective_candidate_count": int(
                    payload.get("former_lane_effective_candidate_count") or 0
                ),
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
        company_lookup_clause, company_lookup_params = _company_identity_lookup_predicate(
            normalized_target_company,
            normalized_company_key,
        )
        with self._lock:
            existing_rows = self._connection.execute(
                """
                SELECT * FROM organization_execution_profiles
                WHERE """
                + company_lookup_clause
                + """ AND asset_view = ?
                ORDER BY updated_at DESC, profile_id DESC
                """,
                (*company_lookup_params, asset_view),
            ).fetchall()
        existing_profile_row = next(
            (self._organization_execution_profile_from_row(row) for row in existing_rows),
            {},
        )
        storage_target_company = _resolve_storage_target_company(
            requested_target_company=normalized_target_company,
            company_key=normalized_company_key,
            existing_rows=[self._organization_execution_profile_from_row(row) for row in existing_rows],
            id_column="profile_id",
        )
        with self._lock, self._connection:
            update_payload = (
                storage_target_company,
                normalized_company_key,
                asset_view,
                int(payload.get("source_registry_id") or 0),
                _normalized_payload_text(payload, "source_snapshot_id"),
                _normalized_payload_text(payload, "source_job_id"),
                _normalized_payload_text(payload, "source_generation_key"),
                int(payload.get("source_generation_sequence") or 0),
                _normalized_payload_text(payload, "source_generation_watermark"),
                _normalized_payload_text(payload, "status", default="ready") or "ready",
                _normalized_payload_text(payload, "org_scale_band", default="unknown") or "unknown",
                _normalized_payload_text(payload, "default_acquisition_mode", default="full_company_roster")
                or "full_company_roster",
                1 if bool(payload.get("prefer_delta_from_baseline")) else 0,
                _normalized_payload_text(payload, "current_lane_default", default="live_acquisition")
                or "live_acquisition",
                _normalized_payload_text(payload, "former_lane_default", default="live_acquisition")
                or "live_acquisition",
                int(payload.get("baseline_candidate_count") or 0),
                int(payload.get("current_lane_effective_candidate_count") or 0),
                int(payload.get("former_lane_effective_candidate_count") or 0),
                float(payload.get("completeness_score") or 0.0),
                _normalized_payload_text(payload, "completeness_band", default="low") or "low",
                float(payload.get("profile_detail_ratio") or 0.0),
                int(payload.get("company_employee_shard_count") or 0),
                int(payload.get("current_profile_search_shard_count") or 0),
                int(payload.get("former_profile_search_shard_count") or 0),
                int(payload.get("company_employee_cap_hit_count") or 0),
                int(payload.get("profile_search_cap_hit_count") or 0),
                json.dumps(_json_safe_payload(payload.get("reason_codes") or []), ensure_ascii=False),
                json.dumps(_json_safe_payload(payload.get("explanation") or {}), ensure_ascii=False),
                json.dumps(_json_safe_payload(payload.get("summary") or {}), ensure_ascii=False),
            )
            if existing_profile_row and str(existing_profile_row.get("target_company") or "").strip() != storage_target_company:
                self._connection.execute(
                    """
                    UPDATE organization_execution_profiles
                    SET target_company = ?, company_key = ?, asset_view = ?, source_registry_id = ?,
                        source_snapshot_id = ?, source_job_id = ?, source_generation_key = ?,
                        source_generation_sequence = ?, source_generation_watermark = ?,
                        status = ?, org_scale_band = ?, default_acquisition_mode = ?, prefer_delta_from_baseline = ?,
                        current_lane_default = ?, former_lane_default = ?,
                        baseline_candidate_count = ?, current_lane_effective_candidate_count = ?,
                        former_lane_effective_candidate_count = ?, completeness_score = ?, completeness_band = ?,
                        profile_detail_ratio = ?, company_employee_shard_count = ?, current_profile_search_shard_count = ?,
                        former_profile_search_shard_count = ?, company_employee_cap_hit_count = ?,
                        profile_search_cap_hit_count = ?, reason_codes_json = ?, explanation_json = ?,
                        summary_json = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE profile_id = ?
                    """,
                    (*update_payload, int(existing_profile_row.get("profile_id") or 0)),
                )
            else:
                self._connection.execute(
                    """
                    INSERT INTO organization_execution_profiles (
                        target_company, company_key, asset_view, source_registry_id, source_snapshot_id, source_job_id,
                        source_generation_key, source_generation_sequence, source_generation_watermark,
                        status, org_scale_band, default_acquisition_mode, prefer_delta_from_baseline,
                        current_lane_default, former_lane_default,
                        baseline_candidate_count, current_lane_effective_candidate_count, former_lane_effective_candidate_count,
                        completeness_score, completeness_band, profile_detail_ratio,
                        company_employee_shard_count, current_profile_search_shard_count, former_profile_search_shard_count,
                        company_employee_cap_hit_count, profile_search_cap_hit_count,
                        reason_codes_json, explanation_json, summary_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(target_company, asset_view) DO UPDATE SET
                        company_key = excluded.company_key,
                        source_registry_id = excluded.source_registry_id,
                        source_snapshot_id = excluded.source_snapshot_id,
                        source_job_id = excluded.source_job_id,
                        source_generation_key = excluded.source_generation_key,
                        source_generation_sequence = excluded.source_generation_sequence,
                        source_generation_watermark = excluded.source_generation_watermark,
                        status = excluded.status,
                        org_scale_band = excluded.org_scale_band,
                        default_acquisition_mode = excluded.default_acquisition_mode,
                        prefer_delta_from_baseline = excluded.prefer_delta_from_baseline,
                        current_lane_default = excluded.current_lane_default,
                        former_lane_default = excluded.former_lane_default,
                        baseline_candidate_count = excluded.baseline_candidate_count,
                        current_lane_effective_candidate_count = excluded.current_lane_effective_candidate_count,
                        former_lane_effective_candidate_count = excluded.former_lane_effective_candidate_count,
                        completeness_score = excluded.completeness_score,
                        completeness_band = excluded.completeness_band,
                        profile_detail_ratio = excluded.profile_detail_ratio,
                        company_employee_shard_count = excluded.company_employee_shard_count,
                        current_profile_search_shard_count = excluded.current_profile_search_shard_count,
                        former_profile_search_shard_count = excluded.former_profile_search_shard_count,
                        company_employee_cap_hit_count = excluded.company_employee_cap_hit_count,
                        profile_search_cap_hit_count = excluded.profile_search_cap_hit_count,
                        reason_codes_json = excluded.reason_codes_json,
                        explanation_json = excluded.explanation_json,
                        summary_json = excluded.summary_json,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    update_payload,
                )
            row = self._connection.execute(
                """
                SELECT * FROM organization_execution_profiles
                WHERE """
                + company_lookup_clause
                + """ AND asset_view = ?
                ORDER BY updated_at DESC, profile_id DESC
                LIMIT 1
                """,
                (*company_lookup_params, asset_view),
            ).fetchone()
        self._mirror_control_plane_row("organization_execution_profiles", row)
        return self._organization_execution_profile_from_row(row)

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
        company_lookup_clause, company_lookup_params = _company_identity_lookup_predicate(
            normalized_target_company,
            normalized_company_key,
        )
        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM organization_execution_profiles
                WHERE """
                + company_lookup_clause
                + """ AND asset_view = ?
                ORDER BY updated_at DESC, profile_id DESC
                LIMIT 1
                """,
                (
                    *company_lookup_params,
                    str(asset_view or "canonical_merged").strip() or "canonical_merged",
                ),
            ).fetchone()
        return self._organization_execution_profile_from_row(row)

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
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
            company_lookup_clause, company_lookup_params = _company_identity_lookup_predicate(
                normalized_target_company,
                normalized_company_key,
            )
            clauses.append(company_lookup_clause)
            params.extend(company_lookup_params)
        if asset_view:
            clauses.append("asset_view = ?")
            params.append(asset_view)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT * FROM organization_execution_profiles
                {where_clause}
                ORDER BY updated_at DESC, profile_id DESC
                LIMIT ?
                """,
                (*params, max(1, int(limit or 100))),
            ).fetchall()
        return [self._organization_execution_profile_from_row(row) for row in rows]

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
        company_lookup_clause, company_lookup_params = _company_identity_lookup_predicate(
            normalized_target_company,
            normalized_company_key,
        )
        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM organization_asset_registry
                WHERE """
                + company_lookup_clause
                + """ AND asset_view = ?
                ORDER BY authoritative DESC, updated_at DESC, registry_id DESC
                LIMIT 1
                """,
                (
                    *company_lookup_params,
                    str(asset_view or "canonical_merged").strip() or "canonical_merged",
                ),
            ).fetchone()
        return self._organization_asset_registry_from_row(row)

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
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            normalized_target_company, normalized_company_key = _normalized_company_scope(target_company)
            company_lookup_clause, company_lookup_params = _company_identity_lookup_predicate(
                normalized_target_company,
                normalized_company_key,
            )
            clauses.append(company_lookup_clause)
            params.extend(company_lookup_params)
        if asset_view:
            clauses.append("asset_view = ?")
            params.append(asset_view)
        if authoritative_only:
            clauses.append("authoritative = 1")
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT * FROM organization_asset_registry
                {where_clause}
                ORDER BY authoritative DESC, updated_at DESC, registry_id DESC
                LIMIT ?
                """,
                (*params, max(1, int(limit or 100))),
            ).fetchall()
        return [self._organization_asset_registry_from_row(row) for row in rows]

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

        def _sqlite_where_clause() -> tuple[str, list[Any]]:
            clauses = ["(lower(target_company) = lower(?)"]
            params: list[Any] = [normalized_target_company]
            if normalized_company_key:
                clauses.append(" OR company_key = ?")
                params.append(normalized_company_key)
            clauses.append(")")
            if normalized_asset_view:
                clauses.append("AND asset_view = ?")
                params.append(normalized_asset_view)
            return " ".join(clauses), params

        def _row_sort_key(row: sqlite3.Row | dict[str, Any]) -> tuple[int, int, str, int]:
            return (
                1 if bool(row["authoritative"]) else 0,
                int(row["candidate_count"] or 0),
                str(row["updated_at"] or ""),
                int(row["registry_id"] or 0),
            )

        def _apply_registry_canonicalization(rows: list[sqlite3.Row | dict[str, Any]], *, use_postgres: bool) -> dict[str, Any]:
            updated_rows = 0
            deleted_rows = 0
            merged_groups = 0
            grouped: dict[tuple[str, str], list[sqlite3.Row | dict[str, Any]]] = {}
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
                keeper_needs_update = (
                    str(keeper["target_company"] or "") != normalized_target_company
                    or authoritative != bool(keeper["authoritative"])
                )
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
                    else:
                        self._connection.execute(
                            """
                            UPDATE organization_asset_registry
                            SET target_company = ?, authoritative = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE registry_id = ?
                            """,
                            (normalized_target_company, 1 if authoritative else 0, keeper_id),
                        )
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
                    else:
                        placeholders = ",".join("?" for _ in extra_ids)
                        self._connection.execute(
                            f"DELETE FROM organization_asset_registry WHERE registry_id IN ({placeholders})",
                            extra_ids,
                        )
                        deleted_rows += len(extra_ids)
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
                else:
                    authoritative_row = self._connection.execute(
                        """
                        SELECT registry_id FROM organization_asset_registry
                        WHERE lower(target_company) = lower(?) AND asset_view = ? AND authoritative = 1
                        ORDER BY updated_at DESC, registry_id DESC
                        LIMIT 1
                        """,
                        (normalized_target_company, normalized_asset_view),
                    ).fetchone()
                    authoritative_id = int(authoritative_row["registry_id"]) if authoritative_row else 0
                    if authoritative_id:
                        self._connection.execute(
                            """
                            UPDATE organization_asset_registry
                            SET authoritative = CASE WHEN registry_id = ? THEN 1 ELSE 0 END,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE lower(target_company) = lower(?) AND asset_view = ?
                            """,
                            (authoritative_id, normalized_target_company, normalized_asset_view),
                        )
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

        sqlite_where_clause, sqlite_params = _sqlite_where_clause()
        with self._lock, self._connection:
            rows = self._connection.execute(
                f"""
                SELECT * FROM organization_asset_registry
                WHERE {sqlite_where_clause}
                ORDER BY snapshot_id, asset_view, registry_id DESC
                """,
                sqlite_params,
            ).fetchall()
            result = _apply_registry_canonicalization(list(rows), use_postgres=False)
        self._replace_control_plane_table_from_sqlite("organization_asset_registry")
        return result

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
        if not existing:
            with self._lock:
                existing_row = self._connection.execute(
                    """
                    SELECT * FROM acquisition_shard_registry
                    WHERE shard_key = ?
                    LIMIT 1
                    """,
                    (shard_key,),
                ).fetchone()
            existing = self._acquisition_shard_registry_from_row(existing_row)
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
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO acquisition_shard_registry (
                    shard_key, target_company, company_key, snapshot_id, asset_view, lane, status, employment_scope,
                    strategy_type, shard_id, shard_title, search_query, query_signature,
                    company_scope_json, locations_json, function_ids_json,
                    result_count, estimated_total_count, provider_cap_hit,
                    source_path, source_job_id,
                    materialization_generation_key, materialization_generation_sequence, materialization_watermark,
                    metadata_json, first_seen_at, last_completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?)
                ON CONFLICT(shard_key) DO UPDATE SET
                    target_company = excluded.target_company,
                    company_key = excluded.company_key,
                    snapshot_id = excluded.snapshot_id,
                    asset_view = excluded.asset_view,
                    lane = excluded.lane,
                    status = excluded.status,
                    employment_scope = excluded.employment_scope,
                    strategy_type = excluded.strategy_type,
                    shard_id = excluded.shard_id,
                    shard_title = excluded.shard_title,
                    search_query = excluded.search_query,
                    query_signature = excluded.query_signature,
                    company_scope_json = excluded.company_scope_json,
                    locations_json = excluded.locations_json,
                    function_ids_json = excluded.function_ids_json,
                    result_count = excluded.result_count,
                    estimated_total_count = excluded.estimated_total_count,
                    provider_cap_hit = excluded.provider_cap_hit,
                    source_path = excluded.source_path,
                    source_job_id = excluded.source_job_id,
                    materialization_generation_key = excluded.materialization_generation_key,
                    materialization_generation_sequence = excluded.materialization_generation_sequence,
                    materialization_watermark = excluded.materialization_watermark,
                    metadata_json = excluded.metadata_json,
                    last_completed_at = CASE
                        WHEN excluded.last_completed_at <> '' THEN excluded.last_completed_at
                        ELSE acquisition_shard_registry.last_completed_at
                    END,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    shard_key,
                    target_company,
                    _normalized_payload_text(payload, "company_key"),
                    snapshot_id,
                    _normalized_payload_text(payload, "asset_view", default="canonical_merged") or "canonical_merged",
                    _normalized_payload_text(payload, "lane"),
                    status,
                    _normalized_payload_text(payload, "employment_scope", default="all") or "all",
                    _normalized_payload_text(payload, "strategy_type"),
                    _normalized_payload_text(payload, "shard_id"),
                    _normalized_payload_text(payload, "shard_title"),
                    _normalized_payload_text(payload, "search_query"),
                    _normalized_payload_text(payload, "query_signature"),
                    json.dumps(_json_safe_payload(payload.get("company_scope") or []), ensure_ascii=False),
                    json.dumps(_json_safe_payload(payload.get("locations") or []), ensure_ascii=False),
                    json.dumps(_json_safe_payload(payload.get("function_ids") or []), ensure_ascii=False),
                    int(payload.get("result_count") or 0),
                    int(payload.get("estimated_total_count") or 0),
                    1 if bool(payload.get("provider_cap_hit")) else 0,
                    _normalized_payload_text(payload, "source_path"),
                    _normalized_payload_text(payload, "source_job_id"),
                    _normalized_payload_text(payload, "materialization_generation_key"),
                    int(payload.get("materialization_generation_sequence") or 0),
                    _normalized_payload_text(payload, "materialization_watermark"),
                    json.dumps(_json_safe_payload(payload.get("metadata") or {}), ensure_ascii=False),
                    _normalized_payload_text(payload, "first_seen_at"),
                    completed_at,
                ),
            )
            row = self._connection.execute(
                """
                SELECT * FROM acquisition_shard_registry
                WHERE shard_key = ?
                LIMIT 1
                """,
                (shard_key,),
            ).fetchone()
        self._mirror_control_plane_row("acquisition_shard_registry", row)
        return self._acquisition_shard_registry_from_row(row)

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
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            clauses.append("lower(target_company) = lower(?)")
            params.append(target_company)
        normalized_snapshot_ids = [str(item).strip() for item in list(snapshot_ids or []) if str(item).strip()]
        if normalized_snapshot_ids:
            placeholders = ",".join("?" for _ in normalized_snapshot_ids)
            clauses.append(f"snapshot_id IN ({placeholders})")
            params.extend(normalized_snapshot_ids)
        if lane:
            clauses.append("lane = ?")
            params.append(lane)
        if employment_scope:
            clauses.append("employment_scope = ?")
            params.append(employment_scope)
        normalized_statuses = [str(item).strip() for item in list(statuses or []) if str(item).strip()]
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT * FROM acquisition_shard_registry
                {where_clause}
                ORDER BY updated_at DESC, shard_key DESC
                LIMIT ?
                """,
                (*params, max(1, int(limit or 1000))),
            ).fetchall()
        return [self._acquisition_shard_registry_from_row(row) for row in rows]

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
            "generation_key": str(row["generation_key"] if "generation_key" in row_keys else dict(row).get("generation_key") or ""),
            "target_company": str(row["target_company"] if "target_company" in row_keys else dict(row).get("target_company") or ""),
            "snapshot_id": str(row["snapshot_id"] if "snapshot_id" in row_keys else dict(row).get("snapshot_id") or ""),
            "asset_view": str(row["asset_view"] if "asset_view" in row_keys else dict(row).get("asset_view") or ""),
            "artifact_kind": str(row["artifact_kind"] if "artifact_kind" in row_keys else dict(row).get("artifact_kind") or ""),
            "artifact_key": str(row["artifact_key"] if "artifact_key" in row_keys else dict(row).get("artifact_key") or ""),
            "lane": str(row["lane"] if "lane" in row_keys else dict(row).get("lane") or ""),
            "employment_scope": str(
                row["employment_scope"] if "employment_scope" in row_keys else dict(row).get("employment_scope") or ""
            ),
            "member_key": str(row["member_key"] if "member_key" in row_keys else dict(row).get("member_key") or ""),
            "member_key_kind": str(
                row["member_key_kind"] if "member_key_kind" in row_keys else dict(row).get("member_key_kind") or ""
            ),
            "candidate_id": str(row["candidate_id"] if "candidate_id" in row_keys else dict(row).get("candidate_id") or ""),
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
        if self._control_plane_postgres_should_skip_sqlite_fallback("asset_membership_index"):
            return []
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT *
                FROM asset_membership_index
                WHERE generation_key = ?
                ORDER BY member_key ASC
                """,
                (normalized_generation_key,),
            ).fetchall()
        return [self._asset_membership_index_from_row(row) for row in rows]

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("asset_materialization_generations"):
            return {}
        with self._lock:
            row = self._connection.execute(
                """
                SELECT *
                FROM asset_materialization_generations
                WHERE generation_key = ?
                LIMIT 1
                """,
                (normalized_generation_key,),
            ).fetchone()
        return self._asset_materialization_generation_from_row(row)

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
            self._call_control_plane_postgres_native("bulk_upsert_rows", "asset_membership_index", payload_rows)

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
        with self._lock, self._connection:
            company_scope_clause, company_scope_params = _company_scope_predicate(
                normalized_target_company,
                normalized_company_key,
            )
            existing = self._connection.execute(
                f"""
                SELECT target_company, generation_key, generation_sequence
                FROM asset_materialization_generations
                WHERE {company_scope_clause}
                  AND snapshot_id = ?
                  AND asset_view = ?
                  AND artifact_kind = ?
                  AND artifact_key = ?
                LIMIT 1
                """,
                (
                    *company_scope_params,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_artifact_kind,
                    normalized_artifact_key,
                ),
            ).fetchone()
            if existing is not None:
                normalized_target_company = str(existing["target_company"] or normalized_target_company).strip()
            previous_generation_key = str(existing["generation_key"] or "").strip() if existing is not None else ""
            previous_sequence = int(existing["generation_sequence"] or 0) if existing is not None else 0
            generation_sequence = (
                previous_sequence if previous_generation_key == generation_key else previous_sequence + 1
            )
            if generation_sequence <= 0:
                generation_sequence = 1
            self._connection.execute(
                """
                INSERT INTO asset_materialization_generations (
                    target_company, company_key, snapshot_id, asset_view, artifact_kind, artifact_key,
                    generation_key, generation_sequence, source_path,
                    payload_signature, member_signature, member_count, summary_json, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(target_company, snapshot_id, asset_view, artifact_kind, artifact_key) DO UPDATE SET
                    company_key = excluded.company_key,
                    generation_key = excluded.generation_key,
                    generation_sequence = excluded.generation_sequence,
                    source_path = excluded.source_path,
                    payload_signature = excluded.payload_signature,
                    member_signature = excluded.member_signature,
                    member_count = excluded.member_count,
                    summary_json = excluded.summary_json,
                    metadata_json = excluded.metadata_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    normalized_target_company,
                    normalized_company_key,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_artifact_kind,
                    normalized_artifact_key,
                    generation_key,
                    generation_sequence,
                    str(source_path or "").strip(),
                    payload_signature,
                    member_signature,
                    len(normalized_members),
                    json.dumps(summary_payload, ensure_ascii=False),
                    json.dumps(metadata_payload, ensure_ascii=False),
                ),
            )
            if previous_generation_key and previous_generation_key != generation_key:
                self._connection.execute(
                    "DELETE FROM asset_membership_index WHERE generation_key = ?",
                    (previous_generation_key,),
                )
            self._connection.execute(
                "DELETE FROM asset_membership_index WHERE generation_key = ?",
                (generation_key,),
            )
            if normalized_members:
                self._connection.executemany(
                    """
                    INSERT INTO asset_membership_index (
                        generation_key, target_company, snapshot_id, asset_view, artifact_kind, artifact_key,
                        lane, employment_scope, member_key, member_key_kind, candidate_id, profile_url_key, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            generation_key,
                            normalized_target_company,
                            normalized_snapshot_id,
                            normalized_asset_view,
                            normalized_artifact_kind,
                            normalized_artifact_key,
                            str(item.get("lane") or "").strip(),
                            _normalize_employment_scope(item.get("employment_scope")),
                            str(item.get("member_key") or "").strip(),
                            str(item.get("member_key_kind") or "candidate_fallback").strip() or "candidate_fallback",
                            str(item.get("candidate_id") or "").strip(),
                            str(item.get("profile_url_key") or "").strip(),
                            json.dumps(_json_safe_payload(item.get("metadata") or {}), ensure_ascii=False),
                        )
                        for item in normalized_members
                    ],
                )
            row = self._connection.execute(
                """
                SELECT *
                FROM asset_materialization_generations
                WHERE target_company = ? AND snapshot_id = ? AND asset_view = ? AND artifact_kind = ? AND artifact_key = ?
                LIMIT 1
                """,
                (
                    normalized_target_company,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_artifact_kind,
                    normalized_artifact_key,
                ),
            ).fetchone()
        return self._asset_materialization_generation_from_row(row)

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

        with self._lock, self._connection:
            company_scope_clause, company_scope_params = _company_scope_predicate(
                normalized_target_company,
                normalized_company_key,
            )
            existing = self._connection.execute(
                f"""
                SELECT target_company, generation_key, generation_sequence
                FROM asset_materialization_generations
                WHERE {company_scope_clause}
                  AND snapshot_id = ?
                  AND asset_view = ?
                  AND artifact_kind = ?
                  AND artifact_key = ?
                LIMIT 1
                """,
                (
                    *company_scope_params,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_artifact_kind,
                    normalized_artifact_key,
                ),
            ).fetchone()
            if existing is not None:
                normalized_target_company = str(existing["target_company"] or normalized_target_company).strip()
            previous_generation_key = str(existing["generation_key"] or "").strip() if existing is not None else ""
            previous_sequence = int(existing["generation_sequence"] or 0) if existing is not None else 0
            generation_sequence = (
                previous_sequence if previous_generation_key == generation_key else previous_sequence + 1
            )
            if generation_sequence <= 0:
                generation_sequence = 1
            self._connection.execute(
                """
                INSERT INTO asset_materialization_generations (
                    target_company, company_key, snapshot_id, asset_view, artifact_kind, artifact_key,
                    generation_key, generation_sequence, source_path,
                    payload_signature, member_signature, member_count, summary_json, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(target_company, snapshot_id, asset_view, artifact_kind, artifact_key) DO UPDATE SET
                    company_key = excluded.company_key,
                    generation_key = excluded.generation_key,
                    generation_sequence = excluded.generation_sequence,
                    source_path = excluded.source_path,
                    payload_signature = excluded.payload_signature,
                    member_signature = excluded.member_signature,
                    member_count = excluded.member_count,
                    summary_json = excluded.summary_json,
                    metadata_json = excluded.metadata_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    normalized_target_company,
                    normalized_company_key,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_artifact_kind,
                    normalized_artifact_key,
                    generation_key,
                    generation_sequence,
                    str(source_path or "").strip(),
                    payload_signature,
                    patch_signature,
                    len(effective_rows),
                    json.dumps(summary_payload, ensure_ascii=False),
                    json.dumps(metadata_payload, ensure_ascii=False),
                ),
            )
            if (
                previous_generation_key
                and previous_generation_key != generation_key
                and previous_generation_key != normalized_base_generation_key
            ):
                self._connection.execute(
                    "DELETE FROM asset_membership_index WHERE generation_key = ?",
                    (previous_generation_key,),
                )
            self._connection.execute(
                "DELETE FROM asset_membership_index WHERE generation_key = ?",
                (generation_key,),
            )
            if normalized_members:
                self._connection.executemany(
                    """
                    INSERT INTO asset_membership_index (
                        generation_key, target_company, snapshot_id, asset_view, artifact_kind, artifact_key,
                        lane, employment_scope, member_key, member_key_kind, candidate_id, profile_url_key, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            generation_key,
                            normalized_target_company,
                            normalized_snapshot_id,
                            normalized_asset_view,
                            normalized_artifact_kind,
                            normalized_artifact_key,
                            str(item.get("lane") or "").strip(),
                            _normalize_employment_scope(item.get("employment_scope")),
                            str(item.get("member_key") or "").strip(),
                            str(item.get("member_key_kind") or "candidate_fallback").strip() or "candidate_fallback",
                            str(item.get("candidate_id") or "").strip(),
                            str(item.get("profile_url_key") or "").strip(),
                            json.dumps(_json_safe_payload(item.get("metadata") or {}), ensure_ascii=False),
                        )
                        for item in normalized_members
                    ],
                )
            row = self._connection.execute(
                """
                SELECT *
                FROM asset_materialization_generations
                WHERE target_company = ? AND snapshot_id = ? AND asset_view = ? AND artifact_kind = ? AND artifact_key = ?
                LIMIT 1
                """,
                (
                    normalized_target_company,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_artifact_kind,
                    normalized_artifact_key,
                ),
            ).fetchone()
        return self._asset_materialization_generation_from_row(row)

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("asset_materialization_generations"):
            return {}
        with self._lock:
            company_scope_clause, company_scope_params = _company_scope_predicate(
                normalized_target_company,
                normalized_company_key,
            )
            row = self._connection.execute(
                f"""
                SELECT *
                FROM asset_materialization_generations
                WHERE {company_scope_clause}
                  AND snapshot_id = ?
                  AND asset_view = ?
                  AND artifact_kind = ?
                  AND artifact_key = ?
                LIMIT 1
                """,
                (
                    *company_scope_params,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_artifact_kind,
                    normalized_artifact_key,
                ),
            ).fetchone()
        return self._asset_materialization_generation_from_row(row)

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
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = (
            "SELECT employment_scope, lane, member_key_kind, COUNT(*) AS row_count "
            "FROM asset_membership_index "
            f"{where_clause} "
            "GROUP BY employment_scope, lane, member_key_kind"
        )
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        employment_scope_counts: dict[str, int] = {}
        lane_counts: dict[str, int] = {}
        member_key_kind_counts: dict[str, int] = {}
        total = 0
        for row in rows:
            row_count = int(row["row_count"] or 0)
            total += row_count
            scope = str(row["employment_scope"] or "").strip()
            lane = str(row["lane"] or "").strip()
            kind = str(row["member_key_kind"] or "").strip()
            if scope:
                employment_scope_counts[scope] = employment_scope_counts.get(scope, 0) + row_count
            if lane:
                lane_counts[lane] = lane_counts.get(lane, 0) + row_count
            if kind:
                member_key_kind_counts[kind] = member_key_kind_counts.get(kind, 0) + row_count
        return {
            "member_count": total,
            "employment_scope_counts": employment_scope_counts,
            "lane_counts": lane_counts,
            "member_key_kind_counts": member_key_kind_counts,
        }

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
        with self._lock, self._connection:
            company_scope_clause, company_scope_params = _company_scope_predicate(
                normalized_target_company,
                normalized_company_key,
            )
            existing = self._connection.execute(
                f"""
                SELECT target_company
                FROM candidate_materialization_state
                WHERE {company_scope_clause}
                  AND snapshot_id = ?
                  AND asset_view = ?
                  AND candidate_id = ?
                LIMIT 1
                """,
                (
                    *company_scope_params,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_candidate_id,
                ),
            ).fetchone()
            if existing is not None:
                normalized_target_company = str(existing["target_company"] or normalized_target_company).strip()
            self._connection.execute(
                """
                INSERT INTO candidate_materialization_state (
                    target_company,
                    company_key,
                    snapshot_id,
                    asset_view,
                    candidate_id,
                    fingerprint,
                    shard_path,
                    list_page,
                    dirty_reason,
                    materialized_at,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(target_company, snapshot_id, asset_view, candidate_id) DO UPDATE SET
                    company_key = excluded.company_key,
                    fingerprint = excluded.fingerprint,
                    shard_path = excluded.shard_path,
                    list_page = excluded.list_page,
                    dirty_reason = excluded.dirty_reason,
                    materialized_at = excluded.materialized_at,
                    metadata_json = excluded.metadata_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    normalized_target_company,
                    normalized_company_key,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_candidate_id,
                    normalized_fingerprint,
                    str(shard_path or "").strip(),
                    max(0, int(list_page or 0)),
                    str(dirty_reason or "").strip(),
                    str(materialized_at or datetime.now(timezone.utc).isoformat()),
                    json.dumps(_json_safe_payload(metadata or {}), ensure_ascii=False),
                ),
            )
            row = self._connection.execute(
                """
                SELECT *
                FROM candidate_materialization_state
                WHERE target_company = ? AND snapshot_id = ? AND asset_view = ? AND candidate_id = ?
                LIMIT 1
                """,
                (
                    normalized_target_company,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_candidate_id,
                ),
            ).fetchone()
        return self._candidate_materialization_state_from_row(row)

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
            for (normalized_target_company, normalized_company_key, normalized_snapshot_id, normalized_asset_view), grouped_rows in grouped_states.items():
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
                            "materialized_at": str(grouped_row.get("materialized_at") or datetime.now(timezone.utc).isoformat()),
                            "metadata_json": str(grouped_row.get("metadata_json") or "{}"),
                            "created_at": str(existing.get("created_at") or now),
                            "updated_at": now,
                        }
                    )
            if payload_rows:
                return int(
                    self._call_control_plane_postgres_native(
                        "bulk_upsert_rows",
                        "candidate_materialization_state",
                        payload_rows,
                    )
                    or 0
                )
            return 0

        total_written = 0
        with self._lock, self._connection:
            for (normalized_target_company, normalized_company_key, normalized_snapshot_id, normalized_asset_view), grouped_rows in grouped_states.items():
                company_scope_clause, company_scope_params = _company_scope_predicate(
                    normalized_target_company,
                    normalized_company_key,
                )
                existing_rows = self._connection.execute(
                    f"""
                    SELECT target_company, candidate_id
                    FROM candidate_materialization_state
                    WHERE {company_scope_clause}
                      AND snapshot_id = ?
                      AND asset_view = ?
                    """,
                    (
                        *company_scope_params,
                        normalized_snapshot_id,
                        normalized_asset_view,
                    ),
                ).fetchall()
                existing_target_company_by_candidate_id = {
                    str(item["candidate_id"] or "").strip(): str(item["target_company"] or normalized_target_company).strip()
                    for item in existing_rows
                    if str(item["candidate_id"] or "").strip()
                }
                rows_to_upsert = [
                    (
                        existing_target_company_by_candidate_id.get(candidate_id, normalized_target_company),
                        normalized_company_key,
                        normalized_snapshot_id,
                        normalized_asset_view,
                        candidate_id,
                        str(grouped_row.get("fingerprint") or "").strip(),
                        str(grouped_row.get("shard_path") or "").strip(),
                        max(0, int(grouped_row.get("list_page") or 0)),
                        str(grouped_row.get("dirty_reason") or "").strip(),
                        str(grouped_row.get("materialized_at") or datetime.now(timezone.utc).isoformat()),
                        str(grouped_row.get("metadata_json") or "{}"),
                    )
                    for candidate_id, grouped_row in grouped_rows.items()
                ]
                if not rows_to_upsert:
                    continue
                self._connection.executemany(
                    """
                    INSERT INTO candidate_materialization_state (
                        target_company,
                        company_key,
                        snapshot_id,
                        asset_view,
                        candidate_id,
                        fingerprint,
                        shard_path,
                        list_page,
                        dirty_reason,
                        materialized_at,
                        metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(target_company, snapshot_id, asset_view, candidate_id) DO UPDATE SET
                        company_key = excluded.company_key,
                        fingerprint = excluded.fingerprint,
                        shard_path = excluded.shard_path,
                        list_page = excluded.list_page,
                        dirty_reason = excluded.dirty_reason,
                        materialized_at = excluded.materialized_at,
                        metadata_json = excluded.metadata_json,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    rows_to_upsert,
                )
                total_written += len(rows_to_upsert)
        return total_written

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
                company_scope_clause, company_scope_params = _company_scope_predicate(
                    normalized_target_company,
                    normalized_company_key,
                )
                with self._lock:
                    rows = self._connection.execute(
                        f"""
                        SELECT *
                        FROM candidate_materialization_state
                        WHERE {company_scope_clause}
                          AND snapshot_id = ?
                          AND asset_view = ?
                        ORDER BY candidate_id ASC
                        """,
                        (
                            *company_scope_params,
                            normalized_snapshot_id,
                            normalized_asset_view,
                        ),
                    ).fetchall()
                existing_rows = [self._candidate_materialization_state_from_row(row) for row in rows]
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

        company_scope_clause, company_scope_params = _company_scope_predicate(
            normalized_target_company,
            normalized_company_key,
        )
        rows_to_insert = [
            (
                str(normalized_existing_states.get(candidate_id, {}).get("target_company") or normalized_target_company),
                normalized_company_key,
                normalized_snapshot_id,
                normalized_asset_view,
                candidate_id,
                str(grouped_row.get("fingerprint") or "").strip(),
                str(grouped_row.get("shard_path") or "").strip(),
                max(0, int(grouped_row.get("list_page") or 0)),
                str(grouped_row.get("dirty_reason") or "").strip(),
                str(grouped_row.get("materialized_at") or datetime.now(timezone.utc).isoformat()),
                str(grouped_row.get("metadata_json") or "{}"),
                str(normalized_existing_states.get(candidate_id, {}).get("created_at") or now),
                now,
            )
            for candidate_id, grouped_row in grouped_rows.items()
        ]
        with self._lock, self._connection:
            self._connection.execute(
                f"""
                DELETE FROM candidate_materialization_state
                WHERE {company_scope_clause}
                  AND snapshot_id = ?
                  AND asset_view = ?
                """,
                (
                    *company_scope_params,
                    normalized_snapshot_id,
                    normalized_asset_view,
                ),
            )
            if rows_to_insert:
                self._connection.executemany(
                    """
                    INSERT INTO candidate_materialization_state (
                        target_company,
                        company_key,
                        snapshot_id,
                        asset_view,
                        candidate_id,
                        fingerprint,
                        shard_path,
                        list_page,
                        dirty_reason,
                        materialized_at,
                        metadata_json,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows_to_insert,
                )
        return len(rows_to_insert)

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("candidate_materialization_state"):
            return {}
        with self._lock:
            company_scope_clause, company_scope_params = _company_scope_predicate(
                normalized_target_company,
                normalized_company_key,
            )
            row = self._connection.execute(
                f"""
                SELECT *
                FROM candidate_materialization_state
                WHERE {company_scope_clause}
                  AND snapshot_id = ?
                  AND asset_view = ?
                  AND candidate_id = ?
                LIMIT 1
                """,
                (
                    *company_scope_params,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    normalized_candidate_id,
                ),
            ).fetchone()
        return self._candidate_materialization_state_from_row(row)

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("candidate_materialization_state"):
            return []
        with self._lock:
            company_scope_clause, company_scope_params = _company_scope_predicate(
                normalized_target_company,
                normalized_company_key,
            )
            rows = self._connection.execute(
                f"""
                SELECT *
                FROM candidate_materialization_state
                WHERE {company_scope_clause}
                  AND snapshot_id = ?
                  AND asset_view = ?
                ORDER BY list_page ASC, candidate_id ASC
                """,
                (
                    *company_scope_params,
                    normalized_snapshot_id,
                    normalized_asset_view,
                ),
            ).fetchall()
        return [self._candidate_materialization_state_from_row(row) for row in rows]

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
        company_scope_clause, company_scope_params = _company_scope_predicate(
            normalized_target_company,
            normalized_company_key,
        )
        clauses = [
            company_scope_clause,
            "snapshot_id = ?",
            "asset_view = ?",
        ]
        params: list[Any] = [
            *company_scope_params,
            normalized_snapshot_id,
            normalized_asset_view,
        ]
        if active_ids:
            placeholders = ",".join("?" for _ in active_ids)
            clauses.append(f"candidate_id NOT IN ({placeholders})")
            params.extend(active_ids)
        with self._lock, self._connection:
            rows = self._connection.execute(
                f"""
                SELECT *
                FROM candidate_materialization_state
                WHERE {" AND ".join(clauses)}
                """,
                tuple(params),
            ).fetchall()
            if rows:
                self._connection.execute(
                    f"""
                    DELETE FROM candidate_materialization_state
                    WHERE {" AND ".join(clauses)}
                    """,
                    tuple(params),
                )
        return [self._candidate_materialization_state_from_row(row) for row in rows]

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
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO snapshot_materialization_runs (
                    run_id,
                    target_company,
                    company_key,
                    snapshot_id,
                    asset_view,
                    status,
                    dirty_candidate_count,
                    completed_candidate_count,
                    reused_candidate_count,
                    summary_json
                ) VALUES (?, ?, ?, ?, ?, 'running', 0, 0, 0, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    target_company = excluded.target_company,
                    company_key = excluded.company_key,
                    snapshot_id = excluded.snapshot_id,
                    asset_view = excluded.asset_view,
                    status = 'running',
                    dirty_candidate_count = 0,
                    completed_candidate_count = 0,
                    reused_candidate_count = 0,
                    summary_json = excluded.summary_json,
                    completed_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    normalized_run_id,
                    normalized_target_company,
                    normalized_company_key,
                    normalized_snapshot_id,
                    normalized_asset_view,
                    json.dumps(_json_safe_payload(summary or {}), ensure_ascii=False),
                ),
            )
            row = self._connection.execute(
                """
                SELECT *
                FROM snapshot_materialization_runs
                WHERE run_id = ?
                LIMIT 1
                """,
                (normalized_run_id,),
            ).fetchone()
        return self._snapshot_materialization_run_from_row(row)

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
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE snapshot_materialization_runs
                SET status = ?,
                    dirty_candidate_count = ?,
                    completed_candidate_count = ?,
                    reused_candidate_count = ?,
                    summary_json = ?,
                    completed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE run_id = ?
                """,
                (
                    str(status or "completed").strip() or "completed",
                    max(0, int(dirty_candidate_count or 0)),
                    max(0, int(completed_candidate_count or 0)),
                    max(0, int(reused_candidate_count or 0)),
                    json.dumps(_json_safe_payload(summary or {}), ensure_ascii=False),
                    normalized_run_id,
                ),
            )
            row = self._connection.execute(
                """
                SELECT *
                FROM snapshot_materialization_runs
                WHERE run_id = ?
                LIMIT 1
                """,
                (normalized_run_id,),
            ).fetchone()
        return self._snapshot_materialization_run_from_row(row)

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
        if self._control_plane_postgres_should_skip_sqlite_fallback("snapshot_materialization_runs"):
            return {}
        with self._lock:
            row = self._connection.execute(
                """
                SELECT *
                FROM snapshot_materialization_runs
                WHERE run_id = ?
                LIMIT 1
                """,
                (normalized_run_id,),
            ).fetchone()
        return self._snapshot_materialization_run_from_row(row)

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
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO cloud_asset_operation_ledger (
                    operation_type, bundle_kind, bundle_id, sync_run_id, status,
                    manifest_path, target_runtime_dir, target_db_path,
                    scoped_companies_json, scoped_snapshot_id, summary_json, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(operation_type or "").strip(),
                    str(bundle_kind or "").strip(),
                    str(bundle_id or "").strip(),
                    str(sync_run_id or "").strip(),
                    str(status or "").strip(),
                    str(manifest_path or "").strip(),
                    str(target_runtime_dir or "").strip(),
                    str(target_db_path or "").strip(),
                    json.dumps(_json_safe_payload(scoped_companies or []), ensure_ascii=False),
                    str(scoped_snapshot_id or "").strip(),
                    json.dumps(_json_safe_payload(summary or {}), ensure_ascii=False),
                    json.dumps(_json_safe_payload(metadata or {}), ensure_ascii=False),
                ),
            )
            row = self._connection.execute(
                """
                SELECT *
                FROM cloud_asset_operation_ledger
                WHERE ledger_id = ?
                LIMIT 1
                """,
                (int(cursor.lastrowid or 0),),
            ).fetchone()
        self._mirror_control_plane_row("cloud_asset_operation_ledger", row)
        return self._cloud_asset_operation_from_row(row)

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
        clauses: list[str] = []
        params: list[Any] = []
        normalized_operation_types = [
            str(item or "").strip() for item in list(operation_types or []) if str(item or "").strip()
        ]
        if normalized_operation_types:
            placeholders = ", ".join("?" for _ in normalized_operation_types)
            clauses.append(f"operation_type IN ({placeholders})")
            params.extend(normalized_operation_types)
        if bundle_kind:
            clauses.append("bundle_kind = ?")
            params.append(str(bundle_kind or "").strip())
        if status:
            clauses.append("status = ?")
            params.append(str(status or "").strip())
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        fetch_limit = normalized_limit if not scoped_company else max(normalized_limit * 5, normalized_limit)
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT *
                FROM cloud_asset_operation_ledger
                {where_clause}
                ORDER BY ledger_id DESC
                LIMIT ?
                """,
                tuple([*params, fetch_limit]),
            ).fetchall()
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

    def normalize_linkedin_profile_url(self, profile_url: str) -> str:
        return _normalize_linkedin_profile_url_key(profile_url)

    def _resolve_linkedin_profile_registry_key(self, profile_url_key: str) -> str:
        normalized_key = str(profile_url_key or "").strip()
        if not normalized_key:
            return ""
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry_aliases"):
            current_key = normalized_key
            seen: set[str] = set()
            while current_key and current_key not in seen:
                seen.add(current_key)
                try:
                    row = self._control_plane_postgres.select_one(
                        "linkedin_profile_registry_aliases",
                        where_sql="alias_url_key = %s",
                        params=[current_key],
                    )
                except Exception:
                    row = None
                if row is None:
                    break
                mapped_key = str(dict(row).get("profile_url_key") or "").strip()
                if not mapped_key or mapped_key == current_key:
                    break
                current_key = mapped_key
            return current_key or normalized_key
        with self._lock:
            return self._resolve_linkedin_profile_registry_key_locked(normalized_key)

    def _list_linkedin_profile_alias_urls(self, canonical_key: str) -> list[str]:
        normalized_canonical_key = str(canonical_key or "").strip()
        if not normalized_canonical_key:
            return []
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry_aliases"):
            alias_rows = self._select_control_plane_rows(
                "linkedin_profile_registry_aliases",
                row_builder=lambda row: dict(row),
                where_sql="profile_url_key = %s",
                params=[normalized_canonical_key],
                order_by_sql="updated_at DESC",
                limit=0,
            )
            if alias_rows or self._control_plane_postgres_should_skip_sqlite_fallback(
                "linkedin_profile_registry_aliases"
            ):
                aliases: list[str] = []
                for row in alias_rows:
                    alias_url = str(dict(row).get("alias_url") or "").strip()
                    if alias_url and alias_url not in aliases:
                        aliases.append(alias_url)
                return aliases
        with self._lock:
            return self._list_linkedin_profile_alias_urls_locked(normalized_canonical_key)

    def _linkedin_profile_registry_row_payload(
        self,
        *,
        profile_url_key: str,
        profile_url: str,
        raw_linkedin_url: str,
        sanity_linkedin_url: str,
        status: str,
        retry_count: int,
        last_error: str,
        last_run_id: str,
        last_dataset_id: str,
        last_snapshot_dir: str,
        last_raw_path: str,
        first_queued_at: str,
        last_queued_at: str,
        last_fetched_at: str,
        last_failed_at: str,
        source_shards: list[str],
        source_jobs: list[str],
        created_at: str = "",
        updated_at: str = "",
    ) -> dict[str, Any]:
        now = _utc_now_timestamp()
        return {
            "profile_url_key": str(profile_url_key or "").strip(),
            "profile_url": str(profile_url or "").strip(),
            "raw_linkedin_url": str(raw_linkedin_url or "").strip(),
            "sanity_linkedin_url": str(sanity_linkedin_url or "").strip(),
            "status": str(status or "").strip() or "queued",
            "retry_count": max(0, int(retry_count or 0)),
            "last_error": str(last_error or "").strip(),
            "last_run_id": str(last_run_id or "").strip(),
            "last_dataset_id": str(last_dataset_id or "").strip(),
            "last_snapshot_dir": str(last_snapshot_dir or "").strip(),
            "last_raw_path": str(last_raw_path or "").strip(),
            "first_queued_at": str(first_queued_at or "").strip(),
            "last_queued_at": str(last_queued_at or "").strip(),
            "last_fetched_at": str(last_fetched_at or "").strip(),
            "last_failed_at": str(last_failed_at or "").strip(),
            "source_shards_json": json.dumps(list(source_shards or []), ensure_ascii=False),
            "source_jobs_json": json.dumps(list(source_jobs or []), ensure_ascii=False),
            "created_at": str(created_at or now),
            "updated_at": str(updated_at or now),
        }

    def get_linkedin_profile_registry(self, profile_url: str) -> dict[str, Any] | None:
        key = _normalize_linkedin_profile_url_key(profile_url)
        if not key:
            return None
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry"):
            resolved_key = self._resolve_linkedin_profile_registry_key(key)
            payload = self._select_control_plane_row(
                "linkedin_profile_registry",
                row_builder=self._linkedin_profile_registry_from_row,
                where_sql="profile_url_key = %s",
                params=[resolved_key],
            )
            if payload is not None:
                payload["alias_urls"] = self._list_linkedin_profile_alias_urls(resolved_key)
                return payload
            if self._control_plane_postgres_should_skip_sqlite_fallback("linkedin_profile_registry"):
                return None
        with self._lock:
            resolved_key = self._resolve_linkedin_profile_registry_key_locked(key)
            row = self._connection.execute(
                """
                SELECT * FROM linkedin_profile_registry
                WHERE profile_url_key = ?
                LIMIT 1
                """,
                (resolved_key,),
            ).fetchone()
            alias_urls = self._list_linkedin_profile_alias_urls_locked(resolved_key)
        if row is None:
            return None
        payload = self._linkedin_profile_registry_from_row(row)
        payload["alias_urls"] = alias_urls
        return payload

    def get_linkedin_profile_registry_bulk(self, profile_urls: list[str]) -> dict[str, dict[str, Any]]:
        keys = _dedupe_preserve_order(
            [_normalize_linkedin_profile_url_key(profile_url) for profile_url in list(profile_urls or [])]
        )
        if not keys:
            return {}
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry"):
            alias_map: dict[str, str] = {}
            if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry_aliases"):
                placeholders = ", ".join("%s" for _ in keys)
                alias_rows = self._select_control_plane_rows(
                    "linkedin_profile_registry_aliases",
                    row_builder=lambda row: dict(row),
                    where_sql=f"alias_url_key IN ({placeholders})",
                    params=keys,
                    limit=0,
                )
                alias_map = {
                    str(dict(row).get("alias_url_key") or "").strip(): str(dict(row).get("profile_url_key") or "").strip()
                    for row in alias_rows
                    if str(dict(row).get("alias_url_key") or "").strip()
                    and str(dict(row).get("profile_url_key") or "").strip()
                }
            canonical_keys = _dedupe_preserve_order(
                [str(alias_map.get(key) or key).strip() for key in keys if str(alias_map.get(key) or key).strip()]
            )
            if canonical_keys:
                placeholders = ", ".join("%s" for _ in canonical_keys)
                rows = self._select_control_plane_rows(
                    "linkedin_profile_registry",
                    row_builder=self._linkedin_profile_registry_from_row,
                    where_sql=f"profile_url_key IN ({placeholders})",
                    params=canonical_keys,
                    limit=0,
                )
                alias_rows_all = self._select_control_plane_rows(
                    "linkedin_profile_registry_aliases",
                    row_builder=lambda row: dict(row),
                    where_sql=f"profile_url_key IN ({placeholders})",
                    params=canonical_keys,
                    order_by_sql="updated_at DESC",
                    limit=0,
                )
                aliases_by_canonical: dict[str, list[str]] = {}
                for alias_row in alias_rows_all:
                    canonical_key = str(dict(alias_row).get("profile_url_key") or "").strip()
                    alias_url = str(dict(alias_row).get("alias_url") or "").strip()
                    if not canonical_key or not alias_url:
                        continue
                    aliases_by_canonical.setdefault(canonical_key, [])
                    if alias_url not in aliases_by_canonical[canonical_key]:
                        aliases_by_canonical[canonical_key].append(alias_url)
                payload_by_canonical: dict[str, dict[str, Any]] = {}
                for row in rows:
                    canonical_key = str(dict(row).get("profile_url_key") or "").strip()
                    if not canonical_key:
                        continue
                    payload = dict(row)
                    payload["alias_urls"] = list(aliases_by_canonical.get(canonical_key) or [])
                    payload_by_canonical[canonical_key] = payload
                resolved: dict[str, dict[str, Any]] = dict(payload_by_canonical)
                for key in keys:
                    canonical_key = str(alias_map.get(key) or key).strip()
                    payload = payload_by_canonical.get(canonical_key)
                    if payload is not None:
                        resolved[key] = dict(payload)
                if resolved or self._control_plane_postgres_should_skip_sqlite_fallback("linkedin_profile_registry"):
                    return resolved
        with self._lock:
            placeholder_keys = ",".join("?" for _ in keys)
            alias_rows = self._connection.execute(
                f"""
                SELECT alias_url_key, profile_url_key
                FROM linkedin_profile_registry_aliases
                WHERE alias_url_key IN ({placeholder_keys})
                """,
                tuple(keys),
            ).fetchall()
            alias_map = {
                str(row["alias_url_key"] or "").strip(): str(row["profile_url_key"] or "").strip()
                for row in alias_rows
                if str(row["alias_url_key"] or "").strip() and str(row["profile_url_key"] or "").strip()
            }
            canonical_keys = _dedupe_preserve_order(
                [str(alias_map.get(key) or key).strip() for key in keys if str(alias_map.get(key) or key).strip()]
            )
            if not canonical_keys:
                return {}
            canonical_placeholders = ",".join("?" for _ in canonical_keys)
            rows = self._connection.execute(
                f"""
                SELECT * FROM linkedin_profile_registry
                WHERE profile_url_key IN ({canonical_placeholders})
                """,
                tuple(canonical_keys),
            ).fetchall()
            alias_rows_all = self._connection.execute(
                f"""
                SELECT profile_url_key, alias_url
                FROM linkedin_profile_registry_aliases
                WHERE profile_url_key IN ({canonical_placeholders})
                ORDER BY updated_at DESC
                """,
                tuple(canonical_keys),
            ).fetchall()
        aliases_by_canonical: dict[str, list[str]] = {}
        for alias_row in alias_rows_all:
            canonical_key = str(alias_row["profile_url_key"] or "").strip()
            alias_url = str(alias_row["alias_url"] or "").strip()
            if not canonical_key or not alias_url:
                continue
            aliases_by_canonical.setdefault(canonical_key, [])
            if alias_url not in aliases_by_canonical[canonical_key]:
                aliases_by_canonical[canonical_key].append(alias_url)
        payload_by_canonical: dict[str, dict[str, Any]] = {}
        for row in rows:
            canonical_key = str(row["profile_url_key"] or "").strip()
            if not canonical_key:
                continue
            payload = self._linkedin_profile_registry_from_row(row)
            payload["alias_urls"] = list(aliases_by_canonical.get(canonical_key) or [])
            payload_by_canonical[canonical_key] = payload

        resolved: dict[str, dict[str, Any]] = {}
        for canonical_key, payload in payload_by_canonical.items():
            resolved[canonical_key] = payload
        for key in keys:
            canonical_key = str(alias_map.get(key) or key).strip()
            payload = payload_by_canonical.get(canonical_key)
            if payload is not None:
                resolved[key] = dict(payload)
        return resolved

    def get_linkedin_profile_registry_aliases(self, profile_url: str) -> list[str]:
        key = _normalize_linkedin_profile_url_key(profile_url)
        if not key:
            return []
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry_aliases"):
            canonical_key = self._resolve_linkedin_profile_registry_key(key)
            aliases = self._list_linkedin_profile_alias_urls(canonical_key)
            if aliases or self._control_plane_postgres_should_skip_sqlite_fallback("linkedin_profile_registry_aliases"):
                return aliases
        with self._lock:
            canonical_key = self._resolve_linkedin_profile_registry_key_locked(key)
            return self._list_linkedin_profile_alias_urls_locked(canonical_key)

    def upsert_linkedin_profile_registry_aliases(
        self,
        profile_url: str,
        alias_urls: list[str],
        *,
        alias_kind: str = "observed",
    ) -> int:
        normalized_profile_url = str(profile_url or "").strip()
        profile_key = _normalize_linkedin_profile_url_key(normalized_profile_url)
        normalized_alias_urls = _normalize_linkedin_profile_url_list(alias_urls)
        if not profile_key and not normalized_alias_urls:
            return 0
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry_aliases"):
            canonical_key = self._resolve_linkedin_profile_registry_key(profile_key) if profile_key else ""
            if not canonical_key and normalized_alias_urls:
                canonical_key = _normalize_linkedin_profile_url_key(normalized_alias_urls[0])
            if not canonical_key:
                return 0
            existing = self._select_control_plane_row(
                "linkedin_profile_registry",
                row_builder=self._linkedin_profile_registry_from_row,
                where_sql="profile_url_key = %s",
                params=[canonical_key],
            )
            canonical_profile_url = str(dict(existing or {}).get("profile_url") or "").strip()
            if not canonical_profile_url:
                canonical_profile_url = normalized_profile_url or canonical_key
                payload = self._linkedin_profile_registry_row_payload(
                    profile_url_key=canonical_key,
                    profile_url=canonical_profile_url,
                    raw_linkedin_url="",
                    sanity_linkedin_url="",
                    status="queued",
                    retry_count=0,
                    last_error="",
                    last_run_id="",
                    last_dataset_id="",
                    last_snapshot_dir="",
                    last_raw_path="",
                    first_queued_at="",
                    last_queued_at="",
                    last_fetched_at="",
                    last_failed_at="",
                    source_shards=[],
                    source_jobs=[],
                    created_at=_utc_now_timestamp(),
                    updated_at=_utc_now_timestamp(),
                )
                self._write_control_plane_row_to_postgres("linkedin_profile_registry", payload)
            all_alias_urls = _normalize_linkedin_profile_url_list([canonical_profile_url, *normalized_alias_urls])
            upserted = 0
            now = _utc_now_timestamp()
            for alias_url in all_alias_urls:
                alias_key = _normalize_linkedin_profile_url_key(alias_url)
                if not alias_key:
                    continue
                if self._write_control_plane_row_to_postgres(
                    "linkedin_profile_registry_aliases",
                    {
                        "alias_url_key": alias_key,
                        "profile_url_key": canonical_key,
                        "alias_url": alias_url,
                        "alias_kind": str(alias_kind or "observed").strip() or "observed",
                        "created_at": now,
                        "updated_at": now,
                    },
                ):
                    upserted += 1
            return upserted
        with self._lock, self._connection:
            canonical_key = self._resolve_linkedin_profile_registry_key_locked(profile_key) if profile_key else ""
            if not canonical_key and normalized_alias_urls:
                canonical_key = _normalize_linkedin_profile_url_key(normalized_alias_urls[0])
            if not canonical_key:
                return 0
            existing = self._connection.execute(
                """
                SELECT profile_url FROM linkedin_profile_registry
                WHERE profile_url_key = ?
                LIMIT 1
                """,
                (canonical_key,),
            ).fetchone()
            canonical_profile_url = str(existing["profile_url"] or "").strip() if existing is not None else ""
            if not canonical_profile_url:
                canonical_profile_url = normalized_profile_url or canonical_key
                self._connection.execute(
                    """
                    INSERT INTO linkedin_profile_registry (
                        profile_url_key,
                        profile_url,
                        status,
                        retry_count,
                        source_shards_json,
                        source_jobs_json
                    ) VALUES (?, ?, 'queued', 0, '[]', '[]')
                    ON CONFLICT(profile_url_key) DO NOTHING
                    """,
                    (canonical_key, canonical_profile_url),
                )
            all_alias_urls = _normalize_linkedin_profile_url_list([canonical_profile_url, *normalized_alias_urls])
            return self._upsert_linkedin_profile_registry_aliases_locked(
                canonical_key=canonical_key,
                canonical_profile_url=canonical_profile_url,
                alias_urls=all_alias_urls,
                alias_kind=alias_kind,
            )

    def acquire_linkedin_profile_registry_lease(
        self,
        profile_url: str,
        *,
        lease_owner: str,
        lease_seconds: int = 240,
        lease_token: str = "",
    ) -> dict[str, Any]:
        normalized_owner = str(lease_owner or "").strip()
        normalized_key = _normalize_linkedin_profile_url_key(profile_url)
        if not normalized_key or not normalized_owner:
            return {
                "profile_url_key": normalized_key,
                "acquired": False,
                "lease_owner": "",
                "lease_token": "",
                "lease_expires_at": "",
            }
        normalized_token = (
            str(lease_token or "").strip()
            or sha1(f"{normalized_key}:{normalized_owner}:{_utc_now_timestamp()}".encode("utf-8")).hexdigest()[:16]
        )
        ttl_seconds = max(5, int(lease_seconds or 0))
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry_leases"):
            canonical_key = self._resolve_linkedin_profile_registry_key(normalized_key)
            existing = self._select_control_plane_row(
                "linkedin_profile_registry_leases",
                row_builder=self._linkedin_profile_registry_lease_from_row,
                where_sql="profile_url_key = %s",
                params=[canonical_key],
            )
            if (
                existing
                and not bool(existing.get("expired"))
                and str(existing.get("lease_owner") or "") not in {"", normalized_owner}
                and str(existing.get("lease_token") or "") not in {"", normalized_token}
            ):
                return {
                    **existing,
                    "acquired": False,
                    "contended": True,
                }
            now = _utc_now_timestamp()
            self._write_control_plane_row_to_postgres(
                "linkedin_profile_registry_leases",
                {
                    "profile_url_key": canonical_key,
                    "lease_owner": normalized_owner,
                    "lease_token": normalized_token,
                    "lease_expires_at": (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "created_at": str(dict(existing or {}).get("created_at") or now),
                    "updated_at": now,
                },
            )
            lease_payload = self.get_linkedin_profile_registry_lease(profile_url) or {}
            return {
                **lease_payload,
                "acquired": bool(lease_payload),
                "contended": False,
            }
        with self._lock, self._connection:
            canonical_key = self._resolve_linkedin_profile_registry_key_locked(normalized_key)
            cursor = self._connection.execute(
                """
                INSERT INTO linkedin_profile_registry_leases (
                    profile_url_key,
                    lease_owner,
                    lease_token,
                    lease_expires_at
                ) VALUES (?, ?, ?, datetime('now', ?))
                ON CONFLICT(profile_url_key) DO UPDATE SET
                    lease_owner = excluded.lease_owner,
                    lease_token = excluded.lease_token,
                    lease_expires_at = excluded.lease_expires_at,
                    updated_at = CURRENT_TIMESTAMP
                WHERE datetime(linkedin_profile_registry_leases.lease_expires_at) <= datetime('now')
                   OR linkedin_profile_registry_leases.lease_owner = excluded.lease_owner
                   OR linkedin_profile_registry_leases.lease_token = excluded.lease_token
                """,
                (
                    canonical_key,
                    normalized_owner,
                    normalized_token,
                    f"+{ttl_seconds} seconds",
                ),
            )
            lease_row = self._connection.execute(
                """
                SELECT * FROM linkedin_profile_registry_leases
                WHERE profile_url_key = ?
                LIMIT 1
                """,
                (canonical_key,),
            ).fetchone()
        lease_payload = self._linkedin_profile_registry_lease_from_row(lease_row)
        return {
            **lease_payload,
            "acquired": bool(cursor.rowcount),
            "contended": bool(
                lease_payload
                and lease_payload.get("lease_owner")
                and str(lease_payload.get("lease_owner")) != normalized_owner
                and not bool(cursor.rowcount)
            ),
        }

    def get_linkedin_profile_registry_lease(self, profile_url: str) -> dict[str, Any] | None:
        normalized_key = _normalize_linkedin_profile_url_key(profile_url)
        if not normalized_key:
            return None
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry_leases"):
            canonical_key = self._resolve_linkedin_profile_registry_key(normalized_key)
            payload = self._select_control_plane_row(
                "linkedin_profile_registry_leases",
                row_builder=self._linkedin_profile_registry_lease_from_row,
                where_sql="profile_url_key = %s",
                params=[canonical_key],
            )
            if payload is not None:
                return payload
            if self._control_plane_postgres_should_skip_sqlite_fallback("linkedin_profile_registry_leases"):
                return None
        with self._lock:
            canonical_key = self._resolve_linkedin_profile_registry_key_locked(normalized_key)
            row = self._connection.execute(
                """
                SELECT * FROM linkedin_profile_registry_leases
                WHERE profile_url_key = ?
                LIMIT 1
                """,
                (canonical_key,),
            ).fetchone()
        return self._linkedin_profile_registry_lease_from_row(row)

    def release_linkedin_profile_registry_lease(
        self,
        profile_url: str,
        *,
        lease_owner: str = "",
        lease_token: str = "",
    ) -> bool:
        normalized_key = _normalize_linkedin_profile_url_key(profile_url)
        if not normalized_key:
            return False
        normalized_owner = str(lease_owner or "").strip()
        normalized_token = str(lease_token or "").strip()
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry_leases"):
            canonical_key = self._resolve_linkedin_profile_registry_key(normalized_key)
            clauses = ["profile_url_key = %s"]
            params: list[Any] = [canonical_key]
            if normalized_owner:
                clauses.append("lease_owner = %s")
                params.append(normalized_owner)
            if normalized_token:
                clauses.append("lease_token = %s")
                params.append(normalized_token)
            try:
                deleted_count = self._control_plane_postgres.delete_rows(
                    table_name="linkedin_profile_registry_leases",
                    where_sql=" AND ".join(clauses),
                    params=params,
                )
            except Exception as exc:
                if self._control_plane_postgres_should_skip_sqlite_fallback("linkedin_profile_registry_leases"):
                    self._raise_control_plane_postgres_write_failure(
                        table_name="linkedin_profile_registry_leases",
                        method_name="delete_rows",
                        reason=f"{type(exc).__name__}: {exc}",
                        error=exc,
                    )
                deleted_count = 0
            if deleted_count or self._control_plane_postgres_should_skip_sqlite_fallback(
                "linkedin_profile_registry_leases"
            ):
                return bool(deleted_count)
        with self._lock, self._connection:
            canonical_key = self._resolve_linkedin_profile_registry_key_locked(normalized_key)
            if normalized_owner and normalized_token:
                cursor = self._connection.execute(
                    """
                    DELETE FROM linkedin_profile_registry_leases
                    WHERE profile_url_key = ? AND lease_owner = ? AND lease_token = ?
                    """,
                    (canonical_key, normalized_owner, normalized_token),
                )
            elif normalized_owner:
                cursor = self._connection.execute(
                    """
                    DELETE FROM linkedin_profile_registry_leases
                    WHERE profile_url_key = ? AND lease_owner = ?
                    """,
                    (canonical_key, normalized_owner),
                )
            elif normalized_token:
                cursor = self._connection.execute(
                    """
                    DELETE FROM linkedin_profile_registry_leases
                    WHERE profile_url_key = ? AND lease_token = ?
                    """,
                    (canonical_key, normalized_token),
                )
            else:
                cursor = self._connection.execute(
                    """
                    DELETE FROM linkedin_profile_registry_leases
                    WHERE profile_url_key = ?
                    """,
                    (canonical_key,),
                )
        return bool(cursor.rowcount)

    def record_linkedin_profile_registry_event(
        self,
        profile_url: str,
        *,
        event_type: str,
        event_status: str = "",
        detail: str = "",
        run_id: str = "",
        dataset_id: str = "",
        metadata: dict[str, Any] | None = None,
        duration_ms: int | None = None,
    ) -> None:
        normalized_key = _normalize_linkedin_profile_url_key(profile_url)
        if not normalized_key:
            return
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry_events"):
            canonical_key = self._resolve_linkedin_profile_registry_key(normalized_key)
            now = _utc_now_timestamp()
            try:
                self._control_plane_postgres.insert_row_with_generated_id(
                    table_name="linkedin_profile_registry_events",
                    row={
                        "profile_url_key": canonical_key,
                        "event_type": str(event_type or "").strip(),
                        "event_status": str(event_status or "").strip(),
                        "detail": str(detail or "").strip(),
                        "run_id": str(run_id or "").strip(),
                        "dataset_id": str(dataset_id or "").strip(),
                        "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
                        "duration_ms": int(duration_ms) if duration_ms is not None else None,
                        "created_at": now,
                    },
                )
            except Exception as exc:
                if self._control_plane_postgres_should_skip_sqlite_fallback("linkedin_profile_registry_events"):
                    self._raise_control_plane_postgres_write_failure(
                        table_name="linkedin_profile_registry_events",
                        method_name="insert_row_with_generated_id",
                        reason=f"{type(exc).__name__}: {exc}",
                        error=exc,
                    )
            if self._control_plane_postgres_should_skip_sqlite_fallback("linkedin_profile_registry_events"):
                return
        with self._lock, self._connection:
            canonical_key = self._resolve_linkedin_profile_registry_key_locked(normalized_key)
            self._connection.execute(
                """
                INSERT INTO linkedin_profile_registry_events (
                    profile_url_key,
                    event_type,
                    event_status,
                    detail,
                    run_id,
                    dataset_id,
                    metadata_json,
                    duration_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    canonical_key,
                    str(event_type or "").strip(),
                    str(event_status or "").strip(),
                    str(detail or "").strip(),
                    str(run_id or "").strip(),
                    str(dataset_id or "").strip(),
                    json.dumps(metadata or {}, ensure_ascii=False),
                    int(duration_ms) if duration_ms is not None else None,
                ),
            )

    def get_linkedin_profile_registry_metrics(self, *, lookback_hours: int = 24) -> dict[str, Any]:
        lookback = max(0, int(lookback_hours or 0))
        where_clause = ""
        params: list[Any] = []
        if lookback > 0:
            where_clause = "WHERE datetime(created_at) >= datetime('now', ?)"
            params.append(f"-{lookback} hours")
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry_events"):
            if lookback > 0:
                pg_rows = self._select_control_plane_rows(
                    "linkedin_profile_registry_events",
                    row_builder=lambda row: dict(row),
                    where_sql="created_at >= %s",
                    params=[
                        (datetime.now(timezone.utc) - timedelta(hours=lookback)).strftime("%Y-%m-%d %H:%M:%S")
                    ],
                    order_by_sql="event_id ASC",
                    limit=0,
                )
            else:
                pg_rows = self._select_control_plane_rows(
                    "linkedin_profile_registry_events",
                    row_builder=lambda row: dict(row),
                    order_by_sql="event_id ASC",
                    limit=0,
                )
            pg_registry_rows = self._select_control_plane_rows(
                "linkedin_profile_registry",
                row_builder=self._linkedin_profile_registry_from_row,
                limit=0,
            )
            if pg_rows or pg_registry_rows or self._control_plane_postgres_should_skip_sqlite_fallback(
                "linkedin_profile_registry_events"
            ):
                rows = pg_rows
                status_counts: dict[str, int] = {}
                for registry_row in pg_registry_rows:
                    status = str(dict(registry_row).get("status") or "").strip()
                    if not status:
                        continue
                    status_counts[status] = int(status_counts.get(status) or 0) + 1
                status_row = [{"status": status, "count": count} for status, count in status_counts.items()]
            else:
                with self._lock:
                    rows = self._connection.execute(
                        f"""
                        SELECT event_type, event_status, detail, metadata_json, duration_ms, created_at
                        FROM linkedin_profile_registry_events
                        {where_clause}
                        ORDER BY event_id ASC
                        """,
                        tuple(params),
                    ).fetchall()
                    status_row = self._connection.execute(
                        """
                        SELECT status, COUNT(*) AS count
                        FROM linkedin_profile_registry
                        GROUP BY status
                        """
                    ).fetchall()
        else:
            with self._lock:
                rows = self._connection.execute(
                    f"""
                    SELECT event_type, event_status, detail, metadata_json, duration_ms, created_at
                    FROM linkedin_profile_registry_events
                    {where_clause}
                    ORDER BY event_id ASC
                    """,
                    tuple(params),
                ).fetchall()
                status_row = self._connection.execute(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM linkedin_profile_registry
                    GROUP BY status
                    """
                ).fetchall()
        total_events = len(rows)
        lookup_attempts = 0
        cache_hits = 0
        live_fetch_requests = 0
        duplicate_skips = 0
        live_fetch_success = 0
        live_fetch_failed = 0
        retry_success = 0
        retry_failures = 0
        unrecoverable_failures = 0
        queue_durations_ms: list[int] = []
        for row in rows:
            event_type = str(row["event_type"] or "").strip()
            event_status = str(row["event_status"] or "").strip().lower()
            if event_type == "lookup_attempt":
                lookup_attempts += 1
            if event_type in {"cache_hit_registry", "cache_hit_local_raw", "cache_hit_lease_wait"}:
                cache_hits += 1
            if event_type == "live_fetch_requested":
                live_fetch_requests += 1
            if event_type in {"duplicate_fetch_blocked", "lease_contended_skip"}:
                duplicate_skips += 1
            metadata: dict[str, Any] = {}
            try:
                metadata = dict(json.loads(row["metadata_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                metadata = {}
            retry_before = int(metadata.get("retry_count_before") or 0)
            if event_type == "live_fetch_success":
                live_fetch_success += 1
                if retry_before > 0:
                    retry_success += 1
                duration_ms = row["duration_ms"]
                if duration_ms is not None:
                    try:
                        queue_durations_ms.append(max(0, int(duration_ms)))
                    except (TypeError, ValueError):
                        pass
            elif event_type == "live_fetch_failed":
                live_fetch_failed += 1
                if retry_before > 0:
                    retry_failures += 1
                if event_status == "unrecoverable":
                    unrecoverable_failures += 1
        queue_duration_p50 = _percentile(queue_durations_ms, 50)
        queue_duration_p95 = _percentile(queue_durations_ms, 95)
        retry_attempts = retry_success + retry_failures
        terminal_attempts = live_fetch_success + live_fetch_failed
        registry_status_counts = {
            str(row["status"] or "").strip(): int(row["count"] or 0)
            for row in status_row
            if str(row["status"] or "").strip()
        }
        return {
            "window_hours": lookback,
            "event_count": total_events,
            "lookup_attempts": lookup_attempts,
            "cache_hits": cache_hits,
            "cache_hit_rate": (cache_hits / lookup_attempts) if lookup_attempts else 0.0,
            "live_fetch_requests": live_fetch_requests,
            "duplicate_fetch_skips": duplicate_skips,
            "duplicate_request_rate": (duplicate_skips / (live_fetch_requests + duplicate_skips))
            if (live_fetch_requests + duplicate_skips)
            else 0.0,
            "live_fetch_success": live_fetch_success,
            "live_fetch_failed": live_fetch_failed,
            "queued_duration_ms_p50": queue_duration_p50,
            "queued_duration_ms_p95": queue_duration_p95,
            "retry_success_count": retry_success,
            "retry_failure_count": retry_failures,
            "retry_success_rate": (retry_success / retry_attempts) if retry_attempts else 0.0,
            "unrecoverable_failures": unrecoverable_failures,
            "unrecoverable_ratio": (unrecoverable_failures / terminal_attempts) if terminal_attempts else 0.0,
            "registry_status_counts": registry_status_counts,
        }

    def get_linkedin_profile_registry_backfill_run(self, run_key: str) -> dict[str, Any] | None:
        normalized_run_key = str(run_key or "").strip()
        if not normalized_run_key:
            return None
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry_backfill_runs"):
            payload = self._select_control_plane_row(
                "linkedin_profile_registry_backfill_runs",
                row_builder=self._linkedin_profile_registry_backfill_from_row,
                where_sql="run_key = %s",
                params=[normalized_run_key],
            )
            if payload is not None:
                return payload
            if self._control_plane_postgres_should_skip_sqlite_fallback("linkedin_profile_registry_backfill_runs"):
                return None
        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM linkedin_profile_registry_backfill_runs
                WHERE run_key = ?
                LIMIT 1
                """,
                (normalized_run_key,),
            ).fetchone()
        if row is None:
            return None
        return self._linkedin_profile_registry_backfill_from_row(row)

    def upsert_linkedin_profile_registry_backfill_run(
        self,
        run_key: str,
        *,
        scope_company: str = "",
        scope_snapshot_id: str = "",
        checkpoint: dict[str, Any] | None = None,
        summary: dict[str, Any] | None = None,
        status: str = "running",
    ) -> dict[str, Any] | None:
        normalized_run_key = str(run_key or "").strip()
        if not normalized_run_key:
            return None
        checkpoint_payload = checkpoint or {}
        summary_payload = summary or {}
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry_backfill_runs"):
            existing = self.get_linkedin_profile_registry_backfill_run(normalized_run_key) or {}
            row_payload = {
                "run_key": normalized_run_key,
                "scope_company": str(scope_company or "").strip(),
                "scope_snapshot_id": str(scope_snapshot_id or "").strip(),
                "checkpoint_json": json.dumps(checkpoint_payload, ensure_ascii=False),
                "summary_json": json.dumps(summary_payload, ensure_ascii=False),
                "status": str(status or "running").strip() or "running",
                "created_at": str(dict(existing).get("created_at") or _utc_now_timestamp()),
                "updated_at": _utc_now_timestamp(),
            }
            self._write_control_plane_row_to_postgres("linkedin_profile_registry_backfill_runs", row_payload)
            return self.get_linkedin_profile_registry_backfill_run(normalized_run_key)
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO linkedin_profile_registry_backfill_runs (
                    run_key,
                    scope_company,
                    scope_snapshot_id,
                    checkpoint_json,
                    summary_json,
                    status
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_key) DO UPDATE SET
                    scope_company = excluded.scope_company,
                    scope_snapshot_id = excluded.scope_snapshot_id,
                    checkpoint_json = excluded.checkpoint_json,
                    summary_json = excluded.summary_json,
                    status = excluded.status,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    normalized_run_key,
                    str(scope_company or "").strip(),
                    str(scope_snapshot_id or "").strip(),
                    json.dumps(checkpoint_payload, ensure_ascii=False),
                    json.dumps(summary_payload, ensure_ascii=False),
                    str(status or "running").strip() or "running",
                ),
            )
            row = self._connection.execute(
                """
                SELECT * FROM linkedin_profile_registry_backfill_runs
                WHERE run_key = ?
                LIMIT 1
                """,
                (normalized_run_key,),
            ).fetchone()
        if row is None:
            return None
        return self._linkedin_profile_registry_backfill_from_row(row)

    def mark_linkedin_profile_registry_queued(
        self,
        profile_url: str,
        *,
        source_shards: list[str] | None = None,
        source_jobs: list[str] | None = None,
        alias_urls: list[str] | None = None,
        raw_linkedin_url: str = "",
        sanity_linkedin_url: str = "",
        run_id: str = "",
        dataset_id: str = "",
        snapshot_dir: str = "",
    ) -> dict[str, Any] | None:
        return self._upsert_linkedin_profile_registry(
            profile_url=profile_url,
            status="queued",
            source_shards=source_shards,
            source_jobs=source_jobs,
            alias_urls=alias_urls,
            raw_linkedin_url=raw_linkedin_url,
            sanity_linkedin_url=sanity_linkedin_url,
            run_id=run_id,
            dataset_id=dataset_id,
            snapshot_dir=snapshot_dir,
            preserve_unrecoverable=True,
        )

    def mark_linkedin_profile_registry_fetched(
        self,
        profile_url: str,
        *,
        raw_path: str,
        source_shards: list[str] | None = None,
        source_jobs: list[str] | None = None,
        alias_urls: list[str] | None = None,
        raw_linkedin_url: str = "",
        sanity_linkedin_url: str = "",
        run_id: str = "",
        dataset_id: str = "",
        snapshot_dir: str = "",
    ) -> dict[str, Any] | None:
        return self._upsert_linkedin_profile_registry(
            profile_url=profile_url,
            status="fetched",
            retry_count=0,
            last_error="",
            raw_path=raw_path,
            source_shards=source_shards,
            source_jobs=source_jobs,
            alias_urls=alias_urls,
            raw_linkedin_url=raw_linkedin_url,
            sanity_linkedin_url=sanity_linkedin_url,
            run_id=run_id,
            dataset_id=dataset_id,
            snapshot_dir=snapshot_dir,
            preserve_unrecoverable=False,
        )

    def mark_linkedin_profile_registry_failed(
        self,
        profile_url: str,
        *,
        error: str,
        retryable: bool = True,
        source_shards: list[str] | None = None,
        source_jobs: list[str] | None = None,
        alias_urls: list[str] | None = None,
        raw_linkedin_url: str = "",
        sanity_linkedin_url: str = "",
        run_id: str = "",
        dataset_id: str = "",
        snapshot_dir: str = "",
    ) -> dict[str, Any] | None:
        return self._upsert_linkedin_profile_registry(
            profile_url=profile_url,
            status="failed_retryable" if retryable else "unrecoverable",
            increment_retry=retryable,
            last_error=str(error or "").strip(),
            source_shards=source_shards,
            source_jobs=source_jobs,
            alias_urls=alias_urls,
            raw_linkedin_url=raw_linkedin_url,
            sanity_linkedin_url=sanity_linkedin_url,
            run_id=run_id,
            dataset_id=dataset_id,
            snapshot_dir=snapshot_dir,
            preserve_unrecoverable=not retryable,
        )

    def upsert_linkedin_profile_registry_sources(
        self,
        profile_url: str,
        *,
        source_shards: list[str] | None = None,
        source_jobs: list[str] | None = None,
        alias_urls: list[str] | None = None,
        raw_linkedin_url: str = "",
        sanity_linkedin_url: str = "",
    ) -> dict[str, Any] | None:
        return self._upsert_linkedin_profile_registry(
            profile_url=profile_url,
            status="",
            source_shards=source_shards,
            source_jobs=source_jobs,
            alias_urls=alias_urls,
            raw_linkedin_url=raw_linkedin_url,
            sanity_linkedin_url=sanity_linkedin_url,
            preserve_unrecoverable=True,
        )

    def _compose_linkedin_profile_registry_effective_payload(
        self,
        *,
        existing_payload: dict[str, Any],
        normalized_status: str,
        normalized_profile_url: str,
        normalized_raw_linkedin_url: str,
        normalized_sanity_linkedin_url: str,
        normalized_alias_urls: list[str],
        normalized_run_id: str,
        normalized_dataset_id: str,
        normalized_snapshot_dir: str,
        normalized_raw_path: str,
        normalized_source_shards: list[str],
        normalized_source_jobs: list[str],
        retry_count: int | None,
        increment_retry: bool,
        last_error: str | None,
        preserve_unrecoverable: bool,
        now_timestamp: str,
    ) -> dict[str, Any]:
        existing = dict(existing_payload or {})
        existing_status = str(existing.get("status") or "").strip()
        merged_source_shards = _merge_registry_label_lists(
            list(existing.get("source_shards") or []),
            normalized_source_shards,
        )
        merged_source_jobs = _merge_registry_label_lists(
            list(existing.get("source_jobs") or []),
            normalized_source_jobs,
        )
        effective_status = normalized_status or existing_status or "queued"
        existing_raw_path = str(existing.get("last_raw_path") or "").strip()
        if preserve_unrecoverable and existing_status == "unrecoverable" and effective_status != "fetched":
            effective_status = "unrecoverable"
        if normalized_status == "unrecoverable":
            effective_status = "unrecoverable"
        if normalized_status == "fetched":
            effective_status = "fetched"
        if normalized_status == "queued" and existing_status == "fetched" and existing_raw_path:
            effective_status = "fetched"
        if retry_count is not None:
            effective_retry_count = max(0, int(retry_count))
        else:
            effective_retry_count = max(0, int(existing.get("retry_count") or 0))
            if increment_retry:
                effective_retry_count += 1
        effective_last_error = str(last_error) if last_error is not None else str(existing.get("last_error") or "")
        if normalized_status == "fetched":
            effective_last_error = ""
        effective_profile_url = normalized_profile_url or str(existing.get("profile_url") or "")
        effective_raw_linkedin_url = normalized_raw_linkedin_url or str(existing.get("raw_linkedin_url") or "")
        effective_sanity_linkedin_url = normalized_sanity_linkedin_url or str(existing.get("sanity_linkedin_url") or "")
        effective_run_id = normalized_run_id or str(existing.get("last_run_id") or "")
        effective_dataset_id = normalized_dataset_id or str(existing.get("last_dataset_id") or "")
        effective_snapshot_dir = normalized_snapshot_dir or str(existing.get("last_snapshot_dir") or "")
        effective_raw_path = normalized_raw_path or str(existing.get("last_raw_path") or "")
        effective_first_queued_at = str(existing.get("first_queued_at") or "")
        effective_last_queued_at = str(existing.get("last_queued_at") or "")
        effective_last_fetched_at = str(existing.get("last_fetched_at") or "")
        effective_last_failed_at = str(existing.get("last_failed_at") or "")
        if normalized_status == "queued":
            if not effective_first_queued_at:
                effective_first_queued_at = now_timestamp
            effective_last_queued_at = now_timestamp
        if normalized_status == "fetched":
            effective_last_fetched_at = now_timestamp
        if normalized_status in {"failed_retryable", "unrecoverable"}:
            effective_last_failed_at = now_timestamp
        effective_alias_urls = _normalize_linkedin_profile_url_list(
            [
                *list(existing.get("alias_urls") or []),
                *normalized_alias_urls,
                effective_profile_url,
                effective_raw_linkedin_url,
                effective_sanity_linkedin_url,
            ]
        )
        return {
            "profile_url": effective_profile_url,
            "raw_linkedin_url": effective_raw_linkedin_url,
            "sanity_linkedin_url": effective_sanity_linkedin_url,
            "status": effective_status,
            "retry_count": effective_retry_count,
            "last_error": effective_last_error,
            "last_run_id": effective_run_id,
            "last_dataset_id": effective_dataset_id,
            "last_snapshot_dir": effective_snapshot_dir,
            "last_raw_path": effective_raw_path,
            "first_queued_at": effective_first_queued_at,
            "last_queued_at": effective_last_queued_at,
            "last_fetched_at": effective_last_fetched_at,
            "last_failed_at": effective_last_failed_at,
            "source_shards": merged_source_shards,
            "source_jobs": merged_source_jobs,
            "alias_urls": effective_alias_urls,
            "created_at": str(existing.get("created_at") or now_timestamp),
            "updated_at": now_timestamp,
        }

    def backfill_linkedin_profile_registry_batch(
        self,
        entries: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> int:
        normalized_entries = [
            _normalize_linkedin_profile_registry_backfill_entry(item)
            for item in list(entries or [])
        ]
        normalized_entries = [item for item in normalized_entries if item is not None]
        if not normalized_entries:
            return 0
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry"):
            return self._backfill_linkedin_profile_registry_batch_postgres(normalized_entries)
        processed = 0
        for entry in normalized_entries:
            if str(entry.get("status") or "") == "fetched":
                self.mark_linkedin_profile_registry_fetched(
                    str(entry.get("profile_url") or ""),
                    raw_path=str(entry.get("raw_path") or ""),
                    source_shards=list(entry.get("source_shards") or []),
                    source_jobs=list(entry.get("source_jobs") or []),
                    alias_urls=list(entry.get("alias_urls") or []),
                    raw_linkedin_url=str(entry.get("raw_linkedin_url") or ""),
                    sanity_linkedin_url=str(entry.get("sanity_linkedin_url") or ""),
                    snapshot_dir=str(entry.get("snapshot_dir") or ""),
                )
            else:
                self.mark_linkedin_profile_registry_failed(
                    str(entry.get("profile_url") or ""),
                    error=str(entry.get("error") or ""),
                    retryable=bool(entry.get("retryable")),
                    source_shards=list(entry.get("source_shards") or []),
                    source_jobs=list(entry.get("source_jobs") or []),
                    alias_urls=list(entry.get("alias_urls") or []),
                    raw_linkedin_url=str(entry.get("raw_linkedin_url") or ""),
                    sanity_linkedin_url=str(entry.get("sanity_linkedin_url") or ""),
                    snapshot_dir=str(entry.get("snapshot_dir") or ""),
                )
            processed += 1
        return processed

    def _backfill_linkedin_profile_registry_batch_postgres(
        self,
        entries: list[dict[str, Any]],
    ) -> int:
        grouped_entries: dict[str, list[dict[str, Any]]] = {}
        alias_to_group: dict[str, str] = {}
        group_order: list[str] = []
        for entry in entries:
            alias_keys = [str(item).strip() for item in list(entry.get("alias_keys") or []) if str(item).strip()]
            related_groups = _dedupe_preserve_order(
                [alias_to_group.get(alias_key, "") for alias_key in alias_keys if alias_to_group.get(alias_key, "")]
            )
            if related_groups:
                primary_group = related_groups[0]
                if primary_group not in grouped_entries:
                    grouped_entries[primary_group] = []
                    group_order.append(primary_group)
                grouped_entries[primary_group].append(entry)
                for other_group in related_groups[1:]:
                    if other_group == primary_group or other_group not in grouped_entries:
                        continue
                    grouped_entries[primary_group].extend(grouped_entries.pop(other_group))
                    if other_group in group_order:
                        group_order.remove(other_group)
                for alias_key in alias_keys:
                    alias_to_group[alias_key] = primary_group
                for merged_entry in grouped_entries[primary_group]:
                    for alias_key in list(merged_entry.get("alias_keys") or []):
                        if str(alias_key or "").strip():
                            alias_to_group[str(alias_key).strip()] = primary_group
                continue
            primary_group = str(entry.get("profile_url_key") or "") or alias_keys[0]
            grouped_entries[primary_group] = [entry]
            group_order.append(primary_group)
            for alias_key in alias_keys:
                alias_to_group[alias_key] = primary_group

        all_alias_keys = _dedupe_preserve_order(
            [
                str(alias_key).strip()
                for group_key in group_order
                for entry in grouped_entries.get(group_key, [])
                for alias_key in list(entry.get("alias_keys") or [])
                if str(alias_key).strip()
            ]
        )
        alias_rows: list[dict[str, Any]] = []
        alias_rows_by_profile: dict[str, list[str]] = {}
        existing_alias_map: dict[str, str] = {}
        if all_alias_keys:
            where_sql = "alias_url_key IN (" + ", ".join(["%s"] * len(all_alias_keys)) + ")"
            alias_rows = self._control_plane_postgres.select_many(
                "linkedin_profile_registry_aliases",
                where_sql=where_sql,
                params=all_alias_keys,
                order_by_sql="updated_at DESC",
                limit=max(100, len(all_alias_keys) * 2),
            )
            for row in alias_rows:
                alias_key = str(row.get("alias_url_key") or "").strip()
                canonical_key = str(row.get("profile_url_key") or "").strip()
                alias_url = str(row.get("alias_url") or "").strip()
                if alias_key and canonical_key and alias_key not in existing_alias_map:
                    existing_alias_map[alias_key] = canonical_key
                if canonical_key and alias_url:
                    aliases = alias_rows_by_profile.setdefault(canonical_key, [])
                    if alias_url not in aliases:
                        aliases.append(alias_url)

        canonical_keys = _dedupe_preserve_order(
            [
                next(
                    (
                        existing_alias_map.get(alias_key, "")
                        for entry in grouped_entries.get(group_key, [])
                        for alias_key in list(entry.get("alias_keys") or [])
                        if existing_alias_map.get(alias_key, "")
                    ),
                    str(grouped_entries.get(group_key, [{}])[0].get("profile_url_key") or ""),
                )
                for group_key in group_order
            ]
        )
        existing_rows_by_key: dict[str, dict[str, Any]] = {}
        if canonical_keys:
            where_sql = "profile_url_key IN (" + ", ".join(["%s"] * len(canonical_keys)) + ")"
            existing_rows = self._control_plane_postgres.select_many(
                "linkedin_profile_registry",
                where_sql=where_sql,
                params=canonical_keys,
                limit=max(100, len(canonical_keys) * 2),
            )
            for row in existing_rows:
                payload = self._linkedin_profile_registry_from_row(row)
                canonical_key = str(payload.get("profile_url_key") or "").strip()
                if not canonical_key:
                    continue
                payload["alias_urls"] = list(alias_rows_by_profile.get(canonical_key, []))
                existing_rows_by_key[canonical_key] = payload

        registry_rows_to_upsert: list[dict[str, Any]] = []
        alias_rows_to_upsert: list[dict[str, Any]] = []
        alias_written_keys: set[str] = set()
        for group_key in group_order:
            batch_entries = list(grouped_entries.get(group_key, []))
            if not batch_entries:
                continue
            canonical_key = next(
                (
                    existing_alias_map.get(alias_key, "")
                    for entry in batch_entries
                    for alias_key in list(entry.get("alias_keys") or [])
                    if existing_alias_map.get(alias_key, "")
                ),
                str(batch_entries[0].get("profile_url_key") or ""),
            )
            if not canonical_key:
                continue
            effective_payload = dict(existing_rows_by_key.get(canonical_key) or {})
            for entry in batch_entries:
                effective_payload = self._compose_linkedin_profile_registry_effective_payload(
                    existing_payload=effective_payload,
                    normalized_status=str(entry.get("status") or ""),
                    normalized_profile_url=str(entry.get("profile_url") or ""),
                    normalized_raw_linkedin_url=str(entry.get("raw_linkedin_url") or ""),
                    normalized_sanity_linkedin_url=str(entry.get("sanity_linkedin_url") or ""),
                    normalized_alias_urls=list(entry.get("alias_urls") or []),
                    normalized_run_id="",
                    normalized_dataset_id="",
                    normalized_snapshot_dir=str(entry.get("snapshot_dir") or ""),
                    normalized_raw_path=str(entry.get("raw_path") or ""),
                    normalized_source_shards=list(entry.get("source_shards") or []),
                    normalized_source_jobs=list(entry.get("source_jobs") or []),
                    retry_count=0 if str(entry.get("status") or "") == "fetched" else None,
                    increment_retry=bool(entry.get("retryable")),
                    last_error=str(entry.get("error") or ""),
                    preserve_unrecoverable=not bool(entry.get("retryable")),
                    now_timestamp=_utc_now_timestamp(),
                )
            registry_rows_to_upsert.append(
                self._linkedin_profile_registry_row_payload(
                    profile_url_key=canonical_key,
                    profile_url=str(effective_payload.get("profile_url") or canonical_key),
                    raw_linkedin_url=str(effective_payload.get("raw_linkedin_url") or ""),
                    sanity_linkedin_url=str(effective_payload.get("sanity_linkedin_url") or ""),
                    status=str(effective_payload.get("status") or "queued"),
                    retry_count=int(effective_payload.get("retry_count") or 0),
                    last_error=str(effective_payload.get("last_error") or ""),
                    last_run_id=str(effective_payload.get("last_run_id") or ""),
                    last_dataset_id=str(effective_payload.get("last_dataset_id") or ""),
                    last_snapshot_dir=str(effective_payload.get("last_snapshot_dir") or ""),
                    last_raw_path=str(effective_payload.get("last_raw_path") or ""),
                    first_queued_at=str(effective_payload.get("first_queued_at") or ""),
                    last_queued_at=str(effective_payload.get("last_queued_at") or ""),
                    last_fetched_at=str(effective_payload.get("last_fetched_at") or ""),
                    last_failed_at=str(effective_payload.get("last_failed_at") or ""),
                    source_shards=list(effective_payload.get("source_shards") or []),
                    source_jobs=list(effective_payload.get("source_jobs") or []),
                    created_at=str(effective_payload.get("created_at") or _utc_now_timestamp()),
                    updated_at=str(effective_payload.get("updated_at") or _utc_now_timestamp()),
                )
            )
            for alias_url in _normalize_linkedin_profile_url_list(
                [
                    str(effective_payload.get("profile_url") or canonical_key),
                    *list(effective_payload.get("alias_urls") or []),
                ]
            ):
                alias_key = _normalize_linkedin_profile_url_key(alias_url)
                if not alias_key or alias_key in alias_written_keys:
                    continue
                alias_written_keys.add(alias_key)
                alias_rows_to_upsert.append(
                    {
                        "alias_url_key": alias_key,
                        "profile_url_key": canonical_key,
                        "alias_url": alias_url,
                        "alias_kind": "observed",
                        "created_at": str(effective_payload.get("created_at") or _utc_now_timestamp()),
                        "updated_at": str(effective_payload.get("updated_at") or _utc_now_timestamp()),
                    }
                )

        if registry_rows_to_upsert:
            self._control_plane_postgres.bulk_upsert_rows("linkedin_profile_registry", registry_rows_to_upsert)
        if alias_rows_to_upsert:
            self._control_plane_postgres.bulk_upsert_rows("linkedin_profile_registry_aliases", alias_rows_to_upsert)
        return len(entries)

    def _upsert_linkedin_profile_registry(
        self,
        *,
        profile_url: str,
        status: str,
        source_shards: list[str] | None = None,
        source_jobs: list[str] | None = None,
        alias_urls: list[str] | None = None,
        raw_linkedin_url: str = "",
        sanity_linkedin_url: str = "",
        alias_kind: str = "observed",
        run_id: str = "",
        dataset_id: str = "",
        snapshot_dir: str = "",
        raw_path: str = "",
        retry_count: int | None = None,
        increment_retry: bool = False,
        last_error: str | None = None,
        preserve_unrecoverable: bool = True,
    ) -> dict[str, Any] | None:
        normalized_key = _normalize_linkedin_profile_url_key(profile_url)
        normalized_profile_url = str(profile_url or "").strip()
        if not normalized_key:
            return None
        normalized_raw_linkedin_url = str(raw_linkedin_url or "").strip()
        normalized_sanity_linkedin_url = str(sanity_linkedin_url or "").strip()
        normalized_alias_urls = _normalize_linkedin_profile_url_list(
            [
                *list(alias_urls or []),
                normalized_profile_url,
                normalized_raw_linkedin_url,
                normalized_sanity_linkedin_url,
            ]
        )
        normalized_status = str(status or "").strip()
        normalized_run_id = str(run_id or "").strip()
        normalized_dataset_id = str(dataset_id or "").strip()
        normalized_snapshot_dir = str(snapshot_dir or "").strip()
        normalized_raw_path = str(raw_path or "").strip()
        normalized_source_shards = _normalize_registry_label_list(source_shards)
        normalized_source_jobs = _normalize_registry_label_list(source_jobs)
        now_timestamp = _utc_now_timestamp()
        if self._control_plane_postgres_should_prefer_read("linkedin_profile_registry"):
            canonical_key = self._resolve_linkedin_profile_registry_key(normalized_key)
            existing_payload = self.get_linkedin_profile_registry(normalized_key) or {}
            effective_payload = self._compose_linkedin_profile_registry_effective_payload(
                existing_payload=existing_payload,
                normalized_status=normalized_status,
                normalized_profile_url=normalized_profile_url,
                normalized_raw_linkedin_url=normalized_raw_linkedin_url,
                normalized_sanity_linkedin_url=normalized_sanity_linkedin_url,
                normalized_alias_urls=normalized_alias_urls,
                normalized_run_id=normalized_run_id,
                normalized_dataset_id=normalized_dataset_id,
                normalized_snapshot_dir=normalized_snapshot_dir,
                normalized_raw_path=normalized_raw_path,
                normalized_source_shards=normalized_source_shards,
                normalized_source_jobs=normalized_source_jobs,
                retry_count=retry_count,
                increment_retry=increment_retry,
                last_error=last_error,
                preserve_unrecoverable=preserve_unrecoverable,
                now_timestamp=now_timestamp,
            )

            row_payload = self._linkedin_profile_registry_row_payload(
                profile_url_key=canonical_key,
                profile_url=str(effective_payload.get("profile_url") or ""),
                raw_linkedin_url=str(effective_payload.get("raw_linkedin_url") or ""),
                sanity_linkedin_url=str(effective_payload.get("sanity_linkedin_url") or ""),
                status=str(effective_payload.get("status") or "queued"),
                retry_count=int(effective_payload.get("retry_count") or 0),
                last_error=str(effective_payload.get("last_error") or ""),
                last_run_id=str(effective_payload.get("last_run_id") or ""),
                last_dataset_id=str(effective_payload.get("last_dataset_id") or ""),
                last_snapshot_dir=str(effective_payload.get("last_snapshot_dir") or ""),
                last_raw_path=str(effective_payload.get("last_raw_path") or ""),
                first_queued_at=str(effective_payload.get("first_queued_at") or ""),
                last_queued_at=str(effective_payload.get("last_queued_at") or ""),
                last_fetched_at=str(effective_payload.get("last_fetched_at") or ""),
                last_failed_at=str(effective_payload.get("last_failed_at") or ""),
                source_shards=list(effective_payload.get("source_shards") or []),
                source_jobs=list(effective_payload.get("source_jobs") or []),
                created_at=str(effective_payload.get("created_at") or now_timestamp),
                updated_at=str(effective_payload.get("updated_at") or now_timestamp),
            )
            self._write_control_plane_row_to_postgres("linkedin_profile_registry", row_payload)
            canonical_profile_url = str(effective_payload.get("profile_url") or canonical_key).strip()
            self.upsert_linkedin_profile_registry_aliases(
                canonical_profile_url,
                list(effective_payload.get("alias_urls") or normalized_alias_urls),
                alias_kind=alias_kind,
            )
            return self.get_linkedin_profile_registry(canonical_profile_url or normalized_key)
        with self._lock, self._connection:
            canonical_key = self._resolve_linkedin_profile_registry_key_locked(normalized_key)
            existing = self._connection.execute(
                """
                SELECT * FROM linkedin_profile_registry
                WHERE profile_url_key = ?
                LIMIT 1
                """,
                (canonical_key,),
            ).fetchone()
            existing_payload = self._linkedin_profile_registry_from_row(existing) if existing is not None else {}
            effective_payload = self._compose_linkedin_profile_registry_effective_payload(
                existing_payload=existing_payload,
                normalized_status=normalized_status,
                normalized_profile_url=normalized_profile_url,
                normalized_raw_linkedin_url=normalized_raw_linkedin_url,
                normalized_sanity_linkedin_url=normalized_sanity_linkedin_url,
                normalized_alias_urls=normalized_alias_urls,
                normalized_run_id=normalized_run_id,
                normalized_dataset_id=normalized_dataset_id,
                normalized_snapshot_dir=normalized_snapshot_dir,
                normalized_raw_path=normalized_raw_path,
                normalized_source_shards=normalized_source_shards,
                normalized_source_jobs=normalized_source_jobs,
                retry_count=retry_count,
                increment_retry=increment_retry,
                last_error=last_error,
                preserve_unrecoverable=preserve_unrecoverable,
                now_timestamp=now_timestamp,
            )

            self._connection.execute(
                """
                INSERT INTO linkedin_profile_registry (
                    profile_url_key,
                    profile_url,
                    raw_linkedin_url,
                    sanity_linkedin_url,
                    status,
                    retry_count,
                    last_error,
                    last_run_id,
                    last_dataset_id,
                    last_snapshot_dir,
                    last_raw_path,
                    first_queued_at,
                    last_queued_at,
                    last_fetched_at,
                    last_failed_at,
                    source_shards_json,
                    source_jobs_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile_url_key) DO UPDATE SET
                    profile_url = excluded.profile_url,
                    raw_linkedin_url = excluded.raw_linkedin_url,
                    sanity_linkedin_url = excluded.sanity_linkedin_url,
                    status = excluded.status,
                    retry_count = excluded.retry_count,
                    last_error = excluded.last_error,
                    last_run_id = excluded.last_run_id,
                    last_dataset_id = excluded.last_dataset_id,
                    last_snapshot_dir = excluded.last_snapshot_dir,
                    last_raw_path = excluded.last_raw_path,
                    first_queued_at = excluded.first_queued_at,
                    last_queued_at = excluded.last_queued_at,
                    last_fetched_at = excluded.last_fetched_at,
                    last_failed_at = excluded.last_failed_at,
                    source_shards_json = excluded.source_shards_json,
                    source_jobs_json = excluded.source_jobs_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    canonical_key,
                    str(effective_payload.get("profile_url") or ""),
                    str(effective_payload.get("raw_linkedin_url") or ""),
                    str(effective_payload.get("sanity_linkedin_url") or ""),
                    str(effective_payload.get("status") or "queued"),
                    int(effective_payload.get("retry_count") or 0),
                    str(effective_payload.get("last_error") or ""),
                    str(effective_payload.get("last_run_id") or ""),
                    str(effective_payload.get("last_dataset_id") or ""),
                    str(effective_payload.get("last_snapshot_dir") or ""),
                    str(effective_payload.get("last_raw_path") or ""),
                    str(effective_payload.get("first_queued_at") or ""),
                    str(effective_payload.get("last_queued_at") or ""),
                    str(effective_payload.get("last_fetched_at") or ""),
                    str(effective_payload.get("last_failed_at") or ""),
                    json.dumps(list(effective_payload.get("source_shards") or []), ensure_ascii=False),
                    json.dumps(list(effective_payload.get("source_jobs") or []), ensure_ascii=False),
                ),
            )
            canonical_profile_url = str(effective_payload.get("profile_url") or canonical_key)
            self._upsert_linkedin_profile_registry_aliases_locked(
                canonical_key=canonical_key,
                canonical_profile_url=canonical_profile_url,
                alias_urls=list(effective_payload.get("alias_urls") or normalized_alias_urls),
                alias_kind=alias_kind,
            )
            row = self._connection.execute(
                """
                SELECT * FROM linkedin_profile_registry
                WHERE profile_url_key = ?
                LIMIT 1
                """,
                (canonical_key,),
            ).fetchone()
        if row is None:
            return None
        payload = self._linkedin_profile_registry_from_row(row)
        payload["alias_urls"] = self.get_linkedin_profile_registry_aliases(
            payload.get("profile_url") or payload.get("profile_url_key") or ""
        )
        return payload

    def _resolve_linkedin_profile_registry_key_locked(self, profile_url_key: str) -> str:
        normalized_key = str(profile_url_key or "").strip()
        if not normalized_key:
            return ""
        current_key = normalized_key
        seen: set[str] = set()
        while current_key and current_key not in seen:
            seen.add(current_key)
            row = self._connection.execute(
                """
                SELECT profile_url_key
                FROM linkedin_profile_registry_aliases
                WHERE alias_url_key = ?
                LIMIT 1
                """,
                (current_key,),
            ).fetchone()
            if row is None:
                break
            mapped_key = str(row["profile_url_key"] or "").strip()
            if not mapped_key or mapped_key == current_key:
                break
            current_key = mapped_key
        return current_key or normalized_key

    def _list_linkedin_profile_alias_urls_locked(self, canonical_key: str) -> list[str]:
        normalized_canonical_key = str(canonical_key or "").strip()
        if not normalized_canonical_key:
            return []
        rows = self._connection.execute(
            """
            SELECT alias_url
            FROM linkedin_profile_registry_aliases
            WHERE profile_url_key = ?
            ORDER BY updated_at DESC
            """,
            (normalized_canonical_key,),
        ).fetchall()
        aliases: list[str] = []
        for row in rows:
            alias_url = str(row["alias_url"] or "").strip()
            if alias_url and alias_url not in aliases:
                aliases.append(alias_url)
        return aliases

    def _upsert_linkedin_profile_registry_aliases_locked(
        self,
        *,
        canonical_key: str,
        canonical_profile_url: str,
        alias_urls: list[str],
        alias_kind: str = "observed",
    ) -> int:
        normalized_canonical_key = str(canonical_key or "").strip()
        if not normalized_canonical_key:
            return 0
        normalized_alias_kind = str(alias_kind or "observed").strip() or "observed"
        normalized_alias_urls = _normalize_linkedin_profile_url_list([canonical_profile_url, *list(alias_urls or [])])
        upserted = 0
        for alias_url in normalized_alias_urls:
            alias_key = _normalize_linkedin_profile_url_key(alias_url)
            if not alias_key:
                continue
            self._connection.execute(
                """
                INSERT INTO linkedin_profile_registry_aliases (
                    alias_url_key,
                    profile_url_key,
                    alias_url,
                    alias_kind
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(alias_url_key) DO UPDATE SET
                    profile_url_key = excluded.profile_url_key,
                    alias_url = excluded.alias_url,
                    alias_kind = excluded.alias_kind,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    alias_key,
                    normalized_canonical_key,
                    alias_url,
                    normalized_alias_kind,
                ),
            )
            upserted += 1
        return upserted

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
        row = self._connection.execute(
            """
            SELECT * FROM plan_review_sessions
            WHERE lower(target_company) = lower(?) AND status = 'pending' AND (
                matching_request_signature = ?
                OR (coalesce(matching_request_signature, '') = '' AND request_signature = ?)
            )
            ORDER BY updated_at DESC, review_id DESC
            LIMIT 1
            """,
            (target, request_sig, request_sig),
        ).fetchone()
        if row is None:
            return None
        return self._plan_review_session_from_row(row)

    def record_criteria_feedback(self, payload: dict[str, Any]) -> dict[str, Any]:
        target_company, metadata = self._prepare_feedback_context(payload)
        feedback_type = str(payload.get("feedback_type") or "").strip()
        subject = str(payload.get("subject") or "").strip()
        value = str(payload.get("value") or "").strip()
        reviewer = str(payload.get("reviewer") or "").strip()
        notes = str(payload.get("notes") or "").strip()
        job_id = str(payload.get("job_id") or "").strip()
        candidate_id = str(payload.get("candidate_id") or "").strip()
        payload_json = json.dumps(_json_safe_payload(metadata), ensure_ascii=False)
        feedback_id = 0
        if self._control_plane_postgres_should_prefer_read("criteria_feedback"):
            row = self._call_control_plane_postgres_native(
                "insert_row_with_generated_id",
                table_name="criteria_feedback",
                row={
                    "job_id": job_id,
                    "candidate_id": candidate_id,
                    "target_company": target_company,
                    "feedback_type": feedback_type,
                    "subject": subject,
                    "value": value,
                    "reviewer": reviewer,
                    "notes": notes,
                    "payload_json": payload_json,
                    "created_at": _utc_now_timestamp(),
                },
            )
            feedback_id = int((row or {}).get("feedback_id") or 0)
        if feedback_id <= 0:
            with self._lock, self._connection:
                cursor = self._connection.execute(
                    """
                    INSERT INTO criteria_feedback (
                        job_id, candidate_id, target_company, feedback_type, subject, value, reviewer, notes, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        candidate_id,
                        target_company,
                        feedback_type,
                        subject,
                        value,
                        reviewer,
                        notes,
                        payload_json,
                    ),
                )
                feedback_id = int(cursor.lastrowid)
        derived_patterns = self._derive_patterns_from_feedback(
            feedback_id,
            {
                **payload,
                "target_company": target_company,
                "metadata": metadata,
            },
        )
        return {"feedback_id": feedback_id, "patterns": derived_patterns}

    def list_criteria_feedback(self, target_company: str = "", limit: int = 100) -> list[dict[str, Any]]:
        postgres_rows = self._select_control_plane_rows(
            "criteria_feedback",
            row_builder=self._criteria_feedback_from_row,
            where_sql="lower(target_company) = lower(%s)" if target_company else "",
            params=[target_company] if target_company else [],
            order_by_sql="feedback_id DESC",
            limit=limit,
        )
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("criteria_feedback"):
            return postgres_rows
        if target_company:
            rows = self._connection.execute(
                """
                SELECT * FROM criteria_feedback
                WHERE lower(target_company) = lower(?)
                ORDER BY feedback_id DESC
                LIMIT ?
                """,
                (target_company, limit),
            ).fetchall()
        else:
            rows = self._connection.execute(
                "SELECT * FROM criteria_feedback ORDER BY feedback_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._criteria_feedback_from_row(row) for row in rows]

    def get_criteria_feedback(self, feedback_id: int) -> dict[str, Any] | None:
        if feedback_id <= 0:
            return None
        postgres_row = self._select_control_plane_row(
            "criteria_feedback",
            row_builder=self._criteria_feedback_from_row,
            where_sql="feedback_id = %s",
            params=[feedback_id],
        )
        if postgres_row is not None:
            return postgres_row
        if self._control_plane_postgres_should_skip_sqlite_fallback("criteria_feedback"):
            return None
        row = self._connection.execute(
            "SELECT * FROM criteria_feedback WHERE feedback_id = ? LIMIT 1",
            (feedback_id,),
        ).fetchone()
        if row is None:
            return None
        return self._criteria_feedback_from_row(row)

    def list_criteria_patterns(
        self,
        target_company: str = "",
        *,
        status: str = "active",
        pattern_type: str = "",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            clauses.append("(lower(target_company) = lower(?) OR target_company = '')")
            params.append(target_company)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if pattern_type:
            clauses.append("pattern_type = ?")
            params.append(pattern_type)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        postgres_where_clauses: list[str] = []
        postgres_params: list[Any] = []
        if target_company:
            postgres_where_clauses.append("(lower(target_company) = lower(%s) OR target_company = %s)")
            postgres_params.extend([target_company, ""])
        if status:
            postgres_where_clauses.append("status = %s")
            postgres_params.append(status)
        if pattern_type:
            postgres_where_clauses.append("pattern_type = %s")
            postgres_params.append(pattern_type)
        postgres_rows = self._select_control_plane_rows(
            "criteria_patterns",
            row_builder=self._criteria_pattern_from_row,
            where_sql=" AND ".join(postgres_where_clauses),
            params=postgres_params,
            order_by_sql="updated_at DESC, pattern_id DESC",
            limit=limit,
        )
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("criteria_patterns"):
            return postgres_rows
        rows = self._connection.execute(
            f"""
            SELECT * FROM criteria_patterns
            {where_clause}
            ORDER BY updated_at DESC, pattern_id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [self._criteria_pattern_from_row(row) for row in rows]

    def record_pattern_suggestions(self, suggestions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not suggestions:
            return []
        if self._control_plane_postgres_should_prefer_read("criteria_pattern_suggestions"):
            for item in suggestions:
                target_company = str(item.get("target_company") or "")
                request_sig = str(item.get("request_signature") or "")
                request_family_sig = str(item.get("request_family_signature") or "")
                matching_request_sig = str(item.get("matching_request_signature") or request_sig or "")
                matching_request_family_sig = str(
                    item.get("matching_request_family_signature") or request_family_sig or ""
                )
                source_feedback_id = int(item.get("source_feedback_id") or 0) or None
                source_job_id = str(item.get("source_job_id") or "")
                candidate_id = str(item.get("candidate_id") or "")
                pattern_type = str(item.get("pattern_type") or "")
                subject = str(item.get("subject") or "")
                value = str(item.get("value") or "")
                status = str(item.get("status") or "suggested")
                confidence = str(item.get("confidence") or "medium")
                rationale = str(item.get("rationale") or "")
                evidence_json = json.dumps(item.get("evidence") or {}, ensure_ascii=False)
                metadata_json = json.dumps(item.get("metadata") or {}, ensure_ascii=False)
                existing_rows = self._select_control_plane_rows(
                    "criteria_pattern_suggestions",
                    row_builder=self._criteria_pattern_suggestion_from_row,
                    where_sql=(
                        "lower(target_company) = lower(%s) AND candidate_id = %s AND pattern_type = %s AND subject = %s "
                        "AND value = %s"
                    ),
                    params=[target_company, candidate_id, pattern_type, subject, value],
                    order_by_sql="updated_at DESC, suggestion_id DESC",
                    limit=50,
                )
                existing_row = next(
                    (
                        row
                        for row in existing_rows
                        if (
                            str(row.get("matching_request_family_signature") or "") == matching_request_family_sig
                            or (
                                not str(row.get("matching_request_family_signature") or "").strip()
                                and str(row.get("request_family_signature") or "") == request_family_sig
                            )
                        )
                    ),
                    None,
                )
                now_timestamp = _utc_now_timestamp()
                if existing_row is not None:
                    self._call_control_plane_postgres_native(
                        "update_row_returning",
                        table_name="criteria_pattern_suggestions",
                        id_column="suggestion_id",
                        id_value=int(existing_row.get("suggestion_id") or 0),
                        row={
                            "request_signature": request_sig,
                            "request_family_signature": request_family_sig,
                            "matching_request_signature": matching_request_sig,
                            "matching_request_family_signature": matching_request_family_sig,
                            "source_feedback_id": source_feedback_id,
                            "source_job_id": source_job_id,
                            "status": status,
                            "confidence": confidence,
                            "rationale": rationale,
                            "evidence_json": evidence_json,
                            "metadata_json": metadata_json,
                            "updated_at": now_timestamp,
                        },
                    )
                    continue
                self._call_control_plane_postgres_native(
                    "insert_row_with_generated_id",
                    table_name="criteria_pattern_suggestions",
                    row={
                        "target_company": target_company,
                        "request_signature": request_sig,
                        "request_family_signature": request_family_sig,
                        "matching_request_signature": matching_request_sig,
                        "matching_request_family_signature": matching_request_family_sig,
                        "source_feedback_id": source_feedback_id,
                        "source_job_id": source_job_id,
                        "candidate_id": candidate_id,
                        "pattern_type": pattern_type,
                        "subject": subject,
                        "value": value,
                        "status": status,
                        "confidence": confidence,
                        "rationale": rationale,
                        "evidence_json": evidence_json,
                        "metadata_json": metadata_json,
                        "created_at": now_timestamp,
                        "updated_at": now_timestamp,
                    },
                )
            source_feedback_ids = [
                int(item.get("source_feedback_id") or 0) for item in suggestions if int(item.get("source_feedback_id") or 0)
            ]
            if not source_feedback_ids:
                return []
            return self.list_pattern_suggestions(source_feedback_id=max(source_feedback_ids), limit=20)
        with self._lock, self._connection:
            for item in suggestions:
                target_company = str(item.get("target_company") or "")
                request_sig = str(item.get("request_signature") or "")
                request_family_sig = str(item.get("request_family_signature") or "")
                matching_request_sig = str(item.get("matching_request_signature") or request_sig or "")
                matching_request_family_sig = str(
                    item.get("matching_request_family_signature") or request_family_sig or ""
                )
                source_feedback_id = int(item.get("source_feedback_id") or 0) or None
                source_job_id = str(item.get("source_job_id") or "")
                candidate_id = str(item.get("candidate_id") or "")
                pattern_type = str(item.get("pattern_type") or "")
                subject = str(item.get("subject") or "")
                value = str(item.get("value") or "")
                status = str(item.get("status") or "suggested")
                confidence = str(item.get("confidence") or "medium")
                rationale = str(item.get("rationale") or "")
                evidence_json = json.dumps(item.get("evidence") or {}, ensure_ascii=False)
                metadata_json = json.dumps(item.get("metadata") or {}, ensure_ascii=False)
                existing_row = self._connection.execute(
                    """
                    SELECT suggestion_id
                    FROM criteria_pattern_suggestions
                    WHERE lower(target_company) = lower(?)
                      AND candidate_id = ?
                      AND pattern_type = ?
                      AND subject = ?
                      AND value = ?
                      AND (
                        matching_request_family_signature = ?
                        OR (
                            coalesce(matching_request_family_signature, '') = ''
                            AND request_family_signature = ?
                        )
                      )
                    LIMIT 1
                    """,
                    (
                        target_company,
                        candidate_id,
                        pattern_type,
                        subject,
                        value,
                        matching_request_family_sig,
                        request_family_sig,
                    ),
                ).fetchone()
                if existing_row is not None:
                    self._connection.execute(
                        """
                        UPDATE criteria_pattern_suggestions
                        SET request_signature = ?,
                            request_family_signature = ?,
                            matching_request_signature = ?,
                            matching_request_family_signature = ?,
                            source_feedback_id = ?,
                            source_job_id = ?,
                            status = ?,
                            confidence = ?,
                            rationale = ?,
                            evidence_json = ?,
                            metadata_json = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE suggestion_id = ?
                        """,
                        (
                            request_sig,
                            request_family_sig,
                            matching_request_sig,
                            matching_request_family_sig,
                            source_feedback_id,
                            source_job_id,
                            status,
                            confidence,
                            rationale,
                            evidence_json,
                            metadata_json,
                            int(existing_row["suggestion_id"] or 0),
                        ),
                    )
                    continue
                try:
                    self._connection.execute(
                        """
                        INSERT INTO criteria_pattern_suggestions (
                            target_company, request_signature, request_family_signature,
                            matching_request_signature, matching_request_family_signature,
                            source_feedback_id, source_job_id, candidate_id,
                            pattern_type, subject, value, status, confidence, rationale, evidence_json, metadata_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            target_company,
                            request_sig,
                            request_family_sig,
                            matching_request_sig,
                            matching_request_family_sig,
                            source_feedback_id,
                            source_job_id,
                            candidate_id,
                            pattern_type,
                            subject,
                            value,
                            status,
                            confidence,
                            rationale,
                            evidence_json,
                            metadata_json,
                        ),
                    )
                except sqlite3.IntegrityError:
                    self._connection.execute(
                        """
                        UPDATE criteria_pattern_suggestions
                        SET request_signature = ?,
                            request_family_signature = ?,
                            matching_request_signature = ?,
                            matching_request_family_signature = ?,
                            source_feedback_id = ?,
                            source_job_id = ?,
                            status = ?,
                            confidence = ?,
                            rationale = ?,
                            evidence_json = ?,
                            metadata_json = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE lower(target_company) = lower(?)
                          AND request_family_signature = ?
                          AND candidate_id = ?
                          AND pattern_type = ?
                          AND subject = ?
                          AND value = ?
                        """,
                        (
                            request_sig,
                            request_family_sig,
                            matching_request_sig,
                            matching_request_family_sig,
                            source_feedback_id,
                            source_job_id,
                            status,
                            confidence,
                            rationale,
                            evidence_json,
                            metadata_json,
                            target_company,
                            request_family_sig,
                            candidate_id,
                            pattern_type,
                            subject,
                            value,
                        ),
                    )
        source_feedback_ids = [
            int(item.get("source_feedback_id") or 0) for item in suggestions if int(item.get("source_feedback_id") or 0)
        ]
        if not source_feedback_ids:
            return []
        return self.list_pattern_suggestions(source_feedback_id=max(source_feedback_ids), limit=20)

    def list_pattern_suggestions(
        self,
        *,
        target_company: str = "",
        status: str = "",
        source_feedback_id: int = 0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            clauses.append("lower(target_company) = lower(?)")
            params.append(target_company)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if source_feedback_id:
            clauses.append("source_feedback_id = ?")
            params.append(source_feedback_id)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        postgres_rows = self._select_control_plane_rows(
            "criteria_pattern_suggestions",
            row_builder=self._criteria_pattern_suggestion_from_row,
            where_sql=" AND ".join(
                [
                    *(["lower(target_company) = lower(%s)"] if target_company else []),
                    *(["status = %s"] if status else []),
                    *(["source_feedback_id = %s"] if source_feedback_id else []),
                ]
            ),
            params=[
                *([target_company] if target_company else []),
                *([status] if status else []),
                *([source_feedback_id] if source_feedback_id else []),
            ],
            order_by_sql="updated_at DESC, suggestion_id DESC",
            limit=limit,
        )
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("criteria_pattern_suggestions"):
            return postgres_rows
        rows = self._connection.execute(
            f"""
            SELECT * FROM criteria_pattern_suggestions
            {where_clause}
            ORDER BY updated_at DESC, suggestion_id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [self._criteria_pattern_suggestion_from_row(row) for row in rows]

    def get_pattern_suggestion(self, suggestion_id: int) -> dict[str, Any] | None:
        if suggestion_id <= 0:
            return None
        postgres_row = self._select_control_plane_row(
            "criteria_pattern_suggestions",
            row_builder=self._criteria_pattern_suggestion_from_row,
            where_sql="suggestion_id = %s",
            params=[suggestion_id],
        )
        if postgres_row is not None:
            return postgres_row
        if self._control_plane_postgres_should_skip_sqlite_fallback("criteria_pattern_suggestions"):
            return None
        row = self._connection.execute(
            "SELECT * FROM criteria_pattern_suggestions WHERE suggestion_id = ? LIMIT 1",
            (suggestion_id,),
        ).fetchone()
        if row is None:
            return None
        return self._criteria_pattern_suggestion_from_row(row)

    def review_pattern_suggestion(
        self,
        *,
        suggestion_id: int,
        action: str,
        reviewer: str = "",
        notes: str = "",
    ) -> dict[str, Any] | None:
        suggestion = self.get_pattern_suggestion(suggestion_id)
        if suggestion is None:
            return None
        normalized_action = str(action or "").strip().lower()
        if normalized_action in {"approve", "approved", "apply", "applied"}:
            status = "applied"
        elif normalized_action in {"reject", "rejected"}:
            status = "rejected"
        else:
            status = "suggested"
        applied_pattern = None
        applied_pattern_id = 0
        if status == "applied":
            metadata = dict(suggestion.get("metadata") or {})
            metadata.update(
                {
                    "source_suggestion_id": suggestion_id,
                    "reviewed_by": reviewer,
                    "review_notes": notes,
                    "suggestion_status": "applied",
                }
            )
            applied_pattern = self.upsert_criteria_pattern(
                target_company=str(suggestion.get("target_company") or ""),
                pattern_type=str(suggestion.get("pattern_type") or ""),
                subject=str(suggestion.get("subject") or ""),
                value=str(suggestion.get("value") or ""),
                status="active",
                confidence=str(suggestion.get("confidence") or "medium"),
                source_feedback_id=int(suggestion.get("source_feedback_id") or 0),
                metadata=metadata,
            )
            applied_pattern_id = int((applied_pattern or {}).get("pattern_id") or 0)
        reviewed: dict[str, Any] | None = None
        if self._control_plane_postgres_should_prefer_read("criteria_pattern_suggestions"):
            updated_row = self._call_control_plane_postgres_native(
                "update_row_returning",
                table_name="criteria_pattern_suggestions",
                id_column="suggestion_id",
                id_value=suggestion_id,
                row={
                    "status": status,
                    "reviewed_by": reviewer,
                    "review_notes": notes,
                    "applied_pattern_id": applied_pattern_id or None,
                    "reviewed_at": _utc_now_timestamp(),
                    "updated_at": _utc_now_timestamp(),
                },
            )
            if updated_row is not None:
                reviewed = self._criteria_pattern_suggestion_from_row(updated_row)
        if reviewed is None:
            with self._lock, self._connection:
                self._connection.execute(
                    """
                    UPDATE criteria_pattern_suggestions
                    SET status = ?, reviewed_by = ?, review_notes = ?, applied_pattern_id = ?, reviewed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE suggestion_id = ?
                    """,
                    (
                        status,
                        reviewer,
                        notes,
                        applied_pattern_id or None,
                        suggestion_id,
                    ),
                )
            reviewed = self.get_pattern_suggestion(suggestion_id)
        return {
            "suggestion": reviewed,
            "applied_pattern": applied_pattern,
            "action": normalized_action or status,
            "status": status,
        }

    def record_criteria_result_diff(
        self,
        *,
        target_company: str,
        trigger_feedback_id: int,
        criteria_version_id: int,
        baseline_job_id: str,
        rerun_job_id: str,
        summary_payload: dict[str, Any],
        diff_payload: dict[str, Any],
        artifact_path: str = "",
    ) -> dict[str, Any]:
        diff_id = 0
        if self._control_plane_postgres_should_prefer_read("criteria_result_diffs"):
            row = self._call_control_plane_postgres_native(
                "insert_row_with_generated_id",
                table_name="criteria_result_diffs",
                row={
                    "target_company": target_company,
                    "trigger_feedback_id": trigger_feedback_id or None,
                    "criteria_version_id": criteria_version_id or None,
                    "baseline_job_id": baseline_job_id,
                    "rerun_job_id": rerun_job_id,
                    "summary_json": json.dumps(summary_payload, ensure_ascii=False),
                    "diff_json": json.dumps(diff_payload, ensure_ascii=False),
                    "artifact_path": artifact_path,
                    "created_at": _utc_now_timestamp(),
                },
            )
            diff_id = int((row or {}).get("diff_id") or 0)
        if diff_id <= 0:
            with self._lock, self._connection:
                cursor = self._connection.execute(
                    """
                    INSERT INTO criteria_result_diffs (
                        target_company, trigger_feedback_id, criteria_version_id, baseline_job_id, rerun_job_id,
                        summary_json, diff_json, artifact_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        target_company,
                        trigger_feedback_id or None,
                        criteria_version_id or None,
                        baseline_job_id,
                        rerun_job_id,
                        json.dumps(summary_payload, ensure_ascii=False),
                        json.dumps(diff_payload, ensure_ascii=False),
                        artifact_path,
                    ),
                )
                diff_id = int(cursor.lastrowid)
        return {"diff_id": diff_id}

    def record_confidence_policy_run(
        self,
        *,
        target_company: str,
        job_id: str,
        criteria_version_id: int,
        trigger_feedback_id: int,
        policy_payload: dict[str, Any],
    ) -> dict[str, Any]:
        policy_run_id = 0
        row_payload = {
            "target_company": target_company,
            "job_id": job_id,
            "criteria_version_id": criteria_version_id or None,
            "trigger_feedback_id": trigger_feedback_id or None,
            "request_signature": str(policy_payload.get("request_signature") or ""),
            "request_family_signature": str(policy_payload.get("request_family_signature") or ""),
            "matching_request_signature": str(
                policy_payload.get("matching_request_signature") or policy_payload.get("request_signature") or ""
            ),
            "matching_request_family_signature": str(
                policy_payload.get("matching_request_family_signature")
                or policy_payload.get("request_family_signature")
                or ""
            ),
            "scope_kind": str(policy_payload.get("scope_kind") or "company"),
            "high_threshold": float(policy_payload.get("high_threshold") or 0.0),
            "medium_threshold": float(policy_payload.get("medium_threshold") or 0.0),
            "summary_json": json.dumps(policy_payload.get("summary") or {}, ensure_ascii=False),
            "policy_json": json.dumps(policy_payload, ensure_ascii=False),
            "created_at": _utc_now_timestamp(),
        }
        if self._control_plane_postgres_should_prefer_read("confidence_policy_runs"):
            row = self._call_control_plane_postgres_native(
                "insert_row_with_generated_id",
                table_name="confidence_policy_runs",
                row=row_payload,
            )
            policy_run_id = int((row or {}).get("policy_run_id") or 0)
        if policy_run_id <= 0:
            with self._lock, self._connection:
                cursor = self._connection.execute(
                    """
                    INSERT INTO confidence_policy_runs (
                        target_company, job_id, criteria_version_id, trigger_feedback_id,
                        request_signature, request_family_signature,
                        matching_request_signature, matching_request_family_signature,
                        scope_kind,
                        high_threshold, medium_threshold, summary_json, policy_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        target_company,
                        job_id,
                        criteria_version_id or None,
                        trigger_feedback_id or None,
                        str(policy_payload.get("request_signature") or ""),
                        str(policy_payload.get("request_family_signature") or ""),
                        str(
                            policy_payload.get("matching_request_signature")
                            or policy_payload.get("request_signature")
                            or ""
                        ),
                        str(
                            policy_payload.get("matching_request_family_signature")
                            or policy_payload.get("request_family_signature")
                            or ""
                        ),
                        str(policy_payload.get("scope_kind") or "company"),
                        float(policy_payload.get("high_threshold") or 0.0),
                        float(policy_payload.get("medium_threshold") or 0.0),
                        json.dumps(policy_payload.get("summary") or {}, ensure_ascii=False),
                        json.dumps(policy_payload, ensure_ascii=False),
                    ),
                )
                policy_run_id = int(cursor.lastrowid)
        return {"policy_run_id": policy_run_id}

    def list_confidence_policy_runs(self, target_company: str = "", limit: int = 50) -> list[dict[str, Any]]:
        postgres_rows = self._select_control_plane_rows(
            "confidence_policy_runs",
            row_builder=self._confidence_policy_run_from_row,
            where_sql="lower(target_company) = lower(%s)" if target_company else "",
            params=[target_company] if target_company else [],
            order_by_sql="policy_run_id DESC",
            limit=limit,
        )
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("confidence_policy_runs"):
            return postgres_rows
        if target_company:
            rows = self._connection.execute(
                """
                SELECT * FROM confidence_policy_runs
                WHERE lower(target_company) = lower(?)
                ORDER BY policy_run_id DESC
                LIMIT ?
                """,
                (target_company, limit),
            ).fetchall()
        else:
            rows = self._connection.execute(
                "SELECT * FROM confidence_policy_runs ORDER BY policy_run_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._confidence_policy_run_from_row(row) for row in rows]

    def create_confidence_policy_control(
        self,
        *,
        target_company: str,
        request_payload: dict[str, Any],
        scope_kind: str,
        control_mode: str,
        high_threshold: float,
        medium_threshold: float,
        reviewer: str = "",
        notes: str = "",
        locked_policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        signature_context = _request_signature_context(request_payload)
        request_sig = str(signature_context.get("request_signature") or "")
        request_family_sig = str(signature_context.get("request_family_signature") or "")
        matching_request_sig = str(signature_context.get("matching_request_signature") or request_sig)
        matching_request_family_sig = str(
            signature_context.get("matching_request_family_signature") or request_family_sig
        )
        normalized_scope = str(scope_kind or "request_family").strip() or "request_family"
        deactivation_params: list[Any] = [target_company, normalized_scope]
        clauses = ["lower(target_company) = lower(?)", "scope_kind = ?", "status = 'active'"]
        if normalized_scope == "request_exact":
            clauses.append(
                "(matching_request_signature = ? OR (coalesce(matching_request_signature, '') = '' AND request_signature = ?))"
            )
            deactivation_params.extend([matching_request_sig, request_sig])
        elif normalized_scope == "request_family":
            clauses.append(
                "("
                "matching_request_family_signature = ? "
                "OR (coalesce(matching_request_family_signature, '') = '' AND request_family_signature = ?)"
                ")"
            )
            deactivation_params.extend([matching_request_family_sig, request_family_sig])
        if self._control_plane_postgres_should_prefer_read("confidence_policy_controls"):
            existing_rows = self._select_control_plane_rows(
                "confidence_policy_controls",
                row_builder=self._confidence_policy_control_from_row,
                where_sql="lower(target_company) = lower(%s) AND status = %s",
                params=[target_company, "active"],
                order_by_sql="updated_at DESC, control_id DESC",
                limit=0,
            )
            for existing_row in existing_rows:
                if not _confidence_policy_control_matches_scope(
                    existing_row,
                    scope_kind=normalized_scope,
                    request_signature=request_sig,
                    request_family_signature=request_family_sig,
                    matching_request_signature=matching_request_sig,
                    matching_request_family_signature=matching_request_family_sig,
                ):
                    continue
                self._call_control_plane_postgres_native(
                    "update_row_returning",
                    table_name="confidence_policy_controls",
                    id_column="control_id",
                    id_value=int(existing_row.get("control_id") or 0),
                    row={
                        "status": "inactive",
                        "updated_at": _utc_now_timestamp(),
                    },
                )
            now = _utc_now_timestamp()
            row = self._call_control_plane_postgres_native(
                "insert_row_with_generated_id",
                table_name="confidence_policy_controls",
                row={
                    "target_company": target_company,
                    "request_signature": request_sig,
                    "request_family_signature": request_family_sig,
                    "matching_request_signature": matching_request_sig,
                    "matching_request_family_signature": matching_request_family_sig,
                    "scope_kind": normalized_scope,
                    "control_mode": str(control_mode or "override"),
                    "status": "active",
                    "high_threshold": float(high_threshold),
                    "medium_threshold": float(medium_threshold),
                    "reviewer": reviewer,
                    "notes": notes,
                    "locked_policy_json": json.dumps(locked_policy or {}, ensure_ascii=False),
                    "created_at": now,
                    "updated_at": now,
                },
            )
            if row is not None:
                return self._confidence_policy_control_from_row(row)
        with self._lock, self._connection:
            self._connection.execute(
                f"""
                UPDATE confidence_policy_controls
                SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
                WHERE {" AND ".join(clauses)}
                """,
                tuple(deactivation_params),
            )
            cursor = self._connection.execute(
                """
                INSERT INTO confidence_policy_controls (
                    target_company, request_signature, request_family_signature,
                    matching_request_signature, matching_request_family_signature,
                    scope_kind, control_mode, status,
                    high_threshold, medium_threshold, reviewer, notes, locked_policy_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)
                """,
                (
                    target_company,
                    request_sig,
                    request_family_sig,
                    matching_request_sig,
                    matching_request_family_sig,
                    normalized_scope,
                    str(control_mode or "override"),
                    float(high_threshold),
                    float(medium_threshold),
                    reviewer,
                    notes,
                    json.dumps(locked_policy or {}, ensure_ascii=False),
                ),
            )
            control_id = int(cursor.lastrowid)
        self._replace_control_plane_table_from_sqlite("confidence_policy_controls")
        return self.get_confidence_policy_control(control_id) or {}

    def find_active_confidence_policy_control(
        self,
        *,
        target_company: str,
        request_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        signature_context = _request_signature_context(request_payload)
        request_sig = str(signature_context.get("request_signature") or "")
        request_family_sig = str(signature_context.get("request_family_signature") or "")
        matching_request_sig = str(signature_context.get("matching_request_signature") or request_sig)
        matching_request_family_sig = str(
            signature_context.get("matching_request_family_signature") or request_family_sig
        )
        rows = self.list_confidence_policy_controls(target_company=target_company, status="active", limit=500)
        best: dict[str, Any] | None = None
        best_rank = -1
        for row in rows:
            scope_kind = str(row.get("scope_kind") or "")
            rank = -1
            selection_reason = ""
            row_matching_request_sig = str(row.get("matching_request_signature") or "").strip()
            row_matching_request_family_sig = str(row.get("matching_request_family_signature") or "").strip()
            if scope_kind == "request_exact" and (
                (matching_request_sig and row_matching_request_sig == matching_request_sig)
                or (
                    not row_matching_request_sig
                    and request_sig
                    and str(row.get("request_signature") or "") == request_sig
                )
            ):
                rank = 3
                selection_reason = "exact_request_control"
            elif scope_kind == "request_family" and (
                (matching_request_family_sig and row_matching_request_family_sig == matching_request_family_sig)
                or (
                    not row_matching_request_family_sig
                    and request_family_sig
                    and str(row.get("request_family_signature") or "") == request_family_sig
                )
            ):
                rank = 2
                selection_reason = "request_family_control"
            elif scope_kind == "company":
                rank = 1
                selection_reason = "company_control"
            if rank <= best_rank:
                continue
            payload = dict(row)
            payload["selection_reason"] = selection_reason
            best = payload
            best_rank = rank
        return best

    def get_confidence_policy_control(self, control_id: int) -> dict[str, Any] | None:
        if control_id <= 0:
            return None
        postgres_row = self._select_control_plane_row(
            "confidence_policy_controls",
            row_builder=self._confidence_policy_control_from_row,
            where_sql="control_id = %s",
            params=[control_id],
        )
        if postgres_row is not None:
            return postgres_row
        if self._control_plane_postgres_should_skip_sqlite_fallback("confidence_policy_controls"):
            return None
        row = self._connection.execute(
            "SELECT * FROM confidence_policy_controls WHERE control_id = ? LIMIT 1",
            (control_id,),
        ).fetchone()
        if row is None:
            return None
        return self._confidence_policy_control_from_row(row)

    def list_confidence_policy_controls(
        self,
        *,
        target_company: str = "",
        status: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            clauses.append("lower(target_company) = lower(?)")
            params.append(target_company)
        if status:
            clauses.append("status = ?")
            params.append(status)
        postgres_rows = self._select_control_plane_rows(
            "confidence_policy_controls",
            row_builder=self._confidence_policy_control_from_row,
            where_sql=" AND ".join(
                [
                    *(["lower(target_company) = lower(%s)"] if target_company else []),
                    *(["status = %s"] if status else []),
                ]
            ),
            params=[
                *([target_company] if target_company else []),
                *([status] if status else []),
            ],
            order_by_sql="updated_at DESC, control_id DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        if self._control_plane_postgres_should_skip_sqlite_fallback("confidence_policy_controls"):
            return []
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection.execute(
            f"""
            SELECT * FROM confidence_policy_controls
            {where_clause}
            ORDER BY updated_at DESC, control_id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [self._confidence_policy_control_from_row(row) for row in rows]

    def deactivate_confidence_policy_control(
        self,
        *,
        control_id: int = 0,
        target_company: str = "",
        request_payload: dict[str, Any] | None = None,
        scope_kind: str = "",
    ) -> dict[str, Any]:
        if control_id:
            if self._control_plane_postgres_should_prefer_read("confidence_policy_controls"):
                row = self._call_control_plane_postgres_native(
                    "update_row_returning",
                    table_name="confidence_policy_controls",
                    id_column="control_id",
                    id_value=control_id,
                    row={
                        "status": "inactive",
                        "updated_at": _utc_now_timestamp(),
                    },
                )
                return {"deactivated_count": 1 if row is not None else 0}
            with self._lock, self._connection:
                self._connection.execute(
                    """
                    UPDATE confidence_policy_controls
                    SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
                    WHERE control_id = ?
                    """,
                    (control_id,),
                )
            self._replace_control_plane_table_from_sqlite("confidence_policy_controls")
            return {"deactivated_count": 1 if self.get_confidence_policy_control(control_id) else 0}
        clauses = ["lower(target_company) = lower(?)", "status = 'active'"]
        params: list[Any] = [target_company]
        normalized_scope = str(scope_kind or "request_family").strip() or "request_family"
        clauses.append("scope_kind = ?")
        params.append(normalized_scope)
        request_payload = request_payload or {}
        signature_context = _request_signature_context(request_payload)
        if normalized_scope == "request_exact":
            clauses.append(
                "(matching_request_signature = ? OR (coalesce(matching_request_signature, '') = '' AND request_signature = ?))"
            )
            params.extend(
                [
                    str(signature_context.get("matching_request_signature") or ""),
                    str(signature_context.get("request_signature") or ""),
                ]
            )
        elif normalized_scope == "request_family":
            clauses.append(
                "("
                "matching_request_family_signature = ? "
                "OR (coalesce(matching_request_family_signature, '') = '' AND request_family_signature = ?)"
                ")"
            )
            params.extend(
                [
                    str(signature_context.get("matching_request_family_signature") or ""),
                    str(signature_context.get("request_family_signature") or ""),
                ]
            )
        if self._control_plane_postgres_should_prefer_read("confidence_policy_controls"):
            existing_rows = self._select_control_plane_rows(
                "confidence_policy_controls",
                row_builder=self._confidence_policy_control_from_row,
                where_sql="lower(target_company) = lower(%s) AND status = %s",
                params=[target_company, "active"],
                order_by_sql="updated_at DESC, control_id DESC",
                limit=0,
            )
            deactivated_count = 0
            for existing_row in existing_rows:
                if not _confidence_policy_control_matches_scope(
                    existing_row,
                    scope_kind=normalized_scope,
                    request_signature=str(signature_context.get("request_signature") or ""),
                    request_family_signature=str(signature_context.get("request_family_signature") or ""),
                    matching_request_signature=str(signature_context.get("matching_request_signature") or ""),
                    matching_request_family_signature=str(
                        signature_context.get("matching_request_family_signature") or ""
                    ),
                ):
                    continue
                row = self._call_control_plane_postgres_native(
                    "update_row_returning",
                    table_name="confidence_policy_controls",
                    id_column="control_id",
                    id_value=int(existing_row.get("control_id") or 0),
                    row={
                        "status": "inactive",
                        "updated_at": _utc_now_timestamp(),
                    },
                )
                if row is not None:
                    deactivated_count += 1
            return {"deactivated_count": deactivated_count}
        with self._lock, self._connection:
            cursor = self._connection.execute(
                f"""
                UPDATE confidence_policy_controls
                SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
                WHERE {" AND ".join(clauses)}
                """,
                tuple(params),
            )
        self._replace_control_plane_table_from_sqlite("confidence_policy_controls")
        return {"deactivated_count": int(cursor.rowcount or 0)}

    def list_criteria_result_diffs(self, target_company: str = "", limit: int = 50) -> list[dict[str, Any]]:
        postgres_rows = self._select_control_plane_rows(
            "criteria_result_diffs",
            row_builder=self._criteria_result_diff_from_row,
            where_sql="lower(target_company) = lower(%s)" if target_company else "",
            params=[target_company] if target_company else [],
            order_by_sql="diff_id DESC",
            limit=limit,
        )
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("criteria_result_diffs"):
            return postgres_rows
        if target_company:
            rows = self._connection.execute(
                """
                SELECT * FROM criteria_result_diffs
                WHERE lower(target_company) = lower(?)
                ORDER BY diff_id DESC
                LIMIT ?
                """,
                (target_company, limit),
            ).fetchall()
        else:
            rows = self._connection.execute(
                "SELECT * FROM criteria_result_diffs ORDER BY diff_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._criteria_result_diff_from_row(row) for row in rows]

    def create_criteria_version(
        self,
        *,
        target_company: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        patterns: list[dict[str, Any]],
        source_kind: str,
        parent_version_id: int = 0,
        trigger_feedback_id: int = 0,
        evolution_stage: str = "planned",
        notes: str = "",
    ) -> dict[str, Any]:
        request_signature = _payload_signature({"target_company": target_company, "request": request_payload})
        version_id = 0
        row_payload = {
            "target_company": target_company,
            "request_signature": request_signature,
            "source_kind": source_kind,
            "parent_version_id": parent_version_id or None,
            "trigger_feedback_id": trigger_feedback_id or None,
            "evolution_stage": evolution_stage,
            "request_json": json.dumps(request_payload, ensure_ascii=False),
            "plan_json": json.dumps(plan_payload, ensure_ascii=False),
            "patterns_json": json.dumps(patterns, ensure_ascii=False),
            "notes": notes,
            "created_at": _utc_now_timestamp(),
        }
        if self._control_plane_postgres_should_prefer_read("criteria_versions"):
            row = self._call_control_plane_postgres_native(
                "insert_row_with_generated_id",
                table_name="criteria_versions",
                row=row_payload,
            )
            version_id = int((row or {}).get("version_id") or 0)
        if version_id <= 0:
            with self._lock, self._connection:
                cursor = self._connection.execute(
                    """
                    INSERT INTO criteria_versions (
                        target_company, request_signature, source_kind, parent_version_id, trigger_feedback_id,
                        evolution_stage, request_json, plan_json, patterns_json, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        target_company,
                        request_signature,
                        source_kind,
                        parent_version_id or None,
                        trigger_feedback_id or None,
                        evolution_stage,
                        json.dumps(request_payload, ensure_ascii=False),
                        json.dumps(plan_payload, ensure_ascii=False),
                        json.dumps(patterns, ensure_ascii=False),
                        notes,
                    ),
                )
                version_id = int(cursor.lastrowid)
        return {"version_id": version_id, "request_signature": request_signature}

    def record_criteria_compiler_run(
        self,
        *,
        version_id: int,
        job_id: str,
        trigger_feedback_id: int = 0,
        provider_name: str,
        compiler_kind: str,
        status: str,
        input_payload: dict[str, Any],
        output_payload: dict[str, Any],
        notes: str = "",
    ) -> dict[str, Any]:
        compiler_run_id = 0
        row_payload = {
            "version_id": version_id,
            "job_id": job_id,
            "trigger_feedback_id": trigger_feedback_id or None,
            "provider_name": provider_name,
            "compiler_kind": compiler_kind,
            "status": status,
            "input_json": json.dumps(input_payload, ensure_ascii=False),
            "output_json": json.dumps(output_payload, ensure_ascii=False),
            "notes": notes,
            "created_at": _utc_now_timestamp(),
        }
        if self._control_plane_postgres_should_prefer_read("criteria_compiler_runs"):
            row = self._call_control_plane_postgres_native(
                "insert_row_with_generated_id",
                table_name="criteria_compiler_runs",
                row=row_payload,
            )
            compiler_run_id = int((row or {}).get("compiler_run_id") or 0)
        if compiler_run_id <= 0:
            with self._lock, self._connection:
                cursor = self._connection.execute(
                    """
                    INSERT INTO criteria_compiler_runs (
                        version_id, job_id, trigger_feedback_id, provider_name, compiler_kind, status, input_json, output_json, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        version_id,
                        job_id,
                        trigger_feedback_id or None,
                        provider_name,
                        compiler_kind,
                        status,
                        json.dumps(input_payload, ensure_ascii=False),
                        json.dumps(output_payload, ensure_ascii=False),
                        notes,
                    ),
                )
                compiler_run_id = int(cursor.lastrowid)
        return {"compiler_run_id": compiler_run_id}

    def list_criteria_versions(self, target_company: str = "", limit: int = 50) -> list[dict[str, Any]]:
        postgres_rows = self._select_control_plane_rows(
            "criteria_versions",
            row_builder=self._criteria_version_from_row,
            where_sql="lower(target_company) = lower(%s)" if target_company else "",
            params=[target_company] if target_company else [],
            order_by_sql="version_id DESC",
            limit=limit,
        )
        if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("criteria_versions"):
            return postgres_rows
        if target_company:
            rows = self._connection.execute(
                """
                SELECT * FROM criteria_versions
                WHERE lower(target_company) = lower(?)
                ORDER BY version_id DESC
                LIMIT ?
                """,
                (target_company, limit),
            ).fetchall()
        else:
            rows = self._connection.execute(
                "SELECT * FROM criteria_versions ORDER BY version_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._criteria_version_from_row(row) for row in rows]

    def get_criteria_version(self, version_id: int) -> dict[str, Any] | None:
        if version_id <= 0:
            return None
        postgres_row = self._select_control_plane_row(
            "criteria_versions",
            row_builder=self._criteria_version_from_row,
            where_sql="version_id = %s",
            params=[version_id],
        )
        if postgres_row is not None:
            return postgres_row
        if self._control_plane_postgres_should_skip_sqlite_fallback("criteria_versions"):
            return None
        row = self._connection.execute(
            "SELECT * FROM criteria_versions WHERE version_id = ?",
            (version_id,),
        ).fetchone()
        if row is None:
            return None
        return self._criteria_version_from_row(row)

    def list_criteria_compiler_runs(
        self, version_id: int = 0, target_company: str = "", limit: int = 100
    ) -> list[dict[str, Any]]:
        if self._control_plane_postgres_should_prefer_read("criteria_compiler_runs"):
            if version_id:
                postgres_rows = self._select_control_plane_rows(
                    "criteria_compiler_runs",
                    row_builder=self._criteria_compiler_run_from_row,
                    where_sql="version_id = %s",
                    params=[version_id],
                    order_by_sql="compiler_run_id DESC",
                    limit=limit,
                )
                if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("criteria_compiler_runs"):
                    return postgres_rows
            elif target_company:
                version_rows = self._select_control_plane_rows(
                    "criteria_versions",
                    row_builder=self._criteria_version_from_row,
                    where_sql="lower(target_company) = lower(%s)",
                    params=[target_company],
                    order_by_sql="version_id DESC",
                    limit=0,
                )
                version_ids = {int(row.get("version_id") or 0) for row in version_rows if int(row.get("version_id") or 0)}
                compiler_rows = self._select_control_plane_rows(
                    "criteria_compiler_runs",
                    row_builder=self._criteria_compiler_run_from_row,
                    order_by_sql="compiler_run_id DESC",
                    limit=0,
                )
                filtered_rows = [
                    row for row in compiler_rows if int(row.get("version_id") or 0) in version_ids
                ][: max(0, int(limit or 0)) or len(compiler_rows)]
                if filtered_rows or self._control_plane_postgres_should_skip_sqlite_fallback("criteria_compiler_runs"):
                    return filtered_rows
            else:
                postgres_rows = self._select_control_plane_rows(
                    "criteria_compiler_runs",
                    row_builder=self._criteria_compiler_run_from_row,
                    order_by_sql="compiler_run_id DESC",
                    limit=limit,
                )
                if postgres_rows or self._control_plane_postgres_should_skip_sqlite_fallback("criteria_compiler_runs"):
                    return postgres_rows
        if version_id:
            rows = self._connection.execute(
                """
                SELECT * FROM criteria_compiler_runs
                WHERE version_id = ?
                ORDER BY compiler_run_id DESC
                LIMIT ?
                """,
                (version_id, limit),
            ).fetchall()
        elif target_company:
            rows = self._connection.execute(
                """
                SELECT ccr.*
                FROM criteria_compiler_runs ccr
                JOIN criteria_versions cv ON cv.version_id = ccr.version_id
                WHERE lower(cv.target_company) = lower(?)
                ORDER BY ccr.compiler_run_id DESC
                LIMIT ?
                """,
                (target_company, limit),
            ).fetchall()
        else:
            rows = self._connection.execute(
                "SELECT * FROM criteria_compiler_runs ORDER BY compiler_run_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._criteria_compiler_run_from_row(row) for row in rows]

    def get_latest_criteria_version(self, target_company: str = "") -> dict[str, Any] | None:
        versions = self.list_criteria_versions(target_company=target_company, limit=1)
        return versions[0] if versions else None

    def _derive_patterns_from_feedback(self, feedback_id: int, payload: dict[str, Any]) -> list[dict[str, Any]]:
        feedback_type = str(payload.get("feedback_type") or "").strip()
        target_company = str(payload.get("target_company") or "").strip()
        subject = str(payload.get("subject") or "").strip()
        value = str(payload.get("value") or "").strip()
        metadata = dict(payload.get("metadata") or {})
        mappings = {
            "accepted_alias": [("alias", "active", "high")],
            "rejected_alias": [("alias", "disabled", "high")],
            "must_have_signal": [("must_signal", "active", "high"), ("confidence_boost", "active", "high")],
            "exclude_signal": [("exclude_signal", "active", "high"), ("confidence_penalty", "active", "high")],
            "false_positive_pattern": [("exclude_signal", "active", "high"), ("confidence_penalty", "active", "high")],
            "false_negative_pattern": [("must_signal", "active", "high"), ("confidence_boost", "active", "high")],
            "confidence_boost_signal": [("confidence_boost", "active", "high")],
            "confidence_penalty_signal": [("confidence_penalty", "active", "high")],
        }
        if feedback_type not in mappings or not subject or not value:
            return []
        created: list[dict[str, Any]] = []
        for pattern_type, _, _ in mappings[feedback_type]:
            pattern_status, pattern_confidence = next(
                (status, confidence)
                for mapped_type, status, confidence in mappings[feedback_type]
                if mapped_type == pattern_type
            )
            pattern = self.upsert_criteria_pattern(
                target_company=target_company,
                pattern_type=pattern_type,
                subject=subject,
                value=value,
                status=pattern_status,
                confidence=pattern_confidence,
                source_feedback_id=feedback_id,
                metadata=metadata,
            )
            if pattern is not None:
                created.append(pattern)
        return created

    def _prepare_feedback_context(self, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        target_company = str(payload.get("target_company") or "").strip()
        metadata = dict(payload.get("metadata") or {})
        request_payload = payload.get("request") or payload.get("request_payload") or {}
        if not isinstance(request_payload, dict) or not request_payload:
            job_id = str(payload.get("job_id") or "").strip()
            if job_id:
                job = self.get_job(job_id)
                if job is not None and isinstance(job.get("request"), dict):
                    request_payload = dict(job["request"])
        if isinstance(request_payload, dict) and request_payload:
            request_payload = JobRequest.from_payload(request_payload).to_record()
            existing_matching = (
                metadata.get("request_matching") if isinstance(metadata.get("request_matching"), dict) else {}
            )
            signature_context = _request_signature_context(
                request_payload,
                execution_bundle_payload={"request_matching": existing_matching} if existing_matching else None,
            )
            request_target_company = str(request_payload.get("target_company") or "").strip()
            if request_target_company and not target_company:
                target_company = request_target_company
            metadata.setdefault("request_payload", request_payload)
            metadata.setdefault("request_matching", _json_safe_payload(signature_context.get("request_matching") or {}))
            metadata.setdefault("request_signature", str(signature_context.get("request_signature") or ""))
            metadata.setdefault(
                "request_family_signature", str(signature_context.get("request_family_signature") or "")
            )
            metadata.setdefault(
                "matching_request_signature", str(signature_context.get("matching_request_signature") or "")
            )
            metadata.setdefault(
                "matching_request_family_signature",
                str(signature_context.get("matching_request_family_signature") or ""),
            )
            metadata.setdefault("target_company", request_target_company)
        return target_company, metadata

    def _criteria_feedback_from_row(self, row: Any) -> dict[str, Any]:
        return {
            "feedback_id": _row_value(row, "feedback_id"),
            "job_id": _row_value(row, "job_id"),
            "candidate_id": _row_value(row, "candidate_id"),
            "target_company": _row_value(row, "target_company"),
            "feedback_type": _row_value(row, "feedback_type"),
            "subject": _row_value(row, "subject"),
            "value": _row_value(row, "value"),
            "reviewer": _row_value(row, "reviewer"),
            "notes": _row_value(row, "notes"),
            "metadata": json.loads(_row_value(row, "payload_json", "{}") or "{}"),
            "created_at": _row_value(row, "created_at"),
        }

    def _criteria_pattern_from_row(self, row: Any) -> dict[str, Any]:
        return {
            "pattern_id": _row_value(row, "pattern_id"),
            "target_company": _row_value(row, "target_company"),
            "pattern_type": _row_value(row, "pattern_type"),
            "subject": _row_value(row, "subject"),
            "value": _row_value(row, "value"),
            "status": _row_value(row, "status"),
            "confidence": _row_value(row, "confidence"),
            "source_feedback_id": _row_value(row, "source_feedback_id"),
            "metadata": json.loads(_row_value(row, "metadata_json", "{}") or "{}"),
            "created_at": _row_value(row, "created_at"),
            "updated_at": _row_value(row, "updated_at"),
        }

    def _criteria_pattern_suggestion_from_row(self, row: Any) -> dict[str, Any]:
        return {
            "suggestion_id": _row_value(row, "suggestion_id"),
            "target_company": _row_value(row, "target_company"),
            "request_signature": _row_value(row, "request_signature"),
            "request_family_signature": _row_value(row, "request_family_signature"),
            "matching_request_signature": str(_row_value(row, "matching_request_signature") or ""),
            "matching_request_family_signature": str(_row_value(row, "matching_request_family_signature") or ""),
            "source_feedback_id": _row_value(row, "source_feedback_id"),
            "source_job_id": _row_value(row, "source_job_id"),
            "candidate_id": _row_value(row, "candidate_id"),
            "pattern_type": _row_value(row, "pattern_type"),
            "subject": _row_value(row, "subject"),
            "value": _row_value(row, "value"),
            "status": _row_value(row, "status"),
            "confidence": _row_value(row, "confidence"),
            "rationale": _row_value(row, "rationale"),
            "evidence": json.loads(_row_value(row, "evidence_json", "{}") or "{}"),
            "metadata": json.loads(_row_value(row, "metadata_json", "{}") or "{}"),
            "reviewed_by": _row_value(row, "reviewed_by"),
            "review_notes": _row_value(row, "review_notes"),
            "applied_pattern_id": _row_value(row, "applied_pattern_id"),
            "reviewed_at": _row_value(row, "reviewed_at"),
            "created_at": _row_value(row, "created_at"),
            "updated_at": _row_value(row, "updated_at"),
        }

    def _criteria_result_diff_from_row(self, row: Any) -> dict[str, Any]:
        return {
            "diff_id": _row_value(row, "diff_id"),
            "target_company": _row_value(row, "target_company"),
            "trigger_feedback_id": _row_value(row, "trigger_feedback_id"),
            "criteria_version_id": _row_value(row, "criteria_version_id"),
            "baseline_job_id": _row_value(row, "baseline_job_id"),
            "rerun_job_id": _row_value(row, "rerun_job_id"),
            "summary": json.loads(_row_value(row, "summary_json", "{}") or "{}"),
            "diff": json.loads(_row_value(row, "diff_json", "{}") or "{}"),
            "artifact_path": _row_value(row, "artifact_path"),
            "created_at": _row_value(row, "created_at"),
        }

    def _confidence_policy_run_from_row(self, row: Any) -> dict[str, Any]:
        return {
            "policy_run_id": _row_value(row, "policy_run_id"),
            "target_company": _row_value(row, "target_company"),
            "job_id": _row_value(row, "job_id"),
            "criteria_version_id": _row_value(row, "criteria_version_id"),
            "trigger_feedback_id": _row_value(row, "trigger_feedback_id"),
            "request_signature": _row_value(row, "request_signature"),
            "request_family_signature": _row_value(row, "request_family_signature"),
            "matching_request_signature": str(_row_value(row, "matching_request_signature") or ""),
            "matching_request_family_signature": str(_row_value(row, "matching_request_family_signature") or ""),
            "scope_kind": _row_value(row, "scope_kind"),
            "high_threshold": _row_value(row, "high_threshold"),
            "medium_threshold": _row_value(row, "medium_threshold"),
            "summary": {
                **json.loads(_row_value(row, "summary_json", "{}") or "{}"),
                "matching_request_signature": str(_row_value(row, "matching_request_signature") or ""),
                "matching_request_family_signature": str(_row_value(row, "matching_request_family_signature") or ""),
            },
            "policy": {
                **json.loads(_row_value(row, "policy_json", "{}") or "{}"),
                "matching_request_signature": str(_row_value(row, "matching_request_signature") or ""),
                "matching_request_family_signature": str(_row_value(row, "matching_request_family_signature") or ""),
            },
            "created_at": _row_value(row, "created_at"),
        }

    def _criteria_version_from_row(self, row: Any) -> dict[str, Any]:
        return {
            "version_id": _row_value(row, "version_id"),
            "target_company": _row_value(row, "target_company"),
            "request_signature": _row_value(row, "request_signature"),
            "source_kind": _row_value(row, "source_kind"),
            "parent_version_id": _row_value(row, "parent_version_id"),
            "trigger_feedback_id": _row_value(row, "trigger_feedback_id"),
            "evolution_stage": _row_value(row, "evolution_stage"),
            "request": json.loads(_row_value(row, "request_json", "{}") or "{}"),
            "plan": json.loads(_row_value(row, "plan_json", "{}") or "{}"),
            "patterns": json.loads(_row_value(row, "patterns_json", "[]") or "[]"),
            "notes": _row_value(row, "notes"),
            "created_at": _row_value(row, "created_at"),
        }

    def _criteria_compiler_run_from_row(self, row: Any) -> dict[str, Any]:
        return {
            "compiler_run_id": _row_value(row, "compiler_run_id"),
            "version_id": _row_value(row, "version_id"),
            "job_id": _row_value(row, "job_id"),
            "trigger_feedback_id": _row_value(row, "trigger_feedback_id"),
            "provider_name": _row_value(row, "provider_name"),
            "compiler_kind": _row_value(row, "compiler_kind"),
            "status": _row_value(row, "status"),
            "input": json.loads(_row_value(row, "input_json", "{}") or "{}"),
            "output": json.loads(_row_value(row, "output_json", "{}") or "{}"),
            "notes": _row_value(row, "notes"),
            "created_at": _row_value(row, "created_at"),
        }

    def _confidence_policy_control_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "control_id": row["control_id"],
            "target_company": row["target_company"],
            "request_signature": row["request_signature"],
            "request_family_signature": row["request_family_signature"],
            "matching_request_signature": str(row["matching_request_signature"] or ""),
            "matching_request_family_signature": str(row["matching_request_family_signature"] or ""),
            "scope_kind": row["scope_kind"],
            "control_mode": row["control_mode"],
            "status": row["status"],
            "high_threshold": row["high_threshold"],
            "medium_threshold": row["medium_threshold"],
            "reviewer": row["reviewer"],
            "notes": row["notes"],
            "locked_policy": json.loads(row["locked_policy_json"] or "{}"),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _job_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
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

    def _job_result_view_from_row(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
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

    def _query_dispatch_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
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

    def _job_progress_event_summary_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
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

    def _job_event_from_row(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
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

    def _asset_materialization_generation_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
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

    def _candidate_materialization_state_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
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

    def _snapshot_materialization_run_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
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

    def _cloud_asset_operation_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
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

    def _organization_execution_profile_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
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
            "reason_codes": [
                _normalize_textual_value(item)
                for item in reason_codes
                if _normalize_textual_value(item)
            ],
            "explanation": explanation,
            "summary": summary,
            "created_at": _normalize_textual_value(row["created_at"]),
            "updated_at": _normalize_textual_value(row["updated_at"]),
        }

    def _organization_asset_registry_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
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
                _normalize_textual_value(item)
                for item in selected_snapshot_ids
                if _normalize_textual_value(item)
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

    def _acquisition_shard_registry_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
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
            "company_scope": [_normalize_textual_value(item) for item in company_scope if _normalize_textual_value(item)],
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

    def _linkedin_profile_registry_from_row(self, row: Any) -> dict[str, Any]:
        if row is None:
            return {}
        source_shards = []
        source_jobs = []
        try:
            source_shards = list(json.loads(row["source_shards_json"] or "[]"))
        except json.JSONDecodeError:
            source_shards = []
        try:
            source_jobs = list(json.loads(row["source_jobs_json"] or "[]"))
        except json.JSONDecodeError:
            source_jobs = []
        return {
            "profile_url_key": str(row["profile_url_key"] or ""),
            "profile_url": str(row["profile_url"] or ""),
            "raw_linkedin_url": str(row["raw_linkedin_url"] or ""),
            "sanity_linkedin_url": str(row["sanity_linkedin_url"] or ""),
            "status": str(row["status"] or ""),
            "retry_count": int(row["retry_count"] or 0),
            "last_error": str(row["last_error"] or ""),
            "last_run_id": str(row["last_run_id"] or ""),
            "last_dataset_id": str(row["last_dataset_id"] or ""),
            "last_snapshot_dir": str(row["last_snapshot_dir"] or ""),
            "last_raw_path": str(row["last_raw_path"] or ""),
            "first_queued_at": str(row["first_queued_at"] or ""),
            "last_queued_at": str(row["last_queued_at"] or ""),
            "last_fetched_at": str(row["last_fetched_at"] or ""),
            "last_failed_at": str(row["last_failed_at"] or ""),
            "source_shards": [str(item).strip() for item in source_shards if str(item).strip()],
            "source_jobs": [str(item).strip() for item in source_jobs if str(item).strip()],
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _linkedin_profile_registry_lease_from_row(self, row: Any) -> dict[str, Any]:
        if row is None:
            return {}
        lease_expires_at = str(row["lease_expires_at"] or "")
        return {
            "profile_url_key": str(row["profile_url_key"] or ""),
            "lease_owner": str(row["lease_owner"] or ""),
            "lease_token": str(row["lease_token"] or ""),
            "lease_expires_at": lease_expires_at,
            "expired": _is_sqlite_timestamp_expired(lease_expires_at),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _workflow_job_lease_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
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

    def _linkedin_profile_registry_backfill_from_row(self, row: Any) -> dict[str, Any]:
        if row is None:
            return {}
        checkpoint_payload: dict[str, Any]
        summary_payload: dict[str, Any]
        try:
            checkpoint_payload = dict(json.loads(row["checkpoint_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            checkpoint_payload = {}
        try:
            summary_payload = dict(json.loads(row["summary_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            summary_payload = {}
        return {
            "run_key": str(row["run_key"] or ""),
            "scope_company": str(row["scope_company"] or ""),
            "scope_snapshot_id": str(row["scope_snapshot_id"] or ""),
            "checkpoint": checkpoint_payload,
            "summary": summary_payload,
            "status": str(row["status"] or ""),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _plan_review_session_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
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

    def _manual_review_item_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "review_item_id": row["review_item_id"],
            "job_id": row["job_id"],
            "candidate_id": row["candidate_id"],
            "target_company": row["target_company"],
            "review_type": row["review_type"],
            "priority": row["priority"],
            "status": row["status"],
            "summary": row["summary"],
            "candidate": json.loads(row["candidate_json"] or "{}"),
            "evidence": json.loads(row["evidence_json"] or "[]"),
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "reviewed_by": row["reviewed_by"],
            "review_notes": row["review_notes"],
            "reviewed_at": row["reviewed_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _candidate_review_record_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
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

    def _target_candidate_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
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
            "follow_up_status": row["follow_up_status"] or "pending_outreach",
            "quality_score": float(quality_score) if quality_score is not None else None,
            "comment": row["comment"] or "",
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "added_at": row["added_at"],
            "updated_at": row["updated_at"],
        }

    def _asset_default_pointer_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
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

    def _asset_default_pointer_history_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
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

    def _frontend_history_link_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
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

    def _agent_runtime_session_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
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

    def _agent_trace_span_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
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

    def _agent_worker_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
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

    def upsert_criteria_pattern(
        self,
        *,
        target_company: str,
        pattern_type: str,
        subject: str,
        value: str,
        status: str,
        confidence: str,
        source_feedback_id: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if self._control_plane_postgres_should_prefer_read("criteria_patterns"):
            row = self._call_control_plane_postgres_native(
                "upsert_row_with_generated_id",
                table_name="criteria_patterns",
                conflict_columns=["target_company", "pattern_type", "subject", "value"],
                row={
                    "target_company": target_company,
                    "pattern_type": pattern_type,
                    "subject": subject,
                    "value": value,
                    "status": status,
                    "confidence": confidence,
                    "source_feedback_id": source_feedback_id or None,
                    "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
                    "updated_at": _utc_now_timestamp(),
                },
                update_columns=[
                    "status",
                    "confidence",
                    "source_feedback_id",
                    "metadata_json",
                    "updated_at",
                ],
            )
            if row is not None:
                return self._criteria_pattern_from_row(row)
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO criteria_patterns (
                    target_company, pattern_type, subject, value, status, confidence, source_feedback_id, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(target_company, pattern_type, subject, value) DO UPDATE SET
                    status = excluded.status,
                    confidence = excluded.confidence,
                    source_feedback_id = excluded.source_feedback_id,
                    metadata_json = excluded.metadata_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    target_company,
                    pattern_type,
                    subject,
                    value,
                    status,
                    confidence,
                    source_feedback_id or None,
                    json.dumps(metadata or {}, ensure_ascii=False),
                ),
            )
        return self._get_criteria_pattern(target_company, pattern_type, subject, value)

    def _get_criteria_pattern(
        self, target_company: str, pattern_type: str, subject: str, value: str
    ) -> dict[str, Any] | None:
        postgres_row = self._select_control_plane_row(
            "criteria_patterns",
            row_builder=self._criteria_pattern_from_row,
            where_sql="target_company = %s AND pattern_type = %s AND subject = %s AND value = %s",
            params=[target_company, pattern_type, subject, value],
        )
        if postgres_row is not None:
            return postgres_row
        if self._control_plane_postgres_should_skip_sqlite_fallback("criteria_patterns"):
            return None
        row = self._connection.execute(
            """
            SELECT * FROM criteria_patterns
            WHERE target_company = ? AND pattern_type = ? AND subject = ? AND value = ?
            LIMIT 1
            """,
            (target_company, pattern_type, subject, value),
        ).fetchone()
        if row is None:
            return None
        return self._criteria_pattern_from_row(row)

    def _ensure_column(self, table_name: str, column_name: str, column_definition: str) -> None:
        rows = self._connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing = {row["name"] for row in rows}
        if column_name in existing:
            return
        self._connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")

    def _backfill_company_key_column(self, table_name: str) -> None:
        rows = self._connection.execute(
            f"""
            SELECT rowid, target_company
            FROM {table_name}
            WHERE COALESCE(company_key, '') = '' AND COALESCE(target_company, '') <> ''
            """
        ).fetchall()
        for row in rows:
            normalized_company_key = resolve_company_alias_key(str(row["target_company"] or ""))
            if not normalized_company_key:
                continue
            self._connection.execute(
                f"UPDATE {table_name} SET company_key = ? WHERE rowid = ?",
                (normalized_company_key, int(row["rowid"])),
            )

    def _insert_candidates_and_evidence(self, candidates: list[Candidate], evidence: list[EvidenceRecord]) -> None:
        candidate_payloads = self._dedupe_candidate_payloads(candidates)
        evidence_payloads = self._dedupe_evidence_payloads(evidence)
        self._connection.executemany(
            """
            INSERT INTO candidates (
                candidate_id, name_en, name_zh, display_name, category, target_company,
                organization, employment_status, role, team, joined_at, left_at,
                current_destination, ethnicity_background, investment_involvement,
                focus_areas, education, work_history, notes, linkedin_url, media_url,
                source_dataset, source_path, metadata_json
            ) VALUES (
                :candidate_id, :name_en, :name_zh, :display_name, :category, :target_company,
                :organization, :employment_status, :role, :team, :joined_at, :left_at,
                :current_destination, :ethnicity_background, :investment_involvement,
                :focus_areas, :education, :work_history, :notes, :linkedin_url, :media_url,
                :source_dataset, :source_path, :metadata_json
            )
            """,
            candidate_payloads,
        )
        self._connection.executemany(
            """
            INSERT INTO evidence (
                evidence_id, candidate_id, source_type, title, url, summary,
                source_dataset, source_path, metadata_json
            ) VALUES (
                :evidence_id, :candidate_id, :source_type, :title, :url, :summary,
                :source_dataset, :source_path, :metadata_json
            )
            ON CONFLICT(evidence_id) DO UPDATE SET
                candidate_id = excluded.candidate_id,
                source_type = excluded.source_type,
                title = excluded.title,
                url = excluded.url,
                summary = excluded.summary,
                source_dataset = excluded.source_dataset,
                source_path = excluded.source_path,
                metadata_json = excluded.metadata_json
            """,
            evidence_payloads,
        )

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


def _payload_signature(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return sha1(serialized.encode("utf-8")).hexdigest()[:16]


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


def _normalize_target_candidate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    quality_score = normalized.get("quality_score")
    parsed_quality_score: float | None = None
    if quality_score not in {None, ""}:
        try:
            parsed_quality_score = float(quality_score)
        except (TypeError, ValueError):
            parsed_quality_score = None
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


def _normalize_linkedin_profile_url_key(profile_url: str) -> str:
    raw_value = str(profile_url or "").strip()
    if not raw_value:
        return ""
    if "://" not in raw_value:
        raw_value = f"https://{raw_value}"
    parsed = parse.urlsplit(raw_value)
    netloc = str(parsed.netloc or "").strip().lower()
    path = str(parsed.path or "").strip()
    if not netloc and path:
        reparsed = parse.urlsplit(f"https://{path}")
        netloc = str(reparsed.netloc or "").strip().lower()
        path = str(reparsed.path or "").strip()
    if not netloc:
        return ""
    normalized_path = re.sub(r"/{2,}", "/", path).rstrip("/")
    if not normalized_path:
        normalized_path = "/"
    return f"https://{netloc}{normalized_path}".lower()


def _normalize_linkedin_profile_url_list(values: list[str] | None) -> list[str]:
    deduped: list[str] = []
    seen_keys: set[str] = set()
    for value in list(values or []):
        normalized_value = str(value or "").strip()
        normalized_key = _normalize_linkedin_profile_url_key(normalized_value)
        if not normalized_key or normalized_key in seen_keys:
            continue
        seen_keys.add(normalized_key)
        deduped.append(normalized_value or normalized_key)
    return deduped


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


def _parse_sqlite_timestamp(value: str) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    for format_string in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            parsed = datetime.strptime(normalized, format_string)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _is_sqlite_timestamp_expired(value: str, *, now: datetime | None = None) -> bool:
    parsed = _parse_sqlite_timestamp(value)
    if parsed is None:
        return True
    reference = now or datetime.now(timezone.utc)
    return parsed <= reference


def _milliseconds_between(start_value: str, end_value: str) -> int | None:
    start = _parse_sqlite_timestamp(start_value)
    end = _parse_sqlite_timestamp(end_value)
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds() * 1000))


def _percentile(values: list[int], percentile: float) -> int:
    if not values:
        return 0
    sorted_values = sorted(max(0, int(value)) for value in values)
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (max(0.0, min(100.0, float(percentile))) / 100.0) * (len(sorted_values) - 1)
    lower = int(rank)
    upper = min(len(sorted_values) - 1, lower + 1)
    if lower == upper:
        return sorted_values[lower]
    weight = rank - lower
    interpolated = (sorted_values[lower] * (1.0 - weight)) + (sorted_values[upper] * weight)
    return int(round(interpolated))


def _normalize_registry_label_list(values: list[str] | None) -> list[str]:
    return _dedupe_preserve_order([str(item or "").strip() for item in list(values or []) if str(item or "").strip()])


def _merge_registry_label_lists(existing: list[str], incoming: list[str]) -> list[str]:
    return _dedupe_preserve_order([*list(existing or []), *list(incoming or [])])


def _normalize_linkedin_profile_registry_backfill_entry(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    normalized = dict(payload or {})
    profile_url = str(normalized.get("profile_url") or "").strip()
    profile_url_key = _normalize_linkedin_profile_url_key(profile_url)
    raw_linkedin_url = str(normalized.get("raw_linkedin_url") or "").strip()
    sanity_linkedin_url = str(normalized.get("sanity_linkedin_url") or "").strip()
    alias_urls = _normalize_linkedin_profile_url_list(
        [
            *list(normalized.get("alias_urls") or []),
            profile_url,
            raw_linkedin_url,
            sanity_linkedin_url,
        ]
    )
    alias_keys = _dedupe_preserve_order(
        [_normalize_linkedin_profile_url_key(alias_url) for alias_url in alias_urls if str(alias_url or "").strip()]
    )
    normalized_profile_key = profile_url_key or (alias_keys[0] if alias_keys else "")
    normalized_status = str(normalized.get("status") or "").strip()
    if normalized_status not in {"fetched", "failed_retryable", "unrecoverable"}:
        normalized_status = "failed_retryable"
    if not normalized_profile_key:
        return None
    retryable = bool(normalized.get("retryable")) and normalized_status not in {"fetched", "unrecoverable"}
    return {
        "profile_url": profile_url or normalized_profile_key,
        "profile_url_key": normalized_profile_key,
        "status": normalized_status,
        "error": str(normalized.get("error") or "").strip(),
        "retryable": retryable,
        "raw_path": str(normalized.get("raw_path") or "").strip(),
        "source_shards": _normalize_registry_label_list(list(normalized.get("source_shards") or [])),
        "source_jobs": _normalize_registry_label_list(list(normalized.get("source_jobs") or [])),
        "alias_urls": alias_urls,
        "alias_keys": alias_keys,
        "raw_linkedin_url": raw_linkedin_url,
        "sanity_linkedin_url": sanity_linkedin_url,
        "snapshot_dir": str(normalized.get("snapshot_dir") or "").strip(),
    }


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


def _job_match_sort_key(match: dict[str, Any], row: sqlite3.Row | None) -> tuple[float, str, str]:
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


def _prepare_manual_review_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for item in items:
        normalized = _normalize_manual_review_item_payload(item)
        key = _manual_review_scope_key(normalized)
        if not key:
            continue
        deduped[key] = normalized
    return list(deduped.values())


def _normalize_manual_review_item_payload(item: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(item or {})
    candidate = dict(normalized.get("candidate") or {})
    evidence = list(normalized.get("evidence") or [])
    metadata = dict(normalized.get("metadata") or {})
    snapshot_id = _infer_manual_review_snapshot_id({"candidate": candidate, "evidence": evidence, "metadata": metadata})
    if snapshot_id:
        metadata["snapshot_id"] = snapshot_id
    normalized["candidate"] = candidate
    normalized["evidence"] = evidence
    normalized["metadata"] = metadata
    return normalized


def _manual_review_scope_key(payload: dict[str, Any], *, include_snapshot: bool = True) -> tuple[Any, ...]:
    target_company = str(payload.get("target_company") or "").strip().lower()
    candidate_id = str(
        payload.get("candidate_id") or (payload.get("candidate") or {}).get("candidate_id") or ""
    ).strip()
    review_type = str(payload.get("review_type") or "").strip()
    if not target_company or not candidate_id or not review_type:
        return ()
    if not include_snapshot:
        return (target_company, candidate_id, review_type)
    snapshot_id = _infer_manual_review_snapshot_id(payload)
    return (target_company, candidate_id, review_type, snapshot_id)


def _infer_manual_review_snapshot_id(payload: dict[str, Any]) -> str:
    metadata = dict(payload.get("metadata") or {})
    candidate = dict(payload.get("candidate") or {})
    evidence = list(payload.get("evidence") or [])
    for value in [
        metadata.get("snapshot_id"),
        metadata.get("source_snapshot_id"),
        dict(candidate.get("metadata") or {}).get("snapshot_id"),
    ]:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    for value in [
        candidate.get("source_path"),
        candidate.get("metadata", {}).get("source_path") if isinstance(candidate.get("metadata"), dict) else "",
    ]:
        snapshot_id = _snapshot_id_from_path(str(value or ""))
        if snapshot_id:
            return snapshot_id
    for item in evidence:
        item_metadata = dict(item.get("metadata") or {})
        snapshot_id = str(item_metadata.get("snapshot_id") or "").strip()
        if snapshot_id:
            return snapshot_id
        snapshot_id = _snapshot_id_from_path(str(item.get("source_path") or ""))
        if snapshot_id:
            return snapshot_id
    return ""


def _snapshot_id_from_path(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    snapshot_ref = extract_company_snapshot_ref(normalized)
    return str(snapshot_ref[1] if snapshot_ref is not None else "")


def _manual_review_item_from_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "review_item_id": row["review_item_id"],
        "job_id": row["job_id"],
        "candidate_id": row["candidate_id"],
        "target_company": row["target_company"],
        "review_type": row["review_type"],
        "priority": row["priority"],
        "status": row["status"],
        "summary": row["summary"],
        "candidate": json.loads(row["candidate_json"] or "{}"),
        "evidence": json.loads(row["evidence_json"] or "[]"),
        "metadata": json.loads(row["metadata_json"] or "{}"),
    }


def _append_review_note(existing_notes: Any, message: str) -> str:
    normalized_message = str(message or "").strip()
    normalized_existing = str(existing_notes or "").strip()
    if not normalized_message:
        return normalized_existing
    if not normalized_existing:
        return normalized_message
    if normalized_message in normalized_existing:
        return normalized_existing
    return f"{normalized_existing} | {normalized_message}"


def _confidence_policy_control_matches_scope(
    control: dict[str, Any],
    *,
    scope_kind: str,
    request_signature: str,
    request_family_signature: str,
    matching_request_signature: str,
    matching_request_family_signature: str,
) -> bool:
    normalized_scope = str(scope_kind or "").strip()
    control_scope = str(control.get("scope_kind") or "").strip()
    if normalized_scope != control_scope:
        return False
    if normalized_scope == "request_exact":
        control_matching_request_signature = str(control.get("matching_request_signature") or "").strip()
        return bool(
            (matching_request_signature and control_matching_request_signature == matching_request_signature)
            or (
                not control_matching_request_signature
                and request_signature
                and str(control.get("request_signature") or "").strip() == request_signature
            )
        )
    if normalized_scope == "request_family":
        control_matching_request_family_signature = str(control.get("matching_request_family_signature") or "").strip()
        return bool(
            (
                matching_request_family_signature
                and control_matching_request_family_signature == matching_request_family_signature
            )
            or (
                not control_matching_request_family_signature
                and request_family_signature
                and str(control.get("request_family_signature") or "").strip() == request_family_signature
            )
        )
    return normalized_scope == "company"
