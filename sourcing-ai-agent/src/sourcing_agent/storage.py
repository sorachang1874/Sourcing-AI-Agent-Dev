from __future__ import annotations

from datetime import datetime, timezone
import json
from hashlib import sha1
from pathlib import Path
import re
import sqlite3
import threading
from typing import Any
from urllib import parse

from .company_registry import normalize_company_key
from .domain import Candidate, EvidenceRecord, JobRequest, normalize_candidate
from .request_matching import (
    MATCH_THRESHOLD,
    build_request_family_match_explanation,
    request_family_score,
    request_family_signature,
    request_signature,
)
from .worker_scheduler import effective_worker_status, wait_stage


def _json_safe_payload(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    to_record = getattr(value, "to_record", None)
    if callable(to_record):
        return _json_safe_payload(to_record())
    if isinstance(value, dict):
        return {
            str(key): _json_safe_payload(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_payload(item) for item in value]
    return value


class SQLiteStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._connection = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            timeout=60.0,
        )
        self._connection.row_factory = sqlite3.Row
        self._configure_connection()
        self.init_schema()

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
                    summary_json TEXT,
                    artifact_path TEXT,
                    request_signature TEXT,
                    request_family_signature TEXT,
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

                CREATE TABLE IF NOT EXISTS job_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    detail TEXT,
                    payload_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
                    status TEXT NOT NULL DEFAULT 'pending',
                    risk_level TEXT NOT NULL DEFAULT 'medium',
                    required_before_execution INTEGER NOT NULL DEFAULT 1,
                    request_json TEXT NOT NULL,
                    plan_json TEXT NOT NULL,
                    gate_json TEXT NOT NULL,
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
                    requester_id TEXT,
                    tenant_id TEXT,
                    idempotency_key TEXT,
                    strategy TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source_job_id TEXT,
                    created_job_id TEXT,
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

                CREATE INDEX IF NOT EXISTS idx_candidates_target_company
                    ON candidates (target_company, category, employment_status);

                CREATE INDEX IF NOT EXISTS idx_evidence_candidate
                    ON evidence (candidate_id);

                CREATE INDEX IF NOT EXISTS idx_job_results_job
                    ON job_results (job_id, rank_index);

                CREATE INDEX IF NOT EXISTS idx_job_events_job
                    ON job_events (job_id, event_id);

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
                """
            )
            self._ensure_column("jobs", "job_type", "TEXT NOT NULL DEFAULT 'retrieval'")
            self._ensure_column("jobs", "stage", "TEXT NOT NULL DEFAULT 'pending'")
            self._ensure_column("jobs", "plan_json", "TEXT")
            self._ensure_column("jobs", "request_signature", "TEXT")
            self._ensure_column("jobs", "request_family_signature", "TEXT")
            self._ensure_column("jobs", "requester_id", "TEXT")
            self._ensure_column("jobs", "tenant_id", "TEXT")
            self._ensure_column("jobs", "idempotency_key", "TEXT")
            self._ensure_column("job_results", "confidence_label", "TEXT")
            self._ensure_column("job_results", "confidence_score", "REAL")
            self._ensure_column("job_results", "confidence_reason", "TEXT")
            self._ensure_column("criteria_versions", "parent_version_id", "INTEGER")
            self._ensure_column("criteria_versions", "trigger_feedback_id", "INTEGER")
            self._ensure_column("criteria_versions", "evolution_stage", "TEXT NOT NULL DEFAULT 'planned'")
            self._ensure_column("criteria_compiler_runs", "trigger_feedback_id", "INTEGER")
            self._ensure_column("confidence_policy_runs", "request_signature", "TEXT")
            self._ensure_column("confidence_policy_runs", "request_family_signature", "TEXT")
            self._ensure_column("confidence_policy_runs", "scope_kind", "TEXT NOT NULL DEFAULT 'company'")
            self._ensure_column("confidence_policy_controls", "request_signature", "TEXT")
            self._ensure_column("confidence_policy_controls", "request_family_signature", "TEXT")
            self._ensure_column("confidence_policy_controls", "scope_kind", "TEXT NOT NULL DEFAULT 'request_family'")
            self._ensure_column("confidence_policy_controls", "control_mode", "TEXT NOT NULL DEFAULT 'override'")
            self._ensure_column("confidence_policy_controls", "status", "TEXT NOT NULL DEFAULT 'active'")
            self._ensure_column("confidence_policy_controls", "reviewer", "TEXT")
            self._ensure_column("confidence_policy_controls", "notes", "TEXT")
            self._ensure_column("confidence_policy_controls", "locked_policy_json", "TEXT")
            self._ensure_column("plan_review_sessions", "request_signature", "TEXT")
            self._ensure_column("plan_review_sessions", "request_family_signature", "TEXT")
            self._ensure_column("plan_review_sessions", "status", "TEXT NOT NULL DEFAULT 'pending'")
            self._ensure_column("plan_review_sessions", "risk_level", "TEXT NOT NULL DEFAULT 'medium'")
            self._ensure_column("plan_review_sessions", "required_before_execution", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column("plan_review_sessions", "decision_json", "TEXT")
            self._ensure_column("plan_review_sessions", "reviewer", "TEXT")
            self._ensure_column("plan_review_sessions", "review_notes", "TEXT")
            self._ensure_column("plan_review_sessions", "approved_at", "TEXT")
            self._ensure_column("criteria_pattern_suggestions", "reviewed_by", "TEXT")
            self._ensure_column("criteria_pattern_suggestions", "review_notes", "TEXT")
            self._ensure_column("criteria_pattern_suggestions", "applied_pattern_id", "INTEGER")
            self._ensure_column("criteria_pattern_suggestions", "reviewed_at", "TEXT")
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
            self._ensure_column("query_dispatches", "requester_id", "TEXT")
            self._ensure_column("query_dispatches", "tenant_id", "TEXT")
            self._ensure_column("query_dispatches", "idempotency_key", "TEXT")
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
            self._ensure_column("organization_asset_registry", "explicit_profile_capture_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "missing_linkedin_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "manual_review_backlog_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "profile_completion_backlog_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "source_snapshot_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "completeness_score", "REAL NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "completeness_band", "TEXT NOT NULL DEFAULT 'low'")
            self._ensure_column("organization_asset_registry", "current_lane_coverage_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("organization_asset_registry", "former_lane_coverage_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("organization_asset_registry", "current_lane_effective_candidate_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "former_lane_effective_candidate_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "current_lane_effective_ready", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "former_lane_effective_ready", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("organization_asset_registry", "source_snapshot_selection_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("organization_asset_registry", "selected_snapshot_ids_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("organization_asset_registry", "source_path", "TEXT")
            self._ensure_column("organization_asset_registry", "source_job_id", "TEXT")
            self._ensure_column("organization_asset_registry", "summary_json", "TEXT NOT NULL DEFAULT '{}'")
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
        self._backfill_job_request_signatures()

    def _backfill_job_request_signatures(self) -> None:
        with self._lock, self._connection:
            rows = self._connection.execute(
                """
                SELECT job_id, request_json
                FROM jobs
                WHERE coalesce(request_signature, '') = '' OR coalesce(request_family_signature, '') = ''
                """
            ).fetchall()
            for row in rows:
                request_payload: dict[str, Any]
                try:
                    request_payload = json.loads(row["request_json"] or "{}")
                except json.JSONDecodeError:
                    request_payload = {}
                self._connection.execute(
                    """
                    UPDATE jobs
                    SET request_signature = ?, request_family_signature = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE job_id = ?
                    """,
                    (
                        request_signature(request_payload),
                        request_family_signature(request_payload),
                        str(row["job_id"] or ""),
                    ),
                )

    def replace_bootstrap_data(self, candidates: list[Candidate], evidence: list[EvidenceRecord]) -> None:
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM evidence")
            self._connection.execute("DELETE FROM candidates")
            self._insert_candidates_and_evidence(candidates, evidence)

    def replace_company_data(self, target_company: str, candidates: list[Candidate], evidence: list[EvidenceRecord]) -> None:
        company_key = target_company.strip().lower()
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

    def replace_company_category_data(
        self,
        target_company: str,
        category: str,
        candidates: list[Candidate],
        evidence: list[EvidenceRecord],
    ) -> None:
        company_key = target_company.strip().lower()
        category_key = category.strip().lower()
        with self._lock, self._connection:
            rows = self._connection.execute(
                """
                SELECT candidate_id FROM candidates
                WHERE lower(target_company) = ? AND lower(category) = ?
                """,
                (company_key, category_key),
            ).fetchall()
            candidate_ids = [row["candidate_id"] for row in rows]
            if candidate_ids:
                placeholders = ",".join("?" for _ in candidate_ids)
                self._connection.execute(
                    f"DELETE FROM evidence WHERE candidate_id IN ({placeholders})",
                    candidate_ids,
                )
            self._connection.execute(
                "DELETE FROM candidates WHERE lower(target_company) = ? AND lower(category) = ?",
                (company_key, category_key),
            )
            self._insert_candidates_and_evidence(candidates, evidence)

    def candidate_count(self) -> int:
        row = self._connection.execute("SELECT COUNT(*) AS count FROM candidates").fetchone()
        return int(row["count"])

    def candidate_count_for_company(self, target_company: str) -> int:
        row = self._connection.execute(
            "SELECT COUNT(*) AS count FROM candidates WHERE lower(target_company) = lower(?)",
            (target_company,),
        ).fetchone()
        return int(row["count"])

    def list_candidates(self) -> list[Candidate]:
        rows = self._connection.execute("SELECT * FROM candidates ORDER BY name_en").fetchall()
        return [self._candidate_from_row(row) for row in rows]

    def list_candidates_for_company(self, target_company: str) -> list[Candidate]:
        rows = self._connection.execute(
            "SELECT * FROM candidates WHERE lower(target_company) = lower(?) ORDER BY name_en",
            (target_company,),
        ).fetchall()
        return [self._candidate_from_row(row) for row in rows]

    def get_candidate(self, candidate_id: str) -> Candidate | None:
        row = self._connection.execute(
            "SELECT * FROM candidates WHERE candidate_id = ? LIMIT 1",
            (candidate_id,),
        ).fetchone()
        if row is None:
            return None
        return self._candidate_from_row(row)

    def find_candidate_by_name(self, *, target_company: str, name_en: str) -> Candidate | None:
        normalized_name = str(name_en or "").strip()
        normalized_company = str(target_company or "").strip()
        if not normalized_name:
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
        return self.get_candidate(candidate.candidate_id) or candidate

    def list_evidence(self, candidate_id: str) -> list[dict[str, Any]]:
        rows = self._connection.execute(
            "SELECT * FROM evidence WHERE candidate_id = ? ORDER BY title", (candidate_id,)
        ).fetchall()
        results = []
        for row in rows:
            results.append(
                {
                    "evidence_id": row["evidence_id"],
                    "source_type": row["source_type"],
                    "title": row["title"],
                    "url": row["url"],
                    "summary": row["summary"],
                    "source_dataset": row["source_dataset"],
                    "source_path": row["source_path"],
                    "metadata": json.loads(row["metadata_json"] or "{}"),
                }
            )
        return results

    def list_evidence_for_company(self, target_company: str) -> list[dict[str, Any]]:
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
        results = []
        for row in rows:
            results.append(
                {
                    "evidence_id": row["evidence_id"],
                    "candidate_id": row["candidate_id"],
                    "source_type": row["source_type"],
                    "title": row["title"],
                    "url": row["url"],
                    "summary": row["summary"],
                    "source_dataset": row["source_dataset"],
                    "source_path": row["source_path"],
                    "metadata": json.loads(row["metadata_json"] or "{}"),
                }
            )
        return results

    def upsert_evidence_records(self, evidence: list[EvidenceRecord]) -> list[dict[str, Any]]:
        if not evidence:
            return []
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
                [self._evidence_payload(item) for item in evidence],
            )
        return self.list_evidence(evidence[0].candidate_id)

    def save_job(
        self,
        job_id: str,
        job_type: str,
        status: str,
        stage: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any] | None = None,
        summary_payload: dict[str, Any] | None = None,
        artifact_path: str = "",
        requester_id: str = "",
        tenant_id: str = "",
        idempotency_key: str = "",
    ) -> None:
        summary_json = json.dumps(_json_safe_payload(summary_payload or {}), ensure_ascii=False)
        request_json = json.dumps(_json_safe_payload(request_payload), ensure_ascii=False)
        plan_json = json.dumps(_json_safe_payload(plan_payload or {}), ensure_ascii=False)
        request_sig = request_signature(request_payload)
        request_family_sig = request_family_signature(request_payload)
        requester_id_value = str(requester_id or "").strip()
        tenant_id_value = str(tenant_id or "").strip()
        idempotency_key_value = str(idempotency_key or "").strip()
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO jobs (
                    job_id, job_type, status, stage, request_json, plan_json, summary_json, artifact_path,
                    request_signature, request_family_signature, requester_id, tenant_id, idempotency_key
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    job_type = excluded.job_type,
                    status = excluded.status,
                    stage = excluded.stage,
                    request_json = excluded.request_json,
                    plan_json = excluded.plan_json,
                    summary_json = excluded.summary_json,
                    artifact_path = excluded.artifact_path,
                    request_signature = excluded.request_signature,
                    request_family_signature = excluded.request_family_signature,
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
                    summary_json,
                    artifact_path,
                    request_sig,
                    request_family_sig,
                    requester_id_value,
                    tenant_id_value,
                    idempotency_key_value,
                ),
            )

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
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO job_events (job_id, stage, status, detail, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, stage, status, detail, payload_json),
            )
            if stage in {"runtime_heartbeat", "runtime_control"}:
                self._compact_runtime_job_events_locked(job_id=job_id, stage=stage, payload=payload_dict)

    def _compact_runtime_job_events_locked(self, *, job_id: str, stage: str, payload: dict[str, Any]) -> None:
        normalized_job_id = str(job_id or "").strip()
        normalized_stage = str(stage or "").strip()
        if not normalized_job_id or normalized_stage not in {"runtime_heartbeat", "runtime_control"}:
            return
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
            return
        delete_ids = matched_ids[keep_latest:]
        self._connection.executemany(
            "DELETE FROM job_events WHERE event_id = ?",
            [(event_id,) for event_id in delete_ids if event_id > 0],
        )

    def replace_job_results(self, job_id: str, results: list[dict[str, Any]]) -> None:
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM job_results WHERE job_id = ?", (job_id,))
            self._connection.executemany(
                """
                INSERT INTO job_results (
                    job_id, candidate_id, rank_index, score, confidence_label, confidence_score, confidence_reason, explanation, matched_fields_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        job_id,
                        result["candidate_id"],
                        result["rank"],
                        result["score"],
                        result.get("confidence_label", ""),
                        result.get("confidence_score", 0.0),
                        result.get("confidence_reason", ""),
                        result["explanation"],
                        json.dumps(result["matched_fields"], ensure_ascii=False),
                    )
                    for result in results
                ],
            )

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        return self._job_from_row(row)

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
        query = (
            f"SELECT * FROM jobs WHERE {' AND '.join(clauses)} "
            "ORDER BY updated_at ASC, created_at ASC LIMIT ?"
        )
        params.append(max(1, int(limit or 100)))
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
        query = (
            f"SELECT * FROM jobs WHERE {' AND '.join(clauses)} "
            "ORDER BY updated_at ASC, created_at ASC LIMIT ?"
        )
        params.append(max(1, int(limit or 100)))
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._job_from_row(row) for row in rows]

    def get_job_results(self, job_id: str) -> list[dict[str, Any]]:
        rows = self._connection.execute(
            """
            SELECT jr.*, c.display_name, c.name_en, c.name_zh, c.category, c.organization,
                   c.role, c.team, c.employment_status, c.focus_areas, c.linkedin_url
            FROM job_results jr
            JOIN candidates c ON c.candidate_id = jr.candidate_id
            WHERE jr.job_id = ?
            ORDER BY jr.rank_index
            """,
            (job_id,),
        ).fetchall()
        return [
            {
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
                "linkedin_url": row["linkedin_url"],
                "rank": row["rank_index"],
                "score": row["score"],
                "confidence_label": row["confidence_label"],
                "confidence_score": row["confidence_score"],
                "confidence_reason": row["confidence_reason"],
                "explanation": row["explanation"],
                "matched_fields": json.loads(row["matched_fields_json"] or "[]"),
            }
            for row in rows
        ]

    def create_plan_review_session(
        self,
        *,
        target_company: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        gate_payload: dict[str, Any],
    ) -> dict[str, Any]:
        status = "pending" if bool(gate_payload.get("required_before_execution")) else "ready"
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO plan_review_sessions (
                    target_company, request_signature, request_family_signature, status, risk_level, required_before_execution,
                    request_json, plan_json, gate_json, decision_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    target_company,
                    request_signature(request_payload),
                    request_family_signature(request_payload),
                    status,
                    str(gate_payload.get("risk_level") or "medium"),
                    1 if bool(gate_payload.get("required_before_execution")) else 0,
                    json.dumps(request_payload, ensure_ascii=False),
                    json.dumps(plan_payload, ensure_ascii=False),
                    json.dumps(gate_payload, ensure_ascii=False),
                    json.dumps({}, ensure_ascii=False),
                ),
            )
            review_id = int(cursor.lastrowid)
        return self.get_plan_review_session(review_id) or {}

    def get_plan_review_session(self, review_id: int) -> dict[str, Any] | None:
        if review_id <= 0:
            return None
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
    ) -> dict[str, Any] | None:
        existing = self.get_plan_review_session(review_id)
        if existing is None:
            return None
        final_request_payload = request_payload if request_payload is not None else dict(existing.get("request") or {})
        final_plan_payload = plan_payload if plan_payload is not None else dict(existing.get("plan") or {})
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE plan_review_sessions
                SET status = ?, reviewer = ?, review_notes = ?, decision_json = ?, request_json = ?, plan_json = ?,
                    approved_at = CASE WHEN ? = 'approved' THEN CURRENT_TIMESTAMP ELSE approved_at END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE review_id = ?
                """,
                (
                    status,
                    reviewer,
                    notes,
                    json.dumps(decision_payload or {}, ensure_ascii=False),
                    json.dumps(final_request_payload, ensure_ascii=False),
                    json.dumps(final_plan_payload, ensure_ascii=False),
                    status,
                    review_id,
                ),
            )
        return self.get_plan_review_session(review_id)

    def replace_manual_review_items(self, job_id: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized_items = _prepare_manual_review_items(items)
        scope_keys = {_manual_review_scope_key(item) for item in normalized_items if _manual_review_scope_key(item)}
        legacy_scope_keys = {
            _manual_review_scope_key(item, include_snapshot=False)
            for item in normalized_items
            if _manual_review_scope_key(item, include_snapshot=False)
        }
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
        return self.list_manual_review_items(job_id=job_id, status="", limit=max(len(normalized_items), 1))

    def list_manual_review_items(
        self,
        *,
        target_company: str = "",
        status: str = "open",
        job_id: str = "",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
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

    def cleanup_manual_review_items(
        self,
        *,
        target_company: str = "",
        snapshot_id: str = "",
    ) -> dict[str, Any]:
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
        existing = self.get_manual_review_item(review_item_id)
        if existing is None:
            return None
        normalized_action = str(action or "").strip().lower()
        if normalized_action in {"resolve", "resolved", "approve", "approved"}:
            status = "resolved"
        elif normalized_action in {"dismiss", "dismissed", "reject", "rejected"}:
            status = "dismissed"
        elif normalized_action in {"escalate", "escalated"}:
            status = "escalated"
        else:
            status = existing.get("status") or "open"
        merged_metadata = dict(existing.get("metadata") or {})
        merged_metadata.update(metadata_merge or {})
        stored_candidate = candidate_payload if isinstance(candidate_payload, dict) and candidate_payload else existing.get("candidate") or {}
        stored_evidence = evidence_payload if isinstance(evidence_payload, list) and evidence_payload else existing.get("evidence") or []
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
        return self.get_manual_review_item(review_item_id)

    def get_manual_review_item(self, review_item_id: int) -> dict[str, Any] | None:
        if review_item_id <= 0:
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
        session = self.get_agent_runtime_session(job_id=job_id)
        if session is None:
            raise RuntimeError(f"Failed to create agent runtime session for {job_id}")
        return session

    def get_agent_runtime_session(self, *, job_id: str = "", session_id: int = 0) -> dict[str, Any] | None:
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
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE agent_runtime_sessions
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE job_id = ?
                """,
                (status, job_id),
            )
        return self.get_agent_runtime_session(job_id=job_id)

    def complete_agent_trace_span(
        self,
        span_id: int,
        *,
        status: str,
        output_payload: dict[str, Any] | None = None,
        handoff_to_lane: str = "",
    ) -> dict[str, Any]:
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
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM agent_trace_spans WHERE span_id = ? LIMIT 1",
                (span_id,),
            ).fetchone()
        if row is None:
            return None
        return self._agent_trace_span_from_row(row)

    def list_agent_trace_spans(self, *, job_id: str = "", session_id: int = 0) -> list[dict[str, Any]]:
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
        normalized_worker_ids = [
            int(worker_id)
            for worker_id in list(worker_ids or [])
            if int(worker_id or 0) > 0
        ]
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
            worker
            for worker_id in refreshed_ids
            if (worker := self.get_agent_worker(worker_id=worker_id)) is not None
        ]

    def claim_agent_worker(
        self,
        worker_id: int,
        *,
        lease_owner: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
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
        normalized_token = str(lease_token or "").strip() or sha1(
            f"{normalized_job_id}:{normalized_owner}".encode("utf-8")
        ).hexdigest()
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
                WHERE {' AND '.join(clauses)}
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
        return [
            {
                "event_id": row["event_id"],
                "stage": row["stage"],
                "status": row["status"],
                "detail": row["detail"],
                "payload": json.loads(row["payload_json"] or "{}"),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def list_jobs(
        self,
        *,
        job_type: str = "",
        statuses: list[str] | None = None,
        stages: list[str] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if job_type:
            clauses.append("job_type = ?")
            params.append(job_type)
        normalized_statuses = [str(item or "").strip().lower() for item in list(statuses or []) if str(item or "").strip()]
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
            f"SELECT * FROM jobs {where_clause} "
            "ORDER BY datetime(updated_at) DESC, datetime(created_at) DESC LIMIT ?"
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
        normalized_statuses = [str(item or "").strip().lower() for item in list(statuses or []) if str(item or "").strip()]
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
        rows = self._connection.execute(
            """
            SELECT * FROM jobs
            WHERE status = 'completed'
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        allowed = set(job_types or ["retrieval", "workflow", "retrieval_rerun"])
        for row in rows:
            job_id = str(row["job_id"] or "")
            if exclude_job_id and job_id == exclude_job_id:
                continue
            if allowed and str(row["job_type"] or "") not in allowed:
                continue
            try:
                request_payload = json.loads(row["request_json"] or "{}")
            except json.JSONDecodeError:
                continue
            if str(request_payload.get("target_company") or "").strip().lower() != target_company.strip().lower():
                continue
            return self._job_from_row(row)
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
        rows = self._connection.execute(
            """
            SELECT * FROM jobs
            WHERE status = 'completed'
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        allowed = set(job_types or ["retrieval", "workflow", "retrieval_rerun"])
        best_job: dict[str, Any] | None = None
        best_match: dict[str, Any] | None = None
        best_sort_key: tuple[float, str, str] | None = None
        for row in rows:
            job_id = str(row["job_id"] or "")
            if exclude_job_id and job_id == exclude_job_id:
                continue
            if allowed and str(row["job_type"] or "") not in allowed:
                continue
            try:
                candidate_request = json.loads(row["request_json"] or "{}")
            except json.JSONDecodeError:
                continue
            if str(candidate_request.get("target_company") or "").strip().lower() != target_company.strip().lower():
                continue
            match = request_family_score(request_payload, candidate_request)
            job_payload = self._job_from_row(row)
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
            fallback_match = request_family_score(request_payload, fallback.get("request") or {})
            fallback["baseline_match"] = {
                "selected_via": "latest_company_fallback",
                "family_score": float(fallback_match.get("score") or 0.0),
                "exact_request_match": bool(fallback_match.get("exact_request_match")),
                "exact_family_match": bool(fallback_match.get("exact_family_match")),
                "request_signature": request_signature(request_payload),
                "request_family_signature": request_family_signature(request_payload),
                "matched_request_signature": request_signature(fallback.get("request") or {}),
                "matched_request_family_signature": request_family_signature(fallback.get("request") or {}),
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
            "request_signature": request_signature(request_payload),
            "request_family_signature": request_family_signature(request_payload),
            "matched_request_signature": request_signature(best_job.get("request") or {}),
            "matched_request_family_signature": request_family_signature(best_job.get("request") or {}),
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
        params: list[Any] = [signature]
        clauses = ["request_signature = ?"]
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
            job_id = str(payload.get("job_id") or "")
            if exclude_job_id and job_id == exclude_job_id:
                continue
            request_payload = dict(payload.get("request") or {})
            if normalized_target_company and str(request_payload.get("target_company") or "").strip().lower() != normalized_target_company:
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
        params: list[Any] = [family_signature]
        clauses = ["request_family_signature = ?"]
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
            job_id = str(payload.get("job_id") or "")
            if exclude_job_id and job_id == exclude_job_id:
                continue
            request_payload = dict(payload.get("request") or {})
            if normalized_target_company and str(request_payload.get("target_company") or "").strip().lower() != normalized_target_company:
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
        params: list[Any] = [signature]
        clauses = ["request_signature = ?"]
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
        results: list[dict[str, Any]] = []
        for row in rows:
            payload = self._job_from_row(row)
            job_id = str(payload.get("job_id") or "")
            if exclude_job_id and job_id == exclude_job_id:
                continue
            request_payload = dict(payload.get("request") or {})
            if normalized_target_company and str(request_payload.get("target_company") or "").strip().lower() != normalized_target_company:
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
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE jobs
                SET status = 'superseded',
                    stage = 'completed',
                    summary_json = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE job_id = ?
                """,
                (
                    json.dumps(summary, ensure_ascii=False),
                    str(job_id),
                ),
            )
            self._connection.execute(
                """
                UPDATE agent_runtime_sessions
                SET status = 'superseded',
                    updated_at = CURRENT_TIMESTAMP
                WHERE job_id = ?
                """,
                (str(job_id),),
            )
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
            if normalized_target_company and str(request_payload.get("target_company") or "").strip().lower() != normalized_target_company:
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
        request_sig = request_signature(request_payload)
        request_family_sig = request_family_signature(request_payload)
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO query_dispatches (
                    target_company,
                    request_signature,
                    request_family_signature,
                    requester_id,
                    tenant_id,
                    idempotency_key,
                    strategy,
                    status,
                    source_job_id,
                    created_job_id,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_target_company,
                    request_sig,
                    request_family_sig,
                    requester_id_value,
                    tenant_id_value,
                    idempotency_key_value,
                    str(strategy or "").strip(),
                    str(status or "").strip(),
                    str(source_job_id or "").strip(),
                    str(created_job_id or "").strip(),
                    json.dumps(payload or {}, ensure_ascii=False),
                ),
            )
            dispatch_id = int(cursor.lastrowid)
        return self.get_query_dispatch(dispatch_id) or {}

    def get_query_dispatch(self, dispatch_id: int) -> dict[str, Any] | None:
        if dispatch_id <= 0:
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
        normalized_target_company = str(payload.get("target_company") or "").strip()
        snapshot_id = str(payload.get("snapshot_id") or "").strip()
        asset_view = str(payload.get("asset_view") or "canonical_merged").strip() or "canonical_merged"
        if not normalized_target_company or not snapshot_id:
            return {}
        if authoritative:
            with self._lock, self._connection:
                self._connection.execute(
                    """
                    UPDATE organization_asset_registry
                    SET authoritative = 0, updated_at = CURRENT_TIMESTAMP
                    WHERE lower(target_company) = lower(?) AND asset_view = ?
                    """,
                    (normalized_target_company, asset_view),
                )
        with self._lock, self._connection:
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
                    source_path, source_job_id, summary_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    summary_json = excluded.summary_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    normalized_target_company,
                    str(payload.get("company_key") or "").strip(),
                    snapshot_id,
                    asset_view,
                    str(payload.get("status") or "ready").strip() or "ready",
                    1 if authoritative or bool(payload.get("authoritative")) else 0,
                    int(payload.get("candidate_count") or 0),
                    int(payload.get("evidence_count") or 0),
                    int(payload.get("profile_detail_count") or 0),
                    int(payload.get("explicit_profile_capture_count") or 0),
                    int(payload.get("missing_linkedin_count") or 0),
                    int(payload.get("manual_review_backlog_count") or 0),
                    int(payload.get("profile_completion_backlog_count") or 0),
                    int(payload.get("source_snapshot_count") or 0),
                    float(payload.get("completeness_score") or 0.0),
                    str(payload.get("completeness_band") or "low").strip() or "low",
                    json.dumps(_json_safe_payload(payload.get("current_lane_coverage") or {}), ensure_ascii=False),
                    json.dumps(_json_safe_payload(payload.get("former_lane_coverage") or {}), ensure_ascii=False),
                    int(payload.get("current_lane_effective_candidate_count") or 0),
                    int(payload.get("former_lane_effective_candidate_count") or 0),
                    1 if bool(payload.get("current_lane_effective_ready")) else 0,
                    1 if bool(payload.get("former_lane_effective_ready")) else 0,
                    json.dumps(_json_safe_payload(payload.get("source_snapshot_selection") or {}), ensure_ascii=False),
                    json.dumps(_json_safe_payload(payload.get("selected_snapshot_ids") or []), ensure_ascii=False),
                    str(payload.get("source_path") or "").strip(),
                    str(payload.get("source_job_id") or "").strip(),
                    json.dumps(_json_safe_payload(payload.get("summary") or {}), ensure_ascii=False),
                ),
            )
            row = self._connection.execute(
                """
                SELECT * FROM organization_asset_registry
                WHERE lower(target_company) = lower(?) AND snapshot_id = ? AND asset_view = ?
                LIMIT 1
                """,
                (normalized_target_company, snapshot_id, asset_view),
            ).fetchone()
        return self._organization_asset_registry_from_row(row)

    def get_authoritative_organization_asset_registry(
        self,
        *,
        target_company: str,
        asset_view: str = "canonical_merged",
    ) -> dict[str, Any]:
        normalized_target_company = str(target_company or "").strip()
        if not normalized_target_company:
            return {}
        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM organization_asset_registry
                WHERE lower(target_company) = lower(?) AND asset_view = ?
                ORDER BY authoritative DESC, updated_at DESC, registry_id DESC
                LIMIT 1
                """,
                (normalized_target_company, str(asset_view or "canonical_merged").strip() or "canonical_merged"),
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
        clauses: list[str] = []
        params: list[Any] = []
        if target_company:
            clauses.append("lower(target_company) = lower(?)")
            params.append(target_company)
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
        normalized_target_company = str(target_company or "").strip()
        normalized_company_key = str(company_key or "").strip() or normalize_company_key(normalized_target_company)
        normalized_asset_view = str(asset_view or "").strip()
        if not normalized_target_company:
            return {"updated_rows": 0, "deleted_rows": 0, "merged_groups": 0}

        clauses = ["(lower(target_company) = lower(?)"]
        params: list[Any] = [normalized_target_company]
        if normalized_company_key:
            clauses.append(" OR company_key = ?")
            params.append(normalized_company_key)
        clauses.append(")")
        if normalized_asset_view:
            clauses.append("AND asset_view = ?")
            params.append(normalized_asset_view)
        where_clause = " ".join(clauses)

        def _row_sort_key(row: sqlite3.Row) -> tuple[int, int, str, int]:
            return (
                int(row["authoritative"] or 0),
                int(row["candidate_count"] or 0),
                str(row["updated_at"] or ""),
                int(row["registry_id"] or 0),
            )

        updated_rows = 0
        deleted_rows = 0
        merged_groups = 0
        with self._lock, self._connection:
            rows = self._connection.execute(
                f"""
                SELECT * FROM organization_asset_registry
                WHERE {where_clause}
                ORDER BY snapshot_id, asset_view, registry_id DESC
                """,
                params,
            ).fetchall()
            grouped: dict[tuple[str, str], list[sqlite3.Row]] = {}
            for row in rows:
                key = (str(row["snapshot_id"] or ""), str(row["asset_view"] or "canonical_merged"))
                grouped.setdefault(key, []).append(row)
            for (_, asset_view_value), group_rows in grouped.items():
                if not group_rows:
                    continue
                keeper = max(
                    group_rows,
                    key=lambda row: (
                        1 if str(row["target_company"] or "") == normalized_target_company else 0,
                        *_row_sort_key(row),
                    ),
                )
                keeper_id = int(keeper["registry_id"])
                authoritative = any(int(row["authoritative"] or 0) for row in group_rows)
                if str(keeper["target_company"] or "") != normalized_target_company or authoritative != bool(keeper["authoritative"]):
                    self._connection.execute(
                        """
                        UPDATE organization_asset_registry
                        SET target_company = ?, authoritative = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE registry_id = ?
                        """,
                        (normalized_target_company, 1 if authoritative else 0, keeper_id),
                    )
                    updated_rows += 1
                extra_ids = [int(row["registry_id"]) for row in group_rows if int(row["registry_id"]) != keeper_id]
                if extra_ids:
                    placeholders = ",".join("?" for _ in extra_ids)
                    self._connection.execute(
                        f"DELETE FROM organization_asset_registry WHERE registry_id IN ({placeholders})",
                        extra_ids,
                    )
                    deleted_rows += len(extra_ids)
                    merged_groups += 1
            if normalized_asset_view and any(
                int(row["authoritative"] or 0) for row in rows
            ):
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

    def upsert_acquisition_shard_registry(self, payload: dict[str, Any]) -> dict[str, Any]:
        shard_key = str(payload.get("shard_key") or "").strip()
        target_company = str(payload.get("target_company") or "").strip()
        snapshot_id = str(payload.get("snapshot_id") or "").strip()
        if not shard_key or not target_company or not snapshot_id:
            return {}
        status = str(payload.get("status") or "completed").strip() or "completed"
        completed_at = datetime.now(timezone.utc).isoformat(timespec="seconds") if status.startswith("completed") else ""
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO acquisition_shard_registry (
                    shard_key, target_company, company_key, snapshot_id, asset_view, lane, status, employment_scope,
                    strategy_type, shard_id, shard_title, search_query, query_signature,
                    company_scope_json, locations_json, function_ids_json,
                    result_count, estimated_total_count, provider_cap_hit,
                    source_path, source_job_id, metadata_json, first_seen_at, last_completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?)
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
                    str(payload.get("company_key") or "").strip(),
                    snapshot_id,
                    str(payload.get("asset_view") or "canonical_merged").strip() or "canonical_merged",
                    str(payload.get("lane") or "").strip(),
                    status,
                    str(payload.get("employment_scope") or "all").strip() or "all",
                    str(payload.get("strategy_type") or "").strip(),
                    str(payload.get("shard_id") or "").strip(),
                    str(payload.get("shard_title") or "").strip(),
                    str(payload.get("search_query") or "").strip(),
                    str(payload.get("query_signature") or "").strip(),
                    json.dumps(_json_safe_payload(payload.get("company_scope") or []), ensure_ascii=False),
                    json.dumps(_json_safe_payload(payload.get("locations") or []), ensure_ascii=False),
                    json.dumps(_json_safe_payload(payload.get("function_ids") or []), ensure_ascii=False),
                    int(payload.get("result_count") or 0),
                    int(payload.get("estimated_total_count") or 0),
                    1 if bool(payload.get("provider_cap_hit")) else 0,
                    str(payload.get("source_path") or "").strip(),
                    str(payload.get("source_job_id") or "").strip(),
                    json.dumps(_json_safe_payload(payload.get("metadata") or {}), ensure_ascii=False),
                    str(payload.get("first_seen_at") or "").strip(),
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

    def normalize_linkedin_profile_url(self, profile_url: str) -> str:
        return _normalize_linkedin_profile_url_key(profile_url)

    def get_linkedin_profile_registry(self, profile_url: str) -> dict[str, Any] | None:
        key = _normalize_linkedin_profile_url_key(profile_url)
        if not key:
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
            [
                _normalize_linkedin_profile_url_key(profile_url)
                for profile_url in list(profile_urls or [])
            ]
        )
        if not keys:
            return {}
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
            canonical_keys = _dedupe_preserve_order([str(alias_map.get(key) or key).strip() for key in keys if str(alias_map.get(key) or key).strip()])
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
        normalized_token = str(lease_token or "").strip() or sha1(
            f"{normalized_key}:{normalized_owner}:{_utc_now_timestamp()}".encode("utf-8")
        ).hexdigest()[:16]
        ttl_seconds = max(5, int(lease_seconds or 0))
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
            "duplicate_request_rate": (duplicate_skips / (live_fetch_requests + duplicate_skips)) if (live_fetch_requests + duplicate_skips) else 0.0,
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
            [*list(alias_urls or []), normalized_profile_url, normalized_raw_linkedin_url, normalized_sanity_linkedin_url]
        )
        normalized_status = str(status or "").strip()
        normalized_run_id = str(run_id or "").strip()
        normalized_dataset_id = str(dataset_id or "").strip()
        normalized_snapshot_dir = str(snapshot_dir or "").strip()
        normalized_raw_path = str(raw_path or "").strip()
        normalized_source_shards = _normalize_registry_label_list(source_shards)
        normalized_source_jobs = _normalize_registry_label_list(source_jobs)
        now_timestamp = _utc_now_timestamp()
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
            existing_status = str(existing_payload.get("status") or "").strip()

            merged_source_shards = _merge_registry_label_lists(
                list(existing_payload.get("source_shards") or []),
                normalized_source_shards,
            )
            merged_source_jobs = _merge_registry_label_lists(
                list(existing_payload.get("source_jobs") or []),
                normalized_source_jobs,
            )
            effective_status = normalized_status or existing_status or "queued"
            existing_raw_path = str(existing_payload.get("last_raw_path") or "").strip()
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
                effective_retry_count = max(0, int(existing_payload.get("retry_count") or 0))
                if increment_retry:
                    effective_retry_count += 1
            effective_last_error = (
                str(last_error)
                if last_error is not None
                else str(existing_payload.get("last_error") or "")
            )
            if normalized_status == "fetched":
                effective_last_error = ""
            effective_profile_url = normalized_profile_url or str(existing_payload.get("profile_url") or "")
            effective_raw_linkedin_url = normalized_raw_linkedin_url or str(existing_payload.get("raw_linkedin_url") or "")
            effective_sanity_linkedin_url = normalized_sanity_linkedin_url or str(existing_payload.get("sanity_linkedin_url") or "")
            effective_run_id = normalized_run_id or str(existing_payload.get("last_run_id") or "")
            effective_dataset_id = normalized_dataset_id or str(existing_payload.get("last_dataset_id") or "")
            effective_snapshot_dir = normalized_snapshot_dir or str(existing_payload.get("last_snapshot_dir") or "")
            effective_raw_path = normalized_raw_path or str(existing_payload.get("last_raw_path") or "")
            effective_first_queued_at = str(existing_payload.get("first_queued_at") or "")
            effective_last_queued_at = str(existing_payload.get("last_queued_at") or "")
            effective_last_fetched_at = str(existing_payload.get("last_fetched_at") or "")
            effective_last_failed_at = str(existing_payload.get("last_failed_at") or "")
            if normalized_status == "queued":
                if not effective_first_queued_at:
                    effective_first_queued_at = now_timestamp
                effective_last_queued_at = now_timestamp
            if normalized_status == "fetched":
                effective_last_fetched_at = now_timestamp
            if normalized_status in {"failed_retryable", "unrecoverable"}:
                effective_last_failed_at = now_timestamp

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
                    effective_profile_url,
                    effective_raw_linkedin_url,
                    effective_sanity_linkedin_url,
                    effective_status,
                    effective_retry_count,
                    effective_last_error,
                    effective_run_id,
                    effective_dataset_id,
                    effective_snapshot_dir,
                    effective_raw_path,
                    effective_first_queued_at,
                    effective_last_queued_at,
                    effective_last_fetched_at,
                    effective_last_failed_at,
                    json.dumps(merged_source_shards, ensure_ascii=False),
                    json.dumps(merged_source_jobs, ensure_ascii=False),
                ),
            )
            canonical_profile_url = effective_profile_url or canonical_key
            self._upsert_linkedin_profile_registry_aliases_locked(
                canonical_key=canonical_key,
                canonical_profile_url=canonical_profile_url,
                alias_urls=normalized_alias_urls,
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
        payload["alias_urls"] = self.get_linkedin_profile_registry_aliases(payload.get("profile_url") or payload.get("profile_url_key") or "")
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
        request_sig = request_signature(request_payload)
        row = self._connection.execute(
            """
            SELECT * FROM plan_review_sessions
            WHERE lower(target_company) = lower(?) AND status = 'pending' AND request_signature = ?
            ORDER BY updated_at DESC, review_id DESC
            LIMIT 1
            """,
            (target, request_sig),
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
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO criteria_feedback (
                    job_id, candidate_id, target_company, feedback_type, subject, value, reviewer, notes, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (job_id, candidate_id, target_company, feedback_type, subject, value, reviewer, notes, payload_json),
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
        return [
            {
                "feedback_id": row["feedback_id"],
                "job_id": row["job_id"],
                "candidate_id": row["candidate_id"],
                "target_company": row["target_company"],
                "feedback_type": row["feedback_type"],
                "subject": row["subject"],
                "value": row["value"],
                "reviewer": row["reviewer"],
                "notes": row["notes"],
                "metadata": json.loads(row["payload_json"] or "{}"),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_criteria_feedback(self, feedback_id: int) -> dict[str, Any] | None:
        if feedback_id <= 0:
            return None
        row = self._connection.execute(
            "SELECT * FROM criteria_feedback WHERE feedback_id = ? LIMIT 1",
            (feedback_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "feedback_id": row["feedback_id"],
            "job_id": row["job_id"],
            "candidate_id": row["candidate_id"],
            "target_company": row["target_company"],
            "feedback_type": row["feedback_type"],
            "subject": row["subject"],
            "value": row["value"],
            "reviewer": row["reviewer"],
            "notes": row["notes"],
            "metadata": json.loads(row["payload_json"] or "{}"),
            "created_at": row["created_at"],
        }

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
        rows = self._connection.execute(
            f"""
            SELECT * FROM criteria_patterns
            {where_clause}
            ORDER BY updated_at DESC, pattern_id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [
            {
                "pattern_id": row["pattern_id"],
                "target_company": row["target_company"],
                "pattern_type": row["pattern_type"],
                "subject": row["subject"],
                "value": row["value"],
                "status": row["status"],
                "confidence": row["confidence"],
                "source_feedback_id": row["source_feedback_id"],
                "metadata": json.loads(row["metadata_json"] or "{}"),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def record_pattern_suggestions(self, suggestions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not suggestions:
            return []
        with self._lock, self._connection:
            for item in suggestions:
                self._connection.execute(
                    """
                    INSERT INTO criteria_pattern_suggestions (
                        target_company, request_signature, request_family_signature, source_feedback_id, source_job_id,
                        candidate_id, pattern_type, subject, value, status, confidence, rationale, evidence_json, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(target_company, request_family_signature, candidate_id, pattern_type, subject, value) DO UPDATE SET
                        source_feedback_id = excluded.source_feedback_id,
                        source_job_id = excluded.source_job_id,
                        status = excluded.status,
                        confidence = excluded.confidence,
                        rationale = excluded.rationale,
                        evidence_json = excluded.evidence_json,
                        metadata_json = excluded.metadata_json,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        str(item.get("target_company") or ""),
                        str(item.get("request_signature") or ""),
                        str(item.get("request_family_signature") or ""),
                        int(item.get("source_feedback_id") or 0) or None,
                        str(item.get("source_job_id") or ""),
                        str(item.get("candidate_id") or ""),
                        str(item.get("pattern_type") or ""),
                        str(item.get("subject") or ""),
                        str(item.get("value") or ""),
                        str(item.get("status") or "suggested"),
                        str(item.get("confidence") or "medium"),
                        str(item.get("rationale") or ""),
                        json.dumps(item.get("evidence") or {}, ensure_ascii=False),
                        json.dumps(item.get("metadata") or {}, ensure_ascii=False),
                    ),
                )
        source_feedback_ids = [int(item.get("source_feedback_id") or 0) for item in suggestions if int(item.get("source_feedback_id") or 0)]
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
        rows = self._connection.execute(
            f"""
            SELECT * FROM criteria_pattern_suggestions
            {where_clause}
            ORDER BY updated_at DESC, suggestion_id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [
            {
                "suggestion_id": row["suggestion_id"],
                "target_company": row["target_company"],
                "request_signature": row["request_signature"],
                "request_family_signature": row["request_family_signature"],
                "source_feedback_id": row["source_feedback_id"],
                "source_job_id": row["source_job_id"],
                "candidate_id": row["candidate_id"],
                "pattern_type": row["pattern_type"],
                "subject": row["subject"],
                "value": row["value"],
                "status": row["status"],
                "confidence": row["confidence"],
                "rationale": row["rationale"],
                "evidence": json.loads(row["evidence_json"] or "{}"),
                "metadata": json.loads(row["metadata_json"] or "{}"),
                "reviewed_by": row["reviewed_by"],
                "review_notes": row["review_notes"],
                "applied_pattern_id": row["applied_pattern_id"],
                "reviewed_at": row["reviewed_at"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def get_pattern_suggestion(self, suggestion_id: int) -> dict[str, Any] | None:
        if suggestion_id <= 0:
            return None
        row = self._connection.execute(
            "SELECT * FROM criteria_pattern_suggestions WHERE suggestion_id = ? LIMIT 1",
            (suggestion_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "suggestion_id": row["suggestion_id"],
            "target_company": row["target_company"],
            "request_signature": row["request_signature"],
            "request_family_signature": row["request_family_signature"],
            "source_feedback_id": row["source_feedback_id"],
            "source_job_id": row["source_job_id"],
            "candidate_id": row["candidate_id"],
            "pattern_type": row["pattern_type"],
            "subject": row["subject"],
            "value": row["value"],
            "status": row["status"],
            "confidence": row["confidence"],
            "rationale": row["rationale"],
            "evidence": json.loads(row["evidence_json"] or "{}"),
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "reviewed_by": row["reviewed_by"],
            "review_notes": row["review_notes"],
            "applied_pattern_id": row["applied_pattern_id"],
            "reviewed_at": row["reviewed_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

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
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO confidence_policy_runs (
                    target_company, job_id, criteria_version_id, trigger_feedback_id,
                    request_signature, request_family_signature, scope_kind,
                    high_threshold, medium_threshold, summary_json, policy_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    target_company,
                    job_id,
                    criteria_version_id or None,
                    trigger_feedback_id or None,
                    str(policy_payload.get("request_signature") or ""),
                    str(policy_payload.get("request_family_signature") or ""),
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
        return [
            {
                "policy_run_id": row["policy_run_id"],
                "target_company": row["target_company"],
                "job_id": row["job_id"],
                "criteria_version_id": row["criteria_version_id"],
                "trigger_feedback_id": row["trigger_feedback_id"],
                "request_signature": row["request_signature"],
                "request_family_signature": row["request_family_signature"],
                "scope_kind": row["scope_kind"],
                "high_threshold": row["high_threshold"],
                "medium_threshold": row["medium_threshold"],
                "summary": json.loads(row["summary_json"] or "{}"),
                "policy": json.loads(row["policy_json"] or "{}"),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

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
        request_sig = request_signature(request_payload) if request_payload else ""
        request_family_sig = request_family_signature(request_payload) if request_payload else ""
        normalized_scope = str(scope_kind or "request_family").strip() or "request_family"
        deactivation_params: list[Any] = [target_company, normalized_scope]
        clauses = ["lower(target_company) = lower(?)", "scope_kind = ?", "status = 'active'"]
        if normalized_scope == "request_exact":
            clauses.append("request_signature = ?")
            deactivation_params.append(request_sig)
        elif normalized_scope == "request_family":
            clauses.append("request_family_signature = ?")
            deactivation_params.append(request_family_sig)
        with self._lock, self._connection:
            self._connection.execute(
                f"""
                UPDATE confidence_policy_controls
                SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
                WHERE {' AND '.join(clauses)}
                """,
                tuple(deactivation_params),
            )
            cursor = self._connection.execute(
                """
                INSERT INTO confidence_policy_controls (
                    target_company, request_signature, request_family_signature, scope_kind, control_mode, status,
                    high_threshold, medium_threshold, reviewer, notes, locked_policy_json
                ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)
                """,
                (
                    target_company,
                    request_sig,
                    request_family_sig,
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
        return self.get_confidence_policy_control(control_id) or {}

    def find_active_confidence_policy_control(
        self,
        *,
        target_company: str,
        request_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        request_sig = request_signature(request_payload) if request_payload else ""
        request_family_sig = request_family_signature(request_payload) if request_payload else ""
        rows = self._connection.execute(
            """
            SELECT * FROM confidence_policy_controls
            WHERE lower(target_company) = lower(?) AND status = 'active'
            ORDER BY updated_at DESC, control_id DESC
            """,
            (target_company,),
        ).fetchall()
        best: dict[str, Any] | None = None
        best_rank = -1
        for row in rows:
            scope_kind = str(row["scope_kind"] or "")
            rank = -1
            selection_reason = ""
            if scope_kind == "request_exact" and request_sig and str(row["request_signature"] or "") == request_sig:
                rank = 3
                selection_reason = "exact_request_control"
            elif scope_kind == "request_family" and request_family_sig and str(row["request_family_signature"] or "") == request_family_sig:
                rank = 2
                selection_reason = "request_family_control"
            elif scope_kind == "company":
                rank = 1
                selection_reason = "company_control"
            if rank <= best_rank:
                continue
            payload = self._confidence_policy_control_from_row(row)
            payload["selection_reason"] = selection_reason
            best = payload
            best_rank = rank
        return best

    def get_confidence_policy_control(self, control_id: int) -> dict[str, Any] | None:
        if control_id <= 0:
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
            with self._lock, self._connection:
                self._connection.execute(
                    """
                    UPDATE confidence_policy_controls
                    SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
                    WHERE control_id = ?
                    """,
                    (control_id,),
                )
            return {"deactivated_count": 1 if self.get_confidence_policy_control(control_id) else 0}
        clauses = ["lower(target_company) = lower(?)", "status = 'active'"]
        params: list[Any] = [target_company]
        normalized_scope = str(scope_kind or "request_family").strip() or "request_family"
        clauses.append("scope_kind = ?")
        params.append(normalized_scope)
        request_payload = request_payload or {}
        if normalized_scope == "request_exact":
            clauses.append("request_signature = ?")
            params.append(request_signature(request_payload))
        elif normalized_scope == "request_family":
            clauses.append("request_family_signature = ?")
            params.append(request_family_signature(request_payload))
        with self._lock, self._connection:
            cursor = self._connection.execute(
                f"""
                UPDATE confidence_policy_controls
                SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
                WHERE {' AND '.join(clauses)}
                """,
                tuple(params),
            )
        return {"deactivated_count": int(cursor.rowcount or 0)}

    def list_criteria_result_diffs(self, target_company: str = "", limit: int = 50) -> list[dict[str, Any]]:
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
        return [
            {
                "diff_id": row["diff_id"],
                "target_company": row["target_company"],
                "trigger_feedback_id": row["trigger_feedback_id"],
                "criteria_version_id": row["criteria_version_id"],
                "baseline_job_id": row["baseline_job_id"],
                "rerun_job_id": row["rerun_job_id"],
                "summary": json.loads(row["summary_json"] or "{}"),
                "diff": json.loads(row["diff_json"] or "{}"),
                "artifact_path": row["artifact_path"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

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
        return [
            {
                "version_id": row["version_id"],
                "target_company": row["target_company"],
                "request_signature": row["request_signature"],
                "source_kind": row["source_kind"],
                "parent_version_id": row["parent_version_id"],
                "trigger_feedback_id": row["trigger_feedback_id"],
                "evolution_stage": row["evolution_stage"],
                "request": json.loads(row["request_json"] or "{}"),
                "plan": json.loads(row["plan_json"] or "{}"),
                "patterns": json.loads(row["patterns_json"] or "[]"),
                "notes": row["notes"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_criteria_version(self, version_id: int) -> dict[str, Any] | None:
        if version_id <= 0:
            return None
        row = self._connection.execute(
            "SELECT * FROM criteria_versions WHERE version_id = ?",
            (version_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "version_id": row["version_id"],
            "target_company": row["target_company"],
            "request_signature": row["request_signature"],
            "source_kind": row["source_kind"],
            "parent_version_id": row["parent_version_id"],
            "trigger_feedback_id": row["trigger_feedback_id"],
            "evolution_stage": row["evolution_stage"],
            "request": json.loads(row["request_json"] or "{}"),
            "plan": json.loads(row["plan_json"] or "{}"),
            "patterns": json.loads(row["patterns_json"] or "[]"),
            "notes": row["notes"],
            "created_at": row["created_at"],
        }

    def list_criteria_compiler_runs(self, version_id: int = 0, target_company: str = "", limit: int = 100) -> list[dict[str, Any]]:
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
        return [
            {
                "compiler_run_id": row["compiler_run_id"],
                "version_id": row["version_id"],
                "job_id": row["job_id"],
                "trigger_feedback_id": row["trigger_feedback_id"],
                "provider_name": row["provider_name"],
                "compiler_kind": row["compiler_kind"],
                "status": row["status"],
                "input": json.loads(row["input_json"] or "{}"),
                "output": json.loads(row["output_json"] or "{}"),
                "notes": row["notes"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

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
            request_target_company = str(request_payload.get("target_company") or "").strip()
            if request_target_company and not target_company:
                target_company = request_target_company
            metadata.setdefault("request_payload", request_payload)
            metadata.setdefault("request_signature", request_signature(request_payload))
            metadata.setdefault("request_family_signature", request_family_signature(request_payload))
            metadata.setdefault("target_company", request_target_company)
        return target_company, metadata

    def _confidence_policy_control_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "control_id": row["control_id"],
            "target_company": row["target_company"],
            "request_signature": row["request_signature"],
            "request_family_signature": row["request_family_signature"],
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
            "summary": summary_payload,
            "artifact_path": row["artifact_path"],
            "request_signature": str(row["request_signature"] or ""),
            "request_family_signature": str(row["request_family_signature"] or ""),
            "requester_id": str(row["requester_id"] or ""),
            "tenant_id": str(row["tenant_id"] or ""),
            "idempotency_key": str(row["idempotency_key"] or ""),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _query_dispatch_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = {}
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except json.JSONDecodeError:
            payload = {}
        return {
            "dispatch_id": int(row["dispatch_id"] or 0),
            "target_company": str(row["target_company"] or ""),
            "request_signature": str(row["request_signature"] or ""),
            "request_family_signature": str(row["request_family_signature"] or ""),
            "requester_id": str(row["requester_id"] or ""),
            "tenant_id": str(row["tenant_id"] or ""),
            "idempotency_key": str(row["idempotency_key"] or ""),
            "strategy": str(row["strategy"] or ""),
            "status": str(row["status"] or ""),
            "source_job_id": str(row["source_job_id"] or ""),
            "created_job_id": str(row["created_job_id"] or ""),
            "payload": payload,
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
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
            "target_company": str(row["target_company"] or ""),
            "company_key": str(row["company_key"] or ""),
            "snapshot_id": str(row["snapshot_id"] or ""),
            "asset_view": str(row["asset_view"] or ""),
            "status": str(row["status"] or ""),
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
            "completeness_band": str(row["completeness_band"] or ""),
            "current_lane_coverage": current_lane_coverage,
            "former_lane_coverage": former_lane_coverage,
            "current_lane_effective_candidate_count": int(row["current_lane_effective_candidate_count"] or 0),
            "former_lane_effective_candidate_count": int(row["former_lane_effective_candidate_count"] or 0),
            "current_lane_effective_ready": bool(row["current_lane_effective_ready"]),
            "former_lane_effective_ready": bool(row["former_lane_effective_ready"]),
            "source_snapshot_selection": source_snapshot_selection,
            "selected_snapshot_ids": [str(item).strip() for item in selected_snapshot_ids if str(item).strip()],
            "source_path": str(row["source_path"] or ""),
            "source_job_id": str(row["source_job_id"] or ""),
            "summary": summary,
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
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
            "shard_key": str(row["shard_key"] or ""),
            "target_company": str(row["target_company"] or ""),
            "company_key": str(row["company_key"] or ""),
            "snapshot_id": str(row["snapshot_id"] or ""),
            "asset_view": str(row["asset_view"] or ""),
            "lane": str(row["lane"] or ""),
            "status": str(row["status"] or ""),
            "employment_scope": str(row["employment_scope"] or ""),
            "strategy_type": str(row["strategy_type"] or ""),
            "shard_id": str(row["shard_id"] or ""),
            "shard_title": str(row["shard_title"] or ""),
            "search_query": str(row["search_query"] or ""),
            "query_signature": str(row["query_signature"] or ""),
            "company_scope": [str(item).strip() for item in company_scope if str(item).strip()],
            "locations": [str(item).strip() for item in locations if str(item).strip()],
            "function_ids": [str(item).strip() for item in function_ids if str(item).strip()],
            "result_count": int(row["result_count"] or 0),
            "estimated_total_count": int(row["estimated_total_count"] or 0),
            "provider_cap_hit": bool(row["provider_cap_hit"]),
            "source_path": str(row["source_path"] or ""),
            "source_job_id": str(row["source_job_id"] or ""),
            "metadata": metadata,
            "first_seen_at": str(row["first_seen_at"] or ""),
            "last_completed_at": str(row["last_completed_at"] or ""),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _linkedin_profile_registry_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
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

    def _linkedin_profile_registry_lease_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
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

    def _linkedin_profile_registry_backfill_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
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
        return {
            "review_id": row["review_id"],
            "target_company": row["target_company"],
            "request_signature": row["request_signature"],
            "request_family_signature": row["request_family_signature"],
            "status": row["status"],
            "risk_level": row["risk_level"],
            "required_before_execution": bool(row["required_before_execution"]),
            "request": json.loads(row["request_json"] or "{}"),
            "plan": json.loads(row["plan_json"] or "{}"),
            "gate": json.loads(row["gate_json"] or "{}"),
            "decision": json.loads(row["decision_json"] or "{}"),
            "reviewer": row["reviewer"],
            "review_notes": row["review_notes"],
            "approved_at": row["approved_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
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

    def _get_criteria_pattern(self, target_company: str, pattern_type: str, subject: str, value: str) -> dict[str, Any] | None:
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
        return {
            "pattern_id": row["pattern_id"],
            "target_company": row["target_company"],
            "pattern_type": row["pattern_type"],
            "subject": row["subject"],
            "value": row["value"],
            "status": row["status"],
            "confidence": row["confidence"],
            "source_feedback_id": row["source_feedback_id"],
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _ensure_column(self, table_name: str, column_name: str, column_definition: str) -> None:
        rows = self._connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing = {row["name"] for row in rows}
        if column_name in existing:
            return
        self._connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")

    def _insert_candidates_and_evidence(self, candidates: list[Candidate], evidence: list[EvidenceRecord]) -> None:
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
            [self._candidate_payload(candidate) for candidate in candidates],
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
            [self._evidence_payload(item) for item in evidence],
        )

    def _candidate_payload(self, candidate: Candidate) -> dict[str, Any]:
        payload = normalize_candidate(candidate).to_record()
        payload["metadata_json"] = json.dumps(payload.pop("metadata"), ensure_ascii=False)
        return payload

    def _evidence_payload(self, evidence: EvidenceRecord) -> dict[str, Any]:
        payload = evidence.to_record()
        payload["metadata_json"] = json.dumps(payload.pop("metadata"), ensure_ascii=False)
        return payload

    def _candidate_from_row(self, row: sqlite3.Row) -> Candidate:
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
    candidate_id = str(payload.get("candidate_id") or (payload.get("candidate") or {}).get("candidate_id") or "").strip()
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
    parts = [segment for segment in normalized.replace("\\", "/").split("/") if segment]
    for index, segment in enumerate(parts):
        if segment == "company_assets" and index + 2 < len(parts):
            return parts[index + 2]
    return ""


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
