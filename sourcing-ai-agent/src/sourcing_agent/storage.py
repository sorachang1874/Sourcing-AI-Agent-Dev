from __future__ import annotations

import json
from hashlib import sha1
from pathlib import Path
import sqlite3
import threading
from typing import Any

from .domain import Candidate, EvidenceRecord, JobRequest, normalize_candidate
from .request_matching import MATCH_THRESHOLD, request_family_score, request_family_signature, request_signature
from .worker_scheduler import effective_worker_status, wait_stage


class SQLiteStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self.init_schema()

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
                """
            )
            self._ensure_column("jobs", "job_type", "TEXT NOT NULL DEFAULT 'retrieval'")
            self._ensure_column("jobs", "stage", "TEXT NOT NULL DEFAULT 'pending'")
            self._ensure_column("jobs", "plan_json", "TEXT")
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
    ) -> None:
        summary_json = json.dumps(summary_payload or {}, ensure_ascii=False)
        request_json = json.dumps(request_payload, ensure_ascii=False)
        plan_json = json.dumps(plan_payload or {}, ensure_ascii=False)
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO jobs (job_id, job_type, status, stage, request_json, plan_json, summary_json, artifact_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    job_type = excluded.job_type,
                    status = excluded.status,
                    stage = excluded.stage,
                    request_json = excluded.request_json,
                    plan_json = excluded.plan_json,
                    summary_json = excluded.summary_json,
                    artifact_path = excluded.artifact_path,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (job_id, job_type, status, stage, request_json, plan_json, summary_json, artifact_path),
            )

    def append_job_event(
        self,
        job_id: str,
        stage: str,
        status: str,
        detail: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        payload_json = json.dumps(payload or {}, ensure_ascii=False)
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO job_events (job_id, stage, status, detail, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, stage, status, detail, payload_json),
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
        row = self._connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        return {
            "job_id": row["job_id"],
            "job_type": row["job_type"],
            "status": row["status"],
            "stage": row["stage"],
            "request": json.loads(row["request_json"]),
            "plan": json.loads(row["plan_json"] or "{}"),
            "summary": json.loads(row["summary_json"] or "{}"),
            "artifact_path": row["artifact_path"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

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

    def list_job_events(self, job_id: str) -> list[dict[str, Any]]:
        rows = self._connection.execute(
            "SELECT * FROM job_events WHERE job_id = ? ORDER BY event_id",
            (job_id,),
        ).fetchall()
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
            return {
                "job_id": job_id,
                "job_type": row["job_type"],
                "status": row["status"],
                "stage": row["stage"],
                "request": request_payload,
                "plan": json.loads(row["plan_json"] or "{}"),
                "summary": json.loads(row["summary_json"] or "{}"),
                "artifact_path": row["artifact_path"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
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
            job_payload = {
                "job_id": job_id,
                "job_type": row["job_type"],
                "status": row["status"],
                "stage": row["stage"],
                "request": candidate_request,
                "plan": json.loads(row["plan_json"] or "{}"),
                "summary": json.loads(row["summary_json"] or "{}"),
                "artifact_path": row["artifact_path"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
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
            fallback["baseline_match"] = {
                "selected_via": "latest_company_fallback",
                "family_score": float(best_match.get("score") or 0.0),
                "exact_request_match": bool(best_match.get("exact_request_match")),
                "exact_family_match": bool(best_match.get("exact_family_match")),
                "request_signature": request_signature(request_payload),
                "request_family_signature": request_family_signature(request_payload),
                "matched_request_signature": request_signature(fallback.get("request") or {}),
                "matched_request_family_signature": request_family_signature(fallback.get("request") or {}),
                "reasons": list(best_match.get("reasons") or []),
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
        }
        return best_job

    def record_criteria_feedback(self, payload: dict[str, Any]) -> dict[str, Any]:
        target_company, metadata = self._prepare_feedback_context(payload)
        feedback_type = str(payload.get("feedback_type") or "").strip()
        subject = str(payload.get("subject") or "").strip()
        value = str(payload.get("value") or "").strip()
        reviewer = str(payload.get("reviewer") or "").strip()
        notes = str(payload.get("notes") or "").strip()
        job_id = str(payload.get("job_id") or "").strip()
        candidate_id = str(payload.get("candidate_id") or "").strip()
        payload_json = json.dumps(metadata, ensure_ascii=False)
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
