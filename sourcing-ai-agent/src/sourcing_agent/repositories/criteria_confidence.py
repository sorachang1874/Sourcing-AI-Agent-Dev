"""Track B ②.1 — criteria/confidence domain repository (verbatim port of the store methods).

This module is the PUBLIC API for the criteria evolution + confidence policy domain. It owns the eight
control-plane tables:

- ``criteria_feedback`` — reviewer feedback events.
- ``criteria_patterns`` — derived/curated criteria patterns.
- ``criteria_pattern_suggestions`` — machine-suggested patterns awaiting review.
- ``criteria_result_diffs`` — before/after rerun diffs.
- ``confidence_policy_runs`` — per-run confidence policy snapshots.
- ``confidence_policy_controls`` — active confidence policy overrides.
- ``criteria_versions`` — versioned criteria plans.
- ``criteria_compiler_runs`` — compiler invocations per version.

``CriteriaConfidenceRepository`` ports every read/write method 1:1 from the retired ``ControlPlaneStore``
methods (②.1). Bodies are verbatim: authority guards, fail-closed raises, and the Tier-A raise / Tier-B
silent ``[]``/``None`` return sentinels are preserved exactly, method for method. The eight row mappers are
hand-written RAW ``_row_value`` (None-)passthrough mappers — they are NOT converted to ``TableDescriptor``;
the write paths persist NULL into nullable bigint FK columns, so descriptor-ization is deferred to the ③
jsonb/typed round to avoid silently changing the passthrough semantics.

The one cross-domain seam is the ``get_job`` lookup used by ``_prepare_feedback_context`` to resolve a
request payload from a job id. It is injected via the ``job_lookup`` callback (kept absent-safe: when no
callback is wired the lookup behaves exactly as a payload with no resolvable job does today).

All writes route through the base ``_call_native_write`` primitive
(``insert_row_with_generated_id`` / ``update_row_returning`` / ``upsert_row_with_generated_id``); no literal
conflict-clause SQL lives in this package.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from hashlib import sha1
from typing import Any

from ..control_plane_repository import Repository
from ..control_plane_serde import json_safe_payload as _json_safe_payload
from ..control_plane_time import utc_now_timestamp
from ..domain import JobRequest
from ..request_matching import request_signature_context


def _row_value(row: Any, key: str, default: Any = "") -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except Exception:
        return default


def _payload_signature(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return sha1(serialized.encode("utf-8")).hexdigest()[:16]


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


class CriteriaConfidenceRepository(Repository):
    """Typed repository for the criteria/confidence domain (full ②.1 surface)."""

    def __init__(self, adapter: Any, *, job_lookup: Callable[[str], Any] | None = None) -> None:
        super().__init__(adapter)
        self._job_lookup = job_lookup

    def record_feedback(self, payload: dict[str, Any]) -> dict[str, Any]:
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
        if self._should_prefer_read("criteria_feedback"):
            row = self._call_native_write(
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
                    "created_at": utc_now_timestamp(),
                },
            )
            feedback_id = int((row or {}).get("feedback_id") or 0)
        if feedback_id <= 0:
            raise RuntimeError(
                "postgres-only invariant violated for criteria_feedback in record_criteria_feedback: should_prefer_read "
                "returned False or native insert returned no feedback_id; legacy SQLite tail retired (B4)"
            )
        derived_patterns = self._derive_patterns_from_feedback(
            feedback_id,
            {
                **payload,
                "target_company": target_company,
                "metadata": metadata,
            },
        )
        return {"feedback_id": feedback_id, "patterns": derived_patterns}

    def list_feedback(self, target_company: str = "", limit: int = 100) -> list[dict[str, Any]]:
        postgres_rows = self._select_rows(
            "criteria_feedback",
            row_builder=self._criteria_feedback_from_row,
            where_sql="lower(target_company) = lower(%s)" if target_company else "",
            params=[target_company] if target_company else [],
            order_by_sql="feedback_id DESC",
            limit=limit,
        )
        if postgres_rows or self._strict_authoritative("criteria_feedback"):
            return postgres_rows
        raise RuntimeError(
            "postgres-only invariant violated for criteria_feedback in list_criteria_feedback: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_feedback(self, feedback_id: int) -> dict[str, Any] | None:
        if feedback_id <= 0:
            return None
        postgres_row = self._select_row(
            "criteria_feedback",
            row_builder=self._criteria_feedback_from_row,
            where_sql="feedback_id = %s",
            params=[feedback_id],
        )
        if postgres_row is not None:
            return postgres_row
        if self._strict_authoritative("criteria_feedback"):
            return None
        raise RuntimeError(
            "postgres-only invariant violated for criteria_feedback in get_criteria_feedback: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_patterns(
        self,
        target_company: str = "",
        *,
        status: str = "active",
        pattern_type: str = "",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
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
        postgres_rows = self._select_rows(
            "criteria_patterns",
            row_builder=self._criteria_pattern_from_row,
            where_sql=" AND ".join(postgres_where_clauses),
            params=postgres_params,
            order_by_sql="updated_at DESC, pattern_id DESC",
            limit=limit,
        )
        if postgres_rows or self._strict_authoritative("criteria_patterns"):
            return postgres_rows
        raise RuntimeError(
            "postgres-only invariant violated for criteria_patterns in list_criteria_patterns: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def record_suggestions(self, suggestions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not suggestions:
            return []
        if self._should_prefer_read("criteria_pattern_suggestions"):
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
                existing_rows = self._select_rows(
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
                now_timestamp = utc_now_timestamp()
                if existing_row is not None:
                    self._call_native_write(
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
                self._call_native_write(
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
            return self.list_suggestions(source_feedback_id=max(source_feedback_ids), limit=20)
        raise RuntimeError(
            "postgres-only invariant violated for criteria_pattern_suggestions in record_pattern_suggestions: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_suggestions(
        self,
        *,
        target_company: str = "",
        status: str = "",
        source_feedback_id: int = 0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        postgres_rows = self._select_rows(
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
        if postgres_rows or self._strict_authoritative("criteria_pattern_suggestions"):
            return postgres_rows
        raise RuntimeError(
            "postgres-only invariant violated for criteria_pattern_suggestions in list_pattern_suggestions: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_suggestion(self, suggestion_id: int) -> dict[str, Any] | None:
        if suggestion_id <= 0:
            return None
        postgres_row = self._select_row(
            "criteria_pattern_suggestions",
            row_builder=self._criteria_pattern_suggestion_from_row,
            where_sql="suggestion_id = %s",
            params=[suggestion_id],
        )
        if postgres_row is not None:
            return postgres_row
        if self._strict_authoritative("criteria_pattern_suggestions"):
            return None
        raise RuntimeError(
            "postgres-only invariant violated for criteria_pattern_suggestions in get_pattern_suggestion: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def review_suggestion(
        self,
        *,
        suggestion_id: int,
        action: str,
        reviewer: str = "",
        notes: str = "",
    ) -> dict[str, Any] | None:
        suggestion = self.get_suggestion(suggestion_id)
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
            applied_pattern = self.upsert_pattern(
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
        if self._should_prefer_read("criteria_pattern_suggestions"):
            updated_row = self._call_native_write(
                "update_row_returning",
                table_name="criteria_pattern_suggestions",
                id_column="suggestion_id",
                id_value=suggestion_id,
                row={
                    "status": status,
                    "reviewed_by": reviewer,
                    "review_notes": notes,
                    "applied_pattern_id": applied_pattern_id or None,
                    "reviewed_at": utc_now_timestamp(),
                    "updated_at": utc_now_timestamp(),
                },
            )
            if updated_row is not None:
                reviewed = self._criteria_pattern_suggestion_from_row(updated_row)
            # update_row_returning None == suggestion row absent; surface {"suggestion": None}
            # (the retired SQLite tail's re-read would find nothing under postgres-only either).
            return {
                "suggestion": reviewed,
                "applied_pattern": applied_pattern,
                "action": normalized_action or status,
                "status": status,
            }
        raise RuntimeError(
            "postgres-only invariant violated for criteria_pattern_suggestions in review_pattern_suggestion: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def record_result_diff(
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
        if self._should_prefer_read("criteria_result_diffs"):
            row = self._call_native_write(
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
                    "created_at": utc_now_timestamp(),
                },
            )
            diff_id = int((row or {}).get("diff_id") or 0)
            if diff_id <= 0:
                # Track B B4: insert_row_with_generated_id returns None (no exception) only for
                # sequence-resolution anomalies; fail closed instead of falling through to the
                # retired SQLite shadow tail.
                self._raise_write_failure(
                    table_name="criteria_result_diffs",
                    method_name="insert_row_with_generated_id",
                    reason="native insert returned no generated id under postgres_only",
                )
            return {"diff_id": diff_id}
        raise RuntimeError(
            "postgres-only invariant violated for criteria_result_diffs in record_criteria_result_diff: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def record_policy_run(
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
            "created_at": utc_now_timestamp(),
        }
        if self._should_prefer_read("confidence_policy_runs"):
            row = self._call_native_write(
                "insert_row_with_generated_id",
                table_name="confidence_policy_runs",
                row=row_payload,
            )
            policy_run_id = int((row or {}).get("policy_run_id") or 0)
            if policy_run_id <= 0:
                # Track B B4: insert_row_with_generated_id returns None (no exception) only for
                # sequence-resolution anomalies; fail closed instead of falling through to the
                # retired SQLite shadow tail.
                self._raise_write_failure(
                    table_name="confidence_policy_runs",
                    method_name="insert_row_with_generated_id",
                    reason="native insert returned no generated id under postgres_only",
                )
            return {"policy_run_id": policy_run_id}
        raise RuntimeError(
            "postgres-only invariant violated for confidence_policy_runs in record_confidence_policy_run: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_policy_runs(self, target_company: str = "", limit: int = 50) -> list[dict[str, Any]]:
        postgres_rows = self._select_rows(
            "confidence_policy_runs",
            row_builder=self._confidence_policy_run_from_row,
            where_sql="lower(target_company) = lower(%s)" if target_company else "",
            params=[target_company] if target_company else [],
            order_by_sql="policy_run_id DESC",
            limit=limit,
        )
        return postgres_rows

    def create_policy_control(
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
        signature_context = request_signature_context(request_payload)
        request_sig = str(signature_context.get("request_signature") or "")
        request_family_sig = str(signature_context.get("request_family_signature") or "")
        matching_request_sig = str(signature_context.get("matching_request_signature") or request_sig)
        matching_request_family_sig = str(
            signature_context.get("matching_request_family_signature") or request_family_sig
        )
        normalized_scope = str(scope_kind or "request_family").strip() or "request_family"
        if self._should_prefer_read("confidence_policy_controls"):
            existing_rows = self._select_rows(
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
                self._call_native_write(
                    "update_row_returning",
                    table_name="confidence_policy_controls",
                    id_column="control_id",
                    id_value=int(existing_row.get("control_id") or 0),
                    row={
                        "status": "inactive",
                        "updated_at": utc_now_timestamp(),
                    },
                )
            now = utc_now_timestamp()
            row = self._call_native_write(
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
            # Track B B4: insert_row_with_generated_id returns None (no exception) only for
            # sequence-resolution anomalies; fail closed instead of falling through to the
            # retired SQLite shadow tail.
            self._raise_write_failure(
                table_name="confidence_policy_controls",
                method_name="insert_row_with_generated_id",
                reason="native insert returned no row under postgres_only",
            )
        raise RuntimeError(
            "postgres-only invariant violated for confidence_policy_controls in create_confidence_policy_control: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def find_active_policy_control(
        self,
        *,
        target_company: str,
        request_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        signature_context = request_signature_context(request_payload)
        request_sig = str(signature_context.get("request_signature") or "")
        request_family_sig = str(signature_context.get("request_family_signature") or "")
        matching_request_sig = str(signature_context.get("matching_request_signature") or request_sig)
        matching_request_family_sig = str(
            signature_context.get("matching_request_family_signature") or request_family_sig
        )
        rows = self.list_policy_controls(target_company=target_company, status="active", limit=500)
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

    def get_policy_control(self, control_id: int) -> dict[str, Any] | None:
        if control_id <= 0:
            return None
        postgres_row = self._select_row(
            "confidence_policy_controls",
            row_builder=self._confidence_policy_control_from_row,
            where_sql="control_id = %s",
            params=[control_id],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_policy_controls(
        self,
        *,
        target_company: str = "",
        status: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        postgres_rows = self._select_rows(
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
        return []

    def deactivate_policy_control(
        self,
        *,
        control_id: int = 0,
        target_company: str = "",
        request_payload: dict[str, Any] | None = None,
        scope_kind: str = "",
    ) -> dict[str, Any]:
        if control_id:
            if self._should_prefer_read("confidence_policy_controls"):
                row = self._call_native_write(
                    "update_row_returning",
                    table_name="confidence_policy_controls",
                    id_column="control_id",
                    id_value=control_id,
                    row={
                        "status": "inactive",
                        "updated_at": utc_now_timestamp(),
                    },
                )
                return {"deactivated_count": 1 if row is not None else 0}
            raise RuntimeError(
                "postgres-only invariant violated for confidence_policy_controls in deactivate_confidence_policy_control: should_prefer_read "
                "returned False; legacy SQLite tail retired (B4)"
            )
        normalized_scope = str(scope_kind or "request_family").strip() or "request_family"
        request_payload = request_payload or {}
        signature_context = request_signature_context(request_payload)
        if self._should_prefer_read("confidence_policy_controls"):
            existing_rows = self._select_rows(
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
                row = self._call_native_write(
                    "update_row_returning",
                    table_name="confidence_policy_controls",
                    id_column="control_id",
                    id_value=int(existing_row.get("control_id") or 0),
                    row={
                        "status": "inactive",
                        "updated_at": utc_now_timestamp(),
                    },
                )
                if row is not None:
                    deactivated_count += 1
            return {"deactivated_count": deactivated_count}
        raise RuntimeError(
            "postgres-only invariant violated for confidence_policy_controls in deactivate_confidence_policy_control: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_result_diffs(self, target_company: str = "", limit: int = 50) -> list[dict[str, Any]]:
        postgres_rows = self._select_rows(
            "criteria_result_diffs",
            row_builder=self._criteria_result_diff_from_row,
            where_sql="lower(target_company) = lower(%s)" if target_company else "",
            params=[target_company] if target_company else [],
            order_by_sql="diff_id DESC",
            limit=limit,
        )
        return postgres_rows

    def create_version(
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
            "created_at": utc_now_timestamp(),
        }
        if self._should_prefer_read("criteria_versions"):
            row = self._call_native_write(
                "insert_row_with_generated_id",
                table_name="criteria_versions",
                row=row_payload,
            )
            version_id = int((row or {}).get("version_id") or 0)
            if version_id > 0:
                return {"version_id": version_id, "request_signature": request_signature}
            self._raise_write_failure(
                table_name="criteria_versions",
                method_name="insert_row_with_generated_id",
                reason="native insert returned no generated version_id under postgres_only",
            )
        raise RuntimeError(
            "postgres-only invariant violated for criteria_versions in create_criteria_version: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def record_compiler_run(
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
            "created_at": utc_now_timestamp(),
        }
        if self._should_prefer_read("criteria_compiler_runs"):
            row = self._call_native_write(
                "insert_row_with_generated_id",
                table_name="criteria_compiler_runs",
                row=row_payload,
            )
            compiler_run_id = int((row or {}).get("compiler_run_id") or 0)
            if compiler_run_id > 0:
                return {"compiler_run_id": compiler_run_id}
            self._raise_write_failure(
                table_name="criteria_compiler_runs",
                method_name="insert_row_with_generated_id",
                reason="native insert returned no generated compiler_run_id under postgres_only",
            )
        raise RuntimeError(
            "postgres-only invariant violated for criteria_compiler_runs in record_criteria_compiler_run: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_versions(self, target_company: str = "", limit: int = 50) -> list[dict[str, Any]]:
        postgres_rows = self._select_rows(
            "criteria_versions",
            row_builder=self._criteria_version_from_row,
            where_sql="lower(target_company) = lower(%s)" if target_company else "",
            params=[target_company] if target_company else [],
            order_by_sql="version_id DESC",
            limit=limit,
        )
        return postgres_rows

    def get_version(self, version_id: int) -> dict[str, Any] | None:
        if version_id <= 0:
            return None
        postgres_row = self._select_row(
            "criteria_versions",
            row_builder=self._criteria_version_from_row,
            where_sql="version_id = %s",
            params=[version_id],
        )
        if postgres_row is not None:
            return postgres_row
        return None

    def list_compiler_runs(
        self, version_id: int = 0, target_company: str = "", limit: int = 100
    ) -> list[dict[str, Any]]:
        if self._should_prefer_read("criteria_compiler_runs"):
            if version_id:
                postgres_rows = self._select_rows(
                    "criteria_compiler_runs",
                    row_builder=self._criteria_compiler_run_from_row,
                    where_sql="version_id = %s",
                    params=[version_id],
                    order_by_sql="compiler_run_id DESC",
                    limit=limit,
                )
                if postgres_rows or self._strict_authoritative("criteria_compiler_runs"):
                    return postgres_rows
            elif target_company:
                version_rows = self._select_rows(
                    "criteria_versions",
                    row_builder=self._criteria_version_from_row,
                    where_sql="lower(target_company) = lower(%s)",
                    params=[target_company],
                    order_by_sql="version_id DESC",
                    limit=0,
                )
                version_ids = {int(row.get("version_id") or 0) for row in version_rows if int(row.get("version_id") or 0)}
                compiler_rows = self._select_rows(
                    "criteria_compiler_runs",
                    row_builder=self._criteria_compiler_run_from_row,
                    order_by_sql="compiler_run_id DESC",
                    limit=0,
                )
                filtered_rows = [
                    row for row in compiler_rows if int(row.get("version_id") or 0) in version_ids
                ][: max(0, int(limit or 0)) or len(compiler_rows)]
                if filtered_rows or self._strict_authoritative("criteria_compiler_runs"):
                    return filtered_rows
            else:
                postgres_rows = self._select_rows(
                    "criteria_compiler_runs",
                    row_builder=self._criteria_compiler_run_from_row,
                    order_by_sql="compiler_run_id DESC",
                    limit=limit,
                )
                if postgres_rows or self._strict_authoritative("criteria_compiler_runs"):
                    return postgres_rows
        raise RuntimeError(
            "postgres-only invariant violated for criteria_compiler_runs in list_criteria_compiler_runs: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_latest_version(self, target_company: str = "") -> dict[str, Any] | None:
        versions = self.list_versions(target_company=target_company, limit=1)
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
            pattern = self.upsert_pattern(
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
                job = self._job_lookup(job_id) if callable(self._job_lookup) else None
                if job is not None and isinstance(job.get("request"), dict):
                    request_payload = dict(job["request"])
        if isinstance(request_payload, dict) and request_payload:
            request_payload = JobRequest.from_payload(request_payload).to_record()
            existing_matching = (
                metadata.get("request_matching") if isinstance(metadata.get("request_matching"), dict) else {}
            )
            signature_context = request_signature_context(
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

    def _confidence_policy_control_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
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

    def upsert_pattern(
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
        if self._should_prefer_read("criteria_patterns"):
            row = self._call_native_write(
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
                    "updated_at": utc_now_timestamp(),
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
            self._raise_write_failure(
                table_name="criteria_patterns",
                method_name="upsert_criteria_pattern",
                reason="native upsert returned no row under postgres_only",
            )
        raise RuntimeError(
            "postgres-only invariant violated for criteria_patterns in upsert_criteria_pattern: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )
