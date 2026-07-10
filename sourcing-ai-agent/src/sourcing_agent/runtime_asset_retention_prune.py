from __future__ import annotations

import hashlib
import json
import os
import posixpath
import shutil
import stat
import subprocess
import tarfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CONTRACT_VERSION = "runtime_asset_retention_prune_v1"
BUNDLE_MANIFEST_CONTRACT_VERSION = "runtime_asset_supersession_cold_bundle_manifest_v1"
INDEPENDENT_REVIEW_EFFECTIVE_CONFIG_CONTRACT_VERSION = "independent_review_effective_config_v2"
REQUIRED_REVIEW_ARTIFACT_METADATA_FIELDS = (
    "reviewer_model",
    "reviewer_reasoning_effort",
    "reviewer_service_tier",
    "reviewer_config_path",
    "reviewer_config_sha256",
    "reviewer_exit_code",
    "reviewer_codex_cli_version",
    "reviewer_thread_id",
    "reviewer_rollout_path",
    "reviewer_rollout_sha256",
    "reviewer_effective_config_path",
    "reviewer_effective_config_sha256",
    "review_scope_mode",
    "review_base_ref",
    "review_resolved_base_commit",
    "review_resolved_head_commit",
    "review_git_diff_sha256",
    "review_git_tree_sha256",
    "review_extra_context_sha256",
    "review_scope_digest_sha256",
    "prompt_path",
    "prompt_sha256",
    "events_path",
    "events_sha256",
    "raw_output_path",
    "raw_output_sha256",
    "command",
)
INDEPENDENT_REVIEW_PINNED_SCOPE_MODE = "pinned_commit_diff"
INDEPENDENT_REVIEW_REFERENCE_SCOPE_MODE = "reference_only_worktree"
_INDEPENDENT_REVIEW_ROLLOUT_SETTING_EVENT_TYPES = {
    "session_configured",
    "thread_settings_applied",
    "turn_context",
}
_INDEPENDENT_REVIEW_CAUSAL_BOOLEAN_FIELDS = (
    "rollout_json_valid",
    "text_utf8_valid",
    "session_source_exec",
    "single_task_turn",
    "no_abort",
    "prompt_response_item_exact",
    "prompt_event_message_exact",
    "final_response_item_exact",
    "final_event_message_exact",
    "task_complete_final_exact",
)
_INDEPENDENT_REVIEW_CAUSAL_FIELDS = {
    "turn_id",
    "prompt_sha256",
    "normalized_final_output_sha256",
    *_INDEPENDENT_REVIEW_CAUSAL_BOOLEAN_FIELDS,
}
_INDEPENDENT_REVIEW_SCOPE_DIGEST_FIELDS = (
    "title",
    "base_ref",
    "scope_mode",
    "resolved_base_commit",
    "resolved_head_commit",
    "files",
    "git_diff_sha256",
    "git_tree_sha256",
    "extra_context_sha256",
)
_REVIEW_GIT_RUN = subprocess.run
ALLOWED_PRUNE_ROOTS = ("runtime/test_env", "output")
PROTECTED_NAMES = {"company_assets", "secrets", "object_store"}
TTL_LOCAL_REBUILDABLE_POLICY = "ttl_local_rebuildable"
TTL_LOCAL_REBUILDABLE_PRUNE_ROOT = "runtime/test_env"
TTL_LOCAL_REBUILDABLE_PROTECTED_NAMES = {
    "jobs",
    "job_locks",
    "services",
    "service_logs",
    "object_sync",
    "runtime_metrics",
    "provider_cache",
    "hot_cache_company_assets",
}
SIGNOFF_OR_PRESSURE_MARKERS = (
    "w6",
    "nightly",
    "pre_manual",
    "pressure",
    "signoff",
    "closeout",
    "smoke",
    "review",
    "rerun",
    "contract",
    "board_runtime",
    "profile_contract",
    "scripted",
)
ACTIVE_PROCESS_TOKENS = (
    "run_simulate_smoke",
    "run_explain_dry_run",
    "run-worker-daemon-service",
    "worker_daemon",
    "service_daemon",
    "dev_backend",
    "uvicorn",
)


@dataclass(frozen=True)
class _ReviewGitProject:
    """Map project-relative review paths onto the containing Git worktree."""

    git_toplevel: Path
    project_prefix: str

    def tree_path(self, project_path: str) -> str:
        return posixpath.join(self.project_prefix, project_path) if self.project_prefix else project_path

    def object_spec(self, revision: str, project_path: str) -> str:
        return f"{revision}:{self.tree_path(project_path)}"

    def pathspec(self, project_path: str) -> str:
        return f":(top,literal){self.tree_path(project_path)}"


def build_runtime_asset_prune_plan(
    *,
    audit_report: dict[str, Any],
    workspace_root: str | Path = ".",
    min_age_days: int = 10,
    target_free_bytes: int = 0,
    max_entries: int = 0,
    review_artifact: str = "",
    review_title: str = "",
    review_required_files: list[str] | None = None,
    accepted_retention_exception: str = "",
    cold_copy_manifest: str = "",
    reuse_index_effect: str = "none_runtime_output_only",
    allowed_retention_classes: list[str] | None = None,
) -> dict[str, Any]:
    """Convert a read-only runtime retention audit into an explicit prune manifest.

    The plan only targets local runtime/output copies. It does not touch canonical
    company snapshots, registry pointers, provider cache entries, or PG state.
    """

    root = _resolve_root(workspace_root)
    generated_at = datetime.now(timezone.utc)
    allowed_classes = _normalize_retention_classes(allowed_retention_classes)
    candidates: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for raw_item in list(audit_report.get("directories") or []):
        item = dict(raw_item or {})
        path_text = str(item.get("path") or "").strip()
        if not path_text:
            skipped.append({"path": path_text, "reason": "missing_path"})
            continue
        reason = _skip_reason(item=item, path_text=path_text, root=root, now=generated_at, min_age_days=min_age_days)
        if reason:
            skipped.append({"path": path_text, "reason": reason})
            continue
        matched_markers = list(item.get("matched_markers") or [])
        retention_class = _effective_retention_class(
            path_text=path_text,
            retention_class=str(item.get("retention_class") or ""),
            matched_markers=matched_markers,
        )
        if allowed_classes and retention_class not in allowed_classes:
            skipped.append({"path": path_text, "reason": "retention_class_not_allowed_for_plan"})
            continue
        candidates.append(
            {
                "action": "remove_local_runtime_copy",
                "path": path_text,
                "size_bytes": int(item.get("size_bytes") or 0),
                "file_count": int(item.get("file_count") or 0),
                "directory_count": int(item.get("directory_count") or 0),
                "latest_mtime": str(item.get("latest_mtime") or ""),
                "retention_class": retention_class,
                "matched_markers": matched_markers,
                "source_retention_class": str(item.get("retention_class") or ""),
                "source_scan_root": str(item.get("scan_root") or ""),
            }
        )
    candidates.sort(key=lambda payload: (int(payload["size_bytes"]), str(payload["path"])), reverse=True)
    selected: list[dict[str, Any]] = []
    selected_bytes = 0
    for item in candidates:
        if max_entries and len(selected) >= max_entries:
            skipped.append({"path": item["path"], "reason": "max_entries_reached"})
            continue
        if target_free_bytes and selected_bytes >= target_free_bytes:
            skipped.append({"path": item["path"], "reason": "target_free_bytes_reached"})
            continue
        selected.append(item)
        selected_bytes += int(item["size_bytes"])
    status = "ready_for_review" if selected else "no_prune_candidates"
    return {
        "contract_version": CONTRACT_VERSION,
        "status": status,
        "generated_at": generated_at.isoformat(),
        "workspace_root": str(root),
        "source_audit_contract_version": str(audit_report.get("contract_version") or ""),
        "source_audit_generated_at": str(audit_report.get("generated_at") or ""),
        "allowed_prune_roots": list(ALLOWED_PRUNE_ROOTS),
        "protected_names": sorted(PROTECTED_NAMES),
        "min_age_days": int(min_age_days),
        "target_free_bytes": int(target_free_bytes or 0),
        "max_entries": int(max_entries or 0),
        "allowed_retention_classes": allowed_classes,
        "destructive_review_evidence": {
            "review_artifact": str(review_artifact or ""),
            "review_title": str(review_title or ""),
            "review_required_files": list(review_required_files or []),
            "accepted_retention_exception": str(accepted_retention_exception or ""),
            "cold_copy_manifest": str(cold_copy_manifest or ""),
            "reuse_index_effect": str(reuse_index_effect or ""),
        },
        "apply_requires": [
            "explicit_manifest",
            "--apply",
            "--reviewed",
            "valid_go_review_artifact",
            "cold_copy_manifest_or_accepted_retention_exception",
            "no_active_runtime_processes",
        ],
        "summary": {
            "candidate_count": len(candidates),
            "selected_count": len(selected),
            "skipped_count": len(skipped),
            "planned_bytes_to_free": selected_bytes,
            "planned_file_count": sum(int(item.get("file_count") or 0) for item in selected),
        },
        "operations": selected,
        "skipped": skipped,
    }


def build_runtime_asset_prune_plan_from_cold_bundle_manifest(
    *,
    bundle_manifest: dict[str, Any],
    workspace_root: str | Path = ".",
    cold_copy_manifest: str,
    review_artifact: str = "",
    review_title: str = "",
    review_required_files: list[str] | None = None,
    review_plan_artifact: str = "",
    reuse_index_effect: str = "none_runtime_output_only",
) -> dict[str, Any]:
    """Build an exact prune plan from a verified supersession cold bundle.

    This intentionally does not rescan retention audit candidates. The deletion
    scope is exactly the archive entries already proven by the cold bundle
    manifest, so old audit filters cannot pull in unrelated runtime artifacts.
    """

    root = _resolve_root(workspace_root)
    generated_at = datetime.now(timezone.utc)
    source_contract_valid = _cold_bundle_source_contract_valid(bundle_manifest)
    source_bundle_manifest_path = str(cold_copy_manifest or "")
    source_bundle_manifest_sha256 = _manifest_file_sha256(root=root, manifest_path=source_bundle_manifest_path)
    review_plan_artifact_path = (
        _workspace_relative_file_path(root=root, raw_path=str(review_plan_artifact or ""))
        if str(review_plan_artifact or "").strip()
        else ""
    )
    operations: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for raw_entry in list(bundle_manifest.get("archives") or []):
        entry = dict(raw_entry or {})
        path_text = str(entry.get("path") or "")
        skip_reason = _cold_bundle_entry_plan_skip_reason(
            entry=entry, root=root, source_contract_valid=source_contract_valid
        )
        if skip_reason:
            skipped.append({"path": path_text, "reason": skip_reason})
            continue
        retention_class = str(entry.get("retention_class") or "").strip()
        operations.append(
            {
                "action": "remove_local_runtime_copy",
                "path": path_text,
                "size_bytes": int(entry.get("size_bytes") or 0),
                "file_count": int(entry.get("file_count") or 0),
                "directory_count": int(entry.get("directory_count") or 0),
                "latest_mtime": str(entry.get("latest_mtime") or ""),
                "retention_class": retention_class,
                "matched_markers": _retention_markers_from_path(path_text),
                "source_retention_class": retention_class,
                "source_scan_root": str(entry.get("scan_root") or ""),
                "source_bundle_archive_path": str(entry.get("archive_path") or ""),
                "source_bundle_archive_sha256": str(entry.get("archive_sha256") or ""),
                "source_manifest_digest_sha256": str(entry.get("source_manifest_digest_sha256") or ""),
            }
        )
    status = (
        "ready_for_review"
        if source_contract_valid and operations and not skipped
        else "blocked_invalid_cold_bundle_manifest"
    )
    plan = {
        "contract_version": CONTRACT_VERSION,
        "status": status,
        "generated_at": generated_at.isoformat(),
        "workspace_root": str(root),
        "source_audit_contract_version": "",
        "source_audit_generated_at": "",
        "source_bundle_contract_version": str(bundle_manifest.get("contract_version") or ""),
        "source_bundle_generated_at": str(bundle_manifest.get("generated_at") or ""),
        "source_bundle_status": str(bundle_manifest.get("status") or ""),
        "source_bundle_manifest_path": source_bundle_manifest_path,
        "source_bundle_manifest_sha256": source_bundle_manifest_sha256,
        "allowed_prune_roots": list(ALLOWED_PRUNE_ROOTS),
        "protected_names": sorted(PROTECTED_NAMES),
        "min_age_days": 0,
        "target_free_bytes": 0,
        "max_entries": 0,
        "allowed_retention_classes": sorted(
            {str(item.get("retention_class") or "") for item in operations if str(item.get("retention_class") or "")}
        ),
        "destructive_review_evidence": {
            "review_artifact": str(review_artifact or ""),
            "review_title": str(review_title or ""),
            "review_required_files": list(review_required_files or []),
            "accepted_retention_exception": "",
            "cold_copy_manifest": source_bundle_manifest_path,
            "reuse_index_effect": str(reuse_index_effect or ""),
        },
        "apply_requires": [
            "explicit_manifest",
            "--apply",
            "--reviewed",
            "valid_go_review_artifact",
            "verified_supersession_cold_bundle_manifest",
            "exact_bundle_scope_only",
            "no_active_runtime_processes",
        ],
        "summary": {
            "candidate_count": len(list(bundle_manifest.get("archives") or [])),
            "selected_count": len(operations),
            "skipped_count": len(skipped),
            "planned_bytes_to_free": sum(int(item.get("size_bytes") or 0) for item in operations),
            "planned_file_count": sum(int(item.get("file_count") or 0) for item in operations),
        },
        "operations": operations,
        "skipped": skipped,
    }
    scope_digest = _prune_plan_scope_digest(plan)
    plan["review_plan_scope_digest_sha256"] = scope_digest
    evidence = dict(plan.get("destructive_review_evidence") or {})
    evidence["review_plan_artifact"] = review_plan_artifact_path
    evidence["review_plan_scope_digest_sha256"] = scope_digest
    source_bundle_relative = (
        _workspace_relative_file_path(root=root, raw_path=source_bundle_manifest_path) or source_bundle_manifest_path
    )
    required_artifacts: list[dict[str, str]] = []
    if source_bundle_relative and source_bundle_manifest_sha256:
        required_artifacts.append(
            {
                "kind": "source_bundle_manifest",
                "path": source_bundle_relative,
                "sha256": source_bundle_manifest_sha256,
            }
        )
    if review_plan_artifact_path:
        required_artifacts.append(
            {
                "kind": "runtime_asset_retention_prune_plan_scope",
                "path": review_plan_artifact_path,
                "sha256": scope_digest,
            }
        )
    evidence["review_required_artifacts"] = required_artifacts
    evidence["review_required_tokens"] = _dedupe_strings(
        [
            BUNDLE_MANIFEST_CONTRACT_VERSION,
            f"source_bundle_manifest_sha256={source_bundle_manifest_sha256}" if source_bundle_manifest_sha256 else "",
            f"review_plan_scope_digest_sha256={scope_digest}",
        ]
    )
    plan["destructive_review_evidence"] = evidence
    return plan


def build_ttl_local_rebuildable_prune_plan(
    *,
    workspace_root: str | Path = ".",
    prune_root: str = TTL_LOCAL_REBUILDABLE_PRUNE_ROOT,
    min_age_days: int = 10,
    target_free_bytes: int = 0,
    max_entries: int = 0,
) -> dict[str, Any]:
    """Build a TTL-based prune plan for rebuildable local test artifacts.

    This policy targets only top-level per-run directories under
    ``runtime/test_env`` and waives the independent review-artifact gate for
    that root only. All other safety rails still hold at apply time: dry-run
    default, explicit ``--apply`` plus ``--reviewed``, active runtime process
    detection, protected-name exclusion, and content drift checks.
    """

    root = _resolve_root(workspace_root)
    generated_at = datetime.now(timezone.utc)
    normalized_prune_root = Path(str(prune_root or "").strip()).as_posix().strip("/")
    prune_root_allowed = normalized_prune_root == TTL_LOCAL_REBUILDABLE_PRUNE_ROOT
    protected_names = sorted(PROTECTED_NAMES | TTL_LOCAL_REBUILDABLE_PROTECTED_NAMES)
    candidates: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    if prune_root_allowed:
        base = root / TTL_LOCAL_REBUILDABLE_PRUNE_ROOT
        try:
            entries = sorted(base.iterdir(), key=lambda item: item.name) if base.is_dir() else []
        except OSError:
            entries = []
        for entry in entries:
            path_text = f"{TTL_LOCAL_REBUILDABLE_PRUNE_ROOT}/{entry.name}"
            if entry.name in PROTECTED_NAMES or entry.name in TTL_LOCAL_REBUILDABLE_PROTECTED_NAMES:
                skipped.append({"path": path_text, "reason": "protected_name"})
                continue
            if entry.is_symlink():
                skipped.append({"path": path_text, "reason": "target_symlink_not_allowed"})
                continue
            if not entry.is_dir():
                skipped.append({"path": path_text, "reason": "non_directory_entry_protected"})
                continue
            if entry.name.startswith("."):
                skipped.append({"path": path_text, "reason": "hidden_entry_protected"})
                continue
            if _is_current_or_latest_path(path_text):
                skipped.append({"path": path_text, "reason": "current_or_latest_alias"})
                continue
            safety_reason = _path_safety_reason(path_text=path_text, root=root)
            if safety_reason:
                skipped.append({"path": path_text, "reason": safety_reason})
                continue
            summary = _directory_size_summary(entry)
            latest_mtime = _parse_datetime(str(summary.get("latest_mtime") or ""))
            if latest_mtime is None:
                skipped.append({"path": path_text, "reason": "missing_or_invalid_latest_mtime"})
                continue
            if min_age_days > 0 and (generated_at - latest_mtime).total_seconds() < min_age_days * 24 * 60 * 60:
                skipped.append({"path": path_text, "reason": "younger_than_min_age_days"})
                continue
            matched_markers = _retention_markers_from_path(path_text)
            retention_class = _effective_retention_class(
                path_text=path_text,
                retention_class="local_rebuildable_test_artifact",
                matched_markers=matched_markers,
            )
            candidates.append(
                {
                    "action": "remove_local_runtime_copy",
                    "path": path_text,
                    "size_bytes": int(summary["size_bytes"]),
                    "file_count": int(summary["file_count"]),
                    "directory_count": int(summary["directory_count"]),
                    "latest_mtime": str(summary.get("latest_mtime") or ""),
                    "retention_class": retention_class,
                    "matched_markers": matched_markers,
                    "source_retention_class": "local_rebuildable_test_artifact",
                    "source_scan_root": TTL_LOCAL_REBUILDABLE_PRUNE_ROOT,
                }
            )
    candidates.sort(key=lambda payload: (int(payload["size_bytes"]), str(payload["path"])), reverse=True)
    selected: list[dict[str, Any]] = []
    selected_bytes = 0
    for item in candidates:
        if max_entries and len(selected) >= max_entries:
            skipped.append({"path": item["path"], "reason": "max_entries_reached"})
            continue
        if target_free_bytes and selected_bytes >= target_free_bytes:
            skipped.append({"path": item["path"], "reason": "target_free_bytes_reached"})
            continue
        selected.append(item)
        selected_bytes += int(item["size_bytes"])
    if not prune_root_allowed:
        status = "blocked_prune_root_not_allowed"
    elif selected:
        status = "ready_for_review"
    else:
        status = "no_prune_candidates"
    return {
        "contract_version": CONTRACT_VERSION,
        "policy": TTL_LOCAL_REBUILDABLE_POLICY,
        "status": status,
        "generated_at": generated_at.isoformat(),
        "workspace_root": str(root),
        "prune_root": normalized_prune_root,
        "allowed_prune_roots": [TTL_LOCAL_REBUILDABLE_PRUNE_ROOT],
        "protected_names": protected_names,
        "min_age_days": int(min_age_days),
        "target_free_bytes": int(target_free_bytes or 0),
        "max_entries": int(max_entries or 0),
        "allowed_retention_classes": [],
        "destructive_review_evidence": {
            "review_artifact": "",
            "review_title": "",
            "review_required_files": [],
            "accepted_retention_exception": "ttl_local_rebuildable_policy_waives_review_artifact_for_runtime_test_env_only",
            "cold_copy_manifest": "",
            "reuse_index_effect": "none_runtime_output_only",
        },
        "apply_requires": [
            "explicit_manifest",
            "--apply",
            "--reviewed",
            "prune_root_under_runtime_test_env_only",
            "protected_names_excluded",
            "no_active_runtime_processes",
        ],
        "summary": {
            "candidate_count": len(candidates),
            "selected_count": len(selected),
            "skipped_count": len(skipped),
            "planned_bytes_to_free": selected_bytes,
            "planned_file_count": sum(int(item.get("file_count") or 0) for item in selected),
        },
        "operations": selected,
        "skipped": skipped,
    }


def apply_runtime_asset_prune_plan(
    *,
    plan: dict[str, Any],
    workspace_root: str | Path = ".",
    apply: bool = False,
    reviewed: bool = False,
    skip_process_check: bool = False,
) -> dict[str, Any]:
    root = _resolve_root(workspace_root)
    started_at = datetime.now(timezone.utc)
    operations = [dict(item or {}) for item in list(plan.get("operations") or [])]
    plan_policy = str(plan.get("policy") or "").strip()
    blockers: list[str] = []
    if str(plan.get("contract_version") or "") != CONTRACT_VERSION:
        blockers.append("unsupported_plan_contract_version")
    if plan_policy and plan_policy != TTL_LOCAL_REBUILDABLE_POLICY:
        blockers.append("unsupported_plan_policy")
    if apply and str(plan.get("status") or "") != "ready_for_review":
        blockers.append("plan_not_ready_for_review")
    if apply and not reviewed:
        blockers.append("apply_requires_reviewed_flag")
    if apply:
        plan_root_mismatch = _plan_workspace_root_mismatch(plan=plan, root=root)
        if plan_root_mismatch:
            blockers.append(plan_root_mismatch)
        if plan_policy == TTL_LOCAL_REBUILDABLE_POLICY:
            blockers.extend(_ttl_local_rebuildable_policy_blockers(plan=plan, operations=operations))
        else:
            blockers.extend(_destructive_evidence_blockers(plan=plan, root=root))
    if apply and skip_process_check:
        blockers.append("process_check_bypass_not_allowed")
        active = []
    elif apply:
        active = detect_active_runtime_processes(
            root=root, operation_paths=[str(item.get("path") or "") for item in operations]
        )
        if active:
            blockers.append("active_runtime_processes_detected")
    else:
        active = []
    results: list[dict[str, Any]] = []
    if blockers:
        return {
            "contract_version": CONTRACT_VERSION,
            "status": "blocked",
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "apply": bool(apply),
            "reviewed": bool(reviewed),
            "workspace_root": str(root),
            "blockers": blockers,
            "active_processes": active,
            "summary": {"operation_count": len(operations), "removed_count": 0, "removed_bytes": 0},
            "results": results,
        }
    removed_count = 0
    removed_bytes = 0
    if apply:
        preflight_results = [
            _apply_one_operation(operation=operation, root=root, apply=False) for operation in operations
        ]
        preflight_failed_count = sum(1 for item in preflight_results if _operation_result_failed(item))
        if preflight_failed_count:
            return {
                "contract_version": CONTRACT_VERSION,
                "status": "blocked",
                "started_at": started_at.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "apply": bool(apply),
                "reviewed": bool(reviewed),
                "workspace_root": str(root),
                "blockers": ["operation_preflight_failed"],
                "active_processes": active,
                "summary": {
                    "operation_count": len(operations),
                    "removed_count": 0,
                    "failed_count": preflight_failed_count,
                    "removed_bytes": 0,
                },
                "results": preflight_results,
            }
    for operation in operations:
        result = _apply_one_operation(operation=operation, root=root, apply=apply)
        results.append(result)
        if result.get("status") == "removed":
            removed_count += 1
            removed_bytes += int(result.get("planned_size_bytes") or 0)
    failed_count = sum(1 for item in results if _operation_result_failed(item))
    if not apply:
        status = "dry_run_ready"
    elif failed_count:
        status = "partial"
    else:
        status = "applied"
    return {
        "contract_version": CONTRACT_VERSION,
        "status": status,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "apply": bool(apply),
        "reviewed": bool(reviewed),
        "workspace_root": str(root),
        "blockers": [],
        "active_processes": active,
        "summary": {
            "operation_count": len(operations),
            "removed_count": removed_count,
            "failed_count": failed_count,
            "removed_bytes": removed_bytes,
        },
        "results": results,
    }


def render_runtime_asset_prune_plan_markdown(plan: dict[str, Any]) -> str:
    summary = dict(plan.get("summary") or {})
    lines = [
        "# Runtime Asset Prune Plan",
        "",
        f"- Status: `{plan.get('status')}`",
        f"- Contract: `{plan.get('contract_version')}`",
        f"- Generated at: `{plan.get('generated_at')}`",
        f"- Selected directories: `{summary.get('selected_count', 0)}`",
        f"- Planned bytes to free: `{summary.get('planned_bytes_to_free', 0)}`",
        f"- Planned files: `{summary.get('planned_file_count', 0)}`",
        f"- Min age days: `{plan.get('min_age_days')}`",
        "",
        "## Apply Requirements",
        "",
    ]
    for requirement in list(plan.get("apply_requires") or []):
        lines.append(f"- `{requirement}`")
    evidence = dict(plan.get("destructive_review_evidence") or {})
    if any(
        str(evidence.get(key) or "").strip()
        for key in ("cold_copy_manifest", "review_plan_artifact", "review_plan_scope_digest_sha256")
    ):
        lines.extend(["", "## Review Scope Evidence", ""])
        for key in ("cold_copy_manifest", "review_plan_artifact", "review_plan_scope_digest_sha256"):
            value = str(evidence.get(key) or "").strip()
            if value:
                lines.append(f"- {key}: `{value}`")
        for artifact in list(evidence.get("review_required_artifacts") or []):
            if not isinstance(artifact, dict):
                continue
            artifact_path = str(artifact.get("path") or "").strip()
            artifact_sha = str(artifact.get("sha256") or "").strip()
            artifact_kind = str(artifact.get("kind") or "").strip()
            if artifact_path:
                lines.append(f"- required artifact: `{artifact_kind}` `{artifact_path}` sha256 `{artifact_sha}`")
    lines.extend(
        [
            "",
            "## Selected Operations",
            "",
            "| path | class | files | bytes | latest mtime |",
            "| --- | --- | ---: | ---: | --- |",
        ]
    )
    for item in list(plan.get("operations") or [])[:200]:
        payload = dict(item or {})
        lines.append(
            "| {path} | `{klass}` | {files} | {bytes} | `{mtime}` |".format(
                path=str(payload.get("path") or ""),
                klass=str(payload.get("retention_class") or ""),
                files=int(payload.get("file_count") or 0),
                bytes=int(payload.get("size_bytes") or 0),
                mtime=str(payload.get("latest_mtime") or ""),
            )
        )
    if len(list(plan.get("operations") or [])) > 200:
        lines.append(
            f"| ... | ... | ... | {len(list(plan.get('operations') or [])) - 200} more operations omitted | ... |"
        )
    return "\n".join(lines).rstrip() + "\n"


def render_runtime_asset_prune_apply_markdown(report: dict[str, Any]) -> str:
    summary = dict(report.get("summary") or {})
    lines = [
        "# Runtime Asset Prune Apply Report",
        "",
        f"- Status: `{report.get('status')}`",
        f"- Apply: `{bool(report.get('apply'))}`",
        f"- Reviewed: `{bool(report.get('reviewed'))}`",
        f"- Started at: `{report.get('started_at')}`",
        f"- Completed at: `{report.get('completed_at')}`",
        f"- Removed directories: `{summary.get('removed_count', 0)}`",
        f"- Removed bytes: `{summary.get('removed_bytes', 0)}`",
        f"- Failed count: `{summary.get('failed_count', 0)}`",
        "",
    ]
    blockers = list(report.get("blockers") or [])
    if blockers:
        lines.extend(["## Blockers", ""])
        for blocker in blockers:
            lines.append(f"- `{blocker}`")
    lines.extend(["## Results", "", "| path | status | planned bytes | reason |", "| --- | --- | ---: | --- |"])
    for item in list(report.get("results") or [])[:200]:
        payload = dict(item or {})
        lines.append(
            "| {path} | `{status}` | {bytes} | `{reason}` |".format(
                path=str(payload.get("path") or ""),
                status=str(payload.get("status") or ""),
                bytes=int(payload.get("planned_size_bytes") or 0),
                reason=str(payload.get("reason") or ""),
            )
        )
    return "\n".join(lines).rstrip() + "\n"


def detect_active_runtime_processes(*, root: Path, operation_paths: list[str] | None = None) -> list[dict[str, Any]]:
    try:
        completed = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            cwd=root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=False,
        )
    except OSError as exc:
        return [{"pid": 0, "command": f"process_check_failed: {exc}"}]
    except subprocess.TimeoutExpired:
        return [{"pid": 0, "command": "process_check_failed: timeout"}]
    if completed.returncode != 0:
        return [{"pid": 0, "command": f"process_check_failed: returncode={completed.returncode}"}]
    active: list[dict[str, Any]] = []
    current_pid = os.getpid()
    process_commands: dict[int, str] = {}
    for line in completed.stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        pid_text, _, command = stripped.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if pid == current_pid:
            continue
        process_commands[pid] = command
        normalized = command.lower()
        if any(token in normalized for token in ACTIVE_PROCESS_TOKENS) and _runtime_process_applies_to_root(
            pid=pid, command=command, root=root
        ):
            active.append({"pid": pid, "command": command[:500]})
    active.extend(
        _active_runtime_pid_file_processes(
            root=root, process_commands=process_commands, operation_paths=operation_paths
        )
    )
    deduped: dict[int, dict[str, Any]] = {}
    for item in active:
        pid = int(item.get("pid") or 0)
        existing = deduped.get(pid)
        if existing and "runtime process reference:" in str(item.get("command") or ""):
            deduped[pid] = item
            continue
        deduped.setdefault(pid, item)
    return list(deduped.values())


def _runtime_process_applies_to_root(*, pid: int, command: str, root: Path) -> bool:
    normalized = str(command or "").lower()
    if any(root_name in normalized for root_name in ALLOWED_PRUNE_ROOTS):
        return True
    cwd = _process_cwd(pid)
    if cwd is None:
        return True
    try:
        cwd.relative_to(_realpath(root))
        return True
    except ValueError:
        return False


def _process_cwd(pid: int) -> Path | None:
    try:
        completed = subprocess.run(
            ["lsof", "-a", "-p", str(int(pid)), "-d", "cwd", "-Fn"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return None
    if completed.returncode != 0:
        return None
    for line in completed.stdout.splitlines():
        if line.startswith("n"):
            return _realpath(line[1:])
    return None


def _apply_one_operation(*, operation: dict[str, Any], root: Path, apply: bool) -> dict[str, Any]:
    path_text = str(operation.get("path") or "")
    planned_size = int(operation.get("size_bytes") or 0)
    reason = _path_safety_reason(path_text=path_text, root=root)
    if reason:
        return {"path": path_text, "status": "blocked", "reason": reason, "planned_size_bytes": planned_size}
    operation_reason = _operation_safety_reason(operation=operation, root=root)
    if operation_reason:
        return {"path": path_text, "status": "blocked", "reason": operation_reason, "planned_size_bytes": planned_size}
    target = root / path_text
    if not target.exists():
        return {"path": path_text, "status": "blocked", "reason": "target_missing", "planned_size_bytes": planned_size}
    if not target.is_dir() or target.is_symlink():
        return {
            "path": path_text,
            "status": "blocked",
            "reason": "target_not_plain_directory",
            "planned_size_bytes": planned_size,
        }
    if not apply:
        return {"path": path_text, "status": "would_remove", "reason": "", "planned_size_bytes": planned_size}
    try:
        shutil.rmtree(target)
    except OSError as exc:
        return {"path": path_text, "status": "error", "reason": str(exc), "planned_size_bytes": planned_size}
    return {"path": path_text, "status": "removed", "reason": "", "planned_size_bytes": planned_size}


def _skip_reason(*, item: dict[str, Any], path_text: str, root: Path, now: datetime, min_age_days: int) -> str:
    path_reason = _path_safety_reason(path_text=path_text, root=root)
    if path_reason:
        return path_reason
    if (
        _is_current_or_latest_path(path_text)
        or str(item.get("retention_class") or "") == "review_current_alias_or_latest_artifact"
    ):
        return "current_or_latest_alias"
    latest_mtime = _parse_datetime(str(item.get("latest_mtime") or ""))
    if latest_mtime and min_age_days > 0:
        age_seconds = (now - latest_mtime).total_seconds()
        if age_seconds < min_age_days * 24 * 60 * 60:
            return "younger_than_min_age_days"
    return ""


def _path_safety_reason(*, path_text: str, root: Path) -> str:
    relative_path = Path(path_text)
    if not path_text:
        return "missing_path"
    if relative_path.is_absolute():
        return "absolute_path_not_allowed"
    parts = relative_path.parts
    if any(part in {"", ".", ".."} for part in parts):
        return "unsafe_path_component"
    if any(part in PROTECTED_NAMES for part in parts):
        return "protected_path_name"
    symlink_reason = _raw_path_symlink_reason(path_text=path_text, root=root)
    if symlink_reason:
        return symlink_reason
    resolved = _realpath(root / path_text)
    root_real = _realpath(root)
    try:
        resolved_relative = resolved.relative_to(root_real)
    except ValueError:
        return "path_escapes_workspace"
    if any(part in PROTECTED_NAMES for part in resolved_relative.parts):
        return "protected_resolved_path_name"
    for allowed_root in ALLOWED_PRUNE_ROOTS:
        allowed = _realpath(root / allowed_root)
        try:
            resolved.relative_to(allowed)
        except ValueError:
            continue
        if resolved == allowed:
            return "allowed_root_delete_not_allowed"
        return ""
    return "outside_allowed_prune_roots"


def _operation_safety_reason(*, operation: dict[str, Any], root: Path) -> str:
    if str(operation.get("action") or "") != "remove_local_runtime_copy":
        return "invalid_action"
    path_text = str(operation.get("path") or "")
    source_scan_root = str(operation.get("source_scan_root") or "")
    if source_scan_root not in ALLOWED_PRUNE_ROOTS:
        return "invalid_source_scan_root"
    if not path_text.startswith(f"{source_scan_root}/"):
        return "path_source_scan_root_mismatch"
    if (
        _is_current_or_latest_path(path_text)
        or str(operation.get("retention_class") or "") == "review_current_alias_or_latest_artifact"
    ):
        return "current_or_latest_alias"
    declared_retention_class = str(operation.get("retention_class") or "").strip()
    if not declared_retention_class:
        return "missing_retention_class"
    effective_retention_class = _effective_retention_class(
        path_text=path_text,
        retention_class=declared_retention_class,
        matched_markers=list(operation.get("matched_markers") or []),
    )
    if declared_retention_class != effective_retention_class:
        return "retention_class_path_mismatch"
    planned_mtime = _parse_datetime(str(operation.get("latest_mtime") or ""))
    if not planned_mtime:
        return "missing_or_invalid_latest_mtime"
    target = root / path_text
    if not target.exists():
        return "target_missing"
    if not target.is_dir() or target.is_symlink():
        return "target_not_plain_directory"
    current = _directory_size_summary(target)
    if _operation_int(operation, "size_bytes") != int(current["size_bytes"]):
        return "size_drift_detected"
    if _operation_int(operation, "file_count") != int(current["file_count"]):
        return "file_count_drift_detected"
    if _operation_int(operation, "directory_count") != int(current["directory_count"]):
        return "directory_count_drift_detected"
    current_mtime = _parse_datetime(str(current.get("latest_mtime") or ""))
    if current_mtime and current_mtime != planned_mtime:
        return "latest_mtime_drift_detected"
    return ""


def _ttl_local_rebuildable_policy_blockers(*, plan: dict[str, Any], operations: list[dict[str, Any]]) -> list[str]:
    """Policy gate for ttl_local_rebuildable plans.

    Replaces the review-artifact evidence gate for rebuildable per-run test
    artifacts: the prune root must be exactly runtime/test_env, a minimum TTL
    must be set, and every operation must be a top-level test_env directory
    whose name is outside both protected-name sets.
    """

    blockers: list[str] = []
    if str(plan.get("prune_root") or "").strip() != TTL_LOCAL_REBUILDABLE_PRUNE_ROOT:
        blockers.append("ttl_policy_prune_root_not_allowed")
    try:
        min_age_days = int(plan.get("min_age_days"))
    except (TypeError, ValueError):
        min_age_days = 0
    if min_age_days < 1:
        blockers.append("ttl_policy_min_age_days_too_low")
    protected = PROTECTED_NAMES | TTL_LOCAL_REBUILDABLE_PROTECTED_NAMES
    root_parts = Path(TTL_LOCAL_REBUILDABLE_PRUNE_ROOT).parts
    for operation in operations:
        path_text = str(dict(operation or {}).get("path") or "")
        parts = Path(path_text).parts
        if len(parts) != len(root_parts) + 1 or parts[: len(root_parts)] != root_parts:
            blockers.append(f"ttl_policy_path_not_top_level_test_env_dir:{path_text}")
            continue
        if parts[-1] in protected:
            blockers.append(f"ttl_policy_protected_name:{path_text}")
    return _dedupe_strings(blockers)


def _destructive_evidence_blockers(*, plan: dict[str, Any], root: Path) -> list[str]:
    evidence = dict(plan.get("destructive_review_evidence") or {})
    blockers: list[str] = []
    review_artifact = str(evidence.get("review_artifact") or "").strip()
    has_cold_copy = bool(str(evidence.get("cold_copy_manifest") or "").strip())
    cold_copy_contract = (
        _cold_copy_manifest_contract_version(root=root, manifest_path=str(evidence.get("cold_copy_manifest") or ""))
        if has_cold_copy
        else ""
    )
    mandatory_review_tokens = (
        _bundle_mandatory_review_tokens(plan=plan, evidence=evidence)
        if cold_copy_contract == BUNDLE_MANIFEST_CONTRACT_VERSION
        else []
    )
    if not str(evidence.get("review_title") or "").strip():
        blockers.append("missing_review_title")
    if not list(evidence.get("review_required_files") or []):
        blockers.append("missing_review_required_files")
    if not review_artifact:
        blockers.append("missing_review_artifact")
    else:
        review_path = Path(review_artifact).expanduser()
        if not review_path.is_absolute():
            review_path = root / review_path
        try:
            resolved_review_path = _realpath(review_path)
            resolved_review_path.relative_to(_realpath(root))
        except ValueError:
            blockers.append("review_artifact_outside_workspace")
        else:
            if not resolved_review_path.exists():
                blockers.append("review_artifact_missing")
            else:
                blockers.extend(
                    validate_independent_review_artifact(
                        artifact_path=resolved_review_path,
                        workspace_root=root,
                        expected_title=str(evidence.get("review_title") or ""),
                        required_files=list(evidence.get("review_required_files") or []),
                        required_artifacts=list(evidence.get("review_required_artifacts") or []),
                        required_tokens=_dedupe_strings(
                            list(evidence.get("review_required_tokens") or []) + mandatory_review_tokens
                        ),
                    )
                )
    if not str(evidence.get("reuse_index_effect") or "").strip():
        blockers.append("missing_reuse_index_effect")
    has_exception = bool(str(evidence.get("accepted_retention_exception") or "").strip())
    if not has_cold_copy and not has_exception:
        blockers.append("missing_cold_copy_or_accepted_retention_exception")
    if has_cold_copy:
        if cold_copy_contract == BUNDLE_MANIFEST_CONTRACT_VERSION:
            blockers.extend(
                _bundle_plan_binding_blockers(
                    plan=plan, root=root, manifest_path=str(evidence.get("cold_copy_manifest") or "")
                )
            )
        blockers.extend(
            _cold_copy_manifest_blockers(
                plan=plan, root=root, manifest_path=str(evidence.get("cold_copy_manifest") or "")
            )
        )
        privileged_classes = _uncopied_privileged_retention_classes(plan)
        if privileged_classes and cold_copy_contract != BUNDLE_MANIFEST_CONTRACT_VERSION:
            blockers.append("missing_supersession_cold_bundle_for_retention_classes:" + ",".join(privileged_classes))
    if not has_cold_copy:
        privileged_classes = _uncopied_privileged_retention_classes(plan)
        if privileged_classes:
            blockers.append("missing_cold_copy_for_retention_classes:" + ",".join(privileged_classes))
    return blockers


def normalize_independent_review_title(value: object) -> str:
    return " ".join(str(value or "").split()) or "Independent review"


def normalize_independent_review_files(files: list[str] | tuple[str, ...]) -> list[str]:
    normalized: set[str] = set()
    for raw_path in files:
        candidate = str(raw_path or "").strip().replace("\\", "/")
        if not candidate:
            continue
        path = posixpath.normpath(candidate)
        if path in {"", "."} or path.startswith("/") or path == ".." or path.startswith("../"):
            raise ValueError(f"review scope path must be workspace-relative: {candidate!r}")
        normalized.add(path)
    return sorted(normalized)


def independent_review_scope_digest(scope: dict[str, Any]) -> str:
    canonical: dict[str, Any] = {
        field: list(scope.get(field) or []) if field == "files" else str(scope.get(field) or "")
        for field in _INDEPENDENT_REVIEW_SCOPE_DIGEST_FIELDS
    }
    canonical["title"] = normalize_independent_review_title(canonical["title"])
    canonical["base_ref"] = str(canonical["base_ref"]).strip()
    canonical["files"] = normalize_independent_review_files(list(canonical["files"]))
    raw = json.dumps(canonical, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _review_git_project(root: Path) -> _ReviewGitProject | None:
    """Resolve project-relative paths without relying on Git's cwd pathspec rules."""

    raw_toplevel = _review_git_text(root, "rev-parse", "--show-toplevel", allow_failure=True)
    if not raw_toplevel:
        return None
    git_toplevel = _realpath(raw_toplevel)
    try:
        relative_project = root.relative_to(git_toplevel)
    except ValueError:
        return None
    project_prefix = "" if relative_project == Path(".") else relative_project.as_posix()
    return _ReviewGitProject(git_toplevel=git_toplevel, project_prefix=project_prefix)


def build_independent_review_scope_evidence(
    *,
    workspace_root: str | Path,
    title: str,
    base_ref: str,
    files: list[str],
    extra_context: str,
) -> dict[str, Any]:
    """Build the immutable Git scope reviewed by a signoff-capable session.

    A missing base, unresolved Git object, or implicit whole-worktree scope
    deliberately degrades the request to reference-only evidence. The scope
    digest pins the resolved base/head Git objects and explicit file set, while
    signoff validation separately requires the current scoped tree to retain the
    reviewed bytes.
    """

    root = _realpath(workspace_root)
    normalized_files = normalize_independent_review_files(files)
    normalized_title = normalize_independent_review_title(title)
    normalized_base = str(base_ref or "").strip()
    git_project = _review_git_project(root)
    resolved_head = _review_git_text(root, "rev-parse", "--verify", "HEAD^{commit}", allow_failure=True)
    resolved_base = (
        _review_git_text(root, "rev-parse", "--verify", f"{normalized_base}^{{commit}}", allow_failure=True)
        if normalized_base
        else ""
    )
    scoped_file_types = (
        {
            path: {
                "base": _review_git_text(
                    root,
                    "cat-file",
                    "-t",
                    git_project.object_spec(resolved_base, path),
                    allow_failure=True,
                ),
                "head": _review_git_text(
                    root,
                    "cat-file",
                    "-t",
                    git_project.object_spec(resolved_head, path),
                    allow_failure=True,
                ),
            }
            for path in normalized_files
        }
        if git_project and resolved_base and resolved_head
        else {}
    )
    pinned = bool(
        normalized_base
        and git_project
        and resolved_base
        and resolved_head
        and normalized_files
        and all("blob" in scoped_file_types.get(path, {}).values() for path in normalized_files)
    )
    scope_mode = INDEPENDENT_REVIEW_PINNED_SCOPE_MODE if pinned else INDEPENDENT_REVIEW_REFERENCE_SCOPE_MODE
    diff_raw = b""
    tree_raw = b""
    if git_project and resolved_base and resolved_head:
        pathspecs = [git_project.pathspec(path) for path in normalized_files]
        diff_args = ["diff", "--binary", "--no-ext-diff", resolved_base, resolved_head, "--", *pathspecs]
        diff_raw = _review_git_bytes(root, *diff_args, allow_failure=True) or b""
    if git_project and resolved_head:
        pathspecs = [git_project.pathspec(path) for path in normalized_files]
        tree_args = ["ls-tree", "-r", resolved_head, "--", *pathspecs]
        tree_raw = _review_git_bytes(root, *tree_args, allow_failure=True) or b""
    scope: dict[str, Any] = {
        "title": normalized_title,
        "base_ref": normalized_base,
        "scope_mode": scope_mode,
        "resolved_base_commit": resolved_base,
        "resolved_head_commit": resolved_head,
        "files": normalized_files,
        "git_diff_sha256": hashlib.sha256(diff_raw).hexdigest(),
        "git_tree_sha256": hashlib.sha256(tree_raw).hexdigest(),
        "extra_context_sha256": hashlib.sha256(
            (str(extra_context or "").strip() or "None.").encode("utf-8")
        ).hexdigest(),
    }
    scope["scope_digest_sha256"] = independent_review_scope_digest(scope)
    return scope


def _review_git_bytes(root: Path, *args: str, allow_failure: bool = False) -> bytes | None:
    try:
        completed = _REVIEW_GIT_RUN(
            ["git", *args],
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError:
        if allow_failure:
            return None
        raise
    if completed.returncode != 0:
        if allow_failure:
            return None
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"git {' '.join(args)} failed: {detail}")
    stdout = completed.stdout
    return stdout.encode("utf-8") if isinstance(stdout, str) else bytes(stdout or b"")


def _review_git_text(root: Path, *args: str, allow_failure: bool = False) -> str:
    raw = _review_git_bytes(root, *args, allow_failure=allow_failure)
    return raw.decode("utf-8", errors="replace").strip() if raw is not None else ""


def validate_independent_review_artifact(
    *,
    artifact_path: str | Path,
    workspace_root: str | Path,
    expected_title: str = "",
    required_files: list[str] | None = None,
    required_artifacts: list[dict[str, Any]] | None = None,
    required_tokens: list[str] | None = None,
    expected_scope_digest: str = "",
) -> list[str]:
    blockers: list[str] = []
    root = _realpath(workspace_root)
    path = Path(artifact_path).expanduser()
    if not path.is_absolute():
        path = root / path
    try:
        path = _realpath(path)
        path.relative_to(root)
    except ValueError:
        return ["review_artifact_outside_workspace"]
    if path.name.endswith(".prompt.md"):
        blockers.append("review_artifact_is_prompt")
        return blockers
    try:
        review_text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ["review_artifact_missing"]
    if "## Review Metadata" not in review_text or "## Reviewer Output" not in review_text:
        blockers.append("review_artifact_missing_runner_metadata")
    metadata = parse_independent_review_artifact_metadata(review_text)
    if not metadata:
        blockers.append("review_artifact_missing_runner_metadata")
    for field in REQUIRED_REVIEW_ARTIFACT_METADATA_FIELDS:
        value = metadata.get(field, "").strip()
        if not value:
            blockers.append(f"review_artifact_missing_metadata:{field}")
    for field in REQUIRED_REVIEW_ARTIFACT_METADATA_FIELDS:
        if _is_default_review_metadata_value(metadata.get(field, "")):
            blockers.append(f"review_artifact_missing_metadata:{field}")
    expected_title = normalize_independent_review_title(expected_title) if str(expected_title or "").strip() else ""
    try:
        normalized_required_files = normalize_independent_review_files(list(required_files or []))
    except ValueError:
        normalized_required_files = []
        blockers.append("review_artifact_invalid_required_file_scope")
    blockers.extend(
        validate_independent_review_effective_config_evidence(
            metadata=metadata,
            workspace_root=root,
            review_text=review_text,
            expected_title=expected_title,
            required_files=normalized_required_files,
            expected_scope_digest=expected_scope_digest,
        )
    )
    if expected_title and normalize_independent_review_title(metadata.get("title", "")) != expected_title:
        blockers.append("review_artifact_title_mismatch")
    for required_file in normalized_required_files:
        if f"`{required_file}`" not in review_text and required_file not in review_text:
            blockers.append(f"review_artifact_missing_scope:{required_file}")
    for artifact in list(required_artifacts or []):
        if not isinstance(artifact, dict):
            blockers.append("review_artifact_invalid_required_artifact")
            continue
        artifact_path = str(artifact.get("path") or "").strip()
        artifact_sha = str(artifact.get("sha256") or "").strip()
        if artifact_path and artifact_path not in review_text:
            blockers.append(f"review_artifact_missing_artifact_scope:{artifact_path}")
        if artifact_sha and artifact_sha not in review_text:
            blockers.append(f"review_artifact_missing_artifact_sha:{artifact_path or artifact_sha[:12]}")
    for token in _dedupe_strings(list(required_tokens or [])):
        token_text = str(token or "").strip()
        if token_text and token_text not in review_text:
            blockers.append(f"review_artifact_missing_token:{token_text[:80]}")
    if "docs/INDEPENDENT_REVIEW_GATE.md" not in review_text:
        blockers.append("review_artifact_missing_independent_review_contract")
    verdict = _final_review_verdict(review_text)
    if verdict != "GO":
        blockers.append("review_artifact_not_go")
    return _dedupe_strings(blockers)


def parse_independent_review_artifact_metadata(review_text: str) -> dict[str, str]:
    metadata: dict[str, str] = {}
    in_metadata = False
    for raw_line in review_text.splitlines():
        line = raw_line.strip()
        if line == "## Review Metadata":
            in_metadata = True
            continue
        if in_metadata and line.startswith("## "):
            break
        if not in_metadata or not line.startswith("- ") or ":" not in line:
            continue
        key, value = line[2:].split(":", 1)
        metadata[key.strip()] = value.strip().strip("`")
    return metadata


def _is_default_review_metadata_value(value: str) -> bool:
    return str(value or "").strip().lower() in {"", "default", "auto", "inherit"}


def validate_independent_review_effective_config_evidence(
    *,
    metadata: dict[str, str],
    workspace_root: str | Path,
    review_text: str = "",
    expected_title: str = "",
    required_files: list[str] | None = None,
    expected_scope_digest: str = "",
) -> list[str]:
    blockers: list[str] = []
    root = _realpath(workspace_root)
    try:
        reviewer_exit_code = int(metadata.get("reviewer_exit_code", ""))
    except (TypeError, ValueError):
        reviewer_exit_code = -1
    if reviewer_exit_code != 0:
        blockers.append("review_artifact_reviewer_exit_not_zero")

    evidence_path_text = str(metadata.get("reviewer_effective_config_path") or "").strip()
    evidence_sha = str(metadata.get("reviewer_effective_config_sha256") or "").strip()
    if not evidence_path_text or not _is_sha256(evidence_sha):
        blockers.append("review_artifact_effective_config_evidence_missing")
        return blockers
    evidence_path = Path(evidence_path_text).expanduser()
    if not evidence_path.is_absolute():
        evidence_path = root / evidence_path
    try:
        resolved_evidence_path = _realpath(evidence_path)
        resolved_evidence_path.relative_to(_realpath(root))
    except ValueError:
        blockers.append("review_artifact_effective_config_outside_workspace")
        return blockers
    try:
        evidence_raw = resolved_evidence_path.read_bytes()
    except OSError:
        blockers.append("review_artifact_effective_config_missing")
        return blockers
    if hashlib.sha256(evidence_raw).hexdigest() != evidence_sha:
        blockers.append("review_artifact_effective_config_sha256_mismatch")
        return blockers
    try:
        payload = json.loads(evidence_raw)
    except (TypeError, ValueError):
        blockers.append("review_artifact_effective_config_invalid_json")
        return blockers
    if not isinstance(payload, dict):
        blockers.append("review_artifact_effective_config_invalid_json")
        return blockers
    if str(payload.get("contract_version") or "") != INDEPENDENT_REVIEW_EFFECTIVE_CONFIG_CONTRACT_VERSION:
        blockers.append("review_artifact_effective_config_contract_mismatch")

    raw_config = payload.get("config")
    raw_session = payload.get("session")
    raw_process = payload.get("process")
    raw_effective = payload.get("effective")
    raw_causal_binding = payload.get("causal_binding")
    raw_scope = payload.get("scope")
    raw_artifacts = payload.get("artifacts")
    config: dict[str, Any] = dict(raw_config) if isinstance(raw_config, dict) else {}
    session: dict[str, Any] = dict(raw_session) if isinstance(raw_session, dict) else {}
    process: dict[str, Any] = dict(raw_process) if isinstance(raw_process, dict) else {}
    effective: dict[str, Any] = dict(raw_effective) if isinstance(raw_effective, dict) else {}
    causal_binding: dict[str, Any] = dict(raw_causal_binding) if isinstance(raw_causal_binding, dict) else {}
    scope: dict[str, Any] = dict(raw_scope) if isinstance(raw_scope, dict) else {}
    artifacts: dict[str, Any] = dict(raw_artifacts) if isinstance(raw_artifacts, dict) else {}

    def artifact_field(kind: str, field: str) -> str:
        value = artifacts.get(kind)
        artifact = dict(value) if isinstance(value, dict) else {}
        return str(artifact.get(field) or "")

    expected_metadata = {
        "reviewer_config_path": str(config.get("path") or ""),
        "reviewer_config_sha256": str(config.get("sha256") or ""),
        "reviewer_exit_code": str(process.get("reviewer_exit_code") if "reviewer_exit_code" in process else ""),
        "reviewer_codex_cli_version": str(session.get("codex_cli_version") or ""),
        "reviewer_thread_id": str(session.get("thread_id") or ""),
        "reviewer_rollout_path": str(session.get("rollout_path") or ""),
        "reviewer_rollout_sha256": str(session.get("rollout_sha256") or ""),
        "reviewer_model": str(effective.get("model") or ""),
        "reviewer_reasoning_effort": str(effective.get("reasoning_effort") or ""),
        "reviewer_service_tier": str(effective.get("service_tier") or ""),
        "review_scope_mode": str(scope.get("scope_mode") or ""),
        "review_base_ref": str(scope.get("base_ref") or ""),
        "review_resolved_base_commit": str(scope.get("resolved_base_commit") or ""),
        "review_resolved_head_commit": str(scope.get("resolved_head_commit") or ""),
        "review_git_diff_sha256": str(scope.get("git_diff_sha256") or ""),
        "review_git_tree_sha256": str(scope.get("git_tree_sha256") or ""),
        "review_extra_context_sha256": str(scope.get("extra_context_sha256") or ""),
        "review_scope_digest_sha256": str(scope.get("scope_digest_sha256") or ""),
        "prompt_path": artifact_field("prompt", "path"),
        "prompt_sha256": artifact_field("prompt", "sha256"),
        "events_path": artifact_field("events", "path"),
        "events_sha256": artifact_field("events", "sha256"),
        "raw_output_path": artifact_field("raw_output", "path"),
        "raw_output_sha256": artifact_field("raw_output", "sha256"),
    }
    for field, expected in expected_metadata.items():
        if not expected or str(metadata.get(field) or "").strip() != expected:
            blockers.append(f"review_artifact_effective_config_mismatch:{field}")
    if not _is_sha256(str(config.get("sha256") or "")):
        blockers.append("review_artifact_effective_config_invalid_config_sha256")
    try:
        evidence_exit_code = int(str(process.get("reviewer_exit_code")))
    except (TypeError, ValueError):
        evidence_exit_code = -1
    if evidence_exit_code != 0:
        blockers.append("review_artifact_effective_config_exit_not_zero")
    session_id = str(session.get("session_id") or "").strip()
    thread_id = str(session.get("thread_id") or "").strip()
    cli_version = str(session.get("codex_cli_version") or "").strip()
    if not session_id or not thread_id or session_id != thread_id or _is_default_review_metadata_value(cli_version):
        blockers.append("review_artifact_effective_config_incomplete_session")

    configured_model = str(config.get("model") or "").strip()
    configured_reasoning = str(config.get("reasoning_effort") or "").strip()
    configured_tier = str(config.get("service_tier") or "").strip()
    if not configured_model or configured_model != str(effective.get("model") or "").strip():
        blockers.append("review_artifact_effective_config_model_differs_from_config")
    if not configured_reasoning or configured_reasoning != str(effective.get("reasoning_effort") or "").strip():
        blockers.append("review_artifact_effective_config_reasoning_differs_from_config")
    if not configured_tier or not _review_service_tier_matches(
        configured_tier,
        str(effective.get("service_tier") or ""),
    ):
        blockers.append("review_artifact_effective_config_tier_differs_from_config")
    claimed_reroutes = payload.get("model_reroutes")
    if not isinstance(claimed_reroutes, list):
        blockers.append("review_artifact_effective_config_invalid_model_reroutes")
    if isinstance(claimed_reroutes, list) and claimed_reroutes:
        blockers.append("review_artifact_effective_config_model_reroute_not_allowed")
    if not isinstance(raw_causal_binding, dict):
        blockers.append("review_artifact_effective_config_causal_binding_missing")
    if set(causal_binding) != _INDEPENDENT_REVIEW_CAUSAL_FIELDS:
        blockers.append("review_artifact_effective_config_causal_binding_invalid_fields")
    if not _is_sha256(str(causal_binding.get("prompt_sha256") or "")) or not _is_sha256(
        str(causal_binding.get("normalized_final_output_sha256") or "")
    ):
        blockers.append("review_artifact_effective_config_causal_binding_invalid_hash")
    if not str(causal_binding.get("turn_id") or "").strip():
        blockers.append("review_artifact_effective_config_causal_binding_missing_turn")
    if any(not isinstance(causal_binding.get(field), bool) for field in _INDEPENDENT_REVIEW_CAUSAL_BOOLEAN_FIELDS):
        blockers.append("review_artifact_effective_config_causal_binding_invalid_boolean")

    prompt_raw, prompt_blockers = _read_independent_review_evidence_file(
        root=root,
        path_text=artifact_field("prompt", "path"),
        expected_sha=artifact_field("prompt", "sha256"),
        blocker_prefix="review_artifact_prompt",
    )
    events_raw, events_blockers = _read_independent_review_evidence_file(
        root=root,
        path_text=artifact_field("events", "path"),
        expected_sha=artifact_field("events", "sha256"),
        blocker_prefix="review_artifact_events",
    )
    raw_output, output_blockers = _read_independent_review_evidence_file(
        root=root,
        path_text=artifact_field("raw_output", "path"),
        expected_sha=artifact_field("raw_output", "sha256"),
        blocker_prefix="review_artifact_raw_output",
    )
    rollout_raw, rollout_blockers = _read_independent_review_evidence_file(
        root=root,
        path_text=str(session.get("rollout_path") or ""),
        expected_sha=str(session.get("rollout_sha256") or ""),
        blocker_prefix="review_artifact_rollout",
    )
    blockers.extend(prompt_blockers + events_blockers + output_blockers + rollout_blockers)

    if prompt_raw is not None:
        blockers.extend(_independent_review_prompt_blockers(prompt_raw=prompt_raw, scope=scope))
    if events_raw is not None:
        blockers.extend(
            _independent_review_events_blockers(
                events_raw=events_raw,
                expected_thread_id=thread_id,
            )
        )
    if raw_output is not None and review_text:
        marker = "## Reviewer Output\n\n"
        artifact_body = review_text.partition(marker)[2].encode("utf-8") if marker in review_text else b""
        if artifact_body != raw_output:
            blockers.append("review_artifact_raw_output_body_mismatch")
    if rollout_raw is not None:
        recomputed, recompute_blockers = _parse_independent_review_rollout(
            rollout_raw=rollout_raw,
            configured_model=configured_model,
        )
        blockers.extend(recompute_blockers)
        recomputed_expected = {
            "session_id": session_id,
            "thread_id": thread_id,
            "codex_cli_version": cli_version,
            "model": str(effective.get("model") or "").strip(),
            "reasoning_effort": str(effective.get("reasoning_effort") or "").strip(),
            "service_tier": _canonical_review_service_tier(effective.get("service_tier")),
            "source": effective.get("source"),
            "model_reroutes": claimed_reroutes,
        }
        for field, claimed in recomputed_expected.items():
            if recomputed.get(field) != claimed:
                blockers.append(f"review_artifact_rollout_recomputed_mismatch:{field}")
        if thread_id and thread_id not in str(session.get("rollout_path") or ""):
            blockers.append("review_artifact_rollout_thread_mismatch")
    if prompt_raw is not None and raw_output is not None and rollout_raw is not None:
        recomputed_causal, causal_blockers = _parse_independent_review_causal_binding(
            rollout_raw=rollout_raw,
            prompt_raw=prompt_raw,
            raw_output=raw_output,
            expected_thread_id=thread_id,
        )
        blockers.extend(causal_blockers)
        for field in sorted(_INDEPENDENT_REVIEW_CAUSAL_FIELDS):
            if causal_binding.get(field) != recomputed_causal.get(field):
                blockers.append(f"review_artifact_causal_binding_recomputed_mismatch:{field}")
    else:
        blockers.append("review_artifact_causal_binding_evidence_incomplete")

    blockers.extend(
        _independent_review_scope_blockers(
            scope=scope,
            metadata=metadata,
            root=root,
            expected_title=expected_title,
            required_files=list(required_files or []),
            expected_scope_digest=expected_scope_digest,
        )
    )
    return _dedupe_strings(blockers)


def _read_independent_review_evidence_file(
    *, root: Path, path_text: str, expected_sha: str, blocker_prefix: str
) -> tuple[bytes | None, list[str]]:
    if not str(path_text or "").strip() or not _is_sha256(expected_sha):
        return None, [f"{blocker_prefix}_evidence_missing"]
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = root / path
    try:
        resolved = _realpath(path)
        resolved.relative_to(root)
    except ValueError:
        return None, [f"{blocker_prefix}_outside_workspace"]
    try:
        raw = resolved.read_bytes()
    except OSError:
        return None, [f"{blocker_prefix}_missing"]
    if hashlib.sha256(raw).hexdigest() != expected_sha:
        return raw, [f"{blocker_prefix}_sha256_mismatch"]
    return raw, []


def _independent_review_prompt_blockers(*, prompt_raw: bytes, scope: dict[str, Any]) -> list[str]:
    try:
        prompt = prompt_raw.decode("utf-8")
    except UnicodeDecodeError:
        return ["review_artifact_prompt_invalid_utf8"]
    expected_lines = (
        f"Title: {normalize_independent_review_title(scope.get('title'))}",
        f"Base/ref: {str(scope.get('base_ref') or '').strip()}",
        f"Resolved base commit: {str(scope.get('resolved_base_commit') or '').strip()}",
        f"Resolved head commit: {str(scope.get('resolved_head_commit') or '').strip()}",
        f"Scope digest: {str(scope.get('scope_digest_sha256') or '').strip()}",
    )
    blockers = ["review_artifact_prompt_scope_mismatch" for line in expected_lines if line not in prompt]
    try:
        files = normalize_independent_review_files(list(scope.get("files") or []))
    except ValueError:
        files = []
        blockers.append("review_artifact_prompt_invalid_file_scope")
    if any(f"- `{path}`" not in prompt for path in files):
        blockers.append("review_artifact_prompt_file_scope_mismatch")
    context_marker = "Additional context:\n"
    context_end_marker = "\n\nRun read-only inspection commands as needed."
    if context_marker not in prompt or context_end_marker not in prompt.partition(context_marker)[2]:
        blockers.append("review_artifact_prompt_context_missing")
    else:
        context = prompt.partition(context_marker)[2].partition(context_end_marker)[0]
        context_sha = hashlib.sha256(context.strip().encode("utf-8")).hexdigest()
        if context_sha != str(scope.get("extra_context_sha256") or ""):
            blockers.append("review_artifact_prompt_context_sha256_mismatch")
    return _dedupe_strings(blockers)


def _independent_review_events_blockers(*, events_raw: bytes, expected_thread_id: str) -> list[str]:
    thread_ids: set[str] = set()
    blockers: list[str] = []
    for line_number, raw_line in enumerate(events_raw.decode("utf-8", errors="replace").splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            event = json.loads(raw_line)
        except (TypeError, ValueError):
            blockers.append(f"review_artifact_events_invalid_json:{line_number}")
            continue
        if not isinstance(event, dict) or str(event.get("type") or "") != "thread.started":
            continue
        raw_payload = event.get("payload")
        payload = dict(raw_payload) if isinstance(raw_payload, dict) else {}
        thread_id = str(event.get("thread_id") or payload.get("thread_id") or "").strip()
        if thread_id:
            thread_ids.add(thread_id)
    if thread_ids != {expected_thread_id}:
        blockers.append("review_artifact_events_thread_mismatch")
    return blockers


def _review_rollout_event_kind_and_payload(event: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    raw_payload = event.get("payload")
    payload = dict(raw_payload) if isinstance(raw_payload, dict) else {}
    if str(event.get("type") or "") == "event_msg":
        return str(payload.get("type") or "").strip(), payload
    return str(event.get("type") or "").strip(), payload


def _review_rollout_response_message_text(payload: dict[str, Any], *, role: str) -> str | None:
    if str(payload.get("type") or "") != "message" or str(payload.get("role") or "") != role:
        return None
    content = payload.get("content")
    if not isinstance(content, list) or not content:
        return None
    expected_part_type = "input_text" if role == "user" else "output_text"
    parts: list[str] = []
    for raw_part in content:
        if not isinstance(raw_part, dict):
            return None
        if str(raw_part.get("type") or "") != expected_part_type or not isinstance(raw_part.get("text"), str):
            return None
        parts.append(str(raw_part["text"]))
    return "".join(parts)


def _review_normalize_trailing_newlines(value: str) -> str:
    return value.rstrip("\r\n")


def _parse_independent_review_causal_binding(
    *,
    rollout_raw: bytes,
    prompt_raw: bytes,
    raw_output: bytes,
    expected_thread_id: str,
) -> tuple[dict[str, Any], list[str]]:
    rollout_json_valid = True
    text_utf8_valid = True
    try:
        prompt = prompt_raw.decode("utf-8")
        final_output = raw_output.decode("utf-8")
    except UnicodeDecodeError:
        prompt = ""
        final_output = ""
        text_utf8_valid = False
    normalized_final = _review_normalize_trailing_newlines(final_output)
    root_session_sources: list[str] = []
    task_starts: list[tuple[int, str]] = []
    task_completes: list[tuple[int, str, str]] = []
    abort_seen = False
    response_user_messages: list[tuple[int, str]] = []
    event_user_messages: list[tuple[int, str]] = []
    response_final_messages: list[tuple[int, str]] = []
    event_final_messages: list[tuple[int, str]] = []

    for line_number, raw_line in enumerate(rollout_raw.decode("utf-8", errors="replace").splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            event = json.loads(raw_line)
        except (TypeError, ValueError):
            rollout_json_valid = False
            continue
        if not isinstance(event, dict):
            rollout_json_valid = False
            continue
        kind, event_payload = _review_rollout_event_kind_and_payload(event)
        if kind == "session_meta":
            session_id = str(event_payload.get("id") or event_payload.get("session_id") or "").strip()
            if session_id == expected_thread_id:
                source = event_payload.get("source")
                root_session_sources.append(source if isinstance(source, str) else "")
        elif kind == "task_started":
            task_starts.append((line_number, str(event_payload.get("turn_id") or "").strip()))
        elif kind == "task_complete":
            task_completes.append(
                (
                    line_number,
                    str(event_payload.get("turn_id") or "").strip(),
                    str(event_payload.get("last_agent_message") or ""),
                )
            )
        elif "abort" in kind.lower():
            abort_seen = True
        elif kind == "response_item":
            user_text = _review_rollout_response_message_text(event_payload, role="user")
            if user_text is not None:
                response_user_messages.append((line_number, user_text))
            assistant_text = _review_rollout_response_message_text(event_payload, role="assistant")
            if assistant_text is not None and str(event_payload.get("phase") or "") == "final_answer":
                response_final_messages.append((line_number, assistant_text))
        elif kind == "user_message" and isinstance(event_payload.get("message"), str):
            event_user_messages.append((line_number, str(event_payload["message"])))
        elif (
            kind == "agent_message"
            and str(event_payload.get("phase") or "") == "final_answer"
            and isinstance(event_payload.get("message"), str)
        ):
            event_final_messages.append((line_number, str(event_payload["message"])))

    single_task_turn = (
        len(task_starts) == 1
        and len(task_completes) == 1
        and bool(task_starts[0][1])
        and task_starts[0][1] == task_completes[0][1]
        and task_starts[0][0] < task_completes[0][0]
    )
    turn_id = task_starts[0][1] if single_task_turn else ""
    start_line = task_starts[0][0] if single_task_turn else -1
    complete_line = task_completes[0][0] if single_task_turn else -1

    def exact_between(observations: list[tuple[int, str]], expected: str, *, normalize_newlines: bool = False) -> bool:
        matches = [
            line_number
            for line_number, observed in observations
            if start_line < line_number < complete_line
            and (
                _review_normalize_trailing_newlines(observed) == _review_normalize_trailing_newlines(expected)
                if normalize_newlines
                else observed == expected
            )
        ]
        return single_task_turn and len(matches) == 1

    binding = {
        "turn_id": turn_id,
        "prompt_sha256": hashlib.sha256(prompt_raw).hexdigest(),
        "normalized_final_output_sha256": hashlib.sha256(normalized_final.encode("utf-8")).hexdigest(),
        "rollout_json_valid": rollout_json_valid,
        "text_utf8_valid": text_utf8_valid,
        "session_source_exec": root_session_sources == ["exec"],
        "single_task_turn": single_task_turn,
        "no_abort": not abort_seen,
        "prompt_response_item_exact": exact_between(response_user_messages, prompt),
        "prompt_event_message_exact": exact_between(event_user_messages, prompt),
        "final_response_item_exact": exact_between(
            response_final_messages,
            final_output,
            normalize_newlines=True,
        ),
        "final_event_message_exact": exact_between(
            event_final_messages,
            final_output,
            normalize_newlines=True,
        ),
        "task_complete_final_exact": (
            single_task_turn
            and bool(normalized_final)
            and _review_normalize_trailing_newlines(task_completes[0][2]) == normalized_final
        ),
    }
    blockers: list[str] = []
    blocker_by_field = {
        "rollout_json_valid": "review_artifact_rollout_causal_invalid_json",
        "text_utf8_valid": "review_artifact_rollout_causal_invalid_utf8",
        "session_source_exec": "review_artifact_rollout_causal_session_source_not_exec",
        "single_task_turn": "review_artifact_rollout_causal_turn_mismatch",
        "no_abort": "review_artifact_rollout_causal_abort_present",
        "prompt_response_item_exact": "review_artifact_rollout_causal_prompt_response_item_mismatch",
        "prompt_event_message_exact": "review_artifact_rollout_causal_prompt_event_message_mismatch",
        "final_response_item_exact": "review_artifact_rollout_causal_final_response_item_mismatch",
        "final_event_message_exact": "review_artifact_rollout_causal_final_event_message_mismatch",
        "task_complete_final_exact": "review_artifact_rollout_causal_task_complete_mismatch",
    }
    for field, blocker in blocker_by_field.items():
        if binding[field] is not True:
            blockers.append(blocker)
    return binding, blockers


def _review_rollout_first_value(mappings: list[dict[str, Any]], keys: tuple[str, ...]) -> str:
    for mapping in mappings:
        for key in keys:
            value = mapping.get(key)
            if isinstance(value, dict):
                value = value.get("model") or value.get("value")
            normalized = str(value or "").strip()
            if normalized:
                return normalized
    return ""


def _review_rollout_settings(kind: str, payload: dict[str, Any]) -> dict[str, str]:
    if kind not in _INDEPENDENT_REVIEW_ROLLOUT_SETTING_EVENT_TYPES:
        return {}
    containers: list[dict[str, Any]] = []
    for key in ("thread_settings", "session_config", "settings", "config"):
        value = payload.get(key)
        if isinstance(value, dict):
            containers.append(dict(value))
    containers.append(payload)
    collaboration_mode = payload.get("collaboration_mode")
    if isinstance(collaboration_mode, dict) and isinstance(collaboration_mode.get("settings"), dict):
        containers.append(dict(collaboration_mode["settings"]))
    values = {
        "model": _review_rollout_first_value(containers, ("model",)),
        "reasoning_effort": _review_rollout_first_value(
            containers, ("reasoning_effort", "model_reasoning_effort", "effort")
        ),
        "service_tier": _canonical_review_service_tier(_review_rollout_first_value(containers, ("service_tier",))),
    }
    return {field: value for field, value in values.items() if value}


def _parse_independent_review_rollout(*, rollout_raw: bytes, configured_model: str) -> tuple[dict[str, Any], list[str]]:
    state: dict[str, str] = {}
    source: dict[str, list[str]] = {"model": [], "reasoning_effort": [], "service_tier": []}
    session_ids: set[str] = set()
    cli_versions: set[str] = set()
    reroutes: list[dict[str, str]] = []
    blockers: list[str] = []
    for line_number, raw_line in enumerate(rollout_raw.decode("utf-8", errors="replace").splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            event = json.loads(raw_line)
        except (TypeError, ValueError):
            blockers.append(f"review_artifact_rollout_invalid_json:{line_number}")
            continue
        if not isinstance(event, dict):
            blockers.append(f"review_artifact_rollout_invalid_event:{line_number}")
            continue
        kind, payload = _review_rollout_event_kind_and_payload(event)
        if kind == "session_meta":
            session_id = str(payload.get("id") or payload.get("session_id") or "").strip()
            cli_version = str(payload.get("cli_version") or "").strip()
            if session_id:
                session_ids.add(session_id)
            if cli_version:
                cli_versions.add(cli_version)
            continue
        if kind == "model_reroute":
            containers = [payload]
            for key in ("reroute", "model_reroute"):
                value = payload.get(key)
                if isinstance(value, dict):
                    containers.insert(0, dict(value))
            from_model = _review_rollout_first_value(
                containers, ("from_model", "source_model", "previous_model", "original_model", "from")
            )
            to_model = _review_rollout_first_value(
                containers, ("to_model", "target_model", "new_model", "rerouted_model", "to")
            )
            current_model = state.get("model") or configured_model
            if not from_model or not to_model or from_model != current_model:
                blockers.append("review_artifact_rollout_ambiguous_model_reroute")
            else:
                state["model"] = to_model
                if "model_reroute" not in source["model"]:
                    source["model"].append("model_reroute")
                reroutes.append({"from_model": from_model, "to_model": to_model})
            continue
        for field, value in _review_rollout_settings(kind, payload).items():
            previous = state.get(field, "")
            matches = (
                _review_service_tier_matches(previous, value)
                if field == "service_tier" and previous
                else previous == value
            )
            if previous and not matches:
                blockers.append(f"review_artifact_rollout_conflicting_{field}")
                continue
            state[field] = _canonical_review_service_tier(value) if field == "service_tier" else value
            if kind not in source[field]:
                source[field].append(kind)
    if len(session_ids) != 1:
        blockers.append("review_artifact_rollout_invalid_session_identity")
    if len(cli_versions) != 1:
        blockers.append("review_artifact_rollout_invalid_cli_version")
    if reroutes:
        blockers.append("review_artifact_rollout_model_reroute_not_allowed")
    if any(not state.get(field) or not source[field] for field in source):
        blockers.append("review_artifact_rollout_incomplete_effective_settings")
    session_id = next(iter(session_ids), "")
    return {
        "session_id": session_id,
        "thread_id": session_id,
        "codex_cli_version": next(iter(cli_versions), ""),
        "model": state.get("model", ""),
        "reasoning_effort": state.get("reasoning_effort", ""),
        "service_tier": state.get("service_tier", ""),
        "source": source,
        "model_reroutes": reroutes,
    }, blockers


def _independent_review_scope_blockers(
    *,
    scope: dict[str, Any],
    metadata: dict[str, str],
    root: Path,
    expected_title: str,
    required_files: list[str],
    expected_scope_digest: str,
) -> list[str]:
    blockers: list[str] = []
    try:
        files = normalize_independent_review_files(list(scope.get("files") or []))
    except ValueError:
        files = []
        blockers.append("review_artifact_scope_invalid_files")
    if files != list(scope.get("files") or []):
        blockers.append("review_artifact_scope_files_not_canonical")
    title = normalize_independent_review_title(scope.get("title"))
    if expected_title and title != expected_title:
        blockers.append("review_artifact_scope_title_mismatch")
    if required_files and files != required_files:
        blockers.append("review_artifact_scope_files_mismatch")
    digest = independent_review_scope_digest(scope)
    claimed_digest = str(scope.get("scope_digest_sha256") or "").strip()
    if not _is_sha256(claimed_digest) or digest != claimed_digest:
        blockers.append("review_artifact_scope_digest_mismatch")
    if str(metadata.get("review_scope_digest_sha256") or "").strip() != claimed_digest:
        blockers.append("review_artifact_scope_metadata_digest_mismatch")
    expected_digest = str(expected_scope_digest or "").strip()
    if expected_digest and (not _is_sha256(expected_digest) or expected_digest != claimed_digest):
        blockers.append("review_artifact_expected_scope_digest_mismatch")
    if str(scope.get("scope_mode") or "") != INDEPENDENT_REVIEW_PINNED_SCOPE_MODE:
        blockers.append("review_artifact_scope_not_signoff_capable")

    base_ref = str(scope.get("base_ref") or "").strip()
    base_commit = str(scope.get("resolved_base_commit") or "").strip()
    head_commit = str(scope.get("resolved_head_commit") or "").strip()
    if not base_ref or not _is_git_object_id(base_commit) or not _is_git_object_id(head_commit):
        blockers.append("review_artifact_scope_missing_pinned_commits")
        return blockers
    if _review_git_bytes(root, "cat-file", "-e", f"{base_commit}^{{commit}}", allow_failure=True) is None:
        blockers.append("review_artifact_scope_base_commit_missing")
    if _review_git_bytes(root, "cat-file", "-e", f"{head_commit}^{{commit}}", allow_failure=True) is None:
        blockers.append("review_artifact_scope_head_commit_missing")
        return blockers
    git_project = _review_git_project(root)
    if git_project is None:
        blockers.append("review_artifact_scope_git_project_unverifiable")
        return blockers
    pathspecs = [git_project.pathspec(path) for path in files]
    for path in files:
        base_entry_type = _review_git_text(
            root,
            "cat-file",
            "-t",
            git_project.object_spec(base_commit, path),
            allow_failure=True,
        )
        head_entry_type = _review_git_text(
            root,
            "cat-file",
            "-t",
            git_project.object_spec(head_commit, path),
            allow_failure=True,
        )
        if "blob" not in {base_entry_type, head_entry_type}:
            blockers.append(f"review_artifact_scope_missing_file:{path}")
        elif path in required_files and head_entry_type != "blob":
            blockers.append(f"review_artifact_scope_required_file_missing_at_head:{path}")

    diff_raw = _review_git_bytes(
        root,
        "diff",
        "--binary",
        "--no-ext-diff",
        base_commit,
        head_commit,
        "--",
        *pathspecs,
        allow_failure=True,
    )
    tree_raw = _review_git_bytes(
        root,
        "ls-tree",
        "-r",
        head_commit,
        "--",
        *pathspecs,
        allow_failure=True,
    )
    if diff_raw is None or hashlib.sha256(diff_raw).hexdigest() != str(scope.get("git_diff_sha256") or ""):
        blockers.append("review_artifact_scope_git_diff_sha256_mismatch")
    if tree_raw is None or hashlib.sha256(tree_raw).hexdigest() != str(scope.get("git_tree_sha256") or ""):
        blockers.append("review_artifact_scope_git_tree_sha256_mismatch")

    current_head_commit = _review_git_text(
        root,
        "rev-parse",
        "--verify",
        "HEAD^{commit}",
        allow_failure=True,
    )
    reviewed_head_is_ancestor = (
        _review_git_bytes(
            root,
            "merge-base",
            "--is-ancestor",
            head_commit,
            current_head_commit,
            allow_failure=True,
        )
        if current_head_commit
        else None
    )
    later_scoped_commits = (
        _review_git_bytes(
            root,
            "log",
            "--format=%H",
            f"{head_commit}..{current_head_commit}",
            "--",
            *pathspecs,
            allow_failure=True,
        )
        if current_head_commit and reviewed_head_is_ancestor is not None
        else None
    )
    current_scope_index_diff = (
        _review_git_bytes(
            root,
            "diff",
            "--binary",
            "--no-ext-diff",
            "--cached",
            current_head_commit,
            "--",
            *pathspecs,
            allow_failure=True,
        )
        if current_head_commit
        else None
    )
    current_scope_worktree_diff = _review_git_bytes(
        root,
        "diff",
        "--binary",
        "--no-ext-diff",
        "--",
        *pathspecs,
        allow_failure=True,
    )
    current_scope_untracked = _review_git_bytes(
        root,
        "ls-files",
        "--others",
        "--exclude-standard",
        "--",
        *pathspecs,
        allow_failure=True,
    )
    if (
        not current_head_commit
        or reviewed_head_is_ancestor is None
        or later_scoped_commits is None
        or current_scope_index_diff is None
        or current_scope_worktree_diff is None
        or current_scope_untracked is None
    ):
        blockers.append("review_artifact_scope_current_tree_unverifiable")
    elif (
        later_scoped_commits.strip()
        or current_scope_index_diff
        or current_scope_worktree_diff
        or current_scope_untracked.strip()
    ):
        blockers.append("review_artifact_scope_current_tree_mismatch")

    return blockers


def _is_git_object_id(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    return len(normalized) in {40, 64} and all(character in "0123456789abcdef" for character in normalized)


def _canonical_review_service_tier(value: object) -> str:
    normalized = str(value or "").strip()
    return "priority" if normalized in {"fast", "priority"} else normalized


def _is_sha256(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    return len(normalized) == 64 and all(character in "0123456789abcdef" for character in normalized)


def _review_service_tier_matches(configured: str, effective: str) -> bool:
    aliases = {"fast": "priority", "priority": "priority"}
    return aliases.get(str(configured or "").strip(), str(configured or "").strip()) == aliases.get(
        str(effective or "").strip(),
        str(effective or "").strip(),
    )


def _bundle_mandatory_review_tokens(*, plan: dict[str, Any], evidence: dict[str, Any]) -> list[str]:
    tokens = [BUNDLE_MANIFEST_CONTRACT_VERSION]
    source_bundle_sha = str(plan.get("source_bundle_manifest_sha256") or "").strip()
    if source_bundle_sha:
        tokens.append(f"source_bundle_manifest_sha256={source_bundle_sha}")
    scope_digest = str(
        evidence.get("review_plan_scope_digest_sha256") or plan.get("review_plan_scope_digest_sha256") or ""
    ).strip()
    if scope_digest:
        tokens.append(f"review_plan_scope_digest_sha256={scope_digest}")
    return _dedupe_strings(tokens)


def _final_review_verdict(review_text: str) -> str:
    for raw_line in reversed([line.strip() for line in review_text.splitlines() if line.strip()]):
        normalized = raw_line.removeprefix("Verdict:").strip().strip("`").strip().upper()
        if normalized in {"GO", "NO-GO"}:
            return normalized
    return ""


def _is_current_or_latest_path(path_text: str) -> bool:
    for part in Path(str(path_text or "")).parts:
        normalized = _normalize_path_token(part)
        if "current" in normalized or "latest" in normalized:
            return True
    return False


def _raw_path_symlink_reason(*, path_text: str, root: Path) -> str:
    current = root
    parts = Path(str(path_text or "")).parts
    for index, part in enumerate(parts):
        current = current / part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            return ""
        except OSError:
            return "path_lstat_failed"
        if not stat.S_ISLNK(mode):
            continue
        if index == len(parts) - 1:
            return "target_symlink_not_allowed"
        return "symlink_path_component_not_allowed"
    return ""


def _normalize_path_token(value: str) -> str:
    return "".join(character.lower() if character.isalnum() else "_" for character in str(value or "").strip()).strip(
        "_"
    )


def _normalize_retention_classes(values: list[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _effective_retention_class(
    *, path_text: str, retention_class: str = "", matched_markers: list[str] | None = None
) -> str:
    tokens = [_normalize_path_token(part) for part in Path(str(path_text or "")).parts]
    tokens.extend(_normalize_path_token(marker) for marker in list(matched_markers or []))
    normalized_path = _normalize_path_token(path_text)
    tokens.append(normalized_path)
    if any("current" in token or "latest" in token for token in tokens):
        return "review_current_alias_or_latest_artifact"
    if any("phase" in token or "milestone" in token for token in tokens):
        return "review_phase_milestone_artifact"
    if any(any(marker in token for marker in SIGNOFF_OR_PRESSURE_MARKERS) for token in tokens):
        return "review_signoff_or_pressure_run_artifact"
    return str(retention_class or "").strip() or "review_cold_archive_candidate"


def _plan_workspace_root_mismatch(*, plan: dict[str, Any], root: Path) -> str:
    plan_root = str(plan.get("workspace_root") or "").strip()
    if not plan_root:
        return "missing_plan_workspace_root"
    if _realpath(plan_root) != _realpath(root):
        return "workspace_root_mismatch"
    return ""


def _cold_copy_manifest_blockers(*, plan: dict[str, Any], root: Path, manifest_path: str) -> list[str]:
    raw_path = str(manifest_path or "").strip()
    if not raw_path:
        return ["missing_cold_copy_manifest"]
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = root / path
    try:
        resolved = _realpath(path)
        resolved.relative_to(_realpath(root))
    except ValueError:
        return ["cold_copy_manifest_outside_workspace"]
    if not resolved.exists():
        return ["cold_copy_manifest_missing"]
    if not resolved.is_file():
        return ["cold_copy_manifest_not_file"]
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ["cold_copy_manifest_invalid_json"]
    contract_version = str(dict(payload).get("contract_version") or "") if isinstance(payload, dict) else ""
    contract_blockers: list[str] = []
    entries = _cold_copy_manifest_entries(payload)
    if contract_version == BUNDLE_MANIFEST_CONTRACT_VERSION:
        entries, archive_field_blockers = _supersession_cold_bundle_archives(payload)
        contract_blockers.extend(archive_field_blockers)
        contract_blockers.extend(
            _supersession_cold_bundle_manifest_blockers(plan=plan, root=root, payload=payload, archives=entries)
        )
    by_path = {str(entry.get("path") or ""): entry for entry in entries if str(entry.get("path") or "")}
    missing_count = 0
    metadata_mismatch_count = 0
    missing_proof_count = 0
    for operation in list(plan.get("operations") or []):
        op = dict(operation or {})
        path_text = str(op.get("path") or "")
        entry = by_path.get(path_text)
        if not entry:
            missing_count += 1
            continue
        for key in ("size_bytes", "file_count", "directory_count"):
            if _manifest_int(entry, key) != _operation_int(op, key):
                metadata_mismatch_count += 1
                break
        else:
            if str(entry.get("latest_mtime") or "") != str(op.get("latest_mtime") or ""):
                metadata_mismatch_count += 1
        if not _manifest_entry_has_proof(entry):
            missing_proof_count += 1
    blockers: list[str] = []
    if missing_count:
        blockers.append(f"cold_copy_manifest_missing_operations:{missing_count}")
    if metadata_mismatch_count:
        blockers.append(f"cold_copy_manifest_metadata_mismatch:{metadata_mismatch_count}")
    if missing_proof_count:
        blockers.append(f"cold_copy_manifest_missing_proof:{missing_proof_count}")
    return contract_blockers + blockers


def _cold_copy_manifest_contract_version(*, root: Path, manifest_path: str) -> str:
    raw_path = str(manifest_path or "").strip()
    if not raw_path:
        return ""
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = root / path
    try:
        resolved = _realpath(path)
        resolved.relative_to(_realpath(root))
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ""
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("contract_version") or "")


def _bundle_plan_binding_blockers(*, plan: dict[str, Any], root: Path, manifest_path: str) -> list[str]:
    blockers: list[str] = []
    if str(plan.get("source_bundle_contract_version") or "") != BUNDLE_MANIFEST_CONTRACT_VERSION:
        blockers.append("missing_source_bundle_plan_binding")
    expected_path = str(plan.get("source_bundle_manifest_path") or "").strip()
    expected_sha = str(plan.get("source_bundle_manifest_sha256") or "").strip()
    evidence = dict(plan.get("destructive_review_evidence") or {})
    if not expected_path:
        blockers.append("missing_source_bundle_manifest_path")
    if len(expected_sha) != 64:
        blockers.append("missing_source_bundle_manifest_sha256")
    expected_relative = _workspace_relative_file_path(root=root, raw_path=expected_path)
    evidence_relative = _workspace_relative_file_path(root=root, raw_path=manifest_path)
    if not expected_relative or not evidence_relative or expected_relative != evidence_relative:
        blockers.append("source_bundle_manifest_path_mismatch")
    current_sha = _manifest_file_sha256(root=root, manifest_path=manifest_path)
    if expected_sha and current_sha and current_sha != expected_sha:
        blockers.append("source_bundle_manifest_sha256_mismatch")
    if expected_sha and not current_sha:
        blockers.append("source_bundle_manifest_sha256_unavailable")
    blockers.extend(
        _bundle_review_scope_binding_blockers(
            plan=plan,
            root=root,
            evidence=evidence,
            source_bundle_path=expected_relative,
            source_bundle_sha256=expected_sha,
        )
    )
    return blockers


def _bundle_review_scope_binding_blockers(
    *,
    plan: dict[str, Any],
    root: Path,
    evidence: dict[str, Any],
    source_bundle_path: str,
    source_bundle_sha256: str,
) -> list[str]:
    blockers: list[str] = []
    review_plan_artifact = str(evidence.get("review_plan_artifact") or "").strip()
    scope_digest = str(
        evidence.get("review_plan_scope_digest_sha256") or plan.get("review_plan_scope_digest_sha256") or ""
    ).strip()
    if not review_plan_artifact:
        blockers.append("missing_review_plan_artifact")
    if len(scope_digest) != 64:
        blockers.append("missing_review_plan_scope_digest_sha256")
    else:
        current_scope_digest = _prune_plan_scope_digest(plan)
        if current_scope_digest != scope_digest:
            blockers.append("review_plan_scope_digest_mismatch")
    if review_plan_artifact and len(scope_digest) == 64:
        blockers.extend(
            _review_plan_artifact_blockers(
                plan=plan, root=root, artifact_path=review_plan_artifact, expected_scope_digest=scope_digest
            )
        )
    if (
        source_bundle_path
        and source_bundle_sha256
        and not _review_required_artifact_present(
            evidence=evidence,
            path=source_bundle_path,
            sha256=source_bundle_sha256,
        )
    ):
        blockers.append("missing_source_bundle_review_required_artifact")
    if (
        review_plan_artifact
        and scope_digest
        and not _review_required_artifact_present(
            evidence=evidence,
            path=review_plan_artifact,
            sha256=scope_digest,
        )
    ):
        blockers.append("missing_review_plan_required_artifact")
    return blockers


def _review_plan_artifact_blockers(
    *, plan: dict[str, Any], root: Path, artifact_path: str, expected_scope_digest: str
) -> list[str]:
    blockers: list[str] = []
    raw_path = str(artifact_path or "").strip()
    if not raw_path:
        return ["missing_review_plan_artifact"]
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = root / path
    try:
        resolved = _realpath(path)
        resolved.relative_to(_realpath(root))
    except ValueError:
        return ["review_plan_artifact_outside_workspace"]
    if not resolved.exists():
        return ["review_plan_artifact_missing"]
    if not resolved.is_file():
        return ["review_plan_artifact_not_file"]
    try:
        artifact_payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ["review_plan_artifact_invalid_json"]
    if not isinstance(artifact_payload, dict):
        return ["review_plan_artifact_invalid_json"]
    if _prune_plan_scope_digest(dict(artifact_payload)) != expected_scope_digest:
        blockers.append("review_plan_artifact_scope_digest_mismatch")
    if _prune_plan_scope_digest(plan) != expected_scope_digest:
        blockers.append("review_plan_scope_digest_mismatch")
    return blockers


def _review_required_artifact_present(*, evidence: dict[str, Any], path: str, sha256: str) -> bool:
    expected_path = str(path or "").strip()
    expected_sha = str(sha256 or "").strip()
    for raw_item in list(evidence.get("review_required_artifacts") or []):
        if not isinstance(raw_item, dict):
            continue
        item = dict(raw_item)
        if (
            str(item.get("path") or "").strip() == expected_path
            and str(item.get("sha256") or "").strip() == expected_sha
        ):
            return True
    return False


def _cold_copy_manifest_entries(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item or {}) for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("operations", "entries", "items", "files", "archives"):
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(item or {}) for item in value if isinstance(item, dict)]
    return []


def _supersession_cold_bundle_archives(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    if "archives" not in payload:
        return [], ["cold_bundle_missing_archives"]
    archives = payload.get("archives")
    if not isinstance(archives, list):
        return [], ["cold_bundle_archives_not_list"]
    return [dict(item or {}) for item in archives if isinstance(item, dict)], []


def _manifest_int(entry: dict[str, Any], key: str) -> int | None:
    if key not in entry:
        return None
    try:
        return int(entry.get(key))
    except (TypeError, ValueError):
        return None


def _manifest_entry_has_proof(entry: dict[str, Any]) -> bool:
    proof_keys = (
        "sha256",
        "sha256_digest",
        "archive_sha256",
        "content_hash",
        "hash",
        "proof",
        "proof_uri",
        "manifest_sha256",
    )
    return any(str(entry.get(key) or "").strip() for key in proof_keys)


def _supersession_cold_bundle_manifest_blockers(
    *, plan: dict[str, Any], root: Path, payload: dict[str, Any], archives: list[dict[str, Any]]
) -> list[str]:
    blockers: list[str] = []
    if str(payload.get("status") or "") != "ready_for_prune_cold_copy_manifest":
        blockers.append("cold_bundle_manifest_not_ready")
    if bool(payload.get("deletion_allowed")):
        blockers.append("cold_bundle_manifest_must_not_allow_deletion")
    if bool(payload.get("source_contract_valid")) is not True:
        blockers.append("cold_bundle_manifest_invalid_source_contract")
    blockers.extend(_cold_bundle_archive_list_blockers(payload=payload, archives=archives))
    by_path = {str(item.get("path") or ""): dict(item or {}) for item in archives if str(item.get("path") or "")}
    operation_paths = [str(dict(item or {}).get("path") or "") for item in list(plan.get("operations") or [])]
    ready_archive_paths = [
        str(dict(item or {}).get("path") or "")
        for item in archives
        if str(dict(item or {}).get("status") or "") == "archive_ready_for_prune_cold_copy"
    ]
    if sorted(operation_paths) != sorted(ready_archive_paths):
        blockers.append("cold_bundle_plan_archive_path_set_mismatch")
    for operation in list(plan.get("operations") or []):
        op = dict(operation or {})
        path_text = str(op.get("path") or "")
        entry = by_path.get(path_text)
        if not entry:
            continue
        if str(entry.get("status") or "") != "archive_ready_for_prune_cold_copy":
            blockers.append(f"cold_bundle_entry_not_ready:{path_text}")
        if bool(entry.get("archive_verified")) is not True:
            blockers.append(f"cold_bundle_entry_not_verified:{path_text}")
        archive_reason = _cold_bundle_archive_file_reason(
            entry=entry, operation=op, root=root, operation_paths=operation_paths
        )
        if archive_reason:
            blockers.append(f"{archive_reason}:{path_text}")
    return blockers


def _cold_bundle_archive_file_reason(
    *,
    entry: dict[str, Any],
    operation: dict[str, Any],
    root: Path,
    operation_paths: list[str],
) -> str:
    raw_archive_path = str(entry.get("archive_path") or "")
    if not raw_archive_path:
        return "cold_bundle_archive_missing_path"
    archive_path = Path(raw_archive_path).expanduser()
    if archive_path.is_absolute():
        return "cold_bundle_archive_absolute_path_not_allowed"
    if any(part in {"", ".", ".."} for part in archive_path.parts):
        return "cold_bundle_archive_unsafe_path_component"
    symlink_reason = _cold_bundle_raw_path_symlink_reason(path=archive_path, root=root)
    if symlink_reason:
        return symlink_reason
    resolved = (root / archive_path).resolve(strict=False)
    try:
        resolved.relative_to(root.resolve())
    except ValueError:
        return "cold_bundle_archive_outside_workspace"
    resolved_relative = resolved.relative_to(root.resolve()).as_posix()
    for allowed_root in ALLOWED_PRUNE_ROOTS:
        if resolved_relative == allowed_root or resolved_relative.startswith(f"{allowed_root}/"):
            return "cold_bundle_archive_inside_prunable_source_root"
    source_path = (root / str(operation.get("path") or "")).resolve(strict=False)
    try:
        resolved.relative_to(source_path)
        return "cold_bundle_archive_inside_source_directory"
    except ValueError:
        pass
    for operation_path in operation_paths:
        if not operation_path or operation_path == str(operation.get("path") or ""):
            continue
        try:
            resolved.relative_to((root / operation_path).resolve(strict=False))
            return "cold_bundle_archive_inside_another_operation_source_directory"
        except ValueError:
            pass
    if not resolved.exists():
        return "cold_bundle_archive_missing"
    if not resolved.is_file() or resolved.is_symlink():
        return "cold_bundle_archive_not_plain_file"
    expected_size = _manifest_int(entry, "archive_size_bytes")
    if expected_size is None or expected_size != int(resolved.stat().st_size):
        return "cold_bundle_archive_size_mismatch"
    expected_sha = str(entry.get("archive_sha256") or "").strip()
    if len(expected_sha) != 64:
        return "cold_bundle_archive_missing_sha256"
    if _file_sha256(resolved) != expected_sha:
        return "cold_bundle_archive_sha256_mismatch"
    if not str(entry.get("source_manifest_digest_sha256") or "").strip():
        return "cold_bundle_missing_source_manifest_digest"
    archive_format = str(entry.get("archive_format") or "")
    archive_structure_reason = _cold_bundle_archive_structure_reason(
        archive_path=resolved, archive_format=archive_format, source_path=str(operation.get("path") or "")
    )
    if archive_structure_reason:
        return archive_structure_reason
    return ""


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _cold_bundle_raw_path_symlink_reason(*, path: Path, root: Path) -> str:
    current = root
    for part in path.parts:
        current = current / part
        try:
            current.lstat()
        except FileNotFoundError:
            return ""
        except OSError:
            return "cold_bundle_archive_lstat_failed"
        if current.is_symlink():
            return "cold_bundle_archive_raw_path_contains_symlink"
    return ""


def _cold_bundle_archive_structure_reason(*, archive_path: Path, archive_format: str, source_path: str) -> str:
    if archive_format == "tar.gzip":
        try:
            with tarfile.open(archive_path, "r:gz") as archive:
                names = archive.getnames()
        except (tarfile.TarError, OSError):
            return "cold_bundle_archive_invalid_tar"
    elif archive_format == "tar.zstd":
        if shutil.which("zstd") is None:
            return "cold_bundle_archive_zstd_unavailable"
        result = subprocess.run(
            ["sh", "-c", 'zstd -dc "$1" | tar -tf -', "sh", str(archive_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return "cold_bundle_archive_invalid_tar"
        names = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    else:
        return "cold_bundle_archive_unsupported_format"
    prefix = str(source_path or "").rstrip("/") + "/"
    if not names:
        return "cold_bundle_archive_empty"
    for name in names:
        if Path(name).is_absolute() or any(part in {"", ".", ".."} for part in Path(name).parts):
            return "cold_bundle_archive_unsafe_member_path"
    if not any(name == source_path or name.startswith(prefix) for name in names):
        return "cold_bundle_archive_missing_source_prefix"
    return ""


def _uncopied_privileged_retention_classes(plan: dict[str, Any]) -> list[str]:
    privileged: set[str] = set()
    for operation in list(plan.get("operations") or []):
        payload = dict(operation or {})
        declared_retention_class = str(payload.get("retention_class") or "").strip()
        retention_class = _effective_retention_class(
            path_text=str(payload.get("path") or ""),
            retention_class=declared_retention_class,
            matched_markers=list(payload.get("matched_markers") or []),
        )
        if not retention_class or retention_class != declared_retention_class:
            continue
        if retention_class == "review_current_alias_or_latest_artifact":
            continue
        if retention_class and retention_class != "review_cold_archive_candidate":
            privileged.add(retention_class)
    return sorted(privileged)


def _cold_bundle_source_contract_valid(manifest: dict[str, Any]) -> bool:
    summary = dict(manifest.get("summary") or {})
    archives = [dict(item or {}) for item in list(manifest.get("archives") or []) if isinstance(item, dict)]
    archive_count = int(summary.get("archive_count") or 0)
    return (
        str(manifest.get("contract_version") or "") == "runtime_asset_supersession_cold_bundle_manifest_v1"
        and str(manifest.get("status") or "") == "ready_for_prune_cold_copy_manifest"
        and bool(manifest.get("archive_created")) is True
        and bool(manifest.get("deletion_allowed")) is False
        and bool(manifest.get("source_contract_valid")) is True
        and archive_count > 0
        and not _cold_bundle_archive_list_blockers(payload=manifest, archives=archives)
        and len(archives) == archive_count
        and int(summary.get("ready_archive_count") or 0) == archive_count
        and int(summary.get("archive_verified_count") or 0) == archive_count
        and int(summary.get("archive_created_count") or 0) == archive_count
        and int(summary.get("blocked_archive_count") or 0) == 0
    )


def _cold_bundle_archive_list_blockers(*, payload: dict[str, Any], archives: list[dict[str, Any]]) -> list[str]:
    summary = dict(payload.get("summary") or {})
    archive_count = _summary_int(summary, "archive_count")
    ready_count = _summary_int(summary, "ready_archive_count")
    verified_count = _summary_int(summary, "archive_verified_count")
    created_count = _summary_int(summary, "archive_created_count")
    blocked_count = _summary_int(summary, "blocked_archive_count")
    blockers: list[str] = []
    if archive_count is None:
        blockers.append("cold_bundle_missing_archive_count")
        archive_count = -1
    if ready_count is None:
        blockers.append("cold_bundle_missing_ready_archive_count")
        ready_count = -1
    if verified_count is None:
        blockers.append("cold_bundle_missing_archive_verified_count")
        verified_count = -1
    if created_count is None:
        blockers.append("cold_bundle_missing_archive_created_count")
        created_count = -1
    if blocked_count is None:
        blockers.append("cold_bundle_missing_blocked_archive_count")
        blocked_count = -1
    if archive_count != len(archives):
        blockers.append("cold_bundle_archive_count_mismatch")
    if ready_count != len(archives):
        blockers.append("cold_bundle_ready_archive_count_mismatch")
    if verified_count != len(archives):
        blockers.append("cold_bundle_verified_archive_count_mismatch")
    if created_count != len(archives):
        blockers.append("cold_bundle_created_archive_count_mismatch")
    if blocked_count != 0:
        blockers.append("cold_bundle_blocked_archive_count_nonzero")
    paths = [str(item.get("path") or "") for item in archives]
    if any(not path for path in paths):
        blockers.append("cold_bundle_archive_missing_source_path")
    if len(set(paths)) != len(paths):
        blockers.append("cold_bundle_duplicate_source_path")
    return _dedupe_strings(blockers)


def _summary_int(summary: dict[str, Any], key: str) -> int | None:
    if key not in summary:
        return None
    try:
        return int(summary.get(key))
    except (TypeError, ValueError):
        return None


def _cold_bundle_entry_plan_skip_reason(*, entry: dict[str, Any], root: Path, source_contract_valid: bool) -> str:
    if not source_contract_valid:
        return "invalid_cold_bundle_manifest_contract"
    if str(entry.get("status") or "") != "archive_ready_for_prune_cold_copy":
        return "cold_bundle_entry_not_ready"
    if bool(entry.get("archive_created")) is not True or bool(entry.get("archive_verified")) is not True:
        return "cold_bundle_entry_not_verified"
    if bool(entry.get("deletion_allowed")):
        return "cold_bundle_entry_must_not_allow_deletion"
    if not str(entry.get("archive_sha256") or "").strip():
        return "cold_bundle_entry_missing_archive_sha256"
    if not str(entry.get("source_manifest_digest_sha256") or "").strip():
        return "cold_bundle_entry_missing_source_manifest_digest"
    path_reason = _path_safety_reason(path_text=str(entry.get("path") or ""), root=root)
    if path_reason:
        return path_reason
    target = root / str(entry.get("path") or "")
    if not target.exists():
        return "target_missing"
    if not target.is_dir() or target.is_symlink():
        return "target_not_plain_directory"
    if str(entry.get("scan_root") or "") not in ALLOWED_PRUNE_ROOTS:
        return "invalid_source_scan_root"
    if _manifest_int(entry, "size_bytes") is None:
        return "missing_size_bytes"
    if _manifest_int(entry, "file_count") is None:
        return "missing_file_count"
    if _manifest_int(entry, "directory_count") is None:
        return "missing_directory_count"
    if not _parse_datetime(str(entry.get("latest_mtime") or "")):
        return "missing_or_invalid_latest_mtime"
    current = _directory_size_summary(target)
    if _manifest_int(entry, "size_bytes") != int(current["size_bytes"]):
        return "size_drift_detected"
    if _manifest_int(entry, "file_count") != int(current["file_count"]):
        return "file_count_drift_detected"
    if _manifest_int(entry, "directory_count") != int(current["directory_count"]):
        return "directory_count_drift_detected"
    current_mtime = _parse_datetime(str(current.get("latest_mtime") or ""))
    planned_mtime = _parse_datetime(str(entry.get("latest_mtime") or ""))
    if current_mtime and planned_mtime and current_mtime != planned_mtime:
        return "latest_mtime_drift_detected"
    retention_class = str(entry.get("retention_class") or "").strip()
    if not retention_class:
        return "missing_retention_class"
    effective_retention_class = _effective_retention_class(
        path_text=str(entry.get("path") or ""),
        retention_class=retention_class,
        matched_markers=_retention_markers_from_path(str(entry.get("path") or "")),
    )
    if retention_class != effective_retention_class:
        return "retention_class_path_mismatch"
    return ""


def _retention_markers_from_path(path_text: str) -> list[str]:
    normalized_path = _normalize_path_token(path_text)
    markers = [marker for marker in SIGNOFF_OR_PRESSURE_MARKERS if marker and marker in normalized_path]
    if "phase" in normalized_path:
        markers.append("phase")
    if "milestone" in normalized_path:
        markers.append("milestone")
    return _dedupe_strings(markers)


def _manifest_file_sha256(*, root: Path, manifest_path: str) -> str:
    raw_path = str(manifest_path or "").strip()
    if not raw_path:
        return ""
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = root / path
    try:
        resolved = _realpath(path)
        resolved.relative_to(_realpath(root))
    except ValueError:
        return ""
    if not resolved.is_file():
        return ""
    return _file_sha256(resolved)


def _workspace_relative_file_path(*, root: Path, raw_path: str) -> str:
    text = str(raw_path or "").strip()
    if not text:
        return ""
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = root / path
    try:
        resolved = _realpath(path)
        return resolved.relative_to(_realpath(root)).as_posix()
    except ValueError:
        return ""


def _prune_plan_scope_digest(plan: dict[str, Any]) -> str:
    operations: list[dict[str, Any]] = []
    for raw_operation in list(plan.get("operations") or []):
        operation = dict(raw_operation or {})
        operations.append(
            {
                "action": str(operation.get("action") or ""),
                "path": str(operation.get("path") or ""),
                "size_bytes": _operation_int(operation, "size_bytes"),
                "file_count": _operation_int(operation, "file_count"),
                "directory_count": _operation_int(operation, "directory_count"),
                "latest_mtime": str(operation.get("latest_mtime") or ""),
                "retention_class": str(operation.get("retention_class") or ""),
                "source_scan_root": str(operation.get("source_scan_root") or ""),
                "source_bundle_archive_path": str(operation.get("source_bundle_archive_path") or ""),
                "source_bundle_archive_sha256": str(operation.get("source_bundle_archive_sha256") or ""),
                "source_manifest_digest_sha256": str(operation.get("source_manifest_digest_sha256") or ""),
            }
        )
    operations.sort(key=lambda item: str(item.get("path") or ""))
    payload = {
        "contract_version": str(plan.get("contract_version") or ""),
        "status": str(plan.get("status") or ""),
        "workspace_root": str(plan.get("workspace_root") or ""),
        "source_bundle_contract_version": str(plan.get("source_bundle_contract_version") or ""),
        "source_bundle_manifest_path": str(plan.get("source_bundle_manifest_path") or ""),
        "source_bundle_manifest_sha256": str(plan.get("source_bundle_manifest_sha256") or ""),
        "source_bundle_status": str(plan.get("source_bundle_status") or ""),
        "allowed_prune_roots": list(plan.get("allowed_prune_roots") or []),
        "allowed_retention_classes": list(plan.get("allowed_retention_classes") or []),
        "operations": operations,
    }
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _operation_result_failed(result: dict[str, Any]) -> bool:
    status = str(result.get("status") or "")
    return status.startswith("blocked") or status == "error"


def _active_runtime_pid_file_processes(
    *,
    root: Path,
    process_commands: dict[int, str],
    operation_paths: list[str] | None = None,
) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []
    for reference_path, pid in _iter_runtime_process_references(root, operation_paths=operation_paths):
        if pid <= 0 or pid == os.getpid():
            continue
        command = process_commands.get(pid)
        if command is None:
            continue
        if not _command_is_workspace_runtime_process(
            command=command, root=root
        ) and not _runtime_process_reference_is_fresh(reference_path):
            # The recorded pid is alive, but the live process is unrecognizable
            # as a workspace runtime process AND the reference file has gone
            # stale far beyond its heartbeat contract: treat as pid reuse after
            # the recorded service died without cleanup. A blanket pid-alive
            # rule would let a dead service block pruning of its own runtime
            # directory forever.
            continue
        active.append(
            {
                "pid": pid,
                "command": f"{command[:450]} [runtime process reference: {_display_path(reference_path, root)}]",
            }
        )
    return active


def _command_is_workspace_runtime_process(*, command: str, root: Path) -> bool:
    normalized = str(command or "").lower()
    if any(token in normalized for token in ACTIVE_PROCESS_TOKENS):
        return True
    return str(_realpath(root)).lower() in normalized


_REFERENCE_FRESHNESS_FLOOR_SECONDS = 3600
_REFERENCE_STALE_MULTIPLIER = 20


def _runtime_process_reference_is_fresh(reference_path: Path) -> bool:
    window_seconds = _REFERENCE_FRESHNESS_FLOOR_SECONDS
    newest: datetime | None = None
    if reference_path.name == "status.json":
        try:
            payload = dict(json.loads(reference_path.read_text(encoding="utf-8")) or {})
        except (OSError, TypeError, ValueError):
            payload = {}
        try:
            stale_after_seconds = int(payload.get("stale_after_seconds") or 0)
        except (TypeError, ValueError):
            stale_after_seconds = 0
        if stale_after_seconds > 0:
            window_seconds = max(window_seconds, stale_after_seconds * _REFERENCE_STALE_MULTIPLIER)
        for key in ("updated_at", "started_at"):
            raw_value = str(payload.get(key) or "").strip()
            if not raw_value:
                continue
            try:
                parsed = datetime.fromisoformat(raw_value)
            except ValueError:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            if newest is None or parsed > newest:
                newest = parsed
    if newest is None:
        try:
            newest = datetime.fromtimestamp(reference_path.stat().st_mtime, tz=timezone.utc)
        except OSError:
            return True
    return (datetime.now(timezone.utc) - newest).total_seconds() <= window_seconds


def _iter_runtime_process_references(root: Path, *, operation_paths: list[str] | None = None) -> list[tuple[Path, int]]:
    references: list[tuple[Path, int]] = []
    scan_bases = _runtime_process_reference_scan_bases(root=root, operation_paths=operation_paths)
    for base, recursive in scan_bases:
        patterns = (
            ("**/service_logs/*.pid", "**/services/*/status.json")
            if recursive
            else ("service_logs/*.pid", "services/*/status.json")
        )
        for pattern in patterns:
            try:
                for reference_path in base.glob(pattern):
                    if not reference_path.is_file():
                        continue
                    if reference_path.name == "status.json":
                        pid = _read_active_status_pid(reference_path)
                    else:
                        pid = _read_pid_file(reference_path)
                    if pid > 0:
                        references.append((reference_path, pid))
            except OSError:
                continue
    return references


def _runtime_process_reference_scan_bases(*, root: Path, operation_paths: list[str] | None) -> list[tuple[Path, bool]]:
    bases: list[tuple[Path, bool]] = []
    seen: set[str] = set()

    def add_base(path: Path, *, recursive: bool) -> None:
        try:
            resolved = _realpath(path)
            resolved.relative_to(_realpath(root))
        except ValueError:
            return
        key = f"{resolved}:{int(recursive)}"
        if key in seen or not resolved.exists() or not resolved.is_dir():
            return
        seen.add(key)
        bases.append((resolved, recursive))

    add_base(root / "runtime", recursive=False)
    for allowed_root in ALLOWED_PRUNE_ROOTS:
        add_base(root / allowed_root, recursive=False)
    for raw_path in list(operation_paths or []):
        path_text = str(raw_path or "")
        if _path_safety_reason(path_text=path_text, root=root):
            continue
        add_base(root / path_text, recursive=True)
    if not operation_paths:
        for allowed_root in ALLOWED_PRUNE_ROOTS:
            add_base(root / allowed_root, recursive=True)
    return bases


def _read_pid_file(path: Path) -> int:
    try:
        return int(path.read_text(encoding="utf-8", errors="replace").strip())
    except (OSError, ValueError):
        return 0


def _read_active_status_pid(path: Path) -> int:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return 0
    try:
        payload_dict = dict(payload or {})
    except (TypeError, ValueError):
        return 0
    status = str(payload_dict.get("status") or "").strip().lower()
    if status in {"stopped", "completed", "failed", "failed_terminal", "cancelled", "canceled"}:
        return 0
    try:
        return int(payload_dict.get("pid") or 0)
    except (TypeError, ValueError):
        return 0


def _display_path(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _operation_int(operation: dict[str, Any], key: str) -> int | None:
    if key not in operation:
        return None
    try:
        return int(operation.get(key))
    except (TypeError, ValueError):
        return None


def _directory_size_summary(path: Path) -> dict[str, Any]:
    size_bytes = 0
    file_count = 0
    directory_count = 0
    latest_mtime = 0.0
    stack = [path]
    while stack:
        current = stack.pop()
        try:
            stat = current.lstat()
        except OSError:
            continue
        latest_mtime = max(latest_mtime, float(stat.st_mtime))
        if current.is_dir() and not current.is_symlink():
            directory_count += 1
            try:
                entries = list(os.scandir(current))
            except OSError:
                continue
            for entry in entries:
                stack.append(Path(entry.path))
            continue
        file_count += 1
        size_bytes += int(stat.st_size)
    return {
        "size_bytes": size_bytes,
        "file_count": file_count,
        "directory_count": max(0, directory_count - 1),
        "latest_mtime": datetime.fromtimestamp(latest_mtime, timezone.utc).isoformat() if latest_mtime else "",
    }


def _parse_datetime(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _resolve_root(value: str | Path) -> Path:
    root = Path(str(value)).expanduser()
    if not root.is_absolute():
        root = Path.cwd() / root
    return _realpath(root)


def _realpath(value: str | Path) -> Path:
    return Path(os.path.realpath(os.path.abspath(str(value))))


def _dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    return deduped


def dumps_report(report: dict[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
