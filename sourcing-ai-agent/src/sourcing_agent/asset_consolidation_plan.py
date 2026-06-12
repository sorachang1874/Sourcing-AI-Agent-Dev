from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ARCHIVE_CANDIDATE_CLASSIFICATION = "archive_candidate_no_increment_duplicate"
KEEP_REUSABLE_CLASSIFICATION = "keep_reusable_shard_source"
KEEP_AUTHORITATIVE_CLASSIFICATION = "keep_authoritative_serving"
REVIEW_LOCAL_ONLY_CLASSIFICATION = "review_local_only_snapshot"
SUBSUMED_STATUS = "subsumed_by_reference"


def build_asset_consolidation_plan(
    *,
    audit_report: dict[str, Any],
    runtime_dir: str | Path,
) -> dict[str, Any]:
    """Build a non-mutating W5c repair/archive plan from W5 audit evidence.

    The plan intentionally consumes the W5 audit report instead of recomputing
    dependencies. W5c planning must be cheap, explainable, and safe to run after
    a long audit without touching registry pointers or asset files.
    """

    runtime_root = Path(runtime_dir).expanduser().resolve()
    companies = [
        _build_company_plan(company=company, runtime_dir=runtime_root)
        for company in list(audit_report.get("companies") or [])
        if isinstance(company, dict)
    ]
    summary = {
        "company_count": len(companies),
        "action_required_company_count": sum(1 for company in companies if company.get("required_actions")),
        "missing_reference_company_count": sum(
            1 for company in companies if company.get("reference_state", {}).get("missing_reference_snapshot_ids")
        ),
        "archive_candidate_count": sum(
            int(company.get("archive_plan", {}).get("archive_candidate_count") or 0) for company in companies
        ),
        "archive_blocked_snapshot_count": sum(
            int(company.get("archive_plan", {}).get("blocked_archive_candidate_count") or 0) for company in companies
        ),
        "archive_ready_snapshot_count": sum(
            int(company.get("archive_plan", {}).get("archive_ready_snapshot_count") or 0) for company in companies
        ),
        "candidate_replacement_count": sum(len(company.get("candidate_authoritative_replacements") or []) for company in companies),
        "reusable_shard_source_snapshot_count": sum(
            len(company.get("preserve_snapshots", {}).get("reusable_shard_sources") or []) for company in companies
        ),
    }
    if summary["missing_reference_company_count"]:
        status = "blocked_missing_reference_identity"
    elif summary["archive_ready_snapshot_count"]:
        status = "ready_for_cold_backup_manifest_review"
    elif summary["archive_candidate_count"]:
        status = "blocked_overlap_review"
    else:
        status = "no_archive_candidates"
    return {
        "status": status,
        "contract_version": "asset_consolidation_plan_v1",
        "read_only": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_audit_contract_version": _normalize_text(audit_report.get("contract_version")),
        "source_audit_generated_at": _normalize_text(audit_report.get("generated_at")),
        "summary": summary,
        "companies": companies,
    }


def render_asset_consolidation_plan_markdown(plan: dict[str, Any]) -> str:
    summary = dict(plan.get("summary") or {})
    lines = [
        "# Asset Consolidation Plan",
        "",
        f"- Status: `{plan.get('status')}`",
        f"- Read-only: `{bool(plan.get('read_only'))}`",
        f"- Generated at: `{plan.get('generated_at')}`",
        f"- Companies: `{summary.get('company_count', 0)}`",
        f"- Companies requiring action: `{summary.get('action_required_company_count', 0)}`",
        f"- Missing-reference companies: `{summary.get('missing_reference_company_count', 0)}`",
        f"- Archive candidates: `{summary.get('archive_candidate_count', 0)}`",
        f"- Archive-ready snapshots: `{summary.get('archive_ready_snapshot_count', 0)}`",
        f"- Blocked archive candidates: `{summary.get('archive_blocked_snapshot_count', 0)}`",
        "",
    ]
    for company in list(plan.get("companies") or []):
        payload = dict(company or {})
        reference_state = dict(payload.get("reference_state") or {})
        archive_plan = dict(payload.get("archive_plan") or {})
        lines.extend(
            [
                f"## {payload.get('target_company') or payload.get('company_key')}",
                "",
                f"- Status: `{payload.get('status')}`",
                f"- Collection: `{payload.get('collection_id')}`",
                f"- Missing references: `{', '.join(reference_state.get('missing_reference_snapshot_ids') or []) or '-'}`",
                f"- Archive candidates: `{archive_plan.get('archive_candidate_count', 0)}`",
                f"- Archive ready: `{archive_plan.get('archive_ready_snapshot_count', 0)}`",
                f"- Next gate: `{payload.get('next_gate')}`",
                "",
            ]
        )
        actions = list(payload.get("required_actions") or [])
        if actions:
            lines.append("### Required Actions")
            lines.append("")
            for action in actions:
                action_payload = dict(action or {})
                lines.append(
                    "- `{action_type}` severity=`{severity}` snapshots=`{snapshots}` reason=`{reason}`".format(
                        action_type=action_payload.get("action_type", ""),
                        severity=action_payload.get("severity", ""),
                        snapshots=", ".join(action_payload.get("snapshot_ids") or []) or "-",
                        reason=action_payload.get("reason", ""),
                    )
                )
            lines.append("")
        replacements = list(payload.get("candidate_authoritative_replacements") or [])[:10]
        if replacements:
            lines.append("### Candidate Payload-Backed Sources")
            lines.append("")
            lines.append(
                "| snapshot | classification | candidates | profiles | reusable shards | local path exists | source path exists |"
            )
            lines.append("| --- | --- | ---: | ---: | ---: | --- | --- |")
            for replacement in replacements:
                replacement_payload = dict(replacement or {})
                lines.append(
                    "| {snapshot_id} | `{classification}` | {candidates} | {profiles} | {shards} | `{local_path_exists}` | `{source_path_exists}` |".format(
                        snapshot_id=replacement_payload.get("snapshot_id", ""),
                        classification=replacement_payload.get("classification", ""),
                        candidates=replacement_payload.get("candidate_count", 0),
                        profiles=replacement_payload.get("profile_detail_count", 0),
                        shards=replacement_payload.get("reusable_shard_count", 0),
                        local_path_exists=bool(replacement_payload.get("local_path_exists")),
                        source_path_exists=bool(replacement_payload.get("source_path_exists")),
                    )
                )
            lines.append("")
        archive_items = list(archive_plan.get("candidates") or [])[:20]
        if archive_items:
            lines.append("### Archive Candidates")
            lines.append("")
            lines.append("| snapshot | decision | overlap | candidates | profiles | reason |")
            lines.append("| --- | --- | --- | ---: | ---: | --- |")
            for item in archive_items:
                item_payload = dict(item or {})
                lines.append(
                    "| {snapshot_id} | `{decision}` | `{overlap}` | {candidates} | {profiles} | {reason} |".format(
                        snapshot_id=item_payload.get("snapshot_id", ""),
                        decision=item_payload.get("decision", ""),
                        overlap=item_payload.get("overlap_status", ""),
                        candidates=item_payload.get("candidate_count", 0),
                        profiles=item_payload.get("profile_detail_count", 0),
                        reason=item_payload.get("reason", ""),
                    )
                )
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _build_company_plan(*, company: dict[str, Any], runtime_dir: Path) -> dict[str, Any]:
    snapshots = [dict(snapshot or {}) for snapshot in list(company.get("snapshots") or []) if isinstance(snapshot, dict)]
    reference_state = _reference_state(company)
    archive_plan = _archive_plan(snapshots)
    preserve_snapshots = _preserve_snapshots(company=company, snapshots=snapshots)
    replacements = _candidate_authoritative_replacements(snapshots=snapshots, runtime_dir=runtime_dir)
    required_actions = _required_actions(
        reference_state=reference_state,
        archive_plan=archive_plan,
        preserve_snapshots=preserve_snapshots,
        replacements=replacements,
    )
    if reference_state["missing_reference_snapshot_ids"]:
        status = "blocked_missing_reference_identity"
        next_gate = "restore_or_rebuild_reference_payloads_then_rerun_overlap"
    elif archive_plan["archive_ready_snapshot_count"]:
        status = "ready_for_cold_backup_manifest_review"
        next_gate = "create_cold_backup_manifest_then_apply_archive_exclusion"
    elif archive_plan["archive_candidate_count"]:
        status = "blocked_overlap_review"
        next_gate = "resolve_overlap_review_states"
    else:
        status = "no_archive_candidates"
        next_gate = "no_archive_action_required"
    return {
        "company_key": _normalize_text(company.get("company_key")),
        "target_company": _normalize_text(company.get("target_company")),
        "collection_id": _normalize_text(company.get("collection_id")),
        "asset_view": _normalize_text(company.get("asset_view")) or "canonical_merged",
        "status": status,
        "reference_state": reference_state,
        "preserve_snapshots": preserve_snapshots,
        "candidate_authoritative_replacements": replacements,
        "archive_plan": archive_plan,
        "required_actions": required_actions,
        "next_gate": next_gate,
    }


def _reference_state(company: dict[str, Any]) -> dict[str, Any]:
    overlap = dict(company.get("overlap_subsumption") or {})
    reference_loads = [dict(item or {}) for item in list(overlap.get("reference_loads") or []) if isinstance(item, dict)]
    missing_ids = [
        _normalize_text(item.get("snapshot_id"))
        for item in reference_loads
        if _normalize_text(item.get("snapshot_id"))
        and (_normalize_text(item.get("status")) != "loaded" or _safe_int(item.get("identity_count")) <= 0)
    ]
    return {
        "authoritative_snapshot_ids": _dedupe_strings(company.get("authoritative_snapshot_ids") or []),
        "selected_snapshot_ids": _dedupe_strings(company.get("selected_snapshot_ids") or []),
        "latest_local_snapshot_ids": _dedupe_strings(company.get("latest_local_snapshot_ids") or []),
        "reference_snapshot_ids": _dedupe_strings(overlap.get("reference_snapshot_ids") or []),
        "reference_identity_count": _safe_int(overlap.get("reference_identity_count")),
        "reference_loads": reference_loads,
        "missing_reference_snapshot_ids": missing_ids,
    }


def _archive_plan(snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = [_archive_candidate_item(snapshot) for snapshot in snapshots if _is_archive_candidate(snapshot)]
    decisions = Counter(item["decision"] for item in candidates)
    overlap_statuses = Counter(item["overlap_status"] or "not_evaluated" for item in candidates)
    return {
        "archive_candidate_count": len(candidates),
        "archive_ready_snapshot_count": decisions.get("archive_ready_for_cold_backup_review", 0),
        "blocked_archive_candidate_count": len(candidates) - decisions.get("archive_ready_for_cold_backup_review", 0),
        "decisions": dict(sorted(decisions.items())),
        "overlap_statuses": dict(sorted(overlap_statuses.items())),
        "candidates": candidates,
    }


def _archive_candidate_item(snapshot: dict[str, Any]) -> dict[str, Any]:
    registry = dict(snapshot.get("registry") or {})
    overlap = dict(snapshot.get("overlap_subsumption") or {})
    blockers = list(snapshot.get("deletion_blockers") or [])
    overlap_status = _normalize_text(overlap.get("status"))
    if blockers:
        decision = "blocked_dependency"
        reason = "deletion_blockers_present"
    elif overlap_status == SUBSUMED_STATUS and bool(snapshot.get("archive_ready")):
        decision = "archive_ready_for_cold_backup_review"
        reason = "overlap_subsumed_by_reference"
    elif overlap_status == "review_reference_identity_missing":
        decision = "blocked_missing_reference_identity"
        reason = "reference_identity_missing"
    elif overlap_status:
        decision = "blocked_overlap_review"
        reason = overlap_status
    else:
        decision = "blocked_overlap_not_evaluated"
        reason = "overlap_evidence_missing"
    return {
        "snapshot_id": _normalize_text(snapshot.get("snapshot_id")),
        "decision": decision,
        "reason": reason,
        "classification": _normalize_text(snapshot.get("classification")),
        "candidate_count": _safe_int(registry.get("candidate_count")),
        "profile_detail_count": _safe_int(registry.get("profile_detail_count")),
        "overlap_status": overlap_status,
        "overlap_ratio": overlap.get("overlap_ratio", 0.0),
        "unique_count": _safe_int(overlap.get("unique_count")),
        "deletion_blockers": blockers,
    }


def _preserve_snapshots(*, company: dict[str, Any], snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    authoritative_ids = set(_dedupe_strings(company.get("authoritative_snapshot_ids") or []))
    selected_ids = set(_dedupe_strings(company.get("selected_snapshot_ids") or []))
    latest_ids = set(_dedupe_strings(company.get("latest_local_snapshot_ids") or []))
    reusable_ids = {
        _normalize_text(snapshot.get("snapshot_id"))
        for snapshot in snapshots
        if _safe_int(dict(snapshot.get("shards") or {}).get("reusable_shard_count")) > 0
    }
    local_only_ids = {
        _normalize_text(snapshot.get("snapshot_id"))
        for snapshot in snapshots
        if _normalize_text(snapshot.get("classification")) == REVIEW_LOCAL_ONLY_CLASSIFICATION
    }
    return {
        "authoritative_registry_sources": sorted(item for item in authoritative_ids if item),
        "selected_source_snapshots": sorted(item for item in selected_ids if item),
        "latest_pointer_snapshots": sorted(item for item in latest_ids if item),
        "reusable_shard_sources": sorted(item for item in reusable_ids if item),
        "local_only_review_snapshots": sorted(item for item in local_only_ids if item),
    }


def _candidate_authoritative_replacements(*, snapshots: list[dict[str, Any]], runtime_dir: Path) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for snapshot in snapshots:
        registry = dict(snapshot.get("registry") or {})
        local_snapshot = dict(snapshot.get("local_snapshot") or {})
        if not registry.get("present") or not local_snapshot.get("present"):
            continue
        snapshot_id = _normalize_text(snapshot.get("snapshot_id"))
        if not snapshot_id:
            continue
        classification = _normalize_text(snapshot.get("classification"))
        if classification == REVIEW_LOCAL_ONLY_CLASSIFICATION:
            continue
        source_path = _normalize_text(registry.get("source_path"))
        local_path = _normalize_text(local_snapshot.get("path"))
        candidates.append(
            {
                "snapshot_id": snapshot_id,
                "classification": classification,
                "candidate_count": _safe_int(registry.get("candidate_count")),
                "profile_detail_count": _safe_int(registry.get("profile_detail_count")),
                "reusable_shard_count": _safe_int(dict(snapshot.get("shards") or {}).get("reusable_shard_count")),
                "local_path": local_path,
                "local_path_exists": _path_exists(local_path, runtime_dir=runtime_dir),
                "source_path": source_path,
                "source_path_exists": _path_exists(source_path, runtime_dir=runtime_dir),
                "latest_pointer": _normalize_text(local_snapshot.get("latest_snapshot_id")) == snapshot_id,
                "deletion_blocker_types": [
                    _normalize_text(dict(blocker or {}).get("type"))
                    for blocker in list(snapshot.get("deletion_blockers") or [])
                    if _normalize_text(dict(blocker or {}).get("type"))
                ],
                "promotion_requires": [
                    "manual_review",
                    "payload_count_verification",
                    "new_authoritative_projection_or_pointer_publication",
                    "rerun_overlap_before_archival",
                ],
            }
        )
    return sorted(
        candidates,
        key=lambda item: (
            _safe_int(item.get("profile_detail_count")),
            _safe_int(item.get("candidate_count")),
            _safe_int(item.get("reusable_shard_count")),
            1 if item.get("latest_pointer") else 0,
            _normalize_text(item.get("snapshot_id")),
        ),
        reverse=True,
    )


def _required_actions(
    *,
    reference_state: dict[str, Any],
    archive_plan: dict[str, Any],
    preserve_snapshots: dict[str, Any],
    replacements: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    missing_references = list(reference_state.get("missing_reference_snapshot_ids") or [])
    if missing_references:
        actions.append(
            {
                "action_type": "restore_or_rebuild_reference_snapshots",
                "severity": "hard",
                "reason": "reference_identity_missing",
                "snapshot_ids": missing_references,
                "description": (
                    "Active authoritative/source references must have local payload-backed identity before "
                    "duplicate snapshots can be archived."
                ),
            }
        )
        actions.append(
            {
                "action_type": "block_archive_until_reference_overlap_proves_subsumption",
                "severity": "hard",
                "reason": "missing_reference_identity_blocks_overlap",
                "snapshot_ids": [item.get("snapshot_id") for item in list(archive_plan.get("candidates") or [])],
            }
        )
    if replacements:
        actions.append(
            {
                "action_type": "review_payload_backed_authoritative_source_candidates",
                "severity": "review",
                "reason": "local_payload_candidates_available",
                "snapshot_ids": [item.get("snapshot_id") for item in replacements[:5]],
                "description": (
                    "These are candidate sources only. Do not switch registry pointers without payload-count "
                    "verification, projection publication, and a rerun of W5 overlap."
                ),
            }
        )
    reusable = list(preserve_snapshots.get("reusable_shard_sources") or [])
    if reusable:
        actions.append(
            {
                "action_type": "preserve_reusable_shard_sources",
                "severity": "hard",
                "reason": "shard_registry_reuse_dependency",
                "snapshot_ids": reusable,
            }
        )
    local_only = list(preserve_snapshots.get("local_only_review_snapshots") or [])
    if local_only:
        actions.append(
            {
                "action_type": "review_local_only_snapshots",
                "severity": "review",
                "reason": "local_payload_without_registry_row",
                "snapshot_ids": local_only,
            }
        )
    return actions


def _is_archive_candidate(snapshot: dict[str, Any]) -> bool:
    return _normalize_text(snapshot.get("classification")) == ARCHIVE_CANDIDATE_CLASSIFICATION


def _path_exists(value: str, *, runtime_dir: Path) -> bool:
    text = _normalize_text(value)
    if not text:
        return False
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = runtime_dir / path
    return path.exists()


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _safe_int(value: Any) -> int:
    try:
        if value in {None, ""}:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _dedupe_strings(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    result: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        text = _normalize_text(value)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result
