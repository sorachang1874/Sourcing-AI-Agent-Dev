from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .asset_paths import load_company_snapshot_json
from .person_identity import (
    build_person_summary_view,
    resolve_candidate_identity_key,
    resolve_person_identity_key,
    resolve_profile_url_key,
)
from .serving_projection_writer import ServingProjectionWriter
from .storage import ControlPlaneStore

CONTRACT_VERSION = "asset_consolidation_repair_apply_v1"
WRITER_ID = "asset_consolidation_repair_apply_v1"
_BLOCKING_PROMOTION_RISKS = {"registry_payload_count_mismatch"}


def apply_asset_consolidation_repair(
    *,
    proposal: dict[str, Any],
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    selections: dict[str, str],
    apply: bool = False,
    manual_review_accepted: bool = False,
    source_proposal_path: str = "",
    writer_id: str = WRITER_ID,
) -> dict[str, Any]:
    """Dry-run or apply W5c.3 authoritative projection repair.

    This executor intentionally publishes only canonical collection projection
    records and pointers. It never edits historical snapshot files and never
    rewrites organization asset registry rows.
    """

    runtime_root = Path(runtime_dir).expanduser().resolve()
    normalized_selections = _normalize_selections(selections)
    writer = ServingProjectionWriter(store, writer_id=writer_id)
    companies = [
        _plan_or_apply_company(
            company=dict(company or {}),
            runtime_dir=runtime_root,
            store=store,
            writer=writer,
            selected_snapshot_id=normalized_selections.get(_company_selection_key(dict(company or {})), ""),
            apply=bool(apply),
            manual_review_accepted=bool(manual_review_accepted),
            source_proposal=proposal,
            source_proposal_path=source_proposal_path,
            writer_id=writer_id,
        )
        for company in list(proposal.get("companies") or [])
        if isinstance(company, dict) and _company_selection_key(dict(company or {})) in normalized_selections
    ]
    missing_selection_keys = sorted(set(normalized_selections) - {_company_selection_key(dict(item or {})) for item in list(proposal.get("companies") or []) if isinstance(item, dict)})
    for missing_key in missing_selection_keys:
        companies.append(
            {
                "company_key": missing_key,
                "collection_id": f"company:{missing_key}" if missing_key else "",
                "status": "blocked_company_not_found_in_proposal",
                "apply": bool(apply),
                "selected_candidate_snapshot_id": normalized_selections.get(missing_key, ""),
                "blocking_reasons": ["company_not_found_in_proposal"],
                "member_count": 0,
                "applied": False,
            }
        )
    summary = {
        "requested_company_count": len(normalized_selections),
        "planned_company_count": len(companies),
        "ready_company_count": sum(1 for item in companies if item.get("status") in {"dry_run_ready", "applied"}),
        "applied_company_count": sum(1 for item in companies if bool(item.get("applied"))),
        "blocked_company_count": sum(1 for item in companies if str(item.get("status") or "").startswith("blocked")),
        "planned_member_count": sum(int(item.get("member_count") or 0) for item in companies),
    }
    if summary["blocked_company_count"]:
        status = "blocked"
    elif apply:
        status = "applied"
    else:
        status = "dry_run_ready"
    return {
        "status": status,
        "contract_version": CONTRACT_VERSION,
        "apply": bool(apply),
        "read_only": not bool(apply),
        "manual_review_accepted": bool(manual_review_accepted),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_dir": str(runtime_root),
        "source_proposal_contract_version": str(proposal.get("contract_version") or ""),
        "source_proposal_generated_at": str(proposal.get("generated_at") or ""),
        "source_proposal_path": str(source_proposal_path or "").strip(),
        "summary": summary,
        "companies": companies,
    }


def render_asset_consolidation_repair_apply_markdown(report: dict[str, Any]) -> str:
    summary = dict(report.get("summary") or {})
    lines = [
        "# Asset Consolidation Repair Apply Report",
        "",
        f"- Status: `{report.get('status')}`",
        f"- Apply: `{bool(report.get('apply'))}`",
        f"- Read-only: `{bool(report.get('read_only'))}`",
        f"- Manual review accepted: `{bool(report.get('manual_review_accepted'))}`",
        f"- Generated at: `{report.get('generated_at')}`",
        f"- Requested companies: `{summary.get('requested_company_count', 0)}`",
        f"- Ready companies: `{summary.get('ready_company_count', 0)}`",
        f"- Applied companies: `{summary.get('applied_company_count', 0)}`",
        f"- Blocked companies: `{summary.get('blocked_company_count', 0)}`",
        f"- Planned members: `{summary.get('planned_member_count', 0)}`",
        "",
    ]
    for company in list(report.get("companies") or []):
        payload = dict(company or {})
        lines.extend(
            [
                f"## {payload.get('target_company') or payload.get('company_key')}",
                "",
                f"- Status: `{payload.get('status')}`",
                f"- Collection: `{payload.get('collection_id')}`",
                f"- Candidate snapshot: `{payload.get('selected_candidate_snapshot_id')}`",
                f"- Planned projection: `{payload.get('planned_projection_id')}`",
                f"- Previous projection: `{dict(payload.get('previous_pointer') or {}).get('active_projection_id') or '-'}`",
                f"- Active projection: `{dict(payload.get('pointer') or {}).get('active_projection_id') or '-'}`",
                f"- Members: `{payload.get('member_count', 0)}`",
                "",
            ]
        )
        blockers = list(payload.get("blocking_reasons") or [])
        if blockers:
            lines.append("### Blocking Reasons")
            lines.append("")
            for blocker in blockers:
                lines.append(f"- `{blocker}`")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _plan_or_apply_company(
    *,
    company: dict[str, Any],
    runtime_dir: Path,
    store: ControlPlaneStore,
    writer: ServingProjectionWriter,
    selected_snapshot_id: str,
    apply: bool,
    manual_review_accepted: bool,
    source_proposal: dict[str, Any],
    source_proposal_path: str,
    writer_id: str,
) -> dict[str, Any]:
    company_key = _normalize_text(company.get("company_key")) or _collection_key(company.get("collection_id"))
    target_company = _normalize_text(company.get("target_company")) or company_key
    collection_id = _normalize_text(company.get("collection_id")) or f"company:{company_key}"
    candidate = _select_candidate(company=company, snapshot_id=selected_snapshot_id)
    previous_pointer = store.get_collection_authoritative_pointer(collection_id)
    base = {
        "company_key": company_key,
        "target_company": target_company,
        "collection_id": collection_id,
        "apply": bool(apply),
        "selected_candidate_snapshot_id": selected_snapshot_id,
        "previous_pointer": previous_pointer,
        "applied": False,
    }
    blockers = _candidate_blockers(company=company, candidate=candidate)
    if apply and not manual_review_accepted:
        blockers.append("manual_review_not_accepted")
    if blockers:
        return {
            **base,
            "status": "blocked_pre_apply_gates",
            "blocking_reasons": blockers,
            "planned_projection_id": "",
            "candidate_payload_path": str(candidate.get("candidate_payload_path") or "") if candidate else "",
            "member_count": 0,
        }
    payload_path = Path(str(candidate.get("candidate_payload_path") or "")).expanduser()
    if not payload_path.is_absolute():
        payload_path = (runtime_dir / payload_path).resolve()
    payload = load_company_snapshot_json(payload_path)
    members = _projection_members_from_payload(
        payload=payload,
        source_snapshot_id=str(candidate.get("snapshot_id") or selected_snapshot_id),
        target_company=target_company,
        collection_id=collection_id,
    )
    member_count = len(members)
    if member_count <= 0:
        return {
            **base,
            "status": "blocked_pre_apply_gates",
            "blocking_reasons": ["candidate_payload_has_no_projectable_members"],
            "planned_projection_id": "",
            "candidate_payload_path": str(payload_path),
            "member_count": 0,
        }
    projection_id = _planned_projection_id(
        collection_id=collection_id,
        snapshot_id=str(candidate.get("snapshot_id") or selected_snapshot_id),
        member_count=member_count,
    )
    publish_payload = _publish_payload(
        company=company,
        candidate=candidate,
        payload_path=payload_path,
        projection_id=projection_id,
        collection_id=collection_id,
        member_count=member_count,
        source_proposal=source_proposal,
        source_proposal_path=source_proposal_path,
        writer_id=writer_id,
    )
    planned = {
        **base,
        "status": "dry_run_ready",
        "blocking_reasons": [],
        "planned_projection_id": projection_id,
        "active_collection_version": str(candidate.get("snapshot_id") or selected_snapshot_id),
        "candidate_payload_path": str(payload_path),
        "candidate_payload_source": str(candidate.get("candidate_payload_source") or ""),
        "member_count": member_count,
        "counts": dict(publish_payload.get("counts") or {}),
        "scope_spec": dict(publish_payload.get("scope_spec") or {}),
        "readiness": dict(publish_payload.get("readiness") or {}),
        "provenance": dict(publish_payload.get("provenance") or {}),
        "metadata": dict(publish_payload.get("metadata") or {}),
    }
    if not apply:
        return planned
    result = writer.publish_collection_authoritative_projection(
        collection_id=collection_id,
        active_collection_version=str(candidate.get("snapshot_id") or selected_snapshot_id),
        projection_id=projection_id,
        members=members,
        replace_members=True,
        scope_label=str(publish_payload.get("scope_label") or ""),
        scope_spec=dict(publish_payload.get("scope_spec") or {}),
        counts=dict(publish_payload.get("counts") or {}),
        readiness=dict(publish_payload.get("readiness") or {}),
        provenance=dict(publish_payload.get("provenance") or {}),
        metadata=dict(publish_payload.get("metadata") or {}),
        state="serving",
    )
    return {
        **planned,
        "status": "applied",
        "applied": True,
        "projection": dict(result.get("projection") or {}),
        "pointer": dict(result.get("pointer") or {}),
        "member_count": int(result.get("member_count") or member_count),
    }


def _publish_payload(
    *,
    company: dict[str, Any],
    candidate: dict[str, Any],
    payload_path: Path,
    projection_id: str,
    collection_id: str,
    member_count: int,
    source_proposal: dict[str, Any],
    source_proposal_path: str,
    writer_id: str,
) -> dict[str, Any]:
    snapshot_id = _normalize_text(candidate.get("snapshot_id"))
    missing_reference_ids = [
        _normalize_text(item) for item in list(company.get("missing_reference_snapshot_ids") or []) if _normalize_text(item)
    ]
    counts = {
        "result_count": member_count,
        "candidate_count": member_count,
        "member_count": member_count,
        "visible_member_count": member_count,
        "source_payload_candidate_count": _safe_int(candidate.get("payload_candidate_count")),
        "source_identity_count": _safe_int(candidate.get("identity_count")),
        "count_scope": "exact_projection",
    }
    return {
        "projection_id": projection_id,
        "scope_label": f"{company.get('target_company') or company.get('company_key')} authoritative asset repair",
        "scope_spec": {
            "target_scope": "full_company_asset",
            "repair_source": "w5c_asset_consolidation_repair",
            "source_snapshot_id": snapshot_id,
            "missing_reference_snapshot_ids": missing_reference_ids,
            "candidate_payload_source": _normalize_text(candidate.get("candidate_payload_source")),
            "projection_membership_source": "candidate_documents_payload",
        },
        "counts": counts,
        "readiness": {
            "member_rows": "complete",
            "profile": "source_payload",
            "card": "source_payload",
            "raw_profile_index": "pending_rebuild",
            "evidence_index": "pending_rebuild",
            "post_apply_required_gate": "rerun_w5b_overlap",
        },
        "provenance": {
            "phase": "W5c.3",
            "source_proposal_contract_version": _normalize_text(source_proposal.get("contract_version")),
            "source_proposal_generated_at": _normalize_text(source_proposal.get("generated_at")),
            "source_proposal_path": _normalize_text(source_proposal_path),
            "source_snapshot_id": snapshot_id,
            "candidate_payload_path": str(payload_path),
            "missing_reference_snapshot_ids": missing_reference_ids,
            "promotion_risks": list(candidate.get("promotion_risks") or []),
        },
        "metadata": {
            "writer_id": writer_id,
            "repair_phase": "W5c.3",
            "manual_review_required": True,
            "manual_review_accepted": True,
            "non_destructive": True,
            "next_required_gate": "rerun_w5b_overlap_before_archive_manifest",
            "historical_snapshots_mutated": False,
            "registry_rows_mutated": False,
            "candidate_payload_hash": _file_sha256(payload_path),
        },
    }


def _projection_members_from_payload(
    *,
    payload: dict[str, Any],
    source_snapshot_id: str,
    target_company: str,
    collection_id: str,
) -> list[dict[str, Any]]:
    members: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw_candidate in enumerate(list(payload.get("candidates") or []), start=1):
        if not isinstance(raw_candidate, dict):
            continue
        candidate = dict(raw_candidate)
        public_summary = dict(candidate.get("public_summary") or {})
        merged_summary_source = {**candidate, **public_summary}
        profile_key = resolve_profile_url_key(
            candidate.get("profile_url_key"),
            public_summary.get("profile_url_key"),
            candidate.get("linkedin_url"),
            public_summary.get("linkedin_url"),
            candidate.get("profile_url"),
            public_summary.get("profile_url"),
        )
        candidate_id = _normalize_text(candidate.get("candidate_id") or candidate.get("id") or public_summary.get("candidate_id"))
        person_key = resolve_person_identity_key(
            person_identity_key=_normalize_text(candidate.get("person_identity_key") or public_summary.get("person_identity_key")),
            profile_url_key=profile_key,
            linkedin_url=_normalize_text(candidate.get("linkedin_url") or public_summary.get("linkedin_url")),
            candidate_identity_key=_normalize_text(candidate.get("candidate_identity_key") or public_summary.get("candidate_identity_key")),
            candidate_id=candidate_id,
        )
        candidate_key = resolve_candidate_identity_key(
            candidate_identity_key=_normalize_text(candidate.get("candidate_identity_key") or public_summary.get("candidate_identity_key")),
            person_identity_key=person_key,
            profile_url_key=profile_key,
            linkedin_url=_normalize_text(candidate.get("linkedin_url") or public_summary.get("linkedin_url")),
            candidate_id=candidate_id,
        )
        if not candidate_key or candidate_key in seen:
            continue
        seen.add(candidate_key)
        summary = build_person_summary_view(
            merged_summary_source,
            candidate_id=candidate_id,
            profile_url_key=profile_key,
            linkedin_url=_normalize_text(candidate.get("linkedin_url") or public_summary.get("linkedin_url")),
            person_identity_key=person_key,
            source_projection_id="",
            source_run_id="",
        )
        members.append(
            {
                "candidate_identity_key": candidate_key,
                "person_identity_key": person_key or candidate_key,
                "profile_url_key": profile_key,
                "candidate_id": candidate_id,
                "rank_index": index,
                "rank_key": f"{index:08d}:{candidate_key}",
                "lane": _normalize_text(candidate.get("lane") or public_summary.get("lane")),
                "employment_scope": _normalize_text(
                    candidate.get("employment_scope")
                    or public_summary.get("employment_scope")
                    or public_summary.get("employment_status")
                ),
                "source_shard_key": _normalize_text(candidate.get("source_shard_key") or public_summary.get("source_shard_key")),
                "row_readiness": "ready",
                "profile_readiness": "ready" if bool(summary.get("has_profile_detail")) else "unknown",
                "card_readiness": "ready",
                "visibility_state": "visible",
                "public_summary": {
                    **summary,
                    "target_company": target_company,
                    "source_collection_id": collection_id,
                    "source_snapshot_id": source_snapshot_id,
                },
                "projection_metrics": {
                    "source_snapshot_id": source_snapshot_id,
                    "repair_phase": "W5c.3",
                },
                "provenance": {
                    "source_snapshot_id": source_snapshot_id,
                    "source_payload": "candidate_documents",
                    "source_rank_index": index,
                },
                "metadata": {
                    "repair_source": "w5c_asset_consolidation_repair",
                    "raw_candidate_payload_not_copied": True,
                },
            }
        )
    return members


def _candidate_blockers(*, company: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if not candidate:
        blockers.append("candidate_snapshot_not_found_in_verified_candidates")
        return blockers
    if _normalize_text(company.get("status")) != "ready_for_manual_authoritative_repair_review":
        blockers.append("company_not_ready_for_manual_authoritative_repair_review")
    if _normalize_text(candidate.get("verification_status")) != "verified_payload_available":
        blockers.append("candidate_verification_status_not_verified_payload_available")
    if bool(candidate.get("truncated")):
        blockers.append("candidate_identity_scan_truncated")
    if _safe_int(candidate.get("identity_count")) <= 0:
        blockers.append("candidate_identity_count_zero")
    if _safe_int(candidate.get("payload_candidate_count")) <= 0:
        blockers.append("candidate_payload_count_zero")
    if _safe_int(candidate.get("registry_payload_candidate_delta")) != 0:
        blockers.append("candidate_registry_payload_count_mismatch")
    risks = {_normalize_text(item) for item in list(candidate.get("promotion_risks") or []) if _normalize_text(item)}
    for risk in sorted(risks & _BLOCKING_PROMOTION_RISKS):
        blockers.append(f"blocking_promotion_risk:{risk}")
    payload_path = Path(str(candidate.get("candidate_payload_path") or "")).expanduser()
    if not str(candidate.get("candidate_payload_path") or "").strip() or not payload_path.exists():
        blockers.append("candidate_payload_path_missing")
    return blockers


def _select_candidate(*, company: dict[str, Any], snapshot_id: str) -> dict[str, Any]:
    normalized_snapshot_id = _normalize_text(snapshot_id)
    for candidate in list(company.get("verified_candidates") or []):
        payload = dict(candidate or {})
        if _normalize_text(payload.get("snapshot_id")) == normalized_snapshot_id:
            return payload
    return {}


def _normalize_selections(selections: dict[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for raw_company, raw_snapshot_id in dict(selections or {}).items():
        company_key = _normalize_company_key(raw_company)
        snapshot_id = _normalize_text(raw_snapshot_id)
        if company_key and snapshot_id:
            normalized[company_key] = snapshot_id
    return normalized


def _company_selection_key(company: dict[str, Any]) -> str:
    return _normalize_company_key(company.get("company_key") or _collection_key(company.get("collection_id")) or company.get("target_company"))


def _collection_key(collection_id: Any) -> str:
    normalized = _normalize_text(collection_id)
    return normalized.removeprefix("company:") if normalized.startswith("company:") else normalized


def _planned_projection_id(*, collection_id: str, snapshot_id: str, member_count: int) -> str:
    digest = hashlib.sha256(f"{collection_id}|{snapshot_id}|{member_count}|{CONTRACT_VERSION}".encode("utf-8")).hexdigest()
    return f"proj_assetrepair_{digest[:24]}"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return ""
    return f"sha256:{digest.hexdigest()}"


def _normalize_company_key(value: Any) -> str:
    normalized = _normalize_text(value).lower()
    normalized = normalized.removeprefix("company:")
    normalized = normalized.replace("_", "-").replace("/", "-").replace(" ", "-")
    return "-".join(part for part in normalized.split("-") if part)


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _safe_int(value: Any) -> int:
    try:
        if value in {None, ""}:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0
