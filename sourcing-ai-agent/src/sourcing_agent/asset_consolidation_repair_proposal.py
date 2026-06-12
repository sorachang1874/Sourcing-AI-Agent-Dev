from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .asset_paths import load_company_snapshot_json
from .linkedin_url_normalization import normalize_linkedin_profile_url_key
from .person_identity import resolve_candidate_identity_key, resolve_profile_url_key

_CANDIDATE_PAYLOAD_RELATIVE_PATHS = (
    Path("normalized_artifacts") / "materialized_candidate_documents.json",
    Path("candidate_documents.json"),
)


def build_asset_consolidation_repair_proposal(
    *,
    plan: dict[str, Any],
    runtime_dir: str | Path,
    candidate_limit_per_company: int = 5,
    identity_candidate_limit: int = 50_000,
) -> dict[str, Any]:
    """Verify local payload-backed authoritative source candidates without mutating state."""

    runtime_root = Path(runtime_dir).expanduser().resolve()
    normalized_candidate_limit = max(1, int(candidate_limit_per_company or 5))
    normalized_identity_limit = max(1, int(identity_candidate_limit or 50_000))
    companies = [
        _build_company_repair_proposal(
            company=dict(company or {}),
            runtime_dir=runtime_root,
            candidate_limit=normalized_candidate_limit,
            identity_candidate_limit=normalized_identity_limit,
        )
        for company in list(plan.get("companies") or [])
        if isinstance(company, dict)
    ]
    summary = {
        "company_count": len(companies),
        "missing_reference_company_count": sum(
            1 for company in companies if company.get("missing_reference_snapshot_ids")
        ),
        "proposal_ready_company_count": sum(
            1 for company in companies if company.get("status") == "ready_for_manual_authoritative_repair_review"
        ),
        "blocked_company_count": sum(1 for company in companies if str(company.get("status") or "").startswith("blocked")),
        "verified_candidate_count": sum(
            len(
                [
                    candidate
                    for candidate in list(company.get("verified_candidates") or [])
                    if candidate.get("verification_status") == "verified_payload_available"
                ]
            )
            for company in companies
        ),
        "recommended_candidate_count": sum(1 for company in companies if company.get("recommended_candidate")),
    }
    if summary["proposal_ready_company_count"]:
        status = "ready_for_manual_authoritative_repair_review"
    elif summary["blocked_company_count"]:
        status = "blocked_no_verified_payload_candidate"
    else:
        status = "no_authoritative_repair_required"
    return {
        "status": status,
        "contract_version": "asset_consolidation_repair_proposal_v1",
        "read_only": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_plan_contract_version": _normalize_text(plan.get("contract_version")),
        "source_plan_generated_at": _normalize_text(plan.get("generated_at")),
        "candidate_limit_per_company": normalized_candidate_limit,
        "identity_candidate_limit": normalized_identity_limit,
        "summary": summary,
        "companies": companies,
    }


def render_asset_consolidation_repair_proposal_markdown(proposal: dict[str, Any]) -> str:
    summary = dict(proposal.get("summary") or {})
    lines = [
        "# Asset Consolidation Repair Proposal",
        "",
        f"- Status: `{proposal.get('status')}`",
        f"- Read-only: `{bool(proposal.get('read_only'))}`",
        f"- Generated at: `{proposal.get('generated_at')}`",
        f"- Companies: `{summary.get('company_count', 0)}`",
        f"- Proposal-ready companies: `{summary.get('proposal_ready_company_count', 0)}`",
        f"- Blocked companies: `{summary.get('blocked_company_count', 0)}`",
        f"- Verified candidates: `{summary.get('verified_candidate_count', 0)}`",
        "",
    ]
    for company in list(proposal.get("companies") or []):
        payload = dict(company or {})
        recommended = dict(payload.get("recommended_candidate") or {})
        lines.extend(
            [
                f"## {payload.get('target_company') or payload.get('company_key')}",
                "",
                f"- Status: `{payload.get('status')}`",
                f"- Missing references: `{', '.join(payload.get('missing_reference_snapshot_ids') or []) or '-'}`",
                f"- Recommended candidate: `{recommended.get('snapshot_id') or '-'}`",
                f"- Recommended action: `{payload.get('recommended_action')}`",
                "",
            ]
        )
        candidates = list(payload.get("verified_candidates") or [])
        if candidates:
            lines.append("| snapshot | verification | payload candidates | identities | registry candidates | registry profiles | risks |")
            lines.append("| --- | --- | ---: | ---: | ---: | ---: | --- |")
            for candidate in candidates:
                candidate_payload = dict(candidate or {})
                lines.append(
                    "| {snapshot_id} | `{status}` | {payload_count} | {identity_count} | {registry_candidates} | {registry_profiles} | {risks} |".format(
                        snapshot_id=candidate_payload.get("snapshot_id", ""),
                        status=candidate_payload.get("verification_status", ""),
                        payload_count=candidate_payload.get("payload_candidate_count", 0),
                        identity_count=candidate_payload.get("identity_count", 0),
                        registry_candidates=candidate_payload.get("registry_candidate_count", 0),
                        registry_profiles=candidate_payload.get("registry_profile_detail_count", 0),
                        risks=", ".join(candidate_payload.get("promotion_risks") or []) or "-",
                    )
                )
            lines.append("")
        gates = list(payload.get("pre_apply_gates") or [])
        if gates:
            lines.append("### Pre-Apply Gates")
            lines.append("")
            for gate in gates:
                lines.append(f"- `{gate}`")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _build_company_repair_proposal(
    *,
    company: dict[str, Any],
    runtime_dir: Path,
    candidate_limit: int,
    identity_candidate_limit: int,
) -> dict[str, Any]:
    missing_reference_ids = [
        _normalize_text(item)
        for item in list(dict(company.get("reference_state") or {}).get("missing_reference_snapshot_ids") or [])
        if _normalize_text(item)
    ]
    candidate_inputs = [
        dict(candidate or {})
        for candidate in list(company.get("candidate_authoritative_replacements") or [])[:candidate_limit]
        if isinstance(candidate, dict)
    ]
    verified_candidates = [
        _verify_repair_candidate(
            candidate=candidate,
            runtime_dir=runtime_dir,
            identity_candidate_limit=identity_candidate_limit,
        )
        for candidate in candidate_inputs
    ]
    eligible = [
        candidate
        for candidate in verified_candidates
        if candidate.get("verification_status") == "verified_payload_available"
        and not candidate.get("truncated")
        and int(candidate.get("identity_count") or 0) > 0
        and int(candidate.get("registry_payload_candidate_delta") or 0) == 0
    ]
    recommended = eligible[0] if eligible else {}
    if missing_reference_ids and recommended:
        status = "ready_for_manual_authoritative_repair_review"
        recommended_action = "publish_new_payload_backed_authoritative_reference_after_manual_review"
    elif missing_reference_ids:
        status = "blocked_no_verified_payload_candidate"
        recommended_action = "restore_missing_reference_payloads_or_build_new_candidate"
    else:
        status = "no_authoritative_repair_required"
        recommended_action = "no_reference_repair_required"
    return {
        "company_key": _normalize_text(company.get("company_key")),
        "target_company": _normalize_text(company.get("target_company")),
        "collection_id": _normalize_text(company.get("collection_id")),
        "status": status,
        "missing_reference_snapshot_ids": missing_reference_ids,
        "verified_candidates": verified_candidates,
        "recommended_candidate": recommended,
        "recommended_action": recommended_action,
        "pre_apply_gates": _pre_apply_gates(has_missing_references=bool(missing_reference_ids), has_recommended=bool(recommended)),
    }


def _verify_repair_candidate(
    *,
    candidate: dict[str, Any],
    runtime_dir: Path,
    identity_candidate_limit: int,
) -> dict[str, Any]:
    local_path = _resolve_path(candidate.get("local_path"), runtime_dir=runtime_dir)
    source_path = _resolve_path(candidate.get("source_path"), runtime_dir=runtime_dir)
    source_snapshot_dir = _snapshot_dir_from_path(source_path, snapshot_id=_normalize_text(candidate.get("snapshot_id")))
    payload_choice = _best_candidate_payload_path([local_path, source_snapshot_dir])
    payload_path = Path(payload_choice.get("path")) if payload_choice.get("path") else None
    base = {
        "snapshot_id": _normalize_text(candidate.get("snapshot_id")),
        "classification": _normalize_text(candidate.get("classification")),
        "registry_candidate_count": _safe_int(candidate.get("candidate_count")),
        "registry_profile_detail_count": _safe_int(candidate.get("profile_detail_count")),
        "reusable_shard_count": _safe_int(candidate.get("reusable_shard_count")),
        "local_path": str(local_path) if local_path is not None else "",
        "local_path_exists": bool(local_path and local_path.exists()),
        "source_path": _normalize_text(candidate.get("source_path")),
        "source_path_exists": bool(candidate.get("source_path_exists")),
        "source_snapshot_dir": str(source_snapshot_dir) if source_snapshot_dir is not None else "",
        "source_snapshot_dir_exists": bool(source_snapshot_dir and source_snapshot_dir.exists()),
        "candidate_payload_path": str(payload_path) if payload_path is not None else "",
        "candidate_payload_source": _normalize_text(payload_choice.get("source")),
        "promotion_risks": _promotion_risks(candidate),
    }
    if payload_path is None:
        registry_delta = _safe_int(candidate.get("candidate_count"))
        return {
            **base,
            "verification_status": "payload_missing",
            "payload_candidate_count": 0,
            "scanned_candidate_count": 0,
            "identity_count": 0,
            "profile_url_identity_count": 0,
            "fallback_identity_count": 0,
            "truncated": False,
            "registry_payload_candidate_delta": registry_delta,
            "promotion_risks": _append_payload_count_risk(
                list(base.get("promotion_risks") or []),
                registry_payload_candidate_delta=registry_delta,
            ),
        }
    payload = load_company_snapshot_json(payload_path)
    candidates = list(payload.get("candidates") or [])
    identity_keys: set[str] = set()
    profile_url_identity_count = 0
    fallback_identity_count = 0
    for item in candidates[:identity_candidate_limit]:
        if not isinstance(item, dict):
            continue
        profile_key = _profile_url_identity_key(item)
        identity_key = f"linkedin:{profile_key}" if profile_key else _fallback_identity_key(item)
        if not identity_key:
            continue
        identity_keys.add(identity_key)
        if profile_key:
            profile_url_identity_count += 1
        else:
            fallback_identity_count += 1
    payload_candidate_count = len(candidates)
    identity_count = len(identity_keys)
    count_delta = _safe_int(candidate.get("candidate_count")) - payload_candidate_count
    if payload_candidate_count <= 0:
        status = "payload_empty"
    elif identity_count <= 0:
        status = "identity_missing"
    elif len(candidates) > identity_candidate_limit:
        status = "identity_scan_truncated"
    else:
        status = "verified_payload_available"
    return {
        **base,
        "verification_status": status,
        "payload_candidate_count": payload_candidate_count,
        "scanned_candidate_count": min(payload_candidate_count, identity_candidate_limit),
        "identity_count": identity_count,
        "profile_url_identity_count": profile_url_identity_count,
        "fallback_identity_count": fallback_identity_count,
        "truncated": len(candidates) > identity_candidate_limit,
        "registry_payload_candidate_delta": count_delta,
        "promotion_risks": _append_payload_count_risk(
            list(base.get("promotion_risks") or []),
            registry_payload_candidate_delta=count_delta,
        ),
        "identity_samples": sorted(identity_keys)[:10],
    }


def _best_candidate_payload_path(snapshot_dirs: list[Path | None]) -> dict[str, Any]:
    choices: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source, snapshot_dir in zip(("local_path", "source_path"), snapshot_dirs, strict=False):
        path = _candidate_payload_path(snapshot_dir)
        if path is None:
            continue
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        payload = load_company_snapshot_json(path)
        choices.append(
            {
                "path": str(path),
                "source": source,
                "candidate_count": len(list(payload.get("candidates") or [])),
            }
        )
    if not choices:
        return {}
    return sorted(choices, key=lambda item: (_safe_int(item.get("candidate_count")), item.get("source") == "source_path"), reverse=True)[0]


def _candidate_payload_path(snapshot_dir: Path | None) -> Path | None:
    if snapshot_dir is None or not snapshot_dir.exists():
        return None
    for relative_path in _CANDIDATE_PAYLOAD_RELATIVE_PATHS:
        path = snapshot_dir / relative_path
        if not path.exists() or not path.is_file():
            continue
        payload = load_company_snapshot_json(path)
        if isinstance(payload.get("candidates"), list):
            return path
    return None


def _snapshot_dir_from_path(path: Path | None, *, snapshot_id: str) -> Path | None:
    if path is None or not snapshot_id:
        return None
    candidate = path if path.is_dir() else path.parent
    for ancestor in [candidate, *candidate.parents]:
        if ancestor.name == snapshot_id:
            return ancestor
    return None


def _profile_url_identity_key(candidate: dict[str, Any]) -> str:
    public_summary = dict(candidate.get("public_summary") or {})
    metadata = dict(candidate.get("metadata") or {})
    return resolve_profile_url_key(
        candidate.get("profile_url_key"),
        public_summary.get("profile_url_key"),
        candidate.get("linkedin_url"),
        public_summary.get("linkedin_url"),
        candidate.get("profile_url"),
        metadata.get("profile_url"),
        metadata.get("linkedin_url"),
        normalize_linkedin_profile_url_key(candidate.get("linkedin_url") or public_summary.get("linkedin_url")),
    )


def _fallback_identity_key(candidate: dict[str, Any]) -> str:
    public_summary = dict(candidate.get("public_summary") or {})
    return resolve_candidate_identity_key(
        candidate_identity_key=_normalize_text(candidate.get("candidate_identity_key")),
        person_identity_key=_normalize_text(candidate.get("person_identity_key")),
        profile_url_key="",
        linkedin_url=_normalize_text(candidate.get("linkedin_url") or public_summary.get("linkedin_url")),
        candidate_id=_normalize_text(candidate.get("candidate_id") or candidate.get("id")),
    )


def _promotion_risks(candidate: dict[str, Any]) -> list[str]:
    risks: list[str] = []
    classification = _normalize_text(candidate.get("classification"))
    if classification == "archive_candidate_no_increment_duplicate":
        risks.append("candidate_currently_classified_archive_candidate")
    if _safe_int(candidate.get("reusable_shard_count")) <= 0:
        risks.append("no_reusable_shard_proof")
    if bool(candidate.get("latest_pointer")):
        risks.append("currently_latest_local_pointer")
    if not bool(candidate.get("source_path_exists")):
        risks.append("registry_source_path_missing_or_stale")
    return risks


def _append_payload_count_risk(risks: list[str], *, registry_payload_candidate_delta: int) -> list[str]:
    result = list(risks)
    if registry_payload_candidate_delta != 0 and "registry_payload_count_mismatch" not in result:
        result.append("registry_payload_count_mismatch")
    return result


def _pre_apply_gates(*, has_missing_references: bool, has_recommended: bool) -> list[str]:
    if not has_missing_references:
        return ["no_reference_repair_required"]
    gates = [
        "manual_review_recommended_candidate_scope_and_counts",
        "publish_new_authoritative_projection_or_registry_pointer",
        "rerun_w5b_overlap_with_repaired_reference_identity",
        "block_archive_until_overlap_subsumed_by_reference",
    ]
    if not has_recommended:
        gates.insert(0, "restore_missing_reference_payloads_or_build_new_candidate")
    return gates


def _resolve_path(value: Any, *, runtime_dir: Path) -> Path | None:
    text = _normalize_text(value)
    if not text:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = runtime_dir / path
    return path


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _safe_int(value: Any) -> int:
    try:
        if value in {None, ""}:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0
