from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from .domain import Candidate, make_evidence_id, merge_candidate, normalize_candidate
from .profile_timeline import normalized_primary_email_metadata, normalized_text_lines

LARGE_ORG_HISTORY_SNAPSHOT_MIN_CANDIDATES = 1000


def load_company_history_snapshots(company_dir: Path, target_company: str) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for snapshot_dir in sorted(path for path in company_dir.iterdir() if path.is_dir()):
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        if not candidate_doc_path.exists():
            continue
        try:
            payload = json.loads(candidate_doc_path.read_text())
        except json.JSONDecodeError:
            continue
        candidates: list[Candidate] = []
        for item in list(payload.get("candidates") or []):
            candidate = candidate_from_payload(item)
            if candidate is None:
                continue
            if normalize_key(candidate.target_company) != normalize_key(target_company):
                continue
            candidates.append(candidate)
        if not candidates:
            continue
        evidence: list[dict[str, Any]] = []
        for item in list(payload.get("evidence") or []):
            evidence_item = evidence_payload_from_item(item)
            if evidence_item is not None:
                evidence.append(evidence_item)
        snapshots.append(
            {
                "snapshot_id": snapshot_dir.name,
                "source_path": str(candidate_doc_path),
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "candidates": candidates,
                "evidence": evidence,
            }
        )
    return snapshots


def select_source_snapshots_for_materialization(
    source_snapshots: list[dict[str, Any]],
    *,
    current_snapshot_id: str,
    preferred_source_snapshot_ids: list[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selection = {
        "mode": "all_history_snapshots",
        "reason": "",
        "selected_snapshot_ids": [str(item.get("snapshot_id") or "").strip() for item in source_snapshots],
        "excluded_snapshot_ids": [],
    }
    if len(source_snapshots) <= 1:
        return source_snapshots, selection

    current_snapshot = next(
        (item for item in source_snapshots if str(item.get("snapshot_id") or "").strip() == current_snapshot_id),
        None,
    )
    if current_snapshot is None:
        selection["reason"] = "current_snapshot_candidate_documents_missing"
        return source_snapshots, selection

    preferred_ids = [
        str(item or "").strip() for item in list(preferred_source_snapshot_ids or []) if str(item or "").strip()
    ]
    if preferred_ids:
        source_index = {
            str(item.get("snapshot_id") or "").strip(): item
            for item in source_snapshots
            if str(item.get("snapshot_id") or "").strip()
        }
        selected_ids: list[str] = []
        for snapshot_ref in [current_snapshot_id, *preferred_ids]:
            if snapshot_ref in source_index and snapshot_ref not in selected_ids:
                selected_ids.append(snapshot_ref)
        if selected_ids:
            selection.update(
                {
                    "mode": "preferred_snapshot_subset",
                    "reason": (
                        "Explicit preferred baseline snapshots were requested for incremental materialization; "
                        "preserve current snapshot plus selected historical baselines."
                    ),
                    "selected_snapshot_ids": selected_ids,
                    "excluded_snapshot_ids": [
                        str(item.get("snapshot_id") or "").strip()
                        for item in source_snapshots
                        if str(item.get("snapshot_id") or "").strip() not in selected_ids
                    ],
                }
            )
            return [source_index[snapshot_ref] for snapshot_ref in selected_ids], selection

    current_candidate_count = int(current_snapshot.get("candidate_count") or 0)
    if current_candidate_count < LARGE_ORG_HISTORY_SNAPSHOT_MIN_CANDIDATES:
        return source_snapshots, selection

    selection.update(
        {
            "mode": "current_snapshot_only_large_org",
            "reason": (
                "Current snapshot candidate_documents already cover a large organization; "
                "skip older snapshot unions and rely on the current snapshot to avoid dirty historical inflation."
            ),
            "selected_snapshot_ids": [current_snapshot_id],
            "excluded_snapshot_ids": [
                str(item.get("snapshot_id") or "").strip()
                for item in source_snapshots
                if str(item.get("snapshot_id") or "").strip() != current_snapshot_id
            ],
        }
    )
    return [current_snapshot], selection


def candidate_from_payload(payload: dict[str, Any]) -> Candidate | None:
    if not isinstance(payload, dict):
        return None
    metadata = dict(payload.get("metadata") or {})
    function_ids = candidate_level_function_ids(
        payload.get("function_ids"),
        metadata.get("function_ids"),
        source_shard_filters=metadata.get("source_shard_filters"),
    )
    if function_ids:
        metadata["function_ids"] = function_ids
    else:
        metadata.pop("function_ids", None)
    if payload.get("experience_lines") and not metadata.get("experience_lines"):
        metadata["experience_lines"] = normalized_text_lines(payload.get("experience_lines"))
    if payload.get("education_lines") and not metadata.get("education_lines"):
        metadata["education_lines"] = normalized_text_lines(payload.get("education_lines"))
    if payload.get("profile_timeline_source") and not metadata.get("profile_timeline_source"):
        metadata["profile_timeline_source"] = str(payload.get("profile_timeline_source") or "").strip()
    if payload.get("profile_timeline_source_path") and not metadata.get("profile_timeline_source_path"):
        metadata["profile_timeline_source_path"] = str(payload.get("profile_timeline_source_path") or "").strip()
    for field_name in (
        "headline",
        "summary",
        "about",
        "profile_location",
        "public_identifier",
        "avatar_url",
        "photo_url",
        "primary_email",
    ):
        if payload.get(field_name) and not metadata.get(field_name):
            metadata[field_name] = str(payload.get(field_name) or "").strip()
    primary_email_metadata = normalized_primary_email_metadata(payload.get("primary_email_metadata"))
    if primary_email_metadata and not normalized_primary_email_metadata(metadata.get("primary_email_metadata")):
        metadata["primary_email_metadata"] = primary_email_metadata
    for field_name in ("languages", "skills"):
        if payload.get(field_name) and not metadata.get(field_name):
            metadata[field_name] = normalized_text_lines(payload.get(field_name))
    record = {
        "candidate_id": str(payload.get("candidate_id") or "").strip(),
        "name_en": str(payload.get("name_en") or "").strip(),
        "name_zh": str(payload.get("name_zh") or "").strip(),
        "display_name": str(payload.get("display_name") or "").strip(),
        "category": str(payload.get("category") or "").strip(),
        "target_company": str(payload.get("target_company") or "").strip(),
        "organization": str(payload.get("organization") or "").strip(),
        "employment_status": str(payload.get("employment_status") or "").strip(),
        "role": str(payload.get("role") or "").strip(),
        "team": str(payload.get("team") or "").strip(),
        "joined_at": str(payload.get("joined_at") or "").strip(),
        "left_at": str(payload.get("left_at") or "").strip(),
        "current_destination": str(payload.get("current_destination") or "").strip(),
        "ethnicity_background": str(payload.get("ethnicity_background") or "").strip(),
        "investment_involvement": str(payload.get("investment_involvement") or "").strip(),
        "focus_areas": str(payload.get("focus_areas") or "").strip(),
        "education": str(payload.get("education") or "").strip(),
        "work_history": str(payload.get("work_history") or "").strip(),
        "notes": str(payload.get("notes") or "").strip(),
        "linkedin_url": str(payload.get("linkedin_url") or "").strip(),
        "media_url": str(payload.get("media_url") or "").strip(),
        "source_dataset": str(payload.get("source_dataset") or "").strip(),
        "source_path": str(payload.get("source_path") or "").strip(),
        "metadata": metadata,
    }
    if not record["candidate_id"] or not record["name_en"] or not record["target_company"]:
        return None
    if not record["display_name"]:
        record["display_name"] = record["name_en"]
    return normalize_candidate(Candidate(**record))


def ingest_materialized_candidate(
    candidate: Candidate,
    *,
    merged_candidates: dict[str, Candidate],
    candidate_aliases: dict[str, str],
    identity_index: dict[str, str],
    prefer_incoming: bool,
    evidence: list[dict[str, Any]] | None = None,
) -> None:
    dedupe_keys = candidate_dedupe_keys(candidate, evidence=evidence)
    canonical_id = ""
    for key in dedupe_keys:
        canonical_id = identity_index.get(key, "")
        if canonical_id:
            break
    canonical_id = canonical_id or candidate.candidate_id
    canonical_candidate = candidate if canonical_id == candidate.candidate_id else _candidate_with_id(candidate, canonical_id)
    existing = merged_candidates.get(canonical_id)
    merged_candidates[canonical_id] = (
        canonical_candidate
        if existing is None
        else merge_candidates(existing, canonical_candidate, prefer_incoming=prefer_incoming)
    )
    candidate_aliases[candidate.candidate_id] = canonical_id
    candidate_aliases.setdefault(canonical_id, canonical_id)
    for key in dedupe_keys:
        identity_index.setdefault(key, canonical_id)


def remap_evidence_candidate(payload: dict[str, Any], candidate_aliases: dict[str, str]) -> dict[str, Any]:
    record = dict(payload)
    candidate_id = str(record.get("candidate_id") or "").strip()
    canonical_id = candidate_aliases.get(candidate_id, candidate_id)
    record["candidate_id"] = canonical_id
    record["evidence_id"] = make_evidence_id(
        canonical_id,
        str(record.get("source_dataset") or record.get("source_type") or "").strip(),
        str(record.get("title") or "").strip(),
        str(record.get("url") or record.get("source_path") or "").strip(),
    )
    return record


def consolidate_materialized_duplicates(
    merged_candidates: dict[str, Candidate],
    merged_evidence: dict[str, dict[str, Any]],
) -> tuple[dict[str, Candidate], dict[str, dict[str, Any]]]:
    evidence_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for evidence in merged_evidence.values():
        candidate_id = str(evidence.get("candidate_id") or "").strip()
        if candidate_id:
            evidence_by_candidate[candidate_id].append(dict(evidence))

    consolidated_candidates: dict[str, Candidate] = {}
    candidate_aliases: dict[str, str] = {}
    identity_index: dict[str, str] = {}
    for candidate in merged_candidates.values():
        ingest_materialized_candidate(
            candidate,
            merged_candidates=consolidated_candidates,
            candidate_aliases=candidate_aliases,
            identity_index=identity_index,
            prefer_incoming=True,
            evidence=evidence_by_candidate.get(candidate.candidate_id, []),
        )

    consolidated_evidence: dict[str, dict[str, Any]] = {}
    for evidence in merged_evidence.values():
        remapped = remap_evidence_candidate(evidence, candidate_aliases)
        consolidated_evidence[evidence_key(remapped)] = remapped
    return consolidated_candidates, consolidated_evidence


def evidence_payload_from_item(payload: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    candidate_id = str(payload.get("candidate_id") or "").strip()
    source_type = str(payload.get("source_type") or "").strip()
    title = str(payload.get("title") or "").strip()
    url = str(payload.get("url") or "").strip()
    source_dataset = str(payload.get("source_dataset") or "").strip()
    source_path = str(payload.get("source_path") or "").strip()
    if not candidate_id or not source_type:
        return None
    return {
        "evidence_id": str(payload.get("evidence_id") or "").strip()
        or make_evidence_id(candidate_id, source_dataset or source_type, title, url or source_path),
        "candidate_id": candidate_id,
        "source_type": source_type,
        "title": title,
        "url": url,
        "summary": str(payload.get("summary") or "").strip(),
        "source_dataset": source_dataset,
        "source_path": source_path,
        "metadata": dict(payload.get("metadata") or {}),
    }


def merge_candidates(existing: Candidate, incoming: Candidate, *, prefer_incoming: bool) -> Candidate:
    existing_score = _materialized_merge_score(existing)
    incoming_score = _materialized_merge_score(incoming)
    if incoming_score > existing_score:
        primary = incoming
        secondary = existing
    elif existing_score > incoming_score:
        primary = existing
        secondary = incoming
    else:
        primary = incoming if prefer_incoming else existing
        secondary = existing if prefer_incoming else incoming
    merged = merge_candidate(primary, secondary)
    authoritative = _select_authoritative_membership_candidate(primary, secondary)
    if authoritative is None:
        return merged
    return _apply_authoritative_membership_fields(merged, authoritative)


def evidence_key(payload: dict[str, Any]) -> str:
    evidence_id = str(payload.get("evidence_id") or "").strip()
    if evidence_id:
        return evidence_id
    return make_evidence_id(
        str(payload.get("candidate_id") or "").strip(),
        str(payload.get("source_dataset") or payload.get("source_type") or "").strip(),
        str(payload.get("title") or "").strip(),
        str(payload.get("url") or payload.get("source_path") or "").strip(),
    )


def build_asset_population_overlay(
    *,
    baseline_candidates: list[Candidate],
    baseline_evidence_lookup: dict[str, list[dict[str, Any]]] | None,
    delta_candidates: list[Candidate],
    delta_evidence_lookup: dict[str, list[dict[str, Any]]] | None,
    member_key_resolver: Any,
) -> dict[str, Any]:
    baseline_evidence_lookup = {
        str(candidate_id or "").strip(): [dict(item) for item in list(items or []) if isinstance(item, dict)]
        for candidate_id, items in dict(baseline_evidence_lookup or {}).items()
        if str(candidate_id or "").strip()
    }
    delta_evidence_lookup = {
        str(candidate_id or "").strip(): [dict(item) for item in list(items or []) if isinstance(item, dict)]
        for candidate_id, items in dict(delta_evidence_lookup or {}).items()
        if str(candidate_id or "").strip()
    }

    def _member_key(candidate: Candidate) -> str:
        resolved = member_key_resolver(candidate)
        return str(resolved or "").strip()

    def _candidate_is_explicit_non_member(candidate: Candidate) -> bool:
        metadata = dict(candidate.metadata or {})
        decision = str(metadata.get("membership_review_decision") or "").strip().lower()
        return bool(
            str(candidate.category or "").strip().lower() == "non_member"
            or decision.endswith("non_member")
            or bool(metadata.get("target_company_mismatch"))
        )

    def _dedupe_evidence_records(
        *records: list[dict[str, Any]],
        candidate_id: str,
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for collection in records:
            for item in list(collection or []):
                if not isinstance(item, dict):
                    continue
                remapped = remap_evidence_candidate(dict(item), {str(item.get("candidate_id") or "").strip(): candidate_id})
                merged[evidence_key(remapped)] = remapped
        return list(merged.values())

    baseline_by_key: dict[str, Candidate] = {}
    baseline_order: list[str] = []
    evidence_by_key: dict[str, list[dict[str, Any]]] = {}
    for candidate in list(baseline_candidates or []):
        member_key = _member_key(candidate)
        if not member_key or member_key in baseline_by_key:
            continue
        baseline_by_key[member_key] = candidate
        baseline_order.append(member_key)
        evidence_by_key[member_key] = list(
            baseline_evidence_lookup.get(str(candidate.candidate_id or "").strip(), [])
        )

    overlay_by_key = dict(baseline_by_key)
    touched_keys: list[str] = []
    touched_candidates: list[Candidate] = []
    added_member_keys: list[str] = []
    updated_member_keys: list[str] = []
    removed_member_keys: list[str] = []

    for delta_candidate in list(delta_candidates or []):
        member_key = _member_key(delta_candidate)
        if not member_key:
            continue
        existing_candidate = overlay_by_key.get(member_key)
        if _candidate_is_explicit_non_member(delta_candidate):
            if existing_candidate is not None:
                overlay_by_key.pop(member_key, None)
                evidence_by_key.pop(member_key, None)
                removed_member_keys.append(member_key)
                if member_key not in touched_keys:
                    touched_keys.append(member_key)
            continue

        baseline_evidence = evidence_by_key.get(member_key, [])
        delta_evidence = list(delta_evidence_lookup.get(str(delta_candidate.candidate_id or "").strip(), []))
        if existing_candidate is None:
            merged_candidate = delta_candidate
            added_member_keys.append(member_key)
        else:
            merged_candidate = merge_candidates(existing_candidate, delta_candidate, prefer_incoming=True)
            if merged_candidate.to_record() != existing_candidate.to_record():
                updated_member_keys.append(member_key)
        overlay_by_key[member_key] = merged_candidate
        evidence_by_key[member_key] = _dedupe_evidence_records(
            baseline_evidence,
            delta_evidence,
            candidate_id=str(merged_candidate.candidate_id or "").strip(),
        )
        if member_key not in touched_keys:
            touched_keys.append(member_key)
            touched_candidates.append(merged_candidate)
        else:
            touched_index = touched_keys.index(member_key)
            if 0 <= touched_index < len(touched_candidates):
                touched_candidates[touched_index] = merged_candidate

    overlay_order = [
        *touched_keys,
        *[member_key for member_key in baseline_order if member_key not in set(touched_keys)],
        *[
            member_key
            for member_key in overlay_by_key
            if member_key not in set(touched_keys) and member_key not in set(baseline_order)
        ],
    ]
    final_candidates: list[Candidate] = []
    final_evidence_lookup: dict[str, list[dict[str, Any]]] = {}
    for member_key in overlay_order:
        candidate = overlay_by_key.get(member_key)
        if candidate is None:
            continue
        final_candidates.append(candidate)
        candidate_id = str(candidate.candidate_id or "").strip()
        if candidate_id:
            final_evidence_lookup[candidate_id] = list(evidence_by_key.get(member_key, []))

    return {
        "candidates": final_candidates,
        "evidence_lookup": final_evidence_lookup,
        "touched_candidates": touched_candidates,
        "touched_member_keys": touched_keys,
        "added_member_keys": added_member_keys,
        "updated_member_keys": updated_member_keys,
        "removed_member_keys": removed_member_keys,
        "delta_candidate_ids": [
            str(candidate.candidate_id or "").strip() for candidate in touched_candidates if str(candidate.candidate_id or "").strip()
        ],
    }


def normalize_string_list(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    normalized: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def candidate_level_function_ids(*sources: Any, source_shard_filters: Any | None = None) -> list[str]:
    values: list[str] = []
    for source in sources:
        values.extend(normalize_string_list(source))
    normalized = normalize_string_list(values)
    if len(normalized) != 1:
        return []
    shard_filters = dict(source_shard_filters or {})
    shard_include = normalize_string_list(shard_filters.get("function_ids"))
    shard_exclude = set(normalize_string_list(shard_filters.get("exclude_function_ids")))
    shard_effective = [item for item in shard_include if item not in shard_exclude]
    if shard_effective and normalized == shard_effective:
        return []
    return normalized


def has_explicit_manual_review_resolution(candidate: Candidate) -> bool:
    metadata = dict(candidate.metadata or {})
    if str(metadata.get("manual_review_artifact_root") or "").strip():
        return True
    if list(metadata.get("manual_review_links") or []):
        return True
    return str(metadata.get("membership_review_decision") or "").strip().lower().startswith("manual_")


def normalize_key(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def candidate_dedupe_keys(candidate: Candidate, *, evidence: list[dict[str, Any]] | None = None) -> list[str]:
    metadata = dict(candidate.metadata or {})
    evidence = evidence or []
    evidence_identifier_inputs: list[Any] = []
    for item in evidence:
        url = str(item.get("url") or "").strip()
        if "linkedin.com/in/" in url.lower():
            evidence_identifier_inputs.append(url)
        metadata_item = dict(item.get("metadata") or {})
        for key in ["profile_url", "linkedin_url", "public_identifier", "username"]:
            value = metadata_item.get(key)
            if value:
                evidence_identifier_inputs.append(value)
    identifiers = linkedin_identifier_set(
        [
            candidate.linkedin_url,
            metadata.get("profile_url"),
            metadata.get("public_identifier"),
            metadata.get("seed_slug"),
            metadata.get("more_profiles"),
            evidence_identifier_inputs,
        ]
    )
    return [f"linkedin:{item}" for item in sorted(identifiers)]


def linkedin_identifier_set(values: list[Any]) -> set[str]:
    identifiers: set[str] = set()
    for value in values:
        _append_linkedin_identifier(identifiers, value)
    return identifiers


def _candidate_with_id(candidate: Candidate, candidate_id: str) -> Candidate:
    record = candidate.to_record()
    record["candidate_id"] = candidate_id
    return Candidate(**record)


def _materialized_merge_score(candidate: Candidate) -> int:
    metadata = dict(candidate.metadata or {})
    score = 0
    status = str(candidate.employment_status or "").strip().lower()
    category = str(candidate.category or "").strip().lower()
    if metadata.get("membership_claim_category"):
        score += 30
    if metadata.get("public_identifier"):
        score += 20
    if metadata.get("profile_url"):
        score += 15
    if status == "current":
        score += 12
    elif status == "former":
        score += 6
    if category == "employee":
        score += 10
    elif category == "former_employee":
        score += 5
    for value in [
        candidate.linkedin_url,
        candidate.role,
        candidate.team,
        candidate.focus_areas,
        candidate.education,
        candidate.work_history,
        candidate.notes,
    ]:
        if str(value or "").strip():
            score += 1
    return score


def _select_authoritative_membership_candidate(*candidates: Candidate) -> Candidate | None:
    ranked = sorted(candidates, key=_authoritative_membership_rank, reverse=True)
    if not ranked:
        return None
    best = ranked[0]
    if _authoritative_membership_rank(best) <= 0:
        return None
    return best


def _authoritative_membership_rank(candidate: Candidate) -> int:
    metadata = dict(candidate.metadata or {})
    score = 0
    if has_explicit_manual_review_resolution(candidate):
        score += 100
    if str(metadata.get("membership_review_decision") or "").strip():
        score += 8
    if candidate.category == "non_member":
        score += 6
    if bool(metadata.get("target_company_mismatch")):
        score += 4
    if bool(metadata.get("membership_review_required")):
        score += 2
    return score


def _apply_authoritative_membership_fields(candidate: Candidate, authoritative: Candidate) -> Candidate:
    record = candidate.to_record()
    source_record = authoritative.to_record()
    metadata = dict(record.get("metadata") or {})
    source_metadata = dict(source_record.get("metadata") or {})

    if has_explicit_manual_review_resolution(authoritative):
        record["category"] = str(source_record.get("category") or "").strip() or record["category"]
        record["employment_status"] = str(source_record.get("employment_status") or "").strip()
        record["organization"] = str(source_record.get("organization") or "").strip()
        if str(source_record.get("role") or "").strip() or authoritative.category == "non_member":
            record["role"] = str(source_record.get("role") or "").strip()
        if str(source_record.get("team") or "").strip():
            record["team"] = str(source_record.get("team") or "").strip()
        if str(source_record.get("linkedin_url") or "").strip() or authoritative.category == "non_member":
            record["linkedin_url"] = str(source_record.get("linkedin_url") or "").strip()
        if str(source_record.get("media_url") or "").strip() or authoritative.category == "non_member":
            record["media_url"] = str(source_record.get("media_url") or "").strip()
        if str(source_record.get("source_path") or "").strip():
            record["source_path"] = str(source_record.get("source_path") or "").strip()
        if str(source_record.get("source_dataset") or "").strip():
            record["source_dataset"] = str(source_record.get("source_dataset") or "").strip()

    decision = str(source_metadata.get("membership_review_decision") or "").strip()
    if not decision and has_explicit_manual_review_resolution(authoritative):
        if record["category"] == "non_member" or bool(source_metadata.get("target_company_mismatch")):
            decision = "manual_non_member"
        elif record["category"] in {"employee", "former_employee"} and str(record["employment_status"] or "").strip():
            decision = "manual_confirmed_member"
    if "membership_review_required" in source_metadata or has_explicit_manual_review_resolution(authoritative):
        metadata["membership_review_required"] = bool(source_metadata.get("membership_review_required"))
    if "membership_review_reason" in source_metadata or has_explicit_manual_review_resolution(authoritative):
        metadata["membership_review_reason"] = str(source_metadata.get("membership_review_reason") or "").strip()
    if decision or "membership_review_decision" in source_metadata:
        metadata["membership_review_decision"] = decision
    if "membership_review_confidence" in source_metadata:
        metadata["membership_review_confidence"] = str(source_metadata.get("membership_review_confidence") or "").strip()
    if "membership_review_rationale" in source_metadata:
        metadata["membership_review_rationale"] = str(source_metadata.get("membership_review_rationale") or "").strip()
    if "membership_review_triggers" in source_metadata:
        metadata["membership_review_triggers"] = list(source_metadata.get("membership_review_triggers") or [])
    if "membership_review_trigger_keywords" in source_metadata:
        metadata["membership_review_trigger_keywords"] = list(
            source_metadata.get("membership_review_trigger_keywords") or []
        )
    if "target_company_mismatch" in source_metadata:
        metadata["target_company_mismatch"] = bool(source_metadata.get("target_company_mismatch"))
    for key in [
        "manual_review_links",
        "manual_review_artifact_root",
        "manual_review_signals",
        "profile_url",
        "public_identifier",
    ]:
        if key in source_metadata:
            metadata[key] = source_metadata.get(key)
    for key in [
        "profile_account_id",
        "profile_location",
        "more_profiles",
        "membership_claim_category",
        "membership_claim_employment_status",
        "raw_linkedin_url",
        "sanity_linkedin_url",
        "source_shards",
        "source_jobs",
        "headline",
        "summary",
        "about",
        "location",
        "languages",
        "skills",
    ]:
        incoming_value = source_metadata.get(key)
        if incoming_value in ("", None, [], {}):
            continue
        existing_value = metadata.get(key)
        if existing_value in ("", None, [], {}):
            metadata[key] = incoming_value

    record["metadata"] = metadata
    return Candidate(**record)


def _append_linkedin_identifier(identifiers: set[str], value: Any) -> None:
    if isinstance(value, dict):
        for key in ["url", "profile_url", "linkedin_url", "public_identifier", "username"]:
            _append_linkedin_identifier(identifiers, value.get(key))
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _append_linkedin_identifier(identifiers, item)
        return
    normalized = _normalize_linkedin_identifier(str(value or ""))
    if normalized:
        identifiers.add(normalized)


def _normalize_linkedin_identifier(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if "linkedin.com" in lowered:
        slug = _extract_linkedin_slug(raw)
        if slug:
            return slug.lower()
        return lowered.rstrip("/")
    return raw.strip().strip("/").lower()


def _extract_linkedin_slug(url: str) -> str:
    match = re.search(r"linkedin\\.com/in/([^/?#]+)", str(url or ""), re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()
