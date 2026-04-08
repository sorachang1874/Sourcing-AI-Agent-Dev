from __future__ import annotations

from collections import defaultdict
import re
from typing import Any

from .domain import Candidate, EvidenceRecord, make_evidence_id, merge_candidate, normalize_name_token


def canonicalize_company_records(
    candidates: list[Candidate],
    evidence: list[EvidenceRecord],
) -> tuple[list[Candidate], list[EvidenceRecord], dict[str, Any]]:
    evidence_by_candidate: dict[str, list[EvidenceRecord]] = defaultdict(list)
    for item in evidence:
        evidence_by_candidate[item.candidate_id].append(item)

    ordered_candidates = sorted(
        candidates,
        key=lambda item: (
            -_candidate_priority(item, evidence_by_candidate.get(item.candidate_id, [])),
            (item.display_name or item.name_en).lower(),
            item.candidate_id,
        ),
    )

    canonical_candidates: dict[str, Candidate] = {}
    canonical_evidence_by_candidate: dict[str, list[EvidenceRecord]] = defaultdict(list)
    candidate_aliases: dict[str, str] = {}
    strong_index: dict[str, str] = {}
    name_index: dict[str, list[str]] = defaultdict(list)
    merge_events = 0
    name_merge_events = 0

    for candidate in ordered_candidates:
        candidate_evidence = list(evidence_by_candidate.get(candidate.candidate_id, []))
        canonical_id, matched_by_name = _resolve_canonical_candidate(
            candidate,
            candidate_evidence,
            canonical_candidates=canonical_candidates,
            canonical_evidence_by_candidate=canonical_evidence_by_candidate,
            strong_index=strong_index,
            name_index=name_index,
        )
        if not canonical_id:
            canonical_id = candidate.candidate_id
            canonical_candidates[canonical_id] = candidate
            canonical_evidence_by_candidate[canonical_id].extend(candidate_evidence)
            _index_candidate_keys(candidate, canonical_id, strong_index, name_index)
            candidate_aliases[candidate.candidate_id] = canonical_id
            continue

        merge_events += 1
        if matched_by_name:
            name_merge_events += 1
        existing = canonical_candidates[canonical_id]
        merged = _merge_canonical_candidates(existing, candidate)
        canonical_candidates[canonical_id] = merged
        canonical_evidence_by_candidate[canonical_id].extend(candidate_evidence)
        candidate_aliases[candidate.candidate_id] = canonical_id
        _index_candidate_keys(merged, canonical_id, strong_index, name_index)

    canonical_evidence_by_key: dict[tuple[str, str, str, str], EvidenceRecord] = {}
    for item in evidence:
        canonical_id = candidate_aliases.get(item.candidate_id, item.candidate_id)
        rebound = _rebind_evidence_candidate(item, canonical_id)
        canonical_evidence_by_key[_evidence_dedupe_key(rebound)] = rebound

    canonicalized_candidates = sorted(
        canonical_candidates.values(),
        key=lambda item: (item.display_name or item.name_en).lower(),
    )
    canonicalized_evidence = sorted(
        canonical_evidence_by_key.values(),
        key=lambda item: (item.candidate_id, item.source_type, item.title, item.url or item.source_path),
    )
    return (
        canonicalized_candidates,
        canonicalized_evidence,
        {
            "input_candidate_count": len(candidates),
            "canonical_candidate_count": len(canonicalized_candidates),
            "input_evidence_count": len(evidence),
            "canonical_evidence_count": len(canonicalized_evidence),
            "merged_candidate_count": merge_events,
            "name_merge_count": name_merge_events,
        },
    )


def _resolve_canonical_candidate(
    candidate: Candidate,
    candidate_evidence: list[EvidenceRecord],
    *,
    canonical_candidates: dict[str, Candidate],
    canonical_evidence_by_candidate: dict[str, list[EvidenceRecord]],
    strong_index: dict[str, str],
    name_index: dict[str, list[str]],
) -> tuple[str, bool]:
    for key in _candidate_strong_keys(candidate, evidence=candidate_evidence):
        canonical_id = strong_index.get(key, "")
        if canonical_id:
            return canonical_id, False

    name_key = _candidate_name_key(candidate)
    if not name_key:
        return "", False
    for canonical_id in name_index.get(name_key, []):
        canonical_candidate = canonical_candidates.get(canonical_id)
        if canonical_candidate is None:
            continue
        canonical_evidence = canonical_evidence_by_candidate.get(canonical_id, [])
        if _can_merge_by_name(canonical_candidate, candidate, canonical_evidence, candidate_evidence):
            return canonical_id, True
    return "", False


def _index_candidate_keys(
    candidate: Candidate,
    canonical_id: str,
    strong_index: dict[str, str],
    name_index: dict[str, list[str]],
) -> None:
    for key in _candidate_strong_keys(candidate):
        strong_index.setdefault(key, canonical_id)
    name_key = _candidate_name_key(candidate)
    if name_key and canonical_id not in name_index[name_key]:
        name_index[name_key].append(canonical_id)


def _candidate_priority(candidate: Candidate, evidence: list[EvidenceRecord]) -> int:
    metadata = dict(candidate.metadata or {})
    score = 0
    score += 20 * _category_rank(candidate.category)
    if str(candidate.employment_status or "").strip().lower() == "current":
        score += 10
    if _candidate_linkedin_identifiers(candidate, evidence=evidence):
        score += 12
    if any(_is_explicit_profile_evidence(item) for item in evidence):
        score += 10
    if str(candidate.education or "").strip():
        score += 4
    if str(candidate.work_history or "").strip() and not _is_roster_baseline_only(candidate):
        score += 4
    if str(candidate.linkedin_url or "").strip():
        score += 3
    if bool(metadata.get("membership_review_required")):
        score += 6
    if candidate.category == "lead" and _candidate_linkedin_identifiers(candidate, evidence=evidence):
        score += 6
    score += min(len(evidence), 10)
    return score


def _category_rank(value: str) -> int:
    normalized = str(value or "").strip().lower()
    if normalized in {"employee", "former_employee"}:
        return 4
    if normalized == "investor":
        return 3
    if normalized == "lead":
        return 1
    return 0


def _candidate_name_key(candidate: Candidate) -> str:
    return normalize_name_token(candidate.display_name or candidate.name_en)


def _candidate_strong_keys(candidate: Candidate, *, evidence: list[EvidenceRecord] | None = None) -> list[str]:
    keys: list[str] = []
    candidate_id = str(candidate.candidate_id or "").strip()
    if candidate_id:
        keys.append(f"id:{candidate_id}")
    for identifier in sorted(_candidate_linkedin_identifiers(candidate, evidence=evidence)):
        keys.append(f"linkedin:{identifier}")
    return keys


def _candidate_linkedin_identifiers(candidate: Candidate, *, evidence: list[EvidenceRecord] | None = None) -> set[str]:
    identifiers: set[str] = set()
    metadata = dict(candidate.metadata or {})
    for value in [
        candidate.linkedin_url,
        metadata.get("profile_url"),
        metadata.get("linkedin_url"),
        metadata.get("public_identifier"),
        metadata.get("seed_slug"),
        metadata.get("manual_review_links"),
        metadata.get("manual_review_signals"),
        metadata.get("more_profiles"),
    ]:
        _append_linkedin_identifier(identifiers, value)
    for item in evidence or []:
        _append_linkedin_identifier(identifiers, item.url)
        _append_linkedin_identifier(identifiers, item.metadata)
    return identifiers


def _append_linkedin_identifier(identifiers: set[str], value: Any) -> None:
    if isinstance(value, dict):
        for key in ["url", "profile_url", "linkedin_url", "public_identifier", "username", "linkedin_urls"]:
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
    lowered = raw.lower().rstrip("/")
    if "linkedin.com" in lowered:
        slug = _extract_linkedin_slug(raw)
        if slug:
            return slug.lower()
        return lowered
    return raw.strip().strip("/").lower()


def _extract_linkedin_slug(url: str) -> str:
    match = re.search(r"linkedin\.com/in/([^/?#]+)", str(url or ""), re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()


def _can_merge_by_name(
    existing: Candidate,
    incoming: Candidate,
    existing_evidence: list[EvidenceRecord],
    incoming_evidence: list[EvidenceRecord],
) -> bool:
    if _candidate_name_key(existing) != _candidate_name_key(incoming):
        return False
    if normalize_name_token(existing.target_company) != normalize_name_token(incoming.target_company):
        return False
    existing_ids = _candidate_linkedin_identifiers(existing, evidence=existing_evidence)
    incoming_ids = _candidate_linkedin_identifiers(incoming, evidence=incoming_evidence)
    if existing_ids & incoming_ids:
        return True

    existing_category = str(existing.category or "").strip().lower()
    incoming_category = str(incoming.category or "").strip().lower()
    if "lead" in {existing_category, incoming_category}:
        stronger = incoming if incoming_category != "lead" else existing
        stronger_evidence = incoming_evidence if stronger is incoming else existing_evidence
        if _candidate_linkedin_identifiers(stronger, evidence=stronger_evidence):
            return True

    existing_role = normalize_name_token(existing.role)
    incoming_role = normalize_name_token(incoming.role)
    same_status = str(existing.employment_status or "").strip().lower() == str(incoming.employment_status or "").strip().lower()
    if existing_ids and not incoming_ids and same_status and (not incoming_role or incoming_role == existing_role):
        return True
    if incoming_ids and not existing_ids and same_status and (not existing_role or existing_role == incoming_role):
        return True

    if (
        same_status
        and existing_category == incoming_category
        and existing_role
        and existing_role == incoming_role
        and (_is_roster_baseline_only(existing) or _is_roster_baseline_only(incoming))
    ):
        return True
    return False


def _merge_canonical_candidates(existing: Candidate, incoming: Candidate) -> Candidate:
    record = merge_candidate(existing, incoming).to_record()
    existing_metadata = dict(existing.metadata or {})
    incoming_metadata = dict(incoming.metadata or {})
    merged_metadata = _merge_metadata(existing_metadata, incoming_metadata)
    record["metadata"] = merged_metadata

    if str(incoming.notes or "").strip():
        record["notes"] = _append_unique_text(str(existing.notes or "").strip(), str(incoming.notes or "").strip(), separator=" | ")
    if str(incoming.focus_areas or "").strip():
        record["focus_areas"] = _append_unique_text(
            str(existing.focus_areas or "").strip(),
            str(incoming.focus_areas or "").strip(),
            separator=" | ",
        )
    if not str(record.get("linkedin_url") or "").strip() and str(incoming.linkedin_url or "").strip():
        record["linkedin_url"] = incoming.linkedin_url
    if not str(record.get("source_path") or "").strip() and str(incoming.source_path or "").strip():
        record["source_path"] = incoming.source_path
    return Candidate(**record)


def _merge_metadata(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if key not in merged or merged[key] in ("", None, [], {}):
            merged[key] = _copy_value(value)
            continue
        if key in {"more_profiles", "membership_review_triggers", "membership_review_trigger_keywords"}:
            merged[key] = _merge_unique_lists(merged.get(key), value)
            continue
        if key in {"manual_review_links"}:
            merged[key] = _merge_unique_lists(merged.get(key), value)
            continue
        if key == "membership_review_required":
            merged[key] = bool(merged.get(key)) or bool(value)
            continue
        if key.startswith("membership_review_") and merged.get("membership_review_required") in (False, "", None) and value not in ("", None, [], {}):
            merged[key] = _copy_value(value)
            continue
    return merged


def _merge_unique_lists(existing: Any, incoming: Any) -> list[Any]:
    values = []
    seen: set[str] = set()
    for item in list(existing or []) + list(incoming or []):
        marker = repr(item)
        if marker in seen:
            continue
        seen.add(marker)
        values.append(_copy_value(item))
    return values


def _copy_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _copy_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_value(item) for item in value]
    return value


def _append_unique_text(existing: str, incoming: str, *, separator: str) -> str:
    base = str(existing or "").strip()
    extra = str(incoming or "").strip()
    if not extra:
        return base
    if not base:
        return extra
    if extra in base:
        return base
    return f"{base}{separator}{extra}"


def _rebind_evidence_candidate(evidence: EvidenceRecord, candidate_id: str) -> EvidenceRecord:
    title = str(evidence.title or "").strip()
    url = str(evidence.url or "").strip()
    source_path = str(evidence.source_path or "").strip()
    source_dataset = str(evidence.source_dataset or evidence.source_type or "").strip()
    return EvidenceRecord(
        evidence_id=make_evidence_id(candidate_id, source_dataset, title, url or source_path),
        candidate_id=candidate_id,
        source_type=str(evidence.source_type or "").strip(),
        title=title,
        url=url,
        summary=str(evidence.summary or "").strip(),
        source_dataset=source_dataset,
        source_path=source_path,
        metadata=_copy_value(dict(evidence.metadata or {})),
    )


def _evidence_dedupe_key(evidence: EvidenceRecord) -> tuple[str, str, str, str]:
    return (
        str(evidence.candidate_id or "").strip(),
        str(evidence.source_dataset or evidence.source_type or "").strip(),
        str(evidence.title or "").strip(),
        str(evidence.url or evidence.source_path or "").strip(),
    )


def _is_explicit_profile_evidence(evidence: EvidenceRecord) -> bool:
    source_type = str(evidence.source_type or "").strip()
    return source_type.startswith("linkedin_profile_") or source_type == "manual_review_analysis"


def _is_roster_baseline_only(candidate: Candidate) -> bool:
    source_dataset = str(candidate.source_dataset or "").strip()
    role = str(candidate.role or "").strip()
    work_history = str(candidate.work_history or "").strip()
    notes = str(candidate.notes or "")
    return (
        source_dataset.endswith("_linkedin_company_people")
        and bool(role)
        and (not work_history or work_history == role)
        and "LinkedIn company roster baseline." in notes
        and not str(candidate.education or "").strip()
    )
