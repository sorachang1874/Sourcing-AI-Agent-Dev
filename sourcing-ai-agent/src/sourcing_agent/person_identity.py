from __future__ import annotations

from typing import Any

from .linkedin_url_normalization import normalize_linkedin_profile_url_key
from .public_candidate_facets import (
    candidate_employment_statuses_for_public_facets,
    candidate_function_bucket_projection_for_public_facets,
)


def _normalized_public_text_list(value: Any, *, limit: int = 12) -> list[str]:
    raw_values: list[Any]
    if isinstance(value, (list, tuple)):
        raw_values = list(value)
    else:
        raw_text = str(value or "").strip()
        raw_values = re_split_public_lines(raw_text) if raw_text else []
    lines: list[str] = []
    seen: set[str] = set()
    for raw_item in raw_values:
        normalized = str(raw_item or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        lines.append(normalized)
        if len(lines) >= limit:
            break
    return lines


def re_split_public_lines(value: str) -> list[str]:
    return [item.strip() for item in value.replace("\n", " | ").split(" | ") if item.strip()]


def resolve_profile_url_key(*values: Any) -> str:
    """Return the canonical LinkedIn profile key from raw/sanity URL-like values."""

    for value in values:
        raw_value = str(value or "").strip()
        if not raw_value:
            continue
        if raw_value.startswith("linkedin:"):
            raw_value = raw_value.removeprefix("linkedin:").strip()
        if raw_value and "://" not in raw_value and "/" not in raw_value and ":" not in raw_value:
            return raw_value
        normalized = normalize_linkedin_profile_url_key(raw_value)
        if normalized:
            return normalized
    return ""


def person_identity_key_for_profile(profile_url_key: str) -> str:
    normalized = resolve_profile_url_key(profile_url_key)
    return f"linkedin:{normalized}" if normalized else ""


def resolve_person_identity_key(
    *,
    person_identity_key: str = "",
    profile_url_key: str = "",
    linkedin_url: str = "",
    candidate_identity_key: str = "",
    candidate_id: str = "",
) -> str:
    """Resolve the v1 global person identity without loading raw profile payloads."""

    explicit = str(person_identity_key or "").strip()
    if explicit.startswith("linkedin:"):
        profile_key = resolve_profile_url_key(explicit.removeprefix("linkedin:"))
        if profile_key:
            return f"linkedin:{profile_key}"
    if explicit:
        return explicit

    profile_key = resolve_profile_url_key(profile_url_key, linkedin_url)
    if profile_key:
        return f"linkedin:{profile_key}"

    candidate_key = str(candidate_identity_key or "").strip()
    if candidate_key.startswith("linkedin:"):
        profile_key = resolve_profile_url_key(candidate_key.removeprefix("linkedin:"))
        if profile_key:
            return f"linkedin:{profile_key}"
    if candidate_key.startswith("person:"):
        return candidate_key

    normalized_candidate_id = str(candidate_id or "").strip()
    if normalized_candidate_id:
        return f"candidate:{normalized_candidate_id}"
    if candidate_key:
        return f"candidate_identity:{candidate_key}"
    return ""


def resolve_candidate_identity_key(
    *,
    candidate_identity_key: str = "",
    person_identity_key: str = "",
    profile_url_key: str = "",
    linkedin_url: str = "",
    candidate_id: str = "",
) -> str:
    explicit = str(candidate_identity_key or "").strip()
    if explicit:
        return explicit
    person_key = resolve_person_identity_key(
        person_identity_key=person_identity_key,
        profile_url_key=profile_url_key,
        linkedin_url=linkedin_url,
        candidate_id=candidate_id,
    )
    if person_key:
        return person_key
    normalized_candidate_id = str(candidate_id or "").strip()
    return f"candidate:{normalized_candidate_id}" if normalized_candidate_id else ""


def build_person_summary_view(
    payload: dict[str, Any],
    *,
    candidate_id: str = "",
    profile_url_key: str = "",
    linkedin_url: str = "",
    person_identity_key: str = "",
    source_projection_id: str = "",
    source_run_id: str = "",
) -> dict[str, Any]:
    """Build a compact, public-safe row summary shared by projections, CRM, and exports.

    Every summary carries the canonical served facet projection fields
    (``function_bucket_ids`` / ``function_bucket_source``, plus the
    authoritative ``employment_statuses`` membership set when non-empty):
    owned values are preserved and closed-validated when the source has them,
    otherwise they are derived from source evidence with marked
    ``legacy_inference`` only for genuinely legacy rows.
    """

    source = dict(payload or {})
    normalized_candidate_id = str(candidate_id or source.get("candidate_id") or source.get("id") or "").strip()
    normalized_linkedin_url = str(
        linkedin_url or source.get("linkedin_url") or source.get("profile_url") or source.get("url") or ""
    ).strip()
    normalized_profile_key = resolve_profile_url_key(
        profile_url_key,
        source.get("profile_url_key"),
        normalized_linkedin_url,
    )
    normalized_person_key = resolve_person_identity_key(
        person_identity_key=person_identity_key,
        profile_url_key=normalized_profile_key,
        linkedin_url=normalized_linkedin_url,
        candidate_id=normalized_candidate_id,
    )
    display_name = str(
        source.get("display_name") or source.get("candidate_name") or source.get("name") or source.get("name_en") or ""
    ).strip()
    headline = str(source.get("headline") or source.get("role") or source.get("title") or "").strip()
    current_company = str(
        source.get("current_company") or source.get("organization") or source.get("company") or ""
    ).strip()
    avatar_url = str(source.get("avatar_url") or source.get("photo_url") or source.get("media_url") or "").strip()
    location = str(source.get("location") or source.get("profile_location") or "").strip()
    profile_summary = str(source.get("summary") or source.get("about") or source.get("bio") or "").strip()
    experience_lines = _normalized_public_text_list(source.get("experience_lines"), limit=12)
    education_lines = _normalized_public_text_list(source.get("education_lines"), limit=8)
    profile_capture_kind = str(source.get("profile_capture_kind") or "").strip()
    profile_detail_present = bool(source.get("has_profile_detail")) or bool(
        profile_capture_kind or experience_lines or education_lines
    )
    summary = {
        "candidate_id": normalized_candidate_id,
        "display_name": display_name,
        "name": str(source.get("name") or display_name).strip(),
        "headline": headline,
        "current_company": current_company,
        "title": str(source.get("title") or source.get("role") or "").strip(),
        "location": location,
        "linkedin_url": normalized_linkedin_url,
        "profile_url_key": normalized_profile_key,
        "person_identity_key": normalized_person_key,
        "avatar_url": avatar_url,
        "avatar_asset_id": str(source.get("avatar_asset_id") or "").strip(),
        "profile_fetched_at": str(source.get("profile_fetched_at") or source.get("last_fetched_at") or "").strip(),
        "profile_indexed_at": str(source.get("profile_indexed_at") or "").strip(),
        "source_projection_id": str(source_projection_id or source.get("source_projection_id") or "").strip(),
        "source_run_id": str(source_run_id or source.get("source_run_id") or "").strip(),
    }
    if profile_summary:
        summary["summary"] = profile_summary
    if experience_lines:
        summary["experience_lines"] = experience_lines
    if education_lines:
        summary["education_lines"] = education_lines
    if profile_detail_present and "has_profile_detail" not in source:
        summary["has_profile_detail"] = True
    if profile_detail_present and "needs_profile_completion" not in source:
        summary["needs_profile_completion"] = False
    for key in (
        "employment_status",
        "matched_keywords",
        "function_ids",
        "skills",
        "education",
        "experience",
        "has_profile_detail",
        "needs_profile_completion",
        "low_profile_richness",
        "profile_capture_kind",
    ):
        if key in source:
            summary[key] = source.get(key)
    # Canonical served facet projection (FT0 §5.2/§6; FT1-FF2): this adapter
    # is the ONE public-summary owner for normal publication, migration,
    # repair, and consolidation, so it must establish — not assume — the
    # owned-field invariant.  It preserves and closed-validates the OWNED
    # function-bucket pair / authoritative employment status set when the
    # source carries them, and otherwise derives the canonical values from
    # source evidence (lane membership > registry evidence > marked
    # legacy_inference for genuinely legacy rows).  Modern owned values can
    # therefore never be silently downgraded before indexing (explicit
    # infra_systems → bare-id engineering; dual-status → display scalar), and
    # malformed provenance or a malformed persisted pair raises instead of
    # falling through to unmarked legacy inference.
    function_bucket_projection = candidate_function_bucket_projection_for_public_facets(source)
    summary["function_bucket_ids"] = list(function_bucket_projection["function_bucket_ids"])
    summary["function_bucket_source"] = str(function_bucket_projection["function_bucket_source"])
    authoritative_statuses = candidate_employment_statuses_for_public_facets(source)
    if authoritative_statuses:
        summary["employment_statuses"] = list(authoritative_statuses)
    return {key: value for key, value in summary.items() if value not in ("", None, [], {})}
