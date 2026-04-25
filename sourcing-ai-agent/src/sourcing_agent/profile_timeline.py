from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from .asset_paths import resolve_snapshot_dir_from_source_path, resolve_source_path_in_runtime
from .company_registry import builtin_company_identity, normalize_company_key


def normalized_text_lines(value: Any) -> list[str]:
    if isinstance(value, list):
        items = value
    elif value in ("", None):
        items = []
    else:
        items = [value]
    return [line for line in [" ".join(str(item or "").split()).strip() for item in items] if line]


def normalized_primary_email_metadata(value: Any) -> dict[str, Any]:
    payload = dict(value or {}) if isinstance(value, dict) else {}
    if not payload and isinstance(value, str):
        text = str(value or "").strip()
        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = {}
            if isinstance(parsed, dict):
                payload = dict(parsed)
    if not payload:
        return {}
    normalized: dict[str, Any] = {}
    source = str(payload.get("source") or payload.get("provider") or "").strip()
    status = str(payload.get("status") or "").strip()
    quality_score = _coerce_numeric_email_quality_score(payload.get("qualityScore") or payload.get("quality_score"))
    found_in_linkedin_profile = _coerce_optional_bool(
        payload.get("foundInLinkedInProfile")
        if "foundInLinkedInProfile" in payload
        else payload.get("found_in_linkedin_profile")
    )
    if source:
        normalized["source"] = source
    if status:
        normalized["status"] = status
    if quality_score is not None:
        normalized["qualityScore"] = quality_score
    if found_in_linkedin_profile is not None:
        normalized["foundInLinkedInProfile"] = found_in_linkedin_profile
    return normalized


def candidate_profile_lookup_url(payload: dict[str, Any], metadata: dict[str, Any] | None = None) -> str:
    metadata = dict(metadata or payload.get("metadata") or {})
    explicit_url = _first_non_empty_text(
        payload.get("linkedin_url"),
        payload.get("profile_url"),
        metadata.get("profile_url"),
        metadata.get("linkedin_url"),
    )
    if explicit_url:
        return explicit_url
    public_identifier = _first_non_empty_text(
        payload.get("public_identifier"),
        payload.get("username"),
        metadata.get("public_identifier"),
        metadata.get("username"),
    )
    normalized_identifier = str(public_identifier or "").strip().strip("/ ")
    if not normalized_identifier:
        return ""
    lowered_identifier = normalized_identifier.lower()
    if "linkedin.com/in/" in lowered_identifier:
        slug = lowered_identifier.split("linkedin.com/in/", 1)[-1].split("?", 1)[0].split("#", 1)[0]
    else:
        slug = normalized_identifier
    normalized_slug = str(slug or "").strip().strip("/ ")
    if not normalized_slug:
        return ""
    return f"https://www.linkedin.com/in/{normalized_slug}/"


def resolve_candidate_profile_timeline(
    *,
    payload: dict[str, Any],
    metadata: dict[str, Any] | None = None,
    source_path: str = "",
    registry_row: dict[str, Any] | None = None,
    timeline_cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    metadata = dict(metadata or payload.get("metadata") or {})
    suppression_context = _email_suppression_context(payload, metadata)
    resolved_source_path = _resolved_profile_source_path(
        source_path or payload.get("source_path") or metadata.get("source_path") or ""
    )
    embedded_experience_lines = normalized_text_lines(
        payload.get("experience_lines") or metadata.get("experience_lines")
    )
    embedded_education_lines = normalized_text_lines(
        payload.get("education_lines") or metadata.get("education_lines")
    )
    embedded_signals = embedded_profile_signal_fields(payload, metadata)
    embedded_email_metadata = normalized_primary_email_metadata(
        payload.get("primary_email_metadata") or metadata.get("primary_email_metadata")
    )

    direct_timeline = profile_snapshot_from_source_path(
        resolved_source_path,
        cache=timeline_cache,
        payload=payload,
        metadata=metadata,
    )
    companion_timeline = _best_companion_profile_snapshot(
        resolved_source_path,
        payload=payload,
        metadata=metadata,
        cache=timeline_cache,
    )
    if companion_timeline:
        direct_timeline = _merge_profile_timeline_snapshots(direct_timeline, companion_timeline)
    candidate_timelines: list[tuple[str, str, dict[str, Any]]] = []
    if _timeline_has_useful_content(direct_timeline):
        candidate_timelines.append(("source_path", resolved_source_path, direct_timeline))

    profile_source_path = _resolved_profile_source_path(
        payload.get("profile_timeline_source_path")
        or metadata.get("profile_timeline_source_path")
        or payload.get("harvest_profile_path")
        or metadata.get("harvest_profile_path")
        or ""
    )
    if profile_source_path and profile_source_path != resolved_source_path:
        profile_timeline = profile_snapshot_from_source_path(
            profile_source_path,
            cache=timeline_cache,
            payload=payload,
            metadata=metadata,
        )
        if _timeline_has_useful_content(profile_timeline):
            candidate_timelines.append(("profile_source_path", profile_source_path, profile_timeline))

    registry_source_path = _resolved_profile_source_path(str(dict(registry_row or {}).get("last_raw_path") or ""))
    if registry_source_path and registry_source_path != resolved_source_path:
        registry_timeline = profile_snapshot_from_source_path(
            registry_source_path,
            cache=timeline_cache,
            payload=payload,
            metadata=metadata,
        )
        if _timeline_has_useful_content(registry_timeline):
            registry_timeline["profile_capture_kind"] = "profile_registry_detail"
            registry_timeline["profile_capture_source_path"] = registry_source_path
            candidate_timelines.append(("profile_registry", registry_source_path, registry_timeline))

    if candidate_timelines:
        selected_source_kind, selected_source_path, merged_timeline = _select_best_profile_timeline_source(candidate_timelines)
        if embedded_experience_lines and not merged_timeline.get("experience_lines"):
            merged_timeline["experience_lines"] = embedded_experience_lines
        if embedded_education_lines and not merged_timeline.get("education_lines"):
            merged_timeline["education_lines"] = embedded_education_lines
        preserve_existing_email_suppression = profile_capture_kind_has_profile_detail(merged_timeline.get("profile_capture_kind"))
        for key in PROFILE_SIGNAL_FIELD_KEYS:
            if key == "primary_email" and preserve_existing_email_suppression:
                continue
            if _has_signal_value(embedded_signals.get(key)) and not _has_signal_value(merged_timeline.get(key)):
                merged_timeline[key] = embedded_signals.get(key)
        if (
            embedded_email_metadata
            and str(merged_timeline.get("primary_email") or "").strip()
            and str(merged_timeline.get("primary_email") or "").strip() == str(embedded_signals.get("primary_email") or "").strip()
            and not normalized_primary_email_metadata(merged_timeline.get("primary_email_metadata"))
        ):
            merged_timeline["primary_email_metadata"] = embedded_email_metadata
        if not str(merged_timeline.get("profile_capture_kind") or "").strip():
            merged_timeline["profile_capture_kind"] = infer_profile_capture_kind(
                payload=payload,
                metadata=metadata,
                source_path=selected_source_path,
            )
        if not str(merged_timeline.get("profile_capture_source_path") or "").strip():
            merged_timeline["profile_capture_source_path"] = str(selected_source_path or "").strip()
        merged_timeline = scrub_unpublishable_primary_email(merged_timeline, context=suppression_context)
        return {
            **merged_timeline,
            "source_kind": selected_source_kind,
            "source_path": selected_source_path,
        }

    if embedded_experience_lines or embedded_education_lines:
        profile_capture_kind = infer_profile_capture_kind(
            payload=payload,
            metadata=metadata,
            source_path=resolved_source_path,
        )
        return scrub_unpublishable_primary_email({
            "experience_lines": embedded_experience_lines,
            "education_lines": embedded_education_lines,
            **embedded_signals,
            "primary_email_metadata": embedded_email_metadata,
            "profile_capture_kind": profile_capture_kind,
            "profile_capture_source_path": resolved_source_path,
            "source_kind": "embedded",
            "source_path": resolved_source_path,
        }, context=suppression_context)
    if any(_has_signal_value(value) for value in embedded_signals.values()):
        profile_capture_kind = infer_profile_capture_kind(
            payload=payload,
            metadata=metadata,
            source_path=resolved_source_path,
        )
        return scrub_unpublishable_primary_email({
            "experience_lines": [],
            "education_lines": [],
            **embedded_signals,
            "primary_email_metadata": embedded_email_metadata,
            "profile_capture_kind": profile_capture_kind,
            "profile_capture_source_path": resolved_source_path,
            "source_kind": "embedded",
            "source_path": resolved_source_path,
        }, context=suppression_context)

    return scrub_unpublishable_primary_email({
        "experience_lines": [],
        "education_lines": [],
        **{key: [] if key in {"languages", "skills"} else "" for key in PROFILE_SIGNAL_FIELD_KEYS},
        "primary_email_metadata": {},
        "profile_capture_kind": "",
        "profile_capture_source_path": "",
        "source_kind": "",
        "source_path": "",
    }, context=suppression_context)


def profile_timeline_lines_from_source_path(
    source_path: str,
    *,
    cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[str]]:
    snapshot = profile_snapshot_from_source_path(source_path, cache=cache)
    return {
        "experience_lines": normalized_text_lines(snapshot.get("experience_lines")),
        "education_lines": normalized_text_lines(snapshot.get("education_lines")),
    }


PROFILE_SIGNAL_FIELD_KEYS = (
    "headline",
    "summary",
    "about",
    "profile_location",
    "public_identifier",
    "avatar_url",
    "photo_url",
    "primary_email",
    "languages",
    "skills",
)

FULL_PROFILE_CAPTURE_KINDS = {
    "harvest_profile_detail",
    "provider_profile_detail",
    "profile_registry_detail",
    "embedded_profile_detail",
}

PARTIAL_PROFILE_CAPTURE_KINDS = {
    "search_seed_preview",
    "embedded_profile_preview",
    "roster_baseline_preview",
}

_PROFILE_CAPTURE_KIND_PRIORITY = {
    "profile_registry_detail": 5,
    "harvest_profile_detail": 4,
    "provider_profile_detail": 4,
    "embedded_profile_detail": 3,
    "embedded_profile_preview": 2,
    "search_seed_preview": 1,
    "roster_baseline_preview": 1,
    "": 0,
}


def profile_capture_kind_has_profile_detail(value: Any) -> bool:
    return str(value or "").strip().lower() in FULL_PROFILE_CAPTURE_KINDS


def primary_email_requires_suppression(
    timeline: dict[str, Any] | None,
    *,
    context: dict[str, Any] | None = None,
) -> bool:
    payload = _email_suppression_payload(timeline, context)
    email = _first_non_empty_text(payload.get("primary_email"), payload.get("email"))
    source_path = _first_non_empty_text(
        payload.get("profile_capture_source_path"),
        payload.get("source_path"),
    )
    capture_kind = infer_profile_capture_kind(
        payload=payload,
        metadata=payload,
        source_path=source_path,
    )
    email_metadata = normalized_primary_email_metadata(payload.get("primary_email_metadata"))
    email_source = str(email_metadata.get("source") or "").strip().lower()
    harvest_source_hint = bool(email_source == "harvestapi" or _source_path_looks_harvest_sourced(source_path))
    if email:
        if harvest_source_hint and not _harvest_email_is_publishable(email_metadata):
            return True
        return False
    if profile_capture_kind_has_profile_detail(capture_kind):
        return True
    lowered_source_path = _normalized_source_path(source_path)
    if capture_kind in PARTIAL_PROFILE_CAPTURE_KINDS and (
        "/harvest_company_employees/" in lowered_source_path
        or "/search_seed_discovery/" in lowered_source_path
        or "/harvest_profile_search/" in lowered_source_path
    ):
        return True
    return "/harvest_profiles/" in lowered_source_path


def scrub_unpublishable_primary_email(
    timeline: dict[str, Any] | None,
    *,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = dict(timeline or {})
    if primary_email_requires_suppression(normalized, context=context):
        normalized["primary_email"] = ""
        normalized.pop("email", None)
        normalized["primary_email_metadata"] = {}
    return normalized


def _email_suppression_payload(
    timeline: dict[str, Any] | None,
    context: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(context or {})
    merged_metadata = dict(merged.get("metadata") or {})
    timeline_payload = dict(timeline or {})
    timeline_metadata = dict(timeline_payload.get("metadata") or {})
    merged.update(timeline_payload)
    if merged_metadata or timeline_metadata:
        merged["metadata"] = {
            **merged_metadata,
            **timeline_metadata,
        }
    return merged


def _email_suppression_context(payload: dict[str, Any], metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    merged = dict(payload or {})
    merged["metadata"] = dict(metadata or payload.get("metadata") or {})
    return merged


def _email_matches_context_company_domain(email: str, payload: dict[str, Any]) -> bool:
    email_domain = _normalized_email_domain(email)
    if not email_domain:
        return False
    for company_domain in _context_company_domains(payload):
        if email_domain == company_domain or email_domain.endswith(f".{company_domain}"):
            return True
    return False


def _context_company_domains(payload: dict[str, Any]) -> set[str]:
    metadata = dict(payload.get("metadata") or {})
    domains: set[str] = set()

    target_company = _first_non_empty_text(
        payload.get("target_company"),
        metadata.get("target_company"),
    )
    normalized_target_key = _resolved_company_identity_key(target_company)

    for value in (
        payload.get("target_company_domain"),
        metadata.get("target_company_domain"),
        dict(payload.get("target_company_identity") or {}).get("domain"),
        dict(metadata.get("target_company_identity") or {}).get("domain"),
    ):
        normalized = _normalized_domain(value)
        if normalized:
            domains.add(normalized)

    target_domain = _resolved_company_domain(target_company)
    if target_domain:
        domains.add(target_domain)

    for company_name in (
        payload.get("organization"),
        metadata.get("organization"),
        payload.get("company"),
        metadata.get("company"),
        payload.get("current_company"),
        metadata.get("current_company"),
    ):
        text = str(company_name or "").strip()
        if not text:
            continue
        normalized_company_key = _resolved_company_identity_key(text)
        if normalized_target_key and normalized_company_key and normalized_company_key != normalized_target_key:
            continue
        company_domain = _resolved_company_domain(text)
        if company_domain:
            domains.add(company_domain)
    return domains


@lru_cache(maxsize=512)
def _resolved_company_identity(name: str) -> tuple[str, str]:
    text = str(name or "").strip()
    if not text:
        return "", ""
    company_key, identity = builtin_company_identity(text)
    domain = _normalized_domain(dict(identity or {}).get("domain"))
    return str(company_key or "").strip(), domain


def _resolved_company_identity_key(name: str) -> str:
    company_key, _ = _resolved_company_identity(name)
    return company_key or normalize_company_key(name)


def _resolved_company_domain(name: str) -> str:
    _, domain = _resolved_company_identity(name)
    return domain


def _normalized_email_domain(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text or "@" not in text:
        return ""
    return _normalized_domain(text.rsplit("@", 1)[-1])


def _normalized_domain(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"^https?://", "", text)
    text = text.split("/", 1)[0]
    text = text.split("@")[-1]
    if text.startswith("www."):
        text = text[4:]
    return text.strip(".")


def _normalized_source_path(value: Any) -> str:
    return str(value or "").strip().lower().replace("\\", "/")


def _source_path_looks_harvest_sourced(value: Any) -> bool:
    lowered_source_path = _normalized_source_path(value)
    return any(
        marker in lowered_source_path
        for marker in (
            "/harvest_profiles/",
            "/harvest_company_employees/",
            "/harvest_profile_search/",
            "/search_seed_discovery/",
        )
    )


def _harvest_email_is_publishable(metadata: dict[str, Any] | None) -> bool:
    normalized = normalized_primary_email_metadata(metadata)
    if str(normalized.get("source") or "").strip().lower() != "harvestapi":
        return False
    return bool(normalized.get("foundInLinkedInProfile") is True)


def infer_profile_capture_kind(
    *,
    payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    source_path: str = "",
) -> str:
    payload = dict(payload or {})
    metadata = dict(metadata or payload.get("metadata") or {})
    explicit = _first_non_empty_text(
        payload.get("profile_capture_kind"),
        metadata.get("profile_capture_kind"),
        payload.get("profile_timeline_capture_kind"),
        metadata.get("profile_timeline_capture_kind"),
    ).lower()
    if explicit:
        return explicit
    for candidate_path in (
        source_path,
        payload.get("profile_capture_source_path"),
        metadata.get("profile_capture_source_path"),
        payload.get("profile_timeline_source_path"),
        metadata.get("profile_timeline_source_path"),
        payload.get("harvest_profile_path"),
        metadata.get("harvest_profile_path"),
        payload.get("source_path"),
        metadata.get("source_path"),
    ):
        inferred = _profile_capture_kind_from_path(candidate_path)
        if inferred:
            return inferred
    experience_lines = normalized_text_lines(payload.get("experience_lines") or metadata.get("experience_lines"))
    education_lines = normalized_text_lines(payload.get("education_lines") or metadata.get("education_lines"))
    if education_lines or len(experience_lines) >= 2:
        return "embedded_profile_detail"
    scalar_signals = [
        payload.get("headline"),
        metadata.get("headline"),
        payload.get("summary"),
        metadata.get("summary"),
        payload.get("about"),
        metadata.get("about"),
        payload.get("primary_email"),
        payload.get("email"),
        metadata.get("primary_email"),
        metadata.get("email"),
    ]
    if experience_lines and any(_first_non_empty_text(value) for value in scalar_signals):
        return "embedded_profile_detail"
    if experience_lines or education_lines:
        return "embedded_profile_preview"
    return ""


def timeline_has_complete_profile_detail(timeline: dict[str, Any]) -> bool:
    experience_lines = normalized_text_lines(dict(timeline or {}).get("experience_lines"))
    education_lines = normalized_text_lines(dict(timeline or {}).get("education_lines"))
    # Treat rich embedded timelines as complete even when the originating source path
    # still looks like a roster/search preview. This prevents partial source hints from
    # downgrading candidates whose structured profile data has already been backfilled.
    if education_lines:
        return True
    if len(experience_lines) >= 2:
        return True
    capture_kind = infer_profile_capture_kind(
        payload=dict(timeline or {}),
        metadata=dict(timeline or {}),
        source_path=str(dict(timeline or {}).get("source_path") or dict(timeline or {}).get("profile_capture_source_path") or ""),
    )
    if capture_kind:
        if profile_capture_kind_has_profile_detail(capture_kind):
            return bool(experience_lines or education_lines)
        return False
    return bool(
        experience_lines
        and (
            _first_non_empty_text(
                dict(timeline or {}).get("summary"),
                dict(timeline or {}).get("about"),
                dict(timeline or {}).get("primary_email"),
            )
            or _profile_text_list(dict(timeline or {}).get("languages"))
            or _profile_text_list(dict(timeline or {}).get("skills"))
        )
    )


def embedded_profile_signal_fields(
    payload: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = dict(metadata or payload.get("metadata") or {})
    skills = _profile_text_list(
        payload.get("skills"),
        metadata.get("skills"),
        payload.get("topSkills"),
        metadata.get("topSkills"),
    )
    languages = _profile_text_list(
        payload.get("languages"),
        metadata.get("languages"),
    )
    return {
        "headline": _first_non_empty_text(payload.get("headline"), metadata.get("headline")),
        "summary": _first_non_empty_text(payload.get("summary"), metadata.get("summary")),
        "about": _first_non_empty_text(payload.get("about"), metadata.get("about")),
        "profile_location": _first_non_empty_text(
            payload.get("profile_location"),
            metadata.get("profile_location"),
            payload.get("location"),
            metadata.get("location"),
        ),
        "public_identifier": _first_non_empty_text(
            payload.get("public_identifier"),
            metadata.get("public_identifier"),
            payload.get("username"),
            metadata.get("username"),
        ),
        "avatar_url": _first_non_empty_text(
            payload.get("avatar_url"),
            payload.get("photo_url"),
            payload.get("media_url"),
            metadata.get("avatar_url"),
            metadata.get("photo_url"),
            metadata.get("media_url"),
        ),
        "photo_url": _first_non_empty_text(
            payload.get("photo_url"),
            payload.get("avatar_url"),
            metadata.get("photo_url"),
            metadata.get("avatar_url"),
        ),
        "primary_email": _first_non_empty_text(
            payload.get("primary_email"),
            payload.get("email"),
            metadata.get("primary_email"),
            metadata.get("email"),
        ),
        "languages": languages,
        "skills": skills,
    }


def profile_snapshot_from_source_path(
    source_path: str,
    *,
    cache: dict[str, dict[str, Any]] | None = None,
    payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    path_text = _resolved_profile_source_path(source_path)
    if not path_text or not path_text.endswith(".json"):
        return {
            "experience_lines": [],
            "education_lines": [],
            **{key: [] if key in {"languages", "skills"} else "" for key in PROFILE_SIGNAL_FIELD_KEYS},
            "primary_email_metadata": {},
            "profile_capture_kind": "",
            "profile_capture_source_path": "",
        }
    cache_key = _profile_snapshot_cache_key(path_text, payload=payload, metadata=metadata)
    if cache is not None and cache_key in cache:
        cached = dict(cache[cache_key] or {})
        return {
            "experience_lines": normalized_text_lines(cached.get("experience_lines")),
            "education_lines": normalized_text_lines(cached.get("education_lines")),
            "headline": _first_non_empty_text(cached.get("headline")),
            "summary": _first_non_empty_text(cached.get("summary")),
            "about": _first_non_empty_text(cached.get("about")),
            "profile_location": _first_non_empty_text(cached.get("profile_location")),
            "public_identifier": _first_non_empty_text(cached.get("public_identifier")),
            "avatar_url": _first_non_empty_text(cached.get("avatar_url"), cached.get("photo_url")),
            "photo_url": _first_non_empty_text(cached.get("photo_url"), cached.get("avatar_url")),
            "primary_email": _first_non_empty_text(cached.get("primary_email")),
            "primary_email_metadata": normalized_primary_email_metadata(cached.get("primary_email_metadata")),
            "languages": _profile_text_list(cached.get("languages")),
            "skills": _profile_text_list(cached.get("skills")),
            "profile_capture_kind": _first_non_empty_text(cached.get("profile_capture_kind")).lower(),
            "profile_capture_source_path": _first_non_empty_text(cached.get("profile_capture_source_path")),
        }
    source_path_obj = Path(path_text).expanduser()
    source_payload = _read_json_payload(source_path_obj)
    if source_payload in ({}, []):
        result = {
            "experience_lines": [],
            "education_lines": [],
            **{key: [] if key in {"languages", "skills"} else "" for key in PROFILE_SIGNAL_FIELD_KEYS},
            "primary_email_metadata": {},
            "profile_capture_kind": "",
            "profile_capture_source_path": "",
        }
    else:
        item = _pick_profile_source_item(
            source_payload,
            payload=payload,
            metadata=metadata,
            source_path=source_path_obj,
        )
        if not isinstance(item, dict) or not item:
            result = {
                "experience_lines": [],
                "education_lines": [],
                **{key: [] if key in {"languages", "skills"} else "" for key in PROFILE_SIGNAL_FIELD_KEYS},
                "primary_email_metadata": {},
                "profile_capture_kind": "",
                "profile_capture_source_path": "",
            }
        else:
            location_payload = dict(item.get("location") or {}) if isinstance(item.get("location"), dict) else {}
            primary_email_snapshot = _profile_primary_email_snapshot(source_payload, item)
            result = {
                "experience_lines": profile_experience_lines(item),
                "education_lines": profile_education_lines(item),
                "headline": _first_non_empty_text(item.get("headline")),
                "summary": _first_non_empty_text(item.get("summary")),
                "about": _first_non_empty_text(item.get("about")),
                "profile_location": _first_non_empty_text(location_payload.get("linkedinText"), item.get("locationName")),
                "public_identifier": _first_non_empty_text(
                    item.get("publicIdentifier"),
                    item.get("public_identifier"),
                    item.get("username"),
                ),
                "avatar_url": _profile_avatar_url(item),
                "photo_url": _profile_photo_url(item),
                "primary_email": str(primary_email_snapshot.get("email") or "").strip(),
                "primary_email_metadata": normalized_primary_email_metadata(primary_email_snapshot.get("metadata")),
                "languages": _profile_text_list(item.get("languages")),
                "skills": _profile_text_list(item.get("skills"), item.get("topSkills")),
                "profile_capture_kind": _profile_capture_kind_from_path(path_text),
                "profile_capture_source_path": path_text,
            }
    if cache is not None:
        cache[cache_key] = dict(result)
    return result


def profile_experience_lines(profile_item: dict[str, Any]) -> list[str]:
    formatted: list[str] = []
    seen: set[str] = set()
    experience_entries: list[dict[str, Any]] = []
    for raw_collection in (
        profile_item.get("experience"),
        profile_item.get("currentPosition"),
        profile_item.get("currentPositions"),
    ):
        if isinstance(raw_collection, list):
            experience_entries.extend(
                dict(item or {}) for item in raw_collection if isinstance(item, dict)
            )
        elif isinstance(raw_collection, dict):
            experience_entries.append(dict(raw_collection))
    for raw_entry in experience_entries[:8]:
        entry = dict(raw_entry or {}) if isinstance(raw_entry, dict) else {}
        title = _first_non_empty_text(entry.get("title"), entry.get("position"))
        company = _first_non_empty_text(entry.get("companyName"), entry.get("company"))
        end_date = entry.get("endDate")
        if not end_date and bool(entry.get("current")):
            end_date = {"text": "Present"}
        date_range = _profile_date_range(
            entry.get("startDate") or entry.get("startedOn"),
            end_date,
        )
        line = ", ".join(part for part in [date_range, company, title] if part)
        if not line:
            continue
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        formatted.append(line)
    return formatted


def profile_education_lines(profile_item: dict[str, Any]) -> list[str]:
    raw_items = list(
        profile_item.get("educations")
        or profile_item.get("education")
        or profile_item.get("profileTopEducation")
        or []
    )
    formatted: list[str] = []
    seen: set[str] = set()
    for raw_entry in raw_items[:6]:
        entry = dict(raw_entry or {}) if isinstance(raw_entry, dict) else {}
        degree = _clean_profile_text(entry.get("degreeName"), entry.get("degree"))
        school = _clean_profile_school_name(entry)
        field = _clean_profile_text(entry.get("fieldOfStudy"), entry.get("field"))
        date_range = _profile_date_range(entry.get("startDate"), entry.get("endDate"))
        line = ", ".join(part for part in [date_range, degree, school, field] if part)
        if not line and school:
            line = school
        if not line:
            continue
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        formatted.append(line)
    return formatted


def _read_json_payload(path: Path) -> Any:
    signature = _json_file_cache_signature(path)
    if signature is None:
        return {}
    path_text, mtime_ns, size = signature
    return _read_json_payload_cached(path_text, mtime_ns, size)


def _json_file_cache_signature(path: Path) -> tuple[str, int, int] | None:
    try:
        resolved = path.resolve()
        stat = resolved.stat()
    except OSError:
        return None
    return str(resolved), int(stat.st_mtime_ns), int(stat.st_size)


@lru_cache(maxsize=32)
def _read_json_payload_cached(path_text: str, mtime_ns: int, size: int) -> Any:
    del mtime_ns, size
    try:
        payload = json.loads(Path(path_text).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload


def _profile_snapshot_cache_key(
    path_text: str,
    *,
    payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> str:
    selector = _profile_selector_key(payload=payload, metadata=metadata)
    return f"{path_text}::{selector}" if selector else path_text


def _profile_selector_key(
    *,
    payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> str:
    payload = dict(payload or {})
    metadata = dict(metadata or payload.get("metadata") or {})
    profile_url = _normalize_profile_lookup_url(candidate_profile_lookup_url(payload, metadata))
    if profile_url:
        return profile_url
    member_key = _candidate_member_key(payload, metadata)
    if member_key:
        return f"member:{member_key}"
    display_name = _normalize_name_key(
        _first_non_empty_text(
            payload.get("display_name"),
            payload.get("name_en"),
            payload.get("full_name"),
            metadata.get("display_name"),
            metadata.get("name_en"),
            metadata.get("full_name"),
        )
    )
    return f"name:{display_name}" if display_name else ""


def _best_companion_profile_snapshot(
    source_path: str,
    *,
    payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    companion_snapshots: list[dict[str, Any]] = []
    for companion_path in _candidate_companion_source_paths(
        source_path,
        payload=payload,
        metadata=metadata,
    ):
        companion = profile_snapshot_from_source_path(
            companion_path,
            cache=cache,
            payload=payload,
            metadata=metadata,
        )
        if _timeline_has_useful_content(companion):
            companion_snapshots.append(companion)
    if not companion_snapshots:
        return {}
    ranked = sorted(
        companion_snapshots,
        key=_profile_timeline_signal_score,
        reverse=True,
    )
    merged = dict(ranked[0] or {})
    for snapshot in ranked[1:]:
        merged = _merge_profile_timeline_snapshots(merged, snapshot)
    return merged


def _candidate_companion_source_paths(
    source_path: str,
    *,
    payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[str]:
    path_text = _resolved_profile_source_path(source_path)
    if not path_text:
        return []
    candidates: list[str] = []
    if "_visible.json" in path_text:
        candidates.append(path_text.replace("_visible.json", "_merged.json"))
        candidates.append(path_text.replace("_visible.json", "_raw.json"))
    if "_raw.json" in path_text:
        candidates.append(path_text.replace("_raw.json", "_merged.json"))
    candidates.extend(
        _candidate_search_seed_raw_source_paths(
            path_text,
            payload=payload,
            metadata=metadata,
        )
    )
    candidates.extend(_candidate_snapshot_profile_queue_source_paths(path_text))
    seen: set[str] = set()
    existing: list[str] = []
    for item in candidates:
        normalized = str(item or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        if Path(normalized).exists():
            existing.append(normalized)
    return existing


def _candidate_search_seed_raw_source_paths(
    source_path: str,
    *,
    payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[str]:
    normalized_source_path = _resolved_profile_source_path(source_path)
    source = Path(normalized_source_path).expanduser()
    if not source.exists():
        return []
    discovery_dir = None
    if source.name in {"summary.json", "entries.json"} and source.parent.name == "search_seed_discovery":
        discovery_dir = source.parent
    else:
        for parent in source.parents:
            if parent.name == "search_seed_discovery":
                discovery_dir = parent
                break
    if discovery_dir is None:
        return []
    summary_path = discovery_dir / "summary.json"
    summary_payload = _read_json_payload(summary_path)
    if not isinstance(summary_payload, dict):
        return []
    payload = dict(payload or {})
    metadata = dict(metadata or payload.get("metadata") or {})
    seed_source_type = _first_non_empty_text(
        metadata.get("seed_source_type"),
        payload.get("seed_source_type"),
    ).lower()
    seed_query = _first_non_empty_text(
        metadata.get("seed_query"),
        payload.get("seed_query"),
        metadata.get("source_query"),
        payload.get("source_query"),
    )
    ranked_paths: list[tuple[int, str]] = []
    for raw_item in list(summary_payload.get("query_summaries") or []):
        query_summary = dict(raw_item or {}) if isinstance(raw_item, dict) else {}
        raw_path = _first_non_empty_text(query_summary.get("raw_path"))
        if raw_path:
            score = 0
            mode = _first_non_empty_text(query_summary.get("mode")).lower()
            query_text = _first_non_empty_text(
                query_summary.get("effective_query_text"),
                query_summary.get("query"),
            )
            if seed_source_type and mode == seed_source_type:
                score += 4
            if seed_query and query_text == seed_query:
                score += 3
            if not seed_source_type and mode == "harvest_profile_search":
                score += 1
            ranked_paths.append((score, raw_path))
        probe_raw_path = _first_non_empty_text(
            dict(query_summary.get("probe") or {}).get("probe_raw_path")
        )
        if probe_raw_path:
            score = 0
            mode = _first_non_empty_text(query_summary.get("mode")).lower()
            query_text = _first_non_empty_text(
                query_summary.get("effective_query_text"),
                query_summary.get("query"),
            )
            if seed_source_type and mode == seed_source_type:
                score += 3
            if seed_query and query_text == seed_query:
                score += 2
            ranked_paths.append((score, probe_raw_path))
    ranked_paths.sort(key=lambda item: item[0], reverse=True)
    return [path for _, path in ranked_paths]


def _candidate_snapshot_profile_queue_source_paths(source_path: str) -> list[str]:
    snapshot_dir = _snapshot_dir_from_source_path(source_path)
    if snapshot_dir is None:
        return []
    harvest_profile_dir = snapshot_dir / "harvest_profiles"
    if not harvest_profile_dir.exists():
        return []
    return [str(path) for path in sorted(harvest_profile_dir.glob("*.queue_dataset_items.json"))]


def _snapshot_dir_from_source_path(source_path: str) -> Path | None:
    return resolve_snapshot_dir_from_source_path(source_path)


def _resolved_profile_source_path(source_path: Any) -> str:
    normalized = str(source_path or "").strip()
    if not normalized:
        return ""
    resolved = resolve_source_path_in_runtime(normalized)
    return str(resolved or normalized).strip()


def _timeline_has_useful_content(snapshot: dict[str, Any]) -> bool:
    if not isinstance(snapshot, dict):
        return False
    return bool(
        normalized_text_lines(snapshot.get("experience_lines"))
        or normalized_text_lines(snapshot.get("education_lines"))
        or any(_has_signal_value(snapshot.get(key)) for key in PROFILE_SIGNAL_FIELD_KEYS)
    )


def _profile_timeline_signal_score(snapshot: dict[str, Any]) -> int:
    if not isinstance(snapshot, dict):
        return 0
    score = 0
    score += len(normalized_text_lines(snapshot.get("experience_lines"))) * 8
    score += len(normalized_text_lines(snapshot.get("education_lines"))) * 6
    for key in ("headline", "summary", "about", "profile_location", "public_identifier"):
        if _has_signal_value(snapshot.get(key)):
            score += 3
    for key in ("avatar_url", "photo_url", "primary_email"):
        if _has_signal_value(snapshot.get(key)):
            score += 2
    for key in ("languages", "skills"):
        score += len(_profile_text_list(snapshot.get(key)))
    return score


def _profile_timeline_source_priority(source_kind: str) -> int:
    normalized = str(source_kind or "").strip().lower()
    if normalized == "profile_source_path":
        return 3
    if normalized == "profile_registry":
        return 2
    if normalized == "source_path":
        return 1
    return 0


def _select_best_profile_timeline_source(
    candidates: list[tuple[str, str, dict[str, Any]]],
) -> tuple[str, str, dict[str, Any]]:
    ranked_candidates = sorted(
        [
            (kind, path, dict(snapshot or {}))
            for kind, path, snapshot in candidates
            if _timeline_has_useful_content(snapshot)
        ],
        key=lambda item: (
            _profile_timeline_signal_score(item[2]),
            _profile_timeline_source_priority(item[0]),
        ),
        reverse=True,
    )
    if not ranked_candidates:
        return "", "", {}
    selected_kind, selected_path, merged = ranked_candidates[0]
    for _, _, snapshot in ranked_candidates[1:]:
        merged = _merge_profile_timeline_snapshots(merged, snapshot)
    return selected_kind, selected_path, merged


def _merge_profile_timeline_snapshots(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    if not overlay:
        return dict(base or {})
    merged = dict(base or {})
    overlay = dict(overlay or {})
    if normalized_text_lines(overlay.get("experience_lines")) and not normalized_text_lines(merged.get("experience_lines")):
        merged["experience_lines"] = normalized_text_lines(overlay.get("experience_lines"))
    if normalized_text_lines(overlay.get("education_lines")) and not normalized_text_lines(merged.get("education_lines")):
        merged["education_lines"] = normalized_text_lines(overlay.get("education_lines"))
    for key in PROFILE_SIGNAL_FIELD_KEYS:
        if _has_signal_value(overlay.get(key)) and not _has_signal_value(merged.get(key)):
            merged[key] = overlay.get(key)
    overlay_email_metadata = normalized_primary_email_metadata(overlay.get("primary_email_metadata"))
    merged_email_metadata = normalized_primary_email_metadata(merged.get("primary_email_metadata"))
    if (
        overlay_email_metadata
        and str(overlay.get("primary_email") or "").strip()
        and str(overlay.get("primary_email") or "").strip() == str(merged.get("primary_email") or "").strip()
        and not merged_email_metadata
    ):
        merged["primary_email_metadata"] = overlay_email_metadata
    base_capture_kind = _first_non_empty_text(merged.get("profile_capture_kind")).lower()
    overlay_capture_kind = _first_non_empty_text(overlay.get("profile_capture_kind")).lower()
    if _profile_capture_kind_priority(overlay_capture_kind) > _profile_capture_kind_priority(base_capture_kind):
        merged["profile_capture_kind"] = overlay_capture_kind
        merged["profile_capture_source_path"] = _first_non_empty_text(overlay.get("profile_capture_source_path"))
    elif not _first_non_empty_text(merged.get("profile_capture_source_path")):
        merged["profile_capture_source_path"] = _first_non_empty_text(overlay.get("profile_capture_source_path"))
    return merged


def _profile_capture_kind_priority(value: str) -> int:
    return _PROFILE_CAPTURE_KIND_PRIORITY.get(str(value or "").strip().lower(), 0)


def _pick_profile_source_item(
    source_payload: Any,
    *,
    payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    source_path: Path | None = None,
) -> dict[str, Any]:
    if isinstance(source_payload, dict):
        if isinstance(source_payload.get("item"), dict):
            return dict(source_payload.get("item") or {})
        for key in ("items", "results", "candidates"):
            candidate_items = source_payload.get(key)
            if isinstance(candidate_items, list):
                matched = _select_roster_profile_item(
                    candidate_items,
                    payload=payload,
                    metadata=metadata,
                    source_path=source_path,
                )
                if matched:
                    return matched
        return dict(source_payload)
    if isinstance(source_payload, list):
        return _select_roster_profile_item(
            source_payload,
            payload=payload,
            metadata=metadata,
            source_path=source_path,
        )
    return {}


def _select_roster_profile_item(
    items: list[Any],
    *,
    payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    source_path: Path | None = None,
) -> dict[str, Any]:
    normalized_url = _normalize_profile_lookup_url(candidate_profile_lookup_url(dict(payload or {}), dict(metadata or {})))
    member_key = _candidate_member_key(dict(payload or {}), dict(metadata or {}))
    display_name = _normalize_name_key(
        _first_non_empty_text(
            dict(payload or {}).get("display_name"),
            dict(payload or {}).get("name_en"),
            dict(payload or {}).get("full_name"),
            dict(metadata or {}).get("display_name"),
            dict(metadata or {}).get("name_en"),
            dict(metadata or {}).get("full_name"),
        )
    )
    source_index = _profile_source_item_index(source_path)
    for lookup_key in (
        f"url:{normalized_url}" if normalized_url else "",
        f"member:{member_key}" if member_key else "",
        f"name:{display_name}" if display_name else "",
    ):
        if lookup_key and lookup_key in source_index:
            indexed_item = source_index.get(lookup_key)
            if isinstance(indexed_item, dict) and indexed_item:
                return dict(indexed_item)
    for raw_item in items:
        item = dict(raw_item or {}) if isinstance(raw_item, dict) else {}
        if not item:
            continue
        item_url = _normalize_profile_lookup_url(
            _first_non_empty_text(
                item.get("linkedin_url"),
                item.get("profile_url"),
                item.get("linkedinUrl"),
                item.get("profileUrl"),
            )
        )
        if normalized_url and item_url and normalized_url == item_url:
            return item
        item_member_key = _candidate_member_key(item, item)
        if member_key and item_member_key and member_key == item_member_key:
            return item
        item_name = _normalize_name_key(
            _first_non_empty_text(
                item.get("display_name"),
                item.get("name_en"),
                item.get("full_name"),
                item.get("fullName"),
            )
        )
        if display_name and item_name and display_name == item_name:
            return item
    return {}


def _profile_source_item_index(source_path: Path | None) -> dict[str, dict[str, Any]]:
    if source_path is None:
        return {}
    signature = _json_file_cache_signature(source_path)
    if signature is None:
        return {}
    path_text, mtime_ns, size = signature
    return _profile_source_item_index_cached(path_text, mtime_ns, size)


@lru_cache(maxsize=32)
def _profile_source_item_index_cached(path_text: str, mtime_ns: int, size: int) -> dict[str, dict[str, Any]]:
    source_payload = _read_json_payload_cached(path_text, mtime_ns, size)
    candidate_items = _profile_source_candidate_items(source_payload)
    if not candidate_items:
        return {}
    index: dict[str, dict[str, Any]] = {}
    for raw_item in candidate_items:
        item = dict(raw_item or {}) if isinstance(raw_item, dict) else {}
        if not item:
            continue
        for key in _profile_source_item_lookup_keys(item):
            index.setdefault(key, item)
    return index


def _profile_source_candidate_items(source_payload: Any) -> list[Any]:
    if isinstance(source_payload, dict):
        for key in ("items", "results", "candidates"):
            candidate_items = source_payload.get(key)
            if isinstance(candidate_items, list):
                return candidate_items
        return []
    if isinstance(source_payload, list):
        return source_payload
    return []


def _profile_source_item_lookup_keys(item: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    item_url = _normalize_profile_lookup_url(
        _first_non_empty_text(
            item.get("linkedin_url"),
            item.get("profile_url"),
            item.get("linkedinUrl"),
            item.get("profileUrl"),
        )
    )
    if item_url:
        keys.append(f"url:{item_url}")
    item_member_key = _candidate_member_key(item, item)
    if item_member_key:
        keys.append(f"member:{item_member_key}")
    item_name = _normalize_name_key(
        _first_non_empty_text(
            item.get("display_name"),
            item.get("name_en"),
            item.get("full_name"),
            item.get("fullName"),
        )
    )
    if item_name:
        keys.append(f"name:{item_name}")
    return keys


def _normalize_profile_lookup_url(value: str) -> str:
    text = _first_non_empty_text(value).rstrip("/")
    return text.lower()


def _profile_capture_kind_from_path(value: Any) -> str:
    path_text = str(value or "").strip()
    if not path_text:
        return ""
    lowered = path_text.lower().replace("\\", "/")
    if "/harvest_profiles/" in lowered:
        return "provider_profile_detail"
    if "/search_seed_discovery/" in lowered or "/harvest_profile_search/" in lowered:
        return "search_seed_preview"
    if "/harvest_company_employees/" in lowered:
        return "roster_baseline_preview"
    return ""


def _candidate_member_key(payload: dict[str, Any], metadata: dict[str, Any] | None = None) -> str:
    metadata = dict(metadata or {})
    direct = _first_non_empty_text(
        payload.get("member_key"),
        payload.get("member_id"),
        payload.get("public_identifier"),
        payload.get("publicIdentifier"),
        metadata.get("member_key"),
        metadata.get("member_id"),
        metadata.get("public_identifier"),
        metadata.get("publicIdentifier"),
    )
    if direct:
        return direct.lower()
    profile_url = candidate_profile_lookup_url(payload, metadata)
    matched = re.search(r"/in/([^/?#]+)/?", profile_url or "", flags=re.IGNORECASE)
    return matched.group(1).lower() if matched else ""


def _normalize_name_key(value: str) -> str:
    return re.sub(r"\s+", " ", _first_non_empty_text(value)).strip().lower()


def _profile_date_range(start: Any, end: Any) -> str:
    start_label = _profile_date_label(start)
    end_label = _profile_date_label(end)
    if start_label and end_label:
        return f"{start_label}~{end_label}"
    return start_label or end_label


def _profile_date_label(value: Any) -> str:
    if isinstance(value, dict):
        year = str(value.get("year") or "").strip()
        text = " ".join(str(value.get("text") or "").split()).strip()
        if year:
            return year
        if re.search(r"present|current|now|至今", text, flags=re.IGNORECASE):
            return "Present"
        matched_year = re.search(r"(19|20)\d{2}", text)
        if matched_year:
            return matched_year.group(0)
        return text
    text = " ".join(str(value or "").split()).strip()
    if re.search(r"present|current|now|至今", text, flags=re.IGNORECASE):
        return "Present"
    matched_year = re.search(r"(19|20)\d{2}", text)
    if matched_year:
        return matched_year.group(0)
    return text


def _first_non_empty_text(*values: Any) -> str:
    for value in values:
        text = " ".join(str(value or "").split()).strip()
        if text:
            return text
    return ""


_PROFILE_JUNK_TEXT = {
    "",
    "null",
    "none",
    "n/a",
    "na",
    "unknown",
    "undefined",
    "-",
    "--",
}


def _clean_profile_text(*values: Any) -> str:
    text = _first_non_empty_text(*values)
    if not text:
        return ""
    normalized = text.strip().strip(".").lower()
    if normalized in _PROFILE_JUNK_TEXT:
        return ""
    return text


def _clean_profile_school_name(entry: dict[str, Any]) -> str:
    school = _clean_profile_text(entry.get("schoolName"), entry.get("school"))
    normalized_school = school.strip().lower()
    if normalized_school and normalized_school not in {"node", "school"}:
        return school
    school_url = _clean_profile_text(entry.get("schoolLinkedinUrl"), entry.get("schoolUrl"))
    inferred_from_url = _school_name_from_linkedin_url(school_url)
    if inferred_from_url:
        return inferred_from_url
    if normalized_school in {"node", "school"}:
        return ""
    return school


def _school_name_from_linkedin_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    matched = re.search(r"/school/([^/?#]+)/?", text, flags=re.IGNORECASE)
    if matched is None:
        return ""
    slug = matched.group(1).strip().strip("/")
    if not slug:
        return ""
    words = [segment for segment in slug.replace("_", "-").split("-") if segment]
    if not words:
        return ""
    return " ".join(word[:1].upper() + word[1:] for word in words)


def _profile_text_list(*values: Any) -> list[str]:
    collected: list[str] = []
    seen: set[str] = set()
    for value in values:
        if isinstance(value, str):
            items = re.split(r"[•,;/\n]+", value)
        elif isinstance(value, list):
            items = value
        elif value in ("", None):
            items = []
        else:
            items = [value]
        for item in items:
            if isinstance(item, dict):
                text = _first_non_empty_text(
                    item.get("name"),
                    item.get("language"),
                    item.get("title"),
                    item.get("value"),
                    item.get("text"),
                )
            else:
                text = _first_non_empty_text(item)
            key = text.lower()
            if not text or key in seen:
                continue
            seen.add(key)
            collected.append(text)
    return collected


def _profile_avatar_url(profile_item: dict[str, Any]) -> str:
    picture_payload = dict(profile_item.get("profilePicture") or {}) if isinstance(profile_item.get("profilePicture"), dict) else {}
    photo_payload = dict(profile_item.get("photo") or {}) if isinstance(profile_item.get("photo"), dict) else {}
    picture_sizes = list(picture_payload.get("sizes") or []) if isinstance(picture_payload.get("sizes"), list) else []
    photo_sizes = list(photo_payload.get("sizes") or []) if isinstance(photo_payload.get("sizes"), list) else []
    return _first_non_empty_text(
        picture_payload.get("url"),
        photo_payload.get("url"),
        *[dict(item or {}).get("url") for item in picture_sizes],
        *[dict(item or {}).get("url") for item in photo_sizes],
        profile_item.get("photoUrl"),
        profile_item.get("pictureUrl"),
        profile_item.get("picture"),
        profile_item.get("profilePictureUrl"),
        profile_item.get("image"),
        profile_item.get("avatar"),
        profile_item.get("avatar_url"),
    )


def _profile_photo_url(profile_item: dict[str, Any]) -> str:
    return _first_non_empty_text(
        profile_item.get("photoUrl"),
        _profile_avatar_url(profile_item),
        profile_item.get("pictureUrl"),
        profile_item.get("profilePictureUrl"),
        profile_item.get("avatar_url"),
    )


def _profile_primary_email_snapshot(source_payload: Any, profile_item: dict[str, Any]) -> dict[str, Any]:
    emails = list(profile_item.get("emails") or []) if isinstance(profile_item.get("emails"), list) else []
    best_metadata: dict[str, Any] = {}
    for item in emails:
        entry = dict(item or {}) if isinstance(item, dict) else {}
        email = _first_non_empty_text(entry.get("email"), entry.get("address"), entry.get("value"))
        if not email:
            continue
        metadata = normalized_primary_email_metadata(
            {
                "source": "harvestapi",
                "status": entry.get("status"),
                "qualityScore": entry.get("qualityScore"),
                "foundInLinkedInProfile": entry.get("foundInLinkedInProfile"),
            }
        )
        if _provider_email_entry_is_acceptable(entry):
            return {"email": email, "metadata": metadata}
        if metadata and not best_metadata:
            best_metadata = metadata
    if emails:
        return {"email": "", "metadata": best_metadata}
    email = _first_non_empty_text(
        profile_item.get("email"),
        profile_item.get("emailAddress"),
        profile_item.get("primaryEmail"),
    )
    if not email:
        return {"email": "", "metadata": {}}
    metadata = normalized_primary_email_metadata(
        profile_item.get("primary_email_metadata")
        or (
            dict(source_payload or {}).get("primary_email_metadata")
            if isinstance(source_payload, dict)
            else {}
        )
    )
    return {"email": email, "metadata": metadata}


def _provider_email_entry_is_acceptable(entry: dict[str, Any]) -> bool:
    return _harvest_email_is_publishable(
        {
            "source": "harvestapi",
            "status": entry.get("status"),
            "qualityScore": entry.get("qualityScore"),
            "foundInLinkedInProfile": entry.get("foundInLinkedInProfile"),
        }
    )


def _coerce_numeric_email_quality_score(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _coerce_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _has_signal_value(value: Any) -> bool:
    if isinstance(value, list):
        return any(_first_non_empty_text(item) for item in value)
    return bool(_first_non_empty_text(value))
