from __future__ import annotations

from hashlib import sha1
from pathlib import Path
from typing import Any


_UNRECOVERABLE_ERROR_PATTERNS = (
    "member is restricted",
    "restricted profile",
    "not found",
    "404",
    "forbidden",
    "private profile",
)


def extract_profile_registry_aliases_from_payload(
    payload: dict[str, Any] | None,
    *,
    requested_url: str = "",
) -> dict[str, Any]:
    raw_payload = dict(payload or {})
    request_metadata = raw_payload.get("_harvest_request")
    request_payload = request_metadata if isinstance(request_metadata, dict) else {}
    item_payload = raw_payload.get("item")
    item = item_payload if isinstance(item_payload, dict) else raw_payload
    candidate_urls = _dedupe_url_list(
        [
            requested_url,
            request_payload.get("profile_url"),
            request_payload.get("raw_linkedin_url"),
            request_payload.get("sanity_linkedin_url"),
            raw_payload.get("raw_linkedin_url"),
            raw_payload.get("sanity_linkedin_url"),
            item.get("linkedinUrl") if isinstance(item, dict) else "",
            item.get("profileUrl") if isinstance(item, dict) else "",
            item.get("url") if isinstance(item, dict) else "",
            item.get("raw_linkedin_url") if isinstance(item, dict) else "",
            item.get("sanity_linkedin_url") if isinstance(item, dict) else "",
        ]
    )
    raw_linkedin_url = (
        _first_non_empty(
            [
                requested_url,
                request_payload.get("profile_url"),
                request_payload.get("raw_linkedin_url"),
                raw_payload.get("raw_linkedin_url"),
                item.get("raw_linkedin_url") if isinstance(item, dict) else "",
            ]
        )
        or (candidate_urls[0] if candidate_urls else "")
    )
    sanity_linkedin_url = (
        _first_non_empty(
            [
                request_payload.get("sanity_linkedin_url"),
                raw_payload.get("sanity_linkedin_url"),
                item.get("sanity_linkedin_url") if isinstance(item, dict) else "",
                item.get("linkedinUrl") if isinstance(item, dict) else "",
                item.get("profileUrl") if isinstance(item, dict) else "",
                item.get("url") if isinstance(item, dict) else "",
            ]
        )
        or (candidate_urls[0] if candidate_urls else "")
    )
    return {
        "alias_urls": candidate_urls,
        "raw_linkedin_url": raw_linkedin_url,
        "sanity_linkedin_url": sanity_linkedin_url,
    }


def profile_cache_path_candidates(harvest_dir: Path, profile_url: str, normalized_profile_url: str = "") -> list[Path]:
    variants = _url_variants(profile_url, normalized_profile_url=normalized_profile_url)
    seen: set[Path] = set()
    candidates: list[Path] = []
    for variant in variants:
        cache_key = sha1(variant.encode("utf-8")).hexdigest()[:16]
        candidate = harvest_dir / f"{cache_key}.json"
        if candidate in seen:
            continue
        seen.add(candidate)
        candidates.append(candidate)
    return candidates


def classify_harvest_profile_payload_status(payload: dict[str, Any] | None) -> str:
    raw_payload = dict(payload or {})
    item_payload = raw_payload.get("item")
    item = item_payload if isinstance(item_payload, dict) else raw_payload
    if _looks_like_harvest_profile_item(item):
        return "fetched"
    if _payload_has_unrecoverable_error(raw_payload):
        return "unrecoverable"
    if _payload_has_any_error(raw_payload):
        return "failed_retryable"
    return "failed_retryable"


def _looks_like_harvest_profile_item(item: dict[str, Any] | None) -> bool:
    payload = dict(item or {})
    if not payload:
        return False
    if payload.get("linkedinUrl") or payload.get("profileUrl") or payload.get("url"):
        return True
    if payload.get("publicIdentifier") or payload.get("public_identifier"):
        return True
    if payload.get("headline") or payload.get("occupation"):
        return True
    if payload.get("experience") or payload.get("educations") or payload.get("education"):
        return True
    return False


def _payload_has_any_error(payload: dict[str, Any]) -> bool:
    lowered = _collect_error_texts(payload)
    return bool(lowered)


def _payload_has_unrecoverable_error(payload: dict[str, Any]) -> bool:
    lowered = _collect_error_texts(payload)
    return any(pattern in text for text in lowered for pattern in _UNRECOVERABLE_ERROR_PATTERNS)


def _collect_error_texts(payload: dict[str, Any]) -> list[str]:
    item_payload = payload.get("item")
    item = item_payload if isinstance(item_payload, dict) else payload
    values: list[str] = []
    for candidate in [
        payload.get("error"),
        payload.get("message"),
        item.get("error") if isinstance(item, dict) else "",
        item.get("message") if isinstance(item, dict) else "",
    ]:
        value = str(candidate or "").strip().lower()
        if value:
            values.append(value)
    errors = payload.get("errors")
    if isinstance(errors, list):
        for error_payload in errors:
            if isinstance(error_payload, dict):
                for key in ("error", "message", "status"):
                    value = str(error_payload.get(key) or "").strip().lower()
                    if value:
                        values.append(value)
            else:
                value = str(error_payload or "").strip().lower()
                if value:
                    values.append(value)
    return values


def _url_variants(profile_url: str, *, normalized_profile_url: str = "") -> list[str]:
    normalized_input = str(profile_url or "").strip()
    normalized_registry = str(normalized_profile_url or "").strip()
    candidates: list[str] = []
    for value in [normalized_input, normalized_registry]:
        if not value:
            continue
        trimmed = value.rstrip("/")
        candidates.append(value)
        candidates.append(trimmed)
        if trimmed:
            candidates.append(f"{trimmed}/")
    return [value for value in _dedupe_url_list(candidates) if value]


def _dedupe_url_list(values: list[Any]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        normalized = str(value or "").strip()
        if not normalized:
            continue
        lower_key = normalized.lower()
        if lower_key in seen:
            continue
        seen.add(lower_key)
        deduped.append(normalized)
    return deduped


def _first_non_empty(values: list[Any]) -> str:
    for value in list(values or []):
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return ""
