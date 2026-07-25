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
_PROFILE_WRAPPER_KEYS = ("item", "data", "profile", "result")


def extract_profile_registry_aliases_from_payload(
    payload: dict[str, Any] | None,
    *,
    requested_url: str = "",
) -> dict[str, Any]:
    raw_payload = dict(payload or {})
    layers = _profile_payload_layers(raw_payload)
    request_payload = next(
        (
            dict(layer.get("_harvest_request") or {})
            for layer in layers
            if isinstance(layer.get("_harvest_request"), dict)
        ),
        {},
    )
    item = _first_profile_payload_item(raw_payload)
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
    raw_linkedin_url = _first_non_empty(
        [
            requested_url,
            request_payload.get("profile_url"),
            request_payload.get("raw_linkedin_url"),
            raw_payload.get("raw_linkedin_url"),
            item.get("raw_linkedin_url") if isinstance(item, dict) else "",
        ]
    ) or (candidate_urls[0] if candidate_urls else "")
    sanity_linkedin_url = _first_non_empty(
        [
            request_payload.get("sanity_linkedin_url"),
            raw_payload.get("sanity_linkedin_url"),
            item.get("sanity_linkedin_url") if isinstance(item, dict) else "",
            item.get("linkedinUrl") if isinstance(item, dict) else "",
            item.get("profileUrl") if isinstance(item, dict) else "",
            item.get("url") if isinstance(item, dict) else "",
        ]
    ) or (candidate_urls[0] if candidate_urls else "")
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
    item = _first_profile_payload_item(raw_payload)
    if _looks_like_harvest_profile_item(item):
        return "fetched"
    if _payload_has_unrecoverable_error(raw_payload):
        return "unrecoverable"
    if _payload_has_any_error(raw_payload):
        return "failed_retryable"
    return "failed_retryable"


def harvest_profile_payload_has_usable_content(payload: dict[str, Any] | None) -> bool:
    return classify_harvest_profile_payload_status(payload) == "fetched"


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
    if (
        payload.get("experience")
        or payload.get("experiences")
        or payload.get("positions")
        or payload.get("educations")
        or payload.get("education")
        or payload.get("schools")
    ):
        return True
    return False


def _payload_has_any_error(payload: dict[str, Any]) -> bool:
    lowered = _collect_error_texts(payload)
    return bool(lowered)


def _payload_has_unrecoverable_error(payload: dict[str, Any]) -> bool:
    lowered = _collect_error_texts(payload)
    return any(pattern in text for text in lowered for pattern in _UNRECOVERABLE_ERROR_PATTERNS)


def _collect_error_texts(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for layer in _profile_payload_layers(payload):
        values.extend(_layer_error_texts(layer))
    return values


def _profile_payload_layers(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    layers: list[dict[str, Any]] = []
    current = dict(payload or {})
    seen_ids: set[int] = set()
    depth = 0
    while isinstance(current, dict) and depth < 8:
        current_id = id(current)
        if current_id in seen_ids:
            break
        seen_ids.add(current_id)
        layers.append(current)
        nested = _next_profile_wrapper_layer(current)
        if nested is None:
            break
        current = nested
        depth += 1
    return layers


def _next_profile_wrapper_layer(layer: dict[str, Any]) -> dict[str, Any] | None:
    for key in _PROFILE_WRAPPER_KEYS:
        candidate = layer.get(key)
        if isinstance(candidate, dict) and candidate is not layer:
            return dict(candidate)
    return None


def _first_profile_payload_item(payload: dict[str, Any]) -> dict[str, Any]:
    layers = _profile_payload_layers(payload)
    for layer in reversed(layers):
        if _looks_like_harvest_profile_item(layer):
            return dict(layer)
    return dict(layers[-1]) if layers else {}


def _layer_error_texts(layer: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("error", "message", "detail", "status", "statusText", "errorMessage", "status_code", "code"):
        values.extend(_normalize_error_value(layer.get(key)))
    errors = layer.get("errors")
    if isinstance(errors, list):
        for error_payload in errors:
            values.extend(_normalize_error_value(error_payload))
    return values


def _normalize_error_value(value: Any) -> list[str]:
    if isinstance(value, dict):
        collected: list[str] = []
        for key in ("error", "message", "detail", "status", "statusText", "errorMessage", "status_code", "code"):
            nested = str(value.get(key) or "").strip().lower()
            if nested:
                collected.append(nested)
        return collected
    normalized = str(value or "").strip().lower()
    return [normalized] if normalized else []


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
