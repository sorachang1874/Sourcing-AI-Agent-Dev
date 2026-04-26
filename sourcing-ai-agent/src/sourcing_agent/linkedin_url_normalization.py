from __future__ import annotations

import re
from urllib import parse


def normalize_linkedin_profile_url_key(profile_url: str) -> str:
    raw_value = str(profile_url or "").strip()
    if not raw_value:
        return ""
    if "://" not in raw_value:
        raw_value = f"https://{raw_value}"
    parsed = parse.urlsplit(raw_value)
    netloc = str(parsed.netloc or "").strip().lower()
    path = str(parsed.path or "").strip()
    if not netloc and path:
        reparsed = parse.urlsplit(f"https://{path}")
        netloc = str(reparsed.netloc or "").strip().lower()
        path = str(reparsed.path or "").strip()
    if not netloc:
        return ""
    normalized_path = re.sub(r"/{2,}", "/", path).rstrip("/")
    if not normalized_path:
        normalized_path = "/"
    return f"https://{netloc}{normalized_path}".lower()


def normalize_linkedin_profile_url_list(values: list[str] | None) -> list[str]:
    deduped: list[str] = []
    seen_keys: set[str] = set()
    for value in list(values or []):
        normalized_value = str(value or "").strip()
        normalized_key = normalize_linkedin_profile_url_key(normalized_value)
        if not normalized_key or normalized_key in seen_keys:
            continue
        seen_keys.add(normalized_key)
        deduped.append(normalized_value or normalized_key)
    return deduped
