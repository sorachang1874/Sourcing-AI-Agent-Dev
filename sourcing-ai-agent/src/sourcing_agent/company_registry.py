from __future__ import annotations

import re
from typing import Any


def normalize_company_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


BUILTIN_COMPANY_IDENTITIES: dict[str, dict[str, Any]] = {
    "anthropic": {
        "canonical_name": "Anthropic",
        "linkedin_slug": "anthropicresearch",
        "domain": "anthropic.com",
        "aliases": ["anthropicresearch"],
        "resolver": "builtin",
        "confidence": "high",
        "local_asset_available": True,
        "notes": "Mapped to the local Anthropic asset package and the verified LinkedIn company slug.",
    },
    "xai": {
        "canonical_name": "xAI",
        "linkedin_slug": "xai",
        "domain": "x.ai",
        "aliases": ["x.ai", "x ai"],
        "resolver": "builtin",
        "confidence": "high",
        "notes": "LinkedIn company slug was validated via live company roster calls.",
    },
    "thinkingmachineslab": {
        "canonical_name": "Thinking Machines Lab",
        "linkedin_slug": "thinkingmachinesai",
        "domain": "thinkingmachines.ai",
        "aliases": ["thinking machines", "thinking machines ai", "thinkingmachinesai", "tml"],
        "resolver": "builtin",
        "confidence": "high",
        "notes": "LinkedIn company slug was validated via live Harvest company-employees calls.",
    },
    "humansand": {
        "canonical_name": "Humans&",
        "linkedin_slug": "humansand",
        "domain": "humansand.ai",
        "aliases": ["humans and", "humansand"],
        "resolver": "builtin",
        "confidence": "high",
        "notes": "LinkedIn company slug was observed in live Harvest company-employees and profile runs.",
    },
}

COMPANY_ALIASES: dict[str, str] = {
    "xai": "xai",
    "xaiinc": "xai",
    "xaicorp": "xai",
    "xaicompany": "xai",
    "xaiorg": "xai",
    "xairesearch": "xai",
    "xaiholdings": "xai",
    "xaiincorporated": "xai",
    "xaiartificialintelligence": "xai",
    "xaiartificial": "xai",
    "xaiai": "xai",
    "xaix": "xai",
    "anthropic": "anthropic",
    "anthropicresearch": "anthropic",
    "thinkingmachineslab": "thinkingmachineslab",
    "thinkingmachines": "thinkingmachineslab",
    "thinkingmachinesai": "thinkingmachineslab",
    "tml": "thinkingmachineslab",
    "humansand": "humansand",
    "humansandai": "humansand",
}


for company_key, payload in BUILTIN_COMPANY_IDENTITIES.items():
    COMPANY_ALIASES.setdefault(company_key, company_key)
    canonical_name = str(payload.get("canonical_name") or "").strip()
    linkedin_slug = str(payload.get("linkedin_slug") or "").strip()
    if canonical_name:
        COMPANY_ALIASES.setdefault(normalize_company_key(canonical_name), company_key)
    if linkedin_slug:
        COMPANY_ALIASES.setdefault(normalize_company_key(linkedin_slug), company_key)
    for alias in list(payload.get("aliases") or []):
        alias_key = normalize_company_key(str(alias or ""))
        if alias_key:
            COMPANY_ALIASES.setdefault(alias_key, company_key)


def resolve_company_alias_key(value: str) -> str:
    normalized = normalize_company_key(value)
    if not normalized:
        return ""
    return str(COMPANY_ALIASES.get(normalized) or normalized)


def builtin_company_identity(value: str) -> tuple[str, dict[str, Any] | None]:
    company_key = resolve_company_alias_key(value)
    return company_key, BUILTIN_COMPANY_IDENTITIES.get(company_key)


def infer_target_company_from_text(text: str) -> dict[str, str]:
    normalized_text = " ".join(str(text or "").split()).strip()
    if not normalized_text:
        return {}

    matches: list[tuple[int, int, str, str]] = []
    seen_company_keys: set[str] = set()
    for alias_text, company_key in _company_text_aliases():
        match = re.search(_company_alias_pattern(alias_text), normalized_text, flags=re.IGNORECASE)
        if match is None:
            continue
        seen_company_keys.add(company_key)
        matches.append((match.start(), -len(alias_text), company_key, alias_text))
    if len(seen_company_keys) != 1 or not matches:
        return {}

    matches.sort()
    _, _, company_key, matched_alias = matches[0]
    builtin = BUILTIN_COMPANY_IDENTITIES.get(company_key)
    if builtin is None:
        return {}
    return {
        "company_key": company_key,
        "canonical_name": str(builtin.get("canonical_name") or "").strip(),
        "matched_alias": matched_alias,
    }


def _company_text_aliases() -> list[tuple[str, str]]:
    aliases: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for company_key, payload in BUILTIN_COMPANY_IDENTITIES.items():
        items = [
            str(payload.get("canonical_name") or "").strip(),
            str(payload.get("linkedin_slug") or "").strip(),
            *[str(item or "").strip() for item in list(payload.get("aliases") or [])],
        ]
        for item in items:
            if not item:
                continue
            key = (item.lower(), company_key)
            if key in seen:
                continue
            seen.add(key)
            aliases.append((item, company_key))
    aliases.sort(key=lambda item: (-len(item[0]), item[0].lower()))
    return aliases


def _company_alias_pattern(alias_text: str) -> str:
    chunks: list[str] = []
    for token in re.findall(r"[a-z0-9]+|&", alias_text.lower()):
        if token == "&":
            chunks.append(r"(?:\s*&\s*|\s+and\s+)")
            continue
        if chunks:
            chunks.append(r"[\W_]*")
        chunks.append(re.escape(token))
    body = "".join(chunks).strip()
    if not body:
        return r"$."
    return rf"(?<![a-z0-9]){body}(?![a-z0-9])"
