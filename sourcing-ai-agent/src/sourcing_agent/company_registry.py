from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .asset_paths import iter_company_asset_company_dirs


def normalize_company_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


# Keep this list intentionally small. Generic company discovery should come from
# runtime identity registration / imported assets, not one-off builtin aliases.
BUILTIN_COMPANY_IDENTITIES: dict[str, dict[str, Any]] = {
    "openai": {
        "canonical_name": "OpenAI",
        "linkedin_slug": "openai",
        "domain": "openai.com",
        "aliases": ["open ai", "openai research"],
        "resolver": "builtin",
        "confidence": "high",
        "notes": "Canonical OpenAI identity used for default deterministic request normalization.",
    },
    "google": {
        "canonical_name": "Google",
        "linkedin_slug": "google",
        "domain": "google.com",
        "aliases": ["alphabet", "google deepmind", "deepmind", "google research"],
        "resolver": "builtin",
        "confidence": "high",
        "notes": "Google is treated as the parent scope; DeepMind-like labels can be disambiguated downstream.",
    },
    "meta": {
        "canonical_name": "Meta",
        "linkedin_slug": "meta",
        "domain": "meta.com",
        "aliases": ["meta ai", "facebook", "facebook ai"],
        "resolver": "builtin",
        "confidence": "medium",
        "notes": "Meta and legacy Facebook aliases map to the same canonical parent-company scope.",
    },
    "microsoft": {
        "canonical_name": "Microsoft",
        "linkedin_slug": "microsoft",
        "domain": "microsoft.com",
        "aliases": ["microsoft ai", "msft"],
        "resolver": "builtin",
        "confidence": "high",
        "notes": "Canonical Microsoft identity.",
    },
    "amazon": {
        "canonical_name": "Amazon",
        "linkedin_slug": "amazon",
        "domain": "amazon.com",
        "aliases": ["aws", "amazon web services"],
        "resolver": "builtin",
        "confidence": "medium",
        "notes": "Amazon and AWS aliases map to the canonical parent-company scope.",
    },
    "apple": {
        "canonical_name": "Apple",
        "linkedin_slug": "apple",
        "domain": "apple.com",
        "aliases": ["apple ai"],
        "resolver": "builtin",
        "confidence": "high",
        "notes": "Canonical Apple identity.",
    },
    "nvidia": {
        "canonical_name": "NVIDIA",
        "linkedin_slug": "nvidia",
        "domain": "nvidia.com",
        "aliases": ["nvidia ai", "nvdia"],
        "resolver": "builtin",
        "confidence": "high",
        "notes": "Canonical NVIDIA identity for large-company scoped search planning.",
    },
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
    "langchain": {
        "canonical_name": "LangChain",
        "linkedin_slug": "langchain",
        "domain": "langchain.com",
        "aliases": ["lang chain"],
        "resolver": "builtin",
        "confidence": "medium",
        "notes": "LangChain identity for deterministic target-company inference.",
    },
}

COMPANY_ALIASES: dict[str, str] = {
    "openai": "openai",
    "openairesearch": "openai",
    "openaiai": "openai",
    "google": "google",
    "alphabet": "google",
    "googledeepmind": "google",
    "deepmind": "google",
    "meta": "meta",
    "metaplatforms": "meta",
    "facebook": "meta",
    "microsoft": "microsoft",
    "msft": "microsoft",
    "amazon": "amazon",
    "aws": "amazon",
    "apple": "apple",
    "nvidia": "nvidia",
    "nvdia": "nvidia",
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
    "langchain": "langchain",
    "langchainai": "langchain",
}

_COMPANY_IDENTITY_REGISTRY_FILENAME = "company_identity_registry.json"
_COMPANY_IDENTITY_SEED_CATALOG_FILENAME = "company_identity_seed_catalog.json"
_GENERIC_COMPANY_HEURISTIC_BLOCKLIST = {
    normalize_company_key(value)
    for value in (
        "Reasoning",
        "Coding",
        "Math",
        "Multimodal",
        "Pre-train",
        "Post-train",
        "Audio",
        "Vision",
        "World model",
        "Alignment",
        "Safety",
        "Researcher",
        "Research Engineer",
        "Engineer",
        "Product Manager",
        "Greater China",
        "United States",
        "China",
    )
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
    return str(_combined_company_aliases().get(normalized) or normalized)


def builtin_company_identity(value: str) -> tuple[str, dict[str, Any] | None]:
    company_key = resolve_company_alias_key(value)
    builtin = BUILTIN_COMPANY_IDENTITIES.get(company_key)
    runtime_identity = _discover_local_company_identities().get(company_key)
    if builtin is None:
        return company_key, runtime_identity
    if runtime_identity is None:
        return company_key, builtin
    merged = dict(builtin)
    if not str(merged.get("domain") or "").strip():
        merged["domain"] = str(runtime_identity.get("domain") or "").strip()
    merged["local_asset_available"] = bool(
        runtime_identity.get("local_asset_available") or builtin.get("local_asset_available")
    )
    merged_aliases = list(merged.get("aliases") or [])
    seen_aliases = {normalize_company_key(alias) for alias in merged_aliases if normalize_company_key(alias)}
    for alias in list(runtime_identity.get("aliases") or []):
        alias_key = normalize_company_key(alias)
        if not alias_key or alias_key in seen_aliases:
            continue
        seen_aliases.add(alias_key)
        merged_aliases.append(str(alias))
    if merged_aliases:
        merged["aliases"] = merged_aliases
    return company_key, merged


def infer_target_company_from_text(text: str) -> dict[str, str]:
    normalized_text = " ".join(str(text or "").split()).strip()
    if not normalized_text:
        return {}

    combined_identities = _combined_company_identities()
    matches: list[tuple[int, int, int, int, int, str, str]] = []
    for alias_text, company_key in _company_text_aliases():
        match = re.search(_company_alias_pattern(alias_text), normalized_text, flags=re.IGNORECASE)
        if match is None:
            continue
        matches.append(
            (
                match.start(),
                -len(alias_text),
                _company_identity_priority(company_key, combined_identities.get(company_key)),
                match.end(),
                -match.end(),
                company_key,
                alias_text,
            )
        )
    if not matches:
        return _infer_generic_company_from_text(normalized_text)

    matches.sort()
    start, _, _, end, _, company_key, matched_alias = matches[0]
    for other_start, _, _, other_end, _, other_company_key, _ in matches[1:]:
        if other_company_key == company_key:
            continue
        overlaps = not (other_end <= start or other_start >= end)
        if overlaps:
            continue
        return {}
    builtin = combined_identities.get(company_key)
    if builtin is None:
        return {}
    return {
        "company_key": company_key,
        "canonical_name": str(builtin.get("canonical_name") or "").strip(),
        "matched_alias": matched_alias,
    }


def _infer_generic_company_from_text(text: str) -> dict[str, str]:
    candidates = _generic_company_candidates(text)
    if not candidates:
        return {}
    scored: list[tuple[int, int, str]] = []
    for candidate in candidates:
        score = _generic_company_candidate_score(text, candidate)
        if score < 3:
            continue
        scored.append((score, len(candidate), candidate))
    if not scored:
        return {}
    scored.sort(key=lambda item: (-item[0], -item[1], item[2].lower()))
    top_score, _, top_candidate = scored[0]
    ambiguous = [candidate for score, _, candidate in scored if score == top_score and candidate != top_candidate]
    if ambiguous:
        return {}
    return {
        "company_key": normalize_company_key(top_candidate),
        "canonical_name": top_candidate,
        "matched_alias": "heuristic_company_span",
    }


def _generic_company_candidates(text: str) -> list[str]:
    normalized_text = " ".join(str(text or "").split()).strip()
    if not normalized_text:
        return []
    candidates: list[str] = []
    seen: set[str] = set()
    patterns = (
        r"[\"'“”‘’](?P<value>[A-Za-z][A-Za-z0-9&._/-]{1,40}(?:\s+[A-Za-z][A-Za-z0-9&._/-]{1,40}){0,4})[\"'“”‘’]",
        r"(?<![A-Za-z0-9])(?P<value>[A-Z][A-Za-z0-9&._/-]{1,40}(?:\s+[A-Z][A-Za-z0-9&._/-]{1,40}){0,4})(?![A-Za-z0-9])",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, normalized_text):
            value = " ".join(str(match.group("value") or "").split()).strip(" '\"“”‘’")
            company_key = normalize_company_key(value)
            if not company_key or company_key in seen or company_key in _GENERIC_COMPANY_HEURISTIC_BLOCKLIST:
                continue
            seen.add(company_key)
            candidates.append(value)
    return candidates


def _generic_company_candidate_score(text: str, candidate: str) -> int:
    escaped = re.escape(candidate)
    normalized_candidate = normalize_company_key(candidate)
    score = 0
    if re.search(
        rf"(?:帮我找|帮我寻找|给我|我想找|我想要|我想了解|寻找|找)\s*[\"'“”‘’]?\s*{escaped}",
        text,
        flags=re.IGNORECASE,
    ):
        score += 3
    if re.search(
        rf"{escaped}\s*(?:的|里|这家公司|这家|公司|团队|成员|员工|roster|people|employee|employees|member|members|做|负责|从事)",
        text,
        flags=re.IGNORECASE,
    ):
        score += 2
    if re.search(rf"(?:^|\s){escaped}(?:\s|$)", text, flags=re.IGNORECASE):
        score += 1
    if len(candidate.split()) >= 2 or re.search(
        r"(?:ai|labs?|lab|intelligence|robotics?|systems?|research|machine|dynamics|chain|superintelligence|inc)$",
        candidate,
        flags=re.IGNORECASE,
    ):
        score += 1
    if normalized_candidate in _GENERIC_COMPANY_HEURISTIC_BLOCKLIST:
        return 0
    return score


def _company_text_aliases() -> list[tuple[str, str]]:
    aliases: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for company_key, payload in _combined_company_identities().items():
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


def _company_identity_priority(company_key: str, payload: dict[str, Any] | None) -> int:
    if company_key in BUILTIN_COMPANY_IDENTITIES:
        return 0
    resolver = str((payload or {}).get("resolver") or "").strip().lower()
    if resolver == "seed_catalog":
        return 1
    if resolver == "local_asset_identity":
        return 2
    return 3


def _default_runtime_dir() -> Path:
    override = str(os.environ.get("SOURCING_COMPANY_REGISTRY_RUNTIME_DIR") or "").strip()
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "runtime"


def company_identity_registry_path(runtime_dir: str | Path | None = None) -> Path:
    root = Path(runtime_dir) if runtime_dir is not None else _default_runtime_dir()
    return root / _COMPANY_IDENTITY_REGISTRY_FILENAME


def company_identity_seed_catalog_path(runtime_dir: str | Path | None = None) -> Path:
    override = str(os.environ.get("SOURCING_COMPANY_IDENTITY_SEED_CATALOG_PATH") or "").strip()
    if override:
        return Path(override)
    root = Path(runtime_dir) if runtime_dir is not None else _default_runtime_dir()
    return root / _COMPANY_IDENTITY_SEED_CATALOG_FILENAME


def load_company_identity_seed_catalog(runtime_dir: str | Path | None = None) -> dict[str, Any]:
    runtime_path = company_identity_seed_catalog_path(runtime_dir)
    bundled_path = _bundled_company_identity_seed_catalog_path()
    candidate_paths = [runtime_path]
    if bundled_path != runtime_path:
        candidate_paths.append(bundled_path)
    for path in candidate_paths:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        payload.setdefault("records", [])
        payload.setdefault("alias_index", {})
        payload["seed_catalog_path"] = str(path)
        payload["seed_catalog_source"] = "runtime" if path == runtime_path else "bundled"
        return payload
    return {
        "updated_at": "",
        "company_count": 0,
        "alias_count": 0,
        "records": [],
        "alias_index": {},
        "seed_catalog_path": str(runtime_path),
        "seed_catalog_source": "missing",
    }


def load_company_identity_registry(runtime_dir: str | Path | None = None) -> dict[str, Any]:
    path = company_identity_registry_path(runtime_dir)
    if not path.exists():
        return {
            "updated_at": "",
            "company_count": 0,
            "alias_count": 0,
            "records": [],
            "alias_index": {},
            "registry_path": str(path),
        }
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {
            "updated_at": "",
            "company_count": 0,
            "alias_count": 0,
            "records": [],
            "alias_index": {},
            "registry_path": str(path),
            "status": "failed_to_parse",
        }
    if not isinstance(payload, dict):
        return {
            "updated_at": "",
            "company_count": 0,
            "alias_count": 0,
            "records": [],
            "alias_index": {},
            "registry_path": str(path),
            "status": "invalid_payload",
        }
    payload.setdefault("records", [])
    payload.setdefault("alias_index", {})
    payload["registry_path"] = str(path)
    return payload


def refresh_company_identity_registry(
    runtime_dir: str | Path | None = None,
    *,
    companies: list[str] | tuple[str, ...] | set[str] | None = None,
) -> dict[str, Any]:
    effective_runtime_dir = Path(runtime_dir) if runtime_dir is not None else _default_runtime_dir()
    selected_filters = {
        normalize_company_key(str(item or ""))
        for item in list(companies or [])
        if normalize_company_key(str(item or ""))
    }
    seed_records = _load_seed_company_identity_records(effective_runtime_dir)
    records_by_key: dict[str, dict[str, Any]] = {
        company_key: dict(record)
        for company_key, record in seed_records.items()
        if not selected_filters or _record_matches_company_filters(record, selected_filters)
    }
    seed_record_count = len(records_by_key)
    scanned_identity_count = 0
    for company_dir in iter_company_asset_company_dirs(
        effective_runtime_dir,
        prefer_hot_cache=True,
        existing_only=True,
    ):
        for identity_path in sorted(company_dir.glob("*/identity.json")):
            try:
                payload = json.loads(identity_path.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict) or not payload:
                continue
            record = _local_company_identity_record(
                payload=payload,
                company_dir=company_dir,
                identity_path=identity_path,
            )
            if not record:
                continue
            if selected_filters and not _record_matches_company_filters(record, selected_filters):
                continue
            scanned_identity_count += 1
            company_key = str(record.get("company_key") or "").strip()
            existing = records_by_key.get(company_key)
            records_by_key[company_key] = _merge_company_identity_records(existing, record)

    payload = _company_identity_registry_payload(list(records_by_key.values()))
    registry_path = company_identity_registry_path(effective_runtime_dir)
    registry_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    return {
        "status": "completed",
        "registry_path": str(registry_path),
        "seed_catalog_path": str(
            load_company_identity_seed_catalog(effective_runtime_dir).get("seed_catalog_path") or ""
        ),
        "seed_company_count": seed_record_count,
        "company_count": int(payload.get("company_count") or 0),
        "alias_count": int(payload.get("alias_count") or 0),
        "scanned_identity_count": scanned_identity_count,
        "companies": list(selected_filters),
    }


def upsert_company_identity_registry_entry(
    *,
    runtime_dir: str | Path | None = None,
    identity_payload: dict[str, Any],
    snapshot_dir: str | Path,
) -> dict[str, Any]:
    effective_runtime_dir = Path(runtime_dir) if runtime_dir is not None else _default_runtime_dir()
    snapshot_path = Path(snapshot_dir)
    company_dir = snapshot_path.parent
    record = _local_company_identity_record(
        payload=dict(identity_payload or {}),
        company_dir=company_dir,
        identity_path=snapshot_path / "identity.json",
    )
    if not record:
        return {
            "status": "skipped",
            "reason": "invalid_identity_payload",
            "registry_path": str(company_identity_registry_path(effective_runtime_dir)),
        }
    records_by_key: dict[str, dict[str, Any]] = {}
    for company_key, item in _load_seed_company_identity_records(effective_runtime_dir).items():
        if str(company_key or "").strip():
            records_by_key[str(company_key)] = dict(item)
    for company_key, item in _load_cached_company_identity_registry_records(effective_runtime_dir).items():
        normalized_key = str(company_key or "").strip()
        if not normalized_key:
            continue
        records_by_key[normalized_key] = _merge_company_identity_records(records_by_key.get(normalized_key), item)
    company_key = str(record.get("company_key") or "").strip()
    records_by_key[company_key] = _merge_company_identity_records(records_by_key.get(company_key), record)
    payload = _company_identity_registry_payload(list(records_by_key.values()))
    registry_path = company_identity_registry_path(effective_runtime_dir)
    registry_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    return {
        "status": "completed",
        "registry_path": str(registry_path),
        "company_key": company_key,
        "canonical_name": str(record.get("canonical_name") or ""),
        "source_snapshot_id": str(record.get("source_snapshot_id") or ""),
    }


def _combined_company_aliases() -> dict[str, str]:
    combined = dict(COMPANY_ALIASES)
    for company_key, payload in _combined_company_identities().items():
        combined.setdefault(company_key, company_key)
        canonical_name = str(payload.get("canonical_name") or "").strip()
        linkedin_slug = str(payload.get("linkedin_slug") or "").strip()
        if canonical_name:
            combined.setdefault(normalize_company_key(canonical_name), company_key)
        if linkedin_slug:
            combined.setdefault(normalize_company_key(linkedin_slug), company_key)
        for alias in list(payload.get("aliases") or []):
            alias_key = normalize_company_key(str(alias or ""))
            if alias_key:
                combined.setdefault(alias_key, company_key)
    return combined


def _combined_company_identities() -> dict[str, dict[str, Any]]:
    combined = {key: dict(value) for key, value in BUILTIN_COMPANY_IDENTITIES.items()}
    for company_key, payload in _discover_local_company_identities().items():
        existing = combined.get(company_key)
        if existing is None:
            combined[company_key] = dict(payload)
            continue
        merged = dict(existing)
        if not str(merged.get("domain") or "").strip():
            merged["domain"] = str(payload.get("domain") or "").strip()
        merged["local_asset_available"] = bool(
            merged.get("local_asset_available") or payload.get("local_asset_available")
        )
        merged_aliases = list(merged.get("aliases") or [])
        seen_aliases = {normalize_company_key(alias) for alias in merged_aliases if normalize_company_key(alias)}
        for alias in list(payload.get("aliases") or []):
            alias_key = normalize_company_key(alias)
            if not alias_key or alias_key in seen_aliases:
                continue
            seen_aliases.add(alias_key)
            merged_aliases.append(str(alias))
        if merged_aliases:
            merged["aliases"] = merged_aliases
        combined[company_key] = merged
    return combined


def _discover_local_company_identities() -> dict[str, dict[str, Any]]:
    runtime_dir = _default_runtime_dir()
    discovered: dict[str, dict[str, Any]] = _load_seed_company_identity_records(runtime_dir)
    for company_key, payload in _load_cached_company_identity_registry_records(runtime_dir).items():
        discovered[company_key] = _merge_company_identity_records(discovered.get(company_key), payload)
    for company_dir in iter_company_asset_company_dirs(
        runtime_dir,
        prefer_hot_cache=True,
        existing_only=True,
    ):
        identity_payload = _latest_company_identity_payload(company_dir)
        if not identity_payload:
            continue
        company_key = normalize_company_key(str(identity_payload.get("company_key") or company_dir.name))
        if not company_key:
            continue
        canonical_name = str(
            identity_payload.get("canonical_name") or identity_payload.get("requested_name") or ""
        ).strip()
        linkedin_slug = str(identity_payload.get("linkedin_slug") or "").strip()
        aliases = _dedupe_alias_values(
            [
                canonical_name,
                str(identity_payload.get("requested_name") or "").strip(),
                linkedin_slug,
                *list(identity_payload.get("aliases") or []),
            ]
        )
        discovered[company_key] = _merge_company_identity_records(
            discovered.get(company_key),
            {
                "canonical_name": canonical_name or company_dir.name,
                "linkedin_slug": linkedin_slug,
                "domain": str(identity_payload.get("domain") or "").strip(),
                "aliases": aliases,
                "resolver": "local_asset_identity",
                "confidence": str(identity_payload.get("confidence") or "medium").strip() or "medium",
                "local_asset_available": True,
                "notes": (f"Resolved from local company asset identity snapshot for {company_dir.name}."),
            },
        )
    return discovered


def _load_cached_company_identity_registry_records(runtime_dir: Path) -> dict[str, dict[str, Any]]:
    payload = load_company_identity_registry(runtime_dir)
    records: dict[str, dict[str, Any]] = {}
    for item in list(payload.get("records") or []):
        if not isinstance(item, dict):
            continue
        company_key = normalize_company_key(str(item.get("company_key") or ""))
        if not company_key:
            continue
        records[company_key] = {
            "company_key": company_key,
            "canonical_name": str(item.get("canonical_name") or company_key).strip() or company_key,
            "linkedin_slug": str(item.get("linkedin_slug") or "").strip(),
            "domain": str(item.get("domain") or "").strip(),
            "aliases": _dedupe_alias_values(list(item.get("aliases") or [])),
            "resolver": str(item.get("resolver") or "local_asset_identity").strip() or "local_asset_identity",
            "confidence": str(item.get("confidence") or "medium").strip() or "medium",
            "local_asset_available": bool(item.get("local_asset_available")),
            "notes": str(item.get("notes") or "").strip(),
        }
    return records


def _load_seed_company_identity_records(runtime_dir: Path) -> dict[str, dict[str, Any]]:
    payload = load_company_identity_seed_catalog(runtime_dir)
    records: dict[str, dict[str, Any]] = {}
    for item in list(payload.get("records") or []):
        if not isinstance(item, dict):
            continue
        company_key = normalize_company_key(str(item.get("company_key") or ""))
        if not company_key:
            continue
        records[company_key] = {
            "company_key": company_key,
            "canonical_name": str(item.get("canonical_name") or company_key).strip() or company_key,
            "linkedin_slug": str(item.get("linkedin_slug") or "").strip(),
            "domain": str(item.get("domain") or "").strip(),
            "aliases": _dedupe_alias_values(list(item.get("aliases") or [])),
            "resolver": "seed_catalog",
            "confidence": str(item.get("confidence") or "medium").strip() or "medium",
            "local_asset_available": bool(item.get("local_asset_available")),
            "notes": str(item.get("notes") or "").strip()
            or "Resolved from runtime-synced company identity seed catalog.",
        }
    return records


def _bundled_company_identity_seed_catalog_path() -> Path:
    return Path(__file__).resolve().parent / "data" / _COMPANY_IDENTITY_SEED_CATALOG_FILENAME


def _latest_company_identity_payload(company_dir: Path) -> dict[str, Any]:
    identity_paths = sorted(company_dir.glob("*/identity.json"))
    for identity_path in reversed(identity_paths):
        try:
            payload = json.loads(identity_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict) and payload:
            return payload
    return {}


def _dedupe_alias_values(values: list[Any]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in values:
        text = str(item or "").strip()
        alias_key = normalize_company_key(text)
        if not alias_key or alias_key in seen:
            continue
        seen.add(alias_key)
        normalized.append(text)
    return normalized


def _record_matches_company_filters(record: dict[str, Any], filters: set[str]) -> bool:
    candidates = {
        normalize_company_key(str(record.get("company_key") or "")),
        normalize_company_key(str(record.get("canonical_name") or "")),
        normalize_company_key(str(record.get("linkedin_slug") or "")),
    }
    candidates.update(normalize_company_key(str(alias or "")) for alias in list(record.get("aliases") or []))
    candidates.discard("")
    return bool(candidates & filters)


def _local_company_identity_record(
    *,
    payload: dict[str, Any],
    company_dir: Path,
    identity_path: Path,
) -> dict[str, Any]:
    company_key = normalize_company_key(str(payload.get("company_key") or company_dir.name))
    if not company_key:
        return {}
    canonical_name = (
        str(payload.get("canonical_name") or payload.get("requested_name") or "").strip() or company_dir.name
    )
    linkedin_slug = str(payload.get("linkedin_slug") or "").strip()
    source_snapshot_id = str(identity_path.parent.name or "").strip()
    aliases = _dedupe_alias_values(
        [
            canonical_name,
            str(payload.get("requested_name") or "").strip(),
            linkedin_slug,
            *list(payload.get("aliases") or []),
        ]
    )
    return {
        "company_key": company_key,
        "canonical_name": canonical_name,
        "linkedin_slug": linkedin_slug,
        "domain": str(payload.get("domain") or "").strip(),
        "aliases": aliases,
        "resolver": "local_asset_identity",
        "confidence": str(payload.get("confidence") or "medium").strip() or "medium",
        "local_asset_available": True,
        "notes": (f"Resolved from local company asset identity snapshot for {company_dir.name}."),
        "source_company_dir": company_dir.name,
        "source_snapshot_id": source_snapshot_id,
        "source_identity_path": str(identity_path),
        "updated_at": _utc_now_iso(),
    }


def _merge_company_identity_records(
    existing: dict[str, Any] | None,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    if not existing:
        merged = dict(candidate)
        merged["aliases"] = _dedupe_alias_values(list(candidate.get("aliases") or []))
        return merged
    preferred = dict(existing)
    alternate = dict(candidate)
    if _company_identity_record_sort_key(alternate) > _company_identity_record_sort_key(preferred):
        preferred, alternate = alternate, preferred
    merged = dict(preferred)
    merged["aliases"] = _dedupe_alias_values(
        [
            *list(preferred.get("aliases") or []),
            *list(alternate.get("aliases") or []),
            str(preferred.get("canonical_name") or "").strip(),
            str(alternate.get("canonical_name") or "").strip(),
            str(preferred.get("linkedin_slug") or "").strip(),
            str(alternate.get("linkedin_slug") or "").strip(),
        ]
    )
    if not str(merged.get("domain") or "").strip():
        merged["domain"] = str(alternate.get("domain") or "").strip()
    if not str(merged.get("linkedin_slug") or "").strip():
        merged["linkedin_slug"] = str(alternate.get("linkedin_slug") or "").strip()
    merged["local_asset_available"] = True
    return merged


def _company_identity_record_sort_key(record: dict[str, Any]) -> tuple[str, int, int, int]:
    confidence = str(record.get("confidence") or "").strip().lower()
    confidence_rank = {"high": 3, "medium": 2, "low": 1}.get(confidence, 0)
    return (
        str(record.get("source_snapshot_id") or "").strip(),
        confidence_rank,
        int(bool(str(record.get("linkedin_slug") or "").strip())),
        int(bool(str(record.get("canonical_name") or "").strip())),
    )


def _company_identity_registry_payload(records: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_records = [
        {
            **dict(item),
            "aliases": _dedupe_alias_values(list(item.get("aliases") or [])),
        }
        for item in list(records or [])
        if isinstance(item, dict) and str(item.get("company_key") or "").strip()
    ]
    normalized_records.sort(
        key=lambda item: (
            str(item.get("canonical_name") or "").lower(),
            str(item.get("company_key") or "").lower(),
        )
    )
    alias_index: dict[str, str] = {}
    for item in normalized_records:
        company_key = str(item.get("company_key") or "").strip()
        if not company_key:
            continue
        alias_index.setdefault(company_key, company_key)
        for value in [
            item.get("canonical_name"),
            item.get("linkedin_slug"),
            *list(item.get("aliases") or []),
        ]:
            alias_key = normalize_company_key(str(value or ""))
            if alias_key:
                alias_index.setdefault(alias_key, company_key)
    return {
        "updated_at": _utc_now_iso(),
        "company_count": len(normalized_records),
        "alias_count": len(alias_index),
        "records": normalized_records,
        "alias_index": alias_index,
    }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
