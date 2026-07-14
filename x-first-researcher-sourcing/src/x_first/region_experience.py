from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

POLICY_SCHEMA_VERSION = "x.region_experience.policy.v1"
EVIDENCE_SCHEMA_VERSION = "x.region_experience.evidence_bundle.v1"
CLASSIFICATION_SCHEMA_VERSION = "x.region_experience.classification.v1"
POLICY_VERSION = "x-region-experience-public-professional-v1"

LABELS = (
    "ASIA_EXPERIENCE",
    "GREATER_CHINA_EXPERIENCE",
    "MAINLAND_CHINA_EXPERIENCE",
)
SOURCE_KINDS = frozenset({"bio", "employment", "education", "research", "post"})
SOURCE_ACTORS = frozenset({"subject_self", "verified_organization", "third_party"})
STATEMENT_SCOPES = frozenset({"subject_explicit_experience", "location_topic_only", "third_party_mention"})
EXPERIENCE_KINDS = frozenset({"based_in", "lived_in", "worked_in", "studied_in", "researched_in"})
DECISION_REASONS = frozenset(
    {
        "eligible_explicit_experience",
        "rejected_non_subject_scope",
        "rejected_source_actor",
        "rejected_experience_kind",
        "rejected_location_alias",
    }
)

EXPECTED_LOCATION_LABELS: Mapping[str, tuple[str, ...]] = {
    "asia_unspecified": ("ASIA_EXPERIENCE",),
    "greater_china_unspecified": ("ASIA_EXPERIENCE", "GREATER_CHINA_EXPERIENCE"),
    "mainland_china": LABELS,
    "hong_kong": ("ASIA_EXPERIENCE", "GREATER_CHINA_EXPERIENCE"),
    "macau": ("ASIA_EXPERIENCE", "GREATER_CHINA_EXPERIENCE"),
    "taiwan": ("ASIA_EXPERIENCE", "GREATER_CHINA_EXPERIENCE"),
    "singapore": ("ASIA_EXPERIENCE", "GREATER_CHINA_EXPERIENCE"),
    "japan": ("ASIA_EXPERIENCE",),
    "south_korea": ("ASIA_EXPERIENCE",),
    "india": ("ASIA_EXPERIENCE",),
}
EXPECTED_LOCATION_ALIASES: Mapping[str, tuple[str, ...]] = {
    "asia_unspecified": ("Asia",),
    "greater_china_unspecified": ("Greater China",),
    "mainland_china": (
        "Mainland China",
        "China Mainland",
        "Beijing",
        "Beijing, Mainland China",
        "Shanghai",
        "Shanghai, Mainland China",
        "Shenzhen",
        "Shenzhen, Mainland China",
        "Guangzhou",
        "Guangzhou, Mainland China",
        "Hangzhou",
        "Hangzhou, Mainland China",
        "Nanjing",
        "Nanjing, Mainland China",
        "Wuhan",
        "Wuhan, Mainland China",
        "Chengdu",
        "Chengdu, Mainland China",
    ),
    "hong_kong": ("Hong Kong", "Hong Kong SAR"),
    "macau": ("Macau", "Macao", "Macau SAR", "Macao SAR"),
    "taiwan": ("Taiwan", "Taipei", "Taipei, Taiwan"),
    "singapore": ("Singapore",),
    "japan": ("Japan", "Tokyo", "Tokyo, Japan"),
    "south_korea": ("South Korea", "Seoul", "Seoul, South Korea"),
    "india": ("India", "Bengaluru", "Bengaluru, India", "Bangalore", "Bangalore, India"),
}
EXPECTED_SOURCE_RULES: Mapping[str, Mapping[str, tuple[str, ...]]] = {
    "bio": {
        "source_actors": ("subject_self",),
        "experience_kinds": ("based_in", "lived_in"),
    },
    "employment": {
        "source_actors": ("subject_self", "verified_organization"),
        "experience_kinds": ("based_in", "worked_in"),
    },
    "education": {
        "source_actors": ("subject_self", "verified_organization"),
        "experience_kinds": ("studied_in",),
    },
    "research": {
        "source_actors": ("subject_self", "verified_organization"),
        "experience_kinds": ("researched_in",),
    },
    "post": {
        "source_actors": ("subject_self",),
        "experience_kinds": ("based_in", "lived_in", "worked_in", "studied_in", "researched_in"),
    },
}
FORBIDDEN_INPUT_FIELDS = frozenset(
    {
        "ancestry",
        "bilingual",
        "citizenship",
        "community",
        "country_of_origin",
        "current_handle",
        "display_name",
        "ethnicity",
        "gender",
        "handle",
        "language",
        "languages",
        "name",
        "nationality",
        "race",
        "religion",
        "social_graph",
        "social_graph_position",
    }
)

_TOP_LEVEL_FIELDS = frozenset({"schema_version", "policy_version", "subject_ref", "evidence"})
_EVIDENCE_FIELDS = frozenset(
    {
        "evidence_id",
        "subject_ref",
        "source_kind",
        "source_actor",
        "statement_scope",
        "experience_kind",
        "location_code",
        "location_text",
        "source_url",
        "observed_at",
        "excerpt",
    }
)
_SUBJECT_REF_PATTERN = re.compile(r"^xregion_subject_[a-z0-9_]+$")
_EVIDENCE_ID_PATTERN = re.compile(r"^xregion_ev_[a-z0-9_]+$")
_CANONICAL_TIMESTAMP_PATTERN = re.compile(
    r"^[0-9]{4}-(?:0[1-9]|1[0-2])-(?:[0-2][0-9]|3[01])T"
    r"(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\.[0-9]{3}Z$"
)


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("expected a JSON object")
    return payload


def canonical_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def normalize_location_alias(value: Any) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or "")).strip().casefold()
    return " ".join(normalized.split())


def _exact_fields(value: Any, *, expected: frozenset[str], location: str, errors: list[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        errors.append(f"XREG_EVIDENCE_INVALID: {location} must be an object")
        return {}
    if set(value) != expected:
        errors.append(f"XREG_EVIDENCE_INVALID: {location} fields do not match the closed contract")
    return value


def _canonical_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or _CANONICAL_TIMESTAMP_PATTERN.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        return False
    return parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z") == value


def _valid_public_url(value: Any) -> bool:
    if not isinstance(value, str) or value != value.strip() or any(char in value for char in "\r\n\t"):
        return False
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    return (
        parsed.scheme == "https"
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
        and parsed.fragment == ""
    )


def _walk_fields(value: Any) -> list[str]:
    fields: list[str] = []
    if isinstance(value, dict):
        for field, child in value.items():
            fields.append(str(field).casefold())
            fields.extend(_walk_fields(child))
    elif isinstance(value, list):
        for child in value:
            fields.extend(_walk_fields(child))
    return fields


def validate_policy(policy: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(policy, dict):
        return ["XREG_POLICY_INVALID: policy must be an object"]
    expected_fields = {
        "schema_version",
        "policy_version",
        "labels",
        "locations",
        "source_rules",
        "forbidden_input_fields",
    }
    if set(policy) != expected_fields:
        errors.append("XREG_POLICY_INVALID: policy fields do not match the closed contract")
    if policy.get("schema_version") != POLICY_SCHEMA_VERSION or policy.get("policy_version") != POLICY_VERSION:
        errors.append("XREG_POLICY_INVALID: policy identity mismatch")

    labels = policy.get("labels") if isinstance(policy.get("labels"), list) else []
    observed_labels: dict[str, tuple[str, ...]] = {}
    for item in labels:
        if not isinstance(item, dict) or set(item) != {"label", "location_codes"}:
            errors.append("XREG_POLICY_INVALID: label entry is not closed")
            continue
        label = str(item.get("label") or "")
        codes = item.get("location_codes") if isinstance(item.get("location_codes"), list) else []
        observed_labels[label] = tuple(str(code) for code in codes)
    expected_by_label = {
        label: tuple(code for code, labels_for_code in EXPECTED_LOCATION_LABELS.items() if label in labels_for_code)
        for label in LABELS
    }
    if observed_labels != expected_by_label:
        errors.append("XREG_POLICY_INVALID: label hierarchy does not match v1")

    locations = policy.get("locations") if isinstance(policy.get("locations"), list) else []
    location_codes: set[str] = set()
    observed_aliases: dict[str, tuple[str, ...]] = {}
    alias_owner: dict[str, str] = {}
    for item in locations:
        if not isinstance(item, dict) or set(item) != {"location_code", "aliases"}:
            errors.append("XREG_POLICY_INVALID: location entry is not closed")
            continue
        location_code = str(item.get("location_code") or "")
        aliases = item.get("aliases") if isinstance(item.get("aliases"), list) else []
        observed_aliases[location_code] = tuple(str(alias) for alias in aliases)
        if location_code in location_codes or location_code not in EXPECTED_LOCATION_LABELS:
            errors.append("XREG_POLICY_INVALID: location code registry mismatch")
        location_codes.add(location_code)
        if not aliases:
            errors.append("XREG_POLICY_INVALID: location alias registry is empty")
        for alias in aliases:
            normalized_alias = normalize_location_alias(alias)
            if not normalized_alias or normalized_alias in alias_owner:
                errors.append("XREG_POLICY_INVALID: location aliases must be nonempty and globally unique")
                continue
            alias_owner[normalized_alias] = location_code
    if location_codes != set(EXPECTED_LOCATION_LABELS):
        errors.append("XREG_POLICY_INVALID: location registry does not match v1")
    if observed_aliases != EXPECTED_LOCATION_ALIASES:
        errors.append("XREG_POLICY_INVALID: location aliases do not match v1")

    source_rules = policy.get("source_rules") if isinstance(policy.get("source_rules"), list) else []
    observed_rules: dict[str, dict[str, tuple[str, ...]]] = {}
    for item in source_rules:
        if not isinstance(item, dict) or set(item) != {"source_kind", "source_actors", "experience_kinds"}:
            errors.append("XREG_POLICY_INVALID: source rule is not closed")
            continue
        source_kind = str(item.get("source_kind") or "")
        actors = item.get("source_actors") if isinstance(item.get("source_actors"), list) else []
        kinds = item.get("experience_kinds") if isinstance(item.get("experience_kinds"), list) else []
        observed_rules[source_kind] = {
            "source_actors": tuple(str(actor) for actor in actors),
            "experience_kinds": tuple(str(kind) for kind in kinds),
        }
    if observed_rules != EXPECTED_SOURCE_RULES:
        errors.append("XREG_POLICY_INVALID: source rules do not match v1")

    forbidden_fields = policy.get("forbidden_input_fields")
    if (
        not isinstance(forbidden_fields, list)
        or frozenset(str(field) for field in forbidden_fields) != FORBIDDEN_INPUT_FIELDS
    ):
        errors.append("XREG_POLICY_INVALID: forbidden field registry does not match v1")
    return errors


def _policy_indexes(policy: Mapping[str, Any]) -> tuple[dict[str, str], dict[str, dict[str, set[str]]]]:
    aliases: dict[str, str] = {}
    for item in policy.get("locations", []):
        if not isinstance(item, dict):
            continue
        location_code = str(item.get("location_code") or "")
        for alias in item.get("aliases", []):
            aliases[normalize_location_alias(alias)] = location_code
    rules: dict[str, dict[str, set[str]]] = {}
    for item in policy.get("source_rules", []):
        if not isinstance(item, dict):
            continue
        rules[str(item.get("source_kind") or "")] = {
            "source_actors": {str(value) for value in item.get("source_actors", [])},
            "experience_kinds": {str(value) for value in item.get("experience_kinds", [])},
        }
    return aliases, rules


def validate_evidence_bundle(payload: Any, *, policy: Any) -> list[str]:
    errors = validate_policy(policy)
    if errors:
        return errors
    bundle = _exact_fields(payload, expected=_TOP_LEVEL_FIELDS, location="bundle", errors=errors)
    if bundle.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
        errors.append("XREG_EVIDENCE_INVALID: schema version mismatch")
    if bundle.get("policy_version") != POLICY_VERSION:
        errors.append("XREG_EVIDENCE_INVALID: policy version mismatch")
    subject_ref = bundle.get("subject_ref")
    if not isinstance(subject_ref, str) or _SUBJECT_REF_PATTERN.fullmatch(subject_ref) is None:
        errors.append("XREG_EVIDENCE_INVALID: subject_ref is invalid")
    forbidden_fields = set(_walk_fields(payload)) & FORBIDDEN_INPUT_FIELDS
    if forbidden_fields:
        errors.append("XREG_EVIDENCE_INVALID: protected or proxy input field is forbidden")

    evidence = bundle.get("evidence")
    if not isinstance(evidence, list):
        errors.append("XREG_EVIDENCE_INVALID: evidence must be an array")
        return errors
    if len(evidence) > 100:
        errors.append("XREG_EVIDENCE_INVALID: evidence exceeds the fixed maximum")
    location_codes = set(EXPECTED_LOCATION_LABELS)
    seen_evidence_ids: set[str] = set()
    for index, value in enumerate(evidence[:100]):
        item = _exact_fields(value, expected=_EVIDENCE_FIELDS, location=f"evidence[{index}]", errors=errors)
        evidence_id = item.get("evidence_id")
        if not isinstance(evidence_id, str) or _EVIDENCE_ID_PATTERN.fullmatch(evidence_id) is None:
            errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].evidence_id is invalid")
        elif evidence_id in seen_evidence_ids:
            errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].evidence_id is duplicated")
        else:
            seen_evidence_ids.add(evidence_id)
        if item.get("subject_ref") != subject_ref:
            errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].subject_ref mismatch")
        if item.get("source_kind") not in SOURCE_KINDS:
            errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].source_kind is invalid")
        if item.get("source_actor") not in SOURCE_ACTORS:
            errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].source_actor is invalid")
        if item.get("statement_scope") not in STATEMENT_SCOPES:
            errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].statement_scope is invalid")
        if item.get("experience_kind") not in EXPERIENCE_KINDS:
            errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].experience_kind is invalid")
        if item.get("location_code") not in location_codes:
            errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].location_code is invalid")
        location_text = item.get("location_text")
        if not isinstance(location_text, str) or not location_text.strip() or len(location_text) > 80:
            errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].location_text is invalid")
        if not _valid_public_url(item.get("source_url")):
            errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].source_url is invalid")
        if not _canonical_timestamp(item.get("observed_at")):
            errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].observed_at is invalid")
        excerpt = item.get("excerpt")
        if not isinstance(excerpt, str) or not excerpt.strip() or len(excerpt) > 280:
            errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].excerpt is invalid")
    return errors


def _classify_evidence(
    item: Mapping[str, Any],
    *,
    aliases: Mapping[str, str],
    rules: Mapping[str, Mapping[str, set[str]]],
) -> tuple[str, bool]:
    if item.get("statement_scope") != "subject_explicit_experience":
        return "rejected_non_subject_scope", False
    source_rule = rules.get(str(item.get("source_kind") or ""), {})
    if item.get("source_actor") not in source_rule.get("source_actors", set()):
        return "rejected_source_actor", False
    if item.get("experience_kind") not in source_rule.get("experience_kinds", set()):
        return "rejected_experience_kind", False
    resolved_location = aliases.get(normalize_location_alias(item.get("location_text")))
    if resolved_location != item.get("location_code"):
        return "rejected_location_alias", False
    return "eligible_explicit_experience", True


def classify_region_experience(payload: Mapping[str, Any], *, policy: Mapping[str, Any]) -> dict[str, Any]:
    errors = validate_evidence_bundle(payload, policy=policy)
    if errors:
        raise ValueError("region evidence bundle is invalid")
    aliases, rules = _policy_indexes(policy)
    decisions: list[dict[str, Any]] = []
    refs_by_label: dict[str, list[str]] = {label: [] for label in LABELS}
    for raw_item in payload["evidence"]:
        item = dict(raw_item)
        reason, eligible = _classify_evidence(item, aliases=aliases, rules=rules)
        decisions.append(
            {
                "evidence_id": item["evidence_id"],
                "status": "eligible" if eligible else "rejected",
                "reason": reason,
            }
        )
        if not eligible:
            continue
        for label in EXPECTED_LOCATION_LABELS[str(item["location_code"])]:
            refs_by_label[label].append(str(item["evidence_id"]))
    labels = [
        {
            "label": label,
            "status": "supported" if refs_by_label[label] else "not_supported",
            "evidence_refs": sorted(refs_by_label[label]),
        }
        for label in LABELS
    ]
    return {
        "schema_version": CLASSIFICATION_SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "subject_ref": payload["subject_ref"],
        "input_sha256": canonical_sha256(payload),
        "evidence_decisions": decisions,
        "labels": labels,
        "claims": {
            "protected_identity_inferred": False,
            "ethnicity_inferred": False,
            "nationality_inferred": False,
            "citizenship_inferred": False,
            "name_used": False,
            "handle_used": False,
            "language_used": False,
            "social_graph_used": False,
            "third_party_mentions_used": False,
            "canonical_writes_authorized": False,
            "outreach_authorized": False,
        },
    }


def validate_classification(
    result: Any,
    *,
    evidence_bundle: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[str]:
    try:
        expected = classify_region_experience(evidence_bundle, policy=policy)
    except (TypeError, ValueError):
        return ["XREG_CLASSIFICATION_INVALID: bound evidence bundle is invalid"]
    if not isinstance(result, dict):
        return ["XREG_CLASSIFICATION_INVALID: result must be an object"]
    if result != expected:
        return ["XREG_CLASSIFICATION_INVALID: result does not match deterministic evidence derivation"]
    return []


def _default_paths() -> tuple[Path, Path, Path]:
    root = project_root()
    return (
        root / "configs/region_experience_policy.v1.json",
        root / "fixtures/region_experience_evidence_fixture_v1.json",
        root / "fixtures/region_experience_classification_fixture_v1.json",
    )


def main() -> int:
    policy_path, evidence_path, result_path = _default_paths()
    try:
        policy = load_json(policy_path)
        evidence = load_json(evidence_path)
        result = load_json(result_path)
        evidence_errors = validate_evidence_bundle(evidence, policy=policy)
        result_errors = validate_classification(result, evidence_bundle=evidence, policy=policy)
    except (OSError, ValueError, json.JSONDecodeError):
        print(json.dumps({"status": "invalid", "errors": ["XREG_IO_INVALID"]}, sort_keys=True))
        return 1
    errors = [*evidence_errors, *result_errors]
    print(json.dumps({"status": "valid" if not errors else "invalid", "errors": errors}, sort_keys=True))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
