from __future__ import annotations

import hashlib
import ipaddress
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
SOURCE_MEDIUMS = (
    "profile_bio",
    "employment_record",
    "education_record",
    "research_record",
    "post",
)
PUBLISHER_ACTORS = ("subject_self", "verified_organization", "third_party")
STATEMENT_SCOPES = ("subject_explicit_experience", "location_topic_only", "third_party_mention")
EXPERIENCE_KINDS = ("based_in", "lived_in", "worked_in", "studied_in", "researched_in")
ACTOR_SUBJECT_RELATIONS = (
    "same_external_account",
    "verified_professional_record",
    "unrelated_or_unknown",
)
PUBLISHER_BINDING_STATUSES = (
    "self_account_match",
    "verified_organization_binding",
    "unverified",
)
EXTRACTION_STATUSES = ("extracted",)
PROPOSAL_STATUSES = ("proposed",)
ADJUDICATION_STATUSES = ("pending", "adjudicated")
ELIGIBILITY_STATUSES = ("unresolved", "eligible", "rejected")

EXPECTED_LOCATION_LABELS: Mapping[str, tuple[str, ...]] = {
    "asia_unspecified": ("ASIA_EXPERIENCE",),
    "mainland_china": LABELS,
    "hong_kong": ("ASIA_EXPERIENCE", "GREATER_CHINA_EXPERIENCE"),
    "macau": ("ASIA_EXPERIENCE", "GREATER_CHINA_EXPERIENCE"),
    "taiwan": ("ASIA_EXPERIENCE", "GREATER_CHINA_EXPERIENCE"),
    "singapore": ("ASIA_EXPERIENCE",),
    "japan": ("ASIA_EXPERIENCE",),
    "south_korea": ("ASIA_EXPERIENCE",),
    "india": ("ASIA_EXPERIENCE",),
}
EXPECTED_LOCATION_ALIASES: Mapping[str, tuple[str, ...]] = {
    "asia_unspecified": ("Asia", "亚洲", "亞洲"),
    "mainland_china": (
        "Mainland China",
        "China Mainland",
        "中国大陆",
        "中國大陸",
        "Beijing",
        "Beijing, Mainland China",
        "北京",
        "Shanghai",
        "Shanghai, Mainland China",
        "上海",
        "Shenzhen",
        "Shenzhen, Mainland China",
        "深圳",
        "Guangzhou",
        "Guangzhou, Mainland China",
        "广州",
        "廣州",
        "Hangzhou",
        "Hangzhou, Mainland China",
        "杭州",
        "Nanjing",
        "Nanjing, Mainland China",
        "南京",
        "Wuhan",
        "Wuhan, Mainland China",
        "武汉",
        "武漢",
        "Chengdu",
        "Chengdu, Mainland China",
        "成都",
    ),
    "hong_kong": ("Hong Kong", "Hong Kong SAR", "香港", "香港特别行政区", "香港特別行政區"),
    "macau": ("Macau", "Macao", "Macau SAR", "Macao SAR", "澳门", "澳門", "澳门特别行政区", "澳門特別行政區"),
    "taiwan": ("Taiwan", "Taipei", "Taipei, Taiwan", "台湾", "臺灣", "台北", "臺北"),
    "singapore": ("Singapore", "新加坡"),
    "japan": ("Japan", "Tokyo", "Tokyo, Japan", "日本", "东京", "東京"),
    "south_korea": (
        "South Korea",
        "Seoul",
        "Seoul, South Korea",
        "韩国",
        "韓國",
        "首尔",
        "首爾",
        "대한민국",
        "서울",
    ),
    "india": (
        "India",
        "Bengaluru",
        "Bengaluru, India",
        "Bangalore",
        "Bangalore, India",
        "印度",
        "班加罗尔",
        "班加羅爾",
    ),
}
ALLOWED_OFFICIAL_HOSTS = (
    "openai.com",
    "anthropic.com",
    "deepmind.google",
    "x.ai",
    "ai.meta.com",
    "thinkingmachines.ai",
)
FORBIDDEN_INPUT_FIELDS = (
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
)

_ULID_FRAGMENT = r"[0-7][0-9A-HJKMNP-TV-Z]{25}"
_SUBJECT_REF_PATTERN = re.compile(rf"^pp_x_{_ULID_FRAGMENT}$")
_EVIDENCE_ID_PATTERN = re.compile(rf"^xev_{_ULID_FRAGMENT}$")
_SOURCE_RECORD_ID_PATTERN = re.compile(rf"^xsrc_{_ULID_FRAGMENT}$")
_BINDING_REF_PATTERN = re.compile(rf"^xbind_{_ULID_FRAGMENT}$")
_ADJUDICATION_REF_PATTERN = re.compile(rf"^xadj_{_ULID_FRAGMENT}$")
_PLATFORM_USER_ID_PATTERN = re.compile(r"^[0-9]{3,32}$")
_CONTENT_VERSION_PATTERN = re.compile(r"^v[0-9]{6}$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_CANONICAL_TIMESTAMP_PATTERN = re.compile(
    r"^[0-9]{4}-(?:0[1-9]|1[0-2])-(?:[0-2][0-9]|3[01])T"
    r"(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\.[0-9]{3}Z$"
)
_X_PROFILE_PATH_PATTERN = re.compile(r"^/[A-Za-z0-9_]{1,15}/?$")
_X_POST_PATH_PATTERN = re.compile(r"^/[A-Za-z0-9_]{1,15}/status/[0-9]{3,32}$")

_TOP_LEVEL_FIELDS = frozenset(
    {"schema_version", "policy_version", "subject_ref", "subject_platform_user_id", "evidence"}
)
_EVIDENCE_FIELDS = frozenset(
    {
        "evidence_id",
        "source_record_id",
        "subject_ref",
        "subject_platform_user_id",
        "source_author_platform_user_id",
        "source_medium",
        "publisher_actor",
        "actor_subject_relation",
        "publisher_binding_status",
        "publisher_binding_ref",
        "actor_binding_sha256",
        "statement_scope",
        "experience_kind",
        "location_code",
        "location_text",
        "source_url",
        "observed_at",
        "content_sha256",
        "content_version",
        "excerpt",
        "excerpt_sha256",
        "extraction_status",
        "proposal_status",
        "adjudication_status",
        "eligibility_status",
        "adjudication_ref",
        "adjudicated_at",
        "adjudication_sha256",
    }
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


def text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def normalize_location_alias(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    normalized = unicodedata.normalize("NFKC", value).strip().casefold()
    return " ".join(normalized.split())


def actor_binding_sha256(item: Mapping[str, Any]) -> str:
    return canonical_sha256(
        {
            "source_record_id": item.get("source_record_id"),
            "source_url": item.get("source_url"),
            "publisher_actor": item.get("publisher_actor"),
            "subject_platform_user_id": item.get("subject_platform_user_id"),
            "source_author_platform_user_id": item.get("source_author_platform_user_id"),
            "actor_subject_relation": item.get("actor_subject_relation"),
            "publisher_binding_status": item.get("publisher_binding_status"),
            "publisher_binding_ref": item.get("publisher_binding_ref"),
        }
    )


def adjudication_sha256(item: Mapping[str, Any]) -> str | None:
    if item.get("adjudication_status") == "pending":
        return None
    return canonical_sha256(
        {
            "evidence_id": item.get("evidence_id"),
            "source_record_id": item.get("source_record_id"),
            "actor_binding_sha256": item.get("actor_binding_sha256"),
            "content_sha256": item.get("content_sha256"),
            "content_version": item.get("content_version"),
            "excerpt_sha256": item.get("excerpt_sha256"),
            "statement_scope": item.get("statement_scope"),
            "experience_kind": item.get("experience_kind"),
            "location_code": item.get("location_code"),
            "location_text": item.get("location_text"),
            "adjudication_status": item.get("adjudication_status"),
            "eligibility_status": item.get("eligibility_status"),
            "adjudication_ref": item.get("adjudication_ref"),
            "adjudicated_at": item.get("adjudicated_at"),
        }
    )


def _expected_policy() -> dict[str, Any]:
    label_entries = [
        {
            "label": label,
            "location_codes": [
                code for code, labels_for_code in EXPECTED_LOCATION_LABELS.items() if label in labels_for_code
            ],
        }
        for label in LABELS
    ]
    location_entries = [
        {"location_code": code, "aliases": list(EXPECTED_LOCATION_ALIASES[code])} for code in EXPECTED_LOCATION_LABELS
    ]
    return {
        "schema_version": POLICY_SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "labels": label_entries,
        "locations": location_entries,
        "source_mediums": list(SOURCE_MEDIUMS),
        "publisher_actors": list(PUBLISHER_ACTORS),
        "statement_scopes": list(STATEMENT_SCOPES),
        "experience_kinds": list(EXPERIENCE_KINDS),
        "actor_subject_relations": list(ACTOR_SUBJECT_RELATIONS),
        "publisher_binding_statuses": list(PUBLISHER_BINDING_STATUSES),
        "evidence_lifecycle": {
            "extraction_statuses": list(EXTRACTION_STATUSES),
            "proposal_statuses": list(PROPOSAL_STATUSES),
            "adjudication_statuses": list(ADJUDICATION_STATUSES),
            "eligibility_statuses": list(ELIGIBILITY_STATUSES),
        },
        "allowed_official_hosts": list(ALLOWED_OFFICIAL_HOSTS),
        "forbidden_input_fields": list(FORBIDDEN_INPUT_FIELDS),
    }


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


def _matches(value: Any, pattern: re.Pattern[str]) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _is_enum(value: Any, values: tuple[str, ...]) -> bool:
    return isinstance(value, str) and value in values


def _canonical_field_name(value: Any) -> str:
    normalized = unicodedata.normalize("NFKC", str(value)).casefold()
    return "".join(char for char in normalized if char.isalnum())


def _walk_fields(value: Any) -> list[str]:
    fields: list[str] = []
    if isinstance(value, dict):
        for field, child in value.items():
            fields.append(_canonical_field_name(field))
            fields.extend(_walk_fields(child))
    elif isinstance(value, list):
        for child in value:
            fields.extend(_walk_fields(child))
    return fields


def _allowed_host(host: str, *, policy: Mapping[str, Any]) -> bool:
    if host in {"x.com", "www.x.com"}:
        return True
    if host.endswith(".invalid") and re.fullmatch(r"[a-z0-9-]+(?:\.[a-z0-9-]+)*\.invalid", host):
        return True
    official_hosts = policy.get("allowed_official_hosts")
    if not isinstance(official_hosts, list):
        return False
    return any(host == base or host.endswith(f".{base}") for base in official_hosts if isinstance(base, str))


def _valid_public_source_url(value: Any, *, source_medium: Any, policy: Mapping[str, Any]) -> bool:
    if (
        not isinstance(value, str)
        or len(value) > 256
        or value != value.strip()
        or any(char in value for char in "\r\n\t")
    ):
        return False
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.netloc != parsed.netloc.casefold()
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or port is not None
    ):
        return False
    host = parsed.hostname.casefold()
    if host != parsed.hostname or host == "localhost" or not _allowed_host(host, policy=policy):
        return False
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        return False
    if not parsed.path or parsed.path == "/" or len(parsed.path) > 180:
        return False
    if host in {"x.com", "www.x.com"}:
        if source_medium == "profile_bio":
            return _X_PROFILE_PATH_PATTERN.fullmatch(parsed.path) is not None
        return _X_POST_PATH_PATTERN.fullmatch(parsed.path) is not None
    return re.fullmatch(r"/[A-Za-z0-9._~!$&'()*+,;=:@%/-]+", parsed.path) is not None


def validate_policy(policy: Any) -> list[str]:
    if not isinstance(policy, dict):
        return ["XREG_POLICY_INVALID: policy must be an object"]
    expected = _expected_policy()
    errors: list[str] = []
    if set(policy) != set(expected):
        errors.append("XREG_POLICY_INVALID: policy fields do not match the closed contract")
    for field, expected_value in expected.items():
        if policy.get(field) != expected_value:
            errors.append(f"XREG_POLICY_INVALID: {field} does not match the exact v1 registry")
    return errors


def _policy_aliases(policy: Mapping[str, Any]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    locations = policy.get("locations")
    if not isinstance(locations, list):
        return aliases
    for item in locations:
        if not isinstance(item, dict) or not isinstance(item.get("location_code"), str):
            continue
        raw_aliases = item.get("aliases")
        if not isinstance(raw_aliases, list):
            continue
        for alias in raw_aliases:
            normalized = normalize_location_alias(alias)
            if normalized:
                aliases[normalized] = item["location_code"]
    return aliases


def _append_item_error(errors: list[str], index: int, field: str, reason: str = "invalid") -> None:
    errors.append(f"XREG_EVIDENCE_INVALID: evidence[{index}].{field} is {reason}")


def _validate_actor_binding(item: Mapping[str, Any], *, index: int, errors: list[str]) -> None:
    actor = item.get("publisher_actor")
    relation = item.get("actor_subject_relation")
    binding = item.get("publisher_binding_status")
    binding_ref = item.get("publisher_binding_ref")
    subject_account = item.get("subject_platform_user_id")
    author_account = item.get("source_author_platform_user_id")
    eligibility = item.get("eligibility_status")

    if actor == "subject_self":
        if author_account != subject_account:
            _append_item_error(errors, index, "source_author_platform_user_id", "not bound to subject account")
        if relation != "same_external_account":
            _append_item_error(errors, index, "actor_subject_relation", "invalid for subject_self")
        if binding != "self_account_match":
            _append_item_error(errors, index, "publisher_binding_status", "invalid for subject_self")
        if binding_ref is not None:
            _append_item_error(errors, index, "publisher_binding_ref", "not null for subject_self")
    elif actor == "verified_organization":
        if author_account == subject_account:
            _append_item_error(errors, index, "source_author_platform_user_id", "not an organization account")
        if relation != "verified_professional_record":
            _append_item_error(errors, index, "actor_subject_relation", "invalid for verified organization")
        if binding != "verified_organization_binding":
            _append_item_error(errors, index, "publisher_binding_status", "invalid for verified organization")
        if not _matches(binding_ref, _BINDING_REF_PATTERN):
            _append_item_error(errors, index, "publisher_binding_ref", "missing verified binding proof")
    elif actor == "third_party":
        if relation != "unrelated_or_unknown":
            _append_item_error(errors, index, "actor_subject_relation", "invalid for third party")
        if binding != "unverified":
            _append_item_error(errors, index, "publisher_binding_status", "invalid for third party")
        if binding_ref is not None:
            _append_item_error(errors, index, "publisher_binding_ref", "not null for third party")
        if eligibility == "eligible":
            _append_item_error(errors, index, "eligibility_status", "eligible for third party")


def _validate_lifecycle(item: Mapping[str, Any], *, index: int, errors: list[str]) -> None:
    adjudication = item.get("adjudication_status")
    eligibility = item.get("eligibility_status")
    adjudication_ref = item.get("adjudication_ref")
    adjudicated_at = item.get("adjudicated_at")
    if adjudication == "pending":
        if eligibility != "unresolved":
            _append_item_error(errors, index, "eligibility_status", "not unresolved while pending")
        if adjudication_ref is not None or adjudicated_at is not None:
            _append_item_error(errors, index, "adjudication_ref", "present while pending")
    elif adjudication == "adjudicated":
        if eligibility not in ("eligible", "rejected"):
            _append_item_error(errors, index, "eligibility_status", "not terminal after adjudication")
        if not _matches(adjudication_ref, _ADJUDICATION_REF_PATTERN):
            _append_item_error(errors, index, "adjudication_ref", "missing adjudication proof")
        if not _canonical_timestamp(adjudicated_at):
            _append_item_error(errors, index, "adjudicated_at")
    if eligibility == "eligible" and item.get("statement_scope") != "subject_explicit_experience":
        _append_item_error(errors, index, "statement_scope", "not explicit subject experience for eligible evidence")


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
    subject_account = bundle.get("subject_platform_user_id")
    if not _matches(subject_ref, _SUBJECT_REF_PATTERN):
        errors.append("XREG_EVIDENCE_INVALID: subject_ref is not an opaque pp_x ULID")
    if not _matches(subject_account, _PLATFORM_USER_ID_PATTERN):
        errors.append("XREG_EVIDENCE_INVALID: subject_platform_user_id is invalid")
    forbidden_registry = {_canonical_field_name(field) for field in FORBIDDEN_INPUT_FIELDS}
    if set(_walk_fields(payload)) & forbidden_registry:
        errors.append("XREG_EVIDENCE_INVALID: protected or proxy input field is forbidden")

    evidence = bundle.get("evidence")
    if not isinstance(evidence, list):
        errors.append("XREG_EVIDENCE_INVALID: evidence must be an array")
        return errors
    if len(evidence) > 100:
        errors.append("XREG_EVIDENCE_INVALID: evidence exceeds the fixed maximum")

    aliases = _policy_aliases(policy)
    location_codes = set(EXPECTED_LOCATION_LABELS)
    seen_evidence_ids: set[str] = set()
    source_records: dict[str, tuple[Any, ...]] = {}
    for index, value in enumerate(evidence[:100]):
        item = _exact_fields(value, expected=_EVIDENCE_FIELDS, location=f"evidence[{index}]", errors=errors)
        evidence_id = item.get("evidence_id")
        if not _matches(evidence_id, _EVIDENCE_ID_PATTERN):
            _append_item_error(errors, index, "evidence_id")
        elif evidence_id in seen_evidence_ids:
            _append_item_error(errors, index, "evidence_id", "duplicated")
        else:
            seen_evidence_ids.add(evidence_id)
        source_record_id = item.get("source_record_id")
        if not _matches(source_record_id, _SOURCE_RECORD_ID_PATTERN):
            _append_item_error(errors, index, "source_record_id")
        if item.get("subject_ref") != subject_ref:
            _append_item_error(errors, index, "subject_ref", "mismatched")
        if item.get("subject_platform_user_id") != subject_account:
            _append_item_error(errors, index, "subject_platform_user_id", "mismatched")
        if not _matches(item.get("source_author_platform_user_id"), _PLATFORM_USER_ID_PATTERN):
            _append_item_error(errors, index, "source_author_platform_user_id")
        if not _is_enum(item.get("source_medium"), SOURCE_MEDIUMS):
            _append_item_error(errors, index, "source_medium")
        if not _is_enum(item.get("publisher_actor"), PUBLISHER_ACTORS):
            _append_item_error(errors, index, "publisher_actor")
        if not _is_enum(item.get("actor_subject_relation"), ACTOR_SUBJECT_RELATIONS):
            _append_item_error(errors, index, "actor_subject_relation")
        if not _is_enum(item.get("publisher_binding_status"), PUBLISHER_BINDING_STATUSES):
            _append_item_error(errors, index, "publisher_binding_status")
        binding_ref = item.get("publisher_binding_ref")
        if binding_ref is not None and not _matches(binding_ref, _BINDING_REF_PATTERN):
            _append_item_error(errors, index, "publisher_binding_ref")
        if not _matches(item.get("actor_binding_sha256"), _SHA256_PATTERN):
            _append_item_error(errors, index, "actor_binding_sha256")
        else:
            try:
                expected_actor_binding = actor_binding_sha256(item)
            except (TypeError, ValueError):
                expected_actor_binding = None
            if item.get("actor_binding_sha256") != expected_actor_binding:
                _append_item_error(errors, index, "actor_binding_sha256", "not bound to actor relation")
        if not _is_enum(item.get("statement_scope"), STATEMENT_SCOPES):
            _append_item_error(errors, index, "statement_scope")
        if not _is_enum(item.get("experience_kind"), EXPERIENCE_KINDS):
            _append_item_error(errors, index, "experience_kind")
        location_code = item.get("location_code")
        location_code_is_valid = isinstance(location_code, str) and location_code in location_codes | {"unregistered"}
        if not location_code_is_valid:
            _append_item_error(errors, index, "location_code")
        location_text = item.get("location_text")
        if (
            not isinstance(location_text, str)
            or not location_text.strip()
            or len(location_text) > 80
            or any(char in location_text for char in "\r\n\t")
        ):
            _append_item_error(errors, index, "location_text")
        else:
            resolved_location = aliases.get(normalize_location_alias(location_text))
            if location_code == "unregistered" and resolved_location is not None:
                _append_item_error(errors, index, "location_text", "a registered alias for an unregistered location")
            elif location_code_is_valid and location_code != "unregistered" and resolved_location != location_code:
                _append_item_error(errors, index, "location_text", "not an exact alias for location_code")
        if not _valid_public_source_url(item.get("source_url"), source_medium=item.get("source_medium"), policy=policy):
            _append_item_error(errors, index, "source_url")
        if not _canonical_timestamp(item.get("observed_at")):
            _append_item_error(errors, index, "observed_at")
        if not _matches(item.get("content_sha256"), _SHA256_PATTERN):
            _append_item_error(errors, index, "content_sha256")
        if not _matches(item.get("content_version"), _CONTENT_VERSION_PATTERN):
            _append_item_error(errors, index, "content_version")
        excerpt = item.get("excerpt")
        if (
            not isinstance(excerpt, str)
            or excerpt != excerpt.strip()
            or not excerpt
            or len(excerpt) > 280
            or any(char in excerpt for char in "\r\n\t")
        ):
            _append_item_error(errors, index, "excerpt")
        elif item.get("excerpt_sha256") != text_sha256(excerpt):
            _append_item_error(errors, index, "excerpt_sha256", "not bound to excerpt")
        if not _matches(item.get("excerpt_sha256"), _SHA256_PATTERN):
            _append_item_error(errors, index, "excerpt_sha256")
        if not _is_enum(item.get("extraction_status"), EXTRACTION_STATUSES):
            _append_item_error(errors, index, "extraction_status")
        if not _is_enum(item.get("proposal_status"), PROPOSAL_STATUSES):
            _append_item_error(errors, index, "proposal_status")
        if not _is_enum(item.get("adjudication_status"), ADJUDICATION_STATUSES):
            _append_item_error(errors, index, "adjudication_status")
        if not _is_enum(item.get("eligibility_status"), ELIGIBILITY_STATUSES):
            _append_item_error(errors, index, "eligibility_status")
        adjudication_ref = item.get("adjudication_ref")
        if adjudication_ref is not None and not _matches(adjudication_ref, _ADJUDICATION_REF_PATTERN):
            _append_item_error(errors, index, "adjudication_ref")
        adjudicated_at = item.get("adjudicated_at")
        if adjudicated_at is not None and not _canonical_timestamp(adjudicated_at):
            _append_item_error(errors, index, "adjudicated_at")
        adjudication_digest = item.get("adjudication_sha256")
        if adjudication_digest is not None and not _matches(adjudication_digest, _SHA256_PATTERN):
            _append_item_error(errors, index, "adjudication_sha256")
        try:
            expected_adjudication = adjudication_sha256(item)
        except (TypeError, ValueError):
            expected_adjudication = None
        if adjudication_digest != expected_adjudication:
            _append_item_error(errors, index, "adjudication_sha256", "not bound to adjudicated claim")

        _validate_actor_binding(item, index=index, errors=errors)
        _validate_lifecycle(item, index=index, errors=errors)

        if _matches(source_record_id, _SOURCE_RECORD_ID_PATTERN):
            source_identity = (
                item.get("source_url"),
                item.get("source_author_platform_user_id"),
                item.get("source_medium"),
                item.get("content_sha256"),
                item.get("content_version"),
                item.get("observed_at"),
                item.get("actor_binding_sha256"),
            )
            existing = source_records.setdefault(source_record_id, source_identity)
            if existing != source_identity:
                _append_item_error(errors, index, "source_record_id", "reused with inconsistent source identity")
    return errors


def classify_region_experience(payload: Mapping[str, Any], *, policy: Mapping[str, Any]) -> dict[str, Any]:
    errors = validate_evidence_bundle(payload, policy=policy)
    if errors:
        raise ValueError("region evidence bundle is invalid")
    decisions: list[dict[str, Any]] = []
    refs_by_label: dict[str, list[str]] = {label: [] for label in LABELS}
    unregistered_refs: list[str] = []
    for raw_item in payload["evidence"]:
        item = dict(raw_item)
        evidence_id = str(item["evidence_id"])
        if item["adjudication_status"] == "pending":
            decisions.append(
                {"evidence_id": evidence_id, "status": "insufficient_evidence", "reason": "pending_adjudication"}
            )
            continue
        if item["eligibility_status"] == "rejected":
            decisions.append({"evidence_id": evidence_id, "status": "rejected", "reason": "rejected_by_adjudication"})
            continue
        if item["location_code"] == "unregistered":
            decisions.append(
                {
                    "evidence_id": evidence_id,
                    "status": "unregistered_location",
                    "reason": "eligible_unregistered_location",
                }
            )
            unregistered_refs.append(evidence_id)
            continue
        decisions.append({"evidence_id": evidence_id, "status": "eligible", "reason": "eligible_explicit_experience"})
        for label in EXPECTED_LOCATION_LABELS[str(item["location_code"])]:
            refs_by_label[label].append(evidence_id)

    sorted_unregistered_refs = sorted(unregistered_refs)
    labels: list[dict[str, Any]] = []
    for label in LABELS:
        evidence_refs = sorted(refs_by_label[label])
        if evidence_refs:
            status = "supported"
        elif sorted_unregistered_refs:
            status = "unregistered_location"
        else:
            status = "insufficient_evidence"
        labels.append(
            {
                "label": label,
                "status": status,
                "evidence_refs": evidence_refs,
                "unregistered_evidence_refs": sorted_unregistered_refs,
            }
        )
    return {
        "schema_version": CLASSIFICATION_SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "policy_sha256": canonical_sha256(policy),
        "subject_ref": payload["subject_ref"],
        "subject_platform_user_id": payload["subject_platform_user_id"],
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
            "third_party_mentions_used_to_support_labels": False,
            "discovery_or_ranking_authorized": False,
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
    except (KeyError, TypeError, ValueError):
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
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        print(json.dumps({"status": "invalid", "errors": ["XREG_IO_INVALID"]}, sort_keys=True))
        return 1
    errors = [*evidence_errors, *result_errors]
    print(json.dumps({"status": "valid" if not errors else "invalid", "errors": errors}, sort_keys=True))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
