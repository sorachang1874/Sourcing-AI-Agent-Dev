"""Provider-neutral profile/Bio evidence proposals for the X-first sibling.

This module validates post-extraction proposals against an immutable profile
snapshot.  It does not call a provider, infer a real name or protected identity,
confirm employment, or write product state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

POLICY_SCHEMA_VERSION = "x.profile.bio_signal.policy.v1"
POLICY_VERSION = "profile-bio-signal-v1.2"
CANONICAL_POLICY_SHA256 = "02a57dd779a5838090a5d841403f3be7809089a6a12fa0e3c84873e74d807fe7"
BUNDLE_SCHEMA_VERSION = "x.profile.bio_evidence.bundle.v1"
ANALYSIS_SCHEMA_VERSION = "x.profile.bio_signal.analysis.v1"

PROPOSAL_KINDS = (
    "observed_chinese_content",
    "china_ecosystem_self_claim",
    "organization_mention",
)
AFFILIATION_RELATIONS = ("current", "previous", "unspecified")
EXTRACTOR_MODES = ("offline_fixture",)

_ULID = r"[0-9A-HJKMNP-TV-Z]{26}"
_SUBJECT_REF_RE = re.compile(rf"pp_x_{_ULID}")
_SNAPSHOT_ID_RE = re.compile(rf"xps_{_ULID}")
_PROPOSAL_ID_RE = re.compile(rf"xbp_{_ULID}")
_PLATFORM_USER_ID_RE = re.compile(r"[1-9][0-9]{1,24}")
_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_HANDLE_MENTION_RE = re.compile(r"(?<![A-Za-z0-9_])@([A-Za-z0-9_]{1,15})(?![A-Za-z0-9_])")
_BARE_ACCOUNT_IDENTIFIER_RE = re.compile(r"[a-z0-9_][a-z0-9_.-]{1,79}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_CANONICAL_TIME_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")
_HAN_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")
_RELATION_CLAUSE_SPLIT_RE = re.compile(r"[\r\n。！？!?；;，,]+")

_TOP_LEVEL_KEYS = {
    "schema_version",
    "policy_version",
    "subject",
    "profile_snapshot",
    "proposals",
    "claims",
}
_SUBJECT_KEYS = {"provisional_person_id", "platform_user_id"}
_PROFILE_KEYS = {
    "snapshot_id",
    "platform_user_id",
    "profile_url",
    "current_handle",
    "display_alias",
    "bio_text",
    "observed_at",
    "content_version",
    "content_sha256",
    "retrieval_mode",
    "native_profile_receipt_ref",
    "field_capabilities",
}
_FIELD_CAPABILITY_KEYS = {"platform_user_id", "current_handle", "display_alias", "bio_text"}
_PROPOSAL_KEYS = {
    "proposal_id",
    "kind",
    "source_field",
    "span_start",
    "span_end",
    "excerpt",
    "excerpt_sha256",
    "proposal_status",
    "extractor",
    "details",
}
_EXTRACTOR_KEYS = {"mode", "version", "tool_call_id"}
_DETAIL_KEYS = {
    "script",
    "language_candidate",
    "ecosystem_id",
    "affiliation_relation",
    "organization_handle",
    "organization_platform_user_id",
    "role_text",
    "resolution_status",
}
_CLAIM_KEYS = {
    "display_alias_used_as_identity",
    "name_signal_used",
    "nationality_inferred",
    "ethnicity_inferred",
    "physical_region_inferred_from_language_or_ecosystem",
    "affiliation_confirmed",
    "discovery_or_ranking_authorized",
    "canonical_write_authorized",
}


def load_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value).casefold()).strip()


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _append(errors: list[str], path: str, message: str) -> None:
    errors.append(f"{path}: {message}")


def _exact_keys(value: Any, expected: set[str], *, path: str, errors: list[str]) -> bool:
    if not isinstance(value, dict):
        _append(errors, path, "must be an object")
        return False
    actual = set(value)
    if actual != expected:
        _append(errors, path, f"keys must equal {sorted(expected)}")
        return False
    return True


def _canonical_time(value: Any) -> bool:
    if not isinstance(value, str) or _CANONICAL_TIME_RE.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        return False
    return parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z") == value


def _contains_marker(text: str, marker: str) -> bool:
    normalized_text = _normalize_text(text)
    normalized_marker = _normalize_text(marker)
    if not normalized_marker:
        return False
    if re.search(r"[a-z0-9]", normalized_marker):
        return (
            re.search(
                rf"(?<![a-z0-9]){re.escape(normalized_marker)}(?![a-z0-9])",
                normalized_text,
            )
            is not None
        )
    return normalized_marker in normalized_text


def _contains_handle(value: str, handle: str) -> bool:
    return re.search(rf"(?<![A-Za-z0-9_])@{re.escape(handle)}(?![A-Za-z0-9_])", value, re.IGNORECASE) is not None


def _handle_relation_clauses(value: str, handle: str) -> list[str]:
    clauses = [clause.strip() for clause in _RELATION_CLAUSE_SPLIT_RE.split(value) if clause.strip()]
    return [clause for clause in clauses if _contains_handle(clause, handle)]


def _clause_handles(value: str) -> set[str]:
    return {match.group(1).casefold() for match in _HANDLE_MENTION_RE.finditer(value)}


def _claim_continuation_allowed(continuation: str, claim_guards: dict[str, list[str]]) -> bool:
    if any(
        _contains_marker(continuation, marker)
        for field in ("post_claim_negation_markers", "third_party_operation_markers")
        for marker in claim_guards[field]
    ):
        return False
    blocked_continuations = [_normalize_text(prefix) for prefix in claim_guards["non_ownership_continuation_prefixes"]]
    return not any(continuation.startswith(prefix) for prefix in blocked_continuations)


def _contains_closed_subject_claim(
    value: str,
    ecosystem: dict[str, Any],
    claim_guards: dict[str, list[str]],
) -> bool:
    clauses = [clause.strip() for clause in _RELATION_CLAUSE_SPLIT_RE.split(value) if clause.strip()]
    negation_prefixes = [_normalize_text(prefix) for prefix in claim_guards["negation_prefixes"]]
    for clause in clauses:
        normalized_clause = _normalize_text(clause)
        if any(normalized_clause.startswith(prefix) for prefix in negation_prefixes):
            continue
        for alias in ecosystem["aliases"]:
            for template in ecosystem["subject_claim_templates"]:
                rendered = _normalize_text(template.replace("{alias}", alias))
                if not normalized_clause.startswith(rendered):
                    continue
                continuation = normalized_clause[len(rendered) :].lstrip()
                if not _claim_continuation_allowed(continuation, claim_guards):
                    continue
                return True
            if not ecosystem["bare_alias_identifier_claim"]:
                continue
            rendered_alias = _normalize_text(alias)
            if not normalized_clause.startswith(rendered_alias):
                continue
            continuation = normalized_clause[len(rendered_alias) :].lstrip()
            if _BARE_ACCOUNT_IDENTIFIER_RE.match(continuation) is None:
                continue
            if _claim_continuation_allowed(continuation, claim_guards):
                return True
    return False


def _relations_in_clause(clause: str, relation_markers: dict[str, list[str]]) -> set[str]:
    if any(_contains_marker(clause, marker) for marker in relation_markers["previous"]):
        return {"previous"}
    if any(_contains_marker(clause, marker) for marker in relation_markers["current"]):
        return {"current"}
    return set()


def _relations_for_handle(
    value: str,
    handle: str,
    relation_markers: dict[str, list[str]],
) -> set[str]:
    detected: set[str] = set()
    for clause in _handle_relation_clauses(value, handle):
        detected.update(_relations_in_clause(clause, relation_markers))
    return detected


def _has_blocked_relation_context(value: str, handle: str, blocked_markers: list[str]) -> bool:
    return any(
        _contains_marker(clause, marker)
        for clause in _handle_relation_clauses(value, handle)
        for marker in blocked_markers
    )


def _valid_profile_url(value: Any, *, current_handle: Any) -> bool:
    if not isinstance(value, str) or len(value) > 300:
        return False
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.query
        or parsed.fragment
    ):
        return False
    if not isinstance(current_handle, str) or _HANDLE_RE.fullmatch(current_handle) is None:
        return False
    host = parsed.hostname.casefold()
    parts = [part for part in parsed.path.split("/") if part]
    return (
        host.endswith(".invalid")
        and bool(parts)
        and all(re.fullmatch(r"[A-Za-z0-9_-]+", part) is not None for part in parts)
        and parts[-1].casefold() == current_handle.casefold()
    )


def _forbidden_key_hits(
    value: Any,
    forbidden: set[str],
    *,
    max_depth: int,
    max_nodes: int,
    path: str = "$",
) -> tuple[list[str], list[str]]:
    hits: list[str] = []
    budget_errors: set[str] = set()
    stack: list[tuple[Any, str, int]] = [(value, path, 0)]
    visited = 0
    exhausted = False
    while stack and not exhausted:
        node, node_path, depth = stack.pop()
        visited += 1
        if visited > max_nodes:
            budget_errors.add("nested_node_budget_exceeded")
            break
        if isinstance(node, (dict, list)) and node and depth >= max_depth:
            budget_errors.add("nested_depth_budget_exceeded")
            continue
        if isinstance(node, dict):
            children = node.items()
        elif isinstance(node, list):
            children = enumerate(node)
        else:
            continue
        for key, child in children:
            if visited + len(stack) >= max_nodes:
                budget_errors.add("nested_node_budget_exceeded")
                stack.clear()
                exhausted = True
                break
            if isinstance(node, dict):
                folded = re.sub(r"[^a-z0-9]", "", unicodedata.normalize("NFKC", str(key)).casefold())
                child_path = f"{node_path}.{key}"
                if folded in forbidden:
                    hits.append(child_path)
            else:
                child_path = f"{node_path}[{key}]"
            stack.append((child, child_path, depth + 1))
    return hits, sorted(budget_errors)


def validate_policy(policy: Any) -> list[str]:
    errors: list[str] = []
    expected_keys = {
        "schema_version",
        "policy_version",
        "proposal_kinds",
        "china_ecosystems",
        "ownership_claim_guards",
        "affiliation_relations",
        "affiliation_blocked_context_markers",
        "limits",
        "forbidden_proposal_fields",
    }
    if not _exact_keys(policy, expected_keys, path="$", errors=errors):
        return errors
    try:
        policy_sha256 = canonical_sha256(policy)
    except (TypeError, ValueError, RecursionError):
        _append(errors, "$", "must be canonical JSON")
        return errors
    if policy_sha256 != CANONICAL_POLICY_SHA256:
        _append(errors, "$", f"must exactly match the pinned {POLICY_VERSION} policy")
        return errors
    if policy["schema_version"] != POLICY_SCHEMA_VERSION:
        _append(errors, "$.schema_version", "unsupported")
    if policy["policy_version"] != POLICY_VERSION:
        _append(errors, "$.policy_version", "unsupported")
    if policy["proposal_kinds"] != list(PROPOSAL_KINDS):
        _append(errors, "$.proposal_kinds", "must equal the closed registry")

    ecosystems = policy["china_ecosystems"]
    if not isinstance(ecosystems, list) or not ecosystems:
        _append(errors, "$.china_ecosystems", "must be a non-empty array")
        ecosystems = []
    ecosystem_ids: set[str] = set()
    for index, item in enumerate(ecosystems):
        item_path = f"$.china_ecosystems[{index}]"
        if not _exact_keys(
            item,
            {"ecosystem_id", "aliases", "subject_claim_templates", "bare_alias_identifier_claim"},
            path=item_path,
            errors=errors,
        ):
            continue
        ecosystem_id = item["ecosystem_id"]
        if not isinstance(ecosystem_id, str) or re.fullmatch(r"[a-z][a-z0-9_]{1,63}", ecosystem_id) is None:
            _append(errors, f"{item_path}.ecosystem_id", "invalid")
        elif ecosystem_id in ecosystem_ids:
            _append(errors, f"{item_path}.ecosystem_id", "duplicate")
        ecosystem_ids.add(str(ecosystem_id))
        for field in ("aliases", "subject_claim_templates"):
            values = item[field]
            if (
                not isinstance(values, list)
                or not values
                or any(not isinstance(value, str) or not value.strip() for value in values)
                or len({_normalize_text(value) for value in values}) != len(values)
            ):
                _append(errors, f"{item_path}.{field}", "must contain unique non-empty strings")
        templates = item["subject_claim_templates"]
        if isinstance(templates, list) and any(
            template.count("{alias}") != 1 for template in templates if isinstance(template, str)
        ):
            _append(errors, f"{item_path}.subject_claim_templates", "each template must contain one {alias}")
        if not isinstance(item["bare_alias_identifier_claim"], bool):
            _append(errors, f"{item_path}.bare_alias_identifier_claim", "must be boolean")

    claim_guards = policy["ownership_claim_guards"]
    if _exact_keys(
        claim_guards,
        {
            "negation_prefixes",
            "non_ownership_continuation_prefixes",
            "post_claim_negation_markers",
            "third_party_operation_markers",
        },
        path="$.ownership_claim_guards",
        errors=errors,
    ):
        for field in (
            "negation_prefixes",
            "non_ownership_continuation_prefixes",
            "post_claim_negation_markers",
            "third_party_operation_markers",
        ):
            values = claim_guards[field]
            if (
                not isinstance(values, list)
                or not values
                or any(not isinstance(value, str) or not value.strip() for value in values)
                or len({_normalize_text(value) for value in values}) != len(values)
            ):
                _append(errors, f"$.ownership_claim_guards.{field}", "must contain unique non-empty strings")

    relations = policy["affiliation_relations"]
    if not isinstance(relations, list) or [
        item.get("relation") for item in relations if isinstance(item, dict)
    ] != list(AFFILIATION_RELATIONS):
        _append(errors, "$.affiliation_relations", "must equal the closed relation registry")
    else:
        for index, item in enumerate(relations):
            item_path = f"$.affiliation_relations[{index}]"
            if not _exact_keys(item, {"relation", "markers"}, path=item_path, errors=errors):
                continue
            markers = item["markers"]
            if not isinstance(markers, list) or any(
                not isinstance(marker, str) or not marker.strip() for marker in markers
            ):
                _append(errors, f"{item_path}.markers", "invalid")
            if item["relation"] == "unspecified" and markers != []:
                _append(errors, f"{item_path}.markers", "unspecified must have no markers")
            if item["relation"] != "unspecified" and not markers:
                _append(errors, f"{item_path}.markers", "specific relation requires markers")

    blocked_context_markers = policy["affiliation_blocked_context_markers"]
    if (
        not isinstance(blocked_context_markers, list)
        or not blocked_context_markers
        or any(not isinstance(value, str) or not value.strip() for value in blocked_context_markers)
        or len({_normalize_text(value) for value in blocked_context_markers}) != len(blocked_context_markers)
    ):
        _append(errors, "$.affiliation_blocked_context_markers", "must contain unique non-empty strings")

    limits = policy["limits"]
    limit_keys = {
        "max_bio_characters",
        "max_display_alias_characters",
        "max_proposals",
        "max_excerpt_characters",
        "max_role_text_characters",
        "max_nested_validation_depth",
        "max_nested_validation_nodes",
    }
    if _exact_keys(limits, limit_keys, path="$.limits", errors=errors):
        if any(not _is_int(value) or value <= 0 for value in limits.values()):
            _append(errors, "$.limits", "all values must be positive integers")

    forbidden = policy["forbidden_proposal_fields"]
    if (
        not isinstance(forbidden, list)
        or not forbidden
        or any(not isinstance(value, str) or not value for value in forbidden)
        or forbidden != sorted(set(forbidden))
    ):
        _append(errors, "$.forbidden_proposal_fields", "must be a sorted unique string registry")
    return errors


def validate_evidence_bundle(bundle: Any, *, policy: Any) -> list[str]:
    errors = validate_policy(policy)
    if errors:
        return [f"policy {error}" for error in errors]
    if not _exact_keys(bundle, _TOP_LEVEL_KEYS, path="$", errors=errors):
        return errors
    if bundle["schema_version"] != BUNDLE_SCHEMA_VERSION:
        _append(errors, "$.schema_version", "unsupported")
    if bundle["policy_version"] != policy["policy_version"]:
        _append(errors, "$.policy_version", "mismatch")

    forbidden = {re.sub(r"[^a-z0-9]", "", value.casefold()) for value in policy["forbidden_proposal_fields"]}
    forbidden_hits, traversal_errors = _forbidden_key_hits(
        bundle,
        forbidden,
        max_depth=policy["limits"]["max_nested_validation_depth"],
        max_nodes=policy["limits"]["max_nested_validation_nodes"],
    )
    for hit in forbidden_hits:
        _append(errors, hit, "forbidden protected-identity or real-name field")
    for traversal_error in traversal_errors:
        _append(errors, "$.validation", traversal_error)
    if traversal_errors:
        return errors

    subject = bundle["subject"]
    if _exact_keys(subject, _SUBJECT_KEYS, path="$.subject", errors=errors):
        if (
            not isinstance(subject["provisional_person_id"], str)
            or _SUBJECT_REF_RE.fullmatch(subject["provisional_person_id"]) is None
        ):
            _append(errors, "$.subject.provisional_person_id", "invalid")
        if (
            not isinstance(subject["platform_user_id"], str)
            or _PLATFORM_USER_ID_RE.fullmatch(subject["platform_user_id"]) is None
        ):
            _append(errors, "$.subject.platform_user_id", "invalid")

    profile = bundle["profile_snapshot"]
    bio_text = ""
    retrieval_mode = ""
    if _exact_keys(profile, _PROFILE_KEYS, path="$.profile_snapshot", errors=errors):
        if not isinstance(profile["snapshot_id"], str) or _SNAPSHOT_ID_RE.fullmatch(profile["snapshot_id"]) is None:
            _append(errors, "$.profile_snapshot.snapshot_id", "invalid")
        if (
            not isinstance(profile["platform_user_id"], str)
            or _PLATFORM_USER_ID_RE.fullmatch(profile["platform_user_id"]) is None
        ):
            _append(errors, "$.profile_snapshot.platform_user_id", "invalid")
        if isinstance(subject, dict) and profile["platform_user_id"] != subject.get("platform_user_id"):
            _append(errors, "$.profile_snapshot.platform_user_id", "must equal subject platform_user_id")
        retrieval_mode = profile["retrieval_mode"]
        if retrieval_mode not in EXTRACTOR_MODES:
            _append(errors, "$.profile_snapshot.retrieval_mode", "unsupported")
        current_handle = profile["current_handle"]
        if not isinstance(current_handle, str) or _HANDLE_RE.fullmatch(current_handle) is None:
            _append(errors, "$.profile_snapshot.current_handle", "invalid")
        if not _valid_profile_url(profile["profile_url"], current_handle=current_handle):
            _append(errors, "$.profile_snapshot.profile_url", "must be a fixture URL bound to current_handle")
        display_alias = profile["display_alias"]
        if not isinstance(display_alias, str) or len(display_alias) > policy["limits"]["max_display_alias_characters"]:
            _append(errors, "$.profile_snapshot.display_alias", "invalid")
        bio_text = profile["bio_text"]
        if (
            not isinstance(bio_text, str)
            or not bio_text.strip()
            or len(bio_text) > policy["limits"]["max_bio_characters"]
            or any(ord(character) == 0 for character in bio_text)
        ):
            _append(errors, "$.profile_snapshot.bio_text", "invalid")
            bio_text = ""
        if not _canonical_time(profile["observed_at"]):
            _append(errors, "$.profile_snapshot.observed_at", "must be canonical UTC milliseconds")
        if (
            not isinstance(profile["content_version"], str)
            or not profile["content_version"].strip()
            or len(profile["content_version"]) > 80
        ):
            _append(errors, "$.profile_snapshot.content_version", "invalid")
        if profile["content_sha256"] != text_sha256(bio_text):
            _append(errors, "$.profile_snapshot.content_sha256", "mismatch")
        receipt = profile["native_profile_receipt_ref"]
        if receipt is not None:
            _append(
                errors, "$.profile_snapshot.native_profile_receipt_ref", "v1 is fixture-only and receipt must be null"
            )
        capabilities = profile["field_capabilities"]
        if _exact_keys(
            capabilities,
            _FIELD_CAPABILITY_KEYS,
            path="$.profile_snapshot.field_capabilities",
            errors=errors,
        ):
            if any(value != "present" for value in capabilities.values()):
                _append(errors, "$.profile_snapshot.field_capabilities", "fixture slice requires all fields present")

    proposals = bundle["proposals"]
    if not isinstance(proposals, list) or not proposals or len(proposals) > policy["limits"]["max_proposals"]:
        _append(errors, "$.proposals", "must be a bounded non-empty array")
        proposals = []
    proposal_ids: set[str] = set()
    affiliation_signatures: set[tuple[str, str]] = set()
    ecosystem_by_id = {item["ecosystem_id"]: item for item in policy["china_ecosystems"]}
    relation_markers = {item["relation"]: item["markers"] for item in policy["affiliation_relations"]}
    blocked_relation_contexts = policy["affiliation_blocked_context_markers"]

    for index, proposal in enumerate(proposals):
        path = f"$.proposals[{index}]"
        if not _exact_keys(proposal, _PROPOSAL_KEYS, path=path, errors=errors):
            continue
        proposal_id = proposal["proposal_id"]
        if not isinstance(proposal_id, str) or _PROPOSAL_ID_RE.fullmatch(proposal_id) is None:
            _append(errors, f"{path}.proposal_id", "invalid")
        elif proposal_id in proposal_ids:
            _append(errors, f"{path}.proposal_id", "duplicate")
        proposal_ids.add(str(proposal_id))
        kind = proposal["kind"]
        if kind not in PROPOSAL_KINDS:
            _append(errors, f"{path}.kind", "unsupported")
        if proposal["source_field"] != "bio_text":
            _append(errors, f"{path}.source_field", "must be bio_text")
        start = proposal["span_start"]
        end = proposal["span_end"]
        excerpt = proposal["excerpt"]
        if not _is_int(start) or not _is_int(end) or start < 0 or end <= start or end > len(bio_text):
            _append(errors, f"{path}.span", "invalid")
        elif not isinstance(excerpt, str) or bio_text[start:end] != excerpt:
            _append(errors, f"{path}.excerpt", "must exactly match the declared Bio span")
        if (
            not isinstance(excerpt, str)
            or not excerpt.strip()
            or len(excerpt) > policy["limits"]["max_excerpt_characters"]
        ):
            _append(errors, f"{path}.excerpt", "invalid")
            excerpt = ""
        if proposal["excerpt_sha256"] != text_sha256(excerpt):
            _append(errors, f"{path}.excerpt_sha256", "mismatch")
        if proposal["proposal_status"] != "proposed":
            _append(errors, f"{path}.proposal_status", "must remain proposed")

        extractor = proposal["extractor"]
        if _exact_keys(extractor, _EXTRACTOR_KEYS, path=f"{path}.extractor", errors=errors):
            if extractor["mode"] != retrieval_mode:
                _append(errors, f"{path}.extractor.mode", "must match snapshot retrieval mode")
            if not isinstance(extractor["version"], str) or not extractor["version"].strip():
                _append(errors, f"{path}.extractor.version", "invalid")
            if extractor["tool_call_id"] is not None:
                _append(errors, f"{path}.extractor.tool_call_id", "v1 fixture tool call must be null")

        details = proposal["details"]
        if not _exact_keys(details, _DETAIL_KEYS, path=f"{path}.details", errors=errors):
            continue
        for field, value in details.items():
            if value is not None and not isinstance(value, str):
                _append(errors, f"{path}.details.{field}", "must be a string or null")
        if kind == "observed_chinese_content":
            if details != {
                "script": "han",
                "language_candidate": "zh",
                "ecosystem_id": None,
                "affiliation_relation": None,
                "organization_handle": None,
                "organization_platform_user_id": None,
                "role_text": None,
                "resolution_status": None,
            }:
                _append(errors, f"{path}.details", "invalid observed-content details")
            if _HAN_RE.search(excerpt) is None:
                _append(errors, f"{path}.excerpt", "does not directly contain Han-script content")
        elif kind == "china_ecosystem_self_claim":
            ecosystem_id = details["ecosystem_id"]
            ecosystem = ecosystem_by_id.get(ecosystem_id) if isinstance(ecosystem_id, str) else None
            expected = {
                "script": None,
                "language_candidate": None,
                "ecosystem_id": ecosystem_id,
                "affiliation_relation": None,
                "organization_handle": None,
                "organization_platform_user_id": None,
                "role_text": None,
                "resolution_status": "subject_claimed",
            }
            if details != expected:
                _append(errors, f"{path}.details", "invalid ecosystem details")
            if ecosystem is None:
                _append(errors, f"{path}.details.ecosystem_id", "unregistered")
            else:
                if not _contains_closed_subject_claim(excerpt, ecosystem, policy["ownership_claim_guards"]):
                    _append(errors, f"{path}.excerpt", "does not match an anchored same-clause subject-claim grammar")
        elif kind == "organization_mention":
            relation = details["affiliation_relation"]
            handle = details["organization_handle"]
            role_text = details["role_text"]
            relation_is_valid = isinstance(relation, str) and relation in AFFILIATION_RELATIONS
            if not relation_is_valid:
                _append(errors, f"{path}.details.affiliation_relation", "unsupported")
            handle_is_valid = isinstance(handle, str) and _HANDLE_RE.fullmatch(handle) is not None
            if not handle_is_valid:
                _append(errors, f"{path}.details.organization_handle", "invalid")
            elif not _contains_handle(excerpt, handle):
                _append(errors, f"{path}.excerpt", "does not contain the exact organization mention")
            if details["organization_platform_user_id"] is not None:
                _append(
                    errors, f"{path}.details.organization_platform_user_id", "handle-only proposal must be unresolved"
                )
            if details["resolution_status"] != "unresolved":
                _append(errors, f"{path}.details.resolution_status", "must be unresolved")
            if role_text is not None:
                role_text_is_valid = (
                    isinstance(role_text, str)
                    and bool(role_text.strip())
                    and len(role_text) <= policy["limits"]["max_role_text_characters"]
                )
                if not role_text_is_valid or not handle_is_valid or not relation_is_valid or relation == "unspecified":
                    _append(
                        errors,
                        f"{path}.details.role_text",
                        "requires a specific relation and a bounded same-clause role/handle binding",
                    )
                else:
                    expected_role_relation = {relation}
                    role_bound = any(
                        _contains_marker(clause, role_text)
                        and _relations_in_clause(clause, relation_markers) == expected_role_relation
                        and _clause_handles(clause) == {handle.casefold()}
                        for clause in _handle_relation_clauses(excerpt, handle)
                    )
                    if not role_bound:
                        _append(
                            errors,
                            f"{path}.details.role_text",
                            "role, relation marker, and sole target handle must share one clause",
                        )
            if handle_is_valid:
                detected_relations = _relations_for_handle(excerpt, handle, relation_markers)
                expected_relations = {relation} if relation_is_valid and relation in ("current", "previous") else set()
                if _has_blocked_relation_context(excerpt, handle, blocked_relation_contexts):
                    _append(errors, f"{path}.excerpt", "handle clause contains blocked negation or recruiting context")
                elif detected_relations != expected_relations:
                    _append(errors, f"{path}.excerpt", "handle clause must contain only the declared relation")
            signature = (str(relation), str(handle).casefold())
            if signature in affiliation_signatures:
                _append(errors, f"{path}.details", "duplicate relation/organization proposal")
            affiliation_signatures.add(signature)

    claims = bundle["claims"]
    if _exact_keys(claims, _CLAIM_KEYS, path="$.claims", errors=errors):
        if any(value is not False for value in claims.values()):
            _append(errors, "$.claims", "all authority/protected-identity claims must be false")
    return errors


def _derived_id(prefix: str, value: Any) -> str:
    return f"{prefix}_{canonical_sha256(value)[:24]}"


def analyze_profile_bio_signals(bundle: Any, *, policy: Any) -> dict[str, Any]:
    errors = validate_evidence_bundle(bundle, policy=policy)
    if errors:
        raise ValueError("; ".join(errors[:20]))
    proposals = sorted(bundle["proposals"], key=lambda item: item["proposal_id"])
    signals: list[dict[str, Any]] = []
    affiliations: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    ecosystem_refs: list[str] = []

    for proposal in proposals:
        proposal_id = proposal["proposal_id"]
        kind = proposal["kind"]
        details = proposal["details"]
        if kind == "observed_chinese_content":
            signals.append(
                {
                    "signal_id": _derived_id("xsig", {"kind": kind, "proposal_id": proposal_id}),
                    "signal_type": kind,
                    "state": "observed",
                    "value": "zh_candidate",
                    "evidence_ref": proposal_id,
                }
            )
        elif kind == "china_ecosystem_self_claim":
            ecosystem_refs.append(proposal_id)
            signals.append(
                {
                    "signal_id": _derived_id("xsig", {"kind": kind, "proposal_id": proposal_id}),
                    "signal_type": "china_ecosystem_experience_lead",
                    "state": "subject_claimed",
                    "value": details["ecosystem_id"],
                    "evidence_ref": proposal_id,
                }
            )
        elif kind == "organization_mention":
            relation = details["affiliation_relation"]
            handle = details["organization_handle"]
            affiliations.append(
                {
                    "affiliation_proposal_id": _derived_id(
                        "xaf",
                        {"handle": handle.casefold(), "proposal_id": proposal_id, "relation": relation},
                    ),
                    "relation": relation,
                    "organization_handle": handle,
                    "organization_platform_user_id": None,
                    "role_text": details["role_text"],
                    "resolution_status": "unresolved",
                    "confirmation_status": "subject_claim_only",
                    "evidence_ref": proposal_id,
                }
            )
            edges.append(
                {
                    "edge_id": _derived_id(
                        "xedge",
                        {
                            "from": bundle["subject"]["platform_user_id"],
                            "handle": handle.casefold(),
                            "proposal_id": proposal_id,
                            "relation": relation,
                        },
                    ),
                    "from_platform_user_id": bundle["subject"]["platform_user_id"],
                    "to_handle_observed": handle,
                    "to_platform_user_id": None,
                    "edge_type": f"subject_claimed_{relation}_affiliation",
                    "resolution_status": "unresolved",
                    "evidence_ref": proposal_id,
                }
            )

    result = {
        "schema_version": ANALYSIS_SCHEMA_VERSION,
        "policy_version": policy["policy_version"],
        "analysis_id": _derived_id(
            "xpba",
            {"bundle_sha256": canonical_sha256(bundle), "policy_sha256": canonical_sha256(policy)},
        ),
        "input_sha256": canonical_sha256(bundle),
        "policy_sha256": canonical_sha256(policy),
        "subject": dict(bundle["subject"]),
        "profile_capability": {
            "retrieval_mode": bundle["profile_snapshot"]["retrieval_mode"],
            "stable_account_identity": "fixture_only_not_live_proven",
            "bio_text": "fixture_present_not_live_proven",
            "display_alias_role": "raw_alias_only",
        },
        "signals": signals,
        "affiliation_proposals": affiliations,
        "graph_edges": edges,
        "experience_leads": {
            "china_digital_ecosystem": {
                "status": "supported" if ecosystem_refs else "insufficient_evidence",
                "evidence_refs": sorted(ecosystem_refs),
            },
            "physical_region_experience": {
                "status": "not_evaluated",
                "evidence_refs": [],
            },
        },
        "claims": {key: False for key in sorted(_CLAIM_KEYS)},
    }
    return result


def validate_analysis(analysis: Any, *, evidence_bundle: Any, policy: Any) -> list[str]:
    errors = validate_evidence_bundle(evidence_bundle, policy=policy)
    if errors:
        return [f"evidence {error}" for error in errors]
    if not isinstance(analysis, dict):
        return ["$: must be an object"]
    _, traversal_errors = _forbidden_key_hits(
        analysis,
        set(),
        max_depth=policy["limits"]["max_nested_validation_depth"],
        max_nodes=policy["limits"]["max_nested_validation_nodes"],
    )
    if traversal_errors:
        return [f"$.validation: {error}" for error in traversal_errors]
    expected = analyze_profile_bio_signals(evidence_bundle, policy=policy)
    try:
        matches_expected = analysis == expected
    except RecursionError:
        return ["$.validation: nested_depth_budget_exceeded"]
    if not matches_expected:
        return ["$: analysis does not equal deterministic recomputation"]
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--policy", type=Path, required=True)
    args = parser.parse_args()
    try:
        bundle = load_json(args.bundle)
        policy = load_json(args.policy)
        errors = validate_evidence_bundle(bundle, policy=policy)
        payload = {"status": "valid", "errors": []} if not errors else {"status": "invalid", "errors": errors}
    except (OSError, json.JSONDecodeError, TypeError, ValueError, RecursionError):
        payload = {"status": "invalid", "errors": ["input_unreadable_or_invalid"]}
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "valid" else 1


if __name__ == "__main__":
    raise SystemExit(main())
