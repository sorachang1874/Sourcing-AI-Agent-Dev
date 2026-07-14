"""Provider-neutral profile/Bio evidence proposals for the X-first sibling.

This module validates post-extraction proposals against an immutable profile
snapshot.  It does not call a provider, infer a real name or protected identity,
confirm employment, or write product state.
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

POLICY_SCHEMA_VERSION = "x.profile.bio_signal.policy.v1"
POLICY_VERSION = "profile-bio-signal-v1.5"
CANONICAL_POLICY_SHA256 = "f3a055002b87d02dd9d63eb2d346661f6698263516c29e8c14b65dfe57b45341"
BUNDLE_SCHEMA_VERSION = "x.profile.bio_evidence.bundle.v1"
ANALYSIS_SCHEMA_VERSION = "x.profile.bio_signal.analysis.v1"

PROPOSAL_KINDS = (
    "observed_chinese_content",
    "china_ecosystem_self_claim",
    "organization_mention",
)
AFFILIATION_RELATIONS = ("current", "previous")
EXTRACTOR_MODES = ("offline_fixture",)

GRAMMAR_MANIFEST_VERSION = "profile-bio-positive-regex-ast-v1"
GRAMMAR_RUNTIME_ID = "profile-bio-policy-regex-interpreter-v1"
_GRAMMAR_RUNTIME_FUNCTION_NAMES = (
    "_normalize_claim_window",
    "_claim_suffix",
    "_fullmatch_policy_pattern",
    "_policy_note_allowed",
    "_claim_continuation_allowed",
    "_contains_closed_subject_claim",
    "_extract_policy_handles",
    "_match_affiliation_positive_grammar",
    "_parse_affiliation_claim",
)

_ULID = r"[0-9A-HJKMNP-TV-Z]{26}"
_SUBJECT_REF_RE = re.compile(rf"pp_x_{_ULID}")
_SNAPSHOT_ID_RE = re.compile(rf"xps_{_ULID}")
_PROPOSAL_ID_RE = re.compile(rf"xbp_{_ULID}")
_PLATFORM_USER_ID_RE = re.compile(r"[1-9][0-9]{1,24}")
_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_CANONICAL_TIME_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")
_HAN_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")

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


def _is_utf8_scalar_text(value: Any) -> bool:
    """Return whether a JSON string contains only Unicode scalar values."""
    if not isinstance(value, str):
        return False
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        return False
    return True


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


def _normalize_claim_window(value: Any, grammar_manifest: dict[str, Any]) -> str | None:
    """Return a compatibility-normalized claim only when every token is reviewable."""
    if not isinstance(value, str):
        return None
    normalized = unicodedata.normalize("NFKC", value)
    if normalized != normalized.strip():
        return None
    rejected_categories = set(grammar_manifest["rejected_unicode_categories"])
    if any(unicodedata.category(character) in rejected_categories for character in normalized):
        return None
    return normalized


def _claim_suffix(claim: str, rendered_prefix: str) -> str | None:
    if claim[: len(rendered_prefix)].casefold() != rendered_prefix.casefold():
        return None
    suffix = claim[len(rendered_prefix) :]
    if suffix.startswith(" "):
        suffix = suffix[1:]
    return suffix


def _fullmatch_policy_pattern(
    value: str,
    pattern: str,
    *,
    case_insensitive: bool,
) -> re.Match[str] | None:
    flags = re.IGNORECASE if case_insensitive else 0
    return re.fullmatch(pattern, value, flags=flags)


def _policy_note_allowed(matched: re.Match[str], continuation: dict[str, Any]) -> bool:
    note_group = continuation["note_group"]
    if note_group is None:
        return True
    note = matched.groupdict().get(note_group)
    if note is None:
        return True
    return note.casefold() in {value.casefold() for value in continuation["note_values"]}


def _claim_continuation_allowed(
    continuation: str,
    continuation_ids: list[str],
    ownership_manifest: dict[str, Any],
    *,
    case_insensitive: bool,
) -> bool:
    continuations = {item["continuation_id"]: item for item in ownership_manifest["continuations"]}
    for continuation_id in continuation_ids:
        definition = continuations[continuation_id]
        matched = _fullmatch_policy_pattern(
            continuation,
            definition["pattern"],
            case_insensitive=case_insensitive,
        )
        if matched is not None and _policy_note_allowed(matched, definition):
            return True
    return False


def _contains_closed_subject_claim(
    value: str,
    ecosystem: dict[str, Any],
    grammar_manifest: dict[str, Any],
) -> bool:
    claim = _normalize_claim_window(value, grammar_manifest)
    if claim is None:
        return False
    ownership_manifest = grammar_manifest["ownership"]
    case_insensitive = grammar_manifest["case_insensitive"]
    claim_forms = {item["form_id"]: item for item in ownership_manifest["claim_forms"]}
    for alias in ecosystem["aliases"]:
        normalized_alias = unicodedata.normalize("NFKC", alias)
        for form_id in ecosystem["claim_form_ids"]:
            form = claim_forms[form_id]
            rendered = unicodedata.normalize("NFKC", form["template"].replace("{alias}", alias))
            continuation = _claim_suffix(claim, rendered)
            if continuation is None:
                continue
            if not continuation and form["allow_empty"]:
                return True
            if continuation and _claim_continuation_allowed(
                continuation,
                form["continuation_ids"],
                ownership_manifest,
                case_insensitive=case_insensitive,
            ):
                return True
        continuation = _claim_suffix(claim, normalized_alias)
        if continuation and _claim_continuation_allowed(
            continuation,
            ecosystem["bare_alias_continuation_ids"],
            ownership_manifest,
            case_insensitive=case_insensitive,
        ):
            return True
    return False


def _extract_policy_handles(claim: str, affiliation_manifest: dict[str, Any]) -> set[str]:
    pattern = re.compile(
        affiliation_manifest["handle_mention_pattern"],
        flags=re.IGNORECASE if affiliation_manifest["case_insensitive"] else 0,
    )
    return {match.group("handle").casefold() for match in pattern.finditer(claim)}


def _match_affiliation_positive_grammar(
    grammar: dict[str, Any],
    claim: str,
    handle: str,
    affiliation_manifest: dict[str, Any],
) -> tuple[bool, str | None]:
    matched = _fullmatch_policy_pattern(
        claim,
        grammar["pattern"],
        case_insensitive=affiliation_manifest["case_insensitive"],
    )
    if matched is None:
        return False, None
    binding = grammar["handle_binding"]
    if binding == "named_group_equals":
        bound = matched.groupdict().get("handle")
        handle_matches = bound is not None and bound.casefold() == handle.casefold()
    elif binding == "mention_set_contains":
        handle_matches = handle.casefold() in _extract_policy_handles(claim, affiliation_manifest)
    else:
        return False, None
    role_group = grammar["role_group"]
    role_span = matched.groupdict().get(role_group) if role_group is not None else None
    return handle_matches, role_span


def _parse_affiliation_claim(
    value: str,
    *,
    handle: str,
    relation: str,
    grammar_manifest: dict[str, Any],
) -> tuple[str, str | None] | None:
    claim = _normalize_claim_window(value, grammar_manifest)
    if claim is None:
        return None
    affiliation_manifest = grammar_manifest["affiliation"]
    grammars = next(
        (item["grammars"] for item in affiliation_manifest["relations"] if item["relation"] == relation),
        [],
    )
    matches: list[tuple[str, str | None]] = []
    for grammar in grammars:
        matched, role_span = _match_affiliation_positive_grammar(
            grammar,
            claim,
            handle,
            affiliation_manifest,
        )
        if matched:
            matches.append((grammar["grammar_id"], role_span))
    return matches[0] if len(matches) == 1 else None


def grammar_runtime_implementation_sha256() -> str | None:
    """Hash the loaded grammar interpreter; source drift or monkeypatching fails closed."""
    try:
        parts = [inspect.getsource(globals()[name]).strip() for name in _GRAMMAR_RUNTIME_FUNCTION_NAMES]
    except (KeyError, OSError, TypeError):
        return None
    return text_sha256("\n\n".join(parts))


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
    host = parsed.hostname
    parts = [part for part in parsed.path.split("/") if part]
    return (
        value.startswith("https://")
        and host.endswith(".invalid")
        and parsed.netloc == host
        and bool(parts)
        and all(re.fullmatch(r"[A-Za-z0-9_-]+", part) is not None for part in parts)
        and parts[-1].casefold() == current_handle.casefold()
    )


def _tree_preflight(
    value: Any,
    forbidden: set[str],
    *,
    max_depth: int,
    max_nodes: int,
    path: str = "$",
) -> tuple[list[str], list[str], bool]:
    hits: list[str] = []
    budget_errors: set[str] = set()
    invalid_unicode_scalar = False
    stack: list[tuple[Any, str, int]] = [(value, path, 0)]
    visited = 0
    exhausted = False
    while stack and not exhausted:
        node, node_path, depth = stack.pop()
        visited += 1
        if visited > max_nodes:
            budget_errors.add("nested_node_budget_exceeded")
            break
        if isinstance(node, str) and not _is_utf8_scalar_text(node):
            invalid_unicode_scalar = True
            continue
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
                if isinstance(key, str) and not _is_utf8_scalar_text(key):
                    invalid_unicode_scalar = True
                    child_path = f"{node_path}.<invalid-key>"
                else:
                    folded = re.sub(r"[^a-z0-9]", "", unicodedata.normalize("NFKC", str(key)).casefold())
                    child_path = f"{node_path}.{key}"
                    if folded in forbidden:
                        hits.append(child_path)
            else:
                child_path = f"{node_path}[{key}]"
            stack.append((child, child_path, depth + 1))
    return hits, sorted(budget_errors), invalid_unicode_scalar


def validate_policy(policy: Any) -> list[str]:
    errors: list[str] = []
    expected_keys = {
        "schema_version",
        "policy_version",
        "proposal_kinds",
        "china_ecosystems",
        "positive_grammar_manifest",
        "grammar_runtime_binding",
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

    runtime_binding = policy["grammar_runtime_binding"]
    if _exact_keys(
        runtime_binding,
        {"runtime_id", "implementation_sha256"},
        path="$.grammar_runtime_binding",
        errors=errors,
    ):
        if runtime_binding["runtime_id"] != GRAMMAR_RUNTIME_ID:
            _append(errors, "$.grammar_runtime_binding.runtime_id", "unsupported")
        loaded_digest = grammar_runtime_implementation_sha256()
        if loaded_digest is None or runtime_binding["implementation_sha256"] != loaded_digest:
            _append(
                errors,
                "$.grammar_runtime_binding.implementation_sha256",
                "loaded grammar interpreter does not match the policy-pinned implementation",
            )

    manifest = policy["positive_grammar_manifest"]
    manifest_keys = {
        "manifest_version",
        "regex_dialect",
        "case_insensitive",
        "rejected_unicode_categories",
        "ownership",
        "affiliation",
    }
    if not _exact_keys(manifest, manifest_keys, path="$.positive_grammar_manifest", errors=errors):
        return errors
    if manifest["manifest_version"] != GRAMMAR_MANIFEST_VERSION:
        _append(errors, "$.positive_grammar_manifest.manifest_version", "unsupported")
    if manifest["regex_dialect"] != "python-re-fullmatch-v1":
        _append(errors, "$.positive_grammar_manifest.regex_dialect", "unsupported")
    if manifest["case_insensitive"] is not True:
        _append(errors, "$.positive_grammar_manifest.case_insensitive", "must be true")
    if manifest["rejected_unicode_categories"] != ["Cc", "Cf"]:
        _append(errors, "$.positive_grammar_manifest.rejected_unicode_categories", "must equal ['Cc', 'Cf']")

    ownership = manifest["ownership"]
    ownership_keys = {"mode", "claim_forms", "continuations"}
    claim_form_ids: set[str] = set()
    continuation_ids: set[str] = set()
    if _exact_keys(ownership, ownership_keys, path="$.positive_grammar_manifest.ownership", errors=errors):
        if ownership["mode"] != "single_window_positive_fullmatch_v2":
            _append(errors, "$.positive_grammar_manifest.ownership.mode", "unsupported")
        for index, continuation in enumerate(ownership["continuations"]):
            item_path = f"$.positive_grammar_manifest.ownership.continuations[{index}]"
            if not _exact_keys(
                continuation,
                {"continuation_id", "ast_node", "pattern", "note_group", "note_values"},
                path=item_path,
                errors=errors,
            ):
                continue
            continuation_id = continuation["continuation_id"]
            if not isinstance(continuation_id, str) or not continuation_id:
                _append(errors, f"{item_path}.continuation_id", "invalid")
            elif continuation_id in continuation_ids:
                _append(errors, f"{item_path}.continuation_id", "duplicate")
            continuation_ids.add(str(continuation_id))
            if continuation["ast_node"] != "regex_fullmatch":
                _append(errors, f"{item_path}.ast_node", "unsupported")
            try:
                compiled = re.compile(continuation["pattern"], flags=re.IGNORECASE)
            except (TypeError, re.error):
                _append(errors, f"{item_path}.pattern", "invalid")
                continue
            note_group = continuation["note_group"]
            if note_group is not None and note_group not in compiled.groupindex:
                _append(errors, f"{item_path}.note_group", "must name a pattern group")
            if note_group is None and continuation["note_values"]:
                _append(errors, f"{item_path}.note_values", "requires note_group")
        for index, form in enumerate(ownership["claim_forms"]):
            item_path = f"$.positive_grammar_manifest.ownership.claim_forms[{index}]"
            if not _exact_keys(
                form,
                {"form_id", "template", "allow_empty", "continuation_ids"},
                path=item_path,
                errors=errors,
            ):
                continue
            form_id = form["form_id"]
            if not isinstance(form_id, str) or not form_id:
                _append(errors, f"{item_path}.form_id", "invalid")
            elif form_id in claim_form_ids:
                _append(errors, f"{item_path}.form_id", "duplicate")
            claim_form_ids.add(str(form_id))
            if not isinstance(form["template"], str) or form["template"].count("{alias}") != 1:
                _append(errors, f"{item_path}.template", "must contain exactly one {alias}")
            if not isinstance(form["allow_empty"], bool):
                _append(errors, f"{item_path}.allow_empty", "must be boolean")
            if not isinstance(form["continuation_ids"], list) or any(
                item not in continuation_ids for item in form["continuation_ids"]
            ):
                _append(errors, f"{item_path}.continuation_ids", "must reference declared continuations")

    affiliation = manifest["affiliation"]
    affiliation_keys = {"case_insensitive", "handle_mention_pattern", "relations"}
    if _exact_keys(affiliation, affiliation_keys, path="$.positive_grammar_manifest.affiliation", errors=errors):
        try:
            handle_pattern = re.compile(
                affiliation["handle_mention_pattern"],
                flags=re.IGNORECASE if affiliation["case_insensitive"] else 0,
            )
        except (TypeError, re.error):
            _append(errors, "$.positive_grammar_manifest.affiliation.handle_mention_pattern", "invalid")
            handle_pattern = None
        if handle_pattern is not None and "handle" not in handle_pattern.groupindex:
            _append(
                errors,
                "$.positive_grammar_manifest.affiliation.handle_mention_pattern",
                "must expose the handle group",
            )
        relations = affiliation["relations"]
        if not isinstance(relations, list) or [item.get("relation") for item in relations] != list(
            AFFILIATION_RELATIONS
        ):
            _append(errors, "$.positive_grammar_manifest.affiliation.relations", "must equal relation registry")
        else:
            grammar_ids: set[str] = set()
            for relation_index, relation_item in enumerate(relations):
                relation_path = f"$.positive_grammar_manifest.affiliation.relations[{relation_index}]"
                if not _exact_keys(relation_item, {"relation", "grammars"}, path=relation_path, errors=errors):
                    continue
                for grammar_index, grammar in enumerate(relation_item["grammars"]):
                    item_path = f"{relation_path}.grammars[{grammar_index}]"
                    if not _exact_keys(
                        grammar,
                        {"grammar_id", "ast_node", "pattern", "handle_binding", "role_group"},
                        path=item_path,
                        errors=errors,
                    ):
                        continue
                    grammar_id = grammar["grammar_id"]
                    if not isinstance(grammar_id, str) or not grammar_id:
                        _append(errors, f"{item_path}.grammar_id", "invalid")
                    elif grammar_id in grammar_ids:
                        _append(errors, f"{item_path}.grammar_id", "duplicate")
                    grammar_ids.add(str(grammar_id))
                    if grammar["ast_node"] != "regex_fullmatch":
                        _append(errors, f"{item_path}.ast_node", "unsupported")
                    if grammar["handle_binding"] not in {"named_group_equals", "mention_set_contains"}:
                        _append(errors, f"{item_path}.handle_binding", "unsupported")
                    try:
                        compiled = re.compile(grammar["pattern"], flags=re.IGNORECASE)
                    except (TypeError, re.error):
                        _append(errors, f"{item_path}.pattern", "invalid")
                        continue
                    if grammar["handle_binding"] == "named_group_equals" and "handle" not in compiled.groupindex:
                        _append(errors, f"{item_path}.pattern", "must expose the handle group")
                    role_group = grammar["role_group"]
                    if role_group is not None and role_group not in compiled.groupindex:
                        _append(errors, f"{item_path}.role_group", "must name a pattern group")

    ecosystems = policy["china_ecosystems"]
    if not isinstance(ecosystems, list) or not ecosystems:
        _append(errors, "$.china_ecosystems", "must be a non-empty array")
        ecosystems = []
    ecosystem_ids: set[str] = set()
    for index, item in enumerate(ecosystems):
        item_path = f"$.china_ecosystems[{index}]"
        if not _exact_keys(
            item,
            {"ecosystem_id", "aliases", "claim_form_ids", "bare_alias_continuation_ids"},
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
        aliases = item["aliases"]
        if (
            not isinstance(aliases, list)
            or not aliases
            or any(not isinstance(value, str) or not value.strip() for value in aliases)
            or len({_normalize_text(value) for value in aliases}) != len(aliases)
        ):
            _append(errors, f"{item_path}.aliases", "must contain unique non-empty strings")
        if (
            not isinstance(item["claim_form_ids"], list)
            or not item["claim_form_ids"]
            or any(form_id not in claim_form_ids for form_id in item["claim_form_ids"])
        ):
            _append(errors, f"{item_path}.claim_form_ids", "must reference declared ownership claim forms")
        if not isinstance(item["bare_alias_continuation_ids"], list) or any(
            continuation_id not in continuation_ids for continuation_id in item["bare_alias_continuation_ids"]
        ):
            _append(
                errors,
                f"{item_path}.bare_alias_continuation_ids",
                "must reference declared ownership continuations",
            )

    limits = policy["limits"]
    limit_keys = {
        "max_bio_characters",
        "max_display_alias_characters",
        "max_extractor_version_characters",
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
    forbidden_hits, traversal_errors, invalid_unicode_scalar = _tree_preflight(
        bundle,
        forbidden,
        max_depth=policy["limits"]["max_nested_validation_depth"],
        max_nodes=policy["limits"]["max_nested_validation_nodes"],
    )
    for hit in forbidden_hits:
        _append(errors, hit, "forbidden protected-identity or real-name field")
    for traversal_error in traversal_errors:
        _append(errors, "$.validation", traversal_error)
    if invalid_unicode_scalar:
        _append(errors, "$.validation", "non_unicode_scalar_text")
    if traversal_errors or invalid_unicode_scalar:
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
    positive_grammar_manifest = policy["positive_grammar_manifest"]

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
            if (
                not isinstance(extractor["version"], str)
                or not extractor["version"].strip()
                or len(extractor["version"]) > policy["limits"]["max_extractor_version_characters"]
            ):
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
                if not _contains_closed_subject_claim(excerpt, ecosystem, positive_grammar_manifest):
                    _append(errors, f"{path}.excerpt", "does not fully match the closed positive ownership grammar")
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
            if details["organization_platform_user_id"] is not None:
                _append(
                    errors, f"{path}.details.organization_platform_user_id", "handle-only proposal must be unresolved"
                )
            if details["resolution_status"] != "unresolved":
                _append(errors, f"{path}.details.resolution_status", "must be unresolved")
            parsed_claim = (
                _parse_affiliation_claim(
                    excerpt,
                    handle=handle,
                    relation=relation,
                    grammar_manifest=positive_grammar_manifest,
                )
                if handle_is_valid and relation_is_valid
                else None
            )
            if parsed_claim is None:
                _append(errors, f"{path}.excerpt", "must fully match one declared positive affiliation grammar")
            else:
                _, parsed_role_span = parsed_claim
                role_window = (
                    _normalize_claim_window(role_text, positive_grammar_manifest) if role_text is not None else None
                )
                role_text_is_bounded = role_text is None or (
                    isinstance(role_text, str)
                    and bool(role_text)
                    and len(role_text) <= policy["limits"]["max_role_text_characters"]
                )
                if not role_text_is_bounded or role_window != parsed_role_span:
                    _append(
                        errors,
                        f"{path}.details.role_text",
                        "must equal the exact bounded role span parsed from the positive affiliation grammar",
                    )
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
    _, traversal_errors, invalid_unicode_scalar = _tree_preflight(
        analysis,
        set(),
        max_depth=policy["limits"]["max_nested_validation_depth"],
        max_nodes=policy["limits"]["max_nested_validation_nodes"],
    )
    if traversal_errors:
        return [f"$.validation: {error}" for error in traversal_errors]
    if invalid_unicode_scalar:
        return ["$.validation: non_unicode_scalar_text"]
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
