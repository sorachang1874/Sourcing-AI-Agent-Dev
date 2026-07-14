"""Luna-native, source-bound profile Bio semantic proposals.

The module builds an OpenAI-compatible Responses API request with strict
structured output, but performs no network I/O itself. A caller must inject a
transport, and any transport declaring itself live is blocked unless the caller
also passes ``execute_live=True``. Model output remains an unverified
professional-context proposal and has no discovery, ranking, eligibility,
employment-confirmation, identity, outreach, or product-write authority.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

REQUEST_SCHEMA_VERSION = "x.profile.bio_semantic.request.v2"
PROMPT_SCHEMA_VERSION = "x.profile.bio_semantic.prompt.v2"
PROMPT_VERSION = "profile-bio-semantic-prompt-v2.0"
MODEL_OUTPUT_SCHEMA_VERSION = "x.profile.bio_semantic.model_output.v2"
REVIEW_SCHEMA_VERSION = "x.profile.bio_semantic.review.v2"
MODEL_ID = "gpt-5.6-luna"
PROVIDER_ID = "openai_compatible_responses"

# Replaced after the new prompt and strict-output schema are finalized.
CANONICAL_PROMPT_SHA256 = "fb401860f83d834e56ad3fec555d2eed30ebb97a79a3b6333fb281a77fdddc98"
CANONICAL_OUTPUT_SCHEMA_SHA256 = "f0aa50c7d2b7b052c8f422ebec3a18b7eb95f71d2d6de6775845e1ce66231b58"

REQUEST_AUTHORITY = {
    "external_facts_allowed": False,
    "protected_identity_inference_allowed": False,
    "discovery_or_ranking_allowed": False,
    "eligibility_decision_allowed": False,
    "canonical_employment_allowed": False,
    "canonical_write_allowed": False,
    "fallback_allowed": False,
}
MODEL_AUTHORITY = {
    "external_facts_used": False,
    "protected_identity_inferred": False,
    "discovery_or_ranking_recommended": False,
    "eligibility_decided": False,
    "canonical_employment_asserted": False,
    "canonical_write_recommended": False,
}
REVIEW_AUTHORITY = {
    "external_facts_accepted": False,
    "protected_identity_inferred": False,
    "discovery_or_ranking_authorized": False,
    "eligibility_decided": False,
    "canonical_employment_confirmed": False,
    "canonical_write_authorized": False,
    "outreach_authorized": False,
}
BUDGETS = {
    "max_external_calls": 1,
    "max_proposals": 12,
    "max_model_output_characters": 12000,
    "max_input_tokens": 4000,
    "max_output_tokens": 1600,
    "max_total_tokens": 5600,
    "timeout_ms": 30000,
    "max_validation_depth": 64,
    "max_validation_nodes": 4096,
}

PROPOSAL_TYPES = frozenset(
    {
        "professional_region_experience",
        "professional_china_digital_ecosystem",
        "professional_affiliation",
        "observed_chinese_professional_content",
    }
)
RELATION_STATES = frozenset(
    {"current_claimed", "previous_claimed", "future_or_aspirational", "unspecified", "not_applicable"}
)
VERDICTS = frozenset({"proposals_available", "no_supported_professional_context", "abstained"})
CONFIDENCE_STATES = frozenset({"high", "medium", "low"})
REASON_CODES = frozenset(
    {
        "explicit_professional_region_experience",
        "explicit_china_digital_ecosystem_activity",
        "explicit_current_organization_claim",
        "explicit_previous_organization_claim",
        "observed_chinese_professional_content",
        "ambiguous_professional_context_requires_review",
    }
)
REASON_CODES_BY_TYPE = {
    "professional_region_experience": frozenset(
        {"explicit_professional_region_experience", "ambiguous_professional_context_requires_review"}
    ),
    "professional_china_digital_ecosystem": frozenset(
        {"explicit_china_digital_ecosystem_activity", "ambiguous_professional_context_requires_review"}
    ),
    "professional_affiliation": frozenset(
        {
            "explicit_current_organization_claim",
            "explicit_previous_organization_claim",
            "ambiguous_professional_context_requires_review",
        }
    ),
    "observed_chinese_professional_content": frozenset({"observed_chinese_professional_content"}),
}

_REQUEST_KEYS = {
    "schema_version",
    "request_id",
    "profile_source_mode",
    "model_execution_mode",
    "subject",
    "profile_snapshot",
    "model_policy",
    "budgets",
    "authority",
}
_SUBJECT_KEYS = {"provisional_person_id", "platform_user_id"}
_PROFILE_KEYS = {
    "snapshot_id",
    "platform_user_id",
    "profile_url",
    "current_handle",
    "bio_text",
    "bio_sha256",
    "observed_at",
    "content_version",
}
_MODEL_POLICY_KEYS = {
    "provider",
    "model_id",
    "reasoning_effort",
    "prompt_version",
    "prompt_sha256",
    "output_schema_version",
    "output_schema_sha256",
    "strict_structured_output",
    "fallback_model",
}
_PROMPT_KEYS = {"schema_version", "prompt_version", "model_id", "developer_instructions"}
_MODEL_OUTPUT_KEYS = {
    "schema_version",
    "request_id",
    "profile_snapshot_id",
    "platform_user_id",
    "bio_sha256",
    "verdict",
    "proposals",
    "authority",
}
_MODEL_PROPOSAL_KEYS = {
    "proposal_type",
    "relation_state",
    "span_start",
    "span_end",
    "excerpt",
    "reason_codes",
    "reason",
    "reason_source",
    "confidence",
    "evidence_basis",
    "requires_independent_verification",
}

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_PLATFORM_USER_ID_RE = re.compile(r"[1-9][0-9]{1,24}")
_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_PROVISIONAL_PERSON_RE = re.compile(r"pp_x_[0-9A-HJKMNP-TV-Z]{26}")
_RESPONSE_ID_RE = re.compile(r"resp_[A-Za-z0-9_-]{8,120}")
_REASONING_ID_RE = re.compile(r"rs_[A-Za-z0-9_-]{8,120}")
_CANONICAL_TIME_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")
_URL_RE = re.compile(r"(?i)(?:https?://|www\.)")
_HANDLE_MENTION_RE = re.compile(r"(?<![A-Za-z0-9_])@[A-Za-z0-9_]{1,15}(?![A-Za-z0-9_])")
_NUMBER_TOKEN_RE = re.compile(r"(?<![A-Za-z0-9_])\d+(?:\.\d+)?(?:[KkMm]|万|千|%)?(?![A-Za-z0-9_])")
_PROHIBITED_REASON_TERMS = (
    "ethnicity",
    "nationality",
    "race",
    "citizenship",
    "ancestry",
    "religion",
    "gender",
    "asian person",
    "asian researcher",
    "ethnic",
    "han person",
    "chinese person",
    "chinese researcher",
    "华人",
    "中国人",
    "亚洲人",
)
_EXTERNAL_FACT_MARKERS = (
    "according to external",
    "known from external",
    "outside the bio",
    "public records show",
    "web search shows",
)


class ResponsesTransport(Protocol):
    """Injected transport boundary; implementations own any actual I/O."""

    is_live: bool
    transport_id: str

    def create_response(self, payload: Mapping[str, Any], *, timeout_ms: int) -> Mapping[str, Any]: ...


@dataclass
class OfflineFakeResponsesTransport:
    """Deterministic no-network transport used only by fixtures and tests."""

    response: Mapping[str, Any]
    is_live: bool = False
    transport_id: str = "offline_fake_responses"
    calls: int = 0
    last_payload: dict[str, Any] | None = field(default=None, init=False)
    last_timeout_ms: int | None = field(default=None, init=False)

    def create_response(self, payload: Mapping[str, Any], *, timeout_ms: int) -> Mapping[str, Any]:
        self.calls += 1
        self.last_payload = copy.deepcopy(dict(payload))
        self.last_timeout_ms = timeout_ms
        return self.response


class _ReviewError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def load_json(path: str | Path) -> Any:
    return _strict_json_loads(Path(path).read_text(encoding="utf-8"))


def _strict_json_loads(text: str) -> Any:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate_json_key")
            value[key] = item
        return value

    def reject_non_finite(token: str) -> Any:
        raise ValueError(f"non_finite_json_number:{token}")

    return json.loads(
        text,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_non_finite,
    )


def canonical_json(value: Any) -> str:
    return json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_utf8_scalar_text(value: Any, *, minimum: int = 0, maximum: int) -> bool:
    if not isinstance(value, str) or not minimum <= len(value) <= maximum or "\x00" in value:
        return False
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        return False
    return True


def _append(errors: list[str], path: str, message: str) -> None:
    errors.append(f"{path}: {message}")


def _exact_keys(value: Any, expected: set[str], *, path: str, errors: list[str]) -> bool:
    if not isinstance(value, dict):
        _append(errors, path, "must be an object")
        return False
    if set(value) != expected:
        _append(errors, path, f"keys must equal {sorted(expected)}")
        return False
    return True


def _scan_json(value: Any, *, max_depth: int, max_nodes: int) -> list[str]:
    """Iteratively reject attacker-sized/deep JSON before recursive operations."""

    errors: set[str] = set()
    stack: list[tuple[Any, int]] = [(value, 0)]
    visited = 0
    exhausted = False
    while stack and not exhausted:
        node, depth = stack.pop()
        visited += 1
        if visited > max_nodes:
            errors.add("nested_node_budget_exceeded")
            break
        if isinstance(node, (dict, list)) and node and depth >= max_depth:
            errors.add("nested_depth_budget_exceeded")
            continue
        children = node.values() if isinstance(node, dict) else node if isinstance(node, list) else ()
        for child in children:
            if visited + len(stack) >= max_nodes:
                errors.add("nested_node_budget_exceeded")
                stack.clear()
                exhausted = True
                break
            stack.append((child, depth + 1))
    return sorted(errors)


def _canonical_time(value: Any) -> bool:
    if not isinstance(value, str) or _CANONICAL_TIME_RE.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        return False
    return parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z") == value


def _valid_fixture_profile_url(value: Any, *, handle: Any) -> bool:
    if not isinstance(value, str) or not isinstance(handle, str) or _HANDLE_RE.fullmatch(handle) is None:
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
    parts = [part for part in parsed.path.split("/") if part]
    if not parts or parts[-1].casefold() != handle.casefold():
        return False
    return parsed.hostname.casefold().endswith(".invalid")


def validate_prompt(prompt: Any) -> list[str]:
    errors: list[str] = []
    traversal = _scan_json(prompt, max_depth=64, max_nodes=4096)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    if not _exact_keys(prompt, _PROMPT_KEYS, path="$", errors=errors):
        return errors
    if prompt["schema_version"] != PROMPT_SCHEMA_VERSION:
        _append(errors, "$.schema_version", "unsupported")
    if prompt["prompt_version"] != PROMPT_VERSION:
        _append(errors, "$.prompt_version", "unsupported")
    if prompt["model_id"] != MODEL_ID:
        _append(errors, "$.model_id", "must equal gpt-5.6-luna")
    instructions = prompt["developer_instructions"]
    if not _is_utf8_scalar_text(instructions, minimum=100, maximum=4000):
        _append(errors, "$.developer_instructions", "must be a bounded non-empty string")
    try:
        digest = canonical_sha256(prompt)
    except (TypeError, ValueError, RecursionError):
        _append(errors, "$", "must be canonical JSON")
    else:
        if digest != CANONICAL_PROMPT_SHA256:
            _append(errors, "$", "must exactly match the pinned semantic prompt")
    return errors


def validate_output_schema(schema: Any) -> list[str]:
    traversal = _scan_json(schema, max_depth=64, max_nodes=4096)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    if not isinstance(schema, dict):
        return ["$: must be an object"]
    try:
        digest = canonical_sha256(schema)
    except (TypeError, ValueError, RecursionError):
        return ["$: must be canonical JSON"]
    errors: list[str] = []
    if digest != CANONICAL_OUTPUT_SCHEMA_SHA256:
        _append(errors, "$", "must exactly match the pinned strict output schema")
    version = schema.get("properties", {}).get("schema_version", {}).get("const")
    if version != MODEL_OUTPUT_SCHEMA_VERSION:
        _append(errors, "$.properties.schema_version.const", "unsupported")
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        _append(errors, "$", "root must be a closed object schema")
    return errors


def validate_request(request: Any, *, prompt: Any, output_schema: Any) -> list[str]:
    errors: list[str] = []
    traversal = _scan_json(request, max_depth=64, max_nodes=4096)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    if not _exact_keys(request, _REQUEST_KEYS, path="$", errors=errors):
        return errors
    if request["schema_version"] != REQUEST_SCHEMA_VERSION:
        _append(errors, "$.schema_version", "unsupported")
    if (
        not isinstance(request["request_id"], str)
        or re.fullmatch(r"xbsv2r_[0-9a-f]{24}", request["request_id"]) is None
    ):
        _append(errors, "$.request_id", "invalid")
    if request["profile_source_mode"] != "offline_fixture":
        _append(errors, "$.profile_source_mode", "v2 supports only offline_fixture source snapshots")
    if request["model_execution_mode"] not in {"offline_fake", "live_canary"}:
        _append(errors, "$.model_execution_mode", "unsupported")

    subject = request["subject"]
    if _exact_keys(subject, _SUBJECT_KEYS, path="$.subject", errors=errors):
        if (
            not isinstance(subject["provisional_person_id"], str)
            or _PROVISIONAL_PERSON_RE.fullmatch(subject["provisional_person_id"]) is None
        ):
            _append(errors, "$.subject.provisional_person_id", "invalid")
        if (
            not isinstance(subject["platform_user_id"], str)
            or _PLATFORM_USER_ID_RE.fullmatch(subject["platform_user_id"]) is None
        ):
            _append(errors, "$.subject.platform_user_id", "invalid")

    profile = request["profile_snapshot"]
    if _exact_keys(profile, _PROFILE_KEYS, path="$.profile_snapshot", errors=errors):
        if (
            not isinstance(profile["snapshot_id"], str)
            or re.fullmatch(r"xbsv2s_[0-9a-f]{24}", profile["snapshot_id"]) is None
        ):
            _append(errors, "$.profile_snapshot.snapshot_id", "invalid")
        if (
            not isinstance(profile["platform_user_id"], str)
            or _PLATFORM_USER_ID_RE.fullmatch(profile["platform_user_id"]) is None
        ):
            _append(errors, "$.profile_snapshot.platform_user_id", "invalid")
        if isinstance(subject, dict) and profile["platform_user_id"] != subject.get("platform_user_id"):
            _append(errors, "$.profile_snapshot.platform_user_id", "must equal subject platform_user_id")
        if not isinstance(profile["current_handle"], str) or _HANDLE_RE.fullmatch(profile["current_handle"]) is None:
            _append(errors, "$.profile_snapshot.current_handle", "invalid")
        if not _valid_fixture_profile_url(profile["profile_url"], handle=profile["current_handle"]):
            _append(errors, "$.profile_snapshot.profile_url", "must be an .invalid fixture URL bound to handle")
        bio_text = profile["bio_text"]
        if not _is_utf8_scalar_text(bio_text, minimum=1, maximum=2000) or not bio_text.strip():
            _append(errors, "$.profile_snapshot.bio_text", "invalid")
            bio_text = ""
        if profile["bio_sha256"] != text_sha256(bio_text):
            _append(errors, "$.profile_snapshot.bio_sha256", "mismatch")
        if not _canonical_time(profile["observed_at"]):
            _append(errors, "$.profile_snapshot.observed_at", "must be canonical UTC milliseconds")
        if (
            not isinstance(profile["content_version"], str)
            or not profile["content_version"].strip()
            or len(profile["content_version"]) > 80
        ):
            _append(errors, "$.profile_snapshot.content_version", "invalid")

    policy = request["model_policy"]
    if _exact_keys(policy, _MODEL_POLICY_KEYS, path="$.model_policy", errors=errors):
        expected_policy = {
            "provider": PROVIDER_ID,
            "model_id": MODEL_ID,
            "reasoning_effort": "low",
            "prompt_version": PROMPT_VERSION,
            "prompt_sha256": CANONICAL_PROMPT_SHA256,
            "output_schema_version": MODEL_OUTPUT_SCHEMA_VERSION,
            "output_schema_sha256": CANONICAL_OUTPUT_SCHEMA_SHA256,
            "strict_structured_output": True,
            "fallback_model": None,
        }
        if policy != expected_policy:
            _append(errors, "$.model_policy", "must exactly bind model, prompt, strict schema, and no fallback")
    if request["budgets"] != BUDGETS:
        _append(errors, "$.budgets", "must equal the versioned hard budget")
    if request["authority"] != REQUEST_AUTHORITY:
        _append(errors, "$.authority", "all model and downstream authority must remain false")
    if validate_prompt(prompt):
        _append(errors, "$.model_policy.prompt_sha256", "does not bind a valid pinned prompt")
    if validate_output_schema(output_schema):
        _append(errors, "$.model_policy.output_schema_sha256", "does not bind a valid pinned output schema")
    return errors


def structured_output_schema(schema: Mapping[str, Any]) -> dict[str, Any]:
    """Return only the strict-output JSON Schema keywords sent to Responses."""

    return copy.deepcopy(
        {
            "type": schema["type"],
            "additionalProperties": schema["additionalProperties"],
            "required": schema["required"],
            "properties": schema["properties"],
        }
    )


def build_responses_request(request: Any, *, prompt: Any, output_schema: Any) -> dict[str, Any]:
    errors = validate_request(request, prompt=prompt, output_schema=output_schema)
    if errors:
        raise ValueError("request_invalid")
    profile = request["profile_snapshot"]
    source_payload = {
        "task": "profile_bio_semantic_review",
        "request_id": request["request_id"],
        "profile_snapshot_id": profile["snapshot_id"],
        "platform_user_id": profile["platform_user_id"],
        "bio_text": profile["bio_text"],
        "bio_sha256": profile["bio_sha256"],
        "proposal_limit": request["budgets"]["max_proposals"],
        "authority": request["authority"],
    }
    return {
        "model": MODEL_ID,
        "reasoning": {"effort": request["model_policy"]["reasoning_effort"]},
        "instructions": prompt["developer_instructions"],
        "input": [
            {
                "role": "user",
                "content": [{"type": "input_text", "text": canonical_json(source_payload)}],
            }
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "x_profile_bio_semantic_model_output_v2",
                "strict": True,
                "schema": structured_output_schema(output_schema),
            }
        },
        "tools": [],
        "max_output_tokens": request["budgets"]["max_output_tokens"],
        "truncation": "disabled",
        "store": False,
        "metadata": {
            "request_id": request["request_id"],
            "profile_snapshot_id": profile["snapshot_id"],
            "prompt_sha256": CANONICAL_PROMPT_SHA256,
            "output_schema_sha256": CANONICAL_OUTPUT_SCHEMA_SHA256,
        },
    }


def _reason_is_source_bound(reason: Any, excerpt: str) -> bool:
    if not _is_utf8_scalar_text(reason, minimum=1, maximum=240) or not reason.strip():
        return False
    folded = reason.casefold()
    if _URL_RE.search(reason) or any(term in folded for term in _PROHIBITED_REASON_TERMS):
        return False
    if any(marker in folded for marker in _EXTERNAL_FACT_MARKERS):
        return False
    excerpt_folded = excerpt.casefold()
    if any(mention.casefold() not in excerpt_folded for mention in _HANDLE_MENTION_RE.findall(reason)):
        return False
    # Natural-language explanations are advisory model prose, not an evidence
    # source. Do not turn an English/CJK vocabulary list into the primary
    # semantic classifier. Stable handles and quantitative facts are the two
    # machine-checkable fact classes that must still be copied from the cited
    # Bio span; the closed reason code plus exact excerpt own the judgment.
    if any(number.casefold() not in excerpt_folded for number in _NUMBER_TOKEN_RE.findall(reason)):
        return False
    return True


def validate_model_output(model_output: Any, *, request: Any) -> list[str]:
    errors: list[str] = []
    budgets = request.get("budgets", BUDGETS) if isinstance(request, dict) else BUDGETS
    max_depth = budgets.get("max_validation_depth", 64) if isinstance(budgets, dict) else 64
    max_nodes = budgets.get("max_validation_nodes", 4096) if isinstance(budgets, dict) else 4096
    if not _is_int(max_depth) or not _is_int(max_nodes) or max_depth <= 0 or max_nodes <= 0:
        max_depth, max_nodes = 64, 4096
    traversal = _scan_json(model_output, max_depth=max_depth, max_nodes=max_nodes)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    if not _exact_keys(model_output, _MODEL_OUTPUT_KEYS, path="$", errors=errors):
        return errors
    profile = request.get("profile_snapshot", {}) if isinstance(request, dict) else {}
    expected_bindings = {
        "schema_version": MODEL_OUTPUT_SCHEMA_VERSION,
        "request_id": request.get("request_id") if isinstance(request, dict) else None,
        "profile_snapshot_id": profile.get("snapshot_id") if isinstance(profile, dict) else None,
        "platform_user_id": profile.get("platform_user_id") if isinstance(profile, dict) else None,
        "bio_sha256": profile.get("bio_sha256") if isinstance(profile, dict) else None,
    }
    for binding_field, expected in expected_bindings.items():
        if model_output[binding_field] != expected:
            _append(errors, f"$.{binding_field}", "does not bind the reviewed request/profile snapshot")
    verdict = model_output["verdict"]
    if verdict not in VERDICTS:
        _append(errors, "$.verdict", "unsupported")
    proposals = model_output["proposals"]
    max_proposals = budgets.get("max_proposals", 0) if isinstance(budgets, dict) else 0
    if not isinstance(proposals, list) or len(proposals) > max_proposals:
        _append(errors, "$.proposals", "must be a bounded array")
        proposals = []
    if verdict == "proposals_available" and not proposals:
        _append(errors, "$.verdict", "proposals_available requires at least one proposal")
    if verdict in {"no_supported_professional_context", "abstained"} and proposals:
        _append(errors, "$.verdict", "empty-result verdict cannot contain proposals")

    bio_text = profile.get("bio_text", "") if isinstance(profile, dict) else ""
    signatures: set[str] = set()
    for index, proposal in enumerate(proposals):
        path = f"$.proposals[{index}]"
        if not _exact_keys(proposal, _MODEL_PROPOSAL_KEYS, path=path, errors=errors):
            continue
        proposal_type = proposal["proposal_type"]
        relation = proposal["relation_state"]
        if proposal_type not in PROPOSAL_TYPES:
            _append(errors, f"{path}.proposal_type", "unsupported")
        if relation not in RELATION_STATES:
            _append(errors, f"{path}.relation_state", "unsupported")
        if proposal_type == "professional_affiliation" and relation == "not_applicable":
            _append(errors, f"{path}.relation_state", "affiliation requires a reversible relation state")
        if proposal_type != "professional_affiliation" and relation != "not_applicable":
            _append(errors, f"{path}.relation_state", "non-affiliation context must be not_applicable")
        start = proposal["span_start"]
        end = proposal["span_end"]
        excerpt = proposal["excerpt"]
        if not _is_int(start) or not _is_int(end) or start < 0 or end <= start or end > len(bio_text):
            _append(errors, f"{path}.span", "invalid")
        elif not isinstance(excerpt, str) or bio_text[start:end] != excerpt:
            _append(errors, f"{path}.excerpt", "must exactly equal the Unicode Bio span")
        if not _is_utf8_scalar_text(excerpt, minimum=1, maximum=500) or not excerpt.strip():
            _append(errors, f"{path}.excerpt", "invalid")
            excerpt = ""
        reason_codes = proposal["reason_codes"]
        if (
            not isinstance(reason_codes, list)
            or not 1 <= len(reason_codes) <= 2
            or any(not isinstance(code, str) or code not in REASON_CODES for code in reason_codes)
            or len(set(reason_codes)) != len(reason_codes)
        ):
            _append(errors, f"{path}.reason_codes", "must be a unique closed reason-code array")
            reason_codes = []
        allowed_codes = REASON_CODES_BY_TYPE.get(str(proposal_type), frozenset())
        if any(code not in allowed_codes for code in reason_codes):
            _append(errors, f"{path}.reason_codes", "does not match proposal_type")
        relation_reason = {
            "current_claimed": "explicit_current_organization_claim",
            "previous_claimed": "explicit_previous_organization_claim",
            "future_or_aspirational": "ambiguous_professional_context_requires_review",
            "unspecified": "ambiguous_professional_context_requires_review",
        }.get(str(relation))
        if relation_reason is not None and relation_reason not in reason_codes:
            _append(errors, f"{path}.reason_codes", "does not match relation_state")
        if not _is_utf8_scalar_text(proposal["reason"], minimum=1, maximum=240) or not _reason_is_source_bound(
            proposal["reason"], excerpt
        ):
            _append(errors, f"{path}.reason", "must be bounded, model-proposed, and supported only by the excerpt")
        if proposal["reason_source"] != "model_proposed":
            _append(errors, f"{path}.reason_source", "must remain model_proposed")
        if proposal["confidence"] not in CONFIDENCE_STATES:
            _append(errors, f"{path}.confidence", "unsupported")
        if proposal["evidence_basis"] != "profile_bio_only":
            _append(errors, f"{path}.evidence_basis", "must remain profile_bio_only")
        if proposal["requires_independent_verification"] is not True:
            _append(errors, f"{path}.requires_independent_verification", "must be true")
        try:
            signature = canonical_json(proposal)
        except (TypeError, ValueError, RecursionError):
            _append(errors, path, "must be canonical JSON")
        else:
            if signature in signatures:
                _append(errors, path, "duplicate proposal")
            signatures.add(signature)
    if model_output["authority"] != MODEL_AUTHORITY:
        _append(errors, "$.authority", "all authority and external-fact claims must remain false")
    return errors


def _bound_request_identity(request: Any) -> tuple[str, str, str, str, str] | None:
    """Return exact request bindings or no artifact identity at all."""

    if not isinstance(request, dict) or _scan_json(request, max_depth=64, max_nodes=4096):
        return None
    request_id = request.get("request_id")
    profile = request.get("profile_snapshot")
    if not isinstance(profile, dict):
        return None
    snapshot_id = profile.get("snapshot_id")
    platform_user_id = profile.get("platform_user_id")
    bio_sha256 = profile.get("bio_sha256")
    if (
        not isinstance(request_id, str)
        or re.fullmatch(r"xbsv2r_[0-9a-f]{24}", request_id) is None
        or not isinstance(snapshot_id, str)
        or re.fullmatch(r"xbsv2s_[0-9a-f]{24}", snapshot_id) is None
        or not isinstance(platform_user_id, str)
        or _PLATFORM_USER_ID_RE.fullmatch(platform_user_id) is None
        or not isinstance(bio_sha256, str)
        or _SHA256_RE.fullmatch(bio_sha256) is None
    ):
        return None
    try:
        request_sha256 = canonical_sha256(request)
    except (TypeError, ValueError, RecursionError):
        return None
    return request_id, request_sha256, snapshot_id, platform_user_id, bio_sha256


def _terminal_result(
    request: Any,
    *,
    status: str,
    error_code: str,
    is_live: bool,
    transport_id: str,
    execute_live: bool,
    provider_external_calls: int = 0,
) -> dict[str, Any]:
    bindings = _bound_request_identity(request)
    if bindings is None:
        raise ValueError("request_binding_invalid")
    request_id, request_sha256, snapshot_id, platform_user_id, bio_sha256 = bindings
    normalized_transport = (
        transport_id
        if transport_id in {"offline_fake_responses", "openai_compatible_responses"}
        else "openai_compatible_responses"
        if is_live
        else "offline_fake_responses"
    )
    return {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "status": status,
        "request_id": request_id,
        "request_sha256": request_sha256,
        "profile_snapshot_id": snapshot_id,
        "platform_user_id": platform_user_id,
        "bio_sha256": bio_sha256,
        "execution": {
            "mode": "live" if is_live else "offline_fake",
            "transport_id": normalized_transport,
            "execute_live": bool(execute_live),
            "provider_external_calls": provider_external_calls,
            "fallback_used": False,
        },
        "model_receipt": None,
        "usage": None,
        "verdict": "abstained",
        "proposals": [],
        "error_codes": [error_code],
        "authority": dict(REVIEW_AUTHORITY),
    }


def _response_text_and_usage(response: Any, *, request: Mapping[str, Any]) -> tuple[str, dict[str, int], str]:
    budgets = request["budgets"]
    traversal = _scan_json(
        response,
        max_depth=budgets["max_validation_depth"],
        max_nodes=budgets["max_validation_nodes"],
    )
    if traversal:
        raise _ReviewError("budget_exceeded")
    if not isinstance(response, dict):
        raise _ReviewError("response_invalid")
    try:
        raw_response_sha256 = canonical_sha256(response)
    except (TypeError, ValueError, RecursionError) as exc:
        raise _ReviewError("response_invalid") from exc
    if (
        response.get("object") != "response"
        or response.get("status") != "completed"
        or response.get("model") != MODEL_ID
        or not isinstance(response.get("id"), str)
        or _RESPONSE_ID_RE.fullmatch(response["id"]) is None
    ):
        raise _ReviewError("response_invalid")
    output = response.get("output")
    if not isinstance(output, list) or not 1 <= len(output) <= 2:
        raise _ReviewError("response_invalid")
    messages: list[dict[str, Any]] = []
    reasoning_items = 0
    for item in output:
        if not isinstance(item, dict):
            raise _ReviewError("response_invalid")
        if item.get("type") == "message":
            messages.append(item)
            continue
        if item.get("type") == "reasoning":
            reasoning_items += 1
            if (
                reasoning_items > 1
                or not isinstance(item.get("id"), str)
                or _REASONING_ID_RE.fullmatch(item["id"]) is None
                or item.get("status") not in {None, "completed"}
                or not isinstance(item.get("summary"), list)
            ):
                raise _ReviewError("response_invalid")
            continue
        # Tools are disabled in the request, so every tool/call/unknown output
        # item is a contract violation even when a message is also present.
        raise _ReviewError("response_invalid")
    if len(messages) != 1:
        raise _ReviewError("response_invalid")
    message = messages[0]
    if message.get("type") != "message" or message.get("role") != "assistant":
        raise _ReviewError("response_invalid")
    content = message.get("content")
    if not isinstance(content, list) or len(content) != 1 or not isinstance(content[0], dict):
        raise _ReviewError("response_invalid")
    output_text = content[0]
    text = output_text.get("text")
    if output_text.get("type") != "output_text" or not isinstance(text, str):
        raise _ReviewError("response_invalid")
    if not isinstance(text, str) or not _is_utf8_scalar_text(text, maximum=len(text)):
        raise _ReviewError("model_output_invalid")
    if len(text) > budgets["max_model_output_characters"]:
        raise _ReviewError("budget_exceeded")
    usage = response.get("usage")
    if not isinstance(usage, dict):
        raise _ReviewError("response_invalid")
    values = {field: usage.get(field) for field in ("input_tokens", "output_tokens", "total_tokens")}
    if any(not _is_int(value) or value < 0 for value in values.values()):
        raise _ReviewError("response_invalid")
    if values["total_tokens"] != values["input_tokens"] + values["output_tokens"]:
        raise _ReviewError("response_invalid")
    if (
        values["input_tokens"] > budgets["max_input_tokens"]
        or values["output_tokens"] > budgets["max_output_tokens"]
        or values["total_tokens"] > budgets["max_total_tokens"]
    ):
        raise _ReviewError("budget_exceeded")
    return text, values, raw_response_sha256


def _completed_result(
    request: Mapping[str, Any],
    *,
    prompt: Mapping[str, Any],
    output_schema: Mapping[str, Any],
    response: Any,
    is_live: bool,
    transport_id: str,
    execute_live: bool,
) -> dict[str, Any]:
    text, usage_values, raw_response_sha256 = _response_text_and_usage(response, request=request)
    try:
        model_output = _strict_json_loads(text)
    except (ValueError, RecursionError) as exc:
        raise _ReviewError("model_output_invalid") from exc
    errors = validate_model_output(model_output, request=request)
    if errors:
        raise _ReviewError("model_output_invalid")
    proposals = []
    for proposal in model_output["proposals"]:
        proposal_id = "xbsv2p_" + canonical_sha256({"request_id": request["request_id"], "proposal": proposal})[:24]
        proposals.append(
            {
                "proposal_id": proposal_id,
                "proposal_type": proposal["proposal_type"],
                "relation_state": proposal["relation_state"],
                "span_start": proposal["span_start"],
                "span_end": proposal["span_end"],
                "excerpt": proposal["excerpt"],
                "excerpt_sha256": text_sha256(proposal["excerpt"]),
                "reason_codes": list(proposal["reason_codes"]),
                "reason": proposal["reason"],
                "reason_source": "model_proposed",
                "confidence": proposal["confidence"],
                "evidence_basis": "profile_bio_only",
                "requires_independent_verification": True,
                "verification_status": "unverified_professional_context_proposal",
            }
        )
    proposals.sort(
        key=lambda proposal: (
            proposal["span_start"],
            proposal["span_end"],
            proposal["proposal_type"],
            proposal["relation_state"],
        )
    )
    profile = request["profile_snapshot"]
    return {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "status": "completed",
        "request_id": request["request_id"],
        "request_sha256": canonical_sha256(request),
        "profile_snapshot_id": profile["snapshot_id"],
        "platform_user_id": profile["platform_user_id"],
        "bio_sha256": profile["bio_sha256"],
        "execution": {
            "mode": "live" if is_live else "offline_fake",
            "transport_id": transport_id,
            "execute_live": execute_live,
            "provider_external_calls": 1 if is_live else 0,
            "fallback_used": False,
        },
        "model_receipt": {
            "response_id": response["id"],
            "response_status": "completed",
            "model_id": MODEL_ID,
            "prompt_version": PROMPT_VERSION,
            "prompt_sha256": canonical_sha256(prompt),
            "output_schema_version": MODEL_OUTPUT_SCHEMA_VERSION,
            "output_schema_sha256": canonical_sha256(output_schema),
            "raw_response_sha256": raw_response_sha256,
            "model_output_sha256": canonical_sha256(model_output),
        },
        "usage": {
            **usage_values,
            "source": "provider_reported" if is_live else "offline_fake",
            "billable": is_live,
        },
        "verdict": model_output["verdict"],
        "proposals": proposals,
        "error_codes": [],
        "authority": dict(REVIEW_AUTHORITY),
    }


def run_semantic_review(
    request: Any,
    *,
    prompt: Any,
    output_schema: Any,
    transport: ResponsesTransport,
    execute_live: bool = False,
) -> dict[str, Any]:
    """Run one bounded review or return a closed terminal failure envelope."""

    is_live = getattr(transport, "is_live", None)
    transport_id = getattr(transport, "transport_id", "")
    live_flag = is_live is True
    if validate_prompt(prompt):
        return _terminal_result(
            request,
            status="failed",
            error_code="prompt_invalid",
            is_live=live_flag,
            transport_id=str(transport_id),
            execute_live=execute_live,
        )
    if validate_output_schema(output_schema):
        return _terminal_result(
            request,
            status="failed",
            error_code="output_schema_invalid",
            is_live=live_flag,
            transport_id=str(transport_id),
            execute_live=execute_live,
        )
    if validate_request(request, prompt=prompt, output_schema=output_schema):
        return _terminal_result(
            request,
            status="failed",
            error_code="request_invalid",
            is_live=live_flag,
            transport_id=str(transport_id),
            execute_live=execute_live,
        )
    if type(is_live) is not bool or transport_id not in {
        "offline_fake_responses",
        "openai_compatible_responses",
    }:
        return _terminal_result(
            request,
            status="failed",
            error_code="transport_failed",
            is_live=live_flag,
            transport_id=str(transport_id),
            execute_live=execute_live,
        )
    if is_live and not execute_live:
        return _terminal_result(
            request,
            status="blocked",
            error_code="execute_live_required",
            is_live=True,
            transport_id=transport_id,
            execute_live=False,
        )
    expected_execution_mode = "live_canary" if is_live else "offline_fake"
    if request["model_execution_mode"] != expected_execution_mode:
        return _terminal_result(
            request,
            status="failed",
            error_code="request_mode_mismatch",
            is_live=is_live,
            transport_id=transport_id,
            execute_live=execute_live,
        )
    payload = build_responses_request(request, prompt=prompt, output_schema=output_schema)
    try:
        response = transport.create_response(payload, timeout_ms=request["budgets"]["timeout_ms"])
    except Exception:  # noqa: BLE001 - transport detail must not escape or echo credentials/provider content
        return _terminal_result(
            request,
            status="failed",
            error_code="transport_failed",
            is_live=is_live,
            transport_id=transport_id,
            execute_live=execute_live,
            provider_external_calls=1 if is_live else 0,
        )
    try:
        return _completed_result(
            request,
            prompt=prompt,
            output_schema=output_schema,
            response=response,
            is_live=is_live,
            transport_id=transport_id,
            execute_live=execute_live,
        )
    except _ReviewError as exc:
        return _terminal_result(
            request,
            status="failed",
            error_code=exc.code,
            is_live=is_live,
            transport_id=transport_id,
            execute_live=execute_live,
            provider_external_calls=1 if is_live else 0,
        )


def validate_review(
    review: Any,
    *,
    request: Any,
    prompt: Any,
    output_schema: Any,
    raw_response: Any,
) -> list[str]:
    """Recompute a completed review from the source request and raw response."""

    traversal = _scan_json(review, max_depth=64, max_nodes=4096)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    request_errors = validate_request(request, prompt=prompt, output_schema=output_schema)
    if request_errors:
        return [f"request {error}" for error in request_errors]
    if not isinstance(review, dict) or review.get("status") != "completed":
        return ["$: only completed reviews can be deterministically recomputed"]
    profile = request["profile_snapshot"]
    if (
        review.get("request_id") != request["request_id"]
        or review.get("request_sha256") != canonical_sha256(request)
        or review.get("profile_snapshot_id") != profile["snapshot_id"]
        or review.get("platform_user_id") != profile["platform_user_id"]
        or review.get("bio_sha256") != profile["bio_sha256"]
    ):
        return ["$: review identity/source binding does not exactly match request"]
    execution = review.get("execution")
    if not isinstance(execution, dict):
        return ["$.execution: must be an object"]
    is_live = execution.get("mode") == "live"
    expected_transport = "openai_compatible_responses" if is_live else "offline_fake_responses"
    if (
        request["model_execution_mode"] != ("live_canary" if is_live else "offline_fake")
        or request["profile_source_mode"] != "offline_fixture"
        or execution.get("transport_id") != expected_transport
        or execution.get("execute_live") is not is_live
        or execution.get("provider_external_calls") != (1 if is_live else 0)
        or execution.get("fallback_used") is not False
    ):
        return ["$.execution: invalid completed execution binding"]
    try:
        expected = _completed_result(
            request,
            prompt=prompt,
            output_schema=output_schema,
            response=raw_response,
            is_live=is_live,
            transport_id=expected_transport,
            execute_live=is_live,
        )
    except _ReviewError as exc:
        return [f"raw_response: {exc.code}"]
    try:
        matches = review == expected
    except RecursionError:
        return ["$.validation: nested_depth_budget_exceeded"]
    return [] if matches else ["$: review does not equal deterministic source-bound recomputation"]
