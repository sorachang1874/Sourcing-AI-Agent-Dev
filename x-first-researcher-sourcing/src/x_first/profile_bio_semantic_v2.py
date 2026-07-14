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
import types
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

REQUEST_SCHEMA_VERSION = "x.profile.bio_semantic.request.v2.2"
PROMPT_SCHEMA_VERSION = "x.profile.bio_semantic.prompt.v2.2"
PROMPT_VERSION = "profile-bio-semantic-prompt-v2.2"
MODEL_OUTPUT_SCHEMA_VERSION = "x.profile.bio_semantic.model_output.v2.2"
REVIEW_SCHEMA_VERSION = "x.profile.bio_semantic.review.v2.2"
MODEL_ID = "gpt-5.6-luna"
PROVIDER_ID = "openai_compatible_responses"
REASON_SOURCE = "closed_deterministic_explanation_template_v2.2"

# Replaced after the v2.2 prompt and strict-output schema are finalized.
CANONICAL_PROMPT_SHA256 = "7504507db5945e08b91848fa1e01ccc7a1d7bd8f48f329e7057ca9b492b9d48f"
CANONICAL_OUTPUT_SCHEMA_SHA256 = "37436cf27cb9b6dcbc74aeaad8fa915cf9da75d3738d2e78a164a90fec0c28f6"
PURE_ADJUDICATION_API_VERSION = "x.profile.bio_semantic.pure_adjudication.v1"
PURE_ADJUDICATION_IMPLEMENTATION_REVISION = "transitive-bytecode-dependency-bundle-v2"
PURE_ADJUDICATION_IMPLEMENTATION_SHA256 = "5cef30928609e0c6a17e06bf309ac45413e3c2475b07a9b5a0bb616f1c55fd45"
PROXY_POLICY_SCHEMA_VERSION = "x.profile.bio.professional_experience_proxy_policy.v1"
PROXY_POLICY_VERSION = "profile-bio-professional-experience-proxy-policy-v1"
CANONICAL_PROXY_POLICY_SHA256 = "a2c115e2505ed08ca5930028a8458218cbc3e0e074bd33ef96f1a51db2609e72"
PROXY_POLICY_PATH = (
    Path(__file__).resolve().parents[2] / "configs/profile_bio_professional_experience_proxy_policy.v1.json"
)
PROXY_POLICY_BINDING = {
    "policy_version": PROXY_POLICY_VERSION,
    "policy_sha256": CANONICAL_PROXY_POLICY_SHA256,
}

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
    "max_json_string_characters": 12000,
    "max_json_string_bytes": 48000,
    "max_raw_response_canonical_bytes": 262144,
    "max_request_canonical_bytes": 65536,
    "max_reasoning_summary_items": 4,
    "max_reasoning_summary_characters": 2000,
    "max_reasoning_summary_bytes": 8000,
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
ERROR_CODES = frozenset(
    {
        "request_invalid",
        "prompt_invalid",
        "output_schema_invalid",
        "proxy_policy_invalid",
        "execute_live_required",
        "request_mode_mismatch",
        "execution_contract_invalid",
        "transport_failed",
        "response_invalid",
        "model_output_invalid",
        "budget_exceeded",
    }
)
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
REASON_CODES_BY_SEMANTIC_STATE = {
    ("professional_region_experience", "not_applicable"): frozenset({"explicit_professional_region_experience"}),
    ("professional_china_digital_ecosystem", "not_applicable"): frozenset(
        {"explicit_china_digital_ecosystem_activity"}
    ),
    ("professional_affiliation", "current_claimed"): frozenset({"explicit_current_organization_claim"}),
    ("professional_affiliation", "previous_claimed"): frozenset({"explicit_previous_organization_claim"}),
    ("professional_affiliation", "future_or_aspirational"): frozenset(
        {"ambiguous_professional_context_requires_review"}
    ),
    ("professional_affiliation", "unspecified"): frozenset({"ambiguous_professional_context_requires_review"}),
    ("observed_chinese_professional_content", "not_applicable"): frozenset({"observed_chinese_professional_content"}),
}
EXPLANATION_TEMPLATES_BY_SEMANTIC_STATE = {
    (
        "professional_region_experience",
        "not_applicable",
    ): "The cited Bio span explicitly states professional region experience.",
    (
        "professional_china_digital_ecosystem",
        "not_applicable",
    ): "The cited Bio span explicitly states China digital-ecosystem professional activity.",
    (
        "professional_affiliation",
        "current_claimed",
    ): "The cited Bio span explicitly states a current claimed organization affiliation.",
    (
        "professional_affiliation",
        "previous_claimed",
    ): "The cited Bio span explicitly states a previous claimed organization affiliation.",
    (
        "professional_affiliation",
        "future_or_aspirational",
    ): "The cited Bio span states a future or aspirational organization affiliation.",
    (
        "professional_affiliation",
        "unspecified",
    ): "The cited Bio span states an organization affiliation with unspecified timing.",
    (
        "observed_chinese_professional_content",
        "not_applicable",
    ): "The cited Bio span contains observed Chinese-language professional content.",
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
    "professional_experience_proxy_policy_version",
    "professional_experience_proxy_policy_sha256",
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
_REVIEW_KEYS = {
    "schema_version",
    "status",
    "request_id",
    "request_sha256",
    "profile_snapshot_id",
    "platform_user_id",
    "bio_sha256",
    "execution",
    "model_receipt",
    "usage",
    "verdict",
    "proposals",
    "professional_experience_proxy_policy",
    "professional_experience_proxy_rollup",
    "error_codes",
    "authority",
}
_EXECUTION_KEYS = {
    "mode",
    "transport_id",
    "execute_live",
    "transport_invocations",
    "provider_external_calls",
    "response_received",
    "fallback_used",
    "phase",
    "contract_error",
}
_MESSAGE_REQUIRED_KEYS = {"id", "type", "status", "role", "content"}
_MESSAGE_OPTIONAL_KEYS = {"phase"}
_OUTPUT_TEXT_REQUIRED_KEYS = {"type", "text", "annotations"}
_OUTPUT_TEXT_OPTIONAL_KEYS = {"logprobs"}
_REASONING_REQUIRED_KEYS = {"id", "type"}
_REASONING_OPTIONAL_KEYS = {"status", "summary"}
_REASONING_SUMMARY_KEYS = {"type", "text"}

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_PLATFORM_USER_ID_RE = re.compile(r"[1-9][0-9]{1,24}")
_HANDLE_RE = re.compile(r"[a-z0-9_]{1,15}")
_PROVISIONAL_PERSON_RE = re.compile(r"pp_x_[0-9A-HJKMNP-TV-Z]{26}")
_RESPONSE_ID_RE = re.compile(r"resp_[A-Za-z0-9_-]{8,120}")
_MESSAGE_ID_RE = re.compile(r"msg_[A-Za-z0-9_-]{8,120}")
_REASONING_ID_RE = re.compile(r"rs_[A-Za-z0-9_-]{8,120}")
_CANONICAL_TIME_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")
_EXECUTION_MODES = frozenset({"offline_fake", "live", "invalid", "not_inspected"})
_TRANSPORT_IDS = frozenset({"offline_fake_responses", "openai_compatible_responses", "invalid", "not_inspected"})
_EXECUTION_PHASES = frozenset({"preflight", "authorization", "transport", "response", "semantic", "completed"})
_EXECUTION_CONTRACT_ERRORS = frozenset(
    {
        "none",
        "not_evaluated",
        "invalid_is_live",
        "invalid_transport_id",
        "transport_pair_mismatch",
        "invalid_execute_live",
        "offline_execute_live",
    }
)
_MISSING = object()
_INVALID_SNAPSHOT = object()


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


def _resolve_proxy_policy(proxy_policy: Any) -> Any:
    if proxy_policy is not None:
        return proxy_policy
    try:
        return load_json(PROXY_POLICY_PATH)
    except (OSError, ValueError, RecursionError):
        return None


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


def _canonical_snapshot(value: Any) -> Any:
    """Detach validated caller-owned JSON before crossing the transport seam."""

    return _strict_json_loads(canonical_json(value))


def _try_canonical_snapshot(value: Any) -> tuple[Any, bool]:
    try:
        return _canonical_snapshot(value), True
    except Exception:  # noqa: BLE001 - caller/transport-owned JSON wrappers are untrusted
        return _INVALID_SNAPSHOT, False


def json_type_strict_equal(left: Any, right: Any) -> bool:
    """Compare JSON values without Python's bool/int or int/float coercions."""

    if isinstance(left, dict) or isinstance(right, dict):
        if not isinstance(left, dict) or not isinstance(right, dict):
            return False
        if set(left) != set(right):
            return False
        return all(json_type_strict_equal(left[key], right[key]) for key in left)
    if isinstance(left, list) or isinstance(right, list):
        if not isinstance(left, list) or not isinstance(right, list):
            return False
        return len(left) == len(right) and all(
            json_type_strict_equal(left_item, right_item) for left_item, right_item in zip(left, right, strict=True)
        )
    if type(left) is not type(right):
        return False
    if left is None:
        return True
    if type(left) in {str, int, float, bool}:
        return bool(left == right)
    return False


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


_PURE_ADJUDICATION_INTEGRITY_SYMBOLS = frozenset(
    {
        "pure_adjudication_implementation_manifest",
        "validate_pure_adjudication_implementation",
        "_pure_adjudication_implementation_bundle",
        "_pure_bundle_dependency_value",
        "_pure_bundle_code_object",
        "_pure_bundle_function",
    }
)


def _pure_bundle_dependency_value(value: Any) -> Any:
    """Project executable dependencies into stable, type-explicit JSON."""

    if value is None:
        return {"kind": "none"}
    if type(value) is bool:
        return {"kind": "bool", "value": value}
    if type(value) is int:
        return {"kind": "int", "value": value}
    if type(value) is float:
        return {"kind": "float", "value": value.hex()}
    if isinstance(value, str):
        return {"kind": "str", "value": value}
    if isinstance(value, bytes):
        return {"kind": "bytes", "value_hex": value.hex()}
    if isinstance(value, types.CodeType):
        return {"kind": "code", "value": _pure_bundle_code_object(value)}
    if isinstance(value, tuple):
        return {"kind": "tuple", "items": [_pure_bundle_dependency_value(item) for item in value]}
    if isinstance(value, list):
        return {"kind": "list", "items": [_pure_bundle_dependency_value(item) for item in value]}
    if isinstance(value, (set, frozenset)):
        items = [_pure_bundle_dependency_value(item) for item in value]
        items.sort(key=canonical_json)
        return {"kind": type(value).__name__, "items": items}
    if isinstance(value, dict):
        items = [
            {
                "key": _pure_bundle_dependency_value(key),
                "value": _pure_bundle_dependency_value(item),
            }
            for key, item in value.items()
        ]
        items.sort(key=lambda item: canonical_json(item["key"]))
        return {"kind": "dict", "items": items}
    if isinstance(value, Path):
        return {"kind": "path", "tail": "/".join(value.parts[-2:])}
    if isinstance(value, re.Pattern):
        return {"kind": "regex", "pattern": value.pattern, "flags": value.flags}
    if isinstance(value, types.ModuleType):
        return {"kind": "module", "name": value.__name__}
    if isinstance(value, types.FunctionType) or isinstance(value, type) or callable(value):
        return {
            "kind": "callable",
            "module": getattr(value, "__module__", type(value).__module__),
            "qualname": getattr(value, "__qualname__", type(value).__qualname__),
        }
    return {
        "kind": "opaque_sentinel",
        "type_module": type(value).__module__,
        "type_qualname": type(value).__qualname__,
    }


def _pure_bundle_code_object(code: types.CodeType) -> dict[str, Any]:
    return {
        "argcount": code.co_argcount,
        "posonlyargcount": code.co_posonlyargcount,
        "kwonlyargcount": code.co_kwonlyargcount,
        "flags": code.co_flags,
        "bytecode_sha256": hashlib.sha256(code.co_code).hexdigest(),
        "constants": [_pure_bundle_dependency_value(value) for value in code.co_consts],
        "names": list(code.co_names),
        "varnames": list(code.co_varnames),
        "freevars": list(code.co_freevars),
        "cellvars": list(code.co_cellvars),
    }


def _pure_bundle_function(function: types.FunctionType) -> dict[str, Any]:
    return {
        "code": _pure_bundle_code_object(function.__code__),
        "defaults": _pure_bundle_dependency_value(function.__defaults__),
        "kwdefaults": _pure_bundle_dependency_value(function.__kwdefaults__),
    }


def _pure_adjudication_implementation_bundle() -> dict[str, Any]:
    """Bind the actual transitive implementation reachable from the pure API.

    Integrity helpers are deliberately outside the closure to avoid hashing the
    expected digest back into itself. Every other module-local function or class
    referenced by executable bytecode is followed automatically; non-local
    callables and all referenced data globals are recorded as dependencies.
    """

    namespace = globals()
    pending: list[tuple[str, Any]] = [("adjudicate_observed_response", namespace["adjudicate_observed_response"])]
    controlled: dict[str, str] = {}
    dependencies: dict[str, Any] = {}
    visited: set[str] = set()

    while pending:
        symbol_name, symbol = pending.pop()
        if symbol_name in visited or symbol_name in _PURE_ADJUDICATION_INTEGRITY_SYMBOLS:
            continue
        visited.add(symbol_name)
        code_objects: list[types.CodeType] = []
        if isinstance(symbol, types.FunctionType) and symbol.__module__ == __name__:
            function_manifest = _pure_bundle_function(symbol)
            controlled[symbol_name] = canonical_sha256(function_manifest)
            code_objects.append(symbol.__code__)
        elif isinstance(symbol, type) and symbol.__module__ == __name__:
            method_manifests: dict[str, Any] = {}
            for method_name, method in vars(symbol).items():
                if isinstance(method, types.FunctionType):
                    method_manifests[method_name] = _pure_bundle_function(method)
                    code_objects.append(method.__code__)
            controlled[symbol_name] = canonical_sha256(
                {
                    "class_name": symbol.__qualname__,
                    "bases": [_pure_bundle_dependency_value(base) for base in symbol.__bases__],
                    "methods": method_manifests,
                }
            )
        else:
            dependencies[symbol_name] = _pure_bundle_dependency_value(symbol)
            continue

        for code in code_objects:
            for dependency_name in code.co_names:
                if dependency_name in _PURE_ADJUDICATION_INTEGRITY_SYMBOLS:
                    continue
                dependency = namespace.get(dependency_name, _MISSING)
                if dependency is _MISSING:
                    continue
                if (
                    isinstance(dependency, types.FunctionType)
                    and dependency.__module__ == __name__
                ) or (isinstance(dependency, type) and dependency.__module__ == __name__):
                    pending.append((dependency_name, dependency))
                else:
                    dependencies[dependency_name] = _pure_bundle_dependency_value(dependency)

    return {
        "format": "cpython-transitive-code-and-dependency-manifest-v1",
        "root_symbols": ["adjudicate_observed_response"],
        "integrity_symbols_excluded_from_self_hash": sorted(_PURE_ADJUDICATION_INTEGRITY_SYMBOLS),
        "controlled_symbol_sha256": dict(sorted(controlled.items())),
        "dependency_manifest": dict(sorted(dependencies.items())),
    }


def pure_adjudication_implementation_manifest() -> dict[str, Any]:
    return {
        "api_version": PURE_ADJUDICATION_API_VERSION,
        "implementation_revision": PURE_ADJUDICATION_IMPLEMENTATION_REVISION,
        "request_schema_version": REQUEST_SCHEMA_VERSION,
        "prompt_version": PROMPT_VERSION,
        "prompt_sha256": CANONICAL_PROMPT_SHA256,
        "model_output_schema_version": MODEL_OUTPUT_SCHEMA_VERSION,
        "model_output_schema_sha256": CANONICAL_OUTPUT_SCHEMA_SHA256,
        "review_schema_version": REVIEW_SCHEMA_VERSION,
        "proxy_policy_version": PROXY_POLICY_VERSION,
        "proxy_policy_sha256": CANONICAL_PROXY_POLICY_SHA256,
        "execution_source": "outer_receipt_ledger_or_fixed_offline_replay_projection",
        "assurance_scope": "trusted_runtime_drift_detection_only",
        "independent_integrity_claimed": False,
        "source_snapshot": "canonical_plain_json_before_adjudication",
        "response_snapshot": "canonical_plain_json_before_parsing",
        "json_equality": "type_strict",
        "implementation_bundle": _pure_adjudication_implementation_bundle(),
    }


def validate_pure_adjudication_implementation() -> list[str]:
    """Detect accidental runtime drift inside an otherwise trusted process.

    This comparison is intentionally not a tamper-proof or independent trust
    root.  A caller that needs authority over provider calls, billing, or live
    evidence must validate the module-external Luna receipt/ledger bundle.
    """

    observed = canonical_sha256(pure_adjudication_implementation_manifest())
    if observed != PURE_ADJUDICATION_IMPLEMENTATION_SHA256:
        return ["pure_adjudication_implementation_digest_mismatch"]
    return []


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


def _scan_json(
    value: Any,
    *,
    max_depth: int,
    max_nodes: int,
    max_string_characters: int = BUDGETS["max_json_string_characters"],
    max_string_bytes: int = BUDGETS["max_json_string_bytes"],
) -> list[str]:
    """Bound every JSON node, key, and string before recursion or hashing."""

    errors: set[str] = set()
    stack: list[tuple[Any, int]] = [(value, 0)]
    visited = 0
    while stack:
        node, depth = stack.pop()
        visited += 1
        if visited > max_nodes:
            errors.add("nested_node_budget_exceeded")
            break
        if isinstance(node, str):
            if len(node) > max_string_characters:
                errors.add("string_character_budget_exceeded")
                continue
            if "\x00" in node:
                errors.add("non_unicode_scalar_text")
                continue
            try:
                encoded = node.encode("utf-8", errors="strict")
            except UnicodeEncodeError:
                errors.add("non_unicode_scalar_text")
                continue
            if len(encoded) > max_string_bytes:
                errors.add("string_byte_budget_exceeded")
            continue
        if isinstance(node, dict):
            if node and depth >= max_depth:
                errors.add("nested_depth_budget_exceeded")
                continue
            if len(node) > max_nodes - visited - len(stack):
                errors.add("nested_node_budget_exceeded")
                break
            for key, child in node.items():
                if not isinstance(key, str):
                    errors.add("non_string_json_key")
                elif len(key) > max_string_characters:
                    errors.add("string_character_budget_exceeded")
                elif "\x00" in key:
                    errors.add("non_unicode_scalar_text")
                else:
                    try:
                        encoded_key = key.encode("utf-8", errors="strict")
                    except UnicodeEncodeError:
                        errors.add("non_unicode_scalar_text")
                    else:
                        if len(encoded_key) > max_string_bytes:
                            errors.add("string_byte_budget_exceeded")
                stack.append((child, depth + 1))
            continue
        if isinstance(node, list):
            if node and depth >= max_depth:
                errors.add("nested_depth_budget_exceeded")
                continue
            if len(node) > max_nodes - visited - len(stack):
                errors.add("nested_node_budget_exceeded")
                break
            stack.extend((child, depth + 1) for child in node)
    return sorted(errors)


def _canonical_size_status(value: Any, *, maximum_bytes: int) -> str:
    """Stream canonical encoding and stop before materializing an oversized form."""

    total = 0
    try:
        chunks = json.JSONEncoder(
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).iterencode(value)
        for chunk in chunks:
            total += len(chunk.encode("utf-8"))
            if total > maximum_bytes:
                return "budget"
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError):
        return "invalid"
    return "ok"


def _valid_transport_pair(*, is_live: Any, transport_id: Any) -> bool:
    return (type(is_live) is bool) and (
        (is_live is False and transport_id == "offline_fake_responses")
        or (is_live is True and transport_id == "openai_compatible_responses")
    )


def _execution_not_inspected(execute_live: Any = None) -> dict[str, Any]:
    """Record asset-preflight termination without consulting transport metadata."""

    del execute_live
    return {
        "mode": "not_inspected",
        "transport_id": "not_inspected",
        "execute_live": None,
        "transport_invocations": 0,
        "provider_external_calls": 0,
        "response_received": False,
        "fallback_used": False,
        "phase": "preflight",
        "contract_error": "not_evaluated",
    }


def _execution_attempt(*, is_live: Any, transport_id: Any, execute_live: Any) -> dict[str, Any]:
    """Project arbitrary invocation metadata into a closed, non-echoing record."""

    mode = "live" if is_live is True else "offline_fake" if is_live is False else "invalid"
    normalized_transport_id = (
        transport_id
        if isinstance(transport_id, str) and transport_id in {"offline_fake_responses", "openai_compatible_responses"}
        else "invalid"
    )
    normalized_execute_live = execute_live if type(execute_live) is bool else None
    if type(is_live) is not bool:
        contract_error = "invalid_is_live"
    elif normalized_transport_id == "invalid":
        contract_error = "invalid_transport_id"
    elif not _valid_transport_pair(is_live=is_live, transport_id=normalized_transport_id):
        contract_error = "transport_pair_mismatch"
    elif type(execute_live) is not bool:
        contract_error = "invalid_execute_live"
    elif is_live is False and execute_live is True:
        contract_error = "offline_execute_live"
    else:
        contract_error = "none"
    return {
        "mode": mode,
        "transport_id": normalized_transport_id,
        "execute_live": normalized_execute_live,
        "transport_invocations": 0,
        "provider_external_calls": 0,
        "response_received": False,
        "fallback_used": False,
        "phase": "preflight",
        "contract_error": contract_error,
    }


def _execution_at_phase(attempt: Mapping[str, Any], phase: str) -> dict[str, Any]:
    """Derive call/response evidence from a closed execution phase."""

    after_invocation = phase in {"transport", "response", "semantic", "completed"}
    response_received = phase in {"response", "semantic", "completed"}
    is_live = attempt.get("mode") == "live"
    return {
        **dict(attempt),
        "phase": phase,
        "transport_invocations": 1 if after_invocation else 0,
        "provider_external_calls": 1 if after_invocation and is_live else 0,
        "response_received": response_received,
    }


def _semantic_identity(proposal: Mapping[str, Any]) -> dict[str, Any]:
    """Return the only fields allowed to own a proposal's machine identity."""

    return {
        "proposal_type": proposal.get("proposal_type"),
        "relation_state": proposal.get("relation_state"),
        "span_start": proposal.get("span_start"),
        "span_end": proposal.get("span_end"),
        "excerpt": proposal.get("excerpt"),
        "reason_codes": sorted(proposal.get("reason_codes", [])),
    }


def _no_professional_experience_proxy_rollup() -> dict[str, Any]:
    return {
        "status": "none",
        "regions": {
            "china": {"strength": "none", "contributing_proposals": []},
            "asia": {"strength": "none", "contributing_proposals": []},
        },
    }


def _professional_experience_proxy_rollup(
    proposals: Any,
    *,
    proxy_policy: Mapping[str, Any],
) -> dict[str, Any]:
    """Apply only the SHA-pinned policy mapping to finalized proposals."""

    regions = {
        region: {"strength": proxy_policy["unmapped_strength"], "contributing_proposals": []}
        for region in proxy_policy["regions"]
    }
    strength_rank = {strength: rank for rank, strength in enumerate(proxy_policy["strength_order"])}
    for proposal in proposals if isinstance(proposals, list) else []:
        if not isinstance(proposal, dict):
            continue
        mapping = proxy_policy["proposal_mappings"].get(proposal.get("proposal_type"))
        if not isinstance(mapping, dict):
            continue
        contribution = {
            "proposal_id": proposal["proposal_id"],
            "proposal_type": proposal["proposal_type"],
            "confidence": proposal["confidence"],
            "mapped_strength": mapping["strength"],
        }
        for region in mapping["regions"]:
            region_rollup = regions[region]
            region_rollup["contributing_proposals"].append(dict(contribution))
            if strength_rank[mapping["strength"]] > strength_rank[region_rollup["strength"]]:
                region_rollup["strength"] = mapping["strength"]
    for region_rollup in regions.values():
        region_rollup["contributing_proposals"].sort(key=lambda item: (item["proposal_id"], item["proposal_type"]))
    return {
        "status": proxy_policy["source_status"]
        if any(region["contributing_proposals"] for region in regions.values())
        else "none",
        "regions": regions,
    }


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
    return (
        parsed.scheme == "https"
        and parsed.netloc == "profiles.invalid"
        and parsed.hostname == "profiles.invalid"
        and parsed.username is None
        and parsed.password is None
        and port is None
        and not parsed.query
        and not parsed.fragment
        and parsed.path == f"/x/{handle}"
    )


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
    properties = schema.get("properties")
    version_schema = properties.get("schema_version") if isinstance(properties, dict) else None
    version = version_schema.get("const") if isinstance(version_schema, dict) else None
    if version != MODEL_OUTPUT_SCHEMA_VERSION:
        _append(errors, "$.properties.schema_version.const", "unsupported")
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        _append(errors, "$", "root must be a closed object schema")
    return errors


def validate_proxy_policy(proxy_policy: Any = None) -> list[str]:
    policy = _resolve_proxy_policy(proxy_policy)
    errors: list[str] = []
    traversal = _scan_json(policy, max_depth=64, max_nodes=4096)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    policy_keys = {
        "schema_version",
        "policy_version",
        "source_status",
        "regions",
        "strength_order",
        "unmapped_strength",
        "proposal_mappings",
        "usage_boundary",
    }
    if not _exact_keys(policy, policy_keys, path="$", errors=errors):
        return errors
    try:
        digest = canonical_sha256(policy)
    except (TypeError, ValueError, RecursionError):
        _append(errors, "$", "must be canonical JSON")
    else:
        if digest != CANONICAL_PROXY_POLICY_SHA256:
            _append(errors, "$", "must exactly match the pinned proxy policy")
    if policy["schema_version"] != PROXY_POLICY_SCHEMA_VERSION:
        _append(errors, "$.schema_version", "unsupported")
    if policy["policy_version"] != PROXY_POLICY_VERSION:
        _append(errors, "$.policy_version", "unsupported")
    if policy["source_status"] != "unverified_model_derived":
        _append(errors, "$.source_status", "unsupported")
    if not json_type_strict_equal(policy["regions"], ["china", "asia"]):
        _append(errors, "$.regions", "must preserve the closed region order")
    if not json_type_strict_equal(policy["strength_order"], ["none", "weak_proxy", "strong_proxy"]):
        _append(errors, "$.strength_order", "must preserve the closed strength order")
    if policy["unmapped_strength"] != "none":
        _append(errors, "$.unmapped_strength", "must fail closed")
    mappings = policy["proposal_mappings"]
    if not isinstance(mappings, dict) or not mappings:
        _append(errors, "$.proposal_mappings", "must be a non-empty object")
    else:
        for proposal_type, mapping in mappings.items():
            if proposal_type not in PROPOSAL_TYPES:
                _append(errors, f"$.proposal_mappings.{proposal_type}", "unsupported proposal type")
                continue
            if not _exact_keys(
                mapping,
                {"strength", "regions"},
                path=f"$.proposal_mappings.{proposal_type}",
                errors=errors,
            ):
                continue
            if not isinstance(mapping["strength"], str) or mapping["strength"] not in {
                "weak_proxy",
                "strong_proxy",
            }:
                _append(errors, f"$.proposal_mappings.{proposal_type}.strength", "unsupported")
            mapping_regions = mapping["regions"]
            if (
                not isinstance(mapping_regions, list)
                or not mapping_regions
                or any(not isinstance(region, str) for region in mapping_regions)
                or len(mapping_regions) != len(set(mapping_regions))
                or any(region not in {"china", "asia"} for region in mapping_regions)
            ):
                _append(errors, f"$.proposal_mappings.{proposal_type}.regions", "invalid")
    expected_usage_boundary = {
        "machine_use": "independently_governed_high_recall_verification_queue_only",
        "final_eligibility_or_ranking": False,
        "physical_region_inference": False,
        "protected_identity_inference": False,
        "canonical_employment": False,
    }
    if not json_type_strict_equal(policy["usage_boundary"], expected_usage_boundary):
        _append(errors, "$.usage_boundary", "must preserve the closed downstream authority boundary")
    return errors


def _proxy_policy_provenance(
    proxy_policy_snapshot: Any,
    *,
    snapshot_valid: bool,
    policy_valid: bool,
) -> dict[str, Any]:
    observed_version: str | None = None
    observed_sha256: str | None = None
    if snapshot_valid:
        if isinstance(proxy_policy_snapshot, dict) and _is_utf8_scalar_text(
            proxy_policy_snapshot.get("policy_version"),
            maximum=200,
        ):
            observed_version = proxy_policy_snapshot["policy_version"]
        try:
            observed_sha256 = canonical_sha256(proxy_policy_snapshot)
        except (TypeError, ValueError, UnicodeEncodeError, RecursionError):
            observed_sha256 = None
    return {
        "expected": dict(PROXY_POLICY_BINDING),
        "observed": {
            "policy_version": observed_version,
            "policy_sha256": observed_sha256,
        },
        "validation_status": "validated_canonical" if policy_valid else "invalid",
    }


def validate_request(
    request: Any,
    *,
    prompt: Any,
    output_schema: Any,
    proxy_policy: Any = None,
) -> list[str]:
    errors: list[str] = []
    traversal = _scan_json(request, max_depth=64, max_nodes=4096)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    request_size_status = _canonical_size_status(
        request,
        maximum_bytes=BUDGETS["max_request_canonical_bytes"],
    )
    if request_size_status == "budget":
        return ["$.validation: request_canonical_byte_budget_exceeded"]
    if request_size_status != "ok":
        return ["$: must be canonical JSON"]
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
    if not isinstance(request["model_execution_mode"], str) or request["model_execution_mode"] not in {
        "offline_fake",
        "live_canary",
    }:
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
            "professional_experience_proxy_policy_version": PROXY_POLICY_VERSION,
            "professional_experience_proxy_policy_sha256": CANONICAL_PROXY_POLICY_SHA256,
            "strict_structured_output": True,
            "fallback_model": None,
        }
        if not json_type_strict_equal(policy, expected_policy):
            _append(errors, "$.model_policy", "must exactly bind model, prompt, strict schema, and no fallback")
    if not json_type_strict_equal(request["budgets"], BUDGETS):
        _append(errors, "$.budgets", "must equal the versioned hard budget")
    if not json_type_strict_equal(request["authority"], REQUEST_AUTHORITY):
        _append(errors, "$.authority", "all model and downstream authority must remain false")
    if validate_prompt(prompt):
        _append(errors, "$.model_policy.prompt_sha256", "does not bind a valid pinned prompt")
    if validate_output_schema(output_schema):
        _append(errors, "$.model_policy.output_schema_sha256", "does not bind a valid pinned output schema")
    if validate_proxy_policy(proxy_policy):
        _append(
            errors,
            "$.model_policy.professional_experience_proxy_policy_sha256",
            "does not bind a valid pinned professional-experience proxy policy",
        )
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


def build_responses_request(
    request: Any,
    *,
    prompt: Any,
    output_schema: Any,
    proxy_policy: Any = None,
) -> dict[str, Any]:
    errors = validate_request(
        request,
        prompt=prompt,
        output_schema=output_schema,
        proxy_policy=proxy_policy,
    )
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
            "professional_experience_proxy_policy_sha256": CANONICAL_PROXY_POLICY_SHA256,
        },
    }


def _expected_explanation(proposal_type: Any, relation: Any) -> str | None:
    if not isinstance(proposal_type, str) or not isinstance(relation, str):
        return None
    return EXPLANATION_TEMPLATES_BY_SEMANTIC_STATE.get((proposal_type, relation))


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
        if not json_type_strict_equal(model_output[binding_field], expected):
            _append(errors, f"$.{binding_field}", "does not bind the reviewed request/profile snapshot")
    verdict = model_output["verdict"]
    if not isinstance(verdict, str) or verdict not in VERDICTS:
        _append(errors, "$.verdict", "unsupported")
    proposals = model_output["proposals"]
    max_proposals = budgets.get("max_proposals", 0) if isinstance(budgets, dict) else 0
    if not isinstance(proposals, list) or len(proposals) > max_proposals:
        _append(errors, "$.proposals", "must be a bounded array")
        proposals = []
    if verdict == "proposals_available" and not proposals:
        _append(errors, "$.verdict", "proposals_available requires at least one proposal")
    if isinstance(verdict, str) and verdict in {"no_supported_professional_context", "abstained"} and proposals:
        _append(errors, "$.verdict", "empty-result verdict cannot contain proposals")

    bio_text = profile.get("bio_text", "") if isinstance(profile, dict) else ""
    signatures: set[str] = set()
    for index, proposal in enumerate(proposals):
        path = f"$.proposals[{index}]"
        if not _exact_keys(proposal, _MODEL_PROPOSAL_KEYS, path=path, errors=errors):
            continue
        proposal_type = proposal["proposal_type"]
        relation = proposal["relation_state"]
        if not isinstance(proposal_type, str) or proposal_type not in PROPOSAL_TYPES:
            _append(errors, f"{path}.proposal_type", "unsupported")
        if not isinstance(relation, str) or relation not in RELATION_STATES:
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
            or len(reason_codes) != 1
            or any(not isinstance(code, str) or code not in REASON_CODES for code in reason_codes)
            or len(set(reason_codes)) != len(reason_codes)
        ):
            _append(errors, f"{path}.reason_codes", "must contain exactly one closed reason code")
            reason_codes = []
        expected_codes = REASON_CODES_BY_SEMANTIC_STATE.get((str(proposal_type), str(relation)), frozenset())
        if frozenset(reason_codes) != expected_codes:
            _append(errors, f"{path}.reason_codes", "must exactly match proposal_type and relation_state")
        expected_explanation = _expected_explanation(proposal_type, relation)
        if proposal["reason"] != expected_explanation:
            _append(
                errors,
                f"{path}.reason",
                "must exactly equal the closed deterministic explanation template",
            )
        if proposal["reason_source"] != REASON_SOURCE:
            _append(errors, f"{path}.reason_source", f"must remain {REASON_SOURCE}")
        if not isinstance(proposal["confidence"], str) or proposal["confidence"] not in CONFIDENCE_STATES:
            _append(errors, f"{path}.confidence", "unsupported")
        if proposal["evidence_basis"] != "profile_bio_only":
            _append(errors, f"{path}.evidence_basis", "must remain profile_bio_only")
        if proposal["requires_independent_verification"] is not True:
            _append(errors, f"{path}.requires_independent_verification", "must be true")
        try:
            signature = canonical_json(_semantic_identity(proposal))
        except (TypeError, ValueError, RecursionError):
            _append(errors, path, "must be canonical JSON")
        else:
            if signature in signatures:
                _append(errors, path, "duplicate proposal")
            signatures.add(signature)
    if not json_type_strict_equal(model_output["authority"], MODEL_AUTHORITY):
        _append(errors, "$.authority", "all authority and external-fact claims must remain false")
    return errors


def _bound_request_identity(request: Any) -> tuple[str, str, str, str, str] | None:
    """Return exact request bindings or no artifact identity at all."""

    if not isinstance(request, dict) or _scan_json(request, max_depth=64, max_nodes=4096):
        return None
    if _canonical_size_status(request, maximum_bytes=BUDGETS["max_request_canonical_bytes"]) != "ok":
        return None
    request_id = request.get("request_id")
    subject = request.get("subject")
    profile = request.get("profile_snapshot")
    if not isinstance(subject, dict) or not isinstance(profile, dict):
        return None
    subject_platform_user_id = subject.get("platform_user_id")
    snapshot_id = profile.get("snapshot_id")
    platform_user_id = profile.get("platform_user_id")
    bio_text = profile.get("bio_text")
    bio_sha256 = profile.get("bio_sha256")
    if (
        not isinstance(request_id, str)
        or re.fullmatch(r"xbsv2r_[0-9a-f]{24}", request_id) is None
        or not isinstance(snapshot_id, str)
        or re.fullmatch(r"xbsv2s_[0-9a-f]{24}", snapshot_id) is None
        or not isinstance(platform_user_id, str)
        or _PLATFORM_USER_ID_RE.fullmatch(platform_user_id) is None
        or subject_platform_user_id != platform_user_id
        or not _is_utf8_scalar_text(bio_text, minimum=1, maximum=2000)
        or not bio_text.strip()
        or not isinstance(bio_sha256, str)
        or _SHA256_RE.fullmatch(bio_sha256) is None
        or text_sha256(bio_text) != bio_sha256
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
    execution_attempt: Mapping[str, Any],
    phase: str,
    proxy_policy_provenance: Mapping[str, Any],
) -> dict[str, Any]:
    bindings = _bound_request_identity(request)
    if bindings is None:
        raise ValueError("request_binding_invalid")
    request_id, request_sha256, snapshot_id, platform_user_id, bio_sha256 = bindings
    return {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "status": status,
        "request_id": request_id,
        "request_sha256": request_sha256,
        "profile_snapshot_id": snapshot_id,
        "platform_user_id": platform_user_id,
        "bio_sha256": bio_sha256,
        "execution": _execution_at_phase(execution_attempt, phase),
        "model_receipt": None,
        "usage": None,
        "verdict": "abstained",
        "proposals": [],
        "professional_experience_proxy_policy": copy.deepcopy(dict(proxy_policy_provenance)),
        "professional_experience_proxy_rollup": _no_professional_experience_proxy_rollup(),
        "error_codes": [error_code],
        "authority": dict(REVIEW_AUTHORITY),
    }


def _response_text_and_usage(response: Any, *, request: Mapping[str, Any]) -> tuple[str, dict[str, int], str]:
    budgets = request["budgets"]
    traversal = _scan_json(
        response,
        max_depth=budgets["max_validation_depth"],
        max_nodes=budgets["max_validation_nodes"],
        max_string_characters=budgets["max_json_string_characters"],
        max_string_bytes=budgets["max_json_string_bytes"],
    )
    if traversal:
        budget_errors = {
            "nested_node_budget_exceeded",
            "nested_depth_budget_exceeded",
            "string_character_budget_exceeded",
            "string_byte_budget_exceeded",
        }
        raise _ReviewError("budget_exceeded" if budget_errors.intersection(traversal) else "response_invalid")
    if not isinstance(response, dict):
        raise _ReviewError("response_invalid")
    size_status = _canonical_size_status(
        response,
        maximum_bytes=budgets["max_raw_response_canonical_bytes"],
    )
    if size_status == "budget":
        raise _ReviewError("budget_exceeded")
    if size_status != "ok":
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
        or response.get("incomplete_details") not in (None, "")
        or response.get("error") not in (None, "")
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
            item_keys = set(item)
            if (
                reasoning_items > 1
                or not _REASONING_REQUIRED_KEYS <= item_keys
                or not item_keys <= _REASONING_REQUIRED_KEYS | _REASONING_OPTIONAL_KEYS
                or not isinstance(item.get("id"), str)
                or _REASONING_ID_RE.fullmatch(item["id"]) is None
                or ("status" in item and item["status"] != "completed")
            ):
                raise _ReviewError("response_invalid")
            summaries = item.get("summary", [])
            if not isinstance(summaries, list) or len(summaries) > budgets["max_reasoning_summary_items"]:
                raise _ReviewError("response_invalid")
            summary_characters = 0
            summary_bytes = 0
            for summary in summaries:
                if (
                    not isinstance(summary, dict)
                    or set(summary) != _REASONING_SUMMARY_KEYS
                    or summary.get("type") != "summary_text"
                    or not _is_utf8_scalar_text(summary.get("text"), maximum=budgets["max_json_string_characters"])
                ):
                    raise _ReviewError("response_invalid")
                summary_characters += len(summary["text"])
                summary_bytes += len(summary["text"].encode("utf-8"))
            if (
                summary_characters > budgets["max_reasoning_summary_characters"]
                or summary_bytes > budgets["max_reasoning_summary_bytes"]
            ):
                raise _ReviewError("budget_exceeded")
            continue
        # Tools are disabled in the request, so every tool/call/unknown output
        # item is a contract violation even when a message is also present.
        raise _ReviewError("response_invalid")
    if len(messages) != 1:
        raise _ReviewError("response_invalid")
    message = messages[0]
    message_keys = set(message)
    if (
        not _MESSAGE_REQUIRED_KEYS <= message_keys
        or not message_keys <= _MESSAGE_REQUIRED_KEYS | _MESSAGE_OPTIONAL_KEYS
        or message.get("type") != "message"
        or message.get("status") != "completed"
        or message.get("role") != "assistant"
        or ("phase" in message and message["phase"] != "final_answer")
        or not isinstance(message.get("id"), str)
        or _MESSAGE_ID_RE.fullmatch(message["id"]) is None
    ):
        raise _ReviewError("response_invalid")
    content = message.get("content")
    if not isinstance(content, list) or len(content) != 1 or not isinstance(content[0], dict):
        raise _ReviewError("response_invalid")
    output_text = content[0]
    text = output_text.get("text")
    output_text_keys = set(output_text)
    if (
        not _OUTPUT_TEXT_REQUIRED_KEYS <= output_text_keys
        or not output_text_keys <= _OUTPUT_TEXT_REQUIRED_KEYS | _OUTPUT_TEXT_OPTIONAL_KEYS
        or output_text.get("type") != "output_text"
        or output_text.get("annotations") != []
        or ("logprobs" in output_text and output_text["logprobs"] not in (None, []))
        or not isinstance(text, str)
    ):
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
    proxy_policy: Mapping[str, Any],
    proxy_policy_provenance: Mapping[str, Any],
    response: Any,
    execution_attempt: Mapping[str, Any],
) -> dict[str, Any]:
    is_live = execution_attempt.get("mode") == "live"
    transport_id = execution_attempt.get("transport_id")
    execute_live = execution_attempt.get("execute_live")
    expected_mode = "live_canary" if is_live else "offline_fake"
    if (
        execution_attempt.get("contract_error") != "none"
        or not _valid_transport_pair(is_live=is_live, transport_id=transport_id)
        or execute_live is not is_live
        or request.get("model_execution_mode") != expected_mode
    ):
        raise _ReviewError("execution_contract_invalid")
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
        proposal_id = (
            "xbsv2p_"
            + canonical_sha256(
                {"request_id": request["request_id"], "semantic_identity": _semantic_identity(proposal)}
            )[:24]
        )
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
                "reason_source": REASON_SOURCE,
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
        "execution": _execution_at_phase(execution_attempt, "completed"),
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
        "professional_experience_proxy_policy": copy.deepcopy(dict(proxy_policy_provenance)),
        "professional_experience_proxy_rollup": _professional_experience_proxy_rollup(
            proposals,
            proxy_policy=proxy_policy,
        ),
        "error_codes": [],
        "authority": dict(REVIEW_AUTHORITY),
    }


def adjudicate_observed_response(
    *,
    request: Any,
    prompt: Any,
    output_schema: Any,
    proxy_policy: Any,
    execution_attempt: Any,
    raw_response: Any,
) -> dict[str, Any]:
    """Purely adjudicate one explicit observed attempt without transport inference."""

    if validate_pure_adjudication_implementation():
        raise ValueError("pure_adjudication_implementation_invalid")
    resolved_proxy_policy = _resolve_proxy_policy(proxy_policy)
    request_snapshot, request_snapshot_valid = _try_canonical_snapshot(request)
    prompt_snapshot, prompt_snapshot_valid = _try_canonical_snapshot(prompt)
    output_schema_snapshot, output_schema_snapshot_valid = _try_canonical_snapshot(output_schema)
    proxy_policy_snapshot, proxy_policy_snapshot_valid = _try_canonical_snapshot(resolved_proxy_policy)
    execution_snapshot, execution_snapshot_valid = _try_canonical_snapshot(execution_attempt)
    response_snapshot, response_snapshot_valid = _try_canonical_snapshot(raw_response)

    if not request_snapshot_valid:
        raise ValueError("request_binding_invalid")
    prompt_errors = ["snapshot_invalid"] if not prompt_snapshot_valid else validate_prompt(prompt_snapshot)
    output_schema_errors = (
        ["snapshot_invalid"] if not output_schema_snapshot_valid else validate_output_schema(output_schema_snapshot)
    )
    proxy_policy_errors = (
        ["snapshot_invalid"]
        if not proxy_policy_snapshot_valid or proxy_policy_snapshot is None
        else validate_proxy_policy(proxy_policy_snapshot)
    )
    policy_valid = not proxy_policy_errors
    policy_provenance = _proxy_policy_provenance(
        proxy_policy_snapshot,
        snapshot_valid=proxy_policy_snapshot_valid,
        policy_valid=policy_valid,
    )
    request_errors = (
        []
        if prompt_errors or output_schema_errors or proxy_policy_errors
        else validate_request(
            request_snapshot,
            prompt=prompt_snapshot,
            output_schema=output_schema_snapshot,
            proxy_policy=proxy_policy_snapshot,
        )
    )
    asset_error_code = (
        "prompt_invalid"
        if prompt_errors
        else "output_schema_invalid"
        if output_schema_errors
        else "proxy_policy_invalid"
        if proxy_policy_errors
        else "request_invalid"
        if request_errors
        else None
    )
    if asset_error_code is not None:
        if raw_response is not None:
            raise ValueError("asset_preflight_response_conflict")
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code=asset_error_code,
            execution_attempt=_execution_not_inspected(),
            phase="preflight",
            proxy_policy_provenance=policy_provenance,
        )

    if (
        not execution_snapshot_valid
        or not isinstance(execution_snapshot, dict)
        or _execution_record_errors(execution_snapshot)
        or execution_snapshot.get("contract_error") != "none"
    ):
        raise ValueError("execution_attempt_invalid")
    phase = execution_snapshot["phase"]
    if phase == "transport":
        if raw_response is not None:
            raise ValueError("transport_failure_must_not_have_response")
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code="transport_failed",
            execution_attempt=execution_snapshot,
            phase="transport",
            proxy_policy_provenance=policy_provenance,
        )
    if phase not in {"response", "semantic", "completed"} or execution_snapshot["response_received"] is not True:
        raise ValueError("execution_attempt_not_response_bearing")
    if raw_response is None or not response_snapshot_valid:
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code="response_invalid",
            execution_attempt=execution_snapshot,
            phase="response",
            proxy_policy_provenance=policy_provenance,
        )
    try:
        return _completed_result(
            request_snapshot,
            prompt=prompt_snapshot,
            output_schema=output_schema_snapshot,
            proxy_policy=proxy_policy_snapshot,
            proxy_policy_provenance=policy_provenance,
            response=response_snapshot,
            execution_attempt=execution_snapshot,
        )
    except _ReviewError as exc:
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code=exc.code,
            execution_attempt=execution_snapshot,
            phase="semantic" if exc.code == "model_output_invalid" else "response",
            proxy_policy_provenance=policy_provenance,
        )


def run_semantic_review(
    request: Any,
    *,
    prompt: Any,
    output_schema: Any,
    transport: ResponsesTransport,
    execute_live: Any = False,
    proxy_policy: Any = None,
) -> dict[str, Any]:
    """Run one bounded review or return a closed terminal failure envelope."""

    resolved_proxy_policy = _resolve_proxy_policy(proxy_policy)
    request_snapshot, request_snapshot_valid = _try_canonical_snapshot(request)
    prompt_snapshot, prompt_snapshot_valid = _try_canonical_snapshot(prompt)
    output_schema_snapshot, output_schema_snapshot_valid = _try_canonical_snapshot(output_schema)
    proxy_policy_snapshot, proxy_policy_snapshot_valid = _try_canonical_snapshot(resolved_proxy_policy)

    if not request_snapshot_valid:
        raise ValueError("request_binding_invalid")
    prompt_errors = ["snapshot_invalid"] if not prompt_snapshot_valid else validate_prompt(prompt_snapshot)
    output_schema_errors = (
        ["snapshot_invalid"] if not output_schema_snapshot_valid else validate_output_schema(output_schema_snapshot)
    )
    proxy_policy_errors = (
        ["snapshot_invalid"]
        if not proxy_policy_snapshot_valid or proxy_policy_snapshot is None
        else validate_proxy_policy(proxy_policy_snapshot)
    )
    policy_valid = not proxy_policy_errors
    policy_provenance = _proxy_policy_provenance(
        proxy_policy_snapshot,
        snapshot_valid=proxy_policy_snapshot_valid,
        policy_valid=policy_valid,
    )
    preflight_execution = _execution_not_inspected(execute_live)
    if prompt_errors:
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code="prompt_invalid",
            execution_attempt=preflight_execution,
            phase="preflight",
            proxy_policy_provenance=policy_provenance,
        )
    if output_schema_errors:
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code="output_schema_invalid",
            execution_attempt=preflight_execution,
            phase="preflight",
            proxy_policy_provenance=policy_provenance,
        )
    if proxy_policy_errors:
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code="proxy_policy_invalid",
            execution_attempt=preflight_execution,
            phase="preflight",
            proxy_policy_provenance=policy_provenance,
        )
    if validate_request(
        request_snapshot,
        prompt=prompt_snapshot,
        output_schema=output_schema_snapshot,
        proxy_policy=proxy_policy_snapshot,
    ):
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code="request_invalid",
            execution_attempt=preflight_execution,
            phase="preflight",
            proxy_policy_provenance=policy_provenance,
        )

    try:
        is_live = getattr(transport, "is_live", None)
        transport_id = getattr(transport, "transport_id", None)
    except Exception:  # noqa: BLE001 - arbitrary transport metadata is untrusted
        is_live = None
        transport_id = None
    execution_attempt = _execution_attempt(
        is_live=is_live,
        transport_id=transport_id,
        execute_live=execute_live,
    )
    if execution_attempt["contract_error"] != "none":
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code="execution_contract_invalid",
            execution_attempt=execution_attempt,
            phase="preflight",
            proxy_policy_provenance=policy_provenance,
        )
    is_live = execution_attempt["mode"] == "live"
    expected_execution_mode = "live_canary" if is_live else "offline_fake"
    if request_snapshot["model_execution_mode"] != expected_execution_mode:
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code="request_mode_mismatch",
            execution_attempt=execution_attempt,
            phase="preflight",
            proxy_policy_provenance=policy_provenance,
        )
    if is_live and execute_live is False:
        return _terminal_result(
            request_snapshot,
            status="blocked",
            error_code="execute_live_required",
            execution_attempt=execution_attempt,
            phase="authorization",
            proxy_policy_provenance=policy_provenance,
        )
    payload = build_responses_request(
        request_snapshot,
        prompt=prompt_snapshot,
        output_schema=output_schema_snapshot,
        proxy_policy=proxy_policy_snapshot,
    )
    try:
        response = transport.create_response(payload, timeout_ms=request_snapshot["budgets"]["timeout_ms"])
    except Exception:  # noqa: BLE001 - transport detail must not escape or echo credentials/provider content
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code="transport_failed",
            execution_attempt=execution_attempt,
            phase="transport",
            proxy_policy_provenance=policy_provenance,
        )
    response_snapshot, response_snapshot_valid = _try_canonical_snapshot(response)
    if not response_snapshot_valid:
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code="response_invalid",
            execution_attempt=execution_attempt,
            phase="response",
            proxy_policy_provenance=policy_provenance,
        )
    try:
        return _completed_result(
            request_snapshot,
            prompt=prompt_snapshot,
            output_schema=output_schema_snapshot,
            proxy_policy=proxy_policy_snapshot,
            proxy_policy_provenance=policy_provenance,
            response=response_snapshot,
            execution_attempt=execution_attempt,
        )
    except _ReviewError as exc:
        return _terminal_result(
            request_snapshot,
            status="failed",
            error_code=exc.code,
            execution_attempt=execution_attempt,
            phase="semantic" if exc.code == "model_output_invalid" else "response",
            proxy_policy_provenance=policy_provenance,
        )


def _execution_record_errors(execution: Any) -> list[str]:
    errors: list[str] = []
    if not _exact_keys(execution, _EXECUTION_KEYS, path="$.execution", errors=errors):
        return errors
    mode = execution["mode"]
    transport_id = execution["transport_id"]
    execute_live = execution["execute_live"]
    phase = execution["phase"]
    contract_error = execution["contract_error"]
    invocations = execution["transport_invocations"]
    provider_calls = execution["provider_external_calls"]
    response_received = execution["response_received"]
    if not isinstance(mode, str) or mode not in _EXECUTION_MODES:
        _append(errors, "$.execution.mode", "unsupported")
    if not isinstance(transport_id, str) or transport_id not in _TRANSPORT_IDS:
        _append(errors, "$.execution.transport_id", "unsupported")
    if type(execute_live) is not bool and execute_live is not None:
        _append(errors, "$.execution.execute_live", "must be boolean or null")
    if not isinstance(phase, str) or phase not in _EXECUTION_PHASES:
        _append(errors, "$.execution.phase", "unsupported")
    if not isinstance(contract_error, str) or contract_error not in _EXECUTION_CONTRACT_ERRORS:
        _append(errors, "$.execution.contract_error", "unsupported")
    if not _is_int(invocations) or invocations not in {0, 1}:
        _append(errors, "$.execution.transport_invocations", "must be zero or one")
    if not _is_int(provider_calls) or provider_calls not in {0, 1}:
        _append(errors, "$.execution.provider_external_calls", "must be zero or one")
    if type(response_received) is not bool:
        _append(errors, "$.execution.response_received", "must be boolean")
    if execution["fallback_used"] is not False:
        _append(errors, "$.execution.fallback_used", "must be false")
    if errors:
        return errors

    is_live = mode == "live"
    pair_valid = _valid_transport_pair(is_live=is_live, transport_id=transport_id)
    if contract_error == "not_evaluated":
        if (mode, transport_id, phase) != ("not_inspected", "not_inspected", "preflight"):
            _append(errors, "$.execution", "not_evaluated requires an uninspected preflight record")
    elif contract_error == "none":
        if mode not in {"offline_fake", "live"} or not pair_valid or type(execute_live) is not bool:
            _append(errors, "$.execution", "valid contract requires one exact transport tuple and boolean flag")
        elif mode == "offline_fake" and execute_live:
            _append(errors, "$.execution", "offline execute-live attempt must be a contract failure")
    elif contract_error == "invalid_is_live":
        if mode != "invalid":
            _append(errors, "$.execution", "invalid_is_live requires mode=invalid")
    elif contract_error == "invalid_transport_id":
        if mode not in {"offline_fake", "live"} or transport_id != "invalid":
            _append(errors, "$.execution", "invalid_transport_id projection is incoherent")
    elif contract_error == "transport_pair_mismatch":
        if mode not in {"offline_fake", "live"} or transport_id == "invalid" or pair_valid:
            _append(errors, "$.execution", "transport_pair_mismatch projection is incoherent")
    elif contract_error == "invalid_execute_live":
        if not pair_valid or execute_live is not None:
            _append(errors, "$.execution", "invalid_execute_live requires a valid pair and null flag")
    elif contract_error == "offline_execute_live":
        if (mode, transport_id, execute_live) != ("offline_fake", "offline_fake_responses", True):
            _append(errors, "$.execution", "offline_execute_live projection is incoherent")

    before_call = phase in {"preflight", "authorization"}
    if before_call:
        if invocations != 0 or provider_calls != 0 or response_received:
            _append(errors, "$.execution", "pre-call phase must have zero calls and no response")
    else:
        authorized = contract_error == "none" and (
            (mode, transport_id, execute_live) == ("offline_fake", "offline_fake_responses", False)
            or (mode, transport_id, execute_live) == ("live", "openai_compatible_responses", True)
        )
        if not authorized or invocations != 1 or provider_calls != (1 if is_live else 0):
            _append(errors, "$.execution", "post-call phase requires one authorized invocation and exact call count")
        if response_received is not (phase != "transport"):
            _append(errors, "$.execution.response_received", "does not match phase")
    if phase == "authorization" and (
        contract_error != "none" or (mode, transport_id, execute_live) != ("live", "openai_compatible_responses", False)
    ):
        _append(errors, "$.execution", "authorization phase requires the exact blocked live tuple")
    if phase != "preflight" and contract_error != "none":
        _append(errors, "$.execution", "contract failures must terminate in preflight")
    return errors


def _attempt_from_transport(transport: ResponsesTransport, execute_live: Any) -> dict[str, Any]:
    try:
        is_live = getattr(transport, "is_live", None)
        transport_id = getattr(transport, "transport_id", None)
    except Exception:  # noqa: BLE001 - arbitrary transport metadata is untrusted
        is_live = None
        transport_id = None
    return _execution_attempt(is_live=is_live, transport_id=transport_id, execute_live=execute_live)


def validate_review(
    review: Any,
    *,
    request: Any,
    prompt: Any,
    output_schema: Any,
    raw_response: Any,
    execution_attempt: Any = _MISSING,
    transport: ResponsesTransport | None = None,
    execute_live: Any = _MISSING,
    proxy_policy: Any = None,
) -> list[str]:
    """Recompute semantic content without granting execution authority.

    ``execution_attempt`` is retained only as a fail-closed compatibility
    boundary and is never accepted.  A JSON object cannot prove that it was
    produced independently merely because it arrived through another argument.
    Offline completed reviews use one fixed, non-billable replay projection.
    Live/provider/billable reviews require the module-external Luna artifact
    validator, which binds the approval ledger, observed HTTP receipt, retained
    response, and result bundle before replaying this semantic adjudication.
    """

    resolved_proxy_policy = _resolve_proxy_policy(proxy_policy)
    review_snapshot, review_snapshot_valid = _try_canonical_snapshot(review)
    request_snapshot, request_snapshot_valid = _try_canonical_snapshot(request)
    prompt_snapshot, prompt_snapshot_valid = _try_canonical_snapshot(prompt)
    output_schema_snapshot, output_schema_snapshot_valid = _try_canonical_snapshot(output_schema)
    proxy_policy_snapshot, proxy_policy_snapshot_valid = _try_canonical_snapshot(resolved_proxy_policy)
    raw_response_snapshot, raw_response_snapshot_valid = _try_canonical_snapshot(raw_response)
    if not review_snapshot_valid:
        return ["$.validation: canonical_snapshot_invalid"]
    if not request_snapshot_valid:
        return ["request: request_binding_invalid"]
    if not raw_response_snapshot_valid:
        raw_response_snapshot = _INVALID_SNAPSHOT

    prompt_errors = ["snapshot_invalid"] if not prompt_snapshot_valid else validate_prompt(prompt_snapshot)
    output_schema_errors = (
        ["snapshot_invalid"] if not output_schema_snapshot_valid else validate_output_schema(output_schema_snapshot)
    )
    proxy_policy_errors = (
        ["snapshot_invalid"]
        if not proxy_policy_snapshot_valid or proxy_policy_snapshot is None
        else validate_proxy_policy(proxy_policy_snapshot)
    )
    policy_valid = not proxy_policy_errors
    policy_provenance = _proxy_policy_provenance(
        proxy_policy_snapshot,
        snapshot_valid=proxy_policy_snapshot_valid,
        policy_valid=policy_valid,
    )
    request_errors = (
        []
        if prompt_errors or output_schema_errors or proxy_policy_errors
        else validate_request(
            request_snapshot,
            prompt=prompt_snapshot,
            output_schema=output_schema_snapshot,
            proxy_policy=proxy_policy_snapshot,
        )
    )

    traversal = _scan_json(review_snapshot, max_depth=64, max_nodes=4096)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    errors: list[str] = []
    if not _exact_keys(review_snapshot, _REVIEW_KEYS, path="$", errors=errors):
        return errors
    bindings = _bound_request_identity(request_snapshot)
    if bindings is None:
        return ["request: request_binding_invalid"]
    request_id, request_sha256, snapshot_id, platform_user_id, bio_sha256 = bindings
    expected_bindings = {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "request_id": request_id,
        "request_sha256": request_sha256,
        "profile_snapshot_id": snapshot_id,
        "platform_user_id": platform_user_id,
        "bio_sha256": bio_sha256,
    }
    for field_name, expected_value in expected_bindings.items():
        if not json_type_strict_equal(review_snapshot[field_name], expected_value):
            _append(errors, f"$.{field_name}", "does not exactly bind the request")
    if not json_type_strict_equal(
        review_snapshot["professional_experience_proxy_policy"],
        policy_provenance,
    ):
        _append(
            errors,
            "$.professional_experience_proxy_policy",
            "does not exactly bind expected and observed proxy policy provenance",
        )
    if not json_type_strict_equal(review_snapshot["authority"], REVIEW_AUTHORITY):
        _append(errors, "$.authority", "all review authority must remain false")
    execution = review_snapshot["execution"]
    errors.extend(_execution_record_errors(execution))
    status = review_snapshot["status"]
    if not isinstance(status, str) or status not in {"completed", "blocked", "failed"}:
        _append(errors, "$.status", "unsupported")
    error_codes = review_snapshot["error_codes"]
    if not isinstance(error_codes, list) or any(not isinstance(code, str) for code in error_codes):
        _append(errors, "$.error_codes", "must be a string array")
    if errors:
        return errors

    if execution_attempt is not _MISSING:
        return [
            "execution_attempt: semantic replay rejects caller-supplied execution authority; "
            "use the outer Luna receipt/ledger validator"
        ]

    asset_error_code = (
        "prompt_invalid"
        if prompt_errors
        else "output_schema_invalid"
        if output_schema_errors
        else "proxy_policy_invalid"
        if proxy_policy_errors
        else "request_invalid"
        if request_errors
        else None
    )
    if asset_error_code is not None:
        if raw_response_snapshot is not None:
            return ["raw_response: asset preflight failure requires no response"]
        expected = _terminal_result(
            request_snapshot,
            status="failed",
            error_code=asset_error_code,
            execution_attempt=_execution_not_inspected(),
            phase="preflight",
            proxy_policy_provenance=policy_provenance,
        )
        return (
            []
            if json_type_strict_equal(review_snapshot, expected)
            else ["$: review does not equal deterministic source-bound recomputation"]
        )

    invocation_supplied = transport is not None or execute_live is not _MISSING
    if status == "completed":
        if request_snapshot.get("model_execution_mode") != "offline_fake":
            return [
                "live_execution_authority: semantic replay cannot validate provider calls or billing; "
                "use the outer Luna receipt/ledger validator"
            ]
        if invocation_supplied:
            return ["invocation: completed semantic replay does not accept transport execution claims"]
        attempt = _execution_attempt(
            is_live=False,
            transport_id="offline_fake_responses",
            execute_live=False,
        )
    elif invocation_supplied:
        if transport is None or execute_live is _MISSING:
            return ["invocation: transport and execute_live must be supplied together"]
        attempt = _attempt_from_transport(transport, execute_live)
        for field_name in ("mode", "transport_id", "execute_live", "fallback_used", "contract_error"):
            if not json_type_strict_equal(execution[field_name], attempt[field_name]):
                return [f"$.execution.{field_name}: does not bind invocation evidence"]
    else:
        return ["invocation: required to recompute blocked or failed review"]

    expected: dict[str, Any]
    if attempt["contract_error"] != "none":
        if raw_response_snapshot is not None:
            return ["raw_response: preflight failure requires no response"]
        expected = _terminal_result(
            request_snapshot,
            status="failed",
            error_code="execution_contract_invalid",
            execution_attempt=attempt,
            phase="preflight",
            proxy_policy_provenance=policy_provenance,
        )
    else:
        is_live = attempt["mode"] == "live"
        expected_mode = "live_canary" if is_live else "offline_fake"
        if request_snapshot["model_execution_mode"] != expected_mode:
            if raw_response_snapshot is not None:
                return ["raw_response: preflight failure requires no response"]
            expected = _terminal_result(
                request_snapshot,
                status="failed",
                error_code="request_mode_mismatch",
                execution_attempt=attempt,
                phase="preflight",
                proxy_policy_provenance=policy_provenance,
            )
        elif is_live and attempt["execute_live"] is False:
            if raw_response_snapshot is not None:
                return ["raw_response: blocked review requires no response"]
            expected = _terminal_result(
                request_snapshot,
                status="blocked",
                error_code="execute_live_required",
                execution_attempt=attempt,
                phase="authorization",
                proxy_policy_provenance=policy_provenance,
            )
        elif status == "completed":
            if raw_response_snapshot is None:
                return ["raw_response: completed review requires a response"]
            try:
                expected = _completed_result(
                    request_snapshot,
                    prompt=prompt_snapshot,
                    output_schema=output_schema_snapshot,
                    proxy_policy=proxy_policy_snapshot,
                    proxy_policy_provenance=policy_provenance,
                    response=raw_response_snapshot,
                    execution_attempt=attempt,
                )
            except _ReviewError as exc:
                return [f"raw_response: {exc.code}"]
        elif status == "failed" and json_type_strict_equal(error_codes, ["transport_failed"]):
            if raw_response_snapshot is not None:
                return ["raw_response: transport failure requires no response"]
            expected = _terminal_result(
                request_snapshot,
                status="failed",
                error_code="transport_failed",
                execution_attempt=attempt,
                phase="transport",
                proxy_policy_provenance=policy_provenance,
            )
        elif status == "failed" and any(
            json_type_strict_equal(error_codes, expected_codes)
            for expected_codes in (["response_invalid"], ["model_output_invalid"], ["budget_exceeded"])
        ):
            if raw_response_snapshot is None:
                return ["raw_response: post-call failure requires a response"]
            try:
                _completed_result(
                    request_snapshot,
                    prompt=prompt_snapshot,
                    output_schema=output_schema_snapshot,
                    proxy_policy=proxy_policy_snapshot,
                    proxy_policy_provenance=policy_provenance,
                    response=raw_response_snapshot,
                    execution_attempt=attempt,
                )
            except _ReviewError as exc:
                expected = _terminal_result(
                    request_snapshot,
                    status="failed",
                    error_code=exc.code,
                    execution_attempt=attempt,
                    phase="semantic" if exc.code == "model_output_invalid" else "response",
                    proxy_policy_provenance=policy_provenance,
                )
            else:
                return ["$: failed review supplied with a successful response"]
        else:
            return ["$: terminal state is not reachable from invocation evidence"]
    return (
        []
        if json_type_strict_equal(review_snapshot, expected)
        else ["$: review does not equal deterministic source-bound recomputation"]
    )
