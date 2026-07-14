"""Deterministic evaluation for bounded Grok CLI X-search explorations.

This module intentionally starts at a sanitized, model-mediated exploration
result plus a session-derived tool receipt.  It can prove which hosted X tools
were called and can validate candidate/evidence structure, but it cannot make
encrypted provider result bodies replayable.  Its output is an experiment
diagnostic and hydration plan, never a Stage 2 collection artifact or a
canonical researcher decision.
"""

from __future__ import annotations

import hashlib
import json
import re
import stat
import unicodedata
from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

EVALUATION_SCHEMA_VERSION = "x.grok_cli.exploration.evaluation.v0"
HYDRATION_TASK_VERSION = "x.grok_cli.candidate_hydration.task.v0"
SUPPORTED_RECEIPT_VERSION = "x.grok_cli.exploration.tool_receipt.v0"
QUERY_POLICY_SCHEMA_VERSION = "x.grok_cli.exploration.query_policy_descriptor.v1"
QUERY_POLICY_REGISTRY_SCHEMA_VERSION = "x.grok_cli.exploration.query_policy_registry.v1"
CANDIDATE_VALUE_POLICY_SCHEMA_VERSION = "x.grok_cli.candidate_value_segment_policy.v1"
CANDIDATE_VALUE_POLICY_CANONICAL_SHA256 = "78800fd8ae6977e49301aaf1c6ee3663d74639d7969d829354a7e1676a917d91"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_QUERY_POLICY_REGISTRY_VERSION = "approved-query-policies-v1"
QUERY_POLICY_REGISTRY_DIRECTORY = PROJECT_ROOT / "configs/grok_cli_exploration_query_policy_registries"
CANDIDATE_VALUE_POLICY_PATH = PROJECT_ROOT / "configs/candidate_value_segment_policy.v1.json"

ALLOWED_TOOL_NAMES = frozenset(
    {
        "x_keyword_search",
        "x_semantic_search",
        "x_user_search",
        "x_thread_fetch",
    }
)
ALLOWED_RELATIONSHIPS = frozenset({"self", "official_lab", "colleague_or_team", "third_party", "historical"})
HIGH_AUTHORITY_RELATIONSHIPS = frozenset({"self", "official_lab", "colleague_or_team"})
CANDIDATE_DIMENSION_STATES = frozenset({"current", "historical", "ambiguous", "unsupported"})
CONFIDENCE_STATES = frozenset({"high", "medium", "low"})
EVIDENCE_KINDS = frozenset({"bio", "post", "mention", "thread"})
SUPPORTED_STATUSES = frozenset({"X_SEARCH_OK", "X_SEARCH_PARTIAL", "X_SEARCH_BLOCKED"})

_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_PLATFORM_USER_ID_RE = re.compile(r"[1-9][0-9]{1,24}")
_POST_ID_RE = re.compile(r"[1-9][0-9]{5,23}")
_PROFILE_URL_RE = re.compile(r"https://x\.com/(?P<handle>[A-Za-z0-9_]{1,15})")
_POST_URL_RE = re.compile(r"https://x\.com/(?P<handle>[A-Za-z0-9_]{1,15})/status/(?P<post_id>[1-9][0-9]{5,23})")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_CLI_VERSION_RE = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
_MODEL_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_POLICY_VERSION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_LAB_ID_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
_SESSION_HASH_FILES = frozenset({"chat_history.jsonl", "events.jsonl", "summary.json", "updates.jsonl"})
_RECEIPT_KEYS = {
    "schema_version",
    "cli_version",
    "model_id",
    "session_id",
    "request_id",
    "generic_web_disabled_by_cli",
    "local_tools_removed_by_denylist",
    "evidence_boundary",
    "tool_counts",
    "calls",
    "raw_session_sha256",
}
_RESULT_KEYS = {
    "status",
    "status_reason",
    "native_x_tool_provenance",
    "counts",
    "candidates",
    "excluded_examples",
    "limitations",
    "local_reconciliation",
}
_QUERY_POLICY_KEYS = {
    "schema_version",
    "policy_version",
    "purpose",
    "lab_id",
    "run_binding_sha256",
    "legacy_full_policy_sha256",
    "allowed_decision_dimensions",
    "professional_experience_proxy_query_allowed",
    "protected_identity_query_allowed",
    "query_manifest",
}
_QUERY_POLICY_MANIFEST_KEYS = {"sequence", "tool_name", "call_sha256"}
_EXPERIMENT_BINDING_KEYS = {
    "lab_id",
    "session_id",
    "request_id",
    "model_id",
    "tool_policy_version",
    "query_policy_version",
    "query_policy_sha256",
    "legacy_full_query_policy_sha256",
    "query_policy_registry_version",
    "query_policy_registry_sha256",
    "query_policy_registry_row_sha256",
    "candidate_value_policy_version",
    "candidate_value_policy_sha256",
}
_QUERY_POLICY_REGISTRY_KEYS = {"schema_version", "registry_version", "policies"}
_QUERY_POLICY_REGISTRY_ROW_KEYS = {
    "policy_version",
    "policy_path",
    "policy_sha256",
    "legacy_full_policy_sha256",
    "policy_schema_version",
    "purpose",
    "lab_id",
    "run_binding_sha256",
    "enabled",
}
_CANDIDATE_VALUE_POLICY_KEYS = {
    "schema_version",
    "policy_version",
    "state_values",
    "priority_tiers",
    "segments",
    "fallback_segment_id",
    "fallback_priority_tier",
    "precision_tranche",
    "hydration",
}
_CANDIDATE_SEGMENT_KEYS = {
    "segment_id",
    "target_lab_affiliation_state",
    "pretraining_experience_state",
    "priority_tier",
    "recall_pool_eligible",
}
_EXPECTED_SEGMENT_BINDINGS = {
    "precision_current_current": ("current", "current", "precision"),
    "recall_current_historical": ("current", "historical", "mixed_recall"),
    "recall_historical_current": ("historical", "current", "mixed_recall"),
    "recall_historical_historical": ("historical", "historical", "historical_recall"),
}
_PRIORITY_TIER_KEYS = {"precision", "mixed_recall", "historical_recall", "needs_evidence"}
_VALUE_SEGMENT_IDS = frozenset(
    {
        "precision_current_current",
        "recall_current_historical",
        "recall_historical_current",
        "recall_historical_historical",
        "needs_evidence",
    }
)
_CANDIDATE_DIMENSION_FIELDS = ("target_lab_affiliation_state", "pretraining_experience_state")
_BASE_DISCOVERY_DIMENSIONS = ["lab_affiliation", "role_function", "pretraining_relevance"]
_HYDRATION_REASON_ORDER = (
        "missing_platform_user_id",
        "missing_bio",
        "target_lab_affiliation_state_unresolved",
        "pretraining_experience_state_unresolved",
        "high_authority_target_lab_affiliation_state_evidence_missing",
        "high_authority_pretraining_experience_state_evidence_missing",
)
_HYDRATION_REASON_VALUES = frozenset(_HYDRATION_REASON_ORDER)
_HYDRATION_TASK_KEYS = {
    "task_version",
    "task_status",
    "task_key",
    "experiment_binding",
    "candidate_state_binding",
    "handle",
    "profile_url",
    "candidate_value_segment",
    "reasons",
    "requested_tools",
    "seed_post_urls",
    "max_tool_calls",
    "max_posts",
    "required_fields",
    "execution_authorized",
}
_CANDIDATE_STATE_BINDING_KEYS = {
    "candidate_sha256",
    "target_lab_affiliation_state",
    "pretraining_experience_state",
    "candidate_value_segment",
}
_EVALUATION_KEYS = {
    "schema_version",
    "status",
    "native_x_call_proof",
    "input_binding",
    "candidate_search_feasibility",
    "researcher_role_function_feasibility",
    "source_replayability",
    "scale_verdict",
    "scale_blockers",
    "next_phase",
    "tool_counts",
    "metrics",
    "candidate_value_assessments",
    "first_field_gate_gaps",
    "hydration_tasks",
    "authority",
}
_EVALUATION_INPUT_BINDING_KEYS = {
    "session_id",
    "request_id",
    "model_id",
    "tool_policy_version",
    "result_sha256",
    "receipt_sha256",
    "query_policy_schema_version",
    "query_policy_version",
    "query_policy_sha256",
    "legacy_full_query_policy_sha256",
    "query_policy_purpose",
    "query_policy_allowed_decision_dimensions",
    "query_policy_registry_schema_version",
    "query_policy_registry_version",
    "query_policy_registry_sha256",
    "query_policy_registry_row_sha256",
    "lab_id",
    "candidate_value_policy_schema_version",
    "candidate_value_policy_version",
    "candidate_value_policy_sha256",
    "result_to_receipt",
    "raw_session",
}
_ASSESSMENT_KEYS = {
    "handle",
    "target_lab_affiliation_state",
    "pretraining_experience_state",
    "candidate_value_segment",
    "recall_pool_eligible",
    "precision_tranche_eligible",
    "high_authority_evidence_complete",
    "hydration_required",
    "hydration_reasons",
}
_METRIC_KEYS = {
    "tool_call_receipt_rows",
    "session_verified_completed_tool_calls",
    "candidates_retained",
    "candidates_per_tool_call",
    "model_mediated_bio_presence_rate",
    "model_mediated_platform_user_id_presence_rate",
    "model_mediated_precision_tranche_rate",
    "model_mediated_recall_pool_rate",
    "model_mediated_high_authority_support_coverage",
    "third_party_only_candidate_rate",
    "post_evidence_records",
    "replayable_provider_post_body_rate",
    "model_reported_observations",
    "candidate_value_segment_counts",
    "precision_tranche_candidates",
    "recall_pool_candidates",
    "observations_mechanically_replayable",
}
_RAW_SESSION_BINDING_KEYS = {
    "status",
    "hashes_verified",
    "tool_calls_verified",
    "model_id_verified",
    "cli_version_verified",
    "tool_disable_flags_verified",
    "owner_only_permissions",
}
_PROTECTED_FIELD_TOKENS = frozenset(
    {
        "ancestry",
        "citizenship",
        "ethnicity",
        "gender",
        "nationality",
        "race",
        "religion",
    }
)
_PROTECTED_QUERY_RE = re.compile(
    r"\b(?:ancestry|citizenship|ethnicity|gender|nationality|race|religion)\b",
    re.IGNORECASE,
)
_PROTECTED_IDENTITY_CJK_RE = re.compile(
    r"(?<![\u3400-\u9fff])(?:华人|华裔|中国人|亚洲人)(?![\u3400-\u9fff])|族裔|种族|国籍|公民身份|宗教|性别"
)


class ExplorationValidationError(ValueError):
    """Raised when an exploratory artifact cannot be mechanically evaluated."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _run_binding_sha256(run_binding: Mapping[str, Any]) -> str:
    """Return the opaque public selector for one private session/request pair."""

    return canonical_sha256(
        {
            "session_id": run_binding.get("session_id"),
            "request_id": run_binding.get("request_id"),
        }
    )


def _reject_nonfinite(value: str) -> None:
    raise ValueError(f"non_finite_number:{value}")


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def _strict_json_loads(value: str) -> Any:
    return json.loads(value, object_pairs_hook=_strict_object, parse_constant=_reject_nonfinite)


def _load_closed_json(path: Path, *, maximum_bytes: int, error: str) -> Any:
    try:
        if not path.is_file() or path.is_symlink() or path.stat().st_size > maximum_bytes:
            raise ValueError(error)
        return _strict_json_loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise ExplorationValidationError(error) from exc


def _load_candidate_value_policy() -> tuple[Mapping[str, Any], str]:
    policy = _load_closed_json(
        CANDIDATE_VALUE_POLICY_PATH,
        maximum_bytes=100_000,
        error="candidate_value_policy_file_invalid",
    )
    if not isinstance(policy, dict) or set(policy) != _CANDIDATE_VALUE_POLICY_KEYS:
        raise ExplorationValidationError("candidate_value_policy_schema_invalid")
    policy_sha256 = canonical_sha256(policy)
    tiers = policy.get("priority_tiers")
    if (
        policy.get("schema_version") != CANDIDATE_VALUE_POLICY_SCHEMA_VERSION
        or _POLICY_VERSION_RE.fullmatch(str(policy.get("policy_version"))) is None
        or policy.get("state_values") != ["current", "historical", "ambiguous", "unsupported"]
        or not isinstance(tiers, dict)
        or set(tiers) != _PRIORITY_TIER_KEYS
        or any(type(value) is not int or not 0 <= value <= 10_000 for value in tiers.values())
        or not tiers["precision"] > tiers["mixed_recall"] > tiers["historical_recall"] > tiers["needs_evidence"]
        or policy.get("fallback_segment_id") != "needs_evidence"
        or policy.get("fallback_priority_tier") != "needs_evidence"
        or policy_sha256 != CANDIDATE_VALUE_POLICY_CANONICAL_SHA256
    ):
        raise ExplorationValidationError("candidate_value_policy_binding_invalid")
    segments = policy.get("segments")
    observed_ids: set[str] = set()
    if not isinstance(segments, list) or len(segments) != 4:
        raise ExplorationValidationError("candidate_value_policy_segments_invalid")
    for segment in segments:
        if not isinstance(segment, dict) or set(segment) != _CANDIDATE_SEGMENT_KEYS:
            raise ExplorationValidationError("candidate_value_policy_segments_invalid")
        binding = (
            segment["target_lab_affiliation_state"],
            segment["pretraining_experience_state"],
            segment["priority_tier"],
        )
        if (
            segment["segment_id"] not in _EXPECTED_SEGMENT_BINDINGS
            or binding != _EXPECTED_SEGMENT_BINDINGS[segment["segment_id"]]
            or segment["segment_id"] in observed_ids
            or segment["recall_pool_eligible"] is not True
        ):
            raise ExplorationValidationError("candidate_value_policy_segments_invalid")
        observed_ids.add(segment["segment_id"])
    if observed_ids != set(_EXPECTED_SEGMENT_BINDINGS):
        raise ExplorationValidationError("candidate_value_policy_segments_invalid")
    precision = policy.get("precision_tranche")
    hydration = policy.get("hydration")
    if (
        not isinstance(precision, dict)
        or set(precision)
        != {
            "required_segment_id",
            "required_confidence",
            "required_high_authority_support",
            "require_stable_platform_user_id",
            "require_bio",
        }
        or precision.get("required_segment_id") != "precision_current_current"
        or precision.get("required_confidence") != "high"
        or precision.get("required_high_authority_support") != list(_CANDIDATE_DIMENSION_FIELDS)
        or precision.get("require_stable_platform_user_id") is not True
        or precision.get("require_bio") is not True
        or not isinstance(hydration, dict)
        or set(hydration)
        != {
            "state_triggers",
            "required_high_authority_support",
            "require_stable_platform_user_id",
            "require_bio",
            "historical_state_triggers_hydration",
        }
        or hydration.get("state_triggers") != ["ambiguous", "unsupported"]
        or hydration.get("required_high_authority_support") != list(_CANDIDATE_DIMENSION_FIELDS)
        or hydration.get("require_stable_platform_user_id") is not True
        or hydration.get("require_bio") is not True
        or hydration.get("historical_state_triggers_hydration") is not False
    ):
        raise ExplorationValidationError("candidate_value_policy_rules_invalid")
    return policy, policy_sha256


def _fraction(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def _protected_key_present(value: Any) -> bool:
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            for key, child in item.items():
                separated = re.sub(r"(?<!^)(?=[A-Z])", "_", str(key))
                tokens = {token.casefold() for token in re.split(r"[^A-Za-z0-9]+", separated) if token}
                if tokens & _PROTECTED_FIELD_TOKENS:
                    return True
                stack.append(child)
        elif isinstance(item, list):
            stack.extend(item)
    return False


def _evidence_supports(candidate: Mapping[str, Any], field: str, *, high_authority_only: bool) -> bool:
    for evidence in candidate["evidence"]:
        if field not in evidence["supports"]:
            continue
        if not high_authority_only or evidence["relationship"] in HIGH_AUTHORITY_RELATIONSHIPS:
            return True
    return False


def _valid_uuid_version(value: Any, *, version: int) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = UUID(value)
        return str(parsed) == value and parsed.version == version
    except ValueError:
        return False


def _valid_session_id(value: Any) -> bool:
    return _valid_uuid_version(value, version=7)


def _valid_request_id(value: Any) -> bool:
    return _valid_uuid_version(value, version=4)


def _tool_subject(arguments: Mapping[str, Any], tool_name: str) -> str | None:
    fields = ("query",) if tool_name != "x_thread_fetch" else ("post_id", "tweet_id", "url", "query")
    values = [arguments.get(field) for field in fields]
    present = [value.strip() for value in values if isinstance(value, str) and value.strip()]
    return present[0] if len(present) == 1 else None


def _tool_arguments_valid(arguments: Mapping[str, Any], tool_name: str) -> bool:
    subject = _tool_subject(arguments, tool_name)
    if (
        subject is None
        or len(subject) > 2_000
        or _PROTECTED_QUERY_RE.search(subject) is not None
        or _PROTECTED_IDENTITY_CJK_RE.search(subject) is not None
    ):
        return False
    if tool_name == "x_keyword_search":
        if set(arguments) != {"query", "limit", "mode"} or arguments["mode"] not in {"Latest", "Top"}:
            return False
        bound = arguments["limit"]
        return isinstance(bound, str) and bound.isdecimal() and 1 <= int(bound) <= 100
    if tool_name == "x_semantic_search":
        if set(arguments) != {"query", "limit"}:
            return False
        bound = arguments["limit"]
        return isinstance(bound, str) and bound.isdecimal() and 1 <= int(bound) <= 100
    if tool_name == "x_user_search":
        if set(arguments) != {"query", "count"}:
            return False
        bound = arguments["count"]
        return isinstance(bound, str) and bound.isdecimal() and 1 <= int(bound) <= 50
    if set(arguments) in ({"post_id"}, {"tweet_id"}):
        return _POST_ID_RE.fullmatch(subject) is not None
    if set(arguments) == {"url"}:
        return _POST_URL_RE.fullmatch(subject) is not None
    return set(arguments) == {"query"}


def _validate_receipt(receipt: Any) -> list[dict[str, Any]]:
    if (
        not isinstance(receipt, dict)
        or set(receipt) != _RECEIPT_KEYS
        or receipt.get("schema_version") != SUPPORTED_RECEIPT_VERSION
    ):
        raise ExplorationValidationError("tool_receipt_schema_invalid")
    if (
        _CLI_VERSION_RE.fullmatch(str(receipt.get("cli_version"))) is None
        or _MODEL_ID_RE.fullmatch(str(receipt.get("model_id"))) is None
        or not _valid_session_id(receipt.get("session_id"))
        or not _valid_request_id(receipt.get("request_id"))
    ):
        raise ExplorationValidationError("tool_receipt_identity_invalid")
    raw_hashes = receipt.get("raw_session_sha256")
    if (
        not isinstance(raw_hashes, dict)
        or set(raw_hashes) != _SESSION_HASH_FILES
        or any(not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None for value in raw_hashes.values())
    ):
        raise ExplorationValidationError("raw_session_hash_manifest_invalid")
    if receipt.get("generic_web_disabled_by_cli") is not True:
        raise ExplorationValidationError("generic_web_not_disabled")
    if receipt.get("local_tools_removed_by_denylist") is not True:
        raise ExplorationValidationError("local_tools_not_removed")
    calls = receipt.get("calls")
    if not isinstance(calls, list) or not calls or len(calls) > 32:
        raise ExplorationValidationError("tool_calls_invalid")
    call_ids: set[str] = set()
    provider_call_ids: set[str] = set()
    observed_counts: Counter[str] = Counter()
    normalized: list[dict[str, Any]] = []
    for call in calls:
        if not isinstance(call, dict) or set(call) != {
            "tool_call_id",
            "provider_call_id",
            "tool_name",
            "arguments",
        }:
            raise ExplorationValidationError("tool_call_shape_invalid")
        call_id = call["tool_call_id"]
        provider_call_id = call["provider_call_id"]
        tool_name = call["tool_name"]
        arguments = call["arguments"]
        if (
            not isinstance(call_id, str)
            or not call_id
            or call_id in call_ids
            or not isinstance(provider_call_id, str)
            or not provider_call_id
            or provider_call_id in provider_call_ids
            or tool_name not in ALLOWED_TOOL_NAMES
            or not isinstance(arguments, dict)
            or len(canonical_json(arguments)) > 4_000
            or not _tool_arguments_valid(arguments, tool_name)
        ):
            raise ExplorationValidationError("tool_call_value_invalid")
        call_ids.add(call_id)
        provider_call_ids.add(provider_call_id)
        observed_counts[tool_name] += 1
        normalized.append(call)
    declared_counts = receipt.get("tool_counts")
    if (
        not isinstance(declared_counts, dict)
        or any(type(value) is not int or value < 1 for value in declared_counts.values())
        or canonical_json(declared_counts) != canonical_json(dict(observed_counts))
    ):
        raise ExplorationValidationError("tool_count_reconciliation_failed")
    if receipt.get("evidence_boundary") != (
        "tool names and queries are replayable; returned post bodies remain encrypted in provider context"
    ):
        raise ExplorationValidationError("evidence_boundary_invalid")
    return normalized


def _query_policy_registry_path(registry_version: str | None) -> tuple[str, Path]:
    selected_version = registry_version or DEFAULT_QUERY_POLICY_REGISTRY_VERSION
    if _POLICY_VERSION_RE.fullmatch(str(selected_version)) is None:
        raise ExplorationValidationError("query_policy_registry_selector_invalid")
    unresolved_directory = QUERY_POLICY_REGISTRY_DIRECTORY
    if unresolved_directory.is_symlink():
        raise ExplorationValidationError("query_policy_registry_path_invalid")
    unresolved_path = unresolved_directory / f"{selected_version}.json"
    if unresolved_path.is_symlink():
        raise ExplorationValidationError("query_policy_registry_path_invalid")
    try:
        unresolved_path.resolve().relative_to(PROJECT_ROOT.resolve())
    except ValueError as exc:
        raise ExplorationValidationError("query_policy_registry_path_invalid") from exc
    return selected_version, unresolved_path


def _approved_query_policy_record(
    receipt: Mapping[str, Any],
    *,
    query_policy_version: str | None,
    query_policy_registry_version: str | None = None,
) -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, str]]:
    selected_registry_version, registry_path = _query_policy_registry_path(query_policy_registry_version)
    registry = _load_closed_json(
        registry_path,
        maximum_bytes=250_000,
        error="query_policy_registry_file_invalid",
    )
    if not isinstance(registry, dict) or set(registry) != _QUERY_POLICY_REGISTRY_KEYS:
        raise ExplorationValidationError("query_policy_registry_schema_invalid")
    if (
        registry.get("schema_version") != QUERY_POLICY_REGISTRY_SCHEMA_VERSION
        or _POLICY_VERSION_RE.fullmatch(str(registry.get("registry_version"))) is None
        or registry.get("registry_version") != selected_registry_version
    ):
        raise ExplorationValidationError("query_policy_registry_binding_invalid")
    policies = registry.get("policies")
    if not isinstance(policies, list) or not policies or len(policies) > 128:
        raise ExplorationValidationError("query_policy_registry_rows_invalid")
    seen_versions: set[str] = set()
    seen_runs: set[str] = set()
    seen_hashes: set[str] = set()
    seen_paths: set[str] = set()
    matches: list[Mapping[str, Any]] = []
    for record in policies:
        if not isinstance(record, dict) or set(record) != _QUERY_POLICY_REGISTRY_ROW_KEYS:
            raise ExplorationValidationError("query_policy_registry_rows_invalid")
        run_key = record.get("run_binding_sha256")
        relative_path = Path(str(record.get("policy_path")))
        if (
            _POLICY_VERSION_RE.fullmatch(str(record.get("policy_version"))) is None
            or record["policy_version"] in seen_versions
            or not isinstance(record.get("policy_path"), str)
            or re.fullmatch(r"configs/[A-Za-z0-9_./-]+\.json", record["policy_path"]) is None
            or relative_path.is_absolute()
            or relative_path.parts[:1] != ("configs",)
            or ".." in relative_path.parts
            or relative_path.as_posix() != record["policy_path"]
            or record["policy_path"] in seen_paths
            or not isinstance(record.get("policy_sha256"), str)
            or _SHA256_RE.fullmatch(record["policy_sha256"]) is None
            or record["policy_sha256"] in seen_hashes
            or not isinstance(record.get("legacy_full_policy_sha256"), str)
            or _SHA256_RE.fullmatch(record["legacy_full_policy_sha256"]) is None
            or record.get("policy_schema_version") != QUERY_POLICY_SCHEMA_VERSION
            or record.get("purpose") != "base_researcher_discovery"
            or not isinstance(record.get("lab_id"), str)
            or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", record["lab_id"]) is None
            or not isinstance(run_key, str)
            or _SHA256_RE.fullmatch(run_key) is None
            or run_key in seen_runs
            or type(record.get("enabled")) is not bool
        ):
            raise ExplorationValidationError("query_policy_registry_rows_invalid")
        seen_versions.add(record["policy_version"])
        seen_runs.add(run_key)
        seen_hashes.add(record["policy_sha256"])
        seen_paths.add(record["policy_path"])
        if (
            record["enabled"]
            and record["run_binding_sha256"] == _run_binding_sha256(receipt)
            and (query_policy_version is None or record["policy_version"] == query_policy_version)
        ):
            matches.append(record)
    if len(matches) != 1:
        raise ExplorationValidationError("approved_query_policy_not_found")
    selected = matches[0]
    unresolved_policy_path = PROJECT_ROOT / selected["policy_path"]
    cursor = PROJECT_ROOT
    for component in Path(selected["policy_path"]).parts:
        cursor /= component
        if cursor.is_symlink():
            raise ExplorationValidationError("query_policy_path_invalid")
    policy_path = unresolved_policy_path.resolve()
    try:
        policy_path.relative_to(PROJECT_ROOT.resolve())
    except ValueError as exc:
        raise ExplorationValidationError("query_policy_path_invalid") from exc
    policy = _load_closed_json(policy_path, maximum_bytes=100_000, error="query_policy_file_invalid")
    if canonical_sha256(policy) != selected["policy_sha256"]:
        raise ExplorationValidationError("approved_query_policy_hash_mismatch")
    approval_binding = {
        "query_policy_registry_version": registry["registry_version"],
        "query_policy_registry_sha256": canonical_sha256(registry),
        "query_policy_registry_row_sha256": canonical_sha256(selected),
    }
    return selected, policy, approval_binding


def _validate_query_policy_body(
    policy: Any,
    approved_record: Mapping[str, Any],
    run_binding: Mapping[str, Any],
) -> tuple[Mapping[str, Any], str, list[dict[str, Any]]]:
    if not isinstance(policy, dict) or set(policy) != _QUERY_POLICY_KEYS:
        raise ExplorationValidationError("query_policy_schema_invalid")
    if (
        policy.get("schema_version") != QUERY_POLICY_SCHEMA_VERSION
        or policy.get("schema_version") != approved_record["policy_schema_version"]
        or policy.get("purpose") != approved_record["purpose"]
        or policy.get("lab_id") != approved_record["lab_id"]
        or policy.get("run_binding_sha256") != approved_record["run_binding_sha256"]
        or policy.get("run_binding_sha256") != _run_binding_sha256(run_binding)
        or policy.get("legacy_full_policy_sha256") != approved_record["legacy_full_policy_sha256"]
        or not isinstance(policy.get("legacy_full_policy_sha256"), str)
        or _SHA256_RE.fullmatch(policy["legacy_full_policy_sha256"]) is None
        or policy.get("policy_version") != approved_record["policy_version"]
        or policy.get("allowed_decision_dimensions") != _BASE_DISCOVERY_DIMENSIONS
        or policy.get("professional_experience_proxy_query_allowed") is not False
        or policy.get("protected_identity_query_allowed") is not False
        or _POLICY_VERSION_RE.fullmatch(str(policy.get("policy_version"))) is None
    ):
        raise ExplorationValidationError("query_policy_binding_invalid")
    manifest = policy.get("query_manifest")
    if not isinstance(manifest, list) or not manifest or len(manifest) > 32:
        raise ExplorationValidationError("query_policy_manifest_invalid")
    normalized_manifest: list[dict[str, Any]] = []
    for sequence, item in enumerate(manifest):
        if not isinstance(item, dict) or set(item) != _QUERY_POLICY_MANIFEST_KEYS:
            raise ExplorationValidationError("query_policy_manifest_invalid")
        tool_name = item.get("tool_name")
        if (
            item.get("sequence") != sequence
            or tool_name not in ALLOWED_TOOL_NAMES
            or type(item.get("sequence")) is not int
            or not isinstance(item.get("call_sha256"), str)
            or _SHA256_RE.fullmatch(item["call_sha256"]) is None
        ):
            raise ExplorationValidationError("query_policy_manifest_invalid")
        normalized_manifest.append(dict(item))
    policy_sha256 = canonical_sha256(policy)
    if policy_sha256 != approved_record["policy_sha256"]:
        raise ExplorationValidationError("approved_query_policy_hash_mismatch")
    return policy, policy_sha256, normalized_manifest


def _validate_query_policy(
    policy: Any,
    approved_record: Mapping[str, Any],
    receipt: Mapping[str, Any],
    calls: list[dict[str, Any]],
) -> tuple[Mapping[str, Any], str]:
    """Bind a receipt to one closed-world, typed, ordered query manifest.

    The text filters in ``_tool_arguments_valid`` are defense in depth. The
    authoritative boundary is the approved registry snapshot plus the exact
    ordered manifest comparison below. No caller-supplied policy body is part
    of the public evaluator API.
    """

    validated_policy, policy_sha256, normalized_manifest = _validate_query_policy_body(
        policy,
        approved_record,
        receipt,
    )
    observed_manifest = [
        {
            "sequence": sequence,
            "tool_name": call["tool_name"],
            "call_sha256": canonical_sha256(
                {"tool_name": call["tool_name"], "arguments": call["arguments"]}
            ),
        }
        for sequence, call in enumerate(calls)
    ]
    if canonical_json(normalized_manifest) != canonical_json(observed_manifest):
        raise ExplorationValidationError("query_policy_manifest_mismatch")
    return validated_policy, policy_sha256


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _raw_session_verification(
    receipt: Mapping[str, Any], calls: list[dict[str, Any]], raw_session_directory: Path | None
) -> dict[str, Any]:
    if raw_session_directory is None:
        return {
            "status": "not_supplied",
            "hashes_verified": False,
            "tool_calls_verified": False,
            "model_id_verified": False,
            "cli_version_verified": False,
            "tool_disable_flags_verified": False,
            "owner_only_permissions": None,
        }
    if raw_session_directory.is_symlink():
        raise ExplorationValidationError("raw_session_directory_invalid")
    root = raw_session_directory.resolve()
    if not root.is_dir() or root.name != receipt["session_id"]:
        raise ExplorationValidationError("raw_session_directory_invalid")
    paths = {name: root / name for name in _SESSION_HASH_FILES}
    if any(not path.is_file() or path.is_symlink() for path in paths.values()):
        raise ExplorationValidationError("raw_session_file_invalid")
    if any(path.stat().st_size > 50_000_000 for path in paths.values()):
        raise ExplorationValidationError("raw_session_file_too_large")
    observed_hashes = {name: _sha256_file(path) for name, path in paths.items()}
    if observed_hashes != receipt["raw_session_sha256"]:
        raise ExplorationValidationError("raw_session_hash_mismatch")

    try:
        summary = _strict_json_loads(paths["summary.json"].read_text(encoding="utf-8"))
    except (ValueError, UnicodeError) as exc:
        raise ExplorationValidationError("raw_session_summary_invalid") from exc
    if (
        not isinstance(summary, dict)
        or not isinstance(summary.get("info"), dict)
        or summary["info"].get("id") != receipt["session_id"]
        or summary.get("request_id") != receipt["request_id"]
        or summary.get("current_model_id") != receipt["model_id"]
    ):
        raise ExplorationValidationError("raw_session_summary_identity_mismatch")

    observed_calls: list[dict[str, Any]] = []
    started_call_ids: set[str] = set()
    completed_call_ids: set[str] = set()
    updates_path = paths["updates.jsonl"]
    with updates_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if len(line) > 5_000_000:
                raise ExplorationValidationError("raw_session_update_too_large")
            try:
                event = _strict_json_loads(line)
            except (ValueError, UnicodeError) as exc:
                raise ExplorationValidationError("raw_session_update_invalid") from exc
            if not isinstance(event, dict):
                raise ExplorationValidationError("raw_session_update_invalid")
            params = event.get("params")
            update = params.get("update") if isinstance(params, dict) else None
            if isinstance(update, dict) and update.get("sessionUpdate") == "tool_call":
                metadata = params.get("_meta")
                if params.get("sessionId") != receipt["session_id"]:
                    raise ExplorationValidationError("raw_session_started_session_identity_invalid")
                if not isinstance(metadata, dict) or metadata.get("promptId") != receipt["request_id"]:
                    raise ExplorationValidationError("raw_session_started_request_identity_invalid")
                started_call_id = update.get("toolCallId")
                if (
                    not isinstance(started_call_id, str)
                    or not started_call_id
                    or started_call_id in started_call_ids
                    or update.get("status") != "in_progress"
                ):
                    raise ExplorationValidationError("raw_session_started_call_invalid")
                started_call_ids.add(started_call_id)
                continue
            raw = update.get("rawOutput") if isinstance(update, dict) else None
            if not isinstance(update, dict) or update.get("sessionUpdate") != "tool_call_update":
                continue
            if update.get("status") != "completed":
                raise ExplorationValidationError("raw_session_terminal_call_invalid")
            if not isinstance(raw, dict) or set(raw) != {"call_id", "id", "input", "name"}:
                raise ExplorationValidationError("raw_session_completed_call_unparseable")
            if params.get("sessionId") != receipt["session_id"] or update.get("toolCallId") != raw.get("id"):
                raise ExplorationValidationError("raw_session_call_identity_invalid")
            if raw["id"] in completed_call_ids:
                raise ExplorationValidationError("raw_session_duplicate_completed_call")
            completed_call_ids.add(raw["id"])
            metadata = params.get("_meta")
            if not isinstance(metadata, dict) or metadata.get("promptId") != receipt["request_id"]:
                raise ExplorationValidationError("raw_session_request_identity_invalid")
            try:
                arguments = _strict_json_loads(raw["input"])
            except (TypeError, ValueError) as exc:
                raise ExplorationValidationError("raw_session_tool_input_invalid") from exc
            observed_calls.append(
                {
                    "tool_call_id": raw["id"],
                    "provider_call_id": raw["call_id"],
                    "tool_name": raw["name"],
                    "arguments": arguments,
                    "line_number": line_number,
                }
            )
    comparable = [
        {key: item[key] for key in ("tool_call_id", "provider_call_id", "tool_name", "arguments")}
        for item in observed_calls
    ]
    if started_call_ids != completed_call_ids or comparable != calls:
        raise ExplorationValidationError("raw_session_tool_calls_mismatch")
    entries = list(root.iterdir())
    owner_only = stat.S_IMODE(root.stat().st_mode) & 0o077 == 0 and all(
        not path.is_symlink() and stat.S_IMODE(path.stat().st_mode) & 0o077 == 0 for path in entries
    )
    return {
        "status": "verified",
        "hashes_verified": True,
        "tool_calls_verified": True,
        "model_id_verified": True,
        "cli_version_verified": False,
        "tool_disable_flags_verified": False,
        "owner_only_permissions": owner_only,
    }


def _canonical_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value.endswith("Z"):
        return False
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return False
    return parsed.isoformat(timespec="seconds").replace("+00:00", "Z") == value


def _validate_candidate(candidate: Any) -> None:
    if not isinstance(candidate, dict):
        raise ExplorationValidationError("candidate_shape_invalid")
    required = {
        "handle",
        "profile_url",
        "platform_user_id",
        "bio_excerpt",
        "target_lab_affiliation_state",
        "pretraining_experience_state",
        "confidence",
        "evidence",
        "caveats",
    }
    if set(candidate) != required:
        raise ExplorationValidationError("candidate_keys_invalid")
    handle = candidate["handle"]
    profile_match = _PROFILE_URL_RE.fullmatch(str(candidate["profile_url"]))
    if (
        not isinstance(handle, str)
        or _HANDLE_RE.fullmatch(handle) is None
        or profile_match is None
        or profile_match.group("handle").casefold() != handle.casefold()
    ):
        raise ExplorationValidationError("candidate_profile_identity_invalid")
    platform_user_id = candidate["platform_user_id"]
    if platform_user_id is not None and (
        not isinstance(platform_user_id, str) or _PLATFORM_USER_ID_RE.fullmatch(platform_user_id) is None
    ):
        raise ExplorationValidationError("candidate_platform_user_id_invalid")
    bio = candidate["bio_excerpt"]
    if bio is not None and (not isinstance(bio, str) or not bio.strip() or len(bio) > 500):
        raise ExplorationValidationError("candidate_bio_invalid")
    if (
        candidate["target_lab_affiliation_state"] not in CANDIDATE_DIMENSION_STATES
        or candidate["pretraining_experience_state"] not in CANDIDATE_DIMENSION_STATES
        or candidate["confidence"] not in CONFIDENCE_STATES
    ):
        raise ExplorationValidationError("candidate_classification_invalid")
    evidence_items = candidate["evidence"]
    if not isinstance(evidence_items, list) or not evidence_items or len(evidence_items) > 24:
        raise ExplorationValidationError("candidate_evidence_invalid")
    evidence_ids: set[tuple[str, str | None, str]] = set()
    bio_evidence_count = 0
    for evidence in evidence_items:
        if not isinstance(evidence, dict) or set(evidence) != {
            "kind",
            "relationship",
            "author_handle",
            "post_id",
            "url",
            "published_at",
            "excerpt",
            "supports",
        }:
            raise ExplorationValidationError("evidence_shape_invalid")
        if evidence["kind"] not in EVIDENCE_KINDS or evidence["relationship"] not in ALLOWED_RELATIONSHIPS:
            raise ExplorationValidationError("evidence_classification_invalid")
        if (
            not isinstance(evidence["author_handle"], str)
            or _HANDLE_RE.fullmatch(evidence["author_handle"]) is None
            or not isinstance(evidence["excerpt"], str)
            or not evidence["excerpt"].strip()
            or len(evidence["excerpt"]) > 280
        ):
            raise ExplorationValidationError("evidence_text_invalid")
        supports = evidence["supports"]
        if (
            not isinstance(supports, list)
            or len(supports) > 2
            or len(supports) != len(set(supports))
            or any(value not in _CANDIDATE_DIMENSION_FIELDS for value in supports)
        ):
            raise ExplorationValidationError("evidence_supports_invalid")
        if evidence["kind"] == "bio":
            profile = _PROFILE_URL_RE.fullmatch(str(evidence["url"]))
            if (
                evidence["post_id"] is not None
                or evidence["published_at"] is not None
                or profile is None
                or evidence["relationship"] != "self"
                or profile.group("handle").casefold() != handle.casefold()
                or evidence["author_handle"].casefold() != handle.casefold()
            ):
                raise ExplorationValidationError("bio_source_invalid")
            bio_evidence_count += 1
        else:
            post = _POST_URL_RE.fullmatch(str(evidence["url"]))
            if (
                post is None
                or not isinstance(evidence["post_id"], str)
                or _POST_ID_RE.fullmatch(evidence["post_id"]) is None
                or post.group("post_id") != evidence["post_id"]
                or post.group("handle").casefold() != evidence["author_handle"].casefold()
                or not _canonical_timestamp(evidence["published_at"])
            ):
                raise ExplorationValidationError("post_source_invalid")
        if evidence["relationship"] == "self" and evidence["author_handle"].casefold() != handle.casefold():
            raise ExplorationValidationError("self_evidence_actor_mismatch")
        identity = (evidence["kind"], evidence["post_id"], evidence["url"])
        if identity in evidence_ids:
            raise ExplorationValidationError("duplicate_evidence")
        evidence_ids.add(identity)
    if (bio is None and bio_evidence_count != 0) or (bio is not None and bio_evidence_count != 1):
        raise ExplorationValidationError("candidate_bio_evidence_mismatch")
    if bio is not None:
        bio_evidence = next(item for item in evidence_items if item["kind"] == "bio")
        normalized_bio = "".join(
            character.casefold()
            for character in unicodedata.normalize("NFKC", bio.removeprefix("Observed Bio:"))
            if character.isalnum()
        )
        normalized_evidence = "".join(
            character.casefold()
            for character in unicodedata.normalize("NFKC", bio_evidence["excerpt"])
            if character.isalnum()
        )
        if len(normalized_evidence) < 8 or normalized_evidence not in normalized_bio:
            raise ExplorationValidationError("candidate_bio_text_binding_invalid")
    caveats = candidate["caveats"]
    if (
        not isinstance(caveats, list)
        or len(caveats) > 12
        or any(not isinstance(item, str) or not item.strip() or len(item) > 500 for item in caveats)
    ):
        raise ExplorationValidationError("candidate_caveats_invalid")


def _validate_result(result: Any, calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(result, dict) or set(result) != _RESULT_KEYS or result.get("status") not in SUPPORTED_STATUSES:
        raise ExplorationValidationError("exploration_result_invalid")
    if _protected_key_present(result):
        raise ExplorationValidationError("protected_identity_field_forbidden")
    provenance = result.get("native_x_tool_provenance")
    counts = result.get("counts")
    candidates = result.get("candidates")
    if (
        not isinstance(provenance, dict)
        or set(provenance)
        != {
            "generic_web_used",
            "tool_calls_reported",
            "tools_reported",
            "queries",
        }
        or not isinstance(counts, dict)
        or not isinstance(candidates, list)
    ):
        raise ExplorationValidationError("exploration_sections_invalid")
    observed_tool_names = sorted({call["tool_name"] for call in calls})

    def expected_query_label(call: Mapping[str, Any]) -> str:
        subject = _tool_subject(call["arguments"], call["tool_name"])
        if call["tool_name"] == "x_keyword_search":
            return f"{subject} [keyword/{call['arguments']['mode']}]"
        labels = {
            "x_semantic_search": "semantic",
            "x_user_search": "user_search",
            "x_thread_fetch": "thread_fetch",
        }
        return f"{subject} [{labels[call['tool_name']]}]"

    expected_reported_queries = Counter(expected_query_label(call) for call in calls)
    reported_queries = provenance.get("queries")
    if not isinstance(reported_queries, list) or any(
        not isinstance(value, str) or not value.strip() or len(value) > 2_000 for value in reported_queries
    ):
        raise ExplorationValidationError("model_tool_provenance_mismatch")
    normalized_reported_queries = Counter(reported_queries)
    tools_reported = provenance.get("tools_reported")
    if (
        not isinstance(tools_reported, list)
        or any(not isinstance(value, str) for value in tools_reported)
        or len(tools_reported) != len(set(tools_reported))
    ):
        raise ExplorationValidationError("model_tool_provenance_mismatch")
    if (
        provenance.get("generic_web_used") is not False
        or type(provenance.get("tool_calls_reported")) is not int
        or provenance.get("tool_calls_reported") != len(calls)
        or sorted(tools_reported) != observed_tool_names
        or normalized_reported_queries != expected_reported_queries
    ):
        raise ExplorationValidationError("model_tool_provenance_mismatch")
    expected_count_keys = {
        "observations_inspected_reported",
        "candidates_retained",
    }
    if (
        set(counts) != expected_count_keys
        or any(type(value) is not int or value < 0 for value in counts.values())
        or counts["observations_inspected_reported"] > 10_000
        or counts["observations_inspected_reported"] < counts["candidates_retained"]
    ):
        raise ExplorationValidationError("exploration_counts_invalid")
    if counts.get("candidates_retained") != len(candidates) or len(candidates) > 25:
        raise ExplorationValidationError("candidate_count_mismatch")
    handles: set[str] = set()
    for candidate in candidates:
        _validate_candidate(candidate)
        folded = candidate["handle"].casefold()
        if folded in handles:
            raise ExplorationValidationError("duplicate_candidate_handle")
        handles.add(folded)
    if result["status"] == "X_SEARCH_BLOCKED" and candidates:
        raise ExplorationValidationError("blocked_result_has_candidates")
    status_reason = result["status_reason"]
    limitations = result["limitations"]
    excluded = result["excluded_examples"]
    if not isinstance(status_reason, str) or not status_reason.strip() or len(status_reason) > 2_000:
        raise ExplorationValidationError("status_reason_invalid")
    if (
        not isinstance(limitations, list)
        or len(limitations) > 20
        or any(not isinstance(item, str) or not item.strip() or len(item) > 1_000 for item in limitations)
    ):
        raise ExplorationValidationError("limitations_invalid")
    if not isinstance(excluded, list) or len(excluded) > 50:
        raise ExplorationValidationError("excluded_examples_invalid")
    excluded_handles: set[str] = set()
    for item in excluded:
        if not isinstance(item, dict) or set(item) != {"handle", "reason"}:
            raise ExplorationValidationError("excluded_examples_invalid")
        item_handle = item["handle"]
        reason = item["reason"]
        if (
            not isinstance(item_handle, str)
            or _HANDLE_RE.fullmatch(item_handle) is None
            or item_handle.casefold() in excluded_handles
            or not isinstance(reason, str)
            or not reason.strip()
            or len(reason) > 1_000
        ):
            raise ExplorationValidationError("excluded_examples_invalid")
        excluded_handles.add(item_handle.casefold())
    if handles & excluded_handles:
        raise ExplorationValidationError("retained_and_excluded_handle_overlap")
    reconciliation = result["local_reconciliation"]
    expected_reconciliation = {
        "candidate_records_validated": len(candidates),
        "evidence_items_validated": sum(len(candidate["evidence"]) for candidate in candidates),
        "post_urls_structurally_validated": sum(
            evidence["kind"] != "bio" for candidate in candidates for evidence in candidate["evidence"]
        ),
        "provider_post_bodies_replayable": False,
        "tool_calls_completed": len(calls),
        "tool_counts": dict(Counter(call["tool_name"] for call in calls)),
    }
    if canonical_json(reconciliation) != canonical_json(expected_reconciliation):
        raise ExplorationValidationError("local_reconciliation_invalid")
    return candidates


def _candidate_segment(candidate: Mapping[str, Any], policy: Mapping[str, Any]) -> Mapping[str, Any]:
    for segment in policy["segments"]:
        if (
            candidate["target_lab_affiliation_state"] == segment["target_lab_affiliation_state"]
            and candidate["pretraining_experience_state"] == segment["pretraining_experience_state"]
        ):
            return segment
    return {
        "segment_id": policy["fallback_segment_id"],
        "priority_tier": policy["fallback_priority_tier"],
        "recall_pool_eligible": False,
    }


def _hydration_reasons(candidate: Mapping[str, Any], policy: Mapping[str, Any]) -> list[str]:
    hydration = policy["hydration"]
    reasons: list[str] = []
    if hydration["require_stable_platform_user_id"] and candidate["platform_user_id"] is None:
        reasons.append("missing_platform_user_id")
    if hydration["require_bio"] and candidate["bio_excerpt"] is None:
        reasons.append("missing_bio")
    if candidate["target_lab_affiliation_state"] in hydration["state_triggers"]:
        reasons.append("target_lab_affiliation_state_unresolved")
    if candidate["pretraining_experience_state"] in hydration["state_triggers"]:
        reasons.append("pretraining_experience_state_unresolved")
    for dimension in hydration["required_high_authority_support"]:
        if not _evidence_supports(candidate, dimension, high_authority_only=True):
            reasons.append(f"high_authority_{dimension}_evidence_missing")
    return reasons


def _round_robin_hydration_candidates(
    candidates: list[dict[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Order tiers strictly while interleaving equal-tier temporal segments."""

    segment_order = [segment["segment_id"] for segment in policy["segments"]] + [policy["fallback_segment_id"]]
    segment_rank = {segment_id: index for index, segment_id in enumerate(segment_order)}
    grouped: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for candidate in candidates:
        segment = _candidate_segment(candidate, policy)
        grouped.setdefault(segment["priority_tier"], {}).setdefault(segment["segment_id"], []).append(candidate)
    confidence_rank = {"high": 3, "medium": 2, "low": 1}
    for segment_groups in grouped.values():
        for queue in segment_groups.values():
            queue.sort(key=lambda item: (-confidence_rank[item["confidence"]], item["handle"].casefold()))

    ordered: list[dict[str, Any]] = []
    for tier in sorted(grouped, key=lambda item: -policy["priority_tiers"][item]):
        segment_groups = grouped[tier]
        tier_segments = sorted(segment_groups, key=segment_rank.__getitem__)
        cursor = 0
        while any(cursor < len(segment_groups[segment_id]) for segment_id in tier_segments):
            for segment_id in tier_segments:
                queue = segment_groups[segment_id]
                if cursor < len(queue):
                    ordered.append(queue[cursor])
            cursor += 1
    return ordered


def _validate_experiment_binding(
    experiment_binding: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    if (
        not isinstance(experiment_binding, Mapping)
        or set(experiment_binding) != _EXPERIMENT_BINDING_KEYS
        or any(not isinstance(value, str) or not value for value in experiment_binding.values())
        or _LAB_ID_RE.fullmatch(experiment_binding["lab_id"]) is None
        or not _valid_session_id(experiment_binding["session_id"])
        or not _valid_request_id(experiment_binding["request_id"])
        or _MODEL_ID_RE.fullmatch(experiment_binding["model_id"]) is None
        or experiment_binding["tool_policy_version"] != SUPPORTED_RECEIPT_VERSION
        or any(
            _SHA256_RE.fullmatch(experiment_binding[field]) is None
            for field in (
                "query_policy_sha256",
                "legacy_full_query_policy_sha256",
                "query_policy_registry_sha256",
                "query_policy_registry_row_sha256",
                "candidate_value_policy_sha256",
            )
        )
    ):
        raise ExplorationValidationError("experiment_binding_invalid")
    approved_record, query_policy, approval_binding = _approved_query_policy_record(
        experiment_binding,
        query_policy_version=experiment_binding["query_policy_version"],
        query_policy_registry_version=experiment_binding["query_policy_registry_version"],
    )
    validated_query_policy, query_policy_sha256, _ = _validate_query_policy_body(
        query_policy,
        approved_record,
        experiment_binding,
    )
    candidate_value_policy, candidate_value_policy_sha256 = _load_candidate_value_policy()
    if (
        experiment_binding["lab_id"] != approved_record["lab_id"]
        or experiment_binding["query_policy_sha256"] != query_policy_sha256
        or experiment_binding["legacy_full_query_policy_sha256"]
        != validated_query_policy["legacy_full_policy_sha256"]
        or experiment_binding["query_policy_registry_version"]
        != approval_binding["query_policy_registry_version"]
        or experiment_binding["query_policy_registry_sha256"]
        != approval_binding["query_policy_registry_sha256"]
        or experiment_binding["query_policy_registry_row_sha256"]
        != approval_binding["query_policy_registry_row_sha256"]
        or experiment_binding["candidate_value_policy_version"] != candidate_value_policy["policy_version"]
        or experiment_binding["candidate_value_policy_sha256"] != candidate_value_policy_sha256
    ):
        raise ExplorationValidationError("experiment_binding_invalid")
    return validated_query_policy, candidate_value_policy


def _validate_hydration_task_output(
    task: Any,
    *,
    expected_binding: Mapping[str, Any] | None = None,
    expected_candidate: Mapping[str, Any] | None = None,
) -> None:
    if not isinstance(task, dict) or set(task) != _HYDRATION_TASK_KEYS:
        raise ExplorationValidationError("hydration_task_schema_invalid")
    binding = task.get("experiment_binding")
    if not isinstance(binding, dict):
        raise ExplorationValidationError("hydration_task_binding_invalid")
    try:
        _validate_experiment_binding(binding)
    except ExplorationValidationError as exc:
        raise ExplorationValidationError("hydration_task_binding_invalid") from exc
    if expected_binding is not None and canonical_json(binding) != canonical_json(expected_binding):
        raise ExplorationValidationError("hydration_task_binding_invalid")

    handle = task.get("handle")
    profile_url = task.get("profile_url")
    candidate_state_binding = task.get("candidate_state_binding")
    reasons = task.get("reasons")
    requested_tools = task.get("requested_tools")
    seed_post_urls = task.get("seed_post_urls")
    required_fields = task.get("required_fields")
    if (
        task.get("task_version") != HYDRATION_TASK_VERSION
        or task.get("task_status") != "planned"
        or not isinstance(task.get("task_key"), str)
        or re.fullmatch(r"xhydrate_[0-9a-f]{24}", task["task_key"]) is None
        or not isinstance(handle, str)
        or _HANDLE_RE.fullmatch(handle) is None
        or profile_url != f"https://x.com/{handle}"
        or task.get("candidate_value_segment") not in _VALUE_SEGMENT_IDS
        or not isinstance(reasons, list)
        or not reasons
        or len(reasons) > len(_HYDRATION_REASON_VALUES)
        or any(not isinstance(reason, str) for reason in reasons)
        or len(set(reasons)) != len(reasons)
        or any(reason not in _HYDRATION_REASON_VALUES for reason in reasons)
        or reasons != sorted(reasons, key=_HYDRATION_REASON_ORDER.index)
        or task.get("execution_authorized") is not False
    ):
        raise ExplorationValidationError("hydration_task_value_invalid")
    if not isinstance(candidate_state_binding, dict) or set(candidate_state_binding) != _CANDIDATE_STATE_BINDING_KEYS:
        raise ExplorationValidationError("hydration_task_candidate_binding_invalid")
    lab_state = candidate_state_binding.get("target_lab_affiliation_state")
    pretraining_state = candidate_state_binding.get("pretraining_experience_state")
    expected_segment = next(
        (
            segment_id
            for segment_id, segment_binding in _EXPECTED_SEGMENT_BINDINGS.items()
            if segment_binding[:2] == (lab_state, pretraining_state)
        ),
        "needs_evidence",
    )
    if (
        not isinstance(candidate_state_binding.get("candidate_sha256"), str)
        or _SHA256_RE.fullmatch(candidate_state_binding["candidate_sha256"]) is None
        or lab_state not in CANDIDATE_DIMENSION_STATES
        or pretraining_state not in CANDIDATE_DIMENSION_STATES
        or candidate_state_binding.get("candidate_value_segment") != expected_segment
        or task["candidate_value_segment"] != expected_segment
    ):
        raise ExplorationValidationError("hydration_task_candidate_binding_invalid")
    if expected_candidate is not None and (
        candidate_state_binding["candidate_sha256"] != canonical_sha256(expected_candidate)
        or lab_state != expected_candidate.get("target_lab_affiliation_state")
        or pretraining_state != expected_candidate.get("pretraining_experience_state")
        or handle != expected_candidate.get("handle")
        or profile_url != expected_candidate.get("profile_url")
    ):
        raise ExplorationValidationError("hydration_task_candidate_binding_invalid")
    if requested_tools not in (["x_user_search"], ["x_user_search", "x_thread_fetch"]):
        raise ExplorationValidationError("hydration_task_tools_invalid")
    if (
        not isinstance(seed_post_urls, list)
        or len(seed_post_urls) != (1 if "x_thread_fetch" in requested_tools else 0)
        or any(not isinstance(url, str) or _POST_URL_RE.fullmatch(url) is None for url in seed_post_urls)
        or type(task.get("max_tool_calls")) is not int
        or task.get("max_tool_calls") != len(requested_tools)
        or type(task.get("max_posts")) is not int
        or task.get("max_posts") != (5 if seed_post_urls else 0)
    ):
        raise ExplorationValidationError("hydration_task_tools_invalid")
    base_fields = ["platform_user_id", "current_handle", "bio_text", "bio_observed_at"]
    post_fields = [
        "canonical_post_id",
        "canonical_post_url",
        "post_author_handle",
        "post_authored_at",
        "bounded_excerpt",
    ]
    if required_fields != base_fields + (post_fields if seed_post_urls else []):
        raise ExplorationValidationError("hydration_task_required_fields_invalid")
    identity = {
        "candidate_state_binding": candidate_state_binding,
        "handle": handle.casefold(),
        "experiment_binding": dict(binding),
        "post_urls": seed_post_urls,
        "reasons": reasons,
        "task_status": task["task_status"],
        "task_version": HYDRATION_TASK_VERSION,
    }
    expected_task_key = f"xhydrate_{hashlib.sha256(canonical_json(identity).encode()).hexdigest()[:24]}"
    if task["task_key"] != expected_task_key:
        raise ExplorationValidationError("hydration_task_key_invalid")


def validate_hydration_task(task: Any) -> None:
    """Fail closed unless one persisted hydration task matches the v0 contract."""

    _validate_hydration_task_output(task)


def _valid_optional_rate(value: Any) -> bool:
    return value is None or (type(value) in {int, float} and 0.0 <= value <= 1.0)


def _validate_evaluation_output_shape(evaluation: Any) -> None:
    """Fail closed on the closed-world shape and internal invariants."""

    if not isinstance(evaluation, dict) or set(evaluation) != _EVALUATION_KEYS:
        raise ExplorationValidationError("evaluation_output_schema_invalid")
    if (
        evaluation.get("schema_version") != EVALUATION_SCHEMA_VERSION
        or evaluation.get("status") != "evaluated"
        or evaluation.get("native_x_call_proof") not in {"receipt_only_unverified", "session_hash_and_calls_verified"}
        or evaluation.get("candidate_search_feasibility")
        not in {
            "receipt_only_model_mediated_result_unverified",
            "model_mediated_precision_tranche_lead_demonstrated",
            "model_mediated_recall_pool_leads_observed",
            "model_mediated_candidate_needs_evidence",
            "model_mediated_lead_not_demonstrated",
        }
        or evaluation.get("researcher_role_function_feasibility") != "not_evaluated"
        or evaluation.get("source_replayability") != "tool_and_query_only"
        or evaluation.get("scale_verdict") not in {"no_go", "eligible_for_independent_review"}
        or evaluation.get("next_phase") not in {"candidate_hydration", "manual_adjudication"}
    ):
        raise ExplorationValidationError("evaluation_output_value_invalid")

    input_binding = evaluation.get("input_binding")
    if not isinstance(input_binding, dict) or set(input_binding) != _EVALUATION_INPUT_BINDING_KEYS:
        raise ExplorationValidationError("evaluation_input_binding_invalid")
    if (
        not _valid_session_id(input_binding.get("session_id"))
        or not _valid_request_id(input_binding.get("request_id"))
        or _MODEL_ID_RE.fullmatch(str(input_binding.get("model_id"))) is None
        or input_binding.get("tool_policy_version") != SUPPORTED_RECEIPT_VERSION
        or input_binding.get("query_policy_schema_version") != QUERY_POLICY_SCHEMA_VERSION
        or input_binding.get("query_policy_registry_schema_version") != QUERY_POLICY_REGISTRY_SCHEMA_VERSION
        or input_binding.get("candidate_value_policy_schema_version") != CANDIDATE_VALUE_POLICY_SCHEMA_VERSION
        or input_binding.get("query_policy_purpose") != "base_researcher_discovery"
        or input_binding.get("query_policy_allowed_decision_dimensions") != _BASE_DISCOVERY_DIMENSIONS
        or input_binding.get("result_to_receipt")
        != "exact_tool_query_reconciliation_model_mediated_result"
        or _LAB_ID_RE.fullmatch(str(input_binding.get("lab_id"))) is None
        or any(
            not isinstance(input_binding.get(field), str)
            or _SHA256_RE.fullmatch(input_binding[field]) is None
            for field in (
                "result_sha256",
                "receipt_sha256",
                "query_policy_sha256",
                "legacy_full_query_policy_sha256",
                "query_policy_registry_sha256",
                "query_policy_registry_row_sha256",
                "candidate_value_policy_sha256",
            )
        )
        or any(
            not isinstance(input_binding.get(field), str)
            or _POLICY_VERSION_RE.fullmatch(input_binding[field]) is None
            for field in (
                "query_policy_version",
                "query_policy_registry_version",
                "candidate_value_policy_version",
            )
        )
    ):
        raise ExplorationValidationError("evaluation_input_binding_invalid")
    raw_session = input_binding.get("raw_session")
    if (
        not isinstance(raw_session, dict)
        or set(raw_session) != _RAW_SESSION_BINDING_KEYS
        or raw_session.get("status") not in {"not_supplied", "verified"}
        or any(
            type(raw_session.get(field)) is not bool
            for field in _RAW_SESSION_BINDING_KEYS - {"status", "owner_only_permissions"}
        )
        or not (
            raw_session.get("owner_only_permissions") is None
            or type(raw_session.get("owner_only_permissions")) is bool
        )
    ):
        raise ExplorationValidationError("evaluation_raw_session_binding_invalid")
    if raw_session["status"] == "not_supplied":
        if (
            any(
                raw_session[field] is not False
                for field in _RAW_SESSION_BINDING_KEYS - {"status", "owner_only_permissions"}
            )
            or raw_session["owner_only_permissions"] is not None
            or evaluation["native_x_call_proof"] != "receipt_only_unverified"
        ):
            raise ExplorationValidationError("evaluation_raw_session_binding_invalid")
    elif (
        any(raw_session[field] is not True for field in ("hashes_verified", "tool_calls_verified", "model_id_verified"))
        or evaluation["native_x_call_proof"] != "session_hash_and_calls_verified"
    ):
        raise ExplorationValidationError("evaluation_raw_session_binding_invalid")

    tool_counts = evaluation.get("tool_counts")
    if (
        not isinstance(tool_counts, dict)
        or not tool_counts
        or set(tool_counts) - ALLOWED_TOOL_NAMES
        or any(type(value) is not int or value < 1 for value in tool_counts.values())
        or not 1 <= sum(tool_counts.values()) <= 32
    ):
        raise ExplorationValidationError("evaluation_tool_counts_invalid")
    assessments = evaluation.get("candidate_value_assessments")
    if not isinstance(assessments, list) or len(assessments) > 25:
        raise ExplorationValidationError("evaluation_assessments_invalid")
    seen_handles: set[str] = set()
    segment_counts: Counter[str] = Counter()
    for assessment in assessments:
        if not isinstance(assessment, dict) or set(assessment) != _ASSESSMENT_KEYS:
            raise ExplorationValidationError("evaluation_assessments_invalid")
        handle = assessment.get("handle")
        lab_state = assessment.get("target_lab_affiliation_state")
        pretraining_state = assessment.get("pretraining_experience_state")
        segment_id = assessment.get("candidate_value_segment")
        reasons = assessment.get("hydration_reasons")
        expected_segment = next(
            (
                candidate_segment_id
                for candidate_segment_id, binding in _EXPECTED_SEGMENT_BINDINGS.items()
                if binding[:2] == (lab_state, pretraining_state)
            ),
            "needs_evidence",
        )
        target_unresolved = "target_lab_affiliation_state_unresolved" in reasons if isinstance(reasons, list) else False
        pretraining_unresolved = (
            "pretraining_experience_state_unresolved" in reasons if isinstance(reasons, list) else False
        )
        high_authority_missing = (
            any(
                reason
                in {
                    "high_authority_target_lab_affiliation_state_evidence_missing",
                    "high_authority_pretraining_experience_state_evidence_missing",
                }
                for reason in reasons
            )
            if isinstance(reasons, list) and all(isinstance(reason, str) for reason in reasons)
            else False
        )
        if (
            not isinstance(handle, str)
            or _HANDLE_RE.fullmatch(handle) is None
            or handle.casefold() in seen_handles
            or lab_state not in CANDIDATE_DIMENSION_STATES
            or pretraining_state not in CANDIDATE_DIMENSION_STATES
            or segment_id != expected_segment
            or any(
                type(assessment.get(field)) is not bool
                for field in (
                    "recall_pool_eligible",
                    "precision_tranche_eligible",
                    "high_authority_evidence_complete",
                    "hydration_required",
                )
            )
            or assessment["recall_pool_eligible"] is (segment_id == "needs_evidence")
            or (
                assessment["precision_tranche_eligible"]
                and segment_id != "precision_current_current"
            )
            or not isinstance(reasons, list)
            or any(not isinstance(reason, str) for reason in reasons)
            or len(set(reasons)) != len(reasons)
            or any(reason not in _HYDRATION_REASON_VALUES for reason in reasons)
            or reasons != sorted(reasons, key=_HYDRATION_REASON_ORDER.index)
            or assessment["hydration_required"] != bool(reasons)
            or target_unresolved != (lab_state in {"ambiguous", "unsupported"})
            or pretraining_unresolved != (pretraining_state in {"ambiguous", "unsupported"})
            or assessment["high_authority_evidence_complete"] == high_authority_missing
            or (
                assessment["precision_tranche_eligible"]
                and (
                    assessment["high_authority_evidence_complete"] is not True
                    or assessment["hydration_required"] is not False
                )
            )
        ):
            raise ExplorationValidationError("evaluation_assessments_invalid")
        seen_handles.add(handle.casefold())
        segment_counts[segment_id] += 1

    metrics = evaluation.get("metrics")
    ordered_segment_ids = [*_EXPECTED_SEGMENT_BINDINGS, "needs_evidence"]
    if not isinstance(metrics, dict) or set(metrics) != _METRIC_KEYS:
        raise ExplorationValidationError("evaluation_metrics_invalid")
    integer_metrics = {
        "tool_call_receipt_rows",
        "candidates_retained",
        "post_evidence_records",
        "model_reported_observations",
        "precision_tranche_candidates",
        "recall_pool_candidates",
    }
    if (
        any(type(metrics.get(field)) is not int or metrics[field] < 0 for field in integer_metrics)
        or not 1 <= metrics["tool_call_receipt_rows"] <= 32
        or metrics["tool_call_receipt_rows"] != sum(tool_counts.values())
        or metrics["candidates_retained"] != len(assessments)
        or metrics["precision_tranche_candidates"]
        != sum(assessment["precision_tranche_eligible"] for assessment in assessments)
        or metrics["recall_pool_candidates"]
        != sum(assessment["recall_pool_eligible"] for assessment in assessments)
        or metrics.get("observations_mechanically_replayable") is not False
        or not isinstance(metrics.get("candidate_value_segment_counts"), dict)
        or set(metrics["candidate_value_segment_counts"]) != set(ordered_segment_ids)
        or any(
            type(metrics["candidate_value_segment_counts"][segment_id]) is not int
            or metrics["candidate_value_segment_counts"][segment_id] != segment_counts[segment_id]
            for segment_id in ordered_segment_ids
        )
        or not (
            metrics.get("candidates_per_tool_call") is None
            or (
                type(metrics.get("candidates_per_tool_call")) in {int, float}
                and metrics["candidates_per_tool_call"] >= 0
            )
        )
        or metrics.get("candidates_per_tool_call")
        != _fraction(metrics["candidates_retained"], metrics["tool_call_receipt_rows"])
        or any(
            not _valid_optional_rate(metrics.get(field))
            for field in (
                "model_mediated_bio_presence_rate",
                "model_mediated_platform_user_id_presence_rate",
                "model_mediated_precision_tranche_rate",
                "model_mediated_recall_pool_rate",
                "model_mediated_high_authority_support_coverage",
                "third_party_only_candidate_rate",
                "replayable_provider_post_body_rate",
            )
        )
        or metrics["model_mediated_precision_tranche_rate"]
        != _fraction(metrics["precision_tranche_candidates"], metrics["candidates_retained"])
        or metrics["model_mediated_recall_pool_rate"]
        != _fraction(metrics["recall_pool_candidates"], metrics["candidates_retained"])
        or metrics["model_mediated_high_authority_support_coverage"]
        != _fraction(
            sum(assessment["high_authority_evidence_complete"] for assessment in assessments),
            metrics["candidates_retained"],
        )
        or metrics["model_reported_observations"] < metrics["candidates_retained"]
        or metrics["replayable_provider_post_body_rate"]
        != (0.0 if metrics["post_evidence_records"] else None)
        or not (
            metrics.get("session_verified_completed_tool_calls") is None
            or (
                type(metrics.get("session_verified_completed_tool_calls")) is int
                and metrics["session_verified_completed_tool_calls"] == metrics["tool_call_receipt_rows"]
            )
        )
        or (
            evaluation["native_x_call_proof"] == "session_hash_and_calls_verified"
        )
        != (metrics["session_verified_completed_tool_calls"] == metrics["tool_call_receipt_rows"])
    ):
        raise ExplorationValidationError("evaluation_metrics_invalid")

    gaps = evaluation.get("first_field_gate_gaps")
    if (
        not isinstance(gaps, dict)
        or set(gaps)
        != {
            "bio_coverage_gap_to_100_percent",
            "stable_id_coverage_gap_to_100_percent",
            "evidence_complete_gap_to_90_percent",
            "provider_post_body_replayability_gap_to_100_percent",
        }
        or any(not _valid_optional_rate(value) for value in gaps.values())
    ):
        raise ExplorationValidationError("evaluation_gate_gaps_invalid")
    expected_gaps = {
        "bio_coverage_gap_to_100_percent": None
        if metrics["model_mediated_bio_presence_rate"] is None
        else round(1.0 - metrics["model_mediated_bio_presence_rate"], 6),
        "stable_id_coverage_gap_to_100_percent": None
        if metrics["model_mediated_platform_user_id_presence_rate"] is None
        else round(1.0 - metrics["model_mediated_platform_user_id_presence_rate"], 6),
        "evidence_complete_gap_to_90_percent": None
        if metrics["model_mediated_high_authority_support_coverage"] is None
        else round(max(0.0, 0.9 - metrics["model_mediated_high_authority_support_coverage"]), 6),
        "provider_post_body_replayability_gap_to_100_percent": 1.0
        if metrics["post_evidence_records"]
        else None,
    }
    if canonical_json(gaps) != canonical_json(expected_gaps):
        raise ExplorationValidationError("evaluation_gate_gaps_invalid")

    if evaluation["native_x_call_proof"] == "receipt_only_unverified":
        expected_feasibility = "receipt_only_model_mediated_result_unverified"
    elif metrics["precision_tranche_candidates"]:
        expected_feasibility = "model_mediated_precision_tranche_lead_demonstrated"
    elif metrics["recall_pool_candidates"]:
        expected_feasibility = "model_mediated_recall_pool_leads_observed"
    elif assessments:
        expected_feasibility = "model_mediated_candidate_needs_evidence"
    else:
        expected_feasibility = "model_mediated_lead_not_demonstrated"
    if evaluation["candidate_search_feasibility"] != expected_feasibility:
        raise ExplorationValidationError("evaluation_feasibility_invalid")

    scale_blockers = evaluation.get("scale_blockers")
    if (
        not isinstance(scale_blockers, list)
        or any(not isinstance(blocker, str) for blocker in scale_blockers)
        or len(scale_blockers) != len(set(scale_blockers))
        or any(not isinstance(blocker, str) or not blocker for blocker in scale_blockers)
    ):
        raise ExplorationValidationError("evaluation_scale_blockers_invalid")
    expected_blockers = [
        "provider_post_bodies_not_replayable",
        "candidate_role_function_not_structured",
        "bio_snapshot_not_source_bound",
        "stable_platform_user_id_not_source_bound",
    ]
    if metrics["model_mediated_platform_user_id_presence_rate"] != 1.0:
        expected_blockers.append("stable_platform_user_id_coverage_below_100_percent")
    if metrics["model_mediated_bio_presence_rate"] != 1.0:
        expected_blockers.append("bio_coverage_below_100_percent")
    if raw_session["owner_only_permissions"] is not True:
        expected_blockers.append("raw_session_not_owner_only")
    if raw_session["cli_version_verified"] is not True:
        expected_blockers.append("cli_binary_version_not_bound_to_raw_session")
    if raw_session["tool_disable_flags_verified"] is not True:
        expected_blockers.append("tool_disable_flags_receipt_only")
    if scale_blockers != expected_blockers or evaluation["scale_verdict"] != (
        "no_go" if expected_blockers else "eligible_for_independent_review"
    ):
        raise ExplorationValidationError("evaluation_scale_blockers_invalid")
    authority = evaluation.get("authority")
    if (
        not isinstance(authority, dict)
        or set(authority)
        != {
            "formal_gate_eligible",
            "canonical_person_or_employment_confirmed",
            "discovery_scale_authorized",
            "product_write_authorized",
            "outreach_authorized",
        }
        or any(value is not False for value in authority.values())
    ):
        raise ExplorationValidationError("evaluation_authority_invalid")

    experiment_binding = {
        "lab_id": input_binding["lab_id"],
        "session_id": input_binding["session_id"],
        "request_id": input_binding["request_id"],
        "model_id": input_binding["model_id"],
        "tool_policy_version": input_binding["tool_policy_version"],
        "query_policy_version": input_binding["query_policy_version"],
        "query_policy_sha256": input_binding["query_policy_sha256"],
        "legacy_full_query_policy_sha256": input_binding["legacy_full_query_policy_sha256"],
        "query_policy_registry_version": input_binding["query_policy_registry_version"],
        "query_policy_registry_sha256": input_binding["query_policy_registry_sha256"],
        "query_policy_registry_row_sha256": input_binding["query_policy_registry_row_sha256"],
        "candidate_value_policy_version": input_binding["candidate_value_policy_version"],
        "candidate_value_policy_sha256": input_binding["candidate_value_policy_sha256"],
    }
    try:
        _validate_experiment_binding(experiment_binding)
    except ExplorationValidationError as exc:
        raise ExplorationValidationError("evaluation_input_binding_invalid") from exc
    hydration_tasks = evaluation.get("hydration_tasks")
    if not isinstance(hydration_tasks, list) or len(hydration_tasks) > 5:
        raise ExplorationValidationError("evaluation_hydration_tasks_invalid")
    assessments_by_handle = {assessment["handle"].casefold(): assessment for assessment in assessments}
    seen_task_handles: set[str] = set()
    seen_task_keys: set[str] = set()
    for task in hydration_tasks:
        _validate_hydration_task_output(task, expected_binding=experiment_binding)
        task_handle = task["handle"].casefold()
        task_key = task["task_key"]
        assessment = assessments_by_handle.get(task_handle)
        if (
            task_handle in seen_task_handles
            or task_key in seen_task_keys
            or assessment is None
            or assessment["hydration_required"] is not True
            or task["candidate_value_segment"] != assessment["candidate_value_segment"]
            or task["candidate_state_binding"]["target_lab_affiliation_state"]
            != assessment["target_lab_affiliation_state"]
            or task["candidate_state_binding"]["pretraining_experience_state"]
            != assessment["pretraining_experience_state"]
            or task["candidate_state_binding"]["candidate_value_segment"]
            != assessment["candidate_value_segment"]
            or task["reasons"] != assessment["hydration_reasons"]
        ):
            raise ExplorationValidationError("evaluation_hydration_tasks_invalid")
        seen_task_handles.add(task_handle)
        seen_task_keys.add(task_key)
    if evaluation["next_phase"] != ("candidate_hydration" if hydration_tasks else "manual_adjudication"):
        raise ExplorationValidationError("evaluation_next_phase_invalid")


def build_hydration_tasks(
    candidates: list[dict[str, Any]],
    *,
    experiment_binding: Mapping[str, str],
    maximum: int = 5,
) -> list[dict[str, Any]]:
    if type(maximum) is not int or not 1 <= maximum <= 10:
        raise ExplorationValidationError("hydration_task_limit_invalid")
    try:
        _, candidate_value_policy = _validate_experiment_binding(experiment_binding)
    except ExplorationValidationError as exc:
        raise ExplorationValidationError("hydration_experiment_binding_invalid") from exc
    for candidate in candidates:
        _validate_candidate(candidate)
    eligible = [candidate for candidate in candidates if _hydration_reasons(candidate, candidate_value_policy)]
    tasks: list[dict[str, Any]] = []
    for candidate in _round_robin_hydration_candidates(eligible, candidate_value_policy)[:maximum]:
        reasons = _hydration_reasons(candidate, candidate_value_policy)
        segment_id = _candidate_segment(candidate, candidate_value_policy)["segment_id"]
        candidate_state_binding = {
            "candidate_sha256": canonical_sha256(candidate),
            "target_lab_affiliation_state": candidate["target_lab_affiliation_state"],
            "pretraining_experience_state": candidate["pretraining_experience_state"],
            "candidate_value_segment": segment_id,
        }
        missing_dimensions = {
            "target_lab_affiliation_state"
            for reason in reasons
            if reason
            in {
                "target_lab_affiliation_state_unresolved",
                "high_authority_target_lab_affiliation_state_evidence_missing",
            }
        } | {
            "pretraining_experience_state"
            for reason in reasons
            if reason
            in {
                "pretraining_experience_state_unresolved",
                "high_authority_pretraining_experience_state_evidence_missing",
            }
        }
        post_candidates = [
            evidence for evidence in candidate["evidence"] if evidence["kind"] in {"post", "mention", "thread"}
        ]
        post_candidates.sort(
            key=lambda evidence: (
                -len(missing_dimensions & set(evidence["supports"])),
                evidence["relationship"] not in HIGH_AUTHORITY_RELATIONSHIPS,
                evidence["url"],
            )
        )
        post_urls = [post_candidates[0]["url"]] if post_candidates else []
        requested_tools = ["x_user_search"] + (["x_thread_fetch"] if post_urls else [])
        identity = {
            "candidate_state_binding": candidate_state_binding,
            "handle": candidate["handle"].casefold(),
            "experiment_binding": dict(experiment_binding),
            "post_urls": post_urls,
            "reasons": reasons,
            "task_status": "planned",
            "task_version": HYDRATION_TASK_VERSION,
        }
        required_fields = ["platform_user_id", "current_handle", "bio_text", "bio_observed_at"]
        if post_urls:
            required_fields.extend(
                [
                    "canonical_post_id",
                    "canonical_post_url",
                    "post_author_handle",
                    "post_authored_at",
                    "bounded_excerpt",
                ]
            )
        task = {
                "task_version": HYDRATION_TASK_VERSION,
                "task_status": "planned",
                "task_key": f"xhydrate_{hashlib.sha256(canonical_json(identity).encode()).hexdigest()[:24]}",
                "experiment_binding": dict(experiment_binding),
                "candidate_state_binding": candidate_state_binding,
                "handle": candidate["handle"],
                "profile_url": candidate["profile_url"],
                "candidate_value_segment": segment_id,
                "reasons": reasons,
                "requested_tools": requested_tools,
                "seed_post_urls": post_urls,
                "max_tool_calls": len(requested_tools),
                "max_posts": 5 if post_urls else 0,
                "required_fields": required_fields,
                "execution_authorized": False,
            }
        _validate_hydration_task_output(
            task,
            expected_binding=experiment_binding,
            expected_candidate=candidate,
        )
        tasks.append(task)
    return tasks


def evaluate_exploration(
    result: Any,
    receipt: Any,
    *,
    raw_session_directory: Path | None = None,
    query_policy_version: str | None = None,
    query_policy_registry_version: str | None = None,
) -> dict[str, Any]:
    """Validate one exploration and recompute decision-relevant diagnostics."""

    calls = _validate_receipt(receipt)
    if query_policy_version is not None and (
        not isinstance(query_policy_version, str) or _POLICY_VERSION_RE.fullmatch(query_policy_version) is None
    ):
        raise ExplorationValidationError("query_policy_selector_invalid")
    if query_policy_registry_version is not None and (
        not isinstance(query_policy_registry_version, str)
        or _POLICY_VERSION_RE.fullmatch(query_policy_registry_version) is None
    ):
        raise ExplorationValidationError("query_policy_registry_selector_invalid")
    approved_record, query_policy, approval_binding = _approved_query_policy_record(
        receipt,
        query_policy_version=query_policy_version,
        query_policy_registry_version=query_policy_registry_version,
    )
    validated_query_policy, query_policy_sha256 = _validate_query_policy(
        query_policy,
        approved_record,
        receipt,
        calls,
    )
    candidate_value_policy, candidate_value_policy_sha256 = _load_candidate_value_policy()
    raw_session = _raw_session_verification(receipt, calls, raw_session_directory)
    candidates = _validate_result(result, calls)
    total = len(candidates)
    evidence = [item for candidate in candidates for item in candidate["evidence"]]
    post_evidence = [item for item in evidence if item["kind"] != "bio"]
    candidate_value_assessments: list[dict[str, Any]] = []
    segment_counts: Counter[str] = Counter()
    precision_rule = candidate_value_policy["precision_tranche"]
    for candidate in candidates:
        segment = _candidate_segment(candidate, candidate_value_policy)
        segment_id = segment["segment_id"]
        segment_counts[segment_id] += 1
        high_authority_complete = all(
            _evidence_supports(candidate, dimension, high_authority_only=True)
            for dimension in precision_rule["required_high_authority_support"]
        )
        precision_eligible = (
            segment_id == precision_rule["required_segment_id"]
            and candidate["confidence"] == precision_rule["required_confidence"]
            and high_authority_complete
            and (not precision_rule["require_stable_platform_user_id"] or candidate["platform_user_id"] is not None)
            and (not precision_rule["require_bio"] or candidate["bio_excerpt"] is not None)
        )
        hydration_reasons = _hydration_reasons(candidate, candidate_value_policy)
        candidate_value_assessments.append(
            {
                "handle": candidate["handle"],
                "target_lab_affiliation_state": candidate["target_lab_affiliation_state"],
                "pretraining_experience_state": candidate["pretraining_experience_state"],
                "candidate_value_segment": segment_id,
                "recall_pool_eligible": segment["recall_pool_eligible"],
                "precision_tranche_eligible": precision_eligible,
                "high_authority_evidence_complete": high_authority_complete,
                "hydration_required": bool(hydration_reasons),
                "hydration_reasons": hydration_reasons,
            }
        )
    precision_candidates = [
        assessment for assessment in candidate_value_assessments if assessment["precision_tranche_eligible"]
    ]
    recall_candidates = [assessment for assessment in candidate_value_assessments if assessment["recall_pool_eligible"]]
    evidence_complete = [
        candidate
        for candidate in candidates
        if all(
            _evidence_supports(candidate, dimension, high_authority_only=True)
            for dimension in _CANDIDATE_DIMENSION_FIELDS
        )
    ]
    third_party_only = [
        candidate
        for candidate in candidates
        if candidate["evidence"]
        and all(item["relationship"] in {"third_party", "historical"} for item in candidate["evidence"])
    ]
    ordered_segment_ids = [segment["segment_id"] for segment in candidate_value_policy["segments"]] + [
        candidate_value_policy["fallback_segment_id"]
    ]
    metrics = {
        "tool_call_receipt_rows": len(calls),
        "session_verified_completed_tool_calls": len(calls) if raw_session["tool_calls_verified"] else None,
        "candidates_retained": total,
        "candidates_per_tool_call": _fraction(total, len(calls)),
        "model_mediated_bio_presence_rate": _fraction(
            sum(candidate["bio_excerpt"] is not None for candidate in candidates), total
        ),
        "model_mediated_platform_user_id_presence_rate": _fraction(
            sum(candidate["platform_user_id"] is not None for candidate in candidates), total
        ),
        "model_mediated_precision_tranche_rate": _fraction(len(precision_candidates), total),
        "model_mediated_recall_pool_rate": _fraction(len(recall_candidates), total),
        "model_mediated_high_authority_support_coverage": _fraction(len(evidence_complete), total),
        "third_party_only_candidate_rate": _fraction(len(third_party_only), total),
        "post_evidence_records": len(post_evidence),
        "replayable_provider_post_body_rate": 0.0 if post_evidence else None,
        "model_reported_observations": result["counts"].get("observations_inspected_reported"),
        "candidate_value_segment_counts": {
            segment_id: segment_counts[segment_id] for segment_id in ordered_segment_ids
        },
        "precision_tranche_candidates": len(precision_candidates),
        "recall_pool_candidates": len(recall_candidates),
        "observations_mechanically_replayable": False,
    }
    experiment_binding = {
        "lab_id": validated_query_policy["lab_id"],
        "session_id": receipt["session_id"],
        "request_id": receipt["request_id"],
        "model_id": receipt["model_id"],
        "tool_policy_version": receipt["schema_version"],
        "query_policy_version": validated_query_policy["policy_version"],
        "query_policy_sha256": query_policy_sha256,
        "legacy_full_query_policy_sha256": validated_query_policy["legacy_full_policy_sha256"],
        "query_policy_registry_version": approval_binding["query_policy_registry_version"],
        "query_policy_registry_sha256": approval_binding["query_policy_registry_sha256"],
        "query_policy_registry_row_sha256": approval_binding["query_policy_registry_row_sha256"],
        "candidate_value_policy_version": candidate_value_policy["policy_version"],
        "candidate_value_policy_sha256": candidate_value_policy_sha256,
    }
    hydration_tasks = build_hydration_tasks(candidates, experiment_binding=experiment_binding)
    scale_blockers = [
        "provider_post_bodies_not_replayable",
        "candidate_role_function_not_structured",
        "bio_snapshot_not_source_bound",
        "stable_platform_user_id_not_source_bound",
    ]
    if metrics["model_mediated_platform_user_id_presence_rate"] != 1.0:
        scale_blockers.append("stable_platform_user_id_coverage_below_100_percent")
    if metrics["model_mediated_bio_presence_rate"] != 1.0:
        scale_blockers.append("bio_coverage_below_100_percent")
    if raw_session["owner_only_permissions"] is not True:
        scale_blockers.append("raw_session_not_owner_only")
    if raw_session["cli_version_verified"] is not True:
        scale_blockers.append("cli_binary_version_not_bound_to_raw_session")
    if raw_session["tool_disable_flags_verified"] is not True:
        scale_blockers.append("tool_disable_flags_receipt_only")
    proof = "session_hash_and_calls_verified" if raw_session["tool_calls_verified"] else "receipt_only_unverified"
    evaluation = {
        "schema_version": EVALUATION_SCHEMA_VERSION,
        "status": "evaluated",
        "native_x_call_proof": proof,
        "input_binding": {
            "session_id": receipt["session_id"],
            "request_id": receipt["request_id"],
            "model_id": receipt["model_id"],
            "tool_policy_version": receipt["schema_version"],
            "result_sha256": canonical_sha256(result),
            "receipt_sha256": canonical_sha256(receipt),
            "query_policy_schema_version": validated_query_policy["schema_version"],
            "query_policy_version": validated_query_policy["policy_version"],
            "query_policy_sha256": query_policy_sha256,
            "legacy_full_query_policy_sha256": validated_query_policy["legacy_full_policy_sha256"],
            "query_policy_purpose": validated_query_policy["purpose"],
            "query_policy_allowed_decision_dimensions": list(validated_query_policy["allowed_decision_dimensions"]),
            "query_policy_registry_schema_version": QUERY_POLICY_REGISTRY_SCHEMA_VERSION,
            "query_policy_registry_version": approval_binding["query_policy_registry_version"],
            "query_policy_registry_sha256": approval_binding["query_policy_registry_sha256"],
            "query_policy_registry_row_sha256": approval_binding["query_policy_registry_row_sha256"],
            "lab_id": validated_query_policy["lab_id"],
            "candidate_value_policy_schema_version": candidate_value_policy["schema_version"],
            "candidate_value_policy_version": candidate_value_policy["policy_version"],
            "candidate_value_policy_sha256": candidate_value_policy_sha256,
            "result_to_receipt": "exact_tool_query_reconciliation_model_mediated_result",
            "raw_session": raw_session,
        },
        "candidate_search_feasibility": "receipt_only_model_mediated_result_unverified"
        if not raw_session["tool_calls_verified"]
        else "model_mediated_precision_tranche_lead_demonstrated"
        if precision_candidates
        else "model_mediated_recall_pool_leads_observed"
        if recall_candidates
        else "model_mediated_candidate_needs_evidence"
        if candidate_value_assessments
        else "model_mediated_lead_not_demonstrated",
        "researcher_role_function_feasibility": "not_evaluated",
        "source_replayability": "tool_and_query_only",
        "scale_verdict": "no_go" if scale_blockers else "eligible_for_independent_review",
        "scale_blockers": scale_blockers,
        "next_phase": "candidate_hydration" if hydration_tasks else "manual_adjudication",
        "tool_counts": dict(Counter(call["tool_name"] for call in calls)),
        "metrics": metrics,
        "candidate_value_assessments": candidate_value_assessments,
        "first_field_gate_gaps": {
            "bio_coverage_gap_to_100_percent": None
            if metrics["model_mediated_bio_presence_rate"] is None
            else round(1.0 - metrics["model_mediated_bio_presence_rate"], 6),
            "stable_id_coverage_gap_to_100_percent": None
            if metrics["model_mediated_platform_user_id_presence_rate"] is None
            else round(1.0 - metrics["model_mediated_platform_user_id_presence_rate"], 6),
            "evidence_complete_gap_to_90_percent": None
            if metrics["model_mediated_high_authority_support_coverage"] is None
            else round(max(0.0, 0.9 - metrics["model_mediated_high_authority_support_coverage"]), 6),
            "provider_post_body_replayability_gap_to_100_percent": 1.0 if post_evidence else None,
        },
        "hydration_tasks": hydration_tasks,
        "authority": {
            "formal_gate_eligible": False,
            "canonical_person_or_employment_confirmed": False,
            "discovery_scale_authorized": False,
            "product_write_authorized": False,
            "outreach_authorized": False,
        },
    }
    _validate_evaluation_output_shape(evaluation)
    return evaluation


def validate_evaluation_output(
    evaluation: Any,
    result: Any,
    receipt: Any,
    *,
    raw_session_directory: Path | None = None,
) -> None:
    """Replay from source inputs and require byte-equivalent canonical output.

    A persisted evaluation is not self-authenticating.  Callers must provide
    the sanitized result and receipt, plus the raw session directory whenever
    the artifact claims session verification.  The registry snapshot selector
    is taken from the artifact itself and then hash-checked during replay.
    """

    _validate_evaluation_output_shape(evaluation)
    input_binding = evaluation["input_binding"]
    replayed = evaluate_exploration(
        result,
        receipt,
        raw_session_directory=raw_session_directory,
        query_policy_version=input_binding["query_policy_version"],
        query_policy_registry_version=input_binding["query_policy_registry_version"],
    )
    if canonical_json(evaluation) != canonical_json(replayed):
        raise ExplorationValidationError("evaluation_source_replay_mismatch")


__all__ = [
    "EVALUATION_SCHEMA_VERSION",
    "ExplorationValidationError",
    "build_hydration_tasks",
    "canonical_sha256",
    "evaluate_exploration",
    "validate_evaluation_output",
    "validate_hydration_task",
]
