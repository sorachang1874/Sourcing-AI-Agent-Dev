from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from x_first.recall_pool_schema import (
    MiniDraft202012Error,
    assert_schema_valid,
    contract_schema_sha256,
)

REQUEST_SCHEMA_VERSION = "x.recall_pool.campaign.request.v1"
POLICY_SCHEMA_VERSION = "x.recall_pool.campaign.policy.v1"
WAVE_REQUEST_SCHEMA_VERSION = "x.recall_pool.campaign.wave_request.v1"
WAVE_RESULT_SCHEMA_VERSION = "x.grok_cli.recall_wave.result_adapter.v1"
MECHANICAL_RECEIPT_SCHEMA_VERSION = "x.recall_pool.campaign.wave_mechanical_receipt.v1"
RESULT_SCHEMA_VERSION = "x.recall_pool.campaign.result.v1"
ALLOWED_TOOL_REGISTRY_VERSION = "native_x_tools.v1"
STRATEGY_COMPARABILITY_RULE_VERSION = "versioned_strategy_and_precommitted_family_call_profile.v2"
STRATEGY_DEFINITION_SCHEMA_VERSION = "x.recall_pool.campaign.strategy_definition.v1"
FAMILY_ATTRIBUTION_BINDING_VERSION = "precommitted_runner_family_attribution.v1"
USER_VISIBLE_CHAT_CONTEXT_HASH_VERSION = "ordered_complete_non_system_chat_rows.v1"
MODEL_MEDIATED_UNVERIFIED = "model_mediated_unverified"

REQUEST_SCHEMA_FILE = "x.recall_pool.campaign.request.v1.schema.json"
POLICY_SCHEMA_FILE = "x.recall_pool.campaign.policy.v1.schema.json"
WAVE_REQUEST_SCHEMA_FILE = "x.recall_pool.campaign.wave_request.v1.schema.json"
MECHANICAL_RECEIPT_SCHEMA_FILE = "x.recall_pool.campaign.wave_mechanical_receipt.v1.schema.json"
RESULT_SCHEMA_FILE = "x.recall_pool.campaign.result.v1.schema.json"
CONTRACT_SCHEMA_FILES = (
    REQUEST_SCHEMA_FILE,
    POLICY_SCHEMA_FILE,
    WAVE_REQUEST_SCHEMA_FILE,
    MECHANICAL_RECEIPT_SCHEMA_FILE,
    RESULT_SCHEMA_FILE,
)

RAW_SESSION_FILES = frozenset(
    {
        "summary.json",
        "updates.jsonl",
        "events.jsonl",
        "chat_history.jsonl",
        "system_prompt.txt",
        "prompt_context.json",
    }
)
ALLOWED_NATIVE_X_TOOLS = frozenset(
    {"x_keyword_search", "x_semantic_search", "x_user_search", "x_thread_fetch"}
)
TEMPORAL_STATES = frozenset({"current", "historical", "ambiguous", "unsupported"})
DIMENSIONS = ("target_lab_affiliation_state", "pretraining_experience_state")
_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_SLUG_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
_VERSION_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,127}")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_TOOL_NAME_RE = re.compile(r"[a-z][a-z0-9_]{0,63}")
_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z")


class CampaignValidationError(ValueError):
    """Stable fail-closed error for the offline campaign lane."""


@dataclass(frozen=True)
class WaveInput:
    """All source bytes needed for direct, non-self-attested replay."""

    wave_id: str
    result_bytes: bytes
    upstream_request_bytes: bytes
    prompt_bytes: bytes
    raw_session_files: Mapping[str, bytes]


@dataclass(frozen=True)
class _TerminalJsonSlice:
    """One terminal object plus its exact UTF-8 byte range in assistant output."""

    payload: dict[str, Any]
    start_byte_offset: int
    end_byte_offset_exclusive: int


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def strategy_definition_sha256(strategy_id: str, strategy_definition: Mapping[str, Any]) -> str:
    """Bind a versioned complete strategy definition, not only its family labels."""

    return canonical_sha256(
        {
            "comparability_rule_version": STRATEGY_COMPARABILITY_RULE_VERSION,
            "strategy_definition": strategy_definition,
            "strategy_id": strategy_id,
        }
    )


def family_attribution_plan_sha256(
    strategy_definition_digest: str,
    family_planned_call_sha256s: Mapping[str, Sequence[str]],
) -> str:
    """Bind the pre-run mapping from ordered planned calls to query families."""

    return canonical_sha256(
        {
            "binding_version": FAMILY_ATTRIBUTION_BINDING_VERSION,
            "family_planned_call_sha256s": {
                family: list(values) for family, values in family_planned_call_sha256s.items()
            },
            "strategy_definition_sha256": strategy_definition_digest,
        }
    )


def user_visible_chat_context_sha256(chat_rows: Sequence[Mapping[str, Any]]) -> str:
    """Bind every ordered non-system chat row visible to the model or user."""

    visible_rows = [dict(row) for row in chat_rows if row.get("type") != "system"]
    return canonical_sha256(
        {
            "hash_version": USER_VISIBLE_CHAT_CONTEXT_HASH_VERSION,
            "visible_rows": visible_rows,
        }
    )


def _legacy_strategy_definition_sha256(strategy_id: str, query_family_ids: Sequence[str]) -> str:
    return canonical_sha256(
        {
            "comparability_rule_version": "exact_strategy_definition_and_family_call_mix.v1",
            "query_family_ids": list(query_family_ids),
            "strategy_id": strategy_id,
        }
    )


def bytes_sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _reject_constant(value: str) -> None:
    raise CampaignValidationError(f"non_finite_number:{value}")


def _closed_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CampaignValidationError("duplicate_json_key")
        result[key] = value
    return result


def strict_json_bytes(raw: bytes, *, error: str) -> Any:
    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_closed_object,
            parse_constant=_reject_constant,
        )
    except (CampaignValidationError, UnicodeError, json.JSONDecodeError) as exc:
        raise CampaignValidationError(error) from exc


def _strict_json_text(raw: str, *, error: str) -> Any:
    if not isinstance(raw, str):
        raise CampaignValidationError(error)
    return strict_json_bytes(raw.encode(), error=error)


def _jsonl_rows(raw: bytes, *, error: str, maximum_lines: int = 1_000_000) -> list[dict[str, Any]]:
    try:
        text = raw.decode("utf-8")
    except UnicodeError as exc:
        raise CampaignValidationError(error) from exc
    lines = text.splitlines()
    if not lines or len(lines) > maximum_lines:
        raise CampaignValidationError(error)
    rows: list[dict[str, Any]] = []
    for line in lines:
        if not line or len(line) > 5_000_000:
            raise CampaignValidationError(error)
        row = _strict_json_text(line, error=error)
        if not isinstance(row, dict):
            raise CampaignValidationError(error)
        rows.append(row)
    return rows


def _is_int(value: Any) -> bool:
    return type(value) is int


def _is_number(value: Any) -> bool:
    return type(value) in {int, float} and math.isfinite(value)


def _fraction(numerator: int, denominator: int) -> float:
    return 0.0 if denominator == 0 else round(numerator / denominator, 6)


def _ratio(numerator: float, denominator: float) -> float | None:
    return None if denominator == 0 else round(numerator / denominator, 6)


def _exact_keys(value: Any, expected: set[str], error: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise CampaignValidationError(error)
    return value


def _nonempty_text(value: Any, *, maximum: int, error: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise CampaignValidationError(error)
    return value.strip()


def _valid_uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


def _valid_handle(value: Any) -> str:
    if not isinstance(value, str) or _HANDLE_RE.fullmatch(value) is None:
        raise CampaignValidationError("candidate_handle_invalid")
    return value


def _canonical_x_url(value: Any, *, expected_handle: str | None = None) -> tuple[str, str]:
    if not isinstance(value, str) or len(value) > 512:
        raise CampaignValidationError("x_url_invalid")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "x.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.query
        or parsed.fragment
    ):
        raise CampaignValidationError("x_url_invalid")
    parts = [part for part in parsed.path.split("/") if part]
    if not parts or _HANDLE_RE.fullmatch(parts[0]) is None:
        raise CampaignValidationError("x_url_invalid")
    if len(parts) not in {1, 3} or (len(parts) == 3 and (parts[1] != "status" or not parts[2].isdigit())):
        raise CampaignValidationError("x_url_invalid")
    observed_handle = parts[0]
    if expected_handle is not None and observed_handle.casefold() != expected_handle.casefold():
        raise CampaignValidationError("x_url_author_handle_mismatch")
    normalized = f"https://x.com/{observed_handle.casefold()}"
    if len(parts) == 3:
        normalized += f"/status/{parts[2]}"
    return normalized, observed_handle


def _schema_guard(payload: Any, filename: str, error: str) -> None:
    try:
        assert_schema_valid(payload, filename)
    except MiniDraft202012Error as exc:
        raise CampaignValidationError(error) from exc


def validate_request(request: Any) -> None:
    request = _exact_keys(
        request,
        {"schema_version", "campaign_id", "target", "waves"},
        "campaign_request_shape_invalid",
    )
    if request["schema_version"] != REQUEST_SCHEMA_VERSION:
        raise CampaignValidationError("campaign_request_version_invalid")
    if not isinstance(request["campaign_id"], str) or _SLUG_RE.fullmatch(request["campaign_id"]) is None:
        raise CampaignValidationError("campaign_id_invalid")
    target = _exact_keys(request["target"], {"lab_id", "research_focus_id"}, "campaign_target_shape_invalid")
    if any(not isinstance(target[field], str) or _SLUG_RE.fullmatch(target[field]) is None for field in target):
        raise CampaignValidationError("campaign_target_invalid")
    waves = request["waves"]
    if not isinstance(waves, list) or not waves:
        raise CampaignValidationError("campaign_waves_invalid")
    wave_ids: set[str] = set()
    source_paths: set[str] = set()
    for binding in waves:
        binding = _exact_keys(
            binding,
            {
                "wave_id",
                "source_path",
                "source_sha256",
                "upstream_request",
                "prompt",
                "raw_session_directory",
            },
            "campaign_wave_binding_shape_invalid",
        )
        wave_id = binding["wave_id"]
        if not isinstance(wave_id, str) or _SLUG_RE.fullmatch(wave_id) is None or wave_id in wave_ids:
            raise CampaignValidationError("campaign_wave_id_invalid")
        wave_ids.add(wave_id)
        for field in ("source_path", "raw_session_directory"):
            if not isinstance(binding[field], str) or not binding[field] or len(binding[field]) > 2048:
                raise CampaignValidationError("campaign_wave_path_invalid")
        if binding["source_path"] in source_paths:
            raise CampaignValidationError("duplicate_campaign_wave_path")
        source_paths.add(binding["source_path"])
        if not isinstance(binding["source_sha256"], str) or _SHA256_RE.fullmatch(binding["source_sha256"]) is None:
            raise CampaignValidationError("campaign_wave_sha256_invalid")
        for field in ("upstream_request", "prompt"):
            nested = _exact_keys(binding[field], {"path", "sha256"}, "campaign_wave_source_binding_invalid")
            if (
                not isinstance(nested["path"], str)
                or not nested["path"]
                or len(nested["path"]) > 2048
                or not isinstance(nested["sha256"], str)
                or _SHA256_RE.fullmatch(nested["sha256"]) is None
            ):
                raise CampaignValidationError("campaign_wave_source_binding_invalid")
    _schema_guard(request, REQUEST_SCHEMA_FILE, "campaign_request_schema_invalid")


def validate_policy(policy: Any) -> None:
    policy = _exact_keys(
        policy,
        {
            "schema_version",
            "policy_version",
            "kill_ceilings",
            "stop_advisory",
            "public_prompt_sha256_allowlist",
            "authority",
        },
        "campaign_policy_shape_invalid",
    )
    if policy["schema_version"] != POLICY_SCHEMA_VERSION:
        raise CampaignValidationError("campaign_policy_schema_version_invalid")
    if not isinstance(policy["policy_version"], str) or _VERSION_RE.fullmatch(policy["policy_version"]) is None:
        raise CampaignValidationError("campaign_policy_version_invalid")
    ceilings = _exact_keys(
        policy["kill_ceilings"],
        {
            "max_wave_files",
            "max_bytes_per_wave",
            "max_raw_session_file_bytes",
            "max_total_input_bytes",
            "max_candidate_rows_in_memory",
            "max_evidence_rows_in_memory",
        },
        "campaign_kill_ceilings_shape_invalid",
    )
    bounds = {
        "max_wave_files": (1, 10_000),
        "max_bytes_per_wave": (1, 1_000_000_000),
        "max_raw_session_file_bytes": (1, 1_000_000_000),
        "max_total_input_bytes": (1, 20_000_000_000),
        "max_candidate_rows_in_memory": (1, 10_000_000),
        "max_evidence_rows_in_memory": (1, 100_000_000),
    }
    for field, (minimum, maximum) in bounds.items():
        if not _is_int(ceilings[field]) or not minimum <= ceilings[field] <= maximum:
            raise CampaignValidationError("campaign_kill_ceiling_invalid")
    advisory = _exact_keys(
        policy["stop_advisory"],
        {
            "minimum_comparable_waves",
            "lookback_comparable_waves",
            "max_latest_new_unique_per_completed_call_for_plateau",
            "minimum_productivity_decline_fraction",
            "require_nonincreasing_productivity",
        },
        "campaign_stop_advisory_shape_invalid",
    )
    if (
        not _is_int(advisory["minimum_comparable_waves"])
        or advisory["minimum_comparable_waves"] < 2
        or not _is_int(advisory["lookback_comparable_waves"])
        or not 2 <= advisory["lookback_comparable_waves"] <= advisory["minimum_comparable_waves"]
        or not _is_number(advisory["max_latest_new_unique_per_completed_call_for_plateau"])
        or not 0 <= advisory["max_latest_new_unique_per_completed_call_for_plateau"] <= 1
        or not _is_number(advisory["minimum_productivity_decline_fraction"])
        or not 0 <= advisory["minimum_productivity_decline_fraction"] <= 1
        or advisory["require_nonincreasing_productivity"] is not True
    ):
        raise CampaignValidationError("campaign_stop_advisory_invalid")
    public_prompt_allowlist = policy["public_prompt_sha256_allowlist"]
    if public_prompt_allowlist != []:
        raise CampaignValidationError("campaign_public_prompt_allowlist_invalid")
    authority = _exact_keys(
        policy["authority"],
        {"business_candidate_limit", "stop_advisory_enforced", "provider_calls_allowed", "product_writes_allowed"},
        "campaign_authority_shape_invalid",
    )
    if authority != {
        "business_candidate_limit": None,
        "stop_advisory_enforced": False,
        "provider_calls_allowed": False,
        "product_writes_allowed": False,
    }:
        raise CampaignValidationError("campaign_authority_invalid")
    _schema_guard(policy, POLICY_SCHEMA_FILE, "campaign_policy_schema_invalid")


def _mechanical_call_shape_valid(shape: Any) -> bool:
    if not isinstance(shape, Mapping) or set(shape) != {
        "tool_name",
        "limit",
        "count",
        "mode",
        "min_score_threshold",
        "thread_argument_shape",
    }:
        return False
    tool_name = shape["tool_name"]
    if tool_name not in ALLOWED_NATIVE_X_TOOLS:
        return False
    nullable_int_fields = (shape["limit"], shape["count"])
    if any(value is not None and (not _is_int(value) or value < 1) for value in nullable_int_fields):
        return False
    threshold = shape["min_score_threshold"]
    if threshold is not None and (not _is_number(threshold) or not 0 <= threshold <= 1):
        return False
    if tool_name == "x_keyword_search":
        return (
            shape["limit"] is not None
            and shape["count"] is None
            and shape["mode"] in {"Latest", "Top"}
            and threshold is None
            and shape["thread_argument_shape"] is None
        )
    if tool_name == "x_semantic_search":
        return (
            shape["limit"] is not None
            and shape["count"] is None
            and shape["mode"] is None
            and shape["thread_argument_shape"] is None
        )
    if tool_name == "x_user_search":
        return (
            shape["limit"] is None
            and shape["count"] is not None
            and shape["mode"] is None
            and threshold is None
            and shape["thread_argument_shape"] is None
        )
    return (
        shape["limit"] is None
        and shape["count"] is None
        and shape["mode"] is None
        and threshold is None
        and shape["thread_argument_shape"] in {"post_id", "tweet_id", "url", "query"}
    )


def _validate_strategy_definition(definition: Any, query_family_ids: Sequence[str]) -> None:
    definition = _exact_keys(
        definition,
        {"schema_version", "query_families"},
        "wave_request_strategy_definition_invalid",
    )
    families = definition["query_families"]
    if definition["schema_version"] != STRATEGY_DEFINITION_SCHEMA_VERSION or not isinstance(families, list):
        raise CampaignValidationError("wave_request_strategy_definition_invalid")
    observed_ids: list[str] = []
    for family in families:
        family = _exact_keys(
            family,
            {"family_id", "family_version", "allowed_call_shapes"},
            "wave_request_strategy_family_invalid",
        )
        family_id = family["family_id"]
        shapes = family["allowed_call_shapes"]
        if (
            not isinstance(family_id, str)
            or _SLUG_RE.fullmatch(family_id) is None
            or not isinstance(family["family_version"], str)
            or _VERSION_RE.fullmatch(family["family_version"]) is None
            or not isinstance(shapes, list)
            or not shapes
            or any(not _mechanical_call_shape_valid(shape) for shape in shapes)
            or len({canonical_json(shape) for shape in shapes}) != len(shapes)
        ):
            raise CampaignValidationError("wave_request_strategy_family_invalid")
        observed_ids.append(family_id)
    if observed_ids != list(query_family_ids) or len(observed_ids) != len(set(observed_ids)):
        raise CampaignValidationError("wave_request_strategy_family_order_invalid")


def validate_wave_request(upstream: Any) -> None:
    upstream = _exact_keys(
        upstream,
        {
            "schema_version",
            "wave_id",
            "wave_result_schema_version",
            "target",
            "prompt_sha256",
            "expected_session_id",
            "expected_request_id",
            "expected_model_id",
            "strategy",
            "execution_context",
        },
        "wave_request_shape_invalid",
    )
    if (
        upstream["schema_version"] != WAVE_REQUEST_SCHEMA_VERSION
        or upstream["wave_result_schema_version"] != WAVE_RESULT_SCHEMA_VERSION
        or not isinstance(upstream["wave_id"], str)
        or _SLUG_RE.fullmatch(upstream["wave_id"]) is None
        or not isinstance(upstream["prompt_sha256"], str)
        or _SHA256_RE.fullmatch(upstream["prompt_sha256"]) is None
        or not _valid_uuid(upstream["expected_session_id"])
        or not _valid_uuid(upstream["expected_request_id"])
        or not isinstance(upstream["expected_model_id"], str)
        or _MODEL_RE.fullmatch(upstream["expected_model_id"]) is None
    ):
        raise CampaignValidationError("wave_request_binding_invalid")
    target = _exact_keys(upstream["target"], {"lab_id", "research_focus_id"}, "wave_request_target_invalid")
    if any(not isinstance(target[field], str) or _SLUG_RE.fullmatch(target[field]) is None for field in target):
        raise CampaignValidationError("wave_request_target_invalid")
    strategy = upstream["strategy"]
    legacy_strategy_keys = {
        "strategy_id",
        "strategy_definition_sha256",
        "query_family_ids",
        "execution_attribution",
    }
    versioned_strategy_keys = legacy_strategy_keys | {"strategy_definition"}
    if not isinstance(strategy, Mapping) or set(strategy) not in {
        frozenset(legacy_strategy_keys),
        frozenset(versioned_strategy_keys),
    }:
        raise CampaignValidationError("wave_request_strategy_shape_invalid")
    families = strategy["query_family_ids"]
    if (
        not isinstance(strategy["strategy_id"], str)
        or _VERSION_RE.fullmatch(strategy["strategy_id"]) is None
        or not isinstance(families, list)
        or not families
        or len(families) != len(set(families))
        or any(not isinstance(item, str) or _SLUG_RE.fullmatch(item) is None for item in families)
    ):
        raise CampaignValidationError("wave_request_strategy_invalid")
    versioned_definition = strategy.get("strategy_definition")
    if versioned_definition is None:
        expected_strategy_digest = _legacy_strategy_definition_sha256(strategy["strategy_id"], families)
    else:
        _validate_strategy_definition(versioned_definition, families)
        expected_strategy_digest = strategy_definition_sha256(strategy["strategy_id"], versioned_definition)
    if strategy["strategy_definition_sha256"] != expected_strategy_digest:
        raise CampaignValidationError("wave_request_strategy_definition_digest_invalid")

    attribution = strategy["execution_attribution"]
    if not isinstance(attribution, Mapping):
        raise CampaignValidationError("wave_request_attribution_shape_invalid")
    if attribution.get("status") in {"unavailable", "exact_replayed_call_attribution"}:
        attribution = _exact_keys(
            attribution,
            {"status", "family_call_sha256s"},
            "wave_request_attribution_shape_invalid",
        )
        if attribution["status"] == "unavailable":
            if attribution["family_call_sha256s"] is not None:
                raise CampaignValidationError("wave_request_attribution_invalid")
        else:
            mapping = attribution["family_call_sha256s"]
            if not isinstance(mapping, Mapping) or set(mapping) != set(families):
                raise CampaignValidationError("wave_request_attribution_invalid")
            all_hashes: list[str] = []
            for values in mapping.values():
                if (
                    not isinstance(values, list)
                    or not values
                    or len(values) != len(set(values))
                    or any(not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None for value in values)
                ):
                    raise CampaignValidationError("wave_request_attribution_invalid")
                all_hashes.extend(values)
            if len(all_hashes) != len(set(all_hashes)):
                raise CampaignValidationError("wave_request_attribution_overlap")
    elif attribution.get("status") == "precommitted_runner_bound":
        attribution = _exact_keys(
            attribution,
            {"status", "family_planned_call_sha256s", "attribution_plan_sha256"},
            "wave_request_attribution_shape_invalid",
        )
        mapping = attribution["family_planned_call_sha256s"]
        if versioned_definition is None or not isinstance(mapping, Mapping) or set(mapping) != set(families):
            raise CampaignValidationError("wave_request_attribution_invalid")
        all_hashes = []
        for values in mapping.values():
            if (
                not isinstance(values, list)
                or not values
                or len(values) != len(set(values))
                or any(not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None for value in values)
            ):
                raise CampaignValidationError("wave_request_attribution_invalid")
            all_hashes.extend(values)
        if len(all_hashes) != len(set(all_hashes)):
            raise CampaignValidationError("wave_request_attribution_overlap")
        expected_plan_digest = family_attribution_plan_sha256(
            strategy["strategy_definition_sha256"],
            mapping,
        )
        if attribution["attribution_plan_sha256"] != expected_plan_digest:
            raise CampaignValidationError("wave_request_attribution_plan_digest_invalid")
    else:
        raise CampaignValidationError("wave_request_attribution_invalid")

    execution_context = upstream["execution_context"]
    legacy_execution_keys = {
        "system_prompt_sha256",
        "prompt_context_sha256",
        "prior_exclusion_binding_status",
    }
    versioned_execution_keys = legacy_execution_keys | {"user_visible_chat_context"}
    if not isinstance(execution_context, Mapping) or set(execution_context) not in {
        frozenset(legacy_execution_keys),
        frozenset(versioned_execution_keys),
    }:
        raise CampaignValidationError("wave_request_execution_context_invalid")
    if (
        not isinstance(execution_context["system_prompt_sha256"], str)
        or _SHA256_RE.fullmatch(execution_context["system_prompt_sha256"]) is None
        or not isinstance(execution_context["prompt_context_sha256"], str)
        or _SHA256_RE.fullmatch(execution_context["prompt_context_sha256"]) is None
        or execution_context["prior_exclusion_binding_status"]
        not in {"not_applicable", "system_prompt_hash_bound_operator_asserted"}
    ):
        raise CampaignValidationError("wave_request_execution_context_invalid")
    if "user_visible_chat_context" in execution_context:
        chat_binding = _exact_keys(
            execution_context["user_visible_chat_context"],
            {"binding_status", "sha256"},
            "wave_request_user_chat_binding_invalid",
        )
        if (
            chat_binding["binding_status"] != "precommitted_exact"
            or not isinstance(chat_binding["sha256"], str)
            or _SHA256_RE.fullmatch(chat_binding["sha256"]) is None
        ):
            raise CampaignValidationError("wave_request_user_chat_binding_invalid")
    _schema_guard(upstream, WAVE_REQUEST_SCHEMA_FILE, "wave_request_schema_invalid")


def _tool_arguments_valid(arguments: Any, tool_name: str) -> bool:
    if not isinstance(arguments, Mapping):
        return False
    if tool_name == "x_keyword_search":
        if set(arguments) != {"query", "limit", "mode"} or arguments["mode"] not in {"Latest", "Top"}:
            return False
        subject = arguments["query"]
        bound = arguments["limit"]
        return (
            isinstance(subject, str)
            and 0 < len(subject.strip()) <= 10_000
            and isinstance(bound, str)
            and bound.isdecimal()
            and 1 <= int(bound) <= 100
        )
    if tool_name == "x_semantic_search":
        if set(arguments) not in ({"query", "limit"}, {"query", "limit", "min_score_threshold"}):
            return False
        subject = arguments["query"]
        bound = arguments["limit"]
        threshold = arguments.get("min_score_threshold")
        threshold_valid = threshold is None
        if isinstance(threshold, str):
            try:
                parsed_threshold = float(threshold)
            except ValueError:
                parsed_threshold = math.nan
            threshold_valid = math.isfinite(parsed_threshold) and 0 <= parsed_threshold <= 1
        return (
            isinstance(subject, str)
            and 0 < len(subject.strip()) <= 10_000
            and isinstance(bound, str)
            and bound.isdecimal()
            and 1 <= int(bound) <= 100
            and threshold_valid
        )
    if tool_name == "x_user_search":
        if set(arguments) != {"query", "count"}:
            return False
        subject = arguments["query"]
        bound = arguments["count"]
        return (
            isinstance(subject, str)
            and 0 < len(subject.strip()) <= 10_000
            and isinstance(bound, str)
            and bound.isdecimal()
            and 1 <= int(bound) <= 50
        )
    if set(arguments) in ({"post_id"}, {"tweet_id"}):
        value = next(iter(arguments.values()))
        return isinstance(value, str) and value.isdigit() and len(value) <= 32
    if set(arguments) == {"url"}:
        try:
            url, _ = _canonical_x_url(arguments["url"])
        except CampaignValidationError:
            return False
        return "/status/" in url
    if set(arguments) == {"query"}:
        value = arguments["query"]
        return isinstance(value, str) and 0 < len(value.strip()) <= 10_000
    return False


def _mechanical_call_shape(tool_name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
    shape = {
        "tool_name": tool_name,
        "limit": None,
        "count": None,
        "mode": None,
        "min_score_threshold": None,
        "thread_argument_shape": None,
    }
    if tool_name == "x_keyword_search":
        shape["limit"] = int(arguments["limit"])
        shape["mode"] = arguments["mode"]
    elif tool_name == "x_semantic_search":
        shape["limit"] = int(arguments["limit"])
        threshold = arguments.get("min_score_threshold")
        shape["min_score_threshold"] = None if threshold is None else float(threshold)
    elif tool_name == "x_user_search":
        shape["count"] = int(arguments["count"])
    else:
        shape["thread_argument_shape"] = next(iter(arguments))
    if not _mechanical_call_shape_valid(shape):
        raise CampaignValidationError("raw_session_mechanical_call_shape_invalid")
    return shape


def _extract_bound_prompt(chat_rows: list[dict[str, Any]]) -> bytes:
    matches: list[str] = []
    for row in chat_rows:
        if row.get("type") != "user" or not isinstance(row.get("content"), list):
            continue
        for item in row["content"]:
            if not isinstance(item, dict) or item.get("type") != "text" or not isinstance(item.get("text"), str):
                continue
            text = item["text"]
            if text.startswith("<user_query>\n") and text.endswith("\n</user_query>"):
                matches.append(text[len("<user_query>\n") : -len("\n</user_query>")])
    if len(matches) != 1:
        raise CampaignValidationError("raw_session_prompt_envelope_invalid")
    return (matches[0] + "\n").encode()


def _validate_bound_system_message(chat_rows: list[dict[str, Any]], system_prompt: bytes) -> None:
    systems = [row for row in chat_rows if row.get("type") == "system"]
    if (
        len(systems) != 1
        or set(systems[0]) != {"type", "content"}
        or not isinstance(systems[0]["content"], str)
    ):
        raise CampaignValidationError("raw_session_system_message_invalid")
    try:
        observed = systems[0]["content"].encode("utf-8")
    except UnicodeError as exc:
        raise CampaignValidationError("raw_session_system_message_invalid") from exc
    if observed != system_prompt:
        raise CampaignValidationError("raw_session_system_prompt_mismatch")


def _extract_unique_terminal_json(raw: bytes) -> _TerminalJsonSlice:
    try:
        text = raw.decode("utf-8")
    except UnicodeError as exc:
        raise CampaignValidationError("assistant_output_utf8_invalid") from exc
    decoder = json.JSONDecoder()
    terminal_slices: list[tuple[str, int, int]] = []
    for match in re.finditer(r"\{", text):
        try:
            value, end = decoder.raw_decode(text, match.start())
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and not text[end:].strip():
            terminal_slices.append(
                (
                    text[match.start() : end],
                    len(text[: match.start()].encode("utf-8")),
                    len(text[:end].encode("utf-8")),
                )
            )
    if len(terminal_slices) != 1:
        raise CampaignValidationError("assistant_output_terminal_json_invalid")
    terminal_text, start_byte_offset, end_byte_offset_exclusive = terminal_slices[0]
    terminal = strict_json_bytes(
        terminal_text.encode("utf-8"),
        error="assistant_output_terminal_json_invalid",
    )
    if not isinstance(terminal, dict):
        raise CampaignValidationError("assistant_output_terminal_json_invalid")
    return _TerminalJsonSlice(
        payload=terminal,
        start_byte_offset=start_byte_offset,
        end_byte_offset_exclusive=end_byte_offset_exclusive,
    )


def _replay_raw_session(
    wave: WaveInput,
    upstream: Mapping[str, Any],
    *,
    result_sha256: str,
    upstream_request_sha256: str,
    result_payload: Mapping[str, Any],
) -> dict[str, Any]:
    if set(wave.raw_session_files) != RAW_SESSION_FILES or any(
        not isinstance(value, bytes) for value in wave.raw_session_files.values()
    ):
        raise CampaignValidationError("raw_session_sources_invalid")
    summary = strict_json_bytes(wave.raw_session_files["summary.json"], error="raw_session_summary_invalid")
    if not isinstance(summary, dict) or not isinstance(summary.get("info"), dict):
        raise CampaignValidationError("raw_session_summary_invalid")
    session_id = summary["info"].get("id")
    request_id = summary.get("request_id")
    model_id = summary.get("current_model_id")
    if (
        session_id != upstream["expected_session_id"]
        or request_id != upstream["expected_request_id"]
        or model_id != upstream["expected_model_id"]
    ):
        raise CampaignValidationError("raw_session_summary_identity_mismatch")

    chat_rows = _jsonl_rows(
        wave.raw_session_files["chat_history.jsonl"],
        error="raw_session_chat_history_invalid",
    )
    _validate_bound_system_message(chat_rows, wave.raw_session_files["system_prompt.txt"])
    execution_context = upstream["execution_context"]
    observed_user_context_sha256 = user_visible_chat_context_sha256(chat_rows)
    configured_user_context = execution_context.get("user_visible_chat_context")
    if configured_user_context is None:
        user_context_binding_status = "legacy_unbound"
    else:
        if configured_user_context["sha256"] != observed_user_context_sha256:
            raise CampaignValidationError("raw_session_user_chat_context_hash_mismatch")
        user_context_binding_status = "precommitted_exact"
    replayed_prompt = _extract_bound_prompt(chat_rows)
    if replayed_prompt != wave.prompt_bytes or bytes_sha256(replayed_prompt) != upstream["prompt_sha256"]:
        raise CampaignValidationError("raw_session_prompt_hash_mismatch")
    if (
        bytes_sha256(wave.raw_session_files["system_prompt.txt"])
        != execution_context["system_prompt_sha256"]
        or bytes_sha256(wave.raw_session_files["prompt_context.json"])
        != execution_context["prompt_context_sha256"]
    ):
        raise CampaignValidationError("raw_session_execution_context_hash_mismatch")
    prompt_context = strict_json_bytes(
        wave.raw_session_files["prompt_context.json"],
        error="raw_session_prompt_context_invalid",
    )
    if not isinstance(prompt_context, dict):
        raise CampaignValidationError("raw_session_prompt_context_invalid")
    working_directory = prompt_context.get("working_directory")
    if (
        not isinstance(working_directory, str)
        or not working_directory
        or summary["info"].get("cwd") != working_directory
    ):
        raise CampaignValidationError("raw_session_working_directory_mismatch")
    event_rows = _jsonl_rows(
        wave.raw_session_files["events.jsonl"],
        error="raw_session_events_invalid",
    )
    turn_started = [row for row in event_rows if row.get("type") == "turn_started"]
    turn_ended = [row for row in event_rows if row.get("type") == "turn_ended"]
    if (
        len(turn_started) != 1
        or len(turn_ended) != 1
        or event_rows[0] is not turn_started[0]
        or event_rows[-1] is not turn_ended[0]
        or turn_started[0].get("session_id") != session_id
        or turn_started[0].get("model_id") != model_id
        or turn_ended[0].get("outcome") != "completed"
    ):
        raise CampaignValidationError("raw_session_cli_terminal_invalid")

    started_call_ids: set[str] = set()
    completed_call_ids: set[str] = set()
    provider_call_ids: set[str] = set()
    calls: list[dict[str, Any]] = []
    update_rows = _jsonl_rows(
        wave.raw_session_files["updates.jsonl"],
        error="raw_session_update_invalid",
    )
    if summary.get("num_messages") != len(update_rows):
        raise CampaignValidationError("raw_session_summary_message_count_mismatch")
    assistant_chunks: list[str] = []
    assistant_chunk_ids: list[int] = []
    assistant_event_ids: set[str] = set()
    assistant_timestamps: list[int] = []
    assistant_update_indices: list[int] = []
    for update_index, row in enumerate(update_rows):
        params = row.get("params")
        update = params.get("update") if isinstance(params, dict) else None
        if not isinstance(update, dict) or update.get("sessionUpdate") != "agent_message_chunk":
            continue
        metadata = params.get("_meta")
        content = update.get("content")
        if (
            set(params) != {"sessionId", "update", "_meta"}
            or params.get("sessionId") != session_id
            or set(update) != {"sessionUpdate", "content"}
            or not isinstance(metadata, dict)
            or metadata.get("promptId") != request_id
            or metadata.get("updateType") != "AgentMessageChunk"
            or not _is_int(metadata.get("chunkId"))
            or not _is_int(metadata.get("agentTimestampMs"))
            or not _is_int(metadata.get("streamStartMs"))
            or not _is_int(metadata.get("turnStartMs"))
            or not _is_int(metadata.get("totalTokens"))
            or not isinstance(metadata.get("eventId"), str)
            or not metadata["eventId"].startswith(f"{session_id}-")
            or metadata["eventId"] in assistant_event_ids
            or not isinstance(content, dict)
            or set(content) != {"type", "text"}
            or content.get("type") != "text"
            or not isinstance(content.get("text"), str)
            or not content["text"]
        ):
            raise CampaignValidationError("raw_session_assistant_chunk_invalid")
        assistant_chunks.append(content["text"])
        assistant_chunk_ids.append(metadata["chunkId"])
        assistant_event_ids.add(metadata["eventId"])
        assistant_timestamps.append(metadata["agentTimestampMs"])
        assistant_update_indices.append(update_index)
    if (
        not assistant_chunks
        or assistant_chunk_ids != sorted(assistant_chunk_ids)
        or len(assistant_chunk_ids) != len(set(assistant_chunk_ids))
        or assistant_timestamps != sorted(assistant_timestamps)
    ):
        raise CampaignValidationError("raw_session_assistant_chunk_order_invalid")
    try:
        assistant_chunk_bytes = [chunk.encode("utf-8") for chunk in assistant_chunks]
    except UnicodeError as exc:
        raise CampaignValidationError("assistant_output_utf8_invalid") from exc
    assistant_output = b"".join(assistant_chunk_bytes) + b"\n"
    terminal_slice = _extract_unique_terminal_json(assistant_output)
    terminal_json = terminal_slice.payload
    chunk_byte_ranges: list[tuple[int, int]] = []
    chunk_cursor = 0
    for chunk in assistant_chunk_bytes:
        next_cursor = chunk_cursor + len(chunk)
        chunk_byte_ranges.append((chunk_cursor, next_cursor))
        chunk_cursor = next_cursor
    terminal_start_chunk_index = next(
        (
            index
            for index, (start, end) in enumerate(chunk_byte_ranges)
            if start <= terminal_slice.start_byte_offset < end
        ),
        None,
    )
    terminal_end_chunk_index = next(
        (
            index
            for index, (start, end) in enumerate(chunk_byte_ranges)
            if start <= terminal_slice.end_byte_offset_exclusive - 1 < end
        ),
        None,
    )
    if terminal_start_chunk_index is None or terminal_end_chunk_index is None:
        raise CampaignValidationError("assistant_output_terminal_json_chunk_mapping_invalid")
    if canonical_json(terminal_json) != canonical_json(result_payload):
        raise CampaignValidationError("assistant_output_result_binding_mismatch")
    user_events = [
        row
        for row in update_rows
        if isinstance(row.get("params"), dict)
        and isinstance(row["params"].get("update"), dict)
        and row["params"]["update"].get("sessionUpdate") == "user_message_chunk"
    ]
    if (
        len(user_events) != 1
        or user_events[0]["params"].get("sessionId") != session_id
        or not isinstance(user_events[0]["params"]["update"].get("_meta"), dict)
        or user_events[0]["params"]["update"]["_meta"].get("modelId") != model_id
    ):
        raise CampaignValidationError("raw_session_user_model_binding_invalid")
    tool_update_indices: list[int] = []
    for update_index, event in enumerate(update_rows):
        params = event.get("params")
        update = params.get("update") if isinstance(params, dict) else None
        if not isinstance(update, dict):
            continue
        metadata = params.get("_meta") if isinstance(params, dict) else None
        if update.get("sessionUpdate") == "tool_call":
            tool_call_id = update.get("toolCallId")
            if (
                params.get("sessionId") != session_id
                or not isinstance(metadata, dict)
                or metadata.get("promptId") != request_id
                or update.get("status") != "in_progress"
                or not isinstance(tool_call_id, str)
                or not tool_call_id
                or tool_call_id in started_call_ids
            ):
                raise CampaignValidationError("raw_session_started_call_invalid")
            started_call_ids.add(tool_call_id)
            tool_update_indices.append(update_index)
            continue
        if update.get("sessionUpdate") != "tool_call_update":
            continue
        raw = update.get("rawOutput")
        if (
            update.get("status") != "completed"
            or not isinstance(raw, dict)
            or set(raw) != {"call_id", "id", "input", "name"}
            or params.get("sessionId") != session_id
            or not isinstance(metadata, dict)
            or metadata.get("promptId") != request_id
            or update.get("toolCallId") != raw.get("id")
            or raw.get("name") not in ALLOWED_NATIVE_X_TOOLS
        ):
            raise CampaignValidationError("raw_session_completed_call_invalid")
        tool_call_id = raw["id"]
        provider_call_id = raw["call_id"]
        if (
            not isinstance(tool_call_id, str)
            or not tool_call_id
            or tool_call_id in completed_call_ids
            or not isinstance(provider_call_id, str)
            or not provider_call_id
            or provider_call_id in provider_call_ids
            or tool_call_id not in started_call_ids
        ):
            raise CampaignValidationError("raw_session_completed_call_identity_invalid")
        arguments = _strict_json_text(raw["input"], error="raw_session_tool_arguments_invalid")
        if not _tool_arguments_valid(arguments, raw["name"]):
            raise CampaignValidationError("raw_session_tool_arguments_invalid")
        completed_call_ids.add(tool_call_id)
        provider_call_ids.add(provider_call_id)
        tool_update_indices.append(update_index)
        call = {
            "tool_call_id": tool_call_id,
            "provider_call_id": provider_call_id,
            "tool_name": raw["name"],
            "arguments": arguments,
        }
        call["call_identity_sha256"] = canonical_sha256(call)
        call["query_identity_sha256"] = canonical_sha256(
            {"tool_name": raw["name"], "arguments": arguments}
        )
        call["planned_call_identity_sha256"] = canonical_sha256(
            {
                "sequence_ordinal": len(calls),
                "tool_name": raw["name"],
                "arguments": arguments,
            }
        )
        call["mechanical_call_shape"] = _mechanical_call_shape(raw["name"], arguments)
        calls.append(call)
    if not calls or started_call_ids != completed_call_ids:
        raise CampaignValidationError("raw_session_terminal_pairing_invalid")
    final_assistant_update_index = assistant_update_indices[-1]
    terminal_start_update_index = assistant_update_indices[terminal_start_chunk_index]
    terminal_end_update_index = assistant_update_indices[terminal_end_chunk_index]
    last_native_x_update_index = max(tool_update_indices)
    if terminal_start_update_index <= last_native_x_update_index:
        raise CampaignValidationError("raw_session_terminal_assistant_causality_invalid")

    strategy = upstream["strategy"]
    attribution = strategy["execution_attribution"]
    call_hashes = [call["call_identity_sha256"] for call in calls]
    strategy_definition_binding_status = (
        "versioned_complete" if "strategy_definition" in strategy else "legacy_ids_only_unbound"
    )
    if attribution["status"] == "precommitted_runner_bound":
        mapping = attribution["family_planned_call_sha256s"]
        mapped = [value for values in mapping.values() for value in values]
        planned_call_hashes = [call["planned_call_identity_sha256"] for call in calls]
        if Counter(mapped) != Counter(planned_call_hashes):
            raise CampaignValidationError("query_family_call_attribution_mismatch")
        if (
            prompt_context.get("recall_pool_family_attribution_plan_sha256")
            != attribution["attribution_plan_sha256"]
        ):
            raise CampaignValidationError("raw_session_runner_attribution_binding_mismatch")
        family_by_planned_hash = {
            planned_hash: family for family, values in mapping.items() for planned_hash in values
        }
        definition_shapes = {
            family["family_id"]: {canonical_json(shape) for shape in family["allowed_call_shapes"]}
            for family in strategy["strategy_definition"]["query_families"]
        }
        calls_by_family: dict[str, list[dict[str, Any]]] = {
            family: [] for family in strategy["query_family_ids"]
        }
        for call in calls:
            family = family_by_planned_hash[call["planned_call_identity_sha256"]]
            if canonical_json(call["mechanical_call_shape"]) not in definition_shapes[family]:
                raise CampaignValidationError("query_family_call_shape_outside_strategy_definition")
            calls_by_family[family].append(call)
        family_call_profiles = []
        for family in strategy["query_family_ids"]:
            shape_counts = Counter(
                canonical_json(call["mechanical_call_shape"]) for call in calls_by_family[family]
            )
            family_call_profiles.append(
                {
                    "family_id": family,
                    "completed_call_count": len(calls_by_family[family]),
                    "call_shapes": [
                        {
                            **strict_json_bytes(shape_json.encode(), error="mechanical_call_shape_json_invalid"),
                            "completed_call_count": count,
                        }
                        for shape_json, count in sorted(shape_counts.items())
                    ],
                }
            )
        coverage_status = "verified_complete"
        family_call_counts = {
            family: len(attribution["family_planned_call_sha256s"][family])
            for family in strategy["query_family_ids"]
        }
        family_attribution_binding_status = "precommitted_runner_bound"
        attribution_plan_digest = attribution["attribution_plan_sha256"]
    else:
        coverage_status = "insufficient_proof"
        family_call_counts = None
        family_call_profiles = None
        family_attribution_binding_status = "legacy_unbound"
        attribution_plan_digest = None

    receipt = {
        "schema_version": MECHANICAL_RECEIPT_SCHEMA_VERSION,
        "wave_id": wave.wave_id,
        "result_sha256": result_sha256,
        "upstream_request_sha256": upstream_request_sha256,
        "prompt_sha256": bytes_sha256(wave.prompt_bytes),
        "session_id": session_id,
        "request_id": request_id,
        "model_id": model_id,
        "source_replay_status": "verified",
        "terminal_pairing": "exact_started_completed",
        "cli_turn_completed": True,
        "provider_terminal_verified": False,
        "provider_terminal_usage_verified": False,
        "assistant_output_sha256": bytes_sha256(assistant_output),
        "assistant_terminal_json_sha256": canonical_sha256(terminal_json),
        "assistant_output_binding_status": "terminal_json_canonical_match",
        "assistant_terminal_json_start_byte_offset": terminal_slice.start_byte_offset,
        "assistant_terminal_json_end_byte_offset_exclusive": terminal_slice.end_byte_offset_exclusive,
        "assistant_terminal_json_start_chunk_index": terminal_start_chunk_index,
        "assistant_terminal_json_end_chunk_index": terminal_end_chunk_index,
        "assistant_terminal_json_start_update_index": terminal_start_update_index,
        "assistant_terminal_json_end_update_index": terminal_end_update_index,
        "terminal_json_start_chunk_after_all_tool_events": True,
        "last_native_x_update_index": last_native_x_update_index,
        "final_assistant_update_index": final_assistant_update_index,
        "allowed_tool_registry_version": ALLOWED_TOOL_REGISTRY_VERSION,
        "source_payload_replay_status": "unavailable_model_mediated_only",
        "started_tool_calls": len(started_call_ids),
        "completed_tool_calls": len(completed_call_ids),
        "tool_counts": dict(sorted(Counter(call["tool_name"] for call in calls).items())),
        "call_identity_sha256s": call_hashes,
        "query_identity_sha256s": sorted({call["query_identity_sha256"] for call in calls}),
        "unique_query_count": len({call["query_identity_sha256"] for call in calls}),
        "raw_session_sha256": {
            name: bytes_sha256(wave.raw_session_files[name]) for name in sorted(RAW_SESSION_FILES)
        },
        "strategy_id": strategy["strategy_id"],
        "strategy_definition_sha256": strategy["strategy_definition_sha256"],
        "strategy_definition_binding_status": strategy_definition_binding_status,
        "query_family_ids": list(strategy["query_family_ids"]),
        "query_family_coverage_status": coverage_status,
        "query_family_call_counts": family_call_counts,
        "query_family_call_profiles": family_call_profiles,
        "family_attribution_binding_status": family_attribution_binding_status,
        "family_attribution_plan_sha256": attribution_plan_digest,
        "prior_exclusion_binding_status": execution_context["prior_exclusion_binding_status"],
        "user_visible_chat_context_sha256": observed_user_context_sha256,
        "user_visible_chat_context_binding_status": user_context_binding_status,
        "request_context_replay_status": (
            "complete"
            if execution_context["prior_exclusion_binding_status"] == "not_applicable"
            and user_context_binding_status == "precommitted_exact"
            else "operator_asserted"
        ),
    }
    _schema_guard(receipt, MECHANICAL_RECEIPT_SCHEMA_FILE, "mechanical_receipt_schema_invalid")
    return receipt


def _validate_evidence(evidence: Any) -> dict[str, Any]:
    evidence = _exact_keys(
        evidence,
        {"kind", "relationship", "author_handle", "post_id", "url", "published_at", "excerpt", "supports"},
        "evidence_shape_invalid",
    )
    kind = _nonempty_text(evidence["kind"], maximum=64, error="evidence_kind_invalid")
    relationship = _nonempty_text(evidence["relationship"], maximum=64, error="evidence_relationship_invalid")
    if _TOOL_NAME_RE.fullmatch(kind) is None or _TOOL_NAME_RE.fullmatch(relationship) is None:
        raise CampaignValidationError("evidence_classification_invalid")
    author_handle = _valid_handle(evidence["author_handle"])
    url, _ = _canonical_x_url(evidence["url"], expected_handle=author_handle)
    post_id = evidence["post_id"]
    if post_id is not None and (not isinstance(post_id, str) or not post_id.isdigit() or len(post_id) > 32):
        raise CampaignValidationError("evidence_post_id_invalid")
    if post_id is not None and ("/status/" not in url or not url.endswith(f"/status/{post_id}")):
        raise CampaignValidationError("evidence_post_binding_invalid")
    published_at = evidence["published_at"]
    if published_at is not None and (
        not isinstance(published_at, str)
        or len(published_at) > 40
        or _TIMESTAMP_RE.fullmatch(published_at) is None
    ):
        raise CampaignValidationError("evidence_published_at_invalid")
    excerpt = _nonempty_text(evidence["excerpt"], maximum=10_000, error="evidence_excerpt_invalid")
    supports = evidence["supports"]
    if not isinstance(supports, list):
        raise CampaignValidationError("evidence_supports_invalid")
    support_claims: list[dict[str, Any]] = []
    for item in supports:
        if isinstance(item, str):
            dimension = item
            asserted_value = None
        elif isinstance(item, Mapping) and set(item) == {"dimension", "asserted_value"}:
            dimension = item["dimension"]
            asserted_value = item["asserted_value"]
        else:
            raise CampaignValidationError("evidence_supports_invalid")
        if dimension not in DIMENSIONS or asserted_value not in TEMPORAL_STATES | {None}:
            raise CampaignValidationError("evidence_supports_invalid")
        support_claims.append(
            {
                "dimension": dimension,
                "asserted_value": asserted_value,
                "source_status": MODEL_MEDIATED_UNVERIFIED,
            }
        )
    if len({canonical_json(claim) for claim in support_claims}) != len(support_claims):
        raise CampaignValidationError("evidence_supports_invalid")
    return {
        "kind": kind,
        "relationship": relationship,
        "author_handle": author_handle,
        "post_id": post_id,
        "url": url,
        "published_at": published_at,
        "excerpt": excerpt,
        "support_claims": sorted(
            support_claims,
            key=lambda claim: (claim["dimension"], str(claim["asserted_value"])),
        ),
        "source_status": MODEL_MEDIATED_UNVERIFIED,
    }


def _validate_candidate(candidate: Any) -> dict[str, Any]:
    candidate = _exact_keys(
        candidate,
        {
            "handle",
            "profile_url",
            "platform_user_id",
            "bio_excerpt",
            "target_lab_affiliation_state",
            "pretraining_experience_state",
            "confidence",
            "evidence",
            "caveats",
        },
        "candidate_shape_invalid",
    )
    handle = _valid_handle(candidate["handle"])
    profile_url, _ = _canonical_x_url(candidate["profile_url"], expected_handle=handle)
    platform_user_id = candidate["platform_user_id"]
    if platform_user_id is not None and (
        not isinstance(platform_user_id, str) or not platform_user_id.isdigit() or len(platform_user_id) > 32
    ):
        raise CampaignValidationError("candidate_platform_user_id_invalid")
    bio_excerpt = candidate["bio_excerpt"]
    if bio_excerpt is not None:
        bio_excerpt = _nonempty_text(bio_excerpt, maximum=20_000, error="candidate_bio_excerpt_invalid")
    if any(candidate[dimension] not in TEMPORAL_STATES for dimension in DIMENSIONS):
        raise CampaignValidationError("candidate_temporal_state_invalid")
    if candidate["confidence"] not in {"high", "medium", "low"}:
        raise CampaignValidationError("candidate_confidence_invalid")
    evidence = candidate["evidence"]
    caveats = candidate["caveats"]
    if not isinstance(evidence, list) or not isinstance(caveats, list) or any(
        not isinstance(item, str) or len(item) > 10_000 for item in caveats
    ):
        raise CampaignValidationError("candidate_evidence_or_caveats_invalid")
    normalized_evidence: list[dict[str, Any]] = []
    rejected_evidence_items = 0
    for item in evidence:
        try:
            normalized_evidence.append(_validate_evidence(item))
        except CampaignValidationError as exc:
            if str(exc) != "x_url_author_handle_mismatch":
                raise
            rejected_evidence_items += 1
    return {
        "handle": handle,
        "profile_url": profile_url,
        "platform_user_id": platform_user_id,
        "bio_excerpt": bio_excerpt,
        "target_lab_affiliation_state": candidate["target_lab_affiliation_state"],
        "pretraining_experience_state": candidate["pretraining_experience_state"],
        "evidence": normalized_evidence,
        "raw_evidence_items": len(evidence),
        "rejected_evidence_items": rejected_evidence_items,
    }


def _validate_wave_payload(payload: Any) -> dict[str, Any]:
    payload = _exact_keys(
        payload,
        {
            "status",
            "status_reason",
            "native_x_tool_provenance",
            "counts",
            "candidates",
            "excluded_examples",
            "limitations",
            "local_reconciliation",
        },
        "wave_payload_shape_invalid",
    )
    if payload["status"] not in {"X_SEARCH_OK", "X_SEARCH_PARTIAL", "X_SEARCH_BLOCKED"}:
        raise CampaignValidationError("wave_status_invalid")
    _nonempty_text(payload["status_reason"], maximum=20_000, error="wave_status_reason_invalid")
    if not isinstance(payload["excluded_examples"], list) or not isinstance(payload["limitations"], list):
        raise CampaignValidationError("wave_narrative_collections_invalid")
    candidates = payload["candidates"]
    if not isinstance(candidates, list):
        raise CampaignValidationError("wave_candidates_invalid")
    counts = payload["counts"]
    if not isinstance(counts, Mapping):
        raise CampaignValidationError("wave_model_counts_invalid")
    model_candidate_count = counts.get("candidates_retained")
    model_observation_count = counts.get("observations_inspected_reported")
    if (
        not _is_int(model_candidate_count)
        or model_candidate_count < 0
        or not _is_int(model_observation_count)
        or model_observation_count < 0
    ):
        raise CampaignValidationError("wave_model_counts_invalid")
    provenance = payload["native_x_tool_provenance"]
    if not isinstance(provenance, Mapping):
        raise CampaignValidationError("wave_model_provenance_invalid")
    tool_calls = provenance.get("tool_calls_reported")
    queries = provenance.get("queries")
    if not _is_int(tool_calls) or tool_calls < 0 or not isinstance(queries, list):
        raise CampaignValidationError("wave_model_provenance_invalid")
    normalized_queries = [
        _nonempty_text(query, maximum=10_000, error="wave_model_query_invalid") for query in queries
    ]
    query_sha256s = sorted({hashlib.sha256(query.encode()).hexdigest() for query in normalized_queries})
    local = payload["local_reconciliation"]
    if not isinstance(local, Mapping):
        raise CampaignValidationError("wave_local_reconciliation_invalid")
    model_evidence_items = local.get("evidence_items_validated")
    if not _is_int(model_evidence_items) or model_evidence_items < 0:
        raise CampaignValidationError("wave_model_evidence_count_invalid")
    local_tool_counts = local.get("tool_counts")
    model_tool_counts: dict[str, int] | None = None
    if isinstance(local_tool_counts, Mapping):
        tentative: dict[str, int] = {}
        for key, value in local_tool_counts.items():
            if not isinstance(key, str) or _TOOL_NAME_RE.fullmatch(key) is None or not _is_int(value) or value < 0:
                tentative = {}
                break
            tentative[key] = value
        if tentative:
            model_tool_counts = tentative
    return {
        "candidates": [_validate_candidate(candidate) for candidate in candidates],
        "model_reported": {
            "candidate_rows": model_candidate_count,
            "evidence_items": model_evidence_items,
            "observations_inspected": model_observation_count,
            "tool_calls": tool_calls,
            "tool_counts": model_tool_counts,
            "query_count": len(normalized_queries),
            "unique_query_count": len(query_sha256s),
            "query_sha256s": query_sha256s,
        },
    }


def _evidence_key(evidence: Mapping[str, Any]) -> str:
    identity = dict(evidence)
    identity["author_handle"] = identity["author_handle"].casefold()
    identity["support_claims"] = sorted(
        identity["support_claims"],
        key=lambda claim: (claim["dimension"], str(claim["asserted_value"])),
    )
    return canonical_sha256(identity)


def _resolve_states(values: Sequence[str]) -> str | None:
    unique = sorted(set(values))
    if not unique:
        return None
    return unique[0] if len(unique) == 1 else "conflict"


def _dimension_summary(
    observations: Sequence[Mapping[str, Any]],
    dimension: str,
) -> dict[str, Any]:
    model_values = sorted({str(row[dimension]) for row in observations})
    return {
        "model_reported_values": model_values,
        "model_reported_resolution": _resolve_states(model_values),
        "evidence_supported_values": [],
        "evidence_supported_resolution": None,
        "evidence_support_status": MODEL_MEDIATED_UNVERIFIED,
    }


def _stop_advisory(
    wave_yields: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    points = []
    for wave in wave_yields:
        receipt = wave["mechanically_observed"]["receipt"]
        points.append(
            {
                "wave_id": wave["wave_id"],
                "replayed_completed_native_x_calls": receipt["completed_tool_calls"],
                "new_unique_handles": wave["yield"]["new_unique_handles"],
                "new_unique_handles_per_replayed_completed_native_x_call": wave["yield"][
                    "new_unique_handles_per_replayed_completed_native_x_call"
                ],
                "strategy_id": receipt["strategy_id"],
                "strategy_definition_sha256": receipt["strategy_definition_sha256"],
                "strategy_definition_binding_status": receipt[
                    "strategy_definition_binding_status"
                ],
                "comparability_rule_version": STRATEGY_COMPARABILITY_RULE_VERSION,
                "query_family_ids": receipt["query_family_ids"],
                "query_family_call_counts": receipt["query_family_call_counts"],
                "query_family_call_profiles": receipt["query_family_call_profiles"],
                "query_family_coverage_status": receipt["query_family_coverage_status"],
                "family_attribution_binding_status": receipt[
                    "family_attribution_binding_status"
                ],
                "request_context_replay_status": receipt["request_context_replay_status"],
            }
        )
    marginal = {
        "metric": "new_unique_handles_per_replayed_completed_native_x_call",
        "wave_points": points,
    }
    latest = points[-1]
    comparable: list[dict[str, Any]] = []
    excluded: list[dict[str, str]] = []
    for point in points:
        if point["request_context_replay_status"] != "complete":
            excluded.append({"wave_id": point["wave_id"], "reason": "request_context_binding_unverified"})
        elif latest["request_context_replay_status"] != "complete":
            excluded.append({"wave_id": point["wave_id"], "reason": "latest_request_context_unverified"})
        elif point["strategy_definition_binding_status"] != "versioned_complete":
            excluded.append({"wave_id": point["wave_id"], "reason": "strategy_definition_binding_unverified"})
        elif latest["strategy_definition_binding_status"] != "versioned_complete":
            excluded.append({"wave_id": point["wave_id"], "reason": "latest_strategy_definition_unverified"})
        elif point["family_attribution_binding_status"] != "precommitted_runner_bound":
            excluded.append({"wave_id": point["wave_id"], "reason": "family_attribution_binding_unverified"})
        elif latest["family_attribution_binding_status"] != "precommitted_runner_bound":
            excluded.append({"wave_id": point["wave_id"], "reason": "latest_family_attribution_unverified"})
        elif point["query_family_coverage_status"] != "verified_complete":
            excluded.append({"wave_id": point["wave_id"], "reason": "query_family_coverage_unverified"})
        elif latest["query_family_coverage_status"] != "verified_complete":
            excluded.append({"wave_id": point["wave_id"], "reason": "latest_wave_coverage_unverified"})
        elif point["strategy_definition_sha256"] != latest["strategy_definition_sha256"]:
            excluded.append({"wave_id": point["wave_id"], "reason": "strategy_definition_not_comparable"})
        elif point["query_family_call_counts"] != latest["query_family_call_counts"]:
            excluded.append({"wave_id": point["wave_id"], "reason": "family_call_mix_not_comparable"})
        elif point["query_family_call_profiles"] != latest["query_family_call_profiles"]:
            excluded.append({"wave_id": point["wave_id"], "reason": "family_call_profile_not_comparable"})
        elif point["strategy_id"] != latest["strategy_id"]:
            excluded.append({"wave_id": point["wave_id"], "reason": "strategy_id_not_comparable"})
        elif set(point["query_family_ids"]) != set(latest["query_family_ids"]):
            excluded.append({"wave_id": point["wave_id"], "reason": "query_family_set_not_comparable"})
        else:
            comparable.append(point)

    config = policy["stop_advisory"]
    minimum = config["minimum_comparable_waves"]
    lookback_count = config["lookback_comparable_waves"]
    if len(comparable) < minimum:
        evaluation_status = "insufficient_proof"
        recommendation = "continue_expansion"
        reason = "insufficient_comparable_strategy_evidence"
        lookback: list[dict[str, Any]] = comparable[-lookback_count:]
        nonincreasing: bool | None = None
        decline_fraction: float | None = None
    else:
        evaluation_status = "evaluated"
        lookback = comparable[-lookback_count:]
        productivity = [
            float(row["new_unique_handles_per_replayed_completed_native_x_call"])
            for row in lookback
        ]
        nonincreasing = all(
            current <= previous for previous, current in zip(productivity, productivity[1:], strict=False)
        )
        first, last = productivity[0], productivity[-1]
        decline_fraction = 0.0 if first == 0 else round(max(0.0, (first - last) / first), 6)
        plateau = (
            last <= config["max_latest_new_unique_per_completed_call_for_plateau"]
            and decline_fraction >= config["minimum_productivity_decline_fraction"]
            and (nonincreasing or not config["require_nonincreasing_productivity"])
        )
        if plateau:
            recommendation = "consider_stopping_after_manual_review"
            reason = "configured_call_productivity_plateau"
        else:
            recommendation = "continue_expansion"
            reason = "call_productivity_above_plateau_floor"
    advisory = {
        "evaluation_status": evaluation_status,
        "recommendation": recommendation,
        "reason": reason,
        "enforced": False,
        "candidate_count_triggered": False,
        "latest_wave_id": latest["wave_id"],
        "comparable_wave_ids": [row["wave_id"] for row in comparable],
        "excluded_wave_reasons": excluded,
        "lookback_wave_ids": [row["wave_id"] for row in lookback],
        "lookback_nonincreasing_productivity": nonincreasing,
        "lookback_productivity_decline_fraction": decline_fraction,
    }
    return marginal, advisory


def merge_campaign(
    request: Mapping[str, Any],
    policy: Mapping[str, Any],
    waves: Sequence[WaveInput],
) -> dict[str, Any]:
    """Replay and merge offline wave sources without imposing a business candidate cap."""

    validate_request(request)
    validate_policy(policy)
    bindings = request["waves"]
    if len(bindings) != len(waves) or len(waves) > policy["kill_ceilings"]["max_wave_files"]:
        raise CampaignValidationError("campaign_wave_input_count_invalid")
    if [binding["wave_id"] for binding in bindings] != [wave.wave_id for wave in waves]:
        raise CampaignValidationError("campaign_wave_input_order_invalid")

    ceiling = policy["kill_ceilings"]
    total_input_bytes = 0
    total_candidate_rows = 0
    total_evidence_rows = 0
    seen_handles: set[str] = set()
    candidate_accumulators: dict[str, dict[str, Any]] = {}
    stable_id_handles: dict[str, set[str]] = defaultdict(set)
    global_evidence_records: set[str] = set()
    global_candidate_evidence_associations: set[tuple[str, str]] = set()
    global_replayed_query_hashes: set[str] = set()
    global_model_query_hashes: set[str] = set()
    wave_yields: list[dict[str, Any]] = []
    model_tool_counts = Counter()
    replayed_tool_counts = Counter()
    model_tool_counts_complete = True
    model_candidate_rows = 0
    model_evidence_items = 0
    model_observations = 0
    model_tool_calls = 0
    model_query_count = 0
    raw_candidate_evidence_rows = 0

    for binding, wave in zip(bindings, waves, strict=True):
        source_size = len(wave.result_bytes)
        upstream_size = len(wave.upstream_request_bytes)
        prompt_size = len(wave.prompt_bytes)
        raw_sizes = [len(raw) for raw in wave.raw_session_files.values()]
        if source_size > ceiling["max_bytes_per_wave"] or any(
            size > ceiling["max_raw_session_file_bytes"] for size in raw_sizes
        ):
            raise CampaignValidationError("campaign_source_kill_ceiling_exceeded")
        total_input_bytes += source_size + upstream_size + prompt_size + sum(raw_sizes)
        if total_input_bytes > ceiling["max_total_input_bytes"]:
            raise CampaignValidationError("max_total_input_bytes_kill_ceiling_exceeded")

        result_sha256 = bytes_sha256(wave.result_bytes)
        upstream_sha256 = bytes_sha256(wave.upstream_request_bytes)
        prompt_sha256 = bytes_sha256(wave.prompt_bytes)
        if (
            result_sha256 != binding["source_sha256"]
            or upstream_sha256 != binding["upstream_request"]["sha256"]
            or prompt_sha256 != binding["prompt"]["sha256"]
        ):
            raise CampaignValidationError("campaign_wave_source_binding_mismatch")
        upstream = strict_json_bytes(
            wave.upstream_request_bytes,
            error="wave_request_json_invalid",
        )
        validate_wave_request(upstream)
        if (
            upstream["wave_id"] != wave.wave_id
            or upstream["target"] != request["target"]
            or upstream["prompt_sha256"] != prompt_sha256
        ):
            raise CampaignValidationError("wave_request_relabel_binding_mismatch")
        payload = strict_json_bytes(wave.result_bytes, error="wave_result_json_invalid")
        normalized = _validate_wave_payload(payload)
        receipt = _replay_raw_session(
            wave,
            upstream,
            result_sha256=result_sha256,
            upstream_request_sha256=upstream_sha256,
            result_payload=payload,
        )

        candidates = normalized["candidates"]
        model = normalized["model_reported"]
        raw_evidence_row_count = sum(row["raw_evidence_items"] for row in candidates)
        evidence_row_count = sum(len(row["evidence"]) for row in candidates)
        rejected_evidence_row_count = sum(row["rejected_evidence_items"] for row in candidates)
        total_candidate_rows += len(candidates)
        total_evidence_rows += raw_evidence_row_count
        if total_candidate_rows > ceiling["max_candidate_rows_in_memory"]:
            raise CampaignValidationError("max_candidate_rows_kill_ceiling_exceeded")
        if total_evidence_rows > ceiling["max_evidence_rows_in_memory"]:
            raise CampaignValidationError("max_evidence_rows_kill_ceiling_exceeded")

        wave_handles = {row["handle"].casefold() for row in candidates}
        new_handles = wave_handles - seen_handles
        previous_handles = wave_handles & seen_handles
        seen_handles.update(wave_handles)
        wave_evidence_records: set[str] = set()
        wave_associations: set[tuple[str, str]] = set()

        for row_index, row in enumerate(candidates):
            handle_key = row["handle"].casefold()
            accumulator = candidate_accumulators.setdefault(
                handle_key,
                {
                    "handle": row["handle"],
                    "handle_variants": set(),
                    "profile_urls": set(),
                    "platform_user_ids": set(),
                    "bio_excerpts": set(),
                    "wave_ids": set(),
                    "state_observations": [],
                    "evidence": {},
                },
            )
            accumulator["handle_variants"].add(row["handle"])
            accumulator["profile_urls"].add(row["profile_url"])
            accumulator["wave_ids"].add(wave.wave_id)
            if row["platform_user_id"] is not None:
                accumulator["platform_user_ids"].add(row["platform_user_id"])
                stable_id_handles[row["platform_user_id"]].add(handle_key)
            if row["bio_excerpt"] is not None:
                accumulator["bio_excerpts"].add(row["bio_excerpt"])

            observation = {
                "wave_id": wave.wave_id,
                "row_index": row_index,
                "target_lab_affiliation_state": row["target_lab_affiliation_state"],
                "pretraining_experience_state": row["pretraining_experience_state"],
                "source_status": MODEL_MEDIATED_UNVERIFIED,
            }
            accumulator["state_observations"].append(observation)
            for evidence in row["evidence"]:
                record_sha = _evidence_key(evidence)
                wave_evidence_records.add(record_sha)
                wave_associations.add((handle_key, record_sha))
                global_evidence_records.add(record_sha)
                global_candidate_evidence_associations.add((handle_key, record_sha))
                existing = accumulator["evidence"].setdefault(
                    record_sha,
                    {"record": evidence, "wave_ids": set()},
                )
                existing["wave_ids"].add(wave.wave_id)

        raw_candidate_evidence_rows += raw_evidence_row_count
        global_replayed_query_hashes.update(receipt["query_identity_sha256s"])
        global_model_query_hashes.update(model["query_sha256s"])
        model_candidate_rows += model["candidate_rows"]
        model_evidence_items += model["evidence_items"]
        model_observations += model["observations_inspected"]
        model_tool_calls += model["tool_calls"]
        model_query_count += model["query_count"]
        if model["tool_counts"] is None:
            model_tool_counts_complete = False
        else:
            model_tool_counts.update(model["tool_counts"])
        replayed_tool_counts.update(receipt["tool_counts"])
        receipt_sha256 = canonical_sha256(receipt)
        completed_calls = receipt["completed_tool_calls"]
        productivity = round(len(new_handles) / completed_calls, 8)
        wave_yields.append(
            {
                "wave_id": wave.wave_id,
                "source_binding": {
                    "wave_result_schema_version": upstream["wave_result_schema_version"],
                    "result_sha256": result_sha256,
                    "upstream_request_sha256": upstream_sha256,
                    "target_sha256": canonical_sha256(upstream["target"]),
                    "prompt_sha256": prompt_sha256,
                    "session_id": receipt["session_id"],
                    "request_id": receipt["request_id"],
                    "model_id": receipt["model_id"],
                    "strategy_id": receipt["strategy_id"],
                    "strategy_definition_sha256": receipt["strategy_definition_sha256"],
                    "query_family_ids": receipt["query_family_ids"],
                },
                "model_reported": model,
                "mechanically_observed": {
                    "parsed_candidate_rows": len(candidates),
                    "raw_candidate_evidence_association_rows": raw_evidence_row_count,
                    "valid_candidate_evidence_association_rows": evidence_row_count,
                    "rejected_evidence_association_rows": rejected_evidence_row_count,
                    "unique_evidence_records_in_wave": len(wave_evidence_records),
                    "unique_candidate_evidence_associations_in_wave": len(wave_associations),
                    "receipt": receipt,
                    "receipt_sha256": receipt_sha256,
                },
                "discrepancies": {
                    "parsed_minus_model_candidate_rows": len(candidates) - model["candidate_rows"],
                    "validated_minus_model_evidence_rows": evidence_row_count - model["evidence_items"],
                    "replayed_minus_model_tool_calls": completed_calls - model["tool_calls"],
                },
                "yield": {
                    "raw_candidate_rows": len(candidates),
                    "unique_handles_in_wave": len(wave_handles),
                    "duplicate_candidate_rows_in_wave": len(candidates) - len(wave_handles),
                    "new_unique_handles": len(new_handles),
                    "previously_seen_unique_handles": len(previous_handles),
                    "cumulative_unique_handles": len(seen_handles),
                    "new_unique_handles_per_replayed_completed_native_x_call": productivity,
                },
            }
        )

    candidates_output: list[dict[str, Any]] = []
    model_state_conflicts = 0
    evidence_state_conflicts = 0
    stable_id_conflict_candidates = 0
    candidates_with_evidence = 0
    candidates_with_bio = 0
    candidates_with_stable_id = 0
    for handle_key, accumulator in sorted(candidate_accumulators.items()):
        observations = accumulator["state_observations"]
        state_summary = {
            dimension: _dimension_summary(observations, dimension) for dimension in DIMENSIONS
        }
        model_conflict = any(
            state_summary[dimension]["model_reported_resolution"] == "conflict"
            for dimension in DIMENSIONS
        )
        evidence_conflict = any(
            state_summary[dimension]["evidence_supported_resolution"] == "conflict"
            for dimension in DIMENSIONS
        )
        model_state_conflicts += model_conflict
        evidence_state_conflicts += evidence_conflict
        platform_ids = sorted(accumulator["platform_user_ids"])
        stable_id_conflict_candidates += len(platform_ids) > 1
        candidates_with_stable_id += bool(platform_ids)
        candidates_with_bio += bool(accumulator["bio_excerpts"])
        candidates_with_evidence += bool(accumulator["evidence"])
        evidence_output = []
        for record_sha, record in sorted(accumulator["evidence"].items()):
            evidence_output.append(
                {
                    **record["record"],
                    "evidence_record_sha256": record_sha,
                    "observed_in_wave_ids": sorted(record["wave_ids"]),
                }
            )
        candidates_output.append(
            {
                "handle_key": handle_key,
                "handle": accumulator["handle"],
                "handle_variants": sorted(accumulator["handle_variants"], key=lambda item: (item.casefold(), item)),
                "profile_urls": sorted(accumulator["profile_urls"]),
                "platform_user_ids": platform_ids,
                "multiple_stable_ids_for_handle": len(platform_ids) > 1,
                "bio_excerpts": sorted(accumulator["bio_excerpts"]),
                "observed_in_wave_ids": sorted(accumulator["wave_ids"]),
                "state_observations": observations,
                "state_summary": state_summary,
                "model_reported_state_conflict": model_conflict,
                "evidence_supported_state_conflict": evidence_conflict,
                "field_source_status": {
                    "candidate_identity": MODEL_MEDIATED_UNVERIFIED,
                    "profile_url": MODEL_MEDIATED_UNVERIFIED,
                    "platform_user_id": MODEL_MEDIATED_UNVERIFIED,
                    "bio_excerpt": MODEL_MEDIATED_UNVERIFIED,
                    "temporal_state": MODEL_MEDIATED_UNVERIFIED,
                    "evidence_excerpt": MODEL_MEDIATED_UNVERIFIED,
                },
                "evidence": evidence_output,
            }
        )

    reverse_index = []
    multi_handle_stable_id_conflicts = 0
    for platform_user_id, handle_keys in sorted(stable_id_handles.items()):
        sorted_handles = sorted(handle_keys)
        if len(sorted_handles) > 1:
            multi_handle_stable_id_conflicts += 1
            proposal = {
                "proposal_id": canonical_sha256(
                    {"platform_user_id": platform_user_id, "handle_keys": sorted_handles}
                ),
                "type": "reversible_handle_history_review",
                "handle_keys": sorted_handles,
                "reason": "same_model_reported_platform_id_across_multiple_handles",
                "review_required": True,
                "auto_merge_authorized": False,
                "reversible": True,
            }
            status = "multi_handle_conflict"
        else:
            proposal = None
            status = "single_handle"
        reverse_index.append(
            {
                "platform_user_id": platform_user_id,
                "platform_user_id_status": MODEL_MEDIATED_UNVERIFIED,
                "handle_keys": sorted_handles,
                "status": status,
                "handle_history_proposal": proposal,
            }
        )

    total_unique = len(candidates_output)
    replayed_completed_calls = sum(row["completed_tool_calls"] for row in (
        wave["mechanically_observed"]["receipt"] for wave in wave_yields
    ))
    marginal_yield, stop_advisory = _stop_advisory(wave_yields, policy)
    metrics = {
        "wave_count": len(waves),
        "raw_candidate_rows": total_candidate_rows,
        "total_unique_handles": total_unique,
        "duplicate_candidate_rows": total_candidate_rows - total_unique,
        "raw_candidate_evidence_association_rows": raw_candidate_evidence_rows,
        "valid_candidate_evidence_association_rows": sum(
            wave["mechanically_observed"]["valid_candidate_evidence_association_rows"]
            for wave in wave_yields
        ),
        "rejected_evidence_association_rows": sum(
            wave["mechanically_observed"]["rejected_evidence_association_rows"] for wave in wave_yields
        ),
        "unique_evidence_records": len(global_evidence_records),
        "unique_candidate_evidence_associations": len(global_candidate_evidence_associations),
        "duplicate_candidate_evidence_association_rows": (
            sum(
                wave["mechanically_observed"]["valid_candidate_evidence_association_rows"]
                for wave in wave_yields
            )
            - len(global_candidate_evidence_associations)
        ),
        "candidates_with_evidence": candidates_with_evidence,
        "evidence_coverage_rate": _fraction(candidates_with_evidence, total_unique),
        "candidates_with_bio": candidates_with_bio,
        "bio_coverage_rate": _fraction(candidates_with_bio, total_unique),
        "candidates_with_stable_id": candidates_with_stable_id,
        "stable_id_coverage_rate": _fraction(candidates_with_stable_id, total_unique),
        "model_reported_state_conflict_candidates": model_state_conflicts,
        "evidence_supported_state_conflict_candidates": evidence_state_conflicts,
        "multiple_stable_ids_for_handle_candidates": stable_id_conflict_candidates,
        "multi_handle_stable_id_conflicts": multi_handle_stable_id_conflicts,
        "model_reported_candidate_rows": model_candidate_rows,
        "model_reported_evidence_items": model_evidence_items,
        "model_reported_observations": model_observations,
        "model_reported_tool_calls": model_tool_calls,
        "model_reported_tool_counts": dict(sorted(model_tool_counts.items())) if model_tool_counts_complete else None,
        "model_reported_query_count": model_query_count,
        "model_reported_global_unique_query_count": len(global_model_query_hashes),
        "replayed_completed_native_x_calls": replayed_completed_calls,
        "replayed_tool_counts": dict(sorted(replayed_tool_counts.items())),
        "replayed_global_unique_query_count": len(global_replayed_query_hashes),
    }
    result = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "campaign_id": request["campaign_id"],
        "target": dict(request["target"]),
        "input_binding": {
            "request_sha256": canonical_sha256(request),
            "policy_version": policy["policy_version"],
            "policy_sha256": canonical_sha256(policy),
            "contract_schema_sha256": {
                filename: contract_schema_sha256(filename) for filename in CONTRACT_SCHEMA_FILES
            },
            "wave_count": len(waves),
        },
        "authority": {
            "offline_only": True,
            "provider_calls_performed": False,
            "business_candidate_limit": None,
            "stop_advisory_enforced": False,
            "product_write_authorized": False,
            "outreach_authorized": False,
            "canonical_person_merge_authorized": False,
        },
        "provenance": {
            "raw_tool_source_payload_available": False,
            "candidate_identity_status": MODEL_MEDIATED_UNVERIFIED,
            "profile_bio_status": MODEL_MEDIATED_UNVERIFIED,
            "platform_user_id_status": MODEL_MEDIATED_UNVERIFIED,
            "evidence_excerpt_status": MODEL_MEDIATED_UNVERIFIED,
            "temporal_state_status": MODEL_MEDIATED_UNVERIFIED,
        },
        "wave_yields": wave_yields,
        "metrics": metrics,
        "marginal_yield": marginal_yield,
        "stop_advisory": stop_advisory,
        "identity_index": {
            "stable_id_reverse_index": reverse_index,
            "multi_handle_stable_id_conflict_count": multi_handle_stable_id_conflicts,
        },
        "candidates": candidates_output,
    }
    validate_campaign_result(result)
    return result


def validate_campaign_result(result: Any) -> None:
    """Validate shape plus the high-value semantic invariants of a persisted result."""

    _schema_guard(result, RESULT_SCHEMA_FILE, "campaign_result_schema_invalid")
    if not isinstance(result, Mapping):
        raise CampaignValidationError("campaign_result_shape_invalid")
    expected_authority = {
        "offline_only": True,
        "provider_calls_performed": False,
        "business_candidate_limit": None,
        "stop_advisory_enforced": False,
        "product_write_authorized": False,
        "outreach_authorized": False,
        "canonical_person_merge_authorized": False,
    }
    if result.get("schema_version") != RESULT_SCHEMA_VERSION or result.get("authority") != expected_authority:
        raise CampaignValidationError("campaign_result_authority_invalid")
    expected_provenance = {
        "raw_tool_source_payload_available": False,
        "candidate_identity_status": MODEL_MEDIATED_UNVERIFIED,
        "profile_bio_status": MODEL_MEDIATED_UNVERIFIED,
        "platform_user_id_status": MODEL_MEDIATED_UNVERIFIED,
        "evidence_excerpt_status": MODEL_MEDIATED_UNVERIFIED,
        "temporal_state_status": MODEL_MEDIATED_UNVERIFIED,
    }
    if result.get("provenance") != expected_provenance:
        raise CampaignValidationError("campaign_result_provenance_invalid")
    binding = result["input_binding"]
    expected_schema_hashes = {
        filename: contract_schema_sha256(filename) for filename in CONTRACT_SCHEMA_FILES
    }
    if binding["contract_schema_sha256"] != expected_schema_hashes:
        raise CampaignValidationError("campaign_result_schema_binding_invalid")
    if binding["wave_count"] != len(result["wave_yields"]):
        raise CampaignValidationError("campaign_result_wave_count_invalid")

    candidate_handle_keys: set[str] = set()
    expected_stable_index: dict[str, set[str]] = defaultdict(set)
    evidence_records: set[str] = set()
    associations: set[tuple[str, str]] = set()
    expected_field_source_status = {
        "candidate_identity": MODEL_MEDIATED_UNVERIFIED,
        "profile_url": MODEL_MEDIATED_UNVERIFIED,
        "platform_user_id": MODEL_MEDIATED_UNVERIFIED,
        "bio_excerpt": MODEL_MEDIATED_UNVERIFIED,
        "temporal_state": MODEL_MEDIATED_UNVERIFIED,
        "evidence_excerpt": MODEL_MEDIATED_UNVERIFIED,
    }
    for candidate in result["candidates"]:
        handle_key = candidate["handle_key"]
        if handle_key in candidate_handle_keys or candidate["handle"].casefold() != handle_key:
            raise CampaignValidationError("campaign_result_candidate_identity_invalid")
        candidate_handle_keys.add(handle_key)
        if candidate["field_source_status"] != expected_field_source_status:
            raise CampaignValidationError("campaign_result_candidate_source_status_invalid")
        if candidate["multiple_stable_ids_for_handle"] != (len(candidate["platform_user_ids"]) > 1):
            raise CampaignValidationError("campaign_result_stable_id_state_invalid")
        for platform_user_id in candidate["platform_user_ids"]:
            expected_stable_index[platform_user_id].add(handle_key)
        observations = candidate["state_observations"]
        for observation in observations:
            if (
                any(observation[dimension] not in TEMPORAL_STATES for dimension in DIMENSIONS)
                or observation["source_status"] != MODEL_MEDIATED_UNVERIFIED
            ):
                raise CampaignValidationError("campaign_result_state_observation_invalid")
        expected_summary = {
            dimension: _dimension_summary(observations, dimension) for dimension in DIMENSIONS
        }
        if candidate["state_summary"] != expected_summary:
            raise CampaignValidationError("campaign_result_state_summary_invalid")
        if candidate["model_reported_state_conflict"] != any(
            expected_summary[dimension]["model_reported_resolution"] == "conflict"
            for dimension in DIMENSIONS
        ):
            raise CampaignValidationError("campaign_result_model_state_conflict_invalid")
        if candidate["evidence_supported_state_conflict"] != any(
            expected_summary[dimension]["evidence_supported_resolution"] == "conflict"
            for dimension in DIMENSIONS
        ):
            raise CampaignValidationError("campaign_result_evidence_state_conflict_invalid")
        for evidence in candidate["evidence"]:
            normalized = {
                key: evidence[key]
                for key in (
                    "kind",
                    "relationship",
                    "author_handle",
                    "post_id",
                    "url",
                    "published_at",
                    "excerpt",
                    "support_claims",
                    "source_status",
                )
            }
            if normalized["source_status"] != MODEL_MEDIATED_UNVERIFIED or any(
                claim["source_status"] != MODEL_MEDIATED_UNVERIFIED
                for claim in normalized["support_claims"]
            ):
                raise CampaignValidationError("campaign_result_evidence_source_status_invalid")
            record_sha = _evidence_key(normalized)
            if evidence["evidence_record_sha256"] != record_sha:
                raise CampaignValidationError("campaign_result_evidence_hash_invalid")
            evidence_records.add(record_sha)
            associations.add((handle_key, record_sha))

    expected_reverse = []
    expected_conflicts = 0
    for platform_user_id, handles in sorted(expected_stable_index.items()):
        sorted_handles = sorted(handles)
        if len(sorted_handles) > 1:
            expected_conflicts += 1
            proposal = {
                "proposal_id": canonical_sha256(
                    {"platform_user_id": platform_user_id, "handle_keys": sorted_handles}
                ),
                "type": "reversible_handle_history_review",
                "handle_keys": sorted_handles,
                "reason": "same_model_reported_platform_id_across_multiple_handles",
                "review_required": True,
                "auto_merge_authorized": False,
                "reversible": True,
            }
            status = "multi_handle_conflict"
        else:
            proposal = None
            status = "single_handle"
        expected_reverse.append(
            {
                "platform_user_id": platform_user_id,
                "platform_user_id_status": MODEL_MEDIATED_UNVERIFIED,
                "handle_keys": sorted_handles,
                "status": status,
                "handle_history_proposal": proposal,
            }
        )
    if result["identity_index"] != {
        "stable_id_reverse_index": expected_reverse,
        "multi_handle_stable_id_conflict_count": expected_conflicts,
    }:
        raise CampaignValidationError("campaign_result_identity_index_invalid")

    replayed_calls = 0
    replayed_queries: set[str] = set()
    for wave in result["wave_yields"]:
        observed = wave["mechanically_observed"]
        receipt = observed["receipt"]
        _schema_guard(receipt, MECHANICAL_RECEIPT_SCHEMA_FILE, "campaign_result_receipt_schema_invalid")
        if observed["receipt_sha256"] != canonical_sha256(receipt):
            raise CampaignValidationError("campaign_result_receipt_hash_invalid")
        if wave["wave_id"] != receipt["wave_id"]:
            raise CampaignValidationError("campaign_result_receipt_wave_invalid")
        if wave["source_binding"]["result_sha256"] != receipt["result_sha256"]:
            raise CampaignValidationError("campaign_result_source_receipt_invalid")
        if wave["source_binding"]["upstream_request_sha256"] != receipt["upstream_request_sha256"]:
            raise CampaignValidationError("campaign_result_source_receipt_invalid")
        if wave["source_binding"]["prompt_sha256"] != receipt["prompt_sha256"]:
            raise CampaignValidationError("campaign_result_source_receipt_invalid")
        if (
            wave["source_binding"]["strategy_definition_sha256"]
            != receipt["strategy_definition_sha256"]
        ):
            raise CampaignValidationError("campaign_result_source_receipt_invalid")
        if (
            receipt["assistant_terminal_json_start_update_index"]
            <= receipt["last_native_x_update_index"]
            or receipt["assistant_terminal_json_start_update_index"]
            > receipt["assistant_terminal_json_end_update_index"]
            or receipt["assistant_terminal_json_end_update_index"]
            > receipt["final_assistant_update_index"]
            or receipt["assistant_terminal_json_start_chunk_index"]
            > receipt["assistant_terminal_json_end_chunk_index"]
            or receipt["assistant_terminal_json_start_byte_offset"]
            >= receipt["assistant_terminal_json_end_byte_offset_exclusive"]
            or receipt["source_payload_replay_status"] != "unavailable_model_mediated_only"
        ):
            raise CampaignValidationError("campaign_result_receipt_causality_or_source_invalid")
        request_context_complete = (
            receipt["prior_exclusion_binding_status"] == "not_applicable"
            and receipt["user_visible_chat_context_binding_status"] == "precommitted_exact"
        )
        if (receipt["request_context_replay_status"] == "complete") != request_context_complete:
            raise CampaignValidationError("campaign_result_request_context_status_invalid")
        coverage_complete = receipt["query_family_coverage_status"] == "verified_complete"
        if coverage_complete:
            profiles = receipt["query_family_call_profiles"]
            counts = receipt["query_family_call_counts"]
            if (
                receipt["strategy_definition_binding_status"] != "versioned_complete"
                or receipt["family_attribution_binding_status"] != "precommitted_runner_bound"
                or receipt["family_attribution_plan_sha256"] is None
                or profiles is None
                or counts is None
                or [profile["family_id"] for profile in profiles] != receipt["query_family_ids"]
                or {
                    profile["family_id"]: profile["completed_call_count"]
                    for profile in profiles
                }
                != counts
                or any(
                    sum(shape["completed_call_count"] for shape in profile["call_shapes"])
                    != profile["completed_call_count"]
                    for profile in profiles
                )
                or sum(counts.values()) != receipt["completed_tool_calls"]
            ):
                raise CampaignValidationError("campaign_result_family_profile_binding_invalid")
        elif (
            receipt["query_family_call_profiles"] is not None
            or receipt["query_family_call_counts"] is not None
            or receipt["family_attribution_plan_sha256"] is not None
            or receipt["family_attribution_binding_status"] != "legacy_unbound"
        ):
            raise CampaignValidationError("campaign_result_unverified_family_profile_invalid")
        replayed_calls += receipt["completed_tool_calls"]
        replayed_queries.update(receipt["query_identity_sha256s"])
        expected_productivity = round(
            wave["yield"]["new_unique_handles"] / receipt["completed_tool_calls"], 8
        )
        if wave["yield"]["new_unique_handles_per_replayed_completed_native_x_call"] != expected_productivity:
            raise CampaignValidationError("campaign_result_productivity_invalid")
    metrics = result["metrics"]
    if (
        metrics["total_unique_handles"] != len(candidate_handle_keys)
        or metrics["unique_evidence_records"] != len(evidence_records)
        or metrics["unique_candidate_evidence_associations"] != len(associations)
        or metrics["raw_candidate_evidence_association_rows"]
        != metrics["valid_candidate_evidence_association_rows"]
        + metrics["rejected_evidence_association_rows"]
        or metrics["duplicate_candidate_evidence_association_rows"]
        != metrics["valid_candidate_evidence_association_rows"] - len(associations)
        or metrics["replayed_completed_native_x_calls"] != replayed_calls
        or metrics["replayed_global_unique_query_count"] != len(replayed_queries)
        or result["stop_advisory"]["enforced"] is not False
        or result["stop_advisory"]["candidate_count_triggered"] is not False
    ):
        raise CampaignValidationError("campaign_result_metrics_invalid")


def replay_and_validate_campaign_result(
    persisted_result: Any,
    request: Mapping[str, Any],
    policy: Mapping[str, Any],
    waves: Sequence[WaveInput],
) -> dict[str, Any]:
    """Rebuild a persisted artifact from its sources and reject any byte-level semantic drift."""

    validate_campaign_result(persisted_result)
    expected = merge_campaign(request, policy, waves)
    if canonical_json(persisted_result) != canonical_json(expected):
        raise CampaignValidationError("persisted_campaign_replay_mismatch")
    return expected
