"""Fail-closed operator for adaptive Grok native-X recall waves.

The operator deliberately separates business recall from runtime safety.  It
does not impose candidate, observation, query, or X-tool-call quotas.  The only
execution brakes are an operator-owned model-turn ceiling and a monotonic wall
deadline with process-group TERM/KILL cleanup.

The public CLI defaults to an offline fixture lane.  The live lane requires an
explicit gate and consumes a request-scoped approval before a process is
started.  Results remain private experiment artifacts; they do not authorize
product, identity, CRM, export, ranking, or outreach writes.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import platform
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

from x_first.grok_cli_exploration import (
    BASE_DISCOVERY_TOOL_ARGUMENT_POLICY_VERSION,
    base_discovery_tool_arguments_allowed,
)
from x_first.native_x_evidence_contract import (
    QUERY_SURFACES,
    SUPPORT_DIMENSIONS,
    TEMPORAL_STATES,
    THREAD_RELATIONS,
    classify_single_handle_query_surface,
    normalize_support_claims,
)

REQUEST_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.request.v2"
RESULT_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.result.v2"
INTENT_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.intent.v2"
RECEIPT_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.operator_receipt.v3"
GRANT_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.live_grant.v2"
CONSUMPTION_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.live_grant_consumption.v2"
PROCESS_LEDGER_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.process_ledger.v2"
DELETION_JOURNAL_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.deletion_journal.v1"
DELETION_RECEIPT_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.deletion_receipt.v1"

PROVIDER_ID = "grok_cli_oauth"
DEFAULT_MODEL_ID = "grok-4.5"
DEFAULT_RUNTIME_ROOT = Path(__file__).resolve().parents[2] / "runtime/adaptive-grok-waves"
DEFAULT_APPROVAL_ROOT = Path.home() / ".local/state/x-first-researcher-sourcing/adaptive-grok-wave-approvals/v2"
DEFAULT_DELETION_ROOT = Path.home() / ".local/state/x-first-researcher-sourcing/adaptive-grok-wave-deletions/v2"
DEFAULT_GROK_BINARY = Path.home() / ".grok/bin/grok"
DEFAULT_GROK_AUTH = Path.home() / ".grok/auth.json"
MAX_GROK_BINARY_BYTES = 268_435_456
MAX_CONTRACT_SCHEMA_BYTES = 16_777_216
MAX_EFFECTIVE_PROMPT_POLICY_BYTES = 4_194_304
MAX_SESSION_TREE_DEPTH = 64
FINAL_SESSION_TREE_SCAN_BUDGET_SECONDS = 5.0
DEFAULT_EFFECTIVE_PROMPT_POLICY = (
    Path(__file__).resolve().parents[2] / "configs/adaptive_grok_wave_effective_prompt_policy.v1.json"
)
EFFECTIVE_PROMPT_POLICY_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.effective_prompt_policy.v1"
EFFECTIVE_PROMPT_POLICY_ID = "adaptive_base_discovery_effective_prompts.v1"
EFFECTIVE_PROMPT_POLICY_OWNER = "x_first_adaptive_wave_operator"
EFFECTIVE_PROMPT_POLICY_BINDING_VERSION = "adaptive-effective-prompt-entry-semantics-v1"
ALLOWED_DISCOVERY_DIMENSIONS = (
    "target_lab_affiliation",
    "professional_role_or_function",
    "public_research_evidence",
    "pretraining_relevance",
)

DISALLOWED_TOOLS = (
    "run_terminal_cmd",
    "grep",
    "read_file",
    "search_replace",
    "list_dir",
    "web_search",
    "web_fetch",
    "todo_write",
    "task",
    "Agent",
)
NATIVE_X_TOOLS = (
    "x_keyword_search",
    "x_semantic_search",
    "x_user_search",
    "x_thread_fetch",
)
AUTHORITY = {
    "canonical_identity_write_authorized": False,
    "outreach_authorized": False,
    "product_write_authorized": False,
    "protected_identity_inference_authorized": False,
    "provider_fallback_authorized": False,
}

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_ID_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,127}")
_REQUEST_ID_RE = re.compile(r"xwave_req_[0-9a-f]{32}")
_RUN_ID_RE = re.compile(r"grok_wave_(?:fixture|live)_[0-9a-f]{32}")
_SESSION_ID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")
_CANONICAL_TIME_RE = re.compile(
    r"[0-9]{4}-(?:0[1-9]|1[0-2])-(?:[0-2][0-9]|3[01])T"
    r"(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\.[0-9]{3}Z"
)
_PENDING_RE = re.compile(r"\.pending-(?P<name>[a-z0-9_.-]{1,96})-[0-9a-f]{32}")

_REQUEST_KEYS = {
    "schema_version",
    "request_id",
    "target",
    "prompt_source",
    "prior_waves",
    "transport",
    "emergency",
    "technical_limits",
    "budget",
    "retention",
    "approval",
    "authority",
}
_TARGET_KEYS = {"lab_id", "research_focus_id", "scope"}
_PROMPT_SOURCE_KEYS = {"path", "sha256"}
_PRIOR_WAVE_KEYS = {"wave_id", "path", "sha256"}
_TRANSPORT_KEYS = {
    "provider_id",
    "model_id",
    "reasoning_effort",
    "grok_binary_sha256",
    "operator_account_ref",
    "oauth_auth_sha256",
}
_EMERGENCY_KEYS = {"max_turns", "deadline_ms", "term_grace_ms", "kill_grace_ms"}
_TECHNICAL_LIMIT_KEYS = {
    "max_stdout_bytes",
    "max_stderr_bytes",
    "max_json_bytes",
    "max_json_depth",
    "max_json_nodes",
    "max_prompt_bytes",
    "max_compiled_prompt_bytes",
    "max_prior_wave_bytes",
    "max_total_prior_wave_bytes",
    "max_prior_json_depth",
    "max_prior_json_nodes",
    "max_session_files",
    "max_session_file_bytes",
    "max_session_total_bytes",
    "max_session_updates_bytes",
    "max_session_update_line_bytes",
}
_BUDGET_KEYS = {
    "pricing_policy_id",
    "max_total_tokens",
    "max_cost_usd_micros",
    "input_token_cost_usd_micros_per_million",
    "output_token_cost_usd_micros_per_million",
}
_RETENTION_KEYS = {"policy_id", "ttl_seconds", "deletion_receipt_required"}
_APPROVAL_KEYS = {"grant_id"}
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
_PROVENANCE_KEYS = {"tools_reported", "tool_calls_reported", "queries", "generic_web_used"}
_COUNT_KEYS = {"observations_inspected_reported", "candidates_retained"}
_HEADLESS_ENVELOPE_KEYS = {"text", "stopReason", "sessionId", "requestId", "num_turns", "usage"}
_HEADLESS_OPTIONAL_KEYS = {"total_cost_usd"}
_HEADLESS_USAGE_KEYS = {"input_tokens", "output_tokens", "total_tokens"}
_CANDIDATE_KEYS = {
    "handle",
    "profile_url",
    "platform_user_id",
    "bio_excerpt",
    "target_lab_affiliation_state",
    "pretraining_experience_state",
    "confidence",
    "evidence",
    "caveats",
    "overlap_status",
}
_EVIDENCE_KEYS = {
    "kind",
    "relationship",
    "subject_handle",
    "author_handle",
    "post_id",
    "url",
    "published_at",
    "excerpt",
    "thread_relation",
    "supports",
}
_SUPPORT_CLAIM_KEYS = {"dimension", "asserted_value"}
_EXCLUDED_KEYS = {"handle", "reason"}
_RECONCILIATION_KEYS = {
    "candidate_records_validated",
    "evidence_items_validated",
    "post_urls_structurally_validated",
    "provider_post_bodies_replayable",
    "tool_calls_completed",
    "tool_counts",
}
_INTENT_KEYS = {
    "schema_version",
    "run_id",
    "request_id",
    "request_sha256",
    "execution_mode",
    "started_at",
    "run_lease_sha256",
    "input_binding",
    "command_binding",
    "approval",
    "emergency",
    "technical_limits",
    "budget",
    "retention",
    "runtime_layout",
    "authority",
}
_RECEIPT_KEYS = {
    "schema_version",
    "run_id",
    "request_id",
    "request_sha256",
    "execution_mode",
    "status",
    "run_lease_sha256",
    "input_binding",
    "command_binding",
    "approval",
    "process",
    "artifacts",
    "session_proof",
    "retention",
    "reconciliation",
    "authority",
}
_INPUT_BINDING_KEYS = {
    "target_sha256",
    "source_prompt_sha256",
    "compiled_prompt_sha256",
    "prior_waves",
    "prior_unique_handle_count",
    "prior_handle_set_sha256",
}
_COMMAND_BINDING_KEYS = {
    "provider_id",
    "model_id",
    "reasoning_effort",
    "session_id",
    "grok_binary_sha256",
    "structured_output_schema_sha256",
    "argv_sha256",
    "command_policy_sha256",
    "environment_policy_sha256",
    "tool_registry_sha256",
    "effective_prompt_policy_sha256",
    "effective_prompt_policy_entry_id",
    "operator_account_ref_sha256",
    "oauth_auth_sha256",
    "max_turns",
}
_APPROVAL_BINDING_KEYS = {"required", "grant_id_sha256", "grant_sha256", "consumption_sha256"}
_PROCESS_KEYS = {
    "started_at",
    "completed_at",
    "elapsed_ms",
    "exit_code",
    "timed_out",
    "term_sent",
    "kill_sent",
    "process_spawn_attempted",
    "child_pid",
    "process_group_id",
    "process_ledger_sha256",
    "kernel_birth_identity_sha256",
    "process_identity_token_sha256",
    "process_group_cleanup_confirmed",
    "execution_error_code",
    "deadline_ms",
    "term_grace_ms",
    "kill_grace_ms",
    "fallback_used",
    "technical_limit_exceeded",
    "technical_limit_kind",
}
_ARTIFACT_KEYS = {
    "raw_stdout_sha256",
    "stderr_sha256",
    "sanitized_output_sha256",
    "structured_output_compliant",
    "structured_output_contract_valid",
    "non_json_prefix_bytes",
    "non_json_suffix_bytes",
    "compiled_prompt_sha256",
    "session_updates_sha256",
    "ephemeral_tree_deleted",
    "session_tree_file_count",
    "session_tree_entry_count",
    "session_tree_max_depth",
    "session_tree_total_bytes",
    "session_tree_max_file_bytes",
}
_SESSION_PROOF_KEYS = {
    "status",
    "updates_sha256",
    "update_bytes",
    "event_count",
    "provider_prompt_id_sha256",
    "effective_model_id",
    "started_tool_calls",
    "completed_tool_calls",
    "tool_counts",
    "query_argument_sha256s",
    "candidate_surface_attempts",
    "terminal_stop_reason",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "model_turns",
    "estimated_cost_usd_micros",
}
_RETENTION_RECEIPT_KEYS = {
    "policy_id",
    "ttl_seconds",
    "delete_after",
    "purge_state",
    "deletion_receipt_required",
}
_RECONCILIATION_RECEIPT_KEYS = {
    "candidate_count",
    "evidence_count",
    "post_url_count",
    "prior_overlap_count",
    "verified_material_update_count",
    "model_reported_tool_calls",
    "mechanically_verified_tool_calls",
    "tool_fact_source",
    "candidate_surface_coverage",
}
_RUNTIME_LAYOUT_KEYS = {
    "compiled_prompt_name",
    "stdout_spool_name",
    "stderr_spool_name",
    "ephemeral_home_name",
    "session_updates_name",
    "run_lock_name",
}
_PROCESS_LEDGER_KEYS = {
    "schema_version",
    "run_id",
    "request_id",
    "run_lease_sha256",
    "session_id",
    "child_pid",
    "process_group_id",
    "kernel_birth_identity",
    "process_identity_token",
    "spawned_at",
}
_GRANT_KEYS = {
    "schema_version",
    "grant_id_hash",
    "execution_scope_sha256",
    "request_id",
    "target_sha256",
    "model_id",
    "grok_binary_sha256",
    "operator_account_ref_sha256",
    "oauth_auth_sha256",
    "result_schema_sha256",
    "command_policy_sha256",
    "tool_registry_sha256",
    "effective_prompt_policy_sha256",
    "effective_prompt_policy_entry_id",
    "environment_policy_sha256",
    "emergency_sha256",
    "technical_limits_sha256",
    "budget_sha256",
    "retention_sha256",
    "issued_at",
    "expires_at",
    "issuer",
    "state",
}
_CONSUMPTION_KEYS = {
    "schema_version",
    "grant_id_hash",
    "grant_sha256",
    "execution_scope_sha256",
    "request_sha256",
    "run_id",
    "run_lease_sha256",
    "consumed_at",
    "state",
}
_DELETION_JOURNAL_KEYS = {
    "schema_version",
    "run_id",
    "request_sha256",
    "run_lease_sha256",
    "operator_receipt_sha256",
    "delete_after",
    "runtime_root_sha256",
    "created_at",
    "state",
}
_DELETION_RECEIPT_KEYS = {
    "schema_version",
    "run_id",
    "request_sha256",
    "operator_receipt_sha256",
    "deletion_journal_sha256",
    "deleted_at",
    "state",
}
_RESULT_STATUSES = {"X_SEARCH_OK", "X_SEARCH_PARTIAL", "X_SEARCH_BLOCKED"}
_DIMENSION_STATES = set(TEMPORAL_STATES)
_CONFIDENCE_STATES = {"high", "medium", "low"}
_EVIDENCE_KINDS = {"bio", "post", "mention", "thread"}
_RELATIONSHIPS = {"self", "official_lab", "colleague_or_team", "third_party", "historical"}
_SUPPORTS = set(SUPPORT_DIMENSIONS)
_TOOL_NAMES = {"x_keyword_search", "x_semantic_search", "x_user_search", "x_thread_fetch"}
_RECEIPT_STATUSES = {
    "fixture_complete",
    "completed",
    "process_failed",
    "timed_out",
    "structured_output_noncompliant",
    "result_contract_invalid",
    "provider_evidence_invalid",
    "crash_recovered",
    "technical_limit_exceeded",
}
_TECHNICAL_LIMIT_KINDS = {
    "stdout_bytes",
    "stderr_bytes",
    "json_bytes",
    "json_structure",
    "session_tree_files",
    "session_tree_file_bytes",
    "session_tree_total_bytes",
    "session_updates_bytes",
    "session_tree_entry_invalid",
    "session_tree_entries",
    "session_tree_depth",
    "session_tree_unexpected_socket",
    "session_tree_scan_deadline",
}


class AdaptiveWaveValidationError(ValueError):
    """Raised when an operator input or artifact fails closed validation."""


@dataclass(frozen=True)
class ProcessResult:
    exit_code: int | None
    timed_out: bool
    term_sent: bool
    kill_sent: bool
    process_spawn_attempted: bool
    child_pid: int | None
    process_group_id: int | None
    kernel_birth_identity: str | None
    process_identity_token: str | None
    process_group_cleanup_confirmed: bool
    execution_error_code: str
    technical_limit_kind: str | None


@dataclass(frozen=True)
class ConsumedGrant:
    grant_sha256: str
    consumption_sha256: str
    consumed_at: datetime
    expires_at: datetime
    target_release_deadline_monotonic: float


@dataclass(frozen=True)
class SessionProof:
    updates_sha256: str
    update_bytes: int
    event_count: int
    provider_prompt_id_sha256: str
    effective_model_id: str
    started_tool_calls: int
    completed_tool_calls: int
    tool_counts: dict[str, int]
    query_argument_sha256s: tuple[str, ...]
    candidate_surface_attempts: tuple[dict[str, str], ...]
    terminal_stop_reason: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    model_turns: int
    estimated_cost_usd_micros: int


@dataclass(frozen=True)
class HeadlessEnvelope:
    """Strict operator projection of the Grok headless stdout envelope."""

    inner_text: str
    provider_request_id_sha256: str
    session_id: str
    terminal_stop_reason: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    model_turns: int


@dataclass(frozen=True)
class SessionTreeMeasurement:
    file_count: int
    entry_count: int
    total_bytes: int
    max_file_bytes: int
    max_depth: int
    limit_kind: str | None


@dataclass(frozen=True)
class EffectivePromptPolicyBinding:
    policy_sha256: str
    policy_entry_id: str


class Executor(Protocol):
    def __call__(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        environment: Mapping[str, str],
        stdout_spool: Path,
        stderr_spool: Path,
        deadline_at: float,
        term_grace_ms: int,
        kill_grace_ms: int,
        max_stdout_bytes: int,
        max_stderr_bytes: int,
        session_tree_root: Path,
        session_updates_path: Path,
        max_session_files: int,
        max_session_file_bytes: int,
        max_session_total_bytes: int,
        max_session_updates_bytes: int,
        monotonic: Callable[[], float],
        on_spawn: Callable[[int, int, str, str], None],
    ) -> ProcessResult: ...


def canonical_json(value: Any) -> str:
    return json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def bytes_sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> None:
    raise ValueError(f"non_finite_number:{value}")


def strict_json_loads(value: str | bytes) -> Any:
    return json.loads(value, object_pairs_hook=_strict_object, parse_constant=_reject_nonfinite)


def strict_json_loads_bounded(
    value: str | bytes,
    *,
    max_bytes: int,
    max_depth: int,
    max_nodes: int,
) -> Any:
    raw = value.encode() if isinstance(value, str) else value
    if len(raw) > max_bytes:
        raise AdaptiveWaveValidationError("json_byte_ceiling_exceeded")
    payload = strict_json_loads(raw)
    stack: list[tuple[Any, int]] = [(payload, 0)]
    nodes = 0
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > max_nodes:
            raise AdaptiveWaveValidationError("json_node_ceiling_exceeded")
        if depth > max_depth:
            raise AdaptiveWaveValidationError("json_depth_ceiling_exceeded")
        if isinstance(current, dict):
            stack.extend((key, depth + 1) for key in current)
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in current)
    return payload


def _timestamp(now: datetime) -> str:
    if now.tzinfo is None:
        raise ValueError("timezone_aware_clock_required")
    return now.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _is_int(value: Any) -> bool:
    return type(value) is int


def _is_sha(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _is_text(value: Any, *, minimum: int = 1, maximum: int = 4096) -> bool:
    return isinstance(value, str) and minimum <= len(value) <= maximum and "\x00" not in value


def validate_request(request: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(request, dict) or set(request) != _REQUEST_KEYS:
        return ["request_shape_invalid"]
    if request.get("schema_version") != REQUEST_SCHEMA_VERSION:
        errors.append("request_schema_version_invalid")
    if not isinstance(request.get("request_id"), str) or _REQUEST_ID_RE.fullmatch(request["request_id"]) is None:
        errors.append("request_id_invalid")
    target = request.get("target")
    if not isinstance(target, dict) or set(target) != _TARGET_KEYS:
        errors.append("target_shape_invalid")
    elif (
        not isinstance(target.get("lab_id"), str)
        or _ID_RE.fullmatch(target["lab_id"]) is None
        or not isinstance(target.get("research_focus_id"), str)
        or _ID_RE.fullmatch(target["research_focus_id"]) is None
        or not _is_text(target.get("scope"), maximum=20_000)
    ):
        errors.append("target_value_invalid")
    prompt = request.get("prompt_source")
    if not isinstance(prompt, dict) or set(prompt) != _PROMPT_SOURCE_KEYS:
        errors.append("prompt_source_shape_invalid")
    elif not _is_text(prompt.get("path"), maximum=4096) or not _is_sha(prompt.get("sha256")):
        errors.append("prompt_source_value_invalid")
    prior_waves = request.get("prior_waves")
    seen_wave_ids: set[str] = set()
    if not isinstance(prior_waves, list):
        errors.append("prior_waves_invalid")
    else:
        for row in prior_waves:
            if not isinstance(row, dict) or set(row) != _PRIOR_WAVE_KEYS:
                errors.append("prior_wave_shape_invalid")
                continue
            wave_id = row.get("wave_id")
            if (
                not isinstance(wave_id, str)
                or _ID_RE.fullmatch(wave_id) is None
                or wave_id in seen_wave_ids
                or not _is_text(row.get("path"), maximum=4096)
                or not _is_sha(row.get("sha256"))
            ):
                errors.append("prior_wave_value_invalid")
            else:
                seen_wave_ids.add(wave_id)
    transport = request.get("transport")
    if not isinstance(transport, dict) or set(transport) != _TRANSPORT_KEYS:
        errors.append("transport_shape_invalid")
    elif (
        transport.get("provider_id") != PROVIDER_ID
        or not isinstance(transport.get("model_id"), str)
        or _ID_RE.fullmatch(transport["model_id"]) is None
        or transport.get("reasoning_effort") not in {"low", "medium", "high"}
        or (transport.get("grok_binary_sha256") is not None and not _is_sha(transport["grok_binary_sha256"]))
        or (
            transport.get("operator_account_ref") is not None
            and (
                not isinstance(transport["operator_account_ref"], str)
                or _ID_RE.fullmatch(transport["operator_account_ref"]) is None
            )
        )
        or (transport.get("oauth_auth_sha256") is not None and not _is_sha(transport["oauth_auth_sha256"]))
        or ((transport.get("operator_account_ref") is None) is not (transport.get("oauth_auth_sha256") is None))
    ):
        errors.append("transport_value_invalid")
    emergency = request.get("emergency")
    if not isinstance(emergency, dict) or set(emergency) != _EMERGENCY_KEYS:
        errors.append("emergency_shape_invalid")
    elif (
        not _is_int(emergency.get("max_turns"))
        or not 1 <= emergency["max_turns"] <= 512
        or not _is_int(emergency.get("deadline_ms"))
        or not 1_000 <= emergency["deadline_ms"] <= 3_600_000
        or not _is_int(emergency.get("term_grace_ms"))
        or not 100 <= emergency["term_grace_ms"] <= 60_000
        or not _is_int(emergency.get("kill_grace_ms"))
        or not 100 <= emergency["kill_grace_ms"] <= 60_000
    ):
        errors.append("emergency_value_invalid")
    technical_limits = request.get("technical_limits")
    if not _technical_limits_valid(technical_limits):
        errors.append("technical_limits_invalid")
    budget = request.get("budget")
    if not _budget_valid(budget):
        errors.append("budget_invalid")
    retention = request.get("retention")
    if not _retention_valid(retention):
        errors.append("retention_invalid")
    approval = request.get("approval")
    if not isinstance(approval, dict) or set(approval) != _APPROVAL_KEYS:
        errors.append("approval_shape_invalid")
    elif approval.get("grant_id") is not None and (
        not isinstance(approval["grant_id"], str) or _ID_RE.fullmatch(approval["grant_id"]) is None
    ):
        errors.append("grant_id_invalid")
    if request.get("authority") != AUTHORITY:
        errors.append("authority_invalid")
    return errors


def _technical_limits_valid(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _TECHNICAL_LIMIT_KEYS
        and _is_int(value.get("max_stdout_bytes"))
        and 1_000_000 <= value["max_stdout_bytes"] <= 536_870_912
        and _is_int(value.get("max_stderr_bytes"))
        and 65_536 <= value["max_stderr_bytes"] <= 67_108_864
        and _is_int(value.get("max_json_bytes"))
        and 1_000_000 <= value["max_json_bytes"] <= value["max_stdout_bytes"]
        and _is_int(value.get("max_json_depth"))
        and 16 <= value["max_json_depth"] <= 512
        and _is_int(value.get("max_json_nodes"))
        and 10_000 <= value["max_json_nodes"] <= 5_000_000
        and _is_int(value.get("max_prompt_bytes"))
        and 65_536 <= value["max_prompt_bytes"] <= 67_108_864
        and _is_int(value.get("max_compiled_prompt_bytes"))
        and value["max_prompt_bytes"] <= value["max_compiled_prompt_bytes"] <= 536_870_912
        and _is_int(value.get("max_prior_wave_bytes"))
        and 1_000_000 <= value["max_prior_wave_bytes"] <= 536_870_912
        and _is_int(value.get("max_total_prior_wave_bytes"))
        and value["max_prior_wave_bytes"] <= value["max_total_prior_wave_bytes"] <= 1_073_741_824
        and _is_int(value.get("max_prior_json_depth"))
        and 16 <= value["max_prior_json_depth"] <= 512
        and _is_int(value.get("max_prior_json_nodes"))
        and 10_000 <= value["max_prior_json_nodes"] <= 5_000_000
        and _is_int(value.get("max_session_files"))
        and 16 <= value["max_session_files"] <= 100_000
        and _is_int(value.get("max_session_file_bytes"))
        and 65_536 <= value["max_session_file_bytes"] <= 536_870_912
        and _is_int(value.get("max_session_total_bytes"))
        and value["max_session_file_bytes"] <= value["max_session_total_bytes"] <= 1_073_741_824
        and _is_int(value.get("max_session_updates_bytes"))
        and 65_536 <= value["max_session_updates_bytes"] <= value["max_session_file_bytes"]
        and _is_int(value.get("max_session_update_line_bytes"))
        and 4_096 <= value["max_session_update_line_bytes"] <= value["max_session_updates_bytes"]
    )


def _budget_valid(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _BUDGET_KEYS
        and isinstance(value.get("pricing_policy_id"), str)
        and _ID_RE.fullmatch(value["pricing_policy_id"]) is not None
        and _is_int(value.get("max_total_tokens"))
        and 1_000 <= value["max_total_tokens"] <= 100_000_000
        and _is_int(value.get("max_cost_usd_micros"))
        and 1 <= value["max_cost_usd_micros"] <= 1_000_000_000_000
        and _is_int(value.get("input_token_cost_usd_micros_per_million"))
        and 0 <= value["input_token_cost_usd_micros_per_million"] <= 1_000_000_000_000
        and _is_int(value.get("output_token_cost_usd_micros_per_million"))
        and 0 <= value["output_token_cost_usd_micros_per_million"] <= 1_000_000_000_000
    )


def _retention_valid(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _RETENTION_KEYS
        and isinstance(value.get("policy_id"), str)
        and _ID_RE.fullmatch(value["policy_id"]) is not None
        and _is_int(value.get("ttl_seconds"))
        and 3_600 <= value["ttl_seconds"] <= 604_800
        and value.get("deletion_receipt_required") is True
    )


def execution_scope_payload(request: Mapping[str, Any]) -> dict[str, Any]:
    """Return the grant-bound request scope, excluding only the grant locator."""

    return {key: request[key] for key in sorted(request) if key != "approval"}


def execution_scope_sha256(request: Mapping[str, Any]) -> str:
    return canonical_sha256(execution_scope_payload(request))


def _validate_handle(value: Any) -> bool:
    return isinstance(value, str) and _HANDLE_RE.fullmatch(value) is not None


def _validate_nonnegative_int(value: Any) -> bool:
    return _is_int(value) and value >= 0


@dataclass(frozen=True)
class PriorCandidateFacts:
    wave_ids: frozenset[str]
    evidence_source_sha256s: frozenset[str]
    target_lab_affiliation_states: frozenset[str]
    pretraining_experience_states: frozenset[str]
    target_lab_affiliation_latest_at: datetime | None
    pretraining_experience_latest_at: datetime | None


def _is_post_url(value: Any) -> bool:
    return (
        isinstance(value, str)
        and re.fullmatch(r"https://x\.com/[A-Za-z0-9_]{1,15}/status/[1-9][0-9]{5,31}", value) is not None
    )


def _parse_post_url(value: Any) -> tuple[str, str] | None:
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"https://x\.com/([A-Za-z0-9_]{1,15})/status/([1-9][0-9]{5,31})", value)
    return (match.group(1), match.group(2)) if match is not None else None


def _parse_evidence_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.endswith("Z"):
        return None
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return None
    return parsed.astimezone(UTC)


def _evidence_source_sha256(evidence: Mapping[str, Any], candidate_handle: str) -> str | None:
    """Fingerprint source identity, not model-editable prose."""

    kind = evidence.get("kind")
    author = evidence.get("author_handle")
    subject = evidence.get("subject_handle", candidate_handle)
    url = evidence.get("url")
    if (
        not _validate_handle(candidate_handle)
        or not _validate_handle(author)
        or not _validate_handle(subject)
        or subject.casefold() != candidate_handle.casefold()
    ):
        return None
    if kind == "bio":
        if url not in {f"https://x.com/{author}", f"https://profiles.invalid/{author}"}:
            return None
        return canonical_sha256(
            {"kind": "bio", "subject_handle": subject.casefold(), "author_handle": author.casefold(), "url": url}
        )
    parsed_url = _parse_post_url(url)
    post_id = evidence.get("post_id")
    if (
        kind not in {"post", "mention", "thread"}
        or parsed_url is None
        or parsed_url[0].casefold() != author.casefold()
        or parsed_url[1] != post_id
    ):
        return None
    return canonical_sha256(
        {
            "subject_handle": subject.casefold(),
            "author_handle": author.casefold(),
            "post_id": post_id,
            "url": url,
        }
    )


def _strict_temporal_transition_proved(
    candidate: Mapping[str, Any],
    evidence_rows: Sequence[Mapping[str, Any]],
    baseline: PriorCandidateFacts,
) -> bool:
    dimensions = (
        (
            "target_lab_affiliation_state",
            baseline.target_lab_affiliation_states,
            baseline.target_lab_affiliation_latest_at,
        ),
        (
            "pretraining_experience_state",
            baseline.pretraining_experience_states,
            baseline.pretraining_experience_latest_at,
        ),
    )
    for dimension, prior_states, latest_at in dimensions:
        next_state = candidate.get(dimension)
        if next_state not in {"current", "historical"} or next_state in prior_states:
            continue
        for evidence in evidence_rows:
            published = _parse_evidence_timestamp(evidence.get("published_at"))
            supports = evidence.get("supports")
            source_sha256 = _evidence_source_sha256(evidence, str(candidate.get("handle", "")))
            try:
                support_claims = normalize_support_claims(supports, allow_legacy=False)
            except ValueError:
                support_claims = ()
            if (
                any(
                    claim.get("dimension") == dimension and claim.get("asserted_value") == next_state
                    for claim in support_claims
                )
                and published is not None
                and source_sha256 is not None
                and source_sha256 not in baseline.evidence_source_sha256s
                and (latest_at is None or published > latest_at)
            ):
                return True
    return False


def _material_update_proved(candidate: Mapping[str, Any], baseline: PriorCandidateFacts) -> bool:
    handle = candidate.get("handle")
    evidence = candidate.get("evidence")
    if not isinstance(handle, str) or not isinstance(evidence, list) or not evidence:
        return False
    source_sha256s = {
        digest
        for row in evidence
        if isinstance(row, dict)
        for digest in [_evidence_source_sha256(row, handle)]
        if digest is not None
    }
    if source_sha256s - baseline.evidence_source_sha256s:
        return True
    return _strict_temporal_transition_proved(candidate, evidence, baseline)


def _candidate_reconciliation(
    result: Mapping[str, Any],
    prior_candidates: Mapping[str, PriorCandidateFacts],
    *,
    session_proof: SessionProof | None = None,
    fixture: bool = False,
) -> dict[str, Any]:
    candidates = result.get("candidates") if isinstance(result.get("candidates"), list) else []
    evidence_count = 0
    post_url_count = 0
    prior_overlap_count = 0
    verified_material_updates = 0
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        evidence = candidate.get("evidence") if isinstance(candidate.get("evidence"), list) else []
        evidence_count += len(evidence)
        post_url_count += sum(1 for row in evidence if isinstance(row, dict) and _is_post_url(row.get("url")))
        baseline = prior_candidates.get(str(candidate.get("handle", "")).casefold())
        if baseline is not None:
            prior_overlap_count += 1
            if _material_update_proved(candidate, baseline):
                verified_material_updates += 1
    provenance = result.get("native_x_tool_provenance")
    reported_calls = provenance.get("tool_calls_reported") if isinstance(provenance, dict) else 0
    surface_attempts: dict[tuple[str, str], set[str]] = {}
    if session_proof is not None:
        for attempt in session_proof.candidate_surface_attempts:
            key = (attempt["handle_key"], attempt["surface"])
            surface_attempts.setdefault(key, set()).add(attempt["query_argument_sha256"])
    candidate_surface_coverage: list[dict[str, Any]] = []
    for candidate in candidates:
        handle = candidate.get("handle") if isinstance(candidate, dict) else None
        if not _validate_handle(handle):
            continue
        handle_key = handle.casefold()
        coverage: dict[str, Any] = {"handle_key": handle_key}
        for surface in ("authored_post", "authored_reply"):
            hashes = sorted(surface_attempts.get((handle_key, surface), set()))
            coverage[surface] = {
                "attempted": bool(hashes),
                "query_argument_sha256s": hashes,
            }
        candidate_surface_coverage.append(coverage)
    candidate_surface_coverage.sort(key=lambda row: row["handle_key"])
    return {
        "candidate_count": len(candidates),
        "evidence_count": evidence_count,
        "post_url_count": post_url_count,
        "prior_overlap_count": prior_overlap_count,
        "verified_material_update_count": verified_material_updates,
        "model_reported_tool_calls": reported_calls if _validate_nonnegative_int(reported_calls) else 0,
        "mechanically_verified_tool_calls": session_proof.completed_tool_calls if session_proof is not None else 0,
        "tool_fact_source": (
            "fixture_not_applicable"
            if fixture
            else "session_transcript_verified"
            if session_proof is not None
            else "session_transcript_unverified"
        ),
        "candidate_surface_coverage": candidate_surface_coverage,
    }


def _evidence_binding_valid(
    evidence: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    live_mode: bool,
) -> bool:
    handle = candidate.get("handle")
    subject = evidence.get("subject_handle")
    author = evidence.get("author_handle")
    if (
        not _validate_handle(handle)
        or not _validate_handle(subject)
        or subject.casefold() != handle.casefold()
        or not _validate_handle(author)
    ):
        return False
    kind = evidence.get("kind")
    if kind == "bio":
        allowed_profile_urls = {f"https://x.com/{handle}"}
        if not live_mode:
            allowed_profile_urls.add(f"https://profiles.invalid/{handle}")
        return (
            author.casefold() == handle.casefold()
            and evidence.get("relationship") == "self"
            and evidence.get("post_id") is None
            and evidence.get("url") in allowed_profile_urls
            and evidence.get("published_at") is None
            and evidence.get("thread_relation") is None
        )
    parsed_url = _parse_post_url(evidence.get("url"))
    published = _parse_evidence_timestamp(evidence.get("published_at"))
    if (
        kind not in {"post", "mention", "thread"}
        or parsed_url is None
        or parsed_url[0].casefold() != author.casefold()
        or parsed_url[1] != evidence.get("post_id")
        or published is None
        or evidence.get("thread_relation") not in THREAD_RELATIONS
    ):
        return False
    return evidence.get("relationship") != "self" or author.casefold() == handle.casefold()


def validate_model_result(
    result: Any,
    *,
    prior_candidates: Mapping[str, PriorCandidateFacts] | None = None,
    live_mode: bool = False,
    require_operator_projection: bool = True,
) -> list[str]:
    """Validate model shape and, when requested, operator-owned projections."""

    prior = prior_candidates or {}
    errors: list[str] = []
    if not isinstance(result, dict) or set(result) != _RESULT_KEYS:
        return ["result_shape_invalid"]
    if result.get("status") not in _RESULT_STATUSES or not _is_text(result.get("status_reason"), maximum=20_000):
        errors.append("result_status_invalid")
    provenance = result.get("native_x_tool_provenance")
    tools: list[Any] = []
    queries: list[Any] = []
    reported_calls: Any = None
    if not isinstance(provenance, dict) or set(provenance) != _PROVENANCE_KEYS:
        errors.append("provenance_shape_invalid")
    else:
        tools = provenance.get("tools_reported")
        queries = provenance.get("queries")
        reported_calls = provenance.get("tool_calls_reported")
        if (
            not isinstance(tools, list)
            or any(not isinstance(tool, str) for tool in tools)
            or len(set(tools)) != len(tools)
            or any(tool not in _TOOL_NAMES for tool in tools)
            or not _validate_nonnegative_int(reported_calls)
            or not isinstance(queries, list)
            or any(not _is_text(query, maximum=20_000) for query in queries)
            or provenance.get("generic_web_used") is not False
        ):
            errors.append("provenance_value_invalid")
    counts = result.get("counts")
    if (
        not isinstance(counts, dict)
        or set(counts) != _COUNT_KEYS
        or any(not _validate_nonnegative_int(counts.get(key)) for key in _COUNT_KEYS)
    ):
        errors.append("counts_invalid")
    candidates = result.get("candidates")
    if not isinstance(candidates, list):
        errors.append("candidates_invalid")
        candidates = []
    seen_handles: set[str] = set()
    for index, candidate in enumerate(candidates):
        if not isinstance(candidate, dict) or set(candidate) != _CANDIDATE_KEYS:
            errors.append(f"candidate_shape_invalid:{index}")
            continue
        handle = candidate.get("handle")
        handle_key = handle.casefold() if isinstance(handle, str) else ""
        if handle_key in seen_handles:
            errors.append(f"candidate_handle_duplicate:{index}")
        seen_handles.add(handle_key)
        allowed_profile_urls = {f"https://x.com/{handle}"}
        if not live_mode:
            allowed_profile_urls.add(f"https://profiles.invalid/{handle}")
        if (
            not _validate_handle(handle)
            or candidate.get("profile_url") not in allowed_profile_urls
            or (
                candidate.get("platform_user_id") is not None
                and not _is_text(candidate["platform_user_id"], maximum=64)
            )
            or (candidate.get("bio_excerpt") is not None and not _is_text(candidate["bio_excerpt"], maximum=2_000))
            or candidate.get("target_lab_affiliation_state") not in _DIMENSION_STATES
            or candidate.get("pretraining_experience_state") not in _DIMENSION_STATES
            or candidate.get("confidence") not in _CONFIDENCE_STATES
            or candidate.get("overlap_status") not in {"novel", "prior_material_update"}
            or not isinstance(candidate.get("caveats"), list)
            or any(not _is_text(item, maximum=2_000) for item in candidate.get("caveats", []))
        ):
            errors.append(f"candidate_value_invalid:{index}")
        evidence_rows = candidate.get("evidence")
        if not isinstance(evidence_rows, list):
            errors.append(f"candidate_evidence_invalid:{index}")
            continue
        evidence_digests: set[str] = set()
        supports_by_dimension: dict[str, set[str]] = {dimension: set() for dimension in _SUPPORTS}
        for evidence_index, evidence in enumerate(evidence_rows):
            if not isinstance(evidence, dict) or set(evidence) != _EVIDENCE_KEYS:
                errors.append(f"evidence_shape_invalid:{index}:{evidence_index}")
                continue
            supports = evidence.get("supports")
            try:
                support_claims = normalize_support_claims(supports, allow_legacy=False)
            except ValueError:
                support_claims = ()
            digest = canonical_sha256(evidence)
            if digest in evidence_digests:
                errors.append(f"evidence_duplicate:{index}:{evidence_index}")
            evidence_digests.add(digest)
            if (
                evidence.get("kind") not in _EVIDENCE_KINDS
                or evidence.get("relationship") not in _RELATIONSHIPS
                or not _validate_handle(evidence.get("subject_handle"))
                or not _validate_handle(evidence.get("author_handle"))
                or (evidence.get("post_id") is not None and not _is_text(evidence["post_id"], maximum=32))
                or (evidence.get("url") is not None and not _is_text(evidence["url"], maximum=2_048))
                or (evidence.get("published_at") is not None and not _is_text(evidence["published_at"], maximum=64))
                or not _is_text(evidence.get("excerpt"), maximum=280)
                or not support_claims
                or not _evidence_binding_valid(evidence, candidate, live_mode=live_mode)
            ):
                errors.append(f"evidence_value_invalid:{index}:{evidence_index}")
            else:
                for claim in support_claims:
                    supports_by_dimension[claim["dimension"]].add(claim["asserted_value"])
        for dimension in _SUPPORTS:
            state = candidate.get(dimension)
            if state in {"current", "historical"} and state not in supports_by_dimension[dimension]:
                errors.append(f"dimension_evidence_missing:{index}:{dimension}")
        baseline = prior.get(handle_key)
        if baseline is None:
            if candidate.get("overlap_status") != "novel":
                errors.append(f"novel_overlap_status_invalid:{index}")
        else:
            if candidate.get("overlap_status") != "prior_material_update" or not _material_update_proved(
                candidate, baseline
            ):
                errors.append(f"prior_overlap_without_material_update:{index}")
    excluded = result.get("excluded_examples")
    if not isinstance(excluded, list):
        errors.append("excluded_examples_invalid")
    else:
        for index, row in enumerate(excluded):
            if (
                not isinstance(row, dict)
                or set(row) != _EXCLUDED_KEYS
                or not _validate_handle(row.get("handle"))
                or not _is_text(row.get("reason"), maximum=2_000)
            ):
                errors.append(f"excluded_example_invalid:{index}")
    limitations = result.get("limitations")
    if not isinstance(limitations, list) or any(not _is_text(item, maximum=4_000) for item in limitations):
        errors.append("limitations_invalid")
    reconciliation = result.get("local_reconciliation")
    evidence_count = sum(
        len(candidate.get("evidence", []))
        for candidate in candidates
        if isinstance(candidate, dict) and isinstance(candidate.get("evidence"), list)
    )
    post_url_count = sum(
        1
        for candidate in candidates
        if isinstance(candidate, dict) and isinstance(candidate.get("evidence"), list)
        for evidence in candidate["evidence"]
        if isinstance(evidence, dict) and _is_post_url(evidence.get("url"))
    )
    if not isinstance(reconciliation, dict) or set(reconciliation) != _RECONCILIATION_KEYS:
        errors.append("local_reconciliation_shape_invalid")
    else:
        tool_counts = reconciliation.get("tool_counts")
        if (
            not _validate_nonnegative_int(reconciliation.get("candidate_records_validated"))
            or not _validate_nonnegative_int(reconciliation.get("evidence_items_validated"))
            or not _validate_nonnegative_int(reconciliation.get("post_urls_structurally_validated"))
            or reconciliation.get("provider_post_bodies_replayable") is not False
            or not _validate_nonnegative_int(reconciliation.get("tool_calls_completed"))
            or not isinstance(tool_counts, dict)
            or any(key not in _TOOL_NAMES or not _validate_nonnegative_int(value) for key, value in tool_counts.items())
            or (
                require_operator_projection
                and isinstance(tool_counts, dict)
                and sum(tool_counts.values()) != reconciliation.get("tool_calls_completed")
            )
            or (
                require_operator_projection
                and (
                    reconciliation.get("candidate_records_validated") != len(candidates)
                    or reconciliation.get("evidence_items_validated") != evidence_count
                    or reconciliation.get("post_urls_structurally_validated") != post_url_count
                )
            )
        ):
            errors.append("local_reconciliation_value_invalid")
    if isinstance(counts, dict) and require_operator_projection:
        if counts.get("candidates_retained") != len(candidates):
            errors.append("candidate_count_mismatch")
    if result.get("status") in {"X_SEARCH_OK", "X_SEARCH_PARTIAL"}:
        if live_mode:
            operator_calls = reconciliation.get("tool_calls_completed") if isinstance(reconciliation, dict) else None
            if require_operator_projection and (not _is_int(operator_calls) or operator_calls <= 0):
                errors.append("native_x_call_required_for_nonblocked_status")
        elif not _is_int(reported_calls) or reported_calls <= 0:
            errors.append("native_x_call_required_for_nonblocked_status")
    if result.get("status") == "X_SEARCH_OK" and not candidates:
        errors.append("ok_status_requires_candidate")
    return errors


def _operator_project_model_result(
    result: Mapping[str, Any],
    *,
    session_proof: SessionProof | None,
    fixture: bool,
) -> dict[str, Any]:
    """Replace model-authored ledger claims with replayable operator facts.

    The raw stdout envelope retains the model's original provenance and local
    reconciliation for diagnosis.  ``sanitized.json`` is the operator-owned
    projection consumed by later local analysis.
    """

    projected = strict_json_loads(canonical_json(result))
    candidates = projected.get("candidates") if isinstance(projected.get("candidates"), list) else []
    evidence_count = sum(
        len(candidate.get("evidence", []))
        for candidate in candidates
        if isinstance(candidate, dict) and isinstance(candidate.get("evidence"), list)
    )
    post_url_count = sum(
        1
        for candidate in candidates
        if isinstance(candidate, dict) and isinstance(candidate.get("evidence"), list)
        for evidence in candidate["evidence"]
        if isinstance(evidence, dict) and _is_post_url(evidence.get("url"))
    )
    counts = projected.get("counts")
    if isinstance(counts, dict):
        counts["candidates_retained"] = len(candidates)
    completed_tool_calls = 0 if fixture or session_proof is None else session_proof.completed_tool_calls
    tool_counts = {} if fixture or session_proof is None else dict(session_proof.tool_counts)
    projected["local_reconciliation"] = {
        "candidate_records_validated": len(candidates),
        "evidence_items_validated": evidence_count,
        "post_urls_structurally_validated": post_url_count,
        "provider_post_bodies_replayable": False,
        "tool_calls_completed": completed_tool_calls,
        "tool_counts": tool_counts,
    }
    return projected


def _live_profile_urls_valid(result: Any) -> bool:
    candidates = result.get("candidates") if isinstance(result, dict) else None
    return isinstance(candidates, list) and all(
        isinstance(candidate, dict) and candidate.get("profile_url") == f"https://x.com/{candidate.get('handle')}"
        for candidate in candidates
    )


def _read_regular_owned_bounded(
    path: Path,
    *,
    maximum_bytes: int,
    required_mode: int | None = None,
) -> bytes:
    """Read one descriptor-bound owner file without a check/open race."""

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AdaptiveWaveValidationError("bound_input_unreadable") from exc
    try:
        before = os.fstat(descriptor)
        if before.st_size > maximum_bytes:
            raise AdaptiveWaveValidationError("bound_input_byte_ceiling_exceeded")
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or before.st_size < 0
            or (required_mode is not None and stat.S_IMODE(before.st_mode) != required_mode)
        ):
            raise AdaptiveWaveValidationError("bound_input_metadata_invalid")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1_048_576, remaining))
            if not chunk:
                raise AdaptiveWaveValidationError("bound_input_changed_during_read")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise AdaptiveWaveValidationError("bound_input_byte_ceiling_exceeded")
        after = os.fstat(descriptor)
        try:
            current = path.stat(follow_symlinks=False)
        except OSError as exc:
            raise AdaptiveWaveValidationError("bound_input_identity_changed") from exc

        def identity(item: os.stat_result) -> tuple[int, int, int, int, int]:
            return item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns, item.st_nlink

        if identity(before) != identity(after) or identity(after) != identity(current):
            raise AdaptiveWaveValidationError("bound_input_identity_changed")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _load_bound_bytes(
    path_value: str,
    expected_sha256: str,
    *,
    require_private: bool = False,
    max_bytes: int | None = None,
) -> bytes:
    path = Path(path_value).expanduser()
    try:
        value = _read_regular_owned_bounded(
            path,
            maximum_bytes=max_bytes if max_bytes is not None else 1_073_741_824,
            required_mode=0o600 if require_private else None,
        )
    except OSError as exc:
        raise AdaptiveWaveValidationError("bound_input_unreadable") from exc
    if bytes_sha256(value) != expected_sha256:
        raise AdaptiveWaveValidationError("bound_input_sha256_mismatch")
    return value


def load_prior_context(
    request: Mapping[str, Any],
) -> tuple[list[str], list[dict[str, Any]], dict[str, PriorCandidateFacts]]:
    """Load SHA-bound exclusions plus immutable facts for material-update proofs."""

    handles: dict[str, str] = {}
    mutable_facts: dict[str, dict[str, Any]] = {}
    bindings: list[dict[str, Any]] = []
    total_prior_bytes = 0
    for row in request["prior_waves"]:
        raw = _load_bound_bytes(
            row["path"],
            row["sha256"],
            require_private=True,
            max_bytes=request["technical_limits"]["max_prior_wave_bytes"],
        )
        total_prior_bytes += len(raw)
        if total_prior_bytes > request["technical_limits"]["max_total_prior_wave_bytes"]:
            raise AdaptiveWaveValidationError("prior_wave_total_byte_ceiling_exceeded")
        try:
            wave = strict_json_loads_bounded(
                raw,
                max_bytes=request["technical_limits"]["max_prior_wave_bytes"],
                max_depth=request["technical_limits"]["max_prior_json_depth"],
                max_nodes=request["technical_limits"]["max_prior_json_nodes"],
            )
        except AdaptiveWaveValidationError:
            raise
        except (UnicodeError, ValueError, RecursionError) as exc:
            raise AdaptiveWaveValidationError("prior_wave_json_invalid") from exc
        candidates = wave.get("candidates") if isinstance(wave, dict) else None
        if not isinstance(candidates, list):
            raise AdaptiveWaveValidationError("prior_wave_candidates_invalid")
        wave_handle_keys: set[str] = set()
        for candidate in candidates:
            handle = candidate.get("handle") if isinstance(candidate, dict) else None
            if not _validate_handle(handle):
                raise AdaptiveWaveValidationError("prior_wave_handle_invalid")
            handle_key = handle.casefold()
            if handle_key in wave_handle_keys:
                raise AdaptiveWaveValidationError("prior_wave_casefold_handle_duplicate")
            wave_handle_keys.add(handle_key)
            handles.setdefault(handle_key, handle)
            facts = mutable_facts.setdefault(
                handle_key,
                {
                    "wave_ids": set(),
                    "evidence_source_sha256s": set(),
                    "target_lab_affiliation_states": set(),
                    "pretraining_experience_states": set(),
                    "target_lab_affiliation_latest_at": None,
                    "pretraining_experience_latest_at": None,
                },
            )
            facts["wave_ids"].add(row["wave_id"])
            if isinstance(candidate, dict):
                for dimension in (
                    "target_lab_affiliation_state",
                    "pretraining_experience_state",
                ):
                    state = candidate.get(dimension)
                    if state in _DIMENSION_STATES:
                        facts[f"{dimension}s"].add(state)
                evidence = candidate.get("evidence")
                if isinstance(evidence, list):
                    for item in evidence:
                        if not isinstance(item, dict):
                            continue
                        source_sha = _evidence_source_sha256(item, handle)
                        if source_sha is not None:
                            facts["evidence_source_sha256s"].add(source_sha)
                        published = _parse_evidence_timestamp(item.get("published_at"))
                        if published is None:
                            continue
                        try:
                            support_claims = normalize_support_claims(item.get("supports"), allow_legacy=True)
                        except ValueError:
                            continue
                        for dimension in {
                            claim["dimension"] for claim in support_claims if claim["dimension"] in _SUPPORTS
                        }:
                            latest_key = f"{dimension.removesuffix('_state')}_latest_at"
                            if facts[latest_key] is None or published > facts[latest_key]:
                                facts[latest_key] = published
        bindings.append(
            {
                "wave_id": row["wave_id"],
                "sha256": row["sha256"],
                "candidate_count": len(candidates),
                "unique_handle_count": len(wave_handle_keys),
                "handle_set_sha256": canonical_sha256(sorted(wave_handle_keys)),
            }
        )
    immutable_facts = {
        key: PriorCandidateFacts(
            wave_ids=frozenset(value["wave_ids"]),
            evidence_source_sha256s=frozenset(value["evidence_source_sha256s"]),
            target_lab_affiliation_states=frozenset(value["target_lab_affiliation_states"]),
            pretraining_experience_states=frozenset(value["pretraining_experience_states"]),
            target_lab_affiliation_latest_at=value["target_lab_affiliation_latest_at"],
            pretraining_experience_latest_at=value["pretraining_experience_latest_at"],
        )
        for key, value in mutable_facts.items()
    }
    return [handles[key] for key in sorted(handles)], bindings, immutable_facts


def load_prior_handles(request: Mapping[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
    handles, bindings, _ = load_prior_context(request)
    return handles, bindings


def compile_prompt(
    base_prompt: str,
    target: Mapping[str, Any],
    prior_handles: Sequence[str],
    *,
    result_schema: Mapping[str, Any] | None = None,
) -> str:
    if not base_prompt.strip():
        raise AdaptiveWaveValidationError("prompt_empty")
    exclusion_json = canonical_json(list(prior_handles))
    target_json = canonical_json(target)
    schema_json = canonical_json(result_schema or _load_result_schema())
    return (
        f"{base_prompt.rstrip()}\n\n"
        "--- OPERATOR-OWNED ADAPTIVE RECALL BOUNDARY ---\n"
        f"Target configuration: {target_json}\n"
        "The following prior handles are case-insensitive exclusion rules only. They are not evidence, seeds, "
        "ranking inputs, or a business stop condition:\n"
        f"{exclusion_json}\n"
        "Use only Grok native X keyword, semantic, user, and thread tools. Do not use generic web search, local "
        "files, shell, browser, connectors, memory, or subagents. Do not mutate X. Do not use another provider or "
        "fallback.\n"
        "Do not impose a candidate, observation, query, or X-tool-call business quota. Continue adaptively while "
        "materially different native-X searches yield novel evidence-bearing handles. The external operator owns "
        "the emergency max-turn ceiling and monotonic deadline.\n"
        "Base discovery may use only target-lab affiliation, professional role/function, public research evidence, "
        "and pretraining relevance. Never infer or query protected identity. Keep target-lab affiliation temporality "
        "and pretraining-experience temporality independent.\n"
        "For each evidence row, emit an explicit typed support claim with the dimension and asserted temporal value. "
        "Classify every non-Bio post as self_post, reply, quote, thread_root, or thread_reply; Bio thread_relation is "
        "always null. These model-organized classifications remain unverified discovery proposals.\n"
        "Return exactly one JSON object matching the supplied schema. Do not emit Markdown, commentary, a prefix, "
        "or a suffix. Model-organized evidence remains a discovery lead, not replayable source truth.\n"
        f"Authoritative result JSON Schema: {schema_json}"
    )


def result_schema_sha256() -> str:
    path = Path(__file__).resolve().parents[2] / "contracts/x.grok.adaptive_recall_wave.result.v2.schema.json"
    return bytes_sha256(_read_regular_owned_bounded(path, maximum_bytes=MAX_CONTRACT_SCHEMA_BYTES))


def tool_registry_sha256() -> str:
    return canonical_sha256(
        {
            "allowed_native_x_tools": list(NATIVE_X_TOOLS),
            "disallowed_tools": list(DISALLOWED_TOOLS),
            "base_discovery_tool_argument_policy_version": BASE_DISCOVERY_TOOL_ARGUMENT_POLICY_VERSION,
            "cli_native_x_allowlist_enforced": False,
            "native_x_session_proof_required": True,
            "generic_web_disabled": True,
            "provider_fallback_authorized": False,
        }
    )


def _load_effective_prompt_policy() -> dict[str, Any]:
    """Load the module-owned live prompt/target owner with a closed shape."""

    try:
        raw = _read_regular_owned_bounded(
            DEFAULT_EFFECTIVE_PROMPT_POLICY,
            maximum_bytes=MAX_EFFECTIVE_PROMPT_POLICY_BYTES,
        )
        policy = strict_json_loads_bounded(
            raw,
            max_bytes=MAX_EFFECTIVE_PROMPT_POLICY_BYTES,
            max_depth=16,
            max_nodes=10_000,
        )
    except (AdaptiveWaveValidationError, OSError, UnicodeError, ValueError) as exc:
        raise AdaptiveWaveValidationError("effective_prompt_policy_unavailable") from exc
    expected_keys = {
        "schema_version",
        "binding_version",
        "policy_id",
        "owner",
        "allowed_discovery_dimensions",
        "entries",
    }
    if (
        not isinstance(policy, dict)
        or set(policy) != expected_keys
        or policy.get("schema_version") != EFFECTIVE_PROMPT_POLICY_SCHEMA_VERSION
        or policy.get("binding_version") != EFFECTIVE_PROMPT_POLICY_BINDING_VERSION
        or policy.get("policy_id") != EFFECTIVE_PROMPT_POLICY_ID
        or policy.get("owner") != EFFECTIVE_PROMPT_POLICY_OWNER
        or policy.get("allowed_discovery_dimensions") != list(ALLOWED_DISCOVERY_DIMENSIONS)
        or not isinstance(policy.get("entries"), list)
    ):
        raise AdaptiveWaveValidationError("effective_prompt_policy_invalid")
    entry_ids: set[str] = set()
    bindings: set[tuple[str, str]] = set()
    for entry in policy["entries"]:
        if not isinstance(entry, dict) or set(entry) != {
            "policy_entry_id",
            "target",
            "source_prompt_sha256",
            "authority",
        }:
            raise AdaptiveWaveValidationError("effective_prompt_policy_invalid")
        entry_id = entry.get("policy_entry_id")
        target = entry.get("target")
        source_prompt_sha = entry.get("source_prompt_sha256")
        if (
            not isinstance(entry_id, str)
            or _ID_RE.fullmatch(entry_id) is None
            or entry_id in entry_ids
            or not isinstance(target, dict)
            or set(target) != _TARGET_KEYS
            or not isinstance(target.get("lab_id"), str)
            or _ID_RE.fullmatch(target["lab_id"]) is None
            or not isinstance(target.get("research_focus_id"), str)
            or _ID_RE.fullmatch(target["research_focus_id"]) is None
            or not _is_text(target.get("scope"), maximum=20_000)
            or not _is_sha(source_prompt_sha)
            or entry.get("authority") not in {"fixture_only", "live_authorized"}
        ):
            raise AdaptiveWaveValidationError("effective_prompt_policy_invalid")
        binding = (canonical_sha256(target), source_prompt_sha)
        if binding in bindings:
            raise AdaptiveWaveValidationError("effective_prompt_policy_invalid")
        entry_ids.add(entry_id)
        bindings.add(binding)
    return policy


def _effective_prompt_policy_entry_sha256(
    policy: Mapping[str, Any],
    entry: Mapping[str, Any],
) -> str:
    """Bind immutable owner semantics plus one selected append-only entry.

    The registry is intentionally extensible.  Hashing its complete bytes
    would make an unrelated additive row invalidate already issued grants and
    retained bundles.  The wire field keeps its v2 name for compatibility, but
    its value is this entry-scoped semantic digest rather than a file digest.
    """

    return canonical_sha256(
        {
            "binding_version": policy["binding_version"],
            "schema_version": policy["schema_version"],
            "policy_id": policy["policy_id"],
            "owner": policy["owner"],
            "allowed_discovery_dimensions": policy["allowed_discovery_dimensions"],
            "entry": dict(entry),
        }
    )


def _approved_effective_prompt_binding(request: Mapping[str, Any]) -> EffectivePromptPolicyBinding:
    policy = _load_effective_prompt_policy()
    target_sha = canonical_sha256(request["target"])
    prompt_sha = request["prompt_source"]["sha256"]
    matches = [
        entry
        for entry in policy["entries"]
        if entry["authority"] == "live_authorized"
        and canonical_sha256(entry["target"]) == target_sha
        and entry["source_prompt_sha256"] == prompt_sha
    ]
    if len(matches) != 1:
        raise PermissionError("effective_prompt_target_not_approved")
    selected_entry = matches[0]
    return EffectivePromptPolicyBinding(
        policy_sha256=_effective_prompt_policy_entry_sha256(policy, selected_entry),
        policy_entry_id=selected_entry["policy_entry_id"],
    )


def _command_policy(request: Mapping[str, Any], *, legacy_plain: bool = False) -> list[str]:
    """Build the approved argv template through the one canonical argv builder."""

    builder = _build_legacy_plain_grok_command if legacy_plain else build_grok_command
    return builder(
        binary=Path("<grok-binary:sha256-bound>"),
        cwd=Path("<isolated-empty-directory>"),
        request=request,
        prompt_file=Path("<compiled-prompt:sha256-bound>"),
        leader_socket=Path("<isolated-leader-socket>"),
        session_id="<operator-session-id>",
        result_schema={"$operator_bound_schema_sha256": result_schema_sha256()},
    )


def command_policy_sha256(request: Mapping[str, Any]) -> str:
    return canonical_sha256(_command_policy(request))


def _legacy_command_policy_sha256(request: Mapping[str, Any]) -> str:
    """Replay-only digest for retained pre-headless adaptive bundles."""

    return canonical_sha256(_command_policy(request, legacy_plain=True))


def _redacted_policy_from_bindings(
    input_binding: Mapping[str, Any],
    command_binding: Mapping[str, Any],
    *,
    legacy_plain: bool = False,
) -> list[str]:
    del input_binding
    synthetic_request = {
        "transport": {
            "model_id": command_binding["model_id"],
            "reasoning_effort": command_binding["reasoning_effort"],
        },
        "emergency": {"max_turns": command_binding["max_turns"]},
    }
    return _command_policy(synthetic_request, legacy_plain=legacy_plain)


def _redacted_environment_policy() -> dict[str, str]:
    return {
        "GROK_DISABLE_AUTOUPDATER": "1",
        "GROK_HOME": "<ephemeral-home>",
        "HOME": "<ephemeral-home>",
        "LANG": "en_US.UTF-8",
        "LC_ALL": "en_US.UTF-8",
        "NO_COLOR": "1",
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
        "TERM": "dumb",
        "TMPDIR": "<ephemeral-home>/tmp",
        "XDG_CACHE_HOME": "<ephemeral-home>/xdg-cache",
        "XDG_CONFIG_HOME": "<ephemeral-home>/xdg-config",
        "XDG_STATE_HOME": "<ephemeral-home>/xdg-state",
        "X_FIRST_PROCESS_IDENTITY": "<spawn-token>",
    }


def build_grok_command(
    *,
    binary: Path,
    cwd: Path,
    request: Mapping[str, Any],
    prompt_file: Path,
    leader_socket: Path,
    session_id: str,
    result_schema: Mapping[str, Any],
) -> list[str]:
    emergency = request["emergency"]
    transport = request["transport"]
    return [
        str(binary),
        "--prompt-file",
        str(prompt_file),
        "--verbatim",
        "--cwd",
        str(cwd),
        "--model",
        transport["model_id"],
        "--reasoning-effort",
        transport["reasoning_effort"],
        "--output-format",
        "json",
        "--json-schema",
        canonical_json(result_schema),
        "--disable-web-search",
        "--disallowed-tools",
        ",".join(DISALLOWED_TOOLS),
        "--max-turns",
        str(emergency["max_turns"]),
        "--session-id",
        session_id,
        "--no-subagents",
        "--no-plan",
        "--no-memory",
        "--no-auto-update",
        "--leader-socket",
        str(leader_socket),
        "--permission-mode",
        "dontAsk",
        "--sandbox",
        "read-only",
    ]


def _build_legacy_plain_grok_command(
    *,
    binary: Path,
    cwd: Path,
    request: Mapping[str, Any],
    prompt_file: Path,
    leader_socket: Path,
    session_id: str,
    result_schema: Mapping[str, Any],
) -> list[str]:
    """Rebuild the pre-headless argv only while replaying retained bundles."""

    del result_schema
    emergency = request["emergency"]
    transport = request["transport"]
    return [
        str(binary),
        "--prompt-file",
        str(prompt_file),
        "--verbatim",
        "--cwd",
        str(cwd),
        "--model",
        transport["model_id"],
        "--reasoning-effort",
        transport["reasoning_effort"],
        "--output-format",
        "plain",
        "--disable-web-search",
        "--disallowed-tools",
        ",".join(DISALLOWED_TOOLS),
        "--max-turns",
        str(emergency["max_turns"]),
        "--session-id",
        session_id,
        "--no-subagents",
        "--no-plan",
        "--no-memory",
        "--no-auto-update",
        "--leader-socket",
        str(leader_socket),
        "--permission-mode",
        "dontAsk",
        "--sandbox",
        "read-only",
    ]


_GATED_LAUNCHER_SOURCE = (
    "import os,sys\n"
    "os.umask(0o077)\n"
    "gate=int(sys.argv[1]); ack=int(sys.argv[2])\n"
    "token=os.environ.get('X_FIRST_PROCESS_IDENTITY','')\n"
    "os.write(ack,(token+'\\n').encode('ascii')); os.close(ack)\n"
    "released=os.read(gate,1); os.close(gate)\n"
    "if released != b'1': os._exit(125)\n"
    "os.execve(sys.argv[3],sys.argv[3:],os.environ)\n"
)


def _kernel_birth_identity(pid: int) -> str:
    """Return a kernel/process-table birth identity for PID-reuse protection."""

    proc_stat = Path(f"/proc/{pid}/stat")
    if proc_stat.is_file():
        raw = proc_stat.read_text()
        closing = raw.rfind(")")
        fields = raw[closing + 2 :].split()
        if closing < 0 or len(fields) <= 19:
            raise AdaptiveWaveValidationError("process_birth_identity_unavailable")
        boot_id_path = Path("/proc/sys/kernel/random/boot_id")
        boot_id = boot_id_path.read_text().strip() if boot_id_path.is_file() else platform.node()
        return f"linux:{boot_id}:{fields[19]}"
    try:
        completed = subprocess.run(  # noqa: S603 - fixed absolute diagnostic binary
            ["/bin/ps", "-o", "lstart=,ppid=,pgid=", "-p", str(pid)],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
            env={"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise AdaptiveWaveValidationError("process_birth_identity_unavailable") from exc
    identity = " ".join(completed.stdout.split())
    if completed.returncode != 0 or not identity:
        raise AdaptiveWaveValidationError("process_birth_identity_unavailable")
    return f"{platform.system().lower()}:{identity}"


def _measure_session_tree(
    root: Path,
    *,
    max_files: int,
    max_file_bytes: int,
    max_total_bytes: int,
    updates_path: Path | None = None,
    max_updates_bytes: int | None = None,
    expected_socket_path: Path | None = None,
    max_depth: int = MAX_SESSION_TREE_DEPTH,
    deadline_at: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> SessionTreeMeasurement:
    """Measure every child entry under a bounded, deadline-aware traversal.

    ``max_files`` is retained as the v2 wire name, but it is deliberately
    enforced as the stricter all-entry ceiling (regular files, directories,
    and the one expected leader socket).  This prevents directory or socket
    floods from bypassing the original file-only counter.
    """

    file_count = 0
    entry_count = 0
    total_bytes = 0
    largest = 0
    deepest = 0
    limit_kind: str | None = None
    try:
        root_metadata = root.lstat()
    except FileNotFoundError:
        return SessionTreeMeasurement(0, 0, 0, 0, 0, None)
    if (
        root.is_symlink()
        or not stat.S_ISDIR(root_metadata.st_mode)
        or root_metadata.st_uid != os.getuid()
        or stat.S_IMODE(root_metadata.st_mode) != 0o700
    ):
        return SessionTreeMeasurement(0, 0, 0, 0, 0, "session_tree_entry_invalid")
    scan_deadline = deadline_at if deadline_at is not None else float("inf")
    stack: list[tuple[Path, int]] = [(root, 0)]
    while stack:
        if monotonic() >= scan_deadline:
            limit_kind = "session_tree_scan_deadline"
            break
        current, current_depth = stack.pop()
        try:
            current_metadata = current.lstat()
            if (
                current.is_symlink()
                or not stat.S_ISDIR(current_metadata.st_mode)
                or current_metadata.st_uid != os.getuid()
                or stat.S_IMODE(current_metadata.st_mode) != 0o700
            ):
                limit_kind = "session_tree_entry_invalid"
                break
            with os.scandir(current) as entries:
                for entry in entries:
                    if monotonic() >= scan_deadline:
                        limit_kind = "session_tree_scan_deadline"
                        break
                    path = current / entry.name
                    metadata = entry.stat(follow_symlinks=False)
                    if entry_count >= max_files:
                        limit_kind = "session_tree_entries"
                        break
                    entry_count += 1
                    depth = current_depth + 1
                    if depth > max_depth:
                        limit_kind = "session_tree_depth"
                        break
                    deepest = max(deepest, depth)
                    if stat.S_ISLNK(metadata.st_mode) or metadata.st_uid != os.getuid():
                        limit_kind = "session_tree_entry_invalid"
                        break
                    if stat.S_ISDIR(metadata.st_mode):
                        if stat.S_IMODE(metadata.st_mode) != 0o700:
                            limit_kind = "session_tree_entry_invalid"
                            break
                        stack.append((path, depth))
                        continue
                    if stat.S_ISSOCK(metadata.st_mode):
                        if expected_socket_path is None or path != expected_socket_path or metadata.st_nlink != 1:
                            limit_kind = "session_tree_unexpected_socket"
                            break
                        continue
                    if (
                        not stat.S_ISREG(metadata.st_mode)
                        or metadata.st_nlink != 1
                        or stat.S_IMODE(metadata.st_mode) != 0o600
                    ):
                        limit_kind = "session_tree_entry_invalid"
                        break
                    file_count += 1
                    next_total_bytes = total_bytes + metadata.st_size
                    total_bytes = min(next_total_bytes, max_total_bytes)
                    largest = max(largest, min(metadata.st_size, max_file_bytes))
                    if updates_path is not None and path == updates_path and max_updates_bytes is not None:
                        if metadata.st_size > max_updates_bytes:
                            limit_kind = "session_updates_bytes"
                            break
                    if metadata.st_size > max_file_bytes:
                        limit_kind = "session_tree_file_bytes"
                        break
                    if next_total_bytes > max_total_bytes:
                        limit_kind = "session_tree_total_bytes"
                        break
                if limit_kind is not None:
                    break
        except OSError:
            limit_kind = "session_tree_entry_invalid"
            break
    return SessionTreeMeasurement(file_count, entry_count, total_bytes, largest, deepest, limit_kind)


class ProcessGroupExecutor:
    """Spool bounded output while enforcing a monotonic process-group boundary."""

    @staticmethod
    def _group_alive(process_group_id: int) -> bool:
        try:
            os.killpg(process_group_id, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    @classmethod
    def _reap_leader_after_group_permission_error(
        cls,
        process: subprocess.Popen[bytes],
        *,
        term_grace_ms: int,
        kill_grace_ms: int,
    ) -> bool:
        """Never leak the direct child even when group signalling is denied."""

        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=max(0.001, term_grace_ms / 1000))
            return True
        except subprocess.TimeoutExpired:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=max(0.001, kill_grace_ms / 1000))
                return True
            except subprocess.TimeoutExpired:
                return False

    @classmethod
    def _cleanup_group(
        cls,
        process: subprocess.Popen[bytes],
        process_group_id: int,
        *,
        term_grace_ms: int,
        kill_grace_ms: int,
        monotonic: Callable[[], float],
    ) -> tuple[bool, bool, bool]:
        term_sent = False
        kill_sent = False
        if cls._group_alive(process_group_id):
            term_sent = True
            try:
                os.killpg(process_group_id, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except PermissionError:
                cls._reap_leader_after_group_permission_error(
                    process,
                    term_grace_ms=term_grace_ms,
                    kill_grace_ms=kill_grace_ms,
                )
                return term_sent, kill_sent, False
            term_deadline = monotonic() + term_grace_ms / 1000
            while cls._group_alive(process_group_id) and monotonic() < term_deadline:
                time.sleep(0.02)
        if cls._group_alive(process_group_id):
            kill_sent = True
            try:
                os.killpg(process_group_id, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except PermissionError:
                cls._reap_leader_after_group_permission_error(
                    process,
                    term_grace_ms=term_grace_ms,
                    kill_grace_ms=kill_grace_ms,
                )
                return term_sent, kill_sent, False
            kill_deadline = monotonic() + kill_grace_ms / 1000
            while cls._group_alive(process_group_id) and monotonic() < kill_deadline:
                time.sleep(0.02)
        remaining = max(0.001, kill_grace_ms / 1000)
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            return term_sent, kill_sent, False
        return term_sent, kill_sent, not cls._group_alive(process_group_id)

    def __call__(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        environment: Mapping[str, str],
        stdout_spool: Path,
        stderr_spool: Path,
        deadline_at: float,
        term_grace_ms: int,
        kill_grace_ms: int,
        max_stdout_bytes: int,
        max_stderr_bytes: int,
        session_tree_root: Path,
        session_updates_path: Path,
        max_session_files: int,
        max_session_file_bytes: int,
        max_session_total_bytes: int,
        max_session_updates_bytes: int,
        monotonic: Callable[[], float],
        on_spawn: Callable[[int, int, str, str], None],
    ) -> ProcessResult:
        spool_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            spool_flags |= os.O_NOFOLLOW
        stdout_fd = os.open(stdout_spool, spool_flags, 0o600)
        stderr_fd = os.open(stderr_spool, spool_flags, 0o600)
        os.fchmod(stdout_fd, 0o600)
        os.fchmod(stderr_fd, 0o600)
        gate_read, gate_write = os.pipe()
        ack_read, ack_write = os.pipe()
        identity_token = os.urandom(32).hex()
        child_environment = dict(environment)
        child_environment["X_FIRST_PROCESS_IDENTITY"] = identity_token
        launcher_command = [
            sys.executable,
            "-I",
            "-S",
            "-c",
            _GATED_LAUNCHER_SOURCE,
            str(gate_read),
            str(ack_write),
            *command,
        ]
        process: subprocess.Popen[bytes] | None = None
        try:
            process = subprocess.Popen(  # noqa: S603 - argv is built by the closed operator
                launcher_command,
                cwd=cwd,
                env=child_environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
                pass_fds=(gate_read, ack_write),
            )
        except (OSError, ValueError):
            for descriptor in (gate_read, gate_write, ack_read, ack_write):
                os.close(descriptor)
            os.close(stdout_fd)
            os.close(stderr_fd)
            return ProcessResult(
                exit_code=None,
                timed_out=False,
                term_sent=False,
                kill_sent=False,
                process_spawn_attempted=False,
                child_pid=None,
                process_group_id=None,
                kernel_birth_identity=None,
                process_identity_token=None,
                process_group_cleanup_confirmed=True,
                execution_error_code="spawn_failed",
                technical_limit_kind=None,
            )
        os.close(gate_read)
        os.close(ack_write)
        if process.stdout is None or process.stderr is None:  # pragma: no cover - guaranteed by PIPE
            os.close(gate_write)
            os.close(ack_read)
            os.close(stdout_fd)
            os.close(stderr_fd)
            return ProcessResult(
                exit_code=None,
                timed_out=False,
                term_sent=False,
                kill_sent=False,
                process_spawn_attempted=False,
                child_pid=None,
                process_group_id=None,
                kernel_birth_identity=None,
                process_identity_token=None,
                process_group_cleanup_confirmed=False,
                execution_error_code="spawn_failed",
                technical_limit_kind=None,
            )
        child_pid = process.pid
        process_group_id = os.getpgid(child_pid)
        kernel_birth_identity: str | None = None
        timed_out = False
        term_sent = False
        kill_sent = False
        cleanup_confirmed = False
        execution_error = "none"
        technical_limit_kind: str | None = None
        selector = selectors.DefaultSelector()
        stream_state = {
            process.stdout: (stdout_fd, max_stdout_bytes, "stdout_bytes"),
            process.stderr: (stderr_fd, max_stderr_bytes, "stderr_bytes"),
        }
        for stream in stream_state:
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ)
        try:
            handshake_ok = False
            try:
                os.set_blocking(ack_read, False)
                ack_selector = selectors.DefaultSelector()
                ack_selector.register(ack_read, selectors.EVENT_READ)
                ack_deadline = min(deadline_at, monotonic() + 2.0)
                acknowledgement = b""
                while monotonic() < ack_deadline and b"\n" not in acknowledgement:
                    if ack_selector.select(min(0.05, max(0.0, ack_deadline - monotonic()))):
                        try:
                            acknowledgement += os.read(ack_read, 128)
                        except BlockingIOError:
                            continue
                    if len(acknowledgement) > 65:
                        break
                ack_selector.close()
                if acknowledgement != (identity_token + "\n").encode():
                    raise AdaptiveWaveValidationError("process_identity_handshake_failed")
                kernel_birth_identity = _kernel_birth_identity(child_pid)
                on_spawn(child_pid, process_group_id, kernel_birth_identity, identity_token)
                os.write(gate_write, b"1")
                handshake_ok = True
            except BaseException:
                if monotonic() >= deadline_at:
                    timed_out = True
                os.close(gate_write)
                gate_write = -1
                term_sent, kill_sent, cleaned = self._cleanup_group(
                    process,
                    process_group_id,
                    term_grace_ms=term_grace_ms,
                    kill_grace_ms=kill_grace_ms,
                    monotonic=monotonic,
                )
                cleanup_confirmed = cleaned
                execution_error = "process_execution_failed" if cleaned else "process_group_cleanup_failed"
            finally:
                os.close(ack_read)
                ack_read = -1
                if gate_write >= 0:
                    os.close(gate_write)
                    gate_write = -1
            while handshake_ok and (selector.get_map() or process.poll() is None):
                if monotonic() >= deadline_at:
                    if process.poll() is None or self._group_alive(process_group_id):
                        timed_out = True
                        break
                measurement = _measure_session_tree(
                    session_tree_root,
                    max_files=max_session_files,
                    max_file_bytes=max_session_file_bytes,
                    max_total_bytes=max_session_total_bytes,
                    updates_path=session_updates_path,
                    max_updates_bytes=max_session_updates_bytes,
                    expected_socket_path=session_tree_root / "leader.sock",
                    max_depth=MAX_SESSION_TREE_DEPTH,
                    deadline_at=deadline_at,
                    monotonic=monotonic,
                )
                if measurement.limit_kind is not None:
                    technical_limit_kind = measurement.limit_kind
                    break
                timeout = min(0.05, max(0.0, deadline_at - monotonic()))
                for key, _ in selector.select(timeout):
                    stream = key.fileobj
                    destination_fd, byte_ceiling, limit_name = stream_state[stream]
                    try:
                        chunk = os.read(stream.fileno(), 65_536)
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(stream)
                        stream.close()
                        continue
                    current_size = os.fstat(destination_fd).st_size
                    remaining = max(0, byte_ceiling - current_size)
                    if remaining:
                        view = memoryview(chunk[:remaining])
                        while view:
                            view = view[os.write(destination_fd, view) :]
                    if len(chunk) > remaining:
                        technical_limit_kind = limit_name
                        break
                if technical_limit_kind is not None:
                    break
                if process.poll() is not None and not selector.get_map():
                    break
            if handshake_ok and (
                timed_out
                or technical_limit_kind is not None
                or process.poll() is None
                or self._group_alive(process_group_id)
            ):
                term_sent, kill_sent, cleaned = self._cleanup_group(
                    process,
                    process_group_id,
                    term_grace_ms=term_grace_ms,
                    kill_grace_ms=kill_grace_ms,
                    monotonic=monotonic,
                )
                cleanup_confirmed = cleaned
                if not cleaned:
                    execution_error = "process_group_cleanup_failed"
            elif handshake_ok:
                cleanup_confirmed = not self._group_alive(process_group_id)
        except BaseException:
            term_sent, kill_sent, cleaned = self._cleanup_group(
                process,
                process_group_id,
                term_grace_ms=term_grace_ms,
                kill_grace_ms=kill_grace_ms,
                monotonic=monotonic,
            )
            cleanup_confirmed = cleaned
            execution_error = "process_execution_failed" if cleaned else "process_group_cleanup_failed"
        finally:
            for descriptor in (gate_write, ack_read):
                if descriptor >= 0:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
            selector.close()
            for stream in stream_state:
                try:
                    stream.close()
                except OSError:
                    pass
            os.fsync(stdout_fd)
            os.fsync(stderr_fd)
            os.close(stdout_fd)
            os.close(stderr_fd)
        return ProcessResult(
            exit_code=process.returncode,
            timed_out=timed_out,
            term_sent=term_sent,
            kill_sent=kill_sent,
            process_spawn_attempted=True,
            child_pid=child_pid,
            process_group_id=process_group_id,
            kernel_birth_identity=kernel_birth_identity,
            process_identity_token=identity_token,
            process_group_cleanup_confirmed=cleanup_confirmed,
            execution_error_code=execution_error,
            technical_limit_kind=technical_limit_kind,
        )


class OfflineFixtureExecutor:
    """Return a deterministic empty wave without spawning or calling a provider."""

    def __call__(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        environment: Mapping[str, str],
        stdout_spool: Path,
        stderr_spool: Path,
        deadline_at: float,
        term_grace_ms: int,
        kill_grace_ms: int,
        max_stdout_bytes: int,
        max_stderr_bytes: int,
        session_tree_root: Path,
        session_updates_path: Path,
        max_session_files: int,
        max_session_file_bytes: int,
        max_session_total_bytes: int,
        max_session_updates_bytes: int,
        monotonic: Callable[[], float],
        on_spawn: Callable[[int, int, str, str], None],
    ) -> ProcessResult:
        del (
            command,
            cwd,
            environment,
            deadline_at,
            term_grace_ms,
            kill_grace_ms,
            max_stdout_bytes,
            max_stderr_bytes,
            session_tree_root,
            session_updates_path,
            max_session_files,
            max_session_file_bytes,
            max_session_total_bytes,
            max_session_updates_bytes,
            monotonic,
            on_spawn,
        )
        payload = {
            "status": "X_SEARCH_BLOCKED",
            "status_reason": "Offline fixture lane; no external execution occurred.",
            "native_x_tool_provenance": {
                "tools_reported": [],
                "tool_calls_reported": 0,
                "queries": [],
                "generic_web_used": False,
            },
            "counts": {"observations_inspected_reported": 0, "candidates_retained": 0},
            "candidates": [],
            "excluded_examples": [],
            "limitations": ["Fixture output proves operator mechanics only."],
            "local_reconciliation": {
                "candidate_records_validated": 0,
                "evidence_items_validated": 0,
                "post_urls_structurally_validated": 0,
                "provider_post_bodies_replayable": False,
                "tool_calls_completed": 0,
                "tool_counts": {},
            },
        }
        stdout_spool.write_bytes((canonical_json(payload) + "\n").encode())
        stderr_spool.write_bytes(b"")
        os.chmod(stdout_spool, 0o600)
        os.chmod(stderr_spool, 0o600)
        return ProcessResult(
            exit_code=0,
            timed_out=False,
            term_sent=False,
            kill_sent=False,
            process_spawn_attempted=False,
            child_pid=None,
            process_group_id=None,
            kernel_birth_identity=None,
            process_identity_token=None,
            process_group_cleanup_confirmed=True,
            execution_error_code="none",
            technical_limit_kind=None,
        )


def _ensure_private_directory(path: Path, *, create: bool) -> None:
    if create:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        info = path.lstat()
    except OSError as exc:
        raise AdaptiveWaveValidationError("private_directory_unavailable") from exc
    if (
        not stat.S_ISDIR(info.st_mode)
        or path.is_symlink()
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise AdaptiveWaveValidationError("private_directory_mode_invalid")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_publish(
    path: Path,
    value: bytes,
    *,
    pre_publish: Callable[[], None] | None = None,
) -> None:
    """Durably publish a 0600 regular file without replacement."""

    _ensure_private_directory(path.parent, create=False)
    pending = path.parent / f".pending-{path.name}-{uuid.uuid4().hex}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(pending, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        remaining = memoryview(value)
        while remaining:
            remaining = remaining[os.write(descriptor, remaining) :]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        if pre_publish is not None:
            pre_publish()
        os.link(pending, path, follow_symlinks=False)
        _fsync_directory(path.parent)
    finally:
        try:
            pending.unlink()
        except FileNotFoundError:
            pass
        _fsync_directory(path.parent)


def recover_pending_publications(run_root: Path) -> int:
    """Remove only module-owned publish temporaries after validating their shape."""

    _ensure_private_directory(run_root, create=False)
    recovered = 0
    for path in run_root.iterdir():
        if _PENDING_RE.fullmatch(path.name) is None:
            continue
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or path.is_symlink()
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise AdaptiveWaveValidationError("pending_publication_invalid")
        path.unlink()
        recovered += 1
    if recovered:
        _fsync_directory(run_root)
    return recovered


@dataclass(frozen=True)
class StagedExecutable:
    path: Path
    sha256: str


def _rehash_staged_executable(path: Path) -> str:
    try:
        raw = _read_regular_owned_bounded(
            path,
            maximum_bytes=MAX_GROK_BINARY_BYTES,
            required_mode=0o700,
        )
    except AdaptiveWaveValidationError as exc:
        raise AdaptiveWaveValidationError("staged_grok_binary_invalid") from exc
    if not raw:
        raise AdaptiveWaveValidationError("staged_grok_binary_invalid")
    return bytes_sha256(raw)


def _stage_verified_binary(locator: Path, run_root: Path, expected_sha256: str) -> StagedExecutable:
    """Copy one stable, owner-controlled executable fd into the private run root."""

    try:
        locator_info = locator.lstat()
        if locator_info.st_uid != os.getuid() or not (
            stat.S_ISLNK(locator_info.st_mode) or stat.S_ISREG(locator_info.st_mode)
        ):
            raise AdaptiveWaveValidationError("grok_binary_locator_owner_invalid")
        locator_link = os.readlink(locator) if stat.S_ISLNK(locator_info.st_mode) else None
        canonical = locator.resolve(strict=True)
        if canonical.is_symlink():
            raise AdaptiveWaveValidationError("grok_binary_final_symlink_invalid")
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        source_fd = os.open(canonical, flags)
    except OSError as exc:
        raise AdaptiveWaveValidationError("grok_binary_open_failed") from exc
    executable_root = run_root / "executable"
    executable_root.mkdir(mode=0o700)
    staged = executable_root / "grok"
    destination_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        destination_flags |= os.O_NOFOLLOW
    destination_fd: int | None = None
    digest = hashlib.sha256()
    try:
        before = os.fstat(source_fd)
        mode = stat.S_IMODE(before.st_mode)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or mode & 0o022
            or mode & stat.S_IXUSR == 0
            or before.st_size <= 0
            or before.st_size > MAX_GROK_BINARY_BYTES
        ):
            raise AdaptiveWaveValidationError("grok_binary_metadata_invalid")
        destination_fd = os.open(staged, destination_flags, 0o700)
        os.fchmod(destination_fd, 0o700)
        while True:
            chunk = os.read(source_fd, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            view = memoryview(chunk)
            while view:
                view = view[os.write(destination_fd, view) :]
        os.fsync(destination_fd)
        after = os.fstat(source_fd)
        current = canonical.stat(follow_symlinks=False)
        locator_after = locator.lstat()

        def identity(item: os.stat_result) -> tuple[int, int, int, int]:
            return item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns

        if (
            identity(before) != identity(after)
            or identity(after) != identity(current)
            or identity(locator_info) != identity(locator_after)
            or (locator_link is not None and os.readlink(locator) != locator_link)
        ):
            raise AdaptiveWaveValidationError("grok_binary_identity_changed")
        observed_sha256 = digest.hexdigest()
        if observed_sha256 != expected_sha256:
            raise AdaptiveWaveValidationError("grok_binary_sha256_mismatch")
    except BaseException:
        try:
            staged.unlink()
        except FileNotFoundError:
            pass
        raise
    finally:
        os.close(source_fd)
        if destination_fd is not None:
            os.close(destination_fd)
    if _rehash_staged_executable(staged) != expected_sha256:
        raise AdaptiveWaveValidationError("staged_grok_binary_invalid")
    _fsync_directory(executable_root)
    return StagedExecutable(staged, expected_sha256)


def _copy_private_auth(source: Path, ephemeral_home: Path) -> Path:
    """Descriptor-copy OAuth state without exposing its bytes to receipts or logs."""

    try:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        source_fd = os.open(source, flags)
    except OSError as exc:
        raise AdaptiveWaveValidationError("grok_auth_open_failed") from exc
    destination = ephemeral_home / "auth.json"
    destination_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        destination_flags |= os.O_NOFOLLOW
    destination_fd: int | None = None
    try:
        info = os.fstat(source_fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
            or not 1 <= info.st_size <= 64_000
        ):
            raise AdaptiveWaveValidationError("grok_auth_metadata_invalid")
        destination_fd = os.open(destination, destination_flags, 0o600)
        os.fchmod(destination_fd, 0o600)
        copied = 0
        while True:
            chunk = os.read(source_fd, 16_384)
            if not chunk:
                break
            copied += len(chunk)
            if copied > 64_000:
                raise AdaptiveWaveValidationError("grok_auth_size_invalid")
            view = memoryview(chunk)
            while view:
                view = view[os.write(destination_fd, view) :]
        os.fsync(destination_fd)
        if copied != info.st_size:
            raise AdaptiveWaveValidationError("grok_auth_copy_incomplete")
        after = os.fstat(source_fd)
        current = source.stat(follow_symlinks=False)

        def identity(item: os.stat_result) -> tuple[int, int, int, int]:
            return item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns

        if identity(info) != identity(after) or identity(after) != identity(current):
            raise AdaptiveWaveValidationError("grok_auth_identity_changed")
    except BaseException:
        try:
            destination.unlink()
        except FileNotFoundError:
            pass
        raise
    finally:
        os.close(source_fd)
        if destination_fd is not None:
            os.close(destination_fd)
    _fsync_directory(ephemeral_home)
    return destination


def _load_result_schema() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[2] / "contracts/x.grok.adaptive_recall_wave.result.v2.schema.json"
    try:
        schema = strict_json_loads(_read_regular_owned_bounded(path, maximum_bytes=MAX_CONTRACT_SCHEMA_BYTES))
    except (OSError, UnicodeError, ValueError) as exc:
        raise AdaptiveWaveValidationError("result_schema_unavailable") from exc
    if (
        not isinstance(schema, dict)
        or schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema"
        or schema.get("additionalProperties") is not False
        or schema.get("properties", {}).get("schema_version") is not None
    ):
        raise AdaptiveWaveValidationError("result_schema_invalid")
    return schema


def _structure_within_limits(payload: Any, *, max_depth: int, max_nodes: int) -> bool:
    stack: list[tuple[Any, int]] = [(payload, 0)]
    nodes = 0
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > max_nodes or depth > max_depth:
            return False
        if isinstance(current, dict):
            stack.extend((key, depth + 1) for key in current)
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in current)
    return True


def _parse_headless_envelope(
    payload: Any,
    *,
    expected_session_id: str,
    max_turns: int,
    max_inner_bytes: int,
) -> HeadlessEnvelope:
    if (
        not isinstance(payload, dict)
        or not _HEADLESS_ENVELOPE_KEYS <= set(payload)
        or not set(payload) <= _HEADLESS_ENVELOPE_KEYS | _HEADLESS_OPTIONAL_KEYS
    ):
        raise AdaptiveWaveValidationError("headless_envelope_shape_invalid")
    inner_text = payload.get("text")
    request_id = payload.get("requestId")
    usage = payload.get("usage")
    model_turns = payload.get("num_turns")
    cost = payload.get("total_cost_usd")
    if (
        not isinstance(inner_text, str)
        or not inner_text.strip()
        or len(inner_text.encode("utf-8")) > max_inner_bytes
        or payload.get("stopReason") != "EndTurn"
        or payload.get("sessionId") != expected_session_id
        or _SESSION_ID_RE.fullmatch(expected_session_id) is None
        or not _is_text(request_id, maximum=160)
        or not _is_int(model_turns)
        or not 1 <= model_turns <= max_turns
        or not isinstance(usage, dict)
        or set(usage) != _HEADLESS_USAGE_KEYS
        or any(not _validate_nonnegative_int(usage.get(key)) for key in usage)
        or usage.get("total_tokens") != usage.get("input_tokens", 0) + usage.get("output_tokens", 0)
        or (
            cost is not None
            and (
                type(cost) not in {int, float}
                or (type(cost) is float and not math.isfinite(cost))
                or cost < 0
            )
        )
    ):
        raise AdaptiveWaveValidationError("headless_envelope_value_invalid")
    return HeadlessEnvelope(
        inner_text=inner_text,
        provider_request_id_sha256=bytes_sha256(request_id.encode()),
        session_id=expected_session_id,
        terminal_stop_reason="end_turn",
        input_tokens=usage["input_tokens"],
        output_tokens=usage["output_tokens"],
        total_tokens=usage["total_tokens"],
        model_turns=model_turns,
    )


def _parse_structured_stdout(
    raw: bytes,
    *,
    technical_limits: Mapping[str, Any],
    prior_candidates: Mapping[str, PriorCandidateFacts],
    live_mode: bool,
    expected_session_id: str | None = None,
    max_turns: int = 512,
    allow_legacy_plain: bool = False,
) -> tuple[Any | None, bytes | None, int, int, bool, bool, str | None, HeadlessEnvelope | None]:
    if len(raw) > technical_limits["max_json_bytes"]:
        return None, None, 0, 0, False, False, "json_bytes", None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None, None, len(raw), 0, False, False, None, None
    decoder = json.JSONDecoder(object_pairs_hook=_strict_object, parse_constant=_reject_nonfinite)
    leading_whitespace = len(text) - len(text.lstrip())
    first_object = text.find("{")
    if first_object < 0:
        return None, None, len(raw), 0, False, False, None, None
    try:
        payload, end = decoder.raw_decode(text, first_object)
    except (ValueError, RecursionError):
        return None, None, len(text[:first_object].encode()), 0, False, False, None, None
    prefix = text[:first_object]
    suffix = text[end:]
    prefix_bytes = len(prefix.encode()) if prefix.strip() else 0
    suffix_bytes = len(suffix.encode()) if suffix.strip() else 0
    syntax_compliant = first_object == leading_whitespace and prefix_bytes == 0 and suffix_bytes == 0
    if not isinstance(payload, dict):
        return payload, None, prefix_bytes, suffix_bytes, syntax_compliant, False, None, None
    if not _structure_within_limits(
        payload,
        max_depth=technical_limits["max_json_depth"],
        max_nodes=technical_limits["max_json_nodes"],
    ):
        return payload, None, prefix_bytes, suffix_bytes, syntax_compliant, False, "json_structure", None
    model_payload: Any = payload
    headless: HeadlessEnvelope | None = None
    if live_mode:
        if expected_session_id is None:
            return payload, None, prefix_bytes, suffix_bytes, syntax_compliant, False, None, None
        try:
            headless = _parse_headless_envelope(
                payload,
                expected_session_id=expected_session_id,
                max_turns=max_turns,
                max_inner_bytes=technical_limits["max_json_bytes"],
            )
            model_payload = strict_json_loads_bounded(
                headless.inner_text,
                max_bytes=technical_limits["max_json_bytes"],
                max_depth=technical_limits["max_json_depth"],
                max_nodes=technical_limits["max_json_nodes"],
            )
        except (AdaptiveWaveValidationError, UnicodeError, ValueError, RecursionError):
            if not allow_legacy_plain or set(payload) != _RESULT_KEYS:
                return payload, None, prefix_bytes, suffix_bytes, syntax_compliant, False, None, None
            model_payload = payload
    if not isinstance(model_payload, dict):
        return model_payload, None, prefix_bytes, suffix_bytes, syntax_compliant, False, None, headless
    sanitized = (canonical_json(model_payload) + "\n").encode()
    contract_valid = not validate_model_result(
        model_payload,
        prior_candidates=prior_candidates,
        live_mode=live_mode,
        # Retained plain bundles predate operator normalization and must
        # replay under their original self-reconciliation semantics.  Only a
        # strict headless envelope admits diagnostic model counters here.
        require_operator_projection=headless is None,
    )
    return (
        model_payload,
        sanitized,
        prefix_bytes,
        suffix_bytes,
        syntax_compliant,
        contract_valid,
        None,
        headless,
    )


def _read_private_json(path: Path) -> Any:
    try:
        return strict_json_loads(_read_regular_owned_bounded(path, maximum_bytes=67_108_864, required_mode=0o600))
    except (AdaptiveWaveValidationError, UnicodeError, ValueError) as exc:
        raise AdaptiveWaveValidationError("private_json_invalid") from exc


def _parse_timestamp(value: str) -> datetime:
    if not isinstance(value, str) or _CANONICAL_TIME_RE.fullmatch(value) is None:
        raise AdaptiveWaveValidationError("timestamp_invalid")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise AdaptiveWaveValidationError("timestamp_invalid") from exc


def _timestamp_valid(value: Any) -> bool:
    try:
        _parse_timestamp(value)
    except AdaptiveWaveValidationError:
        return False
    return True


def _approval_binding(
    request: Mapping[str, Any],
    *,
    required: bool,
    grant_sha256: str | None,
    consumption_sha256: str | None,
) -> dict[str, Any]:
    grant_id = request["approval"]["grant_id"]
    return {
        "required": required,
        "grant_id_sha256": bytes_sha256(grant_id.encode()) if grant_id is not None else None,
        "grant_sha256": grant_sha256,
        "consumption_sha256": consumption_sha256,
    }


def _grant_paths(grant_root: Path, grant_id_hash: str) -> tuple[Path, Path]:
    return grant_root / f"grant-{grant_id_hash}.json", grant_root / f"consumption-{grant_id_hash}.json"


def _auth_fingerprint(path: Path) -> str:
    return bytes_sha256(_read_regular_owned_bounded(path, maximum_bytes=64_000, required_mode=0o600))


def _validate_grant(
    grant: Any,
    request: Mapping[str, Any],
    *,
    now: datetime,
    replay_command_policy_sha256: str | None = None,
) -> list[str]:
    if not isinstance(grant, dict) or set(grant) != _GRANT_KEYS:
        return ["grant_shape_invalid"]
    grant_id = request["approval"]["grant_id"]
    expected_id_hash = bytes_sha256(grant_id.encode()) if isinstance(grant_id, str) else None
    account_ref = request.get("transport", {}).get("operator_account_ref")
    expected_account_hash = bytes_sha256(account_ref.encode()) if isinstance(account_ref, str) else None
    try:
        prompt_policy_binding = _approved_effective_prompt_binding(request)
    except (AdaptiveWaveValidationError, PermissionError):
        prompt_policy_binding = None
    errors: list[str] = []
    if (
        grant.get("schema_version") != GRANT_SCHEMA_VERSION
        or grant.get("grant_id_hash") != expected_id_hash
        or grant.get("execution_scope_sha256") != execution_scope_sha256(request)
        or grant.get("request_id") != request["request_id"]
        or grant.get("target_sha256") != canonical_sha256(request["target"])
        or grant.get("model_id") != request["transport"]["model_id"]
        or grant.get("grok_binary_sha256") != request["transport"]["grok_binary_sha256"]
        or grant.get("operator_account_ref_sha256") != expected_account_hash
        or grant.get("oauth_auth_sha256") != request["transport"]["oauth_auth_sha256"]
        or grant.get("result_schema_sha256") != result_schema_sha256()
        or grant.get("command_policy_sha256")
        != (replay_command_policy_sha256 or command_policy_sha256(request))
        or grant.get("tool_registry_sha256") != tool_registry_sha256()
        or prompt_policy_binding is None
        or grant.get("effective_prompt_policy_sha256") != prompt_policy_binding.policy_sha256
        or grant.get("effective_prompt_policy_entry_id") != prompt_policy_binding.policy_entry_id
        or grant.get("environment_policy_sha256") != canonical_sha256(_redacted_environment_policy())
        or grant.get("emergency_sha256") != canonical_sha256(request["emergency"])
        or grant.get("technical_limits_sha256") != canonical_sha256(request["technical_limits"])
        or grant.get("budget_sha256") != canonical_sha256(request["budget"])
        or grant.get("retention_sha256") != canonical_sha256(request["retention"])
        or grant.get("issuer") != "local_owner_explicit_cli"
        or grant.get("state") != "preissued_single_use"
    ):
        errors.append("grant_binding_invalid")
    try:
        issued = _parse_timestamp(grant.get("issued_at"))
        expires = _parse_timestamp(grant.get("expires_at"))
    except (TypeError, AdaptiveWaveValidationError):
        errors.append("grant_time_invalid")
    else:
        canonical_now = now.astimezone(UTC)
        if not issued <= canonical_now < expires or expires <= issued or (expires - issued).total_seconds() > 3_600:
            errors.append("grant_expired_or_lifetime_invalid")
    return errors


def issue_live_grant(
    *,
    request_path: Path,
    grant_root: Path = DEFAULT_APPROVAL_ROOT,
    auth_source: Path = DEFAULT_GROK_AUTH,
    ttl_seconds: int = 900,
    wall_clock: Callable[[], datetime] = _utc_now,
) -> tuple[dict[str, Any], Path]:
    """Preissue a request-bound owner-only grant without executing Grok."""

    if not _is_int(ttl_seconds) or not 60 <= ttl_seconds <= 3_600:
        raise AdaptiveWaveValidationError("grant_ttl_invalid")
    request = _load_request(request_path)
    prompt_policy_binding = _approved_effective_prompt_binding(request)
    grant_id = request["approval"]["grant_id"]
    transport = request["transport"]
    if (
        not isinstance(grant_id, str)
        or not _is_sha(transport["grok_binary_sha256"])
        or not isinstance(transport["operator_account_ref"], str)
        or not _is_sha(transport["oauth_auth_sha256"])
        or _auth_fingerprint(auth_source) != transport["oauth_auth_sha256"]
    ):
        raise AdaptiveWaveValidationError("live_grant_request_invalid")
    now = wall_clock().astimezone(UTC)
    grant_id_hash = bytes_sha256(grant_id.encode())
    grant = {
        "schema_version": GRANT_SCHEMA_VERSION,
        "grant_id_hash": grant_id_hash,
        "execution_scope_sha256": execution_scope_sha256(request),
        "request_id": request["request_id"],
        "target_sha256": canonical_sha256(request["target"]),
        "model_id": request["transport"]["model_id"],
        "grok_binary_sha256": request["transport"]["grok_binary_sha256"],
        "operator_account_ref_sha256": bytes_sha256(request["transport"]["operator_account_ref"].encode()),
        "oauth_auth_sha256": request["transport"]["oauth_auth_sha256"],
        "result_schema_sha256": result_schema_sha256(),
        "command_policy_sha256": command_policy_sha256(request),
        "tool_registry_sha256": tool_registry_sha256(),
        "effective_prompt_policy_sha256": prompt_policy_binding.policy_sha256,
        "effective_prompt_policy_entry_id": prompt_policy_binding.policy_entry_id,
        "environment_policy_sha256": canonical_sha256(_redacted_environment_policy()),
        "emergency_sha256": canonical_sha256(request["emergency"]),
        "technical_limits_sha256": canonical_sha256(request["technical_limits"]),
        "budget_sha256": canonical_sha256(request["budget"]),
        "retention_sha256": canonical_sha256(request["retention"]),
        "issued_at": _timestamp(now),
        "expires_at": _timestamp(now + timedelta(seconds=ttl_seconds)),
        "issuer": "local_owner_explicit_cli",
        "state": "preissued_single_use",
    }
    _ensure_private_directory(grant_root, create=True)
    grant_path, _ = _grant_paths(grant_root, grant_id_hash)
    _atomic_publish(grant_path, (canonical_json(grant) + "\n").encode())
    return grant, grant_path


def _load_preissued_grant(
    grant_root: Path,
    *,
    request: Mapping[str, Any],
    now: datetime,
) -> tuple[dict[str, Any], bytes]:
    grant_id = request["approval"]["grant_id"]
    if not isinstance(grant_id, str):
        raise PermissionError("preissued_live_grant_required")
    _ensure_private_directory(grant_root, create=True)
    grant_path, _ = _grant_paths(grant_root, bytes_sha256(grant_id.encode()))
    try:
        grant_raw = _read_regular_owned_bounded(grant_path, maximum_bytes=1_048_576, required_mode=0o600)
        grant = strict_json_loads(grant_raw)
    except (AdaptiveWaveValidationError, UnicodeError, ValueError) as exc:
        raise PermissionError("preissued_live_grant_invalid") from exc
    if _validate_grant(grant, request, now=now):
        raise PermissionError("preissued_live_grant_invalid")
    return grant, grant_raw


def _load_and_consume_grant(
    grant_root: Path,
    *,
    request: Mapping[str, Any],
    run_id: str,
    run_lease_sha256: str,
    request_sha256: str,
    expected_grant_sha256: str,
    monotonic: Callable[[], float],
    wall_clock: Callable[[], datetime],
) -> ConsumedGrant:
    grant_id = request["approval"]["grant_id"]
    if not isinstance(grant_id, str):
        raise PermissionError("preissued_live_grant_required")
    _ensure_private_directory(grant_root, create=True)
    grant_id_hash = bytes_sha256(grant_id.encode())
    grant_path, consumption_path = _grant_paths(grant_root, grant_id_hash)
    # Read first, then obtain the authoritative clock immediately before the
    # exclusive consumption publication.  A timestamp captured at intent or
    # auth-preflight time could cross the grant expiry before atomic use.
    grant_path_raw = _read_regular_owned_bounded(grant_path, maximum_bytes=1_048_576, required_mode=0o600)
    try:
        grant = strict_json_loads(grant_path_raw)
    except (UnicodeError, ValueError) as exc:
        raise PermissionError("preissued_live_grant_invalid") from exc
    consumed_at = _timestamp(wall_clock().astimezone(UTC))
    if _validate_grant(grant, request, now=_parse_timestamp(consumed_at)):
        raise PermissionError("preissued_live_grant_invalid")
    grant_raw = grant_path_raw
    grant_sha = bytes_sha256(grant_raw)
    if grant_sha != expected_grant_sha256:
        raise PermissionError("preissued_live_grant_changed")
    payload = {
        "schema_version": CONSUMPTION_SCHEMA_VERSION,
        "grant_id_hash": grant_id_hash,
        "grant_sha256": grant_sha,
        "execution_scope_sha256": execution_scope_sha256(request),
        "request_sha256": request_sha256,
        "run_id": run_id,
        "run_lease_sha256": run_lease_sha256,
        "consumed_at": consumed_at,
        "state": "consumed_after_binary_auth_preflight_before_process_spawn",
    }
    raw = (canonical_json(payload) + "\n").encode()

    def revalidate_immediately_before_link() -> None:
        atomic_clock = wall_clock().astimezone(UTC)
        if atomic_clock < _parse_timestamp(consumed_at) or _validate_grant(grant, request, now=atomic_clock):
            raise PermissionError("preissued_live_grant_invalid")

    try:
        _atomic_publish(consumption_path, raw, pre_publish=revalidate_immediately_before_link)
    except FileExistsError as exc:
        raise PermissionError("live_grant_already_consumed") from exc
    # The exclusive link is the actual single-use transition. Re-read the
    # authoritative wall clock after that transition; a grant that expires in
    # the pre-link/link window remains consumed but must never release Grok.
    post_link_monotonic = monotonic()
    post_link_clock = wall_clock().astimezone(UTC)
    consumed_clock = _parse_timestamp(consumed_at)
    expires_clock = _parse_timestamp(grant["expires_at"])
    if post_link_clock < consumed_clock or _validate_grant(grant, request, now=post_link_clock):
        raise PermissionError("preissued_live_grant_expired_after_consumption")
    release_budget_seconds = (expires_clock - post_link_clock).total_seconds()
    if release_budget_seconds <= 0:
        raise PermissionError("preissued_live_grant_expired_after_consumption")
    return ConsumedGrant(
        grant_sha256=grant_sha,
        consumption_sha256=bytes_sha256(raw),
        consumed_at=consumed_clock,
        expires_at=expires_clock,
        target_release_deadline_monotonic=post_link_monotonic + release_budget_seconds,
    )


def _isolated_environment(ephemeral_home: Path) -> dict[str, str]:
    tmp = ephemeral_home / "tmp"
    xdg_config = ephemeral_home / "xdg-config"
    xdg_state = ephemeral_home / "xdg-state"
    xdg_cache = ephemeral_home / "xdg-cache"
    for path in (tmp, xdg_config, xdg_state, xdg_cache):
        path.mkdir(mode=0o700, exist_ok=False)
    environment = {
        "HOME": str(ephemeral_home),
        "GROK_HOME": str(ephemeral_home),
        "TMPDIR": str(tmp),
        "XDG_CONFIG_HOME": str(xdg_config),
        "XDG_STATE_HOME": str(xdg_state),
        "XDG_CACHE_HOME": str(xdg_cache),
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
        "LANG": "en_US.UTF-8",
        "LC_ALL": "en_US.UTF-8",
        "NO_COLOR": "1",
        "TERM": "dumb",
        "GROK_DISABLE_AUTOUPDATER": "1",
    }
    return environment


def _session_updates_path(ephemeral_home: Path, cwd: Path, session_id: str) -> Path:
    encoded_cwd = quote(str(cwd.resolve()), safe="")
    return ephemeral_home / "sessions" / encoded_cwd / session_id / "updates.jsonl"


def _usage_values(value: Any, expected_model_id: str) -> tuple[int, int, int, int]:
    expected = {
        "inputTokens",
        "outputTokens",
        "totalTokens",
        "cachedReadTokens",
        "reasoningTokens",
        "modelCalls",
        "apiDurationMs",
        "modelUsage",
        "numTurns",
    }
    nested_fields = expected - {"modelUsage", "numTurns"}
    if not isinstance(value, dict) or set(value) != expected:
        raise AdaptiveWaveValidationError("session_usage_shape_invalid")
    scalar_fields = expected - {"modelUsage"}
    if any(not _is_int(value.get(field)) or value[field] < 0 for field in scalar_fields):
        raise AdaptiveWaveValidationError("session_usage_value_invalid")
    if (
        value["totalTokens"] != value["inputTokens"] + value["outputTokens"]
        or value["cachedReadTokens"] > value["inputTokens"]
        or value["reasoningTokens"] > value["outputTokens"]
        or value["modelCalls"] < 1
        or value["numTurns"] < 1
    ):
        raise AdaptiveWaveValidationError("session_usage_reconciliation_invalid")
    model_usage = value["modelUsage"]
    if not isinstance(model_usage, dict) or set(model_usage) != {expected_model_id}:
        raise AdaptiveWaveValidationError("session_usage_model_invalid")
    nested = model_usage[expected_model_id]
    if (
        not isinstance(nested, dict)
        or set(nested) != nested_fields
        or any(not _is_int(nested.get(field)) or nested[field] < 0 for field in nested_fields)
        or any(nested[field] != value[field] for field in nested_fields)
    ):
        raise AdaptiveWaveValidationError("session_usage_model_reconciliation_invalid")
    return value["inputTokens"], value["outputTokens"], value["totalTokens"], value["numTurns"]


def _estimated_cost_usd_micros(input_tokens: int, output_tokens: int, budget: Mapping[str, Any]) -> int:
    numerator = (
        input_tokens * budget["input_token_cost_usd_micros_per_million"]
        + output_tokens * budget["output_token_cost_usd_micros_per_million"]
    )
    return (numerator + 999_999) // 1_000_000


def _parse_session_proof(
    raw: bytes,
    *,
    expected_session_id: str,
    expected_model_id: str,
    expected_stdout: bytes,
    headless_envelope: HeadlessEnvelope | None = None,
    max_line_bytes: int,
    max_turns: int,
    budget: Mapping[str, Any],
) -> SessionProof:
    if not raw or not raw.endswith(b"\n"):
        raise AdaptiveWaveValidationError("session_updates_incomplete")
    lines = raw.splitlines()
    started: set[str] = set()
    completed: set[str] = set()
    provider_call_ids: set[str] = set()
    tool_counts: dict[str, int] = {}
    query_hashes: list[str] = []
    surface_attempts: set[tuple[str, str, str]] = set()
    prompt_ids: set[str] = set()
    model_ids: list[str] = []
    assistant_chunks: list[str] = []
    retry_attempts: list[int] = []
    retry_maximum: int | None = None
    user_events = 0
    terminal: tuple[str, tuple[int, int, int, int]] | None = None
    last_kind: str | None = None
    for index, raw_line in enumerate(lines):
        if len(raw_line) > max_line_bytes:
            raise AdaptiveWaveValidationError("session_update_line_ceiling_exceeded")
        try:
            event = strict_json_loads_bounded(
                raw_line,
                max_bytes=max_line_bytes,
                max_depth=64,
                max_nodes=100_000,
            )
        except (UnicodeError, ValueError) as exc:
            raise AdaptiveWaveValidationError("session_update_json_invalid") from exc
        if (
            not isinstance(event, dict)
            or set(event) != {"method", "params", "timestamp"}
            or not _is_int(event.get("timestamp"))
            or event["timestamp"] < 0
        ):
            raise AdaptiveWaveValidationError("session_update_envelope_invalid")
        params = event.get("params")
        if (
            not isinstance(params, dict)
            or set(params) != {"_meta", "sessionId", "update"}
            or params.get("sessionId") != expected_session_id
        ):
            raise AdaptiveWaveValidationError("session_update_session_mismatch")
        update = params.get("update")
        if not isinstance(update, dict) or not isinstance(update.get("sessionUpdate"), str):
            raise AdaptiveWaveValidationError("session_update_payload_invalid")
        kind = update["sessionUpdate"]
        expected_method = "_x.ai/session/update" if kind in {"retry_state", "turn_completed"} else "session/update"
        if event["method"] != expected_method:
            raise AdaptiveWaveValidationError("session_update_method_invalid")
        if terminal is not None:
            raise AdaptiveWaveValidationError("session_update_after_terminal")
        last_kind = kind
        metadata = params.get("_meta")
        if (
            not isinstance(metadata, dict)
            or not _is_text(metadata.get("eventId"), maximum=512)
            or not metadata["eventId"].startswith(f"{expected_session_id}-")
            or not _is_int(metadata.get("agentTimestampMs"))
            or metadata["agentTimestampMs"] < 0
        ):
            raise AdaptiveWaveValidationError("session_update_metadata_invalid")
        if isinstance(metadata, dict) and metadata.get("promptId") is not None:
            prompt_id = metadata["promptId"]
            if not _is_text(prompt_id, maximum=256):
                raise AdaptiveWaveValidationError("session_prompt_id_invalid")
            prompt_ids.add(prompt_id)
        if kind == "retry_state":
            attempt = update.get("attempt")
            maximum = update.get("max_retries")
            if (
                set(update) != {"sessionUpdate", "type", "attempt", "max_retries", "reason"}
                or update.get("type") != "retrying"
                or not _is_int(attempt)
                or not _is_int(maximum)
                or not 1 <= attempt <= maximum <= 100
                or not _is_text(update.get("reason"), maximum=20_000)
                or user_events != 0
                or started
                or assistant_chunks
                or (retry_attempts and attempt <= retry_attempts[-1])
                or (retry_maximum is not None and maximum != retry_maximum)
            ):
                raise AdaptiveWaveValidationError("session_retry_state_invalid")
            retry_attempts.append(attempt)
            retry_maximum = maximum
        elif kind == "user_message_chunk":
            if user_events != 0 or started or assistant_chunks:
                raise AdaptiveWaveValidationError("session_user_causality_invalid")
            user_events += 1
            update_metadata = update.get("_meta")
            model_id = update_metadata.get("modelId") if isinstance(update_metadata, dict) else None
            if model_id != expected_model_id:
                raise AdaptiveWaveValidationError("session_effective_model_mismatch")
            model_ids.append(model_id)
        elif kind == "tool_call":
            call_id = update.get("toolCallId")
            if (
                user_events != 1
                or not _is_text(call_id, maximum=256)
                or call_id in started
                or call_id in completed
                or update.get("status") not in {None, "in_progress"}
            ):
                raise AdaptiveWaveValidationError("session_tool_start_invalid")
            started.add(call_id)
        elif kind == "tool_call_update":
            call_id = update.get("toolCallId")
            raw_output = update.get("rawOutput")
            if (
                not isinstance(call_id, str)
                or call_id not in started
                or call_id in completed
                or update.get("status") != "completed"
                or not isinstance(raw_output, dict)
                or set(raw_output) != {"call_id", "id", "input", "name"}
                or raw_output.get("id") != call_id
                or raw_output.get("name") not in NATIVE_X_TOOLS
                or not _is_text(raw_output.get("call_id"), maximum=256)
                or not isinstance(raw_output.get("input"), str)
                or raw_output["call_id"] in provider_call_ids
            ):
                raise AdaptiveWaveValidationError("session_tool_completion_invalid")
            try:
                arguments = strict_json_loads_bounded(
                    raw_output["input"],
                    max_bytes=max_line_bytes,
                    max_depth=32,
                    max_nodes=10_000,
                )
            except (TypeError, UnicodeError, ValueError) as exc:
                raise AdaptiveWaveValidationError("session_tool_arguments_invalid") from exc
            if not isinstance(arguments, dict):
                raise AdaptiveWaveValidationError("session_tool_arguments_invalid")
            if not base_discovery_tool_arguments_allowed(arguments, raw_output["name"]):
                raise AdaptiveWaveValidationError("session_tool_subject_boundary_invalid")
            provider_call_ids.add(raw_output["call_id"])
            completed.add(call_id)
            tool_name = raw_output["name"]
            tool_counts[tool_name] = tool_counts.get(tool_name, 0) + 1
            query_sha256 = canonical_sha256({"tool_name": tool_name, "arguments": arguments})
            query_hashes.append(query_sha256)
            if tool_name == "x_keyword_search":
                classified_surface = classify_single_handle_query_surface(arguments.get("query"))
                if classified_surface is not None:
                    handle_key, surface = classified_surface
                    surface_attempts.add((handle_key, surface, query_sha256))
        elif kind == "agent_message_chunk":
            if user_events != 1 or not started or started != completed:
                raise AdaptiveWaveValidationError("session_assistant_causality_invalid")
            content = update.get("content")
            if isinstance(content, dict) and content.get("type") == "text" and isinstance(content.get("text"), str):
                assistant_chunks.append(content["text"])
            elif isinstance(content, str):
                assistant_chunks.append(content)
            else:
                raise AdaptiveWaveValidationError("session_assistant_chunk_invalid")
        elif kind == "turn_completed":
            if index != len(lines) - 1 or user_events != 1 or not assistant_chunks or started != completed:
                raise AdaptiveWaveValidationError("session_terminal_not_final")
            prompt_id = update.get("prompt_id")
            if not _is_text(prompt_id, maximum=256):
                raise AdaptiveWaveValidationError("session_prompt_id_invalid")
            prompt_ids.add(prompt_id)
            if update.get("stop_reason") != "end_turn":
                raise AdaptiveWaveValidationError("session_terminal_stop_invalid")
            usage = _usage_values(update.get("usage"), expected_model_id)
            terminal = ("end_turn", usage)
        elif kind != "agent_thought_chunk":
            raise AdaptiveWaveValidationError("session_event_kind_invalid")
    if user_events != 1 or model_ids != [expected_model_id] or not started or started != completed:
        raise AdaptiveWaveValidationError("session_causality_invalid")
    if len(prompt_ids) != 1 or not assistant_chunks:
        raise AdaptiveWaveValidationError("session_causality_invalid")
    if headless_envelope is None:
        if terminal is None or "".join(assistant_chunks).strip().encode() != expected_stdout.strip():
            raise AdaptiveWaveValidationError("session_causality_invalid")
        input_tokens, output_tokens, total_tokens, model_turns = terminal[1]
    else:
        expected_text = headless_envelope.inner_text.strip()
        if not any(
            "".join(assistant_chunks[index:]).strip() == expected_text
            for index in range(len(assistant_chunks))
        ):
            raise AdaptiveWaveValidationError("session_headless_text_mismatch")
        if terminal is None:
            if last_kind != "agent_message_chunk":
                raise AdaptiveWaveValidationError("session_headless_final_event_invalid")
        elif terminal[1] != (
            headless_envelope.input_tokens,
            headless_envelope.output_tokens,
            headless_envelope.total_tokens,
            headless_envelope.model_turns,
        ):
            raise AdaptiveWaveValidationError("session_headless_usage_mismatch")
        input_tokens = headless_envelope.input_tokens
        output_tokens = headless_envelope.output_tokens
        total_tokens = headless_envelope.total_tokens
        model_turns = headless_envelope.model_turns
    estimated_cost = _estimated_cost_usd_micros(input_tokens, output_tokens, budget)
    if (
        model_turns > max_turns
        or total_tokens > budget["max_total_tokens"]
        or estimated_cost > budget["max_cost_usd_micros"]
    ):
        raise AdaptiveWaveValidationError("session_budget_exceeded")
    return SessionProof(
        updates_sha256=bytes_sha256(raw),
        update_bytes=len(raw),
        event_count=len(lines),
        provider_prompt_id_sha256=bytes_sha256(next(iter(prompt_ids)).encode()),
        effective_model_id=expected_model_id,
        started_tool_calls=len(started),
        completed_tool_calls=len(completed),
        tool_counts=dict(sorted(tool_counts.items())),
        query_argument_sha256s=tuple(query_hashes),
        candidate_surface_attempts=tuple(
            {
                "handle_key": handle_key,
                "surface": surface,
                "query_argument_sha256": query_sha256,
            }
            for handle_key, surface, query_sha256 in sorted(surface_attempts)
        ),
        terminal_stop_reason="end_turn",
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        model_turns=model_turns,
        estimated_cost_usd_micros=estimated_cost,
    )


def _session_proof_payload(proof: SessionProof | None, *, status: str, raw: bytes | None) -> dict[str, Any]:
    if proof is None:
        return {
            "status": status,
            "updates_sha256": bytes_sha256(raw) if raw is not None else None,
            "update_bytes": len(raw) if raw is not None else 0,
            "event_count": 0,
            "provider_prompt_id_sha256": None,
            "effective_model_id": None,
            "started_tool_calls": 0,
            "completed_tool_calls": 0,
            "tool_counts": {},
            "query_argument_sha256s": [],
            "candidate_surface_attempts": [],
            "terminal_stop_reason": None,
            "input_tokens": None,
            "output_tokens": None,
            "total_tokens": None,
            "model_turns": None,
            "estimated_cost_usd_micros": None,
        }
    return {
        "status": "verified",
        "updates_sha256": proof.updates_sha256,
        "update_bytes": proof.update_bytes,
        "event_count": proof.event_count,
        "provider_prompt_id_sha256": proof.provider_prompt_id_sha256,
        "effective_model_id": proof.effective_model_id,
        "started_tool_calls": proof.started_tool_calls,
        "completed_tool_calls": proof.completed_tool_calls,
        "tool_counts": proof.tool_counts,
        "query_argument_sha256s": list(proof.query_argument_sha256s),
        "candidate_surface_attempts": list(proof.candidate_surface_attempts),
        "terminal_stop_reason": proof.terminal_stop_reason,
        "input_tokens": proof.input_tokens,
        "output_tokens": proof.output_tokens,
        "total_tokens": proof.total_tokens,
        "model_turns": proof.model_turns,
        "estimated_cost_usd_micros": proof.estimated_cost_usd_micros,
    }


def _build_static_bindings(
    *,
    request: Mapping[str, Any],
    prompt_raw: bytes,
    compiled_prompt: str,
    prior_bindings: list[dict[str, Any]],
    prior_handles: Sequence[str],
    prior_handle_count: int,
    session_id: str,
    schema_sha256: str,
    binary_sha256: str | None,
    command: Sequence[str],
    effective_prompt_policy: EffectivePromptPolicyBinding | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    input_binding = {
        "target_sha256": canonical_sha256(request["target"]),
        "source_prompt_sha256": bytes_sha256(prompt_raw),
        "compiled_prompt_sha256": bytes_sha256(compiled_prompt.encode()),
        "prior_waves": prior_bindings,
        "prior_unique_handle_count": prior_handle_count,
        "prior_handle_set_sha256": canonical_sha256(sorted(item.casefold() for item in prior_handles)),
    }
    account_ref = request["transport"]["operator_account_ref"]
    command_binding = {
        "provider_id": request["transport"]["provider_id"],
        "model_id": request["transport"]["model_id"],
        "reasoning_effort": request["transport"]["reasoning_effort"],
        "session_id": session_id,
        "grok_binary_sha256": binary_sha256,
        "structured_output_schema_sha256": schema_sha256,
        "argv_sha256": canonical_sha256(list(command)),
        "command_policy_sha256": command_policy_sha256(request),
        "environment_policy_sha256": canonical_sha256(_redacted_environment_policy()),
        "tool_registry_sha256": tool_registry_sha256(),
        "effective_prompt_policy_sha256": (
            effective_prompt_policy.policy_sha256 if effective_prompt_policy is not None else None
        ),
        "effective_prompt_policy_entry_id": (
            effective_prompt_policy.policy_entry_id if effective_prompt_policy is not None else None
        ),
        "operator_account_ref_sha256": bytes_sha256(account_ref.encode()) if account_ref is not None else None,
        "oauth_auth_sha256": request["transport"]["oauth_auth_sha256"],
        "max_turns": request["emergency"]["max_turns"],
    }
    return input_binding, command_binding


def _create_run_root(runtime_root: Path, run_id: str) -> Path:
    _ensure_private_directory(runtime_root, create=True)
    run_root = runtime_root / run_id
    run_root.mkdir(mode=0o700, exist_ok=False)
    try:
        _fsync_directory(runtime_root)
        _ensure_private_directory(run_root, create=False)
    except BaseException:
        try:
            run_root.rmdir()
            _fsync_directory(runtime_root)
        except BaseException as rollback_error:
            raise AdaptiveWaveValidationError("run_root_creation_rollback_failed") from rollback_error
        raise
    return run_root


@contextmanager
def _run_lease(run_root: Path, *, create: bool) -> Any:
    path = run_root / "run.lock"
    flags = os.O_RDWR
    if create:
        flags |= os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise AdaptiveWaveValidationError("run_lease_open_failed") from exc
    try:
        if create:
            token = os.urandom(32).hex().encode()
            os.fchmod(descriptor, 0o600)
            os.write(descriptor, token)
            os.fsync(descriptor)
            _fsync_directory(run_root)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size != 64
        ):
            raise AdaptiveWaveValidationError("run_lease_metadata_invalid")
        os.lseek(descriptor, 0, os.SEEK_SET)
        token = os.read(descriptor, 65)
        if _SHA256_RE.fullmatch(token.decode(errors="ignore")) is None:
            raise AdaptiveWaveValidationError("run_lease_token_invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AdaptiveWaveValidationError("run_active_owner_present") from exc
        try:
            yield bytes_sha256(token)
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _read_run_lease_sha256(run_root: Path) -> str:
    return bytes_sha256(_read_regular_owned_bounded(run_root / "run.lock", maximum_bytes=64, required_mode=0o600))


def _load_request(path: Path) -> dict[str, Any]:
    try:
        request = strict_json_loads(_read_regular_owned_bounded(path, maximum_bytes=4_194_304, required_mode=0o600))
    except (AdaptiveWaveValidationError, OSError, UnicodeError, ValueError) as exc:
        raise AdaptiveWaveValidationError("request_json_invalid") from exc
    errors = validate_request(request)
    if errors:
        raise AdaptiveWaveValidationError("request_contract_invalid")
    return request


def _delete_owned_directory_at(parent_descriptor: int, name: str) -> None:
    """Recursively unlink one current-owner directory without following links.

    The provider can write its isolated home and can therefore remove search
    permission from directories before it exits.  Restore only the minimum
    owner mode needed for deletion, bind every descent to a directory
    descriptor, and unlink non-directories without following them.
    """

    try:
        before = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except OSError as exc:
        raise AdaptiveWaveValidationError("owned_tree_entry_unavailable") from exc
    if not stat.S_ISDIR(before.st_mode) or before.st_uid != os.getuid():
        raise AdaptiveWaveValidationError("owned_tree_directory_invalid")
    try:
        os.chmod(name, 0o700, dir_fd=parent_descriptor, follow_symlinks=False)
        flags = os.O_RDONLY | os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
    except OSError as exc:
        raise AdaptiveWaveValidationError("owned_tree_directory_open_failed") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_uid != os.getuid()
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise AdaptiveWaveValidationError("owned_tree_directory_changed")
        os.fchmod(descriptor, 0o700)
        with os.scandir(descriptor) as entries:
            names = [entry.name for entry in entries]
        for child_name in names:
            try:
                child = os.stat(child_name, dir_fd=descriptor, follow_symlinks=False)
            except OSError as exc:
                raise AdaptiveWaveValidationError("owned_tree_entry_unavailable") from exc
            if child.st_uid != os.getuid():
                raise AdaptiveWaveValidationError("owned_tree_entry_owner_invalid")
            if stat.S_ISDIR(child.st_mode):
                _delete_owned_directory_at(descriptor, child_name)
            else:
                try:
                    os.unlink(child_name, dir_fd=descriptor)
                except OSError as exc:
                    raise AdaptiveWaveValidationError("owned_tree_entry_delete_failed") from exc
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.rmdir(name, dir_fd=parent_descriptor)
    except OSError as exc:
        raise AdaptiveWaveValidationError("owned_tree_directory_delete_failed") from exc


def _delete_owned_directory_tree(path: Path) -> None:
    """Delete an owner-controlled tree durably, including mode-000 children."""

    flags = os.O_RDONLY | os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        parent_descriptor = os.open(path.parent, flags)
    except OSError as exc:
        raise AdaptiveWaveValidationError("owned_tree_parent_open_failed") from exc
    try:
        try:
            os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            return
        _delete_owned_directory_at(parent_descriptor, path.name)
        os.fsync(parent_descriptor)
    finally:
        os.close(parent_descriptor)


def _delete_ephemeral_tree(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise AdaptiveWaveValidationError("ephemeral_tree_invalid")
    _delete_owned_directory_tree(path)
    try:
        path.lstat()
    except FileNotFoundError:
        return
    else:
        raise AdaptiveWaveValidationError("ephemeral_tree_deletion_failed")


@contextmanager
def _discard_run_root_without_intent_on_failure(run_root: Path) -> Any:
    """Leave only crash-recoverable roots that durably published an intent."""

    try:
        yield
    except BaseException:
        intent_path = run_root / "operator-intent.json"
        try:
            intent_metadata = intent_path.lstat()
            durable_intent_present = (
                stat.S_ISREG(intent_metadata.st_mode)
                and not intent_path.is_symlink()
                and intent_metadata.st_uid == os.getuid()
                and intent_metadata.st_nlink == 1
                and stat.S_IMODE(intent_metadata.st_mode) == 0o600
            )
        except OSError:
            durable_intent_present = False
        if not durable_intent_present:
            try:
                run_metadata = run_root.lstat()
            except FileNotFoundError:
                run_metadata = None
            if run_metadata is not None:
                if (
                    run_root.is_symlink()
                    or not stat.S_ISDIR(run_metadata.st_mode)
                    or run_metadata.st_uid != os.getuid()
                ):
                    raise AdaptiveWaveValidationError("preintent_run_root_invalid")
                _delete_owned_directory_tree(run_root)
            try:
                run_root.lstat()
            except FileNotFoundError:
                pass
            else:
                raise AdaptiveWaveValidationError("preintent_run_root_deletion_failed")
        raise


def _run_adaptive_wave(
    *,
    request: Mapping[str, Any],
    execution_mode: str,
    runtime_root: Path,
    approval_root: Path,
    binary: Path,
    auth_source: Path | None,
    executor: Executor,
    monotonic: Callable[[], float],
    wall_clock: Callable[[], datetime],
    run_id: str | None = None,
    session_id: str | None = None,
) -> tuple[dict[str, Any], Path]:
    if validate_request(request):
        raise AdaptiveWaveValidationError("request_contract_invalid")
    if execution_mode not in {"fixture", "live"}:
        raise AdaptiveWaveValidationError("execution_mode_invalid")
    transport = request["transport"]
    if execution_mode == "live" and (
        not _is_sha(transport["grok_binary_sha256"])
        or not isinstance(transport["operator_account_ref"], str)
        or not _is_sha(transport["oauth_auth_sha256"])
        or auth_source is None
    ):
        raise AdaptiveWaveValidationError("live_transport_binding_required")
    request_sha = canonical_sha256(request)
    started_clock = wall_clock().astimezone(UTC)
    effective_prompt_policy: EffectivePromptPolicyBinding | None = None
    grant_sha: str | None = None
    if execution_mode == "live":
        effective_prompt_policy = _approved_effective_prompt_binding(request)
        _, grant_raw = _load_preissued_grant(approval_root, request=request, now=started_clock)
        grant_sha = bytes_sha256(grant_raw)
    prompt_raw = _load_bound_bytes(
        request["prompt_source"]["path"],
        request["prompt_source"]["sha256"],
        require_private=True,
        max_bytes=request["technical_limits"]["max_prompt_bytes"],
    )
    try:
        base_prompt = prompt_raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise AdaptiveWaveValidationError("prompt_utf8_invalid") from exc
    prior_handles, prior_bindings, prior_candidates = load_prior_context(request)
    result_schema = _load_result_schema()
    compiled_prompt = compile_prompt(base_prompt, request["target"], prior_handles, result_schema=result_schema)
    compiled_prompt_raw = compiled_prompt.encode()
    if len(compiled_prompt_raw) > request["technical_limits"]["max_compiled_prompt_bytes"]:
        raise AdaptiveWaveValidationError("compiled_prompt_byte_ceiling_exceeded")
    actual_run_id = run_id or f"grok_wave_{execution_mode}_{uuid.uuid4().hex}"
    actual_session_id = session_id or str(uuid.uuid4())
    if _RUN_ID_RE.fullmatch(actual_run_id) is None or _SESSION_ID_RE.fullmatch(actual_session_id) is None:
        raise AdaptiveWaveValidationError("operator_identity_invalid")
    run_root = _create_run_root(runtime_root, actual_run_id)
    with _discard_run_root_without_intent_on_failure(run_root), _run_lease(run_root, create=True) as run_lease_sha:
        workspace = run_root / "workspace"
        workspace.mkdir(mode=0o700)
        ephemeral_home = run_root / "ephemeral-home"
        ephemeral_home.mkdir(mode=0o700)
        compiled_prompt_path = run_root / "compiled-prompt.txt"
        stdout_spool = run_root / ".stdout-spool"
        stderr_spool = run_root / ".stderr-spool"
        retained_updates_path = run_root / "session-updates.jsonl"
        runtime_layout = {
            "compiled_prompt_name": compiled_prompt_path.name,
            "stdout_spool_name": stdout_spool.name,
            "stderr_spool_name": stderr_spool.name,
            "ephemeral_home_name": ephemeral_home.name,
            "session_updates_name": retained_updates_path.name,
            "run_lock_name": "run.lock",
        }
        _atomic_publish(compiled_prompt_path, compiled_prompt_raw)
        isolated_environment = _isolated_environment(ephemeral_home)
        binary_sha: str | None = None
        command_binary = Path("fixture-grok.invalid")
        if execution_mode == "live":
            try:
                staged_binary = _stage_verified_binary(binary, run_root, transport["grok_binary_sha256"])
            except BaseException:
                _delete_ephemeral_tree(ephemeral_home)
                raise
            binary_sha = staged_binary.sha256
            command_binary = staged_binary.path
        command = build_grok_command(
            binary=command_binary,
            cwd=workspace,
            request=request,
            prompt_file=compiled_prompt_path,
            leader_socket=ephemeral_home / "leader.sock",
            session_id=actual_session_id,
            result_schema=result_schema,
        )
        input_binding, command_binding = _build_static_bindings(
            request=request,
            prompt_raw=prompt_raw,
            compiled_prompt=compiled_prompt,
            prior_bindings=prior_bindings,
            prior_handles=prior_handles,
            prior_handle_count=len(prior_handles),
            session_id=actual_session_id,
            schema_sha256=result_schema_sha256(),
            binary_sha256=binary_sha,
            command=command,
            effective_prompt_policy=effective_prompt_policy,
        )
        started_at = _timestamp(started_clock)
        intent_approval = _approval_binding(
            request,
            required=execution_mode == "live",
            grant_sha256=grant_sha,
            consumption_sha256=None,
        )
        intent = {
            "schema_version": INTENT_SCHEMA_VERSION,
            "run_id": actual_run_id,
            "request_id": request["request_id"],
            "request_sha256": request_sha,
            "execution_mode": execution_mode,
            "started_at": started_at,
            "run_lease_sha256": run_lease_sha,
            "input_binding": input_binding,
            "command_binding": command_binding,
            "approval": intent_approval,
            "emergency": request["emergency"],
            "technical_limits": request["technical_limits"],
            "budget": request["budget"],
            "retention": request["retention"],
            "runtime_layout": runtime_layout,
            "authority": AUTHORITY,
        }
        _atomic_publish(run_root / "operator-request.json", (canonical_json(request) + "\n").encode())
        _atomic_publish(run_root / "operator-intent.json", (canonical_json(intent) + "\n").encode())

        approval_consumption_sha: str | None = None
        consumed_grant: ConsumedGrant | None = None
        process_ledger_sha: str | None = None
        updates_source_path = _session_updates_path(ephemeral_home, workspace, actual_session_id)
        started_monotonic = monotonic()
        execution_deadline = started_monotonic + request["emergency"]["deadline_ms"] / 1000
        updates_raw: bytes | None = None
        session_capture_status = "not_applicable" if execution_mode == "fixture" else "missing"
        measurement = SessionTreeMeasurement(0, 0, 0, 0, 0, None)
        if execution_mode == "live":
            assert auth_source is not None
            try:
                if _auth_fingerprint(auth_source) != transport["oauth_auth_sha256"]:
                    raise AdaptiveWaveValidationError("live_auth_fingerprint_mismatch")
                copied_auth = _copy_private_auth(auth_source, ephemeral_home)
                if _auth_fingerprint(copied_auth) != transport["oauth_auth_sha256"]:
                    raise AdaptiveWaveValidationError("copied_auth_fingerprint_mismatch")
                consumed_grant = _load_and_consume_grant(
                    approval_root,
                    request=request,
                    run_id=actual_run_id,
                    run_lease_sha256=run_lease_sha,
                    request_sha256=request_sha,
                    expected_grant_sha256=grant_sha,
                    monotonic=monotonic,
                    wall_clock=wall_clock,
                )
                grant_sha = consumed_grant.grant_sha256
                approval_consumption_sha = consumed_grant.consumption_sha256
            except BaseException:
                _delete_ephemeral_tree(ephemeral_home)
                raise

        def persist_spawn(
            child_pid: int,
            process_group_id: int,
            kernel_birth_identity: str,
            process_identity_token: str,
        ) -> None:
            nonlocal process_ledger_sha
            launcher_verified_clock = wall_clock().astimezone(UTC)
            ledger = {
                "schema_version": PROCESS_LEDGER_SCHEMA_VERSION,
                "run_id": actual_run_id,
                "request_id": request["request_id"],
                "run_lease_sha256": run_lease_sha,
                "session_id": actual_session_id,
                "child_pid": child_pid,
                "process_group_id": process_group_id,
                "kernel_birth_identity": kernel_birth_identity,
                "process_identity_token": process_identity_token,
                "spawned_at": _timestamp(launcher_verified_clock),
            }
            raw_ledger = (canonical_json(ledger) + "\n").encode()
            _atomic_publish(run_root / "process-ledger.json", raw_ledger)
            process_ledger_sha = bytes_sha256(raw_ledger)
            # The gated launcher has not exec'd the target yet. Recheck after
            # durable ledger publication, immediately before the executor
            # releases the gate; no provider work occurs if this fails.
            release_clock = wall_clock().astimezone(UTC)
            if execution_mode == "live" and (
                consumed_grant is None
                or release_clock < launcher_verified_clock
                or release_clock < consumed_grant.consumed_at
                or release_clock >= consumed_grant.expires_at
                or monotonic() >= consumed_grant.target_release_deadline_monotonic
            ):
                raise PermissionError("live_grant_expired_before_target_release")

        try:
            approval_binding = _approval_binding(
                request,
                required=execution_mode == "live",
                grant_sha256=grant_sha,
                consumption_sha256=approval_consumption_sha,
            )
            process_result = executor(
                command,
                cwd=workspace,
                environment=isolated_environment,
                stdout_spool=stdout_spool,
                stderr_spool=stderr_spool,
                deadline_at=execution_deadline,
                term_grace_ms=request["emergency"]["term_grace_ms"],
                kill_grace_ms=request["emergency"]["kill_grace_ms"],
                max_stdout_bytes=request["technical_limits"]["max_stdout_bytes"],
                max_stderr_bytes=request["technical_limits"]["max_stderr_bytes"],
                session_tree_root=ephemeral_home,
                session_updates_path=updates_source_path,
                max_session_files=request["technical_limits"]["max_session_files"],
                max_session_file_bytes=request["technical_limits"]["max_session_file_bytes"],
                max_session_total_bytes=request["technical_limits"]["max_session_total_bytes"],
                max_session_updates_bytes=request["technical_limits"]["max_session_updates_bytes"],
                monotonic=monotonic,
                on_spawn=persist_spawn,
            )
            if process_result.execution_error_code == "process_group_cleanup_failed":
                raise AdaptiveWaveValidationError("process_group_cleanup_incomplete")
            measurement = _measure_session_tree(
                ephemeral_home,
                max_files=request["technical_limits"]["max_session_files"],
                max_file_bytes=request["technical_limits"]["max_session_file_bytes"],
                max_total_bytes=request["technical_limits"]["max_session_total_bytes"],
                updates_path=updates_source_path,
                max_updates_bytes=request["technical_limits"]["max_session_updates_bytes"],
                expected_socket_path=ephemeral_home / "leader.sock",
                max_depth=MAX_SESSION_TREE_DEPTH,
                deadline_at=monotonic() + FINAL_SESSION_TREE_SCAN_BUDGET_SECONDS,
                monotonic=monotonic,
            )
            if execution_mode == "live" and measurement.limit_kind is None and updates_source_path.exists():
                try:
                    updates_raw = _read_regular_owned_bounded(
                        updates_source_path,
                        maximum_bytes=request["technical_limits"]["max_session_updates_bytes"],
                    )
                    _atomic_publish(retained_updates_path, updates_raw)
                    session_capture_status = "captured"
                except (AdaptiveWaveValidationError, OSError, UnicodeError, ValueError):
                    updates_raw = None
                    session_capture_status = "invalid"
        finally:
            _delete_ephemeral_tree(ephemeral_home)
        elapsed_ms = max(0, round((monotonic() - started_monotonic) * 1000))
        completed_clock = wall_clock().astimezone(UTC)
        completed_at = _timestamp(completed_clock)
        final_measurement_limit = measurement.limit_kind
        if process_result.timed_out and final_measurement_limit == "session_tree_scan_deadline":
            # The executor's process deadline owns this terminal transition.
            # A separately bounded post-process diagnostic scan must not
            # relabel an actual timeout as its own cleanup deadline.
            final_measurement_limit = None
        technical_limit_kind = process_result.technical_limit_kind or final_measurement_limit
        stdout_raw = _read_regular_owned_bounded(
            stdout_spool,
            maximum_bytes=request["technical_limits"]["max_stdout_bytes"],
            required_mode=0o600,
        )
        stderr_raw = _read_regular_owned_bounded(
            stderr_spool,
            maximum_bytes=request["technical_limits"]["max_stderr_bytes"],
            required_mode=0o600,
        )
        _atomic_publish(run_root / "raw.stdout", stdout_raw)
        _atomic_publish(run_root / "stderr.txt", stderr_raw)
        stdout_spool.unlink()
        stderr_spool.unlink()
        (
            parsed_result,
            sanitized,
            prefix_bytes,
            suffix_bytes,
            syntax_compliant,
            contract_valid,
            json_limit_kind,
            headless_envelope,
        ) = _parse_structured_stdout(
            stdout_raw,
            technical_limits=request["technical_limits"],
            prior_candidates=prior_candidates,
            live_mode=execution_mode == "live",
            expected_session_id=actual_session_id,
            max_turns=request["emergency"]["max_turns"],
            allow_legacy_plain=execution_mode == "fixture",
        )
        technical_limit_kind = technical_limit_kind or json_limit_kind

        session_proof: SessionProof | None = None
        session_proof_status = "not_applicable" if execution_mode == "fixture" else "missing"
        if execution_mode == "live" and updates_raw is not None and session_capture_status == "captured":
            try:
                session_proof = _parse_session_proof(
                    updates_raw,
                    expected_session_id=actual_session_id,
                    expected_model_id=transport["model_id"],
                    expected_stdout=stdout_raw,
                    headless_envelope=headless_envelope,
                    max_line_bytes=request["technical_limits"]["max_session_update_line_bytes"],
                    max_turns=request["emergency"]["max_turns"],
                    budget=request["budget"],
                )
                session_proof_status = "verified"
            except (AdaptiveWaveValidationError, OSError, UnicodeError, ValueError):
                session_proof = None
                session_proof_status = "invalid"
        elif execution_mode == "live" and session_capture_status == "invalid":
            session_proof_status = "invalid"

        if contract_valid and isinstance(parsed_result, dict) and (
            execution_mode == "fixture" or session_proof is not None
        ):
            parsed_result = _operator_project_model_result(
                parsed_result,
                session_proof=session_proof,
                fixture=execution_mode == "fixture",
            )
            contract_valid = not validate_model_result(
                parsed_result,
                prior_candidates=prior_candidates,
                live_mode=execution_mode == "live",
            )
            sanitized = (canonical_json(parsed_result) + "\n").encode()
        sanitized_sha: str | None = None
        if sanitized is not None:
            _atomic_publish(run_root / "sanitized.json", sanitized)
            sanitized_sha = bytes_sha256(sanitized)

        if technical_limit_kind is not None:
            status = "technical_limit_exceeded"
        elif process_result.timed_out:
            status = "timed_out"
        elif process_result.execution_error_code != "none" or process_result.exit_code != 0:
            status = "process_failed"
        elif not syntax_compliant:
            status = "structured_output_noncompliant"
        elif not contract_valid:
            status = "result_contract_invalid"
        elif execution_mode == "live" and session_proof is None:
            status = "provider_evidence_invalid"
        else:
            status = "fixture_complete" if execution_mode == "fixture" else "completed"
        session_payload = _session_proof_payload(
            session_proof,
            status=session_proof_status,
            raw=updates_raw,
        )
        reconciliation = _candidate_reconciliation(
            parsed_result if isinstance(parsed_result, dict) else {},
            prior_candidates,
            session_proof=session_proof,
            fixture=execution_mode == "fixture",
        )
        delete_after = _timestamp(started_clock + timedelta(seconds=request["retention"]["ttl_seconds"]))
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "run_id": actual_run_id,
            "request_id": request["request_id"],
            "request_sha256": request_sha,
            "execution_mode": execution_mode,
            "status": status,
            "run_lease_sha256": run_lease_sha,
            "input_binding": input_binding,
            "command_binding": command_binding,
            "approval": approval_binding,
            "process": {
                "started_at": started_at,
                "completed_at": completed_at,
                "elapsed_ms": elapsed_ms,
                "exit_code": process_result.exit_code,
                "timed_out": process_result.timed_out,
                "term_sent": process_result.term_sent,
                "kill_sent": process_result.kill_sent,
                "process_spawn_attempted": process_result.process_spawn_attempted,
                "child_pid": process_result.child_pid,
                "process_group_id": process_result.process_group_id,
                "process_ledger_sha256": process_ledger_sha,
                "kernel_birth_identity_sha256": (
                    bytes_sha256(process_result.kernel_birth_identity.encode())
                    if process_result.kernel_birth_identity is not None
                    else None
                ),
                "process_identity_token_sha256": (
                    bytes_sha256(process_result.process_identity_token.encode())
                    if process_result.process_identity_token is not None
                    else None
                ),
                "process_group_cleanup_confirmed": process_result.process_group_cleanup_confirmed,
                "execution_error_code": process_result.execution_error_code,
                "deadline_ms": request["emergency"]["deadline_ms"],
                "term_grace_ms": request["emergency"]["term_grace_ms"],
                "kill_grace_ms": request["emergency"]["kill_grace_ms"],
                "fallback_used": False,
                "technical_limit_exceeded": technical_limit_kind is not None,
                "technical_limit_kind": technical_limit_kind,
            },
            "artifacts": {
                "raw_stdout_sha256": bytes_sha256(stdout_raw),
                "stderr_sha256": bytes_sha256(stderr_raw),
                "sanitized_output_sha256": sanitized_sha,
                "structured_output_compliant": syntax_compliant,
                "structured_output_contract_valid": contract_valid,
                "non_json_prefix_bytes": prefix_bytes,
                "non_json_suffix_bytes": suffix_bytes,
                "compiled_prompt_sha256": bytes_sha256(compiled_prompt_raw),
                "session_updates_sha256": bytes_sha256(updates_raw) if updates_raw is not None else None,
                "ephemeral_tree_deleted": True,
                "session_tree_file_count": measurement.file_count,
                "session_tree_entry_count": measurement.entry_count,
                "session_tree_max_depth": measurement.max_depth,
                "session_tree_total_bytes": measurement.total_bytes,
                "session_tree_max_file_bytes": measurement.max_file_bytes,
            },
            "session_proof": session_payload,
            "retention": {
                "policy_id": request["retention"]["policy_id"],
                "ttl_seconds": request["retention"]["ttl_seconds"],
                "delete_after": delete_after,
                "purge_state": "pending_expiry",
                "deletion_receipt_required": True,
            },
            "reconciliation": reconciliation,
            "authority": AUTHORITY,
        }
        receipt_errors = validate_operator_receipt(receipt)
        if receipt_errors:
            raise AdaptiveWaveValidationError("generated_receipt_invalid:" + ",".join(receipt_errors))
        _atomic_publish(run_root / "operator-receipt.json", (canonical_json(receipt) + "\n").encode())
        return receipt, run_root


def run_adaptive_grok_wave_fixture(
    *,
    request_path: Path,
    runtime_root: Path = DEFAULT_RUNTIME_ROOT,
) -> tuple[dict[str, Any], Path]:
    """Run deterministic operator mechanics with zero external execution."""

    request = _load_request(request_path)
    return _run_adaptive_wave(
        request=request,
        execution_mode="fixture",
        runtime_root=runtime_root,
        approval_root=runtime_root / ".unused-approval-root",
        binary=Path("grok-fixture.invalid"),
        auth_source=None,
        executor=OfflineFixtureExecutor(),
        monotonic=time.monotonic,
        wall_clock=_utc_now,
    )


def run_adaptive_grok_wave_live(
    *,
    execute_live: bool,
    request_path: Path,
) -> tuple[dict[str, Any], Path]:
    """Production live entrypoint with module-owned transport and clocks."""

    if execute_live is not True:
        raise PermissionError("explicit_execute_live_required")
    request = _load_request(request_path)
    return _run_adaptive_wave(
        request=request,
        execution_mode="live",
        runtime_root=DEFAULT_RUNTIME_ROOT,
        approval_root=DEFAULT_APPROVAL_ROOT,
        binary=DEFAULT_GROK_BINARY,
        auth_source=DEFAULT_GROK_AUTH,
        executor=ProcessGroupExecutor(),
        monotonic=time.monotonic,
        wall_clock=_utc_now,
    )


def _intent_valid(intent: Any) -> bool:
    if not isinstance(intent, dict) or set(intent) != _INTENT_KEYS:
        return False
    return (
        intent.get("schema_version") == INTENT_SCHEMA_VERSION
        and isinstance(intent.get("run_id"), str)
        and _RUN_ID_RE.fullmatch(intent["run_id"]) is not None
        and intent["run_id"].startswith(f"grok_wave_{intent.get('execution_mode')}_")
        and isinstance(intent.get("request_id"), str)
        and _REQUEST_ID_RE.fullmatch(intent["request_id"]) is not None
        and _is_sha(intent.get("request_sha256"))
        and intent.get("execution_mode") in {"fixture", "live"}
        and _timestamp_valid(intent.get("started_at"))
        and _is_sha(intent.get("run_lease_sha256"))
        and _input_binding_valid(intent.get("input_binding"))
        and _command_binding_valid(intent.get("command_binding"))
        and _approval_binding_valid(intent.get("approval"), intent.get("execution_mode"), terminal=False)
        and _emergency_valid(intent.get("emergency"))
        and _technical_limits_valid(intent.get("technical_limits"))
        and _budget_valid(intent.get("budget"))
        and _retention_valid(intent.get("retention"))
        and _runtime_layout_valid(intent.get("runtime_layout"))
        and intent.get("authority") == AUTHORITY
    )


def _emergency_valid(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _EMERGENCY_KEYS
        and _is_int(value.get("max_turns"))
        and 1 <= value["max_turns"] <= 512
        and _is_int(value.get("deadline_ms"))
        and 1_000 <= value["deadline_ms"] <= 3_600_000
        and _is_int(value.get("term_grace_ms"))
        and 100 <= value["term_grace_ms"] <= 60_000
        and _is_int(value.get("kill_grace_ms"))
        and 100 <= value["kill_grace_ms"] <= 60_000
    )


def _input_binding_valid(value: Any) -> bool:
    if not isinstance(value, dict) or set(value) != _INPUT_BINDING_KEYS:
        return False
    prior = value.get("prior_waves")
    return (
        _is_sha(value.get("target_sha256"))
        and _is_sha(value.get("source_prompt_sha256"))
        and _is_sha(value.get("compiled_prompt_sha256"))
        and isinstance(prior, list)
        and all(
            isinstance(row, dict)
            and set(row) == {"wave_id", "sha256", "candidate_count", "unique_handle_count", "handle_set_sha256"}
            and isinstance(row["wave_id"], str)
            and _ID_RE.fullmatch(row["wave_id"]) is not None
            and _is_sha(row["sha256"])
            and _validate_nonnegative_int(row["candidate_count"])
            and _validate_nonnegative_int(row["unique_handle_count"])
            and row["unique_handle_count"] <= row["candidate_count"]
            and _is_sha(row["handle_set_sha256"])
            for row in prior
        )
        and len({row["wave_id"] for row in prior}) == len(prior)
        and _validate_nonnegative_int(value.get("prior_unique_handle_count"))
        and _is_sha(value.get("prior_handle_set_sha256"))
    )


def _command_binding_valid(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _COMMAND_BINDING_KEYS
        and value.get("provider_id") == PROVIDER_ID
        and isinstance(value.get("model_id"), str)
        and _ID_RE.fullmatch(value["model_id"]) is not None
        and value.get("reasoning_effort") in {"low", "medium", "high"}
        and isinstance(value.get("session_id"), str)
        and _SESSION_ID_RE.fullmatch(value["session_id"]) is not None
        and (value.get("grok_binary_sha256") is None or _is_sha(value["grok_binary_sha256"]))
        and _is_sha(value.get("structured_output_schema_sha256"))
        and _is_sha(value.get("argv_sha256"))
        and _is_sha(value.get("command_policy_sha256"))
        and _is_sha(value.get("environment_policy_sha256"))
        and _is_sha(value.get("tool_registry_sha256"))
        and (
            (
                value.get("effective_prompt_policy_sha256") is None
                and value.get("effective_prompt_policy_entry_id") is None
            )
            or (
                _is_sha(value.get("effective_prompt_policy_sha256"))
                and isinstance(value.get("effective_prompt_policy_entry_id"), str)
                and _ID_RE.fullmatch(value["effective_prompt_policy_entry_id"]) is not None
            )
        )
        and (value.get("operator_account_ref_sha256") is None or _is_sha(value["operator_account_ref_sha256"]))
        and (value.get("oauth_auth_sha256") is None or _is_sha(value["oauth_auth_sha256"]))
        and ((value.get("operator_account_ref_sha256") is None) is (value.get("oauth_auth_sha256") is None))
        and _is_int(value.get("max_turns"))
        and 1 <= value["max_turns"] <= 512
    )


def _approval_binding_valid(
    value: Any,
    execution_mode: Any,
    *,
    terminal: bool,
    allow_unconsumed: bool = False,
) -> bool:
    if not isinstance(value, dict) or set(value) != _APPROVAL_BINDING_KEYS:
        return False
    required = execution_mode == "live"
    return (
        value.get("required") is required
        and (value.get("grant_id_sha256") is None or _is_sha(value["grant_id_sha256"]))
        and (value.get("grant_sha256") is None or _is_sha(value["grant_sha256"]))
        and (value.get("consumption_sha256") is None or _is_sha(value["consumption_sha256"]))
        and (
            not required
            or (
                _is_sha(value.get("grant_id_sha256"))
                and _is_sha(value.get("grant_sha256"))
                and (
                    (_is_sha(value.get("consumption_sha256")) or allow_unconsumed)
                    if terminal
                    else value.get("consumption_sha256") is None
                )
            )
        )
        and (required or (value.get("grant_sha256") is None and value.get("consumption_sha256") is None))
    )


def _runtime_layout_valid(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _RUNTIME_LAYOUT_KEYS
        and value
        == {
            "compiled_prompt_name": "compiled-prompt.txt",
            "stdout_spool_name": ".stdout-spool",
            "stderr_spool_name": ".stderr-spool",
            "ephemeral_home_name": "ephemeral-home",
            "session_updates_name": "session-updates.jsonl",
            "run_lock_name": "run.lock",
        }
    )


def _surface_attempts_valid(value: Any, query_hashes: Any) -> bool:
    if not isinstance(value, list) or not isinstance(query_hashes, list):
        return False
    normalized: list[tuple[str, str, str]] = []
    query_hash_set = set(query_hashes)
    for row in value:
        if not isinstance(row, dict) or set(row) != {
            "handle_key",
            "surface",
            "query_argument_sha256",
        }:
            return False
        handle_key = row.get("handle_key")
        surface = row.get("surface")
        query_sha256 = row.get("query_argument_sha256")
        if (
            not _validate_handle(handle_key)
            or handle_key != handle_key.casefold()
            or surface not in QUERY_SURFACES
            or not _is_sha(query_sha256)
            or query_sha256 not in query_hash_set
        ):
            return False
        normalized.append((handle_key, surface, query_sha256))
    return normalized == sorted(set(normalized))


def _candidate_surface_coverage_valid(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    order: list[str] = []
    for row in value:
        if not isinstance(row, dict) or set(row) != {"handle_key", "authored_post", "authored_reply"}:
            return False
        handle_key = row.get("handle_key")
        if not _validate_handle(handle_key) or handle_key != handle_key.casefold():
            return False
        order.append(handle_key)
        for surface in ("authored_post", "authored_reply"):
            state = row.get(surface)
            if not isinstance(state, dict) or set(state) != {"attempted", "query_argument_sha256s"}:
                return False
            attempted = state.get("attempted")
            hashes = state.get("query_argument_sha256s")
            if (
                type(attempted) is not bool
                or not isinstance(hashes, list)
                or hashes != sorted(set(hashes))
                or any(not _is_sha(item) for item in hashes)
                or attempted is not bool(hashes)
            ):
                return False
    return order == sorted(order)


def _receipt_reconciliation_valid(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _RECONCILIATION_RECEIPT_KEYS
        and all(
            _validate_nonnegative_int(value.get(key))
            for key in (
                "candidate_count",
                "evidence_count",
                "post_url_count",
                "prior_overlap_count",
                "verified_material_update_count",
                "model_reported_tool_calls",
                "mechanically_verified_tool_calls",
            )
        )
        and value["prior_overlap_count"] <= value["candidate_count"]
        and value["verified_material_update_count"] <= value["prior_overlap_count"]
        and value.get("tool_fact_source")
        in {"fixture_not_applicable", "session_transcript_verified", "session_transcript_unverified"}
        and _candidate_surface_coverage_valid(value.get("candidate_surface_coverage"))
    )


def _process_ledger_valid(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _PROCESS_LEDGER_KEYS
        and value.get("schema_version") == PROCESS_LEDGER_SCHEMA_VERSION
        and isinstance(value.get("run_id"), str)
        and _RUN_ID_RE.fullmatch(value["run_id"]) is not None
        and isinstance(value.get("request_id"), str)
        and _REQUEST_ID_RE.fullmatch(value["request_id"]) is not None
        and _is_sha(value.get("run_lease_sha256"))
        and isinstance(value.get("session_id"), str)
        and _SESSION_ID_RE.fullmatch(value["session_id"]) is not None
        and _is_int(value.get("child_pid"))
        and value["child_pid"] > 0
        and _is_int(value.get("process_group_id"))
        and value["process_group_id"] > 0
        and _is_text(value.get("kernel_birth_identity"), maximum=512)
        and isinstance(value.get("process_identity_token"), str)
        and _SHA256_RE.fullmatch(value["process_identity_token"]) is not None
        and _timestamp_valid(value.get("spawned_at"))
    )


def _session_proof_valid(value: Any, execution_mode: Any) -> bool:
    if not isinstance(value, dict) or set(value) != _SESSION_PROOF_KEYS:
        return False
    status = value.get("status")
    if status not in {"not_applicable", "missing", "invalid", "verified"}:
        return False
    if execution_mode == "fixture":
        return (
            status == "not_applicable"
            and value.get("updates_sha256") is None
            and value.get("update_bytes") == 0
            and value.get("event_count") == 0
            and value.get("provider_prompt_id_sha256") is None
            and value.get("effective_model_id") is None
            and value.get("started_tool_calls") == 0
            and value.get("completed_tool_calls") == 0
            and value.get("tool_counts") == {}
            and value.get("query_argument_sha256s") == []
            and value.get("candidate_surface_attempts") == []
            and value.get("terminal_stop_reason") is None
            and all(
                value.get(key) is None
                for key in (
                    "input_tokens",
                    "output_tokens",
                    "total_tokens",
                    "model_turns",
                    "estimated_cost_usd_micros",
                )
            )
        )
    if execution_mode != "live" or status == "not_applicable":
        return False
    if status != "verified":
        return (
            (value.get("updates_sha256") is None or _is_sha(value["updates_sha256"]))
            and _validate_nonnegative_int(value.get("update_bytes"))
            and value.get("event_count") == 0
            and value.get("provider_prompt_id_sha256") is None
            and value.get("effective_model_id") is None
            and value.get("started_tool_calls") == 0
            and value.get("completed_tool_calls") == 0
            and value.get("tool_counts") == {}
            and value.get("query_argument_sha256s") == []
            and value.get("candidate_surface_attempts") == []
            and value.get("terminal_stop_reason") is None
            and all(
                value.get(key) is None
                for key in (
                    "input_tokens",
                    "output_tokens",
                    "total_tokens",
                    "model_turns",
                    "estimated_cost_usd_micros",
                )
            )
        )
    tool_counts = value.get("tool_counts")
    query_hashes = value.get("query_argument_sha256s")
    return (
        _is_sha(value.get("updates_sha256"))
        and _validate_nonnegative_int(value.get("update_bytes"))
        and value["update_bytes"] > 0
        and _validate_nonnegative_int(value.get("event_count"))
        and value["event_count"] > 0
        and _is_sha(value.get("provider_prompt_id_sha256"))
        and isinstance(value.get("effective_model_id"), str)
        and _ID_RE.fullmatch(value["effective_model_id"]) is not None
        and _validate_nonnegative_int(value.get("started_tool_calls"))
        and value["started_tool_calls"] > 0
        and value.get("completed_tool_calls") == value["started_tool_calls"]
        and isinstance(tool_counts, dict)
        and all(
            key in NATIVE_X_TOOLS and _validate_nonnegative_int(count) and count > 0
            for key, count in tool_counts.items()
        )
        and sum(tool_counts.values()) == value["completed_tool_calls"]
        and isinstance(query_hashes, list)
        and len(query_hashes) == value["completed_tool_calls"]
        and all(_is_sha(item) for item in query_hashes)
        and _surface_attempts_valid(value.get("candidate_surface_attempts"), query_hashes)
        and value.get("terminal_stop_reason") == "end_turn"
        and all(
            _validate_nonnegative_int(value.get(key))
            for key in (
                "input_tokens",
                "output_tokens",
                "total_tokens",
                "model_turns",
                "estimated_cost_usd_micros",
            )
        )
        and value["total_tokens"] == value["input_tokens"] + value["output_tokens"]
        and value["model_turns"] > 0
    )


def _retention_receipt_valid(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _RETENTION_RECEIPT_KEYS
        and isinstance(value.get("policy_id"), str)
        and _ID_RE.fullmatch(value["policy_id"]) is not None
        and _is_int(value.get("ttl_seconds"))
        and 3_600 <= value["ttl_seconds"] <= 604_800
        and _timestamp_valid(value.get("delete_after"))
        and value.get("purge_state") == "pending_expiry"
        and value.get("deletion_receipt_required") is True
    )


def validate_operator_receipt(receipt: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(receipt, dict) or set(receipt) != _RECEIPT_KEYS:
        return ["receipt_shape_invalid"]
    if receipt.get("schema_version") != RECEIPT_SCHEMA_VERSION:
        errors.append("receipt_schema_version_invalid")
    if not isinstance(receipt.get("run_id"), str) or _RUN_ID_RE.fullmatch(receipt["run_id"]) is None:
        errors.append("receipt_run_id_invalid")
    if not isinstance(receipt.get("request_id"), str) or _REQUEST_ID_RE.fullmatch(receipt["request_id"]) is None:
        errors.append("receipt_request_id_invalid")
    if not _is_sha(receipt.get("request_sha256")):
        errors.append("receipt_request_sha256_invalid")
    if not _is_sha(receipt.get("run_lease_sha256")):
        errors.append("receipt_run_lease_invalid")
    mode = receipt.get("execution_mode")
    status = receipt.get("status")
    if mode not in {"fixture", "live"} or status not in _RECEIPT_STATUSES:
        errors.append("receipt_state_invalid")
    if (
        isinstance(receipt.get("run_id"), str)
        and mode in {"fixture", "live"}
        and not receipt["run_id"].startswith(f"grok_wave_{mode}_")
    ):
        errors.append("receipt_run_mode_mismatch")
    if (mode == "fixture" and status == "completed") or (mode == "live" and status == "fixture_complete"):
        errors.append("receipt_mode_status_mismatch")

    input_binding = receipt.get("input_binding")
    command_binding = receipt.get("command_binding")
    if not _input_binding_valid(input_binding):
        errors.append("receipt_input_binding_invalid")
    if not _command_binding_valid(command_binding):
        errors.append("receipt_command_binding_invalid")
    if not _approval_binding_valid(
        receipt.get("approval"),
        mode,
        terminal=True,
        allow_unconsumed=status == "crash_recovered",
    ):
        errors.append("receipt_approval_invalid")

    process = receipt.get("process")
    process_shape_valid = isinstance(process, dict) and set(process) == _PROCESS_KEYS
    if not process_shape_valid:
        errors.append("receipt_process_shape_invalid")
    else:
        timestamps_valid = all(_timestamp_valid(process.get(key)) for key in ("started_at", "completed_at"))
        timestamps_ordered = False
        if timestamps_valid:
            timestamps_ordered = _parse_timestamp(process["completed_at"]) >= _parse_timestamp(process["started_at"])
        child_values = (process.get("child_pid"), process.get("process_group_id"))
        child_values_valid = all(value is None or (_is_int(value) and value > 0) for value in child_values)
        ledger_hash_valid = process.get("process_ledger_sha256") is None or _is_sha(process["process_ledger_sha256"])
        identity_hashes = (
            process.get("kernel_birth_identity_sha256"),
            process.get("process_identity_token_sha256"),
        )
        identity_hashes_valid = all(value is None or _is_sha(value) for value in identity_hashes)
        spawn_binding_complete = all(
            value is not None for value in (*child_values, process.get("process_ledger_sha256"), *identity_hashes)
        )
        technical_kind = process.get("technical_limit_kind")
        technical_flag = process.get("technical_limit_exceeded")
        if (
            not timestamps_valid
            or not timestamps_ordered
            or (process.get("elapsed_ms") is not None and not _validate_nonnegative_int(process["elapsed_ms"]))
            or (process.get("exit_code") is not None and not _is_int(process["exit_code"]))
            or any(
                type(process.get(key)) is not bool
                for key in (
                    "timed_out",
                    "term_sent",
                    "kill_sent",
                    "process_spawn_attempted",
                    "process_group_cleanup_confirmed",
                    "technical_limit_exceeded",
                )
            )
            or not child_values_valid
            or not ledger_hash_valid
            or not identity_hashes_valid
            or process.get("process_spawn_attempted") is not spawn_binding_complete
            or process.get("execution_error_code")
            not in {
                "none",
                "spawn_failed",
                "process_execution_failed",
                "process_group_cleanup_failed",
                "crash_recovered",
            }
            or not _is_int(process.get("deadline_ms"))
            or not 1_000 <= process["deadline_ms"] <= 3_600_000
            or any(
                not _is_int(process.get(key)) or not 100 <= process[key] <= 60_000
                for key in ("term_grace_ms", "kill_grace_ms")
            )
            or process.get("fallback_used") is not False
            or technical_kind not in _TECHNICAL_LIMIT_KINDS | {None}
            or technical_flag is not (technical_kind is not None)
        ):
            errors.append("receipt_process_value_invalid")

    artifacts = receipt.get("artifacts")
    artifacts_shape_valid = isinstance(artifacts, dict) and set(artifacts) == _ARTIFACT_KEYS
    if not artifacts_shape_valid:
        errors.append("receipt_artifacts_shape_invalid")
    elif (
        not _is_sha(artifacts.get("raw_stdout_sha256"))
        or not _is_sha(artifacts.get("stderr_sha256"))
        or (artifacts.get("sanitized_output_sha256") is not None and not _is_sha(artifacts["sanitized_output_sha256"]))
        or type(artifacts.get("structured_output_compliant")) is not bool
        or type(artifacts.get("structured_output_contract_valid")) is not bool
        or (
            artifacts.get("non_json_prefix_bytes") is not None
            and not _validate_nonnegative_int(artifacts["non_json_prefix_bytes"])
        )
        or (
            artifacts.get("non_json_suffix_bytes") is not None
            and not _validate_nonnegative_int(artifacts["non_json_suffix_bytes"])
        )
        or not _is_sha(artifacts.get("compiled_prompt_sha256"))
        or (artifacts.get("session_updates_sha256") is not None and not _is_sha(artifacts["session_updates_sha256"]))
        or artifacts.get("ephemeral_tree_deleted") is not True
        or any(
            not _validate_nonnegative_int(artifacts.get(key))
            for key in (
                "session_tree_file_count",
                "session_tree_entry_count",
                "session_tree_max_depth",
                "session_tree_total_bytes",
                "session_tree_max_file_bytes",
            )
        )
        or artifacts.get("session_tree_max_depth", MAX_SESSION_TREE_DEPTH + 1) > MAX_SESSION_TREE_DEPTH
    ):
        errors.append("receipt_artifacts_value_invalid")

    session_proof = receipt.get("session_proof")
    if not _session_proof_valid(session_proof, mode):
        errors.append("receipt_session_proof_invalid")
    retention = receipt.get("retention")
    if not _retention_receipt_valid(retention):
        errors.append("receipt_retention_invalid")

    reconciliation = receipt.get("reconciliation")
    reconciliation_valid = _receipt_reconciliation_valid(reconciliation)
    if not reconciliation_valid:
        errors.append("receipt_reconciliation_invalid")
    if (
        reconciliation_valid
        and isinstance(reconciliation, dict)
        and isinstance(session_proof, dict)
        and _session_proof_valid(session_proof, mode)
    ):
        attempt_index: dict[tuple[str, str], set[str]] = {}
        for attempt in session_proof["candidate_surface_attempts"]:
            attempt_index.setdefault((attempt["handle_key"], attempt["surface"]), set()).add(
                attempt["query_argument_sha256"]
            )
        for coverage in reconciliation["candidate_surface_coverage"]:
            for surface in ("authored_post", "authored_reply"):
                expected_hashes = sorted(attempt_index.get((coverage["handle_key"], surface), set()))
                if coverage[surface] != {
                    "attempted": bool(expected_hashes),
                    "query_argument_sha256s": expected_hashes,
                }:
                    errors.append("receipt_candidate_surface_reconciliation_invalid")
                    break
        if status in {"completed", "fixture_complete"} and len(
            reconciliation["candidate_surface_coverage"]
        ) != reconciliation["candidate_count"]:
            errors.append("receipt_candidate_surface_count_invalid")
    if receipt.get("authority") != AUTHORITY:
        errors.append("receipt_authority_invalid")

    if _input_binding_valid(input_binding) and _command_binding_valid(command_binding):
        if command_binding["structured_output_schema_sha256"] != result_schema_sha256():
            errors.append("receipt_result_schema_hash_invalid")
        accepted_command_policies = {
            canonical_sha256(_redacted_policy_from_bindings(input_binding, command_binding)),
            canonical_sha256(
                _redacted_policy_from_bindings(input_binding, command_binding, legacy_plain=True)
            ),
        }
        if command_binding["command_policy_sha256"] not in accepted_command_policies:
            errors.append("receipt_command_policy_hash_invalid")
        if command_binding["environment_policy_sha256"] != canonical_sha256(_redacted_environment_policy()):
            errors.append("receipt_environment_policy_hash_invalid")
        if command_binding["tool_registry_sha256"] != tool_registry_sha256():
            errors.append("receipt_tool_registry_hash_invalid")
        policy_sha = command_binding.get("effective_prompt_policy_sha256")
        policy_entry_id = command_binding.get("effective_prompt_policy_entry_id")
        if mode == "live" and (not _is_sha(policy_sha) or not isinstance(policy_entry_id, str)):
            errors.append("receipt_live_effective_prompt_policy_missing")
        if mode == "fixture" and (policy_sha is not None or policy_entry_id is not None):
            errors.append("receipt_fixture_effective_prompt_policy_invalid")
        if mode == "live" and not _is_sha(command_binding.get("grok_binary_sha256")):
            errors.append("receipt_live_binary_hash_missing")
        if mode == "fixture" and command_binding.get("grok_binary_sha256") is not None:
            errors.append("receipt_fixture_binary_hash_invalid")
        if artifacts_shape_valid and artifacts.get("compiled_prompt_sha256") != input_binding["compiled_prompt_sha256"]:
            errors.append("receipt_compiled_prompt_hash_mismatch")

    if process_shape_valid and artifacts_shape_valid:
        spawned = process.get("process_spawn_attempted") is True
        if mode == "live" and spawned and receipt.get("approval", {}).get("consumption_sha256") is None:
            errors.append("spawn_without_grant_consumption")
        if spawned and process.get("process_group_cleanup_confirmed") is not True:
            errors.append("spawned_process_cleanup_unconfirmed")
        if mode == "fixture" and spawned:
            errors.append("fixture_process_spawn_claim_invalid")
        if status in {"completed", "fixture_complete"} and not (
            process.get("exit_code") == 0
            and process.get("timed_out") is False
            and process.get("execution_error_code") == "none"
            and process.get("technical_limit_exceeded") is False
            and artifacts.get("structured_output_compliant") is True
            and artifacts.get("structured_output_contract_valid") is True
            and _is_sha(artifacts.get("sanitized_output_sha256"))
            and (
                session_proof.get("status") == "verified"
                if mode == "live" and isinstance(session_proof, dict)
                else mode == "fixture"
            )
        ):
            errors.append("receipt_completion_claim_invalid")
        if status == "completed" and not spawned:
            errors.append("receipt_live_completion_spawn_invalid")
        if status == "timed_out" and process.get("timed_out") is not True:
            errors.append("receipt_timeout_claim_invalid")
        if process.get("timed_out") is True and process.get("technical_limit_kind") == "session_tree_scan_deadline":
            errors.append("receipt_timeout_ownership_invalid")
        if status == "technical_limit_exceeded" and process.get("technical_limit_exceeded") is not True:
            errors.append("receipt_technical_limit_claim_invalid")
        if status != "technical_limit_exceeded" and process.get("technical_limit_exceeded") is True:
            errors.append("receipt_unclaimed_technical_limit")
        if status == "structured_output_noncompliant" and artifacts.get("structured_output_compliant") is not False:
            errors.append("receipt_noncompliance_claim_invalid")
        if status == "result_contract_invalid" and not (
            artifacts.get("structured_output_compliant") is True
            and artifacts.get("structured_output_contract_valid") is False
        ):
            errors.append("receipt_contract_failure_claim_invalid")
        if status == "provider_evidence_invalid" and not (
            mode == "live"
            and isinstance(session_proof, dict)
            and session_proof.get("status") in {"missing", "invalid"}
            and process.get("exit_code") == 0
            and process.get("timed_out") is False
            and process.get("execution_error_code") == "none"
            and artifacts.get("structured_output_compliant") is True
            and artifacts.get("structured_output_contract_valid") is True
        ):
            errors.append("receipt_provider_evidence_failure_claim_invalid")
        if status in {"structured_output_noncompliant", "result_contract_invalid"} and not (
            process.get("exit_code") == 0
            and process.get("timed_out") is False
            and process.get("execution_error_code") == "none"
        ):
            errors.append("receipt_structured_failure_process_invalid")
        if status == "process_failed" and not (
            process.get("exit_code") not in {None, 0} or process.get("execution_error_code") != "none"
        ):
            errors.append("receipt_process_failure_claim_invalid")
        if status == "crash_recovered" and not (
            process.get("elapsed_ms") is None
            and process.get("exit_code") is None
            and process.get("execution_error_code") == "crash_recovered"
        ):
            errors.append("receipt_crash_recovery_claim_invalid")
    return errors


def validate_operator_bundle(
    run_root: Path,
    *,
    approval_root: Path = DEFAULT_APPROVAL_ROOT,
    receipt_override: Mapping[str, Any] | None = None,
) -> list[str]:
    """Replay every durable binding without trusting the terminal receipt."""

    # Durable argv bindings are emitted with absolute module-owned paths.  A
    # caller may naturally locate the same bundle through a relative path;
    # normalize that spelling without resolving symlinks before no-follow
    # ownership checks and command reconstruction.
    run_root = Path(os.path.abspath(run_root))
    errors: list[str] = []
    try:
        _ensure_private_directory(run_root, create=False)
        request = _read_private_json(run_root / "operator-request.json")
        intent = _read_private_json(run_root / "operator-intent.json")
        receipt = (
            dict(receipt_override)
            if receipt_override is not None
            else _read_private_json(run_root / "operator-receipt.json")
        )
    except AdaptiveWaveValidationError as exc:
        return [str(exc)]
    request_contract_errors = validate_request(request)
    if request_contract_errors:
        errors.append("request_invalid")
    if not _intent_valid(intent):
        errors.append("intent_invalid")
    errors.extend(validate_operator_receipt(receipt))
    if not isinstance(request, dict) or not isinstance(intent, dict) or not isinstance(receipt, dict):
        return errors + ["bundle_object_invalid"]
    if request_contract_errors:
        return errors
    request_sha = canonical_sha256(request)
    if request_sha != receipt.get("request_sha256") or request_sha != intent.get("request_sha256"):
        errors.append("request_binding_hash_mismatch")
    if request.get("request_id") != intent.get("request_id") or request.get("request_id") != receipt.get("request_id"):
        errors.append("request_id_binding_mismatch")
    for key in ("emergency", "technical_limits", "budget", "retention"):
        if intent.get(key) != request.get(key):
            errors.append(f"intent_{key}_binding_mismatch")
    try:
        lease_sha = _read_run_lease_sha256(run_root)
    except AdaptiveWaveValidationError:
        errors.append("run_lease_replay_invalid")
        lease_sha = None
    if lease_sha is not None and (
        intent.get("run_lease_sha256") != lease_sha or receipt.get("run_lease_sha256") != lease_sha
    ):
        errors.append("run_lease_binding_mismatch")
    if isinstance(intent, dict) and isinstance(receipt, dict):
        for key in (
            "run_id",
            "request_id",
            "request_sha256",
            "execution_mode",
            "run_lease_sha256",
            "input_binding",
            "command_binding",
            "authority",
        ):
            if intent.get(key) != receipt.get(key):
                errors.append(f"intent_receipt_binding_mismatch:{key}")
        intent_approval = intent.get("approval")
        receipt_approval = receipt.get("approval")
        if isinstance(intent_approval, dict) and isinstance(receipt_approval, dict):
            for key in ("required", "grant_id_sha256", "grant_sha256"):
                if intent_approval.get(key) != receipt_approval.get(key):
                    errors.append(f"intent_receipt_approval_mismatch:{key}")
            if intent_approval.get("consumption_sha256") is not None:
                errors.append("intent_contains_terminal_consumption")
    prior_candidates: dict[str, PriorCandidateFacts] = {}
    compiled_prompt_path = run_root / "compiled-prompt.txt"
    try:
        compiled_actual = _read_regular_owned_bounded(
            compiled_prompt_path,
            maximum_bytes=request["technical_limits"]["max_compiled_prompt_bytes"],
            required_mode=0o600,
        )
    except AdaptiveWaveValidationError:
        errors.append("compiled_prompt_permissions_invalid")
        compiled_actual = None
    if compiled_actual is not None:
        try:
            prompt_raw = _load_bound_bytes(
                request["prompt_source"]["path"],
                request["prompt_source"]["sha256"],
                require_private=True,
                max_bytes=request["technical_limits"]["max_prompt_bytes"],
            )
            prior_handles, prior_bindings, prior_candidates = load_prior_context(request)
            compiled_expected = compile_prompt(
                prompt_raw.decode("utf-8"),
                request["target"],
                prior_handles,
                result_schema=_load_result_schema(),
            ).encode()
            if len(compiled_expected) > request["technical_limits"]["max_compiled_prompt_bytes"]:
                raise AdaptiveWaveValidationError("compiled_prompt_byte_ceiling_exceeded")
        except (AdaptiveWaveValidationError, UnicodeDecodeError, KeyError):
            errors.append("compiled_prompt_replay_failed")
            prior_handles, prior_bindings, prior_candidates = [], [], {}
        else:
            if compiled_actual != compiled_expected:
                errors.append("compiled_prompt_content_mismatch")
            expected_input = {
                "target_sha256": canonical_sha256(request["target"]),
                "source_prompt_sha256": bytes_sha256(prompt_raw),
                "compiled_prompt_sha256": bytes_sha256(compiled_expected),
                "prior_waves": prior_bindings,
                "prior_unique_handle_count": len(prior_handles),
                "prior_handle_set_sha256": canonical_sha256(sorted(item.casefold() for item in prior_handles)),
            }
            if intent.get("input_binding") != expected_input or receipt.get("input_binding") != expected_input:
                errors.append("input_binding_replay_mismatch")

    ephemeral_home = run_root / intent.get("runtime_layout", {}).get("ephemeral_home_name", "ephemeral-home")
    if ephemeral_home.exists() or ephemeral_home.is_symlink():
        errors.append("ephemeral_tree_not_deleted")

    mode = receipt.get("execution_mode")
    command_binding = receipt.get("command_binding", {})
    input_binding = receipt.get("input_binding", {})
    new_command_policy = command_policy_sha256(request)
    legacy_command_policy = _legacy_command_policy_sha256(request)
    recorded_command_policy = (
        command_binding.get("command_policy_sha256") if isinstance(command_binding, dict) else None
    )
    legacy_plain_replay = recorded_command_policy == legacy_command_policy
    if recorded_command_policy not in {new_command_policy, legacy_command_policy}:
        errors.append("command_policy_version_unrecognized")
    session_id = command_binding.get("session_id") if isinstance(command_binding, dict) else None
    expected_binary_sha: str | None = None
    if mode == "live":
        staged_path = run_root / "executable/grok"
        try:
            expected_binary_sha = _rehash_staged_executable(staged_path)
        except AdaptiveWaveValidationError:
            errors.append("staged_binary_replay_invalid")
        else:
            if expected_binary_sha != request.get("transport", {}).get("grok_binary_sha256"):
                errors.append("staged_binary_request_hash_mismatch")
    elif (run_root / "executable").exists():
        errors.append("fixture_staged_binary_unexpected")
    if (
        isinstance(session_id, str)
        and _SESSION_ID_RE.fullmatch(session_id) is not None
        and _input_binding_valid(input_binding)
    ):
        runtime_layout = intent.get("runtime_layout", {})
        command_binary = staged_path if mode == "live" else Path("fixture-grok.invalid")
        command_builder = _build_legacy_plain_grok_command if legacy_plain_replay else build_grok_command
        actual_command = command_builder(
            binary=command_binary,
            cwd=run_root / "workspace",
            request=request,
            prompt_file=run_root / runtime_layout.get("compiled_prompt_name", "compiled-prompt.txt"),
            leader_socket=ephemeral_home / "leader.sock",
            session_id=session_id,
            result_schema=_load_result_schema(),
        )
        account_ref = request["transport"]["operator_account_ref"]
        try:
            effective_prompt_policy = _approved_effective_prompt_binding(request) if mode == "live" else None
        except (AdaptiveWaveValidationError, PermissionError):
            effective_prompt_policy = None
            errors.append("effective_prompt_policy_replay_invalid")
        expected_command_binding = {
            "provider_id": request["transport"]["provider_id"],
            "model_id": request["transport"]["model_id"],
            "reasoning_effort": request["transport"]["reasoning_effort"],
            "session_id": session_id,
            "grok_binary_sha256": expected_binary_sha,
            "structured_output_schema_sha256": result_schema_sha256(),
            "argv_sha256": canonical_sha256(actual_command),
            "command_policy_sha256": legacy_command_policy if legacy_plain_replay else new_command_policy,
            "environment_policy_sha256": canonical_sha256(_redacted_environment_policy()),
            "tool_registry_sha256": tool_registry_sha256(),
            "effective_prompt_policy_sha256": (
                effective_prompt_policy.policy_sha256 if effective_prompt_policy is not None else None
            ),
            "effective_prompt_policy_entry_id": (
                effective_prompt_policy.policy_entry_id if effective_prompt_policy is not None else None
            ),
            "operator_account_ref_sha256": (bytes_sha256(account_ref.encode()) if account_ref is not None else None),
            "oauth_auth_sha256": request["transport"]["oauth_auth_sha256"],
            "max_turns": request["emergency"]["max_turns"],
        }
        if command_binding != expected_command_binding or intent.get("command_binding") != expected_command_binding:
            errors.append("command_binding_request_replay_mismatch")
    else:
        errors.append("command_binding_request_replay_unavailable")
    approval = receipt.get("approval", {})
    consumed_grant_clock: datetime | None = None
    expires_grant_clock: datetime | None = None
    if mode == "live":
        grant_id = request.get("approval", {}).get("grant_id")
        if not isinstance(grant_id, str):
            errors.append("live_grant_id_missing")
        else:
            grant_id_hash = bytes_sha256(grant_id.encode())
            grant_path, consumption_path = _grant_paths(approval_root, grant_id_hash)
            try:
                grant = _read_private_json(grant_path)
                grant_raw = _read_regular_owned_bounded(grant_path, maximum_bytes=1_048_576, required_mode=0o600)
            except AdaptiveWaveValidationError:
                errors.append("grant_ledger_unavailable")
            else:
                if approval.get("grant_id_sha256") != grant_id_hash:
                    errors.append("grant_id_hash_mismatch")
                if approval.get("grant_sha256") != bytes_sha256(grant_raw):
                    errors.append("grant_hash_mismatch")
                if consumption_path.exists():
                    try:
                        consumption = _read_private_json(consumption_path)
                        consumption_raw = _read_regular_owned_bounded(
                            consumption_path, maximum_bytes=1_048_576, required_mode=0o600
                        )
                        consumed_clock = _parse_timestamp(consumption.get("consumed_at"))
                        expires_clock = _parse_timestamp(grant.get("expires_at"))
                    except (AdaptiveWaveValidationError, TypeError):
                        errors.append("grant_consumed_at_invalid")
                    else:
                        consumed_grant_clock = consumed_clock
                        expires_grant_clock = expires_clock
                        if _validate_grant(
                            grant,
                            request,
                            now=consumed_clock,
                            replay_command_policy_sha256=(
                                legacy_command_policy if legacy_plain_replay else new_command_policy
                            ),
                        ):
                            errors.append("grant_replay_invalid")
                        expected_consumption = {
                            "schema_version": CONSUMPTION_SCHEMA_VERSION,
                            "grant_id_hash": grant_id_hash,
                            "grant_sha256": bytes_sha256(grant_raw),
                            "execution_scope_sha256": execution_scope_sha256(request),
                            "request_sha256": request_sha,
                            "run_id": receipt.get("run_id"),
                            "run_lease_sha256": receipt.get("run_lease_sha256"),
                            "consumed_at": consumption.get("consumed_at"),
                            "state": "consumed_after_binary_auth_preflight_before_process_spawn",
                        }
                        if consumption != expected_consumption:
                            errors.append("grant_consumption_replay_mismatch")
                        if approval.get("consumption_sha256") != bytes_sha256(consumption_raw):
                            errors.append("grant_consumption_hash_mismatch")
                elif approval.get("consumption_sha256") is not None or receipt.get("status") != "crash_recovered":
                    errors.append("grant_consumption_missing")
    elif isinstance(approval, dict) and (
        approval.get("grant_sha256") is not None or approval.get("consumption_sha256") is not None
    ):
        errors.append("fixture_grant_consumption_invalid")

    process = receipt.get("process", {})
    if isinstance(process, dict):
        expected_emergency_process = {
            "deadline_ms": request["emergency"]["deadline_ms"],
            "term_grace_ms": request["emergency"]["term_grace_ms"],
            "kill_grace_ms": request["emergency"]["kill_grace_ms"],
        }
        if any(process.get(key) != value for key, value in expected_emergency_process.items()):
            errors.append("receipt_emergency_binding_mismatch")
        if process.get("started_at") != intent.get("started_at"):
            errors.append("receipt_started_at_intent_mismatch")
    ledger_path = run_root / "process-ledger.json"
    if isinstance(process, dict) and process.get("process_spawn_attempted") is True:
        try:
            ledger = _read_private_json(ledger_path)
            ledger_raw = _read_regular_owned_bounded(ledger_path, maximum_bytes=1_048_576, required_mode=0o600)
        except AdaptiveWaveValidationError:
            errors.append("process_ledger_unavailable")
        else:
            if not _process_ledger_valid(ledger):
                errors.append("process_ledger_invalid")
            expected_process_facts = {
                "run_id": receipt.get("run_id"),
                "request_id": receipt.get("request_id"),
                "run_lease_sha256": receipt.get("run_lease_sha256"),
                "session_id": receipt.get("command_binding", {}).get("session_id"),
                "child_pid": process.get("child_pid"),
                "process_group_id": process.get("process_group_id"),
            }
            if any(ledger.get(key) != value for key, value in expected_process_facts.items()):
                errors.append("process_ledger_binding_mismatch")
            if process.get("process_ledger_sha256") != bytes_sha256(ledger_raw):
                errors.append("process_ledger_hash_mismatch")
            if process.get("kernel_birth_identity_sha256") != bytes_sha256(
                ledger.get("kernel_birth_identity", "").encode()
            ):
                errors.append("process_birth_identity_hash_mismatch")
            if process.get("process_identity_token_sha256") != bytes_sha256(
                ledger.get("process_identity_token", "").encode()
            ):
                errors.append("process_identity_token_hash_mismatch")
            try:
                launcher_verified_clock = _parse_timestamp(ledger.get("spawned_at"))
            except (AdaptiveWaveValidationError, TypeError):
                errors.append("process_ledger_grant_time_invalid")
            else:
                if (
                    mode == "live"
                    and receipt.get("status") == "completed"
                    and (
                        consumed_grant_clock is None
                        or expires_grant_clock is None
                        or not consumed_grant_clock <= launcher_verified_clock < expires_grant_clock
                    )
                ):
                    errors.append("process_release_outside_grant_window")
    elif ledger_path.exists():
        errors.append("unexpected_process_ledger")

    raw_path = run_root / "raw.stdout"
    stderr_path = run_root / "stderr.txt"
    limits = request.get("technical_limits", {})
    if not _technical_limits_valid(limits):
        return errors + ["technical_limits_replay_invalid"]
    try:
        raw = _read_regular_owned_bounded(raw_path, maximum_bytes=limits["max_stdout_bytes"], required_mode=0o600)
        stderr = _read_regular_owned_bounded(stderr_path, maximum_bytes=limits["max_stderr_bytes"], required_mode=0o600)
    except AdaptiveWaveValidationError:
        errors.append("required_artifact_permissions_invalid")
        return errors
    artifacts = receipt.get("artifacts", {}) if isinstance(receipt, dict) else {}
    if bytes_sha256(raw) != artifacts.get("raw_stdout_sha256"):
        errors.append("raw_stdout_hash_mismatch")
    if bytes_sha256(stderr) != artifacts.get("stderr_sha256"):
        errors.append("stderr_hash_mismatch")
    (
        parsed_result,
        sanitized,
        prefix,
        suffix,
        compliant,
        contract_valid,
        limit_kind,
        headless_envelope,
    ) = _parse_structured_stdout(
        raw,
        technical_limits=limits,
        prior_candidates=prior_candidates,
        live_mode=mode == "live",
        expected_session_id=command_binding.get("session_id") if isinstance(command_binding, dict) else None,
        max_turns=request["emergency"]["max_turns"],
        allow_legacy_plain=mode == "fixture" or legacy_plain_replay,
    )
    if receipt.get("status") != "crash_recovered":
        if artifacts.get("non_json_prefix_bytes") != prefix or artifacts.get("non_json_suffix_bytes") != suffix:
            errors.append("structured_boundary_mismatch")
        if artifacts.get("structured_output_compliant") is not compliant:
            errors.append("structured_compliance_mismatch")
        recorded_limit = process.get("technical_limit_kind") if isinstance(process, dict) else None
        if limit_kind is not None and recorded_limit != limit_kind:
            errors.append("structured_technical_limit_mismatch")
    session_proof: SessionProof | None = None
    session_raw: bytes | None = None
    session_status = "not_applicable" if mode == "fixture" else "missing"
    updates_path = run_root / intent.get("runtime_layout", {}).get("session_updates_name", "session-updates.jsonl")
    if mode == "live" and updates_path.exists():
        try:
            session_raw = _read_regular_owned_bounded(
                updates_path,
                maximum_bytes=limits["max_session_updates_bytes"],
                required_mode=0o600,
            )
            session_proof = _parse_session_proof(
                session_raw,
                expected_session_id=command_binding["session_id"],
                expected_model_id=request["transport"]["model_id"],
                expected_stdout=raw,
                headless_envelope=headless_envelope,
                max_line_bytes=limits["max_session_update_line_bytes"],
                max_turns=request["emergency"]["max_turns"],
                budget=request["budget"],
            )
            session_status = "verified"
        except (AdaptiveWaveValidationError, KeyError, UnicodeError, ValueError):
            session_proof = None
            session_status = "invalid"
    if contract_valid and isinstance(parsed_result, dict) and (mode == "fixture" or session_proof is not None):
        parsed_result = _operator_project_model_result(
            parsed_result,
            session_proof=session_proof,
            fixture=mode == "fixture",
        )
        contract_valid = not validate_model_result(
            parsed_result,
            prior_candidates=prior_candidates,
            live_mode=mode == "live",
        )
        sanitized = (canonical_json(parsed_result) + "\n").encode()
    if receipt.get("status") != "crash_recovered" and (
        artifacts.get("structured_output_contract_valid") is not contract_valid
    ):
        errors.append("structured_contract_mismatch")
    expected_session_payload = _session_proof_payload(
        session_proof,
        status=session_status,
        raw=session_raw,
    )
    if receipt.get("session_proof") != expected_session_payload:
        errors.append("session_proof_replay_mismatch")
    expected_updates_sha = bytes_sha256(session_raw) if session_raw is not None else None
    if artifacts.get("session_updates_sha256") != expected_updates_sha:
        errors.append("session_updates_hash_mismatch")
    if mode == "fixture" and updates_path.exists():
        errors.append("fixture_session_updates_unexpected")
    expected_reconciliation = _candidate_reconciliation(
        parsed_result if isinstance(parsed_result, dict) else {},
        prior_candidates,
        session_proof=session_proof,
        fixture=mode == "fixture",
    )
    if receipt.get("reconciliation") != expected_reconciliation:
        errors.append("receipt_reconciliation_mismatch")
    sanitized_path = run_root / "sanitized.json"
    if sanitized is None:
        if artifacts.get("sanitized_output_sha256") is not None or sanitized_path.exists():
            errors.append("unexpected_sanitized_artifact")
    else:
        try:
            sanitized_actual = _read_regular_owned_bounded(
                sanitized_path, maximum_bytes=limits["max_json_bytes"], required_mode=0o600
            )
        except AdaptiveWaveValidationError:
            errors.append("sanitized_artifact_permissions_invalid")
        else:
            if sanitized_actual != sanitized or bytes_sha256(sanitized) != artifacts.get("sanitized_output_sha256"):
                errors.append("sanitized_artifact_hash_mismatch")
    if _command_binding_valid(command_binding) and _input_binding_valid(input_binding):
        if command_binding["structured_output_schema_sha256"] != result_schema_sha256():
            errors.append("structured_output_schema_hash_mismatch")
        if command_binding["command_policy_sha256"] not in {
            command_policy_sha256(request),
            _legacy_command_policy_sha256(request),
        }:
            errors.append("command_policy_hash_mismatch")
        if command_binding["environment_policy_sha256"] != canonical_sha256(_redacted_environment_policy()):
            errors.append("environment_policy_hash_mismatch")
    compiled_sha = bytes_sha256(compiled_actual) if compiled_actual is not None else None
    if compiled_sha != artifacts.get("compiled_prompt_sha256"):
        errors.append("compiled_prompt_artifact_hash_mismatch")
    if isinstance(artifacts, dict) and (
        artifacts.get("session_tree_file_count", 0) > limits["max_session_files"]
        or artifacts.get("session_tree_entry_count", 0) > limits["max_session_files"]
        or artifacts.get("session_tree_max_depth", 0) > MAX_SESSION_TREE_DEPTH
        or artifacts.get("session_tree_total_bytes", 0) > limits["max_session_total_bytes"]
        or artifacts.get("session_tree_max_file_bytes", 0) > limits["max_session_file_bytes"]
    ):
        errors.append("session_tree_measurement_exceeds_request")
    retention = receipt.get("retention")
    process_started = process.get("started_at") if isinstance(process, dict) else None
    try:
        expected_delete_after = _timestamp(
            _parse_timestamp(process_started) + timedelta(seconds=request["retention"]["ttl_seconds"])
        )
    except (AdaptiveWaveValidationError, TypeError):
        errors.append("retention_time_replay_invalid")
    else:
        expected_retention = {
            "policy_id": request["retention"]["policy_id"],
            "ttl_seconds": request["retention"]["ttl_seconds"],
            "delete_after": expected_delete_after,
            "purge_state": "pending_expiry",
            "deletion_receipt_required": True,
        }
        if retention != expected_retention:
            errors.append("retention_replay_mismatch")
    return errors


def _process_group_members(process_group_id: int) -> list[int]:
    proc_root = Path("/proc")
    members: list[int] = []
    if proc_root.is_dir():
        for entry in proc_root.iterdir():
            if not entry.name.isdigit():
                continue
            try:
                if os.getpgid(int(entry.name)) == process_group_id:
                    members.append(int(entry.name))
            except (ProcessLookupError, PermissionError):
                continue
        return sorted(members)
    try:
        completed = subprocess.run(  # noqa: S603 - fixed absolute diagnostic binary
            ["/bin/ps", "-axo", "pid=,pgid="],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
            env={"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError):
        return []
    for line in completed.stdout.splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0].isdigit() and fields[1].isdigit() and int(fields[1]) == process_group_id:
            members.append(int(fields[0]))
    return sorted(members)


def _process_identity_token_matches(pid: int, token: str) -> bool:
    marker = f"X_FIRST_PROCESS_IDENTITY={token}"
    environ_path = Path(f"/proc/{pid}/environ")
    if environ_path.is_file():
        try:
            return marker.encode() in environ_path.read_bytes().split(b"\x00")
        except OSError:
            return False
    try:
        completed = subprocess.run(  # noqa: S603 - fixed absolute diagnostic binary
            ["/bin/ps", "eww", "-p", str(pid), "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
            env={"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return completed.returncode == 0 and marker in completed.stdout


def _recorded_process_group_identity_matches(ledger: Mapping[str, Any]) -> bool:
    process_group_id = ledger.get("process_group_id")
    child_pid = ledger.get("child_pid")
    token = ledger.get("process_identity_token")
    if not _is_int(process_group_id) or not _is_int(child_pid) or not isinstance(token, str):
        return False
    members = _process_group_members(process_group_id)
    if not members or any(not _process_identity_token_matches(pid, token) for pid in members):
        return False
    if child_pid in members:
        try:
            if _kernel_birth_identity(child_pid) != ledger.get("kernel_birth_identity"):
                return False
        except AdaptiveWaveValidationError:
            return False
    return True


def _terminate_existing_group(
    process_group_id: int,
    *,
    term_grace_ms: int,
    kill_grace_ms: int,
    group_is_alive: Callable[[int], bool],
    identity_still_matches: Callable[[], bool],
    monotonic: Callable[[], float],
) -> tuple[bool, bool]:
    term_sent = False
    kill_sent = False
    if group_is_alive(process_group_id):
        if not identity_still_matches():
            raise AdaptiveWaveValidationError("recovery_process_identity_changed")
        term_sent = True
        try:
            os.killpg(process_group_id, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = monotonic() + term_grace_ms / 1000
        while group_is_alive(process_group_id) and monotonic() < deadline:
            time.sleep(0.02)
    if group_is_alive(process_group_id):
        if not identity_still_matches():
            return term_sent, kill_sent
        kill_sent = True
        try:
            os.killpg(process_group_id, signal.SIGKILL)
        except ProcessLookupError:
            pass
        deadline = monotonic() + kill_grace_ms / 1000
        while group_is_alive(process_group_id) and monotonic() < deadline:
            time.sleep(0.02)
    if group_is_alive(process_group_id):
        if identity_still_matches():
            raise AdaptiveWaveValidationError("recovery_process_group_cleanup_failed")
    return term_sent, kill_sent


def recover_incomplete_run(
    run_root: Path,
    *,
    approval_root: Path = DEFAULT_APPROVAL_ROOT,
    terminate_orphan: bool = False,
    process_group_is_alive: Callable[[int], bool] = ProcessGroupExecutor._group_alive,
    process_group_identity_matches: Callable[[Mapping[str, Any]], bool] = _recorded_process_group_identity_matches,
    terminate_process_group: Callable[..., tuple[bool, bool]] = _terminate_existing_group,
    monotonic: Callable[[], float] = time.monotonic,
    wall_clock: Callable[[], datetime] = _utc_now,
) -> dict[str, Any]:
    """Acquire the exclusive run lease before any recovery read or mutation."""

    _ensure_private_directory(run_root, create=False)
    with _run_lease(run_root, create=False) as lease_sha:
        return _recover_incomplete_run_locked(
            run_root,
            held_lease_sha=lease_sha,
            approval_root=approval_root,
            terminate_orphan=terminate_orphan,
            process_group_is_alive=process_group_is_alive,
            process_group_identity_matches=process_group_identity_matches,
            terminate_process_group=terminate_process_group,
            monotonic=monotonic,
            wall_clock=wall_clock,
        )


def _recover_incomplete_run_locked(
    run_root: Path,
    *,
    held_lease_sha: str,
    approval_root: Path,
    terminate_orphan: bool = False,
    process_group_is_alive: Callable[[int], bool] = ProcessGroupExecutor._group_alive,
    process_group_identity_matches: Callable[[Mapping[str, Any]], bool] = _recorded_process_group_identity_matches,
    terminate_process_group: Callable[..., tuple[bool, bool]] = _terminate_existing_group,
    monotonic: Callable[[], float] = time.monotonic,
    wall_clock: Callable[[], datetime] = _utc_now,
) -> dict[str, Any]:
    """Seal an interrupted run only after its recorded process group is dead."""

    recover_pending_publications(run_root)
    if (run_root / "operator-receipt.json").exists():
        raise AdaptiveWaveValidationError("run_already_terminal")
    intent = _read_private_json(run_root / "operator-intent.json")
    if not _intent_valid(intent):
        raise AdaptiveWaveValidationError("intent_invalid")
    if intent["run_lease_sha256"] != held_lease_sha:
        raise AdaptiveWaveValidationError("recovery_run_lease_binding_invalid")
    request = _read_private_json(run_root / "operator-request.json")
    if validate_request(request) or canonical_sha256(request) != intent["request_sha256"]:
        raise AdaptiveWaveValidationError("recovery_request_invalid")
    prior_handles, _, prior_candidates = load_prior_context(request)
    del prior_handles
    ledger_path = run_root / "process-ledger.json"
    ledger: dict[str, Any] | None = None
    process_ledger_sha: str | None = None
    term_sent = False
    kill_sent = False
    if ledger_path.exists():
        ledger_value = _read_private_json(ledger_path)
        if not _process_ledger_valid(ledger_value):
            raise AdaptiveWaveValidationError("process_ledger_invalid")
        ledger = ledger_value
        if (
            ledger["run_id"] != intent["run_id"]
            or ledger["request_id"] != intent["request_id"]
            or ledger["run_lease_sha256"] != held_lease_sha
            or ledger["session_id"] != intent["command_binding"]["session_id"]
        ):
            raise AdaptiveWaveValidationError("process_ledger_binding_invalid")
        process_ledger_sha = bytes_sha256(
            _read_regular_owned_bounded(ledger_path, maximum_bytes=1_048_576, required_mode=0o600)
        )
        group_id = ledger["process_group_id"]
        if process_group_is_alive(group_id):
            if not terminate_orphan:
                raise AdaptiveWaveValidationError("run_process_group_still_alive")
            if not process_group_identity_matches(ledger):
                raise AdaptiveWaveValidationError("recovery_process_identity_mismatch")
            term_sent, kill_sent = terminate_process_group(
                group_id,
                term_grace_ms=intent["emergency"]["term_grace_ms"],
                kill_grace_ms=intent["emergency"]["kill_grace_ms"],
                group_is_alive=process_group_is_alive,
                identity_still_matches=lambda: process_group_identity_matches(ledger),
                monotonic=monotonic,
            )
        if process_group_is_alive(group_id):
            raise AdaptiveWaveValidationError("run_process_group_still_alive")
    ephemeral_home = run_root / intent["runtime_layout"]["ephemeral_home_name"]
    retained_updates_path = run_root / intent["runtime_layout"]["session_updates_name"]
    measurement = SessionTreeMeasurement(0, 0, 0, 0, 0, None)
    if ephemeral_home.exists():
        source_updates_path = _session_updates_path(
            ephemeral_home,
            run_root / "workspace",
            intent["command_binding"]["session_id"],
        )
        measurement = _measure_session_tree(
            ephemeral_home,
            max_files=intent["technical_limits"]["max_session_files"],
            max_file_bytes=intent["technical_limits"]["max_session_file_bytes"],
            max_total_bytes=intent["technical_limits"]["max_session_total_bytes"],
            updates_path=source_updates_path,
            max_updates_bytes=intent["technical_limits"]["max_session_updates_bytes"],
            expected_socket_path=ephemeral_home / "leader.sock",
            max_depth=MAX_SESSION_TREE_DEPTH,
            deadline_at=monotonic() + FINAL_SESSION_TREE_SCAN_BUDGET_SECONDS,
            monotonic=monotonic,
        )
        if measurement.limit_kind is None and source_updates_path.exists():
            updates_raw = _read_regular_owned_bounded(
                source_updates_path,
                maximum_bytes=intent["technical_limits"]["max_session_updates_bytes"],
            )
            if retained_updates_path.exists():
                if (
                    _read_regular_owned_bounded(
                        retained_updates_path,
                        maximum_bytes=intent["technical_limits"]["max_session_updates_bytes"],
                        required_mode=0o600,
                    )
                    != updates_raw
                ):
                    raise AdaptiveWaveValidationError("recovery_session_updates_mismatch")
            else:
                _atomic_publish(retained_updates_path, updates_raw)
        _delete_ephemeral_tree(ephemeral_home)
    raw_path = run_root / "raw.stdout"
    stderr_path = run_root / "stderr.txt"
    for final_path, spool_name, ceiling in (
        (raw_path, intent["runtime_layout"]["stdout_spool_name"], intent["technical_limits"]["max_stdout_bytes"]),
        (
            stderr_path,
            intent["runtime_layout"]["stderr_spool_name"],
            intent["technical_limits"]["max_stderr_bytes"],
        ),
    ):
        spool_path = run_root / spool_name
        if not final_path.exists() and spool_path.exists():
            try:
                spool_raw = _read_regular_owned_bounded(spool_path, maximum_bytes=ceiling, required_mode=0o600)
            except AdaptiveWaveValidationError:
                raise AdaptiveWaveValidationError("recovery_spool_invalid")
            _atomic_publish(final_path, spool_raw)
            spool_path.unlink()
    if not raw_path.exists():
        _atomic_publish(raw_path, b"")
    if not stderr_path.exists():
        _atomic_publish(stderr_path, b"")
    try:
        raw = _read_regular_owned_bounded(
            raw_path,
            maximum_bytes=intent["technical_limits"]["max_stdout_bytes"],
            required_mode=0o600,
        )
        stderr = _read_regular_owned_bounded(
            stderr_path,
            maximum_bytes=intent["technical_limits"]["max_stderr_bytes"],
            required_mode=0o600,
        )
    except AdaptiveWaveValidationError:
        raise AdaptiveWaveValidationError("recovery_artifact_permissions_invalid")
    recovery_legacy_plain = intent["command_binding"].get("command_policy_sha256") == _legacy_command_policy_sha256(
        request
    )
    parsed_result, sanitized, _, _, _, contract_valid, _, headless_envelope = _parse_structured_stdout(
        raw,
        technical_limits=intent["technical_limits"],
        prior_candidates=prior_candidates,
        live_mode=intent["execution_mode"] == "live",
        expected_session_id=intent["command_binding"]["session_id"],
        max_turns=intent["emergency"]["max_turns"],
        allow_legacy_plain=intent["execution_mode"] == "fixture" or recovery_legacy_plain,
    )
    sanitized_path = run_root / "sanitized.json"
    compiled_prompt_path = run_root / intent["runtime_layout"]["compiled_prompt_name"]
    try:
        compiled_raw = _read_regular_owned_bounded(
            compiled_prompt_path,
            maximum_bytes=intent["technical_limits"]["max_compiled_prompt_bytes"],
            required_mode=0o600,
        )
    except AdaptiveWaveValidationError as exc:
        raise AdaptiveWaveValidationError("recovery_compiled_prompt_invalid") from exc
    approval = dict(intent["approval"])
    if intent["execution_mode"] == "live":
        grant_id = request["approval"]["grant_id"]
        if isinstance(grant_id, str):
            _, consumption_path = _grant_paths(approval_root, bytes_sha256(grant_id.encode()))
            if consumption_path.exists():
                consumption_raw = _read_regular_owned_bounded(
                    consumption_path, maximum_bytes=1_048_576, required_mode=0o600
                )
                approval["consumption_sha256"] = bytes_sha256(consumption_raw)
    session_raw: bytes | None = None
    session_proof: SessionProof | None = None
    session_status = "not_applicable" if intent["execution_mode"] == "fixture" else "missing"
    if intent["execution_mode"] == "live" and retained_updates_path.exists():
        try:
            session_raw = _read_regular_owned_bounded(
                retained_updates_path,
                maximum_bytes=intent["technical_limits"]["max_session_updates_bytes"],
                required_mode=0o600,
            )
            session_proof = _parse_session_proof(
                session_raw,
                expected_session_id=intent["command_binding"]["session_id"],
                expected_model_id=intent["command_binding"]["model_id"],
                expected_stdout=raw,
                headless_envelope=headless_envelope,
                max_line_bytes=intent["technical_limits"]["max_session_update_line_bytes"],
                max_turns=intent["emergency"]["max_turns"],
                budget=intent["budget"],
            )
            session_status = "verified"
        except (AdaptiveWaveValidationError, KeyError, UnicodeError, ValueError):
            session_proof = None
            session_status = "invalid"
    if contract_valid and isinstance(parsed_result, dict) and (
        intent["execution_mode"] == "fixture" or session_proof is not None
    ):
        parsed_result = _operator_project_model_result(
            parsed_result,
            session_proof=session_proof,
            fixture=intent["execution_mode"] == "fixture",
        )
        contract_valid = not validate_model_result(
            parsed_result,
            prior_candidates=prior_candidates,
            live_mode=intent["execution_mode"] == "live",
        )
        sanitized = (canonical_json(parsed_result) + "\n").encode()
    if sanitized_path.exists():
        try:
            sanitized_actual = _read_regular_owned_bounded(
                sanitized_path,
                maximum_bytes=intent["technical_limits"]["max_json_bytes"],
                required_mode=0o600,
            )
        except AdaptiveWaveValidationError as exc:
            raise AdaptiveWaveValidationError("recovery_sanitized_permissions_invalid") from exc
        if sanitized is None or sanitized_actual != sanitized:
            raise AdaptiveWaveValidationError("recovery_sanitized_hash_mismatch")
    elif sanitized is not None:
        _atomic_publish(sanitized_path, sanitized)
    sanitized_sha = bytes_sha256(sanitized) if sanitized is not None else None
    session_payload = _session_proof_payload(session_proof, status=session_status, raw=session_raw)
    delete_after = _timestamp(
        _parse_timestamp(intent["started_at"]) + timedelta(seconds=intent["retention"]["ttl_seconds"])
    )
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "run_id": intent["run_id"],
        "request_id": intent["request_id"],
        "request_sha256": intent["request_sha256"],
        "execution_mode": intent["execution_mode"],
        "status": "crash_recovered",
        "run_lease_sha256": held_lease_sha,
        "input_binding": intent["input_binding"],
        "command_binding": intent["command_binding"],
        "approval": approval,
        "process": {
            "started_at": intent["started_at"],
            "completed_at": _timestamp(wall_clock()),
            "elapsed_ms": None,
            "exit_code": None,
            "timed_out": False,
            "term_sent": term_sent,
            "kill_sent": kill_sent,
            "process_spawn_attempted": ledger is not None,
            "child_pid": ledger["child_pid"] if ledger is not None else None,
            "process_group_id": ledger["process_group_id"] if ledger is not None else None,
            "process_ledger_sha256": process_ledger_sha,
            "kernel_birth_identity_sha256": (
                bytes_sha256(ledger["kernel_birth_identity"].encode()) if ledger is not None else None
            ),
            "process_identity_token_sha256": (
                bytes_sha256(ledger["process_identity_token"].encode()) if ledger is not None else None
            ),
            "process_group_cleanup_confirmed": True,
            "execution_error_code": "crash_recovered",
            "deadline_ms": intent["emergency"]["deadline_ms"],
            "term_grace_ms": intent["emergency"]["term_grace_ms"],
            "kill_grace_ms": intent["emergency"]["kill_grace_ms"],
            "fallback_used": False,
            "technical_limit_exceeded": False,
            "technical_limit_kind": None,
        },
        "artifacts": {
            "raw_stdout_sha256": bytes_sha256(raw),
            "stderr_sha256": bytes_sha256(stderr),
            "sanitized_output_sha256": sanitized_sha,
            "structured_output_compliant": False,
            "structured_output_contract_valid": False,
            "non_json_prefix_bytes": None,
            "non_json_suffix_bytes": None,
            "compiled_prompt_sha256": bytes_sha256(compiled_raw),
            "session_updates_sha256": bytes_sha256(session_raw) if session_raw is not None else None,
            "ephemeral_tree_deleted": not ephemeral_home.exists() and not ephemeral_home.is_symlink(),
            "session_tree_file_count": measurement.file_count,
            "session_tree_entry_count": measurement.entry_count,
            "session_tree_max_depth": measurement.max_depth,
            "session_tree_total_bytes": measurement.total_bytes,
            "session_tree_max_file_bytes": measurement.max_file_bytes,
        },
        "session_proof": session_payload,
        "retention": {
            "policy_id": intent["retention"]["policy_id"],
            "ttl_seconds": intent["retention"]["ttl_seconds"],
            "delete_after": delete_after,
            "purge_state": "pending_expiry",
            "deletion_receipt_required": True,
        },
        "reconciliation": _candidate_reconciliation(
            parsed_result if isinstance(parsed_result, dict) else {},
            prior_candidates,
            session_proof=session_proof,
            fixture=intent["execution_mode"] == "fixture",
        ),
        "authority": AUTHORITY,
    }
    if validate_operator_receipt(receipt):
        raise AdaptiveWaveValidationError("recovery_receipt_invalid")
    replay_errors = validate_operator_bundle(
        run_root,
        approval_root=approval_root,
        receipt_override=receipt,
    )
    if replay_errors:
        raise AdaptiveWaveValidationError("recovery_bundle_replay_invalid:" + ",".join(replay_errors))
    _atomic_publish(run_root / "operator-receipt.json", (canonical_json(receipt) + "\n").encode())
    return receipt


def _deletion_paths(deletion_root: Path, run_id: str) -> tuple[Path, Path]:
    return (
        deletion_root / f"journal-{run_id}.json",
        deletion_root / f"receipt-{run_id}.json",
    )


def _deletion_journal_valid(value: Any) -> bool:
    shape_valid = (
        isinstance(value, dict)
        and set(value) == _DELETION_JOURNAL_KEYS
        and value.get("schema_version") == DELETION_JOURNAL_SCHEMA_VERSION
        and isinstance(value.get("run_id"), str)
        and _RUN_ID_RE.fullmatch(value["run_id"]) is not None
        and all(
            _is_sha(value.get(key))
            for key in (
                "request_sha256",
                "run_lease_sha256",
                "operator_receipt_sha256",
                "runtime_root_sha256",
            )
        )
        and _timestamp_valid(value.get("delete_after"))
        and _timestamp_valid(value.get("created_at"))
        and value.get("state") == "delete_intent_durable"
    )
    if not shape_valid:
        return False
    return _parse_timestamp(value["created_at"]) >= _parse_timestamp(value["delete_after"])


def _deletion_receipt_valid(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == _DELETION_RECEIPT_KEYS
        and value.get("schema_version") == DELETION_RECEIPT_SCHEMA_VERSION
        and isinstance(value.get("run_id"), str)
        and _RUN_ID_RE.fullmatch(value["run_id"]) is not None
        and all(
            _is_sha(value.get(key)) for key in ("request_sha256", "operator_receipt_sha256", "deletion_journal_sha256")
        )
        and _timestamp_valid(value.get("deleted_at"))
        and value.get("state") == "deleted_after_durable_intent"
    )


def _publish_deletion_receipt(
    *,
    deletion_root: Path,
    journal: Mapping[str, Any],
    journal_raw: bytes,
    deleted_at: str,
) -> dict[str, Any]:
    receipt = {
        "schema_version": DELETION_RECEIPT_SCHEMA_VERSION,
        "run_id": journal["run_id"],
        "request_sha256": journal["request_sha256"],
        "operator_receipt_sha256": journal["operator_receipt_sha256"],
        "deletion_journal_sha256": bytes_sha256(journal_raw),
        "deleted_at": deleted_at,
        "state": "deleted_after_durable_intent",
    }
    if not _deletion_receipt_valid(receipt):
        raise AdaptiveWaveValidationError("generated_deletion_receipt_invalid")
    _, receipt_path = _deletion_paths(deletion_root, journal["run_id"])
    _atomic_publish(receipt_path, (canonical_json(receipt) + "\n").encode())
    return receipt


def purge_expired_adaptive_runs(
    *,
    runtime_root: Path = DEFAULT_RUNTIME_ROOT,
    deletion_root: Path = DEFAULT_DELETION_ROOT,
    approval_root: Path = DEFAULT_APPROVAL_ROOT,
    wall_clock: Callable[[], datetime] = _utc_now,
) -> list[dict[str, Any]]:
    """Delete expired private bundles after a durable, replayable intent journal."""

    if not runtime_root.exists():
        return []
    _ensure_private_directory(runtime_root, create=False)
    _ensure_private_directory(deletion_root, create=True)
    recover_pending_publications(deletion_root)
    deletion_name = re.compile(r"(?:journal|receipt)-(grok_wave_(?:fixture|live)_[0-9a-f]{32})\.json")
    for path in deletion_root.iterdir():
        metadata = path.lstat()
        if (
            deletion_name.fullmatch(path.name) is None
            or path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise AdaptiveWaveValidationError("deletion_inventory_invalid")
        if path.name.startswith("receipt-"):
            run_id = deletion_name.fullmatch(path.name).group(1)  # type: ignore[union-attr]
            journal_path, _ = _deletion_paths(deletion_root, run_id)
            if not journal_path.exists():
                raise AdaptiveWaveValidationError("deletion_receipt_without_journal")
    now = wall_clock().astimezone(UTC)
    deleted_at = _timestamp(now)
    runtime_root_sha = canonical_sha256(str(runtime_root.resolve()))
    receipts: list[dict[str, Any]] = []

    # Finish journaled deletions first.  A crash after rmtree but before the
    # receipt is recoverable because the external journal binds the terminal
    # operator receipt and the exact run lease.
    for journal_path in sorted(deletion_root.glob("journal-grok_wave_*.json")):
        journal_raw = _read_regular_owned_bounded(journal_path, maximum_bytes=1_048_576, required_mode=0o600)
        try:
            journal = strict_json_loads(journal_raw)
        except (UnicodeError, ValueError) as exc:
            raise AdaptiveWaveValidationError("deletion_journal_invalid") from exc
        if not _deletion_journal_valid(journal) or journal["runtime_root_sha256"] != runtime_root_sha:
            raise AdaptiveWaveValidationError("deletion_journal_invalid")
        if _parse_timestamp(journal["delete_after"]) > now:
            raise AdaptiveWaveValidationError("deletion_journal_not_expired")
        run_root = runtime_root / journal["run_id"]
        _, receipt_path = _deletion_paths(deletion_root, journal["run_id"])
        if receipt_path.exists():
            receipt = _read_private_json(receipt_path)
            if (
                not _deletion_receipt_valid(receipt)
                or receipt["run_id"] != journal["run_id"]
                or receipt["request_sha256"] != journal["request_sha256"]
                or receipt["operator_receipt_sha256"] != journal["operator_receipt_sha256"]
                or receipt["deletion_journal_sha256"] != bytes_sha256(journal_raw)
                or _parse_timestamp(receipt["deleted_at"]) < _parse_timestamp(journal["created_at"])
            ):
                raise AdaptiveWaveValidationError("deletion_receipt_invalid")
            if run_root.exists() or run_root.is_symlink():
                raise AdaptiveWaveValidationError("deleted_run_reappeared")
            continue
        if run_root.exists() or run_root.is_symlink():
            if run_root.is_symlink() or not run_root.is_dir():
                raise AdaptiveWaveValidationError("deletion_run_path_invalid")
            with _run_lease(run_root, create=False) as lease_sha:
                operator_raw = _read_regular_owned_bounded(
                    run_root / "operator-receipt.json", maximum_bytes=67_108_864, required_mode=0o600
                )
                operator = strict_json_loads(operator_raw)
                if (
                    lease_sha != journal["run_lease_sha256"]
                    or bytes_sha256(operator_raw) != journal["operator_receipt_sha256"]
                    or operator.get("request_sha256") != journal["request_sha256"]
                    or operator.get("retention", {}).get("delete_after") != journal["delete_after"]
                    or validate_operator_bundle(run_root, approval_root=approval_root)
                ):
                    raise AdaptiveWaveValidationError("journaled_bundle_replay_invalid")
                shutil.rmtree(run_root)
                _fsync_directory(runtime_root)
                if run_root.exists() or run_root.is_symlink():
                    raise AdaptiveWaveValidationError("run_bundle_deletion_failed")
        receipts.append(
            _publish_deletion_receipt(
                deletion_root=deletion_root,
                journal=journal,
                journal_raw=journal_raw,
                deleted_at=deleted_at,
            )
        )

    for run_root in sorted(runtime_root.iterdir()):
        if run_root.is_symlink() or not run_root.is_dir() or _RUN_ID_RE.fullmatch(run_root.name) is None:
            raise AdaptiveWaveValidationError("runtime_inventory_invalid")
        journal_path, receipt_path = _deletion_paths(deletion_root, run_root.name)
        if receipt_path.exists():
            raise AdaptiveWaveValidationError("deleted_run_reappeared")
        if journal_path.exists():
            continue
        with _run_lease(run_root, create=False) as lease_sha:
            bundle_errors = validate_operator_bundle(run_root, approval_root=approval_root)
            if bundle_errors:
                raise AdaptiveWaveValidationError("purge_bundle_replay_invalid:" + ",".join(bundle_errors))
            operator_raw = _read_regular_owned_bounded(
                run_root / "operator-receipt.json", maximum_bytes=67_108_864, required_mode=0o600
            )
            operator = strict_json_loads(operator_raw)
            delete_after = _parse_timestamp(operator["retention"]["delete_after"])
            if delete_after > now:
                continue
            journal = {
                "schema_version": DELETION_JOURNAL_SCHEMA_VERSION,
                "run_id": run_root.name,
                "request_sha256": operator["request_sha256"],
                "run_lease_sha256": lease_sha,
                "operator_receipt_sha256": bytes_sha256(operator_raw),
                "delete_after": operator["retention"]["delete_after"],
                "runtime_root_sha256": runtime_root_sha,
                "created_at": deleted_at,
                "state": "delete_intent_durable",
            }
            if not _deletion_journal_valid(journal):
                raise AdaptiveWaveValidationError("generated_deletion_journal_invalid")
            journal_raw = (canonical_json(journal) + "\n").encode()
            _atomic_publish(journal_path, journal_raw)
            shutil.rmtree(run_root)
            _fsync_directory(runtime_root)
            if run_root.exists() or run_root.is_symlink():
                raise AdaptiveWaveValidationError("run_bundle_deletion_failed")
        receipts.append(
            _publish_deletion_receipt(
                deletion_root=deletion_root,
                journal=journal,
                journal_raw=journal_raw,
                deleted_at=deleted_at,
            )
        )
    return receipts
