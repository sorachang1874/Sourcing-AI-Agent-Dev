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

import hashlib
import json
import os
import platform
import re
import selectors
import signal
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

REQUEST_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.request.v1"
RESULT_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.result.v1"
INTENT_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.intent.v1"
RECEIPT_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.operator_receipt.v1"
GRANT_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.live_grant.v1"
CONSUMPTION_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.live_grant_consumption.v1"
PROCESS_LEDGER_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.process_ledger.v1"

PROVIDER_ID = "grok_cli_oauth"
DEFAULT_MODEL_ID = "grok-4.5"
DEFAULT_RUNTIME_ROOT = Path(__file__).resolve().parents[2] / "runtime/adaptive-grok-waves"
DEFAULT_APPROVAL_ROOT = Path.home() / ".local/state/x-first-researcher-sourcing/adaptive-grok-wave-approvals/v1"
DEFAULT_GROK_BINARY = Path.home() / ".grok/bin/grok"
DEFAULT_GROK_AUTH = Path.home() / ".grok/auth.json"

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
    "approval",
    "authority",
}
_TARGET_KEYS = {"lab_id", "research_focus_id", "scope"}
_PROMPT_SOURCE_KEYS = {"path", "sha256"}
_PRIOR_WAVE_KEYS = {"wave_id", "path", "sha256"}
_TRANSPORT_KEYS = {"provider_id", "model_id", "reasoning_effort", "grok_binary_sha256"}
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
}
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
    "supports",
}
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
    "input_binding",
    "command_binding",
    "approval",
    "emergency",
    "technical_limits",
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
    "input_binding",
    "command_binding",
    "approval",
    "process",
    "artifacts",
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
    "cli_flags_sha256",
    "environment_policy_sha256",
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
    "ephemeral_auth_deleted",
    "ephemeral_tree_private",
}
_RECONCILIATION_RECEIPT_KEYS = {
    "candidate_count",
    "evidence_count",
    "post_url_count",
    "prior_overlap_count",
    "verified_material_update_count",
    "model_reported_tool_calls",
    "tool_facts_model_mediated",
}
_RUNTIME_LAYOUT_KEYS = {
    "compiled_prompt_name",
    "stdout_spool_name",
    "stderr_spool_name",
    "ephemeral_home_name",
}
_PROCESS_LEDGER_KEYS = {
    "schema_version",
    "run_id",
    "request_id",
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
    "emergency_sha256",
    "technical_limits_sha256",
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
    "consumed_at",
    "state",
}
_RESULT_STATUSES = {"X_SEARCH_OK", "X_SEARCH_PARTIAL", "X_SEARCH_BLOCKED"}
_DIMENSION_STATES = {"current", "historical", "ambiguous", "unsupported"}
_CONFIDENCE_STATES = {"high", "medium", "low"}
_EVIDENCE_KINDS = {"bio", "post", "mention", "thread"}
_RELATIONSHIPS = {"self", "official_lab", "colleague_or_team", "third_party", "historical"}
_SUPPORTS = {"target_lab_affiliation_state", "pretraining_experience_state"}
_TOOL_NAMES = {"x_keyword_search", "x_semantic_search", "x_user_search", "x_thread_fetch"}
_RECEIPT_STATUSES = {
    "fixture_complete",
    "completed",
    "process_failed",
    "timed_out",
    "structured_output_noncompliant",
    "result_contract_invalid",
    "crash_recovered",
    "technical_limit_exceeded",
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
    return isinstance(value, str) and re.fullmatch(
        r"https://x\.com/[A-Za-z0-9_]{1,15}/status/[1-9][0-9]{5,31}", value
    ) is not None


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
            if (
                isinstance(supports, list)
                and dimension in supports
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
    return {
        "candidate_count": len(candidates),
        "evidence_count": evidence_count,
        "post_url_count": post_url_count,
        "prior_overlap_count": prior_overlap_count,
        "verified_material_update_count": verified_material_updates,
        "model_reported_tool_calls": reported_calls if _validate_nonnegative_int(reported_calls) else 0,
        "tool_facts_model_mediated": True,
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
        )
    parsed_url = _parse_post_url(evidence.get("url"))
    published = _parse_evidence_timestamp(evidence.get("published_at"))
    if (
        kind not in {"post", "mention", "thread"}
        or parsed_url is None
        or parsed_url[0].casefold() != author.casefold()
        or parsed_url[1] != evidence.get("post_id")
        or published is None
    ):
        return False
    return evidence.get("relationship") != "self" or author.casefold() == handle.casefold()


def validate_model_result(
    result: Any,
    *,
    prior_candidates: Mapping[str, PriorCandidateFacts] | None = None,
    live_mode: bool = False,
) -> list[str]:
    """Validate unbounded business arrays and mechanically reconcile their claims."""

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
    if not isinstance(counts, dict) or set(counts) != _COUNT_KEYS or any(
        not _validate_nonnegative_int(counts.get(key)) for key in _COUNT_KEYS
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
        supports_by_dimension: dict[str, int] = {dimension: 0 for dimension in _SUPPORTS}
        for evidence_index, evidence in enumerate(evidence_rows):
            if not isinstance(evidence, dict) or set(evidence) != _EVIDENCE_KEYS:
                errors.append(f"evidence_shape_invalid:{index}:{evidence_index}")
                continue
            supports = evidence.get("supports")
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
                or not isinstance(supports, list)
                or not supports
                or any(not isinstance(item, str) for item in supports)
                or len(set(supports)) != len(supports)
                or any(item not in _SUPPORTS for item in supports)
                or not _evidence_binding_valid(evidence, candidate, live_mode=live_mode)
            ):
                errors.append(f"evidence_value_invalid:{index}:{evidence_index}")
            elif isinstance(supports, list):
                for dimension in supports:
                    supports_by_dimension[dimension] += 1
        for dimension in _SUPPORTS:
            state = candidate.get(dimension)
            if state in {"current", "historical"} and supports_by_dimension[dimension] == 0:
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
            reconciliation.get("candidate_records_validated") != len(candidates)
            or reconciliation.get("evidence_items_validated") != evidence_count
            or reconciliation.get("post_urls_structurally_validated") != post_url_count
            or reconciliation.get("provider_post_bodies_replayable") is not False
            or not _validate_nonnegative_int(reconciliation.get("tool_calls_completed"))
            or not isinstance(tool_counts, dict)
            or any(key not in _TOOL_NAMES or not _validate_nonnegative_int(value) for key, value in tool_counts.items())
            or (
                isinstance(tool_counts, dict)
                and sum(tool_counts.values()) != reconciliation.get("tool_calls_completed")
            )
            or reconciliation.get("tool_calls_completed") != reported_calls
            or (isinstance(tool_counts, dict) and set(tool_counts) != set(tools) if isinstance(tools, list) else True)
            or (isinstance(queries, list) and _is_int(reported_calls) and len(queries) != reported_calls)
        ):
            errors.append("local_reconciliation_value_invalid")
    if isinstance(counts, dict):
        if counts.get("candidates_retained") != len(candidates):
            errors.append("candidate_count_mismatch")
        if (
            _is_int(counts.get("observations_inspected_reported"))
            and counts["observations_inspected_reported"] < evidence_count
        ):
            errors.append("observation_count_below_evidence_count")
    if result.get("status") in {"X_SEARCH_OK", "X_SEARCH_PARTIAL"} and (
        not _is_int(reported_calls) or reported_calls <= 0
    ):
        errors.append("native_x_call_required_for_nonblocked_status")
    if result.get("status") == "X_SEARCH_OK" and not candidates:
        errors.append("ok_status_requires_candidate")
    return errors


def _live_profile_urls_valid(result: Any) -> bool:
    candidates = result.get("candidates") if isinstance(result, dict) else None
    return isinstance(candidates, list) and all(
        isinstance(candidate, dict)
        and candidate.get("profile_url") == f"https://x.com/{candidate.get('handle')}"
        for candidate in candidates
    )


def _load_bound_bytes(
    path_value: str,
    expected_sha256: str,
    *,
    require_private: bool = False,
    max_bytes: int | None = None,
) -> bytes:
    path = Path(path_value).expanduser()
    try:
        if not path.is_file() or path.is_symlink():
            raise AdaptiveWaveValidationError("bound_input_not_regular")
        if require_private and not _private_file_valid(path):
            raise AdaptiveWaveValidationError("bound_input_not_private")
        if max_bytes is not None and path.stat(follow_symlinks=False).st_size > max_bytes:
            raise AdaptiveWaveValidationError("bound_input_byte_ceiling_exceeded")
        value = path.read_bytes()
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
                        supports = item.get("supports")
                        if not isinstance(supports, list):
                            continue
                        for dimension in _SUPPORTS.intersection(supports):
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
        "Return exactly one JSON object matching the supplied schema. Do not emit Markdown, commentary, a prefix, "
        "or a suffix. Model-organized evidence remains a discovery lead, not replayable source truth.\n"
        f"Authoritative result JSON Schema: {schema_json}"
    )


def result_schema_sha256() -> str:
    path = Path(__file__).resolve().parents[2] / "contracts/x.grok.adaptive_recall_wave.result.v1.schema.json"
    return bytes_sha256(path.read_bytes())


def _redacted_command_policy(
    *,
    request: Mapping[str, Any],
    compiled_prompt_sha256: str,
    session_id: str,
) -> list[str]:
    emergency = request["emergency"]
    transport = request["transport"]
    return [
        "<grok-binary:sha256-bound>",
        "--prompt-file",
        f"<prompt:{compiled_prompt_sha256}>",
        "--verbatim",
        "--cwd",
        "<isolated-empty-directory>",
        "--model",
        transport["model_id"],
        "--reasoning-effort",
        transport["reasoning_effort"],
        "--output-format",
        "plain",
        "--tools",
        ",".join(NATIVE_X_TOOLS),
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
        "<isolated-leader-socket>",
        "--permission-mode",
        "dontAsk",
        "--sandbox",
        "read-only",
    ]


def _redacted_policy_from_bindings(
    input_binding: Mapping[str, Any], command_binding: Mapping[str, Any]
) -> list[str]:
    return [
        "<grok-binary:sha256-bound>",
        "--prompt-file",
        f"<prompt:{input_binding['compiled_prompt_sha256']}>",
        "--verbatim",
        "--cwd",
        "<isolated-empty-directory>",
        "--model",
        command_binding["model_id"],
        "--reasoning-effort",
        command_binding["reasoning_effort"],
        "--output-format",
        "plain",
        "--tools",
        ",".join(NATIVE_X_TOOLS),
        "--disable-web-search",
        "--disallowed-tools",
        ",".join(DISALLOWED_TOOLS),
        "--max-turns",
        str(command_binding["max_turns"]),
        "--session-id",
        command_binding["session_id"],
        "--no-subagents",
        "--no-plan",
        "--no-memory",
        "--no-auto-update",
        "--leader-socket",
        "<isolated-leader-socket>",
        "--permission-mode",
        "dontAsk",
        "--sandbox",
        "read-only",
    ]


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
        "--tools",
        ",".join(NATIVE_X_TOOLS),
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
    if not stat.S_ISDIR(info.st_mode) or path.is_symlink() or stat.S_IMODE(info.st_mode) != 0o700:
        raise AdaptiveWaveValidationError("private_directory_mode_invalid")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_publish(path: Path, value: bytes) -> None:
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
        if not stat.S_ISREG(info.st_mode) or path.is_symlink() or stat.S_IMODE(info.st_mode) != 0o600:
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
        info = path.lstat()
    except OSError as exc:
        raise AdaptiveWaveValidationError("staged_grok_binary_unavailable") from exc
    if (
        not stat.S_ISREG(info.st_mode)
        or path.is_symlink()
        or info.st_uid != os.getuid()
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != 0o700
        or info.st_size <= 0
    ):
        raise AdaptiveWaveValidationError("staged_grok_binary_invalid")
    return bytes_sha256(path.read_bytes())


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
    staged_info = staged.stat(follow_symlinks=False)
    if (
        not stat.S_ISREG(staged_info.st_mode)
        or staged_info.st_uid != os.getuid()
        or staged_info.st_nlink != 1
        or stat.S_IMODE(staged_info.st_mode) != 0o700
        or bytes_sha256(staged.read_bytes()) != expected_sha256
    ):
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


def _delete_ephemeral_auth(ephemeral_home: Path) -> None:
    for name in ("auth.json", "auth.json.lock"):
        path = ephemeral_home / name
        try:
            if path.is_symlink():
                raise AdaptiveWaveValidationError("ephemeral_auth_symlink_invalid")
            path.unlink()
        except FileNotFoundError:
            pass
    _fsync_directory(ephemeral_home)


def _harden_ephemeral_tree(root: Path) -> None:
    """Retain only private regular session/config evidence after auth removal."""

    for current_root, directory_names, file_names in os.walk(root, topdown=True, followlinks=False):
        current = Path(current_root)
        if current.is_symlink():
            raise AdaptiveWaveValidationError("ephemeral_tree_symlink_invalid")
        os.chmod(current, 0o700)
        for name in list(directory_names):
            path = current / name
            if path.is_symlink():
                raise AdaptiveWaveValidationError("ephemeral_tree_symlink_invalid")
            os.chmod(path, 0o700)
        for name in file_names:
            path = current / name
            info = path.lstat()
            if stat.S_ISSOCK(info.st_mode):
                path.unlink()
                continue
            if not stat.S_ISREG(info.st_mode) or path.is_symlink():
                raise AdaptiveWaveValidationError("ephemeral_tree_entry_invalid")
            os.chmod(path, 0o600)


def _ephemeral_tree_private(root: Path) -> bool:
    if not root.is_dir() or root.is_symlink() or stat.S_IMODE(root.stat().st_mode) != 0o700:
        return False
    for current_root, directory_names, file_names in os.walk(root, topdown=True, followlinks=False):
        current = Path(current_root)
        if current.is_symlink() or stat.S_IMODE(current.stat().st_mode) != 0o700:
            return False
        for name in directory_names:
            path = current / name
            if path.is_symlink() or stat.S_IMODE(path.stat().st_mode) != 0o700:
                return False
        for name in file_names:
            path = current / name
            if not _private_file_valid(path):
                return False
    return not (root / "auth.json").exists() and not (root / "auth.json.lock").exists()


def _load_result_schema() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[2] / "contracts/x.grok.adaptive_recall_wave.result.v1.schema.json"
    try:
        schema = strict_json_loads(path.read_bytes())
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


def _parse_structured_stdout(
    raw: bytes,
    *,
    technical_limits: Mapping[str, Any],
    prior_candidates: Mapping[str, PriorCandidateFacts],
    live_mode: bool,
) -> tuple[Any | None, bytes | None, int, int, bool, bool, str | None]:
    if len(raw) > technical_limits["max_json_bytes"]:
        return None, None, 0, 0, False, False, "json_bytes"
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None, None, len(raw), 0, False, False, None
    decoder = json.JSONDecoder(object_pairs_hook=_strict_object, parse_constant=_reject_nonfinite)
    leading_whitespace = len(text) - len(text.lstrip())
    first_object = text.find("{")
    if first_object < 0:
        return None, None, len(raw), 0, False, False, None
    try:
        payload, end = decoder.raw_decode(text, first_object)
    except (ValueError, RecursionError):
        return None, None, len(text[:first_object].encode()), 0, False, False, None
    prefix = text[:first_object]
    suffix = text[end:]
    prefix_bytes = len(prefix.encode()) if prefix.strip() else 0
    suffix_bytes = len(suffix.encode()) if suffix.strip() else 0
    syntax_compliant = first_object == leading_whitespace and prefix_bytes == 0 and suffix_bytes == 0
    if not isinstance(payload, dict):
        return payload, None, prefix_bytes, suffix_bytes, syntax_compliant, False, None
    if not _structure_within_limits(
        payload,
        max_depth=technical_limits["max_json_depth"],
        max_nodes=technical_limits["max_json_nodes"],
    ):
        return payload, None, prefix_bytes, suffix_bytes, syntax_compliant, False, "json_structure"
    sanitized = (canonical_json(payload) + "\n").encode()
    contract_valid = not validate_model_result(
        payload,
        prior_candidates=prior_candidates,
        live_mode=live_mode,
    )
    return payload, sanitized, prefix_bytes, suffix_bytes, syntax_compliant, contract_valid, None


def _private_file_valid(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISREG(info.st_mode)
        and not path.is_symlink()
        and info.st_uid == os.getuid()
        and info.st_nlink == 1
        and stat.S_IMODE(info.st_mode) == 0o600
    )


def _read_private_json(path: Path) -> Any:
    if not _private_file_valid(path):
        raise AdaptiveWaveValidationError("private_json_invalid")
    try:
        return strict_json_loads(path.read_bytes())
    except (UnicodeError, ValueError) as exc:
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


def _validate_grant(grant: Any, request: Mapping[str, Any], *, now: datetime) -> list[str]:
    if not isinstance(grant, dict) or set(grant) != _GRANT_KEYS:
        return ["grant_shape_invalid"]
    grant_id = request["approval"]["grant_id"]
    expected_id_hash = bytes_sha256(grant_id.encode()) if isinstance(grant_id, str) else None
    errors: list[str] = []
    if (
        grant.get("schema_version") != GRANT_SCHEMA_VERSION
        or grant.get("grant_id_hash") != expected_id_hash
        or grant.get("execution_scope_sha256") != execution_scope_sha256(request)
        or grant.get("request_id") != request["request_id"]
        or grant.get("target_sha256") != canonical_sha256(request["target"])
        or grant.get("model_id") != request["transport"]["model_id"]
        or grant.get("emergency_sha256") != canonical_sha256(request["emergency"])
        or grant.get("technical_limits_sha256") != canonical_sha256(request["technical_limits"])
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
    ttl_seconds: int = 900,
    wall_clock: Callable[[], datetime] = _utc_now,
) -> tuple[dict[str, Any], Path]:
    """Preissue a request-bound owner-only grant without executing Grok."""

    if not _is_int(ttl_seconds) or not 60 <= ttl_seconds <= 3_600:
        raise AdaptiveWaveValidationError("grant_ttl_invalid")
    request = _load_request(request_path)
    grant_id = request["approval"]["grant_id"]
    if not isinstance(grant_id, str) or not _is_sha(request["transport"]["grok_binary_sha256"]):
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
        "emergency_sha256": canonical_sha256(request["emergency"]),
        "technical_limits_sha256": canonical_sha256(request["technical_limits"]),
        "issued_at": _timestamp(now),
        "expires_at": _timestamp(now + timedelta(seconds=ttl_seconds)),
        "issuer": "local_owner_explicit_cli",
        "state": "preissued_single_use",
    }
    _ensure_private_directory(grant_root, create=True)
    grant_path, _ = _grant_paths(grant_root, grant_id_hash)
    _atomic_publish(grant_path, (canonical_json(grant) + "\n").encode())
    return grant, grant_path


def _load_and_consume_grant(
    grant_root: Path,
    *,
    request: Mapping[str, Any],
    run_id: str,
    request_sha256: str,
    consumed_at: str,
) -> tuple[str, str]:
    grant_id = request["approval"]["grant_id"]
    if not isinstance(grant_id, str):
        raise PermissionError("preissued_live_grant_required")
    _ensure_private_directory(grant_root, create=True)
    grant_id_hash = bytes_sha256(grant_id.encode())
    grant_path, consumption_path = _grant_paths(grant_root, grant_id_hash)
    try:
        grant = _read_private_json(grant_path)
    except AdaptiveWaveValidationError as exc:
        raise PermissionError("preissued_live_grant_invalid") from exc
    if _validate_grant(grant, request, now=_parse_timestamp(consumed_at)):
        raise PermissionError("preissued_live_grant_invalid")
    grant_raw = grant_path.read_bytes()
    grant_sha = bytes_sha256(grant_raw)
    payload = {
        "schema_version": CONSUMPTION_SCHEMA_VERSION,
        "grant_id_hash": grant_id_hash,
        "grant_sha256": grant_sha,
        "execution_scope_sha256": execution_scope_sha256(request),
        "request_sha256": request_sha256,
        "run_id": run_id,
        "consumed_at": consumed_at,
        "state": "consumed_after_binary_auth_preflight_before_process_spawn",
    }
    raw = (canonical_json(payload) + "\n").encode()
    try:
        _atomic_publish(consumption_path, raw)
    except FileExistsError as exc:
        raise PermissionError("live_grant_already_consumed") from exc
    return grant_sha, bytes_sha256(raw)


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
) -> tuple[dict[str, Any], dict[str, Any]]:
    input_binding = {
        "target_sha256": canonical_sha256(request["target"]),
        "source_prompt_sha256": bytes_sha256(prompt_raw),
        "compiled_prompt_sha256": bytes_sha256(compiled_prompt.encode()),
        "prior_waves": prior_bindings,
        "prior_unique_handle_count": prior_handle_count,
        "prior_handle_set_sha256": canonical_sha256(sorted(item.casefold() for item in prior_handles)),
    }
    policy = _redacted_command_policy(
        request=request,
        compiled_prompt_sha256=input_binding["compiled_prompt_sha256"],
        session_id=session_id,
    )
    command_binding = {
        "provider_id": request["transport"]["provider_id"],
        "model_id": request["transport"]["model_id"],
        "reasoning_effort": request["transport"]["reasoning_effort"],
        "session_id": session_id,
        "grok_binary_sha256": binary_sha256,
        "structured_output_schema_sha256": schema_sha256,
        "cli_flags_sha256": canonical_sha256(policy),
        "environment_policy_sha256": canonical_sha256(_redacted_environment_policy()),
        "max_turns": request["emergency"]["max_turns"],
    }
    return input_binding, command_binding


def _create_run_root(runtime_root: Path, run_id: str) -> Path:
    _ensure_private_directory(runtime_root, create=True)
    run_root = runtime_root / run_id
    run_root.mkdir(mode=0o700, exist_ok=False)
    _fsync_directory(runtime_root)
    _ensure_private_directory(run_root, create=False)
    return run_root


def _load_request(path: Path) -> dict[str, Any]:
    try:
        if not _private_file_valid(path):
            raise AdaptiveWaveValidationError("request_json_invalid")
        request = strict_json_loads(path.read_bytes())
    except (OSError, UnicodeError, ValueError) as exc:
        raise AdaptiveWaveValidationError("request_json_invalid") from exc
    errors = validate_request(request)
    if errors:
        raise AdaptiveWaveValidationError("request_contract_invalid")
    return request


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
    request_sha = canonical_sha256(request)
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
    compiled_prompt = compile_prompt(
        base_prompt,
        request["target"],
        prior_handles,
        result_schema=result_schema,
    )
    if len(compiled_prompt.encode()) > request["technical_limits"]["max_compiled_prompt_bytes"]:
        raise AdaptiveWaveValidationError("compiled_prompt_byte_ceiling_exceeded")
    schema_sha = result_schema_sha256()
    actual_run_id = run_id or f"grok_wave_{execution_mode}_{uuid.uuid4().hex}"
    actual_session_id = session_id or str(uuid.uuid4())
    if _RUN_ID_RE.fullmatch(actual_run_id) is None or _SESSION_ID_RE.fullmatch(actual_session_id) is None:
        raise AdaptiveWaveValidationError("operator_identity_invalid")
    run_root = _create_run_root(runtime_root, actual_run_id)
    workspace = run_root / "workspace"
    workspace.mkdir(mode=0o700)
    ephemeral_home = run_root / "ephemeral-home"
    ephemeral_home.mkdir(mode=0o700)
    compiled_prompt_path = run_root / "compiled-prompt.txt"
    stdout_spool = run_root / ".stdout-spool"
    stderr_spool = run_root / ".stderr-spool"
    runtime_layout = {
        "compiled_prompt_name": compiled_prompt_path.name,
        "stdout_spool_name": stdout_spool.name,
        "stderr_spool_name": stderr_spool.name,
        "ephemeral_home_name": ephemeral_home.name,
    }
    _atomic_publish(compiled_prompt_path, compiled_prompt.encode())
    isolated_environment = _isolated_environment(ephemeral_home)
    binary_sha: str | None = None
    command_binary = Path("fixture-grok.invalid")
    if execution_mode == "live":
        expected_binary_sha = request["transport"]["grok_binary_sha256"]
        if not _is_sha(expected_binary_sha):
            raise AdaptiveWaveValidationError("live_binary_sha256_required")
        staged_binary = _stage_verified_binary(binary, run_root, expected_binary_sha)
        binary_sha = staged_binary.sha256
        command_binary = staged_binary.path
        if auth_source is None:
            raise AdaptiveWaveValidationError("live_auth_source_required")
    input_binding, command_binding = _build_static_bindings(
        request=request,
        prompt_raw=prompt_raw,
        compiled_prompt=compiled_prompt,
        prior_bindings=prior_bindings,
        prior_handles=prior_handles,
        prior_handle_count=len(prior_handles),
        session_id=actual_session_id,
        schema_sha256=schema_sha,
        binary_sha256=binary_sha,
    )
    started_at = _timestamp(wall_clock())
    grant_sha: str | None = None
    approval_consumption_sha: str | None = None
    auth_prepared = False
    try:
        if execution_mode == "live":
            assert auth_source is not None  # validated above
            _copy_private_auth(auth_source, ephemeral_home)
            auth_prepared = True
            grant_sha, approval_consumption_sha = _load_and_consume_grant(
                approval_root,
                request=request,
                run_id=actual_run_id,
                request_sha256=request_sha,
                consumed_at=started_at,
            )
        approval_binding = _approval_binding(
            request,
            required=execution_mode == "live",
            grant_sha256=grant_sha,
            consumption_sha256=approval_consumption_sha,
        )
        intent = {
            "schema_version": INTENT_SCHEMA_VERSION,
            "run_id": actual_run_id,
            "request_id": request["request_id"],
            "request_sha256": request_sha,
            "execution_mode": execution_mode,
            "started_at": started_at,
            "input_binding": input_binding,
            "command_binding": command_binding,
            "approval": approval_binding,
            "emergency": request["emergency"],
            "technical_limits": request["technical_limits"],
            "runtime_layout": runtime_layout,
            "authority": AUTHORITY,
        }
        _atomic_publish(run_root / "operator-request.json", (canonical_json(request) + "\n").encode())
        _atomic_publish(run_root / "operator-intent.json", (canonical_json(intent) + "\n").encode())
        command = build_grok_command(
            binary=command_binary,
            cwd=workspace,
            request=request,
            prompt_file=compiled_prompt_path,
            leader_socket=ephemeral_home / "leader.sock",
            session_id=actual_session_id,
            result_schema=result_schema,
        )
    except BaseException:
        if auth_prepared:
            _delete_ephemeral_auth(ephemeral_home)
        _harden_ephemeral_tree(ephemeral_home)
        raise
    process_ledger_sha: str | None = None

    def persist_spawn(
        child_pid: int,
        process_group_id: int,
        kernel_birth_identity: str,
        process_identity_token: str,
    ) -> None:
        nonlocal process_ledger_sha
        ledger = {
            "schema_version": PROCESS_LEDGER_SCHEMA_VERSION,
            "run_id": actual_run_id,
            "request_id": request["request_id"],
            "session_id": actual_session_id,
            "child_pid": child_pid,
            "process_group_id": process_group_id,
            "kernel_birth_identity": kernel_birth_identity,
            "process_identity_token": process_identity_token,
            "spawned_at": _timestamp(wall_clock()),
        }
        raw_ledger = (canonical_json(ledger) + "\n").encode()
        _atomic_publish(run_root / "process-ledger.json", raw_ledger)
        process_ledger_sha = bytes_sha256(raw_ledger)

    started_monotonic = monotonic()
    try:
        process_result = executor(
            command,
            cwd=workspace,
            environment=isolated_environment,
            stdout_spool=stdout_spool,
            stderr_spool=stderr_spool,
            deadline_at=started_monotonic + request["emergency"]["deadline_ms"] / 1000,
            term_grace_ms=request["emergency"]["term_grace_ms"],
            kill_grace_ms=request["emergency"]["kill_grace_ms"],
            max_stdout_bytes=request["technical_limits"]["max_stdout_bytes"],
            max_stderr_bytes=request["technical_limits"]["max_stderr_bytes"],
            monotonic=monotonic,
            on_spawn=persist_spawn,
        )
    finally:
        if auth_prepared:
            _delete_ephemeral_auth(ephemeral_home)
        _harden_ephemeral_tree(ephemeral_home)
    if process_result.execution_error_code == "process_group_cleanup_failed":
        raise AdaptiveWaveValidationError("process_group_cleanup_incomplete")
    elapsed_ms = max(0, round((monotonic() - started_monotonic) * 1000))
    completed_at = _timestamp(wall_clock())
    if not _private_file_valid(stdout_spool) or not _private_file_valid(stderr_spool):
        raise AdaptiveWaveValidationError("process_spool_invalid")
    stdout_size = stdout_spool.stat().st_size
    stderr_size = stderr_spool.stat().st_size
    technical_limit_kind = process_result.technical_limit_kind
    if stdout_size > request["technical_limits"]["max_stdout_bytes"]:
        technical_limit_kind = "stdout_bytes"
    if stderr_size > request["technical_limits"]["max_stderr_bytes"]:
        technical_limit_kind = "stderr_bytes"
    stdout_raw = stdout_spool.read_bytes()[: request["technical_limits"]["max_stdout_bytes"]]
    stderr_raw = stderr_spool.read_bytes()[: request["technical_limits"]["max_stderr_bytes"]]
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
    ) = _parse_structured_stdout(
        stdout_raw,
        technical_limits=request["technical_limits"],
        prior_candidates=prior_candidates,
        live_mode=execution_mode == "live",
    )
    technical_limit_kind = technical_limit_kind or json_limit_kind
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
    else:
        status = "fixture_complete" if execution_mode == "fixture" else "completed"
    mechanical_reconciliation = _candidate_reconciliation(
        parsed_result if isinstance(parsed_result, dict) else {},
        prior_candidates,
    )
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "run_id": actual_run_id,
        "request_id": request["request_id"],
        "request_sha256": request_sha,
        "execution_mode": execution_mode,
        "status": status,
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
            "compiled_prompt_sha256": bytes_sha256(compiled_prompt_path.read_bytes()),
            "ephemeral_auth_deleted": not (ephemeral_home / "auth.json").exists(),
            "ephemeral_tree_private": _ephemeral_tree_private(ephemeral_home),
        },
        "reconciliation": mechanical_reconciliation,
        "authority": AUTHORITY,
    }
    receipt_errors = validate_operator_receipt(receipt)
    if receipt_errors:
        raise AdaptiveWaveValidationError("generated_receipt_invalid")
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
        and _input_binding_valid(intent.get("input_binding"))
        and _command_binding_valid(intent.get("command_binding"))
        and _approval_binding_valid(intent.get("approval"), intent.get("execution_mode"))
        and _emergency_valid(intent.get("emergency"))
        and _technical_limits_valid(intent.get("technical_limits"))
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
            and set(row)
            == {"wave_id", "sha256", "candidate_count", "unique_handle_count", "handle_set_sha256"}
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
        and _is_sha(value.get("cli_flags_sha256"))
        and _is_sha(value.get("environment_policy_sha256"))
        and _is_int(value.get("max_turns"))
        and 1 <= value["max_turns"] <= 512
    )


def _approval_binding_valid(value: Any, execution_mode: Any) -> bool:
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
                and _is_sha(value.get("consumption_sha256"))
            )
        )
        and (required or (value.get("grant_sha256") is None and value.get("consumption_sha256") is None))
    )


def _runtime_layout_valid(value: Any) -> bool:
    return isinstance(value, dict) and set(value) == _RUNTIME_LAYOUT_KEYS and value == {
        "compiled_prompt_name": "compiled-prompt.txt",
        "stdout_spool_name": ".stdout-spool",
        "stderr_spool_name": ".stderr-spool",
        "ephemeral_home_name": "ephemeral-home",
    }


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
            )
        )
        and value["prior_overlap_count"] <= value["candidate_count"]
        and value["verified_material_update_count"] <= value["prior_overlap_count"]
        and value.get("tool_facts_model_mediated") is True
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
    mode = receipt.get("execution_mode")
    status = receipt.get("status")
    if mode not in {"fixture", "live"} or status not in _RECEIPT_STATUSES:
        errors.append("receipt_state_invalid")
    if isinstance(receipt.get("run_id"), str) and mode in {"fixture", "live"} and not receipt["run_id"].startswith(
        f"grok_wave_{mode}_"
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
    if not _approval_binding_valid(receipt.get("approval"), mode):
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
        ledger_hash_valid = process.get("process_ledger_sha256") is None or _is_sha(
            process["process_ledger_sha256"]
        )
        identity_hashes = (
            process.get("kernel_birth_identity_sha256"),
            process.get("process_identity_token_sha256"),
        )
        identity_hashes_valid = all(value is None or _is_sha(value) for value in identity_hashes)
        spawn_binding_complete = all(
            value is not None
            for value in (*child_values, process.get("process_ledger_sha256"), *identity_hashes)
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
            or technical_kind not in {None, "stdout_bytes", "stderr_bytes", "json_bytes", "json_structure"}
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
        or artifacts.get("ephemeral_auth_deleted") is not True
        or artifacts.get("ephemeral_tree_private") is not True
    ):
        errors.append("receipt_artifacts_value_invalid")

    if not _receipt_reconciliation_valid(receipt.get("reconciliation")):
        errors.append("receipt_reconciliation_invalid")
    if receipt.get("authority") != AUTHORITY:
        errors.append("receipt_authority_invalid")

    if _input_binding_valid(input_binding) and _command_binding_valid(command_binding):
        if command_binding["structured_output_schema_sha256"] != result_schema_sha256():
            errors.append("receipt_result_schema_hash_invalid")
        if command_binding["cli_flags_sha256"] != canonical_sha256(
            _redacted_policy_from_bindings(input_binding, command_binding)
        ):
            errors.append("receipt_cli_flags_hash_invalid")
        if command_binding["environment_policy_sha256"] != canonical_sha256(_redacted_environment_policy()):
            errors.append("receipt_environment_policy_hash_invalid")
        if mode == "live" and not _is_sha(command_binding.get("grok_binary_sha256")):
            errors.append("receipt_live_binary_hash_missing")
        if mode == "fixture" and command_binding.get("grok_binary_sha256") is not None:
            errors.append("receipt_fixture_binary_hash_invalid")
        if artifacts_shape_valid and artifacts.get("compiled_prompt_sha256") != input_binding["compiled_prompt_sha256"]:
            errors.append("receipt_compiled_prompt_hash_mismatch")

    if process_shape_valid and artifacts_shape_valid:
        spawned = process.get("process_spawn_attempted") is True
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
        ):
            errors.append("receipt_completion_claim_invalid")
        if status == "completed" and not spawned:
            errors.append("receipt_live_completion_spawn_invalid")
        if status == "timed_out" and process.get("timed_out") is not True:
            errors.append("receipt_timeout_claim_invalid")
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
) -> list[str]:
    """Replay every durable binding without trusting the terminal receipt."""

    errors: list[str] = []
    try:
        _ensure_private_directory(run_root, create=False)
        request = _read_private_json(run_root / "operator-request.json")
        intent = _read_private_json(run_root / "operator-intent.json")
        receipt = _read_private_json(run_root / "operator-receipt.json")
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
    if request.get("request_id") != intent.get("request_id") or request.get("request_id") != receipt.get(
        "request_id"
    ):
        errors.append("request_id_binding_mismatch")
    if intent.get("emergency") != request.get("emergency"):
        errors.append("intent_emergency_binding_mismatch")
    if intent.get("technical_limits") != request.get("technical_limits"):
        errors.append("intent_technical_limits_binding_mismatch")
    if isinstance(intent, dict) and isinstance(receipt, dict):
        for key in (
            "run_id",
            "request_id",
            "request_sha256",
            "execution_mode",
            "input_binding",
            "command_binding",
            "approval",
            "authority",
        ):
            if intent.get(key) != receipt.get(key):
                errors.append(f"intent_receipt_binding_mismatch:{key}")
    prior_candidates: dict[str, PriorCandidateFacts] = {}
    compiled_prompt_path = run_root / "compiled-prompt.txt"
    if not _private_file_valid(compiled_prompt_path):
        errors.append("compiled_prompt_permissions_invalid")
    else:
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
            if compiled_prompt_path.read_bytes() != compiled_expected:
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

    ephemeral_home = run_root / "ephemeral-home"
    if not _ephemeral_tree_private(ephemeral_home):
        errors.append("ephemeral_tree_not_private_or_auth_present")

    mode = receipt.get("execution_mode")
    command_binding = receipt.get("command_binding", {})
    input_binding = receipt.get("input_binding", {})
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
        expected_command_binding = {
            "provider_id": request["transport"]["provider_id"],
            "model_id": request["transport"]["model_id"],
            "reasoning_effort": request["transport"]["reasoning_effort"],
            "session_id": session_id,
            "grok_binary_sha256": expected_binary_sha,
            "structured_output_schema_sha256": result_schema_sha256(),
            "cli_flags_sha256": canonical_sha256(
                _redacted_command_policy(
                    request=request,
                    compiled_prompt_sha256=input_binding["compiled_prompt_sha256"],
                    session_id=session_id,
                )
            ),
            "environment_policy_sha256": canonical_sha256(_redacted_environment_policy()),
            "max_turns": request["emergency"]["max_turns"],
        }
        if command_binding != expected_command_binding:
            errors.append("command_binding_request_replay_mismatch")
    else:
        errors.append("command_binding_request_replay_unavailable")
    approval = receipt.get("approval", {})
    if mode == "live":
        grant_id = request.get("approval", {}).get("grant_id")
        if not isinstance(grant_id, str):
            errors.append("live_grant_id_missing")
        else:
            grant_id_hash = bytes_sha256(grant_id.encode())
            grant_path, consumption_path = _grant_paths(approval_root, grant_id_hash)
            try:
                grant = _read_private_json(grant_path)
                consumption = _read_private_json(consumption_path)
                grant_raw = grant_path.read_bytes()
                consumption_raw = consumption_path.read_bytes()
            except AdaptiveWaveValidationError:
                errors.append("grant_ledger_unavailable")
            else:
                consumed_at = consumption.get("consumed_at")
                try:
                    consumed_clock = _parse_timestamp(consumed_at)
                except (TypeError, AdaptiveWaveValidationError):
                    errors.append("grant_consumed_at_invalid")
                else:
                    if _validate_grant(grant, request, now=consumed_clock):
                        errors.append("grant_replay_invalid")
                expected_consumption = {
                    "schema_version": CONSUMPTION_SCHEMA_VERSION,
                    "grant_id_hash": grant_id_hash,
                    "grant_sha256": bytes_sha256(grant_raw),
                    "execution_scope_sha256": execution_scope_sha256(request),
                    "request_sha256": request_sha,
                    "run_id": receipt.get("run_id"),
                    "consumed_at": intent.get("started_at"),
                    "state": "consumed_after_binary_auth_preflight_before_process_spawn",
                }
                if consumption != expected_consumption:
                    errors.append("grant_consumption_replay_mismatch")
                if approval.get("grant_id_sha256") != grant_id_hash:
                    errors.append("grant_id_hash_mismatch")
                if approval.get("grant_sha256") != bytes_sha256(grant_raw):
                    errors.append("grant_hash_mismatch")
                if approval.get("consumption_sha256") != bytes_sha256(consumption_raw):
                    errors.append("grant_consumption_hash_mismatch")
    elif isinstance(approval, dict) and (
        approval.get("grant_sha256") is not None or approval.get("consumption_sha256") is not None
    ):
        errors.append("fixture_grant_consumption_invalid")

    process = receipt.get("process", {})
    ledger_path = run_root / "process-ledger.json"
    if isinstance(process, dict) and process.get("process_spawn_attempted") is True:
        try:
            ledger = _read_private_json(ledger_path)
            ledger_raw = ledger_path.read_bytes()
        except AdaptiveWaveValidationError:
            errors.append("process_ledger_unavailable")
        else:
            if not _process_ledger_valid(ledger):
                errors.append("process_ledger_invalid")
            expected_process_facts = {
                "run_id": receipt.get("run_id"),
                "request_id": receipt.get("request_id"),
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
    elif ledger_path.exists():
        errors.append("unexpected_process_ledger")

    raw_path = run_root / "raw.stdout"
    stderr_path = run_root / "stderr.txt"
    if not _private_file_valid(raw_path) or not _private_file_valid(stderr_path):
        errors.append("required_artifact_permissions_invalid")
        return errors
    raw = raw_path.read_bytes()
    stderr = stderr_path.read_bytes()
    limits = request.get("technical_limits", {})
    if _technical_limits_valid(limits):
        if len(raw) > limits["max_stdout_bytes"]:
            errors.append("raw_stdout_ceiling_exceeded")
        if len(stderr) > limits["max_stderr_bytes"]:
            errors.append("stderr_ceiling_exceeded")
    artifacts = receipt.get("artifacts", {}) if isinstance(receipt, dict) else {}
    if bytes_sha256(raw) != artifacts.get("raw_stdout_sha256"):
        errors.append("raw_stdout_hash_mismatch")
    if bytes_sha256(stderr) != artifacts.get("stderr_sha256"):
        errors.append("stderr_hash_mismatch")
    if not _technical_limits_valid(limits):
        return errors
    parsed_result, sanitized, prefix, suffix, compliant, contract_valid, limit_kind = _parse_structured_stdout(
        raw,
        technical_limits=limits,
        prior_candidates=prior_candidates,
        live_mode=mode == "live",
    )
    if receipt.get("status") != "crash_recovered":
        if artifacts.get("non_json_prefix_bytes") != prefix or artifacts.get("non_json_suffix_bytes") != suffix:
            errors.append("structured_boundary_mismatch")
        if artifacts.get("structured_output_compliant") is not compliant:
            errors.append("structured_compliance_mismatch")
        if artifacts.get("structured_output_contract_valid") is not contract_valid:
            errors.append("structured_contract_mismatch")
        recorded_limit = process.get("technical_limit_kind") if isinstance(process, dict) else None
        if limit_kind is not None and recorded_limit != limit_kind:
            errors.append("structured_technical_limit_mismatch")
    expected_reconciliation = _candidate_reconciliation(
        parsed_result if isinstance(parsed_result, dict) else {},
        prior_candidates,
    )
    if receipt.get("reconciliation") != expected_reconciliation:
        errors.append("receipt_reconciliation_mismatch")
    sanitized_path = run_root / "sanitized.json"
    if sanitized is None:
        if artifacts.get("sanitized_output_sha256") is not None or sanitized_path.exists():
            errors.append("unexpected_sanitized_artifact")
    else:
        if not _private_file_valid(sanitized_path):
            errors.append("sanitized_artifact_permissions_invalid")
        elif sanitized_path.read_bytes() != sanitized or bytes_sha256(sanitized) != artifacts.get(
            "sanitized_output_sha256"
        ):
            errors.append("sanitized_artifact_hash_mismatch")
    if _command_binding_valid(command_binding) and _input_binding_valid(input_binding):
        if command_binding["structured_output_schema_sha256"] != result_schema_sha256():
            errors.append("structured_output_schema_hash_mismatch")
        expected_policy_sha = canonical_sha256(_redacted_policy_from_bindings(input_binding, command_binding))
        if command_binding["cli_flags_sha256"] != expected_policy_sha:
            errors.append("cli_flags_hash_mismatch")
        if command_binding["environment_policy_sha256"] != canonical_sha256(_redacted_environment_policy()):
            errors.append("environment_policy_hash_mismatch")
    compiled_sha = (
        bytes_sha256(compiled_prompt_path.read_bytes()) if _private_file_valid(compiled_prompt_path) else None
    )
    if compiled_sha != artifacts.get("compiled_prompt_sha256"):
        errors.append("compiled_prompt_artifact_hash_mismatch")
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
    terminate_orphan: bool = False,
    process_group_is_alive: Callable[[int], bool] = ProcessGroupExecutor._group_alive,
    process_group_identity_matches: Callable[[Mapping[str, Any]], bool] = _recorded_process_group_identity_matches,
    terminate_process_group: Callable[..., tuple[bool, bool]] = _terminate_existing_group,
    monotonic: Callable[[], float] = time.monotonic,
    wall_clock: Callable[[], datetime] = _utc_now,
) -> dict[str, Any]:
    """Seal an interrupted run only after its recorded process group is dead."""

    _ensure_private_directory(run_root, create=False)
    recover_pending_publications(run_root)
    if (run_root / "operator-receipt.json").exists():
        raise AdaptiveWaveValidationError("run_already_terminal")
    intent = _read_private_json(run_root / "operator-intent.json")
    if not _intent_valid(intent):
        raise AdaptiveWaveValidationError("intent_invalid")
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
            or ledger["session_id"] != intent["command_binding"]["session_id"]
        ):
            raise AdaptiveWaveValidationError("process_ledger_binding_invalid")
        process_ledger_sha = bytes_sha256(ledger_path.read_bytes())
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
    _delete_ephemeral_auth(run_root / intent["runtime_layout"]["ephemeral_home_name"])
    _harden_ephemeral_tree(run_root / intent["runtime_layout"]["ephemeral_home_name"])
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
            if not _private_file_valid(spool_path) or spool_path.stat().st_size > ceiling:
                raise AdaptiveWaveValidationError("recovery_spool_invalid")
            _atomic_publish(final_path, spool_path.read_bytes())
            spool_path.unlink()
    if not raw_path.exists():
        _atomic_publish(raw_path, b"")
    if not stderr_path.exists():
        _atomic_publish(stderr_path, b"")
    if not _private_file_valid(raw_path) or not _private_file_valid(stderr_path):
        raise AdaptiveWaveValidationError("recovery_artifact_permissions_invalid")
    parsed_result, sanitized, _, _, _, _, _ = _parse_structured_stdout(
        raw_path.read_bytes(),
        technical_limits=intent["technical_limits"],
        prior_candidates=prior_candidates,
        live_mode=intent["execution_mode"] == "live",
    )
    sanitized_path = run_root / "sanitized.json"
    if sanitized_path.exists():
        if not _private_file_valid(sanitized_path):
            raise AdaptiveWaveValidationError("recovery_sanitized_permissions_invalid")
        if sanitized is None or sanitized_path.read_bytes() != sanitized:
            raise AdaptiveWaveValidationError("recovery_sanitized_hash_mismatch")
    elif sanitized is not None:
        _atomic_publish(sanitized_path, sanitized)
    sanitized_sha = bytes_sha256(sanitized_path.read_bytes()) if sanitized_path.exists() else None
    compiled_prompt_path = run_root / intent["runtime_layout"]["compiled_prompt_name"]
    if not _private_file_valid(compiled_prompt_path):
        raise AdaptiveWaveValidationError("recovery_compiled_prompt_invalid")
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "run_id": intent["run_id"],
        "request_id": intent["request_id"],
        "request_sha256": intent["request_sha256"],
        "execution_mode": intent["execution_mode"],
        "status": "crash_recovered",
        "input_binding": intent["input_binding"],
        "command_binding": intent["command_binding"],
        "approval": intent["approval"],
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
            "raw_stdout_sha256": bytes_sha256(raw_path.read_bytes()),
            "stderr_sha256": bytes_sha256(stderr_path.read_bytes()),
            "sanitized_output_sha256": sanitized_sha,
            "structured_output_compliant": False,
            "structured_output_contract_valid": False,
            "non_json_prefix_bytes": None,
            "non_json_suffix_bytes": None,
            "compiled_prompt_sha256": bytes_sha256(compiled_prompt_path.read_bytes()),
            "ephemeral_auth_deleted": not (
                run_root / intent["runtime_layout"]["ephemeral_home_name"] / "auth.json"
            ).exists(),
            "ephemeral_tree_private": _ephemeral_tree_private(
                run_root / intent["runtime_layout"]["ephemeral_home_name"]
            ),
        },
        "reconciliation": _candidate_reconciliation(
            parsed_result if isinstance(parsed_result, dict) else {},
            prior_candidates,
        ),
        "authority": AUTHORITY,
    }
    if validate_operator_receipt(receipt):
        raise AdaptiveWaveValidationError("recovery_receipt_invalid")
    _atomic_publish(run_root / "operator-receipt.json", (canonical_json(receipt) + "\n").encode())
    return receipt
