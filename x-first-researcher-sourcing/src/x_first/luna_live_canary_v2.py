"""Receipt-first Luna semantic live canary v2.

This module deliberately does not pass a live transport object into the
semantic contract.  The HTTP boundary returns an observed, sanitized attempt
receipt.  Semantic adjudication is then replayed from that receipt and the
retained response body, so external-call accounting never comes from an
``is_live`` attribute supplied by an arbitrary transport.

The legacy v1 artifact validator remains in :mod:`x_first.luna_live_canary` and
is frozen to semantic v2.1 assets.  This module owns a distinct result schema,
approval identity, run-id namespace, and semantic-v2.2 asset binding.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import shutil
import stat
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from . import luna_live_canary as legacy
from . import profile_bio_semantic_v2 as semantic

RESULT_SCHEMA_VERSION = "x.profile.bio_semantic.live_canary.result.v2"
EXECUTION_RECEIPT_SCHEMA_VERSION = "x.profile.bio_semantic.live_canary.execution_receipt.v2"
APPROVAL_RECEIPT_SCHEMA_VERSION = "x.profile.bio_semantic.live_canary.approval.v2"
DELETION_RECEIPT_SCHEMA_VERSION = "x.profile.bio_semantic.live_canary.deletion_receipt.v2"
DELETION_JOURNAL_SCHEMA_VERSION = "x.profile.bio_semantic.live_canary.deletion_journal.v2"

MODEL_ID = legacy.MODEL_ID
BASE_URL = legacy.BASE_URL
CATALOG_URL = legacy.CATALOG_URL
RESPONSES_URL = legacy.RESPONSES_URL
KEY_ENVIRONMENT_VARIABLE = legacy.KEY_ENVIRONMENT_VARIABLE
PRODUCTION_EXECUTION_ORIGIN = "production_http"
FIXTURE_EXECUTION_ORIGIN = "offline_fixture"
PRODUCTION_PROBE_ID = "profile_bio_semantic_v22_chshapi_luna_receipt_first_v2"
FIXTURE_PROBE_ID = "profile_bio_semantic_v22_chshapi_luna_offline_fixture_v2"
PRODUCTION_OWNER_ID = "user_state:x-first-researcher-sourcing/luna-live-approvals/v2"
FIXTURE_OWNER_ID = "offline_fixture:x-first-researcher-sourcing/luna-live-approvals/v2"
PRODUCTION_APPROVAL_ID = "user_approved_2026_07_14_chshapi_luna_receipt_first_v2"
FIXTURE_APPROVAL_ID = "offline_fixture_chshapi_luna_receipt_first_v2"
OWNER_APPROVAL_ID = PRODUCTION_APPROVAL_ID

TOTAL_TIMEOUT_MS = legacy.TOTAL_TIMEOUT_MS
MAX_ARTIFACT_BYTES = legacy.MAX_ARTIFACT_BYTES
RETENTION_HOURS = legacy.RETENTION_HOURS

_PRODUCTION_RUN_ID_RE = re.compile(r"luna_canary_v2_run_[0-9a-f]{32}")
_FIXTURE_RUN_ID_RE = re.compile(r"luna_canary_v2_fixture_run_[0-9a-f]{32}")
_SHA256_RE = legacy._SHA256_RE
_CANONICAL_TIME_RE = legacy._CANONICAL_TIME_RE
_ATTEMPT_KEYS = {
    "sequence",
    "operation",
    "method",
    "endpoint",
    "requested_model",
    "request_payload_sha256",
    "started_at",
    "completed_at",
    "elapsed_ms",
    "timeout_ms",
    "outcome",
    "http_status",
    "response_body_sha256",
    "response_content_type",
    "response_received",
    "retry_used",
    "fallback_used",
}
_AUTHORITY = dict(legacy._AUTHORITY)
_BUDGETS = dict(legacy._BUDGETS)
_OUTER_ERROR_CODES = set(legacy._OUTER_ERROR_CODES)
_OUTER_ERROR_CODES.add("response_content_type_invalid")

HttpClient = legacy.HttpClient
HttpResponse = legacy.HttpResponse
UrllibHttpClient = legacy.UrllibHttpClient


@dataclass(frozen=True)
class ObservedHttpAttempt:
    """One sanitized attempt plus an optional in-memory bounded response."""

    receipt: Mapping[str, Any]
    response: HttpResponse | None


class ObservedHttpTransport:
    """Wrap an HTTP client and return observed receipts, never mode claims."""

    def __init__(
        self,
        client: HttpClient,
        *,
        wall_clock: Callable[[], datetime],
        monotonic: Callable[[], float],
    ) -> None:
        self._client = client
        self._wall_clock = wall_clock
        self._monotonic = monotonic

    def execute(
        self,
        *,
        sequence: int,
        operation: str,
        method: str,
        endpoint: str,
        requested_model: str | None,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout_ms: int,
        max_response_bytes: int,
    ) -> ObservedHttpAttempt:
        expected = {
            1: ("model_catalog", "GET", CATALOG_URL, None),
            2: ("semantic_response", "POST", RESPONSES_URL, MODEL_ID),
        }
        if expected.get(sequence) != (operation, method, endpoint, requested_model):
            raise ValueError("closed_observed_route_required")
        if not legacy._is_int(timeout_ms) or not 1 <= timeout_ms <= TOTAL_TIMEOUT_MS:
            raise ValueError("bounded_timeout_required")
        payload_bytes = body or b""
        started_at = legacy._timestamp(self._wall_clock())
        started = self._monotonic()
        response: HttpResponse | None = None
        try:
            response = legacy._validate_http_response(
                self._client.request(
                    method=method,
                    url=endpoint,
                    headers=headers,
                    body=body,
                    timeout_ms=timeout_ms,
                    max_response_bytes=max_response_bytes,
                ),
                max_response_bytes,
            )
        except Exception:  # noqa: BLE001 - transport details must not escape the receipt boundary
            outcome = "transport_failure"
        else:
            outcome = "http_response"
        elapsed_ms = max(0, int((self._monotonic() - started) * 1000))
        completed_at = legacy._timestamp(legacy._parse_timestamp(started_at) + timedelta(milliseconds=elapsed_ms))
        receipt = {
            "sequence": sequence,
            "operation": operation,
            "method": method,
            "endpoint": endpoint,
            "requested_model": requested_model,
            "request_payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
            "started_at": started_at,
            "completed_at": completed_at,
            "elapsed_ms": elapsed_ms,
            "timeout_ms": timeout_ms,
            "outcome": outcome,
            "http_status": response.status_code if response is not None else None,
            "response_body_sha256": hashlib.sha256(response.body).hexdigest() if response is not None else None,
            "response_content_type": _sanitized_content_type(response) if response is not None else None,
            "response_received": response is not None,
            "retry_used": False,
            "fallback_used": False,
        }
        return ObservedHttpAttempt(receipt=receipt, response=response)


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _runtime_root() -> Path:
    return project_root() / "runtime/luna-live-canaries-v2"


def _approval_root() -> Path:
    return Path.home() / ".local/state/x-first-researcher-sourcing/luna-live-approvals-v2"


def _deletion_root() -> Path:
    return Path.home() / ".local/state/x-first-researcher-sourcing/luna-live-deletions-v2"


def _origin_identity(execution_origin: str) -> tuple[str, str, str, re.Pattern[str], str]:
    if execution_origin == PRODUCTION_EXECUTION_ORIGIN:
        return (
            PRODUCTION_PROBE_ID,
            PRODUCTION_OWNER_ID,
            PRODUCTION_APPROVAL_ID,
            _PRODUCTION_RUN_ID_RE,
            "luna_canary_v2_run_",
        )
    if execution_origin == FIXTURE_EXECUTION_ORIGIN:
        return (
            FIXTURE_PROBE_ID,
            FIXTURE_OWNER_ID,
            FIXTURE_APPROVAL_ID,
            _FIXTURE_RUN_ID_RE,
            "luna_canary_v2_fixture_run_",
        )
    raise ValueError("execution_origin_invalid")


@dataclass(frozen=True)
class _RunnerLane:
    """Module-owned lane capability; provenance is never accepted as caller text."""

    execution_origin: str


_PRODUCTION_LANE = _RunnerLane(PRODUCTION_EXECUTION_ORIGIN)
_FIXTURE_LANE = _RunnerLane(FIXTURE_EXECUTION_ORIGIN)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _validate_runner_boundary(
    lane: _RunnerLane,
    *,
    http_client: HttpClient,
    runtime_root: Path,
    approval_root: Path,
    wall_clock: Callable[[], datetime],
    monotonic: Callable[[], float],
) -> str:
    """Fail closed when an injected runner tries to claim production provenance."""

    if lane is _PRODUCTION_LANE:
        if (
            type(http_client) is not UrllibHttpClient
            or runtime_root != _runtime_root()
            or approval_root != _approval_root()
            or wall_clock is not _utc_now
            or monotonic is not time.monotonic
        ):
            raise PermissionError("production_runner_dependencies_not_injectable")
        return PRODUCTION_EXECUTION_ORIGIN
    if lane is _FIXTURE_LANE:
        if http_client is None or isinstance(http_client, UrllibHttpClient):
            raise PermissionError("offline_fixture_network_transport_forbidden")
        return FIXTURE_EXECUTION_ORIGIN
    raise PermissionError("module_owned_runner_lane_required")


def _semantic_assets() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    prompt = semantic.load_json(project_root() / "configs/profile_bio_semantic_prompt.v2.json")
    output_schema = semantic.load_json(project_root() / "contracts/x.profile.bio_semantic.model_output.v2.schema.json")
    proxy_policy = semantic.load_json(
        project_root() / "configs/profile_bio_professional_experience_proxy_policy.v1.json"
    )
    if (
        semantic.validate_prompt(prompt)
        or semantic.validate_output_schema(output_schema)
        or semantic.validate_proxy_policy(proxy_policy)
        or semantic.validate_pure_adjudication_implementation()
    ):
        raise RuntimeError("semantic_v22_assets_invalid")
    return prompt, output_schema, proxy_policy


def build_synthetic_live_request() -> dict[str, Any]:
    """Build the v2.2-only synthetic request; never read legacy v1 assets."""

    prompt, output_schema, proxy_policy = _semantic_assets()
    request = copy.deepcopy(semantic.load_json(project_root() / "fixtures/profile_bio_semantic_request_v2.json"))
    request["request_id"] = "xbsv2r_444444444444444444444444"
    request["model_execution_mode"] = "live_canary"
    if semantic.validate_request(
        request,
        prompt=prompt,
        output_schema=output_schema,
        proxy_policy=proxy_policy,
    ):
        raise RuntimeError("semantic_v22_request_invalid")
    return request


def _consume_approval(
    root: Path,
    *,
    request: Mapping[str, Any],
    run_id: str,
    consumed_at: str,
    execution_origin: str,
) -> dict[str, Any]:
    probe_id, owner_id, owner_approval_id, run_id_pattern, _ = _origin_identity(execution_origin)
    if run_id_pattern.fullmatch(run_id) is None:
        raise ValueError("run_id_origin_mismatch")
    legacy._ensure_private_directory(root)
    receipt = {
        "schema_version": APPROVAL_RECEIPT_SCHEMA_VERSION,
        "execution_origin": execution_origin,
        "owner_id": owner_id,
        "owner_approval_id": owner_approval_id,
        "probe_id": probe_id,
        "semantic_request_schema_version": semantic.REQUEST_SCHEMA_VERSION,
        "semantic_review_schema_version": semantic.REVIEW_SCHEMA_VERSION,
        "semantic_adjudication_api_version": semantic.PURE_ADJUDICATION_API_VERSION,
        "semantic_adjudication_implementation_sha256": semantic.PURE_ADJUDICATION_IMPLEMENTATION_SHA256,
        "request_sha256": legacy._canonical_sha256(request),
        "run_id": run_id,
        "consumed_at": consumed_at,
        "state": "consumed_before_catalog",
    }
    path = root / f"{owner_approval_id}.json"
    serialized = (json.dumps(receipt, allow_nan=False, ensure_ascii=True, sort_keys=True) + "\n").encode()
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise PermissionError("owner_approved_luna_canary_v2_already_consumed") from exc
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(serialized)
        while view:
            view = view[os.write(descriptor, view) :]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    parent_descriptor = os.open(root, os.O_RDONLY)
    try:
        os.fsync(parent_descriptor)
    finally:
        os.close(parent_descriptor)
    return receipt


def _semantic_attempt_from_receipt(attempt: Mapping[str, Any]) -> dict[str, Any]:
    """Create semantic execution facts only after validating an observed POST."""

    if not _attempt_valid(attempt, expected_sequence=2, expected_payload_sha256=None):
        raise ValueError("observed_semantic_attempt_invalid")
    response_received = attempt["outcome"] == "http_response"
    return {
        "mode": "live",
        "transport_id": semantic.PROVIDER_ID,
        "execute_live": True,
        "transport_invocations": 1,
        "provider_external_calls": 1,
        "response_received": response_received,
        "fallback_used": False,
        "phase": "response" if response_received else "transport",
        "contract_error": "none",
    }


def _adjudicate_observed_response(
    *,
    request: Mapping[str, Any],
    prompt: Mapping[str, Any],
    output_schema: Mapping[str, Any],
    proxy_policy: Mapping[str, Any],
    attempt: Mapping[str, Any],
    parsed_response: Any,
) -> dict[str, Any]:
    """Purely adjudicate one receipt-bound response without a transport object."""

    execution_attempt = _semantic_attempt_from_receipt(attempt)
    return semantic.adjudicate_observed_response(
        request=request,
        prompt=prompt,
        output_schema=output_schema,
        proxy_policy=proxy_policy,
        execution_attempt=execution_attempt,
        raw_response=parsed_response,
    )


def _attempt_valid(
    attempt: Any,
    *,
    expected_sequence: int,
    expected_payload_sha256: str | None,
) -> bool:
    expected_route = {
        1: ("model_catalog", "GET", CATALOG_URL, None),
        2: ("semantic_response", "POST", RESPONSES_URL, MODEL_ID),
    }
    if (
        not isinstance(attempt, dict)
        or set(attempt) != _ATTEMPT_KEYS
        or expected_route.get(expected_sequence)
        != (
            attempt.get("operation"),
            attempt.get("method"),
            attempt.get("endpoint"),
            attempt.get("requested_model"),
        )
        or attempt.get("sequence") != expected_sequence
        or not isinstance(attempt.get("request_payload_sha256"), str)
        or _SHA256_RE.fullmatch(attempt["request_payload_sha256"]) is None
        or (expected_payload_sha256 is not None and attempt["request_payload_sha256"] != expected_payload_sha256)
        or not legacy._is_int(attempt.get("timeout_ms"))
        or not 1 <= attempt["timeout_ms"] <= TOTAL_TIMEOUT_MS
        or not legacy._is_int(attempt.get("elapsed_ms"))
        or attempt["elapsed_ms"] < 0
        or attempt.get("retry_used") is not False
        or attempt.get("fallback_used") is not False
    ):
        return False
    try:
        started = legacy._parse_timestamp(attempt["started_at"])
        completed = legacy._parse_timestamp(attempt["completed_at"])
    except (KeyError, TypeError, ValueError):
        return False
    if completed < started or int((completed - started).total_seconds() * 1000) != attempt["elapsed_ms"]:
        return False
    if attempt.get("outcome") == "transport_failure":
        return (
            attempt.get("http_status") is None
            and attempt.get("response_body_sha256") is None
            and attempt.get("response_content_type") is None
            and attempt.get("response_received") is False
        )
    return (
        attempt.get("outcome") == "http_response"
        and legacy._is_int(attempt.get("http_status"))
        and 100 <= attempt["http_status"] <= 599
        and isinstance(attempt.get("response_body_sha256"), str)
        and _SHA256_RE.fullmatch(attempt["response_body_sha256"]) is not None
        and isinstance(attempt.get("response_content_type"), str)
        and len(attempt["response_content_type"]) <= 200
        and attempt["response_content_type"].isascii()
        and all(ord(character) >= 32 for character in attempt["response_content_type"])
        and attempt.get("response_received") is True
    )


def _execution_receipt(
    *,
    run_id: str,
    execution_origin: str,
    request_payload_sha256: str,
    attempts: list[Mapping[str, Any]],
    started_at: str,
    completed_at: str,
    elapsed_ms: int,
) -> dict[str, Any]:
    return {
        "schema_version": EXECUTION_RECEIPT_SCHEMA_VERSION,
        "run_id": run_id,
        "execution_origin": execution_origin,
        "provider": "chshapi_openai_compatible_relay",
        "base_url": BASE_URL,
        "requested_model": MODEL_ID,
        "request_payload_sha256": request_payload_sha256,
        "semantic_adjudication_api_version": semantic.PURE_ADJUDICATION_API_VERSION,
        "semantic_adjudication_implementation_sha256": semantic.PURE_ADJUDICATION_IMPLEMENTATION_SHA256,
        "attempt_count": len(attempts),
        "attempts": [copy.deepcopy(dict(attempt)) for attempt in attempts],
        "started_at": started_at,
        "completed_at": completed_at,
        "elapsed_ms": elapsed_ms,
        "total_timeout_ms": TOTAL_TIMEOUT_MS,
        "max_attempts_per_operation": 1,
        "retry_allowed": False,
        "fallback_allowed": False,
    }


def _execution_receipt_valid(
    receipt: Any,
    *,
    run_id: str,
    execution_origin: str,
    request_payload_sha256: str,
) -> bool:
    try:
        _, _, _, run_id_pattern, _ = _origin_identity(execution_origin)
    except ValueError:
        return False
    keys = {
        "schema_version",
        "run_id",
        "execution_origin",
        "provider",
        "base_url",
        "requested_model",
        "request_payload_sha256",
        "semantic_adjudication_api_version",
        "semantic_adjudication_implementation_sha256",
        "attempt_count",
        "attempts",
        "started_at",
        "completed_at",
        "elapsed_ms",
        "total_timeout_ms",
        "max_attempts_per_operation",
        "retry_allowed",
        "fallback_allowed",
    }
    if (
        not isinstance(receipt, dict)
        or set(receipt) != keys
        or receipt.get("schema_version") != EXECUTION_RECEIPT_SCHEMA_VERSION
        or receipt.get("run_id") != run_id
        or run_id_pattern.fullmatch(run_id) is None
        or receipt.get("execution_origin") != execution_origin
        or receipt.get("provider") != "chshapi_openai_compatible_relay"
        or receipt.get("base_url") != BASE_URL
        or receipt.get("requested_model") != MODEL_ID
        or receipt.get("request_payload_sha256") != request_payload_sha256
        or receipt.get("semantic_adjudication_api_version") != semantic.PURE_ADJUDICATION_API_VERSION
        or receipt.get("semantic_adjudication_implementation_sha256")
        != semantic.PURE_ADJUDICATION_IMPLEMENTATION_SHA256
        or receipt.get("total_timeout_ms") != TOTAL_TIMEOUT_MS
        or receipt.get("max_attempts_per_operation") != 1
        or receipt.get("retry_allowed") is not False
        or receipt.get("fallback_allowed") is not False
        or not isinstance(receipt.get("attempts"), list)
        or not 1 <= len(receipt["attempts"]) <= 2
        or receipt.get("attempt_count") != len(receipt["attempts"])
        or not legacy._is_int(receipt.get("elapsed_ms"))
        or receipt["elapsed_ms"] < 0
    ):
        return False
    if not _attempt_valid(
        receipt["attempts"][0],
        expected_sequence=1,
        expected_payload_sha256=hashlib.sha256(b"").hexdigest(),
    ):
        return False
    if len(receipt["attempts"]) == 2 and not _attempt_valid(
        receipt["attempts"][1],
        expected_sequence=2,
        expected_payload_sha256=request_payload_sha256,
    ):
        return False
    try:
        started = legacy._parse_timestamp(receipt["started_at"])
        completed = legacy._parse_timestamp(receipt["completed_at"])
    except (KeyError, TypeError, ValueError):
        return False
    attempt_times = [
        (legacy._parse_timestamp(attempt["started_at"]), legacy._parse_timestamp(attempt["completed_at"]))
        for attempt in receipt["attempts"]
    ]
    if any(
        current_started < previous_completed
        for (_, previous_completed), (current_started, _) in zip(attempt_times, attempt_times[1:], strict=False)
    ):
        return False
    if any(
        attempt_started < started or attempt_completed > completed
        for attempt_started, attempt_completed in attempt_times
    ):
        return False
    if sum(attempt["elapsed_ms"] for attempt in receipt["attempts"]) > receipt["elapsed_ms"]:
        return False
    if len(receipt["attempts"]) == 2 and receipt["attempts"][1]["timeout_ms"] > receipt["attempts"][0]["timeout_ms"]:
        return False
    return (
        completed >= started
        and int((completed - started).total_seconds() * 1000) == receipt["elapsed_ms"]
        and receipt["started_at"] == receipt["attempts"][0]["started_at"]
        and legacy._parse_timestamp(receipt["completed_at"])
        >= legacy._parse_timestamp(receipt["attempts"][-1]["completed_at"])
    )


_JSON_CONTENT_TYPE_RE = re.compile(
    r"\Aapplication/json(?:\s*;\s*charset\s*=\s*utf-8)?\s*\Z",
    re.IGNORECASE,
)


def _strict_json_content_type(value: Any) -> bool:
    return isinstance(value, str) and _JSON_CONTENT_TYPE_RE.fullmatch(value) is not None


def _sanitized_content_type(response: HttpResponse) -> str:
    value = legacy._content_type(response.headers)
    encoded = value.encode("ascii", errors="ignore")
    if legacy._KEY_BYTES_RE.search(encoded) is not None or legacy._body_contains_secret(response.body):
        return ""
    return value


def _parse_catalog_v2(response: HttpResponse) -> tuple[dict[str, Any], str | None]:
    """Keep legacy catalog grammar but require an exact JSON media type on 200."""

    if response.status_code == 200 and not _strict_json_content_type(_sanitized_content_type(response)):
        return legacy._catalog_receipt(
            status="failed",
            http_status=response.status_code,
            error_code="catalog_invalid",
        ), "catalog_invalid"
    return legacy._parse_catalog(response)


def _normalized_returned_model(value: Any) -> str | None:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= legacy.MAX_MODEL_ID_CHARACTERS
        or not value.isascii()
        or any(ord(character) < 33 for character in value)
    ):
        return None
    return value


def _parsed_raw_response(raw_receipt: Mapping[str, Any] | None) -> Any:
    if raw_receipt is None:
        return None
    if not _strict_json_content_type(raw_receipt.get("content_type")):
        return None
    try:
        body = legacy._decode_raw_response(raw_receipt)
        if body is None:
            return None
        parsed = legacy._strict_json_loads(body)
    except (UnicodeError, ValueError, RecursionError):
        return None
    return None if legacy._scan_json(parsed) else parsed


def _derive_outer_error(
    *,
    catalog: Mapping[str, Any],
    execution: Mapping[str, Any],
    raw_response: Mapping[str, Any] | None,
    semantic_review: Mapping[str, Any] | None,
) -> str | None:
    if execution["elapsed_ms"] > TOTAL_TIMEOUT_MS:
        return "total_timeout_exceeded"
    if catalog.get("status") != "completed":
        return str(catalog.get("error_code"))
    if execution["attempt_count"] == 1:
        return "total_timeout_exceeded"
    response_attempt = execution["attempts"][1]
    if response_attempt["outcome"] == "transport_failure":
        return "response_transport_failed"
    if raw_response is None:
        return "response_transport_failed"
    status = raw_response["http_status"]
    if 300 <= status <= 399:
        return "response_redirect_rejected"
    if status != 200:
        return "response_http_failed"
    if raw_response["secret_redacted"] is True:
        return "response_secret_detected"
    if not _strict_json_content_type(raw_response.get("content_type")):
        return "response_content_type_invalid"
    parsed = _parsed_raw_response(raw_response)
    if isinstance(parsed, dict) and parsed.get("model") != MODEL_ID:
        return "response_model_mismatch"
    if not isinstance(semantic_review, dict) or semantic_review.get("status") != "completed":
        return "semantic_review_failed"
    return None


def _build_result(
    *,
    run_id: str,
    request: Mapping[str, Any],
    approval: Mapping[str, Any],
    catalog: Mapping[str, Any],
    execution: Mapping[str, Any],
    raw_response: Mapping[str, Any] | None,
    semantic_review: Mapping[str, Any] | None,
    payloads: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    execution_origin = str(execution["execution_origin"])
    probe_id, _, owner_approval_id, _, _ = _origin_identity(execution_origin)
    error_code = _derive_outer_error(
        catalog=catalog,
        execution=execution,
        raw_response=raw_response,
        semantic_review=semantic_review,
    )
    status = "completed" if error_code is None else "failed"
    response_attempted = execution["attempt_count"] == 2
    response_received = raw_response is not None
    receipt = semantic_review.get("model_receipt") if isinstance(semantic_review, dict) else None
    usage = semantic_review.get("usage") if isinstance(semantic_review, dict) else None
    returned_model = _normalized_returned_model(receipt.get("model_id")) if isinstance(receipt, dict) else None
    if returned_model is None and raw_response is not None:
        parsed = _parsed_raw_response(raw_response)
        returned_model = _normalized_returned_model(parsed.get("model")) if isinstance(parsed, dict) else None
    completed_at = execution["completed_at"]
    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "status": status,
        "error_codes": [] if error_code is None else [error_code],
        "run": {
            "run_id": run_id,
            "probe_id": probe_id,
            "request_id": request["request_id"],
            "request_sha256": legacy._canonical_sha256(request),
            "owner_approval_id": owner_approval_id,
        },
        "evidence_provenance": {
            "execution_origin": execution_origin,
            "production_transport_observed": execution_origin == PRODUCTION_EXECUTION_ORIGIN,
            "formal_live_evidence_eligible": execution_origin == PRODUCTION_EXECUTION_ORIGIN,
            "caller_injection_used": execution_origin == FIXTURE_EXECUTION_ORIGIN,
        },
        "semantic_contract": {
            "request_schema_version": semantic.REQUEST_SCHEMA_VERSION,
            "review_schema_version": semantic.REVIEW_SCHEMA_VERSION,
            "prompt_version": semantic.PROMPT_VERSION,
            "prompt_sha256": semantic.CANONICAL_PROMPT_SHA256,
            "output_schema_version": semantic.MODEL_OUTPUT_SCHEMA_VERSION,
            "output_schema_sha256": semantic.CANONICAL_OUTPUT_SCHEMA_SHA256,
            "proxy_policy_version": semantic.PROXY_POLICY_VERSION,
            "proxy_policy_sha256": semantic.CANONICAL_PROXY_POLICY_SHA256,
            "pure_adjudication_api_version": semantic.PURE_ADJUDICATION_API_VERSION,
            "pure_adjudication_implementation_sha256": semantic.PURE_ADJUDICATION_IMPLEMENTATION_SHA256,
        },
        "identity": {
            "provider": "chshapi_openai_compatible_relay",
            "base_url": BASE_URL,
            "requested_model": MODEL_ID,
            "returned_model": returned_model,
            "exact_model_match": returned_model == MODEL_ID,
            "fallback_used": False,
            "tools_used": False,
        },
        "catalog": {
            key: catalog[key]
            for key in (
                "status",
                "http_status",
                "model_count",
                "distinct_model_count",
                "model_ids_sha256",
                "exact_model_matches",
                "error_code",
            )
        },
        "semantic": {
            "review_status": semantic_review.get("status") if isinstance(semantic_review, dict) else None,
            "error_codes": semantic_review.get("error_codes", []) if isinstance(semantic_review, dict) else [],
            "review_schema_version": (
                semantic_review.get("schema_version") if isinstance(semantic_review, dict) else None
            ),
        },
        "calls": {
            "catalog_external_calls": 1,
            "model_external_calls": 1 if response_attempted else 0,
            "total_external_calls": execution["attempt_count"],
            "semantic_model_calls": (
                semantic_review.get("execution", {}).get("provider_external_calls", 0)
                if isinstance(semantic_review, dict)
                else 0
            ),
        },
        "usage": copy.deepcopy(usage),
        "timing": {
            "started_at": execution["started_at"],
            "completed_at": completed_at,
            "elapsed_ms": execution["elapsed_ms"],
            "timeout_ms": TOTAL_TIMEOUT_MS,
        },
        "budgets": dict(_BUDGETS),
        "retention": {
            "class": "private_raw_evidence",
            "hours": RETENTION_HOURS,
            "delete_after": legacy._timestamp(legacy._parse_timestamp(completed_at) + timedelta(hours=RETENTION_HOURS)),
            "deletion_status": "pending",
        },
        "response_received": response_received,
        "artifact_inventory": sorted([*payloads, "result.json"]),
        "artifact_sha256s": {name: legacy._canonical_sha256(value) for name, value in sorted(payloads.items())},
        "approval_receipt_sha256": legacy._canonical_sha256(approval),
        "execution_receipt_sha256": legacy._canonical_sha256(execution),
        "authority": dict(_AUTHORITY),
    }


def _run_luna_live_canary_v2(
    *,
    lane: _RunnerLane,
    key: str,
    http_client: HttpClient,
    runtime_root: Path,
    approval_root: Path,
    wall_clock: Callable[[], datetime],
    monotonic: Callable[[], float],
) -> tuple[dict[str, Any], Path]:
    """Shared closed runner; public production and fixture wrappers own provenance."""

    if not legacy._valid_key(key):
        raise PermissionError("chshapi_api_key_missing_or_invalid")
    execution_origin = _validate_runner_boundary(
        lane,
        http_client=http_client,
        runtime_root=runtime_root,
        approval_root=approval_root,
        wall_clock=wall_clock,
        monotonic=monotonic,
    )
    probe_id, _, _, _, run_id_prefix = _origin_identity(execution_origin)
    del probe_id
    prompt, output_schema, proxy_policy = _semantic_assets()
    request = build_synthetic_live_request()
    payload = semantic.build_responses_request(
        request,
        prompt=prompt,
        output_schema=output_schema,
        proxy_policy=proxy_policy,
    )
    if payload.get("model") != MODEL_ID or payload.get("tools") != [] or payload.get("store") is not False:
        raise RuntimeError("semantic_v22_payload_policy_invalid")
    encoded_payload = legacy._canonical_json(payload).encode("utf-8")
    payload_sha256 = hashlib.sha256(encoded_payload).hexdigest()

    current_time = wall_clock
    monotonic_clock = monotonic
    run_id = f"{run_id_prefix}{uuid.uuid4().hex}"
    legacy._ensure_private_directory(runtime_root)
    approval = _consume_approval(
        approval_root,
        request=request,
        run_id=run_id,
        consumed_at=legacy._timestamp(current_time()),
        execution_origin=execution_origin,
    )
    execution_started_at = legacy._timestamp(current_time())
    execution_started = monotonic_clock()
    transport = ObservedHttpTransport(
        http_client,
        wall_clock=current_time,
        monotonic=monotonic_clock,
    )
    attempts: list[Mapping[str, Any]] = []
    catalog_attempt = transport.execute(
        sequence=1,
        operation="model_catalog",
        method="GET",
        endpoint=CATALOG_URL,
        requested_model=None,
        headers={"Accept": "application/json", "Authorization": f"Bearer {key}"},
        body=None,
        timeout_ms=max(1, legacy._remaining_ms(execution_started, monotonic_clock)),
        max_response_bytes=legacy.MAX_CATALOG_BYTES,
    )
    attempts.append(catalog_attempt.receipt)
    if catalog_attempt.response is None:
        catalog = legacy._catalog_receipt(status="failed", http_status=None, error_code="catalog_transport_failed")
        catalog_error: str | None = "catalog_transport_failed"
    else:
        catalog, catalog_error = _parse_catalog_v2(catalog_attempt.response)

    raw_response: dict[str, Any] | None = None
    semantic_review: dict[str, Any] | None = None
    if catalog_error is None and legacy._remaining_ms(execution_started, monotonic_clock) > 0:
        response_attempt = transport.execute(
            sequence=2,
            operation="semantic_response",
            method="POST",
            endpoint=RESPONSES_URL,
            requested_model=MODEL_ID,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            body=encoded_payload,
            timeout_ms=max(1, legacy._remaining_ms(execution_started, monotonic_clock)),
            max_response_bytes=legacy.MAX_RESPONSE_BYTES,
        )
        attempts.append(response_attempt.receipt)
        if response_attempt.response is not None:
            raw_response = legacy._raw_response_receipt(response_attempt.response, key)
        semantic_review = _adjudicate_observed_response(
            request=request,
            prompt=prompt,
            output_schema=output_schema,
            proxy_policy=proxy_policy,
            attempt=response_attempt.receipt,
            parsed_response=_parsed_raw_response(raw_response),
        )

    execution_elapsed_ms = max(0, int((monotonic_clock() - execution_started) * 1000))
    execution_started_at = str(attempts[0]["started_at"])
    execution_started_timestamp = legacy._parse_timestamp(execution_started_at)
    observed_last_completed = legacy._parse_timestamp(str(attempts[-1]["completed_at"]))
    computed_completed = execution_started_timestamp + timedelta(milliseconds=execution_elapsed_ms)
    execution_completed_timestamp = max(observed_last_completed, computed_completed)
    execution_elapsed_ms = int((execution_completed_timestamp - execution_started_timestamp).total_seconds() * 1000)
    execution_completed_at = legacy._timestamp(execution_completed_timestamp)
    execution = _execution_receipt(
        run_id=run_id,
        execution_origin=execution_origin,
        request_payload_sha256=payload_sha256,
        attempts=attempts,
        started_at=execution_started_at,
        completed_at=execution_completed_at,
        elapsed_ms=execution_elapsed_ms,
    )
    payloads: dict[str, Mapping[str, Any]] = {
        "request.json": request,
        "approval-receipt.json": approval,
        "catalog-receipt.json": catalog,
        "execution-receipt.json": execution,
    }
    if semantic_review is not None:
        payloads["semantic-review.json"] = semantic_review
    if raw_response is not None:
        payloads["raw-response.json"] = raw_response
    result = _build_result(
        run_id=run_id,
        request=request,
        approval=approval,
        catalog=catalog,
        execution=execution,
        raw_response=raw_response,
        semantic_review=semantic_review,
        payloads=payloads,
    )
    payloads["result.json"] = result
    artifact_root = legacy._write_bundle(runtime_root, run_id, payloads)
    return result, artifact_root


def run_luna_live_canary_v2(*, execute_live: bool) -> tuple[dict[str, Any], Path]:
    """Production-only entrypoint; transport, roots, clocks, and environment are not injectable."""

    if execute_live is not True:
        raise PermissionError("execute_live_required")
    key = os.environ.get(KEY_ENVIRONMENT_VARIABLE)
    if not legacy._valid_key(key):
        raise PermissionError("chshapi_api_key_missing_or_invalid")
    return _run_luna_live_canary_v2(
        lane=_PRODUCTION_LANE,
        key=key,
        http_client=UrllibHttpClient(),
        runtime_root=_runtime_root(),
        approval_root=_approval_root(),
        wall_clock=_utc_now,
        monotonic=time.monotonic,
    )


def run_luna_live_canary_v2_fixture(
    *,
    http_client: HttpClient,
    runtime_root: Path,
    approval_root: Path,
    wall_clock: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
) -> tuple[dict[str, Any], Path]:
    """Offline-only injected runner whose bundles are never formal/live evidence."""

    if http_client is None or isinstance(http_client, UrllibHttpClient):
        raise PermissionError("offline_fixture_network_transport_forbidden")
    return _run_luna_live_canary_v2(
        lane=_FIXTURE_LANE,
        key="sk-" + "f" * 32,
        http_client=http_client,
        runtime_root=runtime_root,
        approval_root=approval_root,
        wall_clock=wall_clock or (lambda: datetime.now(UTC)),
        monotonic=monotonic or time.monotonic,
    )


def _approval_valid(approval: Any, *, request: Mapping[str, Any], result: Mapping[str, Any]) -> bool:
    keys = {
        "schema_version",
        "execution_origin",
        "owner_id",
        "owner_approval_id",
        "probe_id",
        "semantic_request_schema_version",
        "semantic_review_schema_version",
        "semantic_adjudication_api_version",
        "semantic_adjudication_implementation_sha256",
        "request_sha256",
        "run_id",
        "consumed_at",
        "state",
    }
    run = result.get("run") if isinstance(result.get("run"), dict) else {}
    provenance = result.get("evidence_provenance") if isinstance(result.get("evidence_provenance"), dict) else {}
    execution_origin = provenance.get("execution_origin")
    try:
        probe_id, owner_id, owner_approval_id, run_id_pattern, _ = _origin_identity(execution_origin)
    except ValueError:
        return False
    return (
        isinstance(approval, dict)
        and set(approval) == keys
        and approval.get("schema_version") == APPROVAL_RECEIPT_SCHEMA_VERSION
        and approval.get("execution_origin") == execution_origin
        and approval.get("owner_id") == owner_id
        and approval.get("owner_approval_id") == owner_approval_id
        and approval.get("probe_id") == probe_id
        and approval.get("semantic_request_schema_version") == semantic.REQUEST_SCHEMA_VERSION
        and approval.get("semantic_review_schema_version") == semantic.REVIEW_SCHEMA_VERSION
        and approval.get("semantic_adjudication_api_version") == semantic.PURE_ADJUDICATION_API_VERSION
        and approval.get("semantic_adjudication_implementation_sha256")
        == semantic.PURE_ADJUDICATION_IMPLEMENTATION_SHA256
        and approval.get("request_sha256") == legacy._canonical_sha256(request)
        and approval.get("run_id") == run.get("run_id")
        and run_id_pattern.fullmatch(str(approval.get("run_id"))) is not None
        and approval.get("state") == "consumed_before_catalog"
        and isinstance(approval.get("consumed_at"), str)
        and _CANONICAL_TIME_RE.fullmatch(approval["consumed_at"]) is not None
    )


def validate_approval_receipt_contract_v2(
    approval: Any,
    *,
    request: Mapping[str, Any],
    result: Mapping[str, Any],
) -> list[str]:
    """Strict runtime mirror for the declarative consumed-approval schema."""

    try:
        valid = _approval_valid(approval, request=request, result=result)
    except Exception:  # noqa: BLE001 - arbitrary JSON-like values are untrusted
        valid = False
    return [] if valid else ["LUNA_LIVE_CANARY_V2_APPROVAL_RECEIPT_INVALID"]


def validate_execution_receipt_contract_v2(
    receipt: Any,
    *,
    run_id: str,
    execution_origin: str,
    request_payload_sha256: str,
) -> list[str]:
    """Project strict helper mirroring the declarative execution-receipt schema."""

    try:
        valid = _execution_receipt_valid(
            receipt,
            run_id=run_id,
            execution_origin=execution_origin,
            request_payload_sha256=request_payload_sha256,
        )
    except Exception:  # noqa: BLE001 - arbitrary JSON-like values are untrusted
        valid = False
    return [] if valid else ["LUNA_LIVE_CANARY_V2_EXECUTION_RECEIPT_INVALID"]


def _result_contract_valid(result: Any) -> bool:
    keys = {
        "schema_version",
        "status",
        "error_codes",
        "run",
        "evidence_provenance",
        "semantic_contract",
        "identity",
        "catalog",
        "semantic",
        "calls",
        "usage",
        "timing",
        "budgets",
        "retention",
        "response_received",
        "artifact_inventory",
        "artifact_sha256s",
        "approval_receipt_sha256",
        "execution_receipt_sha256",
        "authority",
    }
    if not isinstance(result, dict) or set(result) != keys or result.get("schema_version") != RESULT_SCHEMA_VERSION:
        return False
    status = result.get("status")
    errors = result.get("error_codes")
    if (
        status not in {"completed", "failed"}
        or not isinstance(errors, list)
        or len(errors) != (0 if status == "completed" else 1)
        or any(error not in _OUTER_ERROR_CODES for error in errors)
    ):
        return False
    provenance = result.get("evidence_provenance")
    if not isinstance(provenance, dict):
        return False
    execution_origin = provenance.get("execution_origin")
    try:
        probe_id, _, owner_approval_id, run_id_pattern, _ = _origin_identity(execution_origin)
    except ValueError:
        return False
    expected_provenance = {
        "execution_origin": execution_origin,
        "production_transport_observed": execution_origin == PRODUCTION_EXECUTION_ORIGIN,
        "formal_live_evidence_eligible": execution_origin == PRODUCTION_EXECUTION_ORIGIN,
        "caller_injection_used": execution_origin == FIXTURE_EXECUTION_ORIGIN,
    }
    run = result.get("run")
    if (
        provenance != expected_provenance
        or not isinstance(run, dict)
        or set(run) != {"run_id", "probe_id", "request_id", "request_sha256", "owner_approval_id"}
        or run_id_pattern.fullmatch(str(run.get("run_id"))) is None
        or run.get("probe_id") != probe_id
        or run.get("request_id") != "xbsv2r_444444444444444444444444"
        or not isinstance(run.get("request_sha256"), str)
        or _SHA256_RE.fullmatch(run["request_sha256"]) is None
        or run.get("owner_approval_id") != owner_approval_id
    ):
        return False
    expected_semantic_contract = {
        "request_schema_version": semantic.REQUEST_SCHEMA_VERSION,
        "review_schema_version": semantic.REVIEW_SCHEMA_VERSION,
        "prompt_version": semantic.PROMPT_VERSION,
        "prompt_sha256": semantic.CANONICAL_PROMPT_SHA256,
        "output_schema_version": semantic.MODEL_OUTPUT_SCHEMA_VERSION,
        "output_schema_sha256": semantic.CANONICAL_OUTPUT_SCHEMA_SHA256,
        "proxy_policy_version": semantic.PROXY_POLICY_VERSION,
        "proxy_policy_sha256": semantic.CANONICAL_PROXY_POLICY_SHA256,
        "pure_adjudication_api_version": semantic.PURE_ADJUDICATION_API_VERSION,
        "pure_adjudication_implementation_sha256": semantic.PURE_ADJUDICATION_IMPLEMENTATION_SHA256,
    }
    if result.get("semantic_contract") != expected_semantic_contract:
        return False
    identity = result.get("identity")
    if (
        not isinstance(identity, dict)
        or set(identity)
        != {
            "provider",
            "base_url",
            "requested_model",
            "returned_model",
            "exact_model_match",
            "fallback_used",
            "tools_used",
        }
        or identity.get("provider") != "chshapi_openai_compatible_relay"
        or identity.get("base_url") != BASE_URL
        or identity.get("requested_model") != MODEL_ID
        or identity.get("returned_model") != _normalized_returned_model(identity.get("returned_model"))
        or identity.get("exact_model_match") is not (identity.get("returned_model") == MODEL_ID)
        or identity.get("fallback_used") is not False
        or identity.get("tools_used") is not False
    ):
        return False
    catalog = result.get("catalog")
    if not isinstance(catalog, dict):
        return False
    catalog_receipt = {
        "schema_version": legacy.CATALOG_RECEIPT_SCHEMA_VERSION,
        "endpoint": CATALOG_URL,
        **catalog,
    }
    if not legacy._catalog_receipt_valid(catalog_receipt):
        return False
    semantic_summary = result.get("semantic")
    if (
        not isinstance(semantic_summary, dict)
        or set(semantic_summary) != {"review_status", "error_codes", "review_schema_version"}
        or semantic_summary.get("review_status") not in {"completed", "failed", None}
        or not isinstance(semantic_summary.get("error_codes"), list)
        or len(semantic_summary["error_codes"]) > 1
        or any(error not in semantic.ERROR_CODES for error in semantic_summary["error_codes"])
        or semantic_summary.get("review_schema_version") not in {semantic.REVIEW_SCHEMA_VERSION, None}
    ):
        return False
    calls = result.get("calls")
    if (
        not isinstance(calls, dict)
        or set(calls)
        != {"catalog_external_calls", "model_external_calls", "total_external_calls", "semantic_model_calls"}
        or any(not legacy._is_int(value) for value in calls.values())
        or calls["catalog_external_calls"] != 1
    ):
        return False
    usage = result.get("usage")
    if usage is not None:
        if (
            not isinstance(usage, dict)
            or set(usage) != {"input_tokens", "output_tokens", "total_tokens", "source", "billable"}
            or any(
                not legacy._is_int(usage.get(key)) or usage[key] < 0
                for key in ("input_tokens", "output_tokens", "total_tokens")
            )
            or usage["total_tokens"] != usage["input_tokens"] + usage["output_tokens"]
            or usage["input_tokens"] > semantic.BUDGETS["max_input_tokens"]
            or usage["output_tokens"] > semantic.BUDGETS["max_output_tokens"]
            or usage["total_tokens"] > semantic.BUDGETS["max_total_tokens"]
            or usage.get("source") != "provider_reported"
            or usage.get("billable") is not True
        ):
            return False
    timing = result.get("timing")
    retention = result.get("retention")
    if not isinstance(timing, dict) or not isinstance(retention, dict):
        return False
    try:
        started = legacy._parse_timestamp(timing["started_at"])
        completed = legacy._parse_timestamp(timing["completed_at"])
        delete_after = legacy._parse_timestamp(retention["delete_after"])
    except (KeyError, TypeError, ValueError):
        return False
    if (
        set(timing) != {"started_at", "completed_at", "elapsed_ms", "timeout_ms"}
        or not legacy._is_int(timing.get("elapsed_ms"))
        or timing["elapsed_ms"] < 0
        or timing.get("timeout_ms") != TOTAL_TIMEOUT_MS
        or completed < started
        or int((completed - started).total_seconds() * 1000) != timing["elapsed_ms"]
        or retention
        != {
            "class": "private_raw_evidence",
            "hours": RETENTION_HOURS,
            "delete_after": legacy._timestamp(completed + timedelta(hours=RETENTION_HOURS)),
            "deletion_status": "pending",
        }
        or delete_after != completed + timedelta(hours=RETENTION_HOURS)
        or result.get("budgets") != _BUDGETS
        or result.get("authority") != _AUTHORITY
    ):
        return False
    inventory = result.get("artifact_inventory")
    hashes = result.get("artifact_sha256s")
    base_inventory = [
        "approval-receipt.json",
        "catalog-receipt.json",
        "execution-receipt.json",
        "request.json",
        "result.json",
    ]
    transport_failure_inventory = [*base_inventory[:-1], "result.json", "semantic-review.json"]
    http_inventory = [*base_inventory[:3], "raw-response.json", *base_inventory[3:], "semantic-review.json"]
    if inventory not in (base_inventory, transport_failure_inventory, http_inventory):
        return False
    if (
        not isinstance(hashes, dict)
        or set(hashes) != set(inventory) - {"result.json"}
        or any(not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None for value in hashes.values())
        or type(result.get("response_received")) is not bool
    ):
        return False
    if inventory == base_inventory:
        expected_calls = {
            "catalog_external_calls": 1,
            "model_external_calls": 0,
            "total_external_calls": 1,
            "semantic_model_calls": 0,
        }
        if semantic_summary != {"review_status": None, "error_codes": [], "review_schema_version": None}:
            return False
        if usage is not None or result["response_received"] is not False:
            return False
    elif inventory == transport_failure_inventory:
        expected_calls = {
            "catalog_external_calls": 1,
            "model_external_calls": 1,
            "total_external_calls": 2,
            "semantic_model_calls": 1,
        }
        if semantic_summary != {
            "review_status": "failed",
            "error_codes": ["transport_failed"],
            "review_schema_version": semantic.REVIEW_SCHEMA_VERSION,
        }:
            return False
        if usage is not None or result["response_received"] is not False:
            return False
    else:
        expected_calls = {
            "catalog_external_calls": 1,
            "model_external_calls": 1,
            "total_external_calls": 2,
            "semantic_model_calls": 1,
        }
        if semantic_summary["review_schema_version"] != semantic.REVIEW_SCHEMA_VERSION:
            return False
        if result["response_received"] is not True:
            return False
    if calls != expected_calls:
        return False
    if status == "completed" and (
        identity["returned_model"] != MODEL_ID
        or semantic_summary
        != {
            "review_status": "completed",
            "error_codes": [],
            "review_schema_version": semantic.REVIEW_SCHEMA_VERSION,
        }
        or usage is None
        or inventory != http_inventory
    ):
        return False
    return True


def validate_result_shape_v2_non_authoritative(result: Any) -> list[str]:
    """Validate result shape only; receipt consistency requires artifact replay."""

    try:
        valid = _result_contract_valid(result)
    except Exception:  # noqa: BLE001 - arbitrary JSON-like values are untrusted
        valid = False
    return [] if valid else ["LUNA_LIVE_CANARY_V2_RESULT_INVALID"]


def _validate_artifact_directory_v2(
    directory: Path,
    *,
    execution_origin: str,
    approval_root: Path,
    now: datetime,
    allow_expired_pending: bool,
) -> list[str]:
    """Shared offline replay with an origin-specific owner trust root."""

    try:
        _, _, owner_approval_id, run_id_pattern, _ = _origin_identity(execution_origin)
        root = Path(directory)
        metadata = root.lstat()
        if (
            root.is_symlink()
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or run_id_pattern.fullmatch(root.name) is None
        ):
            raise ValueError("artifact_directory_unsafe")
        names = {entry.name for entry in root.iterdir()}
        base_names = {
            "request.json",
            "approval-receipt.json",
            "catalog-receipt.json",
            "execution-receipt.json",
            "result.json",
        }
        if not base_names <= names or names - base_names - {"raw-response.json", "semantic-review.json"}:
            raise ValueError("artifact_inventory_invalid")
        values = {name: legacy._read_private_json(root / name) for name in names}
        request = values["request.json"]
        approval = values["approval-receipt.json"]
        catalog = values["catalog-receipt.json"]
        execution = values["execution-receipt.json"]
        result = values["result.json"]
        if not isinstance(request, dict) or request != build_synthetic_live_request():
            raise ValueError("request_invalid")
        prompt, output_schema, proxy_policy = _semantic_assets()
        payload = semantic.build_responses_request(
            request,
            prompt=prompt,
            output_schema=output_schema,
            proxy_policy=proxy_policy,
        )
        payload_sha256 = hashlib.sha256(legacy._canonical_json(payload).encode("utf-8")).hexdigest()
        if not _result_contract_valid(result):
            raise ValueError("result_invalid")
        run = result.get("run") if isinstance(result.get("run"), dict) else {}
        provenance = result.get("evidence_provenance")
        expected_provenance = {
            "execution_origin": execution_origin,
            "production_transport_observed": execution_origin == PRODUCTION_EXECUTION_ORIGIN,
            "formal_live_evidence_eligible": execution_origin == PRODUCTION_EXECUTION_ORIGIN,
            "caller_injection_used": execution_origin == FIXTURE_EXECUTION_ORIGIN,
        }
        if provenance != expected_provenance:
            raise ValueError("execution_origin_invalid")
        if root.name != run.get("run_id") or run_id_pattern.fullmatch(root.name) is None:
            raise ValueError("run_invalid")
        if not _approval_valid(approval, request=request, result=result):
            raise ValueError("approval_invalid")
        ledger_root = approval_root
        ledger_metadata = ledger_root.lstat()
        if (
            ledger_root.is_symlink()
            or not stat.S_ISDIR(ledger_metadata.st_mode)
            or ledger_metadata.st_uid != os.getuid()
            or stat.S_IMODE(ledger_metadata.st_mode) != 0o700
        ):
            raise ValueError("approval_owner_unsafe")
        ledger = legacy._read_private_json(ledger_root / f"{owner_approval_id}.json")
        if ledger != approval:
            raise ValueError("approval_binding_invalid")
        if not _execution_receipt_valid(
            execution,
            run_id=root.name,
            execution_origin=execution_origin,
            request_payload_sha256=payload_sha256,
        ):
            raise ValueError("execution_receipt_invalid")
        if legacy._parse_timestamp(approval["consumed_at"]) > legacy._parse_timestamp(execution["started_at"]):
            raise ValueError("approval_order_invalid")
        if not legacy._catalog_receipt_valid(catalog):
            raise ValueError("catalog_invalid")
        first = execution["attempts"][0]
        if first["outcome"] == "transport_failure":
            if catalog.get("error_code") != "catalog_transport_failed":
                raise ValueError("catalog_attempt_binding_invalid")
        elif catalog.get("http_status") != first["http_status"]:
            raise ValueError("catalog_attempt_binding_invalid")

        raw_response = values.get("raw-response.json")
        semantic_review = values.get("semantic-review.json")
        if execution["attempt_count"] == 1:
            if raw_response is not None or semantic_review is not None:
                raise ValueError("response_inventory_invalid")
        else:
            response_attempt = execution["attempts"][1]
            if semantic_review is None:
                raise ValueError("semantic_inventory_invalid")
            if response_attempt["outcome"] == "transport_failure":
                if raw_response is not None:
                    raise ValueError("response_inventory_invalid")
            else:
                if not isinstance(raw_response, dict):
                    raise ValueError("response_inventory_invalid")
                if (
                    raw_response.get("body_sha256") != response_attempt["response_body_sha256"]
                    or raw_response.get("http_status") != response_attempt["http_status"]
                    or raw_response.get("content_type") != response_attempt["response_content_type"]
                ):
                    raise ValueError("response_attempt_binding_invalid")
            expected_review = _adjudicate_observed_response(
                request=request,
                prompt=prompt,
                output_schema=output_schema,
                proxy_policy=proxy_policy,
                attempt=response_attempt,
                parsed_response=_parsed_raw_response(raw_response),
            )
            if semantic_review != expected_review:
                raise ValueError("semantic_review_not_reproducible")

        hashes = result.get("artifact_sha256s")
        expected_hash_names = names - {"result.json"}
        if not isinstance(hashes, dict) or set(hashes) != expected_hash_names:
            raise ValueError("artifact_hash_inventory_invalid")
        for name in expected_hash_names:
            if hashes[name] != legacy._canonical_sha256(values[name]):
                raise ValueError("artifact_hash_invalid")
        expected_result = _build_result(
            run_id=root.name,
            request=request,
            approval=approval,
            catalog=catalog,
            execution=execution,
            raw_response=raw_response,
            semantic_review=semantic_review,
            payloads={name: value for name, value in values.items() if name != "result.json"},
        )
        if result != expected_result:
            raise ValueError("result_not_deterministically_bound")
        if result.get("approval_receipt_sha256") != legacy._canonical_sha256(approval):
            raise ValueError("approval_hash_invalid")
        if result.get("execution_receipt_sha256") != legacy._canonical_sha256(execution):
            raise ValueError("execution_hash_invalid")
        if result.get("authority") != _AUTHORITY:
            raise ValueError("authority_invalid")
        delete_after = legacy._parse_timestamp(result["retention"]["delete_after"])
        if now.tzinfo is None:
            raise ValueError("validation_time_timezone_required")
        current = now.astimezone(UTC)
        if not allow_expired_pending and current >= delete_after:
            raise ValueError("expired_pending_deletion")
        outer_errors = result.get("error_codes")
        if (
            not isinstance(outer_errors, list)
            or len(outer_errors) > 1
            or any(error not in _OUTER_ERROR_CODES for error in outer_errors)
        ):
            raise ValueError("outer_error_invalid")
    except Exception:  # noqa: BLE001 - validation diagnostics must not expose private evidence
        return ["LUNA_LIVE_CANARY_V2_ARTIFACT_INVALID"]
    return []


def validate_artifact_directory_v2(directory: Path) -> list[str]:
    """Validate only production-origin v2 evidence against the production owner ledger."""

    return _validate_artifact_directory_v2(
        directory,
        execution_origin=PRODUCTION_EXECUTION_ORIGIN,
        approval_root=_approval_root(),
        now=datetime.now(UTC),
        allow_expired_pending=False,
    )


def validate_artifact_directory_v2_fixture(
    directory: Path,
    *,
    approval_root: Path,
    now: datetime | None = None,
    allow_expired_pending: bool = False,
) -> list[str]:
    """Validate offline-fixture evidence; it can never be promoted as live evidence."""

    return _validate_artifact_directory_v2(
        directory,
        execution_origin=FIXTURE_EXECUTION_ORIGIN,
        approval_root=approval_root,
        now=now or datetime.now(UTC),
        allow_expired_pending=allow_expired_pending,
    )


def _create_private_json_exclusive(path: Path, payload: Mapping[str, Any]) -> None:
    if path.parent.is_symlink() or path.is_symlink():
        raise ValueError("deletion_receipt_path_unsafe")
    serialized = (json.dumps(payload, allow_nan=False, ensure_ascii=True, sort_keys=True) + "\n").encode()
    if len(serialized) > MAX_ARTIFACT_BYTES or legacy._KEY_BYTES_RE.search(serialized):
        raise ValueError("deletion_receipt_secret_or_size_violation")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(serialized)
        while view:
            view = view[os.write(descriptor, view) :]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    parent_descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(parent_descriptor)
    finally:
        os.close(parent_descriptor)


_DELETION_INVENTORIES = (
    [
        "approval-receipt.json",
        "catalog-receipt.json",
        "execution-receipt.json",
        "request.json",
        "result.json",
    ],
    [
        "approval-receipt.json",
        "catalog-receipt.json",
        "execution-receipt.json",
        "request.json",
        "result.json",
        "semantic-review.json",
    ],
    [
        "approval-receipt.json",
        "catalog-receipt.json",
        "execution-receipt.json",
        "raw-response.json",
        "request.json",
        "result.json",
        "semantic-review.json",
    ],
)


def _deletion_record_valid(
    receipt: Any,
    *,
    schema_version: str,
    state: str,
) -> bool:
    keys = {
        "schema_version",
        "execution_origin",
        "run_id",
        "bundle_digest_sha256",
        "result_sha256",
        "approval_receipt_sha256",
        "execution_receipt_sha256",
        "artifact_inventory",
        "delete_after",
        "deletion_started_at",
        "deletion_completed_at",
        "deletion_method",
        "state",
    }
    if not isinstance(receipt, dict) or set(receipt) != keys:
        return False
    try:
        _, _, _, run_id_pattern, _ = _origin_identity(receipt["execution_origin"])
        delete_after = legacy._parse_timestamp(receipt["delete_after"])
        started = legacy._parse_timestamp(receipt["deletion_started_at"])
    except (KeyError, TypeError, ValueError):
        return False
    digests = (
        receipt.get("bundle_digest_sha256"),
        receipt.get("result_sha256"),
        receipt.get("approval_receipt_sha256"),
        receipt.get("execution_receipt_sha256"),
    )
    if (
        receipt.get("schema_version") != schema_version
        or run_id_pattern.fullmatch(str(receipt.get("run_id"))) is None
        or any(
            not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None or value == "0" * 64 for value in digests
        )
        or receipt.get("artifact_inventory") not in _DELETION_INVENTORIES
        or receipt.get("deletion_method") != "atomic_owner_directory_rename_then_remove"
        or started < delete_after
        or receipt.get("state") != state
    ):
        return False
    if state == "deletion_started":
        return receipt.get("deletion_completed_at") is None
    if not isinstance(receipt.get("deletion_completed_at"), str):
        return False
    try:
        completed = legacy._parse_timestamp(receipt["deletion_completed_at"])
    except ValueError:
        return False
    return completed >= started


def _deletion_receipt_valid(receipt: Any) -> bool:
    return _deletion_record_valid(
        receipt,
        schema_version=DELETION_RECEIPT_SCHEMA_VERSION,
        state="deleted",
    )


def _deletion_journal_valid(receipt: Any) -> bool:
    return _deletion_record_valid(
        receipt,
        schema_version=DELETION_JOURNAL_SCHEMA_VERSION,
        state="deletion_started",
    )


def _private_owner_directory_valid(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except (FileNotFoundError, OSError):
        return False
    return (
        not path.is_symlink()
        and stat.S_ISDIR(metadata.st_mode)
        and metadata.st_uid == os.getuid()
        and stat.S_IMODE(metadata.st_mode) == 0o700
    )


def _private_owner_file_valid(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except (FileNotFoundError, OSError):
        return False
    return (
        not path.is_symlink()
        and stat.S_ISREG(metadata.st_mode)
        and metadata.st_uid == os.getuid()
        and stat.S_IMODE(metadata.st_mode) == 0o600
    )


def _approval_ledger_valid_for_deletion(
    approval: Any,
    *,
    execution_origin: str,
    run_id: str,
) -> bool:
    try:
        probe_id, owner_id, owner_approval_id, run_id_pattern, _ = _origin_identity(execution_origin)
    except ValueError:
        return False
    keys = {
        "schema_version",
        "execution_origin",
        "owner_id",
        "owner_approval_id",
        "probe_id",
        "semantic_request_schema_version",
        "semantic_review_schema_version",
        "semantic_adjudication_api_version",
        "semantic_adjudication_implementation_sha256",
        "request_sha256",
        "run_id",
        "consumed_at",
        "state",
    }
    return (
        isinstance(approval, dict)
        and set(approval) == keys
        and approval.get("schema_version") == APPROVAL_RECEIPT_SCHEMA_VERSION
        and approval.get("execution_origin") == execution_origin
        and approval.get("owner_id") == owner_id
        and approval.get("owner_approval_id") == owner_approval_id
        and approval.get("probe_id") == probe_id
        and approval.get("semantic_request_schema_version") == semantic.REQUEST_SCHEMA_VERSION
        and approval.get("semantic_review_schema_version") == semantic.REVIEW_SCHEMA_VERSION
        and approval.get("semantic_adjudication_api_version") == semantic.PURE_ADJUDICATION_API_VERSION
        and approval.get("semantic_adjudication_implementation_sha256")
        == semantic.PURE_ADJUDICATION_IMPLEMENTATION_SHA256
        and isinstance(approval.get("request_sha256"), str)
        and _SHA256_RE.fullmatch(approval["request_sha256"]) is not None
        and approval["request_sha256"] != "0" * 64
        and approval.get("run_id") == run_id
        and run_id_pattern.fullmatch(run_id) is not None
        and approval.get("state") == "consumed_before_catalog"
        and isinstance(approval.get("consumed_at"), str)
        and _CANONICAL_TIME_RE.fullmatch(approval["consumed_at"]) is not None
    )


def _validate_deletion_receipt_path(
    path: Path,
    *,
    execution_origin: str,
    runtime_root: Path,
    approval_root: Path,
    deletion_root: Path,
    allow_matching_journal_recovery: bool = False,
) -> list[str]:
    """Validate a tombstone against all surviving owner trust roots."""

    try:
        receipt_path = Path(path)
        runtime_owner = Path(runtime_root)
        approval_owner = Path(approval_root)
        deletion_owner = Path(deletion_root)
        if (
            receipt_path.parent != deletion_owner
            or not _private_owner_directory_valid(runtime_owner)
            or not _private_owner_directory_valid(approval_owner)
            or not _private_owner_directory_valid(deletion_owner)
            or not _private_owner_file_valid(receipt_path)
        ):
            raise ValueError("deletion_owner_invalid")
        receipt = legacy._read_private_json(receipt_path)
        if not _deletion_receipt_valid(receipt) or receipt.get("execution_origin") != execution_origin:
            raise ValueError("deletion_receipt_invalid")
        run_id = receipt["run_id"]
        if receipt_path.name != f"{run_id}.deletion.json":
            raise ValueError("deletion_receipt_name_invalid")
        _, _, owner_approval_id, _, _ = _origin_identity(execution_origin)
        approval_path = approval_owner / f"{owner_approval_id}.json"
        if not _private_owner_file_valid(approval_path):
            raise ValueError("approval_ledger_invalid")
        approval = legacy._read_private_json(approval_path)
        if not _approval_ledger_valid_for_deletion(
            approval,
            execution_origin=execution_origin,
            run_id=run_id,
        ) or receipt.get("approval_receipt_sha256") != legacy._canonical_sha256(approval):
            raise ValueError("approval_ledger_binding_invalid")
        bundle_path = runtime_owner / run_id
        deleting_path = runtime_owner / f".{run_id}.deleting"
        journal_path = deletion_owner / f"{run_id}.deletion.pending.json"
        if (
            bundle_path.exists()
            or bundle_path.is_symlink()
            or deleting_path.exists()
            or deleting_path.is_symlink()
            or (not allow_matching_journal_recovery and (journal_path.exists() or journal_path.is_symlink()))
        ):
            raise ValueError("deletion_not_terminal")
    except Exception:  # noqa: BLE001 - private tombstone diagnostics stay closed
        return ["LUNA_LIVE_CANARY_V2_DELETION_RECEIPT_INVALID"]
    return []


def validate_deletion_receipt_v2(path: Path) -> list[str]:
    """Validate a production tombstone only at canonical owner roots."""

    return _validate_deletion_receipt_path(
        path,
        execution_origin=PRODUCTION_EXECUTION_ORIGIN,
        runtime_root=_runtime_root(),
        approval_root=_approval_root(),
        deletion_root=_deletion_root(),
    )


def validate_deletion_receipt_v2_fixture(
    path: Path,
    *,
    runtime_root: Path,
    approval_root: Path,
    deletion_root: Path,
) -> list[str]:
    """Validate an offline-fixture tombstone without production promotion."""

    return _validate_deletion_receipt_path(
        path,
        execution_origin=FIXTURE_EXECUTION_ORIGIN,
        runtime_root=runtime_root,
        approval_root=approval_root,
        deletion_root=deletion_root,
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _deletion_record_from_bundle(
    *,
    root: Path,
    values: Mapping[str, Mapping[str, Any]],
    execution_origin: str,
    started_at: str,
) -> dict[str, Any]:
    file_hashes = {name: legacy._canonical_sha256(value) for name, value in sorted(values.items())}
    result = values["result.json"]
    return {
        "schema_version": DELETION_JOURNAL_SCHEMA_VERSION,
        "execution_origin": execution_origin,
        "run_id": root.name,
        "bundle_digest_sha256": legacy._canonical_sha256(file_hashes),
        "result_sha256": file_hashes["result.json"],
        "approval_receipt_sha256": file_hashes["approval-receipt.json"],
        "execution_receipt_sha256": file_hashes["execution-receipt.json"],
        "artifact_inventory": sorted(values),
        "delete_after": result["retention"]["delete_after"],
        "deletion_started_at": started_at,
        "deletion_completed_at": None,
        "deletion_method": "atomic_owner_directory_rename_then_remove",
        "state": "deletion_started",
    }


def _deletion_bindings_match(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    binding_keys = {
        "execution_origin",
        "run_id",
        "bundle_digest_sha256",
        "result_sha256",
        "approval_receipt_sha256",
        "execution_receipt_sha256",
        "artifact_inventory",
        "delete_after",
        "deletion_started_at",
        "deletion_method",
    }
    return all(left.get(key) == right.get(key) for key in binding_keys)


def _safe_deleting_directory(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return (
        not path.is_symlink()
        and stat.S_ISDIR(metadata.st_mode)
        and metadata.st_uid == os.getuid()
        and stat.S_IMODE(metadata.st_mode) == 0o700
    )


def _finish_existing_deletion_receipt(
    *,
    receipt_path: Path,
    journal_path: Path,
    root: Path,
    deleting_path: Path,
    execution_origin: str,
    approval_root: Path,
    deletion_root: Path,
) -> tuple[dict[str, Any], Path] | None:
    if not receipt_path.exists():
        return None
    receipt = legacy._read_private_json(receipt_path)
    if (
        _validate_deletion_receipt_path(
            receipt_path,
            execution_origin=execution_origin,
            runtime_root=root.parent,
            approval_root=approval_root,
            deletion_root=deletion_root,
            allow_matching_journal_recovery=True,
        )
        or receipt.get("run_id") != root.name
    ):
        raise ValueError("completed_deletion_receipt_state_invalid")
    if journal_path.exists() or journal_path.is_symlink():
        journal = legacy._read_private_json(journal_path)
        if not _deletion_journal_valid(journal) or not _deletion_bindings_match(journal, receipt):
            raise ValueError("deletion_journal_receipt_binding_invalid")
        journal_path.unlink()
        _fsync_directory(journal_path.parent)
    if _validate_deletion_receipt_path(
        receipt_path,
        execution_origin=execution_origin,
        runtime_root=root.parent,
        approval_root=approval_root,
        deletion_root=deletion_root,
    ):
        raise ValueError("completed_deletion_receipt_state_invalid")
    return receipt, receipt_path


def _purge_expired_artifact_directory_v2(
    directory: Path,
    *,
    execution_origin: str,
    approval_root: Path,
    deletion_root: Path,
    now: datetime,
) -> tuple[dict[str, Any], Path]:
    if now.tzinfo is None:
        raise ValueError("purge_time_timezone_required")
    current = now.astimezone(UTC)
    root = Path(directory)
    legacy._ensure_private_directory(deletion_root)
    receipt_path = deletion_root / f"{root.name}.deletion.json"
    journal_path = deletion_root / f"{root.name}.deletion.pending.json"
    deleting_path = root.parent / f".{root.name}.deleting"
    completed = _finish_existing_deletion_receipt(
        receipt_path=receipt_path,
        journal_path=journal_path,
        root=root,
        deleting_path=deleting_path,
        execution_origin=execution_origin,
        approval_root=approval_root,
        deletion_root=deletion_root,
    )
    if completed is not None:
        return completed

    if journal_path.exists() or journal_path.is_symlink():
        journal = legacy._read_private_json(journal_path)
        if (
            not _deletion_journal_valid(journal)
            or journal.get("execution_origin") != execution_origin
            or journal.get("run_id") != root.name
        ):
            raise ValueError("deletion_journal_invalid")
    else:
        validation = _validate_artifact_directory_v2(
            root,
            execution_origin=execution_origin,
            approval_root=approval_root,
            now=current,
            allow_expired_pending=True,
        )
        if validation:
            raise ValueError("artifact_invalid_for_purge")
        values = {entry.name: legacy._read_private_json(entry) for entry in root.iterdir()}
        delete_after = legacy._parse_timestamp(values["result.json"]["retention"]["delete_after"])
        if current < delete_after:
            raise PermissionError("artifact_retention_not_expired")
        execution = values["execution-receipt.json"]
        if execution.get("execution_origin") != execution_origin:
            raise ValueError("execution_origin_invalid")
        journal = _deletion_record_from_bundle(
            root=root,
            values=values,
            execution_origin=execution_origin,
            started_at=legacy._timestamp(current),
        )
        if not _deletion_journal_valid(journal):
            raise RuntimeError("deletion_journal_invalid")
        _create_private_json_exclusive(journal_path, journal)

    if current < legacy._parse_timestamp(journal["delete_after"]):
        raise PermissionError("artifact_retention_not_expired")
    if root.exists() or root.is_symlink():
        if deleting_path.exists() or deleting_path.is_symlink():
            raise ValueError("deleting_path_collision")
        validation = _validate_artifact_directory_v2(
            root,
            execution_origin=execution_origin,
            approval_root=approval_root,
            now=current,
            allow_expired_pending=True,
        )
        if validation:
            raise ValueError("artifact_invalid_for_purge")
        values = {entry.name: legacy._read_private_json(entry) for entry in root.iterdir()}
        observed = _deletion_record_from_bundle(
            root=root,
            values=values,
            execution_origin=execution_origin,
            started_at=journal["deletion_started_at"],
        )
        if not _deletion_bindings_match(journal, observed):
            raise ValueError("deletion_journal_bundle_binding_invalid")
        os.replace(root, deleting_path)
        _fsync_directory(root.parent)
    if deleting_path.exists() or deleting_path.is_symlink():
        if not _safe_deleting_directory(deleting_path):
            raise ValueError("deleting_path_unsafe")
        shutil.rmtree(deleting_path)
        _fsync_directory(root.parent)

    receipt = dict(journal)
    receipt["schema_version"] = DELETION_RECEIPT_SCHEMA_VERSION
    receipt["deletion_completed_at"] = legacy._timestamp(current)
    receipt["state"] = "deleted"
    if not _deletion_receipt_valid(receipt):
        raise RuntimeError("deletion_receipt_invalid")
    _create_private_json_exclusive(receipt_path, receipt)
    journal_path.unlink()
    _fsync_directory(deletion_root)
    if _validate_deletion_receipt_path(
        receipt_path,
        execution_origin=execution_origin,
        runtime_root=root.parent,
        approval_root=approval_root,
        deletion_root=deletion_root,
    ):
        raise RuntimeError("deletion_receipt_owner_binding_invalid")
    return receipt, receipt_path


def purge_expired_artifact_directory_v2(directory: Path) -> tuple[dict[str, Any], Path]:
    """Purge one expired production bundle using only production trust roots."""

    root = Path(directory)
    runtime_owner = _runtime_root()
    if root.parent != runtime_owner:
        raise PermissionError("production_runtime_owner_required")
    return _purge_expired_artifact_directory_v2(
        root,
        execution_origin=PRODUCTION_EXECUTION_ORIGIN,
        approval_root=_approval_root(),
        deletion_root=_deletion_root(),
        now=datetime.now(UTC),
    )


def purge_expired_artifact_directory_v2_fixture(
    directory: Path,
    *,
    approval_root: Path,
    deletion_root: Path,
    now: datetime,
) -> tuple[dict[str, Any], Path]:
    """Offline fixture purge path used only with explicitly injected private roots."""

    return _purge_expired_artifact_directory_v2(
        directory,
        execution_origin=FIXTURE_EXECUTION_ORIGIN,
        approval_root=approval_root,
        deletion_root=deletion_root,
        now=now,
    )


__all__ = [
    "APPROVAL_RECEIPT_SCHEMA_VERSION",
    "DELETION_JOURNAL_SCHEMA_VERSION",
    "DELETION_RECEIPT_SCHEMA_VERSION",
    "EXECUTION_RECEIPT_SCHEMA_VERSION",
    "FIXTURE_EXECUTION_ORIGIN",
    "HttpClient",
    "HttpResponse",
    "KEY_ENVIRONMENT_VARIABLE",
    "MODEL_ID",
    "OWNER_APPROVAL_ID",
    "ObservedHttpAttempt",
    "ObservedHttpTransport",
    "PRODUCTION_EXECUTION_ORIGIN",
    "RESULT_SCHEMA_VERSION",
    "UrllibHttpClient",
    "build_synthetic_live_request",
    "purge_expired_artifact_directory_v2",
    "run_luna_live_canary_v2",
    "validate_artifact_directory_v2",
    "validate_approval_receipt_contract_v2",
    "validate_deletion_receipt_v2",
    "validate_deletion_receipt_v2_fixture",
    "validate_result_shape_v2_non_authoritative",
]
