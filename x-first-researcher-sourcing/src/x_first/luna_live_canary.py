"""One-shot, fail-closed chshapi relay canary for the semantic-v2 fixture.

The production boundary in this module is deliberately small: one bounded
``GET /models`` followed by, only after an exact model handshake, at most one
bounded ``POST /responses``.  The semantic meaning of the request and response
continues to be owned by :mod:`x_first.profile_bio_semantic_v2`.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from urllib import error
from urllib import request as urllib_request

from . import profile_bio_semantic_v2 as semantic

RESULT_SCHEMA_VERSION = "x.profile.bio_semantic.live_canary.result.v1"
CATALOG_RECEIPT_SCHEMA_VERSION = "x.profile.bio_semantic.live_canary.catalog_receipt.v1"
RAW_RESPONSE_SCHEMA_VERSION = "x.profile.bio_semantic.live_canary.raw_response.v1"
APPROVAL_RECEIPT_SCHEMA_VERSION = "x.profile.bio_semantic.live_canary.approval.v1"

BASE_URL = "https://api.chshapi.org/v1"
CATALOG_URL = f"{BASE_URL}/models"
RESPONSES_URL = f"{BASE_URL}/responses"
MODEL_ID = "gpt-5.6-luna"
KEY_ENVIRONMENT_VARIABLE = "CHSHAPI_API_KEY"
PROBE_ID = "profile_bio_semantic_v2_chshapi_luna_one_shot_v1"
OWNER_ID = "user_state:x-first-researcher-sourcing/luna-live-approvals/v1"
OWNER_APPROVAL_ID = "user_approved_2026_07_14_chshapi_luna_one_shot_v1"

TOTAL_TIMEOUT_MS = 30_000
MAX_CATALOG_BYTES = 262_144
MAX_RESPONSE_BYTES = 1_048_576
MAX_JSON_DEPTH = 64
MAX_JSON_NODES = 16_384
MAX_JSON_LEAVES = 12_288
MAX_CATALOG_MODELS = 4_096
MAX_MODEL_ID_CHARACTERS = 160
RETENTION_HOURS = 24
MAX_ARTIFACT_BYTES = 2_000_000

_KEY_RE = re.compile(r"sk-[A-Za-z0-9_-]{16,252}")
_KEY_BYTES_RE = re.compile(rb"sk-[A-Za-z0-9_-]{16,252}")
_RUN_ID_RE = re.compile(r"luna_canary_run_[0-9a-f]{32}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_CANONICAL_TIME_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")

_AUTHORITY = {
    "discovery_or_ranking_authorized": False,
    "eligibility_decided": False,
    "canonical_employment_confirmed": False,
    "canonical_write_authorized": False,
    "outreach_authorized": False,
}
_BUDGETS = {
    "timeout_ms": TOTAL_TIMEOUT_MS,
    "max_catalog_bytes": MAX_CATALOG_BYTES,
    "max_response_bytes": MAX_RESPONSE_BYTES,
    "max_json_depth": MAX_JSON_DEPTH,
    "max_json_nodes": MAX_JSON_NODES,
    "max_json_leaves": MAX_JSON_LEAVES,
    "max_catalog_external_calls": 1,
    "max_model_external_calls": 1,
    "max_total_external_calls": 2,
    "max_semantic_model_calls": 1,
}
_OUTER_ERROR_CODES = {
    "catalog_transport_failed",
    "catalog_redirect_rejected",
    "catalog_http_failed",
    "catalog_invalid",
    "catalog_model_missing",
    "total_timeout_exceeded",
    "response_transport_failed",
    "response_redirect_rejected",
    "response_http_failed",
    "response_secret_detected",
    "response_model_mismatch",
    "semantic_review_failed",
}


@dataclass(frozen=True)
class HttpResponse:
    """Bounded HTTP response returned by an injected client."""

    status_code: int
    headers: Mapping[str, str]
    body: bytes


class HttpClient(Protocol):
    """Only I/O seam used by the canary; tests inject a deterministic fake."""

    def request(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout_ms: int,
        max_response_bytes: int,
    ) -> HttpResponse: ...


class _NoRedirectHandler(urllib_request.HTTPRedirectHandler):
    def redirect_request(  # type: ignore[override]
        self,
        req: urllib_request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


class UrllibHttpClient:
    """Stdlib transport with redirects disabled and response bytes bounded."""

    @staticmethod
    def _read_bounded(stream: Any, maximum: int) -> bytes:
        value = stream.read(maximum + 1)
        if len(value) > maximum:
            raise ValueError("response_byte_budget_exceeded")
        return value

    def request(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout_ms: int,
        max_response_bytes: int,
    ) -> HttpResponse:
        if url not in {CATALOG_URL, RESPONSES_URL} or method not in {"GET", "POST"}:
            raise ValueError("closed_http_route_required")
        opener = urllib_request.build_opener(_NoRedirectHandler())
        outbound = urllib_request.Request(url=url, data=body, headers=dict(headers), method=method)
        try:
            with opener.open(outbound, timeout=timeout_ms / 1000) as response:
                payload = self._read_bounded(response, max_response_bytes)
                return HttpResponse(
                    status_code=response.status,
                    headers={"content-type": response.headers.get("Content-Type", "")},
                    body=payload,
                )
        except error.HTTPError as exc:
            try:
                payload = self._read_bounded(exc, max_response_bytes)
            finally:
                exc.close()
            return HttpResponse(
                status_code=exc.code,
                headers={"content-type": exc.headers.get("Content-Type", "") if exc.headers else ""},
                body=payload,
            )
        except (error.URLError, TimeoutError, OSError) as exc:
            raise RuntimeError("bounded_http_transport_failed") from exc


@dataclass
class _ReplayTransport:
    response: Any = None
    failure: bool = False
    is_live: bool = True
    transport_id: str = semantic.PROVIDER_ID
    calls: int = 0

    def create_response(self, payload: Mapping[str, Any], *, timeout_ms: int) -> Mapping[str, Any]:
        del payload, timeout_ms
        self.calls += 1
        if self.failure:
            raise RuntimeError("replayed_transport_failure")
        return self.response


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _runtime_root() -> Path:
    return project_root() / "runtime/luna-live-canaries"


def _approval_root() -> Path:
    return Path.home() / ".local/state/x-first-researcher-sourcing/luna-live-approvals"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _strict_json_loads(value: bytes | str) -> Any:
    def duplicate_guard(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("duplicate_json_key")
            result[key] = item
        return result

    def finite_guard(token: str) -> Any:
        raise ValueError(f"non_finite_json_number:{token}")

    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="strict")
    return json.loads(value, object_pairs_hook=duplicate_guard, parse_constant=finite_guard)


def _scan_json(value: Any) -> str | None:
    stack: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    leaves = 0
    while stack:
        item, depth = stack.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES:
            return "json_node_budget_exceeded"
        if isinstance(item, (dict, list)):
            if item and depth >= MAX_JSON_DEPTH:
                return "json_depth_budget_exceeded"
            children = list(item.values()) if isinstance(item, dict) else item
            stack.extend((child, depth + 1) for child in children)
        else:
            leaves += 1
            if leaves > MAX_JSON_LEAVES:
                return "json_leaf_budget_exceeded"
    return None


def _timestamp(value: datetime | None = None) -> str:
    current = (value or datetime.now(UTC)).astimezone(UTC)
    return current.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_timestamp(value: str) -> datetime:
    if _CANONICAL_TIME_RE.fullmatch(value) is None:
        raise ValueError("timestamp_not_canonical")
    parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    if _timestamp(parsed) != value:
        raise ValueError("timestamp_not_canonical")
    return parsed


def _semantic_assets() -> tuple[dict[str, Any], dict[str, Any]]:
    prompt = semantic.load_json(project_root() / "configs/profile_bio_semantic_prompt.v2.json")
    output_schema = semantic.load_json(project_root() / "contracts/x.profile.bio_semantic.model_output.v2.schema.json")
    if semantic.validate_prompt(prompt) or semantic.validate_output_schema(output_schema):
        raise RuntimeError("semantic_v2_assets_invalid")
    return prompt, output_schema


def build_synthetic_live_request() -> dict[str, Any]:
    """Build the pinned synthetic ``.invalid`` request through semantic v2."""

    prompt, output_schema = _semantic_assets()
    request = copy.deepcopy(semantic.load_json(project_root() / "fixtures/profile_bio_semantic_request_v2.json"))
    request["request_id"] = "xbsv2r_333333333333333333333333"
    request["model_execution_mode"] = "live_canary"
    if semantic.validate_request(request, prompt=prompt, output_schema=output_schema):
        raise RuntimeError("semantic_v2_request_invalid")
    return request


def _valid_key(value: Any) -> bool:
    return isinstance(value, str) and _KEY_RE.fullmatch(value) is not None


def _ensure_private_directory(path: Path) -> None:
    if path.is_symlink():
        raise ValueError("private_directory_unsafe")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = path.stat()
    if not path.is_dir() or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise ValueError("private_directory_unsafe")


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    if path.parent.is_symlink() or path.is_symlink():
        raise ValueError("artifact_path_unsafe")
    serialized = (json.dumps(payload, allow_nan=False, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()
    if len(serialized) > MAX_ARTIFACT_BYTES or _KEY_BYTES_RE.search(serialized):
        raise ValueError("artifact_secret_or_size_violation")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _consume_approval(
    root: Path,
    *,
    request: Mapping[str, Any],
    run_id: str,
    consumed_at: str,
) -> dict[str, Any]:
    _ensure_private_directory(root)
    receipt = {
        "schema_version": APPROVAL_RECEIPT_SCHEMA_VERSION,
        "owner_id": OWNER_ID,
        "owner_approval_id": OWNER_APPROVAL_ID,
        "probe_id": PROBE_ID,
        "request_sha256": _canonical_sha256(request),
        "run_id": run_id,
        "consumed_at": consumed_at,
        "state": "consumed_before_catalog",
    }
    path = root / f"{OWNER_APPROVAL_ID}.json"
    serialized = (json.dumps(receipt, allow_nan=False, ensure_ascii=True, sort_keys=True) + "\n").encode()
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise PermissionError("owner_approved_luna_canary_already_consumed") from exc
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(serialized)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    parent_descriptor = os.open(root, os.O_RDONLY)
    try:
        os.fsync(parent_descriptor)
    finally:
        os.close(parent_descriptor)
    return receipt


def _remaining_ms(started: float, monotonic: Callable[[], float]) -> int:
    elapsed = max(0, int((monotonic() - started) * 1000))
    return max(0, TOTAL_TIMEOUT_MS - elapsed)


def _content_type(headers: Mapping[str, str]) -> str:
    for key, value in headers.items():
        if str(key).casefold() == "content-type" and isinstance(value, str):
            if len(value) <= 200 and value.isascii() and all(ord(character) >= 32 for character in value):
                return value
    return ""


def _validate_http_response(value: Any, maximum: int) -> HttpResponse:
    if not isinstance(value, HttpResponse):
        raise ValueError("http_response_invalid")
    if (
        not isinstance(value.status_code, int)
        or isinstance(value.status_code, bool)
        or not 100 <= value.status_code <= 599
        or not isinstance(value.headers, Mapping)
        or not isinstance(value.body, bytes)
        or len(value.body) > maximum
    ):
        raise ValueError("http_response_invalid")
    return value


def _catalog_receipt(
    *,
    status: str,
    http_status: int | None,
    error_code: str | None,
    model_count: int | None = None,
    distinct_model_count: int | None = None,
    model_ids_sha256: str | None = None,
    exact_model_matches: int | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": CATALOG_RECEIPT_SCHEMA_VERSION,
        "endpoint": CATALOG_URL,
        "status": status,
        "http_status": http_status,
        "error_code": error_code,
        "model_count": model_count,
        "distinct_model_count": distinct_model_count,
        "model_ids_sha256": model_ids_sha256,
        "exact_model_matches": exact_model_matches,
    }


def _parse_catalog(response: HttpResponse) -> tuple[dict[str, Any], str | None]:
    if 300 <= response.status_code <= 399:
        return _catalog_receipt(
            status="failed", http_status=response.status_code, error_code="catalog_redirect_rejected"
        ), "catalog_redirect_rejected"
    if response.status_code != 200:
        return _catalog_receipt(
            status="failed", http_status=response.status_code, error_code="catalog_http_failed"
        ), "catalog_http_failed"
    if "application/json" not in _content_type(response.headers).casefold():
        return _catalog_receipt(
            status="failed", http_status=response.status_code, error_code="catalog_invalid"
        ), "catalog_invalid"
    try:
        payload = _strict_json_loads(response.body)
    except (UnicodeError, ValueError, RecursionError):
        return _catalog_receipt(
            status="failed", http_status=response.status_code, error_code="catalog_invalid"
        ), "catalog_invalid"
    if _scan_json(payload) or not isinstance(payload, dict) or payload.get("object") != "list":
        return _catalog_receipt(
            status="failed", http_status=response.status_code, error_code="catalog_invalid"
        ), "catalog_invalid"
    data = payload.get("data")
    if not isinstance(data, list) or not 1 <= len(data) <= MAX_CATALOG_MODELS:
        return _catalog_receipt(
            status="failed", http_status=response.status_code, error_code="catalog_invalid"
        ), "catalog_invalid"
    model_ids: list[str] = []
    for item in data:
        if not isinstance(item, dict):
            return _catalog_receipt(
                status="failed", http_status=response.status_code, error_code="catalog_invalid"
            ), "catalog_invalid"
        model_id = item.get("id")
        if (
            not isinstance(model_id, str)
            or not 1 <= len(model_id) <= MAX_MODEL_ID_CHARACTERS
            or not model_id.isascii()
            or any(ord(character) < 33 for character in model_id)
        ):
            return _catalog_receipt(
                status="failed", http_status=response.status_code, error_code="catalog_invalid"
            ), "catalog_invalid"
        model_ids.append(model_id)
    exact_matches = model_ids.count(MODEL_ID)
    receipt = _catalog_receipt(
        status="completed" if exact_matches == 1 else "failed",
        http_status=200,
        error_code=None if exact_matches == 1 else "catalog_model_missing",
        model_count=len(model_ids),
        distinct_model_count=len(set(model_ids)),
        model_ids_sha256=_canonical_sha256(sorted(model_ids)),
        exact_model_matches=exact_matches,
    )
    return receipt, None if exact_matches == 1 else "catalog_model_missing"


def _catalog_receipt_valid(catalog: Any) -> bool:
    expected_keys = {
        "schema_version",
        "endpoint",
        "status",
        "http_status",
        "error_code",
        "model_count",
        "distinct_model_count",
        "model_ids_sha256",
        "exact_model_matches",
    }
    if (
        not isinstance(catalog, dict)
        or set(catalog) != expected_keys
        or catalog.get("schema_version") != CATALOG_RECEIPT_SCHEMA_VERSION
        or catalog.get("endpoint") != CATALOG_URL
    ):
        return False
    status = catalog.get("status")
    http_status = catalog.get("http_status")
    error_code = catalog.get("error_code")
    model_count = catalog.get("model_count")
    distinct_model_count = catalog.get("distinct_model_count")
    digest = catalog.get("model_ids_sha256")
    exact_matches = catalog.get("exact_model_matches")
    metrics_are_null = all(value is None for value in (model_count, distinct_model_count, digest, exact_matches))
    metrics_are_valid = (
        _is_int(model_count)
        and 1 <= model_count <= MAX_CATALOG_MODELS
        and _is_int(distinct_model_count)
        and 1 <= distinct_model_count <= model_count
        and isinstance(digest, str)
        and _SHA256_RE.fullmatch(digest) is not None
        and _is_int(exact_matches)
        and 0 <= exact_matches <= model_count
    )
    if status == "completed":
        return http_status == 200 and error_code is None and metrics_are_valid and exact_matches == 1
    if status != "failed":
        return False
    if error_code == "catalog_transport_failed":
        return http_status is None and metrics_are_null
    if error_code == "catalog_redirect_rejected":
        return _is_int(http_status) and 300 <= http_status <= 399 and metrics_are_null
    if error_code == "catalog_http_failed":
        return (
            _is_int(http_status)
            and 100 <= http_status <= 599
            and http_status != 200
            and not 300 <= http_status <= 399
            and metrics_are_null
        )
    if error_code == "catalog_invalid":
        return http_status == 200 and metrics_are_null
    if error_code == "catalog_model_missing":
        return http_status == 200 and metrics_are_valid and exact_matches != 1
    return False


def _raw_response_receipt(response: HttpResponse, key: str) -> dict[str, Any]:
    content_type = _content_type(response.headers)
    content_type_bytes = content_type.encode("ascii", errors="ignore")
    secret_present = (
        key.encode() in response.body
        or _KEY_BYTES_RE.search(response.body) is not None
        or key.encode() in content_type_bytes
        or _KEY_BYTES_RE.search(content_type_bytes) is not None
    )
    return {
        "schema_version": RAW_RESPONSE_SCHEMA_VERSION,
        "endpoint": RESPONSES_URL,
        "http_status": response.status_code,
        "content_type": "" if secret_present else content_type,
        "body_sha256": hashlib.sha256(response.body).hexdigest(),
        "body_base64": None if secret_present else base64.b64encode(response.body).decode("ascii"),
        "secret_redacted": secret_present,
    }


def _decode_raw_response(receipt: Mapping[str, Any]) -> bytes | None:
    encoded = receipt.get("body_base64")
    if receipt.get("secret_redacted") is True:
        if encoded is not None:
            raise ValueError("raw_response_invalid")
        return None
    if not isinstance(encoded, str):
        raise ValueError("raw_response_invalid")
    try:
        body = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ValueError("raw_response_invalid") from exc
    if len(body) > MAX_RESPONSE_BYTES or hashlib.sha256(body).hexdigest() != receipt.get("body_sha256"):
        raise ValueError("raw_response_invalid")
    return body


def _semantic_review_from_response(
    request: Mapping[str, Any],
    prompt: Mapping[str, Any],
    output_schema: Mapping[str, Any],
    response: HttpResponse,
    raw_receipt: Mapping[str, Any],
) -> tuple[dict[str, Any], Any | None]:
    parsed: Any | None = None
    transport_failure = (
        response.status_code != 200
        or raw_receipt["secret_redacted"] is True
        or "application/json" not in raw_receipt["content_type"].casefold()
    )
    if not transport_failure:
        try:
            parsed = _strict_json_loads(response.body)
        except (UnicodeError, ValueError, RecursionError):
            transport_failure = True
        else:
            if _scan_json(parsed):
                transport_failure = True
                parsed = None
    transport = _ReplayTransport(response=parsed, failure=transport_failure)
    review = semantic.run_semantic_review(
        request,
        prompt=prompt,
        output_schema=output_schema,
        transport=transport,
        execute_live=True,
    )
    if transport.calls != 1:
        raise RuntimeError("semantic_v2_call_accounting_invalid")
    return review, parsed


def _safe_model_string(value: Any) -> str | None:
    if (
        isinstance(value, str)
        and 1 <= len(value) <= MAX_MODEL_ID_CHARACTERS
        and value.isascii()
        and all(ord(character) >= 33 for character in value)
    ):
        return value
    return None


def _build_result(
    *,
    run_id: str,
    request: Mapping[str, Any],
    approval: Mapping[str, Any],
    catalog: Mapping[str, Any],
    raw_response: Mapping[str, Any] | None,
    semantic_review: Mapping[str, Any] | None,
    error_code: str | None,
    catalog_calls: int,
    model_calls: int,
    started_at: str,
    completed_at: str,
    elapsed_ms: int,
    payloads: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    returned_model: str | None = None
    response_id: str | None = None
    response_status: str | None = None
    semantic_status: str | None = None
    semantic_errors: list[str] = []
    usage: dict[str, Any] | None = None
    if semantic_review is not None:
        semantic_status = semantic_review.get("status") if isinstance(semantic_review.get("status"), str) else None
        semantic_errors = [value for value in semantic_review.get("error_codes", []) if isinstance(value, str)]
        receipt = semantic_review.get("model_receipt")
        if isinstance(receipt, dict):
            returned_model = _safe_model_string(receipt.get("model_id"))
            response_id = receipt.get("response_id") if isinstance(receipt.get("response_id"), str) else None
            response_status = (
                receipt.get("response_status") if isinstance(receipt.get("response_status"), str) else None
            )
        if isinstance(semantic_review.get("usage"), dict):
            usage = copy.deepcopy(semantic_review["usage"])
    if returned_model is None and raw_response is not None and raw_response.get("secret_redacted") is False:
        try:
            body = _decode_raw_response(raw_response)
            parsed = _strict_json_loads(body or b"")
        except (UnicodeError, ValueError, RecursionError):
            parsed = None
        if isinstance(parsed, dict):
            returned_model = _safe_model_string(parsed.get("model"))
            response_id = parsed.get("id") if isinstance(parsed.get("id"), str) and len(parsed["id"]) <= 128 else None
            response_status = (
                parsed.get("status") if isinstance(parsed.get("status"), str) and len(parsed["status"]) <= 32 else None
            )
    status = "completed" if error_code is None and semantic_status == "completed" else "failed"
    inventory = sorted([*payloads, "result.json"])
    expires = _timestamp(_parse_timestamp(completed_at) + timedelta(hours=RETENTION_HOURS))
    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "status": status,
        "error_codes": [] if status == "completed" else [error_code or "semantic_review_failed"],
        "run": {
            "run_id": run_id,
            "probe_id": PROBE_ID,
            "request_id": request["request_id"],
            "request_sha256": _canonical_sha256(request),
            "owner_approval_id": OWNER_APPROVAL_ID,
        },
        "identity": {
            "provider": "chshapi_openai_compatible_relay",
            "base_url": BASE_URL,
            "catalog_endpoint": CATALOG_URL,
            "responses_endpoint": RESPONSES_URL,
            "requested_model": MODEL_ID,
            "returned_model": returned_model,
            "exact_model_match": returned_model == MODEL_ID,
            "fallback_used": False,
            "tools_used": False,
            "response_id": response_id,
            "response_status": response_status,
        },
        "catalog": {
            "status": catalog["status"],
            "http_status": catalog["http_status"],
            "model_count": catalog["model_count"],
            "distinct_model_count": catalog["distinct_model_count"],
            "model_ids_sha256": catalog["model_ids_sha256"],
            "exact_model_matches": catalog["exact_model_matches"],
            "error_code": catalog["error_code"],
        },
        "semantic": {
            "review_status": semantic_status,
            "error_codes": semantic_errors,
            "review_schema_version": semantic.REVIEW_SCHEMA_VERSION if semantic_review is not None else None,
        },
        "calls": {
            "catalog_external_calls": catalog_calls,
            "model_external_calls": model_calls,
            "total_external_calls": catalog_calls + model_calls,
            "semantic_model_calls": (
                semantic_review.get("execution", {}).get("provider_external_calls", 0)
                if isinstance(semantic_review, dict)
                else 0
            ),
        },
        "usage": usage,
        "timing": {
            "started_at": started_at,
            "completed_at": completed_at,
            "elapsed_ms": elapsed_ms,
            "timeout_ms": TOTAL_TIMEOUT_MS,
        },
        "budgets": dict(_BUDGETS),
        "retention": {
            "class": "private_raw_evidence",
            "hours": RETENTION_HOURS,
            "delete_after": expires,
            "deletion_status": "pending",
        },
        "response_received": raw_response is not None,
        "artifact_inventory": inventory,
        "artifact_sha256s": {name: _canonical_sha256(value) for name, value in sorted(payloads.items())},
        "approval_receipt_sha256": _canonical_sha256(approval),
        "authority": dict(_AUTHORITY),
    }


def _write_bundle(root: Path, run_id: str, payloads: Mapping[str, Mapping[str, Any]]) -> Path:
    _ensure_private_directory(root)
    final = root / run_id
    if final.exists() or final.is_symlink():
        raise ValueError("artifact_bundle_exists")
    staging = root / f".{run_id}.{uuid.uuid4().hex}.tmp"
    staging.mkdir(mode=0o700)
    try:
        for name, value in payloads.items():
            _atomic_write_json(staging / name, value)
        staging_descriptor = os.open(staging, os.O_RDONLY)
        try:
            os.fsync(staging_descriptor)
        finally:
            os.close(staging_descriptor)
        os.replace(staging, final)
        descriptor = os.open(root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return final


def run_luna_live_canary(
    *,
    execute_live: bool,
    http_client: HttpClient | None = None,
    environ: Mapping[str, str] | None = None,
    runtime_root: Path | None = None,
    approval_root: Path | None = None,
    wall_clock: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
) -> tuple[dict[str, Any], Path]:
    """Consume the fixed approval and execute at most two external calls."""

    if execute_live is not True:
        raise PermissionError("execute_live_required")
    environment = os.environ if environ is None else environ
    key = environment.get(KEY_ENVIRONMENT_VARIABLE)
    if not _valid_key(key):
        raise PermissionError("chshapi_api_key_missing_or_invalid")

    prompt, output_schema = _semantic_assets()
    synthetic_request = build_synthetic_live_request()
    payload = semantic.build_responses_request(synthetic_request, prompt=prompt, output_schema=output_schema)
    if payload.get("model") != MODEL_ID or payload.get("tools") != [] or payload.get("store") is not False:
        raise RuntimeError("semantic_v2_payload_policy_invalid")

    current_time = wall_clock or (lambda: datetime.now(UTC))
    monotonic_clock = monotonic or time.monotonic
    run_id = f"luna_canary_run_{uuid.uuid4().hex}"
    artifact_owner = runtime_root or _runtime_root()
    # Fail before consuming the one-shot approval if the durable evidence
    # owner is already unsafe at the directory boundary.
    _ensure_private_directory(artifact_owner)
    consumed_at = _timestamp(current_time())
    approval = _consume_approval(
        approval_root or _approval_root(),
        request=synthetic_request,
        run_id=run_id,
        consumed_at=consumed_at,
    )
    started_at = _timestamp(current_time())
    started_monotonic = monotonic_clock()
    client = http_client or UrllibHttpClient()
    catalog_calls = 0
    model_calls = 0
    semantic_review: dict[str, Any] | None = None
    raw_response: dict[str, Any] | None = None
    error_code: str | None = None

    catalog_calls += 1
    try:
        remaining = _remaining_ms(started_monotonic, monotonic_clock)
        if remaining <= 0:
            raise TimeoutError("canary_deadline_exhausted")
        catalog_http = _validate_http_response(
            client.request(
                method="GET",
                url=CATALOG_URL,
                headers={"Accept": "application/json", "Authorization": f"Bearer {key}"},
                body=None,
                timeout_ms=remaining,
                max_response_bytes=MAX_CATALOG_BYTES,
            ),
            MAX_CATALOG_BYTES,
        )
    except Exception:  # noqa: BLE001 - provider and credential details never escape
        catalog = _catalog_receipt(status="failed", http_status=None, error_code="catalog_transport_failed")
        error_code = "catalog_transport_failed"
    else:
        catalog, error_code = _parse_catalog(catalog_http)

    if error_code is None:
        remaining = _remaining_ms(started_monotonic, monotonic_clock)
        if remaining <= 0:
            error_code = "total_timeout_exceeded"
        else:
            model_calls += 1
            try:
                encoded_payload = _canonical_json(payload).encode("utf-8")
                response_http = _validate_http_response(
                    client.request(
                        method="POST",
                        url=RESPONSES_URL,
                        headers={
                            "Accept": "application/json",
                            "Authorization": f"Bearer {key}",
                            "Content-Type": "application/json",
                        },
                        body=encoded_payload,
                        timeout_ms=remaining,
                        max_response_bytes=MAX_RESPONSE_BYTES,
                    ),
                    MAX_RESPONSE_BYTES,
                )
            except Exception:  # noqa: BLE001 - provider and credential details never escape
                error_code = "response_transport_failed"
            else:
                raw_response = _raw_response_receipt(response_http, key)
                try:
                    semantic_review, parsed_response = _semantic_review_from_response(
                        synthetic_request,
                        prompt,
                        output_schema,
                        response_http,
                        raw_response,
                    )
                except Exception:  # noqa: BLE001 - public semantic drift also terminates with a closed review
                    semantic_review = semantic.run_semantic_review(
                        synthetic_request,
                        prompt=prompt,
                        output_schema=output_schema,
                        transport=_ReplayTransport(failure=True),
                        execute_live=True,
                    )
                    parsed_response = None
                    error_code = "semantic_review_failed"
                if error_code is None:
                    if 300 <= response_http.status_code <= 399:
                        error_code = "response_redirect_rejected"
                    elif response_http.status_code != 200:
                        error_code = "response_http_failed"
                    elif raw_response["secret_redacted"]:
                        error_code = "response_secret_detected"
                    elif isinstance(parsed_response, dict) and parsed_response.get("model") != MODEL_ID:
                        error_code = "response_model_mismatch"
                    elif semantic_review["status"] != "completed":
                        error_code = "semantic_review_failed"

    elapsed_ms = max(0, int((monotonic_clock() - started_monotonic) * 1000))
    if elapsed_ms > TOTAL_TIMEOUT_MS:
        error_code = "total_timeout_exceeded"
    completed_at = _timestamp(_parse_timestamp(started_at) + timedelta(milliseconds=elapsed_ms))
    payloads: dict[str, Mapping[str, Any]] = {
        "request.json": synthetic_request,
        "approval-receipt.json": approval,
        "catalog-receipt.json": catalog,
    }
    if raw_response is not None and semantic_review is not None:
        payloads["raw-response.json"] = raw_response
        payloads["semantic-review.json"] = semantic_review
    result = _build_result(
        run_id=run_id,
        request=synthetic_request,
        approval=approval,
        catalog=catalog,
        raw_response=raw_response,
        semantic_review=semantic_review,
        error_code=error_code,
        catalog_calls=catalog_calls,
        model_calls=model_calls,
        started_at=started_at,
        completed_at=completed_at,
        elapsed_ms=elapsed_ms,
        payloads=payloads,
    )
    payloads["result.json"] = result
    artifact_root = _write_bundle(artifact_owner, run_id, payloads)
    return result, artifact_root


def _read_private_json(path: Path) -> Any:
    metadata = path.lstat()
    if (
        path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or not 1 <= metadata.st_size <= MAX_ARTIFACT_BYTES
    ):
        raise ValueError("private_artifact_unsafe")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (
            opened.st_dev != metadata.st_dev
            or opened.st_ino != metadata.st_ino
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or not stat.S_ISREG(opened.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or opened.st_size != metadata.st_size
        ):
            raise ValueError("private_artifact_changed")
        chunks: list[bytes] = []
        remaining = metadata.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1_048_576))
            if not chunk:
                raise ValueError("private_artifact_changed")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ValueError("private_artifact_changed")
    finally:
        os.close(descriptor)
    payload = b"".join(chunks)
    if _KEY_BYTES_RE.search(payload):
        raise ValueError("private_artifact_contains_secret")
    return _strict_json_loads(payload)


def _approval_valid(
    approval: Any,
    *,
    request: Mapping[str, Any],
    result: Mapping[str, Any],
) -> bool:
    expected_keys = {
        "schema_version",
        "owner_id",
        "owner_approval_id",
        "probe_id",
        "request_sha256",
        "run_id",
        "consumed_at",
        "state",
    }
    run = result.get("run") if isinstance(result.get("run"), dict) else {}
    return (
        isinstance(approval, dict)
        and set(approval) == expected_keys
        and approval["schema_version"] == APPROVAL_RECEIPT_SCHEMA_VERSION
        and approval["owner_id"] == OWNER_ID
        and approval["owner_approval_id"] == OWNER_APPROVAL_ID
        and approval["probe_id"] == PROBE_ID
        and approval["request_sha256"] == _canonical_sha256(request)
        and approval["run_id"] == run.get("run_id")
        and approval["state"] == "consumed_before_catalog"
        and isinstance(approval["consumed_at"], str)
        and _CANONICAL_TIME_RE.fullmatch(approval["consumed_at"]) is not None
    )


def _result_shape_valid(result: Any, request: Mapping[str, Any], names: set[str]) -> bool:
    if not isinstance(result, dict):
        return False
    expected_keys = {
        "schema_version",
        "status",
        "error_codes",
        "run",
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
        "authority",
    }
    if set(result) != expected_keys or result["schema_version"] != RESULT_SCHEMA_VERSION:
        return False
    run = result.get("run")
    identity = result.get("identity")
    calls = result.get("calls")
    timing = result.get("timing")
    retention = result.get("retention")
    if not all(isinstance(value, dict) for value in (run, identity, calls, timing, retention)):
        return False
    if (
        _RUN_ID_RE.fullmatch(str(run.get("run_id"))) is None
        or run.get("probe_id") != PROBE_ID
        or run.get("request_id") != request["request_id"]
        or run.get("request_sha256") != _canonical_sha256(request)
        or run.get("owner_approval_id") != OWNER_APPROVAL_ID
        or identity.get("provider") != "chshapi_openai_compatible_relay"
        or identity.get("base_url") != BASE_URL
        or identity.get("catalog_endpoint") != CATALOG_URL
        or identity.get("responses_endpoint") != RESPONSES_URL
        or identity.get("requested_model") != MODEL_ID
        or identity.get("fallback_used") is not False
        or identity.get("tools_used") is not False
        or result.get("budgets") != _BUDGETS
        or result.get("authority") != _AUTHORITY
        or sorted(result.get("artifact_inventory", [])) != sorted(names)
    ):
        return False
    catalog_calls = calls.get("catalog_external_calls")
    model_calls = calls.get("model_external_calls")
    semantic_calls = calls.get("semantic_model_calls")
    if (
        not all(
            _is_int(value) for value in (catalog_calls, model_calls, semantic_calls, calls.get("total_external_calls"))
        )
        or catalog_calls != 1
        or model_calls not in {0, 1}
        or calls.get("total_external_calls") != catalog_calls + model_calls
        or calls["total_external_calls"] > 2
        or semantic_calls not in {0, 1}
        or semantic_calls > model_calls
    ):
        return False
    if (
        timing.get("timeout_ms") != TOTAL_TIMEOUT_MS
        or not _is_int(timing.get("elapsed_ms"))
        or timing["elapsed_ms"] < 0
    ):
        return False
    try:
        started = _parse_timestamp(timing["started_at"])
        completed = _parse_timestamp(timing["completed_at"])
        delete_after = _parse_timestamp(retention["delete_after"])
    except (KeyError, TypeError, ValueError):
        return False
    if (
        completed < started
        or int((completed - started).total_seconds() * 1000) != timing["elapsed_ms"]
        or retention
        != {
            "class": "private_raw_evidence",
            "hours": RETENTION_HOURS,
            "delete_after": _timestamp(completed + timedelta(hours=RETENTION_HOURS)),
            "deletion_status": "pending",
        }
        or delete_after != completed + timedelta(hours=RETENTION_HOURS)
    ):
        return False
    if type(result.get("response_received")) is not bool:
        return False
    completed_status = result.get("status") == "completed"
    if timing["elapsed_ms"] > TOTAL_TIMEOUT_MS and result.get("error_codes") != ["total_timeout_exceeded"]:
        return False
    return (
        result.get("status") in {"completed", "failed"}
        and isinstance(result.get("error_codes"), list)
        and (not result["error_codes"] if completed_status else len(result["error_codes"]) == 1)
        and identity.get("exact_model_match") is (identity.get("returned_model") == MODEL_ID)
        and (not completed_status or identity.get("exact_model_match") is True)
    )


def validate_artifact_directory(directory: Path, *, approval_root: Path | None = None) -> list[str]:
    """Validate a completed or failed bundle without credentials or network."""

    try:
        root = Path(directory)
        metadata = root.lstat()
        if (
            root.is_symlink()
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or _RUN_ID_RE.fullmatch(root.name) is None
        ):
            raise ValueError("artifact_directory_unsafe")
        names = {entry.name for entry in root.iterdir()}
        base_names = {"request.json", "approval-receipt.json", "catalog-receipt.json", "result.json"}
        response_names = {"raw-response.json", "semantic-review.json"}
        if names != base_names and names != base_names | response_names:
            raise ValueError("artifact_inventory_invalid")
        values = {name: _read_private_json(root / name) for name in names}
        request = values["request.json"]
        approval = values["approval-receipt.json"]
        catalog = values["catalog-receipt.json"]
        result = values["result.json"]
        if not isinstance(request, dict) or request != build_synthetic_live_request():
            raise ValueError("request_invalid")
        if root.name != result.get("run", {}).get("run_id") or not _result_shape_valid(result, request, names):
            raise ValueError("result_invalid")
        if not _approval_valid(approval, request=request, result=result):
            raise ValueError("approval_invalid")
        if _parse_timestamp(approval["consumed_at"]) > _parse_timestamp(result["timing"]["started_at"]):
            raise ValueError("approval_order_invalid")
        ledger_root = approval_root or _approval_root()
        ledger_metadata = ledger_root.lstat()
        if (
            ledger_root.is_symlink()
            or not stat.S_ISDIR(ledger_metadata.st_mode)
            or stat.S_IMODE(ledger_metadata.st_mode) != 0o700
        ):
            raise ValueError("approval_owner_unsafe")
        ledger = _read_private_json(ledger_root / f"{OWNER_APPROVAL_ID}.json")
        if ledger != approval or result["approval_receipt_sha256"] != _canonical_sha256(approval):
            raise ValueError("approval_binding_invalid")
        hashes = result.get("artifact_sha256s")
        expected_hash_names = names - {"result.json"}
        if not isinstance(hashes, dict) or set(hashes) != expected_hash_names:
            raise ValueError("artifact_hash_inventory_invalid")
        for name in expected_hash_names:
            if hashes[name] != _canonical_sha256(values[name]):
                raise ValueError("artifact_hash_invalid")
        expected_response_files = result["response_received"] is True
        if expected_response_files != (response_names <= names):
            raise ValueError("response_inventory_invalid")
        if not isinstance(catalog, dict) or result["catalog"] != {
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
        }:
            raise ValueError("catalog_binding_invalid")
        if not _catalog_receipt_valid(catalog):
            raise ValueError("catalog_invalid")
        parsed: Any = None
        if expected_response_files:
            raw = values["raw-response.json"]
            review = values["semantic-review.json"]
            if (
                not isinstance(raw, dict)
                or set(raw)
                != {
                    "schema_version",
                    "endpoint",
                    "http_status",
                    "content_type",
                    "body_sha256",
                    "body_base64",
                    "secret_redacted",
                }
                or raw.get("schema_version") != RAW_RESPONSE_SCHEMA_VERSION
                or raw.get("endpoint") != RESPONSES_URL
                or _SHA256_RE.fullmatch(str(raw.get("body_sha256"))) is None
                or not _is_int(raw.get("http_status"))
                or not 100 <= raw["http_status"] <= 599
                or not isinstance(raw.get("content_type"), str)
                or len(raw["content_type"]) > 200
                or not raw["content_type"].isascii()
                or any(ord(character) < 32 for character in raw["content_type"])
                or type(raw.get("secret_redacted")) is not bool
                or (raw["secret_redacted"] is True and raw.get("body_base64") is not None)
            ):
                raise ValueError("raw_response_invalid")
            body = _decode_raw_response(raw)
            replay_failure = (
                body is None
                or raw.get("http_status") != 200
                or "application/json" not in str(raw.get("content_type", "")).casefold()
            )
            parsed: Any = None
            if not replay_failure:
                try:
                    parsed = _strict_json_loads(body or b"")
                except (UnicodeError, ValueError, RecursionError):
                    replay_failure = True
                else:
                    if _scan_json(parsed):
                        replay_failure = True
                        parsed = None
            prompt, output_schema = _semantic_assets()
            if isinstance(review, dict) and review.get("status") == "completed":
                if replay_failure or semantic.validate_review(
                    review,
                    request=request,
                    prompt=prompt,
                    output_schema=output_schema,
                    raw_response=parsed,
                ):
                    raise ValueError("semantic_review_invalid")
            else:
                expected = semantic.run_semantic_review(
                    request,
                    prompt=prompt,
                    output_schema=output_schema,
                    transport=_ReplayTransport(response=parsed, failure=replay_failure),
                    execute_live=True,
                )
                if review != expected:
                    raise ValueError("semantic_failure_not_reproducible")
            execution = review.get("execution", {}) if isinstance(review, dict) else {}
            if result["calls"]["semantic_model_calls"] != execution.get("provider_external_calls"):
                raise ValueError("semantic_call_accounting_invalid")
            if result["semantic"] != {
                "review_status": review.get("status"),
                "error_codes": review.get("error_codes"),
                "review_schema_version": semantic.REVIEW_SCHEMA_VERSION,
            } or result["usage"] != review.get("usage"):
                raise ValueError("semantic_result_binding_invalid")
        elif result["calls"]["semantic_model_calls"] != 0:
            raise ValueError("semantic_call_accounting_invalid")
        outer_errors = result.get("error_codes")
        if (
            not isinstance(outer_errors, list)
            or any(value not in _OUTER_ERROR_CODES for value in outer_errors)
            or len(outer_errors) > 1
        ):
            raise ValueError("outer_error_invalid")
        if catalog.get("status") == "failed" and result["calls"]["model_external_calls"] != 0:
            raise ValueError("model_call_accounting_invalid")
        if (
            catalog.get("status") == "completed"
            and result["calls"]["model_external_calls"] == 0
            and result["timing"]["elapsed_ms"] < TOTAL_TIMEOUT_MS
        ):
            raise ValueError("model_call_accounting_invalid")
        if result["timing"]["elapsed_ms"] > TOTAL_TIMEOUT_MS:
            derived_error = "total_timeout_exceeded"
        elif catalog.get("status") != "completed":
            derived_error = catalog.get("error_code")
        elif result["calls"]["model_external_calls"] == 0:
            derived_error = "total_timeout_exceeded"
        elif not expected_response_files:
            derived_error = "response_transport_failed"
        elif 300 <= raw["http_status"] <= 399:
            derived_error = "response_redirect_rejected"
        elif raw["http_status"] != 200:
            derived_error = "response_http_failed"
        elif raw["secret_redacted"] is True:
            derived_error = "response_secret_detected"
        elif isinstance(parsed, dict) and parsed.get("model") != MODEL_ID:
            derived_error = "response_model_mismatch"
        elif review.get("status") != "completed":
            derived_error = "semantic_review_failed"
        else:
            derived_error = None
        if outer_errors != ([] if derived_error is None else [derived_error]):
            raise ValueError("outer_terminal_state_invalid")
        expected_result = _build_result(
            run_id=result["run"]["run_id"],
            request=request,
            approval=approval,
            catalog=catalog,
            raw_response=values.get("raw-response.json"),
            semantic_review=values.get("semantic-review.json"),
            error_code=derived_error,
            catalog_calls=result["calls"]["catalog_external_calls"],
            model_calls=result["calls"]["model_external_calls"],
            started_at=result["timing"]["started_at"],
            completed_at=result["timing"]["completed_at"],
            elapsed_ms=result["timing"]["elapsed_ms"],
            payloads={name: value for name, value in values.items() if name != "result.json"},
        )
        if result != expected_result:
            raise ValueError("result_not_deterministically_bound")
    except Exception:  # noqa: BLE001 - validation diagnostics must not expose retained evidence
        return ["LUNA_LIVE_CANARY_ARTIFACT_INVALID"]
    return []


__all__ = [
    "BASE_URL",
    "CATALOG_URL",
    "HttpClient",
    "HttpResponse",
    "KEY_ENVIRONMENT_VARIABLE",
    "MODEL_ID",
    "OWNER_APPROVAL_ID",
    "RESPONSES_URL",
    "UrllibHttpClient",
    "build_synthetic_live_request",
    "run_luna_live_canary",
    "validate_artifact_directory",
]
