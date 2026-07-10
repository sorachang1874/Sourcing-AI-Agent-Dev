#!/usr/bin/env python3
"""Guarded CRM Public Web live/product validation runner.

The default mode is a dry run: it validates the entrypoint, writes a report, and
does not call the backend or any external provider. Live execution requires
explicit CLI and environment confirmation so product validation cannot quietly
become a provider-costing smoke path.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin
from urllib.request import Request, urlopen

from sourcing_agent.runtime_asset_retention_prune import validate_independent_review_artifact

EXPECTED_PUBLIC_WEB_MODEL = "gpt-5.6-sol"
EXPECTED_OWNER = "crm_public_web_v1"
REPO_ROOT = Path(__file__).resolve().parents[1]
INDEPENDENT_REVIEW_ARTIFACT_ROOT = REPO_ROOT / "runtime" / "reviews"
PRE_AGENT_CONTRACT_PASSED_ENV = "CRM_PUBLIC_WEB_LIVE_PRE_AGENT_CONTRACT_PASSED"
INDEPENDENT_REVIEW_PASSED_ENV = "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_PASSED"
INDEPENDENT_REVIEW_ARTIFACT_ENV = "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT"
INDEPENDENT_REVIEW_SCOPE_TOKENS_ENV = "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_SCOPE_TOKENS"
INDEPENDENT_REVIEW_REQUIRED_FILES_ENV = "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_REQUIRED_FILES"
INDEPENDENT_REVIEW_SCOPE_DIGEST_ENV = "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_SCOPE_DIGEST_SHA256"
EXPECTED_INDEPENDENT_REVIEW_TITLE = "W7g CRM Public Web live product validation"
MINIMUM_INDEPENDENT_REVIEW_SCOPE_TOKENS = ("W7g", "CRM Public Web", "live")
MINIMUM_INDEPENDENT_REVIEW_REQUIRED_FILES = (
    "Makefile",
    "docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md",
    "docs/INDEPENDENT_REVIEW_GATE.md",
    "docs/PRE_AGENT_CONTRACT_REVIEW.md",
    "docs/TESTING_PLAYBOOK.md",
    "scripts/run_crm_public_web_live_product_validation.py",
    "scripts/run_independent_review_gate.py",
    "src/sourcing_agent/model_provider.py",
    "src/sourcing_agent/public_web_runtime_core.py",
    "src/sourcing_agent/runtime_asset_retention_prune.py",
    "tests/test_crm_public_web_runtime_boundary.py",
    "tests/test_independent_review_gate_runner.py",
    "tests/test_model_provider.py",
    "tests/test_pre_agent_contract_review.py",
)
CANONICAL_ENDPOINTS = {
    "provider_health": "/api/providers/health",
    "start": "/api/crm/records/public-web-search",
    "poll": "/api/crm/records/public-web-search/poll",
    "detail": "/api/crm/records/{crm_record_id}/public-web-search",
    "export": "/api/crm/records/public-web-export",
}
LEGACY_ENDPOINT_MARKER = "/api/target-candidates"
LEGACY_OWNER_MARKER = "target_candidate_public_web_v1"
RAW_PAYLOAD_MARKERS = {
    "raw_html",
    "raw_pdf",
    "raw_payload",
    "search_payload",
    "provider_payload",
}
TERMINAL_RUN_STATUSES = {
    "completed",
    "completed_with_errors",
    "needs_review",
    "failed",
    "cancelled",
    "canceled",
}
MODEL_VERIFIABLE_RUN_STATUSES = {
    "completed",
    "completed_with_errors",
    "needs_review",
}
LIVE_VALIDATION_FAILED_RUN_STATUSES = {
    "failed",
    "cancelled",
    "canceled",
}
LEGACY_REENABLE_ENVS = {
    "SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS",
    "SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_TO_CRM_SYNC",
}
DISALLOWED_LIVE_PROVIDER_MODES = {"scripted", "simulate", "simulation", "replay", "fake", "mock"}


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _artifact_path(value: str) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def _path_is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _review_verdict_line(line: str) -> str:
    normalized = line.strip()
    normalized = re.sub(r"^[-*]\s+", "", normalized)
    normalized = normalized.strip("` ").upper()
    if re.match(r"^NO-GO\b", normalized):
        return "no_go"
    if re.match(r"^GO\b", normalized):
        return "go"
    return ""


def _dedupe_nonempty(values: list[str] | tuple[str, ...]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _csv_environment_additions(name: str) -> list[str]:
    raw = str(os.environ.get(name) or "").strip()
    return [item.strip() for item in raw.split(",") if item.strip()]


def _required_independent_review_scope_tokens() -> list[str]:
    return _dedupe_nonempty(
        [*MINIMUM_INDEPENDENT_REVIEW_SCOPE_TOKENS, *_csv_environment_additions(INDEPENDENT_REVIEW_SCOPE_TOKENS_ENV)]
    )


def _required_independent_review_files() -> list[str]:
    return _dedupe_nonempty(
        [*MINIMUM_INDEPENDENT_REVIEW_REQUIRED_FILES, *_csv_environment_additions(INDEPENDENT_REVIEW_REQUIRED_FILES_ENV)]
    )


def _required_independent_review_scope_digest() -> str:
    return str(os.environ.get(INDEPENDENT_REVIEW_SCOPE_DIGEST_ENV) or "").strip()


def _missing_review_metadata_fields(text: str) -> list[str]:
    required_markers = [
        "- title:",
        "- base/ref:",
        "- reviewer_model:",
        "- reviewer_reasoning_effort:",
        "- reviewer_service_tier:",
        "- reviewer_config_path:",
        "- reviewer_config_sha256:",
        "- reviewer_exit_code:",
        "- reviewer_codex_cli_version:",
        "- reviewer_thread_id:",
        "- reviewer_rollout_path:",
        "- reviewer_rollout_sha256:",
        "- reviewer_effective_config_path:",
        "- reviewer_effective_config_sha256:",
        "- review_scope_digest_sha256:",
        "- timeout_seconds:",
        "- prompt_path:",
        "- command:",
        "- contract_docs_considered:",
        "- author_validation:",
        "Reviewed scope:",
    ]
    return [marker for marker in required_markers if marker not in text]


def _validate_independent_review_artifact(value: str) -> dict[str, Any]:
    artifact = str(value or "").strip()
    scope_tokens = _required_independent_review_scope_tokens()
    required_files = _required_independent_review_files()
    expected_scope_digest = _required_independent_review_scope_digest()
    result: dict[str, Any] = {
        "artifact": artifact,
        "exists": False,
        "valid_go": False,
        "verdict": "missing",
        "reason": "",
        "expected_title": EXPECTED_INDEPENDENT_REVIEW_TITLE,
        "expected_scope_digest_sha256": expected_scope_digest,
        "required_scope_tokens": scope_tokens,
        "required_files": required_files,
    }
    if not artifact:
        result["reason"] = f"{INDEPENDENT_REVIEW_ARTIFACT_ENV} is empty"
        return result

    path = _artifact_path(artifact)
    result["resolved_path"] = str(path)
    if not path.exists():
        result["reason"] = f"independent review artifact does not exist: {artifact}"
        return result
    result["exists"] = True

    if path.name.endswith(".prompt.md"):
        result["verdict"] = "invalid"
        result["reason"] = "independent review artifact points to a prompt file, not reviewer output"
        return result
    if not _path_is_under(path, INDEPENDENT_REVIEW_ARTIFACT_ROOT):
        result["verdict"] = "invalid"
        result["reason"] = "independent review artifact must be under runtime/reviews"
        return result

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        result["verdict"] = "invalid"
        result["reason"] = f"independent review artifact could not be read: {exc}"
        return result

    stripped = text.strip()
    if not stripped:
        result["verdict"] = "invalid"
        result["reason"] = "independent review artifact is empty"
        return result
    if "## Review Metadata" not in text or "## Reviewer Output" not in text:
        result["verdict"] = "invalid"
        result["reason"] = "independent review artifact is missing runner metadata"
        return result
    missing_metadata = _missing_review_metadata_fields(text)
    if missing_metadata:
        result["verdict"] = "invalid"
        result["reason"] = "independent review artifact is missing metadata fields: " + ", ".join(missing_metadata)
        return result
    if not re.fullmatch(r"[0-9a-f]{64}", expected_scope_digest):
        result["verdict"] = "invalid"
        result["reason"] = f"{INDEPENDENT_REVIEW_SCOPE_DIGEST_ENV} must be a nonempty lowercase SHA-256 digest"
        return result
    text_lower = text.lower()
    missing_scope_tokens = [token for token in scope_tokens if token.lower() not in text_lower]
    if missing_scope_tokens:
        result["verdict"] = "invalid"
        result["reason"] = (
            "independent review artifact does not match required live validation scope tokens: "
            + ", ".join(missing_scope_tokens)
        )
        return result
    missing_required_files = [item for item in required_files if item.lower() not in text_lower]
    if missing_required_files:
        result["verdict"] = "invalid"
        result["reason"] = "independent review artifact does not cover required live validation files: " + ", ".join(
            missing_required_files
        )
        return result
    if "INVALID_REVIEW_ARTIFACT" in text:
        result["verdict"] = "invalid"
        result["reason"] = "independent review artifact was marked invalid by the review runner"
        return result
    if "independent review timed out" in text.lower() or "reviewer produced no output" in text.lower():
        result["verdict"] = "invalid"
        result["reason"] = "independent review artifact records timeout or no-output failure"
        return result

    verdict = ""
    for line in reversed([item for item in text.splitlines() if item.strip()]):
        verdict = _review_verdict_line(line)
        if verdict:
            break
    result["verdict"] = verdict or "invalid"
    if verdict == "go":
        evidence_blockers = validate_independent_review_artifact(
            artifact_path=path,
            workspace_root=REPO_ROOT,
            expected_title=EXPECTED_INDEPENDENT_REVIEW_TITLE,
            expected_scope_digest=expected_scope_digest,
            required_files=required_files,
            required_tokens=scope_tokens,
        )
        if evidence_blockers:
            result["verdict"] = "invalid"
            result["reason"] = "independent review artifact failed durable evidence validation: " + ", ".join(
                evidence_blockers
            )
            result["evidence_blockers"] = evidence_blockers
            return result
        result["valid_go"] = True
        return result
    if verdict == "no_go":
        result["reason"] = "independent review artifact verdict is NO-GO"
        return result
    result["reason"] = "independent review artifact has no final GO verdict"
    return result


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _contains_marker(value: Any, marker: str) -> bool:
    if isinstance(value, dict):
        return any(_contains_marker(key, marker) or _contains_marker(item, marker) for key, item in value.items())
    if isinstance(value, list):
        return any(_contains_marker(item, marker) for item in value)
    return marker in str(value)


def _collect_field_values(value: Any, field_names: set[str]) -> list[str]:
    matches: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key) in field_names:
                matches.append(str(item))
            matches.extend(_collect_field_values(item, field_names))
    elif isinstance(value, list):
        for item in value:
            matches.extend(_collect_field_values(item, field_names))
    return matches


def _request_json(method: str, base_url: str, endpoint: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    url = urljoin(base_url.rstrip("/") + "/", endpoint.lstrip("/"))
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=body, method=method, headers=headers)
    try:
        with urlopen(request, timeout=60) as response:
            response_body = response.read()
            if not response_body:
                return {"status": "ok", "http_status": response.status}
            payload = json.loads(response_body.decode("utf-8"))
            if isinstance(payload, dict):
                payload.setdefault("http_status", response.status)
                return payload
            return {"status": "invalid_response", "http_status": response.status, "body": payload}
    except HTTPError as exc:
        try:
            error_payload = json.loads(exc.read().decode("utf-8"))
        except Exception:
            error_payload = {"status": "http_error", "body": str(exc)}
        error_payload["http_status"] = exc.code
        return error_payload
    except URLError as exc:
        return {"status": "connection_error", "reason": str(exc.reason)}


def _http_request_failed(response: dict[str, Any]) -> bool:
    status = response.get("http_status")
    try:
        if status is not None and int(status) >= 400:
            return True
    except (TypeError, ValueError):
        return True
    return str(response.get("status") or "").strip().lower() in {
        "connection_error",
        "http_error",
        "invalid_response",
        "not_found",
    }


def _append_http_failure(name: str, response: dict[str, Any], failures: list[str]) -> bool:
    if not _http_request_failed(response):
        return False
    status = response.get("http_status")
    reason = response.get("reason") or response.get("status") or response.get("body") or "unknown_error"
    failures.append(f"{name}: endpoint returned non-success response http_status={status or ''} reason={reason}")
    return True


def _request_bytes(method: str, base_url: str, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = urljoin(base_url.rstrip("/") + "/", endpoint.lstrip("/"))
    request = Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method=method,
        headers={"Accept": "application/zip,application/json", "Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=120) as response:
            body = response.read()
            return {
                "status": "ok",
                "http_status": response.status,
                "content_type": response.headers.get("Content-Type", ""),
                "content_length": len(body),
                "headers": {
                    "X-Sourcing-Export-Record-Count": response.headers.get("X-Sourcing-Export-Record-Count", ""),
                    "X-Sourcing-Exported-Record-Count": response.headers.get("X-Sourcing-Exported-Record-Count", ""),
                    "X-Sourcing-Exported-Signal-Count": response.headers.get("X-Sourcing-Exported-Signal-Count", ""),
                    "X-Sourcing-Non-Terminal-Run-Count": response.headers.get("X-Sourcing-Non-Terminal-Run-Count", ""),
                },
            }
    except HTTPError as exc:
        try:
            error_payload = json.loads(exc.read().decode("utf-8"))
        except Exception:
            error_payload = {"status": "http_error", "body": str(exc)}
        error_payload["http_status"] = exc.code
        return error_payload
    except URLError as exc:
        return {"status": "connection_error", "reason": str(exc.reason)}


def _runs_are_terminal(payload: dict[str, Any]) -> bool:
    runs = [item for item in list(payload.get("runs") or []) if isinstance(item, dict)]
    if not runs:
        return False
    return all(str(run.get("status") or "").strip().lower() in TERMINAL_RUN_STATUSES for run in runs)


def _run_summary(run: dict[str, Any]) -> str:
    crm_record_id = str(run.get("crm_record_id") or run.get("record_id") or "").strip()
    run_id = str(run.get("run_id") or "").strip()
    status = str(run.get("status") or "").strip() or "unknown"
    parts = [part for part in (crm_record_id, run_id, status) if part]
    return ":".join(parts) if parts else "unknown"


def _nonterminal_run_summaries(payload: dict[str, Any]) -> list[str]:
    runs = [item for item in list(payload.get("runs") or []) if isinstance(item, dict)]
    return [
        _run_summary(run) for run in runs if str(run.get("status") or "").strip().lower() not in TERMINAL_RUN_STATUSES
    ]


def _failed_terminal_run_summaries(payload: dict[str, Any]) -> list[str]:
    runs = [item for item in list(payload.get("runs") or []) if isinstance(item, dict)]
    return [
        _run_summary(run)
        for run in runs
        if str(run.get("status") or "").strip().lower() in LIVE_VALIDATION_FAILED_RUN_STATUSES
    ]


def _failed_terminal_batch_summaries(payload: dict[str, Any]) -> list[str]:
    batches = [item for item in list(payload.get("batches") or []) if isinstance(item, dict)]
    return [
        ":".join(
            part
            for part in (
                str(batch.get("batch_id") or "").strip(),
                str(batch.get("status") or "").strip() or "unknown",
            )
            if part
        )
        for batch in batches
        if str(batch.get("status") or "").strip().lower() in LIVE_VALIDATION_FAILED_RUN_STATUSES
    ]


def _crm_record_id_from_run(run: dict[str, Any]) -> str:
    return str(run.get("crm_record_id") or run.get("record_id") or "").strip()


def _start_invocation_identity(
    payload: dict[str, Any],
    *,
    requested_crm_record_ids: list[str],
    failures: list[str],
) -> dict[str, Any]:
    batch_id = str(dict(payload.get("batch") or {}).get("batch_id") or "").strip()
    expected_record_ids = _dedupe_nonempty(requested_crm_record_ids)
    runs = [dict(item) for item in list(payload.get("runs") or []) if isinstance(item, dict)]
    run_ids_by_record_id: dict[str, str] = {}
    observed_run_ids: set[str] = set()

    if not batch_id:
        failures.append("start: batch.batch_id is required to bind live validation to this invocation")
    if len(expected_record_ids) != len(requested_crm_record_ids):
        failures.append("start: requested CRM record ids must be nonempty and unique")

    for run in runs:
        record_id = _crm_record_id_from_run(run)
        run_id = str(run.get("run_id") or "").strip()
        run_batch_id = str(run.get("batch_id") or "").strip()
        if not record_id:
            failures.append("start: returned run is missing crm_record_id/record_id")
            continue
        if not run_id:
            failures.append(f"start:{record_id}: returned run is missing run_id")
            continue
        if run_batch_id != batch_id:
            failures.append(
                f"start:{record_id}: run batch_id {run_batch_id or 'missing'} does not match returned batch {batch_id or 'missing'}"
            )
        if record_id not in expected_record_ids:
            failures.append(f"start:{record_id}: returned run was not requested by this invocation")
        if record_id in run_ids_by_record_id:
            failures.append(f"start:{record_id}: multiple runs were returned for one requested record")
        if run_id in observed_run_ids:
            failures.append(f"start:{record_id}: run_id {run_id} was returned for multiple records")
        run_ids_by_record_id[record_id] = run_id
        observed_run_ids.add(run_id)

    missing_record_ids = [record_id for record_id in expected_record_ids if record_id not in run_ids_by_record_id]
    if missing_record_ids:
        failures.append("start: no returned run identity for requested records: " + ", ".join(missing_record_ids))

    return {
        "batch_id": batch_id,
        "run_ids_by_crm_record_id": run_ids_by_record_id,
    }


def _validate_poll_invocation_identity(
    payload: dict[str, Any],
    *,
    invocation_identity: dict[str, Any],
    failures: list[str],
) -> bool:
    initial_failure_count = len(failures)
    expected_batch_id = str(invocation_identity.get("batch_id") or "").strip()
    expected_runs = {
        str(record_id): str(run_id)
        for record_id, run_id in dict(invocation_identity.get("run_ids_by_crm_record_id") or {}).items()
    }
    batches = [dict(item) for item in list(payload.get("batches") or []) if isinstance(item, dict)]
    observed_batch_ids = [str(batch.get("batch_id") or "").strip() for batch in batches]
    if observed_batch_ids != [expected_batch_id]:
        failures.append(
            "poll: returned batch identity does not match this invocation: "
            f"expected {expected_batch_id}, observed {observed_batch_ids or ['missing']}"
        )

    observed_runs: dict[str, str] = {}
    observed_run_ids: set[str] = set()
    for item in list(payload.get("runs") or []):
        if not isinstance(item, dict):
            continue
        run = dict(item)
        record_id = _crm_record_id_from_run(run)
        run_id = str(run.get("run_id") or "").strip()
        run_batch_id = str(run.get("batch_id") or "").strip()
        if not record_id or not run_id:
            failures.append("poll: returned run is missing crm_record_id/record_id or run_id")
            continue
        if run_batch_id != expected_batch_id:
            failures.append(
                f"poll:{record_id}: run batch_id {run_batch_id or 'missing'} does not match this invocation {expected_batch_id}"
            )
        if record_id in observed_runs or run_id in observed_run_ids:
            failures.append(f"poll:{record_id}: duplicate record or run identity was returned")
        observed_runs[record_id] = run_id
        observed_run_ids.add(run_id)

    if observed_runs != expected_runs:
        failures.append(
            "poll: returned run identities do not match this invocation: "
            f"expected {expected_runs}, observed {observed_runs}"
        )
    return len(failures) == initial_failure_count


def _validate_detail_invocation_identity(
    record_id: str,
    payload: dict[str, Any],
    *,
    invocation_identity: dict[str, Any],
    failures: list[str],
) -> bool:
    initial_failure_count = len(failures)
    response_record_id = str(payload.get("crm_record_id") or payload.get("record_id") or "").strip()
    expected_batch_id = str(invocation_identity.get("batch_id") or "").strip()
    expected_run_id = str(
        dict(invocation_identity.get("run_ids_by_crm_record_id") or {}).get(record_id) or ""
    ).strip()
    latest_run = payload.get("latest_run")
    if response_record_id != record_id:
        failures.append(
            f"detail:{record_id}: response record identity {response_record_id or 'missing'} does not match request"
        )
    if not isinstance(latest_run, dict):
        failures.append(f"detail:{record_id}: latest_run is missing for this invocation")
        return False
    latest_run_id = str(latest_run.get("run_id") or "").strip()
    latest_batch_id = str(latest_run.get("batch_id") or "").strip()
    if latest_run_id != expected_run_id:
        failures.append(
            f"detail:{record_id}: latest_run.run_id {latest_run_id or 'missing'} does not match this invocation {expected_run_id or 'missing'}"
        )
    if latest_batch_id != expected_batch_id:
        failures.append(
            f"detail:{record_id}: latest_run.batch_id {latest_batch_id or 'missing'} does not match this invocation {expected_batch_id or 'missing'}"
        )
    return len(failures) == initial_failure_count


def _latest_run_status(payload: dict[str, Any]) -> str:
    latest_run = payload.get("latest_run")
    if isinstance(latest_run, dict):
        status = str(latest_run.get("status") or "").strip().lower()
        if status:
            return status
    return ""


def _validate_no_legacy_or_raw_payload(name: str, payload: dict[str, Any], failures: list[str]) -> None:
    if _contains_marker(payload, LEGACY_ENDPOINT_MARKER):
        failures.append(f"{name}: response contains legacy target-candidate endpoint marker")
    if _contains_marker(payload, LEGACY_OWNER_MARKER):
        failures.append(f"{name}: response contains legacy execution owner {LEGACY_OWNER_MARKER}")
    for marker in RAW_PAYLOAD_MARKERS:
        if _contains_marker(payload, marker):
            failures.append(f"{name}: response contains raw/internal payload marker {marker}")


def _validate_owner_fields(name: str, payload: dict[str, Any], failures: list[str]) -> None:
    values = _collect_field_values(
        payload,
        {
            "public_web_storage_owner",
            "public_web_execution_backend",
            "owner",
            "execution_backend",
            "storage_owner",
        },
    )
    for value in values:
        if value == LEGACY_OWNER_MARKER:
            failures.append(f"{name}: owner/backend field still reports {LEGACY_OWNER_MARKER}")
    ownerish_values = [value for value in values if value in {EXPECTED_OWNER, LEGACY_OWNER_MARKER}]
    if ownerish_values and EXPECTED_OWNER not in ownerish_values:
        failures.append(f"{name}: owner/backend fields did not include {EXPECTED_OWNER}")


def _validate_model_fields(
    name: str,
    payload: dict[str, Any],
    *,
    expected_model: str,
    warnings: list[str],
    failures: list[str],
) -> None:
    del warnings
    if payload.get("model_fallback_used") is not False:
        value = payload.get("model_fallback_used")
        failures.append(f"{name}: model_fallback_used must be false: {value if value is not None else 'missing'}")
    for field in ("model", "model_version", "requested_model", "response_model", "effective_model"):
        value = str(payload.get(field) or "").strip()
        if value != expected_model:
            failures.append(f"{name}: {field} {value or 'missing'} does not match {expected_model}")
    provenance = str(payload.get("model_identity_provenance") or "").strip()
    if provenance != "provider_response":
        failures.append(f"{name}: model_identity_provenance {provenance or 'missing'} is not provider_response")


def _latest_run_model_phase_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    latest_run = payload.get("latest_run")
    if not isinstance(latest_run, dict):
        return {}
    analysis = latest_run.get("analysis")
    if not isinstance(analysis, dict):
        return {}
    phase_metrics = analysis.get("phase_metrics")
    return dict(phase_metrics) if isinstance(phase_metrics, dict) else {}


def _validate_model_fields_for_terminal_detail(
    name: str,
    payload: dict[str, Any],
    *,
    expected_model: str,
    warnings: list[str],
    failures: list[str],
) -> None:
    latest_status = _latest_run_status(payload)
    if latest_status and latest_status not in MODEL_VERIFIABLE_RUN_STATUSES:
        warnings.append(
            f"{name}: model validation skipped because latest run status is {latest_status}; "
            "poll timeout or terminal failure is the primary contract signal"
        )
        return
    phase_metrics = _latest_run_model_phase_metrics(payload)
    if not phase_metrics:
        failures.append(f"{name}: latest_run.analysis.phase_metrics was missing for model-verifiable run")
        return
    _validate_model_fields(
        name,
        phase_metrics,
        expected_model=expected_model,
        warnings=warnings,
        failures=failures,
    )


def _validate_provider_health(
    payload: dict[str, Any],
    *,
    expected_model: str,
    failures: list[str],
) -> None:
    if _append_http_failure("provider_health", payload, failures):
        return
    model_payload = dict(dict(payload.get("providers") or {}).get("model") or {})
    if not model_payload:
        failures.append("provider_health: missing providers.model payload")
        return
    status = str(model_payload.get("status") or "").strip().lower()
    chat_status = str(model_payload.get("chat_status") or "").strip().lower()
    model = str(model_payload.get("model") or "").strip()
    requested_model = str(model_payload.get("requested_model") or "").strip()
    response_model = str(model_payload.get("response_model") or "").strip()
    effective_model = str(model_payload.get("effective_model") or "").strip()
    model_identity_provenance = str(model_payload.get("model_identity_provenance") or "").strip()
    if bool(model_payload.get("circuit_open")):
        failures.append(f"provider_health: model provider circuit is open: {model_payload.get('error') or ''}")
    if status != "ready":
        failures.append(f"provider_health: model provider status is {status or 'missing'}")
    if chat_status and chat_status != "ready":
        failures.append(f"provider_health: model chat_status is {chat_status}")
    if expected_model and model != expected_model:
        failures.append(f"provider_health: model {model or 'missing'} does not match {expected_model}")
    if requested_model != expected_model:
        failures.append(
            f"provider_health: requested_model {requested_model or 'missing'} does not match {expected_model}"
        )
    if response_model != expected_model:
        failures.append(
            f"provider_health: response_model {response_model or 'missing'} does not match {expected_model}"
        )
    if effective_model != expected_model:
        failures.append(
            f"provider_health: effective_model {effective_model or 'missing'} does not match {expected_model}"
        )
    if model_identity_provenance != "provider_response":
        failures.append(
            "provider_health: model_identity_provenance "
            f"{model_identity_provenance or 'missing'} is not provider_response"
        )


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_jsonable(report), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _guard_report(args: argparse.Namespace) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    warnings: list[str] = []
    review_artifact_validation = _validate_independent_review_artifact(
        str(os.environ.get(INDEPENDENT_REVIEW_ARTIFACT_ENV) or "")
    )
    configured_expected_model = str(getattr(args, "expected_model", EXPECTED_PUBLIC_WEB_MODEL) or "").strip()
    if configured_expected_model != EXPECTED_PUBLIC_WEB_MODEL:
        failures.append(
            "--expected-model is compatibility-only and must equal the immutable CRM Public Web product model "
            f"{EXPECTED_PUBLIC_WEB_MODEL}; received {configured_expected_model or 'empty'}"
        )
    for name in sorted(LEGACY_REENABLE_ENVS):
        if _truthy(os.environ.get(name)):
            failures.append(f"{name}=1 would re-enable a retired legacy Public Web path")
    if args.execute_live:
        if not args.force_refresh:
            failures.append(
                "--execute-live requires --force-refresh so this invocation cannot reuse a prior terminal run"
            )
        if not args.confirm_live_provider_cost:
            failures.append("--execute-live requires --confirm-live-provider-cost")
        if not (_truthy(os.environ.get("LIVE_CONFIRM")) or _truthy(os.environ.get("SOURCING_LIVE_PROVIDER_CONFIRM"))):
            failures.append("--execute-live requires LIVE_CONFIRM=1 or SOURCING_LIVE_PROVIDER_CONFIRM=1")
        if not _truthy(os.environ.get(PRE_AGENT_CONTRACT_PASSED_ENV)):
            failures.append(
                f"--execute-live requires {PRE_AGENT_CONTRACT_PASSED_ENV}=1 from the Makefile pre-agent gate"
            )
        if not _truthy(os.environ.get(INDEPENDENT_REVIEW_PASSED_ENV)):
            failures.append(
                f"--execute-live requires {INDEPENDENT_REVIEW_PASSED_ENV}=1 from the independent review gate"
            )
        if not review_artifact_validation["valid_go"]:
            reason = str(review_artifact_validation.get("reason") or "artifact is not a valid GO review")
            failures.append(f"--execute-live independent review artifact is not a valid GO review: {reason}")
        if not args.crm_record_ids:
            failures.append("--execute-live requires at least one --crm-record-id")
        if not args.reviewed_crm_record_ids:
            failures.append("--execute-live requires --reviewed-crm-record-ids")
        provider_mode = str(os.environ.get("SOURCING_EXTERNAL_PROVIDER_MODE") or "").strip().lower()
        if provider_mode in DISALLOWED_LIVE_PROVIDER_MODES:
            failures.append(
                "SOURCING_EXTERNAL_PROVIDER_MODE must not be scripted/simulate/replay/fake/mock for live validation"
            )
    else:
        warnings.append("dry_run_only: no backend or provider call was made")
    return failures, warnings


def _live_prerequisites_report(args: argparse.Namespace) -> dict[str, Any]:
    missing: list[str] = []
    review_artifact_validation = _validate_independent_review_artifact(
        str(os.environ.get(INDEPENDENT_REVIEW_ARTIFACT_ENV) or "")
    )
    if not args.crm_record_ids:
        missing.append("CRM_PUBLIC_WEB_LIVE_RECORD_IDS")
    if not args.reviewed_crm_record_ids:
        missing.append("CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED=1")
    if not args.force_refresh:
        missing.append("CRM_PUBLIC_WEB_LIVE_FORCE_REFRESH=1")
    missing.append("CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_PASSED=1")
    missing.append("CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT")
    missing.append("CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_SCOPE_DIGEST_SHA256")
    missing.extend(
        [
            "LIVE_CONFIRM=1",
            "CRM_PUBLIC_WEB_LIVE_DRY_RUN=0",
            "live backend running at CRM_PUBLIC_WEB_LIVE_BASE_URL",
            "reviewed live provider cost",
        ]
    )
    record_ids = " ".join(args.crm_record_ids) if args.crm_record_ids else "crm_record_id_1 crm_record_id_2"
    review_scope_digest = _required_independent_review_scope_digest() or "<review_scope_digest_sha256>"
    command = (
        "LIVE_CONFIRM=1 make test-crm-public-web-live-product-validation "
        "CRM_PUBLIC_WEB_LIVE_DRY_RUN=0 "
        "CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED=1 "
        "CRM_PUBLIC_WEB_LIVE_FORCE_REFRESH=1 "
        "CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_PASSED=1 "
        'CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT="runtime/reviews/<review>.md" '
        f'CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_SCOPE_DIGEST_SHA256="{review_scope_digest}" '
        f"CRM_PUBLIC_WEB_LIVE_MAX_FETCHES_PER_CANDIDATE={args.max_fetches_per_candidate} "
        f"CRM_PUBLIC_WEB_LIVE_MAX_AI_EVIDENCE_DOCUMENTS={args.max_ai_evidence_documents} "
        f"CRM_PUBLIC_WEB_LIVE_MAX_AI_ENTRY_LINKS={args.max_ai_entry_links} "
        f"CRM_PUBLIC_WEB_LIVE_MAX_REMOTE_SEARCH_WAIT_SECONDS={args.max_remote_search_wait_seconds} "
        f"CRM_PUBLIC_WEB_LIVE_MAX_PROVIDER_PENDING_WAIT_SECONDS={args.max_provider_pending_wait_seconds} "
        f"CRM_PUBLIC_WEB_LIVE_MAX_CONCURRENT_CANDIDATE_ANALYSES={args.max_concurrent_candidate_analyses} "
        f'CRM_PUBLIC_WEB_LIVE_RECORD_IDS="{record_ids}"'
    )
    return {
        "ready_to_execute_live_with_current_args": bool(
            args.crm_record_ids
            and args.reviewed_crm_record_ids
            and args.force_refresh
            and _truthy(os.environ.get(INDEPENDENT_REVIEW_PASSED_ENV))
            and review_artifact_validation["valid_go"]
        ),
        "missing_or_required_before_live": missing,
        "independent_review_artifact_validation": review_artifact_validation,
        "record_id_selection_guidance": [
            "Use reviewed CRMRecord ids, not legacy target-candidate ids.",
            "Prefer 1-3 records with known LinkedIn/profile identity and enough public web footprint for qualitative review.",
            "Do not use the retired /api/target-candidates/public-web routes to prepare or validate this pass.",
        ],
        "recommended_make_command": command,
        "quality_budget": {
            "max_fetches_per_candidate": args.max_fetches_per_candidate,
            "max_ai_evidence_documents": args.max_ai_evidence_documents,
            "max_ai_entry_links": args.max_ai_entry_links,
            "max_remote_search_wait_seconds": args.max_remote_search_wait_seconds,
            "max_provider_pending_wait_seconds": args.max_provider_pending_wait_seconds,
            "max_concurrent_candidate_analyses": args.max_concurrent_candidate_analyses,
            "note": "Live validation uses an expanded evidence window so model quality failures are not caused by an undersized adjudication packet.",
        },
    }


def _run_live(args: argparse.Namespace, failures: list[str], warnings: list[str]) -> dict[str, Any]:
    if not args.force_refresh:
        failures.append("--execute-live requires --force-refresh before any backend or provider request")
        return {"provider_health": {}, "start": {}, "poll_history": [], "details": {}, "export": {}}

    provider_health = _request_json("GET", args.base_url, CANONICAL_ENDPOINTS["provider_health"])
    _validate_provider_health(provider_health, expected_model=EXPECTED_PUBLIC_WEB_MODEL, failures=failures)
    if failures:
        return {"provider_health": provider_health, "start": {}, "poll_history": [], "details": {}, "export": {}}

    start_payload = {
        "crm_record_ids": args.crm_record_ids,
        "force_refresh": True,
        "requested_by": "w7g_crm_public_web_live_product_validation",
        "options": {
            "validation_scope": "w7g_crm_public_web_live_product_validation",
            "expected_model": EXPECTED_PUBLIC_WEB_MODEL,
            "max_fetches_per_candidate": args.max_fetches_per_candidate,
            "max_ai_evidence_documents": args.max_ai_evidence_documents,
            "max_ai_entry_links": args.max_ai_entry_links,
            "max_remote_search_wait_seconds": args.max_remote_search_wait_seconds,
            "max_provider_pending_wait_seconds": args.max_provider_pending_wait_seconds,
            "max_concurrent_candidate_analyses": args.max_concurrent_candidate_analyses,
        },
    }
    start_response = _request_json("POST", args.base_url, CANONICAL_ENDPOINTS["start"], start_payload)
    _validate_no_legacy_or_raw_payload("start", start_response, failures)
    _validate_owner_fields("start", start_response, failures)
    if _append_http_failure("start", start_response, failures):
        return {
            "provider_health": provider_health,
            "start": start_response,
            "poll_history": [],
            "details": {},
            "export": {},
        }

    invocation_identity = _start_invocation_identity(
        start_response,
        requested_crm_record_ids=args.crm_record_ids,
        failures=failures,
    )
    if failures:
        return {
            "provider_health": provider_health,
            "start": start_response,
            "invocation_identity": invocation_identity,
            "poll_history": [],
            "details": {},
            "export": {},
        }

    batch_id = str(invocation_identity["batch_id"])
    poll_payload: dict[str, Any] = {"batch_id": batch_id}
    poll_history: list[dict[str, Any]] = []
    poll_chain_valid = True
    deadline = time.monotonic() + args.poll_timeout_seconds
    while True:
        poll_failure_count = len(failures)
        poll_response = _request_json("POST", args.base_url, CANONICAL_ENDPOINTS["poll"], poll_payload)
        poll_history.append(poll_response)
        _validate_no_legacy_or_raw_payload("poll", poll_response, failures)
        _validate_owner_fields("poll", poll_response, failures)
        if _append_http_failure("poll", poll_response, failures):
            poll_chain_valid = False
            break
        if len(failures) != poll_failure_count:
            poll_chain_valid = False
            break
        poll_chain_valid = _validate_poll_invocation_identity(
            poll_response,
            invocation_identity=invocation_identity,
            failures=failures,
        )
        if not poll_chain_valid:
            break
        failed_batches = _failed_terminal_batch_summaries(poll_response)
        if failed_batches:
            failures.append(
                "poll: CRM Public Web batch reached failed/cancelled terminal state: " + ", ".join(failed_batches)
            )
            poll_chain_valid = False
            break
        if _runs_are_terminal(poll_response):
            failed_runs = _failed_terminal_run_summaries(poll_response)
            if failed_runs:
                failures.append(
                    "poll: CRM Public Web runs reached failed/cancelled terminal states: " + ", ".join(failed_runs)
                )
                poll_chain_valid = False
            break
        if time.monotonic() >= deadline:
            nonterminal_runs = _nonterminal_run_summaries(poll_response)
            detail = f": {', '.join(nonterminal_runs)}" if nonterminal_runs else ""
            failures.append(f"poll: timed out before all CRM Public Web runs reached terminal state{detail}")
            poll_chain_valid = False
            break
        time.sleep(args.poll_interval_seconds)

    if not poll_chain_valid:
        return {
            "provider_health": provider_health,
            "start": start_response,
            "invocation_identity": invocation_identity,
            "poll_history": poll_history,
            "details": {},
            "export": {},
        }

    detail_responses: dict[str, dict[str, Any]] = {}
    detail_chain_valid = True
    for record_id in args.crm_record_ids:
        detail_failure_count = len(failures)
        endpoint = CANONICAL_ENDPOINTS["detail"].format(crm_record_id=quote(record_id, safe=""))
        detail_response = _request_json("GET", args.base_url, endpoint)
        detail_responses[record_id] = detail_response
        _validate_no_legacy_or_raw_payload(f"detail:{record_id}", detail_response, failures)
        _validate_owner_fields(f"detail:{record_id}", detail_response, failures)
        if _append_http_failure(f"detail:{record_id}", detail_response, failures):
            detail_chain_valid = False
            continue
        if len(failures) != detail_failure_count:
            detail_chain_valid = False
            continue
        if not _validate_detail_invocation_identity(
            record_id,
            detail_response,
            invocation_identity=invocation_identity,
            failures=failures,
        ):
            detail_chain_valid = False
            continue
        _validate_model_fields_for_terminal_detail(
            f"detail:{record_id}",
            detail_response,
            expected_model=EXPECTED_PUBLIC_WEB_MODEL,
            warnings=warnings,
            failures=failures,
        )
        if len(failures) != detail_failure_count:
            detail_chain_valid = False

    if not detail_chain_valid or failures:
        return {
            "provider_health": provider_health,
            "start": start_response,
            "invocation_identity": invocation_identity,
            "poll_history": poll_history,
            "details": detail_responses,
            "export": {},
        }

    export_response = _request_bytes(
        "POST",
        args.base_url,
        CANONICAL_ENDPOINTS["export"],
        {"crm_record_ids": args.crm_record_ids, "mode": args.export_mode},
    )
    if str(export_response.get("status") or "") != "ok":
        failures.append(f"export: {export_response.get('status')} {export_response.get('reason') or ''}".strip())

    return {
        "provider_health": provider_health,
        "start": start_response,
        "invocation_identity": invocation_identity,
        "poll_history": poll_history,
        "details": detail_responses,
        "export": export_response,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8777")
    parser.add_argument("--crm-record-id", dest="crm_record_ids", action="append", default=[])
    parser.add_argument("--expected-model", default=EXPECTED_PUBLIC_WEB_MODEL)
    parser.add_argument("--expected-qwen-model", dest="expected_model")
    parser.add_argument("--poll-timeout-seconds", type=int, default=600)
    parser.add_argument("--poll-interval-seconds", type=int, default=5)
    parser.add_argument("--max-fetches-per-candidate", type=int, default=10)
    parser.add_argument("--max-ai-evidence-documents", type=int, default=10)
    parser.add_argument("--max-ai-entry-links", type=int, default=10)
    parser.add_argument("--max-remote-search-wait-seconds", type=int, default=420)
    parser.add_argument("--max-provider-pending-wait-seconds", type=int, default=420)
    parser.add_argument("--max-concurrent-candidate-analyses", type=int, default=1)
    parser.add_argument("--export-mode", default="promoted_only")
    parser.add_argument("--report-json", default="output/w7g_crm_public_web_live_product_validation.json")
    parser.add_argument("--execute-live", action="store_true")
    parser.add_argument("--confirm-live-provider-cost", action="store_true")
    parser.add_argument("--reviewed-crm-record-ids", action="store_true")
    parser.add_argument("--force-refresh", action="store_true")
    args = parser.parse_args()
    args.crm_record_ids = [item.strip() for item in args.crm_record_ids if str(item or "").strip()]

    failures, warnings = _guard_report(args)
    report: dict[str, Any] = {
        "status": "pending",
        "mode": "live" if args.execute_live else "dry_run",
        "canonical_endpoints": CANONICAL_ENDPOINTS,
        "legacy_endpoint_marker_forbidden": LEGACY_ENDPOINT_MARKER,
        "legacy_owner_marker_forbidden": LEGACY_OWNER_MARKER,
        "expected_owner": EXPECTED_OWNER,
        "expected_model": EXPECTED_PUBLIC_WEB_MODEL,
        "configured_expected_model": args.expected_model,
        "crm_record_ids": args.crm_record_ids,
        "crm_record_ids_reviewed": bool(args.reviewed_crm_record_ids),
        "force_refresh": bool(args.force_refresh),
        "independent_review": {
            "passed": _truthy(os.environ.get(INDEPENDENT_REVIEW_PASSED_ENV)),
            "artifact": str(os.environ.get(INDEPENDENT_REVIEW_ARTIFACT_ENV) or "").strip(),
            "expected_title": EXPECTED_INDEPENDENT_REVIEW_TITLE,
            "expected_scope_digest_sha256": _required_independent_review_scope_digest(),
            "artifact_validation": _validate_independent_review_artifact(
                str(os.environ.get(INDEPENDENT_REVIEW_ARTIFACT_ENV) or "")
            ),
        },
        "live_prerequisites": _live_prerequisites_report(args),
        "guard_failures": failures,
        "warnings": warnings,
        "responses": {},
    }
    if not failures and args.execute_live:
        report["responses"] = _run_live(args, failures, warnings)
    report["guard_failures"] = failures
    report["warnings"] = warnings
    report["status"] = "failed" if failures else ("live_validation_passed" if args.execute_live else "dry_run_ready")
    _write_report(Path(args.report_json), report)
    print(
        json.dumps(
            {"status": report["status"], "report_json": args.report_json, "failures": failures}, ensure_ascii=False
        )
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
