from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit

from x_first.contracts import _prohibited_content_errors, load_json, project_root

REQUEST_SCHEMA_VERSION = "x.grok.capability_probe.request.v1"
RESULT_SCHEMA_VERSION = "x.grok.capability_probe.result.v1"
EXECUTION_MODE = "fixture_only"
SAFETY_POLICY_VERSION = "x-first-public-professional-v1"

REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "probe_id",
        "execution_mode",
        "owner_decisions",
        "target",
        "query",
        "hard_budgets",
        "kill_switch",
        "retention",
        "safety_policy_version",
        "claims",
    }
)
RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "probe_id",
        "request_sha256",
        "execution_mode",
        "run",
        "task",
        "capability",
        "provenance",
        "usage",
        "observations",
        "errors",
        "retention",
        "candidate_packets",
        "identity_link_proposals",
        "assertions",
        "canonical_writes",
        "claims",
    }
)
OBJECT_FIELDS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "owner_decisions": frozenset({"live_execution", "legal_privacy", "model_access", "retention_policy"}),
        "target": frozenset({"lab_id", "account_kind", "platform_user_id", "current_handle"}),
        "query": frozenset({"query_kind", "query_template"}),
        "hard_budgets": frozenset(
            {
                "max_executions",
                "max_external_calls",
                "max_pages",
                "max_observations",
                "max_cost_usd",
                "deadline_ms",
            }
        ),
        "kill_switch": frozenset({"armed", "trip_conditions"}),
        "request.retention": frozenset({"class", "delete_after", "bounded_excerpt_max_chars", "full_body_allowed"}),
        "request.claims": frozenset(
            {
                "external_execution_authorized",
                "researcher_mapping_authorized",
                "graph_expansion_authorized",
                "provider_fallback_authorized",
                "canonical_writes_authorized",
                "outreach_authorized",
            }
        ),
        "run": frozenset({"run_id", "status", "started_at", "completed_at"}),
        "task": frozenset({"task_id", "status", "stop_reason"}),
        "capability": frozenset({"verdict", "proof_scope", "x_native_access_proven"}),
        "provenance": frozenset(
            {
                "provider_id",
                "access_mode",
                "model_id",
                "tool_id",
                "provider_request_id",
                "prompt_version",
                "request_sha256",
                "raw_response_sha256",
            }
        ),
        "usage": frozenset(
            {"executions", "external_calls", "pages", "observations", "cost_usd", "elapsed_ms"}
        ),
        "observation": frozenset(
            {
                "observation_id",
                "platform_object_id",
                "platform_user_id",
                "author_handle",
                "canonical_url",
                "authored_at",
                "observed_at",
                "excerpt",
                "full_body_stored",
            }
        ),
        "error": frozenset({"code", "message", "retryable"}),
        "result.retention": frozenset({"class", "delete_after", "full_body_stored", "deletion_status"}),
        "result.claims": frozenset(
            {
                "exhaustive",
                "current_employment_guaranteed",
                "outreach_permission",
                "researcher_mapping_authorized",
            }
        ),
    }
)

OWNER_DECISIONS = MappingProxyType(
    {
        "live_execution": "deferred",
        "legal_privacy": "deferred",
        "model_access": "deferred",
        "retention_policy": "deferred",
    }
)
HARD_BUDGETS = MappingProxyType(
    {
        "max_executions": 1,
        "max_external_calls": 1,
        "max_pages": 1,
        "max_observations": 5,
        "max_cost_usd": 0,
        "deadline_ms": 1000,
    }
)
KILL_SWITCH_TRIP_CONDITIONS = (
    "external_execution_attempted",
    "live_url_observed",
    "budget_exceeded",
    "credential_material_observed",
    "canonical_write_attempted",
)
RUN_STATUS_REGISTRY: Mapping[str, bool] = MappingProxyType(
    {"completed": True, "failed": True, "cancelled": True, "killed": True}
)
TASK_STATUS_REGISTRY: Mapping[str, bool] = MappingProxyType(
    {"succeeded": True, "failed": True, "cancelled": True, "expired": True}
)
CAPABILITY_VERDICTS = frozenset(
    {"fixture_contract_validated", "capability_unavailable", "probe_error", "killed"}
)
TERMINAL_TUPLES = frozenset(
    {
        ("completed", "succeeded", "fixture_contract_validated"),
        ("failed", "failed", "capability_unavailable"),
        ("failed", "failed", "probe_error"),
        ("cancelled", "cancelled", "probe_error"),
        ("killed", "cancelled", "killed"),
        ("killed", "expired", "killed"),
    }
)
CREDENTIAL_FIELD_TOKENS = frozenset({"api_key", "authorization", "credential", "oauth", "password", "secret", "token"})


def canonical_sha256(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _iter_values(value: Any, path: tuple[str, ...] = ()) -> Iterable[tuple[tuple[str, ...], Any]]:
    yield path, value
    if isinstance(value, dict):
        for key, child in value.items():
            yield from _iter_values(child, (*path, str(key)))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _iter_values(child, (*path, str(index)))


def _field_tokens(field: str) -> set[str]:
    separated = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", field)
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", separated).strip("_").lower()
    return {normalized, *(part for part in normalized.split("_") if part)}


def _credential_errors(value: Any, *, root: str) -> list[str]:
    errors: list[str] = []
    for path, _child in _iter_values(value):
        if path and _field_tokens(path[-1]) & CREDENTIAL_FIELD_TOKENS:
            errors.append(f"credential-bearing field is forbidden: {'.'.join((root, *path))}")
    return sorted(set(errors))


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _validate_object(
    value: Any,
    *,
    fields: frozenset[str],
    location: str,
    errors: list[str],
) -> dict[str, Any]:
    if not isinstance(value, dict):
        errors.append(f"{location} must be an object")
        return {}
    missing = sorted(fields - set(value))
    extra = sorted(set(value) - fields)
    if missing:
        errors.append(f"{location} missing fields: {missing}")
    if extra:
        errors.append(f"{location} unexpected fields: {extra}")
    return value


def _validate_shape(value: Any, *, shape: str, location: str, errors: list[str]) -> dict[str, Any]:
    return _validate_object(value, fields=OBJECT_FIELDS[shape], location=location, errors=errors)


def _is_false(value: Any) -> bool:
    return type(value) is bool and value is False


def validate_capability_request(payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    request = _validate_object(payload, fields=REQUEST_FIELDS, location="request", errors=errors)
    if request.get("schema_version") != REQUEST_SCHEMA_VERSION:
        errors.append(f"request.schema_version must be {REQUEST_SCHEMA_VERSION}")
    if request.get("execution_mode") != EXECUTION_MODE:
        errors.append("request.execution_mode must be fixture_only")
    if request.get("probe_id") != "xprobe_fixture_openai_official_v1":
        errors.append("request.probe_id must use the canonical synthetic fixture ID")

    owner_decisions = _validate_shape(
        request.get("owner_decisions"), shape="owner_decisions", location="request.owner_decisions", errors=errors
    )
    if owner_decisions != OWNER_DECISIONS:
        errors.append("all live owner decisions must remain deferred in fixture-only mode")

    target = _validate_shape(request.get("target"), shape="target", location="request.target", errors=errors)
    if target != {
        "lab_id": "openai",
        "account_kind": "official_lab",
        "platform_user_id": "xuid_fixture_official_openai",
        "current_handle": "fixture_openai_official",
    }:
        errors.append("request target must be the canonical synthetic official-lab account")

    query = _validate_shape(request.get("query"), shape="query", location="request.query", errors=errors)
    if query != {
        "query_kind": "recent_public_technical_posts",
        "query_template": "fixture://openai/official/recent-public-technical-posts",
    }:
        errors.append("request query must be the fixed synthetic official-account capability query")

    budgets = _validate_shape(
        request.get("hard_budgets"), shape="hard_budgets", location="request.hard_budgets", errors=errors
    )
    budget_type_invalid = any(
        type(budgets.get(field)) not in ({int, float} if field == "max_cost_usd" else {int})
        for field in HARD_BUDGETS
    )
    if budget_type_invalid:
        errors.append("request hard budgets must use strict numeric types")
    if budgets != HARD_BUDGETS:
        errors.append("request hard budgets must be exactly 1/1/1/5, zero cost, and 1000ms")

    kill_switch = _validate_shape(
        request.get("kill_switch"), shape="kill_switch", location="request.kill_switch", errors=errors
    )
    trip_conditions = kill_switch.get("trip_conditions")
    if kill_switch.get("armed") is not True or trip_conditions != list(KILL_SWITCH_TRIP_CONDITIONS):
        errors.append("request kill switch must be armed with the canonical fail-closed trip registry")

    retention = _validate_shape(
        request.get("retention"), shape="request.retention", location="request.retention", errors=errors
    )
    if retention != {
        "class": "synthetic_fixture_only",
        "delete_after": None,
        "bounded_excerpt_max_chars": 280,
        "full_body_allowed": False,
    }:
        errors.append("request retention must be synthetic-only with bounded excerpts and no full body")
    if request.get("safety_policy_version") != SAFETY_POLICY_VERSION:
        errors.append("request safety policy version mismatch")

    claims = _validate_shape(request.get("claims"), shape="request.claims", location="request.claims", errors=errors)
    for field in OBJECT_FIELDS["request.claims"]:
        if not _is_false(claims.get(field)):
            errors.append(f"request.claims.{field} must be false")

    errors.extend(_prohibited_content_errors(payload, root="request"))
    errors.extend(_credential_errors(payload, root="request"))
    return sorted(set(errors))


def validate_capability_result(
    payload: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
) -> list[str]:
    errors = [f"bound request invalid: {error}" for error in validate_capability_request(request)]
    result = _validate_object(payload, fields=RESULT_FIELDS, location="result", errors=errors)
    if result.get("schema_version") != RESULT_SCHEMA_VERSION:
        errors.append(f"result.schema_version must be {RESULT_SCHEMA_VERSION}")
    if result.get("probe_id") != request.get("probe_id"):
        errors.append("result probe_id must match request")
    if result.get("execution_mode") != EXECUTION_MODE:
        errors.append("result.execution_mode must be fixture_only")
    request_hash = canonical_sha256(request)
    if result.get("request_sha256") != request_hash:
        errors.append("result request_sha256 must bind the canonical request")

    run = _validate_shape(result.get("run"), shape="run", location="result.run", errors=errors)
    task = _validate_shape(result.get("task"), shape="task", location="result.task", errors=errors)
    capability = _validate_shape(
        result.get("capability"), shape="capability", location="result.capability", errors=errors
    )
    run_status = str(run.get("status") or "")
    task_status = str(task.get("status") or "")
    verdict = str(capability.get("verdict") or "")
    if run_status not in RUN_STATUS_REGISTRY:
        errors.append(f"unknown capability run status: {run_status or '<missing>'}")
    if task_status not in TASK_STATUS_REGISTRY:
        errors.append(f"unknown capability task status: {task_status or '<missing>'}")
    if verdict not in CAPABILITY_VERDICTS:
        errors.append(f"unknown capability verdict: {verdict or '<missing>'}")
    if (run_status, task_status, verdict) not in TERMINAL_TUPLES:
        errors.append("capability run/task/verdict terminal tuple is inconsistent")
    if run.get("run_id") != "xprobe_run_fixture_openai_official_v1":
        errors.append("capability fixture run ID mismatch")
    if task.get("task_id") != "xprobe_task_fixture_openai_official_v1":
        errors.append("capability fixture task ID mismatch")
    expected_stop_reason = "fixture_complete" if verdict == "fixture_contract_validated" else "fixture_failed"
    if task.get("stop_reason") != expected_stop_reason:
        errors.append("capability task stop_reason contradicts verdict")
    started_at = _parse_datetime(run.get("started_at"))
    completed_at = _parse_datetime(run.get("completed_at"))
    if started_at is None or completed_at is None:
        errors.append("capability run timestamps must be timezone-aware ISO-8601 values")
    elif started_at > completed_at:
        errors.append("capability run started_at must not follow completed_at")
    if capability.get("proof_scope") != "offline_synthetic_only":
        errors.append("capability proof_scope must remain offline_synthetic_only")
    if capability.get("x_native_access_proven") is not False:
        errors.append("fixture-only results cannot prove X-native access")

    provenance = _validate_shape(
        result.get("provenance"), shape="provenance", location="result.provenance", errors=errors
    )
    expected_provenance = {
        "provider_id": "offline_fixture",
        "access_mode": "fixture",
        "model_id": None,
        "tool_id": None,
        "provider_request_id": None,
        "prompt_version": "fixture-capability-v1",
        "request_sha256": request_hash,
    }
    for field, expected in expected_provenance.items():
        if provenance.get(field) != expected:
            errors.append(f"fixture provenance mismatch: {field}")
    if not isinstance(provenance.get("raw_response_sha256"), str) or not re.fullmatch(
        r"[0-9a-f]{64}", provenance.get("raw_response_sha256", "")
    ):
        errors.append("fixture raw_response_sha256 must be a lowercase SHA-256")

    usage = _validate_shape(result.get("usage"), shape="usage", location="result.usage", errors=errors)
    integer_usage = ("executions", "external_calls", "pages", "observations", "elapsed_ms")
    if any(type(usage.get(field)) is not int or usage.get(field, -1) < 0 for field in integer_usage):
        errors.append("capability usage counters must be non-negative integers")
    if type(usage.get("cost_usd")) not in {int, float} or usage.get("cost_usd", -1) < 0:
        errors.append("capability usage cost_usd must be a non-negative number")
    if any(usage.get(field) != 0 for field in ("executions", "external_calls", "pages", "cost_usd")):
        errors.append("fixture-only capability results must have zero external execution, calls, pages, and cost")
    budgets = request.get("hard_budgets") if isinstance(request.get("hard_budgets"), dict) else {}
    for usage_field, budget_field in (
        ("executions", "max_executions"),
        ("external_calls", "max_external_calls"),
        ("pages", "max_pages"),
        ("observations", "max_observations"),
        ("cost_usd", "max_cost_usd"),
        ("elapsed_ms", "deadline_ms"),
    ):
        if isinstance(usage.get(usage_field), (int, float)) and isinstance(budgets.get(budget_field), (int, float)):
            if usage[usage_field] > budgets[budget_field]:
                errors.append(f"capability usage exceeds request budget: {usage_field}")

    observations_value = result.get("observations")
    if not isinstance(observations_value, list):
        errors.append("result.observations must be an array")
    observations = observations_value if isinstance(observations_value, list) else []
    if usage.get("observations") != len(observations):
        errors.append("capability observation total does not reconcile")
    if verdict == "fixture_contract_validated" and not 1 <= len(observations) <= 5:
        errors.append("successful fixture capability result must contain one to five observations")
    if verdict != "fixture_contract_validated" and observations:
        errors.append("failed fixture capability result cannot retain observations")
    observation_ids: set[str] = set()
    object_ids: set[str] = set()
    canonical_urls: set[str] = set()
    target = request.get("target") if isinstance(request.get("target"), dict) else {}
    max_excerpt_chars = (
        request.get("retention", {}).get("bounded_excerpt_max_chars")
        if isinstance(request.get("retention"), dict)
        else None
    )
    for index, observation_value in enumerate(observations):
        observation = _validate_shape(
            observation_value,
            shape="observation",
            location=f"result.observations[{index}]",
            errors=errors,
        )
        observation_id = str(observation.get("observation_id") or "")
        object_id = str(observation.get("platform_object_id") or "")
        canonical_url = str(observation.get("canonical_url") or "")
        if not re.fullmatch(r"xprobe_obs_fixture_[0-9]{3}", observation_id):
            errors.append(f"capability observation ID is not synthetic: {observation_id}")
        if not re.fullmatch(r"xpost_fixture_[0-9]{3}", object_id):
            errors.append(f"capability platform object ID is not synthetic: {observation_id}")
        if observation_id in observation_ids or object_id in object_ids or canonical_url in canonical_urls:
            errors.append("capability observation identities and URLs must be unique")
        observation_ids.add(observation_id)
        object_ids.add(object_id)
        canonical_urls.add(canonical_url)
        if observation.get("platform_user_id") != target.get("platform_user_id"):
            errors.append(f"capability observation account mismatch: {observation_id}")
        if observation.get("author_handle") != target.get("current_handle"):
            errors.append(f"capability observation handle mismatch: {observation_id}")
        parsed_url = urlsplit(canonical_url)
        expected_path = f"/{target.get('current_handle')}/status/{object_id}"
        if (
            parsed_url.scheme != "https"
            or parsed_url.netloc != "posts.invalid"
            or parsed_url.path != expected_path
            or parsed_url.query
            or parsed_url.fragment
        ):
            errors.append(f"capability observation URL is not canonical synthetic evidence: {observation_id}")
        authored_at = _parse_datetime(observation.get("authored_at"))
        observed_at = _parse_datetime(observation.get("observed_at"))
        if authored_at is None or observed_at is None:
            errors.append(f"capability observation timestamps are invalid: {observation_id}")
        elif authored_at > observed_at:
            errors.append(f"capability observation authored_at follows observed_at: {observation_id}")
        elif started_at is not None and completed_at is not None and not started_at <= observed_at <= completed_at:
            errors.append(f"capability observation observed_at falls outside run: {observation_id}")
        excerpt = observation.get("excerpt")
        if (
            not isinstance(excerpt, str)
            or not excerpt.startswith("Synthetic capability evidence ")
            or not isinstance(max_excerpt_chars, int)
            or len(excerpt) > max_excerpt_chars
        ):
            errors.append(f"capability observation excerpt is not bounded synthetic text: {observation_id}")
        if observation.get("full_body_stored") is not False:
            errors.append(f"capability observation cannot store a full body: {observation_id}")

    errors_value = result.get("errors")
    if not isinstance(errors_value, list):
        errors.append("result.errors must be an array")
    result_errors = errors_value if isinstance(errors_value, list) else []
    expected_error_count = 0 if verdict == "fixture_contract_validated" else 1
    if len(result_errors) != expected_error_count:
        errors.append("capability error count contradicts verdict")
    for index, error_value in enumerate(result_errors):
        error = _validate_shape(
            error_value, shape="error", location=f"result.errors[{index}]", errors=errors
        )
        if not re.fullmatch(r"[a-z][a-z0-9_]{2,63}", str(error.get("code") or "")):
            errors.append(f"capability error code is invalid: {index}")
        if not isinstance(error.get("message"), str) or not error["message"].startswith("Synthetic "):
            errors.append(f"capability error message must be synthetic: {index}")
        if type(error.get("retryable")) is not bool:
            errors.append(f"capability error retryable must be boolean: {index}")

    retention = _validate_shape(
        result.get("retention"), shape="result.retention", location="result.retention", errors=errors
    )
    if retention != {
        "class": "synthetic_fixture_only",
        "delete_after": None,
        "full_body_stored": False,
        "deletion_status": "not_applicable_synthetic",
    }:
        errors.append("result retention must remain synthetic-only and full-body-free")

    for field in ("candidate_packets", "identity_link_proposals", "assertions", "canonical_writes"):
        if result.get(field) != []:
            errors.append(f"fixture capability result {field} must be empty")
    claims = _validate_shape(result.get("claims"), shape="result.claims", location="result.claims", errors=errors)
    for field in OBJECT_FIELDS["result.claims"]:
        if not _is_false(claims.get(field)):
            errors.append(f"result.claims.{field} must be false")

    errors.extend(_prohibited_content_errors(payload, root="result"))
    errors.extend(_credential_errors(payload, root="result"))
    return sorted(set(errors))


def _default_paths() -> tuple[Path, Path]:
    root = project_root()
    return (
        root / "fixtures/capability_probe_request_fixture_v1.json",
        root / "fixtures/capability_probe_result_fixture_v1.json",
    )


def main() -> int:
    default_request, default_result = _default_paths()
    parser = argparse.ArgumentParser(description="Validate offline X-first capability-probe fixtures")
    parser.add_argument("--request", type=Path, default=default_request)
    parser.add_argument("--result", type=Path, default=default_result)
    args = parser.parse_args()
    request = load_json(args.request)
    result = load_json(args.result)
    errors = validate_capability_request(request)
    errors.extend(validate_capability_result(result, request=request))
    errors = sorted(set(errors))
    print(json.dumps({"errors": errors, "status": "valid" if not errors else "invalid"}, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
