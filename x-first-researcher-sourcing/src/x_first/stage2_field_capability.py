"""Offline Stage 2A contracts for replay-bound X profile/Post field capability.

The module is deliberately provider-free.  It validates synthetic replay records
and proves that a normalized profile can become ``replay_bound_exact`` only when
one record co-locates a numeric platform user id, handle, profile URL, exact Bio,
observation time, and content version.  It does not claim that the installed Grok
CLI exposes such a record; metadata-only tool traces remain ``unverified``.
"""

from __future__ import annotations

import copy
import functools
import hashlib
import json
import math
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from x_first.recall_pool_schema import MiniDraft202012Error, assert_schema_valid

FIELD_REGISTRY_SCHEMA_VERSION = "x.stage2.field_registry.v1"
FIELD_REGISTRY_VERSION = "x-stage2-field-registry-v1"
EXPERIMENT_SCHEMA_VERSION = "x.stage2.experiment.request.v1"
COLLECTION_SCHEMA_VERSION = "x.stage2.collection.v1"
EXPECTATION_SCHEMA_VERSION = "x.stage2.capability_expectation.v1"
EVALUATION_SCHEMA_VERSION = "x.stage2.evaluation.v1"
SELECTION_MANIFEST_VERSION = "x.stage2.external_selection.fixture.v1"
SCENARIO_MANIFEST_VERSION = "stage2-field-capability-fixture-scenarios-v1"
EXPECTATION_VERSION = "stage2-field-capability-fixture-expectation-v1"

SCHEMA_FILENAMES = {
    EXPERIMENT_SCHEMA_VERSION: "x.stage2.experiment.request.v1.schema.json",
    COLLECTION_SCHEMA_VERSION: "x.stage2.collection.v1.schema.json",
    EXPECTATION_SCHEMA_VERSION: "x.stage2.capability_expectation.v1.schema.json",
    EVALUATION_SCHEMA_VERSION: "x.stage2.evaluation.v1.schema.json",
}

EXECUTION_MODE = "offline_fixture"
NORMALIZATION_VERSION = "x.stage2.field_normalization.v1"
FIELD_STATES = ("present_exact", "present_bounded", "absent", "unverified")
TERMINAL_STATUSES = ("completed", "quarantined", "failed")
PROFILE_REQUIRED_FIELDS = (
    "platform_user_id",
    "current_handle",
    "profile_url",
    "bio_text",
    "bio_sha256",
    "bio_observed_at",
    "bio_content_version",
)
PROFILE_ALWAYS_EXACT_FIELDS = (
    "platform_user_id",
    "current_handle",
    "profile_url",
    "bio_observed_at",
)
PROFILE_OPTIONAL_BIO_FIELDS = (
    "bio_text",
    "bio_sha256",
    "bio_content_version",
)
CANONICAL_FIELD_DESCRIPTORS = (
    ("platform_user_id", "profile", "numeric_identifier", "same_replayable_profile_record"),
    ("current_handle", "profile", "handle", "same_replayable_profile_record"),
    ("profile_url", "profile", "canonical_url", "same_replayable_profile_record"),
    ("bio_text", "profile", "utf8_text", "same_replayable_profile_record"),
    ("bio_sha256", "profile", "derived_sha256", "derived_from_exact_bio_text"),
    ("bio_observed_at", "profile", "utc_timestamp", "same_replayable_profile_record"),
    ("bio_content_version", "profile", "source_version", "same_replayable_profile_record"),
    ("canonical_post_id", "post", "numeric_identifier", "same_replayable_post_record"),
    ("canonical_post_url", "post", "canonical_url", "same_replayable_post_record"),
    ("post_author_platform_user_id", "post", "numeric_identifier", "same_replayable_post_record"),
    ("post_author_handle", "post", "handle", "same_replayable_post_record"),
    ("post_authored_at", "post", "utc_timestamp", "same_replayable_post_record"),
    ("bounded_excerpt", "post", "bounded_text", "same_replayable_post_record"),
    ("thread_relation", "post", "closed_relation", "same_replayable_post_record"),
)
THREAD_RELATIONS = ("self_post", "reply", "quote", "thread_root", "thread_reply")
FIXTURE_SCENARIO_IDS = (
    "exact_profile_and_same_account_post",
    "conflicting_platform_user_ids",
    "handle_rename_requires_review",
    "metadata_only_source_payload_unavailable",
)
QUARANTINE_REASON_CODES = (
    "multiple_profile_sources",
    "conflicting_platform_user_ids",
    "handle_rename_requires_review",
    "platform_user_id_handle_conflict",
    "cross_account_evidence",
)
ERROR_CODES = (*QUARANTINE_REASON_CODES, "source_payload_unavailable")
TECHNICAL_LIMITS = {
    "max_tasks": 10_000,
    "max_source_records": 100_000,
    "max_canonical_bytes": 32_000_000,
    "max_raw_record_bytes": 262_144,
    "max_validation_depth": 64,
    "max_validation_nodes": 500_000,
    "raw_evidence_ttl_seconds": 86_400,
}
REQUEST_AUTHORITY = {
    "provider_or_network_allowed": False,
    "model_reported_id_as_identity_allowed": False,
    "automatic_identity_merge_allowed": False,
    "discovery_or_ranking_allowed": False,
    "canonical_write_allowed": False,
    "outreach_allowed": False,
}
OUTPUT_AUTHORITY = {
    "live_x_capability_claimed": False,
    "canonical_person_or_employment_confirmed": False,
    "automatic_identity_merge_performed": False,
    "discovery_or_ranking_authorized": False,
    "canonical_write_authorized": False,
    "outreach_authorized": False,
}

_SHA_RE = re.compile(r"[0-9a-f]{64}")
_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_NUMERIC_ID_RE = re.compile(r"[1-9][0-9]{1,23}")
_TIMESTAMP_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z")
_ID_PATTERNS = {
    "experiment_id": re.compile(r"xstage2exp_[0-9a-f]{24}"),
    "task_id": re.compile(r"xstage2task_[0-9a-f]{24}"),
    "opaque_lead_ref": re.compile(r"xlead_[0-9a-f]{24}"),
    "collection_id": re.compile(r"xstage2collection_[0-9a-f]{24}"),
    "receipt_id": re.compile(r"xstage2call_[0-9a-f]{24}"),
    "source_record_id": re.compile(r"xstage2src_[0-9a-f]{24}"),
    "profile_snapshot_id": re.compile(r"xstage2profile_[0-9a-f]{24}"),
    "post_id": re.compile(r"xstage2post_[0-9a-f]{24}"),
    "quarantine_id": re.compile(r"xstage2quarantine_[0-9a-f]{24}"),
    "incident_id": re.compile(r"xstage2incident_[0-9a-f]{24}"),
    "manifest_id": re.compile(r"xstage2expect_[0-9a-f]{24}"),
    "expectation_row_id": re.compile(r"xstage2expectrow_[0-9a-f]{24}"),
    "evaluation_id": re.compile(r"xstage2eval_[0-9a-f]{24}"),
}


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"), parse_constant=_reject_nonfinite)


def _reject_nonfinite(token: str) -> None:
    raise ValueError(f"nonfinite JSON token: {token}")


def canonical_json(value: Any) -> str:
    return json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _schema_preflight(value: Any, schema_version: str) -> list[str]:
    try:
        assert_schema_valid(value, SCHEMA_FILENAMES[schema_version])
    except (KeyError, MiniDraft202012Error, OSError, UnicodeError, ValueError) as error:
        return [f"$.schema: {type(error).__name__}"]
    return []


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_sha(value: Any) -> bool:
    return isinstance(value, str) and _SHA_RE.fullmatch(value) is not None and value != "0" * 64


def _is_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or _TIMESTAMP_RE.fullmatch(value) is None:
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return False
    return True


def _is_scalar_text(value: Any, *, minimum: int = 0, maximum: int = 4096) -> bool:
    if not isinstance(value, str) or not minimum <= len(value) <= maximum:
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _exact_keys(value: Any, keys: set[str], path: str, errors: list[str]) -> bool:
    if not isinstance(value, dict):
        errors.append(f"{path}: must be object")
        return False
    missing = sorted(keys - set(value))
    extra = sorted(set(value) - keys)
    if missing:
        errors.append(f"{path}: missing keys {missing}")
    if extra:
        errors.append(f"{path}: unexpected keys {extra}")
    return not missing and not extra


def _scan_json(value: Any, *, max_depth: int, max_nodes: int) -> list[str]:
    pending: list[tuple[Any, int]] = [(value, 1)]
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > max_nodes:
            return ["validation_node_ceiling_exceeded"]
        if depth > max_depth:
            return ["validation_depth_ceiling_exceeded"]
        if isinstance(item, dict):
            for key, child in item.items():
                if not _is_scalar_text(key, maximum=512):
                    return ["invalid_object_key"]
                pending.append((child, depth + 1))
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
        elif isinstance(item, str):
            if not _is_scalar_text(item, maximum=2_000_000):
                return ["invalid_string"]
        elif isinstance(item, float) and not math.isfinite(item):
            return ["nonfinite_number"]
        elif item is not None and not isinstance(item, (str, int, float, bool)):
            return ["non_json_value"]
    return []


def _bounded(value: Any) -> list[str]:
    errors = _scan_json(
        value,
        max_depth=TECHNICAL_LIMITS["max_validation_depth"],
        max_nodes=TECHNICAL_LIMITS["max_validation_nodes"],
    )
    if errors:
        return errors
    try:
        if len(canonical_json(value).encode("utf-8")) > TECHNICAL_LIMITS["max_canonical_bytes"]:
            return ["canonical_byte_ceiling_exceeded"]
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError):
        return ["not_canonical_json"]
    return []


def _registry_fields(registry: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(field["field_id"] for field in registry["fields"])


def validate_field_registry(registry: Any) -> list[str]:
    errors = _bounded(registry)
    if errors:
        return [f"$.validation: {error}" for error in errors]
    keys = {
        "schema_version",
        "registry_version",
        "field_state_values",
        "fields",
        "source_bound_profile_required_fields",
        "source_bound_profile_rule",
        "source_bound_profile_absence_rule",
        "authority",
    }
    if not _exact_keys(registry, keys, "$", errors):
        return errors
    if registry["schema_version"] != FIELD_REGISTRY_SCHEMA_VERSION:
        errors.append("$.schema_version: unsupported")
    if registry["registry_version"] != FIELD_REGISTRY_VERSION:
        errors.append("$.registry_version: unsupported")
    if registry["field_state_values"] != list(FIELD_STATES):
        errors.append("$.field_state_values: closed ordering mismatch")
    fields = registry["fields"] if isinstance(registry["fields"], list) else []
    if not fields:
        errors.append("$.fields: must be non-empty")
    seen: set[str] = set()
    for index, field in enumerate(fields):
        path = f"$.fields[{index}]"
        if not _exact_keys(field, {"field_id", "object_kind", "value_kind", "source_requirement"}, path, errors):
            continue
        field_id = field["field_id"]
        if not isinstance(field_id, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,63}", field_id) is None:
            errors.append(f"{path}.field_id: invalid")
        elif field_id in seen:
            errors.append(f"{path}.field_id: duplicate")
        else:
            seen.add(field_id)
        if field["object_kind"] not in {"profile", "post"}:
            errors.append(f"{path}.object_kind: unsupported")
        if not _is_scalar_text(field["value_kind"], minimum=1, maximum=64):
            errors.append(f"{path}.value_kind: invalid")
        if field["source_requirement"] not in {
            "same_replayable_profile_record",
            "derived_from_exact_bio_text",
            "same_replayable_post_record",
        }:
            errors.append(f"{path}.source_requirement: unsupported")
    observed_descriptors = tuple(
        (
            field.get("field_id"),
            field.get("object_kind"),
            field.get("value_kind"),
            field.get("source_requirement"),
        )
        for field in fields
        if isinstance(field, dict)
    )
    if observed_descriptors != CANONICAL_FIELD_DESCRIPTORS:
        errors.append("$.fields: v1 canonical 14 descriptors/order mismatch; use a new registry version")
    required = registry["source_bound_profile_required_fields"]
    if required != list(PROFILE_REQUIRED_FIELDS) or any(field not in seen for field in required):
        errors.append("$.source_bound_profile_required_fields: canonical list mismatch")
    if registry["source_bound_profile_rule"] != "single_replayable_raw_provider_record":
        errors.append("$.source_bound_profile_rule: unsupported")
    if registry["source_bound_profile_absence_rule"] != "bio_text_bio_sha256_bio_content_version_all_absent":
        errors.append("$.source_bound_profile_absence_rule: unsupported")
    if registry["authority"] != {
        "identity_merge_authorized": False,
        "employment_confirmation_authorized": False,
        "discovery_or_ranking_authorized": False,
        "canonical_write_authorized": False,
        "outreach_authorized": False,
    }:
        errors.append("$.authority: zero authority required")
    return errors


def _id(prefix: str, value: Any) -> str:
    return f"{prefix}_{canonical_sha256(value)[:24]}"


def _task_identity(task: Mapping[str, Any]) -> dict[str, Any]:
    return {key: copy.deepcopy(task[key]) for key in task if key != "task_id"}


def _task_id(task: Mapping[str, Any]) -> str:
    return _id("xstage2task", _task_identity(task))


def _field_state_map(states: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    return {str(item["field_id"]): str(item["state"]) for item in states}


def _post_fields(registry_fields: Sequence[str]) -> tuple[str, ...]:
    return tuple(field for field in registry_fields if field not in PROFILE_REQUIRED_FIELDS)


def _collection_identity_payload(collection: Mapping[str, Any]) -> dict[str, Any]:
    """Bind every material collection field without creating an id cycle."""

    return {key: copy.deepcopy(value) for key, value in collection.items() if key != "collection_id"}


def _collection_id(collection: Mapping[str, Any]) -> str:
    return _id("xstage2collection", _collection_identity_payload(collection))


def _validate_task(task: Any, *, registry_fields: tuple[str, ...], path: str, errors: list[str]) -> None:
    keys = {
        "task_id",
        "opaque_lead_ref",
        "candidate_row_sha256",
        "lookup_handle",
        "reported_platform_user_id",
        "reported_platform_user_id_status",
        "requested_field_ids",
        "tool_policy",
        "source_receipt_required",
        "authority",
    }
    if not _exact_keys(task, keys, path, errors):
        return
    if not isinstance(task["task_id"], str) or _ID_PATTERNS["task_id"].fullmatch(task["task_id"]) is None:
        errors.append(f"{path}.task_id: invalid")
    elif task["task_id"] != _task_id(task):
        errors.append(f"{path}.task_id: identity mismatch")
    if (
        not isinstance(task["opaque_lead_ref"], str)
        or _ID_PATTERNS["opaque_lead_ref"].fullmatch(task["opaque_lead_ref"]) is None
    ):
        errors.append(f"{path}.opaque_lead_ref: invalid")
    if not _is_sha(task["candidate_row_sha256"]):
        errors.append(f"{path}.candidate_row_sha256: invalid")
    if not isinstance(task["lookup_handle"], str) or _HANDLE_RE.fullmatch(task["lookup_handle"]) is None:
        errors.append(f"{path}.lookup_handle: invalid")
    reported = task["reported_platform_user_id"]
    status = task["reported_platform_user_id_status"]
    if reported is None:
        if status != "absent":
            errors.append(f"{path}.reported_platform_user_id_status: must be absent")
    elif (
        not isinstance(reported, str)
        or _NUMERIC_ID_RE.fullmatch(reported) is None
        or status != "model_mediated_unverified"
    ):
        errors.append(f"{path}.reported_platform_user_id: diagnostic binding invalid")
    if task["requested_field_ids"] != list(registry_fields):
        errors.append(f"{path}.requested_field_ids: must equal registry order")
    tool_policy = task["tool_policy"]
    if not _exact_keys(tool_policy, {"allowed_tools", "fallback_allowed"}, f"{path}.tool_policy", errors):
        pass
    elif tool_policy != {"allowed_tools": ["x_user_search", "x_thread_fetch"], "fallback_allowed": False}:
        errors.append(f"{path}.tool_policy: closed native-X policy required")
    if task["source_receipt_required"] is not True or task["authority"] != REQUEST_AUTHORITY:
        errors.append(f"{path}.authority: source receipt and zero authority required")


def validate_selection_manifest(manifest: Any) -> list[str]:
    errors = _bounded(manifest)
    if errors:
        return [f"$.validation: {error}" for error in errors]
    keys = {"schema_version", "selection_id", "source_artifact", "selected_leads"}
    if not _exact_keys(manifest, keys, "$", errors):
        return errors
    if manifest["schema_version"] != SELECTION_MANIFEST_VERSION:
        errors.append("$.schema_version: unsupported")
    source = manifest["source_artifact"]
    if _exact_keys(source, {"schema_version", "artifact_sha256"}, "$.source_artifact", errors):
        if not _is_scalar_text(source["schema_version"], minimum=1, maximum=128):
            errors.append("$.source_artifact.schema_version: invalid")
        if not _is_sha(source["artifact_sha256"]):
            errors.append("$.source_artifact.artifact_sha256: invalid")
    leads = manifest["selected_leads"] if isinstance(manifest["selected_leads"], list) else []
    if not leads or len(leads) > TECHNICAL_LIMITS["max_tasks"]:
        errors.append("$.selected_leads: technical denominator boundary violated")
    lead_keys = {
        "opaque_lead_ref",
        "candidate_row_sha256",
        "lookup_handle",
        "reported_platform_user_id",
        "reported_platform_user_id_status",
    }
    seen_refs: set[str] = set()
    seen_rows: set[str] = set()
    seen_handles: set[str] = set()
    for index, lead in enumerate(leads):
        path = f"$.selected_leads[{index}]"
        if not _exact_keys(lead, lead_keys, path, errors):
            continue
        ref = lead["opaque_lead_ref"]
        row_sha = lead["candidate_row_sha256"]
        handle = lead["lookup_handle"]
        if not isinstance(ref, str) or _ID_PATTERNS["opaque_lead_ref"].fullmatch(ref) is None:
            errors.append(f"{path}.opaque_lead_ref: invalid")
        if not _is_sha(row_sha):
            errors.append(f"{path}.candidate_row_sha256: invalid")
        if not isinstance(handle, str) or _HANDLE_RE.fullmatch(handle) is None:
            errors.append(f"{path}.lookup_handle: invalid")
        handle_key = handle.casefold() if isinstance(handle, str) else ""
        if ref in seen_refs or row_sha in seen_rows or handle_key in seen_handles:
            errors.append(f"{path}: duplicate external selection denominator")
        seen_refs.add(ref)
        seen_rows.add(row_sha)
        seen_handles.add(handle_key)
        reported = lead["reported_platform_user_id"]
        status = lead["reported_platform_user_id_status"]
        if reported is None:
            if status != "absent":
                errors.append(f"{path}.reported_platform_user_id_status: invalid")
        elif (
            not isinstance(reported, str)
            or _NUMERIC_ID_RE.fullmatch(reported) is None
            or status != "model_mediated_unverified"
        ):
            errors.append(f"{path}.reported_platform_user_id: invalid diagnostic")
    expected_selection_id = _id(
        "xstage2selection",
        {"source_artifact": source, "selected_leads_sha256": canonical_sha256(leads)},
    )
    if manifest["selection_id"] != expected_selection_id:
        errors.append("$.selection_id: identity mismatch")
    return errors


def _scenario_semantics(
    task: Mapping[str, Any],
    scenario_id: str,
    registry_fields: Sequence[str],
) -> dict[str, Any]:
    exact_states = {field: "present_exact" for field in registry_fields}
    exact_states["bounded_excerpt"] = "present_bounded"
    conflict_states = {field: "absent" for field in registry_fields}
    for field in PROFILE_REQUIRED_FIELDS:
        conflict_states[field] = "present_exact"
    conflict_states["platform_user_id"] = "unverified"
    rename_states = {field: "absent" for field in registry_fields}
    for field in PROFILE_REQUIRED_FIELDS:
        rename_states[field] = "present_exact"
    specifications = {
        "exact_profile_and_same_account_post": {
            "expected_terminal_status": "completed",
            "expected_error_codes": [],
            "expected_quarantine_reason": None,
            "expected_field_states": _field_states(registry_fields, exact_states),
            "expected_profile_count": 1,
            "expected_post_count": 1,
            "expected_source_record_kinds": ["profile", "post"],
        },
        "conflicting_platform_user_ids": {
            "expected_terminal_status": "quarantined",
            "expected_error_codes": ["conflicting_platform_user_ids"],
            "expected_quarantine_reason": "conflicting_platform_user_ids",
            "expected_field_states": _field_states(registry_fields, conflict_states),
            "expected_profile_count": 0,
            "expected_post_count": 0,
            "expected_source_record_kinds": ["profile"],
        },
        "handle_rename_requires_review": {
            "expected_terminal_status": "quarantined",
            "expected_error_codes": ["handle_rename_requires_review"],
            "expected_quarantine_reason": "handle_rename_requires_review",
            "expected_field_states": _field_states(registry_fields, rename_states),
            "expected_profile_count": 0,
            "expected_post_count": 0,
            "expected_source_record_kinds": ["profile"],
        },
        "metadata_only_source_payload_unavailable": {
            "expected_terminal_status": "failed",
            "expected_error_codes": ["source_payload_unavailable"],
            "expected_quarantine_reason": None,
            "expected_field_states": _field_states(
                registry_fields,
                {field: "unverified" for field in registry_fields},
            ),
            "expected_profile_count": 0,
            "expected_post_count": 0,
            "expected_source_record_kinds": ["tool_metadata_only"],
        },
    }
    if scenario_id not in specifications:
        raise ValueError("fixture_scenario_id_invalid")
    return {
        "task_id": task["task_id"],
        "scenario_id": scenario_id,
        **copy.deepcopy(specifications[scenario_id]),
    }


def _build_scenario_manifest(
    tasks: Sequence[Mapping[str, Any]],
    registry_fields: Sequence[str],
) -> dict[str, Any]:
    if not tasks:
        raise ValueError("fixture_scenario_denominator_invalid")
    scenario_ids = [FIXTURE_SCENARIO_IDS[index % len(FIXTURE_SCENARIO_IDS)] for index in range(len(tasks))]
    rows = [
        _scenario_semantics(task, scenario_id, registry_fields)
        for task, scenario_id in zip(tasks, scenario_ids, strict=True)
    ]
    body = {"manifest_version": SCENARIO_MANIFEST_VERSION, "rows": rows}
    return {
        **body,
        "scenario_manifest_sha256": canonical_sha256(body),
        "expectation_semantics_sha256": canonical_sha256(rows),
    }


def _validate_scenario_manifest(
    manifest: Any,
    *,
    tasks: Sequence[Mapping[str, Any]],
    registry_fields: Sequence[str],
    path: str,
    errors: list[str],
) -> None:
    keys = {
        "manifest_version",
        "scenario_manifest_sha256",
        "expectation_semantics_sha256",
        "rows",
    }
    if not _exact_keys(manifest, keys, path, errors):
        return
    if manifest["manifest_version"] != SCENARIO_MANIFEST_VERSION:
        errors.append(f"{path}.manifest_version: unsupported")
    rows = manifest["rows"] if isinstance(manifest["rows"], list) else []
    if len(rows) != len(tasks):
        errors.append(f"{path}.rows: task denominator mismatch")
        return
    task_map = {task["task_id"]: task for task in tasks}
    seen_tasks: set[str] = set()
    row_keys = {
        "task_id",
        "scenario_id",
        "expected_terminal_status",
        "expected_error_codes",
        "expected_quarantine_reason",
        "expected_field_states",
        "expected_profile_count",
        "expected_post_count",
        "expected_source_record_kinds",
    }
    for index, row in enumerate(rows):
        row_path = f"{path}.rows[{index}]"
        if not _exact_keys(row, row_keys, row_path, errors):
            continue
        task_id = row["task_id"]
        scenario_id = row["scenario_id"]
        if task_id not in task_map or task_id in seen_tasks:
            errors.append(f"{row_path}.task_id: unknown or duplicate")
            continue
        seen_tasks.add(task_id)
        if not isinstance(scenario_id, str) or scenario_id not in FIXTURE_SCENARIO_IDS:
            errors.append(f"{row_path}.scenario_id: closed value required")
            continue
        expected = _scenario_semantics(task_map[task_id], scenario_id, registry_fields)
        if canonical_json(row) != canonical_json(expected):
            errors.append(f"{row_path}: scenario semantics mismatch")
    body = {"manifest_version": manifest["manifest_version"], "rows": rows}
    if manifest["scenario_manifest_sha256"] != canonical_sha256(body):
        errors.append(f"{path}.scenario_manifest_sha256: mismatch")
    if manifest["expectation_semantics_sha256"] != canonical_sha256(rows):
        errors.append(f"{path}.expectation_semantics_sha256: mismatch")


def validate_experiment_request(request: Any, *, registry: Any, selection_manifest: Any) -> list[str]:
    errors = _bounded(request)
    if errors:
        return [f"$.validation: {error}" for error in errors]
    schema_errors = _schema_preflight(request, EXPERIMENT_SCHEMA_VERSION)
    if schema_errors:
        return schema_errors
    if validate_field_registry(registry):
        return ["$.field_registry: invalid"]
    if validate_selection_manifest(selection_manifest):
        return ["$.selection_manifest: invalid"]
    keys = {
        "schema_version",
        "execution_mode",
        "experiment_id",
        "target",
        "field_registry",
        "normalization_contract_version",
        "source_manifest",
        "fixture_scenario_manifest",
        "tasks",
        "technical_limits",
        "retention_policy",
        "authority",
    }
    if not _exact_keys(request, keys, "$", errors):
        return errors
    if request["schema_version"] != EXPERIMENT_SCHEMA_VERSION or request["execution_mode"] != EXECUTION_MODE:
        errors.append("$.schema_version: offline v1 required")
    target = request["target"]
    if _exact_keys(target, {"lab_id", "frozen_from", "frozen_to"}, "$.target", errors):
        if not isinstance(target["lab_id"], str) or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", target["lab_id"]) is None:
            errors.append("$.target.lab_id: invalid")
        if not _is_timestamp(target["frozen_from"]) or not _is_timestamp(target["frozen_to"]):
            errors.append("$.target: invalid timestamp")
        elif target["frozen_from"] >= target["frozen_to"]:
            errors.append("$.target: invalid window")
    registry_binding = request["field_registry"]
    expected_registry_binding = {
        "registry_version": FIELD_REGISTRY_VERSION,
        "registry_sha256": canonical_sha256(registry),
    }
    if registry_binding != expected_registry_binding:
        errors.append("$.field_registry: binding mismatch")
    if request["normalization_contract_version"] != NORMALIZATION_VERSION:
        errors.append("$.normalization_contract_version: unsupported")
    tasks = request["tasks"] if isinstance(request["tasks"], list) else []
    if not tasks or len(tasks) > TECHNICAL_LIMITS["max_tasks"]:
        errors.append("$.tasks: technical task boundary violated")
    registry_fields = _registry_fields(registry)
    seen_tasks: set[str] = set()
    seen_leads: set[str] = set()
    seen_candidate_rows: set[str] = set()
    seen_handles: set[str] = set()
    for index, task in enumerate(tasks):
        _validate_task(task, registry_fields=registry_fields, path=f"$.tasks[{index}]", errors=errors)
        if isinstance(task, dict):
            if task.get("task_id") in seen_tasks:
                errors.append(f"$.tasks[{index}].task_id: duplicate")
            seen_tasks.add(task.get("task_id"))
            if task.get("opaque_lead_ref") in seen_leads:
                errors.append(f"$.tasks[{index}].opaque_lead_ref: duplicate")
            seen_leads.add(task.get("opaque_lead_ref"))
            if task.get("candidate_row_sha256") in seen_candidate_rows:
                errors.append(f"$.tasks[{index}].candidate_row_sha256: duplicate denominator")
            seen_candidate_rows.add(task.get("candidate_row_sha256"))
            handle_key = task.get("lookup_handle", "").casefold() if isinstance(task.get("lookup_handle"), str) else ""
            if handle_key in seen_handles:
                errors.append(f"$.tasks[{index}].lookup_handle: duplicate account denominator")
            seen_handles.add(handle_key)
    source_manifest = request["source_manifest"]
    selected_fields = (
        "opaque_lead_ref",
        "candidate_row_sha256",
        "lookup_handle",
        "reported_platform_user_id",
        "reported_platform_user_id_status",
    )
    selected = [{field: task.get(field) for field in selected_fields} for task in tasks if isinstance(task, dict)]
    selected_from_manifest = [
        {field: lead[field] for field in selected_fields} for lead in selection_manifest["selected_leads"]
    ]
    expected_manifest = {
        "selection_manifest_version": SELECTION_MANIFEST_VERSION,
        "selection_manifest_sha256": canonical_sha256(selection_manifest),
        "selection_id": selection_manifest["selection_id"],
        "selected_lead_count": len(tasks),
        "selected_leads_sha256": canonical_sha256(selected),
    }
    if source_manifest != expected_manifest or selected != selected_from_manifest:
        errors.append("$.source_manifest: denominator or digest mismatch")
    _validate_scenario_manifest(
        request["fixture_scenario_manifest"],
        tasks=tasks,
        registry_fields=registry_fields,
        path="$.fixture_scenario_manifest",
        errors=errors,
    )
    expected_experiment_id = _id(
        "xstage2exp",
        {
            "target": target,
            "field_registry": registry_binding,
            "normalization_contract_version": request["normalization_contract_version"],
            "source_manifest": source_manifest,
            "fixture_scenario_manifest": request["fixture_scenario_manifest"],
            "task_ids": [task.get("task_id") for task in tasks if isinstance(task, dict)],
        },
    )
    if request["experiment_id"] != expected_experiment_id:
        errors.append("$.experiment_id: identity mismatch")
    if request["technical_limits"] != TECHNICAL_LIMITS:
        errors.append("$.technical_limits: immutable technical ceilings required")
    if request["retention_policy"] != {
        "raw_evidence_storage": "private_owner_only",
        "retention_scope": "synthetic_fixture_simulation_only",
        "directory_mode": "0700",
        "file_mode": "0600",
        "ttl_seconds": TECHNICAL_LIMITS["raw_evidence_ttl_seconds"],
        "deletion_receipt_required": True,
        "live_reuse_allowed": False,
        "promotion_eligible": False,
    }:
        errors.append("$.retention_policy: owner-only TTL contract required")
    if request["authority"] != REQUEST_AUTHORITY:
        errors.append("$.authority: zero authority required")
    return errors


def _validate_field_states(
    states: Any,
    *,
    requested_fields: tuple[str, ...],
    path: str,
    errors: list[str],
) -> dict[str, str]:
    if not isinstance(states, list) or len(states) != len(requested_fields):
        errors.append(f"{path}: terminal field denominator mismatch")
        return {}
    values: dict[str, str] = {}
    for index, item in enumerate(states):
        item_path = f"{path}[{index}]"
        if not _exact_keys(item, {"field_id", "state"}, item_path, errors):
            continue
        field_id = item["field_id"]
        state = item["state"]
        if field_id in values or field_id not in requested_fields:
            errors.append(f"{item_path}.field_id: duplicate or unknown")
        elif state not in FIELD_STATES:
            errors.append(f"{item_path}.state: unsupported")
        else:
            values[field_id] = state
    if list(values) != list(requested_fields):
        errors.append(f"{path}: registry ordering mismatch")
    return values


def _source_record_errors(record: Any, *, task_ids: set[str]) -> list[str]:
    errors: list[str] = []
    keys = {
        "source_record_id",
        "task_id",
        "call_receipt_id",
        "record_kind",
        "provider_path",
        "payload_visibility",
        "replayable",
        "raw_record",
        "raw_record_sha256",
        "observed_at",
    }
    if not _exact_keys(record, keys, "$", errors):
        return errors
    if record["task_id"] not in task_ids:
        errors.append("$.task_id: unknown")
    if (
        not isinstance(record["source_record_id"], str)
        or _ID_PATTERNS["source_record_id"].fullmatch(record["source_record_id"]) is None
    ):
        errors.append("$.source_record_id: invalid")
    if record["record_kind"] not in {"profile", "post", "tool_metadata_only"}:
        errors.append("$.record_kind: unsupported")
    if record["payload_visibility"] not in {"full_fixture_payload", "metadata_only"}:
        errors.append("$.payload_visibility: unsupported")
    if not isinstance(record["replayable"], bool):
        errors.append("$.replayable: must be boolean")
    if not _is_scalar_text(record["provider_path"], minimum=1, maximum=200):
        errors.append("$.provider_path: invalid")
    if not _is_timestamp(record["observed_at"]):
        errors.append("$.observed_at: invalid")
    if not isinstance(record["raw_record"], dict) or record["raw_record_sha256"] != canonical_sha256(
        record["raw_record"]
    ):
        errors.append("$.raw_record_sha256: mismatch")
    if len(canonical_json(record["raw_record"]).encode("utf-8")) > TECHNICAL_LIMITS["max_raw_record_bytes"]:
        errors.append("$.raw_record: byte ceiling exceeded")
    expected_source_id = _id(
        "xstage2src",
        {
            "task_id": record["task_id"],
            "record_kind": record["record_kind"],
            "provider_path": record["provider_path"],
            "raw_record_sha256": record["raw_record_sha256"],
        },
    )
    if record["source_record_id"] != expected_source_id:
        errors.append("$.source_record_id: identity mismatch")
    if record["payload_visibility"] == "full_fixture_payload" and (
        record["record_kind"] not in {"profile", "post"} or record["replayable"] is not True
    ):
        errors.append("$: full fixture payload must be a replayable profile/Post")
    if record["payload_visibility"] == "metadata_only" and (
        record["record_kind"] != "tool_metadata_only" or record["replayable"] is not False
    ):
        errors.append("$: metadata-only source must be non-replayable tool metadata")
    return errors


def _derived_task_field_states(
    sources: Sequence[Mapping[str, Any]],
    *,
    registry_fields: Sequence[str],
) -> list[dict[str, str]]:
    """Derive terminal field states only from consumed raw provider records."""

    values = {field: "unverified" for field in registry_fields}
    profile_sources = [
        source
        for source in sources
        if source.get("record_kind") == "profile"
        and source.get("payload_visibility") == "full_fixture_payload"
        and source.get("replayable") is True
        and isinstance(source.get("raw_record"), dict)
    ]
    post_sources = [
        source
        for source in sources
        if source.get("record_kind") == "post"
        and source.get("payload_visibility") == "full_fixture_payload"
        and source.get("replayable") is True
        and isinstance(source.get("raw_record"), dict)
    ]
    if len(profile_sources) == 1:
        raw = profile_sources[0]["raw_record"]
        platform_ids = raw.get("platform_user_ids")
        if (
            isinstance(platform_ids, list)
            and len(platform_ids) == 1
            and isinstance(platform_ids[0], str)
            and _NUMERIC_ID_RE.fullmatch(platform_ids[0]) is not None
        ):
            values["platform_user_id"] = "present_exact"
        if isinstance(raw.get("current_handle"), str) and _HANDLE_RE.fullmatch(raw["current_handle"]):
            values["current_handle"] = "present_exact"
        if _is_scalar_text(raw.get("profile_url"), minimum=1, maximum=2048):
            values["profile_url"] = "present_exact"
        if _is_timestamp(raw.get("bio_observed_at")):
            values["bio_observed_at"] = "present_exact"
        bio = raw.get("bio_text")
        version = raw.get("bio_content_version")
        if bio is None and version is None:
            for field in PROFILE_OPTIONAL_BIO_FIELDS:
                values[field] = "absent"
        elif _is_scalar_text(bio, maximum=100_000) and _is_scalar_text(version, minimum=1, maximum=200):
            for field in PROFILE_OPTIONAL_BIO_FIELDS:
                values[field] = "present_exact"
    post_fields = _post_fields(registry_fields)
    if post_sources:
        validators = {
            "canonical_post_id": lambda value: isinstance(value, str) and _NUMERIC_ID_RE.fullmatch(value) is not None,
            "canonical_post_url": lambda value: _is_scalar_text(value, minimum=1, maximum=2048),
            "post_author_platform_user_id": lambda value: (
                isinstance(value, str) and _NUMERIC_ID_RE.fullmatch(value) is not None
            ),
            "post_author_handle": lambda value: isinstance(value, str) and _HANDLE_RE.fullmatch(value) is not None,
            "post_authored_at": _is_timestamp,
            "bounded_excerpt": lambda value: _is_scalar_text(value, minimum=1, maximum=280),
            "thread_relation": lambda value: value in THREAD_RELATIONS,
        }
        for field in post_fields:
            if all(validators[field](source["raw_record"].get(field)) for source in post_sources):
                values[field] = "present_bounded" if field == "bounded_excerpt" else "present_exact"
            else:
                values[field] = "unverified"
    elif profile_sources and all(
        source["raw_record"].get("post_fields_explicitly_absent") is True for source in profile_sources
    ):
        for field in post_fields:
            values[field] = "absent"
    return _field_states(registry_fields, values)


def _derived_task_guardrail(
    task: Mapping[str, Any],
    sources: Sequence[Mapping[str, Any]],
    *,
    cross_handle_conflict: bool,
) -> tuple[str | None, list[str]]:
    full_sources = [source for source in sources if source.get("payload_visibility") == "full_fixture_payload"]
    if not full_sources:
        return "source_payload_unavailable", []
    profiles = [source for source in full_sources if source.get("record_kind") == "profile"]
    posts = [source for source in full_sources if source.get("record_kind") == "post"]
    if not profiles or not isinstance(profiles[0].get("raw_record"), dict):
        return "source_payload_unavailable", []
    if len(profiles) > 1:
        return "multiple_profile_sources", list(PROFILE_REQUIRED_FIELDS)
    profile_raw = profiles[0]["raw_record"]
    platform_ids = profile_raw.get("platform_user_ids")
    if (
        not isinstance(platform_ids, list)
        or len(platform_ids) != 1
        or not isinstance(platform_ids[0], str)
        or _NUMERIC_ID_RE.fullmatch(platform_ids[0]) is None
    ):
        return "conflicting_platform_user_ids", ["platform_user_id"]
    current_handle = profile_raw.get("current_handle")
    if not isinstance(current_handle, str) or not isinstance(task.get("lookup_handle"), str):
        return "handle_rename_requires_review", ["current_handle"]
    if cross_handle_conflict:
        return "platform_user_id_handle_conflict", ["platform_user_id", "current_handle"]
    if current_handle.casefold() != task["lookup_handle"].casefold():
        return "handle_rename_requires_review", ["current_handle"]
    for source in posts:
        raw = source.get("raw_record")
        if not isinstance(raw, dict) or (
            raw.get("post_author_platform_user_id") != platform_ids[0]
            or not isinstance(raw.get("post_author_handle"), str)
            or raw["post_author_handle"].casefold() != current_handle.casefold()
        ):
            return "cross_account_evidence", ["post_author_platform_user_id", "post_author_handle"]
    return None, []


def _profile_source_errors(
    profile: Any,
    *,
    task: Mapping[str, Any],
    source: Mapping[str, Any],
    states: Mapping[str, str],
) -> list[str]:
    errors: list[str] = []
    keys = {
        "profile_snapshot_id",
        "task_id",
        "source_record_id",
        "platform_user_id",
        "current_handle",
        "profile_url",
        "bio_text",
        "bio_sha256",
        "bio_observed_at",
        "bio_content_version",
        "source_binding_status",
        "reported_platform_user_id_diagnostic",
        "authority",
    }
    if not _exact_keys(profile, keys, "$", errors):
        return errors
    if profile["task_id"] != task["task_id"] or profile["source_record_id"] != source["source_record_id"]:
        errors.append("$: task/source mismatch")
    if (
        source["record_kind"] != "profile"
        or source["payload_visibility"] != "full_fixture_payload"
        or not source["replayable"]
    ):
        errors.append("$: profile source is not replayable full payload")
        return errors
    raw = source["raw_record"]
    expected_raw_keys = {
        "platform_user_ids",
        "current_handle",
        "profile_url",
        "bio_text",
        "bio_observed_at",
        "bio_content_version",
        "post_fields_explicitly_absent",
    }
    if not _exact_keys(raw, expected_raw_keys, "$.raw_record", errors):
        return errors
    platform_ids = raw["platform_user_ids"]
    if (
        not isinstance(platform_ids, list)
        or len(platform_ids) != 1
        or not isinstance(platform_ids[0], str)
        or _NUMERIC_ID_RE.fullmatch(platform_ids[0]) is None
    ):
        errors.append("$.raw_record.platform_user_ids: exactly one numeric id required")
        return errors
    if not isinstance(raw["current_handle"], str) or _HANDLE_RE.fullmatch(raw["current_handle"]) is None:
        errors.append("$.raw_record.current_handle: invalid")
        return errors
    if not _is_scalar_text(raw["profile_url"], minimum=1, maximum=2048):
        errors.append("$.raw_record.profile_url: invalid")
        return errors
    if not _is_timestamp(raw["bio_observed_at"]):
        errors.append("$.raw_record.bio_observed_at: invalid")
        return errors
    if raw["bio_observed_at"] != source["observed_at"]:
        errors.append("$.raw_record.bio_observed_at: must equal source observation time")
    if not isinstance(raw["post_fields_explicitly_absent"], bool):
        errors.append("$.raw_record.post_fields_explicitly_absent: boolean required")
    if raw["bio_text"] is not None and not _is_scalar_text(raw["bio_text"], maximum=100_000):
        errors.append("$.raw_record.bio_text: must be UTF-8 text or null")
        return errors
    if raw["bio_content_version"] is not None and not _is_scalar_text(
        raw["bio_content_version"], minimum=1, maximum=200
    ):
        errors.append("$.raw_record.bio_content_version: must be text or null")
        return errors
    if (raw["bio_text"] is None) != (raw["bio_content_version"] is None):
        errors.append("$.raw_record: Bio text/version absence must be atomic")
        return errors
    expected_values = {
        "platform_user_id": platform_ids[0],
        "current_handle": raw["current_handle"],
        "profile_url": raw["profile_url"],
        "bio_text": raw["bio_text"],
        "bio_sha256": text_sha256(raw["bio_text"]) if isinstance(raw["bio_text"], str) else None,
        "bio_observed_at": raw["bio_observed_at"],
        "bio_content_version": raw["bio_content_version"],
    }
    if any(profile[field] != value for field, value in expected_values.items()):
        errors.append("$: profile values do not exactly replay one source record")
    if any(states.get(field) != "present_exact" for field in PROFILE_ALWAYS_EXACT_FIELDS):
        errors.append("$: source-bound account fields must be present_exact")
    expected_bio_state = "absent" if raw["bio_text"] is None else "present_exact"
    if any(states.get(field) != expected_bio_state for field in PROFILE_OPTIONAL_BIO_FIELDS):
        errors.append("$: Bio text/hash/version must be atomically exact or explicitly absent")
    if not isinstance(profile["current_handle"], str) or not isinstance(task["lookup_handle"], str):
        errors.append("$.current_handle: invalid")
        return errors
    if profile["current_handle"].casefold() != task["lookup_handle"].casefold():
        errors.append("$.current_handle: handle rename requires quarantine")
    if profile["profile_url"] != f"https://profiles.invalid/x/{profile['current_handle'].casefold()}":
        errors.append("$.profile_url: fixture URL not canonical")
    if profile["bio_text"] is None:
        if profile["bio_sha256"] is not None or profile["bio_content_version"] is not None:
            errors.append("$: absent Bio text/hash/version must all be null")
    elif not isinstance(profile["bio_text"], str) or profile["bio_sha256"] != text_sha256(profile["bio_text"]):
        errors.append("$.bio_sha256: mismatch")
    if profile["source_binding_status"] != "replay_bound_exact":
        errors.append("$.source_binding_status: exact replay required")
    diagnostic = profile["reported_platform_user_id_diagnostic"]
    expected_diagnostic = {
        "value": task["reported_platform_user_id"],
        "status": task["reported_platform_user_id_status"],
        "comparison": "absent"
        if task["reported_platform_user_id"] is None
        else "same_diagnostic_only"
        if task["reported_platform_user_id"] == profile["platform_user_id"]
        else "different_diagnostic_only",
        "identity_authority": False,
    }
    if diagnostic != expected_diagnostic:
        errors.append("$.reported_platform_user_id_diagnostic: must remain diagnostic")
    if profile["authority"] != OUTPUT_AUTHORITY:
        errors.append("$.authority: zero authority required")
    expected_id = _id(
        "xstage2profile",
        {
            "task_id": profile["task_id"],
            "source_record_id": profile["source_record_id"],
            "platform_user_id": profile["platform_user_id"],
            "bio_sha256": profile["bio_sha256"],
            "bio_observed_at": profile["bio_observed_at"],
        },
    )
    if profile["profile_snapshot_id"] != expected_id:
        errors.append("$.profile_snapshot_id: identity mismatch")
    return errors


def validate_profile_source_binding(
    profile: Any,
    *,
    task: Mapping[str, Any],
    source: Mapping[str, Any],
    field_states: Sequence[Mapping[str, Any]],
) -> list[str]:
    """Validate one profile snapshot without granting identity or product authority."""

    try:
        states = _field_state_map(field_states)
        return _profile_source_errors(profile, task=task, source=source, states=states)
    except (AttributeError, IndexError, KeyError, OverflowError, RecursionError, TypeError, UnicodeError, ValueError):
        return ["$: malformed_profile_source_binding"]


def validate_collection(collection: Any, *, request: Any, registry: Any, selection_manifest: Any) -> list[str]:
    errors = _bounded(collection)
    if errors:
        return [f"$.validation: {error}" for error in errors]
    schema_errors = _schema_preflight(collection, COLLECTION_SCHEMA_VERSION)
    if schema_errors:
        return schema_errors
    if validate_experiment_request(request, registry=registry, selection_manifest=selection_manifest):
        return ["$.request: invalid"]
    keys = {
        "schema_version",
        "execution_mode",
        "collection_id",
        "experiment_id",
        "request_sha256",
        "field_registry",
        "terminal_summary",
        "task_rows",
        "call_receipts",
        "source_records",
        "profiles",
        "posts",
        "quarantine",
        "incidents",
        "retention",
        "assertions",
        "canonical_writes",
        "outreach_actions",
        "authority",
    }
    if not _exact_keys(collection, keys, "$", errors):
        return errors
    if collection["schema_version"] != COLLECTION_SCHEMA_VERSION or collection["execution_mode"] != EXECUTION_MODE:
        errors.append("$.schema_version: offline v1 required")
    if collection["experiment_id"] != request["experiment_id"] or collection["request_sha256"] != canonical_sha256(
        request
    ):
        errors.append("$: request binding mismatch")
    if collection["field_registry"] != request["field_registry"]:
        errors.append("$.field_registry: binding mismatch")
    tasks = {task["task_id"]: task for task in request["tasks"]}
    requested_fields = _registry_fields(registry)
    task_rows = collection["task_rows"] if isinstance(collection["task_rows"], list) else []
    if len(task_rows) != len(tasks):
        errors.append("$.task_rows: terminal denominator mismatch")
    rows: dict[str, Mapping[str, Any]] = {}
    row_keys = {
        "task_id",
        "opaque_lead_ref",
        "candidate_row_sha256",
        "lookup_handle",
        "status",
        "call_receipt_ids",
        "source_record_ids",
        "profile_snapshot_ids",
        "post_ids",
        "quarantine_ids",
        "incident_ids",
        "field_states",
        "error_codes",
    }
    state_maps: dict[str, dict[str, str]] = {}
    for index, row in enumerate(task_rows):
        path = f"$.task_rows[{index}]"
        if not _exact_keys(row, row_keys, path, errors):
            continue
        task_id = row["task_id"]
        if task_id not in tasks or task_id in rows:
            errors.append(f"{path}.task_id: unknown or duplicate")
            continue
        task = tasks[task_id]
        rows[task_id] = row
        if any(row[field] != task[field] for field in ("opaque_lead_ref", "candidate_row_sha256", "lookup_handle")):
            errors.append(f"{path}: lead binding mismatch")
        if row["status"] not in TERMINAL_STATUSES:
            errors.append(f"{path}.status: terminal status required")
        for list_field in (
            "call_receipt_ids",
            "source_record_ids",
            "profile_snapshot_ids",
            "post_ids",
            "quarantine_ids",
            "incident_ids",
            "error_codes",
        ):
            if not isinstance(row[list_field], list) or len(row[list_field]) != len(set(row[list_field])):
                errors.append(f"{path}.{list_field}: unique list required")
        state_maps[task_id] = _validate_field_states(
            row["field_states"], requested_fields=requested_fields, path=f"{path}.field_states", errors=errors
        )
    receipts = collection["call_receipts"] if isinstance(collection["call_receipts"], list) else []
    receipt_map: dict[str, Mapping[str, Any]] = {}
    receipt_keys = {
        "receipt_id",
        "task_id",
        "tool_name",
        "status",
        "external_call_count",
        "source_record_ids",
        "started_at",
        "completed_at",
        "cost_status",
        "fallback_used",
    }
    for index, receipt in enumerate(receipts):
        path = f"$.call_receipts[{index}]"
        if not _exact_keys(receipt, receipt_keys, path, errors):
            continue
        receipt_id = receipt["receipt_id"]
        if (
            not isinstance(receipt_id, str)
            or _ID_PATTERNS["receipt_id"].fullmatch(receipt_id) is None
            or receipt_id in receipt_map
        ):
            errors.append(f"{path}.receipt_id: invalid or duplicate")
        else:
            receipt_map[receipt_id] = receipt
        if receipt["task_id"] not in tasks or receipt["tool_name"] not in {"x_user_search", "x_thread_fetch"}:
            errors.append(f"{path}: task/tool invalid")
        if (
            not isinstance(receipt["source_record_ids"], list)
            or not receipt["source_record_ids"]
            or len(receipt["source_record_ids"]) != len(set(receipt["source_record_ids"]))
        ):
            errors.append(f"{path}.source_record_ids: non-empty unique list required")
        if receipt["status"] != "fixture_replayed" or receipt["external_call_count"] != 0:
            errors.append(f"{path}: fixture call receipt cannot claim external execution")
        if receipt["cost_status"] != "not_applicable_offline_fixture" or receipt["fallback_used"] is not False:
            errors.append(f"{path}: cost/fallback invalid")
        if not _is_timestamp(receipt["started_at"]) or not _is_timestamp(receipt["completed_at"]):
            errors.append(f"{path}: invalid timestamps")
        elif receipt["started_at"] > receipt["completed_at"]:
            errors.append(f"{path}: invalid time ordering")
        expected_id = _id(
            "xstage2call",
            {
                "task_id": receipt["task_id"],
                "tool_name": receipt["tool_name"],
                "source_record_ids": receipt["source_record_ids"],
            },
        )
        if receipt_id != expected_id:
            errors.append(f"{path}.receipt_id: identity mismatch")
    source_records = collection["source_records"] if isinstance(collection["source_records"], list) else []
    if len(source_records) > TECHNICAL_LIMITS["max_source_records"]:
        errors.append("$.source_records: technical ceiling exceeded")
    source_map: dict[str, Mapping[str, Any]] = {}
    for index, record in enumerate(source_records):
        path = f"$.source_records[{index}]"
        record_errors = _source_record_errors(record, task_ids=set(tasks))
        errors.extend(f"{path}{error[1:]}" for error in record_errors)
        if isinstance(record, dict) and isinstance(record.get("source_record_id"), str):
            if record["source_record_id"] in source_map:
                errors.append(f"{path}.source_record_id: duplicate")
            source_map[record["source_record_id"]] = record
            receipt = receipt_map.get(record.get("call_receipt_id"))
            if (
                receipt is None
                or receipt.get("task_id") != record.get("task_id")
                or record["source_record_id"] not in receipt.get("source_record_ids", [])
            ):
                errors.append(f"{path}: call receipt binding missing")
            elif not (receipt["started_at"] <= record.get("observed_at", "") <= receipt["completed_at"]):
                errors.append(f"{path}.observed_at: outside call receipt window")
            raw = record.get("raw_record")
            if isinstance(raw, dict) and record.get("record_kind") == "profile":
                if raw.get("bio_observed_at") != record.get("observed_at"):
                    errors.append(f"{path}.observed_at: profile Bio observation mismatch")
            if isinstance(raw, dict) and record.get("record_kind") == "post":
                authored_at = raw.get("post_authored_at")
                if not _is_timestamp(authored_at) or authored_at > record.get("observed_at", ""):
                    errors.append(f"{path}.observed_at: Post authored time cannot exceed observation")

    canonical_post_sources: dict[str, str] = {}
    for source_id, source in source_map.items():
        if source.get("record_kind") != "post" or not isinstance(source.get("raw_record"), dict):
            continue
        canonical_post_id = source["raw_record"].get("canonical_post_id")
        if not isinstance(canonical_post_id, str) or _NUMERIC_ID_RE.fullmatch(canonical_post_id) is None:
            continue
        prior_source = canonical_post_sources.get(canonical_post_id)
        if prior_source is not None and prior_source != source_id:
            errors.append(f"$.source_records[{source_id}].canonical_post_id: duplicate collection object")
        else:
            canonical_post_sources[canonical_post_id] = source_id
    profiles = collection["profiles"] if isinstance(collection["profiles"], list) else []
    profile_map: dict[str, Mapping[str, Any]] = {}
    for index, profile in enumerate(profiles):
        path = f"$.profiles[{index}]"
        task = tasks.get(profile.get("task_id")) if isinstance(profile, dict) else None
        source = source_map.get(profile.get("source_record_id")) if isinstance(profile, dict) else None
        if task is None or source is None:
            errors.append(f"{path}: unknown task/source")
            continue
        profile_errors = validate_profile_source_binding(
            profile,
            task=task,
            source=source,
            field_states=next(
                (row["field_states"] for row in task_rows if row.get("task_id") == task["task_id"]),
                [],
            ),
        )
        errors.extend(f"{path}{error[1:]}" for error in profile_errors)
        profile_id = profile.get("profile_snapshot_id")
        if profile_id in profile_map:
            errors.append(f"{path}.profile_snapshot_id: duplicate")
        profile_map[profile_id] = profile
    post_map: dict[str, Mapping[str, Any]] = {}
    post_keys = {
        "post_id",
        "task_id",
        "source_record_id",
        "canonical_post_id",
        "canonical_post_url",
        "post_author_platform_user_id",
        "post_author_handle",
        "post_authored_at",
        "bounded_excerpt",
        "thread_relation",
        "field_states",
        "source_binding_status",
        "authority",
    }
    for index, post in enumerate(collection["posts"] if isinstance(collection["posts"], list) else []):
        path = f"$.posts[{index}]"
        if not _exact_keys(post, post_keys, path, errors):
            continue
        source = source_map.get(post["source_record_id"])
        task = tasks.get(post["task_id"])
        profile = next((item for item in profiles if item["task_id"] == post["task_id"]), None)
        if (
            source is None
            or task is None
            or source.get("task_id") != post["task_id"]
            or source["record_kind"] != "post"
            or source["payload_visibility"] != "full_fixture_payload"
            or not source["replayable"]
        ):
            errors.append(f"{path}: post source invalid")
            continue
        post_shape_valid = True
        if (
            not isinstance(post["canonical_post_id"], str)
            or _NUMERIC_ID_RE.fullmatch(post["canonical_post_id"]) is None
        ):
            errors.append(f"{path}.canonical_post_id: numeric string required")
            post_shape_valid = False
        if (
            not isinstance(post["post_author_platform_user_id"], str)
            or _NUMERIC_ID_RE.fullmatch(post["post_author_platform_user_id"]) is None
        ):
            errors.append(f"{path}.post_author_platform_user_id: numeric string required")
            post_shape_valid = False
        if not isinstance(post["post_author_handle"], str) or _HANDLE_RE.fullmatch(post["post_author_handle"]) is None:
            errors.append(f"{path}.post_author_handle: invalid")
            post_shape_valid = False
        if not _is_timestamp(post["post_authored_at"]):
            errors.append(f"{path}.post_authored_at: invalid")
            post_shape_valid = False
        if not _is_scalar_text(post["bounded_excerpt"], minimum=1, maximum=280):
            errors.append(f"{path}.bounded_excerpt: bounded UTF-8 text required")
            post_shape_valid = False
        if post["thread_relation"] not in THREAD_RELATIONS:
            errors.append(f"{path}.thread_relation: unsupported")
            post_shape_valid = False
        if post_shape_valid:
            expected_post_url = (
                f"https://posts.invalid/x/{post['post_author_handle'].casefold()}/status/{post['canonical_post_id']}"
            )
            if post["canonical_post_url"] != expected_post_url:
                errors.append(f"{path}.canonical_post_url: noncanonical fixture URL")
                post_shape_valid = False
        raw = source["raw_record"]
        expected_raw = {
            "canonical_post_id": post["canonical_post_id"],
            "canonical_post_url": post["canonical_post_url"],
            "post_author_platform_user_id": post["post_author_platform_user_id"],
            "post_author_handle": post["post_author_handle"],
            "post_authored_at": post["post_authored_at"],
            "bounded_excerpt": post["bounded_excerpt"],
            "thread_relation": post["thread_relation"],
        }
        if raw != expected_raw:
            errors.append(f"{path}: post values do not replay source")
        if not post_shape_valid:
            continue
        if profile is None or (
            post["post_author_platform_user_id"] != profile["platform_user_id"]
            or post["post_author_handle"].casefold() != profile["current_handle"].casefold()
        ):
            errors.append(f"{path}: cross-account Post evidence")
        expected_post_fields = _post_fields(requested_fields)
        post_states = _validate_field_states(
            post["field_states"],
            requested_fields=expected_post_fields,
            path=f"{path}.field_states",
            errors=errors,
        )
        if any(
            post_states.get(field) != ("present_bounded" if field == "bounded_excerpt" else "present_exact")
            for field in expected_post_fields
        ):
            errors.append(f"{path}.field_states: exact/bounded semantics invalid")
        if post["source_binding_status"] != "replay_bound_exact" or post["authority"] != OUTPUT_AUTHORITY:
            errors.append(f"{path}: binding/authority invalid")
        expected_post_id = _id(
            "xstage2post",
            {
                "task_id": post["task_id"],
                "source_record_id": post["source_record_id"],
                "canonical_post_id": post["canonical_post_id"],
            },
        )
        if post["post_id"] != expected_post_id:
            errors.append(f"{path}.post_id: identity mismatch")
        if post["post_id"] in post_map:
            errors.append(f"{path}.post_id: duplicate")
        else:
            post_map[post["post_id"]] = post
    quarantine = collection["quarantine"] if isinstance(collection["quarantine"], list) else []
    quarantine_map: dict[str, Mapping[str, Any]] = {}
    quarantine_keys = {
        "quarantine_id",
        "task_id",
        "reason_code",
        "source_record_ids",
        "field_ids",
        "resolution",
    }
    for index, item in enumerate(quarantine):
        path = f"$.quarantine[{index}]"
        if not _exact_keys(item, quarantine_keys, path, errors):
            continue
        if item["task_id"] not in tasks or item["reason_code"] not in QUARANTINE_REASON_CODES:
            errors.append(f"{path}: invalid quarantine")
        if (
            not isinstance(item["source_record_ids"], list)
            or not item["source_record_ids"]
            or len(item["source_record_ids"]) != len(set(item["source_record_ids"]))
            or any(
                source_id not in source_map or source_map[source_id]["task_id"] != item["task_id"]
                for source_id in item["source_record_ids"]
            )
        ):
            errors.append(f"{path}.source_record_ids: invalid task-bound source set")
        if (
            not isinstance(item["field_ids"], list)
            or not item["field_ids"]
            or len(item["field_ids"]) != len(set(item["field_ids"]))
            or any(field_id not in requested_fields for field_id in item["field_ids"])
        ):
            errors.append(f"{path}.field_ids: invalid")
        if item["resolution"] != "human_review_required":
            errors.append(f"{path}.resolution: invalid")
        expected_id = _id(
            "xstage2quarantine",
            {
                "task_id": item["task_id"],
                "reason_code": item["reason_code"],
                "source_record_ids": item["source_record_ids"],
            },
        )
        if item["quarantine_id"] != expected_id:
            errors.append(f"{path}.quarantine_id: identity mismatch")
        if item["quarantine_id"] in quarantine_map:
            errors.append(f"{path}.quarantine_id: duplicate")
        else:
            quarantine_map[item["quarantine_id"]] = item
    for task_id, row in rows.items():
        if set(row["call_receipt_ids"]) != {
            receipt_id for receipt_id, receipt in receipt_map.items() if receipt["task_id"] == task_id
        }:
            errors.append(f"$.task_rows[{task_id}]: call receipt closure mismatch")
        if set(row["source_record_ids"]) != {
            source_id for source_id, source in source_map.items() if source["task_id"] == task_id
        }:
            errors.append(f"$.task_rows[{task_id}]: source record closure mismatch")
        if set(row["profile_snapshot_ids"]) != {
            profile_id for profile_id, profile in profile_map.items() if profile["task_id"] == task_id
        }:
            errors.append(f"$.task_rows[{task_id}]: profile closure mismatch")
        if set(row["post_ids"]) != {post_id for post_id, post in post_map.items() if post["task_id"] == task_id}:
            errors.append(f"$.task_rows[{task_id}]: post closure mismatch")
        if set(row["quarantine_ids"]) != {
            quarantine_id for quarantine_id, item in quarantine_map.items() if item["task_id"] == task_id
        }:
            errors.append(f"$.task_rows[{task_id}]: quarantine closure mismatch")
        if row["status"] == "completed" and (len(row["profile_snapshot_ids"]) != 1 or row["quarantine_ids"]):
            errors.append(f"$.task_rows[{task_id}]: completed row requires one exact profile")
        if row["status"] == "quarantined" and not row["quarantine_ids"]:
            errors.append(f"$.task_rows[{task_id}]: quarantine missing")
        if row["status"] == "failed" and not row["error_codes"]:
            errors.append(f"$.task_rows[{task_id}]: terminal error missing")
        task_sources = [source for source in source_map.values() if source["task_id"] == task_id]
        expected_task_states = _derived_task_field_states(task_sources, registry_fields=requested_fields)
        if canonical_json(row["field_states"]) != canonical_json(expected_task_states):
            errors.append(f"$.task_rows[{task_id}].field_states: source-derived state mismatch")
        if task_sources and all(source["record_kind"] == "tool_metadata_only" for source in task_sources):
            if row["status"] != "failed" or row["error_codes"] != ["source_payload_unavailable"]:
                errors.append(f"$.task_rows[{task_id}]: metadata-only trace must fail closed")
            if any(state != "unverified" for state in state_maps.get(task_id, {}).values()):
                errors.append(f"$.task_rows[{task_id}]: metadata-only fields must remain unverified")
    for receipt_id, receipt in receipt_map.items():
        bound_source_ids = {
            source_id
            for source_id, source in source_map.items()
            if source["call_receipt_id"] == receipt_id and source["task_id"] == receipt["task_id"]
        }
        if set(receipt["source_record_ids"]) != bound_source_ids:
            errors.append(f"$.call_receipts[{receipt_id}]: source closure mismatch")
    incidents = collection["incidents"] if isinstance(collection["incidents"], list) else []
    incident_map: dict[str, Mapping[str, Any]] = {}
    incident_keys = {"incident_id", "task_id", "code", "severity", "disposition", "source_record_ids"}
    for index, incident in enumerate(incidents):
        path = f"$.incidents[{index}]"
        if not _exact_keys(incident, incident_keys, path, errors):
            continue
        if incident["task_id"] not in tasks or incident["severity"] != "guardrail":
            errors.append(f"{path}: invalid")
        if incident["code"] not in ERROR_CODES:
            errors.append(f"{path}.code: unsupported")
        source_ids = incident["source_record_ids"]
        source_ids_valid = (
            isinstance(source_ids, list)
            and len(source_ids) == len(set(source_ids))
            and all(
                source_id in source_map and source_map[source_id]["task_id"] == incident["task_id"]
                for source_id in source_ids
            )
            and (bool(source_ids) or incident["code"] == "source_payload_unavailable")
        )
        if not source_ids_valid:
            errors.append(f"{path}.source_record_ids: invalid task-bound source set")
        expected_disposition = "terminal_failed" if incident["code"] == "source_payload_unavailable" else "quarantined"
        if incident["disposition"] != expected_disposition:
            errors.append(f"{path}.disposition: state mismatch")
        expected_incident_id = _id("xstage2incident", {key: incident[key] for key in incident if key != "incident_id"})
        if incident["incident_id"] != expected_incident_id:
            errors.append(f"{path}.incident_id: identity mismatch")
        if incident["incident_id"] in incident_map:
            errors.append(f"{path}.incident_id: duplicate")
        else:
            incident_map[incident["incident_id"]] = incident

    for task_id, row in rows.items():
        task_receipts = [receipt for receipt in receipt_map.values() if receipt["task_id"] == task_id]
        task_sources = [source for source in source_map.values() if source["task_id"] == task_id]
        task_quarantine = [item for item in quarantine_map.values() if item["task_id"] == task_id]
        task_incidents = [item for item in incident_map.values() if item["task_id"] == task_id]
        if set(row["incident_ids"]) != {incident["incident_id"] for incident in task_incidents}:
            errors.append(f"$.task_rows[{task_id}]: incident closure mismatch")
        if not task_receipts or not task_sources:
            if (
                row["status"] != "failed"
                or row["error_codes"] != ["source_payload_unavailable"]
                or any(state != "unverified" for state in state_maps.get(task_id, {}).values())
            ):
                errors.append(f"$.task_rows[{task_id}]: missing source receipt/payload must fail unverified")
        if row["status"] == "completed":
            if row["error_codes"] or task_quarantine or task_incidents:
                errors.append(f"$.task_rows[{task_id}]: completed state carries terminal artifacts")
        elif row["status"] == "quarantined":
            if (
                len(task_quarantine) != 1
                or len(task_incidents) != 1
                or row["error_codes"] != [task_quarantine[0]["reason_code"]]
                or task_incidents[0]["code"] != task_quarantine[0]["reason_code"]
                or row["profile_snapshot_ids"]
                or row["post_ids"]
            ):
                errors.append(f"$.task_rows[{task_id}]: typed quarantine state mismatch")
        elif row["status"] == "failed":
            if (
                row["error_codes"] != ["source_payload_unavailable"]
                or task_quarantine
                or len(task_incidents) != 1
                or task_incidents[0]["code"] != "source_payload_unavailable"
                or row["profile_snapshot_ids"]
                or row["post_ids"]
                or any(state != "unverified" for state in state_maps.get(task_id, {}).values())
            ):
                errors.append(f"$.task_rows[{task_id}]: typed failed state mismatch")

    normalized_source_ids = {profile["source_record_id"] for profile in profile_map.values()} | {
        post["source_record_id"] for post in post_map.values()
    }
    quarantined_source_ids = {source_id for item in quarantine_map.values() for source_id in item["source_record_ids"]}
    incident_source_ids = {source_id for item in incident_map.values() for source_id in item["source_record_ids"]}
    consumed_source_ids = normalized_source_ids | quarantined_source_ids | incident_source_ids
    for source_id, source in source_map.items():
        if source["payload_visibility"] == "full_fixture_payload" and source_id not in consumed_source_ids:
            errors.append(f"$.source_records[{source_id}]: full replayable source is unconsumed")

    account_observations: dict[str, list[tuple[str, str]]] = {}
    for source in source_map.values():
        raw = source["raw_record"]
        if source["record_kind"] != "profile" or not isinstance(raw, dict):
            continue
        platform_ids = raw.get("platform_user_ids")
        handle = raw.get("current_handle")
        if not isinstance(platform_ids, list) or not isinstance(handle, str):
            continue
        for platform_id in platform_ids:
            if isinstance(platform_id, str) and _NUMERIC_ID_RE.fullmatch(platform_id):
                account_observations.setdefault(platform_id, []).append((source["task_id"], handle.casefold()))
    cross_handle_conflict_task_ids: set[str] = set()
    for platform_id, observations in account_observations.items():
        handles = {handle for _, handle in observations}
        if len(handles) <= 1:
            continue
        affected_task_ids = {task_id for task_id, _ in observations}
        cross_handle_conflict_task_ids.update(affected_task_ids)

    for task_id, task in tasks.items():
        row = rows.get(task_id)
        if row is None:
            continue
        task_sources = [source for source in source_map.values() if source["task_id"] == task_id]
        task_source_ids = [source["source_record_id"] for source in task_sources]
        reason, field_ids = _derived_task_guardrail(
            task,
            task_sources,
            cross_handle_conflict=task_id in cross_handle_conflict_task_ids,
        )
        task_quarantine = [item for item in quarantine_map.values() if item["task_id"] == task_id]
        task_incidents = [item for item in incident_map.values() if item["task_id"] == task_id]
        if reason is None:
            if row["status"] != "completed" or row["error_codes"] or task_quarantine or task_incidents:
                errors.append(f"$.task_rows[{task_id}]: source-derived completed state mismatch")
            continue
        if reason == "source_payload_unavailable":
            if (
                row["status"] != "failed"
                or row["error_codes"] != [reason]
                or task_quarantine
                or len(task_incidents) != 1
                or task_incidents[0]["code"] != reason
                or task_incidents[0]["source_record_ids"] != task_source_ids
            ):
                errors.append(f"$.task_rows[{task_id}]: source-derived failure reason mismatch")
            continue
        if (
            row["status"] != "quarantined"
            or row["error_codes"] != [reason]
            or len(task_quarantine) != 1
            or task_quarantine[0]["reason_code"] != reason
            or task_quarantine[0]["field_ids"] != field_ids
            or task_quarantine[0]["source_record_ids"] != task_source_ids
            or len(task_incidents) != 1
            or task_incidents[0]["code"] != reason
            or task_incidents[0]["source_record_ids"] != task_source_ids
        ):
            errors.append(f"$.task_rows[{task_id}]: source-derived quarantine reason mismatch")
    counts = Counter(row.get("status") for row in task_rows if isinstance(row, dict))
    expected_summary = {
        "denominator": len(tasks),
        "terminal": len(task_rows),
        "completed": counts["completed"],
        "quarantined": counts["quarantined"],
        "failed": counts["failed"],
        "status": "terminal" if len(task_rows) == len(tasks) else "incomplete",
    }
    if collection["terminal_summary"] != expected_summary:
        errors.append("$.terminal_summary: arithmetic mismatch")
    retention = collection["retention"]
    retention_keys = {
        "storage_class",
        "directory_mode",
        "file_mode",
        "ttl_seconds",
        "created_at",
        "delete_after",
        "deletion_status",
        "deletion_receipt_sha256",
        "raw_evidence_manifest_sha256",
        "incident_on_expiry",
        "retention_scope",
        "live_reuse_allowed",
        "promotion_eligible",
        "expiry_behavior",
    }
    if _exact_keys(retention, retention_keys, "$.retention", errors):
        if retention["storage_class"] != "private_owner_only_fixture_simulation":
            errors.append("$.retention.storage_class: invalid")
        if (
            retention["retention_scope"] != "synthetic_fixture_simulation_only"
            or retention["live_reuse_allowed"] is not False
            or retention["promotion_eligible"] is not False
            or retention["expiry_behavior"] != "never_promote_fixture_evidence"
        ):
            errors.append("$.retention: fixture simulation cannot be reused or promoted")
        if retention["directory_mode"] != "0700" or retention["file_mode"] != "0600":
            errors.append("$.retention: owner-only modes required")
        if retention["ttl_seconds"] != TECHNICAL_LIMITS["raw_evidence_ttl_seconds"]:
            errors.append("$.retention.ttl_seconds: mismatch")
        if not _is_timestamp(retention["created_at"]) or not _is_timestamp(retention["delete_after"]):
            errors.append("$.retention: invalid timestamps")
        else:
            started = datetime.strptime(retention["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
            expires = datetime.strptime(retention["delete_after"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if int((expires - started).total_seconds()) != retention["ttl_seconds"]:
                errors.append("$.retention.delete_after: TTL mismatch")
        if retention["deletion_status"] != "pending" or retention["deletion_receipt_sha256"] is not None:
            errors.append("$.retention: fixture must truthfully remain pending")
        if retention["raw_evidence_manifest_sha256"] != canonical_sha256(source_records):
            errors.append("$.retention.raw_evidence_manifest_sha256: mismatch")
        if retention["incident_on_expiry"] is not True:
            errors.append("$.retention.incident_on_expiry: required")
    if collection["assertions"] or collection["canonical_writes"] or collection["outreach_actions"]:
        errors.append("$: product authority arrays must be empty")
    if collection["authority"] != OUTPUT_AUTHORITY:
        errors.append("$.authority: zero authority required")
    expected_collection_id = _collection_id(collection)
    if collection["collection_id"] != expected_collection_id:
        errors.append("$.collection_id: identity mismatch")
    return errors


def validate_capability_expectation(
    expectation: Any,
    *,
    request: Any,
    registry: Any,
    selection_manifest: Any,
) -> list[str]:
    errors = _bounded(expectation)
    if errors:
        return [f"$.validation: {error}" for error in errors]
    schema_errors = _schema_preflight(expectation, EXPECTATION_SCHEMA_VERSION)
    if schema_errors:
        return schema_errors
    if validate_experiment_request(request, registry=registry, selection_manifest=selection_manifest):
        return ["$.request: invalid"]
    keys = {
        "schema_version",
        "execution_mode",
        "manifest_id",
        "experiment_id",
        "request_sha256",
        "expectation_version",
        "scenario_manifest_sha256",
        "expectation_semantics_sha256",
        "rows",
        "authority",
    }
    if not _exact_keys(expectation, keys, "$", errors):
        return errors
    if expectation["schema_version"] != EXPECTATION_SCHEMA_VERSION or expectation["execution_mode"] != EXECUTION_MODE:
        errors.append("$.schema_version: offline expectation v1 required")
    if (
        expectation["experiment_id"] != request["experiment_id"]
        or expectation["request_sha256"] != canonical_sha256(request)
        or expectation["expectation_version"] != EXPECTATION_VERSION
        or expectation["scenario_manifest_sha256"] != request["fixture_scenario_manifest"]["scenario_manifest_sha256"]
        or expectation["expectation_semantics_sha256"]
        != request["fixture_scenario_manifest"]["expectation_semantics_sha256"]
    ):
        errors.append("$: request/expectation binding invalid")
    tasks = {task["task_id"]: task for task in request["tasks"]}
    rows = expectation["rows"] if isinstance(expectation["rows"], list) else []
    if len(rows) != len(tasks):
        errors.append("$.rows: denominator mismatch")
    seen: set[str] = set()
    row_keys = {
        "expectation_row_id",
        "task_id",
        "scenario_id",
        "expected_terminal_status",
        "expected_error_codes",
        "expected_quarantine_reason",
        "expected_field_states",
        "expected_profile_count",
        "expected_post_count",
        "expected_source_record_kinds",
    }
    registry_fields = _registry_fields(registry)
    for index, row in enumerate(rows):
        path = f"$.rows[{index}]"
        if not _exact_keys(row, row_keys, path, errors):
            continue
        task_id = row["task_id"]
        if task_id not in tasks or task_id in seen:
            errors.append(f"{path}.task_id: unknown or duplicate")
            continue
        seen.add(task_id)
        scenario_id = row["scenario_id"]
        if not isinstance(scenario_id, str) or scenario_id not in FIXTURE_SCENARIO_IDS:
            errors.append(f"{path}.scenario_id: closed value required")
        status = row["expected_terminal_status"]
        error_codes = row["expected_error_codes"]
        quarantine_reason = row["expected_quarantine_reason"]
        if status not in TERMINAL_STATUSES:
            errors.append(f"{path}.expected_terminal_status: unsupported")
        if not isinstance(error_codes, list) or len(error_codes) != len(set(error_codes)):
            errors.append(f"{path}.expected_error_codes: unique list required")
        elif any(code not in ERROR_CODES for code in error_codes):
            errors.append(f"{path}.expected_error_codes: unsupported")
        if quarantine_reason is not None and quarantine_reason not in QUARANTINE_REASON_CODES:
            errors.append(f"{path}.expected_quarantine_reason: unsupported")
        states = _validate_field_states(
            row["expected_field_states"],
            requested_fields=registry_fields,
            path=f"{path}.expected_field_states",
            errors=errors,
        )
        if status == "completed" and (error_codes or quarantine_reason is not None):
            errors.append(f"{path}: completed expectation cannot carry errors/quarantine")
        if status == "quarantined" and (quarantine_reason is None or error_codes != [quarantine_reason]):
            errors.append(f"{path}: quarantine expectation state mismatch")
        if status == "failed" and (
            error_codes != ["source_payload_unavailable"]
            or quarantine_reason is not None
            or any(state != "unverified" for state in states.values())
        ):
            errors.append(f"{path}: failed expectation must be source-payload unavailable/unverified")
        if not _is_int(row["expected_profile_count"]) or not 0 <= row["expected_profile_count"] <= 1:
            errors.append(f"{path}.expected_profile_count: invalid")
        if not _is_int(row["expected_post_count"]) or not 0 <= row["expected_post_count"] <= 1:
            errors.append(f"{path}.expected_post_count: invalid")
        kinds = row["expected_source_record_kinds"]
        if (
            not isinstance(kinds, list)
            or len(kinds) != len(set(kinds))
            or any(kind not in {"profile", "post", "tool_metadata_only"} for kind in kinds)
        ):
            errors.append(f"{path}.expected_source_record_kinds: invalid")
        expected_row_id = _id(
            "xstage2expectrow",
            {key: row[key] for key in row if key != "expectation_row_id"},
        )
        if row["expectation_row_id"] != expected_row_id:
            errors.append(f"{path}.expectation_row_id: identity mismatch")
    semantic_rows = [{key: value for key, value in row.items() if key != "expectation_row_id"} for row in rows]
    if canonical_json(semantic_rows) != canonical_json(request["fixture_scenario_manifest"]["rows"]):
        errors.append("$.rows: request-frozen scenario semantics mismatch")
    if expectation["expectation_semantics_sha256"] != canonical_sha256(semantic_rows):
        errors.append("$.expectation_semantics_sha256: rows mismatch")
    expected_manifest_id = _id(
        "xstage2expect",
        {
            "experiment_id": expectation["experiment_id"],
            "request_sha256": expectation["request_sha256"],
            "expectation_version": expectation["expectation_version"],
            "scenario_manifest_sha256": expectation["scenario_manifest_sha256"],
            "expectation_semantics_sha256": expectation["expectation_semantics_sha256"],
            "rows_sha256": canonical_sha256(rows),
        },
    )
    if expectation["manifest_id"] != expected_manifest_id:
        errors.append("$.manifest_id: identity mismatch")
    if expectation["authority"] != OUTPUT_AUTHORITY:
        errors.append("$.authority: zero authority required")
    return errors


def evaluate_field_capability(
    *,
    request: Mapping[str, Any],
    collection: Mapping[str, Any],
    expectation: Mapping[str, Any],
    registry: Mapping[str, Any],
    selection_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    if validate_collection(
        collection,
        request=request,
        registry=registry,
        selection_manifest=selection_manifest,
    ) or validate_capability_expectation(
        expectation,
        request=request,
        registry=registry,
        selection_manifest=selection_manifest,
    ):
        raise ValueError("stage2_sources_invalid")
    states_by_field = {field_id: Counter() for field_id in _registry_fields(registry)}
    for row in collection["task_rows"]:
        for item in row["field_states"]:
            states_by_field[item["field_id"]][item["state"]] += 1
    field_metrics = [
        {
            "field_id": field_id,
            "denominator": len(request["tasks"]),
            "present_exact": states_by_field[field_id]["present_exact"],
            "present_bounded": states_by_field[field_id]["present_bounded"],
            "absent": states_by_field[field_id]["absent"],
            "unverified": states_by_field[field_id]["unverified"],
        }
        for field_id in _registry_fields(registry)
    ]
    status_counts = Counter(row["status"] for row in collection["task_rows"])
    rows_by_task = {row["task_id"]: row for row in collection["task_rows"]}
    expectation_matches = 0
    mismatch_task_ids: list[str] = []
    for expected in expectation["rows"]:
        task_id = expected["task_id"]
        observed = rows_by_task[task_id]
        observed_states = observed["field_states"]
        observed_quarantine = [item["reason_code"] for item in collection["quarantine"] if item["task_id"] == task_id]
        observed_kinds = sorted(
            record["record_kind"] for record in collection["source_records"] if record["task_id"] == task_id
        )
        matched = (
            observed["status"] == expected["expected_terminal_status"]
            and observed["error_codes"] == expected["expected_error_codes"]
            and observed_states == expected["expected_field_states"]
            and len([profile for profile in collection["profiles"] if profile["task_id"] == task_id])
            == expected["expected_profile_count"]
            and len([post for post in collection["posts"] if post["task_id"] == task_id])
            == expected["expected_post_count"]
            and observed_kinds == sorted(expected["expected_source_record_kinds"])
            and observed_quarantine
            == ([] if expected["expected_quarantine_reason"] is None else [expected["expected_quarantine_reason"]])
        )
        if matched:
            expectation_matches += 1
        else:
            mismatch_task_ids.append(task_id)
    guardrails = {
        "accepted_unbound_profile": 0,
        "unquarantined_identity_conflict": 0,
        "cross_account_post_evidence": 0,
        "model_reported_id_used_as_identity": 0,
        "nonterminal_task": len(request["tasks"]) - collection["terminal_summary"]["terminal"],
        "product_write": len(collection["canonical_writes"]),
        "outreach_action": len(collection["outreach_actions"]),
        "retention_contract_violation": 0,
    }
    evaluation_seed = {
        "experiment_id": request["experiment_id"],
        "request_sha256": canonical_sha256(request),
        "collection_sha256": canonical_sha256(collection),
        "expectation_sha256": canonical_sha256(expectation),
        "field_metrics": field_metrics,
    }
    decision = (
        "offline_fixture_expectation_conformant" if not mismatch_task_ids else "offline_fixture_expectation_mismatch"
    )
    reason_codes = [
        "fixture_only_not_live_capability",
        "metadata_only_tool_trace_remains_unverified",
        "source_bound_profile_requires_single_replayable_record",
        "retention_is_simulation_only_not_promotion_evidence",
    ]
    if mismatch_task_ids:
        reason_codes.append("offline_fixture_expectation_mismatch_detected")
    return {
        "schema_version": EVALUATION_SCHEMA_VERSION,
        "execution_mode": EXECUTION_MODE,
        "evaluation_id": _id("xstage2eval", evaluation_seed),
        "experiment_id": request["experiment_id"],
        "request_sha256": evaluation_seed["request_sha256"],
        "collection_sha256": evaluation_seed["collection_sha256"],
        "expectation_sha256": evaluation_seed["expectation_sha256"],
        "terminal_metrics": {
            "denominator": len(request["tasks"]),
            "terminal": len(collection["task_rows"]),
            "completed": status_counts["completed"],
            "quarantined": status_counts["quarantined"],
            "failed": status_counts["failed"],
            "replay_bound_profiles": len(collection["profiles"]),
        },
        "field_metrics": field_metrics,
        "expectation_metrics": {
            "denominator": len(expectation["rows"]),
            "matched": expectation_matches,
            "mismatched": len(mismatch_task_ids),
            "mismatch_task_ids": mismatch_task_ids,
        },
        "guardrails": guardrails,
        "decision": decision,
        "reason_codes": reason_codes,
        "authority": copy.deepcopy(OUTPUT_AUTHORITY),
    }


def validate_evaluation(
    evaluation: Any,
    *,
    request: Mapping[str, Any],
    collection: Mapping[str, Any],
    expectation: Mapping[str, Any],
    registry: Mapping[str, Any],
    selection_manifest: Mapping[str, Any],
) -> list[str]:
    errors = _bounded(evaluation)
    if errors:
        return [f"$.validation: {error}" for error in errors]
    schema_errors = _schema_preflight(evaluation, EVALUATION_SCHEMA_VERSION)
    if schema_errors:
        return schema_errors
    keys = {
        "schema_version",
        "execution_mode",
        "evaluation_id",
        "experiment_id",
        "request_sha256",
        "collection_sha256",
        "expectation_sha256",
        "terminal_metrics",
        "field_metrics",
        "expectation_metrics",
        "guardrails",
        "decision",
        "reason_codes",
        "authority",
    }
    if not _exact_keys(evaluation, keys, "$", errors):
        return errors
    try:
        expected = evaluate_field_capability(
            request=request,
            collection=collection,
            expectation=expectation,
            registry=registry,
            selection_manifest=selection_manifest,
        )
    except (TypeError, ValueError, KeyError, RecursionError, UnicodeEncodeError):
        return ["$.sources: invalid"]
    if canonical_json(evaluation) != canonical_json(expected):
        errors.append("$: deterministic replay mismatch")
    return errors


def _field_states(registry_fields: Sequence[str], values: Mapping[str, str]) -> list[dict[str, str]]:
    return [{"field_id": field_id, "state": values.get(field_id, "unverified")} for field_id in registry_fields]


def _build_selection_manifest() -> dict[str, Any]:
    task_inputs = [
        ("1" * 24, "fixture_a", "7" * 18, "model_mediated_unverified"),
        ("2" * 24, "fixture_b", None, "absent"),
        ("3" * 24, "fixture_c", "8" * 18, "model_mediated_unverified"),
        ("4" * 24, "fixture_d", None, "absent"),
    ]
    selected_leads = [
        {
            "opaque_lead_ref": f"xlead_{token}",
            "candidate_row_sha256": hashlib.sha256(f"candidate-{token}".encode()).hexdigest(),
            "lookup_handle": handle,
            "reported_platform_user_id": reported_id,
            "reported_platform_user_id_status": status,
        }
        for token, handle, reported_id, status in task_inputs
    ]
    source_artifact = {
        "schema_version": "x.recall_pool.campaign.result.v1",
        "artifact_sha256": hashlib.sha256(b"synthetic-external-selection-artifact").hexdigest(),
    }
    return {
        "schema_version": SELECTION_MANIFEST_VERSION,
        "selection_id": _id(
            "xstage2selection",
            {"source_artifact": source_artifact, "selected_leads_sha256": canonical_sha256(selected_leads)},
        ),
        "source_artifact": source_artifact,
        "selected_leads": selected_leads,
    }


def _build_request(registry: Mapping[str, Any], selection_manifest: Mapping[str, Any]) -> dict[str, Any]:
    fields = list(_registry_fields(registry))
    tasks: list[dict[str, Any]] = []
    for selected in selection_manifest["selected_leads"]:
        task = {
            "task_id": "",
            **copy.deepcopy(selected),
            "requested_field_ids": fields,
            "tool_policy": {"allowed_tools": ["x_user_search", "x_thread_fetch"], "fallback_allowed": False},
            "source_receipt_required": True,
            "authority": copy.deepcopy(REQUEST_AUTHORITY),
        }
        task["task_id"] = _task_id(task)
        tasks.append(task)
    selected_fields = (
        "opaque_lead_ref",
        "candidate_row_sha256",
        "lookup_handle",
        "reported_platform_user_id",
        "reported_platform_user_id_status",
    )
    selected = [{field: task[field] for field in selected_fields} for task in tasks]
    source_manifest = {
        "selection_manifest_version": SELECTION_MANIFEST_VERSION,
        "selection_manifest_sha256": canonical_sha256(selection_manifest),
        "selection_id": selection_manifest["selection_id"],
        "selected_lead_count": len(tasks),
        "selected_leads_sha256": canonical_sha256(selected),
    }
    target = {
        "lab_id": "synthetic_lab",
        "frozen_from": "2026-07-01T00:00:00.000Z",
        "frozen_to": "2026-07-02T00:00:00.000Z",
    }
    registry_binding = {"registry_version": FIELD_REGISTRY_VERSION, "registry_sha256": canonical_sha256(registry)}
    fixture_scenario_manifest = _build_scenario_manifest(tasks, fields)
    experiment_identity = {
        "target": target,
        "field_registry": registry_binding,
        "normalization_contract_version": NORMALIZATION_VERSION,
        "source_manifest": source_manifest,
        "fixture_scenario_manifest": fixture_scenario_manifest,
        "task_ids": [task["task_id"] for task in tasks],
    }
    return {
        "schema_version": EXPERIMENT_SCHEMA_VERSION,
        "execution_mode": EXECUTION_MODE,
        "experiment_id": _id("xstage2exp", experiment_identity),
        "target": target,
        "field_registry": registry_binding,
        "normalization_contract_version": NORMALIZATION_VERSION,
        "source_manifest": source_manifest,
        "fixture_scenario_manifest": fixture_scenario_manifest,
        "tasks": tasks,
        "technical_limits": copy.deepcopy(TECHNICAL_LIMITS),
        "retention_policy": {
            "raw_evidence_storage": "private_owner_only",
            "retention_scope": "synthetic_fixture_simulation_only",
            "directory_mode": "0700",
            "file_mode": "0600",
            "ttl_seconds": TECHNICAL_LIMITS["raw_evidence_ttl_seconds"],
            "deletion_receipt_required": True,
            "live_reuse_allowed": False,
            "promotion_eligible": False,
        },
        "authority": copy.deepcopy(REQUEST_AUTHORITY),
    }


def _build_collection(request: Mapping[str, Any], registry: Mapping[str, Any]) -> dict[str, Any]:
    tasks = request["tasks"]
    registry_fields = _registry_fields(registry)
    timestamps = ("2026-07-14T01:00:00.000Z", "2026-07-14T01:00:01.000Z")
    source_specs: list[tuple[Mapping[str, Any], str, str, Mapping[str, Any]]] = []

    good_profile_raw = {
        "platform_user_ids": ["900000000000000001"],
        "current_handle": "fixture_a",
        "profile_url": "https://profiles.invalid/x/fixture_a",
        "bio_text": "Synthetic pretraining researcher; writes Chinese AI systems notes.",
        "bio_observed_at": timestamps[1],
        "bio_content_version": "fixture-profile-v1",
        "post_fields_explicitly_absent": False,
    }
    good_post_raw = {
        "canonical_post_id": "111111",
        "canonical_post_url": "https://posts.invalid/x/fixture_a/status/111111",
        "post_author_platform_user_id": "900000000000000001",
        "post_author_handle": "fixture_a",
        "post_authored_at": timestamps[1],
        "bounded_excerpt": "Synthetic bounded technical excerpt.",
        "thread_relation": "self_post",
    }
    conflict_profile_raw = {
        "platform_user_ids": ["900000000000000002", "900000000000000022"],
        "current_handle": "fixture_b",
        "profile_url": "https://profiles.invalid/x/fixture_b",
        "bio_text": "Synthetic conflicting account fixture.",
        "bio_observed_at": timestamps[1],
        "bio_content_version": "fixture-profile-v1",
        "post_fields_explicitly_absent": True,
    }
    renamed_profile_raw = {
        "platform_user_ids": ["900000000000000003"],
        "current_handle": "renamed_c",
        "profile_url": "https://profiles.invalid/x/renamed_c",
        "bio_text": "Synthetic handle-rename fixture.",
        "bio_observed_at": timestamps[1],
        "bio_content_version": "fixture-profile-v1",
        "post_fields_explicitly_absent": True,
    }
    metadata_only_raw = {
        "call_id": "fixture-call-metadata",
        "id": "fixture-tool-id",
        "input": {"handle": "fixture_d"},
        "name": "x_user_search",
    }
    source_specs.extend(
        [
            (tasks[0], "profile", "rawOutput.users[0]", good_profile_raw),
            (tasks[0], "post", "rawOutput.posts[0]", good_post_raw),
            (tasks[1], "profile", "rawOutput.users[0]", conflict_profile_raw),
            (tasks[2], "profile", "rawOutput.users[0]", renamed_profile_raw),
            (tasks[3], "tool_metadata_only", "tool_call_update", metadata_only_raw),
        ]
    )
    source_records: list[dict[str, Any]] = []
    call_receipts: list[dict[str, Any]] = []
    for task in tasks:
        task_specs = [spec for spec in source_specs if spec[0]["task_id"] == task["task_id"]]
        final_ids = [
            _id(
                "xstage2src",
                {
                    "task_id": task["task_id"],
                    "record_kind": kind,
                    "provider_path": path,
                    "raw_record_sha256": canonical_sha256(raw),
                },
            )
            for _, kind, path, raw in task_specs
        ]
        receipt_id = _id(
            "xstage2call",
            {"task_id": task["task_id"], "tool_name": "x_user_search", "source_record_ids": final_ids},
        )
        call_receipts.append(
            {
                "receipt_id": receipt_id,
                "task_id": task["task_id"],
                "tool_name": "x_user_search",
                "status": "fixture_replayed",
                "external_call_count": 0,
                "source_record_ids": final_ids,
                "started_at": timestamps[0],
                "completed_at": timestamps[1],
                "cost_status": "not_applicable_offline_fixture",
                "fallback_used": False,
            }
        )
        for source_id, (_, kind, path, raw) in zip(final_ids, task_specs, strict=True):
            source_records.append(
                {
                    "source_record_id": source_id,
                    "task_id": task["task_id"],
                    "call_receipt_id": receipt_id,
                    "record_kind": kind,
                    "provider_path": path,
                    "payload_visibility": "metadata_only" if kind == "tool_metadata_only" else "full_fixture_payload",
                    "replayable": kind != "tool_metadata_only",
                    "raw_record": copy.deepcopy(raw),
                    "raw_record_sha256": canonical_sha256(raw),
                    "observed_at": timestamps[1],
                }
            )
    profiles: list[dict[str, Any]] = []
    good_profile_source = next(
        record
        for record in source_records
        if record["task_id"] == tasks[0]["task_id"] and record["record_kind"] == "profile"
    )
    profile_seed = {
        "task_id": tasks[0]["task_id"],
        "source_record_id": good_profile_source["source_record_id"],
        "platform_user_id": good_profile_raw["platform_user_ids"][0],
        "bio_sha256": text_sha256(good_profile_raw["bio_text"]),
        "bio_observed_at": good_profile_raw["bio_observed_at"],
    }
    profiles.append(
        {
            "profile_snapshot_id": _id("xstage2profile", profile_seed),
            "task_id": tasks[0]["task_id"],
            "source_record_id": good_profile_source["source_record_id"],
            "platform_user_id": good_profile_raw["platform_user_ids"][0],
            "current_handle": good_profile_raw["current_handle"],
            "profile_url": good_profile_raw["profile_url"],
            "bio_text": good_profile_raw["bio_text"],
            "bio_sha256": text_sha256(good_profile_raw["bio_text"]),
            "bio_observed_at": good_profile_raw["bio_observed_at"],
            "bio_content_version": good_profile_raw["bio_content_version"],
            "source_binding_status": "replay_bound_exact",
            "reported_platform_user_id_diagnostic": {
                "value": tasks[0]["reported_platform_user_id"],
                "status": tasks[0]["reported_platform_user_id_status"],
                "comparison": "different_diagnostic_only",
                "identity_authority": False,
            },
            "authority": copy.deepcopy(OUTPUT_AUTHORITY),
        }
    )
    good_post_source = next(
        record
        for record in source_records
        if record["task_id"] == tasks[0]["task_id"] and record["record_kind"] == "post"
    )
    post_field_ids = [field for field in registry_fields if field not in PROFILE_REQUIRED_FIELDS]
    post_seed = {
        "task_id": tasks[0]["task_id"],
        "source_record_id": good_post_source["source_record_id"],
        "canonical_post_id": good_post_raw["canonical_post_id"],
    }
    posts = [
        {
            "post_id": _id("xstage2post", post_seed),
            "task_id": tasks[0]["task_id"],
            "source_record_id": good_post_source["source_record_id"],
            **copy.deepcopy(good_post_raw),
            "field_states": [
                {
                    "field_id": field_id,
                    "state": "present_bounded" if field_id == "bounded_excerpt" else "present_exact",
                }
                for field_id in post_field_ids
            ],
            "source_binding_status": "replay_bound_exact",
            "authority": copy.deepcopy(OUTPUT_AUTHORITY),
        }
    ]
    quarantine: list[dict[str, Any]] = []
    incidents: list[dict[str, Any]] = []
    derived_guardrails: dict[str, tuple[str | None, list[str]]] = {}
    for task in tasks:
        task_sources = [record for record in source_records if record["task_id"] == task["task_id"]]
        reason, field_ids = _derived_task_guardrail(task, task_sources, cross_handle_conflict=False)
        derived_guardrails[task["task_id"]] = (reason, field_ids)
        record_ids = [record["source_record_id"] for record in task_sources]
        if reason is not None and reason != "source_payload_unavailable":
            quarantine_identity = {
                "task_id": task["task_id"],
                "reason_code": reason,
                "source_record_ids": record_ids,
            }
            quarantine.append(
                {
                    "quarantine_id": _id("xstage2quarantine", quarantine_identity),
                    "task_id": task["task_id"],
                    "reason_code": reason,
                    "source_record_ids": record_ids,
                    "field_ids": field_ids,
                    "resolution": "human_review_required",
                }
            )
        if reason is not None:
            incident_body = {
                "task_id": task["task_id"],
                "code": reason,
                "severity": "guardrail",
                "disposition": "terminal_failed" if reason == "source_payload_unavailable" else "quarantined",
                "source_record_ids": record_ids,
            }
            incidents.append({"incident_id": _id("xstage2incident", incident_body), **incident_body})

    task_rows = []
    for task in tasks:
        task_sources = [record for record in source_records if record["task_id"] == task["task_id"]]
        task_profiles = [profile for profile in profiles if profile["task_id"] == task["task_id"]]
        task_posts = [post for post in posts if post["task_id"] == task["task_id"]]
        task_quarantine = [item for item in quarantine if item["task_id"] == task["task_id"]]
        task_incidents = [item for item in incidents if item["task_id"] == task["task_id"]]
        reason, _ = derived_guardrails[task["task_id"]]
        status = (
            "completed" if reason is None else "failed" if reason == "source_payload_unavailable" else "quarantined"
        )
        error_codes = [] if reason is None else [reason]
        task_rows.append(
            {
                "task_id": task["task_id"],
                "opaque_lead_ref": task["opaque_lead_ref"],
                "candidate_row_sha256": task["candidate_row_sha256"],
                "lookup_handle": task["lookup_handle"],
                "status": status,
                "call_receipt_ids": [
                    receipt["receipt_id"] for receipt in call_receipts if receipt["task_id"] == task["task_id"]
                ],
                "source_record_ids": [record["source_record_id"] for record in task_sources],
                "profile_snapshot_ids": [profile["profile_snapshot_id"] for profile in task_profiles],
                "post_ids": [post["post_id"] for post in task_posts],
                "quarantine_ids": [item["quarantine_id"] for item in task_quarantine],
                "incident_ids": [item["incident_id"] for item in task_incidents],
                "field_states": _derived_task_field_states(task_sources, registry_fields=registry_fields),
                "error_codes": error_codes,
            }
        )
    counts = Counter(row["status"] for row in task_rows)
    terminal_summary = {
        "denominator": len(tasks),
        "terminal": len(task_rows),
        "completed": counts["completed"],
        "quarantined": counts["quarantined"],
        "failed": counts["failed"],
        "status": "terminal",
    }
    collection = {
        "schema_version": COLLECTION_SCHEMA_VERSION,
        "execution_mode": EXECUTION_MODE,
        "experiment_id": request["experiment_id"],
        "request_sha256": canonical_sha256(request),
        "field_registry": copy.deepcopy(request["field_registry"]),
        "terminal_summary": terminal_summary,
        "task_rows": task_rows,
        "call_receipts": call_receipts,
        "source_records": source_records,
        "profiles": profiles,
        "posts": posts,
        "quarantine": quarantine,
        "incidents": incidents,
        "retention": {
            "storage_class": "private_owner_only_fixture_simulation",
            "retention_scope": "synthetic_fixture_simulation_only",
            "directory_mode": "0700",
            "file_mode": "0600",
            "ttl_seconds": 86_400,
            "created_at": "2026-07-14T01:00:00.000Z",
            "delete_after": "2026-07-15T01:00:00.000Z",
            "deletion_status": "pending",
            "deletion_receipt_sha256": None,
            "raw_evidence_manifest_sha256": canonical_sha256(source_records),
            "incident_on_expiry": True,
            "live_reuse_allowed": False,
            "promotion_eligible": False,
            "expiry_behavior": "never_promote_fixture_evidence",
        },
        "assertions": [],
        "canonical_writes": [],
        "outreach_actions": [],
        "authority": copy.deepcopy(OUTPUT_AUTHORITY),
    }
    collection["collection_id"] = _collection_id(collection)
    return collection


def _build_expectation(request: Mapping[str, Any], registry: Mapping[str, Any]) -> dict[str, Any]:
    scenario_manifest = request["fixture_scenario_manifest"]
    errors: list[str] = []
    _validate_scenario_manifest(
        scenario_manifest,
        tasks=request["tasks"],
        registry_fields=_registry_fields(registry),
        path="$.fixture_scenario_manifest",
        errors=errors,
    )
    if errors:
        raise ValueError("fixture_scenario_manifest_invalid")
    rows = []
    for scenario_row in scenario_manifest["rows"]:
        row_without_id = copy.deepcopy(scenario_row)
        rows.append(
            {
                "expectation_row_id": _id("xstage2expectrow", row_without_id),
                **row_without_id,
            }
        )
    expectation_identity = {
        "experiment_id": request["experiment_id"],
        "request_sha256": canonical_sha256(request),
        "expectation_version": EXPECTATION_VERSION,
        "scenario_manifest_sha256": scenario_manifest["scenario_manifest_sha256"],
        "expectation_semantics_sha256": scenario_manifest["expectation_semantics_sha256"],
        "rows_sha256": canonical_sha256(rows),
    }
    return {
        "schema_version": EXPECTATION_SCHEMA_VERSION,
        "execution_mode": EXECUTION_MODE,
        "manifest_id": _id("xstage2expect", expectation_identity),
        "experiment_id": request["experiment_id"],
        "request_sha256": canonical_sha256(request),
        "expectation_version": EXPECTATION_VERSION,
        "scenario_manifest_sha256": scenario_manifest["scenario_manifest_sha256"],
        "expectation_semantics_sha256": scenario_manifest["expectation_semantics_sha256"],
        "rows": rows,
        "authority": copy.deepcopy(OUTPUT_AUTHORITY),
    }


def build_fixture_bundle(registry: Mapping[str, Any] | None = None) -> dict[str, Any]:
    resolved_registry = copy.deepcopy(
        registry if registry is not None else load_json(project_root() / "configs/stage2_field_registry.v1.json")
    )
    if validate_field_registry(resolved_registry):
        raise ValueError("stage2_field_registry_invalid")
    selection_manifest = _build_selection_manifest()
    if validate_selection_manifest(selection_manifest):
        raise ValueError("stage2_selection_manifest_invalid")
    request = _build_request(resolved_registry, selection_manifest)
    expectation = _build_expectation(request, resolved_registry)
    collection = _build_collection(request, resolved_registry)
    evaluation = evaluate_field_capability(
        request=request,
        collection=collection,
        expectation=expectation,
        registry=resolved_registry,
        selection_manifest=selection_manifest,
    )
    return {
        "fixture_version": "x.stage2.field_capability.fixture.v1",
        "field_registry_sha256": canonical_sha256(resolved_registry),
        "selection_manifest": selection_manifest,
        "request": request,
        "capability_expectation": expectation,
        "collection": collection,
        "evaluation": evaluation,
    }


def validate_fixture_bundle(bundle: Any, *, registry: Any) -> list[str]:
    errors = _bounded(bundle)
    if errors:
        return [f"$.validation: {error}" for error in errors]
    keys = {
        "fixture_version",
        "field_registry_sha256",
        "selection_manifest",
        "request",
        "capability_expectation",
        "collection",
        "evaluation",
    }
    if not _exact_keys(bundle, keys, "$", errors):
        return errors
    if bundle["fixture_version"] != "x.stage2.field_capability.fixture.v1":
        errors.append("$.fixture_version: unsupported")
    if bundle["field_registry_sha256"] != canonical_sha256(registry):
        errors.append("$.field_registry_sha256: mismatch")
    errors.extend(
        f"$.selection_manifest{error[1:]}" for error in validate_selection_manifest(bundle["selection_manifest"])
    )
    errors.extend(
        f"$.request{error[1:]}"
        for error in validate_experiment_request(
            bundle["request"],
            registry=registry,
            selection_manifest=bundle["selection_manifest"],
        )
    )
    errors.extend(
        f"$.collection{error[1:]}"
        for error in validate_collection(
            bundle["collection"],
            request=bundle["request"],
            registry=registry,
            selection_manifest=bundle["selection_manifest"],
        )
    )
    errors.extend(
        f"$.capability_expectation{error[1:]}"
        for error in validate_capability_expectation(
            bundle["capability_expectation"],
            request=bundle["request"],
            registry=registry,
            selection_manifest=bundle["selection_manifest"],
        )
    )
    errors.extend(
        f"$.evaluation{error[1:]}"
        for error in validate_evaluation(
            bundle["evaluation"],
            request=bundle["request"],
            collection=bundle["collection"],
            expectation=bundle["capability_expectation"],
            registry=registry,
            selection_manifest=bundle["selection_manifest"],
        )
    )
    return errors


def _terminal_total_validator(function: Any) -> Any:
    @functools.wraps(function)
    def wrapped(*args: Any, **kwargs: Any) -> list[str]:
        try:
            return function(*args, **kwargs)
        except (
            AttributeError,
            IndexError,
            KeyError,
            OverflowError,
            RecursionError,
            TypeError,
            UnicodeError,
            ValueError,
        ):
            return ["$: malformed_contract"]

    return wrapped


validate_field_registry = _terminal_total_validator(validate_field_registry)
validate_selection_manifest = _terminal_total_validator(validate_selection_manifest)
validate_experiment_request = _terminal_total_validator(validate_experiment_request)
validate_collection = _terminal_total_validator(validate_collection)
validate_capability_expectation = _terminal_total_validator(validate_capability_expectation)
validate_evaluation = _terminal_total_validator(validate_evaluation)
validate_fixture_bundle = _terminal_total_validator(validate_fixture_bundle)


__all__ = [
    "COLLECTION_SCHEMA_VERSION",
    "EVALUATION_SCHEMA_VERSION",
    "EXPERIMENT_SCHEMA_VERSION",
    "FIELD_REGISTRY_SCHEMA_VERSION",
    "FIELD_REGISTRY_VERSION",
    "FIELD_STATES",
    "EXPECTATION_SCHEMA_VERSION",
    "PROFILE_REQUIRED_FIELDS",
    "PROFILE_ALWAYS_EXACT_FIELDS",
    "PROFILE_OPTIONAL_BIO_FIELDS",
    "TECHNICAL_LIMITS",
    "build_fixture_bundle",
    "canonical_json",
    "canonical_sha256",
    "evaluate_field_capability",
    "load_json",
    "text_sha256",
    "validate_collection",
    "validate_capability_expectation",
    "validate_evaluation",
    "validate_experiment_request",
    "validate_field_registry",
    "validate_fixture_bundle",
    "validate_selection_manifest",
    "validate_profile_source_binding",
]
