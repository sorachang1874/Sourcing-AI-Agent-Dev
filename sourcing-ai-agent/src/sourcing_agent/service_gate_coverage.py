from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .smoke_expectation_contract import validate_smoke_expectations

VALID_SERVICE_GATE_TAG_STATUSES = {"required_now", "required_before_ecs_sync", "planned"}


def _string_list(value: Any) -> list[str]:
    return [str(item).strip() for item in list(value or []) if str(item).strip()]


def _positive_numeric_leaf_values(value: Any) -> list[float]:
    values: list[float] = []
    if isinstance(value, (int, float)):
        values.append(float(value))
        return values
    if isinstance(value, dict):
        for item in value.values():
            values.extend(_positive_numeric_leaf_values(item))
    return values


def _service_gate_expectation_errors(
    *,
    tag: str,
    matrix_name: str,
    case_name: str,
    tag_row: dict[str, Any],
    expectations: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    for key in _string_list(tag_row.get("required_true_expectations")):
        if expectations.get(key) is not True:
            errors.append(f"{matrix_name}:{case_name}:{tag}:{key}:not_true")
    for key in _string_list(tag_row.get("required_present_expectations")):
        if key not in expectations:
            errors.append(f"{matrix_name}:{case_name}:{tag}:{key}:missing")
    for key in _string_list(tag_row.get("required_positive_expectations")):
        value = expectations.get(key)
        positive_values = _positive_numeric_leaf_values(value)
        if not positive_values or any(item <= 0.0 for item in positive_values):
            errors.append(f"{matrix_name}:{case_name}:{tag}:{key}:not_positive")
    return errors


def validate_service_gate_coverage(
    config_dir: str | Path,
    *,
    require_before_ecs_sync: bool = False,
) -> dict[str, Any]:
    """Validate scripted/browser service-gate coverage metadata.

    The default mode gates current local smoke coverage only. The ECS-sync mode
    intentionally fails while historical incident tags remain planned.
    """

    root = Path(config_dir).resolve()
    manifest_path = root / "service_gate_coverage_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows = [dict(item) for item in list(manifest.get("coverage_tags") or []) if isinstance(item, dict)]

    metadata_errors: list[str] = []
    coverage_errors: list[str] = []
    tag_rows: dict[str, dict[str, Any]] = {}
    for row in rows:
        tag = str(row.get("tag") or "").strip()
        status = str(row.get("status") or "").strip()
        if not tag:
            metadata_errors.append("manifest:empty_tag")
            continue
        if tag in tag_rows:
            metadata_errors.append(f"manifest:{tag}:duplicate")
        tag_rows[tag] = row
        if status not in VALID_SERVICE_GATE_TAG_STATUSES:
            metadata_errors.append(f"manifest:{tag}:bad_status:{status}")
        if status == "required_now":
            expectation_keys = (
                _string_list(row.get("required_true_expectations"))
                + _string_list(row.get("required_present_expectations"))
                + _string_list(row.get("required_positive_expectations"))
            )
            if not expectation_keys:
                metadata_errors.append(f"manifest:{tag}:required_now_without_expectations")
            metadata_errors.extend(
                validate_smoke_expectations(
                    {key: True for key in expectation_keys},
                    context=f"manifest:{tag}",
                )
            )
        else:
            for required_field in ("current_gap", "promotion_gate", "owner_next_step"):
                if not str(row.get(required_field) or "").strip():
                    metadata_errors.append(f"manifest:{tag}:missing_{required_field}")

    covered_cases: dict[str, list[dict[str, Any]]] = {}
    matrix_paths = sorted(root.glob("*smoke_matrix.json"))
    case_count = 0
    for matrix_path in matrix_paths:
        payload = json.loads(matrix_path.read_text(encoding="utf-8"))
        for raw_case in list(payload.get("cases") or []):
            if not isinstance(raw_case, dict):
                continue
            case_count += 1
            case = dict(raw_case)
            case_name = str(case.get("case") or "").strip()
            case_tags = _string_list(case.get("coverage_tags"))
            coverage_errors.extend(
                validate_smoke_expectations(
                    dict(case.get("expectations") or {}),
                    context=f"{matrix_path.name}:{case_name or '<unnamed>'}",
                )
            )
            if not case_tags:
                coverage_errors.append(f"{matrix_path.name}:{case_name}:missing_coverage_tags")
            scripted_scenario = str(case.get("scripted_scenario") or "").strip()
            if scripted_scenario and not (root.parents[1] / scripted_scenario).exists():
                coverage_errors.append(f"{matrix_path.name}:{case_name}:missing_scripted_scenario")
            for tag in case_tags:
                if tag not in tag_rows:
                    coverage_errors.append(f"{matrix_path.name}:{case_name}:unknown_tag:{tag}")
                    continue
                covered_cases.setdefault(tag, []).append({"matrix": matrix_path.name, "case": case})

    required_now_tags: list[str] = []
    required_before_ecs_sync_tags: list[str] = []
    missing_required_now_tags: list[str] = []
    missing_required_before_ecs_sync_tags: list[str] = []
    required_statuses = {"required_now"}
    if require_before_ecs_sync:
        required_statuses.add("required_before_ecs_sync")
    for tag, row in tag_rows.items():
        status = str(row.get("status") or "").strip()
        if status == "required_now":
            required_now_tags.append(tag)
        elif status == "required_before_ecs_sync":
            required_before_ecs_sync_tags.append(tag)
        if status not in required_statuses:
            continue
        tagged_cases = covered_cases.get(tag) or []
        if not tagged_cases:
            if status == "required_now":
                missing_required_now_tags.append(tag)
                coverage_errors.append(f"manifest:{tag}:required_now_not_covered")
            elif status == "required_before_ecs_sync":
                missing_required_before_ecs_sync_tags.append(tag)
                coverage_errors.append(f"manifest:{tag}:required_before_ecs_sync_not_covered")
            continue
        if status != "required_now":
            continue
        for tagged_case in tagged_cases:
            case = dict(tagged_case.get("case") or {})
            coverage_errors.extend(
                _service_gate_expectation_errors(
                    tag=tag,
                    matrix_name=str(tagged_case.get("matrix") or ""),
                    case_name=str(case.get("case") or "").strip(),
                    tag_row=row,
                    expectations=dict(case.get("expectations") or {}),
                )
            )

    planned_gaps = [
        {
            "tag": str(row.get("tag") or "").strip(),
            "status": str(row.get("status") or "").strip(),
            "validation_level": str(row.get("validation_level") or "").strip(),
            "current_gap": str(row.get("current_gap") or "").strip(),
            "promotion_gate": str(row.get("promotion_gate") or "").strip(),
            "owner_next_step": str(row.get("owner_next_step") or "").strip(),
        }
        for row in rows
        if str(row.get("status") or "").strip() in {"required_before_ecs_sync", "planned"}
    ]
    covered_tags = sorted(covered_cases)
    errors = metadata_errors + coverage_errors
    return {
        "status": "ok" if not errors else "failed",
        "error_count": len(errors),
        "metadata_errors": metadata_errors,
        "coverage_errors": coverage_errors,
        "manifest_path": str(manifest_path),
        "matrix_count": len(matrix_paths),
        "case_count": case_count,
        "tag_count": len(tag_rows),
        "covered_tags": covered_tags,
        "required_now_tags": sorted(required_now_tags),
        "required_before_ecs_sync_tags": sorted(required_before_ecs_sync_tags),
        "missing_required_now_tags": sorted(missing_required_now_tags),
        "missing_required_before_ecs_sync_tags": sorted(missing_required_before_ecs_sync_tags),
        "planned_gaps": planned_gaps,
    }
