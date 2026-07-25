from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONTRACT_VERSION = "runtime_asset_supersession_audit_v1"
SOURCE_CONTRACT_VERSION = "runtime_asset_retention_audit_v1"
ALLOWED_SCAN_ROOTS = ("runtime/test_env", "output")
REVIEWABLE_RETENTION_CLASSES = {
    "review_signoff_or_pressure_run_artifact",
    "review_phase_milestone_artifact",
}


def build_runtime_asset_supersession_report(*, retention_report: dict[str, Any]) -> dict[str, Any]:
    """Build a read-only supersession review plan for high-proof runtime artifacts.

    This report intentionally does not approve deletion. It identifies older
    artifacts within the same normalized run family so a future cold-copy or
    supersession-proof review can focus on the highest-value candidates.
    """

    source_contract_version = str(retention_report.get("contract_version") or "")
    source_read_only = retention_report.get("read_only")
    source_deletion_allowed = retention_report.get("deletion_allowed")
    source_valid = (
        source_contract_version == SOURCE_CONTRACT_VERSION
        and source_read_only is True
        and source_deletion_allowed is False
    )
    skipped_items: list[dict[str, Any]] = []
    if not source_valid:
        for raw_item in list(retention_report.get("directories") or []):
            item = dict(raw_item or {})
            if str(item.get("retention_class") or "") not in REVIEWABLE_RETENTION_CLASSES:
                continue
            skipped_items.append(
                {
                    "path": str(item.get("path") or ""),
                    "retention_class": str(item.get("retention_class") or ""),
                    "skip_reason": "invalid_source_retention_contract",
                    "source_contract_version": source_contract_version,
                    "source_read_only": source_read_only,
                    "source_deletion_allowed": source_deletion_allowed,
                }
            )

    reviewable_items: list[dict[str, Any]] = []
    families: dict[str, list[dict[str, Any]]] = {}
    for raw_item in ([] if not source_valid else list(retention_report.get("directories") or [])):
        item = dict(raw_item or {})
        retention_class = str(item.get("retention_class") or "")
        if retention_class not in REVIEWABLE_RETENTION_CLASSES:
            continue
        path_text = str(item.get("path") or "")
        effective_scan_root, scan_root_skip_reason = _effective_scan_root(
            path_text,
            workspace_root=str(retention_report.get("workspace_root") or ""),
        )
        source_scan_root = str(item.get("scan_root") or "")
        if not effective_scan_root:
            skipped_items.append(
                {
                    "path": path_text,
                    "retention_class": retention_class,
                    "skip_reason": scan_root_skip_reason or "path_outside_allowed_scan_roots",
                    "source_scan_root": source_scan_root,
                    "allowed_scan_roots": list(ALLOWED_SCAN_ROOTS),
                }
            )
            continue
        if source_scan_root and source_scan_root != effective_scan_root:
            skipped_items.append(
                {
                    "path": path_text,
                    "retention_class": retention_class,
                    "skip_reason": "scan_root_path_mismatch",
                    "source_scan_root": source_scan_root,
                    "effective_scan_root": effective_scan_root,
                }
            )
            continue
        name = Path(path_text).name
        family_key = f"{effective_scan_root}:{_family_key(name)}"
        payload = {
            "path": path_text,
            "name": name,
            "family_key": family_key,
            "retention_class": retention_class,
            "scan_root": effective_scan_root,
            "source_scan_root": source_scan_root,
            "size_bytes": _safe_int(item.get("size_bytes")),
            "file_count": _safe_int(item.get("file_count")),
            "directory_count": _safe_int(item.get("directory_count")),
            "latest_mtime": str(item.get("latest_mtime") or ""),
            "matched_markers": list(item.get("matched_markers") or []),
            "rerun_number": _extract_rerun_number(name),
            "date_tokens": _extract_date_tokens(name),
        }
        reviewable_items.append(payload)
        families.setdefault(family_key, []).append(payload)

    rows: list[dict[str, Any]] = []
    for family_key, members in sorted(families.items()):
        reference = _family_reference(members)
        for member in sorted(members, key=_member_sort_key, reverse=True):
            is_reference = member["path"] == reference["path"]
            if len(members) > 1 and not is_reference:
                supersession_status = "supersession_review_candidate"
                next_gate = "prove_family_reference_supersedes_then_cold_copy_review"
            elif len(members) > 1:
                supersession_status = "family_reference_keep"
                next_gate = "keep_as_family_reference_until_reviewed_replacement_exists"
            else:
                supersession_status = "singleton_keep"
                next_gate = "no_family_supersession_candidate"
            rows.append(
                {
                    **member,
                    "family_size": len(members),
                    "family_reference_path": reference["path"],
                    "family_reference_latest_mtime": reference["latest_mtime"],
                    "supersession_status": supersession_status,
                    "next_gate": next_gate,
                }
            )

    summary = _build_summary(rows, skipped_items)
    status = (
        "blocked_invalid_source_retention_contract"
        if not source_valid
        else ("ready_for_supersession_review" if rows else "no_reviewable_runtime_artifacts")
    )
    return {
        "contract_version": CONTRACT_VERSION,
        "status": status,
        "read_only": True,
        "deletion_allowed": False,
        "source_contract_version": source_contract_version,
        "source_contract_valid": source_valid,
        "source_read_only": source_read_only,
        "source_deletion_allowed": source_deletion_allowed,
        "source_workspace_root": str(retention_report.get("workspace_root") or ""),
        "allowed_scan_roots": list(ALLOWED_SCAN_ROOTS),
        "summary": summary,
        "artifacts": rows,
        "skipped_artifacts": skipped_items,
        "post_review_gates": [
            "confirm_family_reference_is_valid_supersession",
            "copy_or_package_selected_artifacts_to_cold_storage",
            "verify_cold_copy_hash_or_manifest",
            "independent_review_gate_before_any_apply",
            "apply_archive_or_reuse_exclusion_in_separate_reviewed_operation",
        ],
    }


def render_runtime_asset_supersession_markdown(report: dict[str, Any]) -> str:
    summary = dict(report.get("summary") or {})
    lines = [
        "# Runtime Asset Supersession Audit",
        "",
        f"- Status: `{report.get('status')}`",
        f"- Read-only: `{bool(report.get('read_only'))}`",
        f"- Deletion allowed: `{bool(report.get('deletion_allowed'))}`",
        f"- Reviewable artifacts: `{summary.get('reviewable_artifact_count', 0)}`",
        f"- Supersession review candidates: `{summary.get('supersession_review_candidate_count', 0)}`",
        f"- Potential review bytes: `{summary.get('supersession_review_candidate_bytes', 0)}`",
        f"- Family count: `{summary.get('family_count', 0)}`",
        f"- Skipped artifacts: `{summary.get('skipped_artifact_count', 0)}`",
        "",
        "## By Status",
        "",
        "| status | count | bytes | files |",
        "| --- | ---: | ---: | ---: |",
    ]
    for status, payload in sorted(dict(summary.get("by_supersession_status") or {}).items()):
        lines.append(
            "| {status} | {count} | {bytes} | {files} |".format(
                status=status,
                count=int(payload.get("count") or 0),
                bytes=int(payload.get("size_bytes") or 0),
                files=int(payload.get("file_count") or 0),
            )
        )
    by_skip_reason = dict(summary.get("by_skip_reason") or {})
    if by_skip_reason:
        lines.extend(
            [
                "",
                "## Skipped Artifacts",
                "",
                "| reason | count |",
                "| --- | ---: |",
            ]
        )
        for reason, count in sorted(by_skip_reason.items()):
            lines.append(f"| `{reason}` | {int(count or 0)} |")
    lines.extend(
        [
            "",
            "## Candidates",
            "",
            "| path | class | status | family size | reference | bytes | next gate |",
            "| --- | --- | --- | ---: | --- | ---: | --- |",
        ]
    )
    for item in list(report.get("artifacts") or []):
        payload = dict(item or {})
        if str(payload.get("supersession_status") or "") != "supersession_review_candidate":
            continue
        lines.append(
            "| {path} | `{klass}` | `{status}` | {family_size} | `{reference}` | {bytes} | `{next_gate}` |".format(
                path=str(payload.get("path") or ""),
                klass=str(payload.get("retention_class") or ""),
                status=str(payload.get("supersession_status") or ""),
                family_size=int(payload.get("family_size") or 0),
                reference=str(payload.get("family_reference_path") or ""),
                bytes=int(payload.get("size_bytes") or 0),
                next_gate=str(payload.get("next_gate") or ""),
            )
        )
    lines.extend(["", "## Post-Review Gates", ""])
    for gate in list(report.get("post_review_gates") or []):
        lines.append(f"- `{gate}`")
    return "\n".join(lines).rstrip() + "\n"


def dumps_report(report: dict[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _build_summary(rows: list[dict[str, Any]], skipped_items: list[dict[str, Any]]) -> dict[str, Any]:
    by_status: dict[str, dict[str, int]] = {}
    by_class: dict[str, dict[str, int]] = {}
    by_skip_reason: dict[str, int] = {}
    family_keys = {str(row.get("family_key") or "") for row in rows}
    candidate_bytes = 0
    candidate_count = 0
    for row in rows:
        size = _safe_int(row.get("size_bytes"))
        files = _safe_int(row.get("file_count"))
        status = str(row.get("supersession_status") or "")
        klass = str(row.get("retention_class") or "")
        for bucket, key in ((by_status, status), (by_class, klass)):
            payload = bucket.setdefault(key, {"count": 0, "size_bytes": 0, "file_count": 0})
            payload["count"] += 1
            payload["size_bytes"] += size
            payload["file_count"] += files
        if status == "supersession_review_candidate":
            candidate_count += 1
            candidate_bytes += size
    for item in skipped_items:
        reason = str(item.get("skip_reason") or "unknown")
        by_skip_reason[reason] = by_skip_reason.get(reason, 0) + 1
    return {
        "reviewable_artifact_count": len(rows),
        "skipped_artifact_count": len(skipped_items),
        "family_count": len(family_keys),
        "supersession_review_candidate_count": candidate_count,
        "supersession_review_candidate_bytes": candidate_bytes,
        "by_supersession_status": by_status,
        "by_retention_class": by_class,
        "by_skip_reason": by_skip_reason,
    }


def _family_reference(members: list[dict[str, Any]]) -> dict[str, Any]:
    return sorted(members, key=_member_sort_key, reverse=True)[0]


def _member_sort_key(item: dict[str, Any]) -> tuple[int, int, datetime, str]:
    date_rank = max([_date_token_rank(token) for token in list(item.get("date_tokens") or [])] or [0])
    rerun_rank = _safe_int(item.get("rerun_number"))
    mtime = _parse_datetime(str(item.get("latest_mtime") or "")) or datetime.fromtimestamp(0, timezone.utc)
    return date_rank, rerun_rank, mtime, str(item.get("path") or "")


def _family_key(name: str) -> str:
    normalized = _normalize_name(name)
    normalized = re.sub(r"20\d{6}t\d{6}z?", "_date_", normalized)
    normalized = re.sub(r"20\d{6}", "_date_", normalized)
    normalized = re.sub(r"rerun\d+", "rerun", normalized)
    normalized = re.sub(r"(^|_)v\d+($|_)", "_version_", normalized)
    normalized = re.sub(r"(^|_)chunk\d+($|_)", "_chunk_", normalized)
    normalized = re.sub(r"_\d+$", "_number", normalized)
    return re.sub(r"_+", "_", normalized).strip("_")


def _effective_scan_root(path_text: str, *, workspace_root: str = "") -> tuple[str, str]:
    raw_path = str(path_text or "").strip()
    if not raw_path:
        return "", "empty_path"
    raw_parts = [part for part in raw_path.replace("\\", "/").split("/") if part]
    if any(part in {".", ".."} for part in raw_parts):
        return "", "path_contains_traversal"
    path = Path(raw_path)
    if not path.parts:
        return "", "empty_path"
    if any(part in {".", ".."} for part in path.parts):
        return "", "path_contains_traversal"
    if path.is_absolute():
        if not workspace_root:
            return "", "absolute_path_without_workspace_root"
        try:
            path = path.relative_to(Path(workspace_root))
        except ValueError:
            return "", "absolute_path_outside_workspace_root"
    if any(part in {".", ".."} for part in path.parts):
        return "", "path_contains_traversal"
    normalized = str(path)
    for scan_root in ALLOWED_SCAN_ROOTS:
        scan_root_parts = Path(scan_root).parts
        path_parts = path.parts
        if len(path_parts) <= len(scan_root_parts):
            continue
        if path_parts[: len(scan_root_parts)] == scan_root_parts:
            return scan_root, ""
    return "", "path_outside_allowed_scan_roots"


def _extract_rerun_number(name: str) -> int:
    matches = [int(match.group(1)) for match in re.finditer(r"rerun[_-]?(\d+)", name.lower())]
    return max(matches) if matches else 0


def _extract_date_tokens(name: str) -> list[str]:
    lowered = name.lower()
    tokens = re.findall(r"20\d{6}t\d{6}z?", lowered)
    tokens.extend(re.findall(r"20\d{6}", lowered))
    seen: set[str] = set()
    result: list[str] = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def _date_token_rank(token: str) -> int:
    digits = "".join(character for character in str(token or "") if character.isdigit())
    if not digits:
        return 0
    return int(digits[:14].ljust(14, "0"))


def _normalize_name(value: str) -> str:
    return re.sub(r"_+", "_", "".join(character.lower() if character.isalnum() else "_" for character in str(value or ""))).strip("_")


def _parse_datetime(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
