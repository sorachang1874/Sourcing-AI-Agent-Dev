#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from sourcing_agent.candidate_artifacts import (
    _extract_projectable_profile_signals,
    _project_profile_signal_fields_into_record,
    _repair_projected_profile_signal_record_from_profile_source_paths,
    load_company_snapshot_candidate_documents,
)


def _repair_projection_list(
    payload_path: Path,
    signal_by_candidate_id: dict[str, dict[str, object]],
) -> int:
    if not payload_path.exists():
        return 0
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return 0
    updated_count = 0
    projected_payload: list[dict[str, object]] = []
    for item in payload:
        if not isinstance(item, dict):
            projected_payload.append(item)
            continue
        candidate_id = str(item.get("candidate_id") or "").strip()
        signal_payload = dict(signal_by_candidate_id.get(candidate_id) or {})
        projected = _project_profile_signal_fields_into_record(item, signal_payload=signal_payload)
        if projected != item:
            updated_count += 1
        projected_payload.append(projected)
    if updated_count:
        payload_path.write_text(json.dumps(projected_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return updated_count


def _backfill_view(artifact_dir: Path) -> dict[str, object]:
    materialized_path = artifact_dir / "materialized_candidate_documents.json"
    if not materialized_path.exists():
        return {
            "artifact_dir": str(artifact_dir),
            "status": "missing_materialized_candidate_documents",
            "materialized_candidate_count": 0,
            "materialized_updated_count": 0,
            "normalized_updated_count": 0,
            "reusable_updated_count": 0,
        }
    payload = json.loads(materialized_path.read_text(encoding="utf-8"))
    candidates = [dict(item or {}) for item in list(payload.get("candidates") or []) if isinstance(item, dict)]
    timeline_cache: dict[str, dict[str, object]] = {}
    signal_by_candidate_id: dict[str, dict[str, object]] = {}
    updated_count = 0
    repaired_candidates: list[dict[str, object]] = []
    for item in candidates:
        projected = dict(item)
        original = dict(item)
        metadata = dict(projected.get("metadata") or {})
        if _repair_projected_profile_signal_record_from_profile_source_paths(
            projected,
            metadata=metadata,
            timeline_cache=timeline_cache,
        ):
            projected["metadata"] = metadata
        projected = _project_profile_signal_fields_into_record(projected)
        if projected != original:
            updated_count += 1
        repaired_candidates.append(projected)
        candidate_id = str(projected.get("candidate_id") or "").strip()
        if candidate_id:
            signal_by_candidate_id[candidate_id] = _extract_projectable_profile_signals(projected)
    if updated_count:
        payload["candidates"] = repaired_candidates
        materialized_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    normalized_updated_count = _repair_projection_list(
        artifact_dir / "normalized_candidates.json",
        signal_by_candidate_id,
    )
    reusable_updated_count = _repair_projection_list(
        artifact_dir / "reusable_candidate_documents.json",
        signal_by_candidate_id,
    )
    return {
        "artifact_dir": str(artifact_dir),
        "status": "completed",
        "materialized_candidate_count": len(repaired_candidates),
        "materialized_updated_count": updated_count,
        "normalized_updated_count": normalized_updated_count,
        "reusable_updated_count": reusable_updated_count,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill publishable Harvest email metadata into existing candidate artifacts.",
    )
    parser.add_argument("--runtime-dir", default="runtime")
    parser.add_argument("--company", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument(
        "--views",
        nargs="*",
        default=["canonical_merged", "strict_roster_only"],
        choices=["canonical_merged", "strict_roster_only"],
    )
    args = parser.parse_args()

    loaded = load_company_snapshot_candidate_documents(
        runtime_dir=args.runtime_dir,
        target_company=args.company,
        snapshot_id=args.snapshot_id,
        view="canonical_merged",
    )
    snapshot_dir = Path(str(loaded.get("snapshot_dir") or "")).expanduser()
    normalized_dir = snapshot_dir / "normalized_artifacts"

    view_results: list[dict[str, object]] = []
    for view in args.views:
        artifact_dir = normalized_dir if view == "canonical_merged" else (normalized_dir / view)
        view_results.append(_backfill_view(artifact_dir))
    print(
        json.dumps(
            {
                "status": "completed",
                "runtime_dir": str(args.runtime_dir),
                "company": args.company,
                "snapshot_id": args.snapshot_id,
                "views": view_results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
