from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any

from .asset_logger import AssetLogger
from .canonicalization import canonicalize_company_records
from .domain import (
    Candidate,
    EvidenceRecord,
    candidate_profile_signal_text,
    candidate_searchable_text,
    derive_candidate_facets,
    derive_candidate_role_bucket,
    merge_candidate,
    make_evidence_id,
    normalize_candidate,
    sanitize_candidate_notes,
)
from .storage import SQLiteStore


class CandidateArtifactError(RuntimeError):
    pass


_LARGE_ORG_HISTORY_SNAPSHOT_MIN_CANDIDATES = 1000


def materialize_company_candidate_view(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    snapshot_id: str = "",
    preferred_source_snapshot_ids: list[str] | None = None,
) -> dict[str, Any]:
    company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(runtime_dir, target_company, snapshot_id=snapshot_id)
    company_name = str(identity_payload.get("canonical_name") or target_company).strip() or target_company

    merged_candidates: dict[str, Candidate] = {}
    merged_evidence: dict[str, dict[str, Any]] = {}
    candidate_aliases: dict[str, str] = {}
    identity_index: dict[str, str] = {}
    source_snapshots = _load_company_history_snapshots(snapshot_dir.parent, company_name)
    source_snapshots, source_snapshot_selection = _select_source_snapshots_for_materialization(
        source_snapshots,
        current_snapshot_id=snapshot_dir.name,
        preferred_source_snapshot_ids=preferred_source_snapshot_ids,
    )
    for source_snapshot in source_snapshots:
        for candidate in source_snapshot["candidates"]:
            _ingest_materialized_candidate(
                candidate,
                merged_candidates=merged_candidates,
                candidate_aliases=candidate_aliases,
                identity_index=identity_index,
                prefer_incoming=True,
            )
        for evidence in source_snapshot["evidence"]:
            remapped_evidence = _remap_evidence_candidate(evidence, candidate_aliases)
            merged_evidence[_evidence_key(remapped_evidence)] = remapped_evidence

    sqlite_candidates = store.list_candidates_for_company(company_name)
    if not sqlite_candidates and company_name != target_company:
        sqlite_candidates = store.list_candidates_for_company(target_company)
    sqlite_evidence = store.list_evidence_for_company(company_name)
    if not sqlite_evidence and company_name != target_company:
        sqlite_evidence = store.list_evidence_for_company(target_company)

    for candidate in sqlite_candidates:
        _ingest_materialized_candidate(
            candidate,
            merged_candidates=merged_candidates,
            candidate_aliases=candidate_aliases,
            identity_index=identity_index,
            prefer_incoming=True,
        )
    for evidence in sqlite_evidence:
        remapped_evidence = _remap_evidence_candidate(evidence, candidate_aliases)
        merged_evidence[_evidence_key(remapped_evidence)] = remapped_evidence

    merged_candidates, merged_evidence = _consolidate_materialized_duplicates(merged_candidates, merged_evidence)
    canonical_candidates, canonical_evidence, canonicalization_summary = canonicalize_company_records(
        list(merged_candidates.values()),
        build_evidence_records_from_payloads(list(merged_evidence.values())),
    )

    materialized_candidates = canonical_candidates
    materialized_evidence = [item.to_record() for item in canonical_evidence]
    return {
        "target_company": company_name,
        "company_key": company_key,
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": snapshot_dir,
        "company_identity": identity_payload,
        "candidates": materialized_candidates,
        "evidence": materialized_evidence,
        "source_snapshots": source_snapshots,
        "source_snapshot_selection": source_snapshot_selection,
        "sqlite_candidate_count": len(sqlite_candidates),
        "sqlite_evidence_count": len(sqlite_evidence),
        "canonicalization": canonicalization_summary,
    }


def build_company_candidate_artifacts(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    snapshot_id: str = "",
    output_dir: str | Path | None = None,
    preferred_source_snapshot_ids: list[str] | None = None,
) -> dict[str, Any]:
    materialized_view = materialize_company_candidate_view(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        snapshot_id=snapshot_id,
        preferred_source_snapshot_ids=preferred_source_snapshot_ids,
    )
    snapshot_dir = Path(materialized_view["snapshot_dir"])
    artifact_dir = Path(output_dir) if output_dir else (snapshot_dir / "normalized_artifacts")
    artifact_dir.mkdir(parents=True, exist_ok=True)
    logger = AssetLogger(snapshot_dir)

    candidates = list(materialized_view["candidates"])
    evidence = list(materialized_view["evidence"])
    evidence_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in evidence:
        evidence_by_candidate[str(item.get("candidate_id") or "").strip()].append(item)

    merged_view_result = _write_artifact_view(
        logger=logger,
        artifact_dir=artifact_dir,
        materialized_view=materialized_view,
        candidates=candidates,
        evidence=evidence,
        evidence_by_candidate=evidence_by_candidate,
        asset_view="canonical_merged",
    )
    strict_candidates = [
        candidate
        for candidate in candidates
        if _is_strict_roster_candidate(candidate, evidence_by_candidate.get(candidate.candidate_id, []))
    ]
    strict_candidate_ids = {candidate.candidate_id for candidate in strict_candidates}
    strict_evidence = [
        item
        for item in evidence
        if str(item.get("candidate_id") or "").strip() in strict_candidate_ids
    ]
    strict_evidence_by_candidate = {
        candidate_id: items
        for candidate_id, items in evidence_by_candidate.items()
        if candidate_id in strict_candidate_ids
    }
    strict_view_result = _write_artifact_view(
        logger=logger,
        artifact_dir=artifact_dir / "strict_roster_only",
        materialized_view=materialized_view,
        candidates=strict_candidates,
        evidence=strict_evidence,
        evidence_by_candidate=strict_evidence_by_candidate,
        asset_view="strict_roster_only",
    )
    merged_view_result["summary"]["derived_views"] = {
        "strict_roster_only": {
            "candidate_count": strict_view_result["summary"]["candidate_count"],
            "evidence_count": strict_view_result["summary"]["evidence_count"],
            "manual_review_backlog_count": strict_view_result["summary"]["manual_review_backlog_count"],
            "profile_completion_backlog_count": strict_view_result["summary"]["profile_completion_backlog_count"],
            "artifact_dir": str(artifact_dir / "strict_roster_only"),
        }
    }
    summary_path = logger.write_json(
        artifact_dir / "artifact_summary.json",
        merged_view_result["summary"],
        asset_type="candidate_artifact_summary",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    merged_view_result["artifact_paths"]["artifact_summary"] = str(summary_path)
    try:
        from .asset_reuse_planning import (
            build_organization_asset_registry_record,
            ensure_acquisition_shard_registry_for_snapshot,
            upsert_organization_asset_registry_with_guard,
        )
        from .organization_assets import (
            ensure_acquisition_shard_bundles_for_snapshot,
            ensure_organization_completeness_ledger,
        )

        registry_target_company = (
            str(materialized_view.get("company_key") or "").strip()
            or snapshot_dir.parent.name
            or materialized_view["target_company"]
        )
        registry_record = build_organization_asset_registry_record(
            target_company=materialized_view["target_company"],
            company_key=str(materialized_view.get("company_key") or ""),
            snapshot_id=snapshot_dir.name,
            asset_view="canonical_merged",
            summary=merged_view_result["summary"],
            source_path=str(summary_path),
            authoritative=True,
        )
        upsert_organization_asset_registry_with_guard(
            store=store,
            candidate_record=registry_record,
        )
        ensure_acquisition_shard_registry_for_snapshot(
            runtime_dir=runtime_dir,
            store=store,
            target_company=registry_target_company,
            snapshot_id=snapshot_dir.name,
        )
        ensure_acquisition_shard_bundles_for_snapshot(
            runtime_dir=runtime_dir,
            store=store,
            target_company=registry_target_company,
            snapshot_id=snapshot_dir.name,
            asset_view="canonical_merged",
        )
        ensure_organization_completeness_ledger(
            runtime_dir=runtime_dir,
            store=store,
            target_company=registry_target_company,
            snapshot_id=snapshot_dir.name,
            asset_view="canonical_merged",
        )
    except Exception:
        pass
    return {
        "status": "built",
        "target_company": materialized_view["target_company"],
        "snapshot_id": snapshot_dir.name,
        "artifact_dir": str(artifact_dir),
        "artifact_paths": merged_view_result["artifact_paths"],
        "summary": merged_view_result["summary"],
        "views": {
            "canonical_merged": {
                "artifact_dir": str(artifact_dir),
                "artifact_paths": merged_view_result["artifact_paths"],
                "summary": merged_view_result["summary"],
            },
            "strict_roster_only": {
                "artifact_dir": str(artifact_dir / "strict_roster_only"),
                "artifact_paths": strict_view_result["artifact_paths"],
                "summary": strict_view_result["summary"],
            },
        },
    }


def repair_missing_company_candidate_artifacts(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    companies: list[str] | None = None,
    snapshot_id: str = "",
) -> dict[str, Any]:
    from .asset_reuse_planning import backfill_organization_asset_registry_for_company
    from .organization_assets import ensure_organization_completeness_ledger

    runtime_root = Path(runtime_dir)
    company_assets_dir = runtime_root / "company_assets"
    requested_companies = [_normalize_key(item) for item in list(companies or []) if str(item or "").strip()]
    requested_snapshot_id = str(snapshot_id or "").strip()
    if not company_assets_dir.exists():
        return {
            "status": "missing_runtime_company_assets",
            "runtime_dir": str(runtime_root),
            "scanned_snapshot_count": 0,
            "repair_candidate_count": 0,
            "repaired_snapshot_count": 0,
            "skipped_existing_count": 0,
            "missing_candidate_documents_count": 0,
            "registry_refresh_count": 0,
            "errors": [],
            "companies": [],
        }

    scanned_snapshot_count = 0
    repair_candidate_count = 0
    repaired_snapshot_count = 0
    ledger_refresh_only_count = 0
    skipped_existing_count = 0
    missing_candidate_documents_count = 0
    registry_refresh_count = 0
    errors: list[dict[str, Any]] = []
    company_summaries: list[dict[str, Any]] = []

    for company_dir in sorted(path for path in company_assets_dir.iterdir() if path.is_dir()):
        latest_payload = _load_company_snapshot_json(company_dir / "latest_snapshot.json")
        preview_identity = dict(latest_payload.get("company_identity") or {})
        if requested_companies:
            if not any(_score_company_snapshot_dir_match(company_dir, preview_identity, item) > 0 for item in requested_companies):
                continue

        company_summary = {
            "company_dir": company_dir.name,
            "repaired_snapshots": [],
            "ledger_refreshed_snapshots": [],
            "skipped_existing_snapshots": [],
            "missing_candidate_document_snapshots": [],
            "registry_refresh": {},
        }
        for candidate_snapshot_dir in sorted(path for path in company_dir.iterdir() if path.is_dir()):
            if requested_snapshot_id and candidate_snapshot_dir.name != requested_snapshot_id:
                continue
            scanned_snapshot_count += 1
            candidate_doc_path = candidate_snapshot_dir / "candidate_documents.json"
            normalized_dir = candidate_snapshot_dir / "normalized_artifacts"
            materialized_path = normalized_dir / "materialized_candidate_documents.json"
            artifact_summary_path = normalized_dir / "artifact_summary.json"
            ledger_path = normalized_dir / "organization_completeness_ledger.json"
            if not candidate_doc_path.exists():
                missing_candidate_documents_count += 1
                company_summary["missing_candidate_document_snapshots"].append(candidate_snapshot_dir.name)
                continue
            needs_artifact_repair = not materialized_path.exists() or not artifact_summary_path.exists()
            needs_ledger_refresh = not ledger_path.exists()
            if not needs_artifact_repair and not needs_ledger_refresh:
                skipped_existing_count += 1
                company_summary["skipped_existing_snapshots"].append(candidate_snapshot_dir.name)
                continue
            repair_candidate_count += 1
            try:
                identity_payload = _load_company_snapshot_identity(
                    candidate_snapshot_dir,
                    fallback_payload=latest_payload,
                )
                target_company = (
                    str(identity_payload.get("canonical_name") or "").strip()
                    or str(identity_payload.get("requested_name") or "").strip()
                    or str(identity_payload.get("company_key") or "").strip()
                    or company_dir.name
                )
                registry_target_company = (
                    str(identity_payload.get("company_key") or "").strip()
                    or company_dir.name
                    or target_company
                )
                artifact_result: dict[str, Any] = {}
                if needs_artifact_repair:
                    artifact_result = build_company_candidate_artifacts(
                        runtime_dir=runtime_root,
                        store=store,
                        target_company=target_company,
                        snapshot_id=candidate_snapshot_dir.name,
                    )
                ledger_result = ensure_organization_completeness_ledger(
                    runtime_dir=runtime_root,
                    store=store,
                    target_company=registry_target_company,
                    snapshot_id=candidate_snapshot_dir.name,
                    asset_view="canonical_merged",
                )
                snapshot_summary = {
                    "snapshot_id": candidate_snapshot_dir.name,
                    "target_company": str(artifact_result.get("target_company") or target_company),
                    "artifact_dir": str(artifact_result.get("artifact_dir") or normalized_dir),
                    "artifact_paths": dict(artifact_result.get("artifact_paths") or {}),
                    "candidate_count": int(dict(artifact_result.get("summary") or {}).get("candidate_count") or 0),
                    "ledger_path": str(ledger_result.get("ledger_path") or ""),
                }
                if needs_artifact_repair:
                    repaired_snapshot_count += 1
                    company_summary["repaired_snapshots"].append(snapshot_summary)
                else:
                    ledger_refresh_only_count += 1
                    company_summary["ledger_refreshed_snapshots"].append(snapshot_summary)
            except Exception as exc:
                errors.append(
                    {
                        "company_dir": company_dir.name,
                        "snapshot_id": candidate_snapshot_dir.name,
                        "error": str(exc),
                    }
                )
        touched_snapshots = list(company_summary.get("repaired_snapshots") or [])
        if touched_snapshots:
            try:
                refresh_result = backfill_organization_asset_registry_for_company(
                    runtime_dir=runtime_root,
                    store=store,
                    target_company=company_dir.name,
                    asset_view="canonical_merged",
                )
                registry_refresh_count += 1
            except Exception as exc:
                refresh_result = {
                    "status": "failed",
                    "error": str(exc),
                }
                errors.append(
                    {
                        "company_dir": company_dir.name,
                        "snapshot_id": "",
                        "error": f"organization registry refresh failed: {exc}",
                    }
                )
            company_summary["registry_refresh"] = refresh_result
        if (
            company_summary["repaired_snapshots"]
            or company_summary["ledger_refreshed_snapshots"]
            or company_summary["skipped_existing_snapshots"]
            or company_summary["missing_candidate_document_snapshots"]
        ):
            company_summaries.append(company_summary)

    return {
        "status": "completed_with_errors" if errors else "completed",
        "runtime_dir": str(runtime_root),
        "company_filters": [str(item or "").strip() for item in list(companies or []) if str(item or "").strip()],
        "snapshot_filter": requested_snapshot_id,
        "scanned_snapshot_count": scanned_snapshot_count,
        "repair_candidate_count": repair_candidate_count,
        "repaired_snapshot_count": repaired_snapshot_count,
        "ledger_refresh_only_count": ledger_refresh_only_count,
        "skipped_existing_count": skipped_existing_count,
        "missing_candidate_documents_count": missing_candidate_documents_count,
        "registry_refresh_count": registry_refresh_count,
        "errors": errors,
        "companies": company_summaries,
    }


def load_company_snapshot_candidate_documents(
    *,
    runtime_dir: str | Path,
    target_company: str,
    snapshot_id: str = "",
    view: str = "canonical_merged",
) -> dict[str, Any]:
    company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(runtime_dir, target_company, snapshot_id=snapshot_id)
    company_name = str(identity_payload.get("canonical_name") or target_company).strip() or target_company
    normalized_dir = snapshot_dir / "normalized_artifacts"
    asset_view = str(view or "canonical_merged").strip() or "canonical_merged"
    source_kind = "materialized_candidate_documents"
    artifact_summary_path = normalized_dir / "artifact_summary.json"
    if asset_view == "canonical_merged":
        payload_path = normalized_dir / "materialized_candidate_documents.json"
    else:
        payload_path = normalized_dir / asset_view / "materialized_candidate_documents.json"
        artifact_summary_path = normalized_dir / asset_view / "artifact_summary.json"
        source_kind = f"materialized_candidate_documents:{asset_view}"
    if asset_view == "canonical_merged" and not payload_path.exists():
        payload_path = snapshot_dir / "candidate_documents.json"
        source_kind = "candidate_documents"
    if not payload_path.exists():
        raise CandidateArtifactError(f"No candidate document payload found under {snapshot_dir}")
    try:
        payload = json.loads(payload_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise CandidateArtifactError(f"Failed to load candidate payload from {payload_path}") from exc
    if not isinstance(payload, dict):
        raise CandidateArtifactError(f"Unexpected candidate payload format in {payload_path}")

    target_keys = {_normalize_key(target_company), _normalize_key(company_name), _normalize_key(company_key)}
    candidates: list[Candidate] = []
    candidate_ids: set[str] = set()
    for item in list(payload.get("candidates") or []):
        candidate = _candidate_from_payload(item)
        if candidate is None:
            continue
        if _normalize_key(candidate.target_company) not in target_keys:
            continue
        candidates.append(candidate)
        candidate_ids.add(candidate.candidate_id)

    evidence_records = build_evidence_records_from_payloads(list(payload.get("evidence") or []))
    evidence = [item.to_record() for item in evidence_records if not candidate_ids or item.candidate_id in candidate_ids]
    artifact_summary = {}
    if artifact_summary_path.exists():
        try:
            artifact_summary = json.loads(artifact_summary_path.read_text())
        except (OSError, json.JSONDecodeError):
            artifact_summary = {}

    return {
        "status": "loaded",
        "target_company": company_name,
        "company_key": company_key,
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": str(snapshot_dir),
        "company_identity": identity_payload,
        "asset_view": asset_view,
        "source_kind": source_kind,
        "source_path": str(payload_path),
        "artifact_summary": artifact_summary,
        "candidates": candidates,
        "evidence": evidence,
    }


def build_evidence_records_from_payloads(payloads: list[dict[str, Any]]) -> list[EvidenceRecord]:
    records: list[EvidenceRecord] = []
    for item in payloads:
        candidate_id = str(item.get("candidate_id") or "").strip()
        source_type = str(item.get("source_type") or "").strip()
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        source_dataset = str(item.get("source_dataset") or "").strip()
        source_path = str(item.get("source_path") or "").strip()
        if not candidate_id or not source_type:
            continue
        evidence_id = str(item.get("evidence_id") or "").strip() or make_evidence_id(
            candidate_id,
            source_dataset or source_type,
            title,
            url or source_path,
        )
        records.append(
            EvidenceRecord(
                evidence_id=evidence_id,
                candidate_id=candidate_id,
                source_type=source_type,
                title=title,
                url=url,
                summary=str(item.get("summary") or "").strip(),
                source_dataset=source_dataset,
                source_path=source_path,
                metadata=dict(item.get("metadata") or {}),
            )
        )
    return records


def _load_company_history_snapshots(company_dir: Path, target_company: str) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for snapshot_dir in sorted(path for path in company_dir.iterdir() if path.is_dir()):
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        if not candidate_doc_path.exists():
            continue
        try:
            payload = json.loads(candidate_doc_path.read_text())
        except json.JSONDecodeError:
            continue
        candidates: list[Candidate] = []
        for item in list(payload.get("candidates") or []):
            candidate = _candidate_from_payload(item)
            if candidate is None:
                continue
            if _normalize_key(candidate.target_company) != _normalize_key(target_company):
                continue
            candidates.append(candidate)
        if not candidates:
            continue
        evidence: list[dict[str, Any]] = []
        for item in list(payload.get("evidence") or []):
            evidence_item = _evidence_payload_from_item(item)
            if evidence_item is not None:
                evidence.append(evidence_item)
        snapshots.append(
            {
                "snapshot_id": snapshot_dir.name,
                "source_path": str(candidate_doc_path),
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "candidates": candidates,
                "evidence": evidence,
            }
        )
    return snapshots


def _select_source_snapshots_for_materialization(
    source_snapshots: list[dict[str, Any]],
    *,
    current_snapshot_id: str,
    preferred_source_snapshot_ids: list[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selection = {
        "mode": "all_history_plus_sqlite",
        "reason": "",
        "selected_snapshot_ids": [str(item.get("snapshot_id") or "").strip() for item in source_snapshots],
        "excluded_snapshot_ids": [],
    }
    if len(source_snapshots) <= 1:
        return source_snapshots, selection

    current_snapshot = next(
        (item for item in source_snapshots if str(item.get("snapshot_id") or "").strip() == current_snapshot_id),
        None,
    )
    if current_snapshot is None:
        selection["reason"] = "current_snapshot_candidate_documents_missing"
        return source_snapshots, selection

    preferred_ids = [
        str(item or "").strip()
        for item in list(preferred_source_snapshot_ids or [])
        if str(item or "").strip()
    ]
    if preferred_ids:
        source_index = {
            str(item.get("snapshot_id") or "").strip(): item
            for item in source_snapshots
            if str(item.get("snapshot_id") or "").strip()
        }
        selected_ids: list[str] = []
        for snapshot_ref in [current_snapshot_id, *preferred_ids]:
            if snapshot_ref in source_index and snapshot_ref not in selected_ids:
                selected_ids.append(snapshot_ref)
        if selected_ids:
            selection.update(
                {
                    "mode": "preferred_snapshot_subset",
                    "reason": (
                        "Explicit preferred baseline snapshots were requested for incremental materialization; "
                        "preserve current snapshot plus selected historical baselines."
                    ),
                    "selected_snapshot_ids": selected_ids,
                    "excluded_snapshot_ids": [
                        str(item.get("snapshot_id") or "").strip()
                        for item in source_snapshots
                        if str(item.get("snapshot_id") or "").strip() not in selected_ids
                    ],
                }
            )
            return [source_index[snapshot_ref] for snapshot_ref in selected_ids], selection

    current_candidate_count = int(current_snapshot.get("candidate_count") or 0)
    if current_candidate_count < _LARGE_ORG_HISTORY_SNAPSHOT_MIN_CANDIDATES:
        return source_snapshots, selection

    selected = [current_snapshot]
    selection.update(
        {
            "mode": "current_snapshot_only_large_org",
            "reason": (
                "Current snapshot candidate_documents already cover a large organization; "
                "skip older snapshot unions and rely on current snapshot plus SQLite to avoid dirty historical inflation."
            ),
            "selected_snapshot_ids": [current_snapshot_id],
            "excluded_snapshot_ids": [
                str(item.get("snapshot_id") or "").strip()
                for item in source_snapshots
                if str(item.get("snapshot_id") or "").strip() != current_snapshot_id
            ],
        }
    )
    return selected, selection


def _candidate_from_payload(payload: dict[str, Any]) -> Candidate | None:
    if not isinstance(payload, dict):
        return None
    record = {
        "candidate_id": str(payload.get("candidate_id") or "").strip(),
        "name_en": str(payload.get("name_en") or "").strip(),
        "name_zh": str(payload.get("name_zh") or "").strip(),
        "display_name": str(payload.get("display_name") or "").strip(),
        "category": str(payload.get("category") or "").strip(),
        "target_company": str(payload.get("target_company") or "").strip(),
        "organization": str(payload.get("organization") or "").strip(),
        "employment_status": str(payload.get("employment_status") or "").strip(),
        "role": str(payload.get("role") or "").strip(),
        "team": str(payload.get("team") or "").strip(),
        "joined_at": str(payload.get("joined_at") or "").strip(),
        "left_at": str(payload.get("left_at") or "").strip(),
        "current_destination": str(payload.get("current_destination") or "").strip(),
        "ethnicity_background": str(payload.get("ethnicity_background") or "").strip(),
        "investment_involvement": str(payload.get("investment_involvement") or "").strip(),
        "focus_areas": str(payload.get("focus_areas") or "").strip(),
        "education": str(payload.get("education") or "").strip(),
        "work_history": str(payload.get("work_history") or "").strip(),
        "notes": str(payload.get("notes") or "").strip(),
        "linkedin_url": str(payload.get("linkedin_url") or "").strip(),
        "media_url": str(payload.get("media_url") or "").strip(),
        "source_dataset": str(payload.get("source_dataset") or "").strip(),
        "source_path": str(payload.get("source_path") or "").strip(),
        "metadata": dict(payload.get("metadata") or {}),
    }
    if not record["candidate_id"] or not record["name_en"] or not record["target_company"]:
        return None
    if not record["display_name"]:
        record["display_name"] = record["name_en"]
    return normalize_candidate(Candidate(**record))


def _ingest_materialized_candidate(
    candidate: Candidate,
    *,
    merged_candidates: dict[str, Candidate],
    candidate_aliases: dict[str, str],
    identity_index: dict[str, str],
    prefer_incoming: bool,
    evidence: list[dict[str, Any]] | None = None,
) -> None:
    dedupe_keys = _candidate_dedupe_keys(candidate, evidence=evidence)
    canonical_id = ""
    for key in dedupe_keys:
        canonical_id = identity_index.get(key, "")
        if canonical_id:
            break
    canonical_id = canonical_id or candidate.candidate_id
    canonical_candidate = candidate if canonical_id == candidate.candidate_id else _candidate_with_id(candidate, canonical_id)
    existing = merged_candidates.get(canonical_id)
    merged_candidates[canonical_id] = (
        canonical_candidate if existing is None else _merge_candidates(existing, canonical_candidate, prefer_incoming=prefer_incoming)
    )
    candidate_aliases[candidate.candidate_id] = canonical_id
    candidate_aliases.setdefault(canonical_id, canonical_id)
    for key in dedupe_keys:
        identity_index.setdefault(key, canonical_id)


def _candidate_with_id(candidate: Candidate, candidate_id: str) -> Candidate:
    record = candidate.to_record()
    record["candidate_id"] = candidate_id
    return Candidate(**record)


def _remap_evidence_candidate(payload: dict[str, Any], candidate_aliases: dict[str, str]) -> dict[str, Any]:
    record = dict(payload)
    candidate_id = str(record.get("candidate_id") or "").strip()
    canonical_id = candidate_aliases.get(candidate_id, candidate_id)
    record["candidate_id"] = canonical_id
    record["evidence_id"] = make_evidence_id(
        canonical_id,
        str(record.get("source_dataset") or record.get("source_type") or "").strip(),
        str(record.get("title") or "").strip(),
        str(record.get("url") or record.get("source_path") or "").strip(),
    )
    return record


def _consolidate_materialized_duplicates(
    merged_candidates: dict[str, Candidate],
    merged_evidence: dict[str, dict[str, Any]],
) -> tuple[dict[str, Candidate], dict[str, dict[str, Any]]]:
    evidence_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for evidence in merged_evidence.values():
        candidate_id = str(evidence.get("candidate_id") or "").strip()
        if candidate_id:
            evidence_by_candidate[candidate_id].append(dict(evidence))

    consolidated_candidates: dict[str, Candidate] = {}
    candidate_aliases: dict[str, str] = {}
    identity_index: dict[str, str] = {}
    for candidate in merged_candidates.values():
        _ingest_materialized_candidate(
            candidate,
            merged_candidates=consolidated_candidates,
            candidate_aliases=candidate_aliases,
            identity_index=identity_index,
            prefer_incoming=True,
            evidence=evidence_by_candidate.get(candidate.candidate_id, []),
        )

    consolidated_evidence: dict[str, dict[str, Any]] = {}
    for evidence in merged_evidence.values():
        remapped = _remap_evidence_candidate(evidence, candidate_aliases)
        consolidated_evidence[_evidence_key(remapped)] = remapped
    return consolidated_candidates, consolidated_evidence


def _candidate_dedupe_keys(candidate: Candidate, *, evidence: list[dict[str, Any]] | None = None) -> list[str]:
    metadata = dict(candidate.metadata or {})
    evidence = evidence or []
    evidence_identifier_inputs: list[Any] = []
    for item in evidence:
        url = str(item.get("url") or "").strip()
        if "linkedin.com/in/" in url.lower():
            evidence_identifier_inputs.append(url)
        metadata_item = dict(item.get("metadata") or {})
        for key in ["profile_url", "linkedin_url", "public_identifier", "username"]:
            value = metadata_item.get(key)
            if value:
                evidence_identifier_inputs.append(value)
    identifiers = _linkedin_identifier_set(
        [
            candidate.linkedin_url,
            metadata.get("profile_url"),
            metadata.get("public_identifier"),
            metadata.get("seed_slug"),
            metadata.get("more_profiles"),
            evidence_identifier_inputs,
        ]
    )
    return [f"linkedin:{item}" for item in sorted(identifiers)]


def _linkedin_identifier_set(values: list[Any]) -> set[str]:
    identifiers: set[str] = set()
    for value in values:
        _append_linkedin_identifier(identifiers, value)
    return identifiers


def _append_linkedin_identifier(identifiers: set[str], value: Any) -> None:
    if isinstance(value, dict):
        for key in ["url", "profile_url", "linkedin_url", "public_identifier", "username"]:
            _append_linkedin_identifier(identifiers, value.get(key))
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _append_linkedin_identifier(identifiers, item)
        return
    normalized = _normalize_linkedin_identifier(str(value or ""))
    if normalized:
        identifiers.add(normalized)


def _normalize_linkedin_identifier(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if "linkedin.com" in lowered:
        slug = _extract_linkedin_slug(raw)
        if slug:
            return slug.lower()
        return lowered.rstrip("/")
    return raw.strip().strip("/").lower()


def _extract_linkedin_slug(url: str) -> str:
    match = re.search(r"linkedin\.com/in/([^/?#]+)", str(url or ""), re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()


def _evidence_payload_from_item(payload: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    candidate_id = str(payload.get("candidate_id") or "").strip()
    source_type = str(payload.get("source_type") or "").strip()
    title = str(payload.get("title") or "").strip()
    url = str(payload.get("url") or "").strip()
    source_dataset = str(payload.get("source_dataset") or "").strip()
    source_path = str(payload.get("source_path") or "").strip()
    if not candidate_id or not source_type:
        return None
    return {
        "evidence_id": str(payload.get("evidence_id") or "").strip() or make_evidence_id(
            candidate_id,
            source_dataset or source_type,
            title,
            url or source_path,
        ),
        "candidate_id": candidate_id,
        "source_type": source_type,
        "title": title,
        "url": url,
        "summary": str(payload.get("summary") or "").strip(),
        "source_dataset": source_dataset,
        "source_path": source_path,
        "metadata": dict(payload.get("metadata") or {}),
    }


def _merge_candidates(existing: Candidate, incoming: Candidate, *, prefer_incoming: bool) -> Candidate:
    existing_score = _materialized_merge_score(existing)
    incoming_score = _materialized_merge_score(incoming)
    if incoming_score > existing_score:
        primary = incoming
        secondary = existing
    elif existing_score > incoming_score:
        primary = existing
        secondary = incoming
    else:
        primary = incoming if prefer_incoming else existing
        secondary = existing if prefer_incoming else incoming
    merged = merge_candidate(primary, secondary)
    authoritative = _select_authoritative_membership_candidate(primary, secondary)
    if authoritative is None:
        return merged
    return _apply_authoritative_membership_fields(merged, authoritative)


def _materialized_merge_score(candidate: Candidate) -> int:
    metadata = dict(candidate.metadata or {})
    score = 0
    status = str(candidate.employment_status or "").strip().lower()
    category = str(candidate.category or "").strip().lower()
    if metadata.get("membership_claim_category"):
        score += 30
    if metadata.get("public_identifier"):
        score += 20
    if metadata.get("profile_url"):
        score += 15
    if status == "current":
        score += 12
    elif status == "former":
        score += 6
    if category == "employee":
        score += 10
    elif category == "former_employee":
        score += 5
    for value in [
        candidate.linkedin_url,
        candidate.role,
        candidate.team,
        candidate.focus_areas,
        candidate.education,
        candidate.work_history,
        candidate.notes,
    ]:
        if str(value or "").strip():
            score += 1
    return score


def _select_authoritative_membership_candidate(*candidates: Candidate) -> Candidate | None:
    ranked = sorted(candidates, key=_authoritative_membership_rank, reverse=True)
    if not ranked:
        return None
    best = ranked[0]
    if _authoritative_membership_rank(best) <= 0:
        return None
    return best


def _authoritative_membership_rank(candidate: Candidate) -> int:
    metadata = dict(candidate.metadata or {})
    score = 0
    if _has_explicit_manual_review_resolution(candidate):
        score += 100
    if str(metadata.get("membership_review_decision") or "").strip():
        score += 8
    if candidate.category == "non_member":
        score += 6
    if bool(metadata.get("target_company_mismatch")):
        score += 4
    if bool(metadata.get("membership_review_required")):
        score += 2
    return score


def _apply_authoritative_membership_fields(candidate: Candidate, authoritative: Candidate) -> Candidate:
    record = candidate.to_record()
    source_record = authoritative.to_record()
    metadata = dict(record.get("metadata") or {})
    source_metadata = dict(source_record.get("metadata") or {})

    if _has_explicit_manual_review_resolution(authoritative):
        record["category"] = str(source_record.get("category") or "").strip() or record["category"]
        record["employment_status"] = str(source_record.get("employment_status") or "").strip()
        record["organization"] = str(source_record.get("organization") or "").strip()
        if str(source_record.get("role") or "").strip() or authoritative.category == "non_member":
            record["role"] = str(source_record.get("role") or "").strip()
        if str(source_record.get("team") or "").strip():
            record["team"] = str(source_record.get("team") or "").strip()
        if str(source_record.get("linkedin_url") or "").strip() or authoritative.category == "non_member":
            record["linkedin_url"] = str(source_record.get("linkedin_url") or "").strip()
        if str(source_record.get("media_url") or "").strip() or authoritative.category == "non_member":
            record["media_url"] = str(source_record.get("media_url") or "").strip()
        if str(source_record.get("source_path") or "").strip():
            record["source_path"] = str(source_record.get("source_path") or "").strip()
        if str(source_record.get("source_dataset") or "").strip():
            record["source_dataset"] = str(source_record.get("source_dataset") or "").strip()

    decision = str(source_metadata.get("membership_review_decision") or "").strip()
    if not decision and _has_explicit_manual_review_resolution(authoritative):
        if record["category"] == "non_member" or bool(source_metadata.get("target_company_mismatch")):
            decision = "manual_non_member"
        elif record["category"] in {"employee", "former_employee"} and str(record["employment_status"] or "").strip():
            decision = "manual_confirmed_member"
    if "membership_review_required" in source_metadata or _has_explicit_manual_review_resolution(authoritative):
        metadata["membership_review_required"] = bool(source_metadata.get("membership_review_required"))
    if "membership_review_reason" in source_metadata or _has_explicit_manual_review_resolution(authoritative):
        metadata["membership_review_reason"] = str(source_metadata.get("membership_review_reason") or "").strip()
    if decision or "membership_review_decision" in source_metadata:
        metadata["membership_review_decision"] = decision
    if "membership_review_confidence" in source_metadata:
        metadata["membership_review_confidence"] = str(source_metadata.get("membership_review_confidence") or "").strip()
    if "membership_review_rationale" in source_metadata:
        metadata["membership_review_rationale"] = str(source_metadata.get("membership_review_rationale") or "").strip()
    if "membership_review_triggers" in source_metadata:
        metadata["membership_review_triggers"] = list(source_metadata.get("membership_review_triggers") or [])
    if "membership_review_trigger_keywords" in source_metadata:
        metadata["membership_review_trigger_keywords"] = list(source_metadata.get("membership_review_trigger_keywords") or [])
    if "target_company_mismatch" in source_metadata:
        metadata["target_company_mismatch"] = bool(source_metadata.get("target_company_mismatch"))
    for key in ["manual_review_links", "manual_review_artifact_root", "manual_review_signals", "profile_url", "public_identifier"]:
        if key in source_metadata:
            metadata[key] = source_metadata.get(key)
    for key in [
        "profile_account_id",
        "profile_location",
        "more_profiles",
        "membership_claim_category",
        "membership_claim_employment_status",
        "raw_linkedin_url",
        "sanity_linkedin_url",
        "source_shards",
        "source_jobs",
        "headline",
        "summary",
        "about",
        "location",
        "languages",
        "skills",
    ]:
        incoming_value = source_metadata.get(key)
        if incoming_value in ("", None, [], {}):
            continue
        existing_value = metadata.get(key)
        if existing_value in ("", None, [], {}):
            metadata[key] = incoming_value

    record["metadata"] = metadata
    return Candidate(**record)


def _evidence_key(payload: dict[str, Any]) -> str:
    evidence_id = str(payload.get("evidence_id") or "").strip()
    if evidence_id:
        return evidence_id
    return make_evidence_id(
        str(payload.get("candidate_id") or "").strip(),
        str(payload.get("source_dataset") or payload.get("source_type") or "").strip(),
        str(payload.get("title") or "").strip(),
        str(payload.get("url") or payload.get("source_path") or "").strip(),
    )


def _normalize_candidate(candidate: Candidate, evidence: list[dict[str, Any]]) -> dict[str, Any]:
    metadata = dict(candidate.metadata or {})
    functional_facets = derive_candidate_facets(candidate)
    role_bucket = derive_candidate_role_bucket(candidate)
    source_datasets = sorted(
        {
            str(item).strip()
            for item in [candidate.source_dataset] + [str(item.get("source_dataset") or "").strip() for item in evidence]
            if str(item).strip()
        }
    )
    source_types = sorted({str(item.get("source_type") or "").strip() for item in evidence if str(item.get("source_type") or "").strip()})
    urls = _dedupe_urls(
        [candidate.linkedin_url, candidate.media_url]
        + [str(item.get("url") or "").strip() for item in evidence]
    )
    manual_review_confirmed = any(item.get("source_dataset") in {"manual_review", "manual_review_analysis"} for item in evidence) and candidate.category != "lead"
    has_explicit_profile_capture = _has_explicit_profile_capture(evidence)
    has_profile_detail = _has_reusable_profile_fields(candidate) or has_explicit_profile_capture
    has_linkedin_url = bool(str(candidate.linkedin_url or "").strip())
    membership_review_required = bool(metadata.get("membership_review_required"))
    status_bucket = _status_bucket(candidate, membership_review_required=membership_review_required)
    needs_profile_completion = has_linkedin_url and not has_profile_detail
    needs_manual_review = membership_review_required or status_bucket == "lead" or (not has_linkedin_url and not manual_review_confirmed)
    reason = ""
    if membership_review_required:
        reason = str(metadata.get("membership_review_reason") or "").strip() or "suspicious_membership"
    elif status_bucket == "lead":
        reason = "unresolved_lead"
    elif not has_linkedin_url and not manual_review_confirmed:
        reason = "missing_linkedin"
    elif not has_profile_detail:
        reason = "profile_detail_gap"
    return {
        "candidate_id": candidate.candidate_id,
        "display_name": candidate.display_name,
        "name_en": candidate.name_en,
        "category": candidate.category,
        "employment_status": candidate.employment_status,
        "status_bucket": status_bucket,
        "role_bucket": role_bucket,
        "functional_facets": functional_facets,
        "role": candidate.role,
        "team": candidate.team,
        "focus_areas": candidate.focus_areas,
        "has_linkedin_url": has_linkedin_url,
        "has_profile_detail": has_profile_detail,
        "has_explicit_profile_capture": has_explicit_profile_capture,
        "needs_profile_completion": needs_profile_completion,
        "manual_review_confirmed": manual_review_confirmed,
        "needs_manual_review": needs_manual_review,
        "manual_review_reason": reason,
        "membership_review_required": membership_review_required,
        "membership_review_decision": str(metadata.get("membership_review_decision") or "").strip(),
        "manual_review_rationale": str(metadata.get("membership_review_rationale") or "").strip(),
        "manual_review_triggers": list(metadata.get("membership_review_triggers") or []),
        "manual_review_trigger_keywords": list(metadata.get("membership_review_trigger_keywords") or []),
        "evidence_count": len(evidence),
        "source_datasets": source_datasets,
        "source_types": source_types,
        "urls": urls[:12],
        "source_path": candidate.source_path,
    }


def _build_reusable_document(candidate: Candidate, evidence: list[dict[str, Any]], normalized: dict[str, Any]) -> dict[str, Any]:
    evidence_summaries = [str(item.get("summary") or "").strip() for item in evidence if str(item.get("summary") or "").strip()]
    evidence_titles = [str(item.get("title") or "").strip() for item in evidence if str(item.get("title") or "").strip()]
    candidate_notes = candidate_profile_signal_text(candidate, include_notes=True)
    profile_document = candidate_searchable_text(candidate, include_notes=True)
    evidence_document = " | ".join(evidence_summaries[:8] or evidence_titles[:8])
    semantic_document = " | ".join(
        part
        for part in [
            profile_document,
            normalized["status_bucket"],
            normalized["role_bucket"],
            " ".join(normalized["functional_facets"]),
            evidence_document,
        ]
        if str(part or "").strip()
    )
    return {
        "candidate_id": candidate.candidate_id,
        "display_name": candidate.display_name,
        "status_bucket": normalized["status_bucket"],
        "role_bucket": normalized["role_bucket"],
        "functional_facets": normalized["functional_facets"],
        "manual_review_confirmed": normalized["manual_review_confirmed"],
        "has_profile_detail": normalized["has_profile_detail"],
        "has_explicit_profile_capture": normalized["has_explicit_profile_capture"],
        "has_linkedin_url": normalized["has_linkedin_url"],
        "needs_profile_completion": normalized["needs_profile_completion"],
        "source_datasets": normalized["source_datasets"],
        "source_types": normalized["source_types"],
        "urls": normalized["urls"],
        "profile_document": profile_document,
        "evidence_document": evidence_document,
        "semantic_document": semantic_document,
    }


def _write_artifact_view(
    *,
    logger: AssetLogger,
    artifact_dir: Path,
    materialized_view: dict[str, Any],
    candidates: list[Candidate],
    evidence: list[dict[str, Any]],
    evidence_by_candidate: dict[str, list[dict[str, Any]]],
    asset_view: str,
) -> dict[str, Any]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    payloads = _build_artifact_view_payloads(
        materialized_view=materialized_view,
        candidates=candidates,
        evidence=evidence,
        evidence_by_candidate=evidence_by_candidate,
        asset_view=asset_view,
    )
    materialized_path = logger.write_json(
        artifact_dir / "materialized_candidate_documents.json",
        payloads["materialized_documents"],
        asset_type="materialized_candidate_documents",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    normalized_path = logger.write_json(
        artifact_dir / "normalized_candidates.json",
        payloads["normalized_candidates"],
        asset_type="normalized_candidates",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    reusable_path = logger.write_json(
        artifact_dir / "reusable_candidate_documents.json",
        payloads["reusable_documents"],
        asset_type="reusable_candidate_documents",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    backlog_path = logger.write_json(
        artifact_dir / "manual_review_backlog.json",
        payloads["manual_review_backlog"],
        asset_type="manual_review_backlog",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    profile_completion_backlog_path = logger.write_json(
        artifact_dir / "profile_completion_backlog.json",
        payloads["profile_completion_backlog"],
        asset_type="profile_completion_backlog",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    summary_path = logger.write_json(
        artifact_dir / "artifact_summary.json",
        payloads["artifact_summary"],
        asset_type="candidate_artifact_summary",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    return {
        "artifact_paths": {
            "materialized_candidate_documents": str(materialized_path),
            "normalized_candidates": str(normalized_path),
            "reusable_candidate_documents": str(reusable_path),
            "manual_review_backlog": str(backlog_path),
            "profile_completion_backlog": str(profile_completion_backlog_path),
            "artifact_summary": str(summary_path),
        },
        "summary": payloads["artifact_summary"],
    }


def _build_artifact_view_payloads(
    *,
    materialized_view: dict[str, Any],
    candidates: list[Candidate],
    evidence: list[dict[str, Any]],
    evidence_by_candidate: dict[str, list[dict[str, Any]]],
    asset_view: str,
) -> dict[str, Any]:
    normalized_candidates: list[dict[str, Any]] = []
    reusable_documents: list[dict[str, Any]] = []
    manual_review_backlog: list[dict[str, Any]] = []
    profile_completion_backlog: list[dict[str, Any]] = []
    status_counter: Counter[str] = Counter()
    profile_detail_count = 0
    explicit_profile_capture_count = 0
    manual_review_confirmed_count = 0
    missing_linkedin_count = 0

    for candidate in candidates:
        candidate_evidence = evidence_by_candidate.get(candidate.candidate_id, [])
        normalized = _normalize_candidate(candidate, candidate_evidence)
        normalized_candidates.append(normalized)
        reusable_documents.append(_build_reusable_document(candidate, candidate_evidence, normalized))
        status_counter[normalized["status_bucket"]] += 1
        if normalized["has_profile_detail"]:
            profile_detail_count += 1
        if normalized["has_explicit_profile_capture"]:
            explicit_profile_capture_count += 1
        if normalized["manual_review_confirmed"]:
            manual_review_confirmed_count += 1
        if not normalized["has_linkedin_url"]:
            missing_linkedin_count += 1
        if normalized["needs_profile_completion"]:
            profile_completion_backlog.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "display_name": candidate.display_name,
                    "status_bucket": normalized["status_bucket"],
                    "category": candidate.category,
                    "employment_status": candidate.employment_status,
                    "reason": "profile_detail_gap",
                    "linkedin_url": candidate.linkedin_url,
                    "source_datasets": normalized["source_datasets"],
                }
            )
        if normalized["needs_manual_review"]:
            manual_review_backlog.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "display_name": candidate.display_name,
                    "status_bucket": normalized["status_bucket"],
                    "category": candidate.category,
                    "employment_status": candidate.employment_status,
                    "reason": normalized["manual_review_reason"],
                    "known_urls": normalized["urls"],
                    "source_datasets": normalized["source_datasets"],
                }
            )

    materialized_documents = {
        "snapshot": {
            "company_key": materialized_view["company_key"],
            "snapshot_id": str(Path(materialized_view["snapshot_dir"]).name),
            "snapshot_dir": str(materialized_view["snapshot_dir"]),
            "materialized_at": _utc_now_iso(),
            "materialized_from": "sqlite_plus_company_snapshots",
            "asset_view": asset_view,
            "target_company": materialized_view["target_company"],
            "company_identity": materialized_view["company_identity"],
            "source_snapshots": [
                {
                    "snapshot_id": item["snapshot_id"],
                    "candidate_count": item["candidate_count"],
                    "evidence_count": item["evidence_count"],
                    "source_path": item["source_path"],
                }
                for item in materialized_view["source_snapshots"]
            ],
            "source_snapshot_selection": dict(materialized_view.get("source_snapshot_selection") or {}),
            "sqlite_candidate_count": materialized_view["sqlite_candidate_count"],
            "sqlite_evidence_count": materialized_view["sqlite_evidence_count"],
        },
        "candidates": [candidate.to_record() for candidate in candidates],
        "evidence": evidence,
    }
    artifact_summary = {
        "target_company": materialized_view["target_company"],
        "company_key": materialized_view["company_key"],
        "snapshot_id": str(Path(materialized_view["snapshot_dir"]).name),
        "asset_view": asset_view,
        "candidate_count": len(candidates),
        "evidence_count": len(evidence),
        "status_counts": dict(status_counter),
        "profile_detail_count": profile_detail_count,
        "explicit_profile_capture_count": explicit_profile_capture_count,
        "manual_review_confirmed_count": manual_review_confirmed_count,
        "missing_linkedin_count": missing_linkedin_count,
        "manual_review_backlog_count": len(manual_review_backlog),
        "profile_completion_backlog_count": len(profile_completion_backlog),
        "source_snapshot_count": len(materialized_view["source_snapshots"]),
        "source_snapshot_selection_mode": str(
            (materialized_view.get("source_snapshot_selection") or {}).get("mode") or "all_history_plus_sqlite"
        ),
        "source_snapshot_selection": dict(materialized_view.get("source_snapshot_selection") or {}),
        "sqlite_candidate_count": materialized_view["sqlite_candidate_count"],
        "sqlite_evidence_count": materialized_view["sqlite_evidence_count"],
        "created_at": _utc_now_iso(),
    }
    return {
        "materialized_documents": materialized_documents,
        "normalized_candidates": normalized_candidates,
        "reusable_documents": reusable_documents,
        "manual_review_backlog": manual_review_backlog,
        "profile_completion_backlog": profile_completion_backlog,
        "artifact_summary": artifact_summary,
    }


def _is_strict_roster_candidate(candidate: Candidate, evidence: list[dict[str, Any]]) -> bool:
    metadata = dict(candidate.metadata or {})
    if candidate.category not in {"employee", "former_employee", "investor"}:
        return False
    if bool(metadata.get("membership_review_required")):
        return False
    if bool(metadata.get("target_company_mismatch")):
        return False
    membership_decision = str(metadata.get("membership_review_decision") or "").strip().lower()
    if membership_decision.endswith("non_member"):
        return False
    if _has_explicit_manual_review_resolution(candidate):
        return True
    explicit_profile_capture = _has_explicit_profile_capture(evidence)
    functional_facets = derive_candidate_facets(candidate)
    source_tokens = " ".join(
        [
            str(candidate.source_dataset or "").strip().lower(),
            " ".join(str(item.get("source_dataset") or "").strip().lower() for item in evidence),
            " ".join(str(item.get("source_type") or "").strip().lower() for item in evidence),
        ]
    )
    if any(
        token in source_tokens
        for token in [
            "linkedin_company_people",
            "company_employees",
            "company_roster",
            "roster",
            "harvest_profile_search",
            "search_seed",
            "profile_detail",
            "manual_review",
        ]
    ):
        if not functional_facets:
            return False
        return explicit_profile_capture or bool(str(candidate.linkedin_url or "").strip())
    if explicit_profile_capture and functional_facets:
        return True
    return False


def _resolve_company_snapshot(runtime_dir: str | Path, target_company: str, *, snapshot_id: str = "") -> tuple[str, Path, dict[str, Any]]:
    root = Path(runtime_dir)
    company_assets_dir = root / "company_assets"
    normalized_target = _normalize_key(target_company)
    matched_company_dirs: list[tuple[int, str, Path, dict[str, Any]]] = []
    for candidate in sorted(company_assets_dir.iterdir()) if company_assets_dir.exists() else []:
        if not candidate.is_dir():
            continue
        latest_payload = _load_company_snapshot_json(candidate / "latest_snapshot.json")
        preview_identity = dict(latest_payload.get("company_identity") or {})
        preview_snapshot_id = str(snapshot_id or latest_payload.get("snapshot_id") or "").strip()
        if snapshot_id and not (candidate / snapshot_id).exists():
            continue
        if preview_snapshot_id:
            preview_identity = _load_company_snapshot_identity(candidate / preview_snapshot_id, fallback_payload=latest_payload)
        match_score = _score_company_snapshot_dir_match(candidate, preview_identity, normalized_target)
        if match_score > 0:
            matched_company_dirs.append((match_score, preview_snapshot_id, candidate, latest_payload))
    if not matched_company_dirs:
        raise CandidateArtifactError(f"Company assets not found for {target_company}")
    _, _, company_dir, latest_payload = max(
        matched_company_dirs,
        key=lambda item: (item[0], item[1], item[2].name),
    )
    resolved_snapshot_id = str(snapshot_id or latest_payload.get("snapshot_id") or "").strip()
    if not resolved_snapshot_id:
        snapshot_dirs = sorted(path.name for path in company_dir.iterdir() if path.is_dir())
        if not snapshot_dirs:
            raise CandidateArtifactError(f"No snapshots found under {company_dir}")
        resolved_snapshot_id = snapshot_dirs[-1]
    snapshot_dir = company_dir / resolved_snapshot_id
    if not snapshot_dir.exists():
        raise CandidateArtifactError(f"Snapshot not found: {snapshot_dir}")
    identity_payload = _load_company_snapshot_identity(snapshot_dir, fallback_payload=latest_payload)
    resolved_company_key = str(identity_payload.get("company_key") or company_dir.name).strip() or company_dir.name
    return resolved_company_key, snapshot_dir, identity_payload


def _load_company_snapshot_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _load_company_snapshot_identity(snapshot_dir: Path, *, fallback_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    for path, key_path in [
        (snapshot_dir / "identity.json", ("",)),
        (snapshot_dir / "manifest.json", ("company_identity",)),
        (snapshot_dir / "candidate_documents.json", ("snapshot", "company_identity")),
    ]:
        payload = _load_company_snapshot_json(path)
        if not payload:
            continue
        current: Any = payload
        for key in key_path:
            if not key:
                break
            current = dict(current).get(key) if isinstance(current, dict) else {}
        if isinstance(current, dict) and current:
            return dict(current)
    return dict((fallback_payload or {}).get("company_identity") or {})


def _score_company_snapshot_dir_match(company_dir: Path, identity_payload: dict[str, Any], normalized_target: str) -> int:
    if not normalized_target:
        return 0
    company_dir_key = _normalize_key(company_dir.name)
    if company_dir_key and company_dir_key == normalized_target:
        return 98
    for field in ("requested_name", "canonical_name"):
        value = _normalize_key(str(identity_payload.get(field) or ""))
        if value and value == normalized_target:
            return 120

    company_key = _normalize_key(str(identity_payload.get("company_key") or ""))
    if company_key and company_key == normalized_target:
        return 110

    linkedin_slug = _normalize_key(str(identity_payload.get("linkedin_slug") or ""))
    if linkedin_slug and linkedin_slug == normalized_target:
        return 105

    aliases = [
        _normalize_key(str(item))
        for item in list(identity_payload.get("aliases") or [])
        if str(item).strip()
    ]
    if normalized_target in aliases:
        return 100

    directory_key = _normalize_key(company_dir.name)
    if directory_key == normalized_target:
        return 90
    return 0


def _status_bucket(candidate: Candidate, *, membership_review_required: bool = False) -> str:
    if membership_review_required:
        return "lead"
    status = str(candidate.employment_status or "").strip().lower()
    category = str(candidate.category or "").strip().lower()
    if status == "current":
        return "current"
    if status == "former":
        return "former"
    if category == "lead":
        return "lead"
    return category or "unknown"


def _has_explicit_profile_capture(evidence: list[dict[str, Any]]) -> bool:
    return any(
        str(item.get("source_type") or "").strip() in {"linkedin_profile_detail", "manual_review_analysis", "linkedin_profile_non_member"}
        for item in evidence
    )


def _has_explicit_manual_review_resolution(candidate: Candidate) -> bool:
    metadata = dict(candidate.metadata or {})
    if str(metadata.get("manual_review_artifact_root") or "").strip():
        return True
    if list(metadata.get("manual_review_links") or []):
        return True
    return str(metadata.get("membership_review_decision") or "").strip().lower().startswith("manual_")


def _has_reusable_profile_fields(candidate: Candidate) -> bool:
    if str(candidate.education or "").strip():
        return True
    work_history = str(candidate.work_history or "").strip()
    if not work_history:
        return False
    if _is_roster_baseline_only(candidate):
        return False
    return True


def _is_roster_baseline_only(candidate: Candidate) -> bool:
    source_dataset = str(candidate.source_dataset or "").strip()
    role = str(candidate.role or "").strip()
    work_history = str(candidate.work_history or "").strip()
    notes = str(candidate.notes or "")
    return (
        source_dataset.endswith("_linkedin_company_people")
        and bool(work_history)
        and work_history == role
        and "LinkedIn company roster baseline." in notes
        and not str(candidate.education or "").strip()
    )


def _dedupe_urls(items: list[str]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for item in items:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        results.append(value)
    return results


def _normalize_key(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
