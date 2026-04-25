from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

from .asset_paths import resolve_company_snapshot_dir, resolve_snapshot_dir_from_source_path
from .candidate_artifacts import (
    CandidateArtifactError,
    load_authoritative_company_snapshot_candidate_documents,
)
from .company_registry import normalize_company_key, resolve_company_alias_key
from .domain import Candidate, EvidenceRecord, JobRequest
from .snapshot_state import (
    company_identity_from_record,
    load_candidate_document_state,
    read_json_dict,
)
from .storage import SQLiteStore

SNAPSHOT_CANDIDATE_SOURCE_KINDS = {"company_snapshot"}
OUTREACH_LAYER_KEY_BY_INDEX = {
    0: "layer_0_roster",
    1: "layer_1_name_signal",
    2: "layer_2_greater_china_region_experience",
    3: "layer_3_mainland_china_experience_or_chinese_language",
}


def candidate_documents_fallback_enabled(*, store: SQLiteStore | None) -> bool:
    resolver = getattr(store, "candidate_documents_fallback_enabled", None)
    if callable(resolver):
        try:
            return bool(resolver())
        except Exception:
            return False
    return False


def bootstrap_candidate_store_enabled(*, store: SQLiteStore | None) -> bool:
    resolver = getattr(store, "bootstrap_candidate_store_enabled", None)
    if callable(resolver):
        try:
            return bool(resolver())
        except Exception:
            return False
    return False


def candidate_source_is_snapshot_authoritative(candidate_source: Mapping[str, Any] | None) -> bool:
    return (
        str(dict(candidate_source or {}).get("source_kind") or "").strip().lower() in SNAPSHOT_CANDIDATE_SOURCE_KINDS
    )


def empty_candidate_source(
    *,
    target_company: str,
    reason: str,
    snapshot_id: str = "",
    asset_view: str = "",
) -> dict[str, Any]:
    return {
        "source_kind": "empty_source",
        "target_company": str(target_company or "").strip(),
        "snapshot_id": str(snapshot_id or "").strip(),
        "asset_view": str(asset_view or "").strip(),
        "source_path": "",
        "candidates": [],
        "evidence_lookup": {},
        "reason": str(reason or "").strip(),
    }


def load_company_snapshot_candidate_documents_with_materialization_fallback(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    snapshot_id: str,
    view: str = "canonical_merged",
    allow_materialization_fallback: bool = True,
    allow_candidate_documents_fallback: bool = False,
) -> dict[str, Any]:
    return load_authoritative_company_snapshot_candidate_documents(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        snapshot_id=snapshot_id,
        view=view,
        allow_materialization_fallback=allow_materialization_fallback,
        allow_candidate_documents_fallback=allow_candidate_documents_fallback,
    )


def bootstrap_candidate_matches_request_company(*, candidate: Candidate, target_company: str) -> bool:
    normalized_target_company = str(target_company or "").strip()
    if not normalized_target_company:
        return True
    target_key = resolve_company_alias_key(normalized_target_company) or normalize_company_key(normalized_target_company)
    candidate_company_values = [
        str(candidate.target_company or "").strip(),
        str(candidate.organization or "").strip(),
        str(candidate.metadata.get("target_company") or "").strip(),
        str(candidate.metadata.get("organization") or "").strip(),
    ]
    for company_value in candidate_company_values:
        if not company_value:
            continue
        candidate_key = resolve_company_alias_key(company_value) or normalize_company_key(company_value)
        if candidate_key and candidate_key == target_key:
            return True
    return False


def load_bootstrap_candidate_source(*, store: SQLiteStore, request: JobRequest) -> dict[str, Any]:
    target_company = str(request.target_company or "").strip()
    if not bootstrap_candidate_store_enabled(store=store):
        return empty_candidate_source(
            target_company=target_company,
            snapshot_id="",
            asset_view="",
            reason="authoritative_snapshot_missing_bootstrap_disabled",
        )
    bootstrap_candidates = list(store.list_candidates())
    if target_company:
        bootstrap_candidates = [
            candidate
            for candidate in bootstrap_candidates
            if bootstrap_candidate_matches_request_company(candidate=candidate, target_company=target_company)
        ]
    return {
        "source_kind": "legacy_bootstrap_store",
        "target_company": target_company,
        "snapshot_id": "",
        "asset_view": "",
        "source_path": "",
        "candidates": bootstrap_candidates,
        "evidence_lookup": {},
    }


def load_control_plane_candidate_source(*, store: SQLiteStore, request: JobRequest) -> dict[str, Any]:
    target_company = str(request.target_company or "").strip()
    candidates = (
        list(store.list_candidates_for_company(target_company))
        if target_company
        else list(store.list_candidates())
    )
    return {
        "source_kind": "control_plane_candidate_registry",
        "target_company": target_company,
        "snapshot_id": "",
        "asset_view": "",
        "source_path": "",
        "candidates": candidates,
        "evidence_lookup": {},
    }


def load_candidate_document_candidate_source(
    *,
    request: JobRequest,
    candidate_doc_path: str | Path,
    snapshot_id: str = "",
    asset_view: str = "",
) -> dict[str, Any]:
    candidate_doc_file = Path(candidate_doc_path).expanduser()
    if not candidate_doc_file.exists():
        return empty_candidate_source(
            target_company=str(request.target_company or "").strip(),
            snapshot_id=str(snapshot_id or "").strip(),
            asset_view=str(asset_view or request.asset_view or "").strip(),
            reason="candidate_documents_override_missing",
        )
    state = load_candidate_document_state(candidate_doc_file)
    candidates = list(state.get("candidates") or [])
    evidence = list(state.get("evidence") or [])
    if not candidates:
        return empty_candidate_source(
            target_company=str(request.target_company or "").strip(),
            snapshot_id=str(snapshot_id or "").strip(),
            asset_view=str(asset_view or request.asset_view or "").strip(),
            reason="candidate_documents_override_empty",
        )
    payload = read_json_dict(candidate_doc_file)
    snapshot_payload = dict(payload.get("snapshot") or {})
    company_identity = company_identity_from_record(dict(snapshot_payload.get("company_identity") or {}))
    resolved_snapshot_id = (
        str(snapshot_id or "").strip()
        or str(snapshot_payload.get("snapshot_id") or "").strip()
        or candidate_doc_file.parent.name
    )
    resolved_target_company = (
        str(request.target_company or "").strip()
        or str(snapshot_payload.get("target_company") or "").strip()
        or str(company_identity.canonical_name if company_identity is not None else "").strip()
    )
    resolved_asset_view = str(asset_view or request.asset_view or "canonical_merged").strip() or "canonical_merged"
    evidence_lookup: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in evidence:
        if isinstance(item, EvidenceRecord):
            record = item.to_record()
        else:
            record = dict(item or {})
        candidate_id = str(record.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        evidence_lookup[candidate_id].append(record)
    return {
        "source_kind": "company_snapshot",
        "target_company": resolved_target_company,
        "snapshot_id": resolved_snapshot_id,
        "asset_view": resolved_asset_view,
        "source_path": str(candidate_doc_file),
        "candidates": candidates,
        "evidence_lookup": dict(evidence_lookup),
    }


def load_retrieval_candidate_source(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    request: JobRequest,
    snapshot_id: str = "",
    allow_materialization_fallback: bool = True,
    allow_legacy_bootstrap_fallback: bool = False,
    candidate_doc_path: str | Path | None = None,
) -> dict[str, Any]:
    if candidate_doc_path not in (None, ""):
        candidate_doc_source = load_candidate_document_candidate_source(
            request=request,
            candidate_doc_path=candidate_doc_path,
            snapshot_id=snapshot_id,
            asset_view=request.asset_view,
        )
        if list(candidate_doc_source.get("candidates") or []):
            return candidate_doc_source
    if request.target_company:
        try:
            snapshot_payload = load_company_snapshot_candidate_documents_with_materialization_fallback(
                runtime_dir=runtime_dir,
                store=store,
                target_company=request.target_company,
                snapshot_id=snapshot_id,
                view=request.asset_view,
                allow_materialization_fallback=allow_materialization_fallback,
                allow_candidate_documents_fallback=bool(str(snapshot_id or "").strip())
                or str(request.target_scope or "").strip().lower() == "full_company_asset",
            )
        except CandidateArtifactError:
            if request.asset_view != "canonical_merged":
                raise
            snapshot_payload = {}
        if list(snapshot_payload.get("candidates") or []):
            evidence_lookup: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in list(snapshot_payload.get("evidence") or []):
                candidate_id = str(item.get("candidate_id") or "").strip()
                if not candidate_id:
                    continue
                evidence_lookup[candidate_id].append(item)
            return {
                "source_kind": "company_snapshot",
                "target_company": str(snapshot_payload.get("target_company") or request.target_company or "").strip(),
                "snapshot_id": str(snapshot_payload.get("snapshot_id") or ""),
                "asset_view": str(snapshot_payload.get("asset_view") or "canonical_merged"),
                "source_path": str(snapshot_payload.get("source_path") or ""),
                "candidates": list(snapshot_payload.get("candidates") or []),
                "evidence_lookup": dict(evidence_lookup),
            }
    if not str(request.target_company or "").strip() and str(request.target_scope or "").strip().lower() != "full_company_asset":
        return load_control_plane_candidate_source(store=store, request=request)
    if allow_legacy_bootstrap_fallback:
        return load_bootstrap_candidate_source(store=store, request=request)
    return empty_candidate_source(
        target_company=str(request.target_company or "").strip(),
        snapshot_id="",
        asset_view=str(request.asset_view or "").strip(),
        reason="authoritative_snapshot_missing",
    )


def resolve_candidate_source_snapshot_dir(
    *,
    runtime_dir: str | Path,
    request: JobRequest,
    candidate_source: dict[str, Any],
) -> Path | None:
    source_path_value = str(candidate_source.get("source_path") or "").strip()
    if source_path_value:
        snapshot_dir = resolve_snapshot_dir_from_source_path(
            source_path_value,
            runtime_dir=runtime_dir,
        )
        if snapshot_dir is not None:
            return snapshot_dir
    target_company = str(candidate_source.get("target_company") or request.target_company or "").strip()
    snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
    if not target_company or not snapshot_id:
        return None
    snapshot_dir = resolve_company_snapshot_dir(
        runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
    )
    return snapshot_dir if snapshot_dir is not None and snapshot_dir.exists() else None


def load_outreach_layering_context(
    *,
    runtime_dir: str | Path,
    request: JobRequest,
    candidate_source: dict[str, Any],
    runtime_policy: dict[str, Any],
) -> dict[str, Any]:
    if not candidate_source_is_snapshot_authoritative(candidate_source):
        return {}
    snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
    if not snapshot_id:
        return {}
    analysis_path = ""
    runtime_layering = dict(runtime_policy.get("outreach_layering") or {})
    runtime_analysis_paths = dict(runtime_layering.get("analysis_paths") or {})
    runtime_full_path = str(runtime_analysis_paths.get("full") or "").strip()
    if runtime_full_path and Path(runtime_full_path).exists():
        analysis_path = runtime_full_path
    else:
        snapshot_dir = resolve_candidate_source_snapshot_dir(
            runtime_dir=runtime_dir,
            request=request,
            candidate_source=candidate_source,
        )
        if snapshot_dir is not None:
            candidates = sorted(
                snapshot_dir.glob("layered_segmentation/greater_china_outreach_*/layered_analysis.json")
            )
            if candidates:
                analysis_path = str(candidates[-1])
    if not analysis_path:
        return {}
    try:
        analysis_payload = json.loads(Path(analysis_path).read_text())
    except Exception:
        return {}
    layer_map: dict[str, dict[str, Any]] = {}
    for item in list(analysis_payload.get("candidates") or []):
        candidate_id = str(item.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        final_layer = int(item.get("final_layer") or 0)
        layer_map[candidate_id] = {
            "outreach_layer": final_layer,
            "outreach_layer_key": OUTREACH_LAYER_KEY_BY_INDEX.get(final_layer, "layer_0_roster"),
            "outreach_layer_source": str(item.get("final_layer_source") or ""),
        }
    if not layer_map:
        return {}
    layer_schema = dict(analysis_payload.get("layer_schema") or {})
    primary_layer_keys = [
        str(item).strip() for item in list(layer_schema.get("primary_layer_keys") or []) if str(item).strip()
    ] or list(OUTREACH_LAYER_KEY_BY_INDEX.values())
    layer_counts = {
        key: int((analysis_payload.get("layers") or {}).get(key, {}).get("count") or 0)
        for key in primary_layer_keys
    }
    return {
        "status": str(analysis_payload.get("status") or "completed"),
        "snapshot_id": snapshot_id,
        "analysis_path": analysis_path,
        "candidate_layer_coverage": len(layer_map),
        "layer_map": layer_map,
        "layer_counts": layer_counts,
        "cumulative_layer_counts": dict(analysis_payload.get("cumulative_layer_counts") or {}),
        "ai_verification": dict(analysis_payload.get("ai_verification") or {}),
    }
