from __future__ import annotations

import json
import shutil
from dataclasses import replace
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

from .acquisition import _build_former_filter_hints
from .asset_catalog import AssetCatalog
from .asset_logger import AssetLogger
from .asset_paths import load_company_snapshot_json
from .candidate_artifacts import (
    CandidateArtifactError,
    _resolve_company_snapshot,
    build_company_candidate_artifacts,
    build_evidence_records_from_payloads,
)
from .canonicalization import canonicalize_company_records
from .company_asset_completion import CompanyAssetCompletionManager
from .connectors import CompanyIdentity, CompanyRosterSnapshot, build_candidates_from_roster, resolve_company_identity
from .domain import (
    Candidate,
    EvidenceRecord,
    make_candidate_id,
    make_evidence_id,
    normalize_candidate,
    normalize_name_token,
)
from .harvest_connectors import HarvestProfileSearchConnector
from .ingestion import load_bootstrap_bundle
from .model_provider import DeterministicModelClient, ModelClient
from .profile_registry_utils import extract_profile_registry_aliases_from_payload
from .search_provider import build_search_provider
from .seed_discovery import SearchSeedAcquirer, SearchSeedSnapshot, build_candidates_from_seed_snapshot
from .settings import AppSettings
from .storage import SQLiteStore


class CompanyAssetSupplementManager:
    def __init__(
        self,
        *,
        runtime_dir: str | Path,
        store: SQLiteStore,
        settings: AppSettings,
        model_client: ModelClient | None = None,
        asset_completion_manager: CompanyAssetCompletionManager | None = None,
    ) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.store = store
        self.settings = settings
        self.model_client = model_client or DeterministicModelClient()
        self.asset_completion_manager = asset_completion_manager or CompanyAssetCompletionManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=settings,
            model_client=self.model_client,
        )

    def supplement_snapshot(
        self,
        *,
        target_company: str,
        snapshot_id: str = "",
        rebuild_linkedin_stage_1: bool = False,
        run_former_search_seed: bool = False,
        former_search_limit: int = 25,
        former_search_pages: int = 1,
        former_search_queries: list[str] | None = None,
        former_filter_hints: dict[str, list[str]] | None = None,
        profile_scope: str = "none",
        profile_limit: int = 0,
        profile_only_missing_detail: bool = False,
        profile_force_refresh: bool = False,
        repair_current_roster_profile_refs: bool = False,
        repair_current_roster_registry_aliases: bool = False,
        build_artifacts: bool = True,
    ) -> dict[str, Any]:
        if (
            not rebuild_linkedin_stage_1
            and
            not run_former_search_seed
            and str(profile_scope or "none").strip().lower() == "none"
            and not repair_current_roster_profile_refs
            and not repair_current_roster_registry_aliases
        ):
            return {"status": "invalid", "reason": "At least one supplement step must be enabled."}

        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(self.runtime_dir, target_company, snapshot_id=snapshot_id)
        identity = _company_identity_from_payload(identity_payload, fallback_company=target_company, fallback_company_key=company_key)
        logger = AssetLogger(snapshot_dir)

        linkedin_stage_1_rebuild: dict[str, Any] = {}
        if rebuild_linkedin_stage_1:
            linkedin_stage_1_rebuild = self.rebuild_linkedin_stage_1_snapshot(
                target_company=identity.canonical_name or target_company,
                snapshot_id=snapshot_dir.name,
            )

        former_result: dict[str, Any] = {}
        if run_former_search_seed:
            former_result = self.run_former_search_seed(
                target_company=identity.canonical_name or target_company,
                snapshot_id=snapshot_dir.name,
                limit=former_search_limit,
                pages=former_search_pages,
                queries=list(former_search_queries or []),
                filter_hints=dict(former_filter_hints or {}),
            )

        profile_result: dict[str, Any] = {}
        normalized_profile_scope = _normalize_profile_scope(profile_scope)
        if normalized_profile_scope != "none":
            profile_result = self.asset_completion_manager.complete_snapshot_profiles(
                target_company=identity.canonical_name or target_company,
                snapshot_id=snapshot_dir.name,
                employment_scope=normalized_profile_scope,
                profile_limit=profile_limit,
                only_missing_profile_detail=profile_only_missing_detail,
                force_refresh=profile_force_refresh,
                build_artifacts=False,
            )

        roster_profile_ref_repair: dict[str, Any] = {}
        if repair_current_roster_profile_refs:
            roster_profile_ref_repair = self.repair_current_roster_profile_refs(
                target_company=identity.canonical_name or target_company,
                snapshot_id=snapshot_dir.name,
            )

        roster_registry_alias_repair: dict[str, Any] = {}
        if repair_current_roster_registry_aliases:
            roster_registry_alias_repair = self.repair_current_roster_registry_aliases(
                target_company=identity.canonical_name or target_company,
                snapshot_id=snapshot_dir.name,
            )

        artifact_result: dict[str, Any] = {}
        if build_artifacts:
            artifact_result = build_company_candidate_artifacts(
                runtime_dir=self.runtime_dir,
                store=self.store,
                target_company=identity.canonical_name or target_company,
                snapshot_id=snapshot_dir.name,
            )

        supplement_dir = snapshot_dir / "incremental_supplement"
        supplement_dir.mkdir(parents=True, exist_ok=True)
        summary = {
            "status": "completed",
            "target_company": identity.canonical_name or target_company,
            "company_key": identity.company_key or company_key,
            "snapshot_id": snapshot_dir.name,
            "performed_at": datetime.now(timezone.utc).isoformat(),
            "linkedin_stage_1_rebuild": linkedin_stage_1_rebuild,
            "former_search_seed": former_result,
            "profile_enrichment": profile_result,
            "current_roster_profile_ref_repair": roster_profile_ref_repair,
            "current_roster_registry_alias_repair": roster_registry_alias_repair,
            "artifact_result": artifact_result,
        }
        summary_path = logger.write_json(
            supplement_dir / "supplement_summary.json",
            summary,
            asset_type="company_asset_supplement_summary",
            source_kind="company_asset_supplement",
            is_raw_asset=False,
            model_safe=True,
        )
        summary["summary_path"] = str(summary_path)
        return summary

    def import_local_bootstrap_package(
        self,
        *,
        target_company: str,
        snapshot_id: str = "",
        sync_project_local_package: bool = True,
        build_artifacts: bool = True,
    ) -> dict[str, Any]:
        normalized_target_company = str(target_company or "").strip() or "Anthropic"
        if normalize_name_token(normalized_target_company) != normalize_name_token("Anthropic"):
            return {
                "status": "invalid",
                "reason": "local bootstrap package import is currently supported for Anthropic only",
                "target_company": normalized_target_company,
            }

        package_sync: dict[str, Any] = {"status": "skipped", "reason": "project_sync_disabled"}
        if sync_project_local_package:
            package_sync = self.sync_project_local_anthropic_package()
            if str(package_sync.get("status") or "") == "failed":
                return {
                    "status": "failed",
                    "reason": str(package_sync.get("reason") or "project_local_package_sync_failed"),
                    "target_company": normalized_target_company,
                    "package_sync": package_sync,
                }

        catalog = AssetCatalog.discover()
        bundle = load_bootstrap_bundle(catalog)
        summary = self.merge_candidates_into_snapshot(
            target_company=normalized_target_company,
            snapshot_id=snapshot_id,
            candidates=list(bundle.candidates),
            evidence=list(bundle.evidence),
            source_kind="anthropic_local_bootstrap_package",
            source_summary={
                "catalog_source": catalog.anthropic_asset_source,
                "catalog_root": str(catalog.anthropic_root),
                "project_root": str(catalog.anthropic_project_root or ""),
                "external_root": str(catalog.anthropic_external_root or ""),
                "package_sync": package_sync,
                "bootstrap_assets": dict(bundle.stats.get("assets") or {}),
                "bootstrap_candidate_breakdown": dict(bundle.stats.get("candidate_counts") or {}),
                "bootstrap_evidence_count": int(bundle.stats.get("evidence_count") or len(bundle.evidence)),
            },
            build_artifacts=build_artifacts,
            create_snapshot_if_missing=True,
        )
        summary["package_sync"] = package_sync
        return summary

    def sync_project_local_anthropic_package(self) -> dict[str, Any]:
        catalog = AssetCatalog.discover()
        project_package_root = self.settings.project_root / "local_asset_packages" / "anthropic"
        manifest_path = project_package_root / "package_manifest.json"
        if catalog.anthropic_asset_source == "project_local" and catalog.anthropic_project_root is not None:
            return {
                "status": "completed",
                "mode": "already_project_local",
                "project_package_root": str(project_package_root),
                "catalog_root": str(catalog.anthropic_root),
                "manifest_path": str(manifest_path) if manifest_path.exists() else "",
            }

        external_root = catalog.anthropic_external_root or catalog.anthropic_root
        if external_root is None or not Path(external_root).exists():
            return {
                "status": "failed",
                "reason": "external_anthropic_asset_package_not_found",
                "project_package_root": str(project_package_root),
            }

        project_package_root.mkdir(parents=True, exist_ok=True)
        (project_package_root / "data").mkdir(parents=True, exist_ok=True)
        copied_files: list[dict[str, Any]] = []
        copy_pairs = {
            catalog.anthropic_workbook: project_package_root / catalog.anthropic_workbook.name,
            catalog.anthropic_readme: project_package_root / "README.md",
            catalog.anthropic_progress: project_package_root / "PROGRESS.md",
            catalog.legacy_api_accounts: project_package_root / "api_accounts.json",
            catalog.legacy_company_ids: project_package_root / "company_ids.json",
            catalog.investor_members_json: project_package_root / "investor_chinese_members_final.json",
            catalog.anthropic_publications: project_package_root / "data" / "publications_unified.json",
            catalog.scholar_scan_results: project_package_root / "data" / "scholar_scan_results.json",
        }
        for source_path, target_path in copy_pairs.items():
            if not source_path.exists():
                continue
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target_path)
            copied_files.append(
                {
                    "source_path": str(source_path),
                    "target_path": str(target_path),
                    "size_bytes": int(target_path.stat().st_size or 0),
                }
            )

        manifest_payload = {
            "package": "anthropic",
            "status": "completed",
            "source": "external_legacy",
            "source_root": str(external_root),
            "project_package_root": str(project_package_root),
            "copied_at": datetime.now(timezone.utc).isoformat(),
            "selected_workbook": str(project_package_root / catalog.anthropic_workbook.name),
            "copied_file_count": len(copied_files),
            "files": copied_files,
        }
        manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "status": "completed",
            "mode": "copied_from_external_legacy",
            "project_package_root": str(project_package_root),
            "manifest_path": str(manifest_path),
            "copied_file_count": len(copied_files),
            "files": copied_files,
        }

    def merge_candidates_into_snapshot(
        self,
        *,
        target_company: str,
        snapshot_id: str = "",
        candidates: list[Candidate],
        evidence: list[EvidenceRecord],
        source_kind: str,
        source_summary: dict[str, Any] | None = None,
        build_artifacts: bool = True,
        create_snapshot_if_missing: bool = False,
    ) -> dict[str, Any]:
        incoming_candidates = [candidate for candidate in list(candidates or []) if isinstance(candidate, Candidate)]
        incoming_evidence = [item for item in list(evidence or []) if isinstance(item, EvidenceRecord)]
        if not incoming_candidates:
            return {
                "status": "skipped",
                "reason": "no_candidates_to_merge",
                "target_company": target_company,
                "source_kind": source_kind,
            }

        company_key, snapshot_dir, identity, snapshot_created = _resolve_or_create_company_snapshot(
            runtime_dir=self.runtime_dir,
            target_company=target_company,
            snapshot_id=snapshot_id,
            create_if_missing=create_snapshot_if_missing,
        )
        logger = AssetLogger(snapshot_dir)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        existing_payload = dict(load_company_snapshot_json(candidate_doc_path) or {})
        existing_candidates = _candidate_records_from_payload(existing_payload)
        existing_evidence = build_evidence_records_from_payloads(list(existing_payload.get("evidence") or []))
        projected_candidates, projected_evidence, projection_summary = _project_supplement_records_to_snapshot(
            incoming_candidates,
            incoming_evidence,
            target_company=identity.canonical_name or target_company,
        )

        canonical_candidates, canonical_evidence, canonicalization_summary = canonicalize_company_records(
            [*existing_candidates, *projected_candidates],
            [*existing_evidence, *projected_evidence],
        )
        for candidate in canonical_candidates:
            self.store.upsert_candidate(candidate)
        if canonical_evidence:
            self.store.upsert_evidence_records(canonical_evidence)

        existing_sources = dict(existing_payload.get("acquisition_sources") or {})
        source_key = str(source_kind or "supplement_merge").strip() or "supplement_merge"
        existing_source_summary = dict(existing_sources.get(source_key) or {})
        merged_source_summary = {
            **existing_source_summary,
            **dict(source_summary or {}),
            "source_kind": source_key,
            "merged_at": datetime.now(timezone.utc).isoformat(),
            "projected_candidate_count": len(projected_candidates),
            "projected_evidence_count": len(projected_evidence),
            "projection_summary": projection_summary,
        }
        existing_sources[source_key] = merged_source_summary

        snapshot_payload = dict(existing_payload.get("snapshot") or {})
        snapshot_payload.setdefault("snapshot_id", snapshot_dir.name)
        snapshot_payload.setdefault("target_company", identity.canonical_name or target_company)
        snapshot_payload["company_identity"] = identity.to_record()
        snapshot_payload.setdefault("source_kind", source_key)

        candidate_payload = {
            **existing_payload,
            "snapshot": snapshot_payload,
            "acquisition_sources": existing_sources,
            "candidates": [candidate.to_record() for candidate in canonical_candidates],
            "evidence": [item.to_record() for item in canonical_evidence],
            "candidate_count": len(canonical_candidates),
            "evidence_count": len(canonical_evidence),
            "enrichment_mode": "incremental_supplement",
            "enrichment_scope": "supplement",
            "acquisition_canonicalization": canonicalization_summary,
            "enrichment_summary": {
                **dict(existing_payload.get("enrichment_summary") or {}),
                "rebuild_mode": "incremental_supplement_merge",
                "last_supplement_source": source_key,
                "snapshot_authoritative": True,
            },
            "acquisition_stage": {
                **dict(existing_payload.get("acquisition_stage") or {}),
                "task_type": source_key,
                "phase": "incremental_supplement",
                "phase_title": "Incremental Supplement",
            },
        }

        candidate_doc_path = logger.write_json(
            candidate_doc_path,
            candidate_payload,
            asset_type="candidate_documents",
            source_kind=source_key,
            is_raw_asset=False,
            model_safe=True,
        )
        stage_candidate_doc_path = logger.write_json(
            snapshot_dir / f"candidate_documents.{_stage_archive_label(source_key)}.json",
            candidate_payload,
            asset_type="candidate_documents",
            source_kind=source_key,
            is_raw_asset=False,
            model_safe=True,
        )
        manifest_path = logger.write_json(
            snapshot_dir / "manifest.json",
            {
                "snapshot_id": snapshot_dir.name,
                "company_identity": identity.to_record(),
                "candidate_count": len(canonical_candidates),
                "evidence_count": len(canonical_evidence),
                "normalization_scope": {"mode": "company"},
                "historical_profile_inheritance": {},
                "canonicalization": canonicalization_summary,
                "storage": {
                    "execution": "local_runtime",
                    "candidate_store": str(self.store.db_path),
                },
            },
            asset_type="snapshot_manifest",
            source_kind=source_key,
            is_raw_asset=False,
            model_safe=True,
        )

        artifact_result: dict[str, Any] = {}
        if build_artifacts:
            artifact_result = build_company_candidate_artifacts(
                runtime_dir=self.runtime_dir,
                store=self.store,
                target_company=identity.canonical_name or target_company,
                snapshot_id=snapshot_dir.name,
            )

        supplement_dir = snapshot_dir / "incremental_supplement"
        supplement_dir.mkdir(parents=True, exist_ok=True)
        summary = {
            "status": "completed",
            "target_company": identity.canonical_name or target_company,
            "company_key": identity.company_key or company_key,
            "snapshot_id": snapshot_dir.name,
            "snapshot_created": snapshot_created,
            "performed_at": datetime.now(timezone.utc).isoformat(),
            "source_kind": source_key,
            "incoming_candidate_count": len(incoming_candidates),
            "incoming_evidence_count": len(incoming_evidence),
            "projected_candidate_count": len(projected_candidates),
            "projected_evidence_count": len(projected_evidence),
            "candidate_count": len(canonical_candidates),
            "evidence_count": len(canonical_evidence),
            "projection_summary": projection_summary,
            "canonicalization": canonicalization_summary,
            "candidate_doc_path": str(candidate_doc_path),
            "stage_candidate_doc_path": str(stage_candidate_doc_path),
            "manifest_path": str(manifest_path),
            "artifact_result": artifact_result,
            "source_summary": dict(source_summary or {}),
        }
        summary_path = logger.write_json(
            supplement_dir / f"{_stage_archive_label(source_key)}_summary.json",
            summary,
            asset_type="company_asset_incremental_supplement_summary",
            source_kind=source_key,
            is_raw_asset=False,
            model_safe=True,
        )
        summary["summary_path"] = str(summary_path)
        return summary

    def rebuild_linkedin_stage_1_snapshot(
        self,
        *,
        target_company: str,
        snapshot_id: str = "",
    ) -> dict[str, Any]:
        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(self.runtime_dir, target_company, snapshot_id=snapshot_id)
        identity = _company_identity_from_payload(identity_payload, fallback_company=target_company, fallback_company_key=company_key)
        logger = AssetLogger(snapshot_dir)
        supplement_dir = snapshot_dir / "incremental_supplement"
        supplement_dir.mkdir(parents=True, exist_ok=True)

        roster_snapshot = _restore_roster_snapshot_from_snapshot_dir(snapshot_dir=snapshot_dir, identity=identity)
        search_seed_snapshot = _restore_search_seed_snapshot_from_snapshot_dir(snapshot_dir=snapshot_dir, identity=identity)
        if roster_snapshot is None and search_seed_snapshot is None:
            return {
                "status": "skipped",
                "reason": "missing_roster_and_search_seed_snapshots",
                "target_company": identity.canonical_name or target_company,
                "snapshot_id": snapshot_dir.name,
            }

        candidates: list[Candidate] = []
        evidence = []
        acquisition_sources: dict[str, Any] = {}
        roster_candidate_count = 0
        search_seed_candidate_count = 0
        if roster_snapshot is not None:
            roster_candidates, roster_evidence = build_candidates_from_roster(roster_snapshot)
            roster_candidate_count = len(roster_candidates)
            candidates.extend(roster_candidates)
            evidence.extend(roster_evidence)
            acquisition_sources["roster_snapshot"] = roster_snapshot.to_record()
        if search_seed_snapshot is not None:
            search_seed_candidates, search_seed_evidence = build_candidates_from_seed_snapshot(search_seed_snapshot)
            search_seed_candidate_count = len(search_seed_candidates)
            candidates.extend(search_seed_candidates)
            evidence.extend(search_seed_evidence)
            acquisition_sources["search_seed_snapshot"] = search_seed_snapshot.to_record()

        canonical_candidates = candidates
        canonical_evidence = evidence
        canonicalization_summary: dict[str, Any] = {}
        if len(candidates) > 1:
            canonical_candidates, canonical_evidence, canonicalization_summary = canonicalize_company_records(candidates, evidence)

        candidate_payload = {
            "snapshot": (
                roster_snapshot.to_record()
                if roster_snapshot is not None
                else (search_seed_snapshot.to_record() if search_seed_snapshot is not None else {})
            ),
            "acquisition_sources": acquisition_sources,
            "candidates": [candidate.to_record() for candidate in canonical_candidates],
            "evidence": [item.to_record() for item in canonical_evidence],
            "candidate_count": len(canonical_candidates),
            "evidence_count": len(canonical_evidence),
            "enrichment_mode": "linkedin_stage_1",
            "enrichment_scope": "linkedin_stage_1",
            "acquisition_canonicalization": canonicalization_summary,
            "enrichment_summary": {
                "resolved_profile_count": 0,
                "unresolved_candidate_count": 0,
                "publication_match_count": 0,
                "lead_candidate_count": 0,
                "coauthor_edge_count": 0,
                "queued_harvest_worker_count": 0,
                "queued_exploration_count": 0,
                "stop_reason": "",
                "artifact_paths": {},
                "errors": [],
                "rebuild_mode": "supplement_snapshot_stage_1",
            },
            "acquisition_stage": {
                "phase": "linkedin_stage_1",
                "phase_title": "LinkedIn Stage 1",
                "task_id": "supplement::rebuild_linkedin_stage_1",
                "task_type": "rebuild_linkedin_stage_1_snapshot",
            },
            "next_connectors": {
                "note": "LinkedIn stage-1 baseline rebuilt from roster and search-seed snapshot assets.",
            },
        }
        candidate_doc_path = logger.write_json(
            snapshot_dir / "candidate_documents.json",
            candidate_payload,
            asset_type="candidate_documents",
            source_kind="linkedin_stage_1_rebuild",
            is_raw_asset=False,
            model_safe=True,
        )
        stage_candidate_doc_path = logger.write_json(
            snapshot_dir / "candidate_documents.linkedin_stage_1.json",
            candidate_payload,
            asset_type="candidate_documents",
            source_kind="linkedin_stage_1_rebuild",
            is_raw_asset=False,
            model_safe=True,
        )

        self.store.replace_company_data(identity.canonical_name or target_company, canonical_candidates, canonical_evidence)
        manifest_path = logger.write_json(
            snapshot_dir / "manifest.json",
            {
                "snapshot_id": snapshot_dir.name,
                "company_identity": identity.to_record(),
                "candidate_count": len(canonical_candidates),
                "evidence_count": len(canonical_evidence),
                "normalization_scope": {"mode": "company"},
                "historical_profile_inheritance": {},
                "canonicalization": canonicalization_summary,
                "storage": {
                    "execution": "local_runtime",
                    "candidate_store": str(self.store.db_path),
                },
            },
            asset_type="snapshot_manifest",
            source_kind="linkedin_stage_1_rebuild",
            is_raw_asset=False,
            model_safe=True,
        )

        summary = {
            "status": "completed",
            "target_company": identity.canonical_name or target_company,
            "snapshot_id": snapshot_dir.name,
            "performed_at": datetime.now(timezone.utc).isoformat(),
            "step": "rebuild_linkedin_stage_1_snapshot",
            "roster_snapshot_present": roster_snapshot is not None,
            "search_seed_snapshot_present": search_seed_snapshot is not None,
            "roster_candidate_count": roster_candidate_count,
            "search_seed_candidate_count": search_seed_candidate_count,
            "candidate_count": len(canonical_candidates),
            "evidence_count": len(canonical_evidence),
            "candidate_doc_path": str(candidate_doc_path),
            "stage_candidate_doc_path": str(stage_candidate_doc_path),
            "manifest_path": str(manifest_path),
            "acquisition_sources": list(acquisition_sources.keys()),
            "canonicalization": canonicalization_summary,
        }
        summary_path = logger.write_json(
            supplement_dir / "rebuild_linkedin_stage_1_summary.json",
            summary,
            asset_type="company_asset_linkedin_stage_1_rebuild_summary",
            source_kind="company_asset_supplement",
            is_raw_asset=False,
            model_safe=True,
        )
        summary["summary_path"] = str(summary_path)
        return summary

    def repair_current_roster_registry_aliases(
        self,
        *,
        target_company: str,
        snapshot_id: str = "",
    ) -> dict[str, Any]:
        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(self.runtime_dir, target_company, snapshot_id=snapshot_id)
        identity = _company_identity_from_payload(identity_payload, fallback_company=target_company, fallback_company_key=company_key)
        logger = AssetLogger(snapshot_dir)
        visible_path = snapshot_dir / "harvest_company_employees" / "harvest_company_employees_visible.json"
        if not visible_path.exists():
            return {
                "status": "skipped",
                "reason": "missing_current_roster_visible_asset",
                "target_company": identity.canonical_name or target_company,
                "snapshot_id": snapshot_dir.name,
            }

        try:
            visible_entries = json.loads(visible_path.read_text())
        except Exception as exc:
            return {
                "status": "failed",
                "reason": f"invalid_current_roster_visible_asset:{exc}",
                "target_company": identity.canonical_name or target_company,
                "snapshot_id": snapshot_dir.name,
            }

        roster_rows_by_url: dict[str, dict[str, Any]] = {}
        for row in list(visible_entries or []):
            if not isinstance(row, dict):
                continue
            raw_linkedin_url = str(row.get("linkedin_url") or row.get("profile_url") or "").strip()
            if not raw_linkedin_url:
                continue
            normalized_key = raw_linkedin_url.rstrip("/").lower()
            if normalized_key and normalized_key not in roster_rows_by_url:
                roster_rows_by_url[normalized_key] = dict(row)

        if not roster_rows_by_url:
            return {
                "status": "skipped",
                "reason": "no_current_roster_urls",
                "target_company": identity.canonical_name or target_company,
                "snapshot_id": snapshot_dir.name,
                "visible_path": str(visible_path),
            }

        company_asset_root = snapshot_dir.parent
        repair_run_key = f"repair_current_roster_registry_aliases::{identity.company_key or company_key}::{snapshot_dir.name}"
        historical_profile_matches: dict[str, dict[str, Any]] = {}
        examined_profile_files = 0

        for historical_snapshot_dir in sorted(
            [path for path in company_asset_root.iterdir() if path.is_dir()],
            key=lambda item: item.name,
            reverse=True,
        ):
            harvest_dir = historical_snapshot_dir / "harvest_profiles"
            if not harvest_dir.exists():
                continue
            for path in sorted(harvest_dir.glob("*.json")):
                if path.name.endswith(".request.json") or path.name.startswith("harvest_profile_batch_"):
                    continue
                examined_profile_files += 1
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if not isinstance(payload, dict):
                    continue
                request_payload = payload.get("_harvest_request")
                request_dict = request_payload if isinstance(request_payload, dict) else {}
                requested_profile_url = str(
                    request_dict.get("profile_url")
                    or request_dict.get("requested_profile_url")
                    or request_dict.get("raw_linkedin_url")
                    or ""
                ).strip()
                if not requested_profile_url:
                    continue
                requested_key = requested_profile_url.rstrip("/").lower()
                if requested_key not in roster_rows_by_url or requested_key in historical_profile_matches:
                    continue
                alias_metadata = extract_profile_registry_aliases_from_payload(payload, requested_url=requested_profile_url)
                historical_profile_matches[requested_key] = {
                    "raw_path": str(path),
                    "snapshot_dir": str(historical_snapshot_dir),
                    "historical_snapshot_id": historical_snapshot_dir.name,
                    "alias_metadata": alias_metadata,
                }

        repaired_count = 0
        missing_count = 0
        repaired_examples: list[dict[str, Any]] = []
        missing_examples: list[dict[str, Any]] = []
        for requested_key, roster_row in roster_rows_by_url.items():
            match = historical_profile_matches.get(requested_key)
            raw_linkedin_url = str(roster_row.get("linkedin_url") or roster_row.get("profile_url") or "").strip()
            if not match:
                missing_count += 1
                if len(missing_examples) < 20:
                    missing_examples.append(
                        {
                            "full_name": str(roster_row.get("full_name") or "").strip(),
                            "raw_linkedin_url": raw_linkedin_url,
                            "source_shard_id": str(roster_row.get("source_shard_id") or "").strip(),
                        }
                    )
                continue

            alias_metadata = dict(match.get("alias_metadata") or {})
            alias_urls = list(alias_metadata.get("alias_urls") or [])
            sanity_linkedin_url = str(alias_metadata.get("sanity_linkedin_url") or "").strip()
            existing_entry = (
                self.store.get_linkedin_profile_registry(raw_linkedin_url)
                or (self.store.get_linkedin_profile_registry(sanity_linkedin_url) if sanity_linkedin_url else None)
                or {}
            )
            canonical_profile_url = (
                str(existing_entry.get("profile_url") or "").strip()
                or sanity_linkedin_url
                or str(alias_metadata.get("raw_linkedin_url") or "").strip()
                or raw_linkedin_url
            )
            source_labels = [
                "repair:current_roster_registry_aliases",
                f"company:{identity.company_key or company_key}",
                f"snapshot:{snapshot_dir.name}",
            ]
            source_shard_id = str(roster_row.get("source_shard_id") or "").strip()
            if source_shard_id:
                source_labels.append(f"source_shard:{source_shard_id}")

            self.store.mark_linkedin_profile_registry_fetched(
                canonical_profile_url,
                raw_path=str(match.get("raw_path") or ""),
                source_shards=source_labels,
                source_jobs=[repair_run_key],
                alias_urls=[*alias_urls, raw_linkedin_url],
                raw_linkedin_url=raw_linkedin_url,
                sanity_linkedin_url=sanity_linkedin_url,
                snapshot_dir=str(match.get("snapshot_dir") or snapshot_dir),
            )
            repaired_count += 1
            if len(repaired_examples) < 20:
                repaired_examples.append(
                    {
                        "full_name": str(roster_row.get("full_name") or "").strip(),
                        "raw_linkedin_url": raw_linkedin_url,
                        "sanity_linkedin_url": sanity_linkedin_url,
                        "historical_snapshot_id": str(match.get("historical_snapshot_id") or ""),
                        "raw_path": str(match.get("raw_path") or ""),
                        "source_shard_id": source_shard_id,
                    }
                )

        supplement_dir = snapshot_dir / "incremental_supplement"
        supplement_dir.mkdir(parents=True, exist_ok=True)
        summary = {
            "status": "completed",
            "target_company": identity.canonical_name or target_company,
            "snapshot_id": snapshot_dir.name,
            "performed_at": datetime.now(timezone.utc).isoformat(),
            "step": "repair_current_roster_registry_aliases",
            "visible_path": str(visible_path),
            "examined_current_roster_count": len(roster_rows_by_url),
            "examined_historical_profile_file_count": examined_profile_files,
            "matched_historical_profile_count": len(historical_profile_matches),
            "repaired_count": repaired_count,
            "missing_count": missing_count,
            "repaired_examples": repaired_examples,
            "missing_examples": missing_examples,
        }
        summary_path = logger.write_json(
            supplement_dir / "current_roster_registry_alias_repair_summary.json",
            summary,
            asset_type="company_asset_current_roster_registry_alias_repair_summary",
            source_kind="company_asset_supplement",
            is_raw_asset=False,
            model_safe=True,
        )
        summary["summary_path"] = str(summary_path)
        return summary

    def repair_current_roster_profile_refs(
        self,
        *,
        target_company: str,
        snapshot_id: str = "",
    ) -> dict[str, Any]:
        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(self.runtime_dir, target_company, snapshot_id=snapshot_id)
        identity = _company_identity_from_payload(identity_payload, fallback_company=target_company, fallback_company_key=company_key)
        logger = AssetLogger(snapshot_dir)
        visible_path = snapshot_dir / "harvest_company_employees" / "harvest_company_employees_visible.json"
        if not visible_path.exists():
            return {
                "status": "skipped",
                "reason": "missing_current_roster_visible_asset",
                "target_company": identity.canonical_name or target_company,
                "snapshot_id": snapshot_dir.name,
            }
        try:
            visible_entries = json.loads(visible_path.read_text())
        except Exception as exc:
            return {
                "status": "failed",
                "reason": f"invalid_current_roster_visible_asset:{exc}",
                "target_company": identity.canonical_name or target_company,
                "snapshot_id": snapshot_dir.name,
            }

        repaired_candidates: list[dict[str, Any]] = []
        examined_count = 0
        repaired_count = 0
        for row in list(visible_entries or []):
            if not isinstance(row, dict):
                continue
            full_name = str(row.get("full_name") or "").strip()
            member_key = str(row.get("member_key") or "").strip()
            linkedin_url = str(row.get("linkedin_url") or "").strip()
            if not full_name or not member_key or not linkedin_url:
                continue
            examined_count += 1
            candidate_id = sha1(
                "|".join(
                    [
                        normalize_name_token(identity.canonical_name or target_company),
                        normalize_name_token(full_name),
                        member_key,
                    ]
                ).encode("utf-8")
            ).hexdigest()[:16]
            candidate = self.store.get_candidate(candidate_id)
            if candidate is None:
                continue
            updated_candidate, changed = _restore_current_roster_profile_ref(candidate, linkedin_url)
            if not changed:
                continue
            self.store.upsert_candidate(updated_candidate)
            repaired_count += 1
            repaired_candidates.append(
                {
                    "candidate_id": candidate_id,
                    "display_name": updated_candidate.display_name,
                    "restored_profile_url": linkedin_url,
                }
            )

        supplement_dir = snapshot_dir / "incremental_supplement"
        supplement_dir.mkdir(parents=True, exist_ok=True)
        summary = {
            "status": "completed",
            "target_company": identity.canonical_name or target_company,
            "snapshot_id": snapshot_dir.name,
            "performed_at": datetime.now(timezone.utc).isoformat(),
            "step": "repair_current_roster_profile_refs",
            "visible_path": str(visible_path),
            "examined_count": examined_count,
            "repaired_count": repaired_count,
            "repaired_candidates": repaired_candidates,
        }
        summary_path = logger.write_json(
            supplement_dir / "current_roster_profile_ref_repair_summary.json",
            summary,
            asset_type="company_asset_current_roster_profile_ref_repair_summary",
            source_kind="company_asset_supplement",
            is_raw_asset=False,
            model_safe=True,
        )
        summary["summary_path"] = str(summary_path)
        return summary

    def run_former_search_seed(
        self,
        *,
        target_company: str,
        snapshot_id: str = "",
        limit: int = 25,
        pages: int = 1,
        queries: list[str] | None = None,
        filter_hints: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(self.runtime_dir, target_company, snapshot_id=snapshot_id)
        identity = _company_identity_from_payload(identity_payload, fallback_company=target_company, fallback_company_key=company_key)
        logger = AssetLogger(snapshot_dir)
        supplement_dir = snapshot_dir / "incremental_supplement"
        supplement_dir.mkdir(parents=True, exist_ok=True)

        existing_candidate_ids = {candidate.candidate_id for candidate in self.store.list_candidates_for_company(identity.canonical_name or target_company)}
        existing_evidence_ids = {
            str(item.get("evidence_id") or "").strip()
            for item in self.store.list_evidence_for_company(identity.canonical_name or target_company)
            if str(item.get("evidence_id") or "").strip()
        }

        requested_limit = max(1, int(limit or 25))
        requested_pages = max(1, int(pages or 1))
        scoped_filter_hints = _inherit_snapshot_former_filter_hints(snapshot_dir, dict(filter_hints or {}))
        effective_filter_hints = _build_former_filter_hints(
            identity=identity,
            base_filter_hints=scoped_filter_hints,
        )
        cost_policy = {
            "provider_people_search_mode": "fallback_only",
            "provider_people_search_min_expected_results": requested_limit,
            "provider_people_search_pages": requested_pages,
            "allow_shared_provider_cache": True,
        }
        search_acquirer = SearchSeedAcquirer(
            [],
            self.model_client,
            harvest_search_connector=_build_harvest_search_connector(self.settings, limit=requested_limit, pages=requested_pages),
            search_provider=build_search_provider(self.settings.search),
        )
        snapshot = search_acquirer.discover(
            identity,
            snapshot_dir,
            asset_logger=logger,
            search_seed_queries=list(queries or []),
            query_bundles=[],
            filter_hints=effective_filter_hints,
            cost_policy=cost_policy,
            employment_status="former",
            worker_runtime=None,
            job_id="",
            request_payload={},
            plan_payload={},
            runtime_mode="maintenance",
        )
        harvest_query_summary = _first_harvest_query_summary(snapshot.query_summaries)
        probe_plan = dict(harvest_query_summary.get("probe") or {})
        effective_limit = max(1, int(harvest_query_summary.get("effective_limit") or requested_limit))
        effective_pages = max(1, int(harvest_query_summary.get("effective_pages") or requested_pages))

        candidates, evidence = build_candidates_from_seed_snapshot(snapshot)
        for candidate in candidates:
            self.store.upsert_candidate(candidate)
        if evidence:
            self.store.upsert_evidence_records(evidence)

        inserted_candidate_count = len([candidate for candidate in candidates if candidate.candidate_id not in existing_candidate_ids])
        inserted_evidence_count = len([item for item in evidence if str(item.evidence_id or "").strip() not in existing_evidence_ids])
        summary = {
            "status": "completed",
            "target_company": identity.canonical_name or target_company,
            "snapshot_id": snapshot_dir.name,
            "performed_at": datetime.now(timezone.utc).isoformat(),
            "step": "former_search_seed_incremental",
            "requested_limit": requested_limit,
            "requested_pages": requested_pages,
            "effective_limit": effective_limit,
            "effective_pages": effective_pages,
            "former_search_probe": probe_plan,
            "filter_hints": effective_filter_hints,
            "search_seed_snapshot": snapshot.to_record(),
            "inserted_candidate_count": inserted_candidate_count,
            "inserted_evidence_count": inserted_evidence_count,
            "candidate_count_from_snapshot": len(candidates),
            "evidence_count_from_snapshot": len(evidence),
        }
        summary_path = logger.write_json(
            supplement_dir / "former_search_seed_summary.json",
            summary,
            asset_type="company_asset_former_search_seed_summary",
            source_kind="company_asset_supplement",
            is_raw_asset=False,
            model_safe=True,
        )
        summary["summary_path"] = str(summary_path)
        return summary


def _build_harvest_search_connector(settings: AppSettings, *, limit: int, pages: int) -> HarvestProfileSearchConnector:
    requested_items = max(int(limit or 25), max(1, int(pages or 1)) * 25)
    actor_settings = replace(
        settings.harvest.profile_search,
        max_paid_items=max(int(settings.harvest.profile_search.max_paid_items or 0), requested_items),
        max_total_charge_usd=max(
            float(settings.harvest.profile_search.max_total_charge_usd or 0.0),
            round(0.10 * max(1, int(pages or 1)), 2),
        ),
    )
    return HarvestProfileSearchConnector(actor_settings)

def _first_harvest_query_summary(query_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    for item in list(query_summaries or []):
        if str(item.get("mode") or "").strip() == "harvest_profile_search":
            return dict(item)
    return {}


def _resolve_or_create_company_snapshot(
    *,
    runtime_dir: str | Path,
    target_company: str,
    snapshot_id: str = "",
    create_if_missing: bool = False,
) -> tuple[str, Path, CompanyIdentity, bool]:
    try:
        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(runtime_dir, target_company, snapshot_id=snapshot_id)
        identity = _company_identity_from_payload(
            identity_payload,
            fallback_company=target_company,
            fallback_company_key=company_key,
        )
        return company_key, snapshot_dir, identity, False
    except CandidateArtifactError:
        if not create_if_missing:
            raise

    identity = resolve_company_identity(target_company)
    company_key = str(identity.company_key or "").strip() or normalize_name_token(target_company)
    normalized_snapshot_id = str(snapshot_id or "").strip() or _utc_snapshot_id()
    company_dir = Path(runtime_dir) / "company_assets" / company_key
    snapshot_dir = company_dir / normalized_snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    identity_payload = identity.to_record()
    (snapshot_dir / "identity.json").write_text(
        json.dumps(identity_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (company_dir / "latest_snapshot.json").write_text(
        json.dumps(
            {
                "snapshot_id": normalized_snapshot_id,
                "snapshot_dir": str(snapshot_dir),
                "company_identity": identity_payload,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return company_key, snapshot_dir, identity, True


def _utc_snapshot_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def _candidate_records_from_payload(payload: dict[str, Any]) -> list[Candidate]:
    candidates: list[Candidate] = []
    for item in list(payload.get("candidates") or []):
        if not isinstance(item, dict):
            continue
        try:
            candidates.append(normalize_candidate(Candidate(**item)))
        except TypeError:
            continue
    return candidates


def _stage_archive_label(value: str) -> str:
    normalized = "".join(char if char.isalnum() else "_" for char in str(value or "").strip().lower())
    normalized = normalized.strip("_")
    return normalized or "supplement"


def _project_supplement_records_to_snapshot(
    candidates: list[Candidate],
    evidence: list[EvidenceRecord],
    *,
    target_company: str,
) -> tuple[list[Candidate], list[EvidenceRecord], dict[str, Any]]:
    target_key = normalize_name_token(target_company)
    projected_candidates: list[Candidate] = []
    candidate_id_map: dict[str, str] = {}
    projected_candidate_ids: set[str] = set()
    retargeted_candidate_ids: list[str] = []

    for candidate in candidates:
        source_candidate_id = str(candidate.candidate_id or "").strip()
        candidate_record = candidate.to_record()
        metadata = dict(candidate_record.get("metadata") or {})
        if normalize_name_token(candidate.target_company) != target_key:
            retargeted_candidate_ids.append(source_candidate_id)
            projected_candidate_id = make_candidate_id(
                str(candidate.name_en or candidate.display_name or source_candidate_id).strip(),
                str(candidate.organization or target_company).strip() or target_company,
                target_company,
            )
            candidate_record["candidate_id"] = projected_candidate_id
            candidate_record["target_company"] = target_company
            metadata.setdefault("supplement_source_candidate_id", source_candidate_id)
            metadata.setdefault("supplement_source_target_company", str(candidate.target_company or "").strip())
            metadata["supplement_attached_company"] = target_company
        else:
            projected_candidate_id = source_candidate_id
        candidate_record["metadata"] = metadata
        projected_candidate = normalize_candidate(Candidate(**candidate_record))
        projected_candidates.append(projected_candidate)
        projected_candidate_ids.add(projected_candidate.candidate_id)
        if source_candidate_id:
            candidate_id_map[source_candidate_id] = projected_candidate.candidate_id

    projected_evidence: list[EvidenceRecord] = []
    for item in evidence:
        original_candidate_id = str(item.candidate_id or "").strip()
        projected_candidate_id = candidate_id_map.get(original_candidate_id, original_candidate_id)
        if projected_candidate_id not in projected_candidate_ids:
            continue
        evidence_record = item.to_record()
        evidence_record["candidate_id"] = projected_candidate_id
        if projected_candidate_id != original_candidate_id:
            evidence_record["evidence_id"] = make_evidence_id(
                projected_candidate_id,
                str(item.source_dataset or item.source_type or "").strip(),
                str(item.title or "").strip(),
                str(item.url or item.source_path or "").strip(),
            )
        projected_evidence.append(EvidenceRecord(**evidence_record))

    return projected_candidates, projected_evidence, {
        "target_company": target_company,
        "retargeted_candidate_count": len(retargeted_candidate_ids),
        "retargeted_candidate_ids": retargeted_candidate_ids[:50],
    }


def _company_identity_from_payload(
    payload: dict[str, Any],
    *,
    fallback_company: str,
    fallback_company_key: str,
) -> CompanyIdentity:
    return CompanyIdentity(
        requested_name=str(payload.get("requested_name") or fallback_company).strip() or fallback_company,
        canonical_name=str(payload.get("canonical_name") or fallback_company).strip() or fallback_company,
        company_key=str(payload.get("company_key") or fallback_company_key).strip() or fallback_company_key,
        linkedin_slug=str(payload.get("linkedin_slug") or "").strip(),
        linkedin_company_url=str(payload.get("linkedin_company_url") or "").strip(),
        domain=str(payload.get("domain") or "").strip(),
        aliases=[str(item).strip() for item in list(payload.get("aliases") or []) if str(item).strip()],
        resolver=str(payload.get("resolver") or "materialized").strip() or "materialized",
        confidence=str(payload.get("confidence") or "high").strip() or "high",
        notes=str(payload.get("notes") or "").strip(),
        local_asset_available=bool(payload.get("local_asset_available", True)),
        metadata=dict(payload.get("metadata") or {}),
    )


def _restore_roster_snapshot_from_snapshot_dir(
    *,
    snapshot_dir: Path,
    identity: CompanyIdentity,
) -> CompanyRosterSnapshot | None:
    harvest_dir = snapshot_dir / "harvest_company_employees"
    summary_path = harvest_dir / "harvest_company_employees_summary.json"
    merged_path = harvest_dir / "harvest_company_employees_merged.json"
    visible_path = harvest_dir / "harvest_company_employees_visible.json"
    headless_path = harvest_dir / "harvest_company_employees_headless.json"
    if not summary_path.exists() or not merged_path.exists() or not visible_path.exists() or not headless_path.exists():
        return None
    try:
        summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
        raw_entries = json.loads(merged_path.read_text(encoding="utf-8"))
        visible_entries = json.loads(visible_path.read_text(encoding="utf-8"))
        headless_entries = json.loads(headless_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(summary_payload, dict):
        return None
    if not isinstance(raw_entries, list) or not isinstance(visible_entries, list) or not isinstance(headless_entries, list):
        return None
    return CompanyRosterSnapshot(
        snapshot_id=snapshot_dir.name,
        target_company=identity.canonical_name,
        company_identity=identity,
        snapshot_dir=snapshot_dir,
        raw_entries=[dict(item) for item in raw_entries if isinstance(item, dict)],
        visible_entries=[dict(item) for item in visible_entries if isinstance(item, dict)],
        headless_entries=[dict(item) for item in headless_entries if isinstance(item, dict)],
        page_summaries=[dict(item) for item in list(summary_payload.get("page_summaries") or []) if isinstance(item, dict)],
        accounts_used=[str(item).strip() for item in list(summary_payload.get("accounts_used") or []) if str(item).strip()],
        errors=[str(item).strip() for item in list(summary_payload.get("errors") or []) if str(item).strip()],
        stop_reason=str(summary_payload.get("stop_reason") or "").strip(),
        merged_path=merged_path,
        visible_path=visible_path,
        headless_path=headless_path,
        summary_path=summary_path,
    )


def _restore_search_seed_snapshot_from_snapshot_dir(
    *,
    snapshot_dir: Path,
    identity: CompanyIdentity,
) -> SearchSeedSnapshot | None:
    discovery_dir = snapshot_dir / "search_seed_discovery"
    summary_path = discovery_dir / "summary.json"
    entries_path = discovery_dir / "entries.json"
    if not summary_path.exists() or not entries_path.exists():
        return None
    try:
        summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
        entries_payload = json.loads(entries_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(summary_payload, dict) or not isinstance(entries_payload, list):
        return None
    return SearchSeedSnapshot(
        snapshot_id=snapshot_dir.name,
        target_company=identity.canonical_name,
        company_identity=identity,
        snapshot_dir=snapshot_dir,
        entries=[dict(item) for item in entries_payload if isinstance(item, dict)],
        query_summaries=[dict(item) for item in list(summary_payload.get("query_summaries") or []) if isinstance(item, dict)],
        accounts_used=[str(item).strip() for item in list(summary_payload.get("accounts_used") or []) if str(item).strip()],
        errors=[str(item).strip() for item in list(summary_payload.get("errors") or []) if str(item).strip()],
        stop_reason=str(summary_payload.get("stop_reason") or "").strip(),
        summary_path=summary_path,
        entries_path=entries_path,
    )


def _normalize_profile_scope(value: str) -> str:
    normalized = str(value or "none").strip().lower()
    if normalized in {"current", "former", "all"}:
        return normalized
    return "none"


def _restore_current_roster_profile_ref(candidate: Candidate, restored_url: str) -> tuple[Candidate, bool]:
    canonical_url = str(restored_url or "").strip()
    if not canonical_url:
        return candidate, False
    metadata = dict(candidate.metadata or {})
    previous_urls: list[str] = []
    for value in [
        candidate.linkedin_url,
        metadata.get("profile_url"),
        metadata.get("more_profiles"),
    ]:
        if isinstance(value, (list, tuple, set)):
            for item in value:
                text = str(item or "").strip()
                if text and text != canonical_url and text not in previous_urls:
                    previous_urls.append(text)
        else:
            text = str(value or "").strip()
            if text and text != canonical_url and text not in previous_urls:
                previous_urls.append(text)
    changed = candidate.linkedin_url != canonical_url or str(metadata.get("profile_url") or "").strip() != canonical_url
    if not changed:
        return candidate, False
    metadata["profile_url"] = canonical_url
    if previous_urls:
        metadata["more_profiles"] = previous_urls
    updated_record = candidate.to_record()
    updated_record["linkedin_url"] = canonical_url
    updated_record["metadata"] = metadata
    return Candidate(**updated_record), True


def _inherit_snapshot_former_filter_hints(
    snapshot_dir: Path,
    base_filter_hints: dict[str, Any] | None = None,
) -> dict[str, list[str]]:
    normalized = {
        str(key): [str(item).strip() for item in list(values or []) if str(item).strip()]
        for key, values in dict(base_filter_hints or {}).items()
    }
    inferred = _load_snapshot_scope_filter_hints(snapshot_dir)
    for key, values in inferred.items():
        if normalized.get(key):
            continue
        normalized[key] = list(values)
    return normalized


def _load_snapshot_scope_filter_hints(snapshot_dir: Path) -> dict[str, list[str]]:
    harvest_dir = snapshot_dir / "harvest_company_employees"
    shard_filters = _load_segmented_company_filters(harvest_dir / "harvest_company_employees_raw.json")
    if not shard_filters:
        shard_filters = _load_segmented_company_filters(harvest_dir / "adaptive_shard_plan.json")
    if not shard_filters:
        return {}

    inherited: dict[str, list[str]] = {}
    for key in ("locations", "exclude_locations"):
        common_values = _intersect_filter_values(shard_filters, key)
        if common_values:
            inherited[key] = common_values
    return inherited


def _load_segmented_company_filters(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return []
    shard_filters: list[dict[str, Any]] = []
    for item in list(payload.get("shards") or []) if isinstance(payload, dict) else []:
        if not isinstance(item, dict):
            continue
        company_filters = dict(item.get("company_filters") or {})
        if company_filters:
            shard_filters.append(company_filters)
    root_filters = dict(payload.get("root_filters") or {}) if isinstance(payload, dict) else {}
    if root_filters:
        shard_filters.insert(0, root_filters)
    return shard_filters


def _intersect_filter_values(filters: list[dict[str, Any]], key: str) -> list[str]:
    common: set[str] | None = None
    ordered_reference: list[str] = []
    for company_filters in filters:
        raw_values = company_filters.get(key)
        if isinstance(raw_values, (list, tuple, set)):
            values = [str(item).strip() for item in raw_values if str(item).strip()]
        else:
            text = str(raw_values or "").strip()
            values = [text] if text else []
        if not values:
            return []
        deduped = list(dict.fromkeys(values))
        if not ordered_reference:
            ordered_reference = deduped
        current = set(deduped)
        common = current if common is None else common & current
        if not common:
            return []
    return [value for value in ordered_reference if value in (common or set())]
