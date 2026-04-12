from __future__ import annotations

from hashlib import sha1
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .canonicalization import canonicalize_company_records
from .connectors import CompanyRosterSnapshot, build_candidates_from_roster
from .acquisition import _build_former_filter_hints
from .asset_logger import AssetLogger
from .candidate_artifacts import _resolve_company_snapshot, build_company_candidate_artifacts
from .company_asset_completion import CompanyAssetCompletionManager
from .connectors import CompanyIdentity
from .domain import Candidate, normalize_name_token
from .harvest_connectors import HarvestProfileSearchConnector
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
