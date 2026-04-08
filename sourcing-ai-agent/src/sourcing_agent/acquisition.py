from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Any

from .asset_catalog import AssetCatalog
from .asset_logger import AssetLogger
from .agent_runtime import AgentRuntimeCoordinator
from .canonicalization import canonicalize_company_records
from .connectors import (
    CompanyIdentity,
    CompanyRosterSnapshot,
    LinkedInCompanyRosterConnector,
    build_candidates_from_roster,
    load_rapidapi_accounts,
    profile_detail_accounts,
    resolve_company_identity,
)
from .domain import AcquisitionTask, Candidate, EvidenceRecord, JobRequest, make_evidence_id, normalize_candidate, normalize_name_token
from .enrichment import MultiSourceEnricher
from .harvest_connectors import (
    HarvestCompanyEmployeesConnector,
    HarvestProfileConnector,
    HarvestProfileSearchConnector,
    discover_legacy_harvest_token,
    write_harvest_execution_artifact,
)
from .model_provider import ModelClient
from .search_provider import build_search_provider
from .seed_discovery import SearchSeedAcquirer, SearchSeedSnapshot, build_candidates_from_seed_snapshot
from .settings import AppSettings
from .storage import SQLiteStore


@dataclass(slots=True)
class AcquisitionExecution:
    task_id: str
    status: str
    detail: str
    payload: dict[str, Any]
    state_updates: dict[str, Any] = field(default_factory=dict)


class AcquisitionEngine:
    def __init__(
        self,
        catalog: AssetCatalog,
        settings: AppSettings,
        store: SQLiteStore,
        model_client: ModelClient,
        worker_runtime: AgentRuntimeCoordinator | None = None,
    ) -> None:
        self.catalog = catalog
        self.settings = settings
        self.store = store
        self.worker_runtime = worker_runtime
        self.company_assets_dir = settings.company_assets_dir
        self.company_assets_dir.mkdir(parents=True, exist_ok=True)
        self.accounts = load_rapidapi_accounts(settings.secrets_file, catalog.legacy_api_accounts)
        self.roster_connector = LinkedInCompanyRosterConnector(self.accounts)
        legacy_harvest_token = ""
        if Path(settings.project_root).resolve() == Path(catalog.project_root).resolve():
            legacy_harvest_token = discover_legacy_harvest_token(catalog.legacy_api_accounts)
        profile_scraper_settings = self._resolve_harvest_settings(settings.harvest.profile_scraper, legacy_harvest_token)
        profile_search_settings = self._resolve_harvest_settings(settings.harvest.profile_search, legacy_harvest_token)
        company_employees_settings = self._resolve_harvest_settings(settings.harvest.company_employees, legacy_harvest_token)
        harvest_profile_search_connector = HarvestProfileSearchConnector(profile_search_settings)
        search_provider = build_search_provider(settings.search)
        self.harvest_company_connector = HarvestCompanyEmployeesConnector(company_employees_settings)
        self.search_seed_acquirer = SearchSeedAcquirer(
            self.accounts,
            model_client,
            harvest_search_connector=harvest_profile_search_connector,
            search_provider=search_provider,
        )
        self.multi_source_enricher = MultiSourceEnricher(
            catalog,
            self.accounts,
            HarvestProfileConnector(profile_scraper_settings),
            harvest_profile_search_connector,
            model_client,
            search_provider=search_provider,
            worker_runtime=worker_runtime,
        )

    def _resolve_harvest_settings(self, actor_settings: Any, legacy_harvest_token: str) -> Any:
        if getattr(actor_settings, "enabled", False):
            return actor_settings
        if not legacy_harvest_token:
            return actor_settings
        return replace(actor_settings, enabled=True, api_token=legacy_harvest_token)

    def execute_task(
        self,
        task: AcquisitionTask,
        job_request: JobRequest,
        target_company: str,
        state: dict[str, Any],
        bootstrap_summary: dict[str, Any] | None = None,
    ) -> AcquisitionExecution:
        if task.status == "needs_input":
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Target company is missing, so acquisition cannot proceed.",
                payload={},
            )

        company_key = target_company.strip().lower()
        if task.task_type == "resolve_company_identity":
            return self._resolve_company(target_company, task, state)
        strategy_type = str(task.metadata.get("strategy_type") or "")
        if task.task_type == "acquire_full_roster":
            strategy_type = strategy_type or "full_company_roster"
            if strategy_type == "full_company_roster":
                if company_key == "anthropic":
                    return self._anthropic_local_asset_task(task, bootstrap_summary or {}, state)
                return self._acquire_full_roster(task, state, job_request)
            if strategy_type in {"scoped_search_roster", "former_employee_search"}:
                return self._acquire_search_seed_pool(task, state, job_request)
            if strategy_type == "investor_firm_roster":
                return self._acquire_investor_firm_roster(task, state, job_request)
            return self._acquire_full_roster(task, state, job_request)
        if company_key == "anthropic" and strategy_type != "investor_firm_roster":
            return self._anthropic_local_asset_task(task, bootstrap_summary or {}, state)
        if task.task_type == "enrich_profiles_multisource":
            return self._enrich_profiles(task, state, job_request)
        if task.task_type == "normalize_asset_snapshot":
            return self._normalize_snapshot(task, state)
        if task.task_type == "build_retrieval_index":
            return self._build_retrieval_index(task, state)
        return AcquisitionExecution(
            task_id=task.task_id,
            status="blocked",
            detail="No connector is implemented yet for this acquisition step.",
            payload={"task_type": task.task_type, "target_company": target_company},
        )

    def _resolve_company(
        self,
        target_company: str,
        task: AcquisitionTask,
        state: dict[str, Any],
    ) -> AcquisitionExecution:
        identity = resolve_company_identity(target_company, self.catalog.legacy_company_ids)
        if not identity.linkedin_slug and not identity.local_asset_available:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=f"Could not resolve a usable company identity for {target_company}.",
                payload={"target_company": target_company},
            )

        snapshot_id, snapshot_dir = self._ensure_snapshot_dir(identity, state)
        identity_path = snapshot_dir / "identity.json"
        AssetLogger(snapshot_dir).write_json(
            identity_path,
            identity.to_record(),
            asset_type="company_identity",
            source_kind="resolve_company_identity",
            is_raw_asset=False,
            model_safe=True,
        )

        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=f"Resolved {identity.canonical_name} to LinkedIn slug '{identity.linkedin_slug or 'local-only'}'.",
            payload={
                "company_identity": identity.to_record(),
                "snapshot_id": snapshot_id,
                "snapshot_dir": str(snapshot_dir),
                "identity_path": str(identity_path),
            },
            state_updates={
                "company_identity": identity,
                "snapshot_id": snapshot_id,
                "snapshot_dir": snapshot_dir,
            },
        )

    def _anthropic_local_asset_task(
        self,
        task: AcquisitionTask,
        bootstrap_summary: dict[str, Any],
        state: dict[str, Any],
    ) -> AcquisitionExecution:
        identity = state.get("company_identity")
        if not isinstance(identity, CompanyIdentity):
            identity = resolve_company_identity("Anthropic", self.catalog.legacy_company_ids)
        detail = _anthropic_detail(task.task_type, bootstrap_summary)
        snapshot_dir = state.get("snapshot_dir")
        if isinstance(snapshot_dir, Path):
            logger = AssetLogger(snapshot_dir)
            local_summary_path = snapshot_dir / "local_asset_summary.json"
            logger.write_json(
                local_summary_path,
                {
                    "company_identity": identity.to_record(),
                    "bootstrap_summary": bootstrap_summary,
                    "detail": detail,
                    "task_type": task.task_type,
                },
                asset_type="local_asset_summary",
                source_kind="anthropic_local_asset",
                is_raw_asset=False,
                model_safe=True,
            )
        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=detail,
            payload={
                "company_identity": identity.to_record(),
                "bootstrap_summary": bootstrap_summary,
            },
            state_updates={"company_identity": identity},
        )

    def _acquire_full_roster(
        self,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
    ) -> AcquisitionExecution:
        identity = state.get("company_identity")
        snapshot_dir = state.get("snapshot_dir")
        if not isinstance(identity, CompanyIdentity) or not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Company identity must be resolved before roster acquisition.",
                payload={},
            )
        cost_policy = dict(task.metadata.get("cost_policy") or {})
        max_pages = int(task.metadata.get("max_pages", 10) or 10)
        page_limit = int(task.metadata.get("page_limit", 50) or 50)
        job_id = str(state.get("job_id") or "")
        try:
            if self.harvest_company_connector.settings.enabled and bool(cost_policy.get("allow_company_employee_api", True)):
                if self.worker_runtime is not None and job_id:
                    harvest_worker = self._execute_harvest_company_roster_worker(
                        identity=identity,
                        snapshot_dir=snapshot_dir,
                        max_pages=max_pages,
                        page_limit=page_limit,
                        job_id=job_id,
                        request_payload=job_request.to_record(),
                        plan_payload=dict(state.get("plan_payload") or {}),
                        runtime_mode=str(state.get("runtime_mode") or "workflow"),
                    )
                    if str(harvest_worker.get("worker_status") or "") == "queued":
                        summary = dict(harvest_worker.get("summary") or {})
                        return AcquisitionExecution(
                            task_id=task.task_id,
                            status="blocked",
                            detail=(
                                "Roster acquisition queued a background Harvest company-employees run; "
                                "resume after worker recovery completes remote dataset fetch."
                            ),
                            payload={
                                "queued_harvest_worker_count": 1,
                                "stop_reason": "queued_background_harvest",
                                "harvest_worker": summary,
                            },
                        )
                snapshot = self.harvest_company_connector.fetch_company_roster(
                    identity,
                    snapshot_dir,
                    asset_logger=AssetLogger(snapshot_dir),
                    max_pages=max_pages,
                    page_limit=page_limit,
                )
            else:
                snapshot = self.roster_connector.fetch_company_roster(
                    identity,
                    snapshot_dir,
                    asset_logger=AssetLogger(snapshot_dir),
                    max_pages=max_pages,
                    page_limit=page_limit,
                )
        except Exception as exc:
            cached_snapshot = self._load_cached_roster_snapshot(identity, exclude_snapshot_dir=snapshot_dir)
            if cached_snapshot is not None:
                self._write_latest_snapshot_pointer(identity, cached_snapshot.snapshot_id, cached_snapshot.snapshot_dir)
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail=(
                        f"Live roster acquisition failed ({exc}); reused cached snapshot "
                        f"{cached_snapshot.snapshot_id} with {len(cached_snapshot.visible_entries)} visible members."
                    ),
                    payload={
                        **cached_snapshot.to_record(),
                        "acquisition_mode": "cached_snapshot_fallback",
                        "fallback_reason": str(exc),
                    },
                    state_updates={
                        "roster_snapshot": cached_snapshot,
                        "snapshot_id": cached_snapshot.snapshot_id,
                        "snapshot_dir": cached_snapshot.snapshot_dir,
                    },
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=f"Roster acquisition failed: {exc}",
                payload={"target_company": identity.canonical_name},
            )

        if not snapshot.visible_entries:
            cached_snapshot = self._load_cached_roster_snapshot(identity, exclude_snapshot_dir=snapshot_dir)
            if cached_snapshot is not None:
                self._write_latest_snapshot_pointer(identity, cached_snapshot.snapshot_id, cached_snapshot.snapshot_dir)
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail=(
                        f"Live roster acquisition returned no visible members; reused cached snapshot "
                        f"{cached_snapshot.snapshot_id} with {len(cached_snapshot.visible_entries)} visible members."
                    ),
                    payload={
                        **cached_snapshot.to_record(),
                        "acquisition_mode": "cached_snapshot_fallback",
                        "fallback_reason": "no_visible_members",
                    },
                    state_updates={
                        "roster_snapshot": cached_snapshot,
                        "snapshot_id": cached_snapshot.snapshot_id,
                        "snapshot_dir": cached_snapshot.snapshot_dir,
                    },
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Roster acquisition finished but no visible company members were recovered.",
                payload=snapshot.to_record(),
                state_updates={"roster_snapshot": snapshot},
            )

        self._write_latest_snapshot_pointer(identity, snapshot.snapshot_id, snapshot.snapshot_dir)

        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=(
                f"Captured {len(snapshot.raw_entries)} roster rows "
                f"({len(snapshot.visible_entries)} visible, {len(snapshot.headless_entries)} headless)."
            ),
            payload=snapshot.to_record(),
            state_updates={"roster_snapshot": snapshot},
        )

    def _acquire_search_seed_pool(
        self,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
    ) -> AcquisitionExecution:
        identity = state.get("company_identity")
        snapshot_dir = state.get("snapshot_dir")
        if not isinstance(identity, CompanyIdentity) or not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Company identity must be resolved before search-seed acquisition.",
                payload={},
            )

        filter_hints = dict(task.metadata.get("filter_hints") or {})
        search_seed_queries = list(task.metadata.get("search_seed_queries") or [])
        query_bundles = list(task.metadata.get("search_query_bundles") or [])
        cost_policy = dict(task.metadata.get("cost_policy") or {})
        employment_statuses = list(task.metadata.get("employment_statuses") or [])
        employment_status = employment_statuses[0] if employment_statuses else "current"
        if not search_seed_queries and not query_bundles:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Search-seed acquisition requires compiled seed queries.",
                payload={"strategy_type": task.metadata.get("strategy_type", "")},
            )

        snapshot = self.search_seed_acquirer.discover(
            identity,
            snapshot_dir,
            asset_logger=AssetLogger(snapshot_dir),
            search_seed_queries=search_seed_queries,
            query_bundles=query_bundles,
            filter_hints=filter_hints,
            cost_policy=cost_policy,
            employment_status=employment_status,
            worker_runtime=self.worker_runtime,
            job_id=str(state.get("job_id") or ""),
            request_payload=job_request.to_record(),
            plan_payload=dict(state.get("plan_payload") or {}),
            runtime_mode=str(state.get("runtime_mode") or "workflow"),
        )
        queued_query_count = len(
            [item for item in list(snapshot.query_summaries or []) if str(item.get("status") or "") == "queued"]
        )
        if snapshot.stop_reason == "queued_background_search" or queued_query_count > 0:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=(
                    f"Search-seed acquisition queued {queued_query_count} background web searches; "
                    "resume the worker daemon to finish remote task_get before fallback or downstream enrichment."
                ),
                payload={
                    **snapshot.to_record(),
                    "strategy_type": task.metadata.get("strategy_type", ""),
                    "cost_policy": cost_policy,
                    "queued_query_count": queued_query_count,
                },
                state_updates={"search_seed_snapshot": snapshot},
            )
        if not snapshot.entries:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Search-seed acquisition finished but did not recover any candidate leads.",
                payload=snapshot.to_record(),
                state_updates={"search_seed_snapshot": snapshot},
            )

        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=(
                f"Recovered {len(snapshot.entries)} search-seed candidates using "
                f"{'/'.join(task.metadata.get('search_channel_order') or ['web_search'])}."
            ),
            payload={
                **snapshot.to_record(),
                "strategy_type": task.metadata.get("strategy_type", ""),
                "cost_policy": cost_policy,
            },
            state_updates={"search_seed_snapshot": snapshot},
        )

    def _enrich_profiles(self, task: AcquisitionTask, state: dict[str, Any], job_request: JobRequest) -> AcquisitionExecution:
        snapshot = state.get("roster_snapshot")
        search_seed_snapshot = state.get("search_seed_snapshot")
        snapshot_dir = state.get("snapshot_dir")
        strategy_type = str(task.metadata.get("strategy_type") or "")
        if not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Snapshot directory must exist before profile enrichment.",
                payload={},
            )

        if isinstance(snapshot, CompanyRosterSnapshot):
            candidates, evidence = build_candidates_from_roster(snapshot)
        elif isinstance(search_seed_snapshot, SearchSeedSnapshot):
            candidates, evidence = build_candidates_from_seed_snapshot(search_seed_snapshot)
        elif strategy_type == "investor_firm_roster" and state.get("candidates"):
            candidates = list(state.get("candidates") or [])
            evidence = list(state.get("evidence") or [])
        else:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="An acquired roster or search-seed snapshot must exist before profile enrichment.",
                payload={},
            )
        if not candidates:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Acquisition returned no candidates that could be normalized into candidate documents.",
                payload=(
                    snapshot.to_record()
                    if isinstance(snapshot, CompanyRosterSnapshot)
                    else search_seed_snapshot.to_record()
                    if isinstance(search_seed_snapshot, SearchSeedSnapshot)
                    else {"strategy_type": strategy_type}
                ),
            )

        if strategy_type == "investor_firm_roster":
            logger = AssetLogger(snapshot_dir)
            candidate_doc_path = snapshot_dir / "candidate_documents.json"
            logger.write_json(
                candidate_doc_path,
                {
                    "target_company": job_request.target_company,
                    "strategy_type": strategy_type,
                    "candidate_count": len(candidates),
                    "evidence_count": len(evidence),
                    "investor_firm_plan": state.get("investor_firm_plan") or {},
                    "candidates": [candidate.to_record() for candidate in candidates],
                    "evidence": [item.to_record() for item in evidence],
                    "enrichment_mode": "investor_firm_roster_existing_assets",
                },
                asset_type="candidate_documents",
                source_kind="investor_firm_roster",
                is_raw_asset=False,
                model_safe=True,
            )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="completed",
                detail=(
                    f"Prepared {len(candidates)} investor-firm candidate documents from tiered firm roster assets."
                ),
                payload={
                    "candidate_count": len(candidates),
                    "evidence_count": len(evidence),
                    "candidate_doc_path": str(candidate_doc_path),
                    "enrichment_mode": "investor_firm_roster_existing_assets",
                },
                state_updates={
                    "candidates": candidates,
                    "evidence": evidence,
                    "candidate_doc_path": candidate_doc_path,
                },
            )

        enrichment_request = JobRequest(
            raw_user_request=job_request.raw_user_request,
            query=job_request.query,
            target_company=job_request.target_company,
            slug_resolution_limit=int(task.metadata.get("slug_resolution_limit", job_request.slug_resolution_limit) or 0),
            profile_detail_limit=int(task.metadata.get("profile_detail_limit", job_request.profile_detail_limit) or 0),
            publication_scan_limit=int(task.metadata.get("publication_scan_limit", job_request.publication_scan_limit) or 0),
            publication_lead_limit=int(task.metadata.get("publication_lead_limit", job_request.publication_lead_limit) or 0),
            exploration_limit=int(task.metadata.get("exploration_limit", job_request.exploration_limit) or 0),
            scholar_coauthor_follow_up_limit=int(
                task.metadata.get("scholar_coauthor_follow_up_limit", job_request.scholar_coauthor_follow_up_limit) or 0
            ),
        )
        enrichment_identity = snapshot.company_identity if isinstance(snapshot, CompanyRosterSnapshot) else search_seed_snapshot.company_identity
        logger = AssetLogger(snapshot_dir)
        enrichment = self.multi_source_enricher.enrich(
            enrichment_identity,
            snapshot_dir,
            candidates,
            enrichment_request,
            asset_logger=logger,
            job_id=str(state.get("job_id") or ""),
            request_payload=job_request.to_record(),
            plan_payload=dict(state.get("plan_payload") or {}),
            runtime_mode=str(state.get("runtime_mode") or "workflow"),
            parallel_exploration_workers=int(
                dict(task.metadata.get("cost_policy") or {}).get("parallel_exploration_workers", 2) or 2
            ),
            cost_policy=dict(task.metadata.get("cost_policy") or {}),
        )
        if int(getattr(enrichment, "queued_harvest_worker_count", 0) or 0) > 0:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=(
                    "Profile enrichment queued background Harvest profile prefetch work; "
                    "resume after worker recovery completes remote dataset fetch."
                ),
                payload={
                    "queued_harvest_worker_count": int(getattr(enrichment, "queued_harvest_worker_count", 0) or 0),
                    "artifact_paths": dict(enrichment.artifact_paths),
                    "stop_reason": str(getattr(enrichment, "stop_reason", "") or "queued_background_harvest"),
                    "enrichment_mode": "roster_plus_multisource",
                },
            )
        if int(getattr(enrichment, "queued_exploration_count", 0) or 0) > 0:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=(
                    "Profile enrichment queued background exploration work; "
                    "resume after worker recovery completes pending exploration queries."
                ),
                payload={
                    "queued_exploration_count": int(getattr(enrichment, "queued_exploration_count", 0) or 0),
                    "artifact_paths": dict(enrichment.artifact_paths),
                    "stop_reason": str(getattr(enrichment, "stop_reason", "") or "queued_background_exploration"),
                    "enrichment_mode": "roster_plus_multisource",
                },
            )
        candidates = enrichment.candidates
        evidence.extend(enrichment.evidence)

        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        logger.write_json(
            candidate_doc_path,
            {
                "snapshot": (
                    snapshot.to_record()
                    if isinstance(snapshot, CompanyRosterSnapshot)
                    else search_seed_snapshot.to_record()
                ),
                "candidates": [candidate.to_record() for candidate in candidates],
                "evidence": [item.to_record() for item in evidence],
                "enrichment_mode": "roster_plus_multisource",
                "enrichment_summary": enrichment.to_record(),
                "next_connectors": {
                    "profile_detail_accounts": [account.account_id for account in profile_detail_accounts(self.accounts)],
                    "note": (
                        "Roster baseline is normalized. Remaining adapters should focus on broader company search, "
                        "higher-recall slug discovery, and cloud persistence for high-value LinkedIn profile assets."
                    ),
                },
            },
            asset_type="candidate_documents",
            source_kind="multisource_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )

        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=(
                f"Built {len(candidates)} candidate documents from roster + multisource enrichment. "
                f"Resolved {len(enrichment.resolved_profiles)} profile details and matched {len(enrichment.publication_matches)} publications."
            ),
            payload={
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "candidate_doc_path": str(candidate_doc_path),
                "enrichment_mode": "roster_plus_multisource",
                "resolved_profile_count": len(enrichment.resolved_profiles),
                "publication_match_count": len(enrichment.publication_matches),
                "lead_candidate_count": len(enrichment.lead_candidates),
                "artifact_paths": enrichment.artifact_paths,
            },
            state_updates={
                "candidates": candidates,
                "evidence": evidence,
                "candidate_doc_path": candidate_doc_path,
            },
        )

    def _execute_harvest_company_roster_worker(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        max_pages: int,
        page_limit: int,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
    ) -> dict[str, Any]:
        if self.worker_runtime is None or not job_id:
            return {"worker_status": "skipped"}

        worker_handle = self.worker_runtime.begin_worker(
            job_id=job_id,
            request=JobRequest.from_payload(request_payload),
            plan_payload=plan_payload,
            runtime_mode=runtime_mode,
            lane_id="acquisition_specialist",
            worker_key=f"harvest_company_employees::{identity.company_key}",
            stage="acquiring",
            span_name=f"harvest_company_employees:{identity.canonical_name}",
            budget_payload={"max_pages": max_pages, "page_limit": page_limit},
            input_payload={"company_identity": identity.to_record()},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "identity": identity.to_record(),
                "snapshot_dir": str(snapshot_dir),
                "max_pages": max_pages,
                "page_limit": page_limit,
                "request_payload": request_payload,
                "plan_payload": plan_payload,
                "runtime_mode": runtime_mode,
            },
            handoff_from_lane="triage_planner",
        )
        existing = self.worker_runtime.get_worker(worker_handle.worker_id) or {}
        checkpoint = dict(existing.get("checkpoint") or {})
        output_payload = dict(existing.get("output") or {})
        if str(existing.get("status") or "") == "completed" and output_payload:
            self.worker_runtime.complete_worker(
                worker_handle,
                status="completed",
                checkpoint_payload=checkpoint,
                output_payload=output_payload,
                handoff_to_lane="acquisition_specialist",
            )
            return {
                "worker_status": "completed",
                "summary": dict(output_payload.get("summary") or {}),
                "daemon_action": "reused_output",
            }

        logger = AssetLogger(snapshot_dir)
        harvest_dir = snapshot_dir / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        artifact_default_path = harvest_dir / "harvest_company_employees_queue.json"
        execution = self.harvest_company_connector.execute_with_checkpoint(
            identity,
            snapshot_dir,
            max_pages=max_pages,
            page_limit=page_limit,
            checkpoint=checkpoint,
        )
        artifact_paths = {
            str(key): str(value)
            for key, value in dict(checkpoint.get("artifact_paths") or {}).items()
            if str(key).strip()
        }
        for artifact in list(execution.artifacts or []):
            artifact_path = write_harvest_execution_artifact(
                logger=logger,
                artifact=artifact,
                default_path=artifact_default_path,
                asset_type="harvest_company_employees_queue_payload",
                source_kind="harvest_company_employees",
                metadata={"company_key": identity.company_key, "logical_name": execution.logical_name},
            )
            artifact_paths[str(artifact.label)] = str(artifact_path)
        summary = {
            "logical_name": execution.logical_name,
            "company_identity": identity.to_record(),
            "status": "queued" if execution.pending else "completed",
            "message": str(execution.message or ""),
            "run_id": str(execution.checkpoint.get("run_id") or ""),
            "dataset_id": str(execution.checkpoint.get("dataset_id") or ""),
            "artifact_paths": artifact_paths,
            "requested_pages": max_pages,
            "requested_item_limit": page_limit,
        }
        summary_path = harvest_dir / "harvest_company_employees_queue_summary.json"
        logger.write_json(
            summary_path,
            summary,
            asset_type="harvest_company_employees_queue_summary",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        updated_checkpoint = {
            **dict(execution.checkpoint or {}),
            "artifact_paths": artifact_paths,
            "summary_path": str(summary_path),
            "stage": "waiting_remote_harvest" if execution.pending else "completed",
        }
        updated_output = {
            "summary": {**summary, "summary_path": str(summary_path)},
        }
        if execution.pending:
            self.worker_runtime.complete_worker(
                worker_handle,
                status="queued",
                checkpoint_payload=updated_checkpoint,
                output_payload=updated_output,
            )
            return {"worker_status": "queued", "summary": updated_output["summary"]}

        self.worker_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload=updated_checkpoint,
            output_payload=updated_output,
            handoff_to_lane="acquisition_specialist",
        )
        return {"worker_status": "completed", "summary": updated_output["summary"]}

    def _acquire_investor_firm_roster(
        self,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
    ) -> AcquisitionExecution:
        identity = state.get("company_identity")
        snapshot_dir = state.get("snapshot_dir")
        if not isinstance(identity, CompanyIdentity) or not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Company identity must be resolved before investor-firm roster acquisition.",
                payload={},
            )

        all_candidates = self.store.list_candidates()
        candidates = [
            candidate
            for candidate in all_candidates
            if candidate.target_company.strip().lower() == identity.canonical_name.strip().lower()
            and candidate.category == "investor"
        ]
        if not candidates:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=(
                    "Investor-firm roster acquisition needs either an investment-graph connector or existing investor "
                    "assets for this company, but none were found."
                ),
                payload={"strategy_type": "investor_firm_roster", "target_company": identity.canonical_name},
            )

        evidence = _load_evidence_records(self.store, candidates)
        firm_plan = _build_investor_firm_plan(identity.canonical_name, candidates, job_request)
        logger = AssetLogger(snapshot_dir)
        firm_plan_path = snapshot_dir / "investor_firm_plan.json"
        logger.write_json(
            firm_plan_path,
            firm_plan,
            asset_type="investor_firm_plan",
            source_kind="investor_firm_roster",
            is_raw_asset=False,
            model_safe=True,
        )
        roster_path = snapshot_dir / "investor_firm_candidates.json"
        logger.write_json(
            roster_path,
            {
                "target_company": identity.canonical_name,
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "firms": firm_plan["firms"],
                "candidates": [candidate.to_record() for candidate in candidates],
            },
            asset_type="investor_firm_candidates",
            source_kind="investor_firm_roster",
            is_raw_asset=False,
            model_safe=True,
        )
        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=(
                f"Recovered {len(candidates)} investor-firm members across {len(firm_plan['firms'])} firms "
                f"for {identity.canonical_name} using existing investor assets."
            ),
            payload={
                "strategy_type": "investor_firm_roster",
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "firm_count": len(firm_plan["firms"]),
                "firm_plan_path": str(firm_plan_path),
                "roster_path": str(roster_path),
            },
            state_updates={
                "candidates": candidates,
                "evidence": evidence,
                "investor_firm_plan": firm_plan,
                "normalization_scope": {"mode": "category", "category": "investor"},
            },
        )

    def _normalize_snapshot(self, task: AcquisitionTask, state: dict[str, Any]) -> AcquisitionExecution:
        identity = state.get("company_identity")
        snapshot_dir = state.get("snapshot_dir")
        candidates = state.get("candidates") or []
        evidence = state.get("evidence") or []

        if not isinstance(identity, CompanyIdentity) or not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Normalization requires a resolved company identity and snapshot directory.",
                payload={},
            )

        normalization_scope = dict(state.get("normalization_scope") or {})
        inheritance_summary: dict[str, Any] = {}
        canonicalization_summary: dict[str, Any] = {}
        if str(normalization_scope.get("mode") or "") != "category":
            candidates, evidence, inheritance_summary = _inherit_historical_profile_captures(
                self.store,
                identity,
                snapshot_dir,
                candidates,
                evidence,
            )
            candidates, evidence, canonicalization_summary = canonicalize_company_records(candidates, evidence)

        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        existing_candidate_doc_payload: dict[str, Any] = {}
        if candidate_doc_path.exists():
            try:
                loaded_payload = json.loads(candidate_doc_path.read_text())
                if isinstance(loaded_payload, dict):
                    existing_candidate_doc_payload = loaded_payload
            except (OSError, json.JSONDecodeError):
                existing_candidate_doc_payload = {}

        logger = AssetLogger(snapshot_dir)
        logger.write_json(
            candidate_doc_path,
            {
                **existing_candidate_doc_payload,
                "candidates": [candidate.to_record() for candidate in candidates],
                "evidence": [item.to_record() for item in evidence],
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "historical_profile_inheritance": inheritance_summary,
                "canonicalization": canonicalization_summary,
            },
            asset_type="candidate_documents",
            source_kind="normalize_asset_snapshot",
            is_raw_asset=False,
            model_safe=True,
        )

        if str(normalization_scope.get("mode") or "") == "category" and str(normalization_scope.get("category") or "").strip():
            self.store.replace_company_category_data(
                identity.canonical_name,
                str(normalization_scope.get("category") or ""),
                candidates,
                evidence,
            )
        else:
            self.store.replace_company_data(identity.canonical_name, candidates, evidence)
        manifest_path = snapshot_dir / "manifest.json"
        logger.write_json(
            manifest_path,
            {
                "snapshot_id": state.get("snapshot_id"),
                "company_identity": identity.to_record(),
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "normalization_scope": normalization_scope or {"mode": "company"},
                "historical_profile_inheritance": inheritance_summary,
                "canonicalization": canonicalization_summary,
                "storage": {
                    "execution": "local_runtime",
                    "candidate_store": str(self.store.db_path),
                    "future_cloud_direction": (
                        "Persist high-value LinkedIn profile assets and final result artifacts to cloud object "
                        "storage or a managed document store while keeping transient workflow logs local."
                    ),
                },
            },
            asset_type="snapshot_manifest",
            source_kind="normalize_asset_snapshot",
            is_raw_asset=False,
            model_safe=True,
        )

        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=f"Persisted {len(candidates)} candidates for {identity.canonical_name} into SQLite.",
            payload={
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "candidate_doc_path": str(candidate_doc_path),
                "manifest_path": str(manifest_path),
                "historical_profile_inheritance": inheritance_summary,
                "canonicalization": canonicalization_summary,
            },
            state_updates={
                "candidates": candidates,
                "evidence": evidence,
                "candidate_doc_path": candidate_doc_path,
                "manifest_path": manifest_path,
            },
        )

    def _build_retrieval_index(self, task: AcquisitionTask, state: dict[str, Any]) -> AcquisitionExecution:
        identity = state.get("company_identity")
        snapshot_dir = state.get("snapshot_dir")
        if not isinstance(identity, CompanyIdentity) or not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Retrieval index build requires a resolved company identity and snapshot directory.",
                payload={},
            )

        candidate_count = self.store.candidate_count_for_company(identity.canonical_name)
        logger = AssetLogger(snapshot_dir)
        retrieval_summary_path = snapshot_dir / "retrieval_index_summary.json"
        logger.write_json(
            retrieval_summary_path,
            {
                "company_identity": identity.to_record(),
                "candidate_count": candidate_count,
                "index_type": "sqlite_structured_plus_semantic_ready_documents",
            },
            asset_type="retrieval_index_summary",
            source_kind="build_retrieval_index",
            is_raw_asset=False,
            model_safe=True,
        )
        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=f"Prepared retrieval-ready candidate documents for {candidate_count} members.",
            payload={
                "candidate_count": candidate_count,
                "retrieval_index_summary_path": str(retrieval_summary_path),
            },
        )

    def _ensure_snapshot_dir(self, identity: CompanyIdentity, state: dict[str, Any]) -> tuple[str, Path]:
        snapshot_id = str(state.get("snapshot_id") or "").strip()
        if not snapshot_id:
            snapshot_id = datetime.now().strftime("%Y%m%dT%H%M%S")
        snapshot_dir = self.company_assets_dir / identity.company_key / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        return snapshot_id, snapshot_dir

    def _write_latest_snapshot_pointer(self, identity: CompanyIdentity, snapshot_id: str, snapshot_dir: Path) -> None:
        latest_pointer = self.company_assets_dir / identity.company_key / "latest_snapshot.json"
        AssetLogger(self.company_assets_dir / identity.company_key).write_json(
            latest_pointer,
            {
                "company_identity": identity.to_record(),
                "snapshot_id": snapshot_id,
                "snapshot_dir": str(snapshot_dir),
            },
            asset_type="latest_snapshot_pointer",
            source_kind="snapshot_registry",
            is_raw_asset=False,
            model_safe=True,
        )

    def _load_cached_roster_snapshot(
        self,
        identity: CompanyIdentity,
        *,
        exclude_snapshot_dir: Path | None = None,
    ) -> CompanyRosterSnapshot | None:
        company_dir = self.company_assets_dir / identity.company_key
        if not company_dir.exists():
            return None

        summary_paths = sorted(company_dir.glob("*/linkedin_company_people_summary.json"), key=lambda item: item.stat().st_mtime, reverse=True)
        for summary_path in summary_paths:
            snapshot_dir = summary_path.parent
            if exclude_snapshot_dir is not None and snapshot_dir == exclude_snapshot_dir:
                continue
            try:
                summary = json.loads(summary_path.read_text())
                visible_path = snapshot_dir / "linkedin_company_people_visible.json"
                merged_path = snapshot_dir / "linkedin_company_people_all.json"
                headless_path = snapshot_dir / "linkedin_company_people_headless.json"
                visible_entries = json.loads(visible_path.read_text())
                if not visible_entries:
                    continue
                return CompanyRosterSnapshot(
                    snapshot_id=str(summary.get("snapshot_id") or snapshot_dir.name),
                    target_company=str(summary.get("target_company") or identity.canonical_name),
                    company_identity=identity,
                    snapshot_dir=snapshot_dir,
                    raw_entries=json.loads(merged_path.read_text()) if merged_path.exists() else visible_entries,
                    visible_entries=visible_entries,
                    headless_entries=json.loads(headless_path.read_text()) if headless_path.exists() else [],
                    page_summaries=list(summary.get("page_summaries") or []),
                    accounts_used=list(summary.get("accounts_used") or []),
                    errors=list(summary.get("errors") or []),
                    stop_reason=str(summary.get("stop_reason") or "cached_snapshot"),
                    merged_path=merged_path,
                    visible_path=visible_path,
                    headless_path=headless_path,
                    summary_path=summary_path,
                )
            except (OSError, json.JSONDecodeError):
                continue
        return None


def _anthropic_detail(task_type: str, bootstrap_summary: dict[str, Any]) -> str:
    mapping = {
        "resolve_company_identity": "Anthropic is already mapped to the local asset package.",
        "acquire_full_roster": "Used the local Anthropic asset snapshot as the full roster baseline.",
        "enrich_profiles_multisource": "Local asset already contains workbook rows, investor members, and scholar evidence.",
        "normalize_asset_snapshot": "SQLite candidate/evidence store was refreshed.",
        "build_retrieval_index": "Candidate documents are available for structured and semantic-ready retrieval.",
    }
    base = mapping.get(task_type, "Completed using the local Anthropic asset snapshot.")
    if bootstrap_summary:
        base += f" Candidate count: {bootstrap_summary.get('candidate_count', 0)}."
    return base


def _inherit_historical_profile_captures(
    store: SQLiteStore,
    identity: CompanyIdentity,
    snapshot_dir: Path,
    candidates: list[Candidate],
    evidence: list[EvidenceRecord],
) -> tuple[list[Candidate], list[EvidenceRecord], dict[str, Any]]:
    company_dir = snapshot_dir.parent
    historical_sources: list[dict[str, Any]] = []

    sqlite_candidates = store.list_candidates_for_company(identity.canonical_name)
    sqlite_evidence = [
        _evidence_record_from_payload(item)
        for item in store.list_evidence_for_company(identity.canonical_name)
    ]
    sqlite_evidence = [item for item in sqlite_evidence if item is not None]
    if sqlite_candidates or sqlite_evidence:
        historical_sources.append(
            {
                "source_type": "sqlite",
                "snapshot_id": "sqlite",
                "candidate_count": len(sqlite_candidates),
                "evidence_count": len(sqlite_evidence),
                "candidates": list(sqlite_candidates),
                "evidence": list(sqlite_evidence),
            }
        )

    if company_dir.exists():
        for candidate_snapshot_dir in sorted(path for path in company_dir.iterdir() if path.is_dir() and path != snapshot_dir):
            payload_path = candidate_snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
            source_type = "materialized_candidate_documents"
            if not payload_path.exists():
                payload_path = candidate_snapshot_dir / "candidate_documents.json"
                source_type = "candidate_documents"
            if not payload_path.exists():
                continue
            snapshot_candidates, snapshot_evidence = _load_snapshot_candidate_payload(payload_path, identity.canonical_name)
            if not snapshot_candidates and not snapshot_evidence:
                continue
            historical_sources.append(
                {
                    "source_type": source_type,
                    "snapshot_id": candidate_snapshot_dir.name,
                    "candidate_count": len(snapshot_candidates),
                    "evidence_count": len(snapshot_evidence),
                    "candidates": snapshot_candidates,
                    "evidence": snapshot_evidence,
                }
            )

    if not historical_sources:
        return list(candidates), list(evidence), _profile_inheritance_summary()

    match_index: dict[str, list[int]] = {}
    historical_candidates: list[Candidate] = []
    historical_evidence_by_candidate: dict[str, list[EvidenceRecord]] = {}
    for source in historical_sources:
        source_candidates = list(source.get("candidates") or [])
        source_evidence = list(source.get("evidence") or [])
        for candidate in source_candidates:
            history_index = len(historical_candidates)
            historical_candidates.append(candidate)
            for key in _candidate_match_keys(candidate):
                match_index.setdefault(key, []).append(history_index)
        for item in source_evidence:
            historical_evidence_by_candidate.setdefault(item.candidate_id, []).append(item)

    merged_candidates: list[Candidate] = []
    merged_evidence_by_key: dict[tuple[str, str, str, str], EvidenceRecord] = {
        _evidence_dedupe_key(item): item for item in evidence
    }
    matched_candidate_count = 0
    inherited_candidate_count = 0
    inherited_evidence_count = 0
    inherited_review_count = 0
    matched_historical_candidate_ids: set[str] = set()
    carried_forward_candidate_count = 0

    for candidate in candidates:
        matched_history = _match_historical_candidates(candidate, historical_candidates, match_index)
        if not matched_history:
            merged_candidates.append(candidate)
            continue

        matched_candidate_count += 1
        matched_historical_candidate_ids.update(item.candidate_id for item in matched_history if item.candidate_id)
        donor = _select_profile_field_donor(matched_history, historical_evidence_by_candidate)
        review_donor = _select_membership_review_donor(matched_history, historical_evidence_by_candidate)
        merged_candidate = candidate
        if donor is not None:
            merged_candidate = _inherit_profile_fields(merged_candidate, donor)
        review_candidate = merged_candidate
        if review_donor is not None:
            review_candidate = _inherit_membership_review_fields(merged_candidate, review_donor)
            if review_candidate.to_record() != merged_candidate.to_record():
                inherited_review_count += 1
        merged_candidate = review_candidate
        if merged_candidate.to_record() != candidate.to_record():
            inherited_candidate_count += 1
        if bool(merged_candidate.metadata.get("membership_review_required")) and not bool(candidate.metadata.get("membership_review_required")):
            inherited_review_count += 1
        merged_candidates.append(merged_candidate)

        seen_history_candidate_ids: set[str] = set()
        for historical_candidate in matched_history:
            if historical_candidate.candidate_id in seen_history_candidate_ids:
                continue
            seen_history_candidate_ids.add(historical_candidate.candidate_id)
            for historical_evidence in historical_evidence_by_candidate.get(historical_candidate.candidate_id, []):
                if not _is_inheritable_profile_evidence(historical_evidence):
                    continue
                rebound = _rebind_evidence_candidate(historical_evidence, merged_candidate.candidate_id)
                evidence_key = _evidence_dedupe_key(rebound)
                if evidence_key in merged_evidence_by_key:
                    continue
                merged_evidence_by_key[evidence_key] = rebound
                inherited_evidence_count += 1

    carry_forward_groups: dict[str, list[Candidate]] = {}
    for historical_candidate in historical_candidates:
        if historical_candidate.candidate_id in matched_historical_candidate_ids:
            continue
        group_key = str(historical_candidate.candidate_id or "") or f"name:{normalize_name_token(historical_candidate.display_name or historical_candidate.name_en)}"
        carry_forward_groups.setdefault(group_key, []).append(historical_candidate)

    for group in carry_forward_groups.values():
        group_evidence: list[EvidenceRecord] = []
        for historical_candidate in group:
            group_evidence.extend(historical_evidence_by_candidate.get(historical_candidate.candidate_id, []))
        donor = _select_membership_review_donor(group, historical_evidence_by_candidate) or _select_profile_field_donor(group, historical_evidence_by_candidate) or max(
            group,
            key=lambda item: _historical_candidate_rank(item, historical_evidence_by_candidate.get(item.candidate_id, [])),
        )
        if not _should_carry_forward_historical_candidate(donor, group_evidence):
            continue
        merged_candidates.append(donor)
        carried_forward_candidate_count += 1
        for item in group_evidence:
            evidence_key = _evidence_dedupe_key(item)
            if evidence_key in merged_evidence_by_key:
                continue
            merged_evidence_by_key[evidence_key] = item
            inherited_evidence_count += 1

    merged_evidence = sorted(
        merged_evidence_by_key.values(),
        key=lambda item: (item.candidate_id, item.source_type, item.title, item.url or item.source_path),
    )
    return (
        merged_candidates,
        merged_evidence,
        _profile_inheritance_summary(
            history_source_count=len(historical_sources),
            history_candidate_count=len(historical_candidates),
            matched_candidate_count=matched_candidate_count,
            inherited_candidate_count=inherited_candidate_count,
            inherited_evidence_count=inherited_evidence_count,
            inherited_membership_review_count=inherited_review_count,
            carried_forward_candidate_count=carried_forward_candidate_count,
            history_sources=[
                {
                    "snapshot_id": str(item.get("snapshot_id") or ""),
                    "source_type": str(item.get("source_type") or ""),
                    "candidate_count": int(item.get("candidate_count") or 0),
                    "evidence_count": int(item.get("evidence_count") or 0),
                }
                for item in historical_sources
            ],
        ),
    )


def _profile_inheritance_summary(
    *,
    history_source_count: int = 0,
    history_candidate_count: int = 0,
    matched_candidate_count: int = 0,
    inherited_candidate_count: int = 0,
    inherited_evidence_count: int = 0,
    inherited_membership_review_count: int = 0,
    carried_forward_candidate_count: int = 0,
    history_sources: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "history_source_count": history_source_count,
        "history_candidate_count": history_candidate_count,
        "matched_candidate_count": matched_candidate_count,
        "inherited_candidate_count": inherited_candidate_count,
        "inherited_evidence_count": inherited_evidence_count,
        "inherited_membership_review_count": inherited_membership_review_count,
        "carried_forward_candidate_count": carried_forward_candidate_count,
        "history_sources": list(history_sources or []),
    }


def _load_snapshot_candidate_payload(payload_path: Path, target_company: str) -> tuple[list[Candidate], list[EvidenceRecord]]:
    try:
        payload = json.loads(payload_path.read_text())
    except (OSError, json.JSONDecodeError):
        return [], []
    if not isinstance(payload, dict):
        return [], []
    candidates: list[Candidate] = []
    candidate_ids: set[str] = set()
    for item in list(payload.get("candidates") or []):
        candidate = _candidate_from_payload(item)
        if candidate is None:
            continue
        if normalize_name_token(candidate.target_company) != normalize_name_token(target_company):
            continue
        candidates.append(candidate)
        candidate_ids.add(candidate.candidate_id)
    evidence: list[EvidenceRecord] = []
    for item in list(payload.get("evidence") or []):
        evidence_record = _evidence_record_from_payload(item)
        if evidence_record is None:
            continue
        if candidate_ids and evidence_record.candidate_id not in candidate_ids:
            continue
        evidence.append(evidence_record)
    return candidates, evidence


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


def _evidence_record_from_payload(payload: dict[str, Any]) -> EvidenceRecord | None:
    if not isinstance(payload, dict):
        return None
    candidate_id = str(payload.get("candidate_id") or "").strip()
    source_type = str(payload.get("source_type") or "").strip()
    title = str(payload.get("title") or "").strip()
    url = str(payload.get("url") or "").strip()
    source_dataset = str(payload.get("source_dataset") or "").strip() or source_type
    source_path = str(payload.get("source_path") or "").strip()
    if not candidate_id or not source_type:
        return None
    return EvidenceRecord(
        evidence_id=str(payload.get("evidence_id") or "").strip()
        or make_evidence_id(candidate_id, source_dataset, title, url or source_path),
        candidate_id=candidate_id,
        source_type=source_type,
        title=title,
        url=url,
        summary=str(payload.get("summary") or "").strip(),
        source_dataset=source_dataset,
        source_path=source_path,
        metadata=dict(payload.get("metadata") or {}),
    )


def _candidate_match_keys(candidate: Candidate) -> list[str]:
    keys: list[str] = []
    candidate_id = str(candidate.candidate_id or "").strip()
    if candidate_id:
        keys.append(f"id:{candidate_id}")
    for identifier in sorted(_candidate_linkedin_identifiers(candidate)):
        keys.append(f"linkedin:{identifier}")
    name_key = normalize_name_token(candidate.display_name or candidate.name_en)
    if name_key:
        keys.append(f"name:{name_key}")
    return keys


def _candidate_linkedin_identifiers(candidate: Candidate) -> set[str]:
    metadata = dict(candidate.metadata or {})
    identifiers: set[str] = set()
    for value in [
        candidate.linkedin_url,
        metadata.get("profile_url"),
        metadata.get("linkedin_url"),
        metadata.get("public_identifier"),
        metadata.get("seed_slug"),
        metadata.get("more_profiles"),
    ]:
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
    lowered = raw.lower().rstrip("/")
    if "linkedin.com" in lowered:
        slug = _extract_linkedin_slug(raw)
        if slug:
            return slug.lower()
        return lowered
    return raw.strip().strip("/").lower()


def _extract_linkedin_slug(url: str) -> str:
    match = re.search(r"linkedin\.com/in/([^/?#]+)", str(url or ""), re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()


def _match_historical_candidates(
    candidate: Candidate,
    historical_candidates: list[Candidate],
    match_index: dict[str, list[int]],
) -> list[Candidate]:
    matched: list[Candidate] = []
    seen_indexes: set[int] = set()

    candidate_id = str(candidate.candidate_id or "").strip()
    if candidate_id:
        for index in match_index.get(f"id:{candidate_id}", []):
            if index in seen_indexes:
                continue
            seen_indexes.add(index)
            matched.append(historical_candidates[index])
    if matched:
        return matched

    for identifier in sorted(_candidate_linkedin_identifiers(candidate)):
        for index in match_index.get(f"linkedin:{identifier}", []):
            if index in seen_indexes:
                continue
            seen_indexes.add(index)
            matched.append(historical_candidates[index])
    if matched:
        return matched

    name_key = normalize_name_token(candidate.display_name or candidate.name_en)
    if not name_key:
        return matched
    for index in match_index.get(f"name:{name_key}", []):
        if index in seen_indexes:
            continue
        seen_indexes.add(index)
        matched.append(historical_candidates[index])
    return matched


def _select_profile_field_donor(
    historical_candidates: list[Candidate],
    historical_evidence_by_candidate: dict[str, list[EvidenceRecord]],
) -> Candidate | None:
    ranked = sorted(
        historical_candidates,
        key=lambda item: _historical_candidate_rank(item, historical_evidence_by_candidate.get(item.candidate_id, [])),
        reverse=True,
    )
    for candidate in ranked:
        metadata = dict(candidate.metadata or {})
        if candidate.category == "non_member" or bool(metadata.get("target_company_mismatch")):
            continue
        if _historical_candidate_rank(candidate, historical_evidence_by_candidate.get(candidate.candidate_id, [])) <= 0:
            continue
        return candidate
    return None


def _select_membership_review_donor(
    historical_candidates: list[Candidate],
    historical_evidence_by_candidate: dict[str, list[EvidenceRecord]],
) -> Candidate | None:
    ranked = sorted(
        historical_candidates,
        key=lambda item: _membership_review_rank(item, historical_evidence_by_candidate.get(item.candidate_id, [])),
        reverse=True,
    )
    for candidate in ranked:
        if _membership_review_rank(candidate, historical_evidence_by_candidate.get(candidate.candidate_id, [])) > 0:
            return candidate
    return None


def _historical_candidate_rank(candidate: Candidate, evidence: list[EvidenceRecord]) -> int:
    score = 0
    if any(_is_inheritable_profile_evidence(item) for item in evidence):
        score += 10
    if str(candidate.education or "").strip():
        score += 3
    work_history = str(candidate.work_history or "").strip()
    if work_history and not _is_roster_baseline_only(candidate):
        score += 3
    if bool(dict(candidate.metadata or {}).get("membership_review_required")):
        score += 4
    if _has_explicit_manual_review_resolution(candidate, evidence):
        score += 6
    if str(candidate.focus_areas or "").strip() and str(candidate.focus_areas or "").strip() != str(candidate.role or "").strip():
        score += 1
    if str(candidate.linkedin_url or "").strip():
        score += 1
    return score


def _membership_review_rank(candidate: Candidate, evidence: list[EvidenceRecord]) -> int:
    metadata = dict(candidate.metadata or {})
    score = 0
    if _has_explicit_manual_review_resolution(candidate, evidence):
        score += 100
    if bool(metadata.get("membership_review_required")):
        score += 20
    if str(metadata.get("membership_review_decision") or "").strip():
        score += 8
    if candidate.category == "non_member":
        score += 6
    if bool(metadata.get("target_company_mismatch")):
        score += 4
    if score <= 0:
        return 0
    return score + _historical_candidate_rank(candidate, evidence)


def _inherit_profile_fields(candidate: Candidate, historical_candidate: Candidate) -> Candidate:
    current_record = candidate.to_record()
    historical_record = historical_candidate.to_record()
    current_metadata = dict(current_record.get("metadata") or {})
    historical_metadata = dict(historical_record.get("metadata") or {})
    baseline_only = _is_roster_baseline_only(candidate)

    if (not str(current_record.get("role") or "").strip() or baseline_only) and str(historical_record.get("role") or "").strip():
        current_record["role"] = historical_record["role"]
    if (
        not str(current_record.get("focus_areas") or "").strip()
        or baseline_only
        or str(current_record.get("focus_areas") or "").strip() == str(candidate.role or "").strip()
    ) and str(historical_record.get("focus_areas") or "").strip():
        current_record["focus_areas"] = historical_record["focus_areas"]
    if not str(current_record.get("education") or "").strip() and str(historical_record.get("education") or "").strip():
        current_record["education"] = historical_record["education"]
    if (
        not str(current_record.get("work_history") or "").strip()
        or baseline_only
        or str(current_record.get("work_history") or "").strip() == str(candidate.role or "").strip()
    ) and str(historical_record.get("work_history") or "").strip():
        current_record["work_history"] = historical_record["work_history"]
    if not str(current_record.get("linkedin_url") or "").strip() and str(historical_record.get("linkedin_url") or "").strip():
        current_record["linkedin_url"] = historical_record["linkedin_url"]

    for key in [
        "profile_url",
        "public_identifier",
        "profile_account_id",
        "profile_location",
        "more_profiles",
        "membership_claim_category",
        "membership_claim_employment_status",
        "membership_review_required",
        "membership_review_reason",
        "membership_review_decision",
        "membership_review_confidence",
        "membership_review_rationale",
        "membership_review_triggers",
        "membership_review_trigger_keywords",
    ]:
        incoming_value = historical_metadata.get(key)
        if incoming_value in ("", None, [], {}):
            continue
        existing_value = current_metadata.get(key)
        if existing_value in ("", None, [], {}):
            current_metadata[key] = _copy_metadata_value(incoming_value)

    if bool(historical_metadata.get("membership_review_required")):
        current_metadata["membership_review_required"] = True
        if str(historical_metadata.get("membership_review_reason") or "").strip():
            current_metadata["membership_review_reason"] = str(historical_metadata.get("membership_review_reason") or "").strip()
        if str(historical_metadata.get("membership_review_decision") or "").strip():
            current_metadata["membership_review_decision"] = str(historical_metadata.get("membership_review_decision") or "").strip()
        if str(historical_metadata.get("membership_review_confidence") or "").strip():
            current_metadata["membership_review_confidence"] = str(historical_metadata.get("membership_review_confidence") or "").strip()
        if historical_metadata.get("membership_review_triggers"):
            current_metadata["membership_review_triggers"] = list(historical_metadata.get("membership_review_triggers") or [])
        if historical_metadata.get("membership_review_trigger_keywords"):
            current_metadata["membership_review_trigger_keywords"] = list(historical_metadata.get("membership_review_trigger_keywords") or [])
        review_rationale = str(historical_metadata.get("membership_review_rationale") or "").strip()
        if review_rationale:
            current_metadata["membership_review_rationale"] = review_rationale
            current_record["notes"] = _append_unique_note(str(current_record.get("notes") or "").strip(), review_rationale)

    current_record["metadata"] = current_metadata
    if str(historical_record.get("source_path") or "").strip() and current_record != candidate.to_record():
        current_record["source_path"] = historical_record["source_path"]
    return Candidate(**current_record)


def _inherit_membership_review_fields(candidate: Candidate, historical_candidate: Candidate) -> Candidate:
    current_record = candidate.to_record()
    historical_record = historical_candidate.to_record()
    current_metadata = dict(current_record.get("metadata") or {})
    historical_metadata = dict(historical_record.get("metadata") or {})

    if _has_explicit_manual_review_resolution(historical_candidate):
        current_record["category"] = str(historical_record.get("category") or "").strip() or current_record["category"]
        current_record["employment_status"] = str(historical_record.get("employment_status") or "").strip()
        current_record["organization"] = str(historical_record.get("organization") or "").strip()
        if str(historical_record.get("role") or "").strip() or historical_candidate.category == "non_member":
            current_record["role"] = str(historical_record.get("role") or "").strip()
        if str(historical_record.get("team") or "").strip():
            current_record["team"] = str(historical_record.get("team") or "").strip()
        if str(historical_record.get("linkedin_url") or "").strip() or historical_candidate.category == "non_member":
            current_record["linkedin_url"] = str(historical_record.get("linkedin_url") or "").strip()
        if str(historical_record.get("media_url") or "").strip() or historical_candidate.category == "non_member":
            current_record["media_url"] = str(historical_record.get("media_url") or "").strip()
        if str(historical_record.get("current_destination") or "").strip():
            current_record["current_destination"] = str(historical_record.get("current_destination") or "").strip()
        if str(historical_record.get("source_path") or "").strip():
            current_record["source_path"] = str(historical_record.get("source_path") or "").strip()
        if str(historical_record.get("source_dataset") or "").strip():
            current_record["source_dataset"] = str(historical_record.get("source_dataset") or "").strip()
        if str(historical_record.get("notes") or "").strip():
            current_record["notes"] = _append_unique_note(
                str(current_record.get("notes") or "").strip(),
                str(historical_record.get("notes") or "").strip(),
            )

        decision = str(historical_metadata.get("membership_review_decision") or "").strip()
        if not decision:
            if current_record["category"] == "non_member" or bool(historical_metadata.get("target_company_mismatch")):
                decision = "manual_non_member"
            elif current_record["category"] in {"employee", "former_employee"} and str(current_record["employment_status"] or "").strip():
                decision = "manual_confirmed_member"

        current_metadata["membership_review_required"] = bool(historical_metadata.get("membership_review_required"))
        current_metadata["membership_review_reason"] = str(historical_metadata.get("membership_review_reason") or "").strip()
        current_metadata["membership_review_decision"] = decision
        current_metadata["membership_review_confidence"] = str(historical_metadata.get("membership_review_confidence") or "").strip()
        current_metadata["membership_review_rationale"] = str(historical_metadata.get("membership_review_rationale") or "").strip()
        current_metadata["membership_review_triggers"] = list(historical_metadata.get("membership_review_triggers") or [])
        current_metadata["membership_review_trigger_keywords"] = list(
            historical_metadata.get("membership_review_trigger_keywords") or []
        )
        current_metadata["target_company_mismatch"] = bool(historical_metadata.get("target_company_mismatch"))
        if "manual_review_links" in historical_metadata or historical_metadata.get("manual_review_links"):
            current_metadata["manual_review_links"] = _copy_metadata_value(historical_metadata.get("manual_review_links") or [])
        if "manual_review_artifact_root" in historical_metadata or historical_metadata.get("manual_review_artifact_root"):
            current_metadata["manual_review_artifact_root"] = str(historical_metadata.get("manual_review_artifact_root") or "").strip()
        if "manual_review_signals" in historical_metadata:
            current_metadata["manual_review_signals"] = _copy_metadata_value(historical_metadata.get("manual_review_signals") or {})
        if str(historical_record.get("linkedin_url") or "").strip():
            current_metadata["profile_url"] = str(historical_record.get("linkedin_url") or "").strip()
        for key in ["public_identifier", "profile_account_id", "profile_location", "more_profiles"]:
            if key in historical_metadata and historical_metadata.get(key) not in ("", None, [], {}):
                current_metadata[key] = _copy_metadata_value(historical_metadata.get(key))
    elif bool(historical_metadata.get("membership_review_required")):
        current_metadata["membership_review_required"] = True
        if str(historical_metadata.get("membership_review_reason") or "").strip():
            current_metadata["membership_review_reason"] = str(historical_metadata.get("membership_review_reason") or "").strip()
        if str(historical_metadata.get("membership_review_decision") or "").strip():
            current_metadata["membership_review_decision"] = str(historical_metadata.get("membership_review_decision") or "").strip()
        if str(historical_metadata.get("membership_review_confidence") or "").strip():
            current_metadata["membership_review_confidence"] = str(historical_metadata.get("membership_review_confidence") or "").strip()
        if historical_metadata.get("membership_review_triggers"):
            current_metadata["membership_review_triggers"] = list(historical_metadata.get("membership_review_triggers") or [])
        if historical_metadata.get("membership_review_trigger_keywords"):
            current_metadata["membership_review_trigger_keywords"] = list(historical_metadata.get("membership_review_trigger_keywords") or [])
        review_rationale = str(historical_metadata.get("membership_review_rationale") or "").strip()
        if review_rationale:
            current_metadata["membership_review_rationale"] = review_rationale
            current_record["notes"] = _append_unique_note(str(current_record.get("notes") or "").strip(), review_rationale)

    current_record["metadata"] = current_metadata
    return Candidate(**current_record)


def _copy_metadata_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _copy_metadata_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_metadata_value(item) for item in value]
    return value


def _append_unique_note(existing: str, incoming: str) -> str:
    normalized_existing = str(existing or "").strip()
    normalized_incoming = str(incoming or "").strip()
    if not normalized_incoming:
        return normalized_existing
    if not normalized_existing:
        return normalized_incoming
    if normalized_incoming in normalized_existing:
        return normalized_existing
    return f"{normalized_existing} | {normalized_incoming}"


def _is_roster_baseline_only(candidate: Candidate) -> bool:
    source_dataset = str(candidate.source_dataset or "").strip()
    role = str(candidate.role or "").strip()
    work_history = str(candidate.work_history or "").strip()
    notes = str(candidate.notes or "")
    return (
        source_dataset.endswith("_linkedin_company_people")
        and bool(role)
        and (not work_history or work_history == role)
        and "LinkedIn company roster baseline." in notes
        and not str(candidate.education or "").strip()
    )


def _is_inheritable_profile_evidence(evidence: EvidenceRecord) -> bool:
    source_type = str(evidence.source_type or "").strip()
    source_dataset = str(evidence.source_dataset or "").strip()
    if source_dataset == "manual_review" or source_type == "manual_review_link":
        return True
    if source_type == "manual_review_analysis":
        return True
    return source_type.startswith("linkedin_profile_")


def _has_explicit_manual_review_resolution(candidate: Candidate, evidence: list[EvidenceRecord] | None = None) -> bool:
    metadata = dict(candidate.metadata or {})
    if str(metadata.get("manual_review_artifact_root") or "").strip():
        return True
    if list(metadata.get("manual_review_links") or []):
        return True
    decision = str(metadata.get("membership_review_decision") or "").strip().lower()
    if decision.startswith("manual_"):
        return True
    for item in list(evidence or []):
        if str(item.source_dataset or "").strip() == "manual_review":
            return True
    return False


def _should_carry_forward_historical_candidate(candidate: Candidate, evidence: list[EvidenceRecord]) -> bool:
    metadata = dict(candidate.metadata or {})
    if bool(metadata.get("membership_review_required")):
        return True
    if candidate.category in {"lead", "former_employee", "investor", "non_member"}:
        return True
    if str(candidate.education or "").strip():
        return True
    if str(candidate.work_history or "").strip() and not _is_roster_baseline_only(candidate):
        return True
    for item in evidence:
        if _is_inheritable_profile_evidence(item):
            return True
        if str(item.source_type or "").strip() not in {"linkedin_company_people", "company_roster"}:
            return True
    return False


def _rebind_evidence_candidate(evidence: EvidenceRecord, candidate_id: str) -> EvidenceRecord:
    title = str(evidence.title or "").strip()
    url = str(evidence.url or "").strip()
    source_path = str(evidence.source_path or "").strip()
    source_dataset = str(evidence.source_dataset or evidence.source_type or "").strip()
    return EvidenceRecord(
        evidence_id=make_evidence_id(candidate_id, source_dataset, title, url or source_path),
        candidate_id=candidate_id,
        source_type=str(evidence.source_type or "").strip(),
        title=title,
        url=url,
        summary=str(evidence.summary or "").strip(),
        source_dataset=source_dataset,
        source_path=source_path,
        metadata=_copy_metadata_value(dict(evidence.metadata or {})),
    )


def _evidence_dedupe_key(evidence: EvidenceRecord) -> tuple[str, str, str, str]:
    return (
        str(evidence.candidate_id or "").strip(),
        str(evidence.source_dataset or evidence.source_type or "").strip(),
        str(evidence.title or "").strip(),
        str(evidence.url or evidence.source_path or "").strip(),
    )


def _load_evidence_records(store: SQLiteStore, candidates: list[Candidate]) -> list[EvidenceRecord]:
    evidence_records: list[EvidenceRecord] = []
    seen_ids: set[str] = set()
    for candidate in candidates:
        for item in store.list_evidence(candidate.candidate_id):
            evidence_id = str(item.get("evidence_id") or make_evidence_id(candidate.candidate_id, str(item.get("source_dataset") or ""), str(item.get("title") or ""), str(item.get("url") or "")))
            if evidence_id in seen_ids:
                continue
            seen_ids.add(evidence_id)
            evidence_records.append(
                EvidenceRecord(
                    evidence_id=evidence_id,
                    candidate_id=candidate.candidate_id,
                    source_type=str(item.get("source_type") or ""),
                    title=str(item.get("title") or ""),
                    url=str(item.get("url") or ""),
                    summary=str(item.get("summary") or ""),
                    source_dataset=str(item.get("source_dataset") or ""),
                    source_path=str(item.get("source_path") or ""),
                    metadata=dict(item.get("metadata") or {}),
                )
            )
    return evidence_records


def _build_investor_firm_plan(target_company: str, candidates: list[Candidate], job_request: JobRequest) -> dict[str, Any]:
    grouped: dict[str, list[Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.organization or "Unknown Firm", []).append(candidate)
    firms: list[dict[str, Any]] = []
    for organization, members in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0].lower())):
        direct_count = sum(1 for member in members if _investment_involvement_label(member.investment_involvement) == "yes")
        possible_count = sum(1 for member in members if _investment_involvement_label(member.investment_involvement) == "possible")
        if direct_count > 0 or len(members) >= 8:
            tier = "tier_1"
        elif possible_count > 0 or len(members) >= 4:
            tier = "tier_2"
        else:
            tier = "tier_3"
        firms.append(
            {
                "organization": organization,
                "tier": tier,
                "member_count": len(members),
                "direct_investment_member_count": direct_count,
                "possible_investment_member_count": possible_count,
                "sample_members": [member.display_name for member in members[:5]],
                "recommended_channels": _recommended_investor_channels(tier),
            }
        )
    return {
        "target_company": target_company,
        "strategy_type": "investor_firm_roster",
        "target_population": (
            f"Full investor-firm roster for institutions connected to {target_company}, followed by later filtering "
            "for role, involvement, and other sourcing criteria."
        ),
        "keywords": list(job_request.keywords or []),
        "firms": firms,
        "tier_summary": {
            "tier_1": sum(1 for firm in firms if firm["tier"] == "tier_1"),
            "tier_2": sum(1 for firm in firms if firm["tier"] == "tier_2"),
            "tier_3": sum(1 for firm in firms if firm["tier"] == "tier_3"),
        },
    }


def _recommended_investor_channels(tier: str) -> list[str]:
    if tier == "tier_1":
        return ["internal_investor_asset", "linkedin_company_roster", "targeted_web_search"]
    if tier == "tier_2":
        return ["internal_investor_asset", "targeted_web_search", "linkedin_people_search"]
    return ["internal_investor_asset", "targeted_web_search"]


def _investment_involvement_label(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized.startswith("⭐ 是") or normalized.startswith("是"):
        return "yes"
    if normalized.startswith("可能"):
        return "possible"
    if normalized.startswith("否"):
        return "no"
    return "unknown"
