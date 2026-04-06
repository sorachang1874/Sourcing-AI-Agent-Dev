from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
import json
from pathlib import Path
from typing import Any

from .asset_catalog import AssetCatalog
from .asset_logger import AssetLogger
from .agent_runtime import AgentRuntimeCoordinator
from .connectors import (
    CompanyIdentity,
    CompanyRosterSnapshot,
    LinkedInCompanyRosterConnector,
    build_candidates_from_roster,
    load_rapidapi_accounts,
    profile_detail_accounts,
    resolve_company_identity,
)
from .domain import AcquisitionTask, Candidate, EvidenceRecord, JobRequest, make_evidence_id
from .enrichment import MultiSourceEnricher
from .harvest_connectors import (
    HarvestCompanyEmployeesConnector,
    HarvestProfileConnector,
    HarvestProfileSearchConnector,
    discover_legacy_harvest_token,
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
                return self._acquire_full_roster(task, state)
            if strategy_type in {"scoped_search_roster", "former_employee_search"}:
                return self._acquire_search_seed_pool(task, state, job_request)
            if strategy_type == "investor_firm_roster":
                return self._acquire_investor_firm_roster(task, state, job_request)
            return self._acquire_full_roster(task, state)
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

    def _acquire_full_roster(self, task: AcquisitionTask, state: dict[str, Any]) -> AcquisitionExecution:
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
        try:
            if self.harvest_company_connector.settings.enabled and bool(cost_policy.get("allow_company_employee_api", True)):
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
        if str(normalization_scope.get("mode") or "") == "category" and str(normalization_scope.get("category") or "").strip():
            self.store.replace_company_category_data(
                identity.canonical_name,
                str(normalization_scope.get("category") or ""),
                candidates,
                evidence,
            )
        else:
            self.store.replace_company_data(identity.canonical_name, candidates, evidence)
        logger = AssetLogger(snapshot_dir)
        manifest_path = snapshot_dir / "manifest.json"
        logger.write_json(
            manifest_path,
            {
                "snapshot_id": state.get("snapshot_id"),
                "company_identity": identity.to_record(),
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "normalization_scope": normalization_scope or {"mode": "company"},
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
                "manifest_path": str(manifest_path),
            },
            state_updates={"manifest_path": manifest_path},
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
