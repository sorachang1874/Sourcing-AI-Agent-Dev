from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
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
from .company_shard_planning import (
    normalize_company_employee_shard_policy,
    plan_company_employee_shards_from_policy,
)
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
from .search_provider import SearchProviderError, build_search_provider
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


def _effective_cost_policy(
    task_cost_policy: dict[str, Any] | None,
    job_request: JobRequest | None = None,
) -> dict[str, Any]:
    effective = dict(task_cost_policy or {})
    execution_preferences = dict(getattr(job_request, "execution_preferences", {}) or {})
    if bool(execution_preferences.get("force_fresh_run")):
        effective["allow_cached_roster_fallback"] = False
        effective["allow_historical_profile_inheritance"] = False
        effective["allow_shared_provider_cache"] = False
    return effective


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
        self.model_client = model_client
        self.search_provider = search_provider
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
            store=self.store,
        )

    def _resolve_harvest_settings(self, actor_settings: Any, legacy_harvest_token: str) -> Any:
        if getattr(actor_settings, "enabled", False):
            return actor_settings
        if not legacy_harvest_token:
            return actor_settings
        return replace(actor_settings, enabled=True, api_token=legacy_harvest_token)

    def detect_reusable_roster_snapshot(self, target_company: str) -> dict[str, Any] | None:
        identity = resolve_company_identity(
            target_company,
            self.catalog.legacy_company_ids,
            model_client=self.model_client,
        )
        snapshot = self._load_cached_roster_snapshot(identity)
        if snapshot is None:
            return None
        source_kind = (
            "harvest_company_employees"
            if snapshot.summary_path.parent.name == "harvest_company_employees"
            else "linkedin_company_people"
        )
        return {
            "target_company": identity.canonical_name,
            "company_key": identity.company_key,
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_dir": str(snapshot.snapshot_dir),
            "visible_entry_count": len(snapshot.visible_entries),
            "headless_entry_count": len(snapshot.headless_entries),
            "source_kind": source_kind,
            "summary_path": str(snapshot.summary_path),
            "stop_reason": snapshot.stop_reason,
        }

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
        use_local_anthropic_assets = self._should_use_local_anthropic_assets(job_request)
        if task.task_type == "resolve_company_identity":
            return self._resolve_company(target_company, task, state)
        strategy_type = str(task.metadata.get("strategy_type") or "")
        if task.task_type == "acquire_full_roster":
            strategy_type = strategy_type or "full_company_roster"
            if strategy_type == "full_company_roster":
                if company_key == "anthropic" and use_local_anthropic_assets:
                    return self._anthropic_local_asset_task(task, bootstrap_summary or {}, state)
                return self._acquire_full_roster(task, state, job_request)
            if strategy_type in {"scoped_search_roster", "former_employee_search"}:
                return self._acquire_search_seed_pool(task, state, job_request)
            if strategy_type == "investor_firm_roster":
                return self._acquire_investor_firm_roster(task, state, job_request)
            return self._acquire_full_roster(task, state, job_request)
        if company_key == "anthropic" and strategy_type != "investor_firm_roster" and use_local_anthropic_assets:
            return self._anthropic_local_asset_task(task, bootstrap_summary or {}, state)
        if task.task_type == "enrich_profiles_multisource":
            return self._enrich_profiles(task, state, job_request)
        if task.task_type == "normalize_asset_snapshot":
            return self._normalize_snapshot(task, state, job_request)
        if task.task_type == "build_retrieval_index":
            return self._build_retrieval_index(task, state)
        return AcquisitionExecution(
            task_id=task.task_id,
            status="blocked",
            detail="No connector is implemented yet for this acquisition step.",
            payload={"task_type": task.task_type, "target_company": target_company},
        )

    def _should_use_local_anthropic_assets(self, job_request: JobRequest) -> bool:
        execution_preferences = dict(job_request.execution_preferences or {})
        if bool(execution_preferences.get("allow_local_bootstrap_fallback")):
            return True
        return not bool(execution_preferences.get("force_fresh_run"))

    def _resolve_company(
        self,
        target_company: str,
        task: AcquisitionTask,
        state: dict[str, Any],
    ) -> AcquisitionExecution:
        identity = resolve_company_identity(target_company, self.catalog.legacy_company_ids)
        observed_companies: list[dict[str, Any]] = []
        if identity.confidence == "low" and not identity.local_asset_available:
            observed_companies = self._discover_company_identity_candidates(target_company)
            if observed_companies:
                upgraded = resolve_company_identity(
                    target_company,
                    self.catalog.legacy_company_ids,
                    model_client=self.model_client,
                    observed_companies=observed_companies,
                )
                if upgraded.resolver != "heuristic" or upgraded.confidence != "low":
                    upgraded.metadata = {
                        **dict(upgraded.metadata or {}),
                        "observed_company_candidates": observed_companies,
                    }
                    identity = upgraded
                else:
                    identity.metadata = {
                        **dict(identity.metadata or {}),
                        "observed_company_candidates": observed_companies,
                    }
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
                "observed_company_candidate_count": len(observed_companies),
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
        cost_policy = _effective_cost_policy(task.metadata.get("cost_policy"), job_request)
        max_pages = int(task.metadata.get("max_pages", 10) or 10)
        page_limit = int(task.metadata.get("page_limit", 50) or 50)
        company_employee_shards = _normalize_company_employee_shards(task.metadata.get("company_employee_shards"))
        company_employee_shard_policy = normalize_company_employee_shard_policy(
            task.metadata.get("company_employee_shard_policy")
        )
        job_id = str(state.get("job_id") or "")
        allow_cached_roster_fallback = bool(cost_policy.get("allow_cached_roster_fallback", True))
        allow_shared_provider_cache = bool(cost_policy.get("allow_shared_provider_cache", True))
        reuse_existing_roster = bool(
            job_request.execution_preferences.get("reuse_existing_roster") or task.metadata.get("reuse_existing_roster")
        )
        acquisition_mode = "live_roster_acquisition"
        explicitly_requested_former_seed = "run_former_search_seed" in dict(job_request.execution_preferences or {})
        should_run_former_search_seed = self._should_run_default_former_search_seed(task, job_request)
        former_parallel_executor: ThreadPoolExecutor | None = None
        former_parallel_future = None

        def _start_parallel_former_search_seed_if_needed() -> None:
            nonlocal former_parallel_executor, former_parallel_future
            if not should_run_former_search_seed:
                return
            if former_parallel_future is not None:
                return
            if self.worker_runtime is not None and job_id:
                return
            former_parallel_executor = ThreadPoolExecutor(max_workers=1)
            former_parallel_future = former_parallel_executor.submit(
                self._acquire_default_former_search_seed,
                task,
                state,
                job_request,
                identity,
            )

        def _shutdown_parallel_former(wait: bool) -> None:
            nonlocal former_parallel_executor, former_parallel_future
            if former_parallel_executor is None:
                return
            former_parallel_executor.shutdown(wait=wait)
            former_parallel_executor = None
            former_parallel_future = None

        def _kickoff_former_search_seed_for_background_roster() -> tuple[SearchSeedSnapshot | None, str, str, str]:
            if not should_run_former_search_seed:
                return None, "", "", "skipped"
            try:
                former_execution = self._acquire_default_former_search_seed(task, state, job_request, identity)
            except Exception as exc:
                return None, "", str(exc), "failed"
            former_snapshot = former_execution.state_updates.get("search_seed_snapshot")
            return (
                former_snapshot if isinstance(former_snapshot, SearchSeedSnapshot) else None,
                str(former_execution.detail or ""),
                str(former_execution.detail or "") if former_execution.status == "blocked" else "",
                str(former_execution.status or ""),
            )

        try:
            if reuse_existing_roster:
                cached_snapshot = self._load_cached_roster_snapshot(identity, exclude_snapshot_dir=snapshot_dir)
                if cached_snapshot is None:
                    return AcquisitionExecution(
                        task_id=task.task_id,
                        status="blocked",
                        detail=(
                            "Requested incremental roster reuse, but no existing local roster snapshot was available "
                            "for this company."
                        ),
                        payload={"target_company": identity.canonical_name, "reuse_existing_roster": True},
                    )
                snapshot = self._materialize_reused_roster_snapshot(identity, cached_snapshot, snapshot_dir)
                acquisition_mode = "reused_cached_roster"
            elif self.harvest_company_connector.settings.enabled and bool(cost_policy.get("allow_company_employee_api", True)):
                adaptive_shard_plan: dict[str, Any] = {}
                if not company_employee_shards and company_employee_shard_policy:
                    adaptive_shard_plan = self._resolve_adaptive_company_employee_shards(
                        identity=identity,
                        snapshot_dir=snapshot_dir,
                        policy=company_employee_shard_policy,
                    )
                    if str(adaptive_shard_plan.get("status") or "") != "planned":
                        return AcquisitionExecution(
                            task_id=task.task_id,
                            status="blocked",
                            detail=str(adaptive_shard_plan.get("detail") or "Adaptive shard planning failed."),
                            payload={
                                "target_company": identity.canonical_name,
                                "adaptive_shard_plan": adaptive_shard_plan,
                            },
                        )
                    company_employee_shards = _normalize_company_employee_shards(adaptive_shard_plan.get("shards"))
                if company_employee_shards:
                    if self.worker_runtime is not None and job_id:
                        shard_worker_summary = self._execute_segmented_harvest_company_roster_workers(
                            identity=identity,
                            snapshot_dir=snapshot_dir,
                            shards=company_employee_shards,
                            job_id=job_id,
                            request_payload=job_request.to_record(),
                            plan_payload=dict(state.get("plan_payload") or {}),
                            runtime_mode=str(state.get("runtime_mode") or "workflow"),
                            allow_shared_provider_cache=allow_shared_provider_cache,
                        )
                        if int(shard_worker_summary.get("queued_count") or 0) > 0:
                            (
                                former_search_seed_snapshot,
                                former_search_detail,
                                former_search_error,
                                former_search_status,
                            ) = _kickoff_former_search_seed_for_background_roster()
                            payload: dict[str, Any] = {
                                "queued_harvest_worker_count": int(shard_worker_summary.get("queued_count") or 0),
                                "completed_harvest_worker_count": int(
                                    shard_worker_summary.get("completed_count") or 0
                                ),
                                "shard_count": int(shard_worker_summary.get("shard_count") or 0),
                                "stop_reason": "queued_background_harvest_shards",
                                "harvest_workers": list(shard_worker_summary.get("queued_summaries") or []),
                                "completed_harvest_workers": list(
                                    shard_worker_summary.get("completed_summaries") or []
                                ),
                                "former_search_seed_status": former_search_status,
                            }
                            if former_search_detail:
                                payload["former_search_seed_detail"] = former_search_detail
                            if former_search_error:
                                payload["former_search_seed_error"] = former_search_error
                            state_updates: dict[str, Any] = {}
                            if isinstance(former_search_seed_snapshot, SearchSeedSnapshot):
                                payload["former_search_seed_snapshot"] = former_search_seed_snapshot.to_record()
                                payload["former_search_seed_entry_count"] = len(former_search_seed_snapshot.entries)
                                state_updates["search_seed_snapshot"] = former_search_seed_snapshot
                            return AcquisitionExecution(
                                task_id=task.task_id,
                                status="blocked",
                                detail=(
                                    "Roster acquisition queued background segmented Harvest company-employees runs; "
                                    "former seed discovery started in parallel. Resume after worker recovery completes remote dataset fetch."
                                ),
                                payload=payload,
                                state_updates=state_updates,
                            )
                    _start_parallel_former_search_seed_if_needed()
                    snapshot = self._fetch_segmented_harvest_company_roster(
                        identity=identity,
                        snapshot_dir=snapshot_dir,
                        shards=company_employee_shards,
                        allow_shared_provider_cache=allow_shared_provider_cache,
                    )
                elif self.worker_runtime is not None and job_id:
                    harvest_worker = self._execute_harvest_company_roster_worker(
                        identity=identity,
                        snapshot_dir=snapshot_dir,
                        max_pages=max_pages,
                        page_limit=page_limit,
                        job_id=job_id,
                        request_payload=job_request.to_record(),
                        plan_payload=dict(state.get("plan_payload") or {}),
                        runtime_mode=str(state.get("runtime_mode") or "workflow"),
                        allow_shared_provider_cache=allow_shared_provider_cache,
                    )
                    if str(harvest_worker.get("worker_status") or "") == "queued":
                        summary = dict(harvest_worker.get("summary") or {})
                        (
                            former_search_seed_snapshot,
                            former_search_detail,
                            former_search_error,
                            former_search_status,
                        ) = _kickoff_former_search_seed_for_background_roster()
                        payload: dict[str, Any] = {
                            "queued_harvest_worker_count": 1,
                            "stop_reason": "queued_background_harvest",
                            "harvest_worker": summary,
                            "former_search_seed_status": former_search_status,
                        }
                        if former_search_detail:
                            payload["former_search_seed_detail"] = former_search_detail
                        if former_search_error:
                            payload["former_search_seed_error"] = former_search_error
                        state_updates: dict[str, Any] = {}
                        if isinstance(former_search_seed_snapshot, SearchSeedSnapshot):
                            payload["former_search_seed_snapshot"] = former_search_seed_snapshot.to_record()
                            payload["former_search_seed_entry_count"] = len(former_search_seed_snapshot.entries)
                            state_updates["search_seed_snapshot"] = former_search_seed_snapshot
                        return AcquisitionExecution(
                            task_id=task.task_id,
                            status="blocked",
                            detail=(
                                "Roster acquisition queued a background Harvest company-employees run; "
                                "former seed discovery started in parallel. Resume after worker recovery completes remote dataset fetch."
                            ),
                            payload=payload,
                            state_updates=state_updates,
                        )
                    snapshot = self.harvest_company_connector.fetch_company_roster(
                        identity,
                        snapshot_dir,
                        asset_logger=AssetLogger(snapshot_dir),
                        max_pages=max_pages,
                        page_limit=page_limit,
                        allow_shared_provider_cache=allow_shared_provider_cache,
                    )
                else:
                    _start_parallel_former_search_seed_if_needed()
                    snapshot = self.harvest_company_connector.fetch_company_roster(
                        identity,
                        snapshot_dir,
                        asset_logger=AssetLogger(snapshot_dir),
                        max_pages=max_pages,
                        page_limit=page_limit,
                        allow_shared_provider_cache=allow_shared_provider_cache,
                    )
            else:
                _start_parallel_former_search_seed_if_needed()
                snapshot = self.roster_connector.fetch_company_roster(
                    identity,
                    snapshot_dir,
                    asset_logger=AssetLogger(snapshot_dir),
                    max_pages=max_pages,
                    page_limit=page_limit,
                )
        except Exception as exc:
            _shutdown_parallel_former(wait=False)
            cached_snapshot = self._load_cached_roster_snapshot(identity, exclude_snapshot_dir=snapshot_dir) if allow_cached_roster_fallback else None
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
            _shutdown_parallel_former(wait=False)
            cached_snapshot = self._load_cached_roster_snapshot(identity, exclude_snapshot_dir=snapshot_dir) if allow_cached_roster_fallback else None
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

        former_search_seed_snapshot = None
        former_search_detail = ""
        former_search_error = ""
        if should_run_former_search_seed:
            if former_parallel_future is not None:
                try:
                    former_execution = former_parallel_future.result()
                finally:
                    _shutdown_parallel_former(wait=False)
            else:
                former_execution = self._acquire_default_former_search_seed(task, state, job_request, identity)
            former_search_seed_snapshot = former_execution.state_updates.get("search_seed_snapshot")
            former_search_detail = former_execution.detail
            if former_execution.status == "blocked":
                former_search_error = former_execution.detail
                former_payload = dict(former_execution.payload or {})
                former_stop_reason = str(former_payload.get("stop_reason") or "").strip()
                former_queued_query_count = int(former_payload.get("queued_query_count") or 0)
                should_wait_for_former_background = former_stop_reason == "queued_background_search" or former_queued_query_count > 0
                if explicitly_requested_former_seed or should_wait_for_former_background:
                    waiting_reason = (
                        "Former search queued background tasks and will auto-resume to run incremental enrichment on newly discovered URLs."
                        if should_wait_for_former_background and not explicitly_requested_former_seed
                        else former_execution.detail
                    )
                    return AcquisitionExecution(
                        task_id=task.task_id,
                        status="blocked",
                        detail=waiting_reason,
                        payload={
                            **snapshot.to_record(),
                            "acquisition_mode": acquisition_mode,
                            "former_search_seed_status": "blocked",
                            "former_search_seed_detail": former_search_detail,
                            "former_search_seed_stop_reason": former_stop_reason,
                            "former_search_seed_queued_query_count": former_queued_query_count,
                        },
                        state_updates={
                            "roster_snapshot": snapshot,
                            **(
                                {"search_seed_snapshot": former_search_seed_snapshot}
                                if isinstance(former_search_seed_snapshot, SearchSeedSnapshot)
                                else {}
                            ),
                        },
                    )

        detail = (
            f"Captured {len(snapshot.raw_entries)} roster rows "
            f"({len(snapshot.visible_entries)} visible, {len(snapshot.headless_entries)} headless)."
        )
        if acquisition_mode == "reused_cached_roster":
            detail = (
                f"Reused local roster snapshot {snapshot.snapshot_id} with "
                f"{len(snapshot.visible_entries)} visible members."
            )
        if company_employee_shards:
            detail += f" Used {len(company_employee_shards)} segmented Harvest company-employee shards."
        payload = snapshot.to_record()
        payload["acquisition_mode"] = acquisition_mode
        state_updates: dict[str, Any] = {"roster_snapshot": snapshot}
        if isinstance(former_search_seed_snapshot, SearchSeedSnapshot):
            detail += f" Added {len(former_search_seed_snapshot.entries)} former-member search seeds."
            payload["former_search_seed_snapshot"] = former_search_seed_snapshot.to_record()
            payload["former_search_seed_entry_count"] = len(former_search_seed_snapshot.entries)
            state_updates["search_seed_snapshot"] = former_search_seed_snapshot
        elif former_search_error:
            payload["former_search_seed_error"] = former_search_error
        if former_search_detail:
            payload["former_search_seed_detail"] = former_search_detail

        _shutdown_parallel_former(wait=False)
        return AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail=detail,
            payload=payload,
            state_updates=state_updates,
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
        cost_policy = _effective_cost_policy(task.metadata.get("cost_policy"), job_request)
        employment_statuses = list(task.metadata.get("employment_statuses") or [])
        employment_status = employment_statuses[0] if employment_statuses else "current"
        provider_only_former_pass = employment_status == "former" and bool(
            list(filter_hints.get("past_companies") or []) or str(identity.linkedin_company_url or "").strip()
        )
        if not search_seed_queries and not query_bundles and not provider_only_former_pass:
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
        cost_policy = _effective_cost_policy(task.metadata.get("cost_policy"), job_request)
        if not isinstance(snapshot_dir, Path):
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="Snapshot directory must exist before profile enrichment.",
                payload={},
            )

        canonicalization_summary: dict[str, Any] = {}
        source_snapshots: dict[str, Any] = {}
        if isinstance(snapshot, CompanyRosterSnapshot) or isinstance(search_seed_snapshot, SearchSeedSnapshot):
            candidates = []
            evidence = []
            if isinstance(snapshot, CompanyRosterSnapshot):
                roster_candidates, roster_evidence = build_candidates_from_roster(snapshot)
                candidates.extend(roster_candidates)
                evidence.extend(roster_evidence)
                source_snapshots["roster_snapshot"] = snapshot.to_record()
            if isinstance(search_seed_snapshot, SearchSeedSnapshot):
                search_candidates, search_evidence = build_candidates_from_seed_snapshot(search_seed_snapshot)
                candidates.extend(search_candidates)
                evidence.extend(search_evidence)
                source_snapshots["search_seed_snapshot"] = search_seed_snapshot.to_record()
            if len(candidates) > 1:
                candidates, evidence, canonicalization_summary = canonicalize_company_records(candidates, evidence)
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
                    source_snapshots
                    if source_snapshots
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
        full_roster_profile_prefetch = bool(task.metadata.get("full_roster_profile_prefetch"))
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
            parallel_exploration_workers=int(cost_policy.get("parallel_exploration_workers", 2) or 2),
            cost_policy=cost_policy,
            full_roster_profile_prefetch=full_roster_profile_prefetch,
        )
        if full_roster_profile_prefetch and int(getattr(enrichment, "queued_harvest_worker_count", 0) or 0) > 0:
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail=(
                    "Profile enrichment queued background Harvest profile prefetch work; "
                    "resume after worker recovery completes remote dataset fetch."
                ),
                payload={
                    "candidate_count": len(candidates),
                    "artifact_paths": enrichment.artifact_paths,
                    "queued_harvest_worker_count": int(getattr(enrichment, "queued_harvest_worker_count", 0) or 0),
                    "stop_reason": str(getattr(enrichment, "stop_reason", "") or ""),
                },
            )
        candidates = enrichment.candidates
        evidence.extend(enrichment.evidence)

        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        logger.write_json(
            candidate_doc_path,
            {
                "snapshot": source_snapshots.get("roster_snapshot") or source_snapshots.get("search_seed_snapshot") or {},
                "acquisition_sources": source_snapshots,
                "candidates": [candidate.to_record() for candidate in candidates],
                "evidence": [item.to_record() for item in evidence],
                "enrichment_mode": "roster_plus_multisource",
                "acquisition_canonicalization": canonicalization_summary,
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
                "acquisition_canonicalization": canonicalization_summary,
                "queued_harvest_worker_count": int(getattr(enrichment, "queued_harvest_worker_count", 0) or 0),
                "queued_exploration_count": int(getattr(enrichment, "queued_exploration_count", 0) or 0),
                "stop_reason": str(getattr(enrichment, "stop_reason", "") or ""),
            },
            state_updates={
                "candidates": candidates,
                "evidence": evidence,
                "candidate_doc_path": candidate_doc_path,
            },
        )

    def _should_run_default_former_search_seed(self, task: AcquisitionTask, job_request: JobRequest) -> bool:
        if str(task.metadata.get("strategy_type") or "") != "full_company_roster":
            return False
        if "run_former_search_seed" in dict(job_request.execution_preferences or {}):
            return bool(job_request.execution_preferences.get("run_former_search_seed"))
        if not bool(task.metadata.get("include_former_search_seed")):
            return False
        cost_policy = _effective_cost_policy(task.metadata.get("cost_policy"), job_request)
        if str(cost_policy.get("provider_people_search_mode") or "").strip().lower() == "disabled":
            return False
        return True

    def _acquire_default_former_search_seed(
        self,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest,
        identity: CompanyIdentity,
    ) -> AcquisitionExecution:
        filter_hints = _build_former_filter_hints(
            identity=identity,
            base_filter_hints=dict(task.metadata.get("filter_hints") or {}),
        )
        cost_policy = _effective_cost_policy(task.metadata.get("cost_policy"), job_request)
        former_cost_policy = {
            **cost_policy,
            "provider_people_search_mode": "fallback_only",
            "provider_people_search_min_expected_results": int(
                task.metadata.get("former_provider_people_search_min_expected_results") or 50
            ),
        }
        former_search_seed_queries: list[str] = []
        if bool(former_cost_policy.get("large_org_keyword_probe_mode")):
            former_search_seed_queries = [
                str(item).strip()
                for item in list(task.metadata.get("search_seed_queries") or [])
                if str(item).strip()
            ]
            former_search_seed_queries = list(dict.fromkeys(former_search_seed_queries))
        former_task = AcquisitionTask(
            task_id=f"{task.task_id}-former-search-seed",
            task_type="acquire_search_seed_pool",
            title="Acquire former-member search seeds",
            description="Run a provider former-member recall pass after the current roster baseline is available.",
            source_hint="Harvest profile search (past company filter)",
            status="ready",
            blocking=False,
            metadata={
                "strategy_type": "former_employee_search",
                "employment_statuses": ["former"],
                "search_seed_queries": former_search_seed_queries,
                "search_query_bundles": [],
                "filter_hints": filter_hints,
                "cost_policy": former_cost_policy,
            },
        )
        return self._acquire_search_seed_pool(former_task, state, job_request)

    def _discover_company_identity_candidates(self, target_company: str) -> list[dict[str, Any]]:
        observed: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for query in _company_identity_search_queries(target_company):
            try:
                response = self.search_provider.search(query, max_results=5)
            except SearchProviderError:
                continue
            except Exception:
                continue
            for result in response.results:
                candidate = _company_identity_candidate_from_search_result(result.to_record())
                if candidate is None:
                    continue
                key = (
                    str(candidate.get("label") or "").strip().lower(),
                    str(candidate.get("linkedin_slug") or "").strip().lower(),
                )
                if key in seen:
                    continue
                seen.add(key)
                observed.append(candidate)
        return observed

    def _resolve_adaptive_company_employee_shards(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        policy: dict[str, Any],
    ) -> dict[str, Any]:
        snapshot_root = Path(snapshot_dir).resolve()
        normalized_policy = normalize_company_employee_shard_policy(policy)
        if not normalized_policy:
            return {
                "status": "blocked",
                "reason": "invalid_shard_policy",
                "detail": "Adaptive shard policy is missing or invalid.",
                "shards": [],
            }

        logger = AssetLogger(snapshot_root)
        plan = plan_company_employee_shards_from_policy(
            normalized_policy,
            probe_fn=lambda company_filters, context: self.harvest_company_connector.probe_company_roster_query(
                identity,
                snapshot_root,
                asset_logger=logger,
                company_filters=company_filters,
                probe_id=str(context.get("probe_id") or ""),
                title=str(context.get("title") or ""),
                max_pages=int(normalized_policy.get("probe_max_pages") or 1),
                page_limit=int(normalized_policy.get("probe_page_limit") or 25),
            ),
        )
        harvest_dir = snapshot_root / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        plan_path = harvest_dir / "adaptive_shard_plan.json"
        logger.write_json(
            plan_path,
            plan,
            asset_type="harvest_company_employees_adaptive_shard_plan",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        planned = dict(plan)
        planned["plan_path"] = str(plan_path)
        if str(planned.get("status") or "") == "planned" and not str(planned.get("detail") or "").strip():
            planned["detail"] = (
                f"Adaptive Harvest shard planner produced {len(list(planned.get('shards') or []))} shard(s) "
                f"for {identity.canonical_name}."
            )
        elif str(planned.get("status") or "") != "planned" and not str(planned.get("detail") or "").strip():
            planned["detail"] = "Adaptive Harvest shard planning did not produce an executable shard set."
        return planned

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
        allow_shared_provider_cache: bool,
        company_filters: dict[str, Any] | None = None,
        worker_key_suffix: str = "",
        span_name_suffix: str = "",
        root_snapshot_dir: Path | None = None,
    ) -> dict[str, Any]:
        if self.worker_runtime is None or not job_id:
            return {"worker_status": "skipped"}

        normalized_company_filters = dict(company_filters or {})
        effective_root_snapshot_dir = Path(root_snapshot_dir) if root_snapshot_dir is not None else snapshot_dir
        worker_handle = self.worker_runtime.begin_worker(
            job_id=job_id,
            request=JobRequest.from_payload(request_payload),
            plan_payload=plan_payload,
            runtime_mode=runtime_mode,
            lane_id="acquisition_specialist",
            worker_key=f"harvest_company_employees::{identity.company_key}{worker_key_suffix}",
            stage="acquiring",
            span_name=f"harvest_company_employees:{identity.canonical_name}{span_name_suffix}",
            budget_payload={"max_pages": max_pages, "page_limit": page_limit},
            input_payload={
                "company_identity": identity.to_record(),
                "company_filters": normalized_company_filters,
            },
            metadata={
                "recovery_kind": "harvest_company_employees",
                "identity": identity.to_record(),
                "snapshot_dir": str(snapshot_dir),
                "root_snapshot_dir": str(effective_root_snapshot_dir),
                "max_pages": max_pages,
                "page_limit": page_limit,
                "company_filters": normalized_company_filters,
                "worker_key_suffix": str(worker_key_suffix or ""),
                "span_name_suffix": str(span_name_suffix or ""),
                "request_payload": request_payload,
                "plan_payload": plan_payload,
                "runtime_mode": runtime_mode,
                "allow_shared_provider_cache": allow_shared_provider_cache,
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
            company_filters=normalized_company_filters,
            checkpoint=checkpoint,
            allow_shared_provider_cache=allow_shared_provider_cache,
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
            "company_filters": normalized_company_filters,
            "snapshot_dir": str(snapshot_dir),
            "root_snapshot_dir": str(effective_root_snapshot_dir),
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

    def _execute_segmented_harvest_company_roster_workers(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        shards: list[dict[str, Any]],
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool,
    ) -> dict[str, Any]:
        shard_root = snapshot_dir / "harvest_company_employees" / "shards"
        shard_root.mkdir(parents=True, exist_ok=True)

        shard_specs: list[dict[str, Any]] = []
        for index, shard in enumerate(shards, start=1):
            shard_id = _normalize_shard_id(str(shard.get("shard_id") or "shard"))
            shard_title = str(shard.get("title") or "").strip()
            shard_snapshot_dir = shard_root / shard_id
            shard_snapshot_dir.mkdir(parents=True, exist_ok=True)
            shard_specs.append(
                {
                    "index": index,
                    "shard": dict(shard),
                    "shard_id": shard_id,
                    "shard_title": shard_title,
                    "shard_snapshot_dir": shard_snapshot_dir,
                }
            )

        def _run_shard(spec: dict[str, Any]) -> dict[str, Any]:
            shard_payload = dict(spec.get("shard") or {})
            worker_result = self._execute_harvest_company_roster_worker(
                identity=identity,
                snapshot_dir=Path(spec["shard_snapshot_dir"]),
                max_pages=int(shard_payload.get("max_pages") or 1),
                page_limit=int(shard_payload.get("page_limit") or 25),
                job_id=job_id,
                request_payload=request_payload,
                plan_payload=plan_payload,
                runtime_mode=runtime_mode,
                allow_shared_provider_cache=allow_shared_provider_cache,
                company_filters=dict(shard_payload.get("company_filters") or {}),
                worker_key_suffix=f"::{spec['shard_id']}",
                span_name_suffix=f":{spec['shard_id']}",
                root_snapshot_dir=snapshot_dir,
            )
            summary = {
                **dict(worker_result.get("summary") or {}),
                "shard_id": spec["shard_id"],
                "title": spec["shard_title"],
                "strategy_id": str(shard_payload.get("strategy_id") or "").strip(),
                "scope_note": str(shard_payload.get("scope_note") or "").strip(),
                "company_filters": dict(shard_payload.get("company_filters") or {}),
                "snapshot_dir": str(spec["shard_snapshot_dir"]),
                "root_snapshot_dir": str(snapshot_dir),
            }
            return {
                "index": int(spec.get("index") or 0),
                "worker_status": str(worker_result.get("worker_status") or ""),
                "summary": summary,
            }

        shard_results_by_index: dict[int, dict[str, Any]] = {}
        if shard_specs:
            parallel_workers = max(1, min(len(shard_specs), 8))
            if parallel_workers == 1:
                for spec in shard_specs:
                    result = _run_shard(spec)
                    shard_results_by_index[int(result.get("index") or 0)] = result
            else:
                with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                    future_map = {
                        executor.submit(_run_shard, spec): int(spec.get("index") or 0)
                        for spec in shard_specs
                    }
                    for future in as_completed(future_map):
                        result = dict(future.result() or {})
                        index = int(result.get("index") or future_map.get(future) or 0)
                        shard_results_by_index[index] = result

        queued_summaries: list[dict[str, Any]] = []
        completed_summaries: list[dict[str, Any]] = []
        for spec in sorted(shard_specs, key=lambda item: int(item.get("index") or 0)):
            result = dict(shard_results_by_index.get(int(spec.get("index") or 0)) or {})
            summary = dict(result.get("summary") or {})
            if str(result.get("worker_status") or "") == "queued":
                queued_summaries.append(summary)
            else:
                completed_summaries.append(summary)
        return {
            "queued_count": len(queued_summaries),
            "completed_count": len(completed_summaries),
            "queued_summaries": queued_summaries,
            "completed_summaries": completed_summaries,
            "shard_count": len(shards),
        }

    def _fetch_segmented_harvest_company_roster(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        shards: list[dict[str, Any]],
        allow_shared_provider_cache: bool,
    ) -> CompanyRosterSnapshot:
        logger = AssetLogger(snapshot_dir)
        harvest_dir = snapshot_dir / "harvest_company_employees"
        shard_root = harvest_dir / "shards"
        shard_root.mkdir(parents=True, exist_ok=True)

        merged_entries: list[dict[str, Any]] = []
        visible_entries: list[dict[str, Any]] = []
        headless_entries: list[dict[str, Any]] = []
        page_summaries: list[dict[str, Any]] = []
        shard_summaries: list[dict[str, Any]] = []
        errors: list[str] = []
        seen_keys: set[str] = set()

        for shard in shards:
            shard_id = str(shard.get("shard_id") or "shard").strip() or "shard"
            shard_snapshot_dir = shard_root / shard_id
            shard_snapshot_dir.mkdir(parents=True, exist_ok=True)
            shard_snapshot = self.harvest_company_connector.fetch_company_roster(
                identity,
                shard_snapshot_dir,
                asset_logger=logger,
                max_pages=int(shard.get("max_pages") or 1),
                page_limit=int(shard.get("page_limit") or 25),
                company_filters=dict(shard.get("company_filters") or {}),
                allow_shared_provider_cache=allow_shared_provider_cache,
            )
            unique_count = 0
            duplicate_count = 0
            for entry in shard_snapshot.raw_entries:
                normalized_entry = dict(entry)
                normalized_entry["source_shard_id"] = shard_id
                normalized_entry["source_shard_title"] = str(shard.get("title") or "").strip()
                member_key = _roster_entry_key(normalized_entry)
                if member_key in seen_keys:
                    duplicate_count += 1
                    continue
                seen_keys.add(member_key)
                unique_count += 1
                merged_entries.append(normalized_entry)
                if bool(normalized_entry.get("is_headless")):
                    headless_entries.append(normalized_entry)
                else:
                    visible_entries.append(normalized_entry)
            for page_summary in shard_snapshot.page_summaries:
                page_summaries.append(
                    {
                        **dict(page_summary),
                        "shard_id": shard_id,
                        "shard_title": str(shard.get("title") or "").strip(),
                    }
                )
            errors.extend(f"{shard_id}: {error}" for error in shard_snapshot.errors)
            shard_summaries.append(
                {
                    "shard_id": shard_id,
                    "title": str(shard.get("title") or "").strip(),
                    "strategy_id": str(shard.get("strategy_id") or "").strip(),
                    "scope_note": str(shard.get("scope_note") or "").strip(),
                    "company_filters": dict(shard.get("company_filters") or {}),
                    "requested_max_pages": int(shard.get("max_pages") or 1),
                    "requested_page_limit": int(shard.get("page_limit") or 25),
                    "returned_entry_count": len(shard_snapshot.raw_entries),
                    "returned_visible_entry_count": len(shard_snapshot.visible_entries),
                    "returned_headless_entry_count": len(shard_snapshot.headless_entries),
                    "unique_entry_count": unique_count,
                    "duplicate_entry_count": duplicate_count,
                    "stop_reason": shard_snapshot.stop_reason,
                    "summary_path": str(shard_snapshot.summary_path),
                }
            )

        merged_path = harvest_dir / "harvest_company_employees_merged.json"
        visible_path = harvest_dir / "harvest_company_employees_visible.json"
        headless_path = harvest_dir / "harvest_company_employees_headless.json"
        summary_path = harvest_dir / "harvest_company_employees_summary.json"
        raw_manifest_path = harvest_dir / "harvest_company_employees_raw.json"

        logger.write_json(
            raw_manifest_path,
            {
                "segmented": True,
                "strategy_id": str(shards[0].get("strategy_id") or "").strip() if shards else "",
                "company_identity": identity.to_record(),
                "shards": shard_summaries,
            },
            asset_type="harvest_company_employees_payload",
            source_kind="harvest_company_employees",
            is_raw_asset=True,
            model_safe=False,
        )
        logger.write_json(
            merged_path,
            merged_entries,
            asset_type="company_roster_merged",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            visible_path,
            visible_entries,
            asset_type="company_roster_visible",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            headless_path,
            headless_entries,
            asset_type="company_roster_headless",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            summary_path,
            {
                "company_identity": identity.to_record(),
                "segmented": True,
                "strategy_id": str(shards[0].get("strategy_id") or "").strip() if shards else "",
                "raw_entry_count": len(merged_entries),
                "visible_entry_count": len(visible_entries),
                "headless_entry_count": len(headless_entries),
                "page_summaries": page_summaries,
                "shard_summaries": shard_summaries,
                "accounts_used": ["harvest_company_employees"],
                "errors": errors,
                "stop_reason": "completed_segmented",
            },
            asset_type="company_roster_summary",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        return CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=merged_entries,
            visible_entries=visible_entries,
            headless_entries=headless_entries,
            page_summaries=page_summaries,
            accounts_used=["harvest_company_employees"],
            errors=errors,
            stop_reason="completed_segmented",
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=summary_path,
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

    def _normalize_snapshot(
        self,
        task: AcquisitionTask,
        state: dict[str, Any],
        job_request: JobRequest | None = None,
    ) -> AcquisitionExecution:
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
        cost_policy = _effective_cost_policy(task.metadata.get("cost_policy"), job_request)
        inheritance_summary: dict[str, Any] = {}
        canonicalization_summary: dict[str, Any] = {}
        if str(normalization_scope.get("mode") or "") != "category":
            allow_historical_profile_inheritance = bool(cost_policy.get("allow_historical_profile_inheritance", True))
            if allow_historical_profile_inheritance:
                candidates, evidence, inheritance_summary = _inherit_historical_profile_captures(
                    self.store,
                    identity,
                    snapshot_dir,
                    candidates,
                    evidence,
                )
            else:
                inheritance_summary = _profile_inheritance_summary()
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

        summary_paths = list(company_dir.glob("*/linkedin_company_people_summary.json"))
        summary_paths.extend(company_dir.glob("*/harvest_company_employees/harvest_company_employees_summary.json"))
        summary_paths = sorted(summary_paths, key=lambda item: item.stat().st_mtime, reverse=True)
        for summary_path in summary_paths:
            is_harvest_snapshot = summary_path.parent.name == "harvest_company_employees"
            snapshot_dir = summary_path.parent.parent if is_harvest_snapshot else summary_path.parent
            if exclude_snapshot_dir is not None and snapshot_dir == exclude_snapshot_dir:
                continue
            try:
                summary = json.loads(summary_path.read_text())
                if is_harvest_snapshot:
                    roster_dir = snapshot_dir / "harvest_company_employees"
                    visible_path = roster_dir / "harvest_company_employees_visible.json"
                    merged_path = roster_dir / "harvest_company_employees_merged.json"
                    headless_path = roster_dir / "harvest_company_employees_headless.json"
                else:
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

    def _materialize_reused_roster_snapshot(
        self,
        identity: CompanyIdentity,
        source_snapshot: CompanyRosterSnapshot,
        target_snapshot_dir: Path,
    ) -> CompanyRosterSnapshot:
        logger = AssetLogger(target_snapshot_dir)
        try:
            source_summary = json.loads(source_snapshot.summary_path.read_text())
        except (OSError, json.JSONDecodeError):
            source_summary = {}
        is_harvest_snapshot = source_snapshot.summary_path.parent.name == "harvest_company_employees"
        if is_harvest_snapshot:
            roster_dir = target_snapshot_dir / "harvest_company_employees"
            merged_path = roster_dir / "harvest_company_employees_merged.json"
            visible_path = roster_dir / "harvest_company_employees_visible.json"
            headless_path = roster_dir / "harvest_company_employees_headless.json"
            summary_path = roster_dir / "harvest_company_employees_summary.json"
        else:
            roster_dir = target_snapshot_dir
            merged_path = roster_dir / "linkedin_company_people_all.json"
            visible_path = roster_dir / "linkedin_company_people_visible.json"
            headless_path = roster_dir / "linkedin_company_people_headless.json"
            summary_path = roster_dir / "linkedin_company_people_summary.json"
        roster_dir.mkdir(parents=True, exist_ok=True)
        logger.write_json(
            merged_path,
            source_snapshot.raw_entries,
            asset_type="company_roster_merged",
            source_kind="reused_cached_roster",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            visible_path,
            source_snapshot.visible_entries,
            asset_type="company_roster_visible",
            source_kind="reused_cached_roster",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            headless_path,
            source_snapshot.headless_entries,
            asset_type="company_roster_headless",
            source_kind="reused_cached_roster",
            is_raw_asset=False,
            model_safe=True,
        )
        summary_payload = {
            **dict(source_summary),
            "company_identity": identity.to_record(),
            "raw_entry_count": len(source_snapshot.raw_entries),
            "visible_entry_count": len(source_snapshot.visible_entries),
            "headless_entry_count": len(source_snapshot.headless_entries),
            "page_summaries": source_snapshot.page_summaries,
            "accounts_used": list(source_snapshot.accounts_used or []),
            "errors": list(source_snapshot.errors or []),
            "stop_reason": "reused_cached_roster",
            "reused_from_snapshot_id": source_snapshot.snapshot_id,
            "reused_from_snapshot_dir": str(source_snapshot.snapshot_dir),
            "reuse_source_summary_path": str(source_snapshot.summary_path),
        }
        logger.write_json(
            summary_path,
            summary_payload,
            asset_type="company_roster_summary",
            source_kind="reused_cached_roster",
            is_raw_asset=False,
            model_safe=True,
        )
        return CompanyRosterSnapshot(
            snapshot_id=target_snapshot_dir.name,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=target_snapshot_dir,
            raw_entries=list(source_snapshot.raw_entries),
            visible_entries=list(source_snapshot.visible_entries),
            headless_entries=list(source_snapshot.headless_entries),
            page_summaries=list(source_snapshot.page_summaries),
            accounts_used=list(source_snapshot.accounts_used),
            errors=list(source_snapshot.errors),
            stop_reason="reused_cached_roster",
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=summary_path,
        )


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


def _build_former_filter_hints(
    *,
    identity: CompanyIdentity,
    base_filter_hints: dict[str, Any] | None = None,
) -> dict[str, list[str]]:
    base = {
        str(key): [str(item).strip() for item in list(values or []) if str(item).strip()]
        for key, values in dict(base_filter_hints or {}).items()
    }
    company_reference = str(identity.linkedin_company_url or identity.canonical_name or identity.requested_name).strip()
    former_filter_hints: dict[str, list[str]] = {}
    company_candidates: list[str] = []
    for key in ["past_companies", "current_companies"]:
        for item in list(base.get(key) or []):
            normalized = str(item).strip()
            if normalized and normalized not in company_candidates:
                company_candidates.append(normalized)
    if not company_candidates and company_reference:
        company_candidates = [company_reference]
    if company_candidates:
        former_filter_hints["past_companies"] = company_candidates
    for key in [
        "scope_keywords",
        "job_titles",
        "keywords",
        "locations",
        "exclude_locations",
        "function_ids",
        "exclude_function_ids",
    ]:
        values = list(base.get(key) or [])
        if values:
            former_filter_hints[key] = values
    return former_filter_hints


def _normalize_company_employee_shards(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(value, start=1):
        if not isinstance(item, dict):
            continue
        shard_id = _normalize_shard_id(str(item.get("shard_id") or f"shard_{index}"))
        if not shard_id or shard_id in seen_ids:
            continue
        seen_ids.add(shard_id)
        company_filters = dict(item.get("company_filters") or {})
        normalized_filters: dict[str, list[str] | str] = {}
        for key in [
            "companies",
            "locations",
            "exclude_locations",
            "function_ids",
            "exclude_function_ids",
            "job_titles",
            "exclude_job_titles",
            "seniority_level_ids",
            "exclude_seniority_level_ids",
            "schools",
        ]:
            raw_values = company_filters.get(key)
            if isinstance(raw_values, (list, tuple, set)):
                values = [str(entry).strip() for entry in raw_values if str(entry).strip()]
            else:
                text = str(raw_values or "").strip()
                values = [text] if text else []
            if values:
                normalized_filters[key] = list(dict.fromkeys(values))
        search_query = str(company_filters.get("search_query") or "").strip()
        if search_query:
            normalized_filters["search_query"] = search_query
        try:
            max_pages = max(1, int(item.get("max_pages") or 1))
        except (TypeError, ValueError):
            max_pages = 1
        try:
            page_limit = max(1, int(item.get("page_limit") or 25))
        except (TypeError, ValueError):
            page_limit = 25
        normalized.append(
            {
                "strategy_id": str(item.get("strategy_id") or "").strip(),
                "shard_id": shard_id,
                "title": str(item.get("title") or shard_id).strip() or shard_id,
                "scope_note": str(item.get("scope_note") or "").strip(),
                "max_pages": max_pages,
                "page_limit": page_limit,
                "company_filters": normalized_filters,
            }
        )
    return normalized


def _normalize_shard_id(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return cleaned or "shard"


def _roster_entry_key(entry: dict[str, Any]) -> str:
    linkedin_url = str(entry.get("linkedin_url") or entry.get("profile_url") or "").strip().lower()
    if linkedin_url:
        return linkedin_url
    member_key = str(entry.get("member_key") or entry.get("member_id") or "").strip().lower()
    if member_key:
        return member_key
    return "|".join(
        [
            str(entry.get("full_name") or "").strip().lower(),
            str(entry.get("headline") or "").strip().lower(),
            str(entry.get("location") or "").strip().lower(),
        ]
    )


def _company_identity_search_queries(target_company: str) -> list[str]:
    requested = " ".join(str(target_company or "").split()).strip()
    if not requested:
        return []
    queries = [
        f'site:linkedin.com/company "{requested}" LinkedIn',
        f'"{requested}" "LinkedIn" company',
    ]
    if "&" in requested:
        ampersand_expanded = " ".join(requested.replace("&", " and ").split()).strip()
        compact = re.sub(r"[^a-z0-9]+", "", ampersand_expanded.lower())
        if ampersand_expanded and ampersand_expanded.lower() != requested.lower():
            queries.append(f'site:linkedin.com/company "{ampersand_expanded}" LinkedIn')
        if compact and compact not in {re.sub(r"[^a-z0-9]+", "", requested.lower()), ""}:
            queries.append(f'site:linkedin.com/company "{compact}" LinkedIn')
    deduped: list[str] = []
    seen: set[str] = set()
    for item in queries:
        normalized = " ".join(item.split()).strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


def _company_identity_candidate_from_search_result(result: dict[str, Any]) -> dict[str, Any] | None:
    url = str(result.get("url") or "").strip()
    match = re.search(r"linkedin\.com/company/([^/?#]+)", url, flags=re.IGNORECASE)
    if not match:
        return None
    linkedin_slug = str(match.group(1) or "").strip()
    if not linkedin_slug:
        return None
    title = " ".join(str(result.get("title") or "").split()).strip()
    label = _clean_company_result_title(title)
    if not label:
        return None
    return {
        "label": label,
        "linkedin_slug": linkedin_slug,
        "linkedin_company_url": f"https://www.linkedin.com/company/{linkedin_slug}/",
        "domain_hint": _extract_domain_hint(str(result.get("snippet") or "").strip()),
    }


def _clean_company_result_title(title: str) -> str:
    candidate = str(title or "").strip()
    for token in [" | LinkedIn", " - LinkedIn", "| LinkedIn", "- LinkedIn"]:
        if token in candidate:
            candidate = candidate.split(token, 1)[0].strip()
            break
    candidate = re.sub(r"^\s*LinkedIn:\s*", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"^\s*Overview of\s+", "", candidate, flags=re.IGNORECASE)
    candidate = " ".join(candidate.split()).strip(" -|")
    if not candidate:
        return ""
    if re.search(r"\b(job|jobs|hiring|careers|employee|employees)\b", candidate, flags=re.IGNORECASE):
        return ""
    return candidate


def _extract_domain_hint(snippet: str) -> str:
    match = re.search(r"\b([a-z0-9][a-z0-9-]*(?:\.[a-z0-9-]+)+)\b", str(snippet or "").lower())
    if not match:
        return ""
    domain = str(match.group(1) or "").strip().rstrip(".,)")
    if domain.endswith("linkedin.com"):
        return ""
    return domain


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
