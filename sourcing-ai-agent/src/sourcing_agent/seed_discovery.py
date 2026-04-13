from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from html import unescape
import json
import os
from pathlib import Path
import re
from typing import Any
from urllib import error, parse, request

from .agent_runtime import AgentRuntimeCoordinator
from .asset_logger import AssetLogger
from .connectors import CompanyIdentity, RapidApiAccount, search_people_accounts
from .domain import Candidate, EvidenceRecord, JobRequest, format_display_name, make_evidence_id, normalize_name_token
from .harvest_connectors import HarvestProfileSearchConnector
from .model_provider import DeterministicModelClient, ModelClient
from .query_signal_knowledge import canonicalize_scope_signal_label
from .request_normalization import build_effective_request_payload, resolve_request_intent_view
from .search_provider import (
    BaseSearchProvider,
    DuckDuckGoHtmlSearchProvider,
    SearchResponse,
    parse_duckduckgo_html_results,
    search_response_from_record,
    search_response_to_record,
)
from .worker_daemon import AutonomousWorkerDaemon

def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _external_provider_mode() -> str:
    return str(os.getenv("SOURCING_EXTERNAL_PROVIDER_MODE") or "live").strip().lower() or "live"


_LANE_READY_POLL_MIN_INTERVAL_SECONDS = max(
    1,
    _env_int(
        "SEED_DISCOVERY_READY_POLL_MIN_INTERVAL_SECONDS",
        _env_int("WEB_SEARCH_READY_POLL_MIN_INTERVAL_SECONDS", 15),
    ),
)
_LANE_FETCH_MIN_INTERVAL_SECONDS = max(
    1,
    _env_int(
        "SEED_DISCOVERY_FETCH_MIN_INTERVAL_SECONDS",
        _env_int("WEB_SEARCH_FETCH_MIN_INTERVAL_SECONDS", 15),
    ),
)


@dataclass(slots=True)
class SearchSeedSnapshot:
    snapshot_id: str
    target_company: str
    company_identity: CompanyIdentity
    snapshot_dir: Path
    entries: list[dict[str, Any]]
    query_summaries: list[dict[str, Any]]
    accounts_used: list[str]
    errors: list[str]
    stop_reason: str
    summary_path: Path
    entries_path: Path | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "target_company": self.target_company,
            "company_identity": self.company_identity.to_record(),
            "snapshot_dir": str(self.snapshot_dir),
            "entry_count": len(self.entries),
            "query_count": len(self.query_summaries),
            "accounts_used": self.accounts_used,
            "errors": self.errors,
            "stop_reason": self.stop_reason,
            "summary_path": str(self.summary_path),
            "entries_path": str(self.entries_path) if isinstance(self.entries_path, Path) else "",
        }


class SearchSeedAcquirer:
    def __init__(
        self,
        accounts: list[RapidApiAccount],
        model_client: ModelClient | None = None,
        harvest_search_connector: HarvestProfileSearchConnector | None = None,
        search_provider: BaseSearchProvider | None = None,
    ) -> None:
        self.accounts = search_people_accounts(accounts)
        self._exhausted_account_ids: set[str] = set()
        self.model_client = model_client or DeterministicModelClient()
        self.harvest_search_connector = harvest_search_connector
        self.search_provider = search_provider or DuckDuckGoHtmlSearchProvider()

    def _rapidapi_people_search_enabled(self) -> bool:
        return _external_provider_mode() == "live" and bool(self.accounts)

    def _harvest_people_search_enabled(self) -> bool:
        return bool(self.harvest_search_connector and self.harvest_search_connector.settings.enabled)

    def refresh_background_search_workers(self, workers: list[dict[str, Any]]) -> dict[str, Any]:
        grouped_specs: dict[Path, list[dict[str, Any]]] = {}
        discovery_dirs: dict[Path, Path] = {}
        errors: list[str] = []
        worker_updates: dict[int, dict[str, Any]] = {}

        for worker in workers:
            lane_id = str(worker.get("lane_id") or "").strip()
            if lane_id not in {"search_planner", "public_media_specialist"}:
                continue
            metadata = dict(worker.get("metadata") or {})
            input_payload = dict(worker.get("input") or {})
            checkpoint = dict(worker.get("checkpoint") or {})
            worker_key = str(worker.get("worker_key") or "").strip()
            if not worker_key:
                continue

            discovery_dir_text = str(metadata.get("discovery_dir") or "").strip()
            snapshot_dir_text = str(metadata.get("snapshot_dir") or "").strip()
            if discovery_dir_text:
                discovery_dir = Path(discovery_dir_text).expanduser()
            elif snapshot_dir_text:
                discovery_dir = Path(snapshot_dir_text).expanduser() / "search_seed_discovery"
            else:
                continue
            snapshot_dir = (
                Path(snapshot_dir_text).expanduser()
                if snapshot_dir_text
                else discovery_dir.parent
            )
            if not discovery_dir.exists():
                continue

            grouped_specs.setdefault(discovery_dir, []).append(
                {
                    "index": int(metadata.get("index") or input_payload.get("index") or 0),
                    "query_spec": dict(input_payload.get("query_spec") or {}),
                    "lane_id": lane_id,
                    "worker_key": worker_key,
                    "prefetched_search_state": dict(checkpoint.get("search_state") or {}),
                    "prefetched_search_artifact_paths": dict(checkpoint.get("search_artifact_paths") or {}),
                    "prefetched_search_raw_path": str(checkpoint.get("raw_path") or ""),
                    "prefetched_search_manifest_path": str(checkpoint.get("search_manifest_path") or ""),
                    "prefetched_search_manifest_key": str(checkpoint.get("search_manifest_key") or worker_key),
                    "worker_id": int(worker.get("worker_id") or 0),
                }
            )
            discovery_dirs[discovery_dir] = snapshot_dir

        for discovery_dir, pending_specs in grouped_specs.items():
            logger = AssetLogger(discovery_dirs[discovery_dir])
            errors.extend(
                _prepare_batched_search_seed_queries(
                    search_provider=self.search_provider,
                    logger=logger,
                    discovery_dir=discovery_dir,
                    pending_specs=pending_specs,
                    result_limit=10,
                )
            )
            manifest_path = discovery_dir / "web_search_batch_manifest.json"
            manifest_entries = _load_search_batch_manifest_entries(manifest_path)
            for spec in pending_specs:
                task_key = str(spec.get("worker_key") or "").strip()
                entry = dict(manifest_entries.get(task_key) or {})
                if not entry:
                    continue
                worker_id = int(spec.get("worker_id") or 0)
                if worker_id <= 0:
                    continue
                worker_updates[worker_id] = {
                    "search_state": dict(entry.get("search_state") or {}),
                    "search_artifact_paths": {
                        str(key): str(value)
                        for key, value in dict(entry.get("artifact_paths") or {}).items()
                        if str(key).strip() and str(value).strip()
                    },
                    "raw_path": str(entry.get("raw_path") or ""),
                    "search_manifest_path": str(manifest_path),
                    "search_manifest_key": task_key,
                }

        return {
            "errors": errors,
            "worker_updates": worker_updates,
        }

    def discover(
        self,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        *,
        asset_logger: AssetLogger | None = None,
        search_seed_queries: list[str],
        query_bundles: list[dict[str, Any]] | None = None,
        filter_hints: dict[str, list[str]],
        cost_policy: dict[str, Any],
        employment_status: str,
        worker_runtime: AgentRuntimeCoordinator | None = None,
        job_id: str = "",
        request_payload: dict[str, Any] | None = None,
        plan_payload: dict[str, Any] | None = None,
        runtime_mode: str = "workflow",
        intent_view: dict[str, Any] | None = None,
        delta_execution_plan: dict[str, Any] | None = None,
        lane_context: dict[str, Any] | None = None,
    ) -> SearchSeedSnapshot:
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        resolved_intent_view = dict(
            intent_view
            or resolve_request_intent_view(request_payload or {})
            or {}
        )
        effective_request_payload = build_effective_request_payload(
            request_payload or {"target_company": identity.canonical_name},
            intent_view=resolved_intent_view,
        )
        resolved_filter_hints = _resolve_discovery_filter_hints(
            filter_hints=filter_hints,
            intent_view=resolved_intent_view,
        )
        effective_filter_hints = _normalize_harvest_company_filters(identity, resolved_filter_hints)
        resolved_search_seed_queries = _resolve_discovery_search_seed_queries(
            search_seed_queries=search_seed_queries,
            query_bundles=query_bundles or [],
            intent_view=resolved_intent_view,
        )
        effective_search_seed_queries = _resolve_effective_search_seed_queries(
            search_seed_queries=resolved_search_seed_queries,
            delta_execution_plan=delta_execution_plan,
        )
        resolved_query_bundles = _resolve_discovery_query_bundles(
            query_bundles=query_bundles or [],
            intent_view=resolved_intent_view,
        )
        effective_query_bundles = _filter_query_bundles_for_delta(
            resolved_query_bundles,
            allowed_queries=effective_search_seed_queries,
        )
        provider_people_search_mode = str(cost_policy.get("provider_people_search_mode") or "fallback_only").strip().lower()
        provider_search_only = provider_people_search_mode in {"primary_only", "provider_only", "harvest_only"}
        provider_search_primary = provider_people_search_mode in {"primary", "always", "primary_only", "provider_only", "harvest_only"}

        entries: list[dict[str, Any]] = []
        query_summaries: list[dict[str, Any]] = []
        accounts_used: list[str] = []
        errors: list[str] = []
        stop_reason = "completed"

        compiled_queries = _compile_query_specs(effective_search_seed_queries, effective_query_bundles)
        web_result_target = int(cost_policy.get("provider_people_search_min_expected_results", 10) or 10)
        parallel_limit = max(1, min(int(cost_policy.get("parallel_search_workers", 3) or 3), len(compiled_queries) or 1))
        result_limit = max(1, min(int(cost_policy.get("public_media_results_per_query", 10) or 10), 25))
        worker_results: list[dict[str, Any]] = []
        pending_specs = [] if provider_search_only else [
            {
                "index": index,
                "query_spec": query_spec,
                "lane_id": (
                    "public_media_specialist"
                    if query_spec["source_family"] in {"public_interviews", "publication_and_blog"}
                    else "search_planner"
                ),
                "worker_key": f"{query_spec['bundle_id']}::{index:02d}",
                "label": str(query_spec.get("query") or ""),
            }
            for index, query_spec in enumerate(compiled_queries, start=1)
            if query_spec["execution_mode"] != "paid_fallback"
        ]
        daemon_summary = {
            "results": [],
            "retried": [],
            "backlog": [],
            "lane_budget_used": {},
            "lane_budget_caps": {},
            "cycles": 0,
            "daemon_events": [],
        }
        if worker_runtime is not None and job_id and pending_specs:
            errors.extend(
                _prepare_batched_search_seed_queries(
                    search_provider=self.search_provider,
                    logger=logger,
                    discovery_dir=discovery_dir,
                    pending_specs=pending_specs,
                    result_limit=result_limit,
                )
            )
        if pending_specs:
            if worker_runtime is not None and job_id:
                daemon = AutonomousWorkerDaemon.from_plan(
                    plan_payload={"acquisition_strategy": {"cost_policy": cost_policy}},
                    existing_workers=worker_runtime.list_workers(job_id=job_id),
                    total_limit=parallel_limit,
                )
                daemon_summary = daemon.run(
                    pending_specs,
                    executor=lambda spec: self._execute_query_spec(
                        index=int(spec["index"]),
                        query_spec=dict(spec["query_spec"]),
                        identity=identity,
                        discovery_dir=discovery_dir,
                        logger=logger,
                        employment_status=employment_status,
                        worker_runtime=worker_runtime,
                        job_id=job_id,
                        request_payload=effective_request_payload,
                        plan_payload=plan_payload or {},
                        runtime_mode=runtime_mode,
                        result_limit=result_limit,
                        prefetched_search_state=dict(spec.get("prefetched_search_state") or {}),
                        prefetched_search_artifact_paths=dict(spec.get("prefetched_search_artifact_paths") or {}),
                        prefetched_search_raw_path=str(spec.get("prefetched_search_raw_path") or ""),
                        prefetched_search_manifest_path=str(spec.get("prefetched_search_manifest_path") or ""),
                        prefetched_search_manifest_key=str(spec.get("prefetched_search_manifest_key") or ""),
                    ),
                )
                worker_results.extend(list(daemon_summary.get("results") or []))
            else:
                with ThreadPoolExecutor(max_workers=max(1, min(parallel_limit, len(pending_specs)))) as executor:
                    futures = [
                        executor.submit(
                            self._execute_query_spec,
                            index=int(spec["index"]),
                            query_spec=dict(spec["query_spec"]),
                            identity=identity,
                            discovery_dir=discovery_dir,
                            logger=logger,
                            employment_status=employment_status,
                            worker_runtime=worker_runtime,
                            job_id=job_id,
                            request_payload=effective_request_payload,
                            plan_payload=plan_payload or {},
                            runtime_mode=runtime_mode,
                            result_limit=result_limit,
                            prefetched_search_state=dict(spec.get("prefetched_search_state") or {}),
                            prefetched_search_artifact_paths=dict(spec.get("prefetched_search_artifact_paths") or {}),
                            prefetched_search_raw_path=str(spec.get("prefetched_search_raw_path") or ""),
                            prefetched_search_manifest_path=str(spec.get("prefetched_search_manifest_path") or ""),
                            prefetched_search_manifest_key=str(spec.get("prefetched_search_manifest_key") or ""),
                        )
                        for spec in pending_specs
                    ]
                    for future in futures:
                        worker_results.append(future.result())

        worker_results.sort(key=lambda item: int(item.get("index") or 0))
        for result in worker_results:
            entries.extend(list(result.get("entries") or []))
            query_summaries.append(dict(result.get("summary") or {}))
            errors.extend(list(result.get("errors") or []))

        queued_query_count = len([item for item in query_summaries if str(item.get("status") or "") == "queued"])
        queued_background_search = queued_query_count > 0
        if queued_background_search:
            stop_reason = "queued_background_search"

        entries = _dedupe_seed_entries(entries)
        paid_queries = _resolve_provider_people_search_queries(
            identity=identity,
            filter_hints=effective_filter_hints,
            search_seed_queries=[item["query"] for item in compiled_queries if item["execution_mode"] == "paid_fallback"]
            or list(effective_search_seed_queries),
        )
        provider_available = bool(
            self._rapidapi_people_search_enabled() or self._harvest_people_search_enabled()
        )
        should_run_provider_people_search = False
        if provider_search_primary:
            should_run_provider_people_search = True
        elif provider_available and len(entries) < web_result_target and provider_people_search_mode == "fallback_only":
            should_run_provider_people_search = True
        if should_run_provider_people_search:
            needed = max(web_result_target - len(entries), 0)
            provider_limit = max(needed, web_result_target)
            provider_entries, provider_summaries, provider_errors, provider_accounts = self._provider_people_search_fallback(
                identity=identity,
                discovery_dir=discovery_dir,
                asset_logger=logger,
                search_seed_queries=paid_queries,
                filter_hints=effective_filter_hints,
                employment_status=employment_status,
                limit=provider_limit,
                cost_policy=cost_policy,
            )
            entries.extend(provider_entries)
            entries = _dedupe_seed_entries(entries)
            query_summaries.extend(provider_summaries)
            errors.extend(provider_errors)
            accounts_used.extend(provider_accounts)
            if provider_entries and not queued_background_search:
                stop_reason = "provider_people_search_primary" if provider_search_primary else "provider_people_search_fallback"

        entries_path = discovery_dir / "entries.json"
        logger.write_json(
            entries_path,
            entries,
            asset_type="search_seed_entries",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        summary_path = discovery_dir / "summary.json"
        summary_payload = {
            "snapshot_id": snapshot_dir.name,
            "target_company": identity.canonical_name,
            "company_identity": identity.to_record(),
            "entry_count": len(entries),
            "search_seed_queries": effective_search_seed_queries,
            "requested_search_seed_queries": resolved_search_seed_queries,
            "effective_query_bundles": effective_query_bundles,
            "requested_filter_hints": resolved_filter_hints,
            "effective_filter_hints": effective_filter_hints,
            "query_summaries": query_summaries,
            "accounts_used": accounts_used,
            "errors": errors,
            "stop_reason": stop_reason,
            "queued_query_count": queued_query_count,
            "cost_policy": cost_policy,
            "intent_view": resolved_intent_view,
            "delta_execution_plan": dict(delta_execution_plan or {}),
            "lane_context": dict(lane_context or {}),
            "worker_daemon": {
                "cycles": int(daemon_summary.get("cycles") or 0),
                "retried": list(daemon_summary.get("retried") or []),
                "backlog_count": len(list(daemon_summary.get("backlog") or [])),
                "lane_budget_used": dict(daemon_summary.get("lane_budget_used") or {}),
                "lane_budget_caps": dict(daemon_summary.get("lane_budget_caps") or {}),
            },
        }
        logger.write_json(
            summary_path,
            summary_payload,
            asset_type="search_seed_summary",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        return SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=entries,
            query_summaries=query_summaries,
            accounts_used=accounts_used,
            errors=errors,
            stop_reason=stop_reason,
            summary_path=summary_path,
            entries_path=entries_path,
        )

    def _execute_query_spec(
        self,
        *,
        index: int,
        query_spec: dict[str, str],
        identity: CompanyIdentity,
        discovery_dir: Path,
        logger: AssetLogger,
        employment_status: str,
        worker_runtime: AgentRuntimeCoordinator | None,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        result_limit: int,
        prefetched_search_state: dict[str, Any] | None = None,
        prefetched_search_artifact_paths: dict[str, str] | None = None,
        prefetched_search_raw_path: str = "",
        prefetched_search_manifest_path: str = "",
        prefetched_search_manifest_key: str = "",
    ) -> dict[str, Any]:
        query_text = str(query_spec["query"] or "").strip()
        effective_request_payload = build_effective_request_payload(request_payload or {}) if request_payload else {}
        raw_search_path = discovery_dir / f"web_query_{index:02d}.html"
        lane_id = "public_media_specialist" if query_spec["source_family"] in {"public_interviews", "publication_and_blog"} else "search_planner"
        worker_handle = None
        checkpoint: dict[str, Any] = {}
        interrupted = False
        if worker_runtime is not None and job_id:
            worker_handle = worker_runtime.begin_worker(
                job_id=job_id,
                request=JobRequest.from_payload(effective_request_payload),
                plan_payload=plan_payload,
                runtime_mode=runtime_mode,
                lane_id=lane_id,
                worker_key=f"{query_spec['bundle_id']}::{index:02d}",
                stage="acquiring",
                span_name=f"search_bundle:{query_spec['bundle_id']}",
                budget_payload={"max_results": 10, "execution_mode": query_spec["execution_mode"], "query": query_text},
                input_payload={"query_spec": query_spec, "query": query_text, "index": index},
                metadata={
                    "index": index,
                    "identity": identity.to_record(),
                    "snapshot_dir": str(discovery_dir.parent),
                    "discovery_dir": str(discovery_dir),
                    "employment_status": employment_status,
                    "request_payload": effective_request_payload,
                    "plan_payload": plan_payload,
                    "runtime_mode": runtime_mode,
                    "result_limit": result_limit,
                },
                handoff_from_lane="search_planner" if lane_id == "public_media_specialist" else "triage_planner",
            )
            existing = worker_runtime.get_worker(worker_handle.worker_id) or {}
            checkpoint = dict(existing.get("checkpoint") or {})
            output_payload = dict(existing.get("output") or {})
            if str(existing.get("status") or "") == "completed" and output_payload:
                summary = dict(output_payload.get("summary") or {})
                entries = list(output_payload.get("entries") or [])
                errors = list(output_payload.get("errors") or [])
                worker_runtime.complete_worker(
                    worker_handle,
                    status="completed",
                    checkpoint_payload=checkpoint,
                    output_payload=output_payload,
                    handoff_to_lane="acquisition_specialist" if lane_id == "search_planner" else "exploration_specialist",
                )
                return {
                    "index": index,
                    "entries": entries,
                    "summary": summary,
                    "errors": errors,
                    "worker_status": "completed",
                    "daemon_action": "reused_output",
                }
            checkpoint, prefetched_applied = _merge_prefetched_search_checkpoint(
                checkpoint=checkpoint,
                prefetched_search_state=dict(prefetched_search_state or {}),
                prefetched_search_artifact_paths=dict(prefetched_search_artifact_paths or {}),
                prefetched_search_raw_path=str(prefetched_search_raw_path or "").strip(),
                manifest_path=Path(prefetched_search_manifest_path).expanduser()
                if str(prefetched_search_manifest_path or "").strip()
                else None,
                manifest_key=str(prefetched_search_manifest_key or "").strip(),
            )
            if prefetched_applied:
                worker_runtime.checkpoint_worker(
                    worker_handle,
                    checkpoint_payload=checkpoint,
                    output_payload={
                        "search_state": dict(checkpoint.get("search_state") or {}),
                        "search_artifact_paths": dict(checkpoint.get("search_artifact_paths") or {}),
                    },
                )
        try:
            if worker_handle and worker_runtime.should_interrupt_worker(worker_handle):
                summary = _interrupted_query_summary(index, query_spec, query_text, raw_search_path)
                worker_runtime.complete_worker(
                    worker_handle,
                    status="interrupted",
                    checkpoint_payload=checkpoint,
                    output_payload={"summary": summary, "entries": [], "errors": []},
                    handoff_to_lane="review_specialist",
                )
                return {"index": index, "entries": [], "summary": summary, "errors": [], "worker_status": "interrupted"}

            cached_search_path = Path(str(checkpoint.get("raw_path") or "")) if checkpoint.get("raw_path") else None
            search_response: SearchResponse | None = None
            manifest_path = (
                Path(str(checkpoint.get("search_manifest_path") or "")).expanduser()
                if str(checkpoint.get("search_manifest_path") or "").strip()
                else None
            )
            manifest_key = str(checkpoint.get("search_manifest_key") or "").strip()
            if cached_search_path and cached_search_path.exists():
                parsed_results, search_response = _load_cached_search_response(cached_search_path, query_text)
                raw_search_path = cached_search_path
                manifest_search_state = _mark_search_state_as_worker_fetched(
                    dict(checkpoint.get("search_state") or {}),
                    query_text=query_text,
                )
                if manifest_search_state:
                    checkpoint["search_state"] = manifest_search_state
                    _update_search_batch_manifest_entry(
                        logger=logger,
                        manifest_path=manifest_path,
                        task_key=manifest_key,
                        search_state=manifest_search_state,
                        artifact_paths=dict(checkpoint.get("search_artifact_paths") or {}),
                        raw_path=str(cached_search_path),
                    )
            else:
                if worker_handle:
                    search_execution = self.search_provider.execute_with_checkpoint(
                        query_text,
                        max_results=result_limit,
                        checkpoint=dict(checkpoint.get("search_state") or {}),
                    )
                    artifact_paths = dict(checkpoint.get("search_artifact_paths") or {})
                    for artifact in list(search_execution.artifacts or []):
                        artifact_path = _write_search_execution_artifact(
                            logger=logger,
                            artifact=artifact,
                            default_path=raw_search_path,
                            asset_type="web_search_queue_payload",
                            source_kind="search_seed_discovery",
                            metadata={"query": query_text, "provider_name": search_execution.provider_name},
                        )
                        artifact_paths[str(artifact.label)] = str(artifact_path)
                    checkpoint = {
                        **checkpoint,
                        "provider_name": search_execution.provider_name,
                        "search_state": dict(search_execution.checkpoint or {}),
                        "search_artifact_paths": artifact_paths,
                    }
                    if search_execution.pending:
                        summary = {
                            "query": query_text,
                            "bundle_id": query_spec["bundle_id"],
                            "source_family": query_spec["source_family"],
                            "execution_mode": query_spec["execution_mode"],
                            "mode": "web_search",
                            "status": "queued",
                            "raw_path": "",
                            "provider_name": search_execution.provider_name,
                            "result_count": 0,
                            "linkedin_result_count": 0,
                            "seed_entry_count": 0,
                            "search_state": dict(search_execution.checkpoint or {}),
                            "search_artifact_paths": artifact_paths,
                            "message": str(search_execution.message or ""),
                        }
                        worker_runtime.complete_worker(
                            worker_handle,
                            status="queued",
                            checkpoint_payload={**checkpoint, "stage": "waiting_remote_search"},
                            output_payload={"summary": summary, "entries": [], "errors": [], "search_state": dict(search_execution.checkpoint or {})},
                        )
                        return {
                            "index": index,
                            "entries": [],
                            "summary": summary,
                            "errors": [],
                            "worker_status": "queued",
                        }
                    search_response = search_execution.response
                    if search_response is None:
                        raise RuntimeError("Search provider returned no response.")
                    raw_search_path = _write_search_response_raw_asset(
                        logger=logger,
                        response=search_response,
                        default_path=raw_search_path,
                        asset_type="web_search_payload",
                        source_kind="search_seed_discovery",
                        metadata={"query": query_text, "provider_name": search_response.provider_name},
                    )
                    manifest_search_state = _mark_search_state_as_worker_fetched(
                        dict(checkpoint.get("search_state") or {}),
                        fallback_state=dict(search_execution.checkpoint or {}),
                        query_text=query_text,
                    )
                    checkpoint = {
                        **checkpoint,
                        "stage": "fetched_search_results",
                        "raw_path": str(raw_search_path),
                        "provider_name": search_response.provider_name,
                        "search_state": manifest_search_state,
                    }
                    if manifest_search_state:
                        _update_search_batch_manifest_entry(
                            logger=logger,
                            manifest_path=manifest_path,
                            task_key=manifest_key,
                            search_state=manifest_search_state,
                            artifact_paths=dict(checkpoint.get("search_artifact_paths") or {}),
                            raw_path=str(raw_search_path),
                        )
                    worker_runtime.checkpoint_worker(
                        worker_handle,
                        checkpoint_payload=checkpoint,
                        output_payload={"raw_path": str(raw_search_path), "provider_name": search_response.provider_name},
                    )
                else:
                    search_response = self.search_provider.search(query_text, max_results=result_limit)
                    raw_search_path = _write_search_response_raw_asset(
                        logger=logger,
                        response=search_response,
                        default_path=raw_search_path,
                        asset_type="web_search_payload",
                        source_kind="search_seed_discovery",
                        metadata={"query": query_text, "provider_name": search_response.provider_name},
                    )
                    checkpoint = {
                        **checkpoint,
                        "stage": "fetched_search_results",
                        "raw_path": str(raw_search_path),
                        "provider_name": search_response.provider_name,
                    }
                    if worker_handle:
                        worker_runtime.checkpoint_worker(
                            worker_handle,
                            checkpoint_payload=checkpoint,
                            output_payload={"raw_path": str(raw_search_path), "provider_name": search_response.provider_name},
                        )

            if search_response is None:
                raise RuntimeError("Search provider returned no response.")

            parsed_results = [item.to_record() for item in search_response.results]
            public_media_analysis = {}
            if query_spec["source_family"] in {"public_interviews", "publication_and_blog"}:
                public_media_analysis, checkpoint = self._analyze_public_media_results(
                    index=index,
                    query_spec=query_spec,
                    identity=identity,
                    discovery_dir=discovery_dir,
                    logger=logger,
                    parsed_results=parsed_results,
                    checkpoint=checkpoint,
                    worker_runtime=worker_runtime,
                    worker_handle=worker_handle,
                    result_limit=result_limit,
                )
                checkpoint = {**checkpoint, "stage": "public_media_analyzed"}
                if worker_handle and worker_runtime and worker_runtime.should_interrupt_worker(worker_handle):
                    interrupted = True

            linkedin_results = [item for item in parsed_results if "linkedin.com/in/" in item.get("url", "")]
            query_entries = []
            for item in linkedin_results[:result_limit]:
                seed_entry = _seed_entry_from_web_result(
                    item,
                    identity,
                    query_text,
                    employment_status,
                    source_family=query_spec["source_family"],
                )
                if seed_entry is not None:
                    query_entries.append(seed_entry)
            if query_spec["source_family"] in {"public_interviews", "publication_and_blog"}:
                for item in parsed_results[:result_limit]:
                    analysis = public_media_analysis.get(str(item.get("url") or "").strip(), {})
                    if str(analysis.get("confidence_label") or "low") == "low":
                        continue
                    for seed_entry in _lead_entries_from_public_result(
                        item,
                        identity,
                        query_text,
                        employment_status,
                        analysis=analysis,
                        source_family=query_spec["source_family"],
                    ):
                        query_entries.append(seed_entry)

            summary = {
                "query": query_text,
                "bundle_id": query_spec["bundle_id"],
                "source_family": query_spec["source_family"],
                "execution_mode": query_spec["execution_mode"],
                "mode": "web_search",
                "status": "interrupted" if interrupted else "completed",
                "raw_path": str(raw_search_path),
                "provider_name": search_response.provider_name,
                "result_count": len(parsed_results),
                "linkedin_result_count": len(linkedin_results),
                "seed_entry_count": len(query_entries),
            }
            if worker_handle:
                worker_runtime.complete_worker(
                    worker_handle,
                    status="interrupted" if interrupted else "completed",
                    checkpoint_payload={
                        **checkpoint,
                        "stage": "interrupted" if interrupted else "completed",
                        "result_count": len(parsed_results),
                    },
                    output_payload={"summary": summary, "entries": query_entries, "errors": [], "seed_entry_count": len(query_entries)},
                    handoff_to_lane=(
                        "review_specialist"
                        if interrupted
                        else "acquisition_specialist"
                        if lane_id == "search_planner"
                        else "exploration_specialist"
                    ),
                )
            return {
                "index": index,
                "entries": query_entries,
                "summary": summary,
                "errors": [],
                "worker_status": "interrupted" if interrupted else "completed",
            }
        except Exception as exc:
            error_text = f"web_search:{query_text}:{str(exc)[:120]}"
            summary = {
                "query": query_text,
                "bundle_id": query_spec["bundle_id"],
                "source_family": query_spec["source_family"],
                "execution_mode": query_spec["execution_mode"],
                "mode": "web_search",
                "status": "failed",
                "raw_path": str(raw_search_path),
                "result_count": 0,
                "linkedin_result_count": 0,
                "seed_entry_count": 0,
                "error": str(exc),
            }
            if worker_handle:
                worker_runtime.complete_worker(
                    worker_handle,
                    status="failed",
                    checkpoint_payload={**checkpoint, "stage": "failed"},
                    output_payload={"error": str(exc), "summary": summary, "entries": [], "errors": [error_text]},
                    handoff_to_lane="review_specialist",
                )
            return {"index": index, "entries": [], "summary": summary, "errors": [error_text], "worker_status": "failed"}

    def _analyze_public_media_results(
        self,
        *,
        index: int,
        query_spec: dict[str, str],
        identity: CompanyIdentity,
        discovery_dir: Path,
        logger: AssetLogger,
        parsed_results: list[dict[str, str]],
        checkpoint: dict[str, Any],
        worker_runtime: AgentRuntimeCoordinator | None,
        worker_handle,
        result_limit: int,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        public_media_payloads = [
            {
                "title": str(item.get("title") or "").strip(),
                "snippet": str(item.get("snippet") or "").strip(),
                "url": str(item.get("url") or "").strip(),
                "source_family": query_spec["source_family"],
            }
            for item in parsed_results[:result_limit]
        ]
        results_path = discovery_dir / f"public_media_results_{index:02d}.json"
        analysis_path = discovery_dir / f"public_media_analysis_{index:02d}.json"
        logger.write_json(
            results_path,
            {"query": query_spec["query"], "records": public_media_payloads},
            asset_type="public_media_results",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        analyzed_records = []
        analysis_map: dict[str, dict[str, Any]] = {
            str(key): dict(value)
            for key, value in dict(checkpoint.get("public_media_analysis") or {}).items()
            if str(key).strip()
        }
        completed_urls = set(checkpoint.get("completed_urls") or [])
        for record in public_media_payloads:
            if worker_handle and worker_runtime and worker_runtime.should_interrupt_worker(worker_handle):
                break
            if record["url"] in completed_urls and record["url"] in analysis_map:
                analyzed_records.append({**record, "analysis": analysis_map[record["url"]]})
                continue
            analysis = self.model_client.analyze_page_asset(
                {
                    "target_company": identity.canonical_name,
                    "title": record["title"],
                    "description": record["snippet"],
                    "url": record["url"],
                    "text_excerpt": f"{record['title']} {record['snippet']}".strip(),
                    "extracted_links": {"linkedin_urls": [], "personal_urls": [], "x_urls": [], "github_urls": [], "resume_urls": []},
                }
            )
            analyzed_records.append({**record, "analysis": analysis})
            analysis_map[record["url"]] = analysis
            completed_urls.add(record["url"])
            if worker_handle and worker_runtime:
                checkpoint = {
                    **checkpoint,
                    "stage": "public_media_analysis",
                    "completed_urls": sorted(completed_urls),
                    "public_media_analysis": analysis_map,
                }
                worker_runtime.checkpoint_worker(
                    worker_handle,
                    checkpoint_payload=checkpoint,
                    output_payload={"analyzed_count": len(analyzed_records)},
                )
        logger.write_json(
            analysis_path,
            {"query": query_spec["query"], "records": analyzed_records},
            asset_type="public_media_analysis",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        checkpoint = {
            **checkpoint,
            "stage": "public_media_analyzed",
            "completed_urls": sorted(completed_urls),
            "public_media_analysis": analysis_map,
        }
        return analysis_map, checkpoint

    def _provider_people_search_fallback(
        self,
        *,
        identity: CompanyIdentity,
        discovery_dir: Path,
        asset_logger: AssetLogger,
        search_seed_queries: list[str],
        filter_hints: dict[str, list[str]],
        employment_status: str,
        limit: int,
        cost_policy: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], list[str]]:
        entries: list[dict[str, Any]] = []
        query_summaries: list[dict[str, Any]] = []
        errors: list[str] = []
        accounts_used: list[str] = []
        page_count = int(cost_policy.get("provider_people_search_pages", 2 if employment_status == "former" else 1) or 1)
        query_strategy = str(cost_policy.get("provider_people_search_query_strategy") or "all_queries_union").strip().lower()
        stop_after_first_hit = query_strategy in {"first_hit", "first_nonempty", "first_non_empty", "first_match"}
        try:
            max_query_count = int(cost_policy.get("provider_people_search_max_queries") or 0)
        except (TypeError, ValueError):
            max_query_count = 0
        if max_query_count < 0:
            max_query_count = 0
        paid_queries = _resolve_provider_people_search_queries(
            identity=identity,
            filter_hints=filter_hints,
            search_seed_queries=search_seed_queries,
        )
        if (
            employment_status == "former"
            and list(filter_hints.get("past_companies") or [])
            and not bool(cost_policy.get("former_keyword_queries_only"))
        ):
            paid_queries = ["", *paid_queries]
        deduped_queries: list[str] = []
        seen_queries: set[str] = set()
        for item in paid_queries:
            key = str(item or "")
            signature = _search_query_signature(key) or "__empty__"
            if signature in seen_queries:
                continue
            seen_queries.add(signature)
            deduped_queries.append(key)
        if max_query_count > 0:
            deduped_queries = deduped_queries[:max_query_count]

        precomputed_harvest_plans: dict[str, dict[str, Any]] = {}
        if (
            deduped_queries
            and self._harvest_people_search_enabled()
            and not stop_after_first_hit
            and bool(cost_policy.get("provider_people_search_overlap_pruning", True))
        ):
            try:
                overlap_threshold = float(cost_policy.get("provider_people_search_overlap_threshold") or 0.9)
            except (TypeError, ValueError):
                overlap_threshold = 0.9
            overlap_threshold = max(0.0, min(1.0, overlap_threshold))
            try:
                min_probe_rows = int(cost_policy.get("provider_people_search_overlap_min_probe_rows") or 10)
            except (TypeError, ValueError):
                min_probe_rows = 10
            min_probe_rows = max(1, min_probe_rows)
            kept_queries: list[str] = []
            kept_probe_sets: list[set[str]] = []
            for query_text in deduped_queries:
                summary_query = query_text or "__past_company_only__"
                effective_query_text = _normalize_harvest_query_text(
                    query_text=query_text,
                    filter_hints=filter_hints,
                    identity=identity,
                )
                probe_plan = self._resolve_harvest_search_execution_plan(
                    query_text=effective_query_text,
                    filter_hints=filter_hints,
                    employment_status=employment_status,
                    discovery_dir=discovery_dir,
                    asset_logger=asset_logger,
                    requested_limit=max(26, int(limit or 25)),
                    requested_pages=max(2, int(page_count or 1)),
                    allow_shared_provider_cache=bool(cost_policy.get("allow_shared_provider_cache", True)),
                )
                probe_plan = dict(probe_plan or {})
                probe_plan["effective_query_text"] = effective_query_text
                precomputed_harvest_plans[query_text] = probe_plan
                probe_profiles = _extract_harvest_profile_urls_from_result(dict(probe_plan.get("initial_result") or {}))
                max_overlap = 0.0
                overlap_with_query = ""
                if probe_profiles and len(probe_profiles) >= min_probe_rows:
                    for kept_query, kept_profiles in zip(kept_queries, kept_probe_sets):
                        if not kept_profiles:
                            continue
                        overlap = _jaccard_overlap_ratio(probe_profiles, kept_profiles)
                        if overlap > max_overlap:
                            max_overlap = overlap
                            overlap_with_query = kept_query or "__past_company_only__"
                if max_overlap >= overlap_threshold and overlap_with_query:
                    query_summaries.append(
                        {
                            "query": summary_query,
                            "effective_query_text": effective_query_text,
                            "mode": "harvest_profile_search",
                            "status": "skipped_high_overlap",
                            "overlap_ratio": round(max_overlap, 4),
                            "overlap_with_query": overlap_with_query,
                            "probe_profile_count": len(probe_profiles),
                            "probe": {
                                key: value
                                for key, value in probe_plan.items()
                                if key != "initial_result"
                            },
                        }
                    )
                    continue
                kept_queries.append(query_text)
                kept_probe_sets.append(probe_profiles)
            deduped_queries = kept_queries

        def _run_harvest_query(
            summary_query: str,
            query_text: str,
            *,
            precomputed_plan: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            if not self._harvest_people_search_enabled():
                return {"query_entries": [], "query_summary": None, "account_used": ""}
            harvest_plan = dict(precomputed_plan or {})
            effective_query_text = str(harvest_plan.get("effective_query_text") or "").strip()
            if not effective_query_text:
                effective_query_text = _normalize_harvest_query_text(
                    query_text=query_text,
                    filter_hints=filter_hints,
                    identity=identity,
                )
            if not harvest_plan:
                harvest_plan = self._resolve_harvest_search_execution_plan(
                    query_text=effective_query_text,
                    filter_hints=filter_hints,
                    employment_status=employment_status,
                    discovery_dir=discovery_dir,
                    asset_logger=asset_logger,
                    requested_limit=limit,
                    requested_pages=page_count,
                    allow_shared_provider_cache=bool(cost_policy.get("allow_shared_provider_cache", True)),
                )
            harvest_result = harvest_plan.get("initial_result")
            if harvest_result is None:
                harvest_result = self.harvest_search_connector.search_profiles(
                    query_text=effective_query_text,
                    filter_hints=filter_hints,
                    employment_status=employment_status,
                    discovery_dir=discovery_dir,
                    asset_logger=asset_logger,
                    limit=int(harvest_plan.get("effective_limit") or limit),
                    pages=int(harvest_plan.get("effective_pages") or page_count),
                    allow_shared_provider_cache=bool(cost_policy.get("allow_shared_provider_cache", True)),
                    auto_probe=False,
                )
            if harvest_result is None:
                return {"query_entries": [], "query_summary": None, "account_used": ""}

            rows = list(harvest_result.get("rows") or [])
            effective_limit = max(1, int(harvest_plan.get("effective_limit") or limit))
            query_entries: list[dict[str, Any]] = []
            for row in rows[:effective_limit]:
                entry = {
                    "seed_key": _seed_key(str(row.get("full_name") or ""), str(row.get("profile_url") or summary_query)),
                    "full_name": str(row.get("full_name") or "").strip(),
                    "headline": str(row.get("headline") or "").strip(),
                    "location": str(row.get("location") or "").strip(),
                    "source_type": "harvest_profile_search",
                    "source_query": summary_query,
                    "profile_url": str(row.get("profile_url") or "").strip(),
                    "slug": str(row.get("username") or "").strip() or extract_linkedin_slug(str(row.get("profile_url") or "")),
                    "employment_status": employment_status,
                    "target_company": identity.canonical_name,
                    "metadata": {
                        "provider_account_id": "harvest_profile_search",
                        "current_company": str(row.get("current_company") or "").strip(),
                        "scope_keywords": list(filter_hints.get("scope_keywords") or []),
                    },
                }
                if entry["full_name"]:
                    query_entries.append(entry)
            query_summary = {
                "query": summary_query,
                "effective_query_text": effective_query_text,
                "mode": "harvest_profile_search",
                "raw_path": str(harvest_result.get("raw_path") or ""),
                "account_id": "harvest_profile_search",
                "requested_limit": max(1, int(limit or 25)),
                "requested_pages": max(1, int(page_count or 1)),
                "effective_limit": effective_limit,
                "effective_pages": max(1, int(harvest_plan.get("effective_pages") or page_count)),
                "pagination": dict(harvest_result.get("pagination") or {}),
                "probe": {
                    key: value
                    for key, value in dict(harvest_plan).items()
                    if key != "initial_result"
                },
                "seed_entry_count": len(query_entries),
            }
            return {
                "query_entries": query_entries,
                "query_summary": query_summary,
                "account_used": "harvest_profile_search" if query_entries else "",
            }

        parallel_harvest_results: dict[int, dict[str, Any]] = {}
        if (
            deduped_queries
            and not stop_after_first_hit
            and self._harvest_people_search_enabled()
        ):
            try:
                configured_parallel_workers = int(cost_policy.get("provider_people_search_parallel_queries") or 4)
            except (TypeError, ValueError):
                configured_parallel_workers = 4
            parallel_query_workers = max(
                1,
                min(
                    len(deduped_queries),
                    configured_parallel_workers,
                ),
            )
            with ThreadPoolExecutor(max_workers=parallel_query_workers) as executor:
                futures = [
                    executor.submit(
                        _run_harvest_query,
                        query_text or "__past_company_only__",
                        query_text,
                        precomputed_plan=dict(precomputed_harvest_plans.get(query_text) or {}),
                    )
                    for query_text in deduped_queries
                ]
                for index, future in enumerate(futures, start=1):
                    parallel_harvest_results[index] = dict(future.result() or {})

        for index, query_text in enumerate(deduped_queries, start=1):
            summary_query = query_text or "__past_company_only__"
            if self._harvest_people_search_enabled():
                harvest_payload = (
                    dict(parallel_harvest_results.get(index) or {})
                    if not stop_after_first_hit and parallel_harvest_results
                    else _run_harvest_query(
                        summary_query,
                        query_text,
                        precomputed_plan=dict(precomputed_harvest_plans.get(query_text) or {}),
                    )
                )
                query_entries = list(harvest_payload.get("query_entries") or [])
                query_summary = dict(harvest_payload.get("query_summary") or {})
                account_used = str(harvest_payload.get("account_used") or "").strip()
                if query_entries:
                    entries.extend(query_entries)
                if query_summary:
                    query_summaries.append(query_summary)
                if account_used and account_used not in accounts_used:
                    accounts_used.append(account_used)
                if query_entries and stop_after_first_hit:
                    break
            if not self._rapidapi_people_search_enabled():
                continue
            if not query_text:
                continue
            payload, account, provider_errors = self._search_people(query_text, limit=min(limit, 25))
            errors.extend(provider_errors)
            raw_path = discovery_dir / f"provider_query_{index:02d}.json"
            if payload is None or account is None:
                query_summaries.append(
                    {
                        "query": summary_query,
                        "mode": "provider_people_search",
                        "raw_path": str(raw_path),
                        "seed_entry_count": 0,
                    }
                )
                continue
            asset_logger.write_json(
                raw_path,
                payload,
                asset_type="provider_people_search_payload",
                source_kind="search_seed_discovery",
                is_raw_asset=True,
                model_safe=False,
                metadata={"query": query_text, "account_id": account.account_id},
            )
            if account.account_id not in accounts_used:
                accounts_used.append(account.account_id)
            rows = extract_people_search_rows(payload)
            query_entries: list[dict[str, Any]] = []
            for row in rows[:limit]:
                entry = {
                    "seed_key": _seed_key(str(row.get("full_name") or ""), str(row.get("profile_url") or row.get("urn") or query_text)),
                    "full_name": str(row.get("full_name") or "").strip(),
                    "headline": str(row.get("headline") or "").strip(),
                    "location": str(row.get("location") or "").strip(),
                    "source_type": "provider_people_search",
                    "source_query": query_text,
                    "profile_url": str(row.get("profile_url") or "").strip(),
                    "slug": str(row.get("username") or "").strip() or extract_linkedin_slug(str(row.get("profile_url") or "")),
                    "employment_status": employment_status,
                    "target_company": identity.canonical_name,
                    "metadata": {
                        "provider_account_id": account.account_id,
                        "urn": str(row.get("urn") or "").strip(),
                        "scope_keywords": list(filter_hints.get("scope_keywords") or []),
                    },
                }
                if entry["full_name"]:
                    query_entries.append(entry)
                    entries.append(entry)
            query_summaries.append(
                {
                    "query": summary_query,
                    "mode": "provider_people_search",
                    "raw_path": str(raw_path),
                    "account_id": account.account_id,
                    "seed_entry_count": len(query_entries),
                }
            )
            if query_entries and stop_after_first_hit:
                break
        return entries, query_summaries, errors, accounts_used

    def _resolve_harvest_search_execution_plan(
        self,
        *,
        query_text: str,
        filter_hints: dict[str, list[str]],
        employment_status: str,
        discovery_dir: Path,
        asset_logger: AssetLogger | None,
        requested_limit: int,
        requested_pages: int,
        allow_shared_provider_cache: bool,
    ) -> dict[str, Any]:
        plan = {
            "probe_performed": False,
            "probe_query_text": str(query_text or "").strip(),
            "probe_limit": 25,
            "probe_pages": 1,
            "requested_limit": max(1, int(requested_limit or 25)),
            "requested_pages": max(1, int(requested_pages or 1)),
            "effective_limit": max(1, int(requested_limit or 25)),
            "effective_pages": max(1, int(requested_pages or 1)),
            "provider_total_count": 0,
            "provider_total_pages": 0,
            "probe_returned_count": 0,
            "probe_raw_path": "",
            "initial_result": None,
        }
        if self.harvest_search_connector is None or not self.harvest_search_connector.settings.enabled:
            return plan
        company_scoped_search = bool(
            list(filter_hints.get("past_companies") or [])
            or list(filter_hints.get("current_companies") or [])
        )
        if not company_scoped_search:
            return plan
        requested_limit_value = int(plan["requested_limit"])
        requested_pages_value = int(plan["requested_pages"])
        former_past_company_scan = (
            str(employment_status or "").strip().lower() == "former"
            and bool(list(filter_hints.get("past_companies") or []))
            and not str(query_text or "").strip()
        )
        should_probe = requested_limit_value > 25 or requested_pages_value > 1 or former_past_company_scan
        if not should_probe:
            return plan
        probe_result = self.harvest_search_connector.search_profiles(
            query_text=query_text,
            filter_hints=filter_hints,
            employment_status=employment_status,
            discovery_dir=discovery_dir,
            asset_logger=asset_logger,
            limit=25,
            pages=1,
            allow_shared_provider_cache=allow_shared_provider_cache,
            auto_probe=False,
        )
        plan["probe_performed"] = True
        if probe_result is None:
            return plan
        pagination = dict(probe_result.get("pagination") or {})
        total_count = max(0, int(pagination.get("total_elements") or 0))
        total_pages = max(0, int(pagination.get("total_pages") or 0))
        probe_returned_count = max(0, int(pagination.get("returned_count") or len(list(probe_result.get("rows") or []))))
        plan["provider_total_count"] = total_count
        plan["provider_total_pages"] = total_pages
        plan["probe_returned_count"] = probe_returned_count
        plan["probe_raw_path"] = str(probe_result.get("raw_path") or "")
        if total_count > 0:
            effective_pages = max(1, total_pages or ((total_count + 24) // 25))
            plan["effective_limit"] = total_count
            plan["effective_pages"] = effective_pages
            if effective_pages == 1 and probe_returned_count >= total_count:
                plan["initial_result"] = probe_result
            return plan
        plan["initial_result"] = probe_result
        return plan

    def _search_people(self, query_text: str, *, limit: int) -> tuple[dict[str, Any] | None, RapidApiAccount | None, list[str]]:
        if not self._rapidapi_people_search_enabled():
            return None, None, []
        errors_seen: list[str] = []
        for account in self.accounts:
            if account.account_id in self._exhausted_account_ids:
                continue
            url = _build_people_search_url(account, query_text, limit=limit)
            headers = {"x-rapidapi-host": account.host, "x-rapidapi-key": account.api_key, "User-Agent": "Mozilla/5.0"}
            http_request = request.Request(url, headers=headers, method="GET")
            try:
                with request.urlopen(http_request, timeout=30) as response:
                    return json.loads(response.read().decode("utf-8")), account, errors_seen
            except error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="ignore")
                if exc.code == 429:
                    self._exhausted_account_ids.add(account.account_id)
                errors_seen.append(f"provider_people_search:{account.account_id}:{exc.code}:{detail[:120]}")
            except Exception as exc:
                errors_seen.append(f"provider_people_search:{account.account_id}:{str(exc)[:120]}")
        return None, None, errors_seen


def _prepare_batched_search_seed_queries(
    *,
    search_provider: BaseSearchProvider,
    logger: AssetLogger,
    discovery_dir: Path,
    pending_specs: list[dict[str, Any]],
    result_limit: int,
) -> list[str]:
    manifest_path = discovery_dir / "web_search_batch_manifest.json"
    manifest_entries = _load_search_batch_manifest_entries(manifest_path)
    pending_by_key = {
        str(spec.get("worker_key") or "").strip(): spec
        for spec in pending_specs
        if str(spec.get("worker_key") or "").strip()
    }
    for task_key, spec in pending_by_key.items():
        entry = dict(manifest_entries.get(task_key) or {})
        search_state = dict(entry.get("search_state") or {})
        if str(search_state.get("task_id") or "").strip():
            spec["prefetched_search_state"] = search_state
            spec["prefetched_search_artifact_paths"] = dict(entry.get("artifact_paths") or {})
            spec["prefetched_search_raw_path"] = str(entry.get("raw_path") or "")
            spec["prefetched_search_manifest_path"] = str(manifest_path)
            spec["prefetched_search_manifest_key"] = task_key

    unresolved_requests: list[dict[str, Any]] = []
    for task_key, spec in pending_by_key.items():
        if dict(spec.get("prefetched_search_state") or {}).get("task_id"):
            continue
        query_text = str(dict(spec.get("query_spec") or {}).get("query") or "").strip()
        if not query_text:
            continue
        unresolved_requests.append(
            {
                "task_key": task_key,
                "query_text": query_text,
                "max_results": result_limit,
            }
        )
    provider_name = str(
        dict(next(iter(manifest_entries.values()), {})).get("search_state", {}).get("provider_name")
        or getattr(search_provider, "provider_name", "")
        or ""
    ).strip()
    artifact_paths: dict[str, str] = {}
    batch_message = ""
    submitted_query_count = 0

    def _write_manifest_snapshot() -> None:
        if not manifest_entries and not artifact_paths:
            return
        root_artifact_paths = {
            str(key): str(value)
            for key, value in dict(artifact_paths).items()
            if str(key).strip() and str(value).strip()
        }
        for entry in manifest_entries.values():
            for key, value in dict(entry.get("artifact_paths") or {}).items():
                normalized_key = str(key or "").strip()
                normalized_value = str(value or "").strip()
                if normalized_key and normalized_value and normalized_key not in root_artifact_paths:
                    root_artifact_paths[normalized_key] = normalized_value
        logger.write_json(
            manifest_path,
            {
                "provider_name": provider_name,
                "submitted_query_count": submitted_query_count,
                "artifact_paths": root_artifact_paths,
                "entries": [manifest_entries[key] for key in sorted(manifest_entries)],
                "message": batch_message,
                "updated_at": _batch_lane_timestamp(),
            },
            asset_type="web_search_batch_manifest",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=False,
        )

    if unresolved_requests:
        submit_batch = getattr(search_provider, "submit_batch_queries", None)
        if not callable(submit_batch):
            return []

        try:
            batch_result = submit_batch(unresolved_requests)
        except Exception as exc:
            return [f"web_search_batch_submit:{str(exc)[:160]}"]
        if batch_result is None:
            return []

        provider_name = str(getattr(batch_result, "provider_name", "") or provider_name).strip()
        batch_message = str(getattr(batch_result, "message", "") or "")
        submitted_query_count = len(batch_result.tasks or [])
        artifact_paths = {}
        for artifact in list(batch_result.artifacts or []):
            artifact_label = str(getattr(artifact, "label", "artifact") or "artifact")
            default_path = discovery_dir / f"web_search_{artifact_label}.json"
            artifact_path = _write_search_execution_artifact(
                logger=logger,
                artifact=artifact,
                default_path=default_path,
                asset_type="web_search_batch_queue_payload",
                source_kind="search_seed_discovery",
                metadata={"provider_name": provider_name, "submitted_query_count": len(unresolved_requests)},
            )
            artifact_paths[artifact_label] = str(artifact_path)

        for task in list(batch_result.tasks or []):
            task_key = str(getattr(task, "task_key", "") or "").strip()
            if not task_key:
                continue
            entry = {
                "task_key": task_key,
                "query": str(getattr(task, "query_text", "") or "").strip(),
                "search_state": dict(getattr(task, "checkpoint", {}) or {}),
                "artifact_paths": {},
                "metadata": dict(getattr(task, "metadata", {}) or {}),
            }
            artifact_label = str(entry["metadata"].get("artifact_label") or "").strip()
            if artifact_label and artifact_label in artifact_paths:
                entry["artifact_paths"] = {artifact_label: artifact_paths[artifact_label]}
            manifest_entries[task_key] = entry
            spec = pending_by_key.get(task_key)
            if spec is not None:
                spec["prefetched_search_state"] = dict(entry["search_state"] or {})
                spec["prefetched_search_artifact_paths"] = dict(entry["artifact_paths"] or {})
                spec["prefetched_search_manifest_path"] = str(manifest_path)
                spec["prefetched_search_manifest_key"] = task_key
        _write_manifest_snapshot()

    ready_errors = _refresh_batched_search_seed_ready_cache(
        search_provider=search_provider,
        logger=logger,
        discovery_dir=discovery_dir,
        manifest_path=manifest_path,
        manifest_entries=manifest_entries,
        pending_by_key=pending_by_key,
    )
    fetch_errors = _fetch_batched_search_seed_ready_results(
        search_provider=search_provider,
        logger=logger,
        discovery_dir=discovery_dir,
        manifest_path=manifest_path,
        manifest_entries=manifest_entries,
        pending_by_key=pending_by_key,
    )

    if not manifest_entries and not artifact_paths:
        return [*ready_errors, *fetch_errors]
    _write_manifest_snapshot()
    return [*ready_errors, *fetch_errors]


def _refresh_batched_search_seed_ready_cache(
    *,
    search_provider: BaseSearchProvider,
    logger: AssetLogger,
    discovery_dir: Path,
    manifest_path: Path,
    manifest_entries: dict[str, dict[str, Any]],
    pending_by_key: dict[str, dict[str, Any]],
) -> list[str]:
    poll_ready = getattr(search_provider, "poll_ready_batch", None)
    if not callable(poll_ready):
        return []

    poll_specs: list[dict[str, Any]] = []
    attempted_at = _batch_lane_timestamp()
    for task_key, spec in pending_by_key.items():
        entry = dict(manifest_entries.get(task_key) or {})
        search_state = dict(entry.get("search_state") or {})
        task_id = str(search_state.get("task_id") or "").strip()
        if not task_id:
            continue
        if str(search_state.get("status") or "").strip() in {"completed", "fetched_cached", "ready_cached"}:
            continue
        if _timestamp_within_seconds(str(search_state.get("ready_attempted_at") or ""), _LANE_READY_POLL_MIN_INTERVAL_SECONDS):
            continue
        search_state["ready_attempted_at"] = attempted_at
        entry["search_state"] = search_state
        manifest_entries[task_key] = entry
        poll_specs.append(
            {
                "task_key": task_key,
                "query_text": str(entry.get("query") or dict(spec.get("query_spec") or {}).get("query") or "").strip(),
                "checkpoint": search_state,
            }
        )
    if not poll_specs:
        return []

    try:
        ready_result = poll_ready(poll_specs)
    except Exception as exc:
        return [f"web_search_ready_poll:{str(exc)[:160]}"]
    if ready_result is None:
        return []

    poll_token = _batch_lane_timestamp(compact=True)
    poll_artifact_paths: dict[str, str] = {}
    for artifact in list(ready_result.artifacts or []):
        artifact_label = str(getattr(artifact, "label", "artifact") or "artifact")
        artifact_key = f"{artifact_label}_{poll_token}"
        default_path = discovery_dir / f"web_search_{artifact_key}.json"
        artifact_path = _write_search_execution_artifact(
            logger=logger,
            artifact=artifact,
            default_path=default_path,
            asset_type="web_search_ready_poll_payload",
            source_kind="search_seed_discovery",
            metadata={"provider_name": ready_result.provider_name, "poll_token": poll_token},
        )
        poll_artifact_paths[artifact_key] = str(artifact_path)

    for task in list(ready_result.tasks or []):
        task_key = str(getattr(task, "task_key", "") or "").strip()
        if not task_key:
            continue
        entry = dict(manifest_entries.get(task_key) or {})
        search_state = dict(getattr(task, "checkpoint", {}) or {})
        search_state["ready_attempted_at"] = attempted_at
        search_state["ready_poll_token"] = poll_token
        search_state["ready_checked_at"] = _batch_lane_timestamp()
        search_state["ready_poll_source"] = "lane_batch"
        search_state["ready_poll_label"] = "tasks_ready_batch"
        entry["search_state"] = search_state
        entry_artifact_paths = {
            str(key): str(value)
            for key, value in dict(entry.get("artifact_paths") or {}).items()
            if str(key).strip() and str(value).strip()
        }
        entry_artifact_paths.update(poll_artifact_paths)
        entry["artifact_paths"] = entry_artifact_paths
        manifest_entries[task_key] = entry
        spec = pending_by_key.get(task_key)
        if spec is not None:
            spec["prefetched_search_state"] = search_state
            spec["prefetched_search_artifact_paths"] = dict(entry_artifact_paths)
            spec["prefetched_search_manifest_path"] = str(manifest_path)
            spec["prefetched_search_manifest_key"] = task_key
    return []


def _fetch_batched_search_seed_ready_results(
    *,
    search_provider: BaseSearchProvider,
    logger: AssetLogger,
    discovery_dir: Path,
    manifest_path: Path,
    manifest_entries: dict[str, dict[str, Any]],
    pending_by_key: dict[str, dict[str, Any]],
) -> list[str]:
    fetch_ready = getattr(search_provider, "fetch_ready_batch", None)
    if not callable(fetch_ready):
        return []

    fetch_specs: list[dict[str, Any]] = []
    attempted_at = _batch_lane_timestamp()
    for task_key, spec in pending_by_key.items():
        entry = dict(manifest_entries.get(task_key) or {})
        search_state = dict(entry.get("search_state") or {})
        if str(search_state.get("status") or "").strip() != "ready_cached":
            continue
        raw_path = str(entry.get("raw_path") or "").strip()
        if raw_path and Path(raw_path).exists():
            continue
        if _timestamp_within_seconds(str(search_state.get("fetch_attempted_at") or ""), _LANE_FETCH_MIN_INTERVAL_SECONDS):
            continue
        search_state["fetch_attempted_at"] = attempted_at
        search_state["lane_fetch_cooldown_seconds"] = _LANE_FETCH_MIN_INTERVAL_SECONDS
        entry["search_state"] = search_state
        manifest_entries[task_key] = entry
        fetch_specs.append(
            {
                "task_key": task_key,
                "query_text": str(entry.get("query") or dict(spec.get("query_spec") or {}).get("query") or "").strip(),
                "checkpoint": search_state,
            }
        )
    if not fetch_specs:
        return []

    try:
        fetch_result = fetch_ready(fetch_specs)
    except Exception as exc:
        return [f"web_search_task_get:{str(exc)[:160]}"]
    if fetch_result is None:
        return []

    fetch_token = _batch_lane_timestamp(compact=True)
    fetch_artifact_paths: dict[str, str] = {}
    for artifact in list(fetch_result.artifacts or []):
        artifact_label = str(getattr(artifact, "label", "artifact") or "artifact")
        artifact_key = f"{artifact_label}_{fetch_token}"
        default_path = discovery_dir / f"web_search_{artifact_key}.json"
        artifact_path = _write_search_execution_artifact(
            logger=logger,
            artifact=artifact,
            default_path=default_path,
            asset_type="web_search_task_get_payload",
            source_kind="search_seed_discovery",
            metadata={"provider_name": fetch_result.provider_name, "fetch_token": fetch_token},
        )
        fetch_artifact_paths[artifact_key] = str(artifact_path)

    for task in list(fetch_result.tasks or []):
        task_key = str(getattr(task, "task_key", "") or "").strip()
        if not task_key:
            continue
        spec = pending_by_key.get(task_key)
        entry = dict(manifest_entries.get(task_key) or {})
        response = getattr(task, "response", None)
        if response is None or spec is None:
            continue
        index = int(spec.get("index") or 0)
        default_path = discovery_dir / f"web_query_{index:02d}.html"
        raw_path = _write_search_response_raw_asset(
            logger=logger,
            response=response,
            default_path=default_path,
            asset_type="web_search_payload",
            source_kind="search_seed_discovery",
            metadata={"query": str(entry.get("query") or ""), "provider_name": response.provider_name},
        )
        search_state = dict(getattr(task, "checkpoint", {}) or {})
        search_state["fetch_attempted_at"] = attempted_at
        search_state["fetched_at"] = _batch_lane_timestamp()
        search_state["fetch_token"] = fetch_token
        search_state["lane_fetch_cooldown_seconds"] = _LANE_FETCH_MIN_INTERVAL_SECONDS
        entry["search_state"] = search_state
        entry["raw_path"] = str(raw_path)
        entry_artifact_paths = {
            str(key): str(value)
            for key, value in dict(entry.get("artifact_paths") or {}).items()
            if str(key).strip() and str(value).strip()
        }
        entry_artifact_paths.update(fetch_artifact_paths)
        entry["artifact_paths"] = entry_artifact_paths
        manifest_entries[task_key] = entry
        spec["prefetched_search_state"] = search_state
        spec["prefetched_search_artifact_paths"] = dict(entry_artifact_paths)
        spec["prefetched_search_raw_path"] = str(raw_path)
        spec["prefetched_search_manifest_path"] = str(manifest_path)
        spec["prefetched_search_manifest_key"] = task_key
    return []


def _load_search_batch_manifest_entries(manifest_path: Path) -> dict[str, dict[str, Any]]:
    if not manifest_path.exists():
        return {}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    entries: dict[str, dict[str, Any]] = {}
    for item in list(payload.get("entries") or []):
        task_key = str((item or {}).get("task_key") or "").strip()
        if task_key:
            entries[task_key] = dict(item or {})
    return entries


def _update_search_batch_manifest_entry(
    *,
    logger: AssetLogger,
    manifest_path: Path | None,
    task_key: str,
    search_state: dict[str, Any],
    artifact_paths: dict[str, str],
    raw_path: str,
) -> None:
    if manifest_path is None or not str(task_key or "").strip():
        return
    payload: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
    entries = _load_search_batch_manifest_entries(manifest_path)
    entry = dict(entries.get(task_key) or {})
    if not entry:
        return
    entry["search_state"] = dict(search_state or {})
    merged_artifact_paths = {
        str(key): str(value)
        for key, value in dict(entry.get("artifact_paths") or {}).items()
        if str(key).strip() and str(value).strip()
    }
    for key, value in dict(artifact_paths or {}).items():
        normalized_key = str(key or "").strip()
        normalized_value = str(value or "").strip()
        if normalized_key and normalized_value and normalized_key not in merged_artifact_paths:
            merged_artifact_paths[normalized_key] = normalized_value
    if merged_artifact_paths:
        entry["artifact_paths"] = merged_artifact_paths
    normalized_raw_path = str(raw_path or "").strip()
    if normalized_raw_path:
        entry["raw_path"] = normalized_raw_path
    entries[task_key] = entry

    root_artifact_paths = {
        str(key): str(value)
        for key, value in dict(payload.get("artifact_paths") or {}).items()
        if str(key).strip() and str(value).strip()
    }
    for key, value in merged_artifact_paths.items():
        if key not in root_artifact_paths:
            root_artifact_paths[key] = value
    logger.write_json(
        manifest_path,
        {
            "provider_name": str(payload.get("provider_name") or ""),
            "submitted_query_count": int(payload.get("submitted_query_count") or 0),
            "artifact_paths": root_artifact_paths,
            "entries": [entries[key] for key in sorted(entries)],
            "message": str(payload.get("message") or ""),
            "updated_at": _batch_lane_timestamp(),
        },
        asset_type="web_search_batch_manifest",
        source_kind="search_seed_discovery",
        is_raw_asset=False,
        model_safe=False,
    )


def _mark_search_state_as_worker_fetched(
    existing_state: dict[str, Any],
    *,
    fallback_state: dict[str, Any] | None = None,
    query_text: str,
) -> dict[str, Any]:
    normalized_existing = dict(existing_state or {})
    normalized_fallback = dict(fallback_state or {})
    task_id = str(normalized_existing.get("task_id") or normalized_fallback.get("task_id") or "").strip()
    if not task_id:
        return {}
    current_timestamp = _batch_lane_timestamp()
    fetch_token = str(normalized_existing.get("fetch_token") or normalized_fallback.get("fetch_token") or "").strip()
    if not fetch_token:
        fetch_token = f"worker_direct_{_batch_lane_timestamp(compact=True)}"
    return {
        **normalized_fallback,
        **normalized_existing,
        "task_id": task_id,
        "query_text": str(normalized_existing.get("query_text") or normalized_fallback.get("query_text") or query_text),
        "status": "fetched_cached",
        "fetch_attempted_at": str(
            normalized_existing.get("fetch_attempted_at")
            or normalized_fallback.get("fetch_attempted_at")
            or current_timestamp
        ),
        "fetched_at": str(
            normalized_existing.get("fetched_at")
            or normalized_fallback.get("fetched_at")
            or current_timestamp
        ),
        "fetch_token": fetch_token,
    }


def _merge_prefetched_search_checkpoint(
    *,
    checkpoint: dict[str, Any],
    prefetched_search_state: dict[str, Any],
    prefetched_search_artifact_paths: dict[str, str],
    prefetched_search_raw_path: str,
    manifest_path: Path | None,
    manifest_key: str,
) -> tuple[dict[str, Any], bool]:
    updated = dict(checkpoint or {})
    existing_state = dict(updated.get("search_state") or {})

    recovered_entry: dict[str, Any] = {}
    if not prefetched_search_state and manifest_path is not None and manifest_path.exists() and manifest_key:
        recovered_entry = dict(_load_search_batch_manifest_entries(manifest_path).get(manifest_key) or {})
        prefetched_search_state = dict(recovered_entry.get("search_state") or {})
        prefetched_search_artifact_paths = {
            str(key): str(value)
            for key, value in dict(recovered_entry.get("artifact_paths") or {}).items()
            if str(key).strip() and str(value).strip()
        }
        prefetched_search_raw_path = str(recovered_entry.get("raw_path") or prefetched_search_raw_path or "").strip()

    if not str(dict(prefetched_search_state or {}).get("task_id") or "").strip():
        return updated, False

    existing_task_id = str(existing_state.get("task_id") or "").strip()
    prefetched_task_id = str(dict(prefetched_search_state or {}).get("task_id") or "").strip()
    should_replace = False
    if not existing_task_id:
        should_replace = True
    elif existing_task_id == prefetched_task_id:
        existing_poll = str(existing_state.get("ready_poll_token") or "").strip()
        prefetched_poll = str(dict(prefetched_search_state or {}).get("ready_poll_token") or "").strip()
        existing_status = str(existing_state.get("status") or "").strip()
        prefetched_status = str(dict(prefetched_search_state or {}).get("status") or "").strip()
        existing_raw_path = str(updated.get("raw_path") or "").strip()
        prefetched_raw_path = str(prefetched_search_raw_path or "").strip()
        should_replace = (
            prefetched_poll != existing_poll
            or prefetched_status != existing_status
            or (prefetched_raw_path and prefetched_raw_path != existing_raw_path)
        )
    if not should_replace:
        return updated, False

    merged_artifact_paths = {
        str(key): str(value)
        for key, value in dict(updated.get("search_artifact_paths") or {}).items()
        if str(key).strip() and str(value).strip()
    }
    for key, value in dict(prefetched_search_artifact_paths or {}).items():
        normalized_key = str(key or "").strip()
        normalized_value = str(value or "").strip()
        if normalized_key and normalized_value and normalized_key not in merged_artifact_paths:
            merged_artifact_paths[normalized_key] = normalized_value
    updated["search_state"] = dict(prefetched_search_state or {})
    if merged_artifact_paths:
        updated["search_artifact_paths"] = merged_artifact_paths
    normalized_raw_path = str(prefetched_search_raw_path or "").strip()
    if normalized_raw_path:
        current_raw_path = str(updated.get("raw_path") or "").strip()
        if not current_raw_path or not Path(current_raw_path).exists():
            updated["raw_path"] = normalized_raw_path
    if manifest_path is not None and str(manifest_key or "").strip():
        updated["search_manifest_path"] = str(manifest_path)
        updated["search_manifest_key"] = str(manifest_key)
    return updated, True


def _batch_lane_timestamp(*, compact: bool = False) -> str:
    current = datetime.now(timezone.utc)
    if compact:
        return current.strftime("%Y%m%dT%H%M%SZ")
    return current.isoformat()


def _timestamp_within_seconds(value: str, seconds: int) -> bool:
    normalized = str(value or "").strip()
    if not normalized:
        return False
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds() < max(0, int(seconds or 0))


def build_candidates_from_seed_snapshot(snapshot: SearchSeedSnapshot) -> tuple[list[Candidate], list[EvidenceRecord]]:
    source_path = str(snapshot.summary_path)
    dataset_name = f"{snapshot.company_identity.company_key}_search_seed_candidates"
    candidates: list[Candidate] = []
    evidence_items: list[EvidenceRecord] = []
    for row in snapshot.entries:
        full_name = str(row.get("full_name") or "").strip()
        if not full_name:
            continue
        seed_reference = str(row.get("profile_url") or row.get("slug") or row.get("source_query") or "").strip()
        candidate_id = sha1(
            "|".join([normalize_name_token(snapshot.target_company), normalize_name_token(full_name), seed_reference]).encode("utf-8")
        ).hexdigest()[:16]
        profile_url = str(row.get("profile_url") or "").strip()
        slug = str(row.get("slug") or "").strip()
        candidate = Candidate(
            candidate_id=candidate_id,
            name_en=full_name,
            display_name=format_display_name(full_name, ""),
            category=_seed_candidate_category(row),
            target_company=snapshot.target_company,
            organization=snapshot.target_company,
            employment_status=str(row.get("employment_status") or "current"),
            role=str(row.get("headline") or "").strip(),
            team="",
            focus_areas=str(row.get("headline") or "").strip(),
            notes=_build_seed_notes(row),
            linkedin_url=profile_url,
            source_dataset=dataset_name,
            source_path=source_path,
            metadata={
                "seed_slug": slug,
                "seed_query": str(row.get("source_query") or ""),
                "seed_source_type": str(row.get("source_type") or ""),
                "seed_location": str(row.get("location") or ""),
                **dict(row.get("metadata") or {}),
            },
        )
        candidates.append(candidate)
        evidence_items.append(
            EvidenceRecord(
                evidence_id=make_evidence_id(candidate_id, dataset_name, candidate.role or "Search seed", profile_url or source_path),
                candidate_id=candidate_id,
                source_type=str(row.get("source_type") or "search_seed"),
                title=candidate.role or "Search seed",
                url=profile_url,
                summary=f"{full_name} was discovered as a search-seed candidate for {snapshot.target_company}.",
                source_dataset=dataset_name,
                source_path=source_path,
                metadata={"source_query": str(row.get("source_query") or ""), "slug": slug},
            )
        )
    return candidates, evidence_items


def extract_web_search_results(html_text: str) -> list[dict[str, str]]:
    return [item.to_record() for item in parse_duckduckgo_html_results(html_text)]


def infer_name_from_result_title(title: str, identity: CompanyIdentity) -> str:
    candidate = title
    for token in [" - LinkedIn", "| LinkedIn", " | LinkedIn", " - ", " | "]:
        if token in candidate:
            candidate = candidate.split(token, 1)[0]
            break
    candidate = candidate.replace(identity.canonical_name, " ")
    for alias in identity.aliases:
        candidate = candidate.replace(alias, " ")
    candidate = re.sub(r"\bLinkedIn\b", " ", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\s*[-|]+\s*$", " ", candidate).strip()
    candidate = " ".join(candidate.split())
    if len(candidate.split()) < 2:
        return ""
    return candidate


def extract_linkedin_slug(url: str) -> str:
    match = re.search(r"linkedin\.com/in/([^/?#]+)", url)
    if not match:
        return ""
    return match.group(1).strip()


def extract_people_search_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    container = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    rows = list((container or {}).get("data") or [])
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        profile_url = str(row.get("profileUrl") or row.get("profile_url") or "").strip()
        normalized.append(
            {
                "urn": str(row.get("urn") or "").strip(),
                "full_name": str(row.get("fullName") or row.get("full_name") or "").strip(),
                "headline": str(row.get("headline") or "").strip(),
                "location": str(row.get("location") or "").strip(),
                "profile_url": profile_url,
                "username": extract_linkedin_slug(profile_url),
            }
        )
    return normalized


def _fetch_duckduckgo_html(query_text: str) -> str:
    return fetch_search_results_html(query_text, timeout=30).text


def _write_search_response_raw_asset(
    *,
    logger: AssetLogger,
    response: SearchResponse,
    default_path: Path,
    asset_type: str,
    source_kind: str,
    metadata: dict[str, Any] | None = None,
) -> Path:
    record = search_response_to_record(response)
    payload_metadata = {
        "provider_name": response.provider_name,
        "query_text": response.query_text,
        **dict(metadata or {}),
    }
    if response.raw_format == "json":
        raw_path = default_path.with_suffix(".json")
        logger.write_json(
            raw_path,
            record,
            asset_type=asset_type,
            source_kind=source_kind,
            is_raw_asset=True,
            model_safe=False,
            metadata=payload_metadata,
        )
        return raw_path

    raw_path = default_path.with_suffix(".html")
    logger.write_text(
        raw_path,
        str(response.raw_payload or ""),
        asset_type=asset_type,
        source_kind=source_kind,
        content_type=response.content_type or "text/html",
        is_raw_asset=True,
        model_safe=False,
        metadata=payload_metadata,
    )
    return raw_path


def _write_search_execution_artifact(
    *,
    logger: AssetLogger,
    artifact,
    default_path: Path,
    asset_type: str,
    source_kind: str,
    metadata: dict[str, Any] | None = None,
) -> Path:
    raw_path = default_path.with_name(f"{default_path.stem}_{str(getattr(artifact, 'label', 'artifact') or 'artifact')}.json")
    logger.write_json(
        raw_path,
        getattr(artifact, "payload", {}),
        asset_type=asset_type,
        source_kind=source_kind,
        is_raw_asset=True,
        model_safe=False,
        metadata={
            **dict(metadata or {}),
            **dict(getattr(artifact, "metadata", {}) or {}),
        },
    )
    return raw_path


def _load_cached_search_response(path: Path, query_text: str) -> tuple[list[dict[str, str]], SearchResponse]:
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text())
        response = search_response_from_record(payload, fallback_query_text=query_text)
    else:
        html_text = path.read_text()
        response = SearchResponse(
            provider_name="duckduckgo_html",
            query_text=query_text,
            results=parse_duckduckgo_html_results(html_text),
            raw_payload=html_text,
            raw_format="html",
            final_url="",
            content_type="text/html",
        )
    return [item.to_record() for item in response.results], response


def _seed_entry_from_web_result(
    result: dict[str, str],
    identity: CompanyIdentity,
    query_text: str,
    employment_status: str,
    *,
    source_family: str = "public_web_search",
) -> dict[str, Any] | None:
    url = str(result.get("url") or "").strip()
    slug = extract_linkedin_slug(url)
    if not slug:
        return None
    full_name = infer_name_from_result_title(str(result.get("title") or ""), identity)
    if not full_name:
        return None
    return {
        "seed_key": _seed_key(full_name, url),
        "full_name": full_name,
        "headline": "",
        "location": "",
        "source_type": "web_linkedin_url_search",
        "source_query": query_text,
        "profile_url": url,
        "slug": slug,
        "employment_status": employment_status,
        "target_company": identity.canonical_name,
        "metadata": {"title": str(result.get("title") or "").strip(), "source_family": source_family},
    }


def _lead_entries_from_public_result(
    result: dict[str, str],
    identity: CompanyIdentity,
    query_text: str,
    employment_status: str,
    *,
    analysis: dict[str, Any] | None = None,
    source_family: str,
) -> list[dict[str, Any]]:
    url = str(result.get("url") or "").strip()
    if not url or "linkedin.com/in/" in url:
        return []
    title = str(result.get("title") or "").strip()
    if not title:
        return []
    relation = str((analysis or {}).get("target_company_relation") or "").strip().lower()
    confidence_label = str((analysis or {}).get("confidence_label") or "").strip().lower()
    if relation != "explicit" or confidence_label not in {"high", "medium"}:
        return []
    names = infer_public_names_from_result_title(title, identity)
    entries: list[dict[str, Any]] = []
    for name in names[:2]:
        if not _looks_like_person_name(name, identity):
            continue
        entries.append(
            {
                "seed_key": _seed_key(name, url),
                "full_name": name,
                "headline": title,
                "location": "",
                "source_type": "public_media_lead",
                "source_query": query_text,
                "profile_url": "",
                "slug": "",
                "employment_status": employment_status if employment_status in {"former", "current"} else "unknown",
                "target_company": identity.canonical_name,
                "metadata": {
                    "title": title,
                    "source_family": source_family,
                    "lead_url": url,
                },
            }
        )
    return entries


def _seed_key(name: str, reference: str) -> str:
    payload = "|".join([normalize_name_token(name), reference.strip().lower()])
    return sha1(payload.encode("utf-8")).hexdigest()[:16]


def _dedupe_seed_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in entries:
        key = str(entry.get("seed_key") or _seed_key(str(entry.get("full_name") or ""), str(entry.get("profile_url") or entry.get("source_query") or "")))
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped


def _normalize_harvest_company_filters(identity: CompanyIdentity, filter_hints: dict[str, list[str]]) -> dict[str, list[str]]:
    normalized = {
        str(key): [str(item).strip() for item in list(values or []) if str(item).strip()]
        for key, values in dict(filter_hints or {}).items()
    }
    company_url = str(identity.linkedin_company_url or "").strip()
    if not company_url:
        for key, values in list(normalized.items()):
            normalized[key] = _dedupe_filter_values(values, company_filter=key in _HARVEST_COMPANY_FILTER_KEYS)
        return normalized

    target_tokens = {
        _normalize_company_filter_token(identity.canonical_name),
        _normalize_company_filter_token(identity.requested_name),
        _normalize_company_filter_token(identity.company_key),
        _normalize_company_filter_token(identity.linkedin_slug),
        _normalize_company_filter_token(company_url),
    }
    target_tokens.discard("")
    if not target_tokens:
        for key, values in list(normalized.items()):
            normalized[key] = _dedupe_filter_values(values, company_filter=key in _HARVEST_COMPANY_FILTER_KEYS)
        return normalized

    for key in _HARVEST_COMPANY_FILTER_KEYS:
        values = list(normalized.get(key) or [])
        if not values:
            continue
        rewritten_values = [
            company_url if _normalize_company_filter_token(value) in target_tokens else value
            for value in values
        ]
        normalized[key] = _dedupe_filter_values(rewritten_values, company_filter=True)
    for key, values in list(normalized.items()):
        if key in _HARVEST_COMPANY_FILTER_KEYS:
            continue
        normalized[key] = _dedupe_filter_values(values, company_filter=False)
    return normalized


def _normalize_harvest_query_text(
    *,
    query_text: str,
    filter_hints: dict[str, list[str]],
    identity: CompanyIdentity,
) -> str:
    normalized_query = " ".join(str(query_text or "").split()).strip()
    if not normalized_query:
        return ""

    stripped_query = normalized_query
    for token in _harvest_blocked_query_tokens(filter_hints=filter_hints, identity=identity):
        stripped_query = re.sub(re.escape(token), " ", stripped_query, flags=re.IGNORECASE)
    stripped_query = re.sub(
        r"\b(linkedin|employee|employees|former|current|member|members|team|teams)\b",
        " ",
        stripped_query,
        flags=re.IGNORECASE,
    )
    stripped_query = " ".join(stripped_query.split())
    if stripped_query:
        return stripped_query

    keyword_values = [str(item).strip() for item in list(filter_hints.get("keywords") or []) if str(item).strip()]
    if keyword_values:
        return " ".join(keyword_values[:2])
    return normalized_query


_GENERIC_PROVIDER_QUERY_TERMS = {
    "research",
    "researcher",
    "researchers",
    "employee",
    "employees",
    "member",
    "members",
    "team",
    "teams",
    "current",
    "former",
    "people",
    "person",
}

_PROVIDER_QUERY_CANONICAL_ALIASES = {
    "reasoning model": "Reasoning",
    "reasoning models": "Reasoning",
    "chain of thought": "Chain-of-thought",
    "chain-of-thought": "Chain-of-thought",
    "inference time compute": "Inference-time compute",
    "inference-time compute": "Inference-time compute",
    "post train": "Post-train",
    "post-training": "Post-train",
    "pre train": "Pre-train",
    "pre-training": "Pre-train",
    "vision language": "Vision-language",
    "vision-language": "Vision-language",
    "multimodality": "Multimodal",
    "video-generation": "Video generation",
    "infrastructure": "Infra",
}


def _resolve_provider_people_search_queries(
    *,
    identity: CompanyIdentity,
    filter_hints: dict[str, list[str]],
    search_seed_queries: list[str],
) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()

    def _add(value: str) -> None:
        cleaned = _clean_provider_query_text(value)
        if not cleaned:
            return
        signature = _provider_query_family_key(cleaned)
        if signature in seen:
            return
        seen.add(signature)
        queries.append(cleaned)

    for value in list(filter_hints.get("keywords") or []):
        _add(str(value or ""))
    for value in list(filter_hints.get("scope_keywords") or []):
        _add(str(value or ""))
    for value in list(search_seed_queries or []):
        normalized = _normalize_harvest_query_text(
            query_text=str(value or ""),
            filter_hints=filter_hints,
            identity=identity,
        )
        _add(normalized)
    return queries


def _clean_provider_query_text(value: str) -> str:
    normalized = " ".join(str(value or "").split()).strip()
    if not normalized:
        return ""
    stripped = re.sub(
        r"\b(" + "|".join(re.escape(token) for token in sorted(_GENERIC_PROVIDER_QUERY_TERMS)) + r")\b",
        " ",
        normalized,
        flags=re.IGNORECASE,
    )
    stripped = " ".join(stripped.split())
    return stripped


def _provider_query_family_key(value: str) -> str:
    normalized = " ".join(str(value or "").split()).strip()
    if not normalized:
        return ""
    scoped = canonicalize_scope_signal_label(normalized)
    canonical = _PROVIDER_QUERY_CANONICAL_ALIASES.get(normalized.lower(), scoped or normalized)
    return _search_query_signature(canonical) or canonical.lower()


def _harvest_blocked_query_tokens(*, filter_hints: dict[str, list[str]], identity: CompanyIdentity) -> list[str]:
    blocked: list[str] = []
    seen: set[str] = set()

    def _add(value: str) -> None:
        token = " ".join(str(value or "").split()).strip()
        if not token:
            return
        lowered = token.lower()
        if lowered in seen:
            return
        seen.add(lowered)
        blocked.append(token)

    _add(identity.canonical_name)
    _add(identity.requested_name)
    _add(identity.linkedin_slug)
    for alias in list(identity.aliases or []):
        _add(str(alias or ""))
    for key in ["current_companies", "past_companies", "exclude_current_companies", "exclude_past_companies"]:
        for value in list(filter_hints.get(key) or []):
            _add(_company_filter_search_label(str(value or "")))
    for value in list(filter_hints.get("job_titles") or []):
        _add(str(value or ""))
    return blocked


def _company_filter_search_label(value: str) -> str:
    raw = unescape(str(value or "")).strip()
    if not raw:
        return ""
    match = re.search(r"linkedin\.com/company/([^/?#]+)", raw, flags=re.IGNORECASE)
    if match:
        raw = str(match.group(1) or "").strip()
    raw = raw.replace("-", " ").replace("_", " ")
    return " ".join(raw.split())


def _normalize_company_filter_token(value: str) -> str:
    raw = unescape(str(value or "")).strip().lower()
    if not raw:
        return ""
    match = re.search(r"linkedin\.com/company/([^/?#]+)", raw)
    if match:
        return re.sub(r"[^a-z0-9]+", "", str(match.group(1) or "").lower())
    return re.sub(r"[^a-z0-9]+", "", raw)


_HARVEST_COMPANY_FILTER_KEYS = {
    "current_companies",
    "past_companies",
    "exclude_current_companies",
    "exclude_past_companies",
}


def _dedupe_filter_values(values: list[str], *, company_filter: bool) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized:
            continue
        key = _normalize_company_filter_token(normalized) if company_filter else " ".join(normalized.lower().split())
        if not key:
            key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


def _search_query_signature(value: str) -> str:
    normalized = " ".join(str(value or "").lower().split()).strip()
    if not normalized:
        return ""
    compact = re.sub(r"[\s\-_]+", "", normalized)
    alnum = re.sub(r"[^0-9a-z]+", "", compact)
    return alnum or compact


def _extract_harvest_profile_urls_from_result(result: dict[str, Any]) -> set[str]:
    rows = list(dict(result or {}).get("rows") or [])
    urls: set[str] = set()
    for row in rows:
        url = str(dict(row or {}).get("profile_url") or "").strip().lower()
        if not url:
            continue
        urls.add(url.rstrip("/"))
    return urls


def _jaccard_overlap_ratio(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    union = left.union(right)
    if not union:
        return 0.0
    return float(len(left.intersection(right)) / len(union))


def _compile_query_specs(search_seed_queries: list[str], query_bundles: list[dict[str, Any]]) -> list[dict[str, str]]:
    specs: list[dict[str, str]] = []
    for query in search_seed_queries:
        normalized = " ".join(str(query or "").split()).strip()
        if normalized:
            specs.append(
                {
                    "bundle_id": "seed_queries",
                    "source_family": "public_web_search",
                    "execution_mode": "low_cost_web_search",
                    "query": normalized,
                }
            )
    for bundle in query_bundles:
        if not isinstance(bundle, dict):
            continue
        for query in bundle.get("queries") or []:
            normalized = " ".join(str(query or "").split()).strip()
            if not normalized:
                continue
            specs.append(
                {
                    "bundle_id": str(bundle.get("bundle_id") or "bundle"),
                    "source_family": str(bundle.get("source_family") or "public_web_search"),
                    "execution_mode": str(bundle.get("execution_mode") or "low_cost_web_search"),
                    "query": normalized,
                }
            )
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in specs:
        query_signature = _search_query_signature(item["query"]) or item["query"].lower()
        key = (item["execution_mode"], query_signature)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _resolve_effective_search_seed_queries(
    *,
    search_seed_queries: list[str],
    delta_execution_plan: dict[str, Any] | None,
) -> list[str]:
    delta_queries = [
        " ".join(str(item or "").split()).strip()
        for item in list(dict(delta_execution_plan or {}).get("missing_profile_search_queries") or [])
        if " ".join(str(item or "").split()).strip()
    ]
    source = delta_queries or list(search_seed_queries or [])
    deduped: list[str] = []
    seen: set[str] = set()
    for item in source:
        normalized = " ".join(str(item or "").split()).strip()
        if not normalized:
            continue
        signature = _search_query_signature(normalized) or normalized.lower()
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(normalized)
    return deduped


def _resolve_discovery_filter_hints(
    *,
    filter_hints: dict[str, list[str]] | None,
    intent_view: dict[str, Any] | None,
) -> dict[str, list[str]]:
    explicit = {
        str(key): [str(item).strip() for item in list(values or []) if str(item).strip()]
        for key, values in dict(filter_hints or {}).items()
        if str(key).strip()
    }
    if explicit:
        return explicit
    return {
        str(key): [str(item).strip() for item in list(values or []) if str(item).strip()]
        for key, values in dict(dict(intent_view or {}).get("filter_hints") or {}).items()
        if str(key).strip()
    }


def _resolve_discovery_query_bundles(
    *,
    query_bundles: list[dict[str, Any]] | None,
    intent_view: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    explicit = [dict(item) for item in list(query_bundles or []) if isinstance(item, dict)]
    if explicit:
        return explicit
    return [dict(item) for item in list(dict(intent_view or {}).get("search_query_bundles") or []) if isinstance(item, dict)]


def _resolve_discovery_search_seed_queries(
    *,
    search_seed_queries: list[str] | None,
    query_bundles: list[dict[str, Any]] | None,
    intent_view: dict[str, Any] | None,
) -> list[str]:
    explicit_queries = [
        " ".join(str(item or "").split()).strip()
        for item in list(search_seed_queries or [])
        if " ".join(str(item or "").split()).strip()
    ]
    if explicit_queries:
        return explicit_queries
    bundled_queries: list[str] = []
    for bundle in list(query_bundles or []):
        if not isinstance(bundle, dict):
            continue
        for query in list(bundle.get("queries") or []):
            normalized = " ".join(str(query or "").split()).strip()
            if normalized:
                bundled_queries.append(normalized)
    if bundled_queries:
        return bundled_queries
    return [
        " ".join(str(item or "").split()).strip()
        for item in list(dict(intent_view or {}).get("search_seed_queries") or [])
        if " ".join(str(item or "").split()).strip()
    ]


def _filter_query_bundles_for_delta(
    query_bundles: list[dict[str, Any]],
    *,
    allowed_queries: list[str],
) -> list[dict[str, Any]]:
    allowed_signatures = {
        _search_query_signature(query) or " ".join(str(query or "").lower().split())
        for query in list(allowed_queries or [])
        if str(query or "").strip()
    }
    if not allowed_signatures:
        return [dict(bundle or {}) for bundle in list(query_bundles or []) if isinstance(bundle, dict)]
    filtered: list[dict[str, Any]] = []
    for bundle in list(query_bundles or []):
        if not isinstance(bundle, dict):
            continue
        queries = []
        for query in list(bundle.get("queries") or []):
            normalized = " ".join(str(query or "").split()).strip()
            if not normalized:
                continue
            signature = _search_query_signature(normalized) or normalized.lower()
            if signature in allowed_signatures:
                queries.append(normalized)
        if not queries:
            continue
        filtered.append(
            {
                **dict(bundle),
                "queries": queries,
            }
        )
    return filtered


def infer_public_names_from_result_title(title: str, identity: CompanyIdentity) -> list[str]:
    cleaned = " " + title + " "
    cleaned = cleaned.replace(identity.canonical_name, " ")
    for alias in identity.aliases:
        cleaned = cleaned.replace(alias, " ")
    cleaned = re.sub(r"\b(YouTube|Podcast|Interview|with|on|about|episode|official|blog|research|engineering)\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = " ".join(cleaned.split())
    candidates = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}\b", cleaned)
    blocked = {identity.canonical_name.lower(), "Google DeepMind".lower()}
    results: list[str] = []
    for item in candidates:
        normalized = " ".join(item.split()).strip()
        if not normalized or normalized.lower() in blocked:
            continue
        if not _looks_like_person_name(normalized, identity):
            continue
        if normalized not in results:
            results.append(normalized)
    return results[:3]


def _looks_like_person_name(value: str, identity: CompanyIdentity | None = None) -> bool:
    tokens = [token for token in str(value or "").split() if token]
    if len(tokens) < 2 or len(tokens) > 3:
        return False
    blocked_tokens = {
        "acknowledgment",
        "acknowledgments",
        "acknowledgement",
        "acknowledgements",
        "author",
        "authors",
        "authorship",
        "biomedical",
        "blog",
        "build",
        "conference",
        "contributor",
        "contributors",
        "engineering",
        "episode",
        "guidelines",
        "human",
        "interview",
        "launch",
        "launches",
        "nature",
        "official",
        "podcast",
        "research",
        "roadmap",
        "section",
        "team",
        "with",
        "your",
    }
    company_tokens = {
        normalize_name_token(str(identity.canonical_name or "")),
        normalize_name_token(str(identity.requested_name or "")),
    } if identity is not None else set()
    company_tokens.discard("")
    for token in tokens:
        normalized = normalize_name_token(token)
        if not normalized:
            return False
        if normalized in blocked_tokens:
            return False
        if normalized in company_tokens:
            return False
        if not re.fullmatch(r"[^\W\d_]+(?:[-'][^\W\d_]+)*", token, flags=re.UNICODE):
            return False
    return True


def _seed_candidate_category(row: dict[str, Any]) -> str:
    source_type = str(row.get("source_type") or "").strip()
    if source_type == "public_media_lead" and not str(row.get("profile_url") or "").strip():
        return "lead"
    return "former_employee" if row.get("employment_status") == "former" else "employee"


def _interrupted_query_summary(index: int, query_spec: dict[str, str], query_text: str, html_path: Path) -> dict[str, Any]:
    return {
        "query": query_text,
        "bundle_id": query_spec.get("bundle_id", ""),
        "source_family": query_spec.get("source_family", ""),
        "execution_mode": query_spec.get("execution_mode", ""),
        "mode": "web_search",
        "raw_path": str(html_path),
        "result_count": 0,
        "linkedin_result_count": 0,
        "seed_entry_count": 0,
        "status": "interrupted",
        "index": index,
    }


def _build_people_search_url(account: RapidApiAccount, query_text: str, *, limit: int) -> str:
    base = account.base_url.rstrip("/")
    if "z-real-time-linkedin-scraper-api1" in account.host:
        endpoint = base[:-len("/api/search/people")] if base.endswith("/api/search/people") else base
        return endpoint + "/api/search/people?" + parse.urlencode({"keywords": query_text, "limit": limit})
    endpoint_path = str(account.endpoint_search or "/api/search/people").split("?", 1)[0]
    return base + endpoint_path + "?" + parse.urlencode({"keywords": query_text, "limit": limit})


def _build_seed_notes(row: dict[str, Any]) -> str:
    parts = ["Discovered from low-cost search seed acquisition."]
    if row.get("source_query"):
        parts.append(f"Query: {row['source_query']}.")
    if row.get("source_type"):
        parts.append(f"Source: {row['source_type']}.")
    if row.get("location"):
        parts.append(f"Location: {row['location']}.")
    return " ".join(parts)
