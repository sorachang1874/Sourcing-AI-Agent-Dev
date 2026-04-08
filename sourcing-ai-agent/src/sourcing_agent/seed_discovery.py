from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from hashlib import sha1
from html import unescape
import json
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
from .search_provider import (
    BaseSearchProvider,
    DuckDuckGoHtmlSearchProvider,
    SearchResponse,
    parse_duckduckgo_html_results,
    search_response_from_record,
    search_response_to_record,
)
from .worker_daemon import AutonomousWorkerDaemon


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
    ) -> SearchSeedSnapshot:
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        effective_filter_hints = _normalize_harvest_company_filters(identity, filter_hints)

        entries: list[dict[str, Any]] = []
        query_summaries: list[dict[str, Any]] = []
        accounts_used: list[str] = []
        errors: list[str] = []
        stop_reason = "completed"

        compiled_queries = _compile_query_specs(search_seed_queries, query_bundles or [])
        web_result_target = int(cost_policy.get("provider_people_search_min_expected_results", 10) or 10)
        parallel_limit = max(1, min(int(cost_policy.get("parallel_search_workers", 3) or 3), len(compiled_queries) or 1))
        result_limit = max(1, min(int(cost_policy.get("public_media_results_per_query", 10) or 10), 25))
        worker_results: list[dict[str, Any]] = []
        pending_specs = [
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
                        request_payload=request_payload or {},
                        plan_payload=plan_payload or {},
                        runtime_mode=runtime_mode,
                        result_limit=result_limit,
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
                            request_payload=request_payload or {},
                            plan_payload=plan_payload or {},
                            runtime_mode=runtime_mode,
                            result_limit=result_limit,
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
        paid_queries = [item["query"] for item in compiled_queries if item["execution_mode"] == "paid_fallback"] or list(search_seed_queries)
        if (
            not queued_background_search
            and len(entries) < web_result_target
            and self.accounts
            and cost_policy.get("provider_people_search_mode") == "fallback_only"
        ):
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
            if provider_entries:
                stop_reason = "provider_people_search_fallback"

        summary_path = discovery_dir / "summary.json"
        summary_payload = {
            "snapshot_id": snapshot_dir.name,
            "target_company": identity.canonical_name,
            "company_identity": identity.to_record(),
            "entry_count": len(entries),
            "search_seed_queries": search_seed_queries,
            "requested_filter_hints": filter_hints,
            "effective_filter_hints": effective_filter_hints,
            "query_summaries": query_summaries,
            "accounts_used": accounts_used,
            "errors": errors,
            "stop_reason": stop_reason,
            "queued_query_count": queued_query_count,
            "cost_policy": cost_policy,
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
    ) -> dict[str, Any]:
        query_text = str(query_spec["query"] or "").strip()
        raw_search_path = discovery_dir / f"web_query_{index:02d}.html"
        lane_id = "public_media_specialist" if query_spec["source_family"] in {"public_interviews", "publication_and_blog"} else "search_planner"
        worker_handle = None
        checkpoint: dict[str, Any] = {}
        interrupted = False
        if worker_runtime is not None and job_id:
            worker_handle = worker_runtime.begin_worker(
                job_id=job_id,
                request=JobRequest.from_payload(request_payload),
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
                    "request_payload": request_payload,
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
            if cached_search_path and cached_search_path.exists():
                parsed_results, search_response = _load_cached_search_response(cached_search_path, query_text)
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
                    checkpoint = {
                        **checkpoint,
                        "stage": "fetched_search_results",
                        "raw_path": str(raw_search_path),
                        "provider_name": search_response.provider_name,
                        "search_state": {},
                    }
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
        paid_queries = list(search_seed_queries)
        if employment_status == "former" and list(filter_hints.get("past_companies") or []):
            paid_queries = ["", *paid_queries]
        deduped_queries: list[str] = []
        seen_queries: set[str] = set()
        for item in paid_queries:
            key = str(item or "")
            if key in seen_queries:
                continue
            seen_queries.add(key)
            deduped_queries.append(key)

        for index, query_text in enumerate(deduped_queries, start=1):
            summary_query = query_text or "__past_company_only__"
            if self.harvest_search_connector and self.harvest_search_connector.settings.enabled:
                harvest_result = self.harvest_search_connector.search_profiles(
                    query_text=query_text,
                    filter_hints=filter_hints,
                    employment_status=employment_status,
                    discovery_dir=discovery_dir,
                    asset_logger=asset_logger,
                    limit=min(limit, self.harvest_search_connector.settings.max_paid_items),
                    pages=page_count,
                )
                if harvest_result is not None:
                    rows = list(harvest_result.get("rows") or [])
                    query_entries: list[dict[str, Any]] = []
                    for row in rows[:limit]:
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
                            entries.append(entry)
                    query_summaries.append(
                        {
                            "query": summary_query,
                            "mode": "harvest_profile_search",
                            "raw_path": str(harvest_result.get("raw_path") or ""),
                            "account_id": "harvest_profile_search",
                            "seed_entry_count": len(query_entries),
                        }
                    )
                    if query_entries:
                        if "harvest_profile_search" not in accounts_used:
                            accounts_used.append("harvest_profile_search")
                        break
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
            if entries:
                break
        return entries, query_summaries, errors, accounts_used

    def _search_people(self, query_text: str, *, limit: int) -> tuple[dict[str, Any] | None, RapidApiAccount | None, list[str]]:
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
    source_family: str,
) -> list[dict[str, Any]]:
    url = str(result.get("url") or "").strip()
    if not url or "linkedin.com/in/" in url:
        return []
    title = str(result.get("title") or "").strip()
    if not title:
        return []
    names = infer_public_names_from_result_title(title, identity)
    entries: list[dict[str, Any]] = []
    for name in names[:2]:
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
        return normalized

    target_tokens = {
        _normalize_company_filter_token(identity.canonical_name),
        _normalize_company_filter_token(identity.requested_name),
        _normalize_company_filter_token(identity.company_key),
        _normalize_company_filter_token(identity.linkedin_slug),
        _normalize_company_filter_token(company_url),
    }
    target_tokens.update(_normalize_company_filter_token(alias) for alias in identity.aliases if alias)
    target_tokens.discard("")
    if not target_tokens:
        return normalized

    for key in ["current_companies", "past_companies", "exclude_current_companies", "exclude_past_companies"]:
        values = list(normalized.get(key) or [])
        if not values:
            continue
        normalized[key] = [
            company_url if _normalize_company_filter_token(value) in target_tokens else value
            for value in values
        ]
    return normalized


def _normalize_company_filter_token(value: str) -> str:
    raw = unescape(str(value or "")).strip().lower()
    if not raw:
        return ""
    match = re.search(r"linkedin\.com/company/([^/?#]+)", raw)
    if match:
        return re.sub(r"[^a-z0-9]+", "", str(match.group(1) or "").lower())
    return re.sub(r"[^a-z0-9]+", "", raw)


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
        key = (item["execution_mode"], item["query"].lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


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
        if normalized not in results:
            results.append(normalized)
    return results[:3]


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
