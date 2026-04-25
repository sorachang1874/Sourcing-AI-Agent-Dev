from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

from .agent_runtime import AgentRuntimeCoordinator
from .asset_policy import (
    DEFAULT_MODEL_CONTEXT_CHAR_LIMIT,
    is_binary_like_url,
)
from .asset_logger import AssetLogger
from .document_extraction import (
    analyze_remote_document,
    build_candidate_patch_from_signal_bundle,
    build_page_analysis_input as _build_page_analysis_input_impl,
    empty_signal_bundle,
    extract_page_signals as _extract_page_signals_impl,
    merge_signal_bundle,
)
from .domain import Candidate, EvidenceRecord, JobRequest, make_evidence_id, merge_candidate
from .model_provider import DeterministicModelClient, ModelClient
from .runtime_tuning import (
    apply_runtime_timing_overrides_to_search_state,
    resolve_runtime_timing_overrides,
    resolved_lane_fetch_cooldown_seconds,
    resolved_lane_ready_cooldown_seconds,
)
from .search_provider import (
    BaseSearchProvider,
    DuckDuckGoHtmlSearchProvider,
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


def _lane_ready_poll_min_interval_seconds() -> int:
    return max(
        0,
        _env_int(
            "EXPLORATION_READY_POLL_MIN_INTERVAL_SECONDS",
            _env_int("WEB_SEARCH_READY_POLL_MIN_INTERVAL_SECONDS", 15),
        ),
    )


def _lane_fetch_min_interval_seconds() -> int:
    return max(
        0,
        _env_int(
            "EXPLORATION_FETCH_MIN_INTERVAL_SECONDS",
            _env_int("WEB_SEARCH_FETCH_MIN_INTERVAL_SECONDS", 15),
        ),
    )


def _runtime_timing_overrides_from_request_payload(request_payload: dict[str, Any] | None) -> dict[str, Any]:
    if not request_payload:
        return {}
    execution_preferences = dict(JobRequest.from_payload(dict(request_payload or {})).execution_preferences or {})
    return resolve_runtime_timing_overrides(execution_preferences)


def _daemon_plan_payload(
    plan_payload: dict[str, Any] | None,
    *,
    request_payload: dict[str, Any] | None,
    parallel_workers: int,
) -> dict[str, Any]:
    normalized_plan = dict(plan_payload or {})
    acquisition_strategy = dict(normalized_plan.get("acquisition_strategy") or {})
    cost_policy = dict(acquisition_strategy.get("cost_policy") or {})
    cost_policy.setdefault("parallel_exploration_workers", max(1, int(parallel_workers or 1)))
    acquisition_strategy["cost_policy"] = cost_policy
    normalized_plan["acquisition_strategy"] = acquisition_strategy
    if "execution_preferences" not in normalized_plan:
        execution_preferences = dict(JobRequest.from_payload(dict(request_payload or {})).execution_preferences or {})
        if execution_preferences:
            normalized_plan["execution_preferences"] = execution_preferences
    return normalized_plan


@dataclass(slots=True)
class ExploratoryEnrichmentResult:
    candidates: list[Candidate]
    evidence: list[EvidenceRecord]
    artifact_paths: dict[str, str] = field(default_factory=dict)
    explored_candidates: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    queued_candidate_count: int = 0
    stop_reason: str = ""


class ExploratoryWebEnricher:
    def __init__(
        self,
        model_client: ModelClient | None = None,
        *,
        worker_runtime: AgentRuntimeCoordinator | None = None,
        search_provider: BaseSearchProvider | None = None,
    ) -> None:
        self.model_client = model_client or DeterministicModelClient()
        self.worker_runtime = worker_runtime
        self.search_provider = search_provider or DuckDuckGoHtmlSearchProvider()

    def refresh_background_search_workers(self, workers: list[dict[str, Any]]) -> dict[str, Any]:
        grouped_specs: dict[tuple[Path, str], list[dict[str, Any]]] = {}
        snapshot_dirs: dict[tuple[Path, str], Path] = {}
        errors: list[str] = []
        worker_updates: dict[int, dict[str, Any]] = {}

        for worker in workers:
            if str(worker.get("lane_id") or "").strip() != "exploration_specialist":
                continue
            metadata = dict(worker.get("metadata") or {})
            input_payload = dict(worker.get("input") or {})
            snapshot_dir_text = str(metadata.get("snapshot_dir") or "").strip()
            target_company = str(metadata.get("target_company") or "").strip()
            worker_id = int(worker.get("worker_id") or 0)
            if not snapshot_dir_text or not target_company or worker_id <= 0:
                continue
            candidate_payload = dict(input_payload.get("candidate") or {})
            candidate_payload.setdefault("candidate_id", str(input_payload.get("candidate_id") or worker.get("worker_key") or "").strip())
            candidate_payload.setdefault(
                "display_name",
                str(input_payload.get("display_name") or candidate_payload.get("candidate_id") or "").strip(),
            )
            candidate_payload.setdefault(
                "name_en",
                str(candidate_payload.get("display_name") or candidate_payload.get("candidate_id") or "").strip(),
            )
            candidate_payload.setdefault("target_company", target_company)
            if not str(candidate_payload.get("candidate_id") or "").strip():
                continue
            try:
                candidate = Candidate(**candidate_payload)
            except TypeError:
                continue
            snapshot_dir = Path(snapshot_dir_text).expanduser()
            exploration_dir = snapshot_dir / "exploration"
            group_key = (exploration_dir, target_company)
            grouped_specs.setdefault(group_key, []).append(
                {
                    "candidate": candidate,
                    "lane_id": "exploration_specialist",
                    "worker_key": candidate.candidate_id,
                    "label": candidate.display_name,
                    "runtime_timing_overrides": dict(metadata.get("runtime_timing_overrides") or {})
                    or _runtime_timing_overrides_from_request_payload(dict(metadata.get("request_payload") or {})),
                    "worker_id": worker_id,
                }
            )
            snapshot_dirs[group_key] = snapshot_dir

        for group_key, pending_specs in grouped_specs.items():
            exploration_dir, target_company = group_key
            exploration_dir.mkdir(parents=True, exist_ok=True)
            logger = AssetLogger(snapshot_dirs[group_key])
            errors.extend(
                _prepare_batched_exploration_queries(
                    search_provider=self.search_provider,
                    logger=logger,
                    exploration_dir=exploration_dir,
                    pending_specs=pending_specs,
                    target_company=target_company,
                )
            )
            for spec in pending_specs:
                worker_id = int(spec.get("worker_id") or 0)
                if worker_id <= 0:
                    continue
                prefetched_queries = {
                    str(key): dict(value)
                    for key, value in dict(spec.get("prefetched_search_queries") or {}).items()
                    if str(key).strip()
                }
                if not prefetched_queries:
                    continue
                worker_updates[worker_id] = {
                    "prefetched_queries": prefetched_queries,
                }

        return {"errors": errors, "worker_updates": worker_updates}

    def enrich(
        self,
        snapshot_dir: Path,
        candidates: list[Candidate],
        *,
        target_company: str,
        max_candidates: int,
        asset_logger: AssetLogger | None = None,
        job_id: str = "",
        request_payload: dict[str, Any] | None = None,
        plan_payload: dict[str, Any] | None = None,
        runtime_mode: str = "workflow",
        parallel_workers: int = 2,
    ) -> ExploratoryEnrichmentResult:
        exploration_dir = snapshot_dir / "exploration"
        exploration_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        merged_candidates: list[Candidate] = []
        evidence: list[EvidenceRecord] = []
        explored_candidates: list[dict[str, Any]] = []
        errors: list[str] = []

        selected_candidates = candidates[:max_candidates]
        runtime_timing_overrides = _runtime_timing_overrides_from_request_payload(request_payload or {})
        pending_specs = [
            {
                "index": index,
                "candidate": candidate,
                "lane_id": "exploration_specialist",
                "worker_key": candidate.candidate_id,
                "label": candidate.display_name,
                "runtime_timing_overrides": dict(runtime_timing_overrides),
            }
            for index, candidate in enumerate(selected_candidates, start=1)
        ]
        max_workers = max(1, min(parallel_workers, len(selected_candidates) or 1))
        daemon_summary = {
            "results": [],
            "retried": [],
            "backlog": [],
            "lane_budget_used": {},
            "lane_budget_caps": {},
            "cycles": 0,
            "daemon_events": [],
        }
        if self.worker_runtime is not None and job_id and pending_specs:
            errors.extend(
                _prepare_batched_exploration_queries(
                    search_provider=self.search_provider,
                    logger=logger,
                    exploration_dir=exploration_dir,
                    pending_specs=pending_specs,
                    target_company=target_company,
                )
            )
        if pending_specs:
            if self.worker_runtime is not None and job_id:
                daemon = AutonomousWorkerDaemon.from_plan(
                    plan_payload=_daemon_plan_payload(
                        plan_payload,
                        request_payload=request_payload,
                        parallel_workers=parallel_workers,
                    ),
                    existing_workers=self.worker_runtime.list_workers(job_id=job_id, lane_id="exploration_specialist"),
                    total_limit=max_workers,
                )
                daemon_summary = daemon.run(
                    pending_specs,
                    executor=lambda spec: self._explore_candidate(
                        snapshot_dir=snapshot_dir,
                        candidate=spec["candidate"],
                        target_company=target_company,
                        logger=logger,
                        job_id=job_id,
                        request_payload=request_payload or {},
                        plan_payload=plan_payload or {},
                        runtime_mode=runtime_mode,
                        prefetched_search_queries={
                            str(key): dict(value)
                            for key, value in dict(spec.get("prefetched_search_queries") or {}).items()
                            if str(key).strip()
                        },
                    ),
                )
                daemon_results = list(daemon_summary.get("results") or [])
            else:
                with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(pending_specs)))) as executor:
                    futures = [
                        executor.submit(
                            self._explore_candidate,
                            snapshot_dir=snapshot_dir,
                            candidate=spec["candidate"],
                            target_company=target_company,
                            logger=logger,
                            job_id=job_id,
                            request_payload=request_payload or {},
                            plan_payload=plan_payload or {},
                            runtime_mode=runtime_mode,
                            prefetched_search_queries={
                                str(key): dict(value)
                                for key, value in dict(spec.get("prefetched_search_queries") or {}).items()
                                if str(key).strip()
                            },
                        )
                        for spec in pending_specs
                    ]
                    daemon_results = [future.result() for future in futures]
            for result in daemon_results:
                merged_candidates.append(result["candidate"])
                evidence.extend(result["evidence"])
                explored_candidates.append(result["summary"])
                errors.extend(result["errors"])

        summary_path = exploration_dir / "summary.json"
        queued_candidate_count = len(
            [item for item in explored_candidates if str(item.get("status") or "") == "queued"]
        )
        stop_reason = "queued_background_exploration" if queued_candidate_count > 0 else ""
        logger.write_json(
            summary_path,
            {
                "target_company": target_company,
                "explored_candidates": explored_candidates,
                "errors": errors,
                "queued_candidate_count": queued_candidate_count,
                "stop_reason": stop_reason,
                "worker_daemon": {
                    "cycles": int(daemon_summary.get("cycles") or 0),
                    "retried": list(daemon_summary.get("retried") or []),
                    "backlog_count": len(list(daemon_summary.get("backlog") or [])),
                    "lane_budget_used": dict(daemon_summary.get("lane_budget_used") or {}),
                    "lane_budget_caps": dict(daemon_summary.get("lane_budget_caps") or {}),
                },
            },
            asset_type="exploration_summary",
            source_kind="further_exploration",
            is_raw_asset=False,
            model_safe=True,
        )
        return ExploratoryEnrichmentResult(
            candidates=merged_candidates,
            evidence=evidence,
            artifact_paths={"exploration_summary": str(summary_path)},
            explored_candidates=explored_candidates,
            errors=errors,
            queued_candidate_count=queued_candidate_count,
            stop_reason=stop_reason,
        )

    def _explore_candidate(
        self,
        *,
        snapshot_dir: Path,
        candidate: Candidate,
        target_company: str,
        logger: AssetLogger,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        prefetched_search_queries: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        exploration_dir = snapshot_dir / "exploration"
        candidate_dir = exploration_dir / candidate.candidate_id
        candidate_dir.mkdir(parents=True, exist_ok=True)
        queries = _build_exploration_queries(candidate, target_company)
        result_summaries: list[dict[str, Any]] = []
        gathered_signals = empty_signal_bundle()
        errors: list[str] = []
        worker_handle = None
        completed_queries = set()
        interrupted = False
        checkpoint: dict[str, Any] = {}
        runtime_timing_overrides = _runtime_timing_overrides_from_request_payload(request_payload)
        if self.worker_runtime is not None and job_id:
            worker_handle = self.worker_runtime.begin_worker(
                job_id=job_id,
                request=JobRequest.from_payload(request_payload),
                plan_payload=plan_payload,
                runtime_mode=runtime_mode,
                lane_id="exploration_specialist",
                worker_key=candidate.candidate_id,
                stage="enriching",
                span_name=f"explore_candidate:{candidate.display_name}",
                budget_payload={"max_queries": len(queries), "max_pages_per_query": 5},
                input_payload={
                    "candidate_id": candidate.candidate_id,
                    "display_name": candidate.display_name,
                    "candidate": candidate.to_record(),
                },
                metadata={
                    "target_company": target_company,
                    "snapshot_dir": str(snapshot_dir),
                    "request_payload": request_payload,
                    "plan_payload": plan_payload,
                    "runtime_mode": runtime_mode,
                    "runtime_timing_overrides": dict(runtime_timing_overrides),
                },
                handoff_from_lane="public_media_specialist",
            )
            existing = self.worker_runtime.get_worker(worker_handle.worker_id) or {}
            checkpoint = dict(existing.get("checkpoint") or {})
            completed_queries = set(checkpoint.get("completed_queries") or [])
            result_summaries = list(checkpoint.get("result_summaries") or [])
            stored_signals = dict(checkpoint.get("gathered_signals") or {})
            if stored_signals:
                for key, values in stored_signals.items():
                    gathered_signals[key] = list(values or [])
            errors = list(checkpoint.get("errors") or [])
            checkpoint, prefetched_applied = _merge_prefetched_exploration_checkpoint(
                checkpoint=checkpoint,
                prefetched_search_queries={
                    str(key): dict(value)
                    for key, value in dict(prefetched_search_queries or {}).items()
                    if str(key).strip()
                },
            )
            if prefetched_applied:
                self.worker_runtime.checkpoint_worker(
                    worker_handle,
                    checkpoint_payload=checkpoint,
                    output_payload={
                        "prefetched_query_count": len(dict(checkpoint.get("prefetched_queries") or {})),
                    },
                )
            output_payload = dict(existing.get("output") or {})
            if str(existing.get("status") or "") == "completed" and output_payload:
                self.worker_runtime.complete_worker(
                    worker_handle,
                    status="completed",
                    checkpoint_payload=checkpoint,
                    output_payload=output_payload,
                    handoff_to_lane="enrichment_specialist",
                )
                return {
                    "candidate": Candidate(**dict(output_payload.get("candidate") or candidate.to_record())),
                    "evidence": [EvidenceRecord(**item) for item in list(output_payload.get("evidence") or [])],
                    "summary": dict(output_payload.get("summary") or {"candidate_id": candidate.candidate_id}),
                    "errors": list(output_payload.get("errors") or []),
                    "worker_status": "completed",
                    "daemon_action": "reused_output",
                }

        for index, query_text in enumerate(queries, start=1):
            if str(index) in completed_queries:
                continue
            if worker_handle and self.worker_runtime and self.worker_runtime.should_interrupt_worker(worker_handle):
                interrupted = True
                break
            raw_search_path = candidate_dir / f"query_{index:02d}.html"
            prefetched_queries = {
                str(key): dict(value)
                for key, value in dict(checkpoint.get("prefetched_queries") or {}).items()
                if str(key).strip()
            }
            prefetched_raw_path: Path | None = None
            search_execution = None
            try:
                if worker_handle and self.worker_runtime:
                    active_query_index = int(checkpoint.get("active_query_index") or 0)
                    query_prefetch = dict(prefetched_queries.get(str(index)) or {})
                    search_checkpoint = _resolve_worker_search_checkpoint(
                        active_search_state=(
                            dict(checkpoint.get("active_search_state") or {})
                            if active_query_index == index
                            else {}
                        ),
                        prefetched_search_state=dict(query_prefetch.get("search_state") or {}),
                        prefetched_raw_path=str(query_prefetch.get("raw_path") or ""),
                    )
                    if active_query_index == index and search_checkpoint:
                        checkpoint["active_search_state"] = dict(search_checkpoint)
                    search_artifact_paths = {
                        str(key): dict(value)
                        for key, value in dict(checkpoint.get("search_artifact_paths") or {}).items()
                        if str(key).strip()
                    }
                    query_artifact_paths = dict(search_artifact_paths.get(str(index)) or {})
                    for key, value in dict(query_prefetch.get("artifact_paths") or {}).items():
                        normalized_key = str(key or "").strip()
                        normalized_value = str(value or "").strip()
                        if normalized_key and normalized_value and normalized_key not in query_artifact_paths:
                            query_artifact_paths[normalized_key] = normalized_value
                    prefetched_raw_path = Path(str(query_prefetch.get("raw_path") or "")).expanduser() if str(query_prefetch.get("raw_path") or "").strip() else None
                    if prefetched_raw_path is not None and prefetched_raw_path.exists():
                        response = _load_prefetched_exploration_search_response(prefetched_raw_path, query_text)
                        raw_search_path = prefetched_raw_path
                        manifest_search_state = _mark_exploration_search_state_as_worker_fetched(
                            dict(query_prefetch.get("search_state") or {}),
                            query_text=query_text,
                        )
                        if manifest_search_state:
                            query_prefetch["search_state"] = manifest_search_state
                            query_prefetch["raw_path"] = str(raw_search_path)
                            _update_exploration_batch_manifest_entry(
                                logger=logger,
                                manifest_path=exploration_dir / "search_batch_manifest.json",
                                task_key=str(query_prefetch.get("task_key") or ""),
                                search_state=manifest_search_state,
                                artifact_paths=query_artifact_paths,
                                raw_path=str(raw_search_path),
                            )
                        checkpoint = {
                            **checkpoint,
                            "search_artifact_paths": {
                                **search_artifact_paths,
                                **({str(index): query_artifact_paths} if query_artifact_paths else {}),
                            },
                            "prefetched_queries": prefetched_queries,
                        }
                    else:
                        search_checkpoint = apply_runtime_timing_overrides_to_search_state(
                            search_checkpoint,
                            runtime_timing_overrides=runtime_timing_overrides,
                        )
                        search_execution = self.search_provider.execute_with_checkpoint(
                            query_text,
                            max_results=10,
                            checkpoint=search_checkpoint,
                        )
                        for artifact in list(search_execution.artifacts or []):
                            artifact_label = str(getattr(artifact, "label", "artifact") or "artifact").strip()
                            if _should_skip_worker_search_artifact(artifact_label, query_artifact_paths):
                                continue
                            artifact_path = _write_search_execution_artifact(
                                logger=logger,
                                artifact=artifact,
                                default_path=raw_search_path,
                                asset_type="exploration_search_queue_payload",
                                source_kind="further_exploration",
                                metadata={
                                    "candidate_id": candidate.candidate_id,
                                    "query": query_text,
                                    "provider_name": search_execution.provider_name,
                                },
                            )
                            query_artifact_paths[artifact_label or "artifact"] = str(artifact_path)
                        if query_artifact_paths:
                            search_artifact_paths[str(index)] = query_artifact_paths
                        checkpoint = {
                            **checkpoint,
                            "active_query_index": index,
                            "active_query_text": query_text,
                            "active_search_state": dict(search_execution.checkpoint or {}),
                            "search_artifact_paths": search_artifact_paths,
                            "prefetched_queries": prefetched_queries,
                        }
                        if search_execution.pending:
                            summary = {
                                "candidate_id": candidate.candidate_id,
                                "display_name": candidate.display_name,
                                "queries": result_summaries,
                                "signals": gathered_signals,
                                "status": "queued",
                                "active_query_index": index,
                                "active_query_text": query_text,
                                "search_state": dict(search_execution.checkpoint or {}),
                                "search_artifact_paths": query_artifact_paths,
                                "message": str(search_execution.message or ""),
                            }
                            self.worker_runtime.complete_worker(
                                worker_handle,
                                status="queued",
                                checkpoint_payload={**checkpoint, "stage": "waiting_remote_search"},
                                output_payload={
                                    "queued_query_index": index,
                                    "candidate": candidate.to_record(),
                                    "evidence": [],
                                    "summary": summary,
                                    "errors": errors,
                                },
                            )
                            return {
                                "candidate": candidate,
                                "evidence": [],
                                "summary": summary,
                                "errors": errors,
                                "worker_status": "queued",
                            }
                        response = search_execution.response
                        if response is None:
                            raise RuntimeError("Search provider returned no response.")
                else:
                    response = self.search_provider.search(query_text, max_results=10)
                if not (worker_handle and self.worker_runtime and prefetched_raw_path is not None and prefetched_raw_path.exists()):
                    raw_search_path = candidate_dir / f"query_{index:02d}.{ 'json' if response.raw_format == 'json' else 'html'}"
                    if response.raw_format == "json":
                        logger.write_json(
                            raw_search_path,
                            search_response_to_record(response),
                            asset_type="exploration_search_payload",
                            source_kind="further_exploration",
                            is_raw_asset=True,
                            model_safe=False,
                            metadata={
                                "candidate_id": candidate.candidate_id,
                                "query": query_text,
                                "provider_name": response.provider_name,
                            },
                        )
                    else:
                        logger.write_text(
                            raw_search_path,
                            str(response.raw_payload or ""),
                            asset_type="exploration_search_html",
                            source_kind="further_exploration",
                            content_type=response.content_type or "text/html",
                            is_raw_asset=True,
                            model_safe=False,
                            metadata={
                                "candidate_id": candidate.candidate_id,
                                "query": query_text,
                                "provider_name": response.provider_name,
                            },
                        )
                if worker_handle and self.worker_runtime:
                    manifest_search_state = _mark_exploration_search_state_as_worker_fetched(
                        dict(query_prefetch.get("search_state") or {}),
                        fallback_state=dict((getattr(search_execution, "checkpoint", None) or {}) or {}),
                        query_text=query_text,
                    )
                    if manifest_search_state:
                        query_prefetch["search_state"] = manifest_search_state
                        query_prefetch["raw_path"] = str(raw_search_path)
                        _update_exploration_batch_manifest_entry(
                            logger=logger,
                            manifest_path=exploration_dir / "search_batch_manifest.json",
                            task_key=str(query_prefetch.get("task_key") or ""),
                            search_state=manifest_search_state,
                            artifact_paths=query_artifact_paths,
                            raw_path=str(raw_search_path),
                        )
                results = [item.to_record() for item in response.results]
                prefetched_queries.pop(str(index), None)
                checkpoint = {
                    key: value
                    for key, value in checkpoint.items()
                    if key not in {"active_query_index", "active_query_text", "active_search_state"}
                }
                if prefetched_queries:
                    checkpoint["prefetched_queries"] = prefetched_queries
                elif "prefetched_queries" in checkpoint:
                    checkpoint.pop("prefetched_queries", None)
            except Exception as exc:
                checkpoint = {
                    key: value
                    for key, value in checkpoint.items()
                    if key not in {"active_query_index", "active_query_text", "active_search_state"}
                }
                if prefetched_queries:
                    checkpoint["prefetched_queries"] = prefetched_queries
                errors.append(f"exploration_search:{candidate.display_name}:{str(exc)[:120]}")
                result_summaries.append({"query": query_text, "raw_path": str(raw_search_path), "error": str(exc)})
                continue

            page_summaries: list[dict[str, Any]] = []
            for result_index, item in enumerate(results[:5], start=1):
                if worker_handle and self.worker_runtime and self.worker_runtime.should_interrupt_worker(worker_handle):
                    interrupted = True
                    break
                page_summary = {"title": item.get("title", ""), "url": item.get("url", "")}
                try:
                    if _is_fetchable_detail_page(item.get("url", "")):
                        analyzed_page = analyze_remote_document(
                            candidate=candidate,
                            target_company=target_company,
                            source_url=item.get("url", ""),
                            asset_dir=candidate_dir,
                            asset_logger=logger,
                            model_client=self.model_client,
                            source_kind="further_exploration",
                            asset_prefix=f"query_{index:02d}_page_{result_index:02d}",
                        )
                        page_summary["detail_path"] = analyzed_page.raw_path
                        page_summary["analysis_input_path"] = analyzed_page.analysis_input_path
                        page_summary["analysis_path"] = analyzed_page.analysis_path
                        if analyzed_page.extracted_text_path:
                            page_summary["extracted_text_path"] = analyzed_page.extracted_text_path
                        page_summary["signals"] = analyzed_page.signals
                        page_summary["analysis"] = analyzed_page.analysis
                        page_summary["document_type"] = analyzed_page.document_type
                        merge_signal_bundle(gathered_signals, analyzed_page.signals)

                        supporting_documents: list[dict[str, Any]] = []
                        for doc_index, resume_url in enumerate(list(analyzed_page.signals.get("resume_urls") or [])[:2], start=1):
                            normalized_resume_url = str(resume_url or "").strip()
                            if not normalized_resume_url or normalized_resume_url == str(analyzed_page.final_url or "").strip():
                                continue
                            try:
                                analyzed_resume = analyze_remote_document(
                                    candidate=candidate,
                                    target_company=target_company,
                                    source_url=normalized_resume_url,
                                    asset_dir=candidate_dir,
                                    asset_logger=logger,
                                    model_client=self.model_client,
                                    source_kind="further_exploration",
                                    asset_prefix=f"query_{index:02d}_page_{result_index:02d}_support_{doc_index:02d}",
                                )
                                supporting_documents.append(
                                    {
                                        "url": analyzed_resume.final_url,
                                        "document_type": analyzed_resume.document_type,
                                        "detail_path": analyzed_resume.raw_path,
                                        "analysis_input_path": analyzed_resume.analysis_input_path,
                                        "analysis_path": analyzed_resume.analysis_path,
                                        "extracted_text_path": analyzed_resume.extracted_text_path,
                                    }
                                )
                                merge_signal_bundle(gathered_signals, analyzed_resume.signals)
                            except Exception as exc:
                                supporting_documents.append({"url": normalized_resume_url, "error": str(exc)})
                        if supporting_documents:
                            page_summary["supporting_documents"] = supporting_documents
                except Exception as exc:
                    page_summary["error"] = str(exc)
                page_summaries.append(page_summary)
            result_summaries.append(
                {
                    "query": query_text,
                    "raw_path": str(raw_search_path),
                    "provider_name": response.provider_name,
                    "result_count": len(results),
                    "pages": page_summaries,
                }
            )
            completed_queries.add(str(index))
            if worker_handle and self.worker_runtime:
                self.worker_runtime.checkpoint_worker(
                    worker_handle,
                    checkpoint_payload={
                        **checkpoint,
                        "completed_queries": sorted(completed_queries),
                        "result_summaries": result_summaries,
                        "gathered_signals": gathered_signals,
                        "errors": errors,
                        "prefetched_queries": dict(checkpoint.get("prefetched_queries") or {}),
                    },
                    output_payload={"explored_query_count": len(completed_queries)},
                )

        merged_candidate, candidate_evidence = _merge_exploration(candidate, gathered_signals, candidate_dir, target_company=target_company)
        summary = {
            "candidate_id": candidate.candidate_id,
            "display_name": candidate.display_name,
            "queries": result_summaries,
            "signals": gathered_signals,
        }
        if interrupted:
            summary["status"] = "interrupted"
        if worker_handle and self.worker_runtime:
            if interrupted:
                self.worker_runtime.complete_worker(
                    worker_handle,
                    status="interrupted",
                    checkpoint_payload={
                        "completed_queries": sorted(completed_queries),
                        "result_summaries": result_summaries,
                        "gathered_signals": gathered_signals,
                        "errors": errors,
                        "prefetched_queries": dict(checkpoint.get("prefetched_queries") or {}),
                        "stage": "interrupted",
                    },
                    output_payload={
                        "signal_counts": {key: len(value) for key, value in gathered_signals.items()},
                        "candidate": merged_candidate.to_record(),
                        "evidence": [item.to_record() for item in candidate_evidence],
                        "summary": summary,
                        "errors": errors,
                    },
                    handoff_to_lane="review_specialist",
                )
            else:
                self.worker_runtime.complete_worker(
                    worker_handle,
                    status="completed",
                    checkpoint_payload={
                        "completed_queries": sorted(completed_queries),
                        "result_summaries": result_summaries,
                        "gathered_signals": gathered_signals,
                        "errors": errors,
                        "prefetched_queries": dict(checkpoint.get("prefetched_queries") or {}),
                        "stage": "completed",
                    },
                    output_payload={
                        "signal_counts": {key: len(value) for key, value in gathered_signals.items()},
                        "candidate": merged_candidate.to_record(),
                        "evidence": [item.to_record() for item in candidate_evidence],
                        "summary": summary,
                        "errors": errors,
                    },
                    handoff_to_lane="enrichment_specialist",
                )
        return {
            "candidate": merged_candidate,
            "evidence": candidate_evidence,
            "summary": summary,
            "errors": errors,
            "worker_status": "interrupted" if interrupted else "completed",
        }


def _prepare_batched_exploration_queries(
    *,
    search_provider: BaseSearchProvider,
    logger: AssetLogger,
    exploration_dir: Path,
    pending_specs: list[dict[str, Any]],
    target_company: str,
) -> list[str]:
    manifest_path = exploration_dir / "search_batch_manifest.json"
    manifest_entries = _load_exploration_batch_manifest_entries(manifest_path)
    pending_by_candidate_id = {
        str(getattr(spec.get("candidate"), "candidate_id", "") or "").strip(): spec
        for spec in pending_specs
        if str(getattr(spec.get("candidate"), "candidate_id", "") or "").strip()
    }
    for candidate_id, spec in pending_by_candidate_id.items():
        prefetched_queries: dict[str, dict[str, Any]] = {}
        for index, query_text in enumerate(_pending_exploration_queries(spec, target_company), start=1):
            task_key = f"{candidate_id}::{index:02d}"
            entry = dict(manifest_entries.get(task_key) or {})
            search_state = dict(entry.get("search_state") or {})
            if str(search_state.get("task_id") or "").strip():
                prefetched_queries[str(index)] = {
                    "task_key": task_key,
                    "query": str(entry.get("query") or query_text),
                    "search_state": search_state,
                    "artifact_paths": dict(entry.get("artifact_paths") or {}),
                    "raw_path": str(entry.get("raw_path") or ""),
                }
        if prefetched_queries:
            spec["prefetched_search_queries"] = prefetched_queries

    unresolved_requests: list[dict[str, Any]] = []
    for candidate_id, spec in pending_by_candidate_id.items():
        existing_prefetch = {
            str(key): dict(value)
            for key, value in dict(spec.get("prefetched_search_queries") or {}).items()
            if str(key).strip()
        }
        for index, query_text in enumerate(_pending_exploration_queries(spec, target_company), start=1):
            if str(dict(existing_prefetch.get(str(index)) or {}).get("search_state", {}).get("task_id") or "").strip():
                continue
            unresolved_requests.append(
                {
                    "task_key": f"{candidate_id}::{index:02d}",
                    "query_text": query_text,
                    "max_results": 10,
                    "runtime_timing_overrides": dict(spec.get("runtime_timing_overrides") or {}),
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
            asset_type="exploration_search_batch_manifest",
            source_kind="further_exploration",
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
            return [f"exploration_batch_submit:{str(exc)[:160]}"]
        if batch_result is None:
            return []

        provider_name = str(getattr(batch_result, "provider_name", "") or provider_name).strip()
        batch_message = str(getattr(batch_result, "message", "") or "")
        submitted_query_count = len(batch_result.tasks or [])
        for artifact in list(batch_result.artifacts or []):
            artifact_label = str(getattr(artifact, "label", "artifact") or "artifact")
            default_path = exploration_dir / f"search_{artifact_label}.json"
            artifact_path = _write_search_execution_artifact(
                logger=logger,
                artifact=artifact,
                default_path=default_path,
                asset_type="exploration_search_batch_queue_payload",
                source_kind="further_exploration",
                metadata={"provider_name": provider_name, "submitted_query_count": len(unresolved_requests)},
            )
            artifact_paths[artifact_label] = str(artifact_path)

        for task in list(batch_result.tasks or []):
            task_key = str(getattr(task, "task_key", "") or "").strip()
            if not task_key:
                continue
            candidate_id, _, query_index = task_key.partition("::")
            query_index = query_index or "01"
            entry = {
                "task_key": task_key,
                "candidate_id": candidate_id,
                "query_index": query_index,
                "query": str(getattr(task, "query_text", "") or "").strip(),
                "search_state": dict(getattr(task, "checkpoint", {}) or {}),
                "artifact_paths": {},
                "metadata": dict(getattr(task, "metadata", {}) or {}),
            }
            artifact_label = str(entry["metadata"].get("artifact_label") or "").strip()
            if artifact_label and artifact_label in artifact_paths:
                entry["artifact_paths"] = {artifact_label: artifact_paths[artifact_label]}
            manifest_entries[task_key] = entry
            spec = pending_by_candidate_id.get(candidate_id)
            if spec is not None:
                prefetched_queries = {
                    str(key): dict(value)
                    for key, value in dict(spec.get("prefetched_search_queries") or {}).items()
                    if str(key).strip()
                }
                prefetched_queries[str(int(query_index))] = {
                    "task_key": task_key,
                    "query": entry["query"],
                    "search_state": dict(entry["search_state"] or {}),
                    "artifact_paths": dict(entry["artifact_paths"] or {}),
                    "raw_path": str(entry.get("raw_path") or ""),
                }
                spec["prefetched_search_queries"] = prefetched_queries
        _write_manifest_snapshot()

    ready_errors = _refresh_batched_exploration_ready_cache(
        search_provider=search_provider,
        logger=logger,
        exploration_dir=exploration_dir,
        manifest_entries=manifest_entries,
        pending_by_candidate_id=pending_by_candidate_id,
        target_company=target_company,
    )
    fetch_errors = _fetch_batched_exploration_ready_results(
        search_provider=search_provider,
        logger=logger,
        exploration_dir=exploration_dir,
        manifest_entries=manifest_entries,
        pending_by_candidate_id=pending_by_candidate_id,
        target_company=target_company,
    )

    if not manifest_entries and not artifact_paths:
        return [*ready_errors, *fetch_errors]
    _write_manifest_snapshot()
    return [*ready_errors, *fetch_errors]


def _refresh_batched_exploration_ready_cache(
    *,
    search_provider: BaseSearchProvider,
    logger: AssetLogger,
    exploration_dir: Path,
    manifest_entries: dict[str, dict[str, Any]],
    pending_by_candidate_id: dict[str, dict[str, Any]],
    target_company: str,
) -> list[str]:
    poll_ready = getattr(search_provider, "poll_ready_batch", None)
    if not callable(poll_ready):
        return []

    poll_specs: list[dict[str, Any]] = []
    attempted_at = _batch_lane_timestamp()
    for candidate_id, spec in pending_by_candidate_id.items():
        for index, query_text in enumerate(_pending_exploration_queries(spec, target_company), start=1):
            task_key = f"{candidate_id}::{index:02d}"
            entry = dict(manifest_entries.get(task_key) or {})
            search_state = dict(entry.get("search_state") or {})
            task_id = str(search_state.get("task_id") or "").strip()
            if not task_id:
                continue
            if str(search_state.get("status") or "").strip() in {"completed", "fetched_cached", "ready_cached"}:
                continue
            ready_poll_min_interval_seconds = resolved_lane_ready_cooldown_seconds(
                search_state,
                default=_lane_ready_poll_min_interval_seconds(),
            )
            if _timestamp_within_seconds(str(search_state.get("ready_attempted_at") or ""), ready_poll_min_interval_seconds):
                continue
            search_state["ready_attempted_at"] = attempted_at
            entry["search_state"] = search_state
            manifest_entries[task_key] = entry
            poll_specs.append(
                {
                    "task_key": task_key,
                    "query_text": str(entry.get("query") or query_text or "").strip(),
                    "checkpoint": search_state,
                }
            )
    if not poll_specs:
        return []

    try:
        ready_result = poll_ready(poll_specs)
    except Exception as exc:
        return [f"exploration_ready_poll:{str(exc)[:160]}"]
    if ready_result is None:
        return []

    poll_token = _batch_lane_timestamp(compact=True)
    poll_artifact_paths: dict[str, str] = {}
    for artifact in list(ready_result.artifacts or []):
        artifact_label = str(getattr(artifact, "label", "artifact") or "artifact")
        artifact_key = f"{artifact_label}_{poll_token}"
        default_path = exploration_dir / f"search_{artifact_key}.json"
        artifact_path = _write_search_execution_artifact(
            logger=logger,
            artifact=artifact,
            default_path=default_path,
            asset_type="exploration_search_ready_poll_payload",
            source_kind="further_exploration",
            metadata={"provider_name": ready_result.provider_name, "poll_token": poll_token},
        )
        poll_artifact_paths[artifact_key] = str(artifact_path)

    for task in list(ready_result.tasks or []):
        task_key = str(getattr(task, "task_key", "") or "").strip()
        if not task_key:
            continue
        candidate_id, _, query_index = task_key.partition("::")
        query_index = str(int(query_index or "1"))
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
        spec = pending_by_candidate_id.get(candidate_id)
        if spec is not None:
            prefetched_queries = {
                str(key): dict(value)
                for key, value in dict(spec.get("prefetched_search_queries") or {}).items()
                if str(key).strip()
            }
            prefetched_queries[query_index] = {
                "task_key": task_key,
                "query": str(entry.get("query") or ""),
                "search_state": search_state,
                "artifact_paths": dict(entry_artifact_paths),
                "raw_path": str(entry.get("raw_path") or ""),
            }
            spec["prefetched_search_queries"] = prefetched_queries
    return []


def _fetch_batched_exploration_ready_results(
    *,
    search_provider: BaseSearchProvider,
    logger: AssetLogger,
    exploration_dir: Path,
    manifest_entries: dict[str, dict[str, Any]],
    pending_by_candidate_id: dict[str, dict[str, Any]],
    target_company: str,
) -> list[str]:
    fetch_ready = getattr(search_provider, "fetch_ready_batch", None)
    if not callable(fetch_ready):
        return []

    fetch_specs: list[dict[str, Any]] = []
    attempted_at = _batch_lane_timestamp()
    for candidate_id, spec in pending_by_candidate_id.items():
        for index, query_text in enumerate(_pending_exploration_queries(spec, target_company), start=1):
            task_key = f"{candidate_id}::{index:02d}"
            entry = dict(manifest_entries.get(task_key) or {})
            search_state = dict(entry.get("search_state") or {})
            if str(search_state.get("status") or "").strip() != "ready_cached":
                continue
            raw_path = str(entry.get("raw_path") or "").strip()
            if raw_path and Path(raw_path).exists():
                continue
            fetch_min_interval_seconds = resolved_lane_fetch_cooldown_seconds(
                search_state,
                default=_lane_fetch_min_interval_seconds(),
            )
            if _timestamp_within_seconds(str(search_state.get("fetch_attempted_at") or ""), fetch_min_interval_seconds):
                continue
            search_state["fetch_attempted_at"] = attempted_at
            search_state["lane_fetch_cooldown_seconds"] = fetch_min_interval_seconds
            entry["search_state"] = search_state
            manifest_entries[task_key] = entry
            fetch_specs.append(
                {
                    "task_key": task_key,
                    "query_text": str(entry.get("query") or query_text or "").strip(),
                    "checkpoint": search_state,
                }
            )
    if not fetch_specs:
        return []

    try:
        fetch_result = fetch_ready(fetch_specs)
    except Exception as exc:
        return [f"exploration_task_get:{str(exc)[:160]}"]
    if fetch_result is None:
        return []

    fetch_token = _batch_lane_timestamp(compact=True)
    fetch_artifact_paths: dict[str, str] = {}
    for artifact in list(fetch_result.artifacts or []):
        artifact_label = str(getattr(artifact, "label", "artifact") or "artifact")
        artifact_key = f"{artifact_label}_{fetch_token}"
        default_path = exploration_dir / f"search_{artifact_key}.json"
        artifact_path = _write_search_execution_artifact(
            logger=logger,
            artifact=artifact,
            default_path=default_path,
            asset_type="exploration_search_task_get_payload",
            source_kind="further_exploration",
            metadata={"provider_name": fetch_result.provider_name, "fetch_token": fetch_token},
        )
        fetch_artifact_paths[artifact_key] = str(artifact_path)

    for task in list(fetch_result.tasks or []):
        task_key = str(getattr(task, "task_key", "") or "").strip()
        if not task_key:
            continue
        candidate_id, _, query_index_text = task_key.partition("::")
        query_index = int(query_index_text or "1")
        entry = dict(manifest_entries.get(task_key) or {})
        spec = pending_by_candidate_id.get(candidate_id)
        response = getattr(task, "response", None)
        if response is None or spec is None:
            continue
        candidate_dir = exploration_dir / candidate_id
        candidate_dir.mkdir(parents=True, exist_ok=True)
        raw_path = candidate_dir / f"query_{query_index:02d}_prefetched.json"
        logger.write_json(
            raw_path,
            search_response_to_record(response),
            asset_type="exploration_search_payload",
            source_kind="further_exploration",
            is_raw_asset=True,
            model_safe=False,
            metadata={
                "candidate_id": candidate_id,
                "query": str(entry.get("query") or ""),
                "provider_name": response.provider_name,
                "prefetched": True,
            },
        )
        search_state = dict(getattr(task, "checkpoint", {}) or {})
        search_state["fetch_attempted_at"] = attempted_at
        search_state["fetched_at"] = _batch_lane_timestamp()
        search_state["fetch_token"] = fetch_token
        search_state["lane_fetch_cooldown_seconds"] = fetch_min_interval_seconds
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
        prefetched_queries = {
            str(key): dict(value)
            for key, value in dict(spec.get("prefetched_search_queries") or {}).items()
            if str(key).strip()
        }
        prefetched_queries[str(query_index)] = {
            "task_key": task_key,
            "query": str(entry.get("query") or ""),
            "search_state": search_state,
            "artifact_paths": dict(entry_artifact_paths),
            "raw_path": str(raw_path),
        }
        spec["prefetched_search_queries"] = prefetched_queries
    return []


def _load_exploration_batch_manifest_entries(manifest_path: Path) -> dict[str, dict[str, Any]]:
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


def _update_exploration_batch_manifest_entry(
    *,
    logger: AssetLogger,
    manifest_path: Path,
    task_key: str,
    search_state: dict[str, Any],
    artifact_paths: dict[str, str],
    raw_path: str,
) -> None:
    if not str(task_key or "").strip():
        return
    payload: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
    entries = _load_exploration_batch_manifest_entries(manifest_path)
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
        asset_type="exploration_search_batch_manifest",
        source_kind="further_exploration",
        is_raw_asset=False,
        model_safe=False,
    )


def _mark_exploration_search_state_as_worker_fetched(
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


def _merge_prefetched_exploration_checkpoint(
    *,
    checkpoint: dict[str, Any],
    prefetched_search_queries: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], bool]:
    updated = dict(checkpoint or {})
    existing_queries = {
        str(key): dict(value)
        for key, value in dict(updated.get("prefetched_queries") or {}).items()
        if str(key).strip()
    }
    applied = False
    for key, value in dict(prefetched_search_queries or {}).items():
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        search_state = dict((value or {}).get("search_state") or {})
        if not str(search_state.get("task_id") or "").strip():
            continue
        existing_entry = dict(existing_queries.get(normalized_key) or {})
        existing_entry_state = dict(existing_entry.get("search_state") or {})
        existing_task_id = str(existing_entry_state.get("task_id") or "").strip()
        prefetched_task_id = str(search_state.get("task_id") or "").strip()
        should_replace = False
        if not existing_task_id:
            should_replace = True
        elif existing_task_id == prefetched_task_id:
            existing_poll = str(existing_entry_state.get("ready_poll_token") or "").strip()
            prefetched_poll = str(search_state.get("ready_poll_token") or "").strip()
            existing_status = str(existing_entry_state.get("status") or "").strip()
            prefetched_status = str(search_state.get("status") or "").strip()
            existing_raw_path = str(existing_entry.get("raw_path") or "").strip()
            prefetched_raw_path = str((value or {}).get("raw_path") or "").strip()
            should_replace = (
                prefetched_poll != existing_poll
                or prefetched_status != existing_status
                or (prefetched_raw_path and prefetched_raw_path != existing_raw_path)
            )
        if not should_replace:
            continue
        existing_queries[normalized_key] = {
            "task_key": str((value or {}).get("task_key") or "").strip(),
            "query": str((value or {}).get("query") or "").strip(),
            "search_state": search_state,
            "artifact_paths": {
                str(path_key): str(path_value)
                for path_key, path_value in dict((value or {}).get("artifact_paths") or {}).items()
                if str(path_key).strip() and str(path_value).strip()
            },
            "raw_path": str((value or {}).get("raw_path") or "").strip(),
        }
        applied = True
    if existing_queries:
        updated["prefetched_queries"] = existing_queries
    active_index = str(int(updated.get("active_query_index") or 0)) if int(updated.get("active_query_index") or 0) > 0 else ""
    if active_index and active_index in existing_queries:
        active_entry = dict(existing_queries.get(active_index) or {})
        active_search_state = dict(updated.get("active_search_state") or {})
        next_search_state = dict(active_entry.get("search_state") or {})
        active_task_id = str(active_search_state.get("task_id") or "").strip()
        next_task_id = str(next_search_state.get("task_id") or "").strip()
        if next_task_id and (not active_task_id or active_task_id == next_task_id):
            existing_poll = str(active_search_state.get("ready_poll_token") or "").strip()
            next_poll = str(next_search_state.get("ready_poll_token") or "").strip()
            existing_status = str(active_search_state.get("status") or "").strip()
            next_status = str(next_search_state.get("status") or "").strip()
            if not active_task_id or existing_poll != next_poll or existing_status != next_status:
                updated["active_search_state"] = next_search_state
                search_artifact_paths = {
                    str(key): dict(value)
                    for key, value in dict(updated.get("search_artifact_paths") or {}).items()
                    if str(key).strip()
                }
                query_artifact_paths = dict(search_artifact_paths.get(active_index) or {})
                for path_key, path_value in dict(active_entry.get("artifact_paths") or {}).items():
                    normalized_key = str(path_key or "").strip()
                    normalized_value = str(path_value or "").strip()
                    if normalized_key and normalized_value and normalized_key not in query_artifact_paths:
                        query_artifact_paths[normalized_key] = normalized_value
                if query_artifact_paths:
                    search_artifact_paths[active_index] = query_artifact_paths
                    updated["search_artifact_paths"] = search_artifact_paths
                applied = True
    return updated, applied


def _resolve_worker_search_checkpoint(
    *,
    active_search_state: dict[str, Any],
    prefetched_search_state: dict[str, Any],
    prefetched_raw_path: str,
) -> dict[str, Any]:
    active_state = dict(active_search_state or {})
    prefetched_state = dict(prefetched_search_state or {})
    if not prefetched_state:
        return active_state
    if not active_state:
        return prefetched_state
    active_task_id = str(active_state.get("task_id") or "").strip()
    prefetched_task_id = str(prefetched_state.get("task_id") or "").strip()
    if prefetched_task_id and active_task_id and prefetched_task_id != active_task_id:
        return active_state
    if str(prefetched_raw_path or "").strip():
        return prefetched_state
    if _search_state_priority(str(prefetched_state.get("status") or "")) >= _search_state_priority(
        str(active_state.get("status") or "")
    ):
        return prefetched_state
    return active_state


def _search_state_priority(status: str) -> int:
    normalized = str(status or "").strip().lower()
    if normalized in {"completed", "fetched_cached"}:
        return 50
    if normalized == "ready_cached":
        return 40
    if normalized == "waiting_for_ready_cached":
        return 30
    if normalized == "waiting_for_ready":
        return 20
    if normalized == "submitted":
        return 10
    return 0


def _should_skip_worker_search_artifact(artifact_label: str, query_artifact_paths: dict[str, Any]) -> bool:
    normalized_label = str(artifact_label or "").strip().lower()
    if normalized_label != "tasks_ready":
        return False
    return any(str(key or "").strip().lower().startswith("tasks_ready_batch") for key in dict(query_artifact_paths or {}).keys())


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


def extract_page_signals(html_text: str, base_url: str) -> dict[str, Any]:
    return _extract_page_signals_impl(html_text, base_url)


def _build_exploration_queries(candidate: Candidate, target_company: str) -> list[str]:
    name = candidate.display_name or candidate.name_en
    metadata = dict(candidate.metadata or {})
    publication_title = str(metadata.get("publication_title") or "").strip()
    queries = [
        f'"{name}" "{target_company}"',
        f'"{name}" "{target_company}" site:linkedin.com/in',
        f'"{name}" "{target_company}" LinkedIn',
        f'"{name}" "{target_company}" X',
        f'"{name}" "{target_company}" GitHub',
        f'"{name}" "{target_company}" CV',
        f'"{name}" "{target_company}" homepage',
    ]
    if publication_title:
        queries.extend(
            [
                f'"{name}" "{publication_title}"',
                f'"{name}" "{publication_title}" "{target_company}"',
                f'"{name}" "{publication_title}" author',
            ]
        )
    deduped: list[str] = []
    for query in queries:
        normalized = " ".join(query.split())
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    return deduped


def _pending_exploration_queries(spec: dict[str, Any], target_company: str) -> list[str]:
    cached_queries = [str(item).strip() for item in list(spec.get("exploration_queries") or []) if str(item).strip()]
    if cached_queries:
        return cached_queries
    candidate = spec.get("candidate")
    if not isinstance(candidate, Candidate):
        return []
    queries = _build_exploration_queries(candidate, target_company)
    spec["exploration_queries"] = list(queries)
    return queries


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


def _is_fetchable_detail_page(url: str) -> bool:
    url = str(url or "").strip().lower()
    if not url.startswith("http"):
        return False
    blocked = ["duckduckgo.com", "google.com/search", "bing.com/search"]
    if is_binary_like_url(url):
        return False
    return not any(item in url for item in blocked)
def _merge_exploration(candidate: Candidate, signals: dict[str, Any], candidate_dir: Path, *, target_company: str) -> tuple[Candidate, list[EvidenceRecord]]:
    notes = candidate.notes
    note_segments: list[str] = []
    if signals["descriptions"]:
        note_segments.append(" | ".join(signals["descriptions"][:2]))
    if signals.get("validated_summaries"):
        note_segments.append(" | ".join(signals["validated_summaries"][:2]))
    if note_segments:
        notes = " ".join([candidate.notes, "Exploration:", " || ".join(note_segments)]).strip()
    metadata = {
        **candidate.metadata,
        "exploration_links": {
            "linkedin": signals["linkedin_urls"],
            "x": signals["x_urls"],
            "github": signals["github_urls"],
            "personal": signals["personal_urls"],
            "resume": signals["resume_urls"],
        },
        "exploration_descriptions": signals["descriptions"][:4],
        "exploration_role_signals": signals.get("role_signals", [])[:8],
        "exploration_validated_summaries": signals.get("validated_summaries", [])[:4],
        "exploration_education_signals": list(signals.get("education_signals") or [])[:6],
        "exploration_work_history_signals": list(signals.get("work_history_signals") or [])[:8],
        "exploration_affiliation_signals": list(signals.get("affiliation_signals") or [])[:6],
        "exploration_document_types": list(signals.get("document_types") or [])[:6],
    }
    candidate_patch = build_candidate_patch_from_signal_bundle(
        candidate,
        signals,
        target_company=target_company,
        source_url=str(candidate_dir),
    )
    incoming = Candidate(
        candidate_id=candidate.candidate_id,
        name_en=candidate.name_en,
        name_zh=candidate.name_zh,
        display_name=candidate.display_name,
        category=candidate.category,
        target_company=candidate.target_company,
        organization=str(candidate_patch.get("organization") or candidate.organization),
        employment_status=candidate.employment_status,
        role=str(candidate_patch.get("role") or candidate.role),
        team=candidate.team,
        focus_areas=str(candidate_patch.get("focus_areas") or candidate.focus_areas),
        education=str(candidate_patch.get("education") or candidate.education),
        work_history=str(candidate_patch.get("work_history") or candidate.work_history),
        notes=notes,
        linkedin_url=str(candidate_patch.get("linkedin_url") or candidate.linkedin_url or (signals["linkedin_urls"][0] if signals["linkedin_urls"] else "")),
        media_url=str(candidate_patch.get("media_url") or candidate.media_url or _pick_media_url(signals)),
        source_dataset=candidate.source_dataset,
        source_path=str(candidate_dir),
        metadata={**metadata, **dict(candidate_patch.get("metadata") or {})},
    )
    merged = merge_candidate(candidate, incoming)
    evidence: list[EvidenceRecord] = []
    for title, urls, source_type in [
        ("Exploration LinkedIn URL", signals["linkedin_urls"][:1], "exploration_linkedin"),
        ("Exploration X URL", signals["x_urls"][:1], "exploration_x"),
        ("Exploration GitHub URL", signals["github_urls"][:1], "exploration_github"),
        ("Exploration Personal URL", signals["personal_urls"][:1], "exploration_personal"),
        ("Exploration Resume URL", signals["resume_urls"][:1], "exploration_resume"),
    ]:
        for url in urls:
            evidence.append(
                EvidenceRecord(
                    evidence_id=make_evidence_id(candidate.candidate_id, source_type, title, url),
                    candidate_id=candidate.candidate_id,
                    source_type=source_type,
                    title=title,
                    url=url,
                    summary=f"Further exploration recovered {title.lower()} for {candidate.display_name}.",
                    source_dataset=source_type,
                    source_path=str(candidate_dir),
                    metadata={"descriptions": signals["descriptions"][:3]},
                )
            )
    if signals["descriptions"]:
        evidence.append(
            EvidenceRecord(
                evidence_id=make_evidence_id(candidate.candidate_id, "exploration_summary", candidate.display_name, str(candidate_dir)),
                candidate_id=candidate.candidate_id,
                source_type="exploration_summary",
                title=f"{candidate.display_name} exploration summary",
                url="",
                summary=" | ".join(signals["descriptions"][:2]),
                source_dataset="exploration_summary",
                source_path=str(candidate_dir),
                metadata={"descriptions": signals["descriptions"][:4]},
            )
        )
    if signals.get("validated_summaries"):
        evidence.append(
            EvidenceRecord(
                evidence_id=make_evidence_id(candidate.candidate_id, "exploration_model_summary", candidate.display_name, str(candidate_dir)),
                candidate_id=candidate.candidate_id,
                source_type="exploration_model_summary",
                title=f"{candidate.display_name} analyzed exploration summary",
                url="",
                summary=" | ".join(signals["validated_summaries"][:2]),
                source_dataset="exploration_model_summary",
                source_path=str(candidate_dir),
                    metadata={
                        "role_signals": signals.get("role_signals", [])[:8],
                        "analysis_notes": signals.get("analysis_notes", [])[:4],
                        "education_signals": list(signals.get("education_signals") or [])[:4],
                        "work_history_signals": list(signals.get("work_history_signals") or [])[:4],
                    },
                )
            )
    return merged, evidence


def _pick_media_url(signals: dict[str, Any]) -> str:
    for key in ["x_urls", "github_urls", "personal_urls", "resume_urls"]:
        if signals[key]:
            return signals[key][0]
    return ""


def _load_prefetched_exploration_search_response(path: Path, query_text: str):
    payload = json.loads(path.read_text(encoding="utf-8"))
    return search_response_from_record(payload, fallback_query_text=query_text)


def build_page_analysis_input(
    *,
    candidate: Candidate,
    target_company: str,
    source_url: str,
    html_text: str,
    extracted_links: dict[str, list[str]],
    max_excerpt_chars: int = DEFAULT_MODEL_CONTEXT_CHAR_LIMIT,
) -> dict[str, Any]:
    return _build_page_analysis_input_impl(
        candidate=candidate,
        target_company=target_company,
        source_url=source_url,
        html_text=html_text,
        extracted_links=extracted_links,
        max_excerpt_chars=max_excerpt_chars,
    )
