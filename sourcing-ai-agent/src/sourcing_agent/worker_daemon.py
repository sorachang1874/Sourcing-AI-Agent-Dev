from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import inspect
from pathlib import Path
import time
from typing import Any, Callable

from .agent_runtime import AgentWorkerHandle
from .asset_logger import AssetLogger
from .connectors import CompanyIdentity
from .domain import Candidate, JobRequest
from .worker_scheduler import lane_budget_caps_from_plan, lane_limits_from_plan, schedule_work_specs


WorkerExecutor = Callable[[dict[str, Any]], dict[str, Any]]
WorkerResultCallback = Callable[[dict[str, Any]], None]


class AutonomousWorkerDaemon:
    def __init__(
        self,
        *,
        existing_workers: list[dict[str, Any]],
        lane_limits: dict[str, int],
        lane_budget_caps: dict[str, int],
        total_limit: int,
        retry_limit: int = 2,
    ) -> None:
        self.existing_workers = list(existing_workers)
        self.lane_limits = dict(lane_limits)
        self.lane_budget_caps = dict(lane_budget_caps)
        self.total_limit = max(1, int(total_limit or 1))
        self.retry_limit = max(0, int(retry_limit or 0))

    @classmethod
    def from_plan(
        cls,
        *,
        plan_payload: dict[str, Any],
        existing_workers: list[dict[str, Any]],
        total_limit: int,
    ) -> "AutonomousWorkerDaemon":
        acquisition_strategy = dict(plan_payload.get("acquisition_strategy") or {})
        cost_policy = dict(acquisition_strategy.get("cost_policy") or {})
        return cls(
            existing_workers=existing_workers,
            lane_limits={
                lane: int(value)
                for lane, value in dict(plan_payload.get("scheduler_lane_limits") or {}).items()
            }
            or lane_limits_from_plan(plan_payload),
            lane_budget_caps=lane_budget_caps_from_plan(plan_payload),
            total_limit=total_limit,
            retry_limit=int(cost_policy.get("worker_retry_limit", 2) or 2),
        )

    def run(
        self,
        specs: list[dict[str, Any]],
        executor: WorkerExecutor,
        *,
        result_callback: WorkerResultCallback | None = None,
    ) -> dict[str, Any]:
        pending: dict[tuple[str, str], dict[str, Any]] = {
            (str(spec.get("lane_id") or ""), str(spec.get("worker_key") or "")): {**spec}
            for spec in specs
        }
        retry_counts: Counter[tuple[str, str]] = Counter()
        lane_budget_used: Counter[str] = Counter()
        results: list[dict[str, Any]] = []
        retried: list[dict[str, Any]] = []
        daemon_events: list[dict[str, Any]] = []
        cycles = 0
        while pending:
            cycles += 1
            available_specs = [
                spec
                for spec in pending.values()
                if lane_budget_used[str(spec.get("lane_id") or "")]
                < int(self.lane_budget_caps.get(str(spec.get("lane_id") or ""), self.total_limit))
            ]
            if not available_specs:
                break
            scheduled = schedule_work_specs(
                available_specs,
                existing_workers=self.existing_workers,
                lane_limits=self.lane_limits,
                total_limit=self.total_limit,
            )
            selected = list(scheduled["selected"])
            if not selected:
                break
            with ThreadPoolExecutor(max_workers=max(1, min(self.total_limit, len(selected)))) as pool:
                futures = {pool.submit(executor, spec): spec for spec in selected}
                for future in as_completed(futures):
                    spec = futures[future]
                    result = future.result()
                    lane_id = str(spec.get("lane_id") or "")
                    worker_key = str(spec.get("worker_key") or "")
                    key = (lane_id, worker_key)
                    lane_budget_used[lane_id] += 1
                    status = str(result.get("worker_status") or "completed")
                    daemon_events.append(
                        {
                            "cycle": cycles,
                            "lane_id": lane_id,
                            "worker_key": worker_key,
                            "status": status,
                            "attempt": int(retry_counts[key]) + 1,
                        }
                    )
                    if status == "failed" and retry_counts[key] < self.retry_limit:
                        retry_counts[key] += 1
                        retried.append(
                            {
                                "lane_id": lane_id,
                                "worker_key": worker_key,
                                "attempt": int(retry_counts[key]),
                            }
                        )
                        continue
                    pending.pop(key, None)
                    results.append(result)
                    if result_callback is not None:
                        try:
                            result_callback(result)
                        except Exception as exc:
                            daemon_events.append(
                                {
                                    "cycle": cycles,
                                    "lane_id": lane_id,
                                    "worker_key": worker_key,
                                    "status": status,
                                    "attempt": int(retry_counts[key]) + 1,
                                    "callback_error": str(exc),
                                }
                            )
            if cycles > max(1, len(specs) * (self.retry_limit + 2)):
                break
        backlog = list(pending.values())
        return {
            "results": results,
            "retried": retried,
            "backlog": backlog,
            "lane_budget_used": {key: int(value) for key, value in lane_budget_used.items()},
            "lane_budget_caps": self.lane_budget_caps,
            "cycles": cycles,
            "daemon_events": daemon_events,
        }


class PersistentWorkerRecoveryDaemon:
    def __init__(
        self,
        *,
        store,
        agent_runtime,
        acquisition_engine,
        owner_id: str,
        completion_callback: WorkerResultCallback | None = None,
        lease_seconds: int = 300,
        stale_after_seconds: int = 180,
        total_limit: int = 4,
        job_id: str = "",
    ) -> None:
        self.store = store
        self.agent_runtime = agent_runtime
        self.acquisition_engine = acquisition_engine
        self.owner_id = owner_id
        self.completion_callback = completion_callback
        self.lease_seconds = max(30, int(lease_seconds or 300))
        self.stale_after_seconds = max(1, 180 if stale_after_seconds in {None, ""} else int(stale_after_seconds))
        self.total_limit = max(1, int(total_limit or 4))
        self.job_id = str(job_id or "")

    def run_once(self) -> dict[str, Any]:
        recoverable = self.store.list_recoverable_agent_workers(
            limit=max(10, self.total_limit * 10),
            stale_after_seconds=self.stale_after_seconds,
            job_id=self.job_id,
        )
        grouped: dict[str, list[dict[str, Any]]] = {}
        for worker in recoverable:
            grouped.setdefault(str(worker.get("job_id") or ""), []).append(worker)

        job_summaries: list[dict[str, Any]] = []
        total_claimed = 0
        total_executed = 0
        for job_id, workers in grouped.items():
            job = self.store.get_job(job_id)
            if not job or str(job.get("status") or "") == "failed":
                continue
            self._refresh_recoverable_search_workers(workers)
            plan_payload = dict(job.get("plan") or {})
            recoverable_ids = {int(worker.get("worker_id") or 0) for worker in workers}
            existing_workers = [
                {
                    **existing_worker,
                    "status": (
                        "interrupted"
                        if int(existing_worker.get("worker_id") or 0) in recoverable_ids
                        and str(existing_worker.get("status") or "") == "running"
                        else str(existing_worker.get("status") or "")
                    ),
                }
                for existing_worker in self.store.list_agent_workers(job_id=job_id)
            ]
            daemon = AutonomousWorkerDaemon.from_plan(
                plan_payload=plan_payload,
                existing_workers=existing_workers,
                total_limit=self.total_limit,
            )
            specs = [
                {
                    "index": int(dict(worker.get("metadata") or {}).get("index") or worker.get("worker_id") or 0),
                    "lane_id": str(worker.get("lane_id") or ""),
                    "worker_key": str(worker.get("worker_key") or ""),
                    "label": str(dict(worker.get("input") or {}).get("query") or dict(worker.get("input") or {}).get("display_name") or ""),
                    "worker_id": int(worker.get("worker_id") or 0),
                }
                for worker in workers
            ]
            scheduled = schedule_work_specs(
                specs,
                existing_workers=existing_workers,
                lane_limits=daemon.lane_limits,
                total_limit=daemon.total_limit,
            )
            selected_specs = list(scheduled.get("selected") or [])
            claimed_workers: list[dict[str, Any]] = []
            for spec in selected_specs:
                claimed = self.store.claim_agent_worker(
                    int(spec.get("existing_worker_id") or spec.get("worker_id") or 0),
                    lease_owner=self.owner_id,
                    lease_seconds=self.lease_seconds,
                )
                if claimed is not None:
                    claimed_workers.append(claimed)
            total_claimed += len(claimed_workers)
            if not claimed_workers:
                if selected_specs:
                    job_summaries.append(
                        {
                            "job_id": job_id,
                            "claimed_count": 0,
                            "executed_count": 0,
                            "backlog_count": len(list(scheduled.get("backlog") or [])),
                        }
                    )
                continue

            run_summary = daemon.run(
                [
                    {
                        "index": int(dict(worker.get("metadata") or {}).get("index") or worker.get("worker_id") or 0),
                        "lane_id": str(worker.get("lane_id") or ""),
                        "worker_key": str(worker.get("worker_key") or ""),
                        "label": str(dict(worker.get("input") or {}).get("query") or dict(worker.get("input") or {}).get("display_name") or ""),
                        "worker_id": int(worker.get("worker_id") or 0),
                    }
                    for worker in claimed_workers
                ],
                executor=lambda spec: self._execute_claimed_worker(int(spec.get("worker_id") or 0)),
                result_callback=self.completion_callback,
            )
            total_executed += len(list(run_summary.get("results") or []))
            job_summaries.append(
                {
                    "job_id": job_id,
                    "claimed_count": len(claimed_workers),
                    "executed_count": len(list(run_summary.get("results") or [])),
                    "backlog_count": len(list(run_summary.get("backlog") or [])),
                    "retried": list(run_summary.get("retried") or []),
                    "lane_budget_used": dict(run_summary.get("lane_budget_used") or {}),
                    "lane_budget_caps": dict(run_summary.get("lane_budget_caps") or {}),
                }
            )

        return {
            "owner_id": self.owner_id,
            "job_id": self.job_id,
            "recoverable_count": len(recoverable),
            "claimed_count": total_claimed,
            "executed_count": total_executed,
            "jobs": job_summaries,
        }

    def _refresh_recoverable_search_workers(self, workers: list[dict[str, Any]]) -> None:
        search_workers = [
            worker
            for worker in workers
            if str(worker.get("lane_id") or "").strip() in {"search_planner", "public_media_specialist"}
        ]
        exploration_workers = [
            worker
            for worker in workers
            if str(worker.get("lane_id") or "").strip() == "exploration_specialist"
        ]
        search_refresher = getattr(self.acquisition_engine.search_seed_acquirer, "refresh_background_search_workers", None)
        if callable(search_refresher) and search_workers:
            self._apply_search_worker_refresh(search_workers, search_refresher(search_workers))
        exploration_refresher = getattr(
            self.acquisition_engine.multi_source_enricher.exploratory_enricher,
            "refresh_background_search_workers",
            None,
        )
        if callable(exploration_refresher) and exploration_workers:
            self._apply_exploration_worker_refresh(exploration_workers, exploration_refresher(exploration_workers))

    def _apply_search_worker_refresh(self, workers: list[dict[str, Any]], refresh_result: dict[str, Any]) -> None:
        for worker in workers:
            worker_id = int(worker.get("worker_id") or 0)
            if worker_id <= 0:
                continue
            update = dict(dict(refresh_result.get("worker_updates") or {}).get(worker_id) or {})
            if not update:
                continue
            checkpoint = dict(worker.get("checkpoint") or {})
            output = dict(worker.get("output") or {})
            search_state = dict(update.get("search_state") or {})
            search_artifact_paths = {
                str(key): str(value)
                for key, value in dict(update.get("search_artifact_paths") or {}).items()
                if str(key).strip() and str(value).strip()
            }
            raw_path = str(update.get("raw_path") or "").strip()
            manifest_path = str(update.get("search_manifest_path") or "").strip()
            manifest_key = str(update.get("search_manifest_key") or "").strip()

            if search_state:
                checkpoint["search_state"] = search_state
                output["search_state"] = search_state
            if search_artifact_paths:
                checkpoint["search_artifact_paths"] = search_artifact_paths
                output["search_artifact_paths"] = search_artifact_paths
            if raw_path:
                checkpoint["raw_path"] = raw_path
            if manifest_path:
                checkpoint["search_manifest_path"] = manifest_path
            if manifest_key:
                checkpoint["search_manifest_key"] = manifest_key

            summary = dict(output.get("summary") or {})
            if raw_path:
                summary["raw_path"] = raw_path
            if search_state:
                summary["search_state"] = search_state
            if summary:
                output["summary"] = summary

            self.store.checkpoint_agent_worker(
                worker_id,
                checkpoint_payload=checkpoint,
                output_payload=output,
                status=str(worker.get("status") or "queued"),
            )

    def _apply_exploration_worker_refresh(self, workers: list[dict[str, Any]], refresh_result: dict[str, Any]) -> None:
        for worker in workers:
            worker_id = int(worker.get("worker_id") or 0)
            if worker_id <= 0:
                continue
            update = dict(dict(refresh_result.get("worker_updates") or {}).get(worker_id) or {})
            prefetched_queries = {
                str(key): dict(value)
                for key, value in dict(update.get("prefetched_queries") or {}).items()
                if str(key).strip()
            }
            if not prefetched_queries:
                continue
            checkpoint = dict(worker.get("checkpoint") or {})
            output = dict(worker.get("output") or {})
            checkpoint["prefetched_queries"] = prefetched_queries

            summary = dict(output.get("summary") or {})
            summary["prefetched_query_count"] = len(prefetched_queries)
            output["summary"] = summary
            output["prefetched_query_count"] = len(prefetched_queries)

            self.store.checkpoint_agent_worker(
                worker_id,
                checkpoint_payload=checkpoint,
                output_payload=output,
                status=str(worker.get("status") or "queued"),
            )

    def run_forever(self, *, poll_seconds: float = 5.0, max_ticks: int = 0) -> dict[str, Any]:
        tick = 0
        last_summary: dict[str, Any] = {}
        while True:
            tick += 1
            last_summary = self.run_once()
            last_summary["tick"] = tick
            if max_ticks > 0 and tick >= max_ticks:
                break
            time.sleep(max(0.1, float(poll_seconds)))
        return last_summary

    def _execute_claimed_worker(self, worker_id: int) -> dict[str, Any]:
        worker = self.store.get_agent_worker(worker_id=worker_id)
        if worker is None:
            return {"worker_id": worker_id, "worker_status": "missing"}
        try:
            lane_id = str(worker.get("lane_id") or "")
            if lane_id in {"search_planner", "public_media_specialist"}:
                result = self._resume_search_worker(worker)
            elif lane_id == "exploration_specialist":
                result = self._resume_exploration_worker(worker)
            elif lane_id == "acquisition_specialist":
                result = self._resume_acquisition_worker(worker)
            elif lane_id == "enrichment_specialist":
                result = self._resume_enrichment_worker(worker)
            else:
                self.store.release_agent_worker_lease(worker_id, lease_owner=self.owner_id, error_text="unsupported_lane")
                return {
                    "worker_id": worker_id,
                    "lane_id": lane_id,
                    "worker_key": str(worker.get("worker_key") or ""),
                    "worker_status": "skipped",
                    "reason": "unsupported_lane",
                }
            self._persist_completed_worker_result_if_needed(worker, result)
            self.store.release_agent_worker_lease(worker_id, lease_owner=self.owner_id)
            return {
                "worker_id": worker_id,
                "lane_id": lane_id,
                "worker_key": str(worker.get("worker_key") or ""),
                "worker_status": str(result.get("worker_status") or "completed"),
                "result": result,
            }
        except Exception as exc:
            checkpoint = dict((worker or {}).get("checkpoint") or {})
            output = dict((worker or {}).get("output") or {})
            output["daemon_error"] = str(exc)
            self.store.complete_agent_worker(
                worker_id,
                status="failed",
                checkpoint_payload=checkpoint,
                output_payload=output,
            )
            self.store.release_agent_worker_lease(worker_id, lease_owner=self.owner_id, error_text=str(exc))
            return {
                "worker_id": worker_id,
                "lane_id": str(worker.get("lane_id") or ""),
                "worker_key": str(worker.get("worker_key") or ""),
                "worker_status": "failed",
                "error": str(exc),
            }

    def _persist_completed_worker_result_if_needed(
        self,
        worker: dict[str, Any],
        result: dict[str, Any],
    ) -> None:
        desired_status = str(result.get("worker_status") or "").strip().lower()
        if desired_status != "completed":
            return
        worker_id = int(worker.get("worker_id") or 0)
        if worker_id <= 0:
            return
        refreshed = self.store.get_agent_worker(worker_id=worker_id) or worker
        current_status = str(refreshed.get("status") or "").strip().lower()
        if current_status == "completed":
            return
        checkpoint = dict(refreshed.get("checkpoint") or {})
        output = dict(refreshed.get("output") or {})
        summary = dict(result.get("summary") or {})
        if summary:
            output["summary"] = summary
        checkpoint["stage"] = "completed"
        checkpoint["status"] = "completed"
        metadata = dict(refreshed.get("metadata") or {})
        recovery_kind = str(metadata.get("recovery_kind") or "").strip()
        if recovery_kind:
            checkpoint.setdefault("recovery_kind", recovery_kind)
        if self.agent_runtime is not None:
            self.agent_runtime.complete_worker(
                AgentWorkerHandle(
                    session_id=int(refreshed.get("session_id") or 0),
                    span_id=int(refreshed.get("span_id") or 0),
                    worker_id=worker_id,
                    lane_id=str(refreshed.get("lane_id") or ""),
                    worker_key=str(refreshed.get("worker_key") or ""),
                ),
                status="completed",
                checkpoint_payload=checkpoint,
                output_payload=output,
            )
            return
        span_id = int(refreshed.get("span_id") or 0)
        if span_id > 0:
            self.store.complete_agent_trace_span(
                span_id,
                status="completed",
                output_payload=output,
            )
        self.store.complete_agent_worker(
            worker_id,
            status="completed",
            checkpoint_payload=checkpoint,
            output_payload=output,
        )

    def _resume_search_worker(self, worker: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(worker.get("metadata") or {})
        input_payload = dict(worker.get("input") or {})
        checkpoint = dict(worker.get("checkpoint") or {})
        identity = CompanyIdentity(**dict(metadata.get("identity") or {}))
        snapshot_dir = Path(str(metadata.get("snapshot_dir") or "")).expanduser()
        discovery_dir = Path(str(metadata.get("discovery_dir") or snapshot_dir / "search_seed_discovery")).expanduser()
        return self.acquisition_engine.search_seed_acquirer._execute_query_spec(
            index=int(metadata.get("index") or input_payload.get("index") or 1),
            query_spec=dict(input_payload.get("query_spec") or {}),
            identity=identity,
            discovery_dir=discovery_dir,
            logger=AssetLogger(snapshot_dir),
            employment_status=str(metadata.get("employment_status") or "current"),
            worker_runtime=self.agent_runtime,
            job_id=str(worker.get("job_id") or ""),
            request_payload=dict(metadata.get("request_payload") or {}),
            plan_payload=dict(metadata.get("plan_payload") or {}),
            runtime_mode=str(metadata.get("runtime_mode") or "daemon_recovery"),
            result_limit=int(metadata.get("result_limit") or 10),
            prefetched_search_state=dict(checkpoint.get("search_state") or {}),
            prefetched_search_artifact_paths=dict(checkpoint.get("search_artifact_paths") or {}),
            prefetched_search_raw_path=str(checkpoint.get("raw_path") or ""),
            prefetched_search_manifest_path=str(checkpoint.get("search_manifest_path") or ""),
            prefetched_search_manifest_key=str(
                checkpoint.get("search_manifest_key") or worker.get("worker_key") or ""
            ),
        )

    def _resume_exploration_worker(self, worker: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(worker.get("metadata") or {})
        input_payload = dict(worker.get("input") or {})
        checkpoint = dict(worker.get("checkpoint") or {})
        candidate = Candidate(**dict(input_payload.get("candidate") or {}))
        snapshot_dir = Path(str(metadata.get("snapshot_dir") or "")).expanduser()
        return self.acquisition_engine.multi_source_enricher.exploratory_enricher._explore_candidate(
            snapshot_dir=snapshot_dir,
            candidate=candidate,
            target_company=str(metadata.get("target_company") or ""),
            logger=AssetLogger(snapshot_dir),
            job_id=str(worker.get("job_id") or ""),
            request_payload=dict(metadata.get("request_payload") or {}),
            plan_payload=dict(metadata.get("plan_payload") or {}),
            runtime_mode=str(metadata.get("runtime_mode") or "daemon_recovery"),
            prefetched_search_queries={
                str(key): dict(value)
                for key, value in dict(checkpoint.get("prefetched_queries") or {}).items()
                if str(key).strip()
            },
        )

    def _resume_acquisition_worker(self, worker: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(worker.get("metadata") or {})
        recovery_kind = str(metadata.get("recovery_kind") or "")
        if recovery_kind != "harvest_company_employees":
            return {
                "worker_status": "skipped",
                "reason": "unsupported_recovery_kind",
                "recovery_kind": recovery_kind,
            }
        identity = CompanyIdentity(**dict(metadata.get("identity") or {}))
        snapshot_dir = Path(str(metadata.get("snapshot_dir") or "")).expanduser()
        return self._invoke_with_supported_kwargs(
            self.acquisition_engine._execute_harvest_company_roster_worker,
            identity=identity,
            snapshot_dir=snapshot_dir,
            max_pages=int(metadata.get("max_pages") or 10),
            page_limit=int(metadata.get("page_limit") or 25),
            job_id=str(worker.get("job_id") or ""),
            request_payload=dict(metadata.get("request_payload") or {}),
            plan_payload=dict(metadata.get("plan_payload") or {}),
            runtime_mode=str(metadata.get("runtime_mode") or "daemon_recovery"),
            allow_shared_provider_cache=bool(metadata.get("allow_shared_provider_cache", True)),
            company_filters=dict(metadata.get("company_filters") or {}),
            worker_key_suffix=str(metadata.get("worker_key_suffix") or ""),
            span_name_suffix=str(metadata.get("span_name_suffix") or ""),
            root_snapshot_dir=Path(
                str(metadata.get("root_snapshot_dir") or metadata.get("snapshot_dir") or "")
            ).expanduser(),
        )

    def _resume_enrichment_worker(self, worker: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(worker.get("metadata") or {})
        recovery_kind = str(metadata.get("recovery_kind") or "")
        if recovery_kind != "harvest_profile_batch":
            return {
                "worker_status": "skipped",
                "reason": "unsupported_recovery_kind",
                "recovery_kind": recovery_kind,
            }
        snapshot_dir = Path(str(metadata.get("snapshot_dir") or "")).expanduser()
        return self.acquisition_engine.multi_source_enricher._execute_harvest_profile_batch_worker(
            profile_urls=list(metadata.get("profile_urls") or []),
            snapshot_dir=snapshot_dir,
            job_id=str(worker.get("job_id") or ""),
            request_payload=dict(metadata.get("request_payload") or {}),
            plan_payload=dict(metadata.get("plan_payload") or {}),
            runtime_mode=str(metadata.get("runtime_mode") or "daemon_recovery"),
            allow_shared_provider_cache=bool(metadata.get("allow_shared_provider_cache", True)),
        )

    @staticmethod
    def _invoke_with_supported_kwargs(func: Callable[..., dict[str, Any]], /, **kwargs: Any) -> dict[str, Any]:
        signature = inspect.signature(func)
        parameters = signature.parameters
        if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
            return func(**kwargs)
        supported_kwargs = {
            key: value
            for key, value in kwargs.items()
            if key in parameters
        }
        return func(**supported_kwargs)
