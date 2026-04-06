from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time
from typing import Any, Callable

from .asset_logger import AssetLogger
from .connectors import CompanyIdentity
from .domain import Candidate, JobRequest
from .worker_scheduler import lane_budget_caps_from_plan, lane_limits_from_plan, schedule_work_specs


WorkerExecutor = Callable[[dict[str, Any]], dict[str, Any]]


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

    def run(self, specs: list[dict[str, Any]], executor: WorkerExecutor) -> dict[str, Any]:
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
                for future, spec in futures.items():
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
        lease_seconds: int = 300,
        stale_after_seconds: int = 180,
        total_limit: int = 4,
    ) -> None:
        self.store = store
        self.agent_runtime = agent_runtime
        self.acquisition_engine = acquisition_engine
        self.owner_id = owner_id
        self.lease_seconds = max(30, int(lease_seconds or 300))
        self.stale_after_seconds = max(30, int(stale_after_seconds or 180))
        self.total_limit = max(1, int(total_limit or 4))

    def run_once(self) -> dict[str, Any]:
        recoverable = self.store.list_recoverable_agent_workers(
            limit=max(10, self.total_limit * 10),
            stale_after_seconds=self.stale_after_seconds,
        )
        grouped: dict[str, list[dict[str, Any]]] = {}
        for worker in recoverable:
            grouped.setdefault(str(worker.get("job_id") or ""), []).append(worker)

        job_summaries: list[dict[str, Any]] = []
        total_claimed = 0
        total_executed = 0
        for job_id, workers in grouped.items():
            job = self.store.get_job(job_id)
            if not job or str(job.get("status") or "") in {"completed", "failed"}:
                continue
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
            "recoverable_count": len(recoverable),
            "claimed_count": total_claimed,
            "executed_count": total_executed,
            "jobs": job_summaries,
        }

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
            else:
                self.store.release_agent_worker_lease(worker_id, lease_owner=self.owner_id, error_text="unsupported_lane")
                return {
                    "worker_id": worker_id,
                    "lane_id": lane_id,
                    "worker_key": str(worker.get("worker_key") or ""),
                    "worker_status": "skipped",
                    "reason": "unsupported_lane",
                }
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

    def _resume_search_worker(self, worker: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(worker.get("metadata") or {})
        input_payload = dict(worker.get("input") or {})
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
        )

    def _resume_exploration_worker(self, worker: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(worker.get("metadata") or {})
        input_payload = dict(worker.get("input") or {})
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
        )
