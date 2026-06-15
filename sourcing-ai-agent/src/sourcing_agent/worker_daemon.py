from __future__ import annotations

import inspect
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

from .agent_runtime import AgentWorkerHandle
from .asset_logger import AssetLogger
from .connectors import CompanyIdentity
from .crm_public_web_runtime import CRM_PUBLIC_WEB_WORKER_RECOVERY_KIND
from .domain import Candidate
from .runtime_environment import iter_runtime_namespace_path_values, runtime_namespace_matches_path
from .worker_scheduler import lane_budget_caps_from_plan, lane_limits_from_plan, schedule_work_specs
from .workflow_event_response import worker_has_remote_provider_terminal_event_marker

WorkerExecutor = Callable[[dict[str, Any]], dict[str, Any]]
WorkerResultCallback = Callable[[dict[str, Any]], None]

# Historical target-candidate Public Web workers are quarantined here only so
# old rows can terminalize without importing the retired execution facade.
RETIRED_TARGET_PUBLIC_WEB_WORKER_RECOVERY_KIND = "target_candidate_public_web_search"


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _dedupe_texts(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        text = " ".join(str(item or "").split()).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _payload_candidate_count(payload: Any) -> int:
    candidate_ids: set[str] = set()
    count_values: list[int] = []

    def _visit(value: Any) -> None:
        if isinstance(value, dict):
            direct_id = str(value.get("candidate_id") or "").strip()
            if direct_id:
                candidate_ids.add(direct_id)
            direct_ids = value.get("candidate_ids")
            if isinstance(direct_ids, list):
                candidate_ids.update(_dedupe_texts(direct_ids))
            for key in (
                "candidate_count",
                "resolved_candidate_count",
                "board_visible_candidate_count",
                "card_count",
                "requested_url_count",
            ):
                if key in value:
                    count_values.append(max(0, _coerce_int(value.get(key), 0)))
            for nested in value.values():
                _visit(nested)
            return
        if isinstance(value, list):
            for item in value:
                _visit(item)

    _visit(payload)
    if candidate_ids:
        return len(candidate_ids)
    return max(count_values) if count_values else 0


def _worker_is_already_submitted_remote_wait(worker: dict[str, Any]) -> bool:
    payload = dict(worker or {})
    checkpoint = dict(payload.get("checkpoint") or {})
    metadata = dict(payload.get("metadata") or {})
    output = dict(payload.get("output") or {})
    summary = dict(output.get("summary") or {})
    status = str(payload.get("status") or "").strip().lower()
    stage = str(checkpoint.get("stage") or "").strip().lower()
    if stage not in {"waiting_remote_harvest", "waiting_remote_search"} and status not in {
        "waiting_remote_harvest",
        "waiting_remote_search",
    }:
        return False
    if worker_has_remote_provider_terminal_event_marker(payload):
        return False
    recovery_kind = str(metadata.get("recovery_kind") or checkpoint.get("recovery_kind") or "").strip()
    if recovery_kind not in {"harvest_profile_batch", "harvest_company_employees", "search_seed_discovery"}:
        return False
    remote_refs = (
        checkpoint.get("run_id"),
        checkpoint.get("dataset_id"),
        checkpoint.get("actor_run_id"),
        checkpoint.get("defaultDatasetId"),
        metadata.get("run_id"),
        metadata.get("dataset_id"),
        summary.get("run_id"),
        summary.get("dataset_id"),
    )
    return any(str(value or "").strip() for value in remote_refs)


class AutonomousWorkerDaemon:
    def __init__(
        self,
        *,
        existing_workers: list[dict[str, Any]],
        lane_limits: dict[str, int],
        lane_budget_caps: dict[str, int],
        total_limit: int,
        retry_limit: int = 2,
        executor_parallelism: int | None = None,
    ) -> None:
        self.existing_workers = list(existing_workers)
        self.lane_limits = dict(lane_limits)
        self.lane_budget_caps = dict(lane_budget_caps)
        self.total_limit = max(1, int(total_limit or 1))
        self.retry_limit = max(0, int(retry_limit or 0))
        self.executor_parallelism = max(1, int(executor_parallelism or self.total_limit))

    @classmethod
    def from_plan(
        cls,
        *,
        plan_payload: dict[str, Any],
        existing_workers: list[dict[str, Any]],
        total_limit: int,
        executor_parallelism: int | None = None,
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
            executor_parallelism=executor_parallelism,
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
            if self.executor_parallelism <= 1:
                for spec in selected:
                    self._record_completed_result(
                        spec=spec,
                        result=executor(spec),
                        cycle=cycles,
                        retry_counts=retry_counts,
                        lane_budget_used=lane_budget_used,
                        pending=pending,
                        results=results,
                        retried=retried,
                        daemon_events=daemon_events,
                        result_callback=result_callback,
                    )
            else:
                with ThreadPoolExecutor(
                    max_workers=max(1, min(self.executor_parallelism, self.total_limit, len(selected)))
                ) as pool:
                    futures = {pool.submit(executor, spec): spec for spec in selected}
                    for future in as_completed(futures):
                        self._record_completed_result(
                            spec=futures[future],
                            result=future.result(),
                            cycle=cycles,
                            retry_counts=retry_counts,
                            lane_budget_used=lane_budget_used,
                            pending=pending,
                            results=results,
                            retried=retried,
                            daemon_events=daemon_events,
                            result_callback=result_callback,
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

    def _record_completed_result(
        self,
        *,
        spec: dict[str, Any],
        result: dict[str, Any],
        cycle: int,
        retry_counts: Counter[tuple[str, str]],
        lane_budget_used: Counter[str],
        pending: dict[tuple[str, str], dict[str, Any]],
        results: list[dict[str, Any]],
        retried: list[dict[str, Any]],
        daemon_events: list[dict[str, Any]],
        result_callback: WorkerResultCallback | None,
    ) -> None:
        lane_id = str(spec.get("lane_id") or "")
        worker_key = str(spec.get("worker_key") or "")
        key = (lane_id, worker_key)
        lane_budget_used[lane_id] += 1
        status = str(result.get("worker_status") or "completed")
        daemon_events.append(
            {
                "cycle": cycle,
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
            return
        pending.pop(key, None)
        results.append(result)
        if result_callback is not None:
            try:
                result_callback(result)
            except Exception as exc:
                daemon_events.append(
                    {
                        "cycle": cycle,
                        "lane_id": lane_id,
                        "worker_key": worker_key,
                        "status": status,
                        "attempt": int(retry_counts[key]) + 1,
                        "callback_error": str(exc),
                    }
                )


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
        explicit_worker_ids: list[int] | None = None,
        force_release_explicit_worker_leases: bool = False,
        runtime_dir: str | Path | None = None,
        phase_budget_ms: int = 0,
        candidate_limit: int = 0,
    ) -> None:
        self.store = store
        self.agent_runtime = agent_runtime
        self.acquisition_engine = acquisition_engine
        self.owner_id = owner_id
        self.completion_callback = completion_callback
        self.lease_seconds = max(30, int(lease_seconds or 300))
        self.stale_after_seconds = max(0, 180 if stale_after_seconds in {None, ""} else int(stale_after_seconds))
        self.total_limit = max(1, int(total_limit or 4))
        self.job_id = str(job_id or "")
        self.explicit_worker_ids = []
        for worker_id in list(explicit_worker_ids or []):
            try:
                normalized_worker_id = int(worker_id or 0)
            except (TypeError, ValueError):
                continue
            if normalized_worker_id > 0 and normalized_worker_id not in self.explicit_worker_ids:
                self.explicit_worker_ids.append(normalized_worker_id)
        self.force_release_explicit_worker_leases = bool(force_release_explicit_worker_leases)
        self.runtime_dir = Path(runtime_dir).expanduser().resolve() if str(runtime_dir or "").strip() else None
        self.phase_budget_ms = max(0, int(phase_budget_ms or 0))
        self.candidate_limit = max(0, int(candidate_limit or 0))

    def _runtime_namespace_matches_path(self, value: Any) -> bool:
        return runtime_namespace_matches_path(value, configured_runtime_dir=self.runtime_dir)

    def _worker_runtime_namespace_matches(self, worker: dict[str, Any]) -> bool:
        metadata = dict(worker.get("metadata") or {})
        checkpoint = dict(worker.get("checkpoint") or {})
        output = dict(worker.get("output") or {})
        payload = {"metadata": metadata, "checkpoint": checkpoint, "output": output}
        for _, path_value in iter_runtime_namespace_path_values(payload):
            if not self._runtime_namespace_matches_path(path_value):
                return False
        return True

    @staticmethod
    def _worker_candidate_count(worker: dict[str, Any]) -> int:
        payload = dict(worker or {})
        output = dict(payload.get("output") or {})
        checkpoint = dict(payload.get("checkpoint") or {})
        input_payload = dict(payload.get("input") or {})
        for candidate_payload in (
            dict(output.get("inline_incremental_apply") or {}),
            dict(output.get("inline_incremental_ingest") or {}),
            dict(output.get("summary") or {}),
            output,
            checkpoint,
            input_payload,
        ):
            candidate_count = _payload_candidate_count(candidate_payload)
            if candidate_count > 0:
                return candidate_count
            for key in (
                "profile_urls",
                "requested_profile_urls",
                "linkedin_profile_urls",
                "urls",
                "requested_urls",
            ):
                values = candidate_payload.get(key)
                if isinstance(values, list) and values:
                    return len(_dedupe_texts(values))
        return 0

    def run_once(self) -> dict[str, Any]:
        started_monotonic = time.perf_counter()
        explicit_worker_scope = bool(self.explicit_worker_ids)
        explicit_recoverable_by_id: dict[int, dict[str, Any]] = {}
        explicit_worker_count = 0
        for worker_id in self.explicit_worker_ids:
            worker = self.store.get_agent_worker(worker_id=worker_id)
            if not worker:
                continue
            if self.job_id and str(worker.get("job_id") or "") != self.job_id:
                continue
            if str(worker.get("status") or "").strip().lower() == "completed":
                continue
            if self.force_release_explicit_worker_leases:
                self.store.release_agent_worker_lease(
                    worker_id,
                    error_text="terminal_remote_provider_event_wakeup",
                )
                worker = self.store.get_agent_worker(worker_id=worker_id) or worker
            explicit_recoverable_by_id[worker_id] = dict(worker)
            explicit_worker_count += 1

        # Explicit worker wakeups are a per-tick prioritization contract, not a
        # daemon lifetime mode. Once the explicit worker has completed or gone
        # away, the same job-scoped recovery service must resume normal scans so
        # later partial terminal-persist workers and refill work cannot starve.
        explicit_scan_suppressed = bool(explicit_recoverable_by_id)
        recoverable = (
            []
            if explicit_scan_suppressed
            else self.store.list_recoverable_agent_workers(
                limit=max(10, self.total_limit * 10),
                stale_after_seconds=self.stale_after_seconds,
                job_id=self.job_id,
            )
        )
        recoverable_by_id = {int(worker.get("worker_id") or 0): dict(worker) for worker in recoverable}
        recoverable_by_id.update(explicit_recoverable_by_id)
        recoverable = list(recoverable_by_id.values())
        runtime_namespace_skipped_count = 0
        remote_wait_skipped_workers: list[dict[str, Any]] = []
        if recoverable:
            filtered_recoverable: list[dict[str, Any]] = []
            explicit_ids = set(explicit_recoverable_by_id)
            for worker in recoverable:
                worker_id = int(worker.get("worker_id") or 0)
                if worker_id not in explicit_ids and _worker_is_already_submitted_remote_wait(dict(worker)):
                    remote_wait_skipped_workers.append(dict(worker))
                    continue
                filtered_recoverable.append(dict(worker))
            recoverable = filtered_recoverable
        if self.runtime_dir is not None:
            filtered: list[dict[str, Any]] = []
            for worker in recoverable:
                if self._worker_runtime_namespace_matches(worker):
                    filtered.append(worker)
                else:
                    runtime_namespace_skipped_count += 1
            recoverable = filtered
        grouped: dict[str, list[dict[str, Any]]] = {}
        for worker in recoverable:
            grouped.setdefault(str(worker.get("job_id") or ""), []).append(worker)

        job_summaries: list[dict[str, Any]] = []
        total_claimed = 0
        total_executed = 0
        total_candidate_count = 0
        elapsed_budget_exhausted = False
        candidate_budget_exhausted = False

        def _elapsed_ms() -> int:
            return int(max(0.0, (time.perf_counter() - started_monotonic) * 1000))

        for job_id, workers in grouped.items():
            elapsed_ms = _elapsed_ms()
            if total_claimed > 0 and self.phase_budget_ms > 0 and elapsed_ms >= self.phase_budget_ms:
                elapsed_budget_exhausted = True
                break
            job = self.store.get_job(job_id)
            if not job or str(job.get("status") or "") == "failed":
                continue
            if not self._runtime_namespace_matches_path(job.get("artifact_path")):
                runtime_namespace_skipped_count += len(workers)
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
                executor_parallelism=1,
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
            worker_by_id = {int(worker.get("worker_id") or 0): dict(worker) for worker in workers}
            budgeted_selected_specs: list[dict[str, Any]] = []
            selected_candidate_count = 0
            for spec in selected_specs:
                elapsed_ms = _elapsed_ms()
                if (
                    budgeted_selected_specs
                    and self.phase_budget_ms > 0
                    and elapsed_ms >= self.phase_budget_ms
                ):
                    elapsed_budget_exhausted = True
                    break
                worker_id = int(spec.get("existing_worker_id") or spec.get("worker_id") or 0)
                worker_candidate_count = self._worker_candidate_count(worker_by_id.get(worker_id, {}))
                if (
                    budgeted_selected_specs
                    and self.candidate_limit > 0
                    and selected_candidate_count + max(0, worker_candidate_count) > self.candidate_limit
                ):
                    candidate_budget_exhausted = True
                    break
                budgeted_selected_specs.append(dict(spec))
                selected_candidate_count += max(0, worker_candidate_count)
            selected_specs = budgeted_selected_specs
            if not selected_specs:
                if candidate_budget_exhausted:
                    break
                continue
            claimed_workers: list[dict[str, Any]] = []
            results: list[dict[str, Any]] = []
            daemon_events: list[dict[str, Any]] = []
            retried: list[dict[str, Any]] = []
            lane_budget_used: Counter[str] = Counter()
            job_candidate_count = 0
            attempted_selected_count = 0
            for spec in selected_specs:
                worker_id = int(spec.get("existing_worker_id") or spec.get("worker_id") or 0)
                worker_candidate_count = max(0, self._worker_candidate_count(worker_by_id.get(worker_id, {})))
                if total_claimed > 0 and self.phase_budget_ms > 0 and _elapsed_ms() >= self.phase_budget_ms:
                    elapsed_budget_exhausted = True
                    break
                if (
                    total_claimed > 0
                    and self.candidate_limit > 0
                    and total_candidate_count + worker_candidate_count > self.candidate_limit
                ):
                    candidate_budget_exhausted = True
                    break
                attempted_selected_count += 1
                claimed = self.store.claim_agent_worker(
                    worker_id,
                    lease_owner=self.owner_id,
                    lease_seconds=self.lease_seconds,
                )
                if claimed is None:
                    continue
                claimed_workers.append(claimed)
                total_claimed += 1
                job_candidate_count += worker_candidate_count
                total_candidate_count += worker_candidate_count
                lane_id = str(spec.get("lane_id") or "")
                lane_budget_used[lane_id] += 1
                result = self._execute_claimed_worker(worker_id)
                results.append(result)
                daemon_events.append(
                    {
                        "cycle": 1,
                        "lane_id": lane_id,
                        "worker_key": str(spec.get("worker_key") or ""),
                        "status": str(result.get("worker_status") or "completed"),
                        "attempt": 1,
                    }
                )
                total_executed += 1
                if self.phase_budget_ms > 0 and _elapsed_ms() >= self.phase_budget_ms:
                    elapsed_budget_exhausted = True
                    break
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
            callback_results: list[dict[str, Any]] = []
            callback_failures: list[dict[str, Any]] = []
            if callable(self.completion_callback):
                for result in results:
                    try:
                        callback_result = self.completion_callback(dict(result))
                    except Exception as exc:
                        callback_failures.append(
                            {
                                "worker_id": int(dict(result or {}).get("worker_id") or 0),
                                "worker_key": str(dict(result or {}).get("worker_key") or ""),
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )
                        continue
                    if isinstance(callback_result, dict):
                        callback_results.append(dict(callback_result))
            backlog_count = len(list(scheduled.get("backlog") or [])) + max(
                0,
                len(selected_specs) - attempted_selected_count,
            )
            job_summaries.append(
                {
                    "job_id": job_id,
                    "claimed_count": len(claimed_workers),
                    "executed_count": len(results),
                    "completion_callback_count": len(callback_results),
                    "completion_callback_failure_count": len(callback_failures),
                    "backlog_count": backlog_count,
                    "retried": retried,
                    "lane_budget_used": {key: int(value) for key, value in lane_budget_used.items()},
                    "lane_budget_caps": dict(daemon.lane_budget_caps),
                    "completion_callback_results": callback_results,
                    "completion_callback_failures": callback_failures,
                    "candidate_count": job_candidate_count,
                    "daemon_events": daemon_events,
                }
            )
            if candidate_budget_exhausted:
                break
            if elapsed_budget_exhausted:
                break

        return {
            "owner_id": self.owner_id,
            "job_id": self.job_id,
            "recoverable_count": len(recoverable),
            "explicit_worker_count": explicit_worker_count,
            "explicit_worker_scope": explicit_worker_scope,
            "explicit_worker_scan_suppressed": explicit_scan_suppressed,
            "runtime_namespace_skipped_count": runtime_namespace_skipped_count,
            "remote_wait_skipped_count": len(remote_wait_skipped_workers),
            "remote_wait_skipped_worker_ids": [
                int(worker.get("worker_id") or 0)
                for worker in remote_wait_skipped_workers
                if int(worker.get("worker_id") or 0) > 0
            ],
            "claimed_count": total_claimed,
            "executed_count": total_executed,
            "candidate_count": total_candidate_count,
            "phase_budget_ms": self.phase_budget_ms,
            "candidate_limit": self.candidate_limit,
            "elapsed_ms": _elapsed_ms(),
            "elapsed_budget_exhausted": elapsed_budget_exhausted,
            "candidate_budget_exhausted": candidate_budget_exhausted,
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
        recovery_kind = str(metadata.get("recovery_kind") or "")
        if recovery_kind == CRM_PUBLIC_WEB_WORKER_RECOVERY_KIND:
            input_payload = dict(worker.get("input") or {})
            run_id = str(metadata.get("run_id") or input_payload.get("run_id") or "").strip()
            return {
                "worker_status": "completed",
                "run_id": run_id,
                "summary": {
                    "owner": "crm_public_web_phase_commands",
                    "reason": "crm_public_web_agent_worker_recovery_retired",
                    "legacy_worker_quarantined": True,
                    "run_id": run_id,
                    "public_web_storage_owner": "crm_public_web_v1",
                    "execution_backend": "crm_public_web_v1",
                    "migration_phase": "W7f_crm_public_web_worker_daemon_quarantine",
                },
            }
        if recovery_kind == RETIRED_TARGET_PUBLIC_WEB_WORKER_RECOVERY_KIND:
            input_payload = dict(worker.get("input") or {})
            run_id = str(metadata.get("run_id") or input_payload.get("run_id") or "").strip()
            return {
                "worker_status": "completed",
                "run_id": run_id,
                "summary": {
                    "owner": "legacy_target_public_web_migration_only",
                    "reason": "legacy_target_public_web_agent_worker_recovery_retired",
                    "legacy_worker_quarantined": True,
                    "run_id": run_id,
                    "public_web_storage_owner": "target_candidate_public_web_v1",
                    "execution_backend": "target_candidate_public_web_v1",
                    "migration_phase": "W7e_legacy_target_public_web_worker_daemon_quarantine",
                },
            }
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
        checkpoint = dict(worker.get("checkpoint") or {})
        recovery_kind = str(metadata.get("recovery_kind") or "")
        if recovery_kind != "harvest_profile_batch":
            return {
                "worker_status": "skipped",
                "reason": "unsupported_recovery_kind",
                "recovery_kind": recovery_kind,
            }
        snapshot_dir = Path(str(metadata.get("snapshot_dir") or "")).expanduser()
        prefetch_batch_context = dict(
            metadata.get("prefetch_batch_context")
            or checkpoint.get("prefetch_batch_context")
            or {}
        )
        prefetch_batch_context.setdefault("nonblocking_submit", True)
        prefetch_batch_context.setdefault("recovery_submit_policy", "nonblocking_provider_handoff")
        return self.acquisition_engine.multi_source_enricher._execute_harvest_profile_batch_worker(
            profile_urls=list(metadata.get("profile_urls") or []),
            snapshot_dir=snapshot_dir,
            job_id=str(worker.get("job_id") or ""),
            request_payload=dict(metadata.get("request_payload") or {}),
            plan_payload=dict(metadata.get("plan_payload") or {}),
            runtime_mode=str(metadata.get("runtime_mode") or "daemon_recovery"),
            allow_shared_provider_cache=bool(metadata.get("allow_shared_provider_cache", True)),
            prefetch_batch_context=prefetch_batch_context,
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
