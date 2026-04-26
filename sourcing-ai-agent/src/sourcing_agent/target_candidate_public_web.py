from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .public_web_search import (
    DEFAULT_TARGET_CANDIDATE_SOURCE_FAMILIES,
    CandidateSearchOutcome,
    ClassifiedEntryLink,
    PublicWebCandidateContext,
    PublicWebExperimentOptions,
    PublicWebModelClient,
    PublicWebQuerySpec,
    candidate_context_from_target_candidate,
    execute_single_candidate_searches,
    fetch_ready_specs_isolated,
    finalize_candidate_public_web_experiment,
    is_clean_profile_link,
    normalize_public_web_url_key,
    plan_candidate_public_web_queries,
    prepare_candidate_search_plan,
    public_web_link_shape_warnings,
    record_candidate_search_response,
    write_search_execution_artifacts,
)
from .search_provider import BaseSearchProvider

PUBLIC_WEB_JOB_TYPE = "target_candidate_public_web_search"
PUBLIC_WEB_WORKER_LANE = "exploration_specialist"
PUBLIC_WEB_WORKER_RECOVERY_KIND = "target_candidate_public_web_search"
PUBLIC_WEB_TERMINAL_STATUSES = {
    "completed",
    "completed_with_errors",
    "needs_review",
    "failed",
    "cancelled",
}


def start_target_candidate_public_web_batch(
    *,
    store: Any,
    target_candidates: list[dict[str, Any]],
    runtime_dir: str | Path,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_payload = dict(payload or {})
    options = normalize_public_web_product_options(request_payload)
    force_refresh = bool(request_payload.get("force_refresh"))
    requested_by = str(request_payload.get("requested_by") or request_payload.get("user_id") or "").strip()
    requested_record_ids = [
        str(record.get("id") or record.get("record_id") or "").strip()
        for record in target_candidates
        if str(record.get("id") or record.get("record_id") or "").strip()
    ]
    idempotency_key = build_public_web_batch_idempotency_key(
        requested_record_ids=requested_record_ids,
        options=options,
        force_refresh=force_refresh,
        nonce=str(request_payload.get("refresh_nonce") or request_payload.get("nonce") or "") if force_refresh else "",
    )
    if not force_refresh:
        existing = store.get_target_candidate_public_web_batch(idempotency_key=idempotency_key)
        if existing is not None:
            runs = store.list_target_candidate_public_web_runs(batch_id=str(existing.get("batch_id") or ""))
            return {
                "status": "joined",
                "batch": existing,
                "runs": runs,
                "summary": summarize_public_web_runs(runs),
                "idempotency_key": idempotency_key,
            }

    batch_id = str(request_payload.get("batch_id") or "").strip()
    if not batch_id:
        batch_id = f"tc-public-web-batch-{utc_compact_timestamp()}-{short_hash(idempotency_key)}"
    batch_artifact_root = Path(runtime_dir).expanduser() / "public_web" / "target_candidate_search" / batch_id
    batch_artifact_root.mkdir(parents=True, exist_ok=True)
    source_families = list(options.source_families)
    created_runs: list[dict[str, Any]] = []
    for record in target_candidates:
        context = candidate_context_from_target_candidate(record)
        if not context.record_id:
            continue
        run_idempotency_key = build_public_web_run_idempotency_key(
            record_id=context.record_id,
            linkedin_url_key=context.linkedin_url_key,
            options=options,
            force_refresh=force_refresh,
            nonce=str(request_payload.get("refresh_nonce") or request_payload.get("nonce") or "") if force_refresh else "",
        )
        existing_run = None if force_refresh else store.get_target_candidate_public_web_run(
            idempotency_key=run_idempotency_key
        )
        if existing_run is not None:
            created_runs.append(existing_run)
            continue
        run_id = f"tc-public-web-run-{short_hash(run_idempotency_key)}"
        run_artifact_root = batch_artifact_root / "runs" / run_id
        run_artifact_root.mkdir(parents=True, exist_ok=True)
        query_manifest = [
            query.to_record()
            for query in _plan_queries_for_context(context, options=options)
        ]
        created_runs.append(
            store.upsert_target_candidate_public_web_run(
                {
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "record_id": context.record_id,
                    "candidate_id": context.candidate_id,
                    "candidate_name": context.candidate_name,
                    "current_company": context.current_company,
                    "linkedin_url": context.linkedin_url,
                    "linkedin_url_key": context.linkedin_url_key,
                    "person_identity_key": person_identity_key_for_context(context),
                    "idempotency_key": run_idempotency_key,
                    "status": "queued",
                    "phase": "queued",
                    "source_families": source_families,
                    "options": asdict(options),
                    "query_manifest": query_manifest,
                    "artifact_root": str(run_artifact_root),
                    "worker_key": public_web_worker_key(run_id),
                    "summary": {
                        "record_id": context.record_id,
                        "candidate_name": context.candidate_name,
                        "current_company": context.current_company,
                        "linkedin_url_key": context.linkedin_url_key,
                    },
                    "search_checkpoint": {
                        "stage": "queued",
                        "status": "queued",
                        "query_count": len(query_manifest),
                    },
                    "metadata": {
                        "raw_asset_policy": (
                            "Raw HTML/PDF/search payloads are internal analysis inputs and are excluded "
                            "from default export packages."
                        ),
                        "reuse_policy": "v1 automatic reuse requires normalized LinkedIn URL key match.",
                    },
                }
            )
        )

    summary = summarize_public_web_runs(created_runs)
    batch = store.upsert_target_candidate_public_web_batch(
        {
            "batch_id": batch_id,
            "idempotency_key": idempotency_key,
            "status": summary["status"],
            "requested_record_ids": requested_record_ids,
            "source_families": source_families,
            "options": asdict(options),
            "run_ids": [str(run.get("run_id") or "") for run in created_runs if str(run.get("run_id") or "")],
            "summary": summary,
            "requested_by": requested_by,
            "force_refresh": force_refresh,
            "metadata": {
                "workflow_boundary": "user_triggered_target_candidate_public_web_search",
                "default_workflow_stage": "not_enabled",
            },
        }
    )
    return {
        "status": "queued",
        "batch": batch,
        "runs": created_runs,
        "summary": summary,
        "idempotency_key": idempotency_key,
    }


def sync_public_web_batch_summary(store: Any, batch_id: str) -> dict[str, Any]:
    batch = store.get_target_candidate_public_web_batch(batch_id=batch_id)
    if batch is None:
        return {"status": "not_found", "batch_id": batch_id}
    runs = store.list_target_candidate_public_web_runs(batch_id=batch_id)
    summary = summarize_public_web_runs(runs)
    updated = store.upsert_target_candidate_public_web_batch(
        {
            **batch,
            "status": summary["status"],
            "summary": summary,
            "run_ids": [str(run.get("run_id") or "") for run in runs if str(run.get("run_id") or "")],
        }
    )
    return {"status": "updated", "batch": updated, "summary": summary}


def execute_target_candidate_public_web_run_once(
    *,
    store: Any,
    search_provider: BaseSearchProvider,
    model_client: PublicWebModelClient | None,
    runtime_dir: str | Path,
    run_id: str,
    worker: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run = store.get_target_candidate_public_web_run(run_id=run_id)
    if run is None:
        return {"worker_status": "completed", "run_status": "failed", "reason": "run_not_found", "run_id": run_id}
    if str(run.get("status") or "") in PUBLIC_WEB_TERMINAL_STATUSES:
        return {"worker_status": "completed", "run_status": str(run.get("status") or ""), "summary": run.get("summary") or {}}
    record = store.get_target_candidate(str(run.get("record_id") or ""))
    if record is None:
        failed = store.update_target_candidate_public_web_run(
            run_id,
            {
                "status": "failed",
                "phase": "failed",
                "last_error": "target_candidate_not_found",
                "summary": {"run_id": run_id, "error": "target_candidate_not_found"},
            },
        )
        if failed and str(failed.get("batch_id") or ""):
            sync_public_web_batch_summary(store, str(failed.get("batch_id") or ""))
        return {"worker_status": "completed", "run_status": "failed", "summary": (failed or {}).get("summary") or {}}

    context = candidate_context_from_target_candidate(record)
    options = public_web_options_from_record(dict(run.get("options") or {}))
    artifact_root = Path(str(run.get("artifact_root") or "") or _default_run_artifact_root(runtime_dir, run)).expanduser()
    artifact_root.mkdir(parents=True, exist_ok=True)
    experiment_dir = artifact_root.parent
    plan = prepare_candidate_search_plan(
        candidate=context,
        experiment_dir=experiment_dir,
        options=options,
        ordinal=1,
    )
    checkpoint = dict(run.get("search_checkpoint") or {})
    checkpoint.setdefault("query_results", [])
    checkpoint.setdefault("raw_links", [])
    checkpoint.setdefault("errors", [])
    outcome = _outcome_from_checkpoint(checkpoint)

    if not checkpoint.get("tasks") and options.use_batch_search:
        submitted = _submit_public_web_batch_search(
            plan=plan,
            search_provider=search_provider,
            options=options,
            checkpoint=checkpoint,
        )
        run = store.update_target_candidate_public_web_run(
            run_id,
            {
                "status": "search_submitted",
                "phase": "search_submitted",
                "artifact_root": str(plan.candidate_dir),
                "query_manifest": [query.to_record() for query in plan.queries],
                "search_checkpoint": submitted,
                "started_at": str(run.get("started_at") or "") or utc_sql_timestamp(),
            },
        ) or run
        checkpoint = dict(run.get("search_checkpoint") or submitted)

    if checkpoint.get("tasks"):
        checkpoint, outcome = _poll_and_fetch_ready_public_web_tasks(
            plan=plan,
            search_provider=search_provider,
            options=options,
            checkpoint=checkpoint,
        )
        pending_count = len(_pending_task_records(checkpoint))
        if pending_count > 0:
            updated = store.update_target_candidate_public_web_run(
                run_id,
                {
                    "status": "searching",
                    "phase": "searching",
                    "artifact_root": str(plan.candidate_dir),
                    "query_manifest": [query.to_record() for query in plan.queries],
                    "search_checkpoint": checkpoint,
                    "summary": {
                        "run_id": run_id,
                        "record_id": context.record_id,
                        "candidate_name": context.candidate_name,
                        "pending_search_task_count": pending_count,
                        "fetched_search_task_count": len(_fetched_task_records(checkpoint)),
                    },
                },
            )
            _checkpoint_waiting_worker(
                store=store,
                worker=worker,
                run=updated or run,
                checkpoint=checkpoint,
                pending_count=pending_count,
            )
            if str((updated or run).get("batch_id") or ""):
                sync_public_web_batch_summary(store, str((updated or run).get("batch_id") or ""))
            return {
                "worker_status": "running",
                "run_status": "searching",
                "summary": dict((updated or run).get("summary") or {}),
            }
    elif str(checkpoint.get("status") or "") == "batch_unavailable":
        checkpoint["errors"] = []
        outcome = CandidateSearchOutcome(search_mode="sequential")
        execute_single_candidate_searches(
            plan=plan,
            search_provider=search_provider,
            options=options,
            outcome=outcome,
        )
    elif str(checkpoint.get("status") or "") == "submit_failed":
        outcome = _outcome_from_checkpoint(checkpoint)
    elif not options.use_batch_search:
        execute_single_candidate_searches(
            plan=plan,
            search_provider=search_provider,
            options=options,
            outcome=outcome,
        )

    checkpoint["stage"] = "analysis"
    checkpoint["status"] = "search_completed"
    checkpoint["query_results"] = list(outcome.query_results)
    checkpoint["raw_links"] = [link.to_record() for link in outcome.raw_links]
    checkpoint["errors"] = list(outcome.errors)
    store.update_target_candidate_public_web_run(
        run_id,
        {
            "status": "analyzing",
            "phase": "analyzing",
            "search_checkpoint": checkpoint,
            "artifact_root": str(plan.candidate_dir),
        },
    )
    summary = finalize_candidate_public_web_experiment(
        plan=plan,
        outcome=outcome,
        model_client=model_client,
        options=options,
    )
    run_status = str(summary.get("status") or "completed")
    signals = _load_json_from_path(Path(str(summary.get("artifact_root") or plan.candidate_dir)) / "signals.json")
    completed_run = store.update_target_candidate_public_web_run(
        run_id,
        {
            "status": run_status,
            "phase": "completed",
            "summary": summary,
            "analysis_checkpoint": {
                "stage": "completed",
                "status": run_status,
                "ai_adjudication_status": str(dict(signals.get("ai_adjudication") or {}).get("status") or ""),
            },
            "artifact_root": str(summary.get("artifact_root") or plan.candidate_dir),
            "completed_at": utc_sql_timestamp(),
        },
    )
    asset = _upsert_person_asset_from_run(store, completed_run or run, signals=signals)
    _replace_person_public_web_signals_from_run(store, completed_run or run, signals=signals, asset=asset)
    if str((completed_run or run).get("batch_id") or ""):
        sync_public_web_batch_summary(store, str((completed_run or run).get("batch_id") or ""))
    return {
        "worker_status": "completed",
        "run_status": run_status,
        "summary": summary,
    }


def summarize_public_web_runs(runs: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(run.get("status") or "unknown") for run in runs)
    terminal_count = sum(counts.get(status, 0) for status in PUBLIC_WEB_TERMINAL_STATUSES)
    running_count = len(runs) - terminal_count
    if not runs:
        status = "queued"
    elif counts.get("failed", 0) == len(runs):
        status = "failed"
    elif running_count == 0:
        status = "completed_with_errors" if counts.get("completed_with_errors", 0) or counts.get("needs_review", 0) else "completed"
    elif counts.get("searching", 0) or counts.get("search_submitted", 0):
        status = "searching"
    elif counts.get("analyzing", 0):
        status = "analyzing"
    else:
        status = "queued"
    return {
        "status": status,
        "run_count": len(runs),
        "queued_count": int(counts.get("queued", 0)),
        "search_submitted_count": int(counts.get("search_submitted", 0)),
        "searching_count": int(counts.get("searching", 0)),
        "entry_links_ready_count": int(counts.get("entry_links_ready", 0)),
        "fetching_count": int(counts.get("fetching", 0)),
        "analyzing_count": int(counts.get("analyzing", 0)),
        "completed_count": int(counts.get("completed", 0)),
        "completed_with_errors_count": int(counts.get("completed_with_errors", 0)),
        "needs_review_count": int(counts.get("needs_review", 0)),
        "failed_count": int(counts.get("failed", 0)),
        "cancelled_count": int(counts.get("cancelled", 0)),
        "running_count": running_count,
        "updated_at": utc_iso_timestamp(),
    }


def normalize_public_web_product_options(payload: dict[str, Any] | None = None) -> PublicWebExperimentOptions:
    payload = dict(payload or {})
    raw_options = dict(payload.get("options") or {})
    source_families = _normalize_source_families(
        raw_options.get("source_families")
        or payload.get("source_families")
        or DEFAULT_TARGET_CANDIDATE_SOURCE_FAMILIES
    )
    return PublicWebExperimentOptions(
        source_families=tuple(source_families),
        max_queries_per_candidate=_bounded_int(raw_options.get("max_queries_per_candidate"), default=10, low=1, high=16),
        max_results_per_query=_bounded_int(raw_options.get("max_results_per_query"), default=10, low=1, high=20),
        max_entry_links_per_candidate=_bounded_int(
            raw_options.get("max_entry_links_per_candidate"),
            default=40,
            low=1,
            high=80,
        ),
        max_fetches_per_candidate=_bounded_int(raw_options.get("max_fetches_per_candidate"), default=5, low=0, high=12),
        max_ai_evidence_documents=_bounded_int(
            raw_options.get("max_ai_evidence_documents"),
            default=8,
            low=1,
            high=20,
        ),
        fetch_content=_coerce_bool(raw_options.get("fetch_content"), default=True),
        extract_contact_signals=_coerce_bool(raw_options.get("extract_contact_signals"), default=True),
        ai_extraction=str(raw_options.get("ai_extraction") or "auto").strip().lower() or "auto",
        timeout_seconds=_bounded_int(raw_options.get("timeout_seconds"), default=30, low=5, high=90),
        use_batch_search=_coerce_bool(raw_options.get("use_batch_search"), default=True),
        batch_ready_poll_interval_seconds=float(raw_options.get("batch_ready_poll_interval_seconds") or 0.0),
        max_batch_ready_polls=_bounded_int(raw_options.get("max_batch_ready_polls"), default=18, low=1, high=60),
        max_concurrent_fetches_per_candidate=_bounded_int(
            raw_options.get("max_concurrent_fetches_per_candidate"),
            default=4,
            low=1,
            high=8,
        ),
        max_concurrent_candidate_analyses=_bounded_int(
            raw_options.get("max_concurrent_candidate_analyses"),
            default=2,
            low=1,
            high=4,
        ),
    )


def public_web_options_from_record(payload: dict[str, Any]) -> PublicWebExperimentOptions:
    return normalize_public_web_product_options({"options": payload})


def build_public_web_batch_idempotency_key(
    *,
    requested_record_ids: list[str],
    options: PublicWebExperimentOptions,
    force_refresh: bool = False,
    nonce: str = "",
) -> str:
    payload = {
        "record_ids": sorted(str(item or "").strip() for item in requested_record_ids if str(item or "").strip()),
        "options": asdict(options),
        "force_refresh": bool(force_refresh),
        "nonce": str(nonce or "") if force_refresh else "",
    }
    return "target-candidate-public-web-batch:" + short_hash(json.dumps(payload, sort_keys=True, ensure_ascii=False))


def build_public_web_run_idempotency_key(
    *,
    record_id: str,
    linkedin_url_key: str,
    options: PublicWebExperimentOptions,
    force_refresh: bool = False,
    nonce: str = "",
) -> str:
    payload = {
        "record_id": str(record_id or "").strip(),
        "linkedin_url_key": str(linkedin_url_key or "").strip(),
        "options": asdict(options),
        "force_refresh": bool(force_refresh),
        "nonce": str(nonce or "") if force_refresh else "",
    }
    return "target-candidate-public-web-run:" + short_hash(json.dumps(payload, sort_keys=True, ensure_ascii=False))


def public_web_worker_key(run_id: str) -> str:
    return f"public_web_run::{str(run_id or '').strip()}"


def person_identity_key_for_context(context: PublicWebCandidateContext) -> str:
    if context.linkedin_url_key:
        return f"linkedin:{context.linkedin_url_key}"
    return f"target_candidate:{context.record_id}"


def short_hash(value: str) -> str:
    return sha1(str(value or "").encode("utf-8")).hexdigest()[:16]


def utc_iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_sql_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def utc_compact_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _submit_public_web_batch_search(
    *,
    plan: Any,
    search_provider: BaseSearchProvider,
    options: PublicWebExperimentOptions,
    checkpoint: dict[str, Any],
) -> dict[str, Any]:
    query_specs = _batch_query_specs_for_plan(plan, options=options)
    checkpoint.update(
        {
            "stage": "search_submitted",
            "status": "search_submitted",
            "search_mode": "batch_queue",
            "submitted_at": utc_iso_timestamp(),
            "queries": [query.to_record() for query in plan.queries],
            "query_results": list(checkpoint.get("query_results") or []),
            "raw_links": list(checkpoint.get("raw_links") or []),
            "errors": list(checkpoint.get("errors") or []),
        }
    )
    try:
        submission = search_provider.submit_batch_queries(query_specs)
    except Exception as exc:
        checkpoint["status"] = "submit_failed"
        checkpoint.setdefault("errors", []).append(f"batch_submit_failed:{str(exc)[:200]}")
        return checkpoint
    if submission is None:
        checkpoint["status"] = "batch_unavailable"
        checkpoint.setdefault("errors", []).append("batch_submit_failed:provider_batch_unavailable")
        return checkpoint
    write_search_execution_artifacts(plan.logger, artifacts=submission.artifacts, prefix="submit")
    tasks: dict[str, dict[str, Any]] = {}
    query_by_task_key = {str(spec.get("task_key") or ""): dict(spec) for spec in query_specs}
    for task in submission.tasks:
        task_key = str(task.task_key or "")
        original = query_by_task_key.get(task_key) or {}
        tasks[task_key] = {
            "task_key": task_key,
            "task_id": str(dict(task.checkpoint or {}).get("task_id") or dict(task.metadata or {}).get("task_id") or ""),
            "query_text": task.query_text,
            "query": dict(original.get("query") or {}),
            "query_index": int(original.get("query_index") or 0),
            "checkpoint": dict(task.checkpoint or {}),
            "metadata": dict(task.metadata or {}),
            "provider_name": submission.provider_name,
            "status": "submitted",
            "poll_count": 0,
        }
    checkpoint["provider_name"] = submission.provider_name
    checkpoint["tasks"] = tasks
    checkpoint["submitted_task_count"] = len(tasks)
    return checkpoint


def _poll_and_fetch_ready_public_web_tasks(
    *,
    plan: Any,
    search_provider: BaseSearchProvider,
    options: PublicWebExperimentOptions,
    checkpoint: dict[str, Any],
) -> tuple[dict[str, Any], CandidateSearchOutcome]:
    outcome = _outcome_from_checkpoint(checkpoint)
    tasks = {str(key): dict(value) for key, value in dict(checkpoint.get("tasks") or {}).items()}
    pending = [task for task in tasks.values() if str(task.get("status") or "") not in {"fetched", "failed", "timeout"}]
    if not pending:
        return checkpoint, outcome
    ready_specs = []
    try:
        ready = search_provider.poll_ready_batch([_task_poll_spec(task) for task in pending])
    except Exception as exc:
        checkpoint.setdefault("errors", []).append(f"batch_ready_failed:{str(exc)[:200]}")
        return checkpoint, outcome
    if ready is None:
        checkpoint.setdefault("errors", []).append("batch_ready_failed:provider_returned_none")
        return checkpoint, outcome
    write_search_execution_artifacts(
        plan.logger,
        artifacts=ready.artifacts,
        prefix=f"ready_{int(checkpoint.get('ready_poll_count') or 0) + 1:02d}",
    )
    checkpoint["ready_poll_count"] = int(checkpoint.get("ready_poll_count") or 0) + 1
    for task in ready.tasks:
        task_key = str(task.task_key or "")
        current = tasks.get(task_key)
        if current is None:
            continue
        current["checkpoint"] = dict(task.checkpoint or current.get("checkpoint") or {})
        current["task_id"] = str(task.task_id or current.get("task_id") or "")
        current["poll_count"] = int(current.get("poll_count") or 0) + 1
        if bool(task.metadata.get("ready")) or str(task.checkpoint.get("status") or "") == "ready_cached":
            current["status"] = "ready"
            ready_specs.append(_task_poll_spec(current))
        elif int(current.get("poll_count") or 0) >= int(options.max_batch_ready_polls or 1):
            current["status"] = "timeout"
            outcome.errors.append(
                f"search_timeout:{dict(current.get('query') or {}).get('query_id') or task_key}:batch task not ready"
            )
        else:
            current["status"] = "waiting"
        tasks[task_key] = current
    if ready_specs:
        fetched_tasks, fetch_artifacts, fetch_errors = fetch_ready_specs_isolated(
            search_provider=search_provider,
            ready_specs=ready_specs,
        )
        write_search_execution_artifacts(
            plan.logger,
            artifacts=fetch_artifacts,
            prefix=f"fetch_{int(checkpoint.get('fetch_count') or 0) + 1:02d}",
        )
        checkpoint["fetch_count"] = int(checkpoint.get("fetch_count") or 0) + len(fetched_tasks)
        query_specs = {query.query_id: query for query in plan.queries}
        for task in fetched_tasks:
            task_key = str(task.task_key or "")
            current = tasks.get(task_key)
            if current is None:
                continue
            current["checkpoint"] = dict(task.checkpoint or current.get("checkpoint") or {})
            current["status"] = "fetched"
            current["task_id"] = str(task.task_id or current.get("task_id") or "")
            if task.response is not None:
                query_record = dict(current.get("query") or {})
                query = query_specs.get(str(query_record.get("query_id") or "")) or PublicWebQuerySpec(
                    query_id=str(query_record.get("query_id") or task_key),
                    source_family=str(query_record.get("source_family") or "profile_web_presence"),
                    query_text=str(task.query_text or query_record.get("query_text") or ""),
                    objective=str(query_record.get("objective") or ""),
                )
                record_candidate_search_response(
                    plan=plan,
                    query=query,
                    query_index=int(current.get("query_index") or 1),
                    response=task.response,
                    outcome=outcome,
                    duration_seconds=0.0,
                    search_mode="batch_queue",
                )
            tasks[task_key] = current
        for task_key, error_text in fetch_errors.items():
            current = tasks.get(str(task_key))
            if current is not None:
                current["status"] = "failed"
                current["error"] = str(error_text)
                tasks[str(task_key)] = current
            outcome.errors.append(f"search_failed:{task_key}:{str(error_text)[:200]}")
    checkpoint["tasks"] = tasks
    checkpoint["stage"] = "waiting_remote_search" if _pending_task_records(checkpoint) else "search_completed"
    checkpoint["status"] = "searching" if _pending_task_records(checkpoint) else "search_completed"
    checkpoint["query_results"] = list(outcome.query_results)
    checkpoint["raw_links"] = [link.to_record() for link in outcome.raw_links]
    checkpoint["errors"] = list(dict.fromkeys([*list(checkpoint.get("errors") or []), *outcome.errors]))
    return checkpoint, _outcome_from_checkpoint(checkpoint)


def _checkpoint_waiting_worker(
    *,
    store: Any,
    worker: dict[str, Any] | None,
    run: dict[str, Any],
    checkpoint: dict[str, Any],
    pending_count: int,
) -> None:
    worker_id = int(dict(worker or {}).get("worker_id") or 0)
    if worker_id <= 0:
        return
    worker_checkpoint = dict(dict(worker or {}).get("checkpoint") or {})
    worker_checkpoint.update(
        {
            "stage": "waiting_remote_search",
            "run_id": str(run.get("run_id") or ""),
            "batch_id": str(run.get("batch_id") or ""),
            "pending_search_task_count": pending_count,
            "search_checkpoint": checkpoint,
        }
    )
    store.checkpoint_agent_worker(
        worker_id,
        checkpoint_payload=worker_checkpoint,
        output_payload={
            "summary": dict(run.get("summary") or {}),
            "run_id": str(run.get("run_id") or ""),
            "run_status": str(run.get("status") or ""),
        },
        status="running",
    )


def _batch_query_specs_for_plan(plan: Any, *, options: PublicWebExperimentOptions) -> list[dict[str, Any]]:
    specs = []
    for query_index, query in enumerate(plan.queries, start=1):
        task_key = f"01:{query.query_id}:{short_hash(plan.candidate.record_id + '|' + query.query_text)}"
        specs.append(
            {
                "task_key": task_key,
                "query_text": query.query_text,
                "max_results": max(1, int(options.max_results_per_query or 1)),
                "query_index": query_index,
                "query": query.to_record(),
                "metadata": {
                    "record_id": plan.candidate.record_id,
                    "candidate_id": plan.candidate.candidate_id,
                    "query_id": query.query_id,
                    "source_family": query.source_family,
                },
            }
        )
    return specs


def _task_poll_spec(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_key": str(task.get("task_key") or ""),
        "task_id": str(task.get("task_id") or dict(task.get("checkpoint") or {}).get("task_id") or ""),
        "query_text": str(task.get("query_text") or ""),
        "checkpoint": dict(task.get("checkpoint") or {}),
        "metadata": dict(task.get("metadata") or {}),
        "provider_name": str(task.get("provider_name") or dict(task.get("checkpoint") or {}).get("provider_name") or ""),
    }


def _pending_task_records(checkpoint: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(task)
        for task in dict(checkpoint.get("tasks") or {}).values()
        if str(dict(task).get("status") or "") not in {"fetched", "failed", "timeout"}
    ]


def _fetched_task_records(checkpoint: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(task)
        for task in dict(checkpoint.get("tasks") or {}).values()
        if str(dict(task).get("status") or "") == "fetched"
    ]


def _outcome_from_checkpoint(checkpoint: dict[str, Any]) -> CandidateSearchOutcome:
    return CandidateSearchOutcome(
        query_results=[dict(item) for item in list(checkpoint.get("query_results") or []) if isinstance(item, dict)],
        raw_links=[
            _classified_link_from_record(item)
            for item in list(checkpoint.get("raw_links") or [])
            if isinstance(item, dict)
        ],
        errors=[str(item) for item in list(checkpoint.get("errors") or []) if str(item or "").strip()],
        search_mode=str(checkpoint.get("search_mode") or "batch_queue"),
    )


def _classified_link_from_record(record: dict[str, Any]) -> ClassifiedEntryLink:
    return ClassifiedEntryLink(
        url=str(record.get("url") or ""),
        normalized_url=str(record.get("normalized_url") or record.get("url") or ""),
        title=str(record.get("title") or ""),
        snippet=str(record.get("snippet") or ""),
        source_domain=str(record.get("source_domain") or ""),
        entry_type=str(record.get("entry_type") or "other"),
        source_family=str(record.get("source_family") or ""),
        score=float(record.get("score") or 0.0),
        reasons=tuple(str(item) for item in list(record.get("reasons") or [])),
        query_id=str(record.get("query_id") or ""),
        query_text=str(record.get("query_text") or ""),
        provider_name=str(record.get("provider_name") or ""),
        result_rank=int(record.get("result_rank") or 0),
        fetchable=bool(record.get("fetchable")),
        identity_match_label=str(record.get("identity_match_label") or "unreviewed"),
        identity_match_score=float(record.get("identity_match_score") or 0.0),
        confidence_label=str(record.get("confidence_label") or "medium"),
        adjudication=dict(record.get("adjudication") or {}),
    )


def build_person_public_web_signal_rows(
    *,
    run: dict[str, Any],
    signals: dict[str, Any],
    asset: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    run_id = str(run.get("run_id") or "").strip()
    if not run_id:
        return []
    person_identity_key = str(run.get("person_identity_key") or "").strip()
    linkedin_url_key = str(run.get("linkedin_url_key") or "").strip()
    asset_id = str(dict(asset or {}).get("asset_id") or "").strip()
    if not asset_id and person_identity_key.startswith("linkedin:"):
        asset_id = f"person-public-web-{short_hash(person_identity_key)}"
    base = {
        "run_id": run_id,
        "asset_id": asset_id,
        "person_identity_key": person_identity_key,
        "record_id": str(run.get("record_id") or ""),
        "candidate_id": str(run.get("candidate_id") or ""),
        "candidate_name": str(run.get("candidate_name") or dict(run.get("summary") or {}).get("candidate_name") or ""),
        "current_company": str(run.get("current_company") or dict(run.get("summary") or {}).get("current_company") or ""),
        "linkedin_url_key": linkedin_url_key,
    }
    model_provider, model_version = _model_identity_from_signals(signals)
    common_artifact_refs = _model_safe_common_artifact_refs(run=run, signals=signals)
    artifact_refs_by_url = _artifact_refs_by_source_url(signals)
    rows: list[dict[str, Any]] = []

    for index, email in enumerate(list(signals.get("email_candidates") or []), start=1):
        if not isinstance(email, dict):
            continue
        source_url = str(email.get("source_url") or "").strip()
        source_domain = str(email.get("source_domain") or "").strip() or _domain_from_url(source_url)
        normalized_value = str(email.get("normalized_value") or email.get("value") or "").strip().lower()
        rows.append(
            {
                **base,
                "signal_id": "person-public-web-signal-"
                + short_hash(f"{run_id}|email|{normalized_value}|{source_url}|{index}"),
                "signal_kind": "email_candidate",
                "signal_type": str(email.get("email_type") or "unknown").strip() or "unknown",
                "email_type": str(email.get("email_type") or "").strip(),
                "value": str(email.get("value") or normalized_value).strip(),
                "normalized_value": normalized_value,
                "url": source_url,
                "source_url": source_url,
                "source_domain": source_domain,
                "source_family": str(email.get("source_family") or "").strip(),
                "source_title": str(email.get("source_title") or "").strip(),
                "confidence_label": str(email.get("confidence_label") or "").strip(),
                "confidence_score": _coerce_float(email.get("confidence_score")),
                "identity_match_label": str(email.get("identity_match_label") or "needs_review").strip(),
                "identity_match_score": _coerce_float(email.get("identity_match_score")),
                "publishable": bool(email.get("publishable")),
                "promotion_status": str(email.get("promotion_status") or "not_promoted").strip(),
                "suppression_reason": str(email.get("suppression_reason") or "").strip(),
                "evidence_excerpt": _truncate_signal_text(email.get("evidence_excerpt"), limit=700),
                "artifact_refs": _merge_artifact_refs(
                    common_artifact_refs,
                    artifact_refs_by_url.get(normalize_public_web_url_key(source_url), {}),
                ),
                "model_provider": model_provider,
                "model_version": model_version,
                "metadata": {
                    "adjudication": dict(email.get("adjudication") or {}),
                    "raw_asset_policy": "raw_html_pdf_and_search_payloads_excluded_from_default_export",
                },
            }
        )

    for index, link in enumerate(list(signals.get("entry_links") or []), start=1):
        if not isinstance(link, dict):
            continue
        url = str(link.get("normalized_url") or link.get("url") or "").strip()
        if not url:
            continue
        identity_match_label = str(link.get("identity_match_label") or "unreviewed").strip()
        source_domain = str(link.get("source_domain") or "").strip() or _domain_from_url(url)
        signal_type = str(link.get("entry_type") or "other").strip() or "other"
        link_shape_warnings = public_web_link_shape_warnings(signal_type, url)
        clean_profile_link = is_clean_profile_link(signal_type, url)
        publishable = identity_match_label in {"confirmed", "likely_same_person"} and clean_profile_link
        link_suppression_reason = "" if clean_profile_link else "non_profile_link_shape"
        link_adjudication = dict(link.get("adjudication") or {})
        rows.append(
            {
                **base,
                "signal_id": "person-public-web-signal-"
                + short_hash(f"{run_id}|link|{url}|{link.get('entry_type') or ''}|{index}"),
                "signal_kind": "profile_link",
                "signal_type": signal_type,
                "email_type": "",
                "value": url,
                "normalized_value": url,
                "url": url,
                "source_url": url,
                "source_domain": source_domain,
                "source_family": str(link.get("source_family") or "").strip(),
                "source_title": str(link.get("title") or "").strip(),
                "confidence_label": str(link.get("confidence_label") or "").strip(),
                "confidence_score": _coerce_float(link.get("score")),
                "identity_match_label": identity_match_label,
                "identity_match_score": _coerce_float(link.get("identity_match_score")),
                "publishable": publishable,
                "promotion_status": "",
                "suppression_reason": link_suppression_reason,
                "evidence_excerpt": _truncate_signal_text(link.get("snippet"), limit=700),
                "artifact_refs": _merge_artifact_refs(
                    common_artifact_refs,
                    artifact_refs_by_url.get(normalize_public_web_url_key(url), {}),
                ),
                "model_provider": model_provider,
                "model_version": model_version,
                "metadata": {
                    "query_id": str(link.get("query_id") or ""),
                    "query_text": str(link.get("query_text") or ""),
                    "provider_name": str(link.get("provider_name") or ""),
                    "result_rank": int(link.get("result_rank") or 0),
                    "fetchable": bool(link.get("fetchable")),
                    "reasons": list(link.get("reasons") or []),
                    "adjudication": link_adjudication,
                    "link_shape_warnings": link_shape_warnings,
                    "clean_profile_link": clean_profile_link,
                    "raw_asset_policy": "raw_html_pdf_and_search_payloads_excluded_from_default_export",
                },
            }
        )
    return rows


def _replace_person_public_web_signals_from_run(
    store: Any,
    run: dict[str, Any],
    *,
    signals: dict[str, Any],
    asset: dict[str, Any] | None,
) -> int:
    rows = build_person_public_web_signal_rows(run=run, signals=signals, asset=asset)
    return store.replace_person_public_web_signals_for_run(run_id=str(run.get("run_id") or ""), signals=rows)


def _upsert_person_asset_from_run(store: Any, run: dict[str, Any], *, signals: dict[str, Any]) -> dict[str, Any] | None:
    person_identity_key = str(run.get("person_identity_key") or "").strip()
    if not person_identity_key.startswith("linkedin:"):
        return None
    summary = dict(run.get("summary") or {})
    return store.upsert_person_public_web_asset(
        {
            "person_identity_key": person_identity_key,
            "linkedin_url_key": str(run.get("linkedin_url_key") or ""),
            "latest_run_id": str(run.get("run_id") or ""),
            "target_candidate_record_id": str(run.get("record_id") or ""),
            "candidate_name": str(run.get("candidate_name") or summary.get("candidate_name") or ""),
            "current_company": str(run.get("current_company") or summary.get("current_company") or ""),
            "status": str(run.get("status") or summary.get("status") or "completed"),
            "summary": summary,
            "signals": signals,
            "source_run_ids": [str(run.get("run_id") or "")],
            "artifact_root": str(run.get("artifact_root") or summary.get("artifact_root") or ""),
            "metadata": {
                "reuse_policy": "normalized_linkedin_url_key",
                "raw_assets_export_default": "excluded",
            },
        }
    )


def _model_identity_from_signals(signals: dict[str, Any]) -> tuple[str, str]:
    adjudication = dict(signals.get("ai_adjudication") or {})
    result = dict(adjudication.get("result") or {})
    model_provider = str(
        adjudication.get("provider")
        or result.get("provider")
        or result.get("model_provider")
        or result.get("provider_name")
        or ""
    ).strip()
    model_version = str(
        adjudication.get("model_version")
        or adjudication.get("model")
        or result.get("model_version")
        or result.get("model")
        or ""
    ).strip()
    return model_provider, model_version


def _model_safe_common_artifact_refs(*, run: dict[str, Any], signals: dict[str, Any]) -> dict[str, Any]:
    artifact_root = str(run.get("artifact_root") or dict(run.get("summary") or {}).get("artifact_root") or "").strip()
    if not artifact_root:
        artifact_root = str(dict(signals.get("candidate") or {}).get("artifact_root") or "").strip()
    if not artifact_root:
        return {}
    root = Path(artifact_root)
    return {
        "candidate_summary_path": str(root / "candidate_summary.json"),
        "entry_links_path": str(root / "entry_links.json"),
        "signals_path": str(root / "signals.json"),
    }


def _artifact_refs_by_source_url(signals: dict[str, Any]) -> dict[str, dict[str, Any]]:
    refs_by_url: dict[str, dict[str, Any]] = {}
    for document in list(signals.get("fetched_documents") or []):
        if not isinstance(document, dict):
            continue
        refs = {
            key: str(document.get(key) or "").strip()
            for key in ("analysis_path", "evidence_slice_path")
            if str(document.get(key) or "").strip()
        }
        if not refs:
            continue
        for url_key in (
            normalize_public_web_url_key(str(document.get("source_url") or "")),
            normalize_public_web_url_key(str(document.get("final_url") or "")),
        ):
            if url_key:
                refs_by_url[url_key] = refs
    return refs_by_url


def _merge_artifact_refs(*refs: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    allowed = {
        "candidate_summary_path",
        "entry_links_path",
        "signals_path",
        "analysis_path",
        "evidence_slice_path",
    }
    for ref in refs:
        for key, value in dict(ref or {}).items():
            if key in allowed and str(value or "").strip():
                merged[key] = str(value or "").strip()
    return merged


def _domain_from_url(url: str) -> str:
    try:
        return urlparse(str(url or "")).netloc.lower()
    except Exception:
        return ""


def _truncate_signal_text(value: Any, *, limit: int) -> str:
    text = " ".join(str(value or "").strip().split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _coerce_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _load_json_from_path(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _default_run_artifact_root(runtime_dir: str | Path, run: dict[str, Any]) -> Path:
    batch_id = str(run.get("batch_id") or "unbatched")
    run_id = str(run.get("run_id") or "run")
    return Path(runtime_dir).expanduser() / "public_web" / "target_candidate_search" / batch_id / "runs" / run_id


def _plan_queries_for_context(
    context: PublicWebCandidateContext,
    *,
    options: PublicWebExperimentOptions,
) -> list[PublicWebQuerySpec]:
    return plan_candidate_public_web_queries(
        context,
        source_families=options.source_families,
        max_queries=options.max_queries_per_candidate,
    )


def _normalize_source_families(value: Any) -> list[str]:
    raw_items = [value] if isinstance(value, str) else list(value or [])
    allowed = set(DEFAULT_TARGET_CANDIDATE_SOURCE_FAMILIES)
    items: list[str] = []
    for raw_item in raw_items:
        item = str(raw_item or "").strip()
        if item in allowed and item not in items:
            items.append(item)
    return items or list(DEFAULT_TARGET_CANDIDATE_SOURCE_FAMILIES)


def _bounded_int(value: Any, *, default: int, low: int, high: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, low), high)


def _coerce_bool(value: Any, *, default: bool) -> bool:
    if value in {None, ""}:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default
