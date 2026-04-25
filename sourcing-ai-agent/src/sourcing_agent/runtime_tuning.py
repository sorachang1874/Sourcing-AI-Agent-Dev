from __future__ import annotations

import contextlib
import os
import threading
import time
from typing import Any

_ALLOWED_RUNTIME_TUNING_PROFILES = {"fast_smoke"}

_FAST_SMOKE_RUNTIME_TIMING_OVERRIDES = {
    "lane_ready_cooldown_seconds": 0,
    "lane_fetch_cooldown_seconds": 0,
    "task_get_batch_workers": 4,
    "harvest_poll_interval_seconds": 0.25,
    "harvest_retry_backoff_seconds": 0.25,
    "harvest_scripted_sleep_seconds_cap": 0.1,
    "provider_people_search_parallel_queries": 6,
    "harvest_company_roster_parallel_shards": 8,
    "candidate_artifact_parallel_min_candidates": 12,
    "candidate_artifact_max_workers": 4,
    "parallel_search_workers": 6,
    "parallel_exploration_workers": 4,
    "harvest_prefetch_submit_workers": 4,
    "harvest_profile_scrape_global_inflight": 4,
    "harvest_people_search_global_inflight": 4,
    "harvest_company_roster_global_inflight": 4,
    "materialization_global_writer_budget": 2,
    "materialization_coalescing_window_ms": 50,
    "search_worker_unit_budget": 12,
    "public_media_worker_unit_budget": 10,
    "exploration_worker_unit_budget": 8,
}

_GLOBAL_INFLIGHT_LOCK = threading.Lock()
_GLOBAL_INFLIGHT_SEMAPHORES: dict[tuple[str, int], threading.BoundedSemaphore] = {}
_GLOBAL_INFLIGHT_HELD = threading.local()


def normalize_runtime_tuning_profile(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _ALLOWED_RUNTIME_TUNING_PROFILES:
        return normalized
    return ""


def resolve_runtime_timing_overrides(preferences: dict[str, Any] | None) -> dict[str, Any]:
    candidate = dict(preferences or {})
    profile = normalize_runtime_tuning_profile(candidate.get("runtime_tuning_profile"))
    overrides: dict[str, Any] = {}
    if profile == "fast_smoke":
        overrides.update(_FAST_SMOKE_RUNTIME_TIMING_OVERRIDES)
    if profile:
        overrides["runtime_tuning_profile"] = profile
    return overrides


def apply_runtime_timing_overrides_to_search_state(
    search_state: dict[str, Any] | None,
    *,
    runtime_timing_overrides: dict[str, Any] | None,
) -> dict[str, Any]:
    return apply_runtime_timing_overrides_to_mapping(
        search_state,
        runtime_timing_overrides=runtime_timing_overrides,
    )


def apply_runtime_timing_overrides_to_mapping(
    payload: dict[str, Any] | None,
    *,
    runtime_timing_overrides: dict[str, Any] | None,
) -> dict[str, Any]:
    updated = dict(payload or {})
    overrides = dict(runtime_timing_overrides or {})
    effective_overrides = {
        **resolve_runtime_timing_overrides(overrides),
        **overrides,
    }
    profile = normalize_runtime_tuning_profile(effective_overrides.get("runtime_tuning_profile"))
    if profile and not str(updated.get("runtime_tuning_profile") or "").strip():
        updated["runtime_tuning_profile"] = profile
    ready_seconds = _coerce_nonnegative_int(effective_overrides.get("lane_ready_cooldown_seconds"))
    if ready_seconds is not None and "lane_ready_cooldown_seconds" not in updated:
        updated["lane_ready_cooldown_seconds"] = ready_seconds
    fetch_seconds = _coerce_nonnegative_int(effective_overrides.get("lane_fetch_cooldown_seconds"))
    if fetch_seconds is not None and "lane_fetch_cooldown_seconds" not in updated:
        updated["lane_fetch_cooldown_seconds"] = fetch_seconds
    task_workers = _coerce_positive_int(effective_overrides.get("task_get_batch_workers"))
    if task_workers is not None and "task_get_batch_workers" not in updated:
        updated["task_get_batch_workers"] = task_workers
    harvest_poll_interval_seconds = _coerce_nonnegative_float(effective_overrides.get("harvest_poll_interval_seconds"))
    if harvest_poll_interval_seconds is not None and "harvest_poll_interval_seconds" not in updated:
        updated["harvest_poll_interval_seconds"] = harvest_poll_interval_seconds
    harvest_retry_backoff_seconds = _coerce_nonnegative_float(effective_overrides.get("harvest_retry_backoff_seconds"))
    if harvest_retry_backoff_seconds is not None and "harvest_retry_backoff_seconds" not in updated:
        updated["harvest_retry_backoff_seconds"] = harvest_retry_backoff_seconds
    harvest_scripted_sleep_seconds_cap = _coerce_nonnegative_float(
        effective_overrides.get("harvest_scripted_sleep_seconds_cap")
    )
    if harvest_scripted_sleep_seconds_cap is not None and "harvest_scripted_sleep_seconds_cap" not in updated:
        updated["harvest_scripted_sleep_seconds_cap"] = harvest_scripted_sleep_seconds_cap
    for key in (
        "provider_people_search_parallel_queries",
        "harvest_company_roster_parallel_shards",
        "candidate_artifact_parallel_min_candidates",
        "candidate_artifact_max_workers",
        "parallel_search_workers",
        "parallel_exploration_workers",
        "harvest_prefetch_submit_workers",
        "harvest_profile_scrape_global_inflight",
        "harvest_people_search_global_inflight",
        "harvest_company_roster_global_inflight",
        "materialization_global_writer_budget",
        "materialization_coalescing_window_ms",
        "search_worker_unit_budget",
        "public_media_worker_unit_budget",
        "exploration_worker_unit_budget",
    ):
        explicit_value = _coerce_positive_int(effective_overrides.get(key))
        if explicit_value is not None and key not in updated:
            updated[key] = explicit_value
    return updated


def resolved_lane_ready_cooldown_seconds(search_state: dict[str, Any] | None, *, default: int) -> int:
    state = dict(search_state or {})
    explicit = _coerce_nonnegative_int(state.get("lane_ready_cooldown_seconds"))
    if explicit is not None:
        return explicit
    profile_overrides = resolve_runtime_timing_overrides(state)
    profile_value = _coerce_nonnegative_int(profile_overrides.get("lane_ready_cooldown_seconds"))
    if profile_value is not None:
        return profile_value
    return max(0, int(default or 0))


def resolved_lane_fetch_cooldown_seconds(search_state: dict[str, Any] | None, *, default: int) -> int:
    state = dict(search_state or {})
    explicit = _coerce_nonnegative_int(state.get("lane_fetch_cooldown_seconds"))
    if explicit is not None:
        return explicit
    profile_overrides = resolve_runtime_timing_overrides(state)
    profile_value = _coerce_nonnegative_int(profile_overrides.get("lane_fetch_cooldown_seconds"))
    if profile_value is not None:
        return profile_value
    return max(0, int(default or 0))


def resolved_task_get_batch_workers(search_states: list[dict[str, Any]] | None, *, default: int) -> int:
    normalized_default = max(1, int(default or 1))
    candidates: list[int] = []
    for item in list(search_states or []):
        state = dict(item or {})
        explicit = _coerce_positive_int(state.get("task_get_batch_workers"))
        if explicit is not None:
            candidates.append(explicit)
            continue
        profile_overrides = resolve_runtime_timing_overrides(state)
        profile_value = _coerce_positive_int(profile_overrides.get("task_get_batch_workers"))
        if profile_value is not None:
            candidates.append(profile_value)
    if not candidates:
        return normalized_default
    return max(1, min(candidates))


def resolved_harvest_poll_interval_seconds(request_context: dict[str, Any] | None, *, default: float) -> float:
    context = dict(request_context or {})
    explicit = _coerce_nonnegative_float(context.get("harvest_poll_interval_seconds"))
    if explicit is not None:
        return explicit
    profile_overrides = resolve_runtime_timing_overrides(context)
    profile_value = _coerce_nonnegative_float(profile_overrides.get("harvest_poll_interval_seconds"))
    if profile_value is not None:
        return profile_value
    return max(0.0, float(default or 0.0))


def resolved_harvest_retry_backoff_seconds(
    request_context: dict[str, Any] | None,
    *,
    attempt_index: int,
    default: float,
) -> float:
    del attempt_index
    context = dict(request_context or {})
    explicit = _coerce_nonnegative_float(context.get("harvest_retry_backoff_seconds"))
    if explicit is not None:
        return explicit
    profile_overrides = resolve_runtime_timing_overrides(context)
    profile_value = _coerce_nonnegative_float(profile_overrides.get("harvest_retry_backoff_seconds"))
    if profile_value is not None:
        return profile_value
    return max(0.0, float(default or 0.0))


def resolved_harvest_scripted_sleep_seconds_cap(request_context: dict[str, Any] | None) -> float | None:
    context = dict(request_context or {})
    explicit = _coerce_nonnegative_float(context.get("harvest_scripted_sleep_seconds_cap"))
    if explicit is not None:
        return explicit
    profile_overrides = resolve_runtime_timing_overrides(context)
    return _coerce_nonnegative_float(profile_overrides.get("harvest_scripted_sleep_seconds_cap"))


def resolved_runtime_positive_int(request_context: dict[str, Any] | None, *, key: str) -> int | None:
    context = dict(request_context or {})
    explicit = _coerce_positive_int(context.get(key))
    if explicit is not None:
        return explicit
    profile_overrides = resolve_runtime_timing_overrides(context)
    return _coerce_positive_int(profile_overrides.get(key))


def _resolved_policy_positive_int(
    request_context: dict[str, Any] | None,
    *,
    key: str,
    cost_policy: dict[str, Any] | None = None,
    default: int,
) -> int:
    request_value = resolved_runtime_positive_int(request_context, key=key)
    cost_policy_value = _coerce_positive_int(dict(cost_policy or {}).get(key))
    return request_value or cost_policy_value or max(1, int(default or 1))


def resolved_provider_people_search_parallel_queries(
    request_context: dict[str, Any] | None,
    *,
    cost_policy: dict[str, Any] | None = None,
    query_count: int,
    default: int,
) -> int:
    count = max(1, int(query_count or 1))
    configured = _resolved_policy_positive_int(
        request_context,
        key="provider_people_search_parallel_queries",
        cost_policy=cost_policy,
        default=default,
    )
    return max(1, min(count, configured))


def resolved_harvest_company_roster_parallel_shards(
    request_context: dict[str, Any] | None,
    *,
    shard_count: int,
    default: int,
) -> int:
    count = max(1, int(shard_count or 1))
    configured = _resolved_policy_positive_int(
        request_context,
        key="harvest_company_roster_parallel_shards",
        default=default,
    )
    return max(1, min(count, configured))


def resolved_parallel_search_workers(
    request_context: dict[str, Any] | None,
    *,
    cost_policy: dict[str, Any] | None = None,
    default: int,
) -> int:
    return _resolved_policy_positive_int(
        request_context,
        key="parallel_search_workers",
        cost_policy=cost_policy,
        default=default,
    )


def resolved_parallel_exploration_workers(
    request_context: dict[str, Any] | None,
    *,
    cost_policy: dict[str, Any] | None = None,
    default: int,
) -> int:
    return _resolved_policy_positive_int(
        request_context,
        key="parallel_exploration_workers",
        cost_policy=cost_policy,
        default=default,
    )


def resolved_harvest_prefetch_submit_workers(
    request_context: dict[str, Any] | None,
    *,
    chunk_count: int,
    default: int,
) -> int:
    normalized_chunk_count = max(1, int(chunk_count or 1))
    configured = (
        resolved_runtime_positive_int(
            request_context,
            key="harvest_prefetch_submit_workers",
        )
        or max(1, int(default or 1))
    )
    return max(1, min(normalized_chunk_count, configured))


def resolved_runtime_global_inflight_budget(
    request_context: dict[str, Any] | None,
    *,
    key: str,
    default: int,
    env_key: str = "",
) -> int:
    explicit = resolved_runtime_positive_int(request_context, key=key)
    if explicit is not None:
        return explicit
    generic = resolved_runtime_positive_int(request_context, key="harvest_global_inflight_budget")
    if generic is not None and str(key or "").startswith("harvest_"):
        return generic
    env_value = _coerce_positive_int(os.getenv(env_key or f"SOURCING_{str(key or '').upper()}"))
    if env_value is not None:
        return env_value
    if str(key or "").startswith("harvest_"):
        generic_env = _coerce_positive_int(os.getenv("SOURCING_HARVEST_GLOBAL_INFLIGHT_BUDGET"))
        if generic_env is not None:
            return generic_env
    return max(1, int(default or 1))


def resolved_harvest_profile_scrape_global_inflight(request_context: dict[str, Any] | None) -> int:
    return resolved_runtime_global_inflight_budget(
        request_context,
        key="harvest_profile_scrape_global_inflight",
        default=4,
        env_key="SOURCING_HARVEST_PROFILE_SCRAPE_GLOBAL_INFLIGHT",
    )


def resolved_harvest_people_search_global_inflight(request_context: dict[str, Any] | None) -> int:
    return resolved_runtime_global_inflight_budget(
        request_context,
        key="harvest_people_search_global_inflight",
        default=4,
        env_key="SOURCING_HARVEST_PEOPLE_SEARCH_GLOBAL_INFLIGHT",
    )


def resolved_harvest_company_roster_global_inflight(request_context: dict[str, Any] | None) -> int:
    return resolved_runtime_global_inflight_budget(
        request_context,
        key="harvest_company_roster_global_inflight",
        default=4,
        env_key="SOURCING_HARVEST_COMPANY_ROSTER_GLOBAL_INFLIGHT",
    )


def resolved_materialization_global_writer_budget(request_context: dict[str, Any] | None) -> int:
    return resolved_runtime_global_inflight_budget(
        request_context,
        key="materialization_global_writer_budget",
        default=2,
        env_key="SOURCING_MATERIALIZATION_GLOBAL_WRITER_BUDGET",
    )


def resolved_materialization_coalescing_window_ms(
    request_context: dict[str, Any] | None,
    *,
    default: int = 750,
) -> int:
    explicit = resolved_runtime_positive_int(request_context, key="materialization_coalescing_window_ms")
    if explicit is not None:
        return explicit
    env_value = _coerce_positive_int(os.getenv("SOURCING_MATERIALIZATION_COALESCING_WINDOW_MS"))
    if env_value is not None:
        return env_value
    return max(0, int(default or 0))


def build_materialization_streaming_budget_report(
    request_context: dict[str, Any] | None,
    *,
    provider_response_count: int,
    profile_url_count: int,
    pending_delta_count: int,
    active_writer_count: int = 0,
    queued_writer_count: int = 0,
    oldest_pending_delta_age_ms: float = 0.0,
) -> dict[str, Any]:
    normalized_provider_response_count = max(0, int(provider_response_count or 0))
    normalized_profile_url_count = max(0, int(profile_url_count or 0))
    normalized_pending_delta_count = max(0, int(pending_delta_count or 0))
    normalized_active_writer_count = max(0, int(active_writer_count or 0))
    normalized_queued_writer_count = max(0, int(queued_writer_count or 0))
    writer_budget = resolved_materialization_global_writer_budget(request_context)
    coalescing_window_ms = resolved_materialization_coalescing_window_ms(request_context)
    normalized_oldest_delta_age_ms = max(0.0, float(oldest_pending_delta_age_ms or 0.0))
    should_hold_for_coalescing = (
        normalized_pending_delta_count > 1
        and coalescing_window_ms > 0
        and normalized_oldest_delta_age_ms < float(coalescing_window_ms)
    )
    writer_slot_available = normalized_active_writer_count < writer_budget
    if normalized_pending_delta_count <= 0:
        recommended_action = "skip_no_delta"
    elif not writer_slot_available:
        recommended_action = "wait_for_writer_slot"
    elif should_hold_for_coalescing:
        recommended_action = "coalesce_window"
    else:
        recommended_action = "dispatch_delta_writer"
    return {
        "provider_response_count": normalized_provider_response_count,
        "profile_url_count": normalized_profile_url_count,
        "pending_delta_count": normalized_pending_delta_count,
        "active_writer_count": normalized_active_writer_count,
        "queued_writer_count": normalized_queued_writer_count,
        "writer_budget": writer_budget,
        "writer_slot_available": writer_slot_available,
        "coalescing_window_ms": coalescing_window_ms,
        "oldest_pending_delta_age_ms": round(normalized_oldest_delta_age_ms, 2),
        "should_hold_for_coalescing": should_hold_for_coalescing,
        "provider_response_level_streaming_ready": normalized_provider_response_count > 0,
        "delta_materialization_ready": normalized_pending_delta_count > 0,
        "recommended_action": recommended_action,
    }


def _global_inflight_semaphore(lane: str, budget: int) -> threading.BoundedSemaphore:
    normalized_lane = str(lane or "default").strip() or "default"
    normalized_budget = max(1, int(budget or 1))
    key = (normalized_lane, normalized_budget)
    with _GLOBAL_INFLIGHT_LOCK:
        semaphore = _GLOBAL_INFLIGHT_SEMAPHORES.get(key)
        if semaphore is None:
            semaphore = threading.BoundedSemaphore(normalized_budget)
            _GLOBAL_INFLIGHT_SEMAPHORES[key] = semaphore
        return semaphore


@contextlib.contextmanager
def runtime_inflight_slot(
    lane: str,
    *,
    budget: int,
    metadata: dict[str, Any] | None = None,
) -> Any:
    normalized_lane = str(lane or "default").strip() or "default"
    normalized_budget = max(1, int(budget or 1))
    held_counts = getattr(_GLOBAL_INFLIGHT_HELD, "counts", None)
    if not isinstance(held_counts, dict):
        held_counts = {}
        _GLOBAL_INFLIGHT_HELD.counts = held_counts
    held_key = normalized_lane
    if int(held_counts.get(held_key) or 0) > 0:
        yield {
            "lane": normalized_lane,
            "budget": normalized_budget,
            "wait_ms": 0.0,
            "reentrant": True,
            **dict(metadata or {}),
        }
        return
    semaphore = _global_inflight_semaphore(normalized_lane, normalized_budget)
    started = time.perf_counter()
    semaphore.acquire()
    held_counts[held_key] = int(held_counts.get(held_key) or 0) + 1
    wait_ms = round((time.perf_counter() - started) * 1000, 2)
    slot = {
        "lane": normalized_lane,
        "budget": normalized_budget,
        "wait_ms": wait_ms,
        "reentrant": False,
        **dict(metadata or {}),
    }
    try:
        yield slot
    finally:
        current_count = int(held_counts.get(held_key) or 0)
        if current_count <= 1:
            held_counts.pop(held_key, None)
        else:
            held_counts[held_key] = current_count - 1
        semaphore.release()


def resolved_lane_budget_caps(
    request_context: dict[str, Any] | None,
    *,
    cost_policy: dict[str, Any] | None = None,
    default_search_budget: int = 8,
    default_public_media_budget: int = 6,
    default_exploration_budget: int = 5,
) -> dict[str, int]:
    return {
        "search_planner": _resolved_policy_positive_int(
            request_context,
            key="search_worker_unit_budget",
            cost_policy=cost_policy,
            default=default_search_budget,
        ),
        "public_media_specialist": _resolved_policy_positive_int(
            request_context,
            key="public_media_worker_unit_budget",
            cost_policy=cost_policy,
            default=default_public_media_budget,
        ),
        "exploration_specialist": _resolved_policy_positive_int(
            request_context,
            key="exploration_worker_unit_budget",
            cost_policy=cost_policy,
            default=default_exploration_budget,
        ),
    }


def resolved_candidate_artifact_parallelism(
    request_context: dict[str, Any] | None,
    *,
    candidate_count: int,
    default_parallel_min_candidates: int,
    default_max_workers: int,
) -> dict[str, int]:
    normalized_candidate_count = max(0, int(candidate_count or 0))
    min_candidates = (
        resolved_runtime_positive_int(
            request_context,
            key="candidate_artifact_parallel_min_candidates",
        )
        or max(1, int(default_parallel_min_candidates or 1))
    )
    max_workers = (
        resolved_runtime_positive_int(
            request_context,
            key="candidate_artifact_max_workers",
        )
        or max(1, int(default_max_workers or 1))
    )
    if normalized_candidate_count <= 1 or normalized_candidate_count < min_candidates:
        parallel_workers = 1
    else:
        parallel_workers = max(1, min(normalized_candidate_count, max_workers))
    return {
        "candidate_count": normalized_candidate_count,
        "min_candidates": min_candidates,
        "max_workers": max_workers,
        "parallel_workers": parallel_workers,
        "cpu_count": max(1, int(os.cpu_count() or 1)),
    }


def _coerce_nonnegative_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        value = normalized
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return None


def _coerce_positive_int(value: Any) -> int | None:
    coerced = _coerce_nonnegative_int(value)
    if coerced is None or coerced <= 0:
        return None
    return coerced


def _coerce_nonnegative_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        value = normalized
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return None
