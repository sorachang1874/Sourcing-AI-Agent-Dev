"""M2.1 — ProviderTaskSpec registry (data layer; zero execution change).

A ``ProviderTaskSpec`` is the provider-leaf analog of ``CommandTypeSpec``: a
frozen, registry-stored, golden-pinned record describing how one external-provider
call runs as a durable task on the M1 command substrate. It is **pure data** —
behavior (reshape ladders, retryable classifiers, idempotency recipes, budget
resolvers) is referenced by name and resolved at dispatch time in later M2
increments. Every spec binds to a registered ``CommandTypeSpec``.

The field set is the R1-spike-refined one (see ``docs/M2_PROVIDER_TASK_RUNTIME_DESIGN.md``
§9): the submit strategy expresses a compound primary->fallback mode; retry carries
an explicit ``granularity`` (batch | item | work_reshape); and the three fallback
axes — submit-strategy / readiness-mechanism / provider-downgrade — are kept
separate rather than conflated into one ``fallback_chain``.
"""

from __future__ import annotations

from dataclasses import dataclass

from .durable_runtime import (
    DEFAULT_COMMAND_TYPE_SPECS,
    LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
    LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
    PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
)

# ── submit modes (R1 #1: a submit strategy is primary + optional fallback) ──
SUBMIT_MODE_SYNC_RUN = "sync_run"
SUBMIT_MODE_ASYNC_SUBMIT_POLL = "async_submit_poll"
SUBMIT_MODE_BATCH_SUBMIT_POLL_FETCH = "batch_submit_poll_fetch"
SUBMIT_MODE_SINGLE_UNIT_DIRECT = "single_unit_direct"
SUBMIT_MODE_NONE = ""

_SUBMIT_PRIMARY_MODES = frozenset(
    {SUBMIT_MODE_SYNC_RUN, SUBMIT_MODE_ASYNC_SUBMIT_POLL, SUBMIT_MODE_BATCH_SUBMIT_POLL_FETCH}
)
_SUBMIT_FALLBACK_MODES = frozenset({SUBMIT_MODE_NONE, SUBMIT_MODE_ASYNC_SUBMIT_POLL, SUBMIT_MODE_SINGLE_UNIT_DIRECT})

# ── readiness signals (R1 #3: readiness-mechanism fallback is its own axis) ──
READINESS_WEBHOOK_PRIMARY_POLL_FALLBACK = "webhook_primary_poll_fallback"
READINESS_POLL_ONLY = "poll_only"
READINESS_SYNCHRONOUS = "synchronous"
READINESS_FALLBACK_DIRECT_PROBE = "direct_probe"
READINESS_FALLBACK_LOCAL_EVENT_WATCHER = "local_event_watcher"
READINESS_FALLBACK_NONE = ""

_READINESS_PRIMARY_SIGNALS = frozenset(
    {READINESS_WEBHOOK_PRIMARY_POLL_FALLBACK, READINESS_POLL_ONLY, READINESS_SYNCHRONOUS}
)
_READINESS_FALLBACK_MECHANISMS = frozenset(
    {READINESS_FALLBACK_NONE, READINESS_FALLBACK_DIRECT_PROBE, READINESS_FALLBACK_LOCAL_EVENT_WATCHER}
)

# ── retry granularity (R1 #2: not a uniform max_attempts+backoff) ──
RETRY_GRANULARITY_BATCH = "batch"
RETRY_GRANULARITY_ITEM = "item"
RETRY_GRANULARITY_WORK_RESHAPE = "work_reshape"

_RETRY_GRANULARITIES = frozenset({RETRY_GRANULARITY_BATCH, RETRY_GRANULARITY_ITEM, RETRY_GRANULARITY_WORK_RESHAPE})


@dataclass(frozen=True)
class RetryContract:
    """How a provider task retries. Pure data; resolvers/classifiers by name.

    ``granularity`` selects the retry unit: ``batch`` (re-submit the whole batch),
    ``item`` (retry individual items independently), or ``work_reshape`` (re-partition
    the unresolved work into a shrinking batch ladder, then a single-unit direct tail).
    ``reshape_ladder_key`` is required iff granularity is ``work_reshape``.
    """

    granularity: str
    max_attempts: int  # per the granularity unit; 0 = ladder-driven (work_reshape)
    backoff_seconds_key: str  # runtime_tuning resolver name ("" if n/a)
    reshape_ladder_key: str  # work_reshape only: batch-size ladder resolver/fn name ("" otherwise)
    retryable_classifier: str  # getattr-resolved classifier (fn or exception class name)


@dataclass(frozen=True)
class ProviderTaskSpec:
    """Frozen typed description of one external-provider durable task."""

    provider_task_type: str
    provider_family: str
    command_type: str  # MUST be a registered CommandTypeSpec
    submit_primary_mode: str  # SUBMIT_MODE_* (primary)
    submit_fallback_mode: str  # SUBMIT_MODE_* (submit-strategy fallback; "" if none)
    readiness_primary_signal: str  # READINESS_* (primary)
    readiness_fallback_mechanism: str  # READINESS_FALLBACK_* (readiness-mechanism fallback; "" if none)
    after_start_mode: str  # PROVIDER_AFTER_START_CONTROL_MODE_*
    inflight_budget_key: str  # runtime_tuning inflight-budget resolver name
    cost_budget_key: str  # cost / lane-budget resolver name
    retry: RetryContract
    identity_key_recipe: str  # getattr-resolved idempotency-key fn name
    provider_fallback_chain: tuple[str, ...]  # provider-DOWNGRADE chain only (not submit/readiness fallbacks)
    terminal_admit_handler: str  # terminal-admission command type / handler name ("" if none)


# Registry. Seeded (M2.1) with the two providers validated by the R1 spike: the
# existing operation_native_profile_fetch family (Apify Harvest) and DataForSEO
# discovery. Both bind to provider_attempt CommandTypeSpecs with POLL_CANCEL_QUARANTINE.
DEFAULT_PROVIDER_TASK_SPECS: dict[str, ProviderTaskSpec] = {
    "harvest.profile_batch": ProviderTaskSpec(
        provider_task_type="harvest.profile_batch",
        provider_family="apify_harvest",
        command_type=LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
        submit_primary_mode=SUBMIT_MODE_SYNC_RUN,  # run-sync-get-dataset-items
        submit_fallback_mode=SUBMIT_MODE_ASYNC_SUBMIT_POLL,  # R1 #1: sync->async fallback
        readiness_primary_signal=READINESS_WEBHOOK_PRIMARY_POLL_FALLBACK,
        readiness_fallback_mechanism=READINESS_FALLBACK_LOCAL_EVENT_WATCHER,
        after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
        inflight_budget_key="resolved_harvest_profile_actor_global_inflight",
        cost_budget_key="resolved_harvest_profile_lane_budget_cap",
        retry=RetryContract(
            granularity=RETRY_GRANULARITY_WORK_RESHAPE,  # R1 #2: re-batch unresolved URLs
            max_attempts=0,
            backoff_seconds_key="resolved_harvest_retry_backoff_seconds",
            reshape_ladder_key="_live_harvest_profile_retry_batch_sizes",
            retryable_classifier="HarvestRetryableRequestError",
        ),
        identity_key_recipe="profile_url_identity_key",
        provider_fallback_chain=(),
        terminal_admit_handler=LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
    ),
    "dataforseo.discovery_query": ProviderTaskSpec(
        provider_task_type="dataforseo.discovery_query",
        provider_family="dataforseo",
        command_type=LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
        submit_primary_mode=SUBMIT_MODE_BATCH_SUBMIT_POLL_FETCH,  # 3-phase
        submit_fallback_mode=SUBMIT_MODE_NONE,
        readiness_primary_signal=READINESS_POLL_ONLY,  # tasks_ready
        readiness_fallback_mechanism=READINESS_FALLBACK_DIRECT_PROBE,  # R1 #3: per-task ThreadPool probe
        after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
        inflight_budget_key="resolved_dataforseo_batch_submit_global_inflight",
        cost_budget_key="resolved_search_lane_budget_cap",
        retry=RetryContract(
            granularity=RETRY_GRANULARITY_ITEM,  # R1 #2: per-item, MAX=1
            max_attempts=1,
            backoff_seconds_key="resolved_dataforseo_ready_poll_cooldown_seconds",
            reshape_ladder_key="",
            retryable_classifier="_dataforseo_retryable_status_code",
        ),
        identity_key_recipe="dataforseo_task_query_identity_key",
        provider_fallback_chain=(),
        terminal_admit_handler="",
    ),
}


def provider_task_spec(provider_task_type: str) -> ProviderTaskSpec | None:
    return DEFAULT_PROVIDER_TASK_SPECS.get(str(provider_task_type or "").strip())


def unregistered_command_type_bindings() -> list[str]:
    """provider_task_types whose command_type is NOT a registered CommandTypeSpec.

    M2 invariant: every provider task binds to a real durable command type. A
    non-empty result is a registry defect.
    """
    return sorted(
        spec.provider_task_type
        for spec in DEFAULT_PROVIDER_TASK_SPECS.values()
        if spec.command_type not in DEFAULT_COMMAND_TYPE_SPECS
    )
