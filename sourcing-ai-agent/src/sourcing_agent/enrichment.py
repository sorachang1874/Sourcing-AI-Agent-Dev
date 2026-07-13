from __future__ import annotations

import json
import os
import re
import threading
import time
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from html import unescape
from itertools import combinations
from pathlib import Path
from typing import Any, Callable
from urllib import error, parse, request
from xml.etree import ElementTree as ET

from .agent_runtime import AgentRuntimeCoordinator
from .asset_catalog import AssetCatalog
from .asset_logger import AssetLogger
from .connectors import CompanyIdentity, RapidApiAccount, profile_detail_accounts, search_people_accounts
from .domain import (
    Candidate,
    EvidenceRecord,
    JobRequest,
    make_candidate_id,
    make_evidence_id,
    merge_candidate,
    normalize_name_token,
)
from .durable_runtime import (
    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_OWNER,
    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_OWNER,
    DurableRuntimeWriter,
    legacy_job_operation_id,
    legacy_job_workflow_run_id,
    linkedin_profile_refill_submit_idempotency_key,
    linkedin_profile_url_terminal_record_idempotency_key,
)
from .exploratory_enrichment import ExploratoryWebEnricher
from .harvest_connectors import (
    HarvestProfileConnector,
    HarvestProfileSearchConnector,
    harvest_connector_available,
    parse_harvest_profile_payload,
    write_harvest_execution_artifact,
)
from .linkedin_url_normalization import normalize_linkedin_profile_url_key
from .model_provider import ModelClient
from .profile_registry_utils import (
    extract_profile_registry_aliases_from_payload,
    harvest_profile_payload_has_usable_content,
    profile_cache_path_candidates,
)
from .repositories import linkedin_profile_registry_repo
from .runtime_environment import (
    assert_live_provider_access_allowed,
    external_provider_mode,
    infer_runtime_dir_from_path,
)
from .runtime_tuning import (
    acquire_runtime_provider_limiter_slot,
    release_runtime_provider_limiter_slot,
    resolve_runtime_timing_overrides,
    resolved_harvest_profile_actor_global_inflight,
    resolved_harvest_profile_batch_submit_global_inflight,
    resolved_parallel_exploration_workers,
    runtime_inflight_slot,
)
from .search_provider import BaseSearchProvider, DuckDuckGoHtmlSearchProvider, search_response_to_record
from .storage import ControlPlaneStore

TECHNICAL_SIGNAL_TOKENS = {
    "engineer",
    "engineering",
    "research",
    "scientist",
    "technical staff",
    "infrastructure",
    "machine learning",
    "ml",
    "ai",
    "training",
    "inference",
    "gpu",
    "systems",
    "distributed",
}
_REMOTE_HARVEST_PROFILE_WORKER_STATUSES = {"queued", "waiting_remote_harvest"}
_TERMINAL_HARVEST_PROFILE_WORKER_STATUSES = {
    "completed",
    "failed",
    "skipped",
    "cancelled",
    "canceled",
    "superseded",
    "interrupted",
}

SUSPICIOUS_PROFILE_KEYWORDS = {
    "spiritual",
    "healer",
    "healing",
    "psychic",
    "tarot",
    "astrology",
    "spell",
    "sorcery",
    "witchcraft",
    "whatsapp",
    "telegram",
    "روحاني",
    "الروحانية",
    "الروحية",
    "السحر",
    "الحسد",
    "العين",
    "الأرزاق",
    "الارزاق",
    "تنزيل الأموال",
    "تنزيل الاموال",
    "الأوراد",
    "الاوراد",
    "الطاقي",
    "النورانية",
    "الخدام",
}

ACK_STOPWORDS = {
    "anthropic",
    "xai",
    "acknowledgements",
    "acknowledgment",
    "appendix",
    "references",
    "thanks",
    "thank",
    "figure",
    "section",
    "supplementary material",
}

HARVEST_PROFILE_PREFETCH_BATCH_SIZE = 250
HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE = 100
HARVEST_PROFILE_LIVE_FETCH_MAX_CONCURRENCY = 3
HARVEST_PROFILE_NONLIVE_FETCH_CONCURRENCY = 4
PROFILE_REGISTRY_LEASE_SECONDS = 240
PROFILE_REGISTRY_LEASE_WAIT_SECONDS = 18.0
PROFILE_REGISTRY_LEASE_POLL_SECONDS = 0.8


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _safe_int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _external_provider_mode() -> str:
    return external_provider_mode()


def _runtime_timing_overrides_from_request_payload(request_payload: dict[str, Any] | None) -> dict[str, Any]:
    if not request_payload:
        return {}
    execution_preferences = dict(JobRequest.from_payload(dict(request_payload or {})).execution_preferences or {})
    return resolve_runtime_timing_overrides(execution_preferences)


def _runtime_tuning_context_from_request_payload(request_payload: dict[str, Any] | None) -> dict[str, Any]:
    if not request_payload:
        return {}
    execution_preferences = dict(JobRequest.from_payload(dict(request_payload or {})).execution_preferences or {})
    return {
        **resolve_runtime_timing_overrides(execution_preferences),
        **execution_preferences,
    }


def _harvest_profile_batch_runtime_timing_overrides(request_payload: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "harvest_run_status_timeout_seconds": 15,
        "harvest_run_status_wait_for_finish_seconds": 10,
        "harvest_dataset_page_timeout_seconds": 15,
        "harvest_dataset_fetch_max_attempts": 1,
        **_runtime_tuning_context_from_request_payload(request_payload),
    }


def _provider_webhook_url_configured() -> bool:
    return bool(
        str(os.getenv("SOURCING_APIFY_WEBHOOK_URL") or "").strip() or str(os.getenv("APIFY_WEBHOOK_URL") or "").strip()
    )


def _local_provider_event_watch_with_webhook_enabled() -> bool:
    return _env_bool("SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED", True)


def _local_provider_event_watch_max_seconds(request_context: dict[str, Any] | None) -> int:
    context = dict(request_context or {})
    for raw_value in (
        context.get("local_provider_event_watch_max_seconds"),
        os.getenv("SOURCING_LOCAL_PROVIDER_EVENT_WATCH_MAX_SECONDS"),
    ):
        if raw_value is None:
            continue
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return 1800


_LOCAL_PROVIDER_EVENT_CALLBACK_SEMAPHORES_LOCK = threading.Lock()
_LOCAL_PROVIDER_EVENT_CALLBACK_SEMAPHORES: dict[int, threading.BoundedSemaphore] = {}
_LOCAL_PROVIDER_EVENT_WATCHER_LEASE_SECONDS = 7200


def _local_provider_event_callback_semaphore() -> tuple[threading.BoundedSemaphore, int]:
    concurrency = max(1, _env_int("SOURCING_LOCAL_PROVIDER_EVENT_CALLBACK_CONCURRENCY", 1))
    with _LOCAL_PROVIDER_EVENT_CALLBACK_SEMAPHORES_LOCK:
        semaphore = _LOCAL_PROVIDER_EVENT_CALLBACK_SEMAPHORES.get(concurrency)
        if semaphore is None:
            semaphore = threading.BoundedSemaphore(concurrency)
            _LOCAL_PROVIDER_EVENT_CALLBACK_SEMAPHORES[concurrency] = semaphore
    return semaphore, concurrency


def _invoke_local_provider_event_callback_serialized(
    callback: Callable[[dict[str, Any]], Any],
    payload: dict[str, Any],
    *,
    before_invoke: Callable[[], bool] | None = None,
) -> Any:
    semaphore, concurrency = _local_provider_event_callback_semaphore()
    wait_started = time.perf_counter()
    semaphore.acquire()
    wait_ms = int(max(0.0, (time.perf_counter() - wait_started) * 1000))
    try:
        if before_invoke is not None and not before_invoke():
            return {
                "status": "skipped",
                "reason": "local_provider_event_callback_guard_rejected",
                "callback_serialization": {
                    "mode": "bounded_local_provider_event_callback",
                    "concurrency": concurrency,
                    "wait_ms": wait_ms,
                },
            }
        return callback(
            {
                **dict(payload or {}),
                "callback_serialization": {
                    "mode": "bounded_local_provider_event_callback",
                    "concurrency": concurrency,
                    "wait_ms": wait_ms,
                },
            }
        )
    finally:
        semaphore.release()


def _local_provider_event_watcher_lease_is_active(
    lease: dict[str, Any] | None,
    *,
    run_id: str,
    dataset_id: str,
    worker_id: int,
    now: datetime | None = None,
) -> bool:
    payload = dict(lease or {})
    normalized_run_id = str(run_id or "").strip()
    normalized_dataset_id = str(dataset_id or "").strip()
    if not normalized_run_id:
        return False
    if str(payload.get("run_id") or "").strip() != normalized_run_id:
        return False
    if normalized_dataset_id and str(payload.get("dataset_id") or "").strip() != normalized_dataset_id:
        return False
    if int(payload.get("worker_id") or 0) != int(worker_id or 0):
        return False
    expires_at = _profile_prefetch_queue_timestamp(payload.get("expires_at"))
    if expires_at is None:
        return False
    observed_at = now or datetime.now(timezone.utc)
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return expires_at > observed_at.astimezone(timezone.utc)


def _local_provider_event_watcher_terminal_marker_present(
    checkpoint: dict[str, Any] | None,
    *,
    run_id: str,
    dataset_id: str,
) -> bool:
    payload = dict(checkpoint or {})
    if str(payload.get("remote_provider_terminal_event_seen_at") or "").strip():
        event = dict(payload.get("remote_provider_terminal_event") or {})
        event_run_id = str(event.get("run_id") or event.get("actor_run_id") or event.get("actorRunId") or "").strip()
        event_dataset_id = str(
            event.get("dataset_id") or event.get("default_dataset_id") or event.get("defaultDatasetId") or ""
        ).strip()
        if not event or event_run_id == str(run_id or "").strip() or event_dataset_id == str(dataset_id or "").strip():
            return True
    event = dict(payload.get("remote_provider_terminal_event") or {})
    if not event:
        return False
    event_run_id = str(event.get("run_id") or event.get("actor_run_id") or event.get("actorRunId") or "").strip()
    event_dataset_id = str(
        event.get("dataset_id") or event.get("default_dataset_id") or event.get("defaultDatasetId") or ""
    ).strip()
    if event_run_id and event_run_id != str(run_id or "").strip():
        return False
    if event_dataset_id and event_dataset_id != str(dataset_id or "").strip():
        return False
    status = str(event.get("status") or "").strip().upper().replace("_", "-")
    event_type = str(event.get("event_type") or event.get("eventType") or "").strip().upper()
    return (
        bool(event.get("is_terminal"))
        or status in {"SUCCEEDED", "FAILED", "TIMED-OUT", "TIMED_OUT"}
        or "SUCCEEDED" in event_type
    )


def _local_provider_event_watcher_lease_payload(
    *,
    run_id: str,
    dataset_id: str,
    worker_id: int,
    payload_hash: str,
    thread_name: str,
    max_watch_seconds: int,
) -> dict[str, Any]:
    scheduled_at = datetime.now(timezone.utc).replace(microsecond=0)
    lease_seconds = max(1, min(int(max_watch_seconds or 0) + 60, _LOCAL_PROVIDER_EVENT_WATCHER_LEASE_SECONDS))
    return {
        "status": "scheduled",
        "run_id": str(run_id or "").strip(),
        "dataset_id": str(dataset_id or "").strip(),
        "worker_id": int(worker_id or 0),
        "payload_hash": str(payload_hash or "").strip(),
        "thread_name": str(thread_name or "").strip(),
        "scheduled_at": scheduled_at.isoformat(),
        "expires_at": (scheduled_at + timedelta(seconds=lease_seconds)).isoformat(),
    }


def _apify_event_type_for_run_status(status: str) -> str:
    normalized = str(status or "").strip().upper().replace("_", "-")
    if normalized == "SUCCEEDED":
        return "ACTOR.RUN.SUCCEEDED"
    if normalized == "FAILED":
        return "ACTOR.RUN.FAILED"
    if normalized == "TIMED-OUT":
        return "ACTOR.RUN.TIMED_OUT"
    if normalized == "ABORTED":
        return "ACTOR.RUN.ABORTED"
    return ""


def _harvest_remote_identifiers_from_payload(payload: Any) -> tuple[str, str]:
    if not isinstance(payload, dict):
        return "", ""
    record = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    run_id = str(
        record.get("id") or record.get("runId") or record.get("actor_run_id") or record.get("actorRunId") or ""
    ).strip()
    dataset_id = str(
        record.get("defaultDatasetId")
        or record.get("datasetId")
        or record.get("dataset_id")
        or record.get("default_dataset_id")
        or ""
    ).strip()
    return run_id, dataset_id


def _harvest_remote_identifiers_from_artifacts(artifact_paths: dict[str, str]) -> tuple[str, str]:
    run_id = ""
    dataset_id = ""
    for label in ("run_get", "run_post", "cache_hit"):
        path_value = str(dict(artifact_paths or {}).get(label) or "").strip()
        if not path_value:
            continue
        path = Path(path_value).expanduser()
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        candidate_run_id, candidate_dataset_id = _harvest_remote_identifiers_from_payload(payload)
        run_id = run_id or candidate_run_id
        dataset_id = dataset_id or candidate_dataset_id
        if run_id and dataset_id:
            break
    return run_id, dataset_id


def _harvest_execution_remote_identifiers(
    checkpoint: dict[str, Any],
    artifact_paths: dict[str, str],
) -> tuple[str, str]:
    run_id = str(
        checkpoint.get("run_id") or checkpoint.get("actor_run_id") or checkpoint.get("actorRunId") or ""
    ).strip()
    dataset_id = str(
        checkpoint.get("dataset_id") or checkpoint.get("default_dataset_id") or checkpoint.get("defaultDatasetId") or ""
    ).strip()
    if run_id and dataset_id:
        return run_id, dataset_id
    artifact_run_id, artifact_dataset_id = _harvest_remote_identifiers_from_artifacts(artifact_paths)
    return run_id or artifact_run_id, dataset_id or artifact_dataset_id


HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE = max(
    1,
    _env_int("HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE", 25),
)
HARVEST_PROFILE_PREFETCH_BATCH_SIZE = max(
    HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE,
    _env_int("HARVEST_PROFILE_PREFETCH_BATCH_SIZE", 250),
)
HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE_LIVE = max(
    1,
    _env_int("HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE_LIVE", 75),
)
HARVEST_PROFILE_PREFETCH_BATCH_SIZE_LIVE = max(
    HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE_LIVE,
    _env_int("HARVEST_PROFILE_PREFETCH_BATCH_SIZE_LIVE", 150),
)
HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE_LIVE = max(
    1,
    _env_int("HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE_LIVE", 150),
)
HARVEST_PROFILE_MIN_NON_TAIL_BATCH_SIZE = max(
    1,
    _env_int("HARVEST_PROFILE_MIN_NON_TAIL_BATCH_SIZE", 10),
)
HARVEST_PROFILE_TINY_BATCH_MAX_SIZE = max(
    1,
    _env_int("HARVEST_PROFILE_TINY_BATCH_MAX_SIZE", 5),
)
HARVEST_PROFILE_LOW_VOLUME_COMPANY_MAX_URLS = max(
    HARVEST_PROFILE_TINY_BATCH_MAX_SIZE,
    _env_int("HARVEST_PROFILE_LOW_VOLUME_COMPANY_MAX_URLS", 20),
)
HARVEST_PROFILE_DISPATCH_CLAIM_TTL_SECONDS = max(
    5,
    _env_int("HARVEST_PROFILE_DISPATCH_CLAIM_TTL_SECONDS", 120),
)
HARVEST_PROFILE_TINY_TAIL_COALESCING_MIN_AGE_MS = max(
    0,
    _env_int("HARVEST_PROFILE_TINY_TAIL_COALESCING_MIN_AGE_MS", 1_000),
)
HARVEST_PROFILE_PREFETCH_ACTOR_SLOT_URL_TARGET = max(
    1,
    _env_int("HARVEST_PROFILE_PREFETCH_ACTOR_SLOT_URL_TARGET", 50),
)
HARVEST_PROFILE_PREFETCH_SCALE_THRESHOLD_URLS = max(
    HARVEST_PROFILE_PREFETCH_ACTOR_SLOT_URL_TARGET,
    _env_int("HARVEST_PROFILE_PREFETCH_SCALE_THRESHOLD_URLS", 400),
)
HARVEST_PROFILE_PREFETCH_MAX_BATCH_COUNT_FOR_LARGE_READY_SET = max(
    1,
    _env_int("HARVEST_PROFILE_PREFETCH_MAX_BATCH_COUNT_FOR_LARGE_READY_SET", 8),
)
HARVEST_PROFILE_PREFETCH_DURABLE_UNIT_MAX_URLS = max(
    HARVEST_PROFILE_PREFETCH_ACTOR_SLOT_URL_TARGET,
    _env_int("HARVEST_PROFILE_PREFETCH_DURABLE_UNIT_MAX_URLS", 200),
)
HARVEST_PROFILE_PREFETCH_PROVIDER_ENVELOPE_MAX_URLS = max(
    HARVEST_PROFILE_PREFETCH_DURABLE_UNIT_MAX_URLS,
    _env_int("HARVEST_PROFILE_PREFETCH_PROVIDER_ENVELOPE_MAX_URLS", 300),
)
HARVEST_PROFILE_TERMINAL_PERSIST_URL_LIMIT = max(
    1,
    _env_int("HARVEST_PROFILE_TERMINAL_PERSIST_URL_LIMIT", 100),
)
HARVEST_PROFILE_TERMINAL_PERSIST_CHUNK_URLS = max(
    1,
    min(
        HARVEST_PROFILE_TERMINAL_PERSIST_URL_LIMIT,
        _env_int("HARVEST_PROFILE_TERMINAL_PERSIST_CHUNK_URLS", 25),
    ),
)
HARVEST_PROFILE_TERMINAL_PERSIST_BUDGET_MS = max(
    0,
    _env_int("HARVEST_PROFILE_TERMINAL_PERSIST_BUDGET_MS", 12_000),
)
PROFILE_REFILL_NORMAL_QUEUE_STATES = (
    "deferred_budget",
    "deferred_coalescing",
    "dispatch_reserved",
    "dispatch_claimed",
)
PROFILE_REFILL_RETRY_QUEUE_STATES = ("retry_wait",)
PROFILE_REFILL_PROVIDER_OWNED_QUEUE_STATE = "planned_dispatch"
PROFILE_REFILL_RETRY_PROVIDER_SUBMIT_TRIGGER_KIND = "profile_retry_provider_submit"
PROFILE_REFILL_RETRY_PROVIDER_SUBMITTED_PLAN_REASON = "retry_remote_provider_submitted"
PROFILE_REFILL_RETRY_PROVIDER_SUBMIT_MARKERS = {
    PROFILE_REFILL_RETRY_PROVIDER_SUBMIT_TRIGGER_KIND,
    PROFILE_REFILL_RETRY_PROVIDER_SUBMITTED_PLAN_REASON,
}


def _harvest_profile_priority_prefetch_batch_size() -> int:
    return max(
        1, _env_int("HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE", HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE)
    )


def _harvest_profile_prefetch_batch_size() -> int:
    return max(
        _harvest_profile_priority_prefetch_batch_size(),
        _env_int("HARVEST_PROFILE_PREFETCH_BATCH_SIZE", HARVEST_PROFILE_PREFETCH_BATCH_SIZE),
    )


def _harvest_profile_priority_prefetch_batch_size_live() -> int:
    return max(
        1,
        _env_int(
            "HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE_LIVE", HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE_LIVE
        ),
    )


def _harvest_profile_prefetch_batch_size_live() -> int:
    return max(
        _harvest_profile_priority_prefetch_batch_size_live(),
        _env_int("HARVEST_PROFILE_PREFETCH_BATCH_SIZE_LIVE", HARVEST_PROFILE_PREFETCH_BATCH_SIZE_LIVE),
    )


def _harvest_profile_live_fetch_batch_size_live() -> int:
    return max(1, _env_int("HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE_LIVE", HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE_LIVE))


def _harvest_profile_min_non_tail_batch_size() -> int:
    return max(1, _env_int("HARVEST_PROFILE_MIN_NON_TAIL_BATCH_SIZE", HARVEST_PROFILE_MIN_NON_TAIL_BATCH_SIZE))


def _harvest_profile_tiny_batch_max_size() -> int:
    return max(1, _env_int("HARVEST_PROFILE_TINY_BATCH_MAX_SIZE", HARVEST_PROFILE_TINY_BATCH_MAX_SIZE))


def _harvest_profile_low_volume_company_max_urls() -> int:
    return max(
        _harvest_profile_tiny_batch_max_size(),
        _env_int("HARVEST_PROFILE_LOW_VOLUME_COMPANY_MAX_URLS", HARVEST_PROFILE_LOW_VOLUME_COMPANY_MAX_URLS),
    )


def _harvest_profile_tiny_tail_coalescing_min_age_ms() -> int:
    return max(
        0,
        _env_int(
            "HARVEST_PROFILE_TINY_TAIL_COALESCING_MIN_AGE_MS",
            HARVEST_PROFILE_TINY_TAIL_COALESCING_MIN_AGE_MS,
        ),
    )


def _harvest_profile_prefetch_actor_slot_url_target() -> int:
    return max(
        1,
        _env_int(
            "HARVEST_PROFILE_PREFETCH_ACTOR_SLOT_URL_TARGET",
            HARVEST_PROFILE_PREFETCH_ACTOR_SLOT_URL_TARGET,
        ),
    )


def _harvest_profile_prefetch_scale_threshold_urls() -> int:
    return max(
        _harvest_profile_prefetch_actor_slot_url_target(),
        _env_int(
            "HARVEST_PROFILE_PREFETCH_SCALE_THRESHOLD_URLS",
            HARVEST_PROFILE_PREFETCH_SCALE_THRESHOLD_URLS,
        ),
    )


def _harvest_profile_prefetch_max_batch_count_for_large_ready_set() -> int:
    return max(
        1,
        _env_int(
            "HARVEST_PROFILE_PREFETCH_MAX_BATCH_COUNT_FOR_LARGE_READY_SET",
            HARVEST_PROFILE_PREFETCH_MAX_BATCH_COUNT_FOR_LARGE_READY_SET,
        ),
    )


def _harvest_profile_prefetch_durable_unit_max_urls() -> int:
    return max(
        _harvest_profile_prefetch_actor_slot_url_target(),
        _env_int(
            "HARVEST_PROFILE_PREFETCH_DURABLE_UNIT_MAX_URLS",
            HARVEST_PROFILE_PREFETCH_DURABLE_UNIT_MAX_URLS,
        ),
    )


def _harvest_profile_prefetch_provider_envelope_max_urls() -> int:
    return max(
        _harvest_profile_prefetch_durable_unit_max_urls(),
        _env_int(
            "HARVEST_PROFILE_PREFETCH_PROVIDER_ENVELOPE_MAX_URLS",
            HARVEST_PROFILE_PREFETCH_PROVIDER_ENVELOPE_MAX_URLS,
        ),
    )


def _harvest_profile_terminal_persist_url_limit() -> int:
    return max(
        1,
        _env_int(
            "HARVEST_PROFILE_TERMINAL_PERSIST_URL_LIMIT",
            HARVEST_PROFILE_TERMINAL_PERSIST_URL_LIMIT,
        ),
    )


def _harvest_profile_terminal_persist_chunk_urls() -> int:
    return max(
        1,
        min(
            _harvest_profile_terminal_persist_url_limit(),
            _env_int(
                "HARVEST_PROFILE_TERMINAL_PERSIST_CHUNK_URLS",
                HARVEST_PROFILE_TERMINAL_PERSIST_CHUNK_URLS,
            ),
        ),
    )


def _harvest_profile_terminal_persist_budget_ms() -> int:
    return max(
        0,
        _env_int(
            "HARVEST_PROFILE_TERMINAL_PERSIST_BUDGET_MS",
            HARVEST_PROFILE_TERMINAL_PERSIST_BUDGET_MS,
        ),
    )


def _recommended_harvest_profile_prefetch_actor_slot_batch_size(total_urls: int) -> tuple[int, str]:
    count = max(0, int(total_urls or 0))
    actor_slot_target = _harvest_profile_prefetch_actor_slot_url_target()
    if count <= 0:
        return actor_slot_target, "no_ready_urls"
    if count <= actor_slot_target:
        return actor_slot_target, "actor_slot_item_packing"
    provider_envelope_max = _harvest_profile_prefetch_provider_envelope_max_urls()
    if count > actor_slot_target and count <= provider_envelope_max:
        return count, "single_durable_unit_ready_set"
    scale_threshold = _harvest_profile_prefetch_scale_threshold_urls()
    max_batch_count = _harvest_profile_prefetch_max_batch_count_for_large_ready_set()
    target_batch_count = max(1, (count + provider_envelope_max - 1) // provider_envelope_max)
    if target_batch_count <= max_batch_count:
        balanced_size = max(
            actor_slot_target,
            min(provider_envelope_max, (count + target_batch_count - 1) // target_batch_count),
        )
        reason = (
            "large_ready_set_provider_envelope_target"
            if count > scale_threshold
            else "bounded_ready_set_balanced_provider_envelopes"
        )
        return balanced_size, reason
    return provider_envelope_max, "large_ready_set_provider_envelope_cap"


def _profile_prefetch_source_mix_value(source_mix: dict[str, Any] | None, key: str) -> int:
    try:
        return int(dict(source_mix or {}).get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _should_split_roster_ready_set_across_actor_slots(
    *,
    total_urls: int,
    source_mix: dict[str, Any] | None,
    available_new_worker_count: int,
) -> bool:
    """Roster waves should use open actor slots instead of becoming one large shard."""

    count = max(0, int(total_urls or 0))
    actor_slot_target = _harvest_profile_prefetch_actor_slot_url_target()
    durable_unit_max = _harvest_profile_prefetch_durable_unit_max_urls()
    if count <= actor_slot_target or count > durable_unit_max:
        return False
    if max(0, int(available_new_worker_count or 0)) < 2:
        return False
    roster_count = _profile_prefetch_source_mix_value(source_mix, "company_roster")
    if roster_count <= 0:
        return False
    labeled_count = sum(
        _profile_prefetch_source_mix_value(source_mix, key)
        for key in ("company_roster", "profile_search", "targeted", "other")
    )
    return roster_count / max(1, labeled_count) >= 0.5


def _recommended_harvest_profile_prefetch_batch_size(total_candidates: int, *, priority: bool) -> int:
    default_size = (
        _harvest_profile_priority_prefetch_batch_size() if priority else _harvest_profile_prefetch_batch_size()
    )
    count = max(0, int(total_candidates or 0))
    if _external_provider_mode() == "live":
        live_default_size = (
            _harvest_profile_priority_prefetch_batch_size_live()
            if priority
            else _harvest_profile_prefetch_batch_size_live()
        )
        if priority:
            if count >= 1000:
                return min(live_default_size, 100)
            if count >= 200:
                return min(live_default_size, 75)
            return live_default_size
        if count >= 2000:
            return min(live_default_size, 200)
        if count >= 500:
            return min(live_default_size, 150)
        if count >= 150:
            return min(live_default_size, 100)
        return min(live_default_size, 75)
    if priority:
        if count >= 2000:
            return min(default_size, 20)
        if count >= 1000:
            return min(default_size, 25)
        if count >= 300:
            return min(default_size, 40)
        return default_size
    if count >= 5000:
        return min(default_size, 30)
    if count >= 2000:
        return min(default_size, 40)
    if count >= 1000:
        return min(default_size, 50)
    if count >= 300:
        return min(default_size, 75)
    return default_size


def _recommended_harvest_profile_live_fetch_batch_size(total_urls: int) -> int:
    count = max(0, int(total_urls or 0))
    if _external_provider_mode() == "live":
        live_fetch_batch_size = _harvest_profile_live_fetch_batch_size_live()
        if count >= 200:
            return live_fetch_batch_size
        if count >= 50:
            return min(live_fetch_batch_size, 100)
        return min(live_fetch_batch_size, 50)
    if count >= 2000:
        return min(HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE, 40)
    if count >= 500:
        return min(HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE, 50)
    if count >= 200:
        return min(HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE, 75)
    return HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE


def _harvest_profile_request_source_mix(
    source_shards_by_url: dict[str, list[str]] | None,
    *,
    total_urls: int,
) -> dict[str, int]:
    mix = {
        "company_roster": 0,
        "profile_search": 0,
        "targeted": 0,
        "other": 0,
    }
    for labels in dict(source_shards_by_url or {}).values():
        normalized_labels = " ".join(
            str(item or "").strip().lower() for item in list(labels or []) if str(item or "").strip()
        )
        if not normalized_labels:
            mix["other"] += 1
            continue
        if any(
            token in normalized_labels
            for token in (
                "publication_lead_targeted",
                "dataset:publication_lead",
                "dataset:targeted_name",
                "resolution_source:publication_lead",
            )
        ):
            mix["targeted"] += 1
            continue
        if any(
            token in normalized_labels
            for token in (
                "harvest_company_employees",
                "linkedin_company_people",
                "company_people",
                "company_roster",
                "linkedin_company_roster",
            )
        ):
            mix["company_roster"] += 1
            continue
        if "harvest_profile_search" in normalized_labels:
            mix["profile_search"] += 1
            continue
        mix["other"] += 1
    labeled_total = sum(mix.values())
    if labeled_total < max(0, int(total_urls or 0)):
        mix["other"] += max(0, int(total_urls or 0) - labeled_total)
    return mix


def _recommended_harvest_profile_live_fetch_window(
    total_urls: int,
    *,
    source_shards_by_url: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    count = max(0, int(total_urls or 0))
    source_mix = _harvest_profile_request_source_mix(
        source_shards_by_url,
        total_urls=count,
    )
    if count <= 0:
        return {
            "batch_size": 1,
            "max_workers": 1,
            "batch_count": 0,
            "source_mix": source_mix,
        }

    labeled_total = max(1, sum(source_mix.values()))
    roster_ratio = float(source_mix.get("company_roster") or 0) / labeled_total
    profile_search_ratio = float(source_mix.get("profile_search") or 0) / labeled_total
    targeted_ratio = float(source_mix.get("targeted") or 0) / labeled_total

    if _external_provider_mode() == "live":
        if count <= 40:
            desired_batch_count = 1
            target_workers = 1
        elif targeted_ratio >= 0.35:
            target_batch_size = 35 if count <= 120 else 45
            desired_batch_count = max(2, (count + target_batch_size - 1) // target_batch_size)
            target_workers = 2
        elif roster_ratio >= 0.6:
            if count >= 360:
                target_batch_size = 125
                minimum_batches = 3
            elif count >= 180:
                target_batch_size = 110
                minimum_batches = 2
            else:
                target_batch_size = 100
                minimum_batches = 2
            desired_batch_count = max(minimum_batches, (count + target_batch_size - 1) // target_batch_size)
            target_workers = 2 if count >= 180 else 1
        elif profile_search_ratio >= 0.5:
            target_batch_size = 45 if count < 240 else 55
            desired_batch_count = max(3 if count >= 120 else 2, (count + target_batch_size - 1) // target_batch_size)
            target_workers = min(4, desired_batch_count)
        else:
            target_batch_size = 40 if count < 120 else 60
            desired_batch_count = max(2 if count < 120 else 4, (count + target_batch_size - 1) // target_batch_size)
            target_workers = 2
    else:
        if count <= 60:
            desired_batch_count = 1
        else:
            target_batch_size = 40 if targeted_ratio >= 0.35 else 50
            desired_batch_count = max(2, (count + target_batch_size - 1) // target_batch_size)
        target_workers = HARVEST_PROFILE_NONLIVE_FETCH_CONCURRENCY

    desired_batch_count = max(1, desired_batch_count)
    batch_size = max(1, (count + desired_batch_count - 1) // desired_batch_count)
    batch_count = max(1, (count + batch_size - 1) // batch_size)
    return {
        "batch_size": batch_size,
        "max_workers": min(max(1, int(target_workers or 1)), batch_count),
        "batch_count": batch_count,
        "source_mix": source_mix,
    }


def _recommended_harvest_profile_prefetch_dispatch_window(
    total_urls: int,
    *,
    priority: bool,
    source_shards_by_url: dict[str, list[str]] | dict[str, set[str]] | None = None,
) -> dict[str, Any]:
    count = max(0, int(total_urls or 0))
    provider_mode = _external_provider_mode()
    normalized_source_shards = {
        str(profile_url or "").strip(): [
            str(label or "").strip() for label in list(labels or []) if str(label or "").strip()
        ]
        for profile_url, labels in dict(source_shards_by_url or {}).items()
        if str(profile_url or "").strip()
    }
    if provider_mode in {"live", "scripted"}:
        live_window = _recommended_harvest_profile_live_fetch_window(
            count,
            source_shards_by_url=normalized_source_shards,
        )
        batch_size, batch_size_reason = _recommended_harvest_profile_prefetch_actor_slot_batch_size(count)
        batch_count = max(1, (count + batch_size - 1) // batch_size) if count else 0
        base_strategy = (
            "adaptive_live_prefetch_window" if provider_mode == "live" else "adaptive_scripted_prefetch_window"
        )
        return {
            **live_window,
            "batch_size": batch_size,
            "batch_count": batch_count,
            "max_workers": min(max(1, int(live_window.get("max_workers") or 1)), max(batch_count, 1)),
            "strategy": (
                "actor_slot_item_packing_live_prefetch_window"
                if provider_mode == "live"
                else "actor_slot_item_packing_scripted_prefetch_window"
            ),
            "base_strategy": base_strategy,
            "batch_size_contract": "profile_actor_slot_ready_item_packing",
            "batch_size_reason": batch_size_reason,
            "actor_slot_url_target": _harvest_profile_prefetch_actor_slot_url_target(),
            "durable_unit_max_urls": _harvest_profile_prefetch_durable_unit_max_urls(),
            "provider_envelope_max_urls": _harvest_profile_prefetch_provider_envelope_max_urls(),
            "large_ready_set_max_batch_count": _harvest_profile_prefetch_max_batch_count_for_large_ready_set(),
            "large_ready_set_threshold_urls": _harvest_profile_prefetch_scale_threshold_urls(),
            "priority": bool(priority),
        }
    batch_size = _recommended_harvest_profile_prefetch_batch_size(count, priority=priority)
    batch_count = max(1, (count + batch_size - 1) // batch_size) if count else 0
    return {
        "batch_size": batch_size,
        "max_workers": HARVEST_PROFILE_NONLIVE_FETCH_CONCURRENCY,
        "batch_count": batch_count,
        "source_mix": _harvest_profile_request_source_mix(
            normalized_source_shards,
            total_urls=count,
        ),
        "strategy": "configured_prefetch_batch_size",
        "priority": bool(priority),
    }


def _chunk_strings(values: list[str], chunk_size: int) -> list[list[str]]:
    size = max(1, int(chunk_size or 1))
    return [values[index : index + size] for index in range(0, len(values), size)]


def _actor_slot_chunk_strings(values: list[str], chunk_size: int) -> list[list[str]]:
    """Pack ready URL items into actor slots before opening the next worker."""

    return _chunk_strings(values, chunk_size)


def _balanced_chunk_strings(values: list[str], chunk_size: int) -> list[list[str]]:
    normalized = [str(value or "").strip() for value in list(values or []) if str(value or "").strip()]
    if not normalized:
        return []
    size = max(1, int(chunk_size or 1))
    if len(normalized) <= size:
        return [normalized]
    chunk_count = max(1, (len(normalized) + size - 1) // size)
    base_size, remainder = divmod(len(normalized), chunk_count)
    chunks: list[list[str]] = []
    offset = 0
    for index in range(chunk_count):
        current_size = base_size + (1 if index < remainder else 0)
        chunks.append(normalized[offset : offset + current_size])
        offset += current_size
    return chunks


def _coalesce_tiny_profile_dispatch_chunks(
    chunks: list[list[str]],
    *,
    requested_url_count: int,
    candidate_count: int,
    min_non_tail_batch_size: int | None = None,
    tiny_batch_max_size: int | None = None,
) -> list[list[str]]:
    normalized_chunks = [
        [str(profile_url or "").strip() for profile_url in list(chunk or []) if str(profile_url or "").strip()]
        for chunk in list(chunks or [])
    ]
    normalized_chunks = [chunk for chunk in normalized_chunks if chunk]
    if len(normalized_chunks) <= 1:
        return normalized_chunks
    low_volume_company_max_urls = _harvest_profile_low_volume_company_max_urls()
    if (
        max(0, int(requested_url_count or 0)) <= low_volume_company_max_urls
        and max(0, int(candidate_count or 0)) <= low_volume_company_max_urls
    ):
        return normalized_chunks

    minimum_size = max(1, int(min_non_tail_batch_size or _harvest_profile_min_non_tail_batch_size()))
    tiny_size = max(1, int(tiny_batch_max_size or _harvest_profile_tiny_batch_max_size()))
    coalesced: list[list[str]] = []
    pending_tail: list[str] = []
    for index, chunk in enumerate(normalized_chunks):
        is_last = index == len(normalized_chunks) - 1
        if len(chunk) <= tiny_size:
            pending_tail.extend(chunk)
            if is_last:
                if coalesced and len(pending_tail) < minimum_size:
                    coalesced[-1].extend(pending_tail)
                else:
                    coalesced.append(list(pending_tail))
                pending_tail = []
            elif len(pending_tail) >= minimum_size:
                coalesced.append(list(pending_tail))
                pending_tail = []
            continue
        if pending_tail:
            if len(pending_tail) + len(chunk) <= max(minimum_size, len(chunk)):
                chunk = [*pending_tail, *chunk]
            else:
                coalesced.append(list(pending_tail))
            pending_tail = []
        coalesced.append(chunk)
    if pending_tail:
        if coalesced and len(pending_tail) < minimum_size:
            coalesced[-1].extend(pending_tail)
        else:
            coalesced.append(list(pending_tail))
    return coalesced


def _split_profile_prefetch_dispatch_specs(
    dispatch_specs: list[tuple[int, list[str]]],
    *,
    available_new_worker_count: int,
) -> tuple[list[tuple[int, list[str]]], list[str]]:
    if available_new_worker_count <= 0:
        return [], [
            str(profile_url or "").strip()
            for _, chunk in dispatch_specs
            for profile_url in list(chunk or [])
            if str(profile_url or "").strip()
        ]
    active_specs = dispatch_specs[:available_new_worker_count]
    deferred_urls = [
        str(profile_url or "").strip()
        for _, chunk in dispatch_specs[available_new_worker_count:]
        for profile_url in list(chunk or [])
        if str(profile_url or "").strip()
    ]
    return active_specs, deferred_urls


@dataclass(slots=True)
class ProfilePrefetchQueueItem:
    profile_url: str
    source_shards: list[str] = field(default_factory=list)
    source_jobs: list[str] = field(default_factory=list)
    priority: bool = False
    queue_state: str = "ready"
    registry_key: str = ""
    registry_status: str = ""
    refill_plan_batch_size: int = 0
    refill_plan_batch_count: int = 0
    refill_plan_window_url_count: int = 0
    item_store: str = "linkedin_profile_registry"

    def to_record(self) -> dict[str, Any]:
        return {
            "kind": "linkedin_profile_prefetch_item",
            "item_store": self.item_store,
            "profile_url": self.profile_url,
            "registry_key": self.registry_key,
            "queue_state": self.queue_state,
            "registry_status": self.registry_status,
            "refill_plan_batch_size": max(0, int(self.refill_plan_batch_size or 0)),
            "refill_plan_batch_count": max(0, int(self.refill_plan_batch_count or 0)),
            "refill_plan_window_url_count": max(0, int(self.refill_plan_window_url_count or 0)),
            "priority": bool(self.priority),
            "source_shards": list(self.source_shards),
            "source_jobs": list(self.source_jobs),
        }


def _build_profile_prefetch_queue_items(
    profile_urls: list[str],
    *,
    source_shards_by_url: dict[str, list[str]] | dict[str, set[str]] | None = None,
    source_jobs: list[str] | None = None,
    priority: bool = False,
    queue_state: str = "ready",
    registry_entries: dict[str, dict[str, Any]] | None = None,
) -> list[ProfilePrefetchQueueItem]:
    normalized_source_jobs = [str(item or "").strip() for item in list(source_jobs or []) if str(item or "").strip()]
    items: list[ProfilePrefetchQueueItem] = []
    seen_urls: set[str] = set()
    for profile_url in list(profile_urls or []):
        normalized_profile_url = str(profile_url or "").strip()
        if not normalized_profile_url or normalized_profile_url in seen_urls:
            continue
        seen_urls.add(normalized_profile_url)
        registry_key = normalize_linkedin_profile_url_key(normalized_profile_url)
        registry_entry = dict(dict(registry_entries or {}).get(registry_key) or {})
        registry_refill_state = str(registry_entry.get("refill_queue_state") or "").strip()
        source_shards = sorted(
            {
                str(label or "").strip()
                for label in list(dict(source_shards_by_url or {}).get(normalized_profile_url) or [])
                if str(label or "").strip()
            }
        )
        items.append(
            ProfilePrefetchQueueItem(
                profile_url=normalized_profile_url,
                source_shards=source_shards,
                source_jobs=normalized_source_jobs,
                priority=bool(priority),
                queue_state=registry_refill_state or (str(queue_state or "ready").strip() or "ready"),
                registry_key=registry_key,
                registry_status=str(registry_entry.get("status") or "").strip().lower(),
                refill_plan_batch_size=max(0, int(registry_entry.get("refill_plan_batch_size") or 0)),
                refill_plan_batch_count=max(0, int(registry_entry.get("refill_plan_batch_count") or 0)),
                refill_plan_window_url_count=max(
                    0,
                    int(registry_entry.get("refill_plan_window_url_count") or 0),
                ),
            )
        )
    return items


def _apply_durable_refill_wave_dispatch_window(
    dispatch_window: dict[str, Any],
    *,
    dispatch_urls: list[str],
    registry_entries: dict[str, dict[str, Any]],
    append_trigger_replan: bool,
    retry_isolated_refill: bool,
) -> dict[str, Any]:
    """Preserve the original batch window while draining the same deferred wave."""

    resolved = dict(dispatch_window or {})
    if append_trigger_replan or retry_isolated_refill or not dispatch_urls:
        return resolved
    inherited_batch_sizes: list[int] = []
    inherited_batch_counts: list[int] = []
    inherited_window_counts: list[int] = []
    for profile_url in list(dispatch_urls or []):
        registry_key = normalize_linkedin_profile_url_key(profile_url)
        entry = dict(registry_entries.get(registry_key) or {})
        queue_state = str(entry.get("refill_queue_state") or "").strip()
        if queue_state not in {"deferred_budget", "deferred_coalescing", "dispatch_reserved", "dispatch_claimed"}:
            continue
        batch_size = max(0, int(entry.get("refill_plan_batch_size") or 0))
        if batch_size > 0:
            inherited_batch_sizes.append(batch_size)
        batch_count = max(0, int(entry.get("refill_plan_batch_count") or 0))
        if batch_count > 0:
            inherited_batch_counts.append(batch_count)
        window_count = max(0, int(entry.get("refill_plan_window_url_count") or 0))
        if window_count > 0:
            inherited_window_counts.append(window_count)
    inherited_batch_size = max(inherited_batch_sizes or [0])
    current_batch_size = max(1, int(resolved.get("batch_size") or 1))
    # R6 durable-wave inheritance: a refill recompute of an already-formed deferred
    # wave must inherit that wave's identity (size + reason + contract) so the wave is
    # not re-emitted as a fresh dispatch (invariant 1). We claim the window whenever a
    # durable wave exists and is at least as large as the freshly recomputed window —
    # equality included. If the recomputed window is strictly larger we keep it (the
    # set has grown beyond the recorded wave) and let the canonical sizer own the shape.
    if inherited_batch_size <= 0 or inherited_batch_size < current_batch_size:
        return resolved
    batch_count = max(1, (len(dispatch_urls) + inherited_batch_size - 1) // inherited_batch_size)
    resolved["batch_size"] = inherited_batch_size
    resolved["batch_count"] = batch_count
    resolved["max_workers"] = min(
        max(1, int(resolved.get("max_workers") or 1)),
        max(batch_count, 1),
    )
    resolved["batch_size_contract"] = "profile_actor_slot_durable_wave_item_packing"
    resolved["batch_size_reason"] = "durable_refill_wave_batch_size"
    resolved["durable_refill_wave_batch_size"] = inherited_batch_size
    resolved["durable_refill_wave_batch_count"] = max(inherited_batch_counts or [0])
    resolved["durable_refill_wave_window_url_count"] = max(inherited_window_counts or [0])
    resolved["durable_refill_wave_item_count"] = len(dispatch_urls)
    return resolved


@dataclass(slots=True)
class ProfilePrefetchBatchPlan:
    requested_url_count: int
    candidate_count: int
    queue_items: list[ProfilePrefetchQueueItem]
    dispatch_urls: list[str]
    dispatch_item_specs: list[tuple[int, list[ProfilePrefetchQueueItem]]]
    dispatch_window: dict[str, Any]
    worker_budget: dict[str, int]
    dispatch_specs: list[tuple[int, list[str]]]
    deferred_items: list[ProfilePrefetchQueueItem]
    deferred_urls: list[str]
    original_dispatch_chunk_count: int
    coalesced_dispatch_chunk_count: int
    tiny_batch_coalesced_count: int
    tail_coalescing_items: list[ProfilePrefetchQueueItem] = field(default_factory=list)
    plan_reason: str = "ready_to_dispatch"

    @property
    def planned_dispatch_worker_count(self) -> int:
        return len(self.dispatch_specs)

    @property
    def queue_item_count(self) -> int:
        return len(self.queue_items)

    @property
    def planned_dispatch_item_count(self) -> int:
        return sum(len(chunk) for _, chunk in self.dispatch_item_specs)

    @property
    def planned_deferred_item_count(self) -> int:
        return len(self.deferred_items)

    def to_record(self) -> dict[str, Any]:
        available_slot_count = int(self.worker_budget.get("available_new_worker_count") or 0)
        planned_new_worker_count = self.planned_dispatch_worker_count
        unfilled_available_slot_count = max(0, available_slot_count - planned_new_worker_count)
        deferred_item_count = self.planned_deferred_item_count
        tail_coalescing_item_count = len(list(self.tail_coalescing_items or []))
        worker_budget_deferred_item_count = max(0, deferred_item_count - tail_coalescing_item_count)
        retry_wait_item_count = sum(
            1 for item in list(self.queue_items or []) if str(item.queue_state or "").strip() == "retry_wait"
        )
        normal_item_count = max(0, self.queue_item_count - retry_wait_item_count)
        retry_isolation = retry_wait_item_count > 0 and normal_item_count == 0
        if self.queue_item_count <= 0:
            refill_saturation = "no_ready_items"
        elif available_slot_count <= 0:
            refill_saturation = "no_available_slots"
        elif worker_budget_deferred_item_count > 0 and unfilled_available_slot_count <= 0:
            refill_saturation = "worker_budget_saturated"
        elif worker_budget_deferred_item_count > 0:
            refill_saturation = "underfilled_with_deferred_items"
        elif tail_coalescing_item_count > 0:
            refill_saturation = "tail_coalescing_wait"
        elif unfilled_available_slot_count > 0:
            refill_saturation = "ready_items_exhausted"
        else:
            refill_saturation = "filled_available_slots"
        return {
            "kind": "profile_prefetch_batch_plan",
            "schema_version": 1,
            "item_store": "linkedin_profile_registry",
            "refill_policy": ("retry_wait_isolated_refill" if retry_isolation else "continuous_ready_item_refill"),
            "plan_reason": self.plan_reason,
            "retry_isolation": retry_isolation,
            "requested_url_count": self.requested_url_count,
            "candidate_count": self.candidate_count,
            "queue_item_count": self.queue_item_count,
            "normal_queue_item_count": normal_item_count,
            "retry_wait_item_count": retry_wait_item_count,
            "planned_dispatch_worker_count": self.planned_dispatch_worker_count,
            "planned_dispatch_item_count": self.planned_dispatch_item_count,
            "planned_deferred_item_count": deferred_item_count,
            "planned_tail_coalescing_item_count": tail_coalescing_item_count,
            "planned_worker_budget_deferred_item_count": worker_budget_deferred_item_count,
            "available_slot_count": available_slot_count,
            "planned_new_worker_count": planned_new_worker_count,
            "unfilled_available_slot_count": unfilled_available_slot_count,
            "underfilled_with_deferred_items": bool(
                worker_budget_deferred_item_count > 0 and unfilled_available_slot_count > 0
            ),
            "refill_saturation": refill_saturation,
            "original_dispatch_chunk_count": self.original_dispatch_chunk_count,
            "coalesced_dispatch_chunk_count": self.coalesced_dispatch_chunk_count,
            "tiny_batch_coalesced_count": self.tiny_batch_coalesced_count,
            "recommended_batch_size": int(self.dispatch_window.get("batch_size") or 0),
            "recommended_batch_count": int(self.dispatch_window.get("batch_count") or 0),
            "recommended_max_workers": int(self.dispatch_window.get("max_workers") or 0),
            "dispatch_strategy": str(self.dispatch_window.get("strategy") or "").strip(),
            "base_dispatch_strategy": str(self.dispatch_window.get("base_strategy") or "").strip(),
            "batch_size_contract": str(self.dispatch_window.get("batch_size_contract") or "").strip(),
            "batch_size_reason": str(self.dispatch_window.get("batch_size_reason") or "").strip(),
            "actor_slot_url_target": int(self.dispatch_window.get("actor_slot_url_target") or 0),
            "durable_unit_max_urls": int(self.dispatch_window.get("durable_unit_max_urls") or 0),
            "provider_envelope_max_urls": int(self.dispatch_window.get("provider_envelope_max_urls") or 0),
            "large_ready_set_max_batch_count": int(self.dispatch_window.get("large_ready_set_max_batch_count") or 0),
            "large_ready_set_threshold_urls": int(self.dispatch_window.get("large_ready_set_threshold_urls") or 0),
            "active_worker_count": int(self.worker_budget.get("active_worker_count") or 0),
            "scheduler_reserved_worker_count": int(self.worker_budget.get("scheduler_reserved_worker_count") or 0),
            "effective_active_worker_count": int(self.worker_budget.get("effective_active_worker_count") or 0),
            "submit_budget": int(self.worker_budget.get("submit_budget") or 0),
            "actor_budget": int(self.worker_budget.get("actor_budget") or 0),
            "available_new_worker_count": int(self.worker_budget.get("available_new_worker_count") or 0),
        }


def _build_profile_prefetch_batch_plan(
    *,
    dispatch_urls: list[str],
    requested_url_count: int,
    candidate_count: int,
    priority: bool,
    source_shards_by_url: dict[str, list[str]] | dict[str, set[str]] | None,
    worker_budget: dict[str, int],
    dispatch_window: dict[str, Any] | None = None,
    queue_items: list[ProfilePrefetchQueueItem] | None = None,
    allow_under_target_final_tail_dispatch: bool = False,
) -> ProfilePrefetchBatchPlan:
    normalized_items = list(queue_items or [])
    if not normalized_items:
        normalized_items = _build_profile_prefetch_queue_items(
            dispatch_urls,
            source_shards_by_url=source_shards_by_url,
            priority=priority,
            queue_state="ready",
        )
    normalized_urls = [item.profile_url for item in normalized_items if str(item.profile_url or "").strip()]
    normal_items = [item for item in normalized_items if str(item.queue_state or "").strip() != "retry_wait"]
    normal_count = len(normal_items)
    retry_isolated_items = bool(normalized_items) and normal_count == 0
    normalized_source_shards = {
        item.profile_url: list(item.source_shards) for item in normalized_items if str(item.profile_url or "").strip()
    }
    items_by_url = {item.profile_url: item for item in normalized_items}
    resolved_dispatch_window = dict(
        dispatch_window
        or _recommended_harvest_profile_prefetch_dispatch_window(
            len(normalized_urls),
            priority=priority,
            source_shards_by_url=normalized_source_shards,
        )
    )
    # Actor-slot packing is the canonical durable-scheduler envelope shape (R1) and
    # must apply on every provider mode (live/scripted/simulate). The refill and
    # canonical hot paths run under "simulate", where the legacy window sizer emits
    # tiny batch sizes; without this repack those tiny windows fragment the wave into
    # sub-actor-slot chunks that the tail guard then defers. Gating this on
    # live/scripted was the root of the regression cluster.
    #
    # Two-tier reconciliation:
    #   (a) Sub-actor-slot window (`batch_size < ACTOR_SLOT_URL_TARGET`): the incoming
    #       window is a tiny/legacy/mocked artifact carrying no deliberate sizing intent.
    #       Floor it to exactly the actor-slot target (R1.a) and let R5 wave-bounding
    #       split the surplus into a clean deferred wave. (D1: a 105-url refill set with
    #       one available worker packs to 50/55 actor slots, not a single 105 envelope
    #       the wave guard cannot split.)
    #   (b) Window exactly at the actor-slot target (== `ACTOR_SLOT_URL_TARGET`): this is
    #       the bare actor-slot planning window with no larger sizing intent yet. Only on
    #       the terminal/quiescent tail path (`allow_under_target_final_tail_dispatch`) do we
    #       grow it to the canonical single-durable / balanced provider-envelope size ("fewer
    #       larger envelopes", R1.b–d) so a terminal roster wave collapses into one durable
    #       unit. On a normal append/replan wave the 50-window is left as the actor-slot
    #       packing granularity so R5 wave-bounding and the ordinal submit gate split a head
    #       chunk from its tail (a backpressured head must not be overtaken by its tail).
    #   (c) Window strictly above the actor-slot target: a deliberate larger sizing
    #       (single-durable, balanced large-envelope, or an inherited durable wave) — it is
    #       preserved verbatim and R6 may still claim it.
    if normalized_urls and not retry_isolated_items:
        actor_slot_target = _harvest_profile_prefetch_actor_slot_url_target()
        current_batch_size = max(1, int(resolved_dispatch_window.get("batch_size") or 1))
        if current_batch_size < actor_slot_target:
            actor_batch_size = actor_slot_target
            actor_batch_size_reason = "actor_slot_item_packing"
        elif current_batch_size == actor_slot_target and allow_under_target_final_tail_dispatch:
            actor_batch_size, actor_batch_size_reason = _recommended_harvest_profile_prefetch_actor_slot_batch_size(
                len(normalized_urls)
            )
        else:
            actor_batch_size = current_batch_size
            actor_batch_size_reason = str(resolved_dispatch_window.get("batch_size_reason") or "")
        if current_batch_size < actor_batch_size:
            batch_count = max(1, (len(normalized_urls) + actor_batch_size - 1) // actor_batch_size)
            resolved_dispatch_window["batch_size"] = actor_batch_size
            resolved_dispatch_window["batch_count"] = batch_count
            resolved_dispatch_window["max_workers"] = min(
                max(1, int(resolved_dispatch_window.get("max_workers") or 1)),
                max(batch_count, 1),
            )
            resolved_dispatch_window["batch_size_contract"] = "profile_actor_slot_ready_item_packing"
            resolved_dispatch_window["batch_size_reason"] = actor_batch_size_reason
            resolved_dispatch_window["actor_slot_url_target"] = actor_slot_target
            resolved_dispatch_window["durable_unit_max_urls"] = _harvest_profile_prefetch_durable_unit_max_urls()
            resolved_dispatch_window["provider_envelope_max_urls"] = (
                _harvest_profile_prefetch_provider_envelope_max_urls()
            )
            resolved_dispatch_window["large_ready_set_max_batch_count"] = (
                _harvest_profile_prefetch_max_batch_count_for_large_ready_set()
            )
            resolved_dispatch_window["large_ready_set_threshold_urls"] = (
                _harvest_profile_prefetch_scale_threshold_urls()
            )
    available_new_worker_count = int(dict(worker_budget or {}).get("available_new_worker_count") or 0)
    if (
        normalized_urls
        and not retry_isolated_items
        and _should_split_roster_ready_set_across_actor_slots(
            total_urls=len(normalized_urls),
            source_mix=dict(resolved_dispatch_window.get("source_mix") or {}),
            available_new_worker_count=available_new_worker_count,
        )
    ):
        actor_slot_target = _harvest_profile_prefetch_actor_slot_url_target()
        batch_count = max(1, (len(normalized_urls) + actor_slot_target - 1) // actor_slot_target)
        resolved_dispatch_window["batch_size"] = actor_slot_target
        resolved_dispatch_window["batch_count"] = batch_count
        resolved_dispatch_window["max_workers"] = min(
            max(1, available_new_worker_count),
            max(batch_count, 1),
        )
        resolved_dispatch_window["batch_size_contract"] = "profile_actor_slot_ready_item_packing"
        resolved_dispatch_window["batch_size_reason"] = "roster_actor_slot_fill"
        resolved_dispatch_window["actor_slot_url_target"] = actor_slot_target
        resolved_dispatch_window["durable_unit_max_urls"] = _harvest_profile_prefetch_durable_unit_max_urls()
        resolved_dispatch_window["provider_envelope_max_urls"] = _harvest_profile_prefetch_provider_envelope_max_urls()
        resolved_dispatch_window["large_ready_set_max_batch_count"] = (
            _harvest_profile_prefetch_max_batch_count_for_large_ready_set()
        )
        resolved_dispatch_window["large_ready_set_threshold_urls"] = _harvest_profile_prefetch_scale_threshold_urls()
    low_volume_tail_allowed = (
        not normalized_items
        or all(
            str(item.queue_state or "").strip()
            in {
                "ready",
                "retry_wait",
                "deferred_budget",
                "deferred_coalescing",
                "dispatch_reserved",
                "dispatch_claimed",
            }
            for item in normalized_items
        )
    ) and (
        max(0, int(requested_url_count or 0)) <= _harvest_profile_low_volume_company_max_urls()
        and max(0, int(candidate_count or 0)) <= _harvest_profile_low_volume_company_max_urls()
    )
    if (
        normalized_urls
        and not retry_isolated_items
        and normal_count < _harvest_profile_prefetch_actor_slot_url_target()
        and not low_volume_tail_allowed
        and not allow_under_target_final_tail_dispatch
    ):
        return ProfilePrefetchBatchPlan(
            requested_url_count=max(0, int(requested_url_count or 0)),
            candidate_count=max(0, int(candidate_count or 0)),
            queue_items=normalized_items,
            dispatch_urls=normalized_urls,
            dispatch_item_specs=[],
            dispatch_window=resolved_dispatch_window,
            worker_budget={str(key): int(value or 0) for key, value in dict(worker_budget or {}).items()},
            dispatch_specs=[],
            deferred_items=normalized_items,
            deferred_urls=normalized_urls,
            original_dispatch_chunk_count=0,
            coalesced_dispatch_chunk_count=0,
            tiny_batch_coalesced_count=0,
            tail_coalescing_items=normalized_items,
            plan_reason="deferred_coalescing_sub_50_tail",
        )
    dispatch_chunks = _actor_slot_chunk_strings(
        normalized_urls,
        int(resolved_dispatch_window.get("batch_size") or 1),
    )
    original_chunk_count = len(dispatch_chunks)
    dispatch_chunks = _coalesce_tiny_profile_dispatch_chunks(
        dispatch_chunks,
        requested_url_count=max(0, int(requested_url_count or 0)),
        candidate_count=max(0, int(candidate_count or 0)),
    )
    item_chunks = [
        [items_by_url[profile_url] for profile_url in chunk if profile_url in items_by_url] for chunk in dispatch_chunks
    ]
    all_dispatch_item_specs = list(enumerate([chunk for chunk in item_chunks if chunk], start=1))
    tail_coalescing_items: list[ProfilePrefetchQueueItem] = []
    dispatchable_item_specs: list[tuple[int, list[ProfilePrefetchQueueItem]]] = []
    actor_slot_target = _harvest_profile_prefetch_actor_slot_url_target()
    allow_under_target_dispatch = bool(
        retry_isolated_items or low_volume_tail_allowed or allow_under_target_final_tail_dispatch
    )
    for chunk_index, chunk in all_dispatch_item_specs:
        if not allow_under_target_dispatch and len(chunk) < actor_slot_target:
            tail_coalescing_items.extend(chunk)
            continue
        dispatchable_item_specs.append((chunk_index, chunk))
    if available_new_worker_count <= 0:
        active_item_specs: list[tuple[int, list[ProfilePrefetchQueueItem]]] = []
        deferred_items = [item for _, chunk in dispatchable_item_specs for item in chunk]
    else:
        active_item_specs = dispatchable_item_specs[:available_new_worker_count]
        deferred_items = [item for _, chunk in dispatchable_item_specs[available_new_worker_count:] for item in chunk]
    for item in tail_coalescing_items:
        if item not in deferred_items:
            deferred_items.append(item)
    dispatch_specs = [(chunk_index, [item.profile_url for item in chunk]) for chunk_index, chunk in active_item_specs]
    deferred_urls = [item.profile_url for item in deferred_items]
    plan_reason = "ready_to_dispatch"
    if not normalized_urls:
        plan_reason = "no_profile_urls"
    elif original_chunk_count <= 0:
        plan_reason = "no_dispatch_chunks"
    elif not dispatch_specs:
        plan_reason = "worker_budget_exhausted"
        if tail_coalescing_items and len(tail_coalescing_items) == len(deferred_items):
            plan_reason = "deferred_coalescing_sub_50_tail"
    elif retry_isolated_items:
        plan_reason = "retry_wait_isolated_dispatch"
    elif allow_under_target_final_tail_dispatch and normal_count < _harvest_profile_prefetch_actor_slot_url_target():
        plan_reason = "queue_quiescent_final_tail"
    return ProfilePrefetchBatchPlan(
        requested_url_count=max(0, int(requested_url_count or 0)),
        candidate_count=max(0, int(candidate_count or 0)),
        queue_items=normalized_items,
        dispatch_urls=normalized_urls,
        dispatch_item_specs=active_item_specs,
        dispatch_window=resolved_dispatch_window,
        worker_budget={str(key): int(value or 0) for key, value in dict(worker_budget or {}).items()},
        dispatch_specs=dispatch_specs,
        deferred_items=deferred_items,
        deferred_urls=deferred_urls,
        original_dispatch_chunk_count=original_chunk_count,
        coalesced_dispatch_chunk_count=len(dispatch_chunks),
        tiny_batch_coalesced_count=max(0, original_chunk_count - len(dispatch_chunks)),
        tail_coalescing_items=tail_coalescing_items,
        plan_reason=plan_reason,
    )


def _record_profile_prefetch_batch_plan_items(
    store: Any,
    plan: ProfilePrefetchBatchPlan,
    *,
    source_shards_by_url: dict[str, list[str]] | dict[str, set[str]] | None = None,
    source_jobs: list[str] | None = None,
    snapshot_dir: Path | None = None,
    trigger_kind: str = "profile_prefetch_refill",
    record_active_items: bool = True,
    active_queue_state: str = "dispatch_reserved",
    active_reason: str = "scheduler_dispatch_reserved",
) -> dict[str, Any]:
    if store is None:
        return {"status": "skipped", "reason": "store_unavailable"}
    registry_repo = linkedin_profile_registry_repo(store)
    recorder = getattr(registry_repo, "record_refill_plan_items", None)
    if not callable(recorder):
        return {"status": "skipped", "reason": "refill_item_recorder_unavailable"}
    active_chunks = (
        [
            [item.profile_url for item in list(chunk or []) if str(item.profile_url or "").strip()]
            for _, chunk in list(plan.dispatch_item_specs or [])
        ]
        if bool(record_active_items)
        else []
    )
    active_chunks = [chunk for chunk in active_chunks if chunk]
    active_urls = [profile_url for chunk in active_chunks for profile_url in chunk]
    deferred_urls = [
        item.profile_url for item in list(plan.deferred_items or []) if str(item.profile_url or "").strip()
    ]
    if not active_urls and not deferred_urls:
        return {"status": "skipped", "reason": "no_refill_items"}
    tail_coalescing_urls = {
        item.profile_url for item in list(plan.tail_coalescing_items or []) if str(item.profile_url or "").strip()
    }
    budget_deferred_urls = [profile_url for profile_url in deferred_urls if profile_url not in tail_coalescing_urls]
    tail_deferred_urls = [profile_url for profile_url in deferred_urls if profile_url in tail_coalescing_urls]
    active_not_before_at = (
        datetime.now(timezone.utc).replace(microsecond=0)
        + timedelta(seconds=HARVEST_PROFILE_DISPATCH_CLAIM_TTL_SECONDS)
    ).strftime("%Y-%m-%d %H:%M:%S")
    deferred_retry_wait = bool(budget_deferred_urls) and all(
        str(item.queue_state or "").strip() == "retry_wait"
        for item in list(plan.deferred_items or [])
        if str(item.profile_url or "").strip() and item.profile_url not in tail_coalescing_urls
    )
    deferred_queue_state = "retry_wait" if deferred_retry_wait else "deferred_budget"
    deferred_reason = (
        ("profile_retry_wait" if deferred_retry_wait else "worker_budget_deferred") if budget_deferred_urls else ""
    )
    recorded_payloads: list[dict[str, Any]] = []
    normalized_active_queue_state = str(active_queue_state or "dispatch_reserved").strip() or "dispatch_reserved"
    normalized_active_reason = (
        str(active_reason or "scheduler_dispatch_reserved").strip() or "scheduler_dispatch_reserved"
    )
    plan_window_url_count = max(0, int(plan.queue_item_count or 0))
    plan_batch_size = max(0, int(plan.dispatch_window.get("batch_size") or 0))
    plan_batch_count = max(0, int(plan.dispatch_window.get("batch_count") or 0))
    for active_chunk in active_chunks:
        active_payload_hash = sha1(json.dumps(sorted(active_chunk), ensure_ascii=False).encode("utf-8")).hexdigest()[
            :16
        ]
        recorded_payloads.append(
            dict(
                recorder(
                    active_profile_urls=active_chunk,
                    source_shards_by_url=source_shards_by_url or {},
                    source_jobs=source_jobs or [],
                    snapshot_dir=str(snapshot_dir or ""),
                    trigger_kind=trigger_kind,
                    plan_reason=plan.plan_reason,
                    active_queue_state=normalized_active_queue_state,
                    active_reason=normalized_active_reason,
                    active_refill_not_before_at=active_not_before_at,
                    active_owner_payload_hash=active_payload_hash,
                    deferred_reason="",
                    deferred_queue_state=deferred_queue_state,
                    refill_not_before_at="",
                    refill_plan_batch_size=plan_batch_size,
                    refill_plan_batch_count=plan_batch_count,
                    refill_plan_window_url_count=plan_window_url_count,
                )
                or {}
            )
        )
    if budget_deferred_urls:
        recorded_payloads.append(
            dict(
                recorder(
                    deferred_profile_urls=budget_deferred_urls,
                    source_shards_by_url=source_shards_by_url or {},
                    source_jobs=source_jobs or [],
                    snapshot_dir=str(snapshot_dir or ""),
                    trigger_kind=trigger_kind,
                    plan_reason=plan.plan_reason,
                    deferred_reason=deferred_reason,
                    deferred_queue_state=deferred_queue_state,
                    refill_not_before_at="",
                    refill_plan_batch_size=plan_batch_size,
                    refill_plan_batch_count=plan_batch_count,
                    refill_plan_window_url_count=plan_window_url_count,
                )
                or {}
            )
        )
    if tail_deferred_urls:
        tail_not_before_at = (
            datetime.now(timezone.utc).replace(microsecond=0)
            + timedelta(milliseconds=max(0, int(_harvest_profile_tiny_tail_coalescing_min_age_ms() or 0)))
        ).strftime("%Y-%m-%d %H:%M:%S")
        recorded_payloads.append(
            dict(
                recorder(
                    deferred_profile_urls=tail_deferred_urls,
                    source_shards_by_url=source_shards_by_url or {},
                    source_jobs=source_jobs or [],
                    snapshot_dir=str(snapshot_dir or ""),
                    trigger_kind=trigger_kind,
                    plan_reason=(
                        "deferred_coalescing_sub_50_tail"
                        if str(plan.plan_reason or "").strip() == "deferred_coalescing_sub_50_tail"
                        else plan.plan_reason
                    ),
                    deferred_reason="sub_50_tail_waiting_for_more_discovery",
                    deferred_queue_state="deferred_coalescing",
                    refill_not_before_at=tail_not_before_at,
                    refill_plan_batch_size=plan_batch_size,
                    refill_plan_batch_count=plan_batch_count,
                    refill_plan_window_url_count=plan_window_url_count,
                )
                or {}
            )
        )
    if len(recorded_payloads) == 1:
        return recorded_payloads[0]
    active_item_count = sum(int(payload.get("active_item_count") or 0) for payload in recorded_payloads)
    deferred_item_count = sum(int(payload.get("deferred_item_count") or 0) for payload in recorded_payloads)
    return {
        "status": "recorded" if active_item_count > 0 or deferred_item_count > 0 else "skipped",
        "item_store": "linkedin_profile_registry",
        "active_item_count": active_item_count,
        "deferred_item_count": deferred_item_count,
        "trigger_kind": trigger_kind,
        "plan_reason": plan.plan_reason,
        "split_tail_coalescing": bool(tail_deferred_urls),
        "tail_coalescing_item_count": len(tail_deferred_urls),
        "budget_deferred_item_count": len(budget_deferred_urls),
        "records": recorded_payloads,
    }


def _mark_profile_prefetch_urls_dispatch_claimed(
    store: Any,
    profile_urls: list[str],
    *,
    source_shards_by_url: dict[str, list[str]] | dict[str, set[str]] | None = None,
    source_jobs: list[str] | None = None,
    snapshot_dir: Path | str | None = None,
    trigger_kind: str = "profile_prefetch_submit_claim",
    plan_reason: str = "provider_submit_claimed",
    owner_payload_hash: str = "",
) -> dict[str, Any]:
    if store is None:
        return {"status": "skipped", "reason": "store_unavailable"}
    registry_repo = linkedin_profile_registry_repo(store)
    recorder = getattr(registry_repo, "record_refill_plan_items", None)
    if not callable(recorder):
        return {"status": "skipped", "reason": "refill_item_recorder_unavailable"}
    normalized_urls = [
        str(profile_url or "").strip() for profile_url in list(profile_urls or []) if str(profile_url or "").strip()
    ]
    if not normalized_urls:
        return {"status": "skipped", "reason": "no_dispatch_claimed_urls"}
    dispatch_claim_not_before_at = (
        datetime.now(timezone.utc).replace(microsecond=0)
        + timedelta(seconds=HARVEST_PROFILE_DISPATCH_CLAIM_TTL_SECONDS)
    ).strftime("%Y-%m-%d %H:%M:%S")
    return dict(
        recorder(
            active_profile_urls=normalized_urls,
            source_shards_by_url=source_shards_by_url or {},
            source_jobs=source_jobs or [],
            snapshot_dir=str(snapshot_dir or ""),
            trigger_kind=trigger_kind,
            plan_reason=plan_reason,
            active_queue_state="dispatch_claimed",
            active_reason="provider_submit_claimed",
            active_refill_not_before_at=dispatch_claim_not_before_at,
            active_owner_payload_hash=str(owner_payload_hash or "").strip(),
        )
        or {}
    )


def _mark_profile_prefetch_urls_deferred_for_dispatch_retry(
    store: Any,
    profile_urls: list[str],
    *,
    source_shards_by_url: dict[str, list[str]] | dict[str, set[str]] | None = None,
    source_jobs: list[str] | None = None,
    snapshot_dir: Path | str | None = None,
    trigger_kind: str = "profile_prefetch_dispatch_retry",
    plan_reason: str = "provider_submit_not_started",
    deferred_reason: str = "provider_submit_not_started",
    refill_not_before_at: str = "",
) -> dict[str, Any]:
    if store is None:
        return {"status": "skipped", "reason": "store_unavailable"}
    registry_repo = linkedin_profile_registry_repo(store)
    recorder = getattr(registry_repo, "record_refill_plan_items", None)
    if not callable(recorder):
        return {"status": "skipped", "reason": "refill_item_recorder_unavailable"}
    normalized_urls = [
        str(profile_url or "").strip() for profile_url in list(profile_urls or []) if str(profile_url or "").strip()
    ]
    if not normalized_urls:
        return {"status": "skipped", "reason": "no_deferred_dispatch_retry_urls"}
    return dict(
        recorder(
            deferred_profile_urls=normalized_urls,
            source_shards_by_url=source_shards_by_url or {},
            source_jobs=source_jobs or [],
            snapshot_dir=str(snapshot_dir or ""),
            trigger_kind=trigger_kind,
            plan_reason=plan_reason,
            deferred_reason=deferred_reason,
            deferred_queue_state="deferred_budget",
            refill_not_before_at=str(refill_not_before_at or ""),
        )
        or {}
    )


def _mark_profile_prefetch_urls_dispatch_owned(
    store: Any,
    profile_urls: list[str],
    *,
    source_shards_by_url: dict[str, list[str]] | dict[str, set[str]] | None = None,
    source_jobs: list[str] | None = None,
    snapshot_dir: Path | str | None = None,
    trigger_kind: str = "profile_prefetch_refill",
    plan_reason: str = "remote_provider_submitted",
    owner_worker_id: int = 0,
    owner_run_id: str = "",
    owner_dataset_id: str = "",
    owner_payload_hash: str = "",
) -> dict[str, Any]:
    """Promote durable URL items only after a provider worker owns them."""

    if store is None:
        return {"status": "skipped", "reason": "store_unavailable"}
    registry_repo = linkedin_profile_registry_repo(store)
    recorder = getattr(registry_repo, "record_refill_plan_items", None)
    if not callable(recorder):
        return {"status": "skipped", "reason": "refill_item_recorder_unavailable"}
    normalized_urls = [
        str(profile_url or "").strip() for profile_url in list(profile_urls or []) if str(profile_url or "").strip()
    ]
    if not normalized_urls:
        return {"status": "skipped", "reason": "no_dispatch_owned_urls"}
    return dict(
        recorder(
            active_profile_urls=normalized_urls,
            source_shards_by_url=source_shards_by_url or {},
            source_jobs=source_jobs or [],
            snapshot_dir=str(snapshot_dir or ""),
            trigger_kind=trigger_kind,
            plan_reason=plan_reason,
            active_queue_state="planned_dispatch",
            active_owner_worker_id=max(0, int(owner_worker_id or 0)),
            active_owner_run_id=str(owner_run_id or "").strip(),
            active_owner_dataset_id=str(owner_dataset_id or "").strip(),
            active_owner_payload_hash=str(owner_payload_hash or "").strip(),
        )
        or {}
    )


def _load_harvest_profile_batch_resume_checkpoint_from_summary(
    summary_path: Path,
) -> dict[str, Any]:
    """Recover remote-run identity if a process died after submit but before checkpoint."""

    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(summary, dict):
        return {}
    status = str(summary.get("status") or "").strip().lower()
    if status not in {"queued", "pending", "running", "completed"}:
        return {}
    checkpoint: dict[str, Any] = {
        "summary_path": str(summary_path),
        "stage": "waiting_remote_harvest" if status in {"queued", "pending", "running"} else "completed",
    }
    for key in ("run_id", "dataset_id", "payload_hash", "provider_mode"):
        value = str(summary.get(key) or "").strip()
        if value:
            checkpoint[key] = value
    artifact_paths = {
        str(key): str(value)
        for key, value in dict(summary.get("artifact_paths") or {}).items()
        if str(key).strip() and str(value).strip()
    }
    if artifact_paths:
        checkpoint["artifact_paths"] = artifact_paths
    pending_path = str(artifact_paths.get("scripted_harvest_pending") or "").strip()
    if pending_path:
        try:
            pending_payload = json.loads(Path(pending_path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pending_payload = {}
        if isinstance(pending_payload, dict):
            for key in (
                "scripted_remote_wait_after_submit",
                "scripted_remote_wait_seconds",
                "scripted_remote_ready_epoch_ms",
            ):
                value = pending_payload.get(key)
                if value not in (None, ""):
                    checkpoint[key] = value
    return checkpoint


def _load_completed_harvest_profile_batch_replay(summary_path: Path) -> dict[str, Any]:
    """Load terminal batch evidence that can be replayed without touching the provider."""

    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(summary, dict):
        return {}
    if str(summary.get("status") or "").strip().lower() != "completed":
        return {}
    artifact_paths = {
        str(key): str(value)
        for key, value in dict(summary.get("artifact_paths") or {}).items()
        if str(key).strip() and str(value).strip()
    }
    dataset_items_path = str(artifact_paths.get("dataset_items") or "").strip()
    if not dataset_items_path:
        derived_dataset_path = summary_path.with_name(
            summary_path.name.replace(".queue_summary.json", ".queue_dataset_items.json")
        )
        if derived_dataset_path.exists():
            dataset_items_path = str(derived_dataset_path)
            artifact_paths["dataset_items"] = dataset_items_path
    if not dataset_items_path:
        return {}
    try:
        body = json.loads(Path(dataset_items_path).expanduser().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return {
        "summary": summary,
        "body": body,
        "artifact_paths": artifact_paths,
        "dataset_items_path": dataset_items_path,
        "run_id": str(summary.get("run_id") or "").strip(),
        "dataset_id": str(summary.get("dataset_id") or "").strip(),
        "payload_hash": str(summary.get("payload_hash") or "").strip(),
    }


def _profile_batch_envelope_small_batch_reason(
    *,
    batch_size: int,
    requested_url_count: int,
    candidate_count: int,
    deferred_url_count: int,
    failed_url_count: int = 0,
    queue_quiescent: bool = False,
) -> str:
    actor_slot_target = _harvest_profile_prefetch_actor_slot_url_target()
    low_volume_company_max_urls = _harvest_profile_low_volume_company_max_urls()
    if batch_size >= actor_slot_target:
        return ""
    if failed_url_count > 0 and batch_size <= failed_url_count:
        return "retry_isolation"
    if (
        requested_url_count <= low_volume_company_max_urls
        and candidate_count <= low_volume_company_max_urls
        and deferred_url_count <= 0
    ):
        return "low_volume_company"
    if deferred_url_count <= 0 and bool(queue_quiescent):
        return "queue_quiescent_final_tail"
    if deferred_url_count <= 0:
        return "final_tail_unproven"
    return "normal_batch_under_target_with_deferred_backlog"


def _profile_batch_envelope_allowed_tiny_reason(reason: str) -> bool:
    return str(reason or "").strip() in {
        "cache_filtered_actor_slot",
        "final_tail",
        "queue_quiescent_final_tail",
        "retry_isolation",
        "urgent_user_visible",
        "low_volume_company",
        "prior_batch_backpressure_ordinal_gate",
    }


def _profile_prefetch_queue_timestamp(value: Any) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    for candidate in (normalized, normalized.replace("Z", "+00:00")):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
    try:
        return datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _profile_prefetch_queue_oldest_pending_age_ms(
    registry_entries: dict[str, dict[str, Any]],
    *,
    now: datetime | None = None,
) -> int | None:
    observed_at = now or datetime.now(timezone.utc)
    oldest: datetime | None = None
    for entry in dict(registry_entries or {}).values():
        payload = dict(entry or {})
        status = str(payload.get("status") or "").strip().lower()
        if status not in {"queued", "failed_retryable", "deferred_coalescing"}:
            continue
        queued_at = (
            payload.get("first_queued_at")
            or payload.get("last_queued_at")
            or payload.get("last_failed_at")
            or payload.get("updated_at")
            or payload.get("created_at")
        )
        parsed = _profile_prefetch_queue_timestamp(queued_at)
        if parsed is None:
            continue
        if oldest is None or parsed < oldest:
            oldest = parsed
    if oldest is None:
        return None
    return max(0, int((observed_at - oldest).total_seconds() * 1000))


def _profile_prefetch_oldest_deferred_coalescing_age_ms(
    profile_urls: list[str],
    registry_entries: dict[str, dict[str, Any]],
    *,
    now: datetime | None = None,
) -> int | None:
    observed_at = now or datetime.now(timezone.utc)
    oldest: datetime | None = None
    for profile_url in list(profile_urls or []):
        registry_key = normalize_linkedin_profile_url_key(profile_url)
        entry = dict(dict(registry_entries or {}).get(registry_key) or {})
        if str(entry.get("refill_queue_state") or "").strip() != "deferred_coalescing":
            continue
        deferred_at = entry.get("last_refill_planned_at") or entry.get("updated_at") or entry.get("created_at")
        parsed = _profile_prefetch_queue_timestamp(deferred_at)
        if parsed is None:
            continue
        if oldest is None or parsed < oldest:
            oldest = parsed
    if oldest is None:
        return None
    return max(0, int((observed_at - oldest).total_seconds() * 1000))


def _profile_refill_not_before_ready(value: Any, *, now: datetime | None = None) -> bool:
    parsed = _profile_prefetch_queue_timestamp(value)
    if parsed is None:
        return True
    observed_at = now or datetime.now(timezone.utc)
    return parsed <= observed_at


def _profile_prefetch_queue_unique_count(values: list[str] | set[str] | tuple[str, ...]) -> int:
    seen: set[str] = set()
    for value in list(values or []):
        normalized = str(value or "").strip()
        if normalized:
            seen.add(normalized)
    return len(seen)


def _profile_prefetch_requested_registry_terminal_summary(
    requested_profile_urls: list[str],
    registry_entries: dict[str, dict[str, Any]] | None,
) -> dict[str, Any]:
    requested_keys: list[str] = []
    for profile_url in list(requested_profile_urls or []):
        registry_key = normalize_linkedin_profile_url_key(profile_url)
        if registry_key and registry_key not in requested_keys:
            requested_keys.append(registry_key)
    registry_payload = dict(registry_entries or {})
    fetched_count = 0
    fetched_missing_raw_path_count = 0
    unrecoverable_count = 0
    missing_count = 0
    open_count = 0
    open_state_counts: dict[str, int] = {}
    terminal_queue_state_leak_count = 0
    for registry_key in requested_keys:
        entry = dict(registry_payload.get(registry_key) or {})
        if not entry:
            missing_count += 1
            continue
        status_value = str(entry.get("status") or "").strip().lower()
        refill_state = str(entry.get("refill_queue_state") or "").strip().lower()
        if refill_state and status_value in {"fetched", "unrecoverable"}:
            terminal_queue_state_leak_count += 1
        if status_value == "fetched":
            if str(entry.get("last_raw_path") or "").strip():
                fetched_count += 1
            else:
                fetched_missing_raw_path_count += 1
                open_count += 1
                open_state_counts["fetched_missing_raw_path"] = (
                    int(open_state_counts.get("fetched_missing_raw_path") or 0) + 1
                )
            continue
        if status_value == "unrecoverable":
            unrecoverable_count += 1
            continue
        open_count += 1
        state_key = refill_state or status_value or "unknown"
        open_state_counts[state_key] = int(open_state_counts.get(state_key) or 0) + 1
    requested_count = len(requested_keys)
    terminal_count = fetched_count + unrecoverable_count
    all_requested_terminal = (
        requested_count > 0
        and terminal_count == requested_count
        and missing_count == 0
        and open_count == 0
        and terminal_queue_state_leak_count == 0
    )
    return {
        "requested_url_count": requested_count,
        "terminal_url_count": terminal_count,
        "fetched_url_count": fetched_count,
        "fetched_missing_raw_path_count": fetched_missing_raw_path_count,
        "unrecoverable_url_count": unrecoverable_count,
        "missing_url_count": missing_count,
        "open_url_count": open_count,
        "open_state_counts": open_state_counts,
        "terminal_queue_state_leak_count": terminal_queue_state_leak_count,
        "all_requested_terminal": all_requested_terminal,
    }


def _build_profile_prefetch_queue_snapshot(
    *,
    requested_profile_urls: list[str],
    cached_profile_urls: list[str] | set[str] | tuple[str, ...] | None = None,
    ready_profile_urls: list[str] | set[str] | tuple[str, ...] | None = None,
    newly_queued_profile_urls: list[str] | set[str] | tuple[str, ...] | None = None,
    already_queued_profile_urls: list[str] | set[str] | tuple[str, ...] | None = None,
    deferred_profile_urls: list[str] | set[str] | tuple[str, ...] | None = None,
    failed_profile_urls: list[str] | set[str] | tuple[str, ...] | None = None,
    registry_entries: dict[str, dict[str, Any]] | None = None,
    active_worker_count: int = 0,
    queued_worker_count: int = 0,
    actor_budget: int = 0,
    submit_budget: int = 0,
    available_new_worker_count: int = 0,
    scheduler_reserved_worker_count: int = 0,
    effective_active_worker_count: int = 0,
    batch_envelopes: list[dict[str, Any]] | None = None,
    status: str = "",
    reason: str = "",
) -> dict[str, Any]:
    requested_count = _profile_prefetch_queue_unique_count(requested_profile_urls)
    cached_count = _profile_prefetch_queue_unique_count(cached_profile_urls or [])
    ready_count = _profile_prefetch_queue_unique_count(ready_profile_urls or [])
    newly_queued_count = _profile_prefetch_queue_unique_count(newly_queued_profile_urls or [])
    already_queued_count = _profile_prefetch_queue_unique_count(already_queued_profile_urls or [])
    deferred_count = _profile_prefetch_queue_unique_count(deferred_profile_urls or [])
    failed_count = _profile_prefetch_queue_unique_count(failed_profile_urls or [])
    queued_count = newly_queued_count + already_queued_count
    registry_status_counts: dict[str, int] = {}
    refill_queue_state_counts: dict[str, int] = {}
    planned_dispatch_owner_missing_count = 0
    planned_dispatch_remote_owner_count = 0
    terminal_queue_state_leak_count = 0
    for entry in dict(registry_entries or {}).values():
        entry_payload = dict(entry or {})
        status_value = str(entry_payload.get("status") or "missing").strip().lower() or "missing"
        registry_status_counts[status_value] = registry_status_counts.get(status_value, 0) + 1
        refill_state = str(entry_payload.get("refill_queue_state") or "").strip().lower()
        if refill_state:
            refill_queue_state_counts[refill_state] = refill_queue_state_counts.get(refill_state, 0) + 1
        if refill_state == "planned_dispatch":
            has_remote_owner = bool(
                int(entry_payload.get("refill_owner_worker_id") or 0) > 0
                or str(entry_payload.get("refill_owner_run_id") or "").strip()
                or str(entry_payload.get("refill_owner_dataset_id") or "").strip()
                or str(entry_payload.get("last_run_id") or "").strip()
                or str(entry_payload.get("last_dataset_id") or "").strip()
            )
            if has_remote_owner:
                planned_dispatch_remote_owner_count += 1
            else:
                planned_dispatch_owner_missing_count += 1
        if refill_state and status_value in {"fetched", "unrecoverable"}:
            terminal_queue_state_leak_count += 1
    terminal_summary = _profile_prefetch_requested_registry_terminal_summary(
        requested_profile_urls,
        registry_entries or {},
    )
    envelope_payloads = [dict(item) for item in list(batch_envelopes or []) if isinstance(item, dict)]
    tiny_flush_reasons = sorted(
        {
            str(item.get("small_batch_reason") or "").strip()
            for item in envelope_payloads
            if bool(item.get("is_tiny_batch")) and str(item.get("small_batch_reason") or "").strip()
        }
    )
    active_count = max(0, int(active_worker_count or 0))
    queued_worker_count_value = max(0, int(queued_worker_count or 0))
    actor_budget_value = max(0, int(actor_budget or 0))
    submit_budget_value = max(0, int(submit_budget or 0))
    available_worker_count = max(0, int(available_new_worker_count or 0))
    scheduler_reserved_count = max(0, int(scheduler_reserved_worker_count or 0))
    effective_active_count = max(active_count, scheduler_reserved_count, int(effective_active_worker_count or 0))
    slot_occupancy_count = effective_active_count + queued_worker_count_value
    local_queue_quiescent = deferred_count <= 0 and failed_count <= 0
    remote_queue_quiescent = queued_count <= 0 and slot_occupancy_count <= 0
    return {
        "kind": "linkedin_profile_prefetch_queue",
        "schema_version": 1,
        "item_store": "linkedin_profile_registry",
        "status": str(status or "").strip(),
        "reason": str(reason or "").strip(),
        "requested_url_count": requested_count,
        "cached_url_count": cached_count,
        "ready_url_count": ready_count,
        "newly_queued_url_count": newly_queued_count,
        "already_queued_url_count": already_queued_count,
        "queued_url_count": queued_count,
        "deferred_url_count": deferred_count,
        "failed_url_count": failed_count,
        "pending_url_count": queued_count + deferred_count + failed_count,
        "ready_after_dispatch_url_count": deferred_count + failed_count,
        "active_worker_count": active_count,
        "queued_worker_count": queued_worker_count_value,
        "scheduler_reserved_worker_count": scheduler_reserved_count,
        "effective_active_worker_count": effective_active_count,
        "actor_budget": actor_budget_value,
        "submit_budget": submit_budget_value,
        "available_new_worker_count": available_worker_count,
        "slot_occupancy_basis": "max(active_workers_plus_newly_queued_workers,scheduler_reserved_workers)",
        "slot_occupancy_count": slot_occupancy_count,
        "local_queue_quiescent": local_queue_quiescent,
        "remote_queue_quiescent": remote_queue_quiescent,
        "queue_quiescent": local_queue_quiescent and remote_queue_quiescent,
        "oldest_pending_item_age_ms": _profile_prefetch_queue_oldest_pending_age_ms(registry_entries or {}),
        "registry_status_counts": registry_status_counts,
        "refill_queue_state_counts": refill_queue_state_counts,
        "planned_dispatch_remote_owner_count": planned_dispatch_remote_owner_count,
        "planned_dispatch_owner_missing_count": planned_dispatch_owner_missing_count,
        "terminal_queue_state_leak_count": terminal_queue_state_leak_count,
        "registry_terminal_url_count": int(terminal_summary.get("terminal_url_count") or 0),
        "registry_fetched_url_count": int(terminal_summary.get("fetched_url_count") or 0),
        "registry_unrecoverable_url_count": int(terminal_summary.get("unrecoverable_url_count") or 0),
        "registry_missing_url_count": int(terminal_summary.get("missing_url_count") or 0),
        "registry_open_url_count": int(terminal_summary.get("open_url_count") or 0),
        "registry_all_requested_terminal": bool(terminal_summary.get("all_requested_terminal")),
        "batch_envelope_count": len(envelope_payloads),
        "tiny_batch_count": sum(1 for item in envelope_payloads if bool(item.get("is_tiny_batch"))),
        "tiny_tail_flush_reasons": tiny_flush_reasons,
    }


def _build_profile_prefetch_batch_envelope(
    *,
    chunk_index: int,
    profile_url_chunk: list[str],
    requested_url_count: int,
    candidate_count: int,
    active_worker_count: int,
    actor_budget: int,
    submit_budget: int,
    recommended_batch_size: int,
    recommended_batch_count: int,
    recommended_max_workers: int,
    dispatch_strategy: str,
    deferred_url_count: int,
    queued_worker_count: int = 0,
    dispatched_url_count: int | None = None,
    failed_url_count: int = 0,
    tail_coalescing_url_count: int = 0,
    status: str = "planned",
    flush_reason: str = "adaptive_prefetch_window",
    summary_path: str = "",
    small_batch_reason_override: str = "",
    queue_quiescent: bool = False,
) -> dict[str, Any]:
    normalized_profile_urls = [
        str(url or "").strip() for url in list(profile_url_chunk or []) if str(url or "").strip()
    ]
    profile_url_keys = [
        normalize_linkedin_profile_url_key(profile_url)
        for profile_url in normalized_profile_urls
        if normalize_linkedin_profile_url_key(profile_url)
    ]
    batch_size = len(normalized_profile_urls)
    resolved_dispatched_count = batch_size if dispatched_url_count is None else max(0, int(dispatched_url_count or 0))
    failed_count = max(0, int(failed_url_count or 0))
    effective_dispatch_size = resolved_dispatched_count
    if dispatched_url_count is not None and resolved_dispatched_count <= 0:
        effective_dispatch_size = failed_count
    small_batch_reason = str(small_batch_reason_override or "").strip()
    if not small_batch_reason and effective_dispatch_size > 0:
        small_batch_reason = _profile_batch_envelope_small_batch_reason(
            batch_size=effective_dispatch_size,
            requested_url_count=max(0, int(requested_url_count or 0)),
            candidate_count=max(0, int(candidate_count or 0)),
            deferred_url_count=max(0, int(deferred_url_count or 0)),
            failed_url_count=failed_count,
            queue_quiescent=bool(queue_quiescent),
        )
    active_before = max(0, int(active_worker_count or 0))
    queued_workers = max(0, int(queued_worker_count or 0))
    actor_budget_value = max(0, int(actor_budget or 0))
    submit_budget_value = max(0, int(submit_budget or 0))
    status_value = str(status or "").strip() or "planned"
    backpressure_status = status_value == "backpressure"
    effective_active_for_slot_audit = active_before if not backpressure_status else actor_budget_value
    idle_actor_slots_after_dispatch = max(0, actor_budget_value - effective_active_for_slot_audit - queued_workers)
    tail_coalescing_count = max(0, int(tail_coalescing_url_count or 0))
    worker_budget_deferred_count = max(0, max(0, int(deferred_url_count or 0)) - tail_coalescing_count)
    provider_slot_underuse_with_backlog = bool(
        status_value in {"queued", "running", "waiting_remote_harvest"}
        and resolved_dispatched_count > 0
        and worker_budget_deferred_count > 0
        and idle_actor_slots_after_dispatch > 0
    )
    underuse_reason = ""
    if provider_slot_underuse_with_backlog and submit_budget_value < actor_budget_value:
        underuse_reason = "submit_budget_below_actor_budget"
    elif provider_slot_underuse_with_backlog:
        underuse_reason = "idle_actor_slots_with_deferred_profile_urls"
    tiny_batch_max_size = _harvest_profile_tiny_batch_max_size()
    is_tiny_batch = bool(0 < effective_dispatch_size <= tiny_batch_max_size)
    return {
        "kind": "harvest_profile_scraper_batch",
        "chunk_index": max(0, int(chunk_index or 0)),
        "status": status_value,
        "flush_reason": str(flush_reason or "").strip() or "adaptive_prefetch_window",
        "batch_size": batch_size,
        "profile_url_count": len(profile_url_keys),
        "profile_url_keys": profile_url_keys,
        "profile_url_sample": normalized_profile_urls[:5],
        "dispatched_url_count": resolved_dispatched_count,
        "failed_url_count": failed_count,
        "requested_url_count": max(0, int(requested_url_count or 0)),
        "candidate_count": max(0, int(candidate_count or 0)),
        "deferred_url_count": max(0, int(deferred_url_count or 0)),
        "tail_coalescing_url_count": tail_coalescing_count,
        "worker_budget_deferred_url_count": worker_budget_deferred_count,
        "active_worker_count_before_dispatch": active_before,
        "queued_worker_count_after_dispatch": queued_workers,
        "actor_budget": actor_budget_value,
        "submit_budget": submit_budget_value,
        "recommended_batch_size": max(0, int(recommended_batch_size or 0)),
        "recommended_batch_count": max(0, int(recommended_batch_count or 0)),
        "recommended_max_workers": max(0, int(recommended_max_workers or 0)),
        "dispatch_strategy": str(dispatch_strategy or "").strip(),
        "small_batch_threshold": tiny_batch_max_size,
        "min_non_tail_batch_size": _harvest_profile_min_non_tail_batch_size(),
        "is_tiny_batch": is_tiny_batch,
        "small_batch_reason": small_batch_reason,
        "tiny_batch_allowed": not is_tiny_batch or _profile_batch_envelope_allowed_tiny_reason(small_batch_reason),
        "idle_actor_slots_after_dispatch": idle_actor_slots_after_dispatch,
        "provider_slot_underuse_with_backlog": provider_slot_underuse_with_backlog,
        "underuse_reason": underuse_reason,
        "backpressure_exempt_from_underuse": backpressure_status,
        "queue_quiescent": bool(queue_quiescent),
        "summary_path": str(summary_path or "").strip(),
    }


@dataclass(slots=True)
class MultiSourceEnrichmentResult:
    candidates: list[Candidate]
    evidence: list[EvidenceRecord]
    resolved_profiles: list[dict[str, Any]] = field(default_factory=list)
    unresolved_candidates: list[dict[str, Any]] = field(default_factory=list)
    publication_matches: list[dict[str, Any]] = field(default_factory=list)
    lead_candidates: list[Candidate] = field(default_factory=list)
    coauthor_edges: list[dict[str, Any]] = field(default_factory=list)
    artifact_paths: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    queued_harvest_worker_count: int = 0
    queued_exploration_count: int = 0
    stop_reason: str = ""
    profile_prefetch: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return {
            "resolved_profile_count": len(self.resolved_profiles),
            "unresolved_candidate_count": len(self.unresolved_candidates),
            "publication_match_count": len(self.publication_matches),
            "lead_candidate_count": len(self.lead_candidates),
            "coauthor_edge_count": len(self.coauthor_edges),
            "queued_harvest_worker_count": self.queued_harvest_worker_count,
            "queued_exploration_count": self.queued_exploration_count,
            "stop_reason": self.stop_reason,
            "artifact_paths": self.artifact_paths,
            "profile_prefetch": self.profile_prefetch,
            "errors": self.errors,
        }


@dataclass(slots=True)
class PublicationRecord:
    publication_id: str
    source: str
    source_dataset: str
    source_path: str
    title: str
    url: str
    year: int | None
    authors: list[str]
    acknowledgement_names: list[str]
    abstract: str = ""
    topics: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return {
            "publication_id": self.publication_id,
            "source": self.source,
            "source_dataset": self.source_dataset,
            "source_path": self.source_path,
            "title": self.title,
            "url": self.url,
            "year": self.year,
            "authors": self.authors,
            "acknowledgement_names": self.acknowledgement_names,
            "abstract": self.abstract,
            "topics": self.topics,
        }


class MultiSourceEnricher:
    def __init__(
        self,
        catalog: AssetCatalog,
        accounts: list[RapidApiAccount],
        harvest_profile_connector: HarvestProfileConnector | None = None,
        harvest_profile_search_connector: HarvestProfileSearchConnector | None = None,
        model_client: ModelClient | None = None,
        search_provider: BaseSearchProvider | None = None,
        worker_runtime: AgentRuntimeCoordinator | None = None,
        store: ControlPlaneStore | None = None,
        remote_provider_event_callback: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        self.catalog = catalog
        self.model_client = model_client
        self.store = store
        resolved_search_provider = search_provider or DuckDuckGoHtmlSearchProvider()
        self.slug_resolver = LinkedInSearchSlugResolver(accounts, search_provider=resolved_search_provider)
        self.profile_connector = LinkedInProfileDetailConnector(accounts)
        self.publication_connector = CompanyPublicationConnector(catalog)
        self.harvest_profile_connector = harvest_profile_connector
        self.harvest_profile_search_connector = harvest_profile_search_connector
        self.worker_runtime = worker_runtime
        self.remote_provider_event_callback = remote_provider_event_callback
        self.durable_runtime_writer = DurableRuntimeWriter(store) if store is not None else None
        self.exploratory_enricher = ExploratoryWebEnricher(
            model_client,
            worker_runtime=worker_runtime,
            search_provider=resolved_search_provider,
        )

    def _durable_runtime_writer(self) -> DurableRuntimeWriter | None:
        if self.store is None:
            self.durable_runtime_writer = None
            return None
        if self.durable_runtime_writer is None or getattr(self.durable_runtime_writer, "store", None) is not self.store:
            self.durable_runtime_writer = DurableRuntimeWriter(self.store)
        return self.durable_runtime_writer

    @staticmethod
    def _workflow_command_observation(
        command: dict[str, Any] | None,
        *,
        migration_phase: str = "W2b_profile_refill_submit",
    ) -> dict[str, Any]:
        payload = dict(command or {})
        record = {
            key: payload.get(key)
            for key in (
                "workflow_run_id",
                "operation_id",
                "command_id",
                "command_type",
                "owner",
                "idempotency_key",
                "status",
                "attempt",
                "last_error",
            )
            if payload.get(key) not in (None, "")
        }
        if record:
            record["migration_phase"] = str(migration_phase or "").strip() or "durable_runtime_command_owner"
            record["normal_path"] = True
        return record

    def _plan_linkedin_profile_url_terminal_record_command(
        self,
        *,
        job_id: str,
        snapshot_dir: Path,
        entries: list[dict[str, Any]],
        terminal_scope: str,
        actor: str,
        source: str,
    ) -> dict[str, Any]:
        normalized_entries = [
            dict(entry or {})
            for entry in list(entries or [])
            if str(dict(entry or {}).get("profile_url") or "").strip()
            and str(dict(entry or {}).get("status") or "").strip()
        ]
        if self.store is None or not normalized_entries:
            return {}
        normalized_job_id = str(job_id or "").strip()
        workflow_run_id = legacy_job_workflow_run_id(normalized_job_id)
        if not workflow_run_id:
            return {}
        operation_id = legacy_job_operation_id(normalized_job_id)
        command_idempotency_key = linkedin_profile_url_terminal_record_idempotency_key(
            job_id=normalized_job_id,
            snapshot_dir=str(snapshot_dir),
            entries=normalized_entries,
            terminal_scope=terminal_scope,
        )
        if not command_idempotency_key:
            return {}
        runtime_writer = self._durable_runtime_writer()
        if runtime_writer is None:
            return {}
        runtime_writer.append_event_and_reduce(
            workflow_run_id=workflow_run_id,
            operation_id=operation_id,
            event_family="workflow_event",
            event_type="WorkflowStarted",
            idempotency_key=f"{workflow_run_id}:legacy_job_profile_url_terminal_started",
            actor=str(actor or "profile_terminal_recorder"),
            source=str(source or "profile_terminal_record_planner"),
            payload={
                "workflow_type": "linkedin_acquisition",
                "stage_key": "profile_url_terminal_record",
                "job_id": normalized_job_id,
                "snapshot_dir": str(snapshot_dir),
                "migration_phase": "W2c_profile_url_terminal_record",
            },
        )
        apply_result = runtime_writer.append_event_and_reduce(
            workflow_run_id=workflow_run_id,
            operation_id=operation_id,
            event_family="workflow_event",
            event_type="CommandPlanRequested",
            idempotency_key=f"{command_idempotency_key}:plan",
            actor=str(actor or "profile_terminal_recorder"),
            source=str(source or "profile_terminal_record_planner"),
            payload={
                "workflow_type": "linkedin_acquisition",
                "stage_key": "profile_url_terminal_record",
                "command_type": LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
                "idempotency_key": command_idempotency_key,
                "payload": {
                    "job_id": normalized_job_id,
                    "snapshot_dir": str(snapshot_dir),
                    "snapshot_id": snapshot_dir.name,
                    "terminal_scope": str(terminal_scope or "").strip(),
                    "entry_count": len(normalized_entries),
                    "entries": normalized_entries,
                    "migration_phase": "W2c_profile_url_terminal_record",
                },
                "max_attempts": 5,
                "retry_policy": {
                    "kind": "profile_url_terminal_record",
                    "retry_delay_seconds": 10,
                },
            },
        )
        command = dict((apply_result.commands or ({},))[0] or {})
        if not command:
            for existing_command in self.store.list_workflow_commands(
                workflow_run_id=workflow_run_id,
                limit=0,
            ):
                if str(existing_command.get("idempotency_key") or "") == command_idempotency_key:
                    command = dict(existing_command or {})
                    break
        if not command:
            return {}
        return {
            "workflow_run_id": workflow_run_id,
            "operation_id": operation_id,
            "command_id": str(command.get("command_id") or ""),
            "command_type": str(command.get("command_type") or ""),
            "owner": str(command.get("owner") or ""),
            "idempotency_key": str(command.get("idempotency_key") or command_idempotency_key),
            "status": str(command.get("status") or ""),
            "attempt": int(command.get("attempt") or 0),
            "payload": dict(command.get("payload") or {}),
        }

    def run_linkedin_profile_url_terminal_record_command_once(
        self,
        command: dict[str, Any],
        *,
        lease_seconds: int = 300,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        payload = dict(command_payload.get("payload") or {})
        entries = [
            dict(entry or {})
            for entry in list(payload.get("entries") or [])
            if str(dict(entry or {}).get("profile_url") or "").strip()
        ]
        job_id = str(payload.get("job_id") or "").strip()

        def _command_observation(command_record: dict[str, Any] | None = None) -> dict[str, Any]:
            return self._workflow_command_observation(
                command_record or command_payload,
                migration_phase="W2c_profile_url_terminal_record",
            )

        def _result(
            *,
            status: str,
            reason: str,
            command_record: dict[str, Any] | None = None,
            runtime_command_contention: bool = False,
            recorded_count: int = 0,
            fetched_count: int = 0,
            failed_count: int = 0,
        ) -> dict[str, Any]:
            observed_command = _command_observation(command_record)
            if observed_command and runtime_command_contention:
                observed_command["runtime_command_contention"] = True
            return {
                "status": status,
                "reason": reason,
                "workflow_command": observed_command,
                "recorded_count": int(recorded_count or 0),
                "fetched_count": int(fetched_count or 0),
                "failed_count": int(failed_count or 0),
                "entry_count": len(entries),
            }

        if self.store is None or not command_id or not entries:
            return _result(status="skipped", reason="profile_url_terminal_record_command_payload_invalid")
        current_command = self.store.get_workflow_command(command_id) or command_payload
        current_status = str(current_command.get("status") or "").strip()
        if current_status == "succeeded":
            result_payload = dict(current_command.get("result") or {})
            return _result(
                status="completed",
                reason="typed_command_already_succeeded",
                command_record=current_command,
                recorded_count=int(result_payload.get("recorded_count") or len(entries)),
                fetched_count=int(result_payload.get("fetched_count") or 0),
                failed_count=int(result_payload.get("failed_count") or 0),
            )
        if current_status in {"claimed", "running", "failed_terminal", "cancelled", "superseded"}:
            return _result(
                status="queued",
                reason="typed_command_already_owned",
                command_record=current_command,
                runtime_command_contention=True,
            )

        lease_owner = f"{LINKEDIN_PROFILE_URL_TERMINAL_RECORD_OWNER}:{job_id or 'unknown_job'}:{command_id}"
        claimed = self.store.claim_workflow_command(
            command_id,
            lease_owner=lease_owner,
            lease_seconds=max(1, int(lease_seconds or 300)),
        )
        if not claimed:
            refreshed = self.store.get_workflow_command(command_id) or current_command
            return _result(
                status="queued",
                reason="typed_command_claim_contention",
                command_record=refreshed,
                runtime_command_contention=True,
            )
        running = self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner) or claimed
        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        workflow_run_id = str(running.get("workflow_run_id") or command_payload.get("workflow_run_id") or "").strip()
        operation_run_id = str(running.get("operation_id") or command_payload.get("operation_id") or "").strip()
        attempt_number = max(1, int(running.get("attempt") or 1))
        activity: dict[str, Any] = {}
        activity_attempt: dict[str, Any] = {}
        if workflow_run_id:
            activity = self.store.repos.workflow_runtime.upsert_activity_run(
                {
                    "workspace_id": workspace_id,
                    "workflow_run_id": workflow_run_id,
                    "operation_run_id": operation_run_id,
                    "command_id": command_id,
                    "activity_type": LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
                    "owner": LINKEDIN_PROFILE_URL_TERMINAL_RECORD_OWNER,
                    "status": "running",
                    "phase": "profile_url_terminal_record_running",
                    "idempotency_key": f"workflow_activity:{LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE}:{command_id}",
                    "input": {
                        "job_id": job_id,
                        "snapshot_dir": str(payload.get("snapshot_dir") or ""),
                        "snapshot_id": str(payload.get("snapshot_id") or ""),
                        "terminal_scope": str(payload.get("terminal_scope") or ""),
                        "entry_count": len(entries),
                        "profile_url_count": len(entries),
                    },
                    "entity_counts": {
                        "entry_count": len(entries),
                        "profile_url_count": len(entries),
                    },
                    "metadata": {
                        "activity_boundary": "profile_url_terminal_record",
                        "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                        "migration_phase": "W11_profile_url_terminal_activity_spine",
                        "lease_owner": lease_owner,
                        "workflow_command_id": command_id,
                        "workflow_command_type": LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
                        "workflow_command_owner": LINKEDIN_PROFILE_URL_TERMINAL_RECORD_OWNER,
                    },
                }
            )
            activity_run_id = str(activity.get("activity_run_id") or "").strip()
            if activity_run_id:
                activity_attempt = self.store.repos.workflow_runtime.upsert_activity_attempt(
                    {
                        "workspace_id": workspace_id,
                        "activity_run_id": activity_run_id,
                        "workflow_run_id": workflow_run_id,
                        "command_id": command_id,
                        "attempt_number": attempt_number,
                        "status": "running",
                        "provider": LINKEDIN_PROFILE_URL_TERMINAL_RECORD_OWNER,
                        "provider_request_ref": f"profile_url_terminal_record:{job_id or 'unknown_job'}:{command_id}",
                        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        "input": {
                            "job_id": job_id,
                            "snapshot_id": str(payload.get("snapshot_id") or ""),
                            "terminal_scope": str(payload.get("terminal_scope") or ""),
                            "entry_count": len(entries),
                        },
                        "idempotency_key": (
                            f"workflow_activity_attempt:{command_id}:profile_url_terminal_record:{attempt_number}"
                        ),
                        "metadata": {
                            "activity_boundary": "profile_url_terminal_record",
                            "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                            "lease_owner": lease_owner,
                        },
                    }
                )

        def _finish_activity_and_record_deltas(
            *,
            status: str,
            phase: str,
            output: dict[str, Any],
            delta_status: str,
            delta_kind: str,
            reason: str,
            error: dict[str, Any] | None = None,
            attempt_status: str = "",
        ) -> dict[str, Any]:
            if not activity or not workflow_run_id:
                return {}
            completed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            normalized_attempt_status = str(attempt_status or status).strip() or status
            final_attempt = (
                self.store.repos.workflow_runtime.upsert_activity_attempt(
                    {
                        **activity_attempt,
                        "status": normalized_attempt_status,
                        "completed_at": completed_at,
                        "output": dict(output or {}),
                        "error": dict(error or {}),
                    }
                )
                if activity_attempt
                else {}
            )
            final_activity = self.store.repos.workflow_runtime.upsert_activity_run(
                {
                    **activity,
                    "status": status,
                    "phase": phase,
                    "output": {
                        **dict(activity.get("output") or {}),
                        **dict(output or {}),
                        "latest_attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                    },
                    "entity_counts": {
                        **dict(activity.get("entity_counts") or {}),
                        "recorded_count": int(output.get("recorded_count") or 0),
                        "fetched_count": int(output.get("fetched_count") or 0),
                        "failed_count": int(output.get("failed_count") or 0),
                    },
                    "metadata": {
                        **dict(activity.get("metadata") or {}),
                        "latest_attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                    },
                }
            )
            delta_ids: list[str] = []
            if delta_status == "recorded":
                for entry in entries:
                    profile_url = str(entry.get("profile_url") or "").strip()
                    if not profile_url:
                        continue
                    profile_url_key = normalize_linkedin_profile_url_key(profile_url) or profile_url
                    terminal_status = str(entry.get("status") or "").strip() or "unknown"
                    delta = self.store.repos.workflow_runtime.upsert_entity_delta(
                        {
                            "workspace_id": workspace_id,
                            "workflow_run_id": workflow_run_id,
                            "operation_run_id": operation_run_id,
                            "command_id": command_id,
                            "activity_run_id": str(final_activity.get("activity_run_id") or "").strip(),
                            "attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                            "entity_type": "profile_url_terminal_record",
                            "entity_key": profile_url_key,
                            "delta_kind": delta_kind,
                            "status": delta_status,
                            "reason": reason,
                            "source_ref": {
                                "command_id": command_id,
                                "job_id": job_id,
                                "snapshot_id": str(payload.get("snapshot_id") or ""),
                                "terminal_scope": str(payload.get("terminal_scope") or ""),
                                "profile_url": profile_url,
                            },
                            "entity_payload": {
                                "profile_url": profile_url,
                                "profile_url_key": profile_url_key,
                                "terminal_status": terminal_status,
                                "raw_path": str(entry.get("raw_path") or ""),
                                "run_id": str(entry.get("run_id") or ""),
                                "dataset_id": str(entry.get("dataset_id") or ""),
                                "error": str(entry.get("error") or ""),
                                "retryable": bool(entry.get("retryable")),
                            },
                            "projection_effect": {
                                "entered_projection": False,
                                "profile_terminal_recorded": True,
                                "profile_fetched": terminal_status == "fetched",
                                "local_apply_eligible": terminal_status == "fetched",
                            },
                            "metadata": {
                                "activity_boundary": "profile_url_terminal_record",
                                "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                            },
                            "idempotency_key": (
                                "workflow_command_entity_delta:profile_url_terminal_record:"
                                f"{command_id}:profile_url_terminal_record:{profile_url_key}"
                            ),
                        }
                    )
                    if delta.get("delta_id"):
                        delta_ids.append(str(delta.get("delta_id") or ""))
            else:
                delta = self.store.repos.workflow_runtime.upsert_entity_delta(
                    {
                        "workspace_id": workspace_id,
                        "workflow_run_id": workflow_run_id,
                        "operation_run_id": operation_run_id,
                        "command_id": command_id,
                        "activity_run_id": str(final_activity.get("activity_run_id") or "").strip(),
                        "attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                        "entity_type": "profile_url_terminal_record",
                        "entity_key": command_id,
                        "delta_kind": delta_kind,
                        "status": delta_status,
                        "reason": reason,
                        "source_ref": {
                            "command_id": command_id,
                            "job_id": job_id,
                            "snapshot_id": str(payload.get("snapshot_id") or ""),
                            "terminal_scope": str(payload.get("terminal_scope") or ""),
                        },
                        "entity_payload": dict(output or {}),
                        "projection_effect": {
                            "entered_projection": False,
                            "profile_terminal_recorded": False,
                            "local_apply_eligible": False,
                        },
                        "metadata": {
                            "activity_boundary": "profile_url_terminal_record",
                            "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                        },
                        "idempotency_key": (
                            "workflow_command_entity_delta:profile_url_terminal_record:"
                            f"{command_id}:profile_url_terminal_record:{command_id}"
                        ),
                    }
                )
                if delta.get("delta_id"):
                    delta_ids.append(str(delta.get("delta_id") or ""))
            return {
                "activity_run_id": str(final_activity.get("activity_run_id") or "").strip(),
                "activity_attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                "entity_delta_id": delta_ids[0] if delta_ids else "",
                "entity_delta_ids": delta_ids[:20],
                "entity_delta_count": len(delta_ids),
            }

        fetched_count = 0
        failed_count = 0
        try:
            registry_repo = linkedin_profile_registry_repo(self.store)
            batch_writer = getattr(registry_repo, "backfill_batch", None)
            if callable(batch_writer):
                recorded_count = int(batch_writer(entries) or 0)
                fetched_count = sum(1 for entry in entries if str(entry.get("status") or "") == "fetched")
                failed_count = max(0, recorded_count - fetched_count)
            else:
                recorded_count = 0
                for entry in entries:
                    profile_url = str(entry.get("profile_url") or "").strip()
                    if not profile_url:
                        continue
                    if str(entry.get("status") or "").strip() == "fetched":
                        self.store.repos.linkedin_profile_registry.mark_fetched(
                            profile_url,
                            raw_path=str(entry.get("raw_path") or ""),
                            source_shards=list(entry.get("source_shards") or []),
                            source_jobs=list(entry.get("source_jobs") or []),
                            alias_urls=list(entry.get("alias_urls") or []),
                            raw_linkedin_url=str(entry.get("raw_linkedin_url") or ""),
                            sanity_linkedin_url=str(entry.get("sanity_linkedin_url") or ""),
                            run_id=str(entry.get("run_id") or ""),
                            dataset_id=str(entry.get("dataset_id") or ""),
                            snapshot_dir=str(entry.get("snapshot_dir") or payload.get("snapshot_dir") or ""),
                        )
                        fetched_count += 1
                    else:
                        self.store.repos.linkedin_profile_registry.mark_failed(
                            profile_url,
                            error=str(entry.get("error") or ""),
                            retryable=bool(entry.get("retryable")),
                            source_shards=list(entry.get("source_shards") or []),
                            source_jobs=list(entry.get("source_jobs") or []),
                            alias_urls=list(entry.get("alias_urls") or []),
                            raw_linkedin_url=str(entry.get("raw_linkedin_url") or ""),
                            sanity_linkedin_url=str(entry.get("sanity_linkedin_url") or ""),
                            run_id=str(entry.get("run_id") or ""),
                            dataset_id=str(entry.get("dataset_id") or ""),
                            snapshot_dir=str(entry.get("snapshot_dir") or payload.get("snapshot_dir") or ""),
                        )
                        failed_count += 1
                    recorded_count += 1
            activity_evidence = _finish_activity_and_record_deltas(
                status="succeeded",
                phase="profile_url_terminal_record_completed",
                output={
                    "recorded_count": recorded_count,
                    "fetched_count": fetched_count,
                    "failed_count": failed_count,
                    "entry_count": len(entries),
                    "terminal_scope": str(payload.get("terminal_scope") or ""),
                },
                delta_status="recorded",
                delta_kind="profile_url_terminal_recorded",
                reason="profile_url_terminal_record_command_executed",
            )
            terminal_command = self.store.mark_workflow_command_succeeded(
                command_id,
                result={
                    "recorded_count": recorded_count,
                    "fetched_count": fetched_count,
                    "failed_count": failed_count,
                    "entry_count": len(entries),
                    "terminal_scope": str(payload.get("terminal_scope") or ""),
                    **activity_evidence,
                },
            )
        except Exception as exc:
            _finish_activity_and_record_deltas(
                status="retry_wait",
                phase="profile_url_terminal_record_retry_wait",
                output={
                    "recorded_count": 0,
                    "fetched_count": fetched_count,
                    "failed_count": failed_count,
                    "entry_count": len(entries),
                    "terminal_scope": str(payload.get("terminal_scope") or ""),
                },
                delta_status="not_applied",
                delta_kind="profile_url_terminal_record_not_applied",
                reason="profile_url_terminal_record_command_failed",
                error={"error_type": type(exc).__name__, "message": str(exc)},
                attempt_status="retry_wait",
            )
            terminal_command = self.store.mark_workflow_command_failed(
                command_id,
                error_text=f"{type(exc).__name__}: {exc}",
                retryable=True,
                retry_delay_seconds=10,
            )
            return _result(
                status="failed",
                reason="profile_url_terminal_record_command_failed",
                command_record=terminal_command or running,
                recorded_count=0,
            )
        return _result(
            status="completed",
            reason="profile_url_terminal_record_command_executed",
            command_record=terminal_command or running,
            recorded_count=recorded_count,
            fetched_count=fetched_count,
            failed_count=failed_count,
        )

    def drain_linkedin_profile_url_terminal_record_commands(
        self,
        *,
        workflow_run_id: str = "",
        limit: int = 20,
    ) -> dict[str, Any]:
        if self.store is None:
            return {
                "status": "skipped",
                "reason": "store_unavailable",
                "command_count": 0,
                "executed_command_count": 0,
                "recorded_count": 0,
                "fetched_count": 0,
                "failed_count": 0,
            }
        ready_commands = self.store.list_ready_workflow_commands(
            workflow_run_id=str(workflow_run_id or "").strip(),
            owner=LINKEDIN_PROFILE_URL_TERMINAL_RECORD_OWNER,
            command_type=LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
            limit=max(1, int(limit or 20)),
        )
        results: list[dict[str, Any]] = []
        recorded_count = 0
        fetched_count = 0
        failed_count = 0
        executed_command_count = 0
        for command in list(ready_commands or []):
            result = self.run_linkedin_profile_url_terminal_record_command_once(dict(command or {}))
            if str(result.get("status") or "") == "completed":
                executed_command_count += 1
            recorded_count += int(result.get("recorded_count") or 0)
            fetched_count += int(result.get("fetched_count") or 0)
            failed_count += int(result.get("failed_count") or 0)
            results.append(result)
        return {
            "status": "active" if executed_command_count > 0 or recorded_count > 0 else "idle",
            "reason": "" if ready_commands else "no_ready_profile_url_terminal_record_commands",
            "workflow_run_id": str(workflow_run_id or "").strip(),
            "command_count": len(ready_commands),
            "executed_command_count": executed_command_count,
            "recorded_count": recorded_count,
            "fetched_count": fetched_count,
            "failed_count": failed_count,
            "results": results,
        }

    def run_linkedin_profile_refill_submit_command_once(
        self,
        command: dict[str, Any],
        *,
        lease_seconds: int = 7200,
    ) -> dict[str, Any]:
        """Execute one `linkedin.profile_refill.submit_batch` command as its owner.

        This is the W2b migration entrypoint. The refill scheduler may call it
        synchronously for now, but provider submit claim/run/terminal semantics
        live here so the next slice can move this method into a daemon loop
        without changing command payloads.
        """

        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        payload = dict(command_payload.get("payload") or {})
        profile_url_chunk = [
            str(profile_url or "").strip()
            for profile_url in list(payload.get("profile_urls") or [])
            if str(profile_url or "").strip()
        ]
        chunk_index = int(payload.get("chunk_index") or 0)
        snapshot_dir = Path(str(payload.get("snapshot_dir") or "")).expanduser()
        job_id = str(payload.get("job_id") or "").strip()

        def _owned_result(
            *,
            status: str,
            reason: str,
            command_record: dict[str, Any] | None = None,
            runtime_command_contention: bool = True,
        ) -> dict[str, Any]:
            observed_command = self._workflow_command_observation(command_record or command_payload)
            if observed_command and runtime_command_contention:
                observed_command["runtime_command_contention"] = True
            return {
                "status": status,
                "reason": reason,
                "workflow_command": observed_command,
                "dispatch_result": {
                    "chunk_index": chunk_index,
                    "queued_urls": list(profile_url_chunk),
                    "deferred_urls": [],
                    "failed_urls": [],
                    "dispatched_url_count": 0,
                    "error_message": "",
                    "summary_path": "",
                    "provider_submit_started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "provider_submit_finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "provider_submit_elapsed_ms": 0,
                    "owner_worker_id": 0,
                    "owner_run_id": "",
                    "owner_dataset_id": "",
                    "owner_payload_hash": "",
                    "worker_status": status,
                    "small_batch_reason": reason,
                    "deferred_reason": reason,
                },
            }

        if self.store is None:
            # Fail-closed (decision #1 / invariant 7): a missing durable store is an
            # infrastructure outage, not a "confirmed empty queue". It must surface as a
            # loud blocked terminal that the aggregation/recovery chain can distinguish
            # from "nothing to dispatch" — never a synthetic queued tail. We do NOT set
            # runtime_command_contention here: contention means "another owner holds the
            # command", which is false when there is no store at all.
            return _owned_result(
                status="blocked",
                reason="profile_refill_store_unavailable",
                runtime_command_contention=False,
            )
        if not command_id or not job_id or not profile_url_chunk or not snapshot_dir:
            return _owned_result(
                status="skipped",
                reason="profile_refill_submit_command_payload_invalid",
            )
        current_command = self.store.get_workflow_command(command_id) or command_payload
        current_status = str(current_command.get("status") or "").strip()
        if current_status in {"claimed", "running", "succeeded", "failed_terminal", "cancelled", "superseded"}:
            return _owned_result(
                status="queued",
                reason="typed_command_already_owned",
                command_record=current_command,
            )

        lease_owner = f"linkedin_profile_owner:{job_id}:{command_id}"
        claimed = self.store.claim_workflow_command(
            command_id,
            lease_owner=lease_owner,
            lease_seconds=max(1, int(lease_seconds or 7200)),
        )
        if not claimed:
            refreshed = self.store.get_workflow_command(command_id) or current_command
            return _owned_result(
                status="queued",
                reason="typed_command_claim_contention",
                command_record=refreshed,
            )
        running = self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner) or claimed

        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        workflow_run_id = str(running.get("workflow_run_id") or command_payload.get("workflow_run_id") or "").strip()
        operation_run_id = str(running.get("operation_id") or command_payload.get("operation_id") or "").strip()
        attempt_number = max(1, int(running.get("attempt") or 1))
        activity: dict[str, Any] = {}
        activity_attempt: dict[str, Any] = {}
        if workflow_run_id:
            activity = self.store.repos.workflow_runtime.upsert_activity_run(
                {
                    "workspace_id": workspace_id,
                    "workflow_run_id": workflow_run_id,
                    "operation_run_id": operation_run_id,
                    "command_id": command_id,
                    "activity_type": LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
                    "owner": LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_OWNER,
                    "status": "running",
                    "phase": "profile_refill_submit_running",
                    "idempotency_key": f"workflow_activity:{LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE}:{command_id}",
                    "input": {
                        "job_id": job_id,
                        "snapshot_dir": str(snapshot_dir),
                        "chunk_index": chunk_index,
                        "profile_urls": profile_url_chunk,
                        "profile_url_count": len(profile_url_chunk),
                    },
                    "entity_counts": {
                        "profile_url_count": len(profile_url_chunk),
                    },
                    "metadata": {
                        "activity_boundary": "profile_refill_submit",
                        "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                        "migration_phase": "W11_profile_refill_submit_activity_spine",
                        "lease_owner": lease_owner,
                        "workflow_command_id": command_id,
                        "workflow_command_type": LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
                        "workflow_command_owner": LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_OWNER,
                    },
                }
            )
            activity_run_id = str(activity.get("activity_run_id") or "").strip()
            if activity_run_id:
                activity_attempt = self.store.repos.workflow_runtime.upsert_activity_attempt(
                    {
                        "workspace_id": workspace_id,
                        "activity_run_id": activity_run_id,
                        "workflow_run_id": workflow_run_id,
                        "command_id": command_id,
                        "attempt_number": attempt_number,
                        "status": "running",
                        "provider": "harvest_profile",
                        "provider_request_ref": f"profile_refill_submit:{job_id}:{command_id}",
                        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        "input": {
                            "job_id": job_id,
                            "snapshot_dir": str(snapshot_dir),
                            "chunk_index": chunk_index,
                            "profile_url_count": len(profile_url_chunk),
                        },
                        "idempotency_key": (
                            f"workflow_activity_attempt:{command_id}:profile_refill_submit:{attempt_number}"
                        ),
                        "metadata": {
                            "activity_boundary": "profile_refill_submit",
                            "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                            "lease_owner": lease_owner,
                        },
                    }
                )

        def _finish_refill_submit_activity(
            *,
            status: str,
            phase: str,
            output: dict[str, Any],
            delta_kind: str,
            delta_status: str,
            reason: str,
            profile_urls: list[str],
            error: dict[str, Any] | None = None,
            artifact_refs: list[str] | None = None,
        ) -> dict[str, Any]:
            if not activity or not workflow_run_id:
                return {}
            completed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            final_attempt = (
                self.store.repos.workflow_runtime.upsert_activity_attempt(
                    {
                        **activity_attempt,
                        "status": status,
                        "completed_at": completed_at,
                        "output": dict(output or {}),
                        "error": dict(error or {}),
                        "artifact_refs": list(artifact_refs or []),
                    }
                )
                if activity_attempt
                else {}
            )
            final_activity = self.store.repos.workflow_runtime.upsert_activity_run(
                {
                    **activity,
                    "status": status,
                    "phase": phase,
                    "output": {
                        **dict(activity.get("output") or {}),
                        **dict(output or {}),
                        "latest_attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                    },
                    "artifact_refs": list(artifact_refs or []),
                    "entity_counts": {
                        **dict(activity.get("entity_counts") or {}),
                        "queued_url_count": len(list(output.get("queued_urls") or [])),
                        "deferred_url_count": len(list(output.get("deferred_urls") or [])),
                        "failed_url_count": len(list(output.get("failed_urls") or [])),
                        "dispatched_url_count": int(output.get("dispatched_url_count") or 0),
                    },
                    "metadata": {
                        **dict(activity.get("metadata") or {}),
                        "latest_attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                    },
                }
            )
            delta_ids: list[str] = []
            for profile_url in [
                str(profile_url or "").strip()
                for profile_url in list(profile_urls or [])
                if str(profile_url or "").strip()
            ]:
                profile_url_key = normalize_linkedin_profile_url_key(profile_url) or profile_url
                delta = self.store.repos.workflow_runtime.upsert_entity_delta(
                    {
                        "workspace_id": workspace_id,
                        "workflow_run_id": workflow_run_id,
                        "operation_run_id": operation_run_id,
                        "command_id": command_id,
                        "activity_run_id": str(final_activity.get("activity_run_id") or "").strip(),
                        "attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                        "entity_type": "profile_refill_submit",
                        "entity_key": profile_url_key,
                        "delta_kind": delta_kind,
                        "status": delta_status,
                        "reason": reason,
                        "source_ref": {
                            "command_id": command_id,
                            "job_id": job_id,
                            "snapshot_dir": str(snapshot_dir),
                            "chunk_index": chunk_index,
                            "profile_url": profile_url,
                        },
                        "entity_payload": {
                            "profile_url": profile_url,
                            "profile_url_key": profile_url_key,
                            "worker_status": str(output.get("worker_status") or ""),
                            "owner_worker_id": int(output.get("owner_worker_id") or 0),
                            "owner_run_id": str(output.get("owner_run_id") or ""),
                            "owner_dataset_id": str(output.get("owner_dataset_id") or ""),
                            "error_message": str(output.get("error_message") or ""),
                        },
                        "projection_effect": {
                            "entered_projection": False,
                            "provider_submit_queued": delta_status == "recorded",
                            "provider_terminal_pending": delta_status == "recorded",
                            "profile_terminal_record_pending": delta_status == "recorded",
                        },
                        "artifact_refs": list(artifact_refs or []),
                        "metadata": {
                            "activity_boundary": "profile_refill_submit",
                            "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                        },
                        "idempotency_key": (
                            "workflow_command_entity_delta:profile_refill_submit:"
                            f"{command_id}:profile_refill_submit:{profile_url_key}"
                        ),
                    }
                )
                if delta.get("delta_id"):
                    delta_ids.append(str(delta.get("delta_id") or ""))
            return {
                "activity_run_id": str(final_activity.get("activity_run_id") or "").strip(),
                "activity_attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                "entity_delta_id": delta_ids[0] if delta_ids else "",
                "entity_delta_ids": delta_ids[:20],
                "entity_delta_count": len(delta_ids),
            }

        submit_started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        submit_started_monotonic = time.perf_counter()
        try:
            harvest_worker = self._execute_harvest_profile_batch_worker(
                profile_urls=profile_url_chunk,
                snapshot_dir=snapshot_dir,
                job_id=job_id,
                request_payload=dict(payload.get("request_payload") or {}),
                plan_payload=dict(payload.get("plan_payload") or {}),
                runtime_mode=str(payload.get("runtime_mode") or ""),
                allow_shared_provider_cache=bool(payload.get("allow_shared_provider_cache", True)),
                load_cached_profile_payloads=bool(payload.get("load_cached_profile_payloads", True)),
                prefetch_batch_context={
                    "requested_url_count": int(payload.get("requested_url_count") or len(profile_url_chunk)),
                    "candidate_count": int(payload.get("candidate_count") or len(profile_url_chunk)),
                    "planned_deferred_url_count": int(payload.get("planned_deferred_url_count") or 0),
                    "planned_dispatch_worker_count": int(payload.get("planned_dispatch_worker_count") or 0),
                    "batch_plan_reason": str(payload.get("batch_plan_reason") or ""),
                    "allow_under_target_final_tail_dispatch": bool(
                        payload.get("allow_under_target_final_tail_dispatch")
                    ),
                    "chunk_index": chunk_index,
                    "retry_isolation": bool(payload.get("retry_isolated_refill")),
                    "nonblocking_submit": bool(payload.get("nonblocking_submit")),
                },
            )
        except Exception as exc:
            harvest_worker = {
                "worker_status": "failed",
                "failed_urls": profile_url_chunk,
                "summary": {
                    "message": f"{type(exc).__name__}: {exc}",
                    "failed_urls": profile_url_chunk,
                },
            }
        submit_elapsed_ms = int(max(0.0, (time.perf_counter() - submit_started_monotonic) * 1000))
        submit_finished_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        worker_summary = dict(harvest_worker.get("summary") or {})
        worker_status = str(harvest_worker.get("worker_status") or "").strip()
        queued_urls: list[str] = []
        deferred_urls: list[str] = []
        failed_urls: list[str] = []
        error_message = ""
        dispatched_url_count = 0
        deferred_reason = ""
        if worker_status == "queued":
            queued_urls = [
                str(profile_url or "").strip()
                for profile_url in list(worker_summary.get("queued_urls") or worker_summary.get("requested_urls") or [])
                if str(profile_url or "").strip()
            ]
            dispatched_url_count = len(queued_urls)
        elif worker_status == "backpressure":
            deferred_urls = [
                str(profile_url or "").strip()
                for profile_url in list(
                    worker_summary.get("deferred_urls")
                    or worker_summary.get("requested_urls")
                    or profile_url_chunk
                    or []
                )
                if str(profile_url or "").strip()
            ]
            deferred_reason = str(worker_summary.get("small_batch_reason") or worker_summary.get("message") or "")
        elif worker_status == "completed":
            dispatched_url_count = int(worker_summary.get("dispatched_url_count") or 0)
        elif worker_status == "failed":
            failed_urls = [
                str(profile_url or "").strip()
                for profile_url in list(
                    harvest_worker.get("failed_urls")
                    or worker_summary.get("failed_urls")
                    or worker_summary.get("queued_urls")
                    or []
                )
                if str(profile_url or "").strip()
            ]
            error_message = str(worker_summary.get("message") or "").strip()
        small_batch_reason = str(worker_summary.get("small_batch_reason") or "").strip()
        if not small_batch_reason and str(payload.get("batch_plan_reason") or "") == "queue_quiescent_final_tail":
            small_batch_reason = "queue_quiescent_final_tail"
        if not deferred_reason:
            deferred_reason = small_batch_reason
        dispatch_result = {
            "chunk_index": chunk_index,
            "queued_urls": queued_urls,
            "deferred_urls": deferred_urls,
            "failed_urls": failed_urls,
            "dispatched_url_count": dispatched_url_count,
            "error_message": error_message,
            "summary_path": str(worker_summary.get("summary_path") or "").strip(),
            "provider_submit_started_at": submit_started_at,
            "provider_submit_finished_at": submit_finished_at,
            "provider_submit_elapsed_ms": submit_elapsed_ms,
            "owner_worker_id": int(worker_summary.get("worker_id") or 0),
            "owner_run_id": str(worker_summary.get("run_id") or "").strip(),
            "owner_dataset_id": str(worker_summary.get("dataset_id") or "").strip(),
            "owner_payload_hash": str(worker_summary.get("payload_hash") or "").strip(),
            "worker_status": worker_status,
            "small_batch_reason": small_batch_reason,
            "deferred_reason": deferred_reason,
        }
        artifact_refs = [str(dispatch_result.get("summary_path") or "").strip()]
        artifact_refs = [artifact_ref for artifact_ref in artifact_refs if artifact_ref]
        effect_profile_urls = queued_urls or deferred_urls or failed_urls or profile_url_chunk
        if worker_status in {"queued", "completed"}:
            activity_evidence = _finish_refill_submit_activity(
                status="succeeded",
                phase=f"profile_refill_submit_{worker_status}",
                output=dispatch_result,
                delta_kind="profile_refill_submit_queued",
                delta_status="recorded",
                reason="profile_refill_submit_command_executed",
                profile_urls=effect_profile_urls,
                artifact_refs=artifact_refs,
            )
        else:
            activity_evidence = _finish_refill_submit_activity(
                status="retry_wait" if worker_status in {"backpressure", "failed"} else "failed",
                phase=f"profile_refill_submit_{worker_status or 'failed'}",
                output=dispatch_result,
                delta_kind="profile_refill_submit_not_applied",
                delta_status="not_applied",
                reason="profile_refill_submit_command_failed",
                profile_urls=effect_profile_urls,
                error={"message": error_message or str(worker_summary.get("message") or "")},
                artifact_refs=artifact_refs,
            )
        if worker_status in {"queued", "completed"}:
            terminal_command = self.store.mark_workflow_command_succeeded(
                command_id,
                result={
                    "worker_status": worker_status,
                    "queued_url_count": len(queued_urls),
                    "deferred_url_count": len(deferred_urls),
                    "failed_url_count": len(failed_urls),
                    "dispatched_url_count": dispatched_url_count,
                    "summary_path": str(dispatch_result.get("summary_path") or ""),
                    "owner_worker_id": int(dispatch_result.get("owner_worker_id") or 0),
                    "owner_run_id": str(dispatch_result.get("owner_run_id") or ""),
                    "owner_dataset_id": str(dispatch_result.get("owner_dataset_id") or ""),
                    "owner_payload_hash": str(dispatch_result.get("owner_payload_hash") or ""),
                    **activity_evidence,
                },
            )
        else:
            terminal_command = self.store.mark_workflow_command_failed(
                command_id,
                error_text=error_message
                or str(worker_summary.get("message") or "").strip()
                or worker_status
                or "profile_refill_submit_failed",
                retryable=worker_status in {"backpressure", "failed"},
                retry_delay_seconds=30,
            )
        return {
            "status": "completed",
            "reason": "profile_refill_submit_command_executed",
            "workflow_command": self._workflow_command_observation(terminal_command or running),
            "dispatch_result": dispatch_result,
            "harvest_worker": harvest_worker,
        }

    def drain_linkedin_profile_refill_submit_commands(
        self,
        *,
        workflow_run_id: str = "",
        limit: int = 10,
    ) -> dict[str, Any]:
        if self.store is None:
            return {
                "status": "skipped",
                "reason": "store_unavailable",
                "command_count": 0,
                "dispatched_url_count": 0,
                "queued_worker_count": 0,
                "deferred_url_count": 0,
            }
        ready_commands = self.store.list_ready_workflow_commands(
            workflow_run_id=str(workflow_run_id or "").strip(),
            owner="linkedin_profile_owner",
            command_type=LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
            limit=max(1, int(limit or 10)),
        )
        results: list[dict[str, Any]] = []
        dispatched_url_count = 0
        queued_worker_count = 0
        deferred_url_count = 0
        for command in list(ready_commands or []):
            result = self.run_linkedin_profile_refill_submit_command_once(dict(command or {}))
            dispatch_result = dict(result.get("dispatch_result") or {})
            dispatched_url_count += int(dispatch_result.get("dispatched_url_count") or 0)
            if list(dispatch_result.get("queued_urls") or []):
                queued_worker_count += 1
            deferred_url_count += len(list(dispatch_result.get("deferred_urls") or []))
            results.append(result)
        return {
            "status": "active"
            if dispatched_url_count > 0 or queued_worker_count > 0 or deferred_url_count > 0
            else "idle",
            "reason": "linkedin_profile_owner_submit_command_drain",
            "command_count": len(ready_commands),
            "executed_command_count": len(results),
            "workflow_run_id": str(workflow_run_id or "").strip(),
            "dispatched_url_count": dispatched_url_count,
            "queued_worker_count": queued_worker_count,
            "deferred_url_count": deferred_url_count,
            "results": results,
        }

    @staticmethod
    def _path_identity_values(path_value: str | Path) -> set[str]:
        raw_value = str(path_value or "").strip()
        if not raw_value:
            return set()
        path = Path(raw_value).expanduser()
        values = {str(path)}
        try:
            values.add(str(path.resolve(strict=False)))
        except OSError:
            pass
        return {item for item in values if item}

    def _active_harvest_profile_batch_worker_count(self, *, job_id: str, snapshot_dir: Path) -> int:
        if self.worker_runtime is None or not str(job_id or "").strip():
            return 0
        normalized_snapshot_values = self._path_identity_values(snapshot_dir)
        active_count = 0
        try:
            workers = self.worker_runtime.list_workers(job_id=str(job_id), lane_id="enrichment_specialist")
        except Exception:
            return 0
        for worker in list(workers or []):
            metadata = dict(worker.get("metadata") or {})
            if str(metadata.get("recovery_kind") or "").strip() != "harvest_profile_batch":
                continue
            worker_snapshot_dir = str(metadata.get("snapshot_dir") or "").strip()
            if worker_snapshot_dir and self._path_identity_values(worker_snapshot_dir).isdisjoint(
                normalized_snapshot_values
            ):
                continue
            status = str(worker.get("status") or "").strip().lower()
            if status in _TERMINAL_HARVEST_PROFILE_WORKER_STATUSES:
                continue
            checkpoint = dict(worker.get("checkpoint") or {})
            remote_terminal_event = dict(checkpoint.get("remote_provider_terminal_event") or {})
            if str(checkpoint.get("remote_provider_terminal_event_seen_at") or "").strip() or bool(
                remote_terminal_event.get("is_terminal")
            ):
                continue
            output = dict(worker.get("output") or {})
            summary = dict(output.get("summary") or {})
            checkpoint_stage = str(checkpoint.get("stage") or "").strip().lower()
            has_remote_identifier = bool(
                str(checkpoint.get("run_id") or summary.get("run_id") or "").strip()
                or str(checkpoint.get("dataset_id") or summary.get("dataset_id") or "").strip()
            )
            has_provider_slot = bool(
                str(dict(checkpoint.get("provider_limiter_lease") or {}).get("lease_token") or "").strip()
            )
            if checkpoint_stage == "waiting_remote_harvest":
                active_count += 1
            elif status in _REMOTE_HARVEST_PROFILE_WORKER_STATUSES and has_remote_identifier:
                active_count += 1
            elif status == "running" and (has_remote_identifier or has_provider_slot):
                active_count += 1
        return active_count

    def _profile_prefetch_reserved_or_owned_worker_count(self, *, job_id: str, snapshot_dir: Path) -> int:
        """Count durable scheduler-owned profile batches for this job snapshot.

        The profile scheduler reserves registry rows before provider submit. A
        concurrent replan must treat those reservations as actor-slot occupancy
        even if the local worker has not yet reached `waiting_remote_harvest`.
        Once the remote provider has emitted a terminal event for a
        `planned_dispatch` owner, the actor slot is available again even if the
        worker body is still being persisted or local-apply has not run.
        """

        if self.store is None or not str(job_id or "").strip():
            return 0
        registry_repo = linkedin_profile_registry_repo(self.store)
        list_items = getattr(registry_repo, "list_refill_queue_items", None)
        if not callable(list_items):
            return 0
        try:
            items = list(
                list_items(
                    states=["dispatch_reserved", "dispatch_claimed", "planned_dispatch"],
                    source_job=str(job_id or "").strip(),
                    snapshot_dir=str(snapshot_dir),
                    limit=10000,
                    ready_only=False,
                )
                or []
            )
        except Exception:
            return 0
        terminal_owner_keys: set[str] = set()
        if self.worker_runtime is not None:
            list_workers = getattr(self.worker_runtime, "list_workers", None)
            if callable(list_workers):
                normalized_snapshot_values = self._path_identity_values(snapshot_dir)
                try:
                    workers = list(list_workers(job_id=str(job_id), lane_id="enrichment_specialist") or [])
                except Exception:
                    workers = []
                for worker in workers:
                    worker_payload = dict(worker or {})
                    metadata = dict(worker_payload.get("metadata") or {})
                    if str(metadata.get("recovery_kind") or "").strip() != "harvest_profile_batch":
                        continue
                    worker_snapshot_dir = str(metadata.get("snapshot_dir") or "").strip()
                    if worker_snapshot_dir and self._path_identity_values(worker_snapshot_dir).isdisjoint(
                        normalized_snapshot_values
                    ):
                        continue
                    checkpoint = dict(worker_payload.get("checkpoint") or {})
                    remote_terminal_event = dict(checkpoint.get("remote_provider_terminal_event") or {})
                    status = str(worker_payload.get("status") or "").strip().lower()
                    if not (
                        status in _TERMINAL_HARVEST_PROFILE_WORKER_STATUSES
                        or str(checkpoint.get("remote_provider_terminal_event_seen_at") or "").strip()
                        or bool(remote_terminal_event.get("is_terminal"))
                    ):
                        continue
                    output = dict(worker_payload.get("output") or {})
                    summary = dict(output.get("summary") or {})
                    for value in (
                        str(worker_payload.get("worker_id") or "").strip(),
                        str(checkpoint.get("run_id") or summary.get("run_id") or "").strip(),
                        str(checkpoint.get("dataset_id") or summary.get("dataset_id") or "").strip(),
                        str(checkpoint.get("payload_hash") or summary.get("payload_hash") or "").strip(),
                    ):
                        if value:
                            terminal_owner_keys.add(value)
        owner_keys: set[tuple[str, str]] = set()
        unowned_count = 0
        for item in items:
            payload = dict(item or {})
            status = str(payload.get("status") or "").strip().lower()
            if status in {"fetched", "unrecoverable"}:
                continue
            state = str(payload.get("refill_queue_state") or "").strip().lower()
            if state not in {"dispatch_reserved", "dispatch_claimed", "planned_dispatch"}:
                continue
            owner_key = (
                str(payload.get("refill_owner_worker_id") or "").strip()
                or str(payload.get("refill_owner_run_id") or "").strip()
                or str(payload.get("refill_owner_dataset_id") or "").strip()
                or str(payload.get("refill_owner_payload_hash") or "").strip()
            )
            if state == "planned_dispatch" and owner_key and owner_key in terminal_owner_keys:
                continue
            if owner_key:
                owner_keys.add((state, owner_key))
            else:
                unowned_count += 1
        fallback_slot_count = 0
        if unowned_count > 0:
            fallback_slot_count = (
                unowned_count + _harvest_profile_prefetch_actor_slot_url_target() - 1
            ) // _harvest_profile_prefetch_actor_slot_url_target()
        return len(owner_keys) + fallback_slot_count

    @staticmethod
    def _harvest_profile_batch_worker_url_keys(worker: dict[str, Any]) -> set[str]:
        values: list[str] = []
        worker_payloads = [
            dict(worker.get("metadata") or {}),
            dict(worker.get("input") or {}),
            dict(worker.get("checkpoint") or {}),
            dict(dict(worker.get("output") or {}).get("summary") or {}),
        ]
        for payload in worker_payloads:
            for field_name in (
                "profile_urls",
                "requested_urls",
                "queued_urls",
                "failed_urls",
                "deferred_urls",
            ):
                values.extend(
                    [str(item or "").strip() for item in list(payload.get(field_name) or []) if str(item or "").strip()]
                )
        return {
            normalize_linkedin_profile_url_key(profile_url)
            for profile_url in values
            if normalize_linkedin_profile_url_key(profile_url)
        }

    @staticmethod
    def _harvest_profile_batch_worker_remote_identifiers(worker: dict[str, Any]) -> tuple[str, str]:
        checkpoint = dict(worker.get("checkpoint") or {})
        summary = dict(dict(worker.get("output") or {}).get("summary") or {})
        run_id = str(
            checkpoint.get("run_id")
            or checkpoint.get("actor_run_id")
            or checkpoint.get("actorRunId")
            or summary.get("run_id")
            or summary.get("actor_run_id")
            or summary.get("actorRunId")
            or ""
        ).strip()
        dataset_id = str(
            checkpoint.get("dataset_id")
            or checkpoint.get("default_dataset_id")
            or checkpoint.get("defaultDatasetId")
            or summary.get("dataset_id")
            or summary.get("default_dataset_id")
            or summary.get("defaultDatasetId")
            or ""
        ).strip()
        return run_id, dataset_id

    def _harvest_profile_batch_worker_is_active_for_url(
        self,
        worker: dict[str, Any],
        *,
        profile_url: str,
        snapshot_dir: Path,
        allow_remote_identifier_match_without_url: bool = False,
    ) -> bool:
        metadata = dict(worker.get("metadata") or {})
        worker_key = str(worker.get("worker_key") or "").strip()
        recovery_kind = str(metadata.get("recovery_kind") or "").strip()
        if recovery_kind != "harvest_profile_batch" and not worker_key.startswith("harvest_profile_batch::"):
            return False
        status = str(worker.get("status") or "").strip().lower()
        if status in {"completed", "failed", "cancelled", "canceled", "superseded"}:
            return False
        profile_key = normalize_linkedin_profile_url_key(profile_url)
        worker_url_keys = self._harvest_profile_batch_worker_url_keys(worker)
        if profile_key and worker_url_keys and profile_key not in worker_url_keys:
            return False
        if profile_key and not worker_url_keys and not allow_remote_identifier_match_without_url:
            return False
        worker_snapshot_dir = str(metadata.get("snapshot_dir") or "").strip()
        if worker_snapshot_dir and not allow_remote_identifier_match_without_url:
            current_snapshot_values = self._path_identity_values(snapshot_dir)
            if self._path_identity_values(worker_snapshot_dir).isdisjoint(current_snapshot_values):
                return False
        checkpoint = dict(worker.get("checkpoint") or {})
        checkpoint_stage = str(checkpoint.get("stage") or "").strip().lower()
        run_id, dataset_id = self._harvest_profile_batch_worker_remote_identifiers(worker)
        has_remote_identifier = bool(run_id or dataset_id)
        has_provider_slot = bool(
            str(dict(checkpoint.get("provider_limiter_lease") or {}).get("lease_token") or "").strip()
        )
        if checkpoint_stage == "waiting_remote_harvest":
            return True
        if status in _REMOTE_HARVEST_PROFILE_WORKER_STATUSES and has_remote_identifier:
            return True
        if status == "running" and (has_remote_identifier or has_provider_slot):
            return True
        return False

    def _queued_profile_registry_entry_has_active_worker(
        self,
        profile_url: str,
        registry_entry: dict[str, Any],
        *,
        job_id: str,
        snapshot_dir: Path,
    ) -> bool:
        entry = dict(registry_entry or {})
        workers_by_id: dict[int, tuple[dict[str, Any], bool]] = {}
        inspected_worker_state = False

        def _add_workers(
            items: list[dict[str, Any]] | tuple[dict[str, Any], ...], *, remote_identifier_match: bool
        ) -> None:
            nonlocal inspected_worker_state
            inspected_worker_state = True
            for worker in list(items or []):
                try:
                    worker_id = int(dict(worker or {}).get("worker_id") or 0)
                except (TypeError, ValueError):
                    worker_id = 0
                key = worker_id if worker_id > 0 else id(worker)
                previous = workers_by_id.get(key)
                workers_by_id[key] = (dict(worker or {}), bool(remote_identifier_match or (previous or ({}, False))[1]))

        run_id = str(entry.get("last_run_id") or entry.get("run_id") or "").strip()
        dataset_id = str(entry.get("last_dataset_id") or entry.get("dataset_id") or "").strip()
        if self.store is not None and (run_id or dataset_id):
            list_by_remote = getattr(self.store, "list_agent_workers_by_remote_provider_identifiers", None)
            if callable(list_by_remote):
                try:
                    _add_workers(
                        list_by_remote(run_id=run_id, dataset_id=dataset_id, limit=20),
                        remote_identifier_match=True,
                    )
                except Exception:
                    pass
        if str(job_id or "").strip() and self.worker_runtime is not None:
            list_workers = getattr(self.worker_runtime, "list_workers", None)
            if callable(list_workers):
                try:
                    _add_workers(
                        list_workers(job_id=str(job_id), lane_id="enrichment_specialist"),
                        remote_identifier_match=False,
                    )
                except Exception:
                    pass
        if str(job_id or "").strip() and self.store is not None:
            list_workers = getattr(self.store, "list_agent_workers", None)
            if callable(list_workers):
                try:
                    _add_workers(
                        list_workers(job_id=str(job_id), lane_id="enrichment_specialist"),
                        remote_identifier_match=False,
                    )
                except Exception:
                    pass

        for worker, remote_identifier_match in workers_by_id.values():
            if self._harvest_profile_batch_worker_is_active_for_url(
                worker,
                profile_url=profile_url,
                snapshot_dir=snapshot_dir,
                allow_remote_identifier_match_without_url=remote_identifier_match,
            ):
                return True
        if inspected_worker_state:
            return False
        # If this process cannot inspect worker state, preserve the registry's
        # conservative duplicate-prevention semantics instead of double-submitting.
        return True

    @staticmethod
    def _queued_profile_registry_entry_has_provider_owner(registry_entry: dict[str, Any]) -> bool:
        entry = dict(registry_entry or {})
        try:
            if int(entry.get("refill_owner_worker_id") or 0) > 0:
                return True
        except (TypeError, ValueError):
            pass
        for field_name in (
            "refill_owner_run_id",
            "refill_owner_dataset_id",
            "refill_owner_payload_hash",
            "last_run_id",
            "last_dataset_id",
            "run_id",
            "dataset_id",
        ):
            if str(entry.get(field_name) or "").strip():
                return True
        return False

    def _append_already_queued_profile_url(
        self,
        profile_url: str,
        already_queued_urls: list[str],
        *,
        source_shards_by_url: dict[str, list[str]] | dict[str, set[str]] | None,
        normalized_source_jobs: list[str],
    ) -> None:
        if profile_url not in already_queued_urls:
            already_queued_urls.append(profile_url)
        registry_repo = linkedin_profile_registry_repo(self.store)
        upsert_sources = getattr(registry_repo, "upsert_sources", None)
        if callable(upsert_sources):
            source_shards = list(dict(source_shards_by_url or {}).get(profile_url) or [])
            upsert_sources(
                profile_url,
                source_shards=source_shards,
                source_jobs=normalized_source_jobs,
            )

    def _profile_refill_retry_gate(
        self,
        *,
        job_id: str,
        snapshot_dir: Path,
        normal_ready_item_count: int,
    ) -> dict[str, Any]:
        """Return whether retry-only profile refill may start for a job snapshot.

        Retry is a separate wave. It may start only after the normal wave is
        closed: no current-event/registry normal items and no first-attempt
        provider-owned URL still waiting for item-level terminal state.
        """

        normalized_job_id = str(job_id or "").strip()
        gate = {
            "gate_policy": "retry_after_normal_profile_wave_closure",
            "retry_allowed": False,
            "normal_wave_quiescent": False,
            "normal_wave_closed": False,
            "normal_ready_item_count": max(0, int(normal_ready_item_count or 0)),
            "normal_open_item_count": 0,
            "normal_open_state_counts": {},
            "normal_open_urls": [],
            "normal_owned_inflight_item_count": 0,
            "normal_owned_inflight_urls": [],
            "normal_owned_stale_item_count": 0,
            "inspected_owned_item_count": 0,
        }
        if self.store is None or not normalized_job_id:
            gate["reason"] = "retry_gate_scope_unavailable"
            return gate
        registry_repo = linkedin_profile_registry_repo(self.store)
        list_refill_items = getattr(registry_repo, "list_refill_queue_items", None)
        if not callable(list_refill_items):
            gate["reason"] = "refill_queue_selector_unavailable"
            return gate
        normal_open_items: list[dict[str, Any]] = []
        try:
            normal_open_items = list(
                list_refill_items(
                    states=list(PROFILE_REFILL_NORMAL_QUEUE_STATES),
                    source_job=normalized_job_id,
                    snapshot_dir=str(snapshot_dir),
                    limit=1000,
                    ready_only=False,
                )
                or []
            )
        except Exception as exc:
            gate["reason"] = "refill_queue_normal_selector_failed"
            gate["error"] = str(exc)
            return gate
        try:
            owned_items = list(
                list_refill_items(
                    states=[PROFILE_REFILL_PROVIDER_OWNED_QUEUE_STATE],
                    source_job=normalized_job_id,
                    snapshot_dir=str(snapshot_dir),
                    limit=500,
                    ready_only=False,
                )
                or []
            )
        except Exception as exc:
            gate["reason"] = "refill_queue_owned_selector_failed"
            gate["error"] = str(exc)
            return gate

        normal_open_state_counts: dict[str, int] = {}
        normal_open_urls: list[str] = []
        for item in normal_open_items:
            entry = dict(item or {})
            refill_state = str(entry.get("refill_queue_state") or "").strip()
            if not refill_state:
                continue
            normal_open_state_counts[refill_state] = int(normal_open_state_counts.get(refill_state) or 0) + 1
            profile_url = str(entry.get("profile_url") or "").strip()
            if profile_url and len(normal_open_urls) < 20:
                normal_open_urls.append(profile_url)
        gate["normal_open_item_count"] = sum(normal_open_state_counts.values())
        gate["normal_open_state_counts"] = normal_open_state_counts
        gate["normal_open_urls"] = normal_open_urls
        if gate["normal_open_item_count"] > 0:
            gate["reason"] = "normal_wave_open_items_pending"
            return gate
        if gate["normal_ready_item_count"] > 0:
            gate["reason"] = "normal_ready_items_pending"
            return gate

        pending_urls: list[str] = []
        stale_count = 0
        inspected_count = 0
        for item in owned_items:
            entry = dict(item or {})
            trigger_kind = str(entry.get("last_refill_trigger_kind") or "").strip()
            plan_reason = str(entry.get("last_refill_plan_reason") or "").strip()
            if {
                trigger_kind,
                plan_reason,
            }.intersection(PROFILE_REFILL_RETRY_PROVIDER_SUBMIT_MARKERS):
                # Retry-owned provider runs do not block dispatching additional
                # retry_wait items after the normal wave has closed.
                continue
            profile_url = str(entry.get("profile_url") or "").strip()
            if not profile_url:
                continue
            inspected_count += 1
            if self._queued_profile_registry_entry_has_active_worker(
                profile_url,
                entry,
                job_id=normalized_job_id,
                snapshot_dir=snapshot_dir,
            ):
                pending_urls.append(profile_url)
            else:
                stale_count += 1

        gate["inspected_owned_item_count"] = inspected_count
        gate["normal_owned_inflight_item_count"] = len(pending_urls)
        gate["normal_owned_inflight_urls"] = pending_urls[:20]
        gate["normal_owned_stale_item_count"] = stale_count
        if pending_urls:
            gate["reason"] = "normal_provider_owned_items_pending"
            return gate
        if stale_count > 0:
            gate["reason"] = "normal_provider_owned_items_unresolved"
            return gate
        gate["retry_allowed"] = True
        gate["normal_wave_quiescent"] = True
        gate["normal_wave_closed"] = True
        gate["reason"] = "normal_profile_wave_quiescent"
        return gate

    def _record_stale_queued_profile_reclaimed(
        self,
        profile_url: str,
        registry_entry: dict[str, Any],
        *,
        job_id: str,
    ) -> None:
        if self.store is None:
            return
        registry_repo = linkedin_profile_registry_repo(self.store)
        record_event = getattr(registry_repo, "record_event", None)
        if not callable(record_event):
            return
        record_event(
            profile_url,
            event_type="stale_queued_registry_reclaimed",
            event_status=str(registry_entry.get("status") or "queued"),
            detail="queued registry entry has no active harvest profile worker",
            metadata={
                "job_id": str(job_id or "").strip(),
                "last_run_id": str(registry_entry.get("last_run_id") or ""),
                "last_dataset_id": str(registry_entry.get("last_dataset_id") or ""),
                "last_snapshot_dir": str(registry_entry.get("last_snapshot_dir") or ""),
            },
        )

    def _profile_urls_have_cached_harvest_payloads(self, profile_urls: list[str], snapshot_dir: Path) -> bool:
        normalized_urls = [
            str(profile_url or "").strip() for profile_url in list(profile_urls or []) if str(profile_url or "").strip()
        ]
        if not normalized_urls:
            return True
        registry_entries: dict[str, dict[str, Any]] = {}
        if self.store is not None:
            registry_entries = self.store.repos.linkedin_profile_registry.get_bulk(normalized_urls)
        for profile_url in normalized_urls:
            registry_key = normalize_linkedin_profile_url_key(profile_url)
            registry_entry = dict(registry_entries.get(registry_key) or {})
            cached = _load_harvest_profile_payload_from_registry_or_snapshot(
                registry_entry=registry_entry,
                snapshot_dir=snapshot_dir,
                profile_url=profile_url,
                normalized_profile_key=registry_key,
            )
            if cached is None:
                return False
        return True

    def _harvest_profile_prefetch_new_worker_budget(
        self,
        *,
        job_id: str,
        snapshot_dir: Path,
        runtime_tuning_context: dict[str, Any] | None,
        default_submit_budget: int = 1,
    ) -> dict[str, int]:
        actor_budget = resolved_harvest_profile_actor_global_inflight(runtime_tuning_context)
        submit_budget = resolved_harvest_profile_batch_submit_global_inflight(
            runtime_tuning_context,
            default=max(int(default_submit_budget or 1), int(actor_budget or 1)),
        )
        active_count = self._active_harvest_profile_batch_worker_count(job_id=job_id, snapshot_dir=snapshot_dir)
        reserved_worker_count = self._profile_prefetch_reserved_or_owned_worker_count(
            job_id=job_id,
            snapshot_dir=snapshot_dir,
        )
        effective_active_count = max(active_count, reserved_worker_count)
        effective_budget = min(max(1, int(submit_budget or 1)), max(1, int(actor_budget or 1)))
        return {
            "submit_budget": submit_budget,
            "actor_budget": actor_budget,
            "active_worker_count": active_count,
            "scheduler_reserved_worker_count": reserved_worker_count,
            "effective_active_worker_count": effective_active_count,
            "available_new_worker_count": max(0, effective_budget - effective_active_count),
        }

    @staticmethod
    def _split_prefetch_dispatch_specs(
        dispatch_specs: list[tuple[int, list[str]]],
        *,
        available_new_worker_count: int,
    ) -> tuple[list[tuple[int, list[str]]], list[str]]:
        return _split_profile_prefetch_dispatch_specs(
            dispatch_specs,
            available_new_worker_count=available_new_worker_count,
        )

    def _schedule_local_provider_event_watcher(
        self,
        *,
        run_id: str,
        dataset_id: str,
        worker_id: int,
        job_id: str,
        payload_hash: str,
        snapshot_dir: Path,
        runtime_timing_overrides: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return {"status": "skipped", "reason": "run_id_missing"}
        provider_mode = _external_provider_mode()
        if provider_mode == "scripted" and not _env_bool("SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED", False):
            return {"status": "skipped", "reason": "scripted_local_provider_event_watch_disabled"}
        webhook_configured = _provider_webhook_url_configured()
        if webhook_configured and not _local_provider_event_watch_with_webhook_enabled():
            return {"status": "skipped", "reason": "provider_webhook_configured"}
        if not _env_bool("SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED", True):
            return {"status": "skipped", "reason": "local_provider_event_watch_disabled"}
        if self.harvest_profile_connector is None or not callable(
            getattr(self.harvest_profile_connector, "get_actor_run_status", None)
        ):
            return {"status": "skipped", "reason": "run_status_probe_unavailable"}
        callback = self.remote_provider_event_callback
        if not callable(callback):
            return {"status": "skipped", "reason": "remote_provider_event_callback_missing"}
        max_watch_seconds = _local_provider_event_watch_max_seconds(runtime_timing_overrides)
        runtime_dir = infer_runtime_dir_from_path(snapshot_dir)
        watch_request_context = {
            **dict(runtime_timing_overrides or {}),
            "provider_mode": provider_mode,
            "external_provider_mode": provider_mode,
        }
        if runtime_dir is not None:
            watch_request_context["runtime_dir"] = str(runtime_dir)
        thread_name = f"harvest-profile-run-watch-{normalized_run_id[:12]}"
        watcher_lease = _local_provider_event_watcher_lease_payload(
            run_id=normalized_run_id,
            dataset_id=dataset_id,
            worker_id=worker_id,
            payload_hash=payload_hash,
            thread_name=thread_name,
            max_watch_seconds=max_watch_seconds,
        )

        def _worker_still_needs_terminal_event() -> bool:
            current_worker = {}
            if self.store is not None:
                get_worker = getattr(self.store, "get_agent_worker", None)
                if callable(get_worker):
                    try:
                        current_worker = dict(get_worker(worker_id=int(worker_id or 0)) or {})
                    except Exception:
                        current_worker = {}
            current_checkpoint = dict(current_worker.get("checkpoint") or {})
            if not current_checkpoint:
                return True
            current_run_id = str(current_checkpoint.get("run_id") or "").strip()
            current_dataset_id = str(current_checkpoint.get("dataset_id") or "").strip()
            if current_run_id and current_run_id != normalized_run_id:
                return False
            if dataset_id and current_dataset_id and current_dataset_id != str(dataset_id or "").strip():
                return False
            return not _local_provider_event_watcher_terminal_marker_present(
                current_checkpoint,
                run_id=normalized_run_id,
                dataset_id=dataset_id,
            )

        def _watch() -> None:
            deadline = time.monotonic() + max_watch_seconds
            last_status = ""
            while time.monotonic() < deadline:
                try:
                    status_payload = dict(
                        self.harvest_profile_connector.get_actor_run_status(
                            normalized_run_id,
                            runtime_timing_overrides=watch_request_context,
                        )
                        or {}
                    )
                except Exception:
                    time.sleep(2.0)
                    continue
                status = str(status_payload.get("status") or "").strip().upper()
                previous_status = last_status
                last_status = status or last_status
                if bool(status_payload.get("is_terminal")):
                    event_type = _apify_event_type_for_run_status(status)
                    if not event_type:
                        return
                    if not _worker_still_needs_terminal_event():
                        return
                    raw_status_payload = dict(status_payload.get("raw") or {})
                    started_at = str(
                        status_payload.get("started_at")
                        or raw_status_payload.get("startedAt")
                        or raw_status_payload.get("started_at")
                        or ""
                    ).strip()
                    finished_at = str(
                        status_payload.get("finished_at")
                        or status_payload.get("remote_completed_at")
                        or raw_status_payload.get("finishedAt")
                        or raw_status_payload.get("finished_at")
                        or raw_status_payload.get("endedAt")
                        or raw_status_payload.get("ended_at")
                        or ""
                    ).strip()
                    event_created_at = str(
                        raw_status_payload.get("eventCreatedAt")
                        or raw_status_payload.get("event_created_at")
                        or finished_at
                    ).strip()
                    _invoke_local_provider_event_callback_serialized(
                        callback,
                        {
                            "provider": "apify",
                            "eventType": event_type,
                            "eventData": {
                                "actorRunId": str(status_payload.get("run_id") or normalized_run_id).strip(),
                                "defaultDatasetId": str(status_payload.get("dataset_id") or dataset_id).strip(),
                                "status": status,
                                **({"startedAt": started_at} if started_at else {}),
                                **({"finishedAt": finished_at} if finished_at else {}),
                                **({"eventCreatedAt": event_created_at} if event_created_at else {}),
                            },
                            "owner_id": f"local-long-poll-{normalized_run_id}",
                            "worker_scan_limit": 100,
                            "total_limit": 3,
                            "explicit_job_followup_rounds": 1,
                            "source": "local_provider_event_watcher",
                            "job_id": str(job_id or "").strip(),
                            "worker_id": int(worker_id or 0),
                            "payload_hash": str(payload_hash or "").strip(),
                        },
                        before_invoke=_worker_still_needs_terminal_event,
                    )
                    return
                time.sleep(1.0 if status and status != previous_status else 2.0)

        thread = threading.Thread(target=_watch, name=thread_name, daemon=True)
        thread.start()
        return {
            "status": "scheduled",
            "mode": "local_long_poll_provider_event",
            "run_id": normalized_run_id,
            "dataset_id": str(dataset_id or "").strip(),
            "worker_id": int(worker_id or 0),
            "max_watch_seconds": max_watch_seconds,
            "thread_name": thread_name,
            "provider_webhook_configured": webhook_configured,
            "watcher_lease": watcher_lease,
        }

    def _partition_already_queued_profile_urls(
        self,
        profile_urls: list[str],
        *,
        snapshot_dir: Path,
        source_shards_by_url: dict[str, list[str]] | dict[str, set[str]] | None = None,
        source_jobs: list[str] | None = None,
        refill_dispatch_profile_urls: set[str] | None = None,
        dispatch_claimed_profile_urls: set[str] | None = None,
        expected_owner_payload_hash: str = "",
    ) -> tuple[list[str], list[str]]:
        normalized_urls = [
            str(profile_url or "").strip() for profile_url in list(profile_urls or []) if str(profile_url or "").strip()
        ]
        if self.store is None or not normalized_urls:
            return normalized_urls, []
        registry_entries = self.store.repos.linkedin_profile_registry.get_bulk(normalized_urls)
        dispatch_urls: list[str] = []
        already_queued_urls: list[str] = []
        normalized_source_jobs = [
            str(item or "").strip() for item in list(source_jobs or []) if str(item or "").strip()
        ]
        refill_dispatch_keys = {
            normalize_linkedin_profile_url_key(profile_url)
            for profile_url in set(refill_dispatch_profile_urls or set())
            if str(profile_url or "").strip()
        }
        dispatch_claimed_keys = {
            normalize_linkedin_profile_url_key(profile_url)
            for profile_url in set(dispatch_claimed_profile_urls or set())
            if str(profile_url or "").strip()
        }
        for profile_url in normalized_urls:
            registry_key = normalize_linkedin_profile_url_key(profile_url)
            registry_entry = dict(registry_entries.get(registry_key) or {})
            registry_status = str(registry_entry.get("status") or "").strip().lower()
            if registry_status in {"fetched", "unrecoverable"}:
                continue
            if registry_status == "queued":
                refill_state = str(registry_entry.get("refill_queue_state") or "").strip()
                if (
                    refill_state == PROFILE_REFILL_PROVIDER_OWNED_QUEUE_STATE
                    and self._refill_registry_entry_matches_scope(
                        registry_entry,
                        job_id=normalized_source_jobs[0] if normalized_source_jobs else "",
                        snapshot_dir=snapshot_dir,
                    )
                    and self._queued_profile_registry_entry_has_provider_owner(registry_entry)
                ):
                    # `planned_dispatch` is provider-owned. Even if the local
                    # worker has already reached a terminal marker and local
                    # apply has not yet recorded fetched state, a later
                    # append/reconcile must not reclaim the URL and submit a
                    # duplicate actor request.
                    self._append_already_queued_profile_url(
                        profile_url,
                        already_queued_urls,
                        source_shards_by_url=source_shards_by_url,
                        normalized_source_jobs=normalized_source_jobs,
                    )
                    continue
                if refill_state in {"dispatch_reserved", "dispatch_claimed"}:
                    source_job = normalized_source_jobs[0] if normalized_source_jobs else ""
                    if not self._refill_registry_entry_authorizes_dispatch(
                        profile_url,
                        registry_entry,
                        job_id=source_job,
                        snapshot_dir=snapshot_dir,
                        refill_dispatch_keys=refill_dispatch_keys,
                        dispatch_claimed_keys=dispatch_claimed_keys,
                        expected_owner_payload_hash=expected_owner_payload_hash,
                    ):
                        self._append_already_queued_profile_url(
                            profile_url,
                            already_queued_urls,
                            source_shards_by_url=source_shards_by_url,
                            normalized_source_jobs=normalized_source_jobs,
                        )
                        continue
                source_job = normalized_source_jobs[0] if normalized_source_jobs else ""
                if self._refill_registry_entry_authorizes_dispatch(
                    profile_url,
                    registry_entry,
                    job_id=source_job,
                    snapshot_dir=snapshot_dir,
                    refill_dispatch_keys=refill_dispatch_keys,
                    dispatch_claimed_keys=dispatch_claimed_keys,
                    expected_owner_payload_hash=expected_owner_payload_hash,
                ):
                    dispatch_urls.append(profile_url)
                    continue
                if self._queued_profile_registry_entry_has_active_worker(
                    profile_url,
                    registry_entry,
                    job_id=source_job,
                    snapshot_dir=snapshot_dir,
                ):
                    self._append_already_queued_profile_url(
                        profile_url,
                        already_queued_urls,
                        source_shards_by_url=source_shards_by_url,
                        normalized_source_jobs=normalized_source_jobs,
                    )
                    continue
                self._record_stale_queued_profile_reclaimed(
                    profile_url,
                    registry_entry,
                    job_id=normalized_source_jobs[0] if normalized_source_jobs else "",
                )
            dispatch_urls.append(profile_url)
        return dispatch_urls, already_queued_urls

    @staticmethod
    def _refill_registry_entry_matches_scope(
        registry_entry: dict[str, Any],
        *,
        job_id: str,
        snapshot_dir: Path,
    ) -> bool:
        entry = dict(registry_entry or {})
        normalized_job_id = str(job_id or "").strip()
        if normalized_job_id:
            source_jobs = {
                str(item or "").strip() for item in list(entry.get("source_jobs") or []) if str(item or "").strip()
            }
            if source_jobs and normalized_job_id not in source_jobs:
                return False
        normalized_snapshot_dir = str(snapshot_dir or "").strip()
        entry_snapshot_dir = str(entry.get("last_snapshot_dir") or "").strip()
        if normalized_snapshot_dir and entry_snapshot_dir and entry_snapshot_dir != normalized_snapshot_dir:
            return False
        return True

    def _refill_registry_entry_authorizes_dispatch(
        self,
        profile_url: str,
        registry_entry: dict[str, Any],
        *,
        job_id: str,
        snapshot_dir: Path,
        refill_dispatch_keys: set[str],
        dispatch_claimed_keys: set[str],
        expected_owner_payload_hash: str = "",
    ) -> bool:
        """Use the durable refill item as the hot-path ownership contract.

        A queued profile row can mean either "remote actor already owns this URL" or
        "the local refill queue owns this URL and is about to submit it." The
        refill queue state disambiguates those cases so dispatch does not fall back
        to O(URLs x workers) worker scans on every refill tick.
        """

        entry = dict(registry_entry or {})
        registry_key = normalize_linkedin_profile_url_key(profile_url)
        if not registry_key:
            return False
        if not refill_dispatch_keys and not dispatch_claimed_keys:
            return False
        refill_state = str(entry.get("refill_queue_state") or "").strip().lower()
        if refill_state not in {
            "deferred_budget",
            "deferred_coalescing",
            "retry_wait",
            "dispatch_reserved",
            "dispatch_claimed",
        }:
            return False
        if not self._refill_registry_entry_matches_scope(entry, job_id=job_id, snapshot_dir=snapshot_dir):
            return False
        if refill_state in {"dispatch_reserved", "dispatch_claimed"}:
            expected_payload_hash = str(expected_owner_payload_hash or "").strip()
            owner_payload_hash = str(entry.get("refill_owner_payload_hash") or "").strip()
            if not expected_payload_hash or not owner_payload_hash:
                return False
            if owner_payload_hash != expected_payload_hash:
                return False
            return registry_key in refill_dispatch_keys or registry_key in dispatch_claimed_keys
        return registry_key in refill_dispatch_keys

    def _profile_prefetch_scheduler_lock(self, *, job_id: str, snapshot_dir: Path) -> Any:
        lock_context = getattr(self.store, "profile_prefetch_scheduler_lock", None)
        if not callable(lock_context):
            return nullcontext()
        return lock_context(source_job=str(job_id or "").strip(), snapshot_dir=str(snapshot_dir))

    def _profile_prefetch_scheduler_lock_required(self) -> bool:
        store = self.store
        if store is None:
            return False
        predicate = getattr(store, "_control_plane_postgres_should_prefer_read", None)
        if callable(predicate):
            try:
                return bool(predicate("linkedin_profile_registry"))
            except Exception:
                return False
        return False

    def enrich(
        self,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        candidates: list[Candidate],
        job_request: JobRequest,
        *,
        asset_logger: AssetLogger | None = None,
        job_id: str = "",
        request_payload: dict[str, Any] | None = None,
        plan_payload: dict[str, Any] | None = None,
        runtime_mode: str = "workflow",
        parallel_exploration_workers: int = 2,
        cost_policy: dict[str, Any] | None = None,
        full_roster_profile_prefetch: bool = False,
        enrichment_scope: str = "full",
    ) -> MultiSourceEnrichmentResult:
        logger = asset_logger or AssetLogger(snapshot_dir)
        effective_cost_policy = dict(cost_policy or {})
        allow_shared_provider_cache = bool(effective_cost_policy.get("allow_shared_provider_cache", True))
        effective_request_payload = dict(request_payload or job_request.to_record() or {})
        normalized_scope = str(enrichment_scope or "full").strip().lower()
        linkedin_stage_enabled = normalized_scope not in {"public_web_stage_2", "public_web_only"}
        public_web_stage_enabled = normalized_scope not in {"linkedin_stage_1", "linkedin_only"}
        candidate_map = _candidate_name_map(candidates)
        evidence: list[EvidenceRecord] = []
        resolved_profiles: list[dict[str, Any]] = []
        unresolved_candidates: list[dict[str, Any]] = []
        artifact_paths: dict[str, str] = {}
        errors: list[str] = []
        queued_harvest_worker_count = 0
        queued_exploration_count = 0
        profile_prefetch_summary: dict[str, Any] = {}

        prioritized = _prioritize_candidates(candidates)
        slug_resolution_limit = min(job_request.slug_resolution_limit, len(prioritized))
        profile_detail_limit = job_request.profile_detail_limit
        profile_fetch_count = 0
        prefetched_harvest_profiles: dict[str, dict[str, Any]] = {}
        background_prefetch_urls: set[str] = set()
        if (
            linkedin_stage_enabled
            and self.harvest_profile_connector is not None
            and slug_resolution_limit > 0
            and profile_detail_limit > 0
        ):
            resolution_prefetch_candidates = prioritized[:slug_resolution_limit]
            background_prefetch_candidates = (
                prioritized if full_roster_profile_prefetch else resolution_prefetch_candidates
            )
            resolution_candidate_ids = {
                str(candidate.candidate_id or "").strip()
                for candidate in resolution_prefetch_candidates
                if str(candidate.candidate_id or "").strip()
            }
            known_profile_urls: list[str] = []
            known_profile_url_set: set[str] = set()
            resolution_prefetch_urls: list[str] = []
            resolution_prefetch_url_set: set[str] = set()
            resolution_candidate_profile_urls: list[str] = []
            resolution_candidate_profile_url_set: set[str] = set()
            for candidate in resolution_prefetch_candidates:
                for profile_url in _candidate_profile_urls(candidate):
                    normalized_profile_url = str(profile_url or "").strip()
                    if not normalized_profile_url or normalized_profile_url in resolution_candidate_profile_url_set:
                        continue
                    resolution_candidate_profile_url_set.add(normalized_profile_url)
                    resolution_candidate_profile_urls.append(normalized_profile_url)
            cached_resolution_prefetch_profiles = self._hydrate_cached_prefetch_profiles(
                resolution_candidate_profile_urls,
                snapshot_dir,
                source_jobs=[job_id] if str(job_id or "").strip() else [],
            )
            if cached_resolution_prefetch_profiles:
                prefetched_harvest_profiles.update(cached_resolution_prefetch_profiles)
            cached_resolution_prefetch_url_set = {
                str(profile_url or "").strip()
                for profile_url in list(cached_resolution_prefetch_profiles.keys())
                if str(profile_url or "").strip()
            }
            worker_prefetch_enabled = (
                self.worker_runtime is not None
                and bool(job_id)
                and bool(getattr(getattr(self.harvest_profile_connector, "settings", None), "enabled", False))
            )

            for candidate in background_prefetch_candidates:
                candidate_id = str(candidate.candidate_id or "").strip()
                candidate_profile_urls = _candidate_profile_urls(candidate)
                for profile_url in candidate_profile_urls:
                    normalized_profile_url = str(profile_url or "").strip()
                    if not normalized_profile_url or normalized_profile_url in known_profile_url_set:
                        continue
                    known_profile_url_set.add(normalized_profile_url)
                    known_profile_urls.append(normalized_profile_url)
                    if candidate_id and candidate_id in resolution_candidate_ids:
                        if (
                            normalized_profile_url not in resolution_prefetch_url_set
                            and normalized_profile_url not in cached_resolution_prefetch_url_set
                        ):
                            resolution_prefetch_url_set.add(normalized_profile_url)
                            resolution_prefetch_urls.append(normalized_profile_url)

            if worker_prefetch_enabled:
                profile_prefetch_summary = self.queue_background_profile_prefetch(
                    candidates=background_prefetch_candidates,
                    snapshot_dir=snapshot_dir,
                    job_id=job_id,
                    request_payload=effective_request_payload,
                    plan_payload=plan_payload or {},
                    runtime_mode=runtime_mode,
                    allow_shared_provider_cache=allow_shared_provider_cache,
                    priority=not full_roster_profile_prefetch,
                    load_cached_profile_payloads=not full_roster_profile_prefetch,
                    nonblocking_submit=bool(full_roster_profile_prefetch),
                    allow_under_target_final_tail_dispatch=bool(full_roster_profile_prefetch),
                )
                queued_harvest_worker_count = max(
                    queued_harvest_worker_count,
                    int(profile_prefetch_summary.get("queued_worker_count") or 0),
                    int(profile_prefetch_summary.get("active_worker_count") or 0),
                )
                background_prefetch_urls.update(
                    str(profile_url or "").strip()
                    for field_name in ("queued_urls", "deferred_urls", "failed_urls")
                    for profile_url in list(profile_prefetch_summary.get(field_name) or [])
                    if str(profile_url or "").strip()
                )
                if str(profile_prefetch_summary.get("status") or "").strip().lower() == "queued":
                    background_prefetch_urls.update(resolution_prefetch_urls)
                    if queued_harvest_worker_count <= 0 and (
                        int(profile_prefetch_summary.get("deferred_url_count") or 0) > 0 or background_prefetch_urls
                    ):
                        queued_harvest_worker_count = 1
                for summary_path_value in list(profile_prefetch_summary.get("summary_paths") or []):
                    summary_path_text = str(summary_path_value or "").strip()
                    if summary_path_text:
                        artifact_paths.setdefault(
                            "harvest_profile_batch_queue",
                            summary_path_text,
                        )
                for error_message in list(profile_prefetch_summary.get("errors") or []):
                    error_text = str(error_message or "").strip()
                    if error_text:
                        errors.append(f"harvest_profile_prefetch:{error_text}")

            if known_profile_urls and worker_prefetch_enabled:
                if (
                    full_roster_profile_prefetch
                    and str(profile_prefetch_summary.get("status") or "").strip().lower() == "queued"
                ):
                    return MultiSourceEnrichmentResult(
                        candidates=sorted(list(candidate_map.values()), key=lambda item: item.display_name),
                        evidence=evidence,
                        artifact_paths=artifact_paths,
                        errors=errors,
                        queued_harvest_worker_count=queued_harvest_worker_count,
                        stop_reason="queued_background_harvest",
                        profile_prefetch=profile_prefetch_summary,
                    )
                if (
                    queued_harvest_worker_count == 0
                    and str(profile_prefetch_summary.get("status") or "").strip().lower() != "queued"
                    and resolution_prefetch_urls
                ):
                    prefetched_harvest_profiles.update(
                        self._hydrate_cached_prefetch_profiles(
                            resolution_prefetch_urls,
                            snapshot_dir,
                            source_jobs=[job_id] if str(job_id or "").strip() else [],
                        )
                    )
                elif not full_roster_profile_prefetch:
                    background_prefetch_urls.update(resolution_prefetch_urls)
        if linkedin_stage_enabled:
            search_inputs: list[Candidate] = []
            for candidate in prioritized[:slug_resolution_limit]:
                verified, profile_fetch_count = self._resolve_candidate_with_known_refs(
                    candidate,
                    identity,
                    snapshot_dir,
                    profile_fetch_count,
                    profile_detail_limit,
                    candidate_map,
                    resolved_profiles,
                    evidence,
                    asset_logger=logger,
                    prefetched_harvest_profiles=prefetched_harvest_profiles,
                    background_prefetch_urls=background_prefetch_urls,
                    allow_shared_provider_cache=allow_shared_provider_cache,
                    source_job_id=job_id,
                )
                if not verified:
                    search_inputs.append(candidate)

            search_results = self.slug_resolver.resolve(search_inputs, identity, snapshot_dir, asset_logger=logger)
            if search_results["summary_path"]:
                artifact_paths["slug_search_summary"] = str(search_results["summary_path"])
            errors.extend(search_results.get("errors", []))

            for item in search_results["results"]:
                candidate = candidate_map.get(item["candidate_key"])
                if candidate is None:
                    continue

                verified = False
                for slug in item["slugs"]:
                    if profile_fetch_count >= profile_detail_limit:
                        break
                    profile_fetch_count += 1
                    profile = self.profile_connector.fetch_profile(slug, snapshot_dir, asset_logger=logger)
                    if profile is None:
                        continue
                    if not _profile_matches_candidate(
                        profile["parsed"], candidate, identity, model_client=self.model_client
                    ):
                        continue

                    merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                        candidate,
                        profile["parsed"],
                        profile["raw_path"],
                        profile["account_id"],
                        slug,
                        identity,
                        model_client=self.model_client,
                        resolution_source="slug_search",
                    )
                    candidate_map[item["candidate_key"]] = merged_candidate
                    resolved_profiles.append(resolved_profile)
                    evidence.extend(profile_evidence)
                    verified = True
                    break

                if not verified:
                    unresolved_candidates.append(
                        {
                            "candidate_id": candidate.candidate_id,
                            "display_name": candidate.display_name,
                            "attempted_slugs": item["slugs"],
                            "query_summaries": item["queries"],
                        }
                    )

        publication_result = {
            "artifact_paths": {},
            "errors": [],
            "matched_candidates": [],
            "lead_candidates": [],
            "scholar_coauthor_prospects": [],
            "evidence": [],
            "publication_matches": [],
            "coauthor_edges": [],
        }
        if public_web_stage_enabled:
            publication_result = self.publication_connector.enrich(
                identity=identity,
                snapshot_dir=snapshot_dir,
                candidates=list(candidate_map.values()),
                asset_logger=logger,
                max_publications=job_request.publication_scan_limit,
                max_leads=job_request.publication_lead_limit,
                request_payload=request_payload or job_request.to_record(),
                plan_payload=plan_payload or {},
                existing_evidence=evidence,
            )
            artifact_paths.update(publication_result["artifact_paths"])
            errors.extend(publication_result.get("errors", []))

            for candidate in publication_result["matched_candidates"]:
                candidate_map[_candidate_key(candidate)] = candidate
            for lead_candidate in publication_result["lead_candidates"]:
                key = _candidate_key(lead_candidate)
                existing = candidate_map.get(key)
                candidate_map[key] = merge_candidate(existing, lead_candidate) if existing else lead_candidate
        scholar_coauthor_prospects = list(publication_result.get("scholar_coauthor_prospects") or [])

        lead_candidates_to_resolve = (
            _prioritize_candidates(publication_result["lead_candidates"]) if linkedin_stage_enabled else []
        )
        remaining_profile_budget = max(profile_detail_limit - profile_fetch_count, 0)
        if linkedin_stage_enabled and remaining_profile_budget > 0 and lead_candidates_to_resolve:
            lead_search_limit = min(remaining_profile_budget, len(lead_candidates_to_resolve))
            lead_search_results = self.slug_resolver.resolve(
                lead_candidates_to_resolve[:lead_search_limit],
                identity,
                snapshot_dir,
                asset_logger=logger,
            )
            errors.extend(lead_search_results.get("errors", []))
            for item in lead_search_results["results"]:
                lead_candidate = candidate_map.get(item["candidate_key"])
                if lead_candidate is None:
                    continue
                verified = False
                for slug in item["slugs"]:
                    if profile_fetch_count >= profile_detail_limit:
                        break
                    profile_fetch_count += 1
                    profile = self.profile_connector.fetch_profile(slug, snapshot_dir, asset_logger=logger)
                    if profile is None or not _profile_matches_candidate(
                        profile["parsed"],
                        lead_candidate,
                        identity,
                        model_client=self.model_client,
                    ):
                        continue
                    merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                        lead_candidate,
                        profile["parsed"],
                        profile["raw_path"],
                        profile["account_id"],
                        slug,
                        identity,
                        model_client=self.model_client,
                        resolution_source="publication_lead_second_pass",
                    )
                    candidate_map[item["candidate_key"]] = merged_candidate
                    resolved_profiles.append(resolved_profile)
                    evidence.extend(profile_evidence)
                    verified = True
                    break
                if not verified:
                    unresolved_candidates.append(
                        {
                            "candidate_id": lead_candidate.candidate_id,
                            "display_name": lead_candidate.display_name,
                            "attempted_slugs": item["slugs"],
                            "query_summaries": item["queries"],
                            "resolution_source": "publication_lead_second_pass",
                        }
                    )

        exploration_targets = (
            _exploration_targets(list(candidate_map.values()), unresolved_candidates)
            if public_web_stage_enabled
            else []
        )
        if public_web_stage_enabled and job_request.exploration_limit > 0 and exploration_targets:
            runtime_timing_overrides = _runtime_timing_overrides_from_request_payload(
                request_payload or job_request.to_record()
            )
            exploration = self.exploratory_enricher.enrich(
                snapshot_dir,
                exploration_targets,
                target_company=identity.canonical_name,
                max_candidates=job_request.exploration_limit,
                asset_logger=logger,
                job_id=job_id,
                request_payload=request_payload or job_request.to_record(),
                plan_payload=plan_payload or {},
                runtime_mode=runtime_mode,
                parallel_workers=resolved_parallel_exploration_workers(
                    runtime_timing_overrides,
                    cost_policy=effective_cost_policy,
                    default=parallel_exploration_workers,
                ),
            )
            artifact_paths.update(exploration.artifact_paths)
            errors.extend(exploration.errors)
            evidence.extend(exploration.evidence)
            for explored_candidate in exploration.candidates:
                candidate_map[_candidate_key(explored_candidate)] = merge_candidate(
                    candidate_map.get(_candidate_key(explored_candidate), explored_candidate),
                    explored_candidate,
                )
            queued_exploration_count += int(getattr(exploration, "queued_candidate_count", 0) or 0)
            post_exploration_candidates = _prioritize_candidates(exploration.candidates)
            for explored_candidate in post_exploration_candidates:
                if profile_fetch_count >= profile_detail_limit:
                    break
                if not linkedin_stage_enabled:
                    break
                if not _should_attempt_known_profile_resolution_after_exploration(explored_candidate, identity):
                    continue
                verified, profile_fetch_count = self._resolve_candidate_with_known_refs(
                    explored_candidate,
                    identity,
                    snapshot_dir,
                    profile_fetch_count,
                    profile_detail_limit,
                    candidate_map,
                    resolved_profiles,
                    evidence,
                    asset_logger=logger,
                    allow_shared_provider_cache=allow_shared_provider_cache,
                    source_job_id=job_id,
                )
                if verified:
                    _drop_unresolved_candidate(unresolved_candidates, explored_candidate.candidate_id)
                elif _candidate_key(explored_candidate) not in {
                    _candidate_key(item) for item in lead_candidates_to_resolve
                }:
                    unresolved_candidates.append(
                        {
                            "candidate_id": explored_candidate.candidate_id,
                            "display_name": explored_candidate.display_name,
                            "attempted_slugs": [],
                            "query_summaries": [],
                            "resolution_source": "exploration_follow_up",
                        }
                    )

        scholar_coauthor_follow_up_limit = max(int(job_request.scholar_coauthor_follow_up_limit or 0), 0)
        if (
            public_web_stage_enabled
            and linkedin_stage_enabled
            and scholar_coauthor_prospects
            and scholar_coauthor_follow_up_limit > 0
        ):
            (
                profile_fetch_count,
                scholar_coauthor_follow_up_summary_path,
                scholar_coauthor_errors,
                scholar_coauthor_queued_count,
            ) = self._follow_up_roster_anchored_scholar_coauthor_prospects(
                prospects=scholar_coauthor_prospects,
                identity=identity,
                snapshot_dir=snapshot_dir,
                candidate_map=candidate_map,
                resolved_profiles=resolved_profiles,
                evidence=evidence,
                profile_fetch_count=profile_fetch_count,
                profile_detail_limit=profile_detail_limit,
                exploration_limit=scholar_coauthor_follow_up_limit,
                asset_logger=logger,
                job_id=job_id,
                request_payload=request_payload or job_request.to_record(),
                plan_payload=plan_payload or {},
                runtime_mode=runtime_mode,
                allow_shared_provider_cache=allow_shared_provider_cache,
            )
            errors.extend(scholar_coauthor_errors)
            if scholar_coauthor_follow_up_summary_path is not None:
                artifact_paths["scholar_coauthor_follow_up"] = str(scholar_coauthor_follow_up_summary_path)
            queued_exploration_count += scholar_coauthor_queued_count

        remaining_profile_budget = max(profile_detail_limit - profile_fetch_count, 0)
        remaining_leads_to_resolve = [
            item
            for item in _prioritize_candidates(
                [candidate_map.get(_candidate_key(candidate), candidate) for candidate in lead_candidates_to_resolve]
            )
            if item.category == "lead"
        ]
        gated_leads_to_resolve = remaining_leads_to_resolve
        if public_web_stage_enabled and remaining_leads_to_resolve:
            gated_leads_to_resolve, publication_lead_gate_summary_path = self._gate_publication_leads_after_exploration(
                lead_candidates=remaining_leads_to_resolve,
                identity=identity,
                snapshot_dir=snapshot_dir,
                candidate_map=candidate_map,
                unresolved_candidates=unresolved_candidates,
                asset_logger=logger,
                allow_targeted_name_search=bool(effective_cost_policy.get("allow_targeted_name_search_api", False)),
            )
            if publication_lead_gate_summary_path is not None:
                artifact_paths["publication_lead_public_web_gate"] = str(publication_lead_gate_summary_path)
        if (
            public_web_stage_enabled
            and linkedin_stage_enabled
            and bool(effective_cost_policy.get("allow_targeted_name_search_api", False))
            and remaining_profile_budget > 0
            and gated_leads_to_resolve
        ):
            _, targeted_resolution_summary_path = self._resolve_publication_leads_with_harvest_search(
                lead_candidates=gated_leads_to_resolve,
                identity=identity,
                snapshot_dir=snapshot_dir,
                remaining_profile_budget=remaining_profile_budget,
                candidate_map=candidate_map,
                resolved_profiles=resolved_profiles,
                unresolved_candidates=unresolved_candidates,
                evidence=evidence,
                asset_logger=logger,
                allow_shared_provider_cache=allow_shared_provider_cache,
                source_job_id=job_id,
            )
            if targeted_resolution_summary_path is not None:
                artifact_paths["publication_lead_targeted_resolution"] = str(targeted_resolution_summary_path)

        evidence.extend(publication_result["evidence"])
        final_candidates = list(candidate_map.values())
        stop_reason = ""
        if queued_exploration_count > 0:
            stop_reason = "queued_background_exploration"
        elif queued_harvest_worker_count > 0:
            stop_reason = "queued_background_harvest"
        return MultiSourceEnrichmentResult(
            candidates=sorted(final_candidates, key=lambda item: item.display_name),
            evidence=evidence,
            resolved_profiles=resolved_profiles,
            unresolved_candidates=unresolved_candidates,
            publication_matches=publication_result["publication_matches"],
            lead_candidates=publication_result["lead_candidates"],
            coauthor_edges=publication_result["coauthor_edges"],
            artifact_paths=artifact_paths,
            errors=errors,
            queued_harvest_worker_count=queued_harvest_worker_count,
            queued_exploration_count=queued_exploration_count,
            stop_reason=stop_reason,
            profile_prefetch=profile_prefetch_summary,
        )

    def _hydrate_cached_prefetch_profiles(
        self,
        profile_urls: list[str],
        snapshot_dir: Path,
        *,
        source_shards_by_url: dict[str, list[str]] | None = None,
        source_jobs: list[str] | None = None,
        load_profile_payloads: bool = True,
    ) -> dict[str, dict[str, Any]]:
        normalized_urls: list[str] = []
        for profile_url in profile_urls:
            normalized_profile_url = str(profile_url or "").strip()
            if normalized_profile_url and normalized_profile_url not in normalized_urls:
                normalized_urls.append(normalized_profile_url)
        if not normalized_urls:
            return {}

        normalized_source_jobs = [
            str(item or "").strip() for item in list(source_jobs or []) if str(item or "").strip()
        ]
        source_shards_by_url = {
            str(profile_url or "").strip(): list(values or [])
            for profile_url, values in dict(source_shards_by_url or {}).items()
            if str(profile_url or "").strip()
        }
        registry_entries: dict[str, dict[str, Any]] = {}
        if self.store is not None:
            registry_entries = self.store.repos.linkedin_profile_registry.get_bulk(normalized_urls)

        cached_profiles: dict[str, dict[str, Any]] = {}
        for profile_url in normalized_urls:
            registry_entry = {}
            registry_key = profile_url
            if self.store is not None:
                registry_key = normalize_linkedin_profile_url_key(profile_url)
                registry_entry = dict(registry_entries.get(registry_key) or {})
            if not load_profile_payloads:
                cached_marker = _profile_registry_cached_marker(registry_entry)
                if cached_marker is not None:
                    cached_profiles[profile_url] = cached_marker
                continue
            cached = _load_harvest_profile_payload_from_registry_or_snapshot(
                registry_entry=registry_entry,
                snapshot_dir=snapshot_dir,
                profile_url=profile_url,
                normalized_profile_key=registry_key,
            )
            if cached is None:
                continue
            cached_profiles[profile_url] = cached
            if self.store is None:
                continue
            alias_metadata = _profile_registry_alias_metadata(profile_url, cached)
            self.store.repos.linkedin_profile_registry.mark_fetched(
                profile_url,
                raw_path=str(cached.get("raw_path") or ""),
                source_shards=list(source_shards_by_url.get(profile_url) or []),
                source_jobs=normalized_source_jobs,
                alias_urls=list(alias_metadata.get("alias_urls") or []),
                raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or profile_url),
                sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                snapshot_dir=str(snapshot_dir),
            )
        return cached_profiles

    def queue_background_profile_prefetch(
        self,
        *,
        candidates: list[Candidate],
        extra_profile_urls: list[str] | None = None,
        snapshot_dir: Path,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool,
        priority: bool = False,
        load_cached_profile_payloads: bool = True,
        submit_provider: bool = True,
        nonblocking_submit: bool = False,
        allow_under_target_final_tail_dispatch: bool | None = None,
        dispatch_worker_limit: int | None = None,
        refill_item_limit: int | None = None,
        execute_profile_refill_submit_commands: bool = True,
    ) -> dict[str, Any]:
        prefetch_started_monotonic = time.perf_counter()
        prefetch_started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if self.harvest_profile_connector is None or self.worker_runtime is None or not job_id:
            return {"status": "skipped", "reason": "worker_prefetch_unavailable"}
        append_trigger_replan = bool(
            candidates or [url for url in list(extra_profile_urls or []) if str(url or "").strip()]
        )

        normalized_urls: list[str] = []
        seen_urls: set[str] = set()
        source_shards_by_url: dict[str, list[str]] = {}
        for candidate in list(candidates or []):
            candidate_source_shards = _profile_registry_sources_for_candidate(candidate)
            for profile_url in _candidate_profile_urls(candidate):
                normalized_profile_url = str(profile_url or "").strip()
                if not normalized_profile_url:
                    continue
                if normalized_profile_url not in seen_urls:
                    seen_urls.add(normalized_profile_url)
                    normalized_urls.append(normalized_profile_url)
                existing_shards = set(source_shards_by_url.get(normalized_profile_url) or [])
                existing_shards.update(candidate_source_shards)
                source_shards_by_url[normalized_profile_url] = sorted(existing_shards)
        for profile_url in list(extra_profile_urls or []):
            normalized_profile_url = str(profile_url or "").strip()
            if not normalized_profile_url:
                continue
            if normalized_profile_url not in seen_urls:
                seen_urls.add(normalized_profile_url)
                normalized_urls.append(normalized_profile_url)
            existing_shards = set(source_shards_by_url.get(normalized_profile_url) or [])
            existing_shards.add("search_seed_raw_profile_url")
            source_shards_by_url[normalized_profile_url] = sorted(existing_shards)
        refill_queue_items: list[dict[str, Any]] = []
        retry_wait_gate: dict[str, Any] = {
            "gate_policy": "retry_after_normal_profile_wave_closure",
            "retry_allowed": False,
            "normal_wave_quiescent": False,
            "normal_wave_closed": False,
            "normal_ready_item_count": 0,
            "normal_open_item_count": 0,
            "reason": "not_evaluated",
        }
        resolved_refill_item_limit = (
            max(1, min(10000, int(refill_item_limit or 0))) if refill_item_limit is not None else 5000
        )
        if self.store is not None and str(job_id or "").strip():
            registry_repo = linkedin_profile_registry_repo(self.store)
            list_refill_items = getattr(registry_repo, "list_refill_queue_items", None)
            if callable(list_refill_items):
                try:
                    if append_trigger_replan:
                        # Append-time replan may ignore deferred_coalescing timers so
                        # near-simultaneous probe shards can form a full actor
                        # envelope, but it must not pull not-yet-expired local
                        # submit reservations back into a duplicate plan.
                        selected_refill_items: list[dict[str, Any]] = []
                        selected_keys: set[str] = set()
                        for states, ready_only in (
                            (["deferred_budget"], True),
                            (["deferred_coalescing"], False),
                            (["dispatch_reserved", "dispatch_claimed"], True),
                        ):
                            for item in list(
                                list_refill_items(
                                    states=states,
                                    source_job=job_id,
                                    snapshot_dir=str(snapshot_dir),
                                    limit=5000 if append_trigger_replan else resolved_refill_item_limit,
                                    ready_only=ready_only,
                                )
                                or []
                            ):
                                payload = dict(item or {})
                                item_key = str(
                                    payload.get("profile_url_key")
                                    or normalize_linkedin_profile_url_key(payload.get("profile_url"))
                                    or ""
                                ).strip()
                                if item_key and item_key in selected_keys:
                                    continue
                                if item_key:
                                    selected_keys.add(item_key)
                                selected_refill_items.append(payload)
                        refill_queue_items = selected_refill_items
                    else:
                        refill_queue_items = list(
                            list_refill_items(
                                states=list(PROFILE_REFILL_NORMAL_QUEUE_STATES),
                                source_job=job_id,
                                snapshot_dir=str(snapshot_dir),
                                limit=resolved_refill_item_limit,
                                ready_only=True,
                            )
                            or []
                        )
                except Exception:
                    refill_queue_items = []
                if not refill_queue_items and not normalized_urls:
                    retry_wait_gate = self._profile_refill_retry_gate(
                        job_id=job_id,
                        snapshot_dir=snapshot_dir,
                        normal_ready_item_count=0,
                    )
                    if bool(retry_wait_gate.get("retry_allowed")):
                        try:
                            refill_queue_items = list(
                                list_refill_items(
                                    states=list(PROFILE_REFILL_RETRY_QUEUE_STATES),
                                    source_job=job_id,
                                    snapshot_dir=str(snapshot_dir),
                                    limit=resolved_refill_item_limit,
                                )
                                or []
                            )
                        except Exception:
                            refill_queue_items = []
                    else:
                        return {
                            "status": "queued",
                            "reason": "retry_wait_blocked_by_normal_profile_wave",
                            "requested_url_count": 0,
                            "refill_queue_item_count": 0,
                            "dispatched_url_count": 0,
                            "cached_profile_count": 0,
                            "queued_worker_count": 0,
                            "retry_wait_gate": retry_wait_gate,
                            "metrics": {
                                "prefetch_started_at": prefetch_started_at,
                                "prefetch_finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                "prefetch_elapsed_ms": int(
                                    max(0.0, (time.perf_counter() - prefetch_started_monotonic) * 1000)
                                ),
                                "candidate_count": len(list(candidates or [])),
                                "extra_profile_url_count": len(
                                    [url for url in list(extra_profile_urls or []) if str(url or "").strip()]
                                ),
                                "requested_url_count": 0,
                                "refill_queue_item_count": 0,
                                "cached_profile_count": 0,
                                "registry_cache_marker_count": 0,
                                "cached_profile_payload_count": 0,
                                "load_cached_profile_payloads": bool(load_cached_profile_payloads),
                                "retry_isolated_refill": False,
                                "retry_wait_gate": retry_wait_gate,
                            },
                        }
        retry_isolated_refill = bool(refill_queue_items) and all(
            str(dict(item or {}).get("refill_queue_state") or "").strip() == "retry_wait" for item in refill_queue_items
        )
        for refill_item in refill_queue_items:
            profile_url = str(dict(refill_item or {}).get("profile_url") or "").strip()
            if not profile_url or profile_url in seen_urls:
                continue
            seen_urls.add(profile_url)
            normalized_urls.append(profile_url)
            existing_shards = set(source_shards_by_url.get(profile_url) or [])
            existing_shards.update(
                str(label or "").strip()
                for label in list(dict(refill_item or {}).get("source_shards") or [])
                if str(label or "").strip()
            )
            existing_shards.add("linkedin_profile_registry_refill")
            source_shards_by_url[profile_url] = sorted(existing_shards)
        refill_queue_profile_urls = {
            str(dict(item or {}).get("profile_url") or "").strip()
            for item in refill_queue_items
            if str(dict(item or {}).get("profile_url") or "").strip()
        }
        dispatch_claimed_profile_urls = {
            str(dict(item or {}).get("profile_url") or "").strip()
            for item in refill_queue_items
            if str(dict(item or {}).get("profile_url") or "").strip()
            and str(dict(item or {}).get("refill_queue_state") or "").strip() == "dispatch_claimed"
        }
        resolved_dispatch_worker_limit = (
            max(0, _safe_int_value(dispatch_worker_limit)) if dispatch_worker_limit is not None else 0
        )
        if not normalized_urls:
            return {"status": "skipped", "reason": "no_profile_urls"}

        cached_profiles = self._hydrate_cached_prefetch_profiles(
            normalized_urls,
            snapshot_dir,
            source_shards_by_url=source_shards_by_url,
            source_jobs=[job_id],
            load_profile_payloads=load_cached_profile_payloads,
        )
        registry_cache_marker_count = sum(
            1
            for cached_profile in cached_profiles.values()
            if bool(dict(cached_profile or {}).get("profile_registry_marker"))
        )

        def _with_prefetch_metrics(payload: dict[str, Any]) -> dict[str, Any]:
            elapsed_ms = int(max(0.0, (time.perf_counter() - prefetch_started_monotonic) * 1000))
            metrics = {
                "prefetch_started_at": prefetch_started_at,
                "prefetch_finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "prefetch_elapsed_ms": elapsed_ms,
                "candidate_count": len(list(candidates or [])),
                "extra_profile_url_count": len(
                    [url for url in list(extra_profile_urls or []) if str(url or "").strip()]
                ),
                "requested_url_count": len(normalized_urls),
                "refill_queue_item_count": len(refill_queue_items),
                "cached_profile_count": len(cached_profiles),
                "registry_cache_marker_count": registry_cache_marker_count,
                "cached_profile_payload_count": max(0, len(cached_profiles) - registry_cache_marker_count),
                "load_cached_profile_payloads": bool(load_cached_profile_payloads),
                "retry_isolated_refill": retry_isolated_refill,
                "retry_wait_gate": retry_wait_gate,
                "dispatch_worker_limit": resolved_dispatch_worker_limit,
                "refill_item_limit": resolved_refill_item_limit,
            }
            existing_metrics = dict(payload.get("metrics") or {})
            payload["metrics"] = {**metrics, **existing_metrics}
            payload.setdefault("prefetch_elapsed_ms", elapsed_ms)
            payload.setdefault("registry_cache_marker_count", registry_cache_marker_count)
            payload.setdefault("retry_wait_gate", retry_wait_gate)
            return payload

        def _current_prefetch_registry_entries() -> dict[str, dict[str, Any]]:
            if self.store is None:
                return {}
            try:
                return dict(self.store.repos.linkedin_profile_registry.get_bulk(normalized_urls) or {})
            except Exception:
                return {}

        def _with_prefetch_queue(
            payload: dict[str, Any],
            *,
            ready_profile_urls: list[str] | None = None,
            newly_queued_profile_urls: list[str] | None = None,
            already_queued_profile_urls: list[str] | None = None,
            deferred_profile_urls: list[str] | None = None,
            failed_profile_urls: list[str] | None = None,
            batch_envelopes: list[dict[str, Any]] | None = None,
            worker_budget: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            budget_payload = dict(worker_budget or {})
            registry_entries = _current_prefetch_registry_entries()
            terminal_summary = _profile_prefetch_requested_registry_terminal_summary(
                normalized_urls,
                registry_entries,
            )
            if bool(terminal_summary.get("all_requested_terminal")):
                payload = dict(payload)
                existing_status = str(payload.get("status") or "").strip().lower()
                existing_reason = str(payload.get("reason") or "").strip()
                payload["status"] = "completed"
                if existing_status != "completed" or not existing_reason:
                    payload["reason"] = (
                        "registry_all_requested_profiles_terminal"
                        if int(terminal_summary.get("unrecoverable_url_count") or 0) > 0
                        else "registry_all_requested_profiles_fetched"
                    )
                payload["terminal_proof_only"] = True
                payload["submit_anchor"] = False
                payload["dispatched_url_count"] = 0
                payload["queued_worker_count"] = 0
                payload["active_worker_count"] = 0
                payload["deferred_url_count"] = 0
                payload["queued_urls"] = []
                payload["deferred_urls"] = []
                payload["failed_urls"] = []
                payload["summary_paths"] = []
                payload["batch_envelopes"] = []
                payload["batch_plans"] = []
                payload["profile_prefetch_batch_plans"] = []
                payload["batch_plan"] = {}
                payload["latest_batch_plan"] = {}
                payload["profile_prefetch_batch_plan"] = {}
                payload["tiny_batch_count"] = 0
                payload["unexplained_tiny_batch_count"] = 0
                payload["provider_slot_underuse_with_backlog_count"] = 0
                payload["ordinal_gate_triggered"] = False
                payload["ordinal_gate_deferred_url_count"] = 0
                payload["ordinal_gate_deferred_urls"] = []
                ready_profile_urls = []
                newly_queued_profile_urls = []
                already_queued_profile_urls = []
                deferred_profile_urls = []
                failed_profile_urls = []
            queue_snapshot = _build_profile_prefetch_queue_snapshot(
                requested_profile_urls=normalized_urls,
                cached_profile_urls=list(cached_profiles.keys()),
                ready_profile_urls=ready_profile_urls or [],
                newly_queued_profile_urls=newly_queued_profile_urls or [],
                already_queued_profile_urls=already_queued_profile_urls or [],
                deferred_profile_urls=deferred_profile_urls or [],
                failed_profile_urls=failed_profile_urls or [],
                registry_entries=registry_entries,
                active_worker_count=int(
                    payload.get("active_worker_count") or budget_payload.get("active_worker_count") or 0
                ),
                queued_worker_count=int(payload.get("queued_worker_count") or 0),
                actor_budget=int(payload.get("actor_budget") or budget_payload.get("actor_budget") or 0),
                submit_budget=int(payload.get("submit_budget") or budget_payload.get("submit_budget") or 0),
                available_new_worker_count=int(budget_payload.get("available_new_worker_count") or 0),
                scheduler_reserved_worker_count=int(budget_payload.get("scheduler_reserved_worker_count") or 0),
                effective_active_worker_count=int(budget_payload.get("effective_active_worker_count") or 0),
                batch_envelopes=batch_envelopes or list(payload.get("batch_envelopes") or []),
                status=str(payload.get("status") or ""),
                reason=str(payload.get("reason") or ""),
            )
            payload["profile_prefetch_queue"] = queue_snapshot
            payload["registry_terminal_summary"] = terminal_summary
            return _with_prefetch_metrics(payload)

        dispatch_urls = [
            profile_url
            for profile_url in normalized_urls
            if str(profile_url or "").strip() and str(profile_url or "").strip() not in cached_profiles
        ]
        dispatch_urls, already_queued_urls = self._partition_already_queued_profile_urls(
            dispatch_urls,
            snapshot_dir=snapshot_dir,
            source_shards_by_url=source_shards_by_url,
            source_jobs=[job_id],
            refill_dispatch_profile_urls=refill_queue_profile_urls,
            dispatch_claimed_profile_urls=dispatch_claimed_profile_urls,
        )
        if not dispatch_urls:
            if already_queued_urls:
                return _with_prefetch_queue(
                    {
                        "status": "queued",
                        "reason": "reused_active_harvest_profile_queue",
                        "requested_url_count": len(normalized_urls),
                        "refill_queue_item_count": len(refill_queue_items),
                        "dispatched_url_count": 0,
                        "cached_profile_count": len(cached_profiles),
                        "queued_worker_count": 0,
                        "already_queued_url_count": len(already_queued_urls),
                        "queued_urls": already_queued_urls,
                        "summary_paths": [],
                    },
                    already_queued_profile_urls=already_queued_urls,
                )
            return _with_prefetch_queue(
                {
                    "status": "completed",
                    "reason": "reused_local_raw_cache",
                    "requested_url_count": len(normalized_urls),
                    "refill_queue_item_count": len(refill_queue_items),
                    "dispatched_url_count": 0,
                    "cached_profile_count": len(cached_profiles),
                    "queued_worker_count": 0,
                    "summary_paths": [],
                }
            )

        runtime_tuning_context = _runtime_tuning_context_from_request_payload(dict(request_payload or {}))
        requested_candidate_count = (
            len(list(candidates or []))
            + len([url for url in list(extra_profile_urls or []) if str(url or "").strip()])
            + len(refill_queue_items)
        )
        scheduler_lock_evidence: dict[str, Any] = {}
        scheduler_lock_required = self._profile_prefetch_scheduler_lock_required()
        prefetch_dispatch_window: dict[str, Any] = {}
        prefetch_worker_budget: dict[str, Any] = {}
        prefetch_batch_plan: ProfilePrefetchBatchPlan | None = None
        refill_plan_items: dict[str, Any] = {}
        dispatch_source_shards_by_url: dict[str, list[str]] = {}
        revalidated_terminal_urls: list[str] = []
        no_dispatch_after_revalidation = False
        with self._profile_prefetch_scheduler_lock(job_id=job_id, snapshot_dir=snapshot_dir) as lock_evidence:
            scheduler_lock_evidence = dict(lock_evidence or {})
            if bool(scheduler_lock_evidence.get("busy")) or scheduler_lock_evidence.get("acquired") is False:
                lock_kind = str(
                    scheduler_lock_evidence.get("kind")
                    or scheduler_lock_evidence.get("lock_kind")
                    or ("none" if not scheduler_lock_required else "")
                ).strip()
                return _with_prefetch_metrics(
                    {
                        "status": "queued",
                        "reason": "profile_prefetch_scheduler_lock_busy",
                        "requested_url_count": len(normalized_urls),
                        "scheduler_lock": {
                            "required": bool(scheduler_lock_required),
                            "kind": lock_kind,
                            "lock_kind": str(
                                scheduler_lock_evidence.get("lock_kind") or scheduler_lock_evidence.get("kind") or ""
                            ).strip(),
                            "distributed": bool(scheduler_lock_evidence.get("distributed")),
                            "acquired": False,
                            "busy": True,
                            "source": str(scheduler_lock_evidence.get("source") or "").strip(),
                            "reason": str(
                                scheduler_lock_evidence.get("reason") or "profile_prefetch_scheduler_lock_busy"
                            ).strip(),
                            "scope": str(scheduler_lock_evidence.get("scope") or "").strip(),
                        },
                        "refill_queue_item_count": len(refill_queue_items),
                        "dispatched_url_count": 0,
                        "cached_profile_count": len(cached_profiles),
                        "queued_worker_count": 0,
                        "deferred_url_count": len(dispatch_urls),
                        "summary_paths": [],
                    }
                )
            latest_cached_markers = self._hydrate_cached_prefetch_profiles(
                dispatch_urls,
                snapshot_dir,
                source_shards_by_url=source_shards_by_url,
                source_jobs=[job_id],
                load_profile_payloads=False,
            )
            if latest_cached_markers:
                cached_profiles.update(latest_cached_markers)
                latest_cached_urls = {
                    str(profile_url or "").strip()
                    for profile_url in latest_cached_markers
                    if str(profile_url or "").strip()
                }
                dispatch_urls = [
                    profile_url
                    for profile_url in dispatch_urls
                    if str(profile_url or "").strip() not in latest_cached_urls
                ]
            latest_registry_entries = _current_prefetch_registry_entries()
            for profile_url in list(dispatch_urls):
                registry_key = normalize_linkedin_profile_url_key(profile_url)
                registry_entry = dict(latest_registry_entries.get(registry_key) or {})
                registry_status = str(registry_entry.get("status") or "").strip().lower()
                if registry_status in {"fetched", "unrecoverable"}:
                    revalidated_terminal_urls.append(profile_url)
            if revalidated_terminal_urls:
                terminal_url_set = set(revalidated_terminal_urls)
                dispatch_urls = [profile_url for profile_url in dispatch_urls if profile_url not in terminal_url_set]
            dispatch_urls, revalidated_already_queued_urls = self._partition_already_queued_profile_urls(
                dispatch_urls,
                snapshot_dir=snapshot_dir,
                source_shards_by_url=source_shards_by_url,
                source_jobs=[job_id],
                refill_dispatch_profile_urls=refill_queue_profile_urls,
                dispatch_claimed_profile_urls=dispatch_claimed_profile_urls,
            )
            for profile_url in revalidated_already_queued_urls:
                if profile_url not in already_queued_urls:
                    already_queued_urls.append(profile_url)
            if not dispatch_urls:
                no_dispatch_after_revalidation = True
                refill_plan_items = {
                    "status": "skipped",
                    "reason": "no_dispatch_after_lock_revalidation",
                    "revalidated_terminal_url_count": len(revalidated_terminal_urls),
                    "already_queued_url_count": len(already_queued_urls),
                }
            dispatch_source_shards_by_url = {
                profile_url: list(source_shards_by_url.get(profile_url) or []) for profile_url in dispatch_urls
            }
            resolved_allow_under_target_final_tail_dispatch = (
                not append_trigger_replan
                if allow_under_target_final_tail_dispatch is None
                else bool(allow_under_target_final_tail_dispatch)
            )
            if no_dispatch_after_revalidation:
                prefetch_worker_budget = {
                    "active_worker_count": 0,
                    "actor_budget": 0,
                    "submit_budget": 0,
                    "available_new_worker_count": 0,
                }
            else:
                latest_registry_entries = _current_prefetch_registry_entries()
                prefetch_dispatch_window = _recommended_harvest_profile_prefetch_dispatch_window(
                    len(dispatch_urls),
                    priority=priority,
                    source_shards_by_url=dispatch_source_shards_by_url,
                )
                prefetch_dispatch_window = _apply_durable_refill_wave_dispatch_window(
                    prefetch_dispatch_window,
                    dispatch_urls=dispatch_urls,
                    registry_entries=latest_registry_entries,
                    append_trigger_replan=append_trigger_replan,
                    retry_isolated_refill=retry_isolated_refill,
                )
                prefetch_worker_budget = self._harvest_profile_prefetch_new_worker_budget(
                    job_id=job_id,
                    snapshot_dir=snapshot_dir,
                    runtime_tuning_context=runtime_tuning_context,
                    default_submit_budget=int(prefetch_dispatch_window.get("max_workers") or 1),
                )
                if dispatch_worker_limit is not None:
                    prefetch_worker_budget["dispatch_worker_limit"] = resolved_dispatch_worker_limit
                    prefetch_worker_budget["available_new_worker_count"] = min(
                        max(0, int(prefetch_worker_budget.get("available_new_worker_count") or 0)),
                        resolved_dispatch_worker_limit,
                    )
                dispatch_queue_items = _build_profile_prefetch_queue_items(
                    dispatch_urls,
                    source_shards_by_url=dispatch_source_shards_by_url,
                    source_jobs=[job_id],
                    priority=priority,
                    queue_state="retry_wait" if retry_isolated_refill else "ready",
                    registry_entries=latest_registry_entries,
                )
                prefetch_batch_plan = _build_profile_prefetch_batch_plan(
                    dispatch_urls=dispatch_urls,
                    requested_url_count=len(normalized_urls),
                    candidate_count=requested_candidate_count,
                    priority=priority,
                    source_shards_by_url=dispatch_source_shards_by_url,
                    worker_budget=prefetch_worker_budget,
                    dispatch_window=prefetch_dispatch_window,
                    queue_items=dispatch_queue_items,
                    allow_under_target_final_tail_dispatch=resolved_allow_under_target_final_tail_dispatch,
                )
                refill_plan_items = _record_profile_prefetch_batch_plan_items(
                    self.store,
                    prefetch_batch_plan,
                    source_shards_by_url=dispatch_source_shards_by_url,
                    source_jobs=[job_id],
                    snapshot_dir=snapshot_dir,
                    trigger_kind="profile_prefetch_replan",
                    record_active_items=bool(submit_provider),
                    active_queue_state="dispatch_reserved",
                    active_reason="scheduler_dispatch_reserved",
                )
        scheduler_lock_summary = {
            "required": bool(scheduler_lock_required),
            "kind": str(
                scheduler_lock_evidence.get("kind")
                or scheduler_lock_evidence.get("lock_kind")
                or ("none" if not scheduler_lock_required else "")
            ).strip(),
            "lock_kind": str(
                scheduler_lock_evidence.get("lock_kind") or scheduler_lock_evidence.get("kind") or ""
            ).strip(),
            "distributed": bool(scheduler_lock_evidence.get("distributed")),
            "acquired": scheduler_lock_evidence.get("acquired") is not False,
            "busy": bool(scheduler_lock_evidence.get("busy")),
            "source": str(scheduler_lock_evidence.get("source") or "").strip(),
        }
        if no_dispatch_after_revalidation:
            if already_queued_urls:
                return _with_prefetch_queue(
                    {
                        "status": "queued",
                        "reason": "reused_active_harvest_profile_queue_after_lock_revalidation",
                        "requested_url_count": len(normalized_urls),
                        "scheduler_lock": scheduler_lock_summary,
                        "refill_queue_item_count": len(refill_queue_items),
                        "dispatched_url_count": 0,
                        "cached_profile_count": len(cached_profiles),
                        "queued_worker_count": 0,
                        "already_queued_url_count": len(already_queued_urls),
                        "queued_urls": already_queued_urls,
                        "summary_paths": [],
                        "refill_plan_items": refill_plan_items,
                    },
                    already_queued_profile_urls=already_queued_urls,
                    worker_budget=prefetch_worker_budget,
                )
            return _with_prefetch_queue(
                {
                    "status": "completed",
                    "reason": "registry_terminal_after_lock_revalidation",
                    "requested_url_count": len(normalized_urls),
                    "scheduler_lock": scheduler_lock_summary,
                    "refill_queue_item_count": len(refill_queue_items),
                    "dispatched_url_count": 0,
                    "cached_profile_count": len(cached_profiles),
                    "queued_worker_count": 0,
                    "summary_paths": [],
                    "refill_plan_items": refill_plan_items,
                },
                worker_budget=prefetch_worker_budget,
            )
        assert prefetch_batch_plan is not None
        prefetch_dispatch_window = prefetch_batch_plan.dispatch_window
        prefetch_worker_budget = prefetch_batch_plan.worker_budget
        dispatch_specs = prefetch_batch_plan.dispatch_specs
        deferred_urls = list(prefetch_batch_plan.deferred_urls)
        tiny_batch_coalesced_count = prefetch_batch_plan.tiny_batch_coalesced_count
        if prefetch_batch_plan.plan_reason == "no_dispatch_chunks":
            return _with_prefetch_queue(
                {
                    "status": "skipped",
                    "reason": "no_dispatch_chunks",
                    "requested_url_count": len(normalized_urls),
                    "scheduler_lock": scheduler_lock_summary,
                    "refill_queue_item_count": len(refill_queue_items),
                    "dispatched_url_count": 0,
                    "cached_profile_count": len(cached_profiles),
                    "queued_worker_count": 0,
                    "summary_paths": [],
                    "batch_plan_reason": prefetch_batch_plan.plan_reason,
                    "batch_plan": prefetch_batch_plan.to_record(),
                    "refill_plan_items": refill_plan_items,
                }
            )

        if not dispatch_specs:
            return _with_prefetch_queue(
                {
                    "status": "queued",
                    "reason": "harvest_profile_prefetch_backpressure",
                    "requested_url_count": len(normalized_urls),
                    "scheduler_lock": scheduler_lock_summary,
                    "refill_queue_item_count": len(refill_queue_items),
                    "dispatched_url_count": 0,
                    "cached_profile_count": len(cached_profiles),
                    "queued_worker_count": 0,
                    "active_worker_count": int(prefetch_worker_budget.get("active_worker_count") or 0),
                    "submit_budget": int(prefetch_worker_budget.get("submit_budget") or 1),
                    "actor_budget": int(prefetch_worker_budget.get("actor_budget") or 1),
                    "recommended_batch_size": int(prefetch_dispatch_window.get("batch_size") or 1),
                    "recommended_batch_count": int(prefetch_dispatch_window.get("batch_count") or 0),
                    "recommended_max_workers": int(prefetch_dispatch_window.get("max_workers") or 1),
                    "dispatch_strategy": str(prefetch_dispatch_window.get("strategy") or ""),
                    "batch_envelopes": [],
                    "tiny_batch_count": 0,
                    "unexplained_tiny_batch_count": 0,
                    "provider_slot_underuse_with_backlog_count": 0,
                    "tiny_batch_coalesced_count": tiny_batch_coalesced_count,
                    "deferred_url_count": len(deferred_urls),
                    "deferred_urls": deferred_urls,
                    "batch_plan_reason": prefetch_batch_plan.plan_reason,
                    "batch_plan": prefetch_batch_plan.to_record(),
                    "refill_plan_items": refill_plan_items,
                    "summary_paths": [],
                },
                ready_profile_urls=dispatch_urls,
                already_queued_profile_urls=already_queued_urls,
                deferred_profile_urls=deferred_urls,
                worker_budget=prefetch_worker_budget,
            )
        if not bool(submit_provider):
            dispatch_deferred_urls: list[str] = []
            for _, profile_url_chunk in list(dispatch_specs or []):
                for profile_url in list(profile_url_chunk or []):
                    normalized_profile_url = str(profile_url or "").strip()
                    if normalized_profile_url and normalized_profile_url not in dispatch_deferred_urls:
                        dispatch_deferred_urls.append(normalized_profile_url)
            if self.store is not None and dispatch_deferred_urls:
                _mark_profile_prefetch_urls_deferred_for_dispatch_retry(
                    self.store,
                    dispatch_deferred_urls,
                    source_shards_by_url=dispatch_source_shards_by_url,
                    source_jobs=[job_id] if str(job_id or "").strip() else [],
                    snapshot_dir=snapshot_dir,
                    trigger_kind="profile_prefetch_callback_deferred_submit",
                    plan_reason="provider_submit_deferred_to_refill_daemon",
                    deferred_reason="callback_must_not_submit_provider",
                )
            callback_deferred_urls = list(deferred_urls)
            for profile_url in dispatch_deferred_urls:
                if profile_url not in callback_deferred_urls:
                    callback_deferred_urls.append(profile_url)
            return _with_prefetch_queue(
                {
                    "status": "queued",
                    "reason": "provider_submit_deferred_to_refill_daemon",
                    "requested_url_count": len(normalized_urls),
                    "scheduler_lock": scheduler_lock_summary,
                    "refill_queue_item_count": len(refill_queue_items),
                    "dispatched_url_count": 0,
                    "cached_profile_count": len(cached_profiles),
                    "queued_worker_count": 0,
                    "active_worker_count": int(prefetch_worker_budget.get("active_worker_count") or 0),
                    "submit_budget": int(prefetch_worker_budget.get("submit_budget") or 1),
                    "actor_budget": int(prefetch_worker_budget.get("actor_budget") or 1),
                    "recommended_batch_size": int(prefetch_dispatch_window.get("batch_size") or 1),
                    "recommended_batch_count": int(prefetch_dispatch_window.get("batch_count") or 0),
                    "recommended_max_workers": int(prefetch_dispatch_window.get("max_workers") or 1),
                    "dispatch_strategy": str(prefetch_dispatch_window.get("strategy") or ""),
                    "batch_envelopes": [],
                    "tiny_batch_count": 0,
                    "unexplained_tiny_batch_count": 0,
                    "provider_slot_underuse_with_backlog_count": 0,
                    "tiny_batch_coalesced_count": tiny_batch_coalesced_count,
                    "deferred_url_count": len(callback_deferred_urls),
                    "deferred_urls": callback_deferred_urls,
                    "callback_deferred_submit_url_count": len(dispatch_deferred_urls),
                    "batch_plan_reason": prefetch_batch_plan.plan_reason,
                    "batch_plan": prefetch_batch_plan.to_record(),
                    "refill_plan_items": refill_plan_items,
                    "queued_urls": [],
                    "failed_urls": [],
                    "summary_paths": [],
                    "errors": [],
                },
                ready_profile_urls=dispatch_urls,
                already_queued_profile_urls=already_queued_urls,
                deferred_profile_urls=callback_deferred_urls,
                worker_budget=prefetch_worker_budget,
            )
        planned_deferred_url_count = len(deferred_urls)
        planned_tail_coalescing_url_count = len(
            [
                item
                for item in list(prefetch_batch_plan.tail_coalescing_items or [])
                if str(getattr(item, "profile_url", "") or "").strip()
            ]
        )
        planned_dispatch_worker_count = prefetch_batch_plan.planned_dispatch_worker_count
        dispatch_results: list[dict[str, Any]] = []
        provider_submit_elapsed_values_ms: list[int] = []
        provider_submit_started_values: list[str] = []
        provider_submit_finished_values: list[str] = []
        workflow_run_id = legacy_job_workflow_run_id(job_id)
        operation_id = legacy_job_operation_id(job_id)

        def _plan_profile_refill_submit_command(
            *,
            chunk_index: int,
            profile_url_chunk: list[str],
        ) -> dict[str, Any]:
            runtime_writer = self._durable_runtime_writer()
            if runtime_writer is None or not workflow_run_id:
                return {}
            command_idempotency_key = linkedin_profile_refill_submit_idempotency_key(
                job_id=job_id,
                snapshot_dir=str(snapshot_dir),
                profile_urls=profile_url_chunk,
                submit_scope=("retry_wait" if retry_isolated_refill else str(prefetch_batch_plan.plan_reason or "")),
            )
            if not command_idempotency_key:
                return {}
            runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                event_family="workflow_event",
                event_type="WorkflowStarted",
                idempotency_key=f"{workflow_run_id}:legacy_job_profile_refill_started",
                actor="profile_prefetch_scheduler",
                source="queue_background_profile_prefetch",
                payload={
                    "workflow_type": "linkedin_acquisition",
                    "stage_key": "profile_fetch",
                    "job_id": str(job_id),
                    "snapshot_dir": str(snapshot_dir),
                    "migration_phase": "W2b_profile_refill_submit",
                },
            )
            apply_result = runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{command_idempotency_key}:plan",
                actor="profile_prefetch_scheduler",
                source="queue_background_profile_prefetch",
                payload={
                    "workflow_type": "linkedin_acquisition",
                    "stage_key": "profile_fetch",
                    "command_type": LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
                    "idempotency_key": command_idempotency_key,
                    "payload": {
                        "job_id": str(job_id),
                        "snapshot_dir": str(snapshot_dir),
                        "snapshot_id": snapshot_dir.name,
                        "chunk_index": int(chunk_index),
                        "profile_url_count": len(profile_url_chunk),
                        "profile_urls": list(profile_url_chunk),
                        "requested_url_count": len(normalized_urls),
                        "candidate_count": requested_candidate_count,
                        "planned_deferred_url_count": planned_deferred_url_count,
                        "planned_dispatch_worker_count": planned_dispatch_worker_count,
                        "batch_plan_reason": prefetch_batch_plan.plan_reason,
                        "allow_under_target_final_tail_dispatch": (
                            prefetch_batch_plan.plan_reason == "queue_quiescent_final_tail"
                            or (
                                bool(resolved_allow_under_target_final_tail_dispatch)
                                and len(profile_url_chunk) < _harvest_profile_prefetch_actor_slot_url_target()
                                and planned_deferred_url_count <= 0
                            )
                        ),
                        "retry_isolated_refill": retry_isolated_refill,
                        "submit_scope": (
                            "retry_wait" if retry_isolated_refill else str(prefetch_batch_plan.plan_reason or "")
                        ),
                        "request_payload": dict(request_payload or {}),
                        "plan_payload": dict(plan_payload or {}),
                        "runtime_mode": str(runtime_mode or ""),
                        "allow_shared_provider_cache": bool(allow_shared_provider_cache),
                        "load_cached_profile_payloads": bool(load_cached_profile_payloads),
                        "nonblocking_submit": bool(nonblocking_submit),
                    },
                    "max_attempts": 3,
                    "retry_policy": {
                        "kind": "profile_refill_submit",
                        "retry_delay_seconds": 30,
                    },
                },
            )
            command = dict((apply_result.commands or ({},))[0] or {})
            if not command:
                existing_commands = self.store.list_workflow_commands(
                    workflow_run_id=workflow_run_id,
                    limit=0,
                )
                for existing_command in list(existing_commands or []):
                    if str(existing_command.get("idempotency_key") or "") == command_idempotency_key:
                        command = dict(existing_command or {})
                        break
            if not command:
                return {}
            return {
                "workflow_run_id": workflow_run_id,
                "operation_id": operation_id,
                "command_id": str(command.get("command_id") or ""),
                "command_type": str(command.get("command_type") or ""),
                "owner": str(command.get("owner") or ""),
                "idempotency_key": str(command.get("idempotency_key") or command_idempotency_key),
                "status": str(command.get("status") or ""),
                "attempt": int(command.get("attempt") or 0),
                "payload": dict(command.get("payload") or {}),
            }

        def _dispatch_prefetch_chunk(
            *,
            chunk_index: int,
            profile_url_chunk: list[str],
        ) -> dict[str, Any]:
            if not profile_url_chunk:
                return {
                    "chunk_index": chunk_index,
                    "queued_urls": [],
                    "deferred_urls": [],
                    "failed_urls": [],
                    "dispatched_url_count": 0,
                    "error_message": "",
                    "summary_path": "",
                }
            command = _plan_profile_refill_submit_command(
                chunk_index=chunk_index,
                profile_url_chunk=profile_url_chunk,
            )
            if not bool(execute_profile_refill_submit_commands):
                command_record = self._workflow_command_observation(command)
                return {
                    "chunk_index": chunk_index,
                    "queued_urls": list(profile_url_chunk),
                    "deferred_urls": [],
                    "failed_urls": [],
                    "dispatched_url_count": 0,
                    "error_message": "",
                    "summary_path": "",
                    "provider_submit_started_at": "",
                    "provider_submit_finished_at": "",
                    "provider_submit_elapsed_ms": 0,
                    "owner_worker_id": 0,
                    "owner_run_id": "",
                    "owner_dataset_id": "",
                    "owner_payload_hash": "",
                    "workflow_command": command_record,
                    "batch_envelope": _build_profile_prefetch_batch_envelope(
                        chunk_index=chunk_index,
                        profile_url_chunk=profile_url_chunk,
                        requested_url_count=len(normalized_urls),
                        candidate_count=requested_candidate_count,
                        active_worker_count=int(
                            prefetch_worker_budget.get("effective_active_worker_count")
                            or prefetch_worker_budget.get("active_worker_count")
                            or 0
                        ),
                        actor_budget=int(prefetch_worker_budget.get("actor_budget") or 0),
                        submit_budget=int(prefetch_worker_budget.get("submit_budget") or 0),
                        recommended_batch_size=int(prefetch_dispatch_window.get("batch_size") or 0),
                        recommended_batch_count=int(prefetch_dispatch_window.get("batch_count") or 0),
                        recommended_max_workers=int(prefetch_dispatch_window.get("max_workers") or 0),
                        dispatch_strategy=str(prefetch_dispatch_window.get("strategy") or ""),
                        deferred_url_count=planned_deferred_url_count,
                        tail_coalescing_url_count=planned_tail_coalescing_url_count,
                        queued_worker_count=1,
                        dispatched_url_count=0,
                        failed_url_count=0,
                        status="queued",
                        flush_reason="typed_command_planned_for_owner_daemon",
                        summary_path="",
                        small_batch_reason_override="typed_command_planned_for_owner_daemon",
                        queue_quiescent=False,
                    ),
                }
            owner_result = self.run_linkedin_profile_refill_submit_command_once(command)
            dispatch_result = dict(owner_result.get("dispatch_result") or {})
            workflow_command = dict(owner_result.get("workflow_command") or {})
            worker_status = str(dispatch_result.get("worker_status") or "unknown").strip() or "unknown"
            owner_status = str(owner_result.get("status") or "").strip().lower()
            if owner_status == "blocked":
                # Fail-closed bubble (decision #1 / invariant 7): a store-unavailable
                # envelope must NOT masquerade as a queued tail. Emit zero queued_urls so
                # the aggregation cannot count it toward queued_worker_count, and surface
                # the URLs as blocked (not queued/deferred) with an explicit error so the
                # recovery chain sees an observable infrastructure failure rather than a
                # silently-satisfied dispatched=0 terminal.
                blocked_reason = str(owner_result.get("reason") or "profile_refill_store_unavailable").strip()
                return {
                    "chunk_index": chunk_index,
                    "queued_urls": [],
                    "deferred_urls": [],
                    "failed_urls": [],
                    "blocked_urls": list(profile_url_chunk),
                    "dispatched_url_count": 0,
                    "error_message": blocked_reason,
                    "summary_path": "",
                    "blocked": True,
                    "blocked_reason": blocked_reason,
                    "workflow_command": workflow_command,
                    "batch_envelope": _build_profile_prefetch_batch_envelope(
                        chunk_index=chunk_index,
                        profile_url_chunk=profile_url_chunk,
                        requested_url_count=len(normalized_urls),
                        candidate_count=requested_candidate_count,
                        active_worker_count=int(
                            prefetch_worker_budget.get("effective_active_worker_count")
                            or prefetch_worker_budget.get("active_worker_count")
                            or 0
                        ),
                        actor_budget=int(prefetch_worker_budget.get("actor_budget") or 0),
                        submit_budget=int(prefetch_worker_budget.get("submit_budget") or 0),
                        recommended_batch_size=int(prefetch_dispatch_window.get("batch_size") or 0),
                        recommended_batch_count=int(prefetch_dispatch_window.get("batch_count") or 0),
                        recommended_max_workers=int(prefetch_dispatch_window.get("max_workers") or 0),
                        dispatch_strategy=str(prefetch_dispatch_window.get("strategy") or ""),
                        deferred_url_count=planned_deferred_url_count,
                        tail_coalescing_url_count=planned_tail_coalescing_url_count,
                        queued_worker_count=0,
                        dispatched_url_count=0,
                        failed_url_count=0,
                        status="blocked",
                        flush_reason=blocked_reason,
                        summary_path="",
                        small_batch_reason_override=blocked_reason,
                        queue_quiescent=False,
                    ),
                }
            if bool(workflow_command.get("runtime_command_contention")):
                return {
                    **dispatch_result,
                    "workflow_command": workflow_command,
                    "batch_envelope": _build_profile_prefetch_batch_envelope(
                        chunk_index=chunk_index,
                        profile_url_chunk=profile_url_chunk,
                        requested_url_count=len(normalized_urls),
                        candidate_count=requested_candidate_count,
                        active_worker_count=int(
                            prefetch_worker_budget.get("effective_active_worker_count")
                            or prefetch_worker_budget.get("active_worker_count")
                            or 0
                        ),
                        actor_budget=int(prefetch_worker_budget.get("actor_budget") or 0),
                        submit_budget=int(prefetch_worker_budget.get("submit_budget") or 0),
                        recommended_batch_size=int(prefetch_dispatch_window.get("batch_size") or 0),
                        recommended_batch_count=int(prefetch_dispatch_window.get("batch_count") or 0),
                        recommended_max_workers=int(prefetch_dispatch_window.get("max_workers") or 0),
                        dispatch_strategy=str(prefetch_dispatch_window.get("strategy") or ""),
                        deferred_url_count=planned_deferred_url_count,
                        tail_coalescing_url_count=planned_tail_coalescing_url_count,
                        queued_worker_count=1,
                        dispatched_url_count=0,
                        failed_url_count=0,
                        status="queued",
                        flush_reason="typed_command_already_owned",
                        summary_path=str(dispatch_result.get("summary_path") or ""),
                        small_batch_reason_override="typed_command_already_owned",
                        queue_quiescent=False,
                    ),
                }
            submit_elapsed_ms = int(dispatch_result.get("provider_submit_elapsed_ms") or 0)
            provider_submit_elapsed_values_ms.append(submit_elapsed_ms)
            provider_submit_started_values.append(str(dispatch_result.get("provider_submit_started_at") or ""))
            provider_submit_finished_values.append(str(dispatch_result.get("provider_submit_finished_at") or ""))
            queued_urls = [
                str(profile_url or "").strip()
                for profile_url in list(dispatch_result.get("queued_urls") or [])
                if str(profile_url or "").strip()
            ]
            deferred_urls = [
                str(profile_url or "").strip()
                for profile_url in list(dispatch_result.get("deferred_urls") or [])
                if str(profile_url or "").strip()
            ]
            failed_urls = [
                str(profile_url or "").strip()
                for profile_url in list(dispatch_result.get("failed_urls") or [])
                if str(profile_url or "").strip()
            ]
            dispatched_url_count = int(dispatch_result.get("dispatched_url_count") or 0)
            deferred_reason = str(
                dispatch_result.get("deferred_reason") or dispatch_result.get("small_batch_reason") or ""
            ).strip()
            small_batch_reason = str(dispatch_result.get("small_batch_reason") or "").strip()
            return {
                **dispatch_result,
                "workflow_command": workflow_command,
                "batch_envelope": _build_profile_prefetch_batch_envelope(
                    chunk_index=chunk_index,
                    profile_url_chunk=profile_url_chunk,
                    requested_url_count=len(normalized_urls),
                    candidate_count=requested_candidate_count,
                    active_worker_count=int(
                        prefetch_worker_budget.get("effective_active_worker_count")
                        or prefetch_worker_budget.get("active_worker_count")
                        or 0
                    ),
                    actor_budget=int(prefetch_worker_budget.get("actor_budget") or 0),
                    submit_budget=int(prefetch_worker_budget.get("submit_budget") or 0),
                    recommended_batch_size=int(prefetch_dispatch_window.get("batch_size") or 0),
                    recommended_batch_count=int(prefetch_dispatch_window.get("batch_count") or 0),
                    recommended_max_workers=int(prefetch_dispatch_window.get("max_workers") or 0),
                    dispatch_strategy=str(prefetch_dispatch_window.get("strategy") or ""),
                    deferred_url_count=planned_deferred_url_count + len(deferred_urls),
                    tail_coalescing_url_count=planned_tail_coalescing_url_count,
                    queued_worker_count=planned_dispatch_worker_count if queued_urls else 0,
                    dispatched_url_count=dispatched_url_count,
                    failed_url_count=len(failed_urls),
                    status=worker_status or "unknown",
                    flush_reason="retry_isolation" if retry_isolated_refill else "adaptive_prefetch_window",
                    summary_path=str(dispatch_result.get("summary_path") or "").strip(),
                    small_batch_reason_override=deferred_reason or ("retry_isolation" if retry_isolated_refill else ""),
                    queue_quiescent=small_batch_reason == "queue_quiescent_final_tail",
                ),
            }

        ordinal_gate_deferred_urls: list[str] = []
        ordinal_gate_triggered = False
        for chunk_index, dispatch_chunk in dispatch_specs:
            if ordinal_gate_triggered:
                ordinal_gate_deferred_chunk = [
                    str(profile_url or "").strip()
                    for profile_url in list(dispatch_chunk or [])
                    if str(profile_url or "").strip()
                ]
                for profile_url in ordinal_gate_deferred_chunk:
                    if profile_url not in ordinal_gate_deferred_urls:
                        ordinal_gate_deferred_urls.append(profile_url)
                if self.store is not None and ordinal_gate_deferred_chunk:
                    _mark_profile_prefetch_urls_deferred_for_dispatch_retry(
                        self.store,
                        ordinal_gate_deferred_chunk,
                        source_shards_by_url=dispatch_source_shards_by_url,
                        source_jobs=[job_id] if str(job_id or "").strip() else [],
                        snapshot_dir=snapshot_dir,
                        trigger_kind="profile_prefetch_ordinal_gate_deferred",
                        plan_reason="prior_batch_backpressure_ordinal_gate",
                        deferred_reason="prior_batch_backpressure_ordinal_gate",
                    )
                dispatch_results.append(
                    {
                        "chunk_index": chunk_index,
                        "queued_urls": [],
                        "deferred_urls": ordinal_gate_deferred_chunk,
                        "failed_urls": [],
                        "dispatched_url_count": 0,
                        "error_message": "",
                        "summary_path": "",
                        "batch_envelope": _build_profile_prefetch_batch_envelope(
                            chunk_index=chunk_index,
                            profile_url_chunk=ordinal_gate_deferred_chunk,
                            requested_url_count=len(normalized_urls),
                            candidate_count=requested_candidate_count,
                            active_worker_count=int(
                                prefetch_worker_budget.get("effective_active_worker_count")
                                or prefetch_worker_budget.get("active_worker_count")
                                or 0
                            ),
                            actor_budget=int(prefetch_worker_budget.get("actor_budget") or 0),
                            submit_budget=int(prefetch_worker_budget.get("submit_budget") or 0),
                            recommended_batch_size=int(prefetch_dispatch_window.get("batch_size") or 0),
                            recommended_batch_count=int(prefetch_dispatch_window.get("batch_count") or 0),
                            recommended_max_workers=int(prefetch_dispatch_window.get("max_workers") or 0),
                            dispatch_strategy=str(prefetch_dispatch_window.get("strategy") or ""),
                            deferred_url_count=planned_deferred_url_count + len(ordinal_gate_deferred_urls),
                            tail_coalescing_url_count=0,
                            queued_worker_count=0,
                            dispatched_url_count=0,
                            failed_url_count=0,
                            status="deferred",
                            flush_reason="ordinal_submit_gate",
                            small_batch_reason_override="prior_batch_backpressure_ordinal_gate",
                        ),
                    }
                )
                continue
            dispatch_result = _dispatch_prefetch_chunk(
                chunk_index=chunk_index,
                profile_url_chunk=dispatch_chunk,
            )
            dispatch_results.append(dispatch_result)
            worker_status = str(dict(dispatch_result or {}).get("batch_envelope", {}).get("status") or "").strip()
            worker_deferred_urls = [
                str(profile_url or "").strip()
                for profile_url in list(dict(dispatch_result or {}).get("deferred_urls") or [])
                if str(profile_url or "").strip()
            ]
            worker_failed_urls = [
                str(profile_url or "").strip()
                for profile_url in list(dict(dispatch_result or {}).get("failed_urls") or [])
                if str(profile_url or "").strip()
            ]
            error_message = str(dict(dispatch_result or {}).get("error_message") or "").strip()
            if self.store is not None and worker_deferred_urls:
                worker_deferred_reason = (
                    str(dict(dispatch_result or {}).get("error_message") or "").strip()
                    or str(
                        dict(dispatch_result or {}).get("batch_envelope", {}).get("small_batch_reason") or ""
                    ).strip()
                    or "provider_submit_not_started"
                )
                if worker_deferred_reason == "harvest_profile_provider_limiter_backpressure":
                    worker_deferred_reason = "provider_limiter_backpressure"
                elif worker_deferred_reason == "harvest_profile_submit_slot_backpressure":
                    worker_deferred_reason = "submit_slot_backpressure"
                _mark_profile_prefetch_urls_deferred_for_dispatch_retry(
                    self.store,
                    worker_deferred_urls,
                    source_shards_by_url=dispatch_source_shards_by_url,
                    source_jobs=[job_id] if str(job_id or "").strip() else [],
                    snapshot_dir=snapshot_dir,
                    trigger_kind="profile_prefetch_dispatch_deferred",
                    plan_reason=worker_deferred_reason,
                    deferred_reason=worker_deferred_reason,
                )
            if (
                worker_status in {"backpressure", "failed"}
                or worker_deferred_urls
                or worker_failed_urls
                or error_message
            ):
                ordinal_gate_triggered = True

        queued_urls: list[str] = []
        failed_urls: list[str] = []
        blocked_urls: list[str] = []
        blocked_reasons: list[str] = []
        summary_paths: list[str] = []
        batch_envelopes: list[dict[str, Any]] = []
        workflow_commands: list[dict[str, Any]] = []
        errors: list[str] = []
        queued_worker_count = 0
        dispatched_url_count = 0
        for result in sorted(dispatch_results, key=lambda item: int(item.get("chunk_index") or 0)):
            batch_envelope = dict(result.get("batch_envelope") or {})
            if batch_envelope:
                batch_envelopes.append(batch_envelope)
            workflow_command = dict(result.get("workflow_command") or {})
            if workflow_command:
                workflow_commands.append(workflow_command)
            dispatched_url_count += int(result.get("dispatched_url_count") or 0)
            if bool(result.get("blocked")):
                # A blocked envelope is an infrastructure outage signal: never counted as
                # queued and never used as a satisfied terminal (invariant 7).
                for profile_url in list(result.get("blocked_urls") or []):
                    normalized_profile_url = str(profile_url or "").strip()
                    if normalized_profile_url and normalized_profile_url not in blocked_urls:
                        blocked_urls.append(normalized_profile_url)
                blocked_reason_value = str(result.get("blocked_reason") or "").strip()
                if blocked_reason_value and blocked_reason_value not in blocked_reasons:
                    blocked_reasons.append(blocked_reason_value)
            chunk_queued_urls = [
                str(profile_url or "").strip()
                for profile_url in list(result.get("queued_urls") or [])
                if str(profile_url or "").strip()
            ]
            if chunk_queued_urls:
                queued_worker_count += 1
                for profile_url in chunk_queued_urls:
                    if profile_url not in queued_urls:
                        queued_urls.append(profile_url)
            for profile_url in list(result.get("deferred_urls") or []):
                normalized_profile_url = str(profile_url or "").strip()
                if normalized_profile_url and normalized_profile_url not in deferred_urls:
                    deferred_urls.append(normalized_profile_url)
            for profile_url in list(result.get("failed_urls") or []):
                normalized_profile_url = str(profile_url or "").strip()
                if normalized_profile_url and normalized_profile_url not in failed_urls:
                    failed_urls.append(normalized_profile_url)
            error_message = str(result.get("error_message") or "").strip()
            if error_message:
                errors.append(error_message)
            summary_path_value = str(result.get("summary_path") or "").strip()
            if summary_path_value:
                summary_paths.append(summary_path_value)

        if failed_urls and self.store is not None:
            for profile_url in failed_urls:
                self.store.repos.linkedin_profile_registry.mark_failed(
                    profile_url,
                    error="background_prefetch_dispatch_failed",
                    retryable=True,
                    source_shards=list(source_shards_by_url.get(profile_url) or []),
                    source_jobs=[job_id],
                    snapshot_dir=str(snapshot_dir),
                )

        if self.store is not None:
            for result in sorted(dispatch_results, key=lambda item: int(item.get("chunk_index") or 0)):
                chunk_queued_urls = [
                    str(profile_url or "").strip()
                    for profile_url in list(result.get("queued_urls") or [])
                    if str(profile_url or "").strip()
                ]
                if not chunk_queued_urls:
                    continue
                owner_worker_id = int(result.get("owner_worker_id") or 0)
                owner_run_id = str(result.get("owner_run_id") or "").strip()
                owner_dataset_id = str(result.get("owner_dataset_id") or "").strip()
                owner_payload_hash = str(result.get("owner_payload_hash") or "").strip()
                if not (owner_worker_id > 0 or owner_run_id or owner_dataset_id):
                    continue
                _mark_profile_prefetch_urls_dispatch_owned(
                    self.store,
                    chunk_queued_urls,
                    source_shards_by_url=source_shards_by_url,
                    source_jobs=[job_id],
                    snapshot_dir=snapshot_dir,
                    trigger_kind="profile_prefetch_provider_submit",
                    plan_reason=(
                        PROFILE_REFILL_RETRY_PROVIDER_SUBMITTED_PLAN_REASON
                        if retry_isolated_refill
                        else "remote_provider_submitted"
                    ),
                    owner_worker_id=owner_worker_id,
                    owner_run_id=owner_run_id,
                    owner_dataset_id=owner_dataset_id,
                    owner_payload_hash=owner_payload_hash,
                )

        # Fail-closed terminal bubble (decision #1 / invariant 7): if any chunk came back
        # blocked (store/infrastructure unavailable), the overall envelope is a blocked
        # terminal — it must NOT report completed (would claim "all satisfied") nor queued
        # with dispatched=0 (would claim "tail still in flight"). The two-signal shape
        # (status="blocked" + explicit reason + blocked_urls) lets the recovery chain
        # tell "infrastructure missing" apart from "confirmed nothing to dispatch".
        if blocked_urls:
            overall_status = "blocked"
        elif queued_worker_count > 0 or deferred_urls:
            overall_status = "queued"
        else:
            overall_status = "completed"
        blocked_reason = blocked_reasons[0] if blocked_reasons else ""
        return _with_prefetch_queue(
            {
                "status": overall_status,
                **(
                    {
                        "reason": blocked_reason or "profile_refill_store_unavailable",
                        "blocked": True,
                        "blocked_url_count": len(blocked_urls),
                        "blocked_urls": blocked_urls,
                        "blocked_reason": blocked_reason or "profile_refill_store_unavailable",
                    }
                    if blocked_urls
                    else {}
                ),
                "requested_url_count": len(normalized_urls),
                "scheduler_lock": scheduler_lock_summary,
                "refill_queue_item_count": len(refill_queue_items),
                "dispatched_url_count": dispatched_url_count,
                "cached_profile_count": len(cached_profiles),
                "queued_worker_count": queued_worker_count,
                "active_worker_count": int(prefetch_worker_budget.get("active_worker_count") or 0),
                "submit_budget": int(prefetch_worker_budget.get("submit_budget") or 1),
                "actor_budget": int(prefetch_worker_budget.get("actor_budget") or 1),
                "recommended_batch_size": int(prefetch_dispatch_window.get("batch_size") or 1),
                "recommended_batch_count": int(prefetch_dispatch_window.get("batch_count") or 0),
                "recommended_max_workers": int(prefetch_dispatch_window.get("max_workers") or 1),
                "dispatch_strategy": str(prefetch_dispatch_window.get("strategy") or ""),
                "retry_isolated_refill": retry_isolated_refill,
                "ordinal_submit_gate": "same_plan_chunk_order",
                "ordinal_gate_triggered": ordinal_gate_triggered,
                "ordinal_gate_deferred_url_count": len(ordinal_gate_deferred_urls),
                "ordinal_gate_deferred_urls": ordinal_gate_deferred_urls,
                "batch_envelopes": batch_envelopes,
                "workflow_commands": workflow_commands,
                "workflow_command_count": len(workflow_commands),
                "workflow_command_status_counts": {
                    status: sum(
                        1 for item in workflow_commands if str(dict(item or {}).get("status") or "").strip() == status
                    )
                    for status in sorted(
                        {
                            str(dict(item or {}).get("status") or "").strip()
                            for item in workflow_commands
                            if str(dict(item or {}).get("status") or "").strip()
                        }
                    )
                },
                "execute_profile_refill_submit_commands": bool(execute_profile_refill_submit_commands),
                "tiny_batch_count": sum(1 for item in batch_envelopes if bool(item.get("is_tiny_batch"))),
                "unexplained_tiny_batch_count": sum(
                    1
                    for item in batch_envelopes
                    if bool(item.get("is_tiny_batch")) and not bool(item.get("tiny_batch_allowed"))
                ),
                "provider_slot_underuse_with_backlog_count": sum(
                    1 for item in batch_envelopes if bool(item.get("provider_slot_underuse_with_backlog"))
                ),
                "tiny_batch_coalesced_count": tiny_batch_coalesced_count,
                "deferred_url_count": len(deferred_urls),
                "deferred_urls": deferred_urls,
                "batch_plan_reason": prefetch_batch_plan.plan_reason,
                "batch_plan": prefetch_batch_plan.to_record(),
                "refill_plan_items": refill_plan_items,
                "provider_submit_attempt_count": len(provider_submit_elapsed_values_ms),
                "provider_submit_started_at": provider_submit_started_values[0]
                if provider_submit_started_values
                else "",
                "provider_submit_finished_at": provider_submit_finished_values[-1]
                if provider_submit_finished_values
                else "",
                "provider_submit_elapsed_ms": max(provider_submit_elapsed_values_ms)
                if provider_submit_elapsed_values_ms
                else 0,
                "provider_submit_total_elapsed_ms": sum(provider_submit_elapsed_values_ms),
                "queued_urls": queued_urls,
                "failed_urls": failed_urls,
                "summary_paths": summary_paths,
                "errors": errors,
            },
            ready_profile_urls=dispatch_urls,
            newly_queued_profile_urls=queued_urls,
            already_queued_profile_urls=already_queued_urls,
            deferred_profile_urls=deferred_urls,
            failed_profile_urls=failed_urls,
            batch_envelopes=batch_envelopes,
            worker_budget=prefetch_worker_budget,
        )

    def _execute_harvest_profile_batch_worker(
        self,
        *,
        profile_urls: list[str],
        snapshot_dir: Path,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool,
        load_cached_profile_payloads: bool = True,
        prefetch_batch_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self.harvest_profile_connector is None or self.worker_runtime is None or not job_id:
            return {"worker_status": "skipped"}

        normalized_urls: list[str] = []
        for profile_url in profile_urls:
            value = str(profile_url or "").strip()
            if value and value not in normalized_urls:
                normalized_urls.append(value)
        if not normalized_urls:
            return {"worker_status": "completed", "summary": {"requested_url_count": 0, "status": "completed"}}

        registry_repo = linkedin_profile_registry_repo(self.store)
        resume_payload_hash = sha1(json.dumps(sorted(normalized_urls), ensure_ascii=False).encode("utf-8")).hexdigest()[
            :16
        ]
        resume_summary_path = (
            snapshot_dir / "harvest_profiles" / f"harvest_profile_batch_{resume_payload_hash}.queue_summary.json"
        )
        existing_resume_worker: dict[str, Any] = {}
        existing_resume_checkpoint: dict[str, Any] = {}
        if self.store is not None:
            get_worker = getattr(self.store, "get_agent_worker", None)
            if callable(get_worker):
                try:
                    existing_resume_worker = dict(
                        get_worker(
                            job_id=job_id,
                            lane_id="enrichment_specialist",
                            worker_key=f"harvest_profile_batch::{resume_payload_hash}",
                        )
                        or {}
                    )
                except Exception:
                    existing_resume_worker = {}
                existing_resume_checkpoint = dict(existing_resume_worker.get("checkpoint") or {})
        summary_resume_checkpoint = _load_harvest_profile_batch_resume_checkpoint_from_summary(resume_summary_path)
        if summary_resume_checkpoint:
            merged_checkpoint = dict(existing_resume_checkpoint)
            for key, value in summary_resume_checkpoint.items():
                if value in (None, "", [], {}):
                    continue
                if key == "artifact_paths":
                    merged_checkpoint[key] = {
                        **dict(summary_resume_checkpoint.get("artifact_paths") or {}),
                        **dict(merged_checkpoint.get("artifact_paths") or {}),
                    }
                    continue
                if key == "stage" and str(merged_checkpoint.get("stage") or "").strip() not in {
                    "",
                    "submitting_remote_harvest",
                }:
                    continue
                if merged_checkpoint.get(key) in (None, "", [], {}):
                    merged_checkpoint[key] = value
            existing_resume_checkpoint = merged_checkpoint
        resume_remote_run = bool(
            existing_resume_checkpoint.get("run_id")
            and str(existing_resume_worker.get("status") or "").strip().lower() != "completed"
        )
        existing_resume_metadata = dict(existing_resume_worker.get("metadata") or {})
        context_payload: dict[str, Any] = {}
        for context_candidate in (
            existing_resume_metadata.get("prefetch_batch_context"),
            existing_resume_checkpoint.get("prefetch_batch_context"),
            prefetch_batch_context,
        ):
            if isinstance(context_candidate, dict):
                context_payload.update(dict(context_candidate))
        context_payload.setdefault("nonblocking_submit", bool(context_payload.get("nonblocking_submit")))
        requested_context_count = _safe_int_value(context_payload.get("requested_url_count")) or len(normalized_urls)
        candidate_context_count = _safe_int_value(context_payload.get("candidate_count")) or len(normalized_urls)
        planned_deferred_context_count = _safe_int_value(context_payload.get("planned_deferred_url_count"))
        planned_dispatch_worker_count = _safe_int_value(context_payload.get("planned_dispatch_worker_count"))
        planned_batch_reason = str(context_payload.get("batch_plan_reason") or "").strip()
        under_target_final_tail_authorized = bool(context_payload.get("allow_under_target_final_tail_dispatch")) or (
            planned_batch_reason == "queue_quiescent_final_tail"
        )

        def _worker_effective_small_batch_reason(reason: str, *, dispatch_url_count: int) -> str:
            normalized_reason = str(reason or "").strip()
            if dispatch_url_count >= _harvest_profile_prefetch_actor_slot_url_target():
                return normalized_reason
            if len(normalized_urls) >= _harvest_profile_prefetch_actor_slot_url_target() and cached_profiles:
                return "cache_filtered_actor_slot"
            if under_target_final_tail_authorized and planned_deferred_context_count <= 0:
                return "queue_quiescent_final_tail"
            return normalized_reason

        if resume_remote_run:
            cached_profiles: dict[str, dict[str, Any]] = {}
            dispatch_urls = list(normalized_urls)
            already_queued_urls: list[str] = []
            url_claims: dict[str, dict[str, Any]] = {}
        else:
            cached_profiles = self._hydrate_cached_prefetch_profiles(
                normalized_urls,
                snapshot_dir,
                source_jobs=[job_id] if str(job_id or "").strip() else [],
                load_profile_payloads=load_cached_profile_payloads,
            )
            dispatch_urls = [
                profile_url
                for profile_url in normalized_urls
                if str(profile_url or "").strip() and str(profile_url or "").strip() not in cached_profiles
            ]
            dispatch_urls, already_queued_urls = self._partition_already_queued_profile_urls(
                dispatch_urls,
                snapshot_dir=snapshot_dir,
                source_shards_by_url={profile_url: ["enrichment_background_prefetch"] for profile_url in dispatch_urls},
                source_jobs=[job_id] if str(job_id or "").strip() else [],
                refill_dispatch_profile_urls=set(dispatch_urls),
                dispatch_claimed_profile_urls=set(dispatch_urls),
                expected_owner_payload_hash=resume_payload_hash,
            )
            claim_contended_urls: list[str] = []
            if not dispatch_urls:
                summary_status = "queued" if already_queued_urls else "completed"
                return {
                    "worker_status": summary_status,
                    "summary": {
                        "logical_name": "harvest_profile_scraper_batch",
                        "requested_url_count": len(normalized_urls),
                        "requested_urls": list(normalized_urls),
                        "queued_urls": list(already_queued_urls),
                        "dispatched_url_count": 0,
                        "reused_cached_count": len(cached_profiles),
                        "already_queued_url_count": len(already_queued_urls),
                        "status": summary_status,
                        "message": (
                            "reused_local_raw_cache"
                            if not already_queued_urls
                            else "reused_active_harvest_profile_queue"
                        ),
                    },
                    "cached_profiles": cached_profiles,
                }

            dispatch_registry_entries: dict[str, dict[str, Any]] = {}
            if self.store is not None and dispatch_urls:
                try:
                    dispatch_registry_entries = dict(
                        self.store.repos.linkedin_profile_registry.get_bulk(dispatch_urls) or {}
                    )
                except Exception:
                    dispatch_registry_entries = {}
            oldest_deferred_coalescing_age_ms = _profile_prefetch_oldest_deferred_coalescing_age_ms(
                dispatch_urls,
                dispatch_registry_entries,
            )
            tiny_tail_coalescing_age_proven = (
                self.store is None
                or _harvest_profile_tiny_tail_coalescing_min_age_ms() <= 0
                or (
                    oldest_deferred_coalescing_age_ms is not None
                    and oldest_deferred_coalescing_age_ms >= _harvest_profile_tiny_tail_coalescing_min_age_ms()
                )
            )
            queue_quiescent = (
                under_target_final_tail_authorized
                or planned_deferred_context_count <= 0
                and planned_dispatch_worker_count <= 1
                and tiny_tail_coalescing_age_proven
            )
            small_batch_reason = _profile_batch_envelope_small_batch_reason(
                batch_size=len(dispatch_urls),
                requested_url_count=requested_context_count,
                candidate_count=candidate_context_count,
                deferred_url_count=planned_deferred_context_count,
                queue_quiescent=queue_quiescent,
            )
            small_batch_reason = _worker_effective_small_batch_reason(
                small_batch_reason,
                dispatch_url_count=len(dispatch_urls),
            )
            if (
                dispatch_urls
                and len(dispatch_urls) < _harvest_profile_prefetch_actor_slot_url_target()
                and not _profile_batch_envelope_allowed_tiny_reason(small_batch_reason)
            ):
                coalescing_not_before_at = (
                    datetime.now(timezone.utc).replace(microsecond=0)
                    + timedelta(milliseconds=max(0, int(_harvest_profile_tiny_tail_coalescing_min_age_ms() or 0)))
                ).strftime("%Y-%m-%d %H:%M:%S")
                coalescing_refill_items: dict[str, Any] = {}
                if self.store is not None:
                    recorder = getattr(registry_repo, "record_refill_plan_items", None)
                    if callable(recorder):
                        coalescing_refill_items = dict(
                            recorder(
                                deferred_profile_urls=[*dispatch_urls, *already_queued_urls],
                                source_shards_by_url={
                                    profile_url: ["enrichment_background_prefetch"]
                                    for profile_url in [*dispatch_urls, *already_queued_urls]
                                },
                                source_jobs=[job_id] if str(job_id or "").strip() else [],
                                snapshot_dir=str(snapshot_dir),
                                trigger_kind="profile_tiny_tail_coalescing",
                                plan_reason="tiny_tail_coalescing_wait",
                                deferred_reason=small_batch_reason,
                                deferred_queue_state="deferred_coalescing",
                                refill_not_before_at=coalescing_not_before_at,
                            )
                            or {}
                        )
                    else:
                        mark_deferred = getattr(registry_repo, "mark_deferred_for_coalescing", None)
                        if callable(mark_deferred):
                            for profile_url in dispatch_urls:
                                mark_deferred(
                                    profile_url,
                                    reason=small_batch_reason,
                                    source_shards=["enrichment_background_prefetch"],
                                    source_jobs=[job_id] if str(job_id or "").strip() else [],
                                    snapshot_dir=str(snapshot_dir),
                                )
                return {
                    "worker_status": "backpressure",
                    "summary": {
                        "logical_name": "harvest_profile_scraper_batch",
                        "requested_url_count": len(normalized_urls),
                        "requested_urls": list(normalized_urls),
                        "deferred_urls": list(dispatch_urls),
                        "queued_urls": list(already_queued_urls),
                        "dispatched_url_count": 0,
                        "reused_cached_count": len(cached_profiles),
                        "already_queued_url_count": len(already_queued_urls),
                        "status": "queued",
                        "message": "harvest_profile_tiny_batch_deferred_for_coalescing",
                        "small_batch_reason": small_batch_reason,
                        "tiny_tail_coalescing_min_age_ms": _harvest_profile_tiny_tail_coalescing_min_age_ms(),
                        "oldest_deferred_coalescing_age_ms": oldest_deferred_coalescing_age_ms,
                        "coalescing_not_before_at": coalescing_not_before_at,
                        "coalescing_scheduler": "linkedin_profile_registry",
                        "coalescing_refill_items": coalescing_refill_items,
                        "claim_timing": "after_tiny_tail_coalescing_gate",
                    },
                    "cached_profiles": cached_profiles,
                    "deferred_urls": list(dispatch_urls),
                }

            url_claims = {}
            if self.store is not None:
                claimed_dispatch_urls: list[str] = []
                lease_owner = _profile_registry_lease_owner(
                    f"background_prefetch:{job_id or 'job'}:{resume_payload_hash}"
                )
                lease_payloads_by_url: dict[str, dict[str, Any]] = {}
                batch_lease_payload: dict[str, Any] | None = None
                batch_acquire = getattr(registry_repo, "acquire_leases", None)
                if callable(batch_acquire):
                    try:
                        batch_lease_payload = dict(
                            batch_acquire(
                                dispatch_urls,
                                lease_owner=lease_owner,
                                lease_seconds=PROFILE_REGISTRY_LEASE_SECONDS,
                            )
                            or {}
                        )
                    except Exception:
                        batch_lease_payload = None
                if batch_lease_payload is not None:
                    lease_payloads_by_url = {
                        str(profile_url or "").strip(): dict(payload or {})
                        for profile_url, payload in dict(batch_lease_payload.get("leases_by_url") or {}).items()
                        if str(profile_url or "").strip()
                    }
                    claimed_dispatch_urls = [
                        str(profile_url or "").strip()
                        for profile_url in list(batch_lease_payload.get("acquired_urls") or [])
                        if str(profile_url or "").strip()
                    ]
                    url_claims.update(
                        {
                            profile_url: dict(lease_payloads_by_url.get(profile_url) or {})
                            for profile_url in claimed_dispatch_urls
                        }
                    )
                    contended_profile_urls = [
                        str(profile_url or "").strip()
                        for profile_url in list(batch_lease_payload.get("contended_urls") or [])
                        if str(profile_url or "").strip()
                    ]
                    contended_registry_entries: dict[str, dict[str, Any]] = {}
                    if contended_profile_urls:
                        try:
                            contended_registry_entries = dict(
                                self.store.repos.linkedin_profile_registry.get_bulk(contended_profile_urls) or {}
                            )
                        except Exception:
                            contended_registry_entries = {}
                    for profile_url in contended_profile_urls:
                        normalized_profile_url = str(profile_url or "").strip()
                        if not normalized_profile_url:
                            continue
                        registry_key = normalize_linkedin_profile_url_key(normalized_profile_url)
                        registry_entry = dict(contended_registry_entries.get(registry_key) or {})
                        registry_status = str(registry_entry.get("status") or "").strip().lower()
                        refill_state = str(registry_entry.get("refill_queue_state") or "").strip().lower()
                        provider_owned_same_scope = (
                            registry_status == "queued"
                            and refill_state == PROFILE_REFILL_PROVIDER_OWNED_QUEUE_STATE
                            and self._refill_registry_entry_matches_scope(
                                registry_entry,
                                job_id=job_id,
                                snapshot_dir=snapshot_dir,
                            )
                            and self._queued_profile_registry_entry_has_provider_owner(registry_entry)
                        )
                        if normalized_profile_url not in already_queued_urls:
                            already_queued_urls.append(normalized_profile_url)
                        if provider_owned_same_scope:
                            self.store.repos.linkedin_profile_registry.upsert_sources(
                                normalized_profile_url,
                                source_shards=["enrichment_background_prefetch"],
                                source_jobs=[job_id] if str(job_id or "").strip() else [],
                            )
                            self.store.repos.linkedin_profile_registry.record_event(
                                normalized_profile_url,
                                event_type="provider_owned_lease_contention_skip",
                                event_status=str(registry_entry.get("refill_owner_run_id") or ""),
                                detail="background prefetch URL lease contended by provider-owned queue",
                            )
                            continue
                        if normalized_profile_url not in claim_contended_urls:
                            claim_contended_urls.append(normalized_profile_url)
                        self.store.repos.linkedin_profile_registry.upsert_sources(
                            normalized_profile_url,
                            source_shards=["enrichment_background_prefetch"],
                            source_jobs=[job_id] if str(job_id or "").strip() else [],
                        )
                        self.store.repos.linkedin_profile_registry.record_event(
                            normalized_profile_url,
                            event_type="lease_contended_skip",
                            event_status=str(
                                dict(lease_payloads_by_url.get(normalized_profile_url) or {}).get("lease_owner") or ""
                            ),
                            detail="background prefetch URL claim contended",
                        )
                else:
                    for profile_url in dispatch_urls:
                        lease_payload = self.store.repos.linkedin_profile_registry.acquire_lease(
                            profile_url,
                            lease_owner=lease_owner,
                            lease_seconds=PROFILE_REGISTRY_LEASE_SECONDS,
                        )
                        lease_payloads_by_url[profile_url] = dict(lease_payload or {})
                        if bool(lease_payload.get("acquired")):
                            url_claims[profile_url] = lease_payload
                            claimed_dispatch_urls.append(profile_url)
                            continue
                    for profile_url in dispatch_urls:
                        if profile_url in claimed_dispatch_urls:
                            continue
                        lease_payload = dict(lease_payloads_by_url.get(profile_url) or {})
                        already_queued_urls.append(profile_url)
                        claim_contended_urls.append(profile_url)
                        self.store.repos.linkedin_profile_registry.upsert_sources(
                            profile_url,
                            source_shards=["enrichment_background_prefetch"],
                            source_jobs=[job_id] if str(job_id or "").strip() else [],
                        )
                        self.store.repos.linkedin_profile_registry.record_event(
                            profile_url,
                            event_type="lease_contended_skip",
                            event_status=str(lease_payload.get("lease_owner") or ""),
                            detail="background prefetch URL claim contended",
                        )
                dispatch_urls = claimed_dispatch_urls
                if dispatch_urls:
                    try:
                        post_lease_registry_entries = dict(
                            self.store.repos.linkedin_profile_registry.get_bulk(dispatch_urls) or {}
                        )
                    except Exception:
                        post_lease_registry_entries = {}
                    blocked_after_claim_urls: list[str] = []
                    for profile_url in list(dispatch_urls):
                        registry_key = normalize_linkedin_profile_url_key(profile_url)
                        registry_entry = dict(post_lease_registry_entries.get(registry_key) or {})
                        registry_status = str(registry_entry.get("status") or "").strip().lower()
                        refill_state = str(registry_entry.get("refill_queue_state") or "").strip()
                        owner_payload_hash = str(registry_entry.get("refill_owner_payload_hash") or "").strip()
                        if registry_status in {"fetched", "unrecoverable"}:
                            blocked_after_claim_urls.append(profile_url)
                            continue
                        if (
                            refill_state == PROFILE_REFILL_PROVIDER_OWNED_QUEUE_STATE
                            and self._queued_profile_registry_entry_has_provider_owner(registry_entry)
                        ):
                            blocked_after_claim_urls.append(profile_url)
                            continue
                        if (
                            refill_state in {"dispatch_reserved", "dispatch_claimed"}
                            and owner_payload_hash
                            and owner_payload_hash != resume_payload_hash
                        ):
                            blocked_after_claim_urls.append(profile_url)
                    if blocked_after_claim_urls:
                        blocked_set = set(blocked_after_claim_urls)
                        for profile_url in blocked_after_claim_urls:
                            lease_payload = dict(url_claims.get(profile_url) or {})
                            lease_owner_value = str(lease_payload.get("lease_owner") or "")
                            lease_token_value = str(lease_payload.get("lease_token") or "")
                            if lease_owner_value or lease_token_value:
                                self.store.repos.linkedin_profile_registry.release_lease(
                                    profile_url,
                                    lease_owner=lease_owner_value,
                                    lease_token=lease_token_value,
                                )
                            url_claims.pop(profile_url, None)
                            if profile_url not in already_queued_urls:
                                already_queued_urls.append(profile_url)
                        dispatch_urls = [profile_url for profile_url in dispatch_urls if profile_url not in blocked_set]
            if dispatch_urls:
                post_claim_registry_entries = dispatch_registry_entries
                missing_post_claim_entries = [
                    profile_url
                    for profile_url in dispatch_urls
                    if normalize_linkedin_profile_url_key(profile_url) not in post_claim_registry_entries
                ]
                if self.store is not None and missing_post_claim_entries:
                    try:
                        post_claim_registry_entries = dict(
                            self.store.repos.linkedin_profile_registry.get_bulk(dispatch_urls) or {}
                        )
                    except Exception:
                        post_claim_registry_entries = dispatch_registry_entries
                post_claim_oldest_deferred_coalescing_age_ms = _profile_prefetch_oldest_deferred_coalescing_age_ms(
                    dispatch_urls,
                    post_claim_registry_entries,
                )
                post_claim_tiny_tail_coalescing_age_proven = (
                    self.store is None
                    or _harvest_profile_tiny_tail_coalescing_min_age_ms() <= 0
                    or (
                        post_claim_oldest_deferred_coalescing_age_ms is not None
                        and post_claim_oldest_deferred_coalescing_age_ms
                        >= _harvest_profile_tiny_tail_coalescing_min_age_ms()
                    )
                )
                post_claim_queue_quiescent = (
                    under_target_final_tail_authorized
                    or planned_deferred_context_count <= 0
                    and planned_dispatch_worker_count <= 1
                    and post_claim_tiny_tail_coalescing_age_proven
                )
                post_claim_small_batch_reason = _profile_batch_envelope_small_batch_reason(
                    batch_size=len(dispatch_urls),
                    requested_url_count=requested_context_count,
                    candidate_count=candidate_context_count,
                    deferred_url_count=planned_deferred_context_count,
                    queue_quiescent=post_claim_queue_quiescent,
                )
                post_claim_small_batch_reason = _worker_effective_small_batch_reason(
                    post_claim_small_batch_reason,
                    dispatch_url_count=len(dispatch_urls),
                )
            else:
                post_claim_oldest_deferred_coalescing_age_ms = oldest_deferred_coalescing_age_ms
                post_claim_small_batch_reason = small_batch_reason
            if (
                dispatch_urls
                and len(dispatch_urls) < _harvest_profile_prefetch_actor_slot_url_target()
                and not _profile_batch_envelope_allowed_tiny_reason(post_claim_small_batch_reason)
            ):
                coalescing_not_before_at = (
                    datetime.now(timezone.utc).replace(microsecond=0)
                    + timedelta(milliseconds=max(0, int(_harvest_profile_tiny_tail_coalescing_min_age_ms() or 0)))
                ).strftime("%Y-%m-%d %H:%M:%S")
                coalescing_refill_items: dict[str, Any] = {}
                if self.store is not None:
                    recorder = getattr(registry_repo, "record_refill_plan_items", None)
                    if callable(recorder):
                        coalescing_refill_items = dict(
                            recorder(
                                deferred_profile_urls=[*dispatch_urls, *already_queued_urls],
                                source_shards_by_url={
                                    profile_url: ["enrichment_background_prefetch"]
                                    for profile_url in [*dispatch_urls, *already_queued_urls]
                                },
                                source_jobs=[job_id] if str(job_id or "").strip() else [],
                                snapshot_dir=str(snapshot_dir),
                                trigger_kind="profile_tiny_tail_coalescing",
                                plan_reason="tiny_tail_coalescing_wait",
                                deferred_reason=post_claim_small_batch_reason,
                                deferred_queue_state="deferred_coalescing",
                                refill_not_before_at=coalescing_not_before_at,
                            )
                            or {}
                        )
                    else:
                        mark_deferred = getattr(registry_repo, "mark_deferred_for_coalescing", None)
                        if callable(mark_deferred):
                            for profile_url in dispatch_urls:
                                mark_deferred(
                                    profile_url,
                                    reason=post_claim_small_batch_reason,
                                    source_shards=["enrichment_background_prefetch"],
                                    source_jobs=[job_id] if str(job_id or "").strip() else [],
                                    snapshot_dir=str(snapshot_dir),
                                )
                    for profile_url, lease_payload in list(url_claims.items()):
                        self.store.repos.linkedin_profile_registry.release_lease(
                            profile_url,
                            lease_owner=str(lease_payload.get("lease_owner") or ""),
                            lease_token=str(lease_payload.get("lease_token") or ""),
                        )
                url_claims.clear()
                return {
                    "worker_status": "backpressure",
                    "summary": {
                        "logical_name": "harvest_profile_scraper_batch",
                        "requested_url_count": len(normalized_urls),
                        "requested_urls": list(normalized_urls),
                        "deferred_urls": list(dispatch_urls),
                        "queued_urls": list(already_queued_urls),
                        "dispatched_url_count": 0,
                        "reused_cached_count": len(cached_profiles),
                        "already_queued_url_count": len(already_queued_urls),
                        "status": "queued",
                        "message": "harvest_profile_tiny_batch_deferred_for_coalescing",
                        "small_batch_reason": post_claim_small_batch_reason,
                        "tiny_tail_coalescing_min_age_ms": _harvest_profile_tiny_tail_coalescing_min_age_ms(),
                        "oldest_deferred_coalescing_age_ms": post_claim_oldest_deferred_coalescing_age_ms,
                        "coalescing_not_before_at": coalescing_not_before_at,
                        "coalescing_scheduler": "linkedin_profile_registry",
                        "coalescing_refill_items": coalescing_refill_items,
                        "claim_timing": "after_provider_slot_claim_contention",
                    },
                    "cached_profiles": cached_profiles,
                    "deferred_urls": list(dispatch_urls),
                }
            if not dispatch_urls:
                if self.store is not None and claim_contended_urls:
                    _mark_profile_prefetch_urls_deferred_for_dispatch_retry(
                        self.store,
                        claim_contended_urls,
                        source_shards_by_url={
                            profile_url: ["enrichment_background_prefetch"] for profile_url in claim_contended_urls
                        },
                        source_jobs=[job_id] if str(job_id or "").strip() else [],
                        snapshot_dir=snapshot_dir,
                        trigger_kind="profile_prefetch_lease_contention",
                        plan_reason="profile_registry_lease_contention",
                        deferred_reason="profile_registry_lease_contention",
                    )
                summary_status = "backpressure" if claim_contended_urls else "queued"
                summary_message = (
                    "profile_registry_lease_contention"
                    if claim_contended_urls
                    else "profile_urls_already_queued_or_claimed"
                )
                claim_contended_set = set(claim_contended_urls)
                queued_or_claimed_urls = [
                    profile_url for profile_url in already_queued_urls if profile_url not in claim_contended_set
                ]
                return {
                    "worker_status": summary_status,
                    "summary": {
                        "logical_name": "harvest_profile_scraper_batch",
                        "requested_url_count": len(normalized_urls),
                        "requested_urls": list(normalized_urls),
                        "queued_urls": queued_or_claimed_urls,
                        "deferred_urls": list(claim_contended_urls),
                        "dispatched_url_count": 0,
                        "reused_cached_count": len(cached_profiles),
                        "already_queued_url_count": len(already_queued_urls),
                        "status": "queued",
                        "message": summary_message,
                    },
                    "cached_profiles": cached_profiles,
                }

        def _release_url_claims() -> None:
            if self.store is None:
                return
            acquired_claim_urls = [
                profile_url
                for profile_url, lease_payload in list(url_claims.items())
                if bool(dict(lease_payload or {}).get("acquired"))
            ]
            if not acquired_claim_urls:
                url_claims.clear()
                return
            release_many = getattr(registry_repo, "release_leases", None)
            first_claim = dict(url_claims.get(acquired_claim_urls[0]) or {})
            release_owner = str(first_claim.get("lease_owner") or "")
            release_token = str(first_claim.get("lease_token") or "")
            can_batch_release = bool(
                release_owner
                and release_token
                and all(
                    str(dict(url_claims.get(profile_url) or {}).get("lease_owner") or "") == release_owner
                    and str(dict(url_claims.get(profile_url) or {}).get("lease_token") or "") == release_token
                    for profile_url in acquired_claim_urls
                )
            )
            if callable(release_many) and can_batch_release:
                release_many(
                    acquired_claim_urls,
                    lease_owner=release_owner,
                    lease_token=release_token,
                )
            else:
                for profile_url in acquired_claim_urls:
                    lease_payload = dict(url_claims.get(profile_url) or {})
                    self.store.repos.linkedin_profile_registry.release_lease(
                        profile_url,
                        lease_owner=str(lease_payload.get("lease_owner") or ""),
                        lease_token=str(lease_payload.get("lease_token") or ""),
                    )
            url_claims.clear()

        dispatch_claim_recorded = False
        dispatch_claim_record: dict[str, Any] = {}

        def _record_dispatch_claim_after_slot_confirmed() -> None:
            nonlocal dispatch_claim_recorded, dispatch_claim_record
            if dispatch_claim_recorded or resume_remote_run or self.store is None or not dispatch_urls:
                return
            # Replan/dispatch reservation is already serialized by the scheduler lock and
            # the URL leases above. Once a provider slot is confirmed, this hot path must
            # not re-enter the job/snapshot scheduler lock because that can hold an actor
            # slot idle before the provider submit starts.
            dispatch_claim_record = _mark_profile_prefetch_urls_dispatch_claimed(
                self.store,
                dispatch_urls,
                source_shards_by_url={profile_url: ["enrichment_background_prefetch"] for profile_url in dispatch_urls},
                source_jobs=[job_id] if str(job_id or "").strip() else [],
                snapshot_dir=snapshot_dir,
                trigger_kind="profile_prefetch_submit_claim",
                plan_reason="provider_slot_confirmed",
                owner_payload_hash=payload_hash,
            )
            recorded_count = _safe_int_value(dispatch_claim_record.get("active_item_count"))
            if recorded_count < len(dispatch_urls):
                raise RuntimeError(
                    "profile_prefetch_dispatch_claim_incomplete: "
                    f"recorded={recorded_count} expected={len(dispatch_urls)}"
                )
            dispatch_claim_recorded = True

        runtime_timing_overrides = _harvest_profile_batch_runtime_timing_overrides(request_payload)
        if resume_remote_run:
            payload_hash = resume_payload_hash
            provider_limiter_lease = dict(existing_resume_checkpoint.get("provider_limiter_lease") or {})
        else:
            payload_hash = sha1(json.dumps(sorted(dispatch_urls), ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
            try:
                provider_limiter_lease = acquire_runtime_provider_limiter_slot(
                    self.store,
                    limiter_key="harvest_profile_scraper_actor",
                    budget=resolved_harvest_profile_actor_global_inflight(runtime_timing_overrides),
                    lease_owner=f"harvest_profile_batch:{job_id}:{payload_hash}",
                    lease_seconds=7200,
                    metadata={
                        "source": "enrichment_background_prefetch",
                        "job_id": job_id,
                        "payload_hash": payload_hash,
                        "requested_url_count": len(dispatch_urls),
                    },
                    wait_timeout_seconds=0.0,
                    poll_seconds=0.05,
                )
            except RuntimeError as exc:
                if self.store is not None:
                    _mark_profile_prefetch_urls_deferred_for_dispatch_retry(
                        self.store,
                        dispatch_urls,
                        source_shards_by_url={
                            profile_url: ["enrichment_background_prefetch"] for profile_url in dispatch_urls
                        },
                        source_jobs=[job_id] if str(job_id or "").strip() else [],
                        snapshot_dir=snapshot_dir,
                        trigger_kind="profile_prefetch_provider_backpressure",
                        plan_reason="provider_limiter_backpressure",
                        deferred_reason="provider_limiter_backpressure",
                    )
                _release_url_claims()
                return {
                    "worker_status": "backpressure",
                    "summary": {
                        "logical_name": "harvest_profile_scraper_batch",
                        "requested_url_count": len(normalized_urls),
                        "requested_urls": list(normalized_urls),
                        "deferred_urls": list(dispatch_urls),
                        "dispatched_url_count": 0,
                        "reused_cached_count": len(cached_profiles),
                        "already_queued_url_count": len(already_queued_urls),
                        "status": "queued",
                        "message": "harvest_profile_provider_limiter_backpressure",
                        "provider_limiter": {
                            "limiter_key": "harvest_profile_scraper_actor",
                            "budget": resolved_harvest_profile_actor_global_inflight(runtime_timing_overrides),
                            "error": str(exc),
                        },
                    },
                    "cached_profiles": cached_profiles,
                    "deferred_urls": list(dispatch_urls),
                }
        try:
            worker_handle = self.worker_runtime.begin_worker(
                job_id=job_id,
                request=JobRequest.from_payload(request_payload),
                plan_payload=plan_payload,
                runtime_mode=runtime_mode,
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::{payload_hash}",
                stage="enriching",
                span_name=f"harvest_profile_batch:{payload_hash}",
                budget_payload={"requested_url_count": len(dispatch_urls)},
                input_payload={"profile_urls": list(dispatch_urls)},
                metadata={
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(snapshot_dir),
                    "profile_urls": list(dispatch_urls),
                    "request_payload": request_payload,
                    "plan_payload": plan_payload,
                    "runtime_mode": runtime_mode,
                    "allow_shared_provider_cache": allow_shared_provider_cache,
                    "prefetch_batch_context": context_payload,
                },
                handoff_from_lane="acquisition_specialist",
            )
        except Exception:
            if self.store is not None:
                _mark_profile_prefetch_urls_deferred_for_dispatch_retry(
                    self.store,
                    dispatch_urls,
                    source_shards_by_url={
                        profile_url: ["enrichment_background_prefetch"] for profile_url in dispatch_urls
                    },
                    source_jobs=[job_id] if str(job_id or "").strip() else [],
                    snapshot_dir=snapshot_dir,
                    trigger_kind="profile_prefetch_worker_begin_failed",
                    plan_reason="worker_begin_failed",
                    deferred_reason="worker_begin_failed",
                )
            _release_url_claims()
            release_runtime_provider_limiter_slot(self.store, provider_limiter_lease)
            raise
        existing = self.worker_runtime.get_worker(worker_handle.worker_id) or {}
        checkpoint = dict(existing.get("checkpoint") or {})
        if resume_remote_run and existing_resume_checkpoint:
            checkpoint = {**checkpoint, **existing_resume_checkpoint}
        output_payload = dict(existing.get("output") or {})
        if str(existing.get("status") or "") == "completed" and output_payload:
            if not self._profile_urls_have_cached_harvest_payloads(dispatch_urls, snapshot_dir):
                previous_checkpoint = dict(checkpoint)
                previous_summary = dict(output_payload.get("summary") or {})
                checkpoint = {
                    "stage": "replaying_incomplete_harvest_profile_batch",
                    "replay_reason": "completed_worker_missing_cached_harvest_profiles",
                    "previous_run_id": str(previous_checkpoint.get("run_id") or ""),
                    "previous_dataset_id": str(previous_checkpoint.get("dataset_id") or ""),
                    "previous_summary_path": str(previous_summary.get("summary_path") or ""),
                }
                output_payload = {
                    "previous_summary": previous_summary,
                    "replay_reason": "completed_worker_missing_cached_harvest_profiles",
                }
                self.worker_runtime.checkpoint_worker(
                    worker_handle,
                    status="running",
                    checkpoint_payload=checkpoint,
                    output_payload=output_payload,
                )
            else:
                _release_url_claims()
                release_runtime_provider_limiter_slot(self.store, provider_limiter_lease)
                self.worker_runtime.complete_worker(
                    worker_handle,
                    status="completed",
                    checkpoint_payload=checkpoint,
                    output_payload=output_payload,
                    handoff_to_lane="enrichment_specialist",
                )
                return {
                    "worker_status": "completed",
                    "summary": dict(output_payload.get("summary") or {}),
                    "daemon_action": "reused_output",
                }
        logger = AssetLogger(snapshot_dir)
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        artifact_default_path = harvest_dir / f"harvest_profile_batch_{payload_hash}.queue.json"
        artifact_paths = {
            str(key): str(value)
            for key, value in dict(checkpoint.get("artifact_paths") or {}).items()
            if str(key).strip()
        }
        summary_path = harvest_dir / f"harvest_profile_batch_{payload_hash}.queue_summary.json"
        if not provider_limiter_lease:
            provider_limiter_lease = dict(checkpoint.get("provider_limiter_lease") or {})
        terminal_replay = _load_completed_harvest_profile_batch_replay(summary_path)
        if terminal_replay:
            replay_summary = {
                **dict(terminal_replay.get("summary") or {}),
                "summary_path": str(summary_path),
                "replay_reason": "completed_queue_summary_recovered",
            }
            replay_artifact_paths = {
                **dict(terminal_replay.get("artifact_paths") or {}),
                **artifact_paths,
            }
            replay_checkpoint = {
                **checkpoint,
                "artifact_paths": replay_artifact_paths,
                "summary_path": str(summary_path),
                "stage": "completed",
                "status": "completed",
                "provider_limiter_lease": provider_limiter_lease,
                "replay_reason": "completed_queue_summary_recovered",
            }
            replay_run_id = str(terminal_replay.get("run_id") or "").strip()
            replay_dataset_id = str(terminal_replay.get("dataset_id") or "").strip()
            replay_payload_hash = str(terminal_replay.get("payload_hash") or "").strip()
            if replay_run_id:
                replay_checkpoint["run_id"] = replay_run_id
            if replay_dataset_id:
                replay_checkpoint["dataset_id"] = replay_dataset_id
            if replay_payload_hash:
                replay_checkpoint["payload_hash"] = replay_payload_hash
            return self._complete_harvest_profile_batch_worker_from_terminal_body(
                worker_handle=worker_handle,
                dispatch_urls=dispatch_urls,
                normalized_urls=normalized_urls,
                snapshot_dir=snapshot_dir,
                job_id=job_id,
                body=terminal_replay.get("body"),
                cached_profiles=cached_profiles,
                checkpoint=replay_checkpoint,
                output_payload={
                    "summary": replay_summary,
                    "terminal_replay": {
                        "status": "completed",
                        "source": "completed_queue_summary",
                        "dataset_items_path": str(terminal_replay.get("dataset_items_path") or ""),
                    },
                },
                provider_limiter_lease=provider_limiter_lease,
                release_url_claims=_release_url_claims,
            )
        if provider_limiter_lease and not resume_remote_run:
            try:
                self.worker_runtime.checkpoint_worker(
                    worker_handle,
                    status="running",
                    checkpoint_payload={
                        **checkpoint,
                        "stage": "submitting_remote_harvest",
                        "provider_limiter_lease": provider_limiter_lease,
                    },
                    output_payload=output_payload,
                )
                checkpoint = {
                    **checkpoint,
                    "stage": "submitting_remote_harvest",
                    "provider_limiter_lease": provider_limiter_lease,
                }
                _record_dispatch_claim_after_slot_confirmed()
            except Exception as exc:
                error_message = str(exc)
                if self.store is not None:
                    _mark_profile_prefetch_urls_deferred_for_dispatch_retry(
                        self.store,
                        dispatch_urls,
                        source_shards_by_url={
                            profile_url: ["enrichment_background_prefetch"] for profile_url in dispatch_urls
                        },
                        source_jobs=[job_id] if str(job_id or "").strip() else [],
                        snapshot_dir=snapshot_dir,
                        trigger_kind="profile_prefetch_submit_claim_failed",
                        plan_reason="provider_slot_confirmed_claim_failed",
                        deferred_reason="provider_slot_confirmed_claim_failed",
                    )
                _release_url_claims()
                release_runtime_provider_limiter_slot(self.store, provider_limiter_lease)
                failure_summary = {
                    "logical_name": "harvest_profile_scraper_batch",
                    "requested_url_count": len(normalized_urls),
                    "requested_urls": list(normalized_urls),
                    "failed_urls": list(dispatch_urls),
                    "deferred_urls": list(dispatch_urls),
                    "dispatched_url_count": 0,
                    "reused_cached_count": len(cached_profiles),
                    "status": "failed",
                    "message": error_message,
                    "retryable": True,
                    "dispatch_claim_lock_policy": "url_lease_plus_provider_slot_no_scheduler_lock",
                }
                self.worker_runtime.complete_worker(
                    worker_handle,
                    status="failed",
                    checkpoint_payload={
                        **checkpoint,
                        "stage": "failed",
                        "last_error": error_message,
                        "provider_limiter_lease": provider_limiter_lease,
                    },
                    output_payload={"summary": failure_summary},
                )
                return {
                    "worker_status": "failed",
                    "summary": failure_summary,
                    "cached_profiles": cached_profiles,
                    "failed_urls": list(dispatch_urls),
                    "deferred_urls": list(dispatch_urls),
                }

        try:
            if not provider_limiter_lease and not resume_remote_run:
                provider_limiter_lease = acquire_runtime_provider_limiter_slot(
                    self.store,
                    limiter_key="harvest_profile_scraper_actor",
                    budget=resolved_harvest_profile_actor_global_inflight(runtime_timing_overrides),
                    lease_owner=f"harvest_profile_batch:{job_id}:{payload_hash}",
                    lease_seconds=7200,
                    metadata={
                        "source": "enrichment_background_prefetch",
                        "job_id": job_id,
                        "payload_hash": payload_hash,
                        "requested_url_count": len(dispatch_urls),
                    },
                    wait_timeout_seconds=0.0,
                    poll_seconds=0.05,
                )
                _record_dispatch_claim_after_slot_confirmed()
            nonblocking_submit = bool(dict(prefetch_batch_context or {}).get("nonblocking_submit"))
            with runtime_inflight_slot(
                "harvest_profile_batch_submit",
                budget=resolved_harvest_profile_batch_submit_global_inflight(runtime_timing_overrides),
                metadata={"requested_url_count": len(dispatch_urls), "source": "enrichment_background_prefetch"},
                blocking=not nonblocking_submit,
            ) as submit_slot:
                if not bool(dict(submit_slot or {}).get("acquired", True)):
                    if self.store is not None:
                        _mark_profile_prefetch_urls_deferred_for_dispatch_retry(
                            self.store,
                            dispatch_urls,
                            source_shards_by_url={
                                profile_url: ["enrichment_background_prefetch"] for profile_url in dispatch_urls
                            },
                            source_jobs=[job_id] if str(job_id or "").strip() else [],
                            snapshot_dir=snapshot_dir,
                            trigger_kind="profile_prefetch_submit_slot_backpressure",
                            plan_reason="submit_slot_backpressure",
                            deferred_reason="submit_slot_backpressure",
                        )
                    _release_url_claims()
                    release_runtime_provider_limiter_slot(self.store, provider_limiter_lease)
                    return {
                        "worker_status": "backpressure",
                        "summary": {
                            "logical_name": "harvest_profile_scraper_batch",
                            "requested_url_count": len(normalized_urls),
                            "requested_urls": list(normalized_urls),
                            "deferred_urls": list(dispatch_urls),
                            "dispatched_url_count": 0,
                            "reused_cached_count": len(cached_profiles),
                            "already_queued_url_count": len(already_queued_urls),
                            "status": "queued",
                            "message": "harvest_profile_submit_slot_backpressure",
                            "submit_slot": dict(submit_slot or {}),
                        },
                        "cached_profiles": cached_profiles,
                        "deferred_urls": list(dispatch_urls),
                    }
                execution = self.harvest_profile_connector.execute_batch_with_checkpoint(
                    dispatch_urls,
                    snapshot_dir,
                    checkpoint=checkpoint,
                    allow_shared_provider_cache=allow_shared_provider_cache,
                    runtime_timing_overrides=runtime_timing_overrides,
                )
        except Exception as exc:
            error_message = str(exc)
            checkpoint_run_id = str(checkpoint.get("run_id") or "")
            checkpoint_dataset_id = str(checkpoint.get("dataset_id") or "")
            failure_summary = {
                "logical_name": "harvest_profile_scraper_batch",
                "requested_url_count": len(normalized_urls),
                "requested_urls": list(normalized_urls),
                "failed_urls": list(dispatch_urls),
                "dispatched_url_count": len(dispatch_urls),
                "unresolved_urls": list(dispatch_urls),
                "unresolved_url_count": len(dispatch_urls),
                "persisted_profile_count": 0,
                "reused_cached_count": len(cached_profiles),
                "status": "failed",
                "message": error_message,
                "artifact_paths": artifact_paths,
                "retryable": True,
                "terminal_envelope_outcome": "provider_failed_url_retry_recorded",
            }
            logger.write_json(
                summary_path,
                failure_summary,
                asset_type="harvest_profile_batch_queue_summary",
                source_kind="harvest_profile_scraper",
                is_raw_asset=False,
                model_safe=True,
            )
            updated_checkpoint = {
                **checkpoint,
                "artifact_paths": artifact_paths,
                "summary_path": str(summary_path),
                "stage": "failed",
                "status": "completed",
                "last_error": error_message,
                "provider_limiter_lease": provider_limiter_lease,
                "terminal_envelope_outcome": "provider_failed_url_retry_recorded",
            }
            updated_output = {"summary": {**failure_summary, "summary_path": str(summary_path)}}
            if self.store is not None:
                for profile_url in dispatch_urls:
                    self.store.repos.linkedin_profile_registry.mark_failed(
                        str(profile_url or ""),
                        error=error_message,
                        retryable=True,
                        source_shards=["enrichment_background_prefetch"],
                        source_jobs=[job_id] if str(job_id or "").strip() else [],
                        run_id=checkpoint_run_id,
                        dataset_id=checkpoint_dataset_id,
                        snapshot_dir=str(snapshot_dir),
                    )
            _release_url_claims()
            release_runtime_provider_limiter_slot(self.store, provider_limiter_lease)
            self.worker_runtime.complete_worker(
                worker_handle,
                status="completed",
                checkpoint_payload=updated_checkpoint,
                output_payload=updated_output,
            )
            return {
                "worker_status": "completed",
                "summary": updated_output["summary"],
                "cached_profiles": cached_profiles,
                "failed_urls": list(dispatch_urls),
                "terminal_envelope_outcome": "provider_failed_url_retry_recorded",
            }

        for artifact in list(execution.artifacts or []):
            artifact_path = write_harvest_execution_artifact(
                logger=logger,
                artifact=artifact,
                default_path=artifact_default_path,
                asset_type="harvest_profile_batch_queue_payload",
                source_kind="harvest_profile_scraper",
                metadata={
                    "logical_name": execution.logical_name,
                    "requested_url_count": len(dispatch_urls),
                    "payload_hash": payload_hash,
                },
            )
            artifact_paths[str(artifact.label)] = str(artifact_path)
        if (
            not execution.pending
            and execution.body is not None
            and not str(artifact_paths.get("dataset_items") or "").strip()
        ):
            dataset_items_path = harvest_dir / f"harvest_profile_batch_{payload_hash}.queue_dataset_items.json"
            logger.write_json(
                dataset_items_path,
                execution.body,
                asset_type="harvest_profile_batch_queue_dataset_items",
                source_kind="harvest_profile_scraper",
                is_raw_asset=True,
                model_safe=False,
                metadata={
                    "logical_name": execution.logical_name,
                    "requested_url_count": len(dispatch_urls),
                    "payload_hash": payload_hash,
                    "terminal_replay_source": "sync_terminal_body",
                },
            )
            artifact_paths["dataset_items"] = str(dataset_items_path)
        execution_checkpoint = dict(execution.checkpoint or {})
        run_id, dataset_id = _harvest_execution_remote_identifiers(execution_checkpoint, artifact_paths)
        summary = {
            "logical_name": execution.logical_name,
            "requested_url_count": len(normalized_urls),
            "requested_urls": list(normalized_urls),
            "queued_urls": list(dispatch_urls),
            "dispatched_url_count": len(dispatch_urls),
            "reused_cached_count": len(cached_profiles),
            "status": "queued" if execution.pending else "completed",
            "message": str(execution.message or ""),
            "worker_id": int(worker_handle.worker_id),
            "run_id": run_id,
            "dataset_id": dataset_id,
            "payload_hash": payload_hash,
            "small_batch_reason": _profile_batch_envelope_small_batch_reason(
                batch_size=len(dispatch_urls),
                requested_url_count=max(0, int(requested_context_count or 0)),
                candidate_count=max(0, int(candidate_context_count or 0)),
                deferred_url_count=max(0, int(planned_deferred_context_count or 0)),
                queue_quiescent=bool(under_target_final_tail_authorized),
            ),
            "artifact_paths": artifact_paths,
            "provider_limiter": {
                "limiter_key": str(provider_limiter_lease.get("limiter_key") or ""),
                "active_count": int(provider_limiter_lease.get("active_count") or 0),
                "budget": int(provider_limiter_lease.get("budget") or 0),
                "wait_ms": provider_limiter_lease.get("wait_ms"),
            },
            "dispatch_claim_lock_policy": "url_lease_plus_provider_slot_no_scheduler_lock",
            "dispatch_claim_recorded": bool(dispatch_claim_recorded),
            "dispatch_claim_record": dispatch_claim_record,
        }
        summary["small_batch_reason"] = _worker_effective_small_batch_reason(
            str(summary.get("small_batch_reason") or ""),
            dispatch_url_count=len(dispatch_urls),
        )
        logger.write_json(
            summary_path,
            summary,
            asset_type="harvest_profile_batch_queue_summary",
            source_kind="harvest_profile_scraper",
            is_raw_asset=False,
            model_safe=True,
        )
        next_stage = "waiting_remote_harvest" if execution.pending else "completed"
        updated_checkpoint = {
            **execution_checkpoint,
            "artifact_paths": artifact_paths,
            "summary_path": str(summary_path),
            "stage": next_stage,
            "provider_limiter_lease": provider_limiter_lease,
            "prefetch_batch_context": context_payload,
        }
        # Stamp the moment we hand a worker off to the remote provider so workflow_efficiency
        # can compute an idle-window metric (`time_since_remote_wait_started_ms`) without
        # relying on the lease `created_at` (which may pre-date the actual remote submit if
        # the lease was reused after a checkpointed retry). Diagnostic only.
        if next_stage == "waiting_remote_harvest" and not execution_checkpoint.get("remote_wait_started_at"):
            updated_checkpoint["remote_wait_started_at"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S+00:00"
            )
        if run_id:
            updated_checkpoint["run_id"] = run_id
        if dataset_id:
            updated_checkpoint["dataset_id"] = dataset_id
        updated_output = {"summary": {**summary, "summary_path": str(summary_path)}}
        if execution.pending:
            if self.store is not None:
                _mark_profile_prefetch_urls_dispatch_owned(
                    self.store,
                    dispatch_urls,
                    source_shards_by_url={
                        profile_url: ["enrichment_background_prefetch"] for profile_url in dispatch_urls
                    },
                    source_jobs=[job_id] if str(job_id or "").strip() else [],
                    snapshot_dir=snapshot_dir,
                    trigger_kind=(
                        PROFILE_REFILL_RETRY_PROVIDER_SUBMIT_TRIGGER_KIND
                        if bool(dict(prefetch_batch_context or {}).get("retry_isolation"))
                        else "profile_prefetch_provider_submit"
                    ),
                    plan_reason=(
                        PROFILE_REFILL_RETRY_PROVIDER_SUBMITTED_PLAN_REASON
                        if bool(dict(prefetch_batch_context or {}).get("retry_isolation"))
                        else "remote_provider_submitted"
                    ),
                    owner_worker_id=int(worker_handle.worker_id),
                    owner_run_id=run_id,
                    owner_dataset_id=dataset_id,
                    owner_payload_hash=payload_hash,
                )
                queued_many = getattr(registry_repo, "mark_queued_many", None)
                if callable(queued_many):
                    queued_many(
                        dispatch_urls,
                        source_shards=["enrichment_background_prefetch"],
                        source_jobs=[job_id] if str(job_id or "").strip() else [],
                        run_id=run_id,
                        dataset_id=dataset_id,
                        snapshot_dir=str(snapshot_dir),
                    )
                else:
                    for profile_url in dispatch_urls:
                        self.store.repos.linkedin_profile_registry.mark_queued(
                            profile_url,
                            source_shards=["enrichment_background_prefetch"],
                            source_jobs=[job_id] if str(job_id or "").strip() else [],
                            run_id=run_id,
                            dataset_id=dataset_id,
                            snapshot_dir=str(snapshot_dir),
                        )
            refreshed_worker = self.worker_runtime.get_worker(worker_handle.worker_id) or {}
            if str(refreshed_worker.get("status") or "").strip().lower() == "completed":
                _release_url_claims()
                release_runtime_provider_limiter_slot(self.store, provider_limiter_lease)
                return {
                    "worker_status": "completed",
                    "summary": dict(
                        dict(refreshed_worker.get("output") or {}).get("summary") or updated_output["summary"]
                    ),
                    "cached_profiles": cached_profiles,
                    "daemon_action": "remote_event_completed_during_submit",
                }
            _release_url_claims()
            self.worker_runtime.complete_worker(
                worker_handle,
                status="queued",
                checkpoint_payload=updated_checkpoint,
                output_payload=updated_output,
            )
            existing_watcher = dict(updated_checkpoint.get("local_provider_event_watcher") or {})
            if _local_provider_event_watcher_lease_is_active(
                dict(existing_watcher.get("watcher_lease") or existing_watcher),
                run_id=run_id,
                dataset_id=dataset_id,
                worker_id=int(worker_handle.worker_id),
            ):
                watcher = {
                    **existing_watcher,
                    "status": "already_scheduled",
                    "reason": "local_provider_event_watcher_lease_active",
                }
            elif _local_provider_event_watcher_terminal_marker_present(
                updated_checkpoint,
                run_id=run_id,
                dataset_id=dataset_id,
            ):
                watcher = {
                    "status": "skipped",
                    "reason": "remote_provider_terminal_event_already_seen",
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "worker_id": int(worker_handle.worker_id),
                }
            else:
                watcher = self._schedule_local_provider_event_watcher(
                    run_id=run_id,
                    dataset_id=dataset_id,
                    worker_id=int(worker_handle.worker_id),
                    job_id=job_id,
                    payload_hash=payload_hash,
                    snapshot_dir=snapshot_dir,
                    runtime_timing_overrides={
                        **dict(runtime_timing_overrides or {}),
                        **{
                            key: value
                            for key, value in updated_checkpoint.items()
                            if key
                            in {
                                "scripted_remote_wait_after_submit",
                                "scripted_remote_wait_seconds",
                                "scripted_remote_ready_epoch_ms",
                            }
                        },
                    },
                )
            if str(watcher.get("status") or "") == "scheduled":
                updated_checkpoint["local_provider_event_watcher"] = dict(watcher)
                updated_output["summary"]["local_provider_event_watcher"] = watcher
                self.worker_runtime.checkpoint_worker(
                    worker_handle,
                    status="queued",
                    checkpoint_payload=updated_checkpoint,
                    output_payload=updated_output,
                )
            elif str(watcher.get("status") or "") == "already_scheduled":
                updated_output["summary"]["local_provider_event_watcher"] = watcher
            return {
                "worker_status": "queued",
                "summary": updated_output["summary"],
                "cached_profiles": cached_profiles,
                "local_provider_event_watcher": watcher,
            }

        return self._complete_harvest_profile_batch_worker_from_terminal_body(
            worker_handle=worker_handle,
            dispatch_urls=dispatch_urls,
            normalized_urls=normalized_urls,
            snapshot_dir=snapshot_dir,
            job_id=job_id,
            body=execution.body,
            cached_profiles=cached_profiles,
            checkpoint=updated_checkpoint,
            output_payload=updated_output,
            provider_limiter_lease=provider_limiter_lease,
            release_url_claims=_release_url_claims,
        )

    def _complete_harvest_profile_batch_worker_from_terminal_body(
        self,
        *,
        worker_handle: Any,
        dispatch_urls: list[str],
        normalized_urls: list[str],
        snapshot_dir: Path,
        job_id: str,
        body: Any,
        cached_profiles: dict[str, dict[str, Any]],
        checkpoint: dict[str, Any],
        output_payload: dict[str, Any],
        provider_limiter_lease: dict[str, Any] | None,
        release_url_claims: Callable[[], None],
    ) -> dict[str, Any]:
        updated_checkpoint = dict(checkpoint or {})
        updated_output = dict(output_payload or {})
        updated_summary = dict(updated_output.get("summary") or {})
        persisted_profile_count = 0
        unresolved_urls: list[str] = []
        run_id = str(updated_checkpoint.get("run_id") or "")
        dataset_id = str(updated_checkpoint.get("dataset_id") or "")
        payload_hash = str(updated_checkpoint.get("payload_hash") or "").strip()

        def _write_terminal_registry_entries(
            entries: list[dict[str, Any]],
            *,
            fetched_fallback: bool = False,
            terminal_scope: str = "",
        ) -> dict[str, Any]:
            if self.store is None or not entries:
                return {"status": "skipped", "reason": "store_or_entries_unavailable", "entry_count": 0}
            command = self._plan_linkedin_profile_url_terminal_record_command(
                job_id=job_id,
                snapshot_dir=snapshot_dir,
                entries=entries,
                terminal_scope=terminal_scope,
                actor="harvest_profile_batch_worker",
                source="_complete_harvest_profile_batch_worker_from_terminal_body",
            )
            if command:
                result = self.run_linkedin_profile_url_terminal_record_command_once(command)
                return {
                    **dict(result or {}),
                    "workflow_command": self._workflow_command_observation(
                        dict(result.get("workflow_command") or command),
                        migration_phase="W2c_profile_url_terminal_record",
                    ),
                }
            # Migration bridge for lightweight fake stores that do not expose the
            # durable runtime tables. ControlPlaneStore normal paths should use
            # the command owner above.
            if hasattr(self.store, "append_workflow_event") or hasattr(self.store, "upsert_workflow_command"):
                return {
                    "status": "failed",
                    "reason": "profile_url_terminal_record_command_planning_failed",
                    "entry_count": len(entries),
                    "migration_phase": "W2c_profile_url_terminal_record",
                    "legacy_bridge_used": False,
                }
            registry_repo = linkedin_profile_registry_repo(self.store)
            batch_writer = getattr(registry_repo, "backfill_batch", None)
            if callable(batch_writer):
                recorded_count = int(batch_writer(entries) or 0)
                return {
                    "status": "completed",
                    "reason": "legacy_fake_store_terminal_record_bridge",
                    "entry_count": len(entries),
                    "recorded_count": recorded_count,
                    "migration_phase": "W2c_profile_url_terminal_record",
                    "legacy_bridge_used": True,
                }
            recorded_count = 0
            for entry in entries:
                profile_url = str(entry.get("profile_url") or "")
                if fetched_fallback:
                    self.store.repos.linkedin_profile_registry.mark_fetched(
                        profile_url,
                        raw_path=str(entry.get("raw_path") or ""),
                        source_shards=list(entry.get("source_shards") or []),
                        source_jobs=list(entry.get("source_jobs") or []),
                        run_id=str(entry.get("run_id") or ""),
                        dataset_id=str(entry.get("dataset_id") or ""),
                        snapshot_dir=str(entry.get("snapshot_dir") or ""),
                    )
                    recorded_count += 1
                else:
                    self.store.repos.linkedin_profile_registry.mark_failed(
                        profile_url,
                        error=str(entry.get("error") or ""),
                        retryable=bool(entry.get("retryable")),
                        source_shards=list(entry.get("source_shards") or []),
                        source_jobs=list(entry.get("source_jobs") or []),
                        run_id=str(entry.get("run_id") or ""),
                        dataset_id=str(entry.get("dataset_id") or ""),
                        snapshot_dir=str(entry.get("snapshot_dir") or ""),
                    )
                    recorded_count += 1
            return {
                "status": "completed",
                "reason": "legacy_fake_store_terminal_record_bridge",
                "entry_count": len(entries),
                "recorded_count": recorded_count,
                "migration_phase": "W2c_profile_url_terminal_record",
                "legacy_bridge_used": True,
            }

        if body is not None:
            logger = AssetLogger(snapshot_dir)
            progress = dict(updated_checkpoint.get("terminal_persist_progress") or {})
            processed_url_count = min(
                len(dispatch_urls),
                max(0, _safe_int_value(progress.get("processed_url_count"))),
            )
            persisted_profile_count = max(0, _safe_int_value(progress.get("persisted_profile_count")))
            unresolved_urls = [
                str(item or "").strip()
                for item in list(progress.get("unresolved_urls") or [])
                if str(item or "").strip()
            ]
            terminal_record_history = [
                dict(item or {})
                for item in list(
                    progress.get("profile_url_terminal_record_commands")
                    or updated_summary.get("profile_url_terminal_record_commands")
                    or []
                )
                if isinstance(item, dict)
            ]
            if terminal_record_history:
                updated_summary["profile_url_terminal_record_commands"] = terminal_record_history
                updated_summary["profile_url_terminal_record_command_count"] = len(terminal_record_history)
                updated_summary["profile_url_terminal_recorded_count"] = sum(
                    int(item.get("recorded_count") or 0) for item in terminal_record_history
                )
            started_monotonic = time.perf_counter()
            processed_this_invocation = 0
            chunk_limit = _harvest_profile_terminal_persist_chunk_urls()
            url_limit = _harvest_profile_terminal_persist_url_limit()
            budget_ms = _harvest_profile_terminal_persist_budget_ms()

            def _elapsed_ms() -> int:
                return int(max(0.0, (time.perf_counter() - started_monotonic) * 1000))

            while processed_url_count < len(dispatch_urls) and processed_this_invocation < url_limit:
                if processed_this_invocation > 0 and budget_ms > 0 and _elapsed_ms() >= budget_ms:
                    break
                remaining_budget = max(1, url_limit - processed_this_invocation)
                chunk_urls = dispatch_urls[
                    processed_url_count : min(
                        len(dispatch_urls),
                        processed_url_count + min(chunk_limit, remaining_budget),
                    )
                ]
                if not chunk_urls:
                    break
                persisted = self.harvest_profile_connector.persist_profiles_from_batch_body(
                    chunk_urls,
                    body,
                    snapshot_dir,
                    asset_logger=logger,
                )
                persisted_profiles = dict(persisted.get("profiles") or {})
                chunk_unresolved_urls = [
                    str(item) for item in list(persisted.get("unresolved_urls") or []) if str(item).strip()
                ]
                persisted_profile_count += len(persisted_profiles)
                unresolved_urls = _dedupe_strings([*unresolved_urls, *chunk_unresolved_urls])
                if self.store is not None:
                    fetched_entries = [
                        {
                            "profile_url": str(profile_url or ""),
                            "status": "fetched",
                            "raw_path": str(dict(payload or {}).get("raw_path") or ""),
                            "source_shards": ["enrichment_background_prefetch"],
                            "source_jobs": [job_id] if str(job_id or "").strip() else [],
                            "run_id": run_id,
                            "dataset_id": dataset_id,
                            "snapshot_dir": str(snapshot_dir),
                        }
                        for profile_url, payload in persisted_profiles.items()
                        if str(profile_url or "").strip()
                    ]
                    failed_entries = [
                        {
                            "profile_url": str(profile_url or ""),
                            "status": "failed_retryable",
                            "error": "background_prefetch_unresolved",
                            "retryable": True,
                            "source_shards": ["enrichment_background_prefetch"],
                            "source_jobs": [job_id] if str(job_id or "").strip() else [],
                            "run_id": run_id,
                            "dataset_id": dataset_id,
                            "snapshot_dir": str(snapshot_dir),
                        }
                        for profile_url in chunk_unresolved_urls
                        if str(profile_url or "").strip()
                    ]
                    terminal_record_results = [
                        _write_terminal_registry_entries(
                            fetched_entries,
                            fetched_fallback=True,
                            terminal_scope=f"profile_worker:{int(getattr(worker_handle, 'worker_id', 0) or 0)}:fetched:{processed_url_count}",
                        ),
                        _write_terminal_registry_entries(
                            failed_entries,
                            fetched_fallback=False,
                            terminal_scope=f"profile_worker:{int(getattr(worker_handle, 'worker_id', 0) or 0)}:failed:{processed_url_count}",
                        ),
                    ]
                    terminal_record_results = [
                        dict(item or {})
                        for item in terminal_record_results
                        if int(dict(item or {}).get("entry_count") or 0) > 0
                    ]
                    if terminal_record_results:
                        existing_records = list(terminal_record_history)
                        updated_summary["profile_url_terminal_record_commands"] = [
                            *existing_records,
                            *terminal_record_results,
                        ]
                        terminal_record_history = list(updated_summary["profile_url_terminal_record_commands"])
                        updated_summary["profile_url_terminal_record_command_count"] = len(
                            updated_summary["profile_url_terminal_record_commands"]
                        )
                        updated_summary["profile_url_terminal_recorded_count"] = sum(
                            int(item.get("recorded_count") or 0)
                            for item in updated_summary["profile_url_terminal_record_commands"]
                        )
                    failed_terminal_records = [
                        dict(item)
                        for item in terminal_record_results
                        if str(item.get("status") or "").strip() == "failed"
                    ]
                    pending_terminal_records = [
                        dict(item)
                        for item in terminal_record_results
                        if int(item.get("recorded_count") or 0) < int(item.get("entry_count") or 0)
                        and str(item.get("status") or "").strip() != "failed"
                    ]
                    if failed_terminal_records or pending_terminal_records:
                        release_url_claims()
                        release_runtime_provider_limiter_slot(self.store, provider_limiter_lease)
                        updated_summary["status"] = "running"
                        updated_summary["message"] = (
                            "profile_url_terminal_record_command_failed"
                            if failed_terminal_records
                            else "profile_url_terminal_record_command_pending"
                        )
                        if failed_terminal_records:
                            updated_summary["profile_url_terminal_record_failures"] = failed_terminal_records
                        if pending_terminal_records:
                            updated_summary["profile_url_terminal_record_pending"] = pending_terminal_records
                        updated_output["summary"] = updated_summary
                        self.worker_runtime.checkpoint_worker(
                            worker_handle,
                            status="running",
                            checkpoint_payload={
                                **updated_checkpoint,
                                "stage": "persisting_terminal_harvest_profiles",
                                "status": "running",
                                "provider_limiter_lease": provider_limiter_lease,
                            },
                            output_payload=updated_output,
                        )
                        return {
                            "worker_status": "running",
                            "summary": updated_output["summary"],
                            "cached_profiles": cached_profiles,
                            "terminal_record_failed": bool(failed_terminal_records),
                            "terminal_record_pending": bool(pending_terminal_records),
                        }
                processed_url_count += len(chunk_urls)
                processed_this_invocation += len(chunk_urls)

            updated_output["persisted_profile_count"] = persisted_profile_count
            updated_output["unresolved_urls"] = unresolved_urls
            updated_summary["persisted_profile_count"] = persisted_profile_count
            updated_summary["unresolved_url_count"] = len(unresolved_urls)
            updated_summary["terminal_persist_progress"] = {
                "schema_version": 1,
                "processed_url_count": processed_url_count,
                "remaining_url_count": max(0, len(dispatch_urls) - processed_url_count),
                "processed_url_count_this_invocation": processed_this_invocation,
                "requested_url_count": len(dispatch_urls),
                "persisted_profile_count": persisted_profile_count,
                "unresolved_url_count": len(unresolved_urls),
                "chunk_url_limit": chunk_limit,
                "invocation_url_limit": url_limit,
                "budget_ms": budget_ms,
            }
            updated_output["summary"] = updated_summary
            if processed_url_count < len(dispatch_urls):
                remaining_urls = dispatch_urls[processed_url_count:]
                terminal_progress = {
                    "schema_version": 1,
                    "stage": "persisting_terminal_harvest_profiles",
                    "processed_url_count": processed_url_count,
                    "remaining_url_count": len(remaining_urls),
                    "requested_url_count": len(dispatch_urls),
                    "persisted_profile_count": persisted_profile_count,
                    "unresolved_urls": unresolved_urls,
                    "unresolved_url_count": len(unresolved_urls),
                    "profile_url_terminal_record_commands": terminal_record_history,
                    "profile_url_terminal_record_command_count": len(terminal_record_history),
                    "profile_url_terminal_recorded_count": sum(
                        int(item.get("recorded_count") or 0) for item in terminal_record_history
                    ),
                    "last_invocation_url_count": processed_this_invocation,
                    "chunk_url_limit": chunk_limit,
                    "invocation_url_limit": url_limit,
                    "budget_ms": budget_ms,
                    "elapsed_ms": _elapsed_ms(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                if self.store is not None and remaining_urls:
                    _mark_profile_prefetch_urls_dispatch_owned(
                        self.store,
                        remaining_urls,
                        source_shards_by_url={
                            profile_url: ["enrichment_background_prefetch"] for profile_url in remaining_urls
                        },
                        source_jobs=[job_id] if str(job_id or "").strip() else [],
                        snapshot_dir=snapshot_dir,
                        trigger_kind="profile_terminal_persist_partial",
                        plan_reason="terminal_persist_pending",
                        owner_worker_id=int(getattr(worker_handle, "worker_id", 0) or 0),
                        owner_run_id=run_id,
                        owner_dataset_id=dataset_id,
                        owner_payload_hash=payload_hash,
                    )
                release_url_claims()
                release_runtime_provider_limiter_slot(self.store, provider_limiter_lease)
                updated_summary["status"] = "running"
                updated_summary["message"] = "terminal_harvest_profile_persist_partial"
                updated_summary["terminal_persist_progress"] = {
                    **dict(updated_summary.get("terminal_persist_progress") or {}),
                    **terminal_progress,
                    "unresolved_urls": unresolved_urls,
                }
                updated_output["summary"] = updated_summary
                self.worker_runtime.checkpoint_worker(
                    worker_handle,
                    status="running",
                    checkpoint_payload={
                        **updated_checkpoint,
                        "stage": "persisting_terminal_harvest_profiles",
                        "status": "running",
                        "terminal_persist_progress": terminal_progress,
                        "provider_limiter_lease": provider_limiter_lease,
                    },
                    output_payload=updated_output,
                )
                return {
                    "worker_status": "running",
                    "summary": updated_output["summary"],
                    "cached_profiles": cached_profiles,
                    "terminal_persist_partial": True,
                    "terminal_persist_progress": terminal_progress,
                }
            updated_checkpoint.pop("terminal_persist_progress", None)
        elif self.store is not None:
            failed_entries = [
                {
                    "profile_url": str(profile_url or ""),
                    "status": "failed_retryable",
                    "error": "background_prefetch_empty_response",
                    "retryable": True,
                    "source_shards": ["enrichment_background_prefetch"],
                    "source_jobs": [job_id] if str(job_id or "").strip() else [],
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "snapshot_dir": str(snapshot_dir),
                }
                for profile_url in dispatch_urls
                if str(profile_url or "").strip()
            ]
            terminal_record = _write_terminal_registry_entries(
                failed_entries,
                fetched_fallback=False,
                terminal_scope=f"profile_worker:{int(getattr(worker_handle, 'worker_id', 0) or 0)}:empty_response",
            )
            if int(dict(terminal_record or {}).get("entry_count") or 0) > 0:
                updated_summary["profile_url_terminal_record_commands"] = [dict(terminal_record or {})]
                updated_summary["profile_url_terminal_record_command_count"] = 1
                updated_summary["profile_url_terminal_recorded_count"] = int(
                    dict(terminal_record or {}).get("recorded_count") or 0
                )
                updated_output["summary"] = updated_summary
            terminal_record_payload = dict(terminal_record or {})
            if str(terminal_record_payload.get("status") or "") == "failed" or int(
                terminal_record_payload.get("recorded_count") or 0
            ) < int(terminal_record_payload.get("entry_count") or 0):
                release_url_claims()
                release_runtime_provider_limiter_slot(self.store, provider_limiter_lease)
                updated_summary["status"] = "running"
                updated_summary["message"] = (
                    "profile_url_terminal_record_command_failed"
                    if str(terminal_record_payload.get("status") or "") == "failed"
                    else "profile_url_terminal_record_command_pending"
                )
                if str(terminal_record_payload.get("status") or "") == "failed":
                    updated_summary["profile_url_terminal_record_failures"] = [terminal_record_payload]
                else:
                    updated_summary["profile_url_terminal_record_pending"] = [terminal_record_payload]
                updated_output["summary"] = updated_summary
                self.worker_runtime.checkpoint_worker(
                    worker_handle,
                    status="running",
                    checkpoint_payload={
                        **updated_checkpoint,
                        "stage": "persisting_terminal_harvest_profiles",
                        "status": "running",
                        "provider_limiter_lease": provider_limiter_lease,
                    },
                    output_payload=updated_output,
                )
                return {
                    "worker_status": "running",
                    "summary": updated_output["summary"],
                    "cached_profiles": cached_profiles,
                    "terminal_record_failed": str(terminal_record_payload.get("status") or "") == "failed",
                    "terminal_record_pending": str(terminal_record_payload.get("status") or "") != "failed",
                }

        release_url_claims()
        release_runtime_provider_limiter_slot(self.store, provider_limiter_lease)
        if self.store is not None:
            latest_worker = self.store.get_agent_worker(worker_id=int(worker_handle.worker_id)) or {}
            latest_output = dict(dict(latest_worker).get("output") or {})
            # Terminal summary replay can race with event-level local-apply closure.
            # Re-completing the worker must not erase durable consumption markers
            # that were written while the replay callback was persisting profiles.
            for marker_key in ("inline_incremental_apply", "inline_incremental_ingest"):
                if marker_key not in updated_output and latest_output.get(marker_key):
                    updated_output[marker_key] = latest_output.get(marker_key)
        self.worker_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload={**updated_checkpoint, "stage": "completed", "status": "completed"},
            output_payload=updated_output,
            handoff_to_lane="enrichment_specialist",
        )
        return {
            "worker_status": "completed",
            "summary": updated_output["summary"],
            "cached_profiles": cached_profiles,
        }

    def _fetch_harvest_profiles_for_urls(
        self,
        profile_urls: list[str],
        snapshot_dir: Path,
        *,
        asset_logger: AssetLogger,
        prefetched_harvest_profiles: dict[str, dict[str, Any]] | None = None,
        background_prefetch_urls: set[str] | None = None,
        allow_shared_provider_cache: bool = True,
        source_shards_by_url: dict[str, list[str]] | None = None,
        source_jobs: list[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        normalized_urls: list[str] = []
        for profile_url in profile_urls:
            normalized_profile_url = str(profile_url or "").strip()
            if normalized_profile_url and normalized_profile_url not in normalized_urls:
                normalized_urls.append(normalized_profile_url)
        if self.harvest_profile_connector is None or not normalized_urls:
            return {}

        pending_prefetch_urls = {
            str(item or "").strip() for item in list(background_prefetch_urls or set()) if str(item or "").strip()
        }
        source_shards_by_url = {
            str(profile_url or "").strip(): list(values or [])
            for profile_url, values in dict(source_shards_by_url or {}).items()
            if str(profile_url or "").strip()
        }
        normalized_source_jobs = [
            str(item or "").strip() for item in list(source_jobs or []) if str(item or "").strip()
        ]
        registry_entries: dict[str, dict[str, Any]] = {}
        if self.store is not None:
            registry_entries = self.store.repos.linkedin_profile_registry.get_bulk(normalized_urls)
        fetched: dict[str, dict[str, Any]] = {}
        scheduler_required_urls: list[str] = []

        def _record_event(
            profile_url: str,
            *,
            event_type: str,
            event_status: str = "",
            detail: str = "",
            metadata: dict[str, Any] | None = None,
            duration_ms: int | None = None,
        ) -> None:
            if self.store is None:
                return
            self.store.repos.linkedin_profile_registry.record_event(
                profile_url,
                event_type=event_type,
                event_status=event_status,
                detail=detail,
                metadata=metadata or {},
                duration_ms=duration_ms,
            )

        def _wait_for_contended_fetches(contended_urls: dict[str, str]) -> None:
            if self.store is None or not contended_urls:
                return
            remaining = dict(contended_urls)
            deadline = time.monotonic() + PROFILE_REGISTRY_LEASE_WAIT_SECONDS
            while remaining and time.monotonic() < deadline:
                resolved_any = False
                for profile_url, normalized_registry_key in list(remaining.items()):
                    registry_entry = self.store.repos.linkedin_profile_registry.get(profile_url) or {}
                    registry_status = str(registry_entry.get("status") or "").strip().lower()
                    cached = _load_harvest_profile_payload_from_registry_or_snapshot(
                        registry_entry=registry_entry,
                        snapshot_dir=snapshot_dir,
                        profile_url=profile_url,
                        normalized_profile_key=normalized_registry_key,
                    )
                    if cached is not None:
                        fetched[profile_url] = cached
                        _record_event(profile_url, event_type="cache_hit_lease_wait")
                        remaining.pop(profile_url, None)
                        resolved_any = True
                        continue
                    if registry_status == "unrecoverable":
                        remaining.pop(profile_url, None)
                        resolved_any = True
                if remaining and not resolved_any:
                    time.sleep(PROFILE_REGISTRY_LEASE_POLL_SECONDS)
            for profile_url in remaining:
                _record_event(profile_url, event_type="lease_contended_skip")

        contended_urls: dict[str, str] = {}
        for normalized_profile_url in normalized_urls:
            profile = None
            if prefetched_harvest_profiles is not None:
                profile = prefetched_harvest_profiles.get(normalized_profile_url)
            if profile is not None:
                fetched[normalized_profile_url] = profile
                if self.store is not None:
                    alias_metadata = _profile_registry_alias_metadata(normalized_profile_url, dict(profile or {}))
                    self.store.repos.linkedin_profile_registry.mark_fetched(
                        normalized_profile_url,
                        raw_path=str(dict(profile or {}).get("raw_path") or ""),
                        source_shards=list(source_shards_by_url.get(normalized_profile_url) or []),
                        source_jobs=normalized_source_jobs,
                        alias_urls=list(alias_metadata.get("alias_urls") or []),
                        raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or normalized_profile_url),
                        sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                        snapshot_dir=str(snapshot_dir),
                    )
                    _record_event(normalized_profile_url, event_type="cache_hit_prefetched")
                continue
            if normalized_profile_url in pending_prefetch_urls:
                if self.store is not None:
                    self.store.repos.linkedin_profile_registry.mark_queued(
                        normalized_profile_url,
                        source_shards=list(source_shards_by_url.get(normalized_profile_url) or []),
                        source_jobs=normalized_source_jobs,
                        snapshot_dir=str(snapshot_dir),
                    )
                    _record_event(normalized_profile_url, event_type="lookup_pending_prefetch")
                continue
            registry_key = normalized_profile_url
            registry_status = ""
            if self.store is not None:
                registry_key = normalize_linkedin_profile_url_key(normalized_profile_url)
                registry_entry = dict(registry_entries.get(registry_key) or {})
                registry_status = str(registry_entry.get("status") or "").strip().lower()
                _record_event(
                    normalized_profile_url,
                    event_type="lookup_attempt",
                    event_status=registry_status,
                    metadata={"source_shards": list(source_shards_by_url.get(normalized_profile_url) or [])},
                )
                cached = _load_harvest_profile_payload_from_registry_or_snapshot(
                    registry_entry=registry_entry,
                    snapshot_dir=snapshot_dir,
                    profile_url=normalized_profile_url,
                    normalized_profile_key=registry_key,
                )
                if cached is not None:
                    fetched[normalized_profile_url] = cached
                    alias_metadata = _profile_registry_alias_metadata(normalized_profile_url, cached)
                    self.store.repos.linkedin_profile_registry.mark_fetched(
                        normalized_profile_url,
                        raw_path=str(cached.get("raw_path") or ""),
                        source_shards=list(source_shards_by_url.get(normalized_profile_url) or []),
                        source_jobs=normalized_source_jobs,
                        alias_urls=list(alias_metadata.get("alias_urls") or []),
                        raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or normalized_profile_url),
                        sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                        snapshot_dir=str(snapshot_dir),
                    )
                    _record_event(
                        normalized_profile_url,
                        event_type="cache_hit_registry" if registry_status == "fetched" else "cache_hit_local_raw",
                    )
                    continue
                if registry_status == "fetched":
                    self.store.repos.linkedin_profile_registry.mark_failed(
                        normalized_profile_url,
                        error="registry_cached_raw_missing_or_invalid",
                        retryable=True,
                        source_shards=list(source_shards_by_url.get(normalized_profile_url) or []),
                        source_jobs=normalized_source_jobs,
                        snapshot_dir=str(snapshot_dir),
                    )
                if registry_status == "queued":
                    if self._queued_profile_registry_entry_has_active_worker(
                        normalized_profile_url,
                        registry_entry,
                        job_id=normalized_source_jobs[0] if normalized_source_jobs else "",
                        snapshot_dir=snapshot_dir,
                    ):
                        contended_urls[normalized_profile_url] = registry_key
                        continue
                    self._record_stale_queued_profile_reclaimed(
                        normalized_profile_url,
                        registry_entry,
                        job_id=normalized_source_jobs[0] if normalized_source_jobs else "",
                    )
                if registry_status == "unrecoverable":
                    self.store.repos.linkedin_profile_registry.upsert_sources(
                        normalized_profile_url,
                        source_shards=list(source_shards_by_url.get(normalized_profile_url) or []),
                        source_jobs=normalized_source_jobs,
                    )
                    _record_event(
                        normalized_profile_url, event_type="cache_skip_unrecoverable", event_status="unrecoverable"
                    )
                    continue
                self.store.repos.linkedin_profile_registry.upsert_sources(
                    normalized_profile_url,
                    source_shards=list(source_shards_by_url.get(normalized_profile_url) or []),
                    source_jobs=normalized_source_jobs,
                )
                scheduler_required_urls.append(normalized_profile_url)
                _record_event(
                    normalized_profile_url,
                    event_type="cache_miss_scheduler_required",
                    event_status="deferred_budget" if normalized_source_jobs else "source_job_missing",
                    detail="profile hydration is cache-only; provider submit belongs to profile scheduler",
                    metadata={"source_jobs": list(normalized_source_jobs)},
                )
                continue
            _record_event(
                normalized_profile_url,
                event_type="cache_miss_scheduler_required",
                event_status="source_job_missing",
                detail="profile hydration is cache-only; provider submit belongs to profile scheduler",
            )
        if scheduler_required_urls and self.store is not None and normalized_source_jobs:
            registry_repo = linkedin_profile_registry_repo(self.store)
            recorder = getattr(registry_repo, "record_refill_plan_items", None)
            if callable(recorder):
                recorder(
                    deferred_profile_urls=scheduler_required_urls,
                    source_shards_by_url={
                        profile_url: list(source_shards_by_url.get(profile_url) or [])
                        for profile_url in scheduler_required_urls
                    },
                    source_jobs=normalized_source_jobs,
                    snapshot_dir=str(snapshot_dir),
                    trigger_kind="profile_cache_only_hydration_scheduler_required",
                    plan_reason="cache_miss_scheduler_required",
                    deferred_reason="direct_profile_fetch_retired",
                    deferred_queue_state="deferred_budget",
                )
        if contended_urls:
            _wait_for_contended_fetches(contended_urls)
        return fetched

    def _follow_up_roster_anchored_scholar_coauthor_prospects(
        self,
        *,
        prospects: list[Candidate],
        identity: CompanyIdentity,
        snapshot_dir: Path,
        candidate_map: dict[str, Candidate],
        resolved_profiles: list[dict[str, Any]],
        evidence: list[EvidenceRecord],
        profile_fetch_count: int,
        profile_detail_limit: int,
        exploration_limit: int,
        asset_logger: AssetLogger,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool = True,
    ) -> tuple[int, Path | None, list[str], int]:
        if not prospects or exploration_limit <= 0:
            return profile_fetch_count, None, [], 0

        follow_up_dir = snapshot_dir / "publications" / "roster_anchored_scholar_coauthors"
        follow_up_dir.mkdir(parents=True, exist_ok=True)
        summary_path = follow_up_dir / "follow_up_summary.json"
        progress_path = follow_up_dir / "follow_up_progress.json"
        patch_path = follow_up_dir / "follow_up_candidate_patch.json"
        decisions, errors, processed_candidate_ids = _load_scholar_coauthor_follow_up_progress(
            progress_path, summary_path
        )
        evidence_keys = {_evidence_resume_key(item) for item in evidence}
        _restore_scholar_coauthor_follow_up_patch(
            patch_path,
            candidate_map=candidate_map,
            resolved_profiles=resolved_profiles,
            evidence=evidence,
            evidence_keys=evidence_keys,
        )
        decisions_by_candidate_id = {
            str(item.get("candidate_id") or "").strip(): dict(item)
            for item in decisions
            if str(item.get("candidate_id") or "").strip()
        }
        queued_prospect_count = 0

        ordered_prospects = [
            prospect
            for prospect in _prioritize_scholar_coauthor_prospects(prospects)
            if prospect.candidate_id not in processed_candidate_ids
        ][:exploration_limit]

        for prospect in ordered_prospects:
            updated_candidates: list[Candidate] = []
            new_evidence_records: list[EvidenceRecord] = []
            new_resolved_profiles: list[dict[str, Any]] = []
            try:
                exploration_result = self.exploratory_enricher._explore_candidate(
                    snapshot_dir=snapshot_dir,
                    candidate=prospect,
                    target_company=identity.canonical_name,
                    logger=asset_logger,
                    job_id=job_id,
                    request_payload=request_payload,
                    plan_payload=plan_payload,
                    runtime_mode=runtime_mode,
                )
                explored_candidate = exploration_result["candidate"]
                errors.extend(list(exploration_result.get("errors") or []))
                if _is_queued_exploration_summary(exploration_result.get("summary") or {}):
                    queued_prospect_count += 1
                    decision_record = {
                        "candidate_id": explored_candidate.candidate_id,
                        "display_name": explored_candidate.display_name,
                        "state": "queued_background_exploration",
                        "confirmed_by_public_web": False,
                        "linkedin_url": explored_candidate.linkedin_url,
                        "next_step": "await_background_search_recovery",
                        "summary": "Background exploration is still queued; resume after worker recovery finishes.",
                        "seed_names": list(explored_candidate.metadata.get("scholar_coauthor_seed_names") or []),
                        "papers": list(explored_candidate.metadata.get("scholar_coauthor_papers") or [])[:8],
                        "profile_verified": False,
                        "exploration_summary": dict(exploration_result.get("summary") or {}),
                    }
                else:
                    decision = _scholar_coauthor_prospect_public_web_resolution(explored_candidate, identity)
                    profile_verified = False
                    prior_evidence_count = len(evidence)
                    prior_resolved_count = len(resolved_profiles)
                    if (
                        decision["confirmed_by_public_web"]
                        and _candidate_known_linkedin_url(explored_candidate)
                        and profile_fetch_count < profile_detail_limit
                    ):
                        _append_unique_evidence_records(
                            evidence,
                            list(exploration_result.get("evidence") or []),
                            evidence_keys=evidence_keys,
                        )
                        profile_verified, profile_fetch_count = self._resolve_candidate_with_known_refs(
                            _annotate_scholar_coauthor_resolution(explored_candidate, decision),
                            identity,
                            snapshot_dir,
                            profile_fetch_count,
                            profile_detail_limit,
                            candidate_map,
                            resolved_profiles,
                            evidence,
                            asset_logger=asset_logger,
                            allow_shared_provider_cache=allow_shared_provider_cache,
                            source_job_id=job_id,
                        )
                    new_evidence_records = list(evidence[prior_evidence_count:])
                    for item in new_evidence_records:
                        evidence_keys.add(_evidence_resume_key(item))
                    new_resolved_profiles = list(resolved_profiles[prior_resolved_count:])
                    if profile_verified:
                        updated_candidate = candidate_map.get(_candidate_key(explored_candidate))
                        if updated_candidate is not None:
                            updated_candidates.append(updated_candidate)
                    decision_record = {
                        "candidate_id": explored_candidate.candidate_id,
                        "display_name": explored_candidate.display_name,
                        "state": decision["state"],
                        "confirmed_by_public_web": decision["confirmed_by_public_web"],
                        "linkedin_url": decision["linkedin_url"],
                        "next_step": decision["next_step"],
                        "summary": decision["summary"],
                        "seed_names": list(explored_candidate.metadata.get("scholar_coauthor_seed_names") or []),
                        "papers": list(explored_candidate.metadata.get("scholar_coauthor_papers") or [])[:8],
                        "profile_verified": profile_verified,
                        "exploration_summary": dict(exploration_result.get("summary") or {}),
                    }
            except Exception as exc:
                error_message = f"scholar_coauthor_follow_up:{prospect.display_name}:{str(exc)[:160]}"
                errors.append(error_message)
                decision_record = {
                    "candidate_id": prospect.candidate_id,
                    "display_name": prospect.display_name,
                    "state": "follow_up_error",
                    "confirmed_by_public_web": False,
                    "linkedin_url": prospect.linkedin_url,
                    "next_step": "retry_follow_up",
                    "summary": f"Scholar coauthor follow-up failed: {str(exc)[:160]}",
                    "seed_names": list(prospect.metadata.get("scholar_coauthor_seed_names") or []),
                    "papers": list(prospect.metadata.get("scholar_coauthor_papers") or [])[:8],
                    "profile_verified": False,
                    "exploration_summary": {},
                    "error": str(exc)[:300],
                }

            decisions_by_candidate_id[decision_record["candidate_id"]] = decision_record
            if not _is_queued_scholar_coauthor_follow_up_decision(decision_record):
                processed_candidate_ids.add(decision_record["candidate_id"])
            _persist_scholar_coauthor_follow_up_patch(
                patch_path,
                updated_candidates=updated_candidates,
                resolved_profiles=new_resolved_profiles,
                new_evidence=new_evidence_records,
                asset_logger=asset_logger,
            )
            _write_scholar_coauthor_follow_up_state(
                progress_path=progress_path,
                summary_path=summary_path,
                identity=identity,
                prospect_total=len(prospects),
                processed_candidate_ids=processed_candidate_ids,
                decisions=list(decisions_by_candidate_id.values()),
                errors=errors,
                asset_logger=asset_logger,
                completed=False,
            )

        _write_scholar_coauthor_follow_up_state(
            progress_path=progress_path,
            summary_path=summary_path,
            identity=identity,
            prospect_total=len(prospects),
            processed_candidate_ids=processed_candidate_ids,
            decisions=list(decisions_by_candidate_id.values()),
            errors=errors,
            asset_logger=asset_logger,
            completed=len(processed_candidate_ids) >= len({prospect.candidate_id for prospect in prospects}),
        )
        return profile_fetch_count, summary_path, errors, queued_prospect_count

    def _gate_publication_leads_after_exploration(
        self,
        *,
        lead_candidates: list[Candidate],
        identity: CompanyIdentity,
        snapshot_dir: Path,
        candidate_map: dict[str, Candidate],
        unresolved_candidates: list[dict[str, Any]],
        asset_logger: AssetLogger,
        allow_targeted_name_search: bool,
    ) -> tuple[list[Candidate], Path | None]:
        if not lead_candidates:
            return [], None

        resolution_dir = snapshot_dir / "publication_lead_resolution"
        resolution_dir.mkdir(parents=True, exist_ok=True)
        decisions: list[dict[str, Any]] = []
        gated_leads: list[Candidate] = []

        for lead_candidate in lead_candidates:
            current_candidate = candidate_map.get(_candidate_key(lead_candidate), lead_candidate)
            decision = _publication_lead_public_web_resolution(
                current_candidate,
                identity,
                allow_targeted_name_search=allow_targeted_name_search,
            )
            current_candidate = _annotate_publication_lead_resolution(current_candidate, decision)
            candidate_map[_candidate_key(current_candidate)] = current_candidate
            _drop_unresolved_candidate(unresolved_candidates, current_candidate.candidate_id)
            unresolved_candidates.append(
                {
                    "candidate_id": current_candidate.candidate_id,
                    "display_name": current_candidate.display_name,
                    "attempted_slugs": [],
                    "query_summaries": [],
                    "resolution_source": "publication_lead_public_web_verification",
                    "publication_lead_resolution_state": decision["state"],
                    "public_web_confirmed": decision["confirmed_by_public_web"],
                    "linkedin_url": decision["linkedin_url"],
                    "next_step": decision["next_step"],
                    "summary": decision["summary"],
                }
            )
            if decision["eligible_for_targeted_name_search"]:
                gated_leads.append(current_candidate)
            decisions.append(
                {
                    "candidate_id": current_candidate.candidate_id,
                    "display_name": current_candidate.display_name,
                    "source_dataset": current_candidate.source_dataset,
                    "source_path": current_candidate.source_path,
                    "publication_title": _candidate_publication_title(current_candidate),
                    "publication_url": str(current_candidate.metadata.get("publication_url") or "").strip(),
                    **decision,
                }
            )

        summary_path = resolution_dir / "public_web_gate.json"
        asset_logger.write_json(
            summary_path,
            {
                "target_company": identity.canonical_name,
                "candidate_count": len(decisions),
                "eligible_for_targeted_name_search_count": len(gated_leads),
                "decisions": decisions,
            },
            asset_type="publication_lead_public_web_gate",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        return gated_leads, summary_path

    def _resolve_publication_leads_with_harvest_search(
        self,
        *,
        lead_candidates: list[Candidate],
        identity: CompanyIdentity,
        snapshot_dir: Path,
        remaining_profile_budget: int,
        candidate_map: dict[str, Candidate],
        resolved_profiles: list[dict[str, Any]],
        unresolved_candidates: list[dict[str, Any]],
        evidence: list[EvidenceRecord],
        asset_logger: AssetLogger,
        allow_shared_provider_cache: bool = True,
        source_job_id: str = "",
    ) -> tuple[int, Path | None]:
        if (
            self.harvest_profile_search_connector is None
            or not harvest_connector_available(self.harvest_profile_search_connector.settings)
            or self.harvest_profile_connector is None
            or remaining_profile_budget <= 0
        ):
            return 0, None

        resolution_dir = snapshot_dir / "publication_lead_resolution"
        resolution_dir.mkdir(parents=True, exist_ok=True)
        search_root = resolution_dir / "search_runs"
        search_root.mkdir(parents=True, exist_ok=True)
        company_url = str(identity.linkedin_company_url or "").strip()
        searches_used = 0
        attempt_runtime: list[dict[str, Any]] = []

        for lead_candidate in lead_candidates:
            current_candidate = candidate_map.get(_candidate_key(lead_candidate), lead_candidate)
            if current_candidate.category != "lead":
                continue
            query_text = (current_candidate.display_name or current_candidate.name_en).strip()
            if not query_text:
                continue
            candidate_dir = search_root / current_candidate.candidate_id
            candidate_dir.mkdir(parents=True, exist_ok=True)
            attempt_runtime.append(
                {
                    "candidate": current_candidate,
                    "candidate_dir": candidate_dir,
                    "attempt": {
                        "candidate_id": current_candidate.candidate_id,
                        "display_name": current_candidate.display_name,
                        "query_text": query_text,
                        "attempts": [],
                        "resolved": False,
                    },
                    "variants": {},
                }
            )

        search_variants = [
            {
                "employment_status": "current",
                "filter_hints": {"current_companies": [company_url] if company_url else []},
                "label": "current_company_exact_name",
            },
            {
                "employment_status": "former",
                "filter_hints": {"past_companies": [company_url] if company_url else []},
                "label": "past_company_exact_name",
            },
        ]

        for variant in search_variants:
            if searches_used >= remaining_profile_budget:
                break
            phase_urls: list[str] = []
            for runtime_item in attempt_runtime:
                attempt = runtime_item["attempt"]
                if attempt["resolved"] or searches_used >= remaining_profile_budget:
                    continue
                current_candidate = candidate_map.get(
                    _candidate_key(runtime_item["candidate"]), runtime_item["candidate"]
                )
                runtime_item["candidate"] = current_candidate
                if current_candidate.category != "lead":
                    continue
                search_result = self.harvest_profile_search_connector.search_profiles(
                    query_text=str(attempt.get("query_text") or ""),
                    filter_hints=dict(variant["filter_hints"]),
                    employment_status=str(variant["employment_status"]),
                    discovery_dir=runtime_item["candidate_dir"],
                    asset_logger=asset_logger,
                    limit=min(self.harvest_profile_search_connector.settings.max_paid_items, 10),
                    allow_shared_provider_cache=allow_shared_provider_cache,
                )
                searches_used += 1
                variant_summary = {
                    "label": variant["label"],
                    "employment_status": variant["employment_status"],
                    "raw_path": str(search_result.get("raw_path") or "") if search_result else "",
                    "row_count": len(list(search_result.get("rows") or [])) if search_result else 0,
                    "matched_names": [],
                    "profile_url": "",
                    "verified": False,
                }
                rows = []
                if search_result is not None:
                    rows = [
                        row
                        for row in list(search_result.get("rows") or [])
                        if _names_match(current_candidate.name_en, str(row.get("full_name") or ""))
                    ]
                    variant_summary["matched_names"] = [str(row.get("full_name") or "") for row in rows]
                profile_urls: list[str] = []
                for row in rows:
                    profile_url = str(row.get("profile_url") or "").strip()
                    if profile_url and profile_url not in profile_urls:
                        profile_urls.append(profile_url)
                    if profile_url and profile_url not in phase_urls:
                        phase_urls.append(profile_url)
                attempt["attempts"].append(variant_summary)
                runtime_item["variants"][str(variant["label"])] = {
                    "rows": rows,
                    "summary": variant_summary,
                }

            fetched_profiles = self._fetch_harvest_profiles_for_urls(
                phase_urls,
                snapshot_dir,
                asset_logger=asset_logger,
                allow_shared_provider_cache=allow_shared_provider_cache,
                source_shards_by_url={
                    profile_url: [f"publication_lead_targeted:{variant['label']}"] for profile_url in phase_urls
                },
                source_jobs=[source_job_id] if str(source_job_id or "").strip() else [],
            )
            for runtime_item in attempt_runtime:
                attempt = runtime_item["attempt"]
                if attempt["resolved"]:
                    continue
                variant_runtime = dict(runtime_item["variants"].get(str(variant["label"])) or {})
                if not variant_runtime:
                    continue
                current_candidate = candidate_map.get(
                    _candidate_key(runtime_item["candidate"]), runtime_item["candidate"]
                )
                runtime_item["candidate"] = current_candidate
                if current_candidate.category != "lead":
                    continue
                for row in list(variant_runtime.get("rows") or []):
                    profile_url = str(row.get("profile_url") or "").strip()
                    if not profile_url:
                        continue
                    profile = fetched_profiles.get(profile_url)
                    if profile is None or not _profile_matches_candidate(
                        profile["parsed"],
                        current_candidate,
                        identity,
                        model_client=self.model_client,
                    ):
                        continue
                    merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                        current_candidate,
                        profile["parsed"],
                        profile["raw_path"],
                        profile["account_id"],
                        _extract_seed_slug(current_candidate) or extract_linkedin_slug(profile_url),
                        identity,
                        model_client=self.model_client,
                        resolution_source=f"publication_lead_targeted_harvest_{variant['employment_status']}",
                    )
                    candidate_map[_candidate_key(current_candidate)] = merged_candidate
                    resolved_profiles.append(resolved_profile)
                    evidence.extend(profile_evidence)
                    _drop_unresolved_candidate(unresolved_candidates, current_candidate.candidate_id)
                    summary = variant_runtime.get("summary")
                    if isinstance(summary, dict):
                        summary["profile_url"] = profile_url
                        summary["verified"] = True
                    attempt["resolved"] = True
                    break

        attempts_summary = [dict(item["attempt"] or {}) for item in attempt_runtime]
        unresolved_candidate_ids = {str(item.get("candidate_id") or "").strip() for item in unresolved_candidates}
        for attempt in attempts_summary:
            if attempt.get("resolved"):
                continue
            candidate_id = str(attempt.get("candidate_id") or "").strip()
            if not candidate_id or candidate_id in unresolved_candidate_ids:
                continue
            unresolved_candidates.append(
                {
                    "candidate_id": candidate_id,
                    "display_name": str(attempt.get("display_name") or ""),
                    "attempted_slugs": [],
                    "query_summaries": list(attempt.get("attempts") or []),
                    "resolution_source": "publication_lead_targeted_harvest",
                }
            )
            unresolved_candidate_ids.add(candidate_id)

        summary_path = resolution_dir / "summary.json"
        asset_logger.write_json(
            summary_path,
            {
                "target_company": identity.canonical_name,
                "attempt_count": len(attempts_summary),
                "searches_used": searches_used,
                "attempts": attempts_summary,
            },
            asset_type="publication_lead_targeted_resolution_summary",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        return searches_used, summary_path

    def _resolve_candidate_with_known_refs(
        self,
        candidate: Candidate,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        profile_fetch_count: int,
        profile_detail_limit: int,
        candidate_map: dict[str, Candidate],
        resolved_profiles: list[dict[str, Any]],
        evidence: list[EvidenceRecord],
        *,
        asset_logger: AssetLogger,
        prefetched_harvest_profiles: dict[str, dict[str, Any]] | None = None,
        background_prefetch_urls: set[str] | None = None,
        allow_shared_provider_cache: bool = True,
        source_job_id: str = "",
    ) -> tuple[bool, int]:
        if profile_fetch_count >= profile_detail_limit:
            return False, profile_fetch_count

        profile_urls = _candidate_profile_urls(candidate)
        harvested_profiles = self._fetch_harvest_profiles_for_urls(
            profile_urls,
            snapshot_dir,
            asset_logger=asset_logger,
            prefetched_harvest_profiles=prefetched_harvest_profiles,
            background_prefetch_urls=background_prefetch_urls,
            allow_shared_provider_cache=allow_shared_provider_cache,
            source_shards_by_url={
                profile_url: _profile_registry_sources_for_candidate(candidate) for profile_url in profile_urls
            },
            source_jobs=[source_job_id] if str(source_job_id or "").strip() else [],
        )
        for profile_url in profile_urls:
            normalized_profile_url = str(profile_url or "").strip()
            profile = harvested_profiles.get(normalized_profile_url)
            if profile is None or not _profile_matches_candidate(
                profile["parsed"],
                candidate,
                identity,
                model_client=self.model_client,
            ):
                continue
            profile_fetch_count += 1
            merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                candidate,
                profile["parsed"],
                profile["raw_path"],
                profile["account_id"],
                _extract_seed_slug(candidate) or extract_linkedin_slug(normalized_profile_url),
                identity,
                model_client=self.model_client,
                resolution_source="known_profile_url_harvest",
            )
            candidate_map[_candidate_key(candidate)] = merged_candidate
            resolved_profiles.append(resolved_profile)
            evidence.extend(profile_evidence)
            return True, profile_fetch_count

        seed_slug = _extract_seed_slug(candidate)
        if seed_slug and profile_fetch_count < profile_detail_limit:
            profile_fetch_count += 1
            profile = self.profile_connector.fetch_profile(seed_slug, snapshot_dir, asset_logger=asset_logger)
            if profile is not None and _profile_matches_candidate(
                profile["parsed"],
                candidate,
                identity,
                model_client=self.model_client,
            ):
                merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                    candidate,
                    profile["parsed"],
                    profile["raw_path"],
                    profile["account_id"],
                    seed_slug,
                    identity,
                    model_client=self.model_client,
                    resolution_source="seed_slug",
                )
                candidate_map[_candidate_key(candidate)] = merged_candidate
                resolved_profiles.append(resolved_profile)
                evidence.extend(profile_evidence)
                return True, profile_fetch_count
        return False, profile_fetch_count


class LinkedInSearchSlugResolver:
    def __init__(self, accounts: list[RapidApiAccount], *, search_provider: BaseSearchProvider | None = None) -> None:
        self.accounts = search_people_accounts(accounts)
        self._exhausted_account_ids: set[str] = set()
        self.web_fallback = DuckDuckGoLinkedInResolver(search_provider=search_provider)

    def resolve(
        self,
        candidates: list[Candidate],
        identity: CompanyIdentity,
        snapshot_dir: Path,
        *,
        asset_logger: AssetLogger | None = None,
    ) -> dict[str, Any]:
        search_dir = snapshot_dir / "slug_search"
        search_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        results: list[dict[str, Any]] = []
        errors: list[str] = []
        web_results = self.web_fallback.resolve(candidates, identity, snapshot_dir, asset_logger=logger)
        errors.extend(web_results.get("errors", []))
        web_result_map = {item["candidate_key"]: item for item in web_results.get("results", [])}

        for candidate in candidates:
            existing_item = web_result_map.get(_candidate_key(candidate), {})
            existing_queries = list(existing_item.get("queries", []))
            existing_slugs = list(existing_item.get("slugs", []))
            if existing_slugs or not self.accounts:
                results.append(
                    {
                        "candidate_id": candidate.candidate_id,
                        "candidate_key": _candidate_key(candidate),
                        "display_name": candidate.display_name,
                        "slugs": existing_slugs,
                        "queries": existing_queries,
                    }
                )
                continue

            queries = _build_people_search_queries(candidate, identity)
            slug_candidates: list[str] = []
            query_summaries: list[dict[str, Any]] = list(existing_queries)

            for index, query in enumerate(queries, start=1):
                raw_path = search_dir / f"{candidate.candidate_id}_q{index:02d}.json"
                payload = None
                cached_from: Path | None = None
                if raw_path.exists():
                    try:
                        payload = json.loads(raw_path.read_text())
                        cached_from = raw_path
                    except json.JSONDecodeError:
                        payload = None
                if payload is None:
                    payload, cached_from = self._load_cached_search_payload(
                        snapshot_dir.parent, snapshot_dir, candidate.candidate_id, index
                    )
                account: RapidApiAccount | None = None
                if payload is None:
                    payload, account, search_errors = self._search_people(query)
                    errors.extend(search_errors)
                if payload is None:
                    query_summaries.append(
                        {
                            "query": query,
                            "raw_path": "",
                            "account_id": "",
                            "rows": [],
                            "slugs": [],
                            "mode": "provider_people_search",
                        }
                    )
                    continue
                logger.write_json(
                    raw_path,
                    payload,
                    asset_type="provider_people_search_payload",
                    source_kind="slug_resolution",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={"query": query, "candidate_id": candidate.candidate_id},
                )

                matched_rows: list[dict[str, Any]] = []
                for row_index, row in enumerate(extract_search_people_rows(payload), start=1):
                    if not _search_result_name_matches_candidate(row, candidate):
                        continue
                    row_summary = {
                        "full_name": row.get("full_name", ""),
                        "headline": row.get("headline", ""),
                        "location": row.get("location", ""),
                        "urn": row.get("urn", ""),
                    }
                    if cached_from:
                        row_summary["cached_search_from"] = str(cached_from)
                    basic_profile = self._fetch_basic_profile(
                        row.get("urn", ""), search_dir, account, asset_logger=logger
                    )
                    if basic_profile is not None:
                        row_summary["basic_profile_path"] = str(basic_profile["raw_path"])
                        row_summary["profile_url"] = basic_profile["parsed"].get("profile_url", "")
                        row_summary["username"] = basic_profile["parsed"].get("username", "")
                        slug = row_summary["username"] or extract_linkedin_slug(row_summary["profile_url"])
                        if slug and slug not in slug_candidates:
                            slug_candidates.append(slug)
                    matched_rows.append(row_summary)
                    if row_index >= 2 and slug_candidates:
                        break

                query_summaries.append(
                    {
                        "query": query,
                        "raw_path": str(raw_path),
                        "account_id": account.account_id if account else "cache",
                        "rows": matched_rows,
                        "slugs": list(slug_candidates),
                        "mode": "provider_people_search",
                    }
                )
                if slug_candidates:
                    break

            results.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "candidate_key": _candidate_key(candidate),
                    "display_name": candidate.display_name,
                    "slugs": list(existing_slugs) + [item for item in slug_candidates if item not in existing_slugs],
                    "queries": query_summaries,
                }
            )

        summary_path = search_dir / "summary.json"
        logger.write_json(
            summary_path,
            {"resolver": "web_first_then_provider", "results": results, "errors": errors},
            asset_type="slug_search_summary",
            source_kind="slug_resolution",
            is_raw_asset=False,
            model_safe=True,
        )
        return {"results": results, "summary_path": summary_path, "errors": errors}

    def _load_cached_search_payload(
        self,
        company_dir: Path,
        snapshot_dir: Path,
        candidate_id: str,
        query_index: int,
    ) -> tuple[dict[str, Any] | None, Path | None]:
        pattern = f"*/slug_search/{candidate_id}_q{query_index:02d}.json"
        candidates = [path for path in company_dir.glob(pattern) if snapshot_dir not in path.parents]
        if not candidates:
            return None, None
        cached_path = max(candidates, key=lambda item: item.stat().st_mtime)
        try:
            return json.loads(cached_path.read_text()), cached_path
        except (OSError, json.JSONDecodeError):
            return None, None

    def _search_people(self, query: str) -> tuple[dict[str, Any] | None, RapidApiAccount | None, list[str]]:
        errors: list[str] = []
        for account in self.accounts:
            if account.account_id in self._exhausted_account_ids:
                continue
            url = build_people_search_url(account, query, limit=5)
            assert_live_provider_access_allowed(
                provider_name="rapidapi_linkedin",
                operation="people_search",
                payload={"query": query, "limit": 5, "host": account.host},
            )
            headers = {"x-rapidapi-host": account.host, "x-rapidapi-key": account.api_key, "User-Agent": "Mozilla/5.0"}
            http_request = request.Request(url, headers=headers, method="GET")
            try:
                with request.urlopen(http_request, timeout=30) as response:
                    return json.loads(response.read().decode("utf-8")), account, errors
            except error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="ignore")
                if exc.code == 429:
                    self._exhausted_account_ids.add(account.account_id)
                errors.append(f"search_people:{account.account_id}:{exc.code}:{detail[:120]}")
            except Exception as exc:
                errors.append(f"search_people:{account.account_id}:{str(exc)[:120]}")
        return None, None, errors

    def _fetch_basic_profile(
        self,
        profile_ref: str,
        search_dir: Path,
        account: RapidApiAccount | None,
        *,
        asset_logger: AssetLogger,
    ) -> dict[str, Any] | None:
        profile_ref = str(profile_ref or "").strip()
        if not profile_ref:
            return None
        basic_dir = search_dir / "basic_profiles"
        basic_dir.mkdir(parents=True, exist_ok=True)
        cache_key = sha1(profile_ref.encode("utf-8")).hexdigest()[:16]
        if account is not None:
            raw_path = basic_dir / f"{cache_key}_{account.account_id}.json"
            if raw_path.exists():
                try:
                    payload = json.loads(raw_path.read_text())
                    return {"raw_path": raw_path, "parsed": parse_basic_linkedin_profile_payload(payload)}
                except json.JSONDecodeError:
                    pass
        else:
            raw_path = basic_dir / f"{cache_key}_cache.json"

        company_dir = search_dir.parent.parent
        cached_paths = sorted(company_dir.glob(f"*/slug_search/basic_profiles/{cache_key}_*.json"))
        if cached_paths:
            cached_path = cached_paths[-1]
            try:
                payload = json.loads(cached_path.read_text())
                asset_logger.write_json(
                    raw_path,
                    payload,
                    asset_type="linkedin_basic_profile_payload",
                    source_kind="slug_resolution_basic_profile",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={"profile_ref": profile_ref, "copied_from_cache": True},
                )
                return {"raw_path": raw_path, "parsed": parse_basic_linkedin_profile_payload(payload)}
            except (OSError, json.JSONDecodeError):
                pass

        if account is None:
            return None

        url = build_basic_profile_url(account, profile_ref)
        assert_live_provider_access_allowed(
            provider_name="rapidapi_linkedin",
            operation="basic_profile",
            runtime_dir=search_dir,
            payload={"profile_ref": profile_ref, "host": account.host},
        )
        headers = {"x-rapidapi-host": account.host, "x-rapidapi-key": account.api_key, "User-Agent": "Mozilla/5.0"}
        http_request = request.Request(url, headers=headers, method="GET")
        try:
            with request.urlopen(http_request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            if exc.code == 429:
                self._exhausted_account_ids.add(account.account_id)
            return None
        except Exception:
            return None

        asset_logger.write_json(
            raw_path,
            payload,
            asset_type="linkedin_basic_profile_payload",
            source_kind="slug_resolution_basic_profile",
            is_raw_asset=True,
            model_safe=False,
            metadata={"profile_ref": profile_ref, "account_id": account.account_id},
        )
        return {"raw_path": raw_path, "parsed": parse_basic_linkedin_profile_payload(payload)}


class DuckDuckGoLinkedInResolver:
    def __init__(self, *, search_provider: BaseSearchProvider | None = None) -> None:
        self.search_provider = search_provider or DuckDuckGoHtmlSearchProvider()

    def resolve(
        self,
        candidates: list[Candidate],
        identity: CompanyIdentity,
        snapshot_dir: Path,
        *,
        asset_logger: AssetLogger | None = None,
    ) -> dict[str, Any]:
        search_dir = snapshot_dir / "slug_search"
        search_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        results: list[dict[str, Any]] = []
        errors: list[str] = []

        for candidate in candidates:
            queries = _build_slug_queries(candidate, identity)
            slug_candidates: list[str] = []
            query_summaries: list[dict[str, Any]] = []
            for index, query in enumerate(queries, start=1):
                try:
                    response = self.search_provider.search(query, max_results=10)
                except Exception as exc:
                    query_summaries.append({"query": query, "raw_path": "", "slugs": [], "error": str(exc)})
                    errors.append(f"slug_search:{candidate.display_name}:{str(exc)[:160]}")
                    continue
                raw_path = (
                    search_dir
                    / f"{candidate.candidate_id}_q{index:02d}.{'json' if response.raw_format == 'json' else 'html'}"
                )
                if response.raw_format == "json":
                    logger.write_json(
                        raw_path,
                        search_response_to_record(response),
                        asset_type="web_linkedin_search_payload",
                        source_kind="slug_resolution",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={
                            "query": query,
                            "candidate_id": candidate.candidate_id,
                            "provider_name": response.provider_name,
                        },
                    )
                else:
                    logger.write_text(
                        raw_path,
                        str(response.raw_payload or ""),
                        asset_type="web_linkedin_search_html",
                        source_kind="slug_resolution",
                        content_type=response.content_type or "text/html",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={
                            "query": query,
                            "candidate_id": candidate.candidate_id,
                            "provider_name": response.provider_name,
                        },
                    )
                urls = [
                    str(item.url or "").strip()
                    for item in response.results
                    if "linkedin.com/in/" in str(item.url or "")
                ]
                slugs = []
                for url in urls:
                    slug = extract_linkedin_slug(url)
                    if slug and slug not in slug_candidates:
                        slug_candidates.append(slug)
                        slugs.append(slug)
                query_summaries.append(
                    {
                        "query": query,
                        "raw_path": str(raw_path),
                        "slugs": slugs,
                        "provider_name": response.provider_name,
                        "result_count": len(response.results),
                    }
                )
                if slug_candidates:
                    break
            results.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "candidate_key": _candidate_key(candidate),
                    "display_name": candidate.display_name,
                    "slugs": slug_candidates,
                    "queries": query_summaries,
                }
            )

        summary_path = search_dir / "summary.json"
        logger.write_json(
            summary_path,
            {"results": results, "errors": errors},
            asset_type="web_slug_search_summary",
            source_kind="slug_resolution",
            is_raw_asset=False,
            model_safe=True,
        )
        return {"results": results, "summary_path": summary_path, "errors": errors}


class LinkedInProfileDetailConnector:
    def __init__(self, accounts: list[RapidApiAccount]) -> None:
        self.accounts = profile_detail_accounts(accounts)
        self._exhausted_account_ids: set[str] = set()

    def fetch_profile(
        self, slug: str, snapshot_dir: Path, *, asset_logger: AssetLogger | None = None
    ) -> dict[str, Any] | None:
        profiles_dir = snapshot_dir / "profiles"
        profiles_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        for account in self.accounts:
            if account.account_id in self._exhausted_account_ids:
                continue
            raw_path = profiles_dir / f"{slug}_{account.account_id}.json"
            if raw_path.exists():
                try:
                    payload = json.loads(raw_path.read_text())
                    logger.record_existing(
                        raw_path,
                        asset_type="linkedin_profile_detail_payload",
                        source_kind="profile_detail_connector",
                        content_type="application/json",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={"slug": slug, "account_id": account.account_id, "cached": True},
                    )
                    return {
                        "account_id": account.account_id,
                        "raw_path": raw_path,
                        "parsed": parse_linkedin_profile_payload(payload),
                    }
                except json.JSONDecodeError:
                    pass

            if external_provider_mode() != "live":
                return None

            url = build_profile_detail_url(account, slug)
            assert_live_provider_access_allowed(
                provider_name="rapidapi_linkedin",
                operation="profile_detail",
                runtime_dir=snapshot_dir,
                payload={"slug": slug, "host": account.host},
            )
            headers = {"x-rapidapi-host": account.host, "x-rapidapi-key": account.api_key, "User-Agent": "Mozilla/5.0"}
            if "real-time-linkedin-data-scraper-api" in account.host:
                headers["Content-Type"] = "application/json"
            http_request = request.Request(url, headers=headers, method="GET")
            try:
                with request.urlopen(http_request, timeout=30) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                logger.write_json(
                    raw_path,
                    payload,
                    asset_type="linkedin_profile_detail_payload",
                    source_kind="profile_detail_connector",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={"slug": slug, "account_id": account.account_id, "cached": False},
                )
                return {
                    "account_id": account.account_id,
                    "raw_path": raw_path,
                    "parsed": parse_linkedin_profile_payload(payload),
                }
            except error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="ignore")
                if exc.code == 429:
                    self._exhausted_account_ids.add(account.account_id)
                if exc.code in {404, 429}:
                    continue
                if detail:
                    continue
            except Exception:
                continue
        return None


class CompanyPublicationConnector:
    def __init__(self, catalog: AssetCatalog) -> None:
        self.catalog = catalog

    def enrich(
        self,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        candidates: list[Candidate],
        *,
        asset_logger: AssetLogger | None = None,
        max_publications: int,
        max_leads: int,
        request_payload: dict[str, Any] | None = None,
        plan_payload: dict[str, Any] | None = None,
        existing_evidence: list[EvidenceRecord] | None = None,
    ) -> dict[str, Any]:
        publications_dir = snapshot_dir / "publications"
        publications_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)

        if max_publications <= 0:
            return {
                "matched_candidates": candidates,
                "lead_candidates": [],
                "evidence": [],
                "publication_matches": [],
                "coauthor_edges": [],
                "scholar_coauthor_prospects": [],
                "artifact_paths": {},
            }

        errors: list[str] = []
        try:
            publications = self._collect_publications(
                identity,
                publications_dir,
                max_publications,
                asset_logger=logger,
                request_payload=request_payload or {},
                plan_payload=plan_payload or {},
            )
        except Exception as exc:
            errors.append(f"publication_collection:{str(exc)[:160]}")
            publications = []
        candidate_map = _candidate_name_map(candidates)
        lead_map: dict[str, Candidate] = {}
        evidence: list[EvidenceRecord] = []
        publication_matches: list[dict[str, Any]] = []
        coauthor_edges: set[tuple[str, str]] = set()
        scholar_coauthor_result = self._collect_roster_anchored_scholar_coauthors(
            identity,
            publications_dir,
            candidates,
            publications=publications,
            existing_evidence=existing_evidence or [],
            asset_logger=logger,
        )
        evidence.extend(scholar_coauthor_result["evidence"])

        for publication in publications:
            matched_candidates: dict[str, Candidate] = {}
            for name in publication.authors:
                candidate = candidate_map.get(_name_key(name))
                if candidate is not None:
                    matched_candidates[candidate.candidate_id] = candidate
                    evidence.append(
                        EvidenceRecord(
                            evidence_id=make_evidence_id(
                                candidate.candidate_id, publication.source_dataset, publication.title, publication.url
                            ),
                            candidate_id=candidate.candidate_id,
                            source_type="publication_author",
                            title=publication.title,
                            url=publication.url,
                            summary=f"{candidate.display_name} appears as an author on this company-related publication.",
                            source_dataset=publication.source_dataset,
                            source_path=publication.source_path,
                            metadata={"publication_id": publication.publication_id, "matched_name": name},
                        )
                    )
                elif len(lead_map) < max_leads and _looks_like_person_name(name):
                    lead = _build_publication_lead(identity, publication, name, "Publication author lead")
                    lead_map.setdefault(_candidate_key(lead), lead)

            for name in publication.acknowledgement_names:
                candidate = candidate_map.get(_name_key(name))
                if candidate is not None:
                    evidence.append(
                        EvidenceRecord(
                            evidence_id=make_evidence_id(
                                candidate.candidate_id,
                                publication.source_dataset,
                                f"{publication.title} acknowledgement",
                                publication.url,
                            ),
                            candidate_id=candidate.candidate_id,
                            source_type="publication_acknowledgement",
                            title=publication.title,
                            url=publication.url,
                            summary=f"{candidate.display_name} is referenced in the acknowledgement evidence for this publication.",
                            source_dataset=publication.source_dataset,
                            source_path=publication.source_path,
                            metadata={"publication_id": publication.publication_id, "matched_name": name},
                        )
                    )
                elif len(lead_map) < max_leads and _looks_like_person_name(name):
                    lead = _build_publication_lead(identity, publication, name, "Acknowledgement lead")
                    lead_map.setdefault(_candidate_key(lead), lead)

            matched_candidate_list = list(matched_candidates.values())
            matched_names = sorted(
                {candidate.display_name or candidate.name_en for candidate in matched_candidate_list}
            )
            for candidate in matched_candidate_list:
                coauthors = [name for name in matched_names if name != (candidate.display_name or candidate.name_en)]
                if not coauthors:
                    continue
                evidence.append(
                    EvidenceRecord(
                        evidence_id=make_evidence_id(
                            candidate.candidate_id,
                            publication.source_dataset,
                            f"{publication.title} coauthor",
                            publication.url,
                        ),
                        candidate_id=candidate.candidate_id,
                        source_type="publication_coauthor",
                        title=publication.title,
                        url=publication.url,
                        summary=f"{candidate.display_name or candidate.name_en} co-authored this company-related publication with {', '.join(coauthors)}.",
                        source_dataset=publication.source_dataset,
                        source_path=publication.source_path,
                        metadata={
                            "publication_id": publication.publication_id,
                            "coauthors": coauthors,
                            "matched_candidates": matched_names,
                        },
                    )
                )

            for left, right in combinations(matched_names, 2):
                coauthor_edges.add((left, right))
            if matched_names:
                publication_matches.append(
                    {
                        "publication_id": publication.publication_id,
                        "title": publication.title,
                        "url": publication.url,
                        "matched_candidates": matched_names,
                    }
                )

        coauthor_graph = [{"source": left, "target": right} for left, right in sorted(coauthor_edges)]
        publication_summary_path = publications_dir / "publication_matches.json"
        coauthor_graph_path = publications_dir / "coauthor_graph.json"
        lead_candidates_path = publications_dir / "publication_leads.json"
        logger.write_json(
            publication_summary_path,
            {"publications": [item.to_record() for item in publications], "matches": publication_matches},
            asset_type="publication_matches",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            coauthor_graph_path,
            coauthor_graph,
            asset_type="publication_coauthor_graph",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            lead_candidates_path,
            [candidate.to_record() for candidate in lead_map.values()],
            asset_type="publication_leads",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        artifact_paths = {
            "publication_matches": str(publication_summary_path),
            "coauthor_graph": str(coauthor_graph_path),
            "publication_leads": str(lead_candidates_path),
        }
        artifact_paths.update(dict(scholar_coauthor_result.get("artifact_paths") or {}))

        return {
            "matched_candidates": candidates,
            "lead_candidates": list(lead_map.values()),
            "evidence": evidence,
            "publication_matches": publication_matches,
            "coauthor_edges": coauthor_graph,
            "scholar_coauthor_prospects": list(scholar_coauthor_result.get("prospect_candidates") or []),
            "errors": errors,
            "artifact_paths": artifact_paths,
        }

    def _collect_publications(
        self,
        identity: CompanyIdentity,
        publications_dir: Path,
        max_publications: int,
        *,
        asset_logger: AssetLogger,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
    ) -> list[PublicationRecord]:
        if identity.company_key == "anthropic" and self.catalog.anthropic_publications.exists():
            asset_logger.record_existing(
                self.catalog.anthropic_publications,
                asset_type="local_publication_bundle",
                source_kind="publication_enrichment",
                content_type="application/json",
                is_raw_asset=True,
                model_safe=False,
                metadata={"company": identity.canonical_name},
            )
            return _load_local_publications(self.catalog.anthropic_publications, max_publications)
        if self._should_skip_remote_publication_collection(request_payload):
            return []
        publication_plan = dict(plan_payload.get("publication_coverage") or {})
        source_families = {
            str(item.get("family") or "").strip()
            for item in publication_plan.get("source_families") or []
            if isinstance(item, dict)
        }
        seed_queries = [str(item).strip() for item in publication_plan.get("seed_queries") or [] if str(item).strip()]
        results: list[PublicationRecord] = []
        if source_families & {
            "official_research",
            "official_engineering",
            "official_blog_and_docs",
            "product_subbrand_pages",
        }:
            results.extend(
                self._collect_official_surface_publications(
                    identity,
                    publications_dir,
                    max_publications=max_publications,
                    seed_queries=seed_queries,
                    asset_logger=asset_logger,
                )
            )
        remaining_limit = max(max_publications - len(results), 0)
        if remaining_limit > 0 and (not source_families or "publication_platforms" in source_families):
            results.extend(
                self._search_arxiv_publications(
                    identity,
                    publications_dir,
                    remaining_limit,
                    asset_logger=asset_logger,
                )
            )
        return _dedupe_publication_records(results, max_publications)

    def _should_skip_remote_publication_collection(self, request_payload: dict[str, Any]) -> bool:
        external_provider_mode = _external_provider_mode()
        if external_provider_mode in {"simulate", "scripted"}:
            return True
        runtime_timing_overrides = _runtime_timing_overrides_from_request_payload(request_payload)
        return str(runtime_timing_overrides.get("runtime_tuning_profile") or "").strip().lower() == "fast_smoke"

    def _collect_official_surface_publications(
        self,
        identity: CompanyIdentity,
        publications_dir: Path,
        *,
        max_publications: int,
        seed_queries: list[str],
        asset_logger: AssetLogger,
    ) -> list[PublicationRecord]:
        site_root = _identity_site_root(identity)
        if not site_root:
            return []
        official_dir = publications_dir / "official_surfaces"
        official_dir.mkdir(parents=True, exist_ok=True)
        home_url = f"{site_root}/"
        try:
            home_html = _fetch_text(home_url)
        except Exception:
            home_html = ""
        if home_html:
            asset_logger.write_text(
                official_dir / "home.html",
                home_html,
                asset_type="official_surface_home_html",
                source_kind="publication_enrichment",
                content_type="text/html",
                is_raw_asset=True,
                model_safe=False,
                metadata={"url": home_url},
            )
        discovery_manifest: dict[str, Any] = {"site_root": site_root, "seed_queries": seed_queries, "surfaces": []}
        surface_urls = _discover_official_surface_urls(identity, site_root, home_html)
        records: list[PublicationRecord] = []
        for surface_url in surface_urls:
            if len(records) >= max_publications:
                break
            try:
                surface_text = _fetch_text(surface_url)
            except Exception:
                continue
            surface_path = official_dir / _surface_asset_name(surface_url)
            content_type = "application/xml" if surface_url.endswith(".xml") else "text/html"
            asset_logger.write_text(
                surface_path,
                surface_text,
                asset_type="official_surface_payload",
                source_kind="publication_enrichment",
                content_type=content_type,
                is_raw_asset=True,
                model_safe=False,
                metadata={"url": surface_url},
            )
            surface_records: list[PublicationRecord] = []
            if surface_url.endswith(".xml"):
                surface_records = _extract_publications_from_rss(surface_text, surface_url, str(surface_path))
            else:
                surface_records = _extract_publications_from_surface_index(
                    surface_text,
                    surface_url,
                    str(surface_path),
                )
            if not surface_records and surface_url.rstrip("/").endswith(
                ("/tinker", "/research", "/engineering", "/docs")
            ):
                surface_records = _extract_single_surface_page_record(surface_text, surface_url, str(surface_path))
            enriched_records: list[PublicationRecord] = []
            for record in surface_records:
                if len(records) + len(enriched_records) >= max_publications:
                    break
                enriched_records.append(
                    self._hydrate_official_surface_record(record, official_dir, asset_logger=asset_logger)
                )
            discovery_manifest["surfaces"].append(
                {
                    "url": surface_url,
                    "record_count": len(enriched_records),
                }
            )
            records.extend(enriched_records)
        manifest_path = official_dir / "surface_manifest.json"
        asset_logger.write_json(
            manifest_path,
            discovery_manifest,
            asset_type="official_surface_manifest",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        return _dedupe_publication_records(records, max_publications)

    def _hydrate_official_surface_record(
        self,
        record: PublicationRecord,
        official_dir: Path,
        *,
        asset_logger: AssetLogger,
    ) -> PublicationRecord:
        if record.authors and any(_looks_like_person_name(name) for name in record.authors):
            return record
        url = str(record.url or "").strip()
        if not url:
            return record
        try:
            page_html = _fetch_text(url)
        except Exception:
            return record
        raw_path = official_dir / _surface_asset_name(url)
        asset_logger.write_text(
            raw_path,
            page_html,
            asset_type="official_surface_page_html",
            source_kind="publication_enrichment",
            content_type="text/html",
            is_raw_asset=True,
            model_safe=False,
            metadata={"url": url},
        )
        byline_authors = _extract_page_authors(page_html)
        if not byline_authors:
            byline_authors = list(record.authors)
        title = record.title or _extract_page_title(page_html) or record.publication_id
        return PublicationRecord(
            publication_id=record.publication_id,
            source=record.source,
            source_dataset=record.source_dataset,
            source_path=str(raw_path),
            title=title,
            url=url,
            year=record.year or _extract_year_from_html(page_html),
            authors=byline_authors,
            acknowledgement_names=record.acknowledgement_names,
        )

    def _search_arxiv_publications(
        self,
        identity: CompanyIdentity,
        publications_dir: Path,
        max_publications: int,
        *,
        asset_logger: AssetLogger,
    ) -> list[PublicationRecord]:
        queries = [identity.canonical_name]
        if identity.domain:
            domain_root = identity.domain.replace(".com", "").replace(".ai", "").replace(".org", "")
            if domain_root and domain_root not in queries:
                queries.append(domain_root)

        results: list[PublicationRecord] = []
        seen_ids: set[str] = set()
        for query_text in queries:
            search_url = "https://arxiv.org/search/?" + parse.urlencode(
                {
                    "query": query_text,
                    "searchtype": "affiliation",
                    "abstracts": "show",
                    "order": "-announced_date_first",
                    "size": 25,
                }
            )
            try:
                raw_search = _fetch_text(search_url)
            except Exception:
                continue
            raw_search_path = publications_dir / f"arxiv_search_{normalize_name_token(query_text) or 'query'}.html"
            asset_logger.write_text(
                raw_search_path,
                raw_search,
                asset_type="arxiv_affiliation_search_html",
                source_kind="publication_enrichment",
                content_type="text/html",
                is_raw_asset=True,
                model_safe=False,
                metadata={"query": query_text},
            )
            paper_ids = re.findall(r'href="https://arxiv\.org/abs/([0-9]{4}\.[0-9]{4,5})"', raw_search)
            if not paper_ids:
                paper_ids = re.findall(r'href="/abs/([0-9]{4}\.[0-9]{4,5})"', raw_search)

            for paper_id in paper_ids:
                if paper_id in seen_ids or len(results) >= max_publications:
                    continue
                seen_ids.add(paper_id)
                abs_url = f"https://arxiv.org/abs/{paper_id}"
                try:
                    abs_text = _fetch_text(abs_url)
                except Exception:
                    continue
                abs_path = publications_dir / f"{paper_id}_abs.html"
                asset_logger.write_text(
                    abs_path,
                    abs_text,
                    asset_type="arxiv_abs_html",
                    source_kind="publication_enrichment",
                    content_type="text/html",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={"paper_id": paper_id},
                )
                title_match = re.search(r'citation_title" content="([^"]+)"', abs_text)
                author_matches = re.findall(r'citation_author" content="([^"]+)"', abs_text)
                title = unescape(title_match.group(1)).strip() if title_match else paper_id
                year_match = re.search(r"originally announced</span>\s*([A-Za-z]{3},\s+\d{1,2}\s+\d{4})", abs_text)
                year = None
                if year_match:
                    year_digits = re.findall(r"\d{4}", year_match.group(1))
                    if year_digits:
                        year = int(year_digits[0])
                acknowledgement_names: list[str] = []
                html_url = f"https://arxiv.org/html/{paper_id}"
                try:
                    html_text = _fetch_text(html_url)
                    html_path = publications_dir / f"{paper_id}_full.html"
                    asset_logger.write_text(
                        html_path,
                        html_text,
                        asset_type="arxiv_full_html",
                        source_kind="publication_enrichment",
                        content_type="text/html",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={"paper_id": paper_id},
                    )
                    acknowledgement_names = extract_acknowledgement_names_from_html(html_text)
                except Exception:
                    acknowledgement_names = []

                results.append(
                    PublicationRecord(
                        publication_id=paper_id,
                        source="arxiv_affiliation_search",
                        source_dataset=f"{identity.company_key}_arxiv_affiliation",
                        source_path=str(abs_path),
                        title=title,
                        url=abs_url,
                        year=year,
                        authors=[unescape(item).strip() for item in author_matches if unescape(item).strip()],
                        acknowledgement_names=acknowledgement_names,
                    )
                )
                if len(results) >= max_publications:
                    break
            if len(results) >= max_publications:
                break
        return results

    def _collect_roster_anchored_scholar_coauthors(
        self,
        identity: CompanyIdentity,
        publications_dir: Path,
        candidates: list[Candidate],
        *,
        publications: list[PublicationRecord] | None = None,
        existing_evidence: list[EvidenceRecord] | None = None,
        asset_logger: AssetLogger,
    ) -> dict[str, Any]:
        scholar_dir = publications_dir / "roster_anchored_scholar_coauthors"
        scholar_dir.mkdir(parents=True, exist_ok=True)
        selected_seeds = _select_roster_anchored_scholar_seed_candidates(
            candidates,
            publications=publications or [],
            existing_evidence=existing_evidence or [],
            max_seeds=8,
        )
        seed_candidates = [item["candidate"] for item in selected_seeds]
        if not seed_candidates:
            return {"evidence": [], "prospect_candidates": [], "artifact_paths": {}}

        candidate_map = _candidate_name_map(candidates)
        evidence: list[EvidenceRecord] = []
        prospect_map: dict[str, dict[str, Any]] = {}
        scholar_edge_map: dict[tuple[str, str], dict[str, Any]] = {}
        seed_results: list[dict[str, Any]] = []
        seed_roster_records: list[dict[str, Any]] = []
        seed_publications: list[dict[str, Any]] = []
        recent_year_min = max(datetime.now(timezone.utc).year - 2, 2024)

        for seed_spec in selected_seeds:
            seed_candidate = seed_spec["candidate"]
            seed_signals = dict(seed_spec["selection_signals"])
            search_year_min = recent_year_min
            seed_publication_records = self._search_roster_anchored_scholar_publications(
                seed_candidate,
                identity,
                scholar_dir,
                max_publications=8,
                recent_year_min=search_year_min,
                asset_logger=asset_logger,
            )
            expanded_year_min = max(search_year_min - 2, 2022)
            if (
                not seed_publication_records
                and seed_signals.get("publication_signal_count", 0) > 0
                and expanded_year_min < search_year_min
            ):
                search_year_min = expanded_year_min
                seed_publication_records = self._search_roster_anchored_scholar_publications(
                    seed_candidate,
                    identity,
                    scholar_dir,
                    max_publications=8,
                    recent_year_min=search_year_min,
                    asset_logger=asset_logger,
                )
            publication_topics = sorted(
                {
                    topic
                    for publication in seed_publication_records
                    for topic in publication.topics
                    if str(topic or "").strip()
                }
            )
            seed_results.append(
                {
                    "seed_candidate_id": seed_candidate.candidate_id,
                    "seed_name": seed_candidate.display_name or seed_candidate.name_en,
                    "paper_count": len(seed_publication_records),
                    "query_recent_year_min": search_year_min,
                    "titles": [item.title for item in seed_publication_records[:8]],
                    "topics": publication_topics[:10],
                    "selection_score": int(seed_spec["selection_score"]),
                    "selection_signals": seed_signals,
                }
            )
            seed_display_name = seed_candidate.display_name or seed_candidate.name_en
            seed_roster_records.append(
                {
                    **seed_candidate.to_record(),
                    "selection_score": int(seed_spec["selection_score"]),
                    "selection_signals": seed_signals,
                }
            )
            seed_publications.append(
                {
                    "seed_candidate_id": seed_candidate.candidate_id,
                    "seed_name": seed_display_name,
                    "selection_score": int(seed_spec["selection_score"]),
                    "selection_signals": seed_signals,
                    "query_recent_year_min": search_year_min,
                    "publications": [
                        _build_seed_publication_record(seed_candidate, publication)
                        for publication in seed_publication_records
                    ],
                }
            )
            for publication in seed_publication_records:
                paper_ref = _build_scholar_publication_reference(publication)
                for author_name in _dedupe_names(publication.authors):
                    if _name_key(author_name) == _candidate_key(seed_candidate):
                        continue
                    if not _looks_like_person_name(author_name):
                        continue
                    matched_candidate = candidate_map.get(_name_key(author_name))
                    if matched_candidate is not None:
                        matched_name = matched_candidate.display_name or matched_candidate.name_en
                        _record_scholar_edge(
                            scholar_edge_map,
                            source_name=seed_display_name,
                            target_name=matched_name,
                            source_candidate_id=seed_candidate.candidate_id,
                            target_candidate_id=matched_candidate.candidate_id,
                            target_type="roster_member",
                            paper_ref=paper_ref,
                        )
                        evidence.append(
                            EvidenceRecord(
                                evidence_id=make_evidence_id(
                                    matched_candidate.candidate_id,
                                    publication.source_dataset,
                                    f"{publication.title} scholar coauthor with {seed_display_name}",
                                    publication.url,
                                ),
                                candidate_id=matched_candidate.candidate_id,
                                source_type="scholar_coauthor",
                                title=publication.title,
                                url=publication.url,
                                summary=f"{matched_name} co-authored this scholar publication with confirmed roster member {seed_display_name}.",
                                source_dataset=publication.source_dataset,
                                source_path=publication.source_path,
                                metadata={
                                    "publication_id": publication.publication_id,
                                    "seed_candidate_id": seed_candidate.candidate_id,
                                    "seed_name": seed_display_name,
                                    "matched_name": author_name,
                                },
                            )
                        )
                        continue

                    key = _name_key(author_name)
                    prospect = prospect_map.setdefault(
                        key,
                        {
                            "name": author_name,
                            "seed_names": [],
                            "papers": [],
                        },
                    )
                    if seed_display_name not in prospect["seed_names"]:
                        prospect["seed_names"].append(seed_display_name)
                    _record_scholar_edge(
                        scholar_edge_map,
                        source_name=seed_display_name,
                        target_name=author_name,
                        source_candidate_id=seed_candidate.candidate_id,
                        target_candidate_id="",
                        target_type="prospect",
                        paper_ref=paper_ref,
                    )
                    paper_record = dict(paper_ref)
                    paper_record["seed_name"] = seed_display_name
                    if paper_record not in prospect["papers"]:
                        prospect["papers"].append(paper_record)

        graph_path = scholar_dir / "scholar_coauthor_graph.json"
        prospects_path = scholar_dir / "scholar_coauthor_prospects.json"
        seed_roster_path = scholar_dir / "seed_roster.json"
        seed_publications_path = scholar_dir / "seed_publications.json"
        summary_path = scholar_dir / "summary.json"
        prospect_candidates = [
            _build_roster_anchored_scholar_coauthor_prospect(
                identity,
                prospect,
                source_path=prospects_path,
            )
            for prospect in sorted(
                prospect_map.values(),
                key=lambda item: (-len(item["seed_names"]), -len(item["papers"]), item["name"].lower()),
            )
        ]
        graph_edges = [
            {
                **edge,
                "paper_count": len(edge["papers"]),
            }
            for _, edge in sorted(
                scholar_edge_map.items(),
                key=lambda item: (
                    -len(item[1]["papers"]),
                    item[1]["source"].lower(),
                    item[1]["target"].lower(),
                ),
            )
        ]
        asset_logger.write_json(
            graph_path,
            graph_edges,
            asset_type="scholar_coauthor_graph",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        asset_logger.write_json(
            seed_roster_path,
            seed_roster_records,
            asset_type="scholar_coauthor_seed_roster",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        asset_logger.write_json(
            seed_publications_path,
            seed_publications,
            asset_type="scholar_coauthor_seed_publications",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        asset_logger.write_json(
            prospects_path,
            [candidate.to_record() for candidate in prospect_candidates],
            asset_type="scholar_coauthor_prospects",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        asset_logger.write_json(
            summary_path,
            {
                "target_company": identity.canonical_name,
                "seed_count": len(seed_candidates),
                "recent_year_min": recent_year_min,
                "seed_results": seed_results,
                "selection_strategy": "publication_signal_first",
                "graph_edge_count": len(graph_edges),
                "internal_overlap_count": sum(1 for edge in graph_edges if edge["target_type"] == "roster_member"),
                "prospect_count": len(prospect_candidates),
            },
            asset_type="scholar_coauthor_summary",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        return {
            "evidence": evidence,
            "prospect_candidates": prospect_candidates,
            "artifact_paths": {
                "scholar_coauthor_graph": str(graph_path),
                "scholar_coauthor_seed_roster": str(seed_roster_path),
                "scholar_coauthor_seed_publications": str(seed_publications_path),
                "scholar_coauthor_prospects": str(prospects_path),
                "scholar_coauthor_summary": str(summary_path),
            },
        }

    def _search_roster_anchored_scholar_publications(
        self,
        seed_candidate: Candidate,
        identity: CompanyIdentity,
        scholar_dir: Path,
        *,
        max_publications: int,
        recent_year_min: int,
        asset_logger: AssetLogger,
    ) -> list[PublicationRecord]:
        seed_name = (seed_candidate.display_name or seed_candidate.name_en).strip()
        if not seed_name:
            return []
        query = f'au:"{seed_name}"'
        feed_url = "https://export.arxiv.org/api/query?" + parse.urlencode(
            {
                "search_query": query,
                "start": 0,
                "max_results": max_publications,
                "sortBy": "submittedDate",
                "sortOrder": "descending",
            }
        )
        try:
            raw_feed = _fetch_text(feed_url)
        except Exception:
            return []
        raw_path = scholar_dir / f"{seed_candidate.candidate_id}_arxiv_author_feed.xml"
        asset_logger.write_text(
            raw_path,
            raw_feed,
            asset_type="scholar_author_feed_xml",
            source_kind="publication_enrichment",
            content_type="application/xml",
            is_raw_asset=True,
            model_safe=False,
            metadata={"seed_candidate_id": seed_candidate.candidate_id, "seed_name": seed_name, "url": feed_url},
        )
        try:
            root = ET.fromstring(raw_feed)
        except ET.ParseError:
            return []
        namespace = {"atom": "http://www.w3.org/2005/Atom"}
        results: list[PublicationRecord] = []
        seen: set[str] = set()
        for entry in root.findall("atom:entry", namespace):
            title = " ".join((entry.findtext("atom:title", default="", namespaces=namespace) or "").split()).strip()
            url = str(entry.findtext("atom:id", default="", namespaces=namespace) or "").strip()
            published = str(entry.findtext("atom:published", default="", namespaces=namespace) or "").strip()
            abstract = " ".join(
                (entry.findtext("atom:summary", default="", namespaces=namespace) or "").split()
            ).strip()
            year = None
            if published[:4].isdigit():
                year = int(published[:4])
            if year is not None and year < recent_year_min:
                continue
            authors = [
                " ".join((author.findtext("atom:name", default="", namespaces=namespace) or "").split()).strip()
                for author in entry.findall("atom:author", namespace)
            ]
            authors = [item for item in authors if item]
            topics = []
            for category in entry.findall("atom:category", namespace):
                term = str(category.attrib.get("term") or "").strip()
                if term and term not in topics:
                    topics.append(term)
            publication_id = url.rsplit("/", 1)[-1] if url else sha1(title.encode("utf-8")).hexdigest()[:12]
            if publication_id in seen or not title or len(authors) < 2:
                continue
            seen.add(publication_id)
            results.append(
                PublicationRecord(
                    publication_id=publication_id,
                    source="arxiv_author_seed_search",
                    source_dataset=f"{identity.company_key}_roster_anchored_scholar",
                    source_path=str(raw_path),
                    title=title,
                    url=url,
                    year=year,
                    authors=authors,
                    acknowledgement_names=[],
                    abstract=abstract,
                    topics=topics,
                )
            )
            if len(results) >= max_publications:
                break
        return results


def _select_roster_anchored_scholar_seed_candidates(
    candidates: list[Candidate],
    *,
    publications: list[PublicationRecord],
    existing_evidence: list[EvidenceRecord],
    max_seeds: int,
) -> list[dict[str, Any]]:
    eligible = [
        candidate
        for candidate in candidates
        if candidate.category in {"employee", "former_employee"}
        and _looks_like_person_name(candidate.display_name or candidate.name_en)
    ]
    if not eligible:
        return []

    candidate_map = _candidate_name_map(eligible)
    publication_author_counts: dict[str, int] = {}
    publication_ack_counts: dict[str, int] = {}
    for publication in publications:
        for name in publication.authors:
            matched_candidate = candidate_map.get(_name_key(name))
            if matched_candidate is None:
                continue
            publication_author_counts[matched_candidate.candidate_id] = (
                publication_author_counts.get(matched_candidate.candidate_id, 0) + 1
            )
        for name in publication.acknowledgement_names:
            matched_candidate = candidate_map.get(_name_key(name))
            if matched_candidate is None:
                continue
            publication_ack_counts[matched_candidate.candidate_id] = (
                publication_ack_counts.get(matched_candidate.candidate_id, 0) + 1
            )

    evidence_counts: dict[str, dict[str, int]] = {}
    for item in existing_evidence:
        candidate_counts = evidence_counts.setdefault(item.candidate_id, {})
        candidate_counts[item.source_type] = candidate_counts.get(item.source_type, 0) + 1

    ranked: list[dict[str, Any]] = []
    for candidate in eligible:
        metadata = dict(candidate.metadata or {})
        candidate_evidence_counts = evidence_counts.get(candidate.candidate_id, {})
        publication_metadata_count = int(
            bool(
                str(metadata.get("publication_id") or "").strip() or str(metadata.get("publication_url") or "").strip()
            )
        )
        current_publication_author_matches = int(publication_author_counts.get(candidate.candidate_id, 0))
        current_publication_ack_matches = int(publication_ack_counts.get(candidate.candidate_id, 0))
        existing_publication_author_evidence = int(candidate_evidence_counts.get("publication_author", 0))
        existing_publication_ack_evidence = int(candidate_evidence_counts.get("publication_acknowledgement", 0))
        existing_profile_publication_evidence = int(candidate_evidence_counts.get("linkedin_profile_publication", 0))
        manual_confirmed = str(metadata.get("membership_review_decision") or "").strip() == "manual_confirmed_member"

        score = _candidate_priority(candidate)
        score += publication_metadata_count * 80
        score += current_publication_author_matches * 36
        score += current_publication_ack_matches * 20
        score += existing_publication_author_evidence * 28
        score += existing_publication_ack_evidence * 16
        score += existing_profile_publication_evidence * 18
        if manual_confirmed:
            score += 6
        if candidate.linkedin_url:
            score += 2

        publication_signal_count = (
            publication_metadata_count
            + current_publication_author_matches
            + current_publication_ack_matches
            + existing_publication_author_evidence
            + existing_publication_ack_evidence
            + existing_profile_publication_evidence
        )
        reasons: list[str] = []
        if publication_metadata_count:
            reasons.append("candidate_metadata_publication")
        if current_publication_author_matches:
            reasons.append("current_publication_author_match")
        if current_publication_ack_matches:
            reasons.append("current_publication_acknowledgement_match")
        if existing_publication_author_evidence:
            reasons.append("historical_publication_author_evidence")
        if existing_publication_ack_evidence:
            reasons.append("historical_publication_acknowledgement_evidence")
        if existing_profile_publication_evidence:
            reasons.append("linkedin_profile_publication")
        if manual_confirmed:
            reasons.append("manual_confirmed_member")

        ranked.append(
            {
                "candidate": candidate,
                "selection_score": score,
                "selection_signals": {
                    "publication_signal_count": publication_signal_count,
                    "candidate_priority": _candidate_priority(candidate),
                    "publication_metadata_count": publication_metadata_count,
                    "current_publication_author_matches": current_publication_author_matches,
                    "current_publication_ack_matches": current_publication_ack_matches,
                    "existing_publication_author_evidence": existing_publication_author_evidence,
                    "existing_publication_ack_evidence": existing_publication_ack_evidence,
                    "existing_profile_publication_evidence": existing_profile_publication_evidence,
                    "manual_confirmed_member": manual_confirmed,
                    "selection_reasons": reasons,
                },
            }
        )

    ranked.sort(
        key=lambda item: (
            -int(item["selection_score"]),
            -int(item["selection_signals"]["publication_signal_count"]),
            -int(item["selection_signals"]["current_publication_author_matches"]),
            -int(item["selection_signals"]["existing_profile_publication_evidence"]),
            (item["candidate"].display_name or item["candidate"].name_en).lower(),
        )
    )
    return ranked[:max_seeds]


def _build_seed_publication_record(seed_candidate: Candidate, publication: PublicationRecord) -> dict[str, Any]:
    return {
        **publication.to_record(),
        "seed_candidate_id": seed_candidate.candidate_id,
        "seed_name": seed_candidate.display_name or seed_candidate.name_en,
        "coauthors": [
            name for name in _dedupe_names(publication.authors) if _name_key(name) != _candidate_key(seed_candidate)
        ],
        "research_direction": list(publication.topics[:8]),
        "abstract_excerpt": publication.abstract[:500],
    }


def _build_scholar_publication_reference(publication: PublicationRecord) -> dict[str, Any]:
    return {
        "publication_id": publication.publication_id,
        "title": publication.title,
        "url": publication.url,
        "year": publication.year,
        "topics": list(publication.topics[:8]),
    }


def _record_scholar_edge(
    edge_map: dict[tuple[str, str], dict[str, Any]],
    *,
    source_name: str,
    target_name: str,
    source_candidate_id: str,
    target_candidate_id: str,
    target_type: str,
    paper_ref: dict[str, Any],
) -> None:
    key = (source_name, target_name)
    edge = edge_map.setdefault(
        key,
        {
            "source": source_name,
            "target": target_name,
            "edge_type": "scholar_coauthor",
            "source_candidate_id": source_candidate_id,
            "target_candidate_id": target_candidate_id,
            "target_type": target_type,
            "papers": [],
        },
    )
    if paper_ref not in edge["papers"]:
        edge["papers"].append(paper_ref)


def extract_linkedin_profile_urls_from_search_html(html_text: str) -> list[str]:
    raw_urls = re.findall(r'result__a" href="([^"]+)"', html_text)
    results: list[str] = []
    for raw_url in raw_urls:
        candidate = unescape(raw_url)
        if "duckduckgo.com/l/" in candidate:
            parsed = parse.urlparse(candidate)
            query = parse.parse_qs(parsed.query)
            candidate = query.get("uddg", [candidate])[0]
        if "linkedin.com/in/" not in candidate:
            continue
        if candidate not in results:
            results.append(candidate)
    return results


def extract_linkedin_slug(url: str) -> str:
    match = re.search(r"linkedin\.com/in/([^/?#]+)", url)
    if not match:
        return ""
    return match.group(1).strip()


def build_people_search_url(account: RapidApiAccount, query: str, *, limit: int = 5) -> str:
    base = account.base_url.rstrip("/")
    if "z-real-time-linkedin-scraper-api1" in account.host:
        endpoint = base[: -len("/api/search/people")] if base.endswith("/api/search/people") else base
        return endpoint + "/api/search/people?" + parse.urlencode({"keywords": query, "limit": limit})
    endpoint_path = str(account.endpoint_search or "/api/search/people").split("?", 1)[0]
    return base + endpoint_path + "?" + parse.urlencode({"keywords": query, "limit": limit})


def build_basic_profile_url(account: RapidApiAccount, username: str) -> str:
    base = account.base_url.rstrip("/")
    if "z-real-time-linkedin-scraper-api1" in account.host:
        endpoint = base[: -len("/api/profile")] if base.endswith("/api/profile") else base
        return endpoint + "/api/profile?" + parse.urlencode({"username": username})
    endpoint_path = "/api/profile"
    return base + endpoint_path + "?" + parse.urlencode({"username": username})


def build_profile_detail_url(account: RapidApiAccount, slug: str) -> str:
    host = account.host
    base = account.base_url.rstrip("/")
    if "real-time-linkedin-data-scraper-api" in host:
        endpoint = base[: -len("/people/profile")] if base.endswith("/people/profile") else base
        return (
            endpoint
            + "/people/profile?"
            + parse.urlencode(
                {
                    "profile_id": slug,
                    "bypass_cache": "false",
                    "include_contact_info": "false",
                    "include_network_info": "false",
                }
            )
        )
    endpoint = base[: -len("/profile/detail")] if base.endswith("/profile/detail") else base
    return endpoint + "/profile/detail?" + parse.urlencode({"username": slug})


def extract_search_people_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    container = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    rows = list((container or {}).get("data") or [])
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "urn": str(row.get("urn") or "").strip(),
                "full_name": str(row.get("fullName") or row.get("full_name") or "").strip(),
                "headline": str(row.get("headline") or "").strip(),
                "location": str(row.get("location") or "").strip(),
            }
        )
    return normalized


def parse_basic_linkedin_profile_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    first_name = str(data.get("firstName") or data.get("first_name") or "").strip()
    last_name = str(data.get("lastName") or data.get("last_name") or "").strip()
    username = str(data.get("username") or data.get("public_identifier") or "").strip()
    headline = str(data.get("headline") or "").strip()
    location = ""
    raw_location = data.get("location") or {}
    if isinstance(raw_location, dict):
        location = str(raw_location.get("locationName") or raw_location.get("locationShortName") or "").strip()
    else:
        location = str(raw_location or "").strip()
    profile_url = str(data.get("profileUrl") or data.get("profile_url") or "").strip()
    if not profile_url and username:
        profile_url = f"https://www.linkedin.com/in/{username}/"
    full_name = " ".join(part for part in [first_name, last_name] if part).strip()
    return {
        "full_name": full_name,
        "username": username,
        "headline": headline,
        "location": location,
        "profile_url": profile_url,
        "urn": str(data.get("urn") or "").strip(),
    }


def parse_linkedin_profile_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if "raw" in payload:
        payload = payload["raw"]
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    basic_info = data.get("basic_info") if isinstance(data.get("basic_info"), dict) else data
    experience = list(data.get("experience") or [])
    education = list(data.get("education") or [])
    languages = list(data.get("languages") or basic_info.get("languages") or [])
    skills = list(data.get("skills") or basic_info.get("skills") or [])
    publications = list(data.get("publications") or [])

    first_name = str(basic_info.get("first_name") or data.get("first_name") or "").strip()
    last_name = str(basic_info.get("last_name") or data.get("last_name") or "").strip()
    full_name = (
        str(basic_info.get("fullname") or "").strip()
        or " ".join(part for part in [first_name, last_name] if part).strip()
    )
    headline = str(basic_info.get("headline") or data.get("headline") or "").strip()
    profile_url = str(basic_info.get("profile_url") or data.get("profile_url") or "").strip()
    public_identifier = str(basic_info.get("public_identifier") or data.get("public_identifier") or "").strip()
    summary = str(basic_info.get("about") or data.get("summary") or "").strip()

    location = ""
    raw_location = basic_info.get("location") or data.get("location") or {}
    if isinstance(raw_location, dict):
        location = str(raw_location.get("full") or raw_location.get("name") or "").strip()
    else:
        location = str(raw_location or "").strip()

    current_company = ""
    for item in experience:
        if item.get("is_current"):
            companies = _experience_company_labels(item)
            current_company = companies[0] if companies else ""
            break
    if not current_company and experience:
        companies = _experience_company_labels(experience[0])
        current_company = companies[0] if companies else ""
    if not current_company:
        current_company = str(basic_info.get("current_company") or "").strip()

    return {
        "full_name": full_name,
        "first_name": first_name,
        "last_name": last_name,
        "headline": headline,
        "profile_url": profile_url,
        "public_identifier": public_identifier,
        "summary": summary,
        "location": location,
        "current_company": current_company,
        "experience": experience,
        "education": education,
        "languages": languages,
        "skills": skills,
        "publications": publications,
    }


def extract_acknowledgement_names_from_html(html_text: str) -> list[str]:
    if len(html_text) < 5000:
        return []
    lower_text = html_text.lower()
    start = lower_text.find("acknowledg")
    if start < 0:
        return []
    snippet = html_text[start : start + 12000]
    text = re.sub(r"<[^>]+>", " ", snippet)
    text = unescape(text)
    text = " ".join(text.split())
    names = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][A-Za-z\-\']+)+\b", text)
    filtered: list[str] = []
    for name in names:
        lowered = name.lower()
        if lowered in ACK_STOPWORDS:
            continue
        if any(token in lowered for token in ACK_STOPWORDS):
            continue
        if name not in filtered:
            filtered.append(name)
    return filtered[:20]


def _build_people_search_queries(candidate: Candidate, identity: CompanyIdentity) -> list[str]:
    name = candidate.display_name or candidate.name_en
    compact_role = re.sub(r"[\|\(\)@]+", " ", (candidate.role or "").replace(identity.canonical_name, "")).strip()
    compact_role = " ".join(compact_role.split()[:4])
    queries = [
        f"{name} {identity.canonical_name}".strip(),
        name,
    ]
    if compact_role:
        queries.append(f"{name} {compact_role}".strip())

    deduped: list[str] = []
    for query in queries:
        normalized = " ".join(query.split())
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    return deduped


def _build_slug_queries(candidate: Candidate, identity: CompanyIdentity) -> list[str]:
    name = candidate.display_name or candidate.name_en
    compact_role = re.sub(r"[\|\(\)@]+", " ", (candidate.role or "").replace(identity.canonical_name, "")).strip()
    compact_role = " ".join(compact_role.split()[:4])
    queries = [
        f'"{name}" "{identity.canonical_name}" LinkedIn',
        f"{name} {identity.canonical_name} LinkedIn",
        f'"{name}" LinkedIn',
    ]
    if compact_role:
        role_fragment = compact_role
        queries.append(f'"{name}" "{role_fragment}" LinkedIn')
    return queries


def _search_result_name_matches_candidate(row: dict[str, Any], candidate: Candidate) -> bool:
    row_name = str(row.get("full_name") or "").strip()
    if not row_name:
        return False
    return _names_match(candidate.name_en, row_name)


def _profile_matches_candidate(
    profile: dict[str, Any],
    candidate: Candidate,
    identity: CompanyIdentity,
    *,
    model_client: ModelClient | None = None,
) -> bool:
    identifiers_overlap = bool(_candidate_profile_identifiers(candidate) & _profile_identifiers(profile))
    full_name = str(profile.get("full_name") or "").strip()
    if identifiers_overlap:
        if full_name and not _names_match(candidate.name_en, full_name):
            return False
        resolved_category, _ = _classify_profile_membership(profile, identity, model_client=model_client)
        if resolved_category in {"employee", "former_employee"}:
            return True
        return bool(
            candidate.category in {"employee", "former_employee"}
            or candidate.employment_status in {"current", "former"}
        )
    if not _names_match(candidate.name_en, full_name):
        return False
    resolved_category, _ = _classify_profile_membership(profile, identity, model_client=model_client)
    return resolved_category in {"employee", "former_employee"}


def _merge_profile_into_candidate(
    candidate: Candidate,
    profile: dict[str, Any],
    raw_path: Path,
    account_id: str,
    identity: CompanyIdentity,
    *,
    model_client: ModelClient | None = None,
    membership_review: dict[str, Any] | None = None,
) -> Candidate:
    resolved_category, resolved_employment_status = _classify_profile_membership(
        profile,
        identity,
        model_client=model_client,
    )
    can_refresh_membership = candidate.category in {"lead", "employee", "former_employee"}
    membership_review = dict(membership_review or {})
    review_required = bool(membership_review.get("requires_manual_review"))
    review_note = str(membership_review.get("note") or "").strip()
    review_rationale = str(membership_review.get("rationale") or "").strip()
    profile_media_url = _profile_media_url(profile)
    profile_avatar_url = str(profile.get("avatar_url") or profile_media_url).strip()
    profile_photo_url = str(profile.get("photo_url") or profile_media_url).strip()
    incoming = Candidate(
        candidate_id=candidate.candidate_id,
        name_en=profile.get("full_name") or candidate.name_en,
        display_name=profile.get("full_name") or candidate.display_name,
        category=resolved_category if can_refresh_membership else candidate.category,
        target_company=candidate.target_company,
        organization=candidate.organization,
        employment_status=resolved_employment_status if can_refresh_membership else candidate.employment_status,
        role=profile.get("headline") or candidate.role,
        team=candidate.team or _infer_team_from_text(profile.get("headline", "")),
        focus_areas=candidate.focus_areas or profile.get("headline", ""),
        education=_format_education(profile.get("education", [])),
        work_history=_format_experience(profile.get("experience", [])),
        notes=_join_nonempty(
            candidate.notes, review_note, review_rationale, profile.get("summary", ""), profile.get("location", "")
        ),
        linkedin_url=profile.get("profile_url", ""),
        media_url=profile_media_url,
        source_dataset=candidate.source_dataset,
        source_path=str(raw_path),
        metadata={
            **candidate.metadata,
            "public_identifier": profile.get("public_identifier", ""),
            "profile_account_id": account_id,
            "profile_location": profile.get("location", ""),
            "avatar_url": profile_avatar_url,
            "photo_url": profile_photo_url,
            "more_profiles": list(profile.get("more_profiles", []) or []),
            "headline": profile.get("headline", ""),
            "summary": profile.get("summary", ""),
            "languages": _format_profile_languages(profile.get("languages", [])),
            "skills": _format_profile_skills(profile.get("skills", [])),
            "membership_claim_category": resolved_category,
            "membership_claim_employment_status": resolved_employment_status,
            "membership_review_required": review_required,
            "membership_review_reason": "suspicious_membership" if review_required else "",
            "membership_review_decision": str(membership_review.get("decision") or "").strip(),
            "membership_review_confidence": str(membership_review.get("confidence_label") or "").strip(),
            "membership_review_rationale": review_rationale,
            "membership_review_triggers": list(membership_review.get("trigger_reasons") or []),
            "membership_review_trigger_keywords": list(membership_review.get("trigger_keywords") or []),
        },
    )
    merged = merge_candidate(candidate, incoming)
    merged_record = merged.to_record()
    if can_refresh_membership:
        merged_record["category"] = resolved_category
        merged_record["employment_status"] = resolved_employment_status
    if profile.get("profile_url"):
        merged_record["linkedin_url"] = profile["profile_url"]
    if profile_media_url and not str(merged_record.get("media_url") or "").strip():
        merged_record["media_url"] = profile_media_url
    if profile.get("headline"):
        merged_record["focus_areas"] = _join_nonempty(merged.focus_areas, profile["headline"])
    if incoming.education:
        merged_record["education"] = incoming.education
    if incoming.work_history:
        merged_record["work_history"] = incoming.work_history
    merged_record["notes"] = incoming.notes
    merged_record["source_path"] = str(raw_path)
    merged_record["metadata"] = incoming.metadata
    return Candidate(**merged_record)


def _profile_evidence_record(
    candidate: Candidate,
    profile: dict[str, Any],
    raw_path: Path,
    *,
    membership_review: dict[str, Any] | None = None,
) -> EvidenceRecord:
    title = profile.get("headline") or "LinkedIn profile detail"
    url = profile.get("profile_url") or candidate.linkedin_url
    membership_review = dict(membership_review or {})
    if membership_review.get("requires_manual_review"):
        summary = f"LinkedIn profile detail captured for {candidate.display_name}; target-company membership requires manual review."
    else:
        summary = f"LinkedIn profile detail validated {candidate.display_name} at {candidate.target_company}."
    return EvidenceRecord(
        evidence_id=make_evidence_id(candidate.candidate_id, "linkedin_profile_detail", title, url or str(raw_path)),
        candidate_id=candidate.candidate_id,
        source_type="linkedin_profile_detail",
        title=title,
        url=url,
        summary=summary,
        source_dataset="linkedin_profile_detail",
        source_path=str(raw_path),
        metadata={
            "public_identifier": profile.get("public_identifier", ""),
            "membership_review_required": bool(membership_review.get("requires_manual_review")),
            "membership_review_decision": str(membership_review.get("decision") or "").strip(),
        },
    )


def _profile_membership_review_evidence_record(
    candidate: Candidate,
    profile: dict[str, Any],
    raw_path: Path,
    review: dict[str, Any],
) -> EvidenceRecord:
    title = profile.get("headline") or "LinkedIn membership review"
    url = profile.get("profile_url") or candidate.linkedin_url
    rationale = str(review.get("rationale") or "").strip()
    summary = (
        f"LinkedIn membership review flagged {candidate.display_name} for manual review."
        if review.get("requires_manual_review")
        else f"LinkedIn membership review completed for {candidate.display_name}."
    )
    if rationale:
        summary = _join_nonempty(summary, rationale)
    return EvidenceRecord(
        evidence_id=make_evidence_id(
            candidate.candidate_id, "linkedin_profile_membership_review", title, url or str(raw_path)
        ),
        candidate_id=candidate.candidate_id,
        source_type="linkedin_profile_membership_review",
        title=title,
        url=url,
        summary=summary,
        source_dataset="linkedin_profile_membership_review",
        source_path=str(raw_path),
        metadata={
            "decision": str(review.get("decision") or "").strip(),
            "confidence_label": str(review.get("confidence_label") or "").strip(),
            "trigger_reasons": list(review.get("trigger_reasons") or []),
            "trigger_keywords": list(review.get("trigger_keywords") or []),
        },
    )


def _profile_non_member_evidence_record(
    candidate: Candidate, profile: dict[str, Any], raw_path: Path
) -> EvidenceRecord:
    title = profile.get("headline") or "LinkedIn profile detail"
    url = profile.get("profile_url") or candidate.linkedin_url
    summary = (
        f"LinkedIn profile detail did not confirm {candidate.display_name} as a {candidate.target_company} member."
    )
    return EvidenceRecord(
        evidence_id=make_evidence_id(
            candidate.candidate_id, "linkedin_profile_non_member", title, url or str(raw_path)
        ),
        candidate_id=candidate.candidate_id,
        source_type="linkedin_profile_non_member",
        title=title,
        url=url,
        summary=summary,
        source_dataset="linkedin_profile_non_member",
        source_path=str(raw_path),
        metadata={"public_identifier": profile.get("public_identifier", "")},
    )


def _extract_seed_slug(candidate: Candidate) -> str:
    seed_slug = str(candidate.metadata.get("seed_slug") or "").strip()
    if seed_slug:
        return seed_slug
    return extract_linkedin_slug(candidate.linkedin_url)


def _apply_verified_profile(
    candidate: Candidate,
    profile: dict[str, Any],
    raw_path: Path,
    account_id: str,
    slug: str,
    identity: CompanyIdentity,
    *,
    model_client: ModelClient | None = None,
    resolution_source: str,
) -> tuple[Candidate, dict[str, Any], list[EvidenceRecord]]:
    resolved_category, resolved_employment_status = _classify_profile_membership(
        profile,
        identity,
        model_client=model_client,
    )
    membership_review = _review_profile_membership(
        profile,
        identity,
        resolved_category=resolved_category,
        resolved_employment_status=resolved_employment_status,
        model_client=model_client,
    )
    if str(membership_review.get("decision") or "").strip() == "non_member":
        return _apply_non_member_profile(candidate, profile, raw_path, account_id)
    merged_candidate = _merge_profile_into_candidate(
        candidate,
        profile,
        raw_path,
        account_id,
        identity,
        model_client=model_client,
        membership_review=membership_review,
    )
    resolved_profile = {
        "candidate_id": candidate.candidate_id,
        "display_name": candidate.display_name,
        "slug": slug,
        "account_id": account_id,
        "profile_url": profile.get("profile_url", ""),
        "raw_path": str(raw_path),
        "resolution_source": resolution_source,
        "resolved_category": resolved_category,
        "resolved_employment_status": resolved_employment_status,
        "membership_review_required": bool(membership_review.get("requires_manual_review")),
        "membership_review_decision": str(membership_review.get("decision") or "").strip(),
        "membership_review_rationale": str(membership_review.get("rationale") or "").strip(),
    }
    evidence = [_profile_evidence_record(merged_candidate, profile, raw_path, membership_review=membership_review)]
    if membership_review.get("triggered"):
        evidence.append(
            _profile_membership_review_evidence_record(merged_candidate, profile, raw_path, membership_review)
        )
    for publication_item in profile.get("publications", [])[:5]:
        publication_url = str(publication_item.get("url", "")).strip()
        publication_title = str(
            publication_item.get("title") or publication_item.get("name") or "LinkedIn publication"
        ).strip()
        if not publication_url or not publication_title:
            continue
        evidence.append(
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    merged_candidate.candidate_id,
                    "linkedin_profile_publication",
                    publication_title,
                    publication_url,
                ),
                candidate_id=merged_candidate.candidate_id,
                source_type="linkedin_profile_publication",
                title=publication_title,
                url=publication_url,
                summary=f"{merged_candidate.display_name} listed this publication on LinkedIn.",
                source_dataset="linkedin_profile_publication",
                source_path=str(raw_path),
                metadata={"slug": slug, "account_id": account_id, "resolution_source": resolution_source},
            )
        )
    return merged_candidate, resolved_profile, evidence


def _apply_non_member_profile(
    candidate: Candidate,
    profile: dict[str, Any],
    raw_path: Path,
    account_id: str,
) -> tuple[Candidate, dict[str, Any], list[EvidenceRecord]]:
    profile_media_url = _profile_media_url(profile)
    profile_avatar_url = str(profile.get("avatar_url") or profile_media_url).strip()
    profile_photo_url = str(profile.get("photo_url") or profile_media_url).strip()
    incoming = Candidate(
        candidate_id=candidate.candidate_id,
        name_en=profile.get("full_name") or candidate.name_en,
        display_name=profile.get("full_name") or candidate.display_name,
        category="non_member",
        target_company=candidate.target_company,
        organization=profile.get("current_company") or candidate.organization,
        employment_status="",
        role=profile.get("headline") or candidate.role,
        team=candidate.team or _infer_team_from_text(profile.get("headline", "")),
        focus_areas=candidate.focus_areas or profile.get("headline", ""),
        education=_format_education(profile.get("education", [])),
        work_history=_format_experience(profile.get("experience", [])),
        notes=_join_nonempty(
            candidate.notes,
            "Profile detail fetched but target-company experience was not confirmed.",
            profile.get("summary", ""),
            profile.get("location", ""),
        ),
        linkedin_url=profile.get("profile_url", "") or candidate.linkedin_url,
        media_url=profile_media_url,
        source_dataset=candidate.source_dataset,
        source_path=str(raw_path),
        metadata={
            **candidate.metadata,
            "public_identifier": profile.get("public_identifier", ""),
            "profile_account_id": account_id,
            "profile_location": profile.get("location", ""),
            "avatar_url": profile_avatar_url,
            "photo_url": profile_photo_url,
            "more_profiles": list(profile.get("more_profiles", []) or []),
            "headline": profile.get("headline", ""),
            "summary": profile.get("summary", ""),
            "languages": _format_profile_languages(profile.get("languages", [])),
            "skills": _format_profile_skills(profile.get("skills", [])),
            "target_company_mismatch": True,
        },
    )
    merged = merge_candidate(candidate, incoming)
    merged_record = merged.to_record()
    merged_record["category"] = "non_member"
    merged_record["employment_status"] = ""
    merged_record["organization"] = incoming.organization
    if incoming.linkedin_url:
        merged_record["linkedin_url"] = incoming.linkedin_url
    if profile_media_url and not str(merged_record.get("media_url") or "").strip():
        merged_record["media_url"] = profile_media_url
    if incoming.education:
        merged_record["education"] = incoming.education
    if incoming.work_history:
        merged_record["work_history"] = incoming.work_history
    merged_record["notes"] = incoming.notes
    merged_record["source_path"] = str(raw_path)
    merged_record["metadata"] = incoming.metadata
    merged_candidate = Candidate(**merged_record)
    resolution = {
        "candidate_id": candidate.candidate_id,
        "display_name": candidate.display_name,
        "account_id": account_id,
        "profile_url": profile.get("profile_url", ""),
        "raw_path": str(raw_path),
        "resolution_source": "company_asset_completion_non_member",
    }
    evidence = [_profile_non_member_evidence_record(merged_candidate, profile, raw_path)]
    return merged_candidate, resolution, evidence


def _profile_media_url(profile: dict[str, Any]) -> str:
    for key in ("avatar_url", "photo_url", "media_url", "picture_url", "profile_picture_url"):
        value = str(profile.get(key) or "").strip()
        if value:
            return value
    return ""


def _dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _profile_registry_sources_for_candidate(candidate: Candidate) -> list[str]:
    metadata = dict(candidate.metadata or {})
    labels: list[str] = []
    candidate_id = str(candidate.candidate_id or "").strip()
    if candidate_id:
        labels.append(f"candidate:{candidate_id}")
    source_dataset = str(candidate.source_dataset or "").strip()
    if source_dataset:
        labels.append(f"dataset:{source_dataset}")
    source_path = str(candidate.source_path or "").strip()
    if source_path:
        labels.append(f"source_path:{source_path[:240]}")
    for key in [
        "source_shard_id",
        "source_shard_title",
        "source_family",
        "source_query",
        "source_type",
        "seed_source_type",
        "seed_query",
        "mode",
        "account_id",
        "query_bundle_id",
        "strategy_id",
    ]:
        value = str(metadata.get(key) or "").strip()
        if value:
            labels.append(f"{key}:{value[:160]}")
    for key in ["source_queries", "scope_keywords", "keywords"]:
        values = metadata.get(key)
        if isinstance(values, (list, tuple, set)):
            for item in list(values)[:3]:
                value = str(item or "").strip()
                if value:
                    labels.append(f"{key}:{value[:160]}")
    return _dedupe_strings(labels)


def _profile_registry_lease_owner(prefix: str) -> str:
    normalized_prefix = str(prefix or "profile_registry").strip() or "profile_registry"
    return f"{normalized_prefix}:{os.getpid()}:{threading.get_ident()}"


def _profile_registry_alias_metadata(profile_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    payload_dict = dict(payload or {})
    alias_metadata = dict(payload_dict.get("profile_registry_aliases") or {})
    if not alias_metadata:
        parsed = dict(payload_dict.get("parsed") or {})
        alias_urls = _dedupe_strings(
            [
                profile_url,
                str(parsed.get("requested_profile_url") or ""),
                str(parsed.get("profile_url") or ""),
                str(parsed.get("url") or ""),
            ]
        )
        alias_metadata = {
            "alias_urls": alias_urls,
            "raw_linkedin_url": str(parsed.get("requested_profile_url") or profile_url),
            "sanity_linkedin_url": str(parsed.get("profile_url") or ""),
        }
    alias_urls = _dedupe_strings(
        [
            *list(alias_metadata.get("alias_urls") or []),
            profile_url,
            str(alias_metadata.get("raw_linkedin_url") or ""),
            str(alias_metadata.get("sanity_linkedin_url") or ""),
        ]
    )
    return {
        "alias_urls": alias_urls,
        "raw_linkedin_url": str(alias_metadata.get("raw_linkedin_url") or profile_url).strip(),
        "sanity_linkedin_url": str(alias_metadata.get("sanity_linkedin_url") or "").strip(),
    }


def _load_harvest_profile_payload_from_snapshot_cache(
    *,
    snapshot_dir: Path,
    profile_url: str,
    normalized_profile_key: str = "",
) -> dict[str, Any] | None:
    harvest_dir = snapshot_dir / "harvest_profiles"
    if not harvest_dir.exists():
        return None
    for candidate_path in profile_cache_path_candidates(
        harvest_dir,
        profile_url,
        normalized_profile_url=str(normalized_profile_key or "").strip(),
    ):
        cached_payload = _load_harvest_profile_payload_from_raw_path(str(candidate_path))
        if cached_payload is not None:
            return cached_payload
    return None


def _load_harvest_profile_payload_from_registry_or_snapshot(
    *,
    registry_entry: dict[str, Any] | None,
    snapshot_dir: Path,
    profile_url: str,
    normalized_profile_key: str = "",
) -> dict[str, Any] | None:
    entry = dict(registry_entry or {})
    cached = _load_harvest_profile_payload_from_raw_path(str(entry.get("last_raw_path") or ""))
    if cached is not None:
        return cached
    return _load_harvest_profile_payload_from_snapshot_cache(
        snapshot_dir=snapshot_dir,
        profile_url=profile_url,
        normalized_profile_key=normalized_profile_key,
    )


def _profile_registry_cached_marker(registry_entry: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return a lightweight fetched marker without parsing the raw Harvest profile payload."""
    entry = dict(registry_entry or {})
    if str(entry.get("status") or "").strip().lower() != "fetched":
        return None
    raw_path = str(entry.get("last_raw_path") or "").strip()
    if not raw_path:
        return None
    path = Path(raw_path).expanduser()
    if not path.exists():
        return None
    return {
        "raw_path": str(path),
        "account_id": "harvest_profile_registry",
        "profile_registry_marker": True,
    }


def _profile_registry_queue_duration_ms(registry_entry: dict[str, Any]) -> int | None:
    queued_at = str(registry_entry.get("last_queued_at") or registry_entry.get("first_queued_at") or "").strip()
    fetched_at = str(registry_entry.get("last_fetched_at") or "").strip()
    if not queued_at or not fetched_at:
        return None
    try:
        queued_dt = datetime.strptime(queued_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        fetched_dt = datetime.strptime(fetched_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return max(0, int((fetched_dt - queued_dt).total_seconds() * 1000))


def _load_harvest_profile_payload_from_raw_path(raw_path: str) -> dict[str, Any] | None:
    normalized_raw_path = str(raw_path or "").strip()
    if not normalized_raw_path:
        return None
    path = Path(normalized_raw_path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not harvest_profile_payload_has_usable_content(payload):
        return None
    alias_metadata = extract_profile_registry_aliases_from_payload(payload)
    return {
        "raw_path": path,
        "account_id": "harvest_profile_registry",
        "raw_payload": payload,
        "parsed": parse_harvest_profile_payload(payload),
        "profile_registry_aliases": alias_metadata,
    }


def _candidate_profile_urls(candidate: Candidate) -> list[str]:
    urls: list[str] = []
    for value in [candidate.linkedin_url, str(candidate.metadata.get("profile_url") or "").strip()]:
        normalized = str(value or "").strip()
        if normalized and normalized not in urls:
            urls.append(normalized)
    return urls


def _candidate_profile_identifiers(candidate: Candidate) -> set[str]:
    metadata = dict(candidate.metadata or {})
    return _linkedin_identifier_set(
        [
            candidate.linkedin_url,
            metadata.get("profile_url"),
            metadata.get("seed_slug"),
            metadata.get("public_identifier"),
            metadata.get("more_profiles"),
        ]
    )


def _profile_identifiers(profile: dict[str, Any]) -> set[str]:
    # Track B diagnosis fix: `requested_profile_url` is the URL that was *requested* (the candidate's
    # seed LinkedIn URL), not the *resolved* person the provider returned. Including it here let a
    # former-false-positive resolve-target overlap the candidate's identity and be miscounted as a
    # member. Identity matching must use only resolved-person identifiers. (requested_profile_url keeps
    # its legitimate registry-alias-linking uses elsewhere — those are separate code paths.)
    return _linkedin_identifier_set(
        [
            profile.get("profile_url"),
            profile.get("public_identifier"),
            profile.get("username"),
            profile.get("more_profiles"),
        ]
    )


def _linkedin_identifier_set(values: list[Any]) -> set[str]:
    identifiers: set[str] = set()
    for value in values:
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
    raw = unescape(str(value or "")).strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if "linkedin.com" in lowered:
        slug = extract_linkedin_slug(raw)
        if slug:
            return slug.strip().lower()
        parsed = parse.urlparse(raw if "://" in raw else f"https://{raw.lstrip('/')}")
        host = parsed.netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        if host.startswith("m."):
            host = host[2:]
        path = re.sub(r"/+", "/", parsed.path or "").strip().rstrip("/")
        return f"{host}{path.lower()}"
    return raw.strip().strip("/").lower()


def _company_variants(identity: CompanyIdentity) -> set[str]:
    variants = {_normalize(identity.canonical_name)}
    variants.update(_normalize(alias) for alias in identity.aliases if alias)
    variants.update(
        _normalize(item)
        for item in [identity.linkedin_slug, _company_slug_from_url(identity.linkedin_company_url)]
        if item
    )
    variants.update(_verified_equivalent_company_variants(identity))
    return {item for item in variants if item}


def _company_reference_labels(identity: CompanyIdentity) -> list[str]:
    labels: list[str] = []
    for value in [
        identity.canonical_name,
        *identity.aliases,
        identity.linkedin_slug,
        _company_slug_from_url(identity.linkedin_company_url),
    ]:
        text = str(value or "").strip()
        if text and text not in labels:
            labels.append(text)
    metadata = dict(identity.metadata or {})
    for item in list(metadata.get("equivalent_company_names") or []):
        if not isinstance(item, dict):
            continue
        text = str(item.get("name") or item.get("company_name") or item.get("canonical_name") or "").strip()
        if text and text not in labels:
            labels.append(text)
    return labels


def _verified_equivalent_company_variants(identity: CompanyIdentity) -> set[str]:
    metadata = dict(identity.metadata or {})
    equivalent_items = list(metadata.get("equivalent_company_names") or [])
    identity_company_keys = {
        _normalize(str(identity.linkedin_slug or "")),
        _normalize(_company_slug_from_url(identity.linkedin_company_url)),
    }
    identity_company_keys.discard("")
    if not identity_company_keys:
        return set()

    variants: set[str] = set()
    for item in equivalent_items:
        if not isinstance(item, dict):
            continue
        normalized = _normalize(str(item.get("name") or item.get("company_name") or item.get("canonical_name") or ""))
        if not normalized:
            continue
        item_company_keys = {
            _normalize(str(item.get("linkedin_slug") or "")),
            _normalize(
                _company_slug_from_url(str(item.get("linkedin_company_url") or item.get("companyLinkedinUrl") or ""))
            ),
        }
        item_company_keys.discard("")
        if item_company_keys & identity_company_keys:
            variants.add(normalized)
    return variants


def _company_label_matches_identity(
    label: str,
    identity: CompanyIdentity,
    *,
    company_variants: set[str] | None = None,
    model_client: ModelClient | None = None,
) -> bool:
    normalized = _normalize(label)
    if not normalized:
        return False
    variants = company_variants or _company_variants(identity)
    if normalized in variants:
        return True
    if not _should_attempt_ai_company_equivalence(label, identity, company_variants=variants):
        return False
    return _ai_company_equivalence_matches(label, identity, model_client=model_client)


def _should_attempt_ai_company_equivalence(
    label: str,
    identity: CompanyIdentity,
    *,
    company_variants: set[str],
) -> bool:
    normalized = _normalize(label)
    if not normalized or normalized in company_variants:
        return False
    label_tokens = _company_name_tokens(label)
    if not label_tokens and len(normalized) < 10:
        return False
    for reference in _company_reference_labels(identity):
        reference_normalized = _normalize(reference)
        if not reference_normalized or reference_normalized == normalized:
            continue
        reference_tokens = _company_name_tokens(reference)
        overlap = label_tokens & reference_tokens
        if len(overlap) >= 2 and len(overlap) >= max(2, min(len(label_tokens), len(reference_tokens)) - 1):
            return True
        prefix_length = _common_prefix_length(normalized, reference_normalized)
        if prefix_length >= 10 and prefix_length / max(1, min(len(normalized), len(reference_normalized))) >= 0.6:
            return True
    return False


def _company_name_tokens(value: str) -> set[str]:
    stopwords = {"and", "the", "inc", "llc", "corp", "co", "company", "limited", "ltd", "plc", "gmbh", "sa"}
    return {
        token
        for token in re.split(r"[^a-z0-9]+", str(value or "").lower())
        if token and token not in stopwords and len(token) >= 2
    }


def _common_prefix_length(left: str, right: str) -> int:
    count = 0
    for left_char, right_char in zip(left, right):
        if left_char != right_char:
            break
        count += 1
    return count


def _ai_company_equivalence_matches(
    label: str,
    identity: CompanyIdentity,
    *,
    model_client: ModelClient | None = None,
) -> bool:
    if model_client is None:
        return False
    if not isinstance(identity.metadata, dict):
        identity.metadata = {}
    cache = identity.metadata.setdefault("_runtime_ai_company_equivalence_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        identity.metadata["_runtime_ai_company_equivalence_cache"] = cache
    normalized = _normalize(label)
    cached = cache.get(normalized)
    if isinstance(cached, dict):
        return bool(cached.get("is_equivalent"))
    decision = model_client.judge_company_equivalence(
        {
            "target_company": {
                "canonical_name": identity.canonical_name,
                "linkedin_slug": identity.linkedin_slug,
                "linkedin_company_url": identity.linkedin_company_url,
                "aliases": _company_reference_labels(identity),
            },
            "observed_companies": [
                {
                    "label": str(label or "").strip(),
                    "normalized_label": normalized,
                }
            ],
        }
    )
    matched_label = str(decision.get("matched_label") or "").strip()
    is_equivalent = str(decision.get("decision") or "uncertain").strip().lower() == "same_company" and (
        not matched_label or _normalize(matched_label) == normalized
    )
    cache[normalized] = {
        "is_equivalent": is_equivalent,
        "matched_label": matched_label,
        "decision": str(decision.get("decision") or "uncertain").strip(),
        "confidence_label": str(decision.get("confidence_label") or "").strip(),
        "rationale": str(decision.get("rationale") or "").strip(),
    }
    return is_equivalent


def _experience_matches_company(
    item: dict[str, Any],
    company_variants: set[str],
    *,
    identity: CompanyIdentity | None = None,
    model_client: ModelClient | None = None,
) -> bool:
    for company in _experience_company_labels(item):
        if _normalize(company) in company_variants:
            return True
        if identity is not None and _company_label_matches_identity(
            company,
            identity,
            company_variants=company_variants,
            model_client=model_client,
        ):
            return True
    return False


def _experience_company_labels(item: dict[str, Any]) -> list[str]:
    values = [
        item.get("company"),
        item.get("company_name"),
        item.get("companyName"),
        item.get("companyUniversalName"),
        _company_slug_from_url(str(item.get("companyLinkedinUrl") or "")),
    ]
    results: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in results:
            results.append(text)
    return results


def _company_slug_from_url(url: str) -> str:
    match = re.search(r"linkedin\.com/company/([^/?#]+)", str(url or ""), re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()


def _matched_profile_experiences(
    profile: dict[str, Any],
    identity: CompanyIdentity,
    *,
    company_variants: set[str] | None = None,
    model_client: ModelClient | None = None,
) -> list[dict[str, Any]]:
    variants = company_variants or _company_variants(identity)
    matched: list[dict[str, Any]] = []
    for item in profile.get("experience", []) or []:
        if _experience_matches_company(
            item,
            variants,
            identity=identity,
            model_client=model_client,
        ):
            matched.append(item)
    return matched


def _review_profile_membership(
    profile: dict[str, Any],
    identity: CompanyIdentity,
    *,
    resolved_category: str,
    resolved_employment_status: str,
    model_client: ModelClient | None = None,
) -> dict[str, Any]:
    if resolved_category not in {"employee", "former_employee"}:
        return {
            "triggered": False,
            "decision": "",
            "confidence_label": "",
            "rationale": "",
            "requires_manual_review": False,
            "trigger_reasons": [],
            "trigger_keywords": [],
            "note": "",
        }

    company_variants = _company_variants(identity)
    matched_experiences = _matched_profile_experiences(
        profile,
        identity,
        company_variants=company_variants,
        model_client=model_client,
    )
    trigger_keywords = _profile_membership_suspicious_keywords(profile, matched_experiences)
    if len(trigger_keywords) < 2:
        return {
            "triggered": False,
            "decision": "",
            "confidence_label": "",
            "rationale": "",
            "requires_manual_review": False,
            "trigger_reasons": [],
            "trigger_keywords": [],
            "note": "",
        }

    response = (
        model_client.judge_profile_membership(
            {
                "target_company": {
                    "canonical_name": identity.canonical_name,
                    "linkedin_slug": identity.linkedin_slug,
                    "linkedin_company_url": identity.linkedin_company_url,
                    "aliases": _company_reference_labels(identity),
                },
                "candidate_membership": {
                    "resolved_category": resolved_category,
                    "resolved_employment_status": resolved_employment_status,
                    "current_company": str(profile.get("current_company") or "").strip(),
                    "matched_experiences": [
                        {
                            "title": str(item.get("title") or item.get("position") or "").strip(),
                            "company_name": str(
                                item.get("companyName") or item.get("company_name") or item.get("company") or ""
                            ).strip(),
                            "company_linkedin_url": str(item.get("companyLinkedinUrl") or "").strip(),
                            "is_current": bool(
                                item.get("is_current") or item.get("isCurrent") or not item.get("endDate")
                            ),
                            "description": str(item.get("description") or "").strip(),
                            "skills": [
                                str(value).strip() for value in list(item.get("skills") or []) if str(value).strip()
                            ][:5],
                        }
                        for item in matched_experiences[:3]
                    ],
                },
                "profile": {
                    "full_name": str(profile.get("full_name") or "").strip(),
                    "headline": str(profile.get("headline") or "").strip(),
                    "summary": str(profile.get("summary") or "").strip()[:1200],
                    "location": str(profile.get("location") or "").strip(),
                },
                "heuristic_triggers": {
                    "reasons": ["suspicious_profile_content"],
                    "keywords": trigger_keywords[:12],
                },
            }
        )
        if model_client is not None
        else {}
    )
    decision = str(response.get("decision") or "uncertain").strip().lower()
    if decision not in {"confirmed_member", "suspicious_member", "non_member", "uncertain"}:
        decision = "uncertain"
    confidence_label = str(response.get("confidence_label") or "low").strip() or "low"
    rationale = str(response.get("rationale") or "").strip()
    requires_manual_review = decision in {"suspicious_member", "uncertain"}
    note = ""
    if requires_manual_review:
        note = "Structured profile signals target-company membership, but the profile content requires manual review."
    return {
        "triggered": True,
        "decision": decision,
        "confidence_label": confidence_label,
        "rationale": rationale,
        "requires_manual_review": requires_manual_review,
        "trigger_reasons": ["suspicious_profile_content"],
        "trigger_keywords": trigger_keywords[:12],
        "note": note,
    }


def _profile_membership_suspicious_keywords(
    profile: dict[str, Any], matched_experiences: list[dict[str, Any]]
) -> list[str]:
    parts = [
        str(profile.get("headline") or "").strip(),
        str(profile.get("summary") or "").strip(),
    ]
    for item in matched_experiences[:3]:
        parts.append(str(item.get("description") or "").strip())
        parts.extend(str(value).strip() for value in list(item.get("skills") or []) if str(value).strip())
    combined = " ".join(part for part in parts if part).lower()
    hits: list[str] = []
    for keyword in sorted(SUSPICIOUS_PROFILE_KEYWORDS):
        if keyword in combined and keyword not in hits:
            hits.append(keyword)
    return hits


def _classify_profile_membership(
    profile: dict[str, Any],
    identity: CompanyIdentity,
    *,
    model_client: ModelClient | None = None,
) -> tuple[str, str]:
    company_variants = _company_variants(identity)
    current_company = str(profile.get("current_company", "")).strip()
    if _company_label_matches_identity(
        current_company,
        identity,
        company_variants=company_variants,
        model_client=model_client,
    ):
        return "employee", "current"
    for item in _matched_profile_experiences(
        profile,
        identity,
        company_variants=company_variants,
        model_client=model_client,
    ):
        if item.get("is_current"):
            return "employee", "current"
        return "former_employee", "former"
    return "lead", ""


def _exploration_targets(candidates: list[Candidate], unresolved_candidates: list[dict[str, Any]]) -> list[Candidate]:
    unresolved_ids = {
        str(item.get("candidate_id") or "").strip() for item in unresolved_candidates if item.get("candidate_id")
    }
    targets: list[Candidate] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate.candidate_id in seen:
            continue
        should_explore = (
            candidate.candidate_id in unresolved_ids or candidate.category == "lead" or not candidate.linkedin_url
        )
        if should_explore:
            seen.add(candidate.candidate_id)
            targets.append(candidate)
    return _prioritize_candidates(targets)


def _drop_unresolved_candidate(unresolved_candidates: list[dict[str, Any]], candidate_id: str) -> None:
    unresolved_candidates[:] = [
        item
        for item in unresolved_candidates
        if str(item.get("candidate_id") or "").strip() != str(candidate_id or "").strip()
    ]


def _load_local_publications(path: Path, max_publications: int) -> list[PublicationRecord]:
    payload = json.loads(path.read_text())
    publications = list((payload.get("publications") or {}).values())
    publications.sort(key=lambda item: (item.get("year") or 0, item.get("month") or 0), reverse=True)
    results: list[PublicationRecord] = []
    for item in publications:
        if not item.get("is_anthropic_paper"):
            continue
        results.append(
            PublicationRecord(
                publication_id=str(item.get("id") or item.get("url") or item.get("title")),
                source=str(item.get("source") or "local_publications"),
                source_dataset="anthropic_publications_unified",
                source_path=str(path),
                title=str(item.get("title") or "").strip(),
                url=str(item.get("url") or "").strip(),
                year=item.get("year"),
                authors=[str(name).strip() for name in item.get("authors_list") or [] if str(name).strip()],
                acknowledgement_names=[
                    str(name).strip()
                    for name in (item.get("ack") or {}).get("names", [])
                    + (item.get("ack") or {}).get("chinese_candidates", [])
                    if str(name).strip()
                ],
            )
        )
        if len(results) >= max_publications:
            break
    return results


def _dedupe_publication_records(records: list[PublicationRecord], limit: int) -> list[PublicationRecord]:
    deduped: list[PublicationRecord] = []
    seen: set[str] = set()
    for record in records:
        key = str(record.url or record.publication_id or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(record)
        if len(deduped) >= limit:
            break
    return deduped


def _identity_site_root(identity: CompanyIdentity) -> str:
    domain = str(identity.domain or "").strip().lower()
    if not domain:
        return ""
    return f"https://{domain.rstrip('/')}"


def _discover_official_surface_urls(identity: CompanyIdentity, site_root: str, home_html: str) -> list[str]:
    candidates = [
        f"{site_root}/index.xml",
        f"{site_root}/blog/",
        f"{site_root}/blog/index.xml",
        f"{site_root}/news/",
        f"{site_root}/news/index.xml",
    ]
    home_links = set(re.findall(r'href="([^"]+)"', home_html or ""))
    for raw_link in home_links:
        link = str(raw_link or "").strip()
        if not link:
            continue
        if link.startswith("http://") or link.startswith("https://"):
            if not link.startswith(site_root):
                continue
            normalized = link.rstrip("/")
        elif link.startswith("/"):
            normalized = f"{site_root}{link}".rstrip("/")
        else:
            continue
        lowered = normalized.lower()
        if any(
            token in lowered
            for token in ["/blog", "/news", "/docs", "/research", "/engineering", "/tinker", "index.xml"]
        ):
            candidates.append(
                normalized
                if normalized.endswith(".xml")
                else f"{normalized}/"
                if not normalized.endswith("/")
                else normalized
            )
            if not normalized.endswith(".xml"):
                candidates.append(f"{normalized}/index.xml")
    deduped: list[str] = []
    seen: set[str] = set()
    for url in candidates:
        normalized = str(url or "").strip()
        if not normalized:
            continue
        if normalized.endswith("/") and normalized != f"{site_root}/":
            normalized = normalized.rstrip("/") + "/"
        lowered = normalized.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(normalized)
    return deduped


def _surface_asset_name(url: str) -> str:
    parsed = parse.urlparse(url)
    path_token = normalize_name_token(parsed.path or "surface")
    if not path_token:
        path_token = "surface"
    suffix = ".xml" if parsed.path.endswith(".xml") else ".html"
    return f"{path_token}{suffix}"


def _extract_publications_from_rss(xml_text: str, feed_url: str, source_path: str) -> list[PublicationRecord]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    items = root.findall(".//item")
    results: list[PublicationRecord] = []
    for item in items:
        title = _xml_text(item.find("title"))
        url = _xml_text(item.find("link"))
        if not title or not url:
            continue
        description = unescape(_xml_text(item.find("description")))
        authors = _parse_author_text(_extract_author_text_from_description(description))
        publication_id = sha1("|".join([feed_url, title, url]).encode("utf-8")).hexdigest()[:16]
        results.append(
            PublicationRecord(
                publication_id=publication_id,
                source="official_rss_feed",
                source_dataset=f"{normalize_name_token(parse.urlparse(feed_url).netloc)}_official_feed",
                source_path=source_path,
                title=title,
                url=url,
                year=_extract_year_from_text(_xml_text(item.find("pubDate"))),
                authors=authors,
                acknowledgement_names=[],
            )
        )
    return results


def _xml_text(element: ET.Element | None) -> str:
    if element is None or element.text is None:
        return ""
    return str(element.text).strip()


def _extract_author_text_from_description(description: str) -> str:
    match = re.search(r"author-date[^>]*>\s*([^<]+?)\s*</div>", description, flags=re.IGNORECASE)
    if match:
        return " ".join(unescape(match.group(1)).split())
    return ""


def _extract_publications_from_surface_index(
    html_text: str, surface_url: str, source_path: str
) -> list[PublicationRecord]:
    results: list[PublicationRecord] = []
    item_pattern = re.compile(
        r'<a class="post-item-link" href="([^"]+)".*?<div class="post-title">(.*?)</div>(?:.*?<div class="author-date">\s*(.*?)\s*</div>)?',
        re.DOTALL,
    )
    for raw_url, raw_title, raw_author in item_pattern.findall(html_text):
        absolute_url = parse.urljoin(surface_url, unescape(raw_url))
        title = " ".join(unescape(re.sub(r"<[^>]+>", " ", raw_title)).split())
        if not title or not absolute_url:
            continue
        author_text = " ".join(unescape(re.sub(r"<[^>]+>", " ", raw_author or "")).split())
        authors = _parse_author_text(author_text)
        results.append(
            PublicationRecord(
                publication_id=sha1("|".join([surface_url, title, absolute_url]).encode("utf-8")).hexdigest()[:16],
                source="official_surface_index",
                source_dataset=f"{normalize_name_token(parse.urlparse(surface_url).path or 'official_surface')}_index",
                source_path=source_path,
                title=title,
                url=absolute_url,
                year=None,
                authors=authors,
                acknowledgement_names=[],
            )
        )
    return results


def _extract_single_surface_page_record(html_text: str, url: str, source_path: str) -> list[PublicationRecord]:
    title = _extract_page_title(html_text)
    if not title:
        return []
    authors = _extract_page_authors(html_text)
    return [
        PublicationRecord(
            publication_id=sha1("|".join([url, title]).encode("utf-8")).hexdigest()[:16],
            source="official_surface_page",
            source_dataset=f"{normalize_name_token(parse.urlparse(url).path or 'official_surface')}_page",
            source_path=source_path,
            title=title,
            url=url,
            year=_extract_year_from_html(html_text),
            authors=authors,
            acknowledgement_names=[],
        )
    ]


def _extract_page_title(html_text: str) -> str:
    for pattern in [
        r'<meta itemprop="name" content="([^"]+)"',
        r'<meta property="og:title" content="([^"]+)"',
        r"<title>(.*?)</title>",
        r'<h1 class="post-title">(.*?)</h1>',
    ]:
        match = re.search(pattern, html_text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            title = " ".join(unescape(re.sub(r"<[^>]+>", " ", match.group(1))).split())
            if title:
                return title
    return ""


def _extract_page_authors(html_text: str) -> list[str]:
    author_candidates: list[str] = []
    for pattern in [
        r'<span class="author">\s*(.*?)\s*</span>',
        r'<div class="author-date">\s*(.*?)\s*</div>',
        r'<meta name="author" content="([^"]+)"',
        r"author\s*=\s*\{([^}]+)\}",
    ]:
        for match in re.findall(pattern, html_text, flags=re.IGNORECASE | re.DOTALL):
            cleaned = " ".join(unescape(re.sub(r"<[^>]+>", " ", match)).split())
            if cleaned:
                author_candidates.append(cleaned)
    authors: list[str] = []
    for item in author_candidates:
        for author in _parse_author_text(item):
            if author not in authors:
                authors.append(author)
    return authors


def _parse_author_text(text: str) -> list[str]:
    normalized = " ".join(str(text or "").split()).strip()
    if not normalized:
        return []
    lowered = normalized.lower()
    if " in collaboration with " in lowered:
        normalized = normalized[: lowered.index(" in collaboration with ")].strip()
    if normalized.lower() in {"thinking machines lab", "thinking machines"}:
        return []
    normalized = normalized.replace("&", " and ")
    parts = re.split(r"\s+(?:and)\s+|,\s*", normalized)
    authors: list[str] = []
    for part in parts:
        cleaned = " ".join(part.split()).strip(" -")
        if not cleaned or cleaned.lower() in {"thinking machines lab", "thinking machines"}:
            continue
        if _looks_like_person_name(cleaned) and cleaned not in authors:
            authors.append(cleaned)
    return authors


def _extract_year_from_text(text: str) -> int | None:
    match = re.search(r"\b(20\d{2}|19\d{2})\b", str(text or ""))
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _extract_year_from_html(html_text: str) -> int | None:
    for pattern in [
        r'<meta itemprop="datePublished" content="([^"]+)"',
        r"<time[^>]*>([^<]+)</time>",
    ]:
        match = re.search(pattern, html_text, flags=re.IGNORECASE)
        if match:
            year = _extract_year_from_text(match.group(1))
            if year is not None:
                return year
    return None


def _build_publication_lead(
    identity: CompanyIdentity, publication: PublicationRecord, name: str, role: str
) -> Candidate:
    candidate_id = make_candidate_id(name, identity.canonical_name, identity.canonical_name)
    return Candidate(
        candidate_id=candidate_id,
        name_en=name,
        display_name=name,
        category="lead",
        target_company=identity.canonical_name,
        organization=identity.canonical_name,
        role=role,
        notes=f"Discovered from {publication.source} publication evidence: {publication.title}",
        source_dataset=publication.source_dataset,
        source_path=publication.source_path,
        metadata={
            "publication_id": publication.publication_id,
            "publication_url": publication.url,
            "publication_title": publication.title,
            "publication_source": publication.source,
            "lead_discovery_method": "publication_author_acknowledgement_scan",
        },
    )


def _build_roster_anchored_scholar_coauthor_prospect(
    identity: CompanyIdentity,
    prospect: dict[str, Any],
    *,
    source_path: Path,
) -> Candidate:
    name = str(prospect.get("name") or "").strip()
    papers = list(prospect.get("papers") or [])
    top_paper = papers[0] if papers else {}
    notes = _join_nonempty(
        f"Discovered from roster-anchored scholar coauthor expansion for {identity.canonical_name}.",
        f"Seed members: {', '.join(list(prospect.get('seed_names') or [])[:4])}"
        if list(prospect.get("seed_names") or [])
        else "",
        f"Top paper: {str(top_paper.get('title') or '').strip()}" if top_paper else "",
    )
    return Candidate(
        candidate_id=make_candidate_id(name, identity.canonical_name, identity.canonical_name),
        name_en=name,
        display_name=name,
        category="lead",
        target_company=identity.canonical_name,
        organization=identity.canonical_name,
        role="Roster-Anchored Scholar Coauthor Prospect",
        notes=notes,
        source_dataset="roster_anchored_scholar_coauthor_prospect",
        source_path=str(source_path),
        metadata={
            "publication_title": str(top_paper.get("title") or "").strip(),
            "publication_url": str(top_paper.get("url") or "").strip(),
            "lead_discovery_method": "roster_anchored_scholar_coauthor_expansion",
            "scholar_coauthor_seed_names": list(prospect.get("seed_names") or [])[:8],
            "scholar_coauthor_papers": papers[:12],
        },
    )


def _dedupe_names(values: list[str]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _name_key(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        text = str(value or "").strip()
        if text:
            results.append(text)
    return results


def _publication_lead_public_web_resolution(
    candidate: Candidate,
    identity: CompanyIdentity,
    *,
    allow_targeted_name_search: bool,
) -> dict[str, Any]:
    return _public_web_membership_resolution(
        candidate,
        identity,
        allow_targeted_name_search=allow_targeted_name_search,
        explicit_approval_key="publication_lead_targeted_search_approved",
        no_evidence_state="publication_source_only_unconfirmed",
        no_evidence_label="publication provenance is the only current source",
        confirmed_without_linkedin_next_step="await_user_confirmation_before_paid_search",
    )


def _scholar_coauthor_prospect_public_web_resolution(candidate: Candidate, identity: CompanyIdentity) -> dict[str, Any]:
    return _public_web_membership_resolution(
        candidate,
        identity,
        allow_targeted_name_search=False,
        explicit_approval_key="",
        no_evidence_state="scholar_coauthor_only_unconfirmed",
        no_evidence_label="scholar coauthor provenance is the only current source",
        confirmed_without_linkedin_next_step="hold_for_known_linkedin_or_manual_follow_up",
    )


def _public_web_membership_resolution(
    candidate: Candidate,
    identity: CompanyIdentity,
    *,
    allow_targeted_name_search: bool,
    explicit_approval_key: str,
    no_evidence_state: str,
    no_evidence_label: str,
    confirmed_without_linkedin_next_step: str,
) -> dict[str, Any]:
    linkedin_url = _candidate_known_linkedin_url(candidate)
    affiliation_matches = _publication_lead_company_signal_matches(
        candidate,
        identity,
        metadata_keys=["exploration_affiliation_signals", "analysis_affiliation_signals"],
    )
    work_history_matches = _publication_lead_company_signal_matches(
        candidate,
        identity,
        metadata_keys=["exploration_work_history_signals", "analysis_work_history_signals"],
    )
    summary_matches = _publication_lead_summary_matches(candidate, identity)
    confirmed_by_public_web = bool(affiliation_matches or work_history_matches or summary_matches)
    explicit_approval = bool(explicit_approval_key and candidate.metadata.get(explicit_approval_key))

    if confirmed_by_public_web and linkedin_url:
        state = "confirmed_public_web_with_linkedin"
        next_step = "profile_detail_by_known_linkedin"
    elif confirmed_by_public_web:
        state = "confirmed_public_web_missing_linkedin"
        next_step = confirmed_without_linkedin_next_step
    elif linkedin_url:
        state = "linkedin_discovered_membership_unconfirmed"
        next_step = "hold_for_manual_confirmation_before_profile_scrape"
    else:
        state = no_evidence_state
        next_step = "hold_for_secondary_public_evidence_or_manual_review"

    eligible_for_targeted_name_search = bool(
        allow_targeted_name_search and explicit_approval and confirmed_by_public_web and not linkedin_url
    )

    summary_parts = []
    if affiliation_matches:
        summary_parts.append("public web captured explicit affiliation signals")
    if work_history_matches:
        summary_parts.append("public web captured work-history signals")
    if summary_matches:
        summary_parts.append("public web validated target-company summaries")
    if not summary_parts:
        summary_parts.append(no_evidence_label)
    if linkedin_url:
        summary_parts.append("LinkedIn URL recovered from exploration")
    summary_parts.append(f"next step: {next_step}")

    return {
        "state": state,
        "confirmed_by_public_web": confirmed_by_public_web,
        "linkedin_url": linkedin_url,
        "eligible_for_targeted_name_search": eligible_for_targeted_name_search,
        "next_step": next_step,
        "summary": "; ".join(summary_parts),
        "affiliation_matches": affiliation_matches[:4],
        "work_history_matches": work_history_matches[:4],
        "summary_matches": summary_matches[:4],
    }


def _annotate_publication_lead_resolution(candidate: Candidate, decision: dict[str, Any]) -> Candidate:
    record = candidate.to_record()
    metadata = dict(candidate.metadata or {})
    metadata.update(
        {
            "lead_resolution_strategy": "publication_lead_public_web_verification",
            "publication_lead_resolution_state": str(decision.get("state") or "").strip(),
            "publication_lead_public_web_confirmed": bool(decision.get("confirmed_by_public_web")),
            "publication_lead_targeted_name_search_eligible": bool(decision.get("eligible_for_targeted_name_search")),
            "publication_lead_next_step": str(decision.get("next_step") or "").strip(),
            "publication_lead_public_web_summary": str(decision.get("summary") or "").strip(),
            "publication_lead_public_web_affiliation_matches": list(decision.get("affiliation_matches") or [])[:4],
            "publication_lead_public_web_work_history_matches": list(decision.get("work_history_matches") or [])[:4],
            "publication_lead_public_web_summary_matches": list(decision.get("summary_matches") or [])[:4],
        }
    )
    record["metadata"] = metadata
    return Candidate(**record)


def _annotate_scholar_coauthor_resolution(candidate: Candidate, decision: dict[str, Any]) -> Candidate:
    record = candidate.to_record()
    metadata = dict(candidate.metadata or {})
    metadata.update(
        {
            "lead_resolution_strategy": "roster_anchored_scholar_coauthor_public_web_verification",
            "scholar_coauthor_resolution_state": str(decision.get("state") or "").strip(),
            "scholar_coauthor_public_web_confirmed": bool(decision.get("confirmed_by_public_web")),
            "scholar_coauthor_next_step": str(decision.get("next_step") or "").strip(),
            "scholar_coauthor_public_web_summary": str(decision.get("summary") or "").strip(),
            "scholar_coauthor_public_web_affiliation_matches": list(decision.get("affiliation_matches") or [])[:4],
            "scholar_coauthor_public_web_work_history_matches": list(decision.get("work_history_matches") or [])[:4],
            "scholar_coauthor_public_web_summary_matches": list(decision.get("summary_matches") or [])[:4],
        }
    )
    record["metadata"] = metadata
    return Candidate(**record)


def _load_scholar_coauthor_follow_up_progress(
    progress_path: Path, summary_path: Path
) -> tuple[list[dict[str, Any]], list[str], set[str]]:
    payload = _load_json_payload(progress_path)
    if not payload:
        payload = _load_json_payload(summary_path)
    decisions = [dict(item) for item in list(payload.get("decisions") or []) if isinstance(item, dict)]
    errors = [str(item) for item in list(payload.get("errors") or []) if str(item).strip()]
    processed_candidate_ids = {
        str(item.get("candidate_id") or "").strip()
        for item in decisions
        if str(item.get("candidate_id") or "").strip() and not _is_queued_scholar_coauthor_follow_up_decision(item)
    }
    decision_lookup = {
        str(item.get("candidate_id") or "").strip(): dict(item)
        for item in decisions
        if str(item.get("candidate_id") or "").strip()
    }
    processed_candidate_ids.update(
        candidate_id
        for candidate_id in (
            str(item).strip() for item in list(payload.get("processed_candidate_ids") or []) if str(item).strip()
        )
        if not _is_queued_scholar_coauthor_follow_up_decision(decision_lookup.get(candidate_id, {}))
    )
    return decisions, errors, processed_candidate_ids


def _is_queued_exploration_summary(summary: dict[str, Any]) -> bool:
    return str(summary.get("status") or "").strip() == "queued"


def _is_queued_scholar_coauthor_follow_up_decision(decision: dict[str, Any]) -> bool:
    if str(decision.get("state") or "").strip() == "queued_background_exploration":
        return True
    exploration_summary = dict(decision.get("exploration_summary") or {})
    return _is_queued_exploration_summary(exploration_summary)


def _restore_scholar_coauthor_follow_up_patch(
    patch_path: Path,
    *,
    candidate_map: dict[str, Candidate],
    resolved_profiles: list[dict[str, Any]],
    evidence: list[EvidenceRecord],
    evidence_keys: set[tuple[str, str, str, str]],
) -> None:
    payload = _load_json_payload(patch_path)
    if not payload:
        return
    for item in list(payload.get("updated_candidates") or []):
        if not isinstance(item, dict):
            continue
        candidate = Candidate(**item)
        candidate_map[_candidate_key(candidate)] = candidate
    _append_unique_resolved_profiles(
        resolved_profiles,
        [item for item in list(payload.get("resolved_profiles") or []) if isinstance(item, dict)],
    )
    _append_unique_evidence_records(
        evidence,
        [EvidenceRecord(**item) for item in list(payload.get("new_evidence") or []) if isinstance(item, dict)],
        evidence_keys=evidence_keys,
    )


def _persist_scholar_coauthor_follow_up_patch(
    patch_path: Path,
    *,
    updated_candidates: list[Candidate],
    resolved_profiles: list[dict[str, Any]],
    new_evidence: list[EvidenceRecord],
    asset_logger: AssetLogger,
) -> None:
    payload = _load_json_payload(patch_path)
    candidate_records = [dict(item) for item in list(payload.get("updated_candidates") or []) if isinstance(item, dict)]
    resolved_profile_records = [
        dict(item) for item in list(payload.get("resolved_profiles") or []) if isinstance(item, dict)
    ]
    evidence_records = [dict(item) for item in list(payload.get("new_evidence") or []) if isinstance(item, dict)]
    _upsert_follow_up_records(
        candidate_records,
        [item.to_record() for item in updated_candidates],
        key_fn=lambda item: str(item.get("candidate_id") or "").strip() or str(item.get("display_name") or "").strip(),
    )
    _upsert_follow_up_records(
        resolved_profile_records,
        list(resolved_profiles),
        key_fn=_resolved_profile_patch_key,
    )
    _upsert_follow_up_records(
        evidence_records,
        [item.to_record() for item in new_evidence],
        key_fn=_evidence_patch_key,
    )
    asset_logger.write_json(
        patch_path,
        {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "updated_candidates": candidate_records,
            "resolved_profiles": resolved_profile_records,
            "new_evidence": evidence_records,
        },
        asset_type="scholar_coauthor_follow_up_patch",
        source_kind="publication_enrichment",
        is_raw_asset=False,
        model_safe=True,
    )


def _write_scholar_coauthor_follow_up_state(
    *,
    progress_path: Path,
    summary_path: Path,
    identity: CompanyIdentity,
    prospect_total: int,
    processed_candidate_ids: set[str],
    decisions: list[dict[str, Any]],
    errors: list[str],
    asset_logger: AssetLogger,
    completed: bool,
) -> None:
    ordered_decisions = sorted(
        [dict(item) for item in decisions if str(item.get("candidate_id") or "").strip()],
        key=lambda item: (str(item.get("display_name") or "").lower(), str(item.get("candidate_id") or "")),
    )
    payload = {
        "target_company": identity.canonical_name,
        "candidate_count": len(ordered_decisions),
        "prospect_total": int(prospect_total),
        "processed_candidate_ids": sorted(str(item) for item in processed_candidate_ids if str(item).strip()),
        "remaining_candidate_count": max(int(prospect_total) - len(processed_candidate_ids), 0),
        "status": "completed" if completed else "partial",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "decisions": ordered_decisions,
        "errors": [str(item) for item in errors if str(item).strip()],
    }
    asset_logger.write_json(
        progress_path,
        payload,
        asset_type="scholar_coauthor_follow_up_progress",
        source_kind="publication_enrichment",
        is_raw_asset=False,
        model_safe=True,
    )
    asset_logger.write_json(
        summary_path,
        payload,
        asset_type="scholar_coauthor_follow_up",
        source_kind="publication_enrichment",
        is_raw_asset=False,
        model_safe=True,
    )


def _append_unique_resolved_profiles(target: list[dict[str, Any]], items: list[dict[str, Any]]) -> None:
    seen = {_resolved_profile_patch_key(item) for item in target}
    for item in items:
        key = _resolved_profile_patch_key(item)
        if key in seen:
            continue
        target.append(dict(item))
        seen.add(key)


def _append_unique_evidence_records(
    target: list[EvidenceRecord],
    items: list[EvidenceRecord],
    *,
    evidence_keys: set[tuple[str, str, str, str]],
) -> None:
    for item in items:
        key = _evidence_resume_key(item)
        if key in evidence_keys:
            continue
        target.append(item)
        evidence_keys.add(key)


def _upsert_follow_up_records(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
    *,
    key_fn: Any,
) -> None:
    index_by_key = {key_fn(item): position for position, item in enumerate(existing) if key_fn(item)}
    for item in incoming:
        key = key_fn(item)
        if not key:
            continue
        if key in index_by_key:
            existing[index_by_key[key]] = dict(item)
            continue
        existing.append(dict(item))
        index_by_key[key] = len(existing) - 1


def _resolved_profile_patch_key(item: dict[str, Any]) -> str:
    if not isinstance(item, dict):
        return ""
    candidate_id = str(item.get("candidate_id") or "").strip()
    profile_url = str(item.get("profile_url") or "").strip()
    raw_path = str(item.get("raw_path") or "").strip()
    resolution_source = str(item.get("resolution_source") or "").strip()
    if candidate_id or profile_url or raw_path or resolution_source:
        return "|".join([candidate_id, profile_url, raw_path, resolution_source])
    return ""


def _evidence_patch_key(item: dict[str, Any]) -> str:
    if not isinstance(item, dict):
        return ""
    evidence_id = str(item.get("evidence_id") or "").strip()
    if evidence_id:
        return evidence_id
    return "|".join(
        [
            str(item.get("candidate_id") or "").strip(),
            str(item.get("source_type") or "").strip(),
            str(item.get("title") or "").strip(),
            str(item.get("url") or "").strip(),
        ]
    )


def _evidence_resume_key(item: EvidenceRecord) -> tuple[str, str, str, str]:
    evidence_id = str(item.evidence_id or "").strip()
    if evidence_id:
        return (evidence_id, "", "", "")
    return (
        str(item.candidate_id or "").strip(),
        str(item.source_type or "").strip(),
        str(item.title or "").strip(),
        str(item.url or "").strip(),
    )


def _load_json_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _candidate_known_linkedin_url(candidate: Candidate) -> str:
    if str(candidate.linkedin_url or "").strip():
        return str(candidate.linkedin_url or "").strip()
    links = dict(candidate.metadata.get("exploration_links") or {})
    linkedin_items = list(links.get("linkedin") or [])
    for item in linkedin_items:
        value = str(item or "").strip()
        if value:
            return value
    return ""


def _candidate_publication_title(candidate: Candidate) -> str:
    return str(candidate.metadata.get("publication_title") or "").strip()


def _should_attempt_known_profile_resolution_after_exploration(candidate: Candidate, identity: CompanyIdentity) -> bool:
    if not _is_publication_lead_candidate(candidate):
        return True
    decision = _publication_lead_public_web_resolution(candidate, identity, allow_targeted_name_search=False)
    return bool(decision.get("confirmed_by_public_web"))


def _is_publication_lead_candidate(candidate: Candidate) -> bool:
    if candidate.category != "lead":
        return False
    source_dataset = str(candidate.source_dataset or "").strip().lower()
    discovery_method = str(candidate.metadata.get("lead_discovery_method") or "").strip().lower()
    return "publication" in source_dataset or discovery_method == "publication_author_acknowledgement_scan"


def _publication_lead_company_signal_matches(
    candidate: Candidate,
    identity: CompanyIdentity,
    *,
    metadata_keys: list[str],
) -> list[dict[str, str]]:
    company_variants = _company_variants(identity)
    matches: list[dict[str, str]] = []
    for metadata_key in metadata_keys:
        for item in list(candidate.metadata.get(metadata_key) or []):
            if not isinstance(item, dict):
                continue
            label = str(item.get("organization") or item.get("company") or "").strip()
            if not label:
                continue
            if not _company_label_matches_identity(label, identity, company_variants=company_variants):
                continue
            normalized = {str(key): str(value).strip() for key, value in item.items() if str(value).strip()}
            normalized["source_key"] = metadata_key
            if normalized not in matches:
                matches.append(normalized)
    return matches


def _publication_lead_summary_matches(candidate: Candidate, identity: CompanyIdentity) -> list[str]:
    matches: list[str] = []
    for value in list(candidate.metadata.get("exploration_validated_summaries") or []):
        text = str(value or "").strip()
        if not text:
            continue
        if _company_text_mentions_identity(text, identity) and text not in matches:
            matches.append(text)
    return matches


def _company_text_mentions_identity(text: str, identity: CompanyIdentity) -> bool:
    normalized_text = _normalize(text)
    if not normalized_text:
        return False
    for label in _company_reference_labels(identity):
        normalized_label = _normalize(label)
        if normalized_label and normalized_label in normalized_text:
            return True
    return False


def _candidate_name_map(candidates: list[Candidate]) -> dict[str, Candidate]:
    return {_candidate_key(candidate): candidate for candidate in candidates}


def _candidate_key(candidate: Candidate) -> str:
    return _name_key(candidate.name_en)


def _name_key(name: str) -> str:
    return normalize_name_token(name)


def _prioritize_candidates(candidates: list[Candidate]) -> list[Candidate]:
    return sorted(candidates, key=lambda item: (-_candidate_priority(item), item.display_name))


def _prioritize_scholar_coauthor_prospects(candidates: list[Candidate]) -> list[Candidate]:
    return sorted(
        candidates,
        key=lambda item: (
            -len(list(item.metadata.get("scholar_coauthor_seed_names") or [])),
            -len(list(item.metadata.get("scholar_coauthor_papers") or [])),
            -int(bool(_candidate_known_linkedin_url(item))),
            -int(bool(str(item.metadata.get("publication_title") or "").strip())),
            -_candidate_priority(item),
            item.display_name,
        ),
    )


def _candidate_priority(candidate: Candidate) -> int:
    text = f"{candidate.role} {candidate.team} {candidate.focus_areas}".lower()
    score = 0
    weighted_tokens = {
        "chief technology officer": 10,
        "cto": 8,
        "co-founder": 8,
        "cofounder": 8,
        "founder": 6,
        "chief executive officer": 6,
        "ceo": 5,
        "infrastructure": 8,
        "infra": 8,
        "training": 6,
        "inference": 6,
        "research engineer": 6,
        "research scientist": 6,
        "member of technical staff": 6,
        "technical staff": 5,
        "engineer": 3,
        "engineering": 3,
        "research": 3,
        "scientist": 3,
        "machine learning": 3,
        "systems": 3,
        "distributed": 3,
    }
    for token, weight in weighted_tokens.items():
        if token in text:
            score += weight
    if candidate.category == "employee" and candidate.employment_status == "current":
        score += 2
    if "." in candidate.display_name:
        score -= 2
    return score


def _names_match(left: str, right: str) -> bool:
    left_tokens = set(_name_tokens(left))
    right_tokens = set(_name_tokens(right))
    if not left_tokens or not right_tokens:
        return False
    return (
        left_tokens <= right_tokens
        or right_tokens <= left_tokens
        or len(left_tokens & right_tokens) >= max(2, min(len(left_tokens), len(right_tokens)))
    )


def _name_tokens(value: str) -> list[str]:
    tokens: list[str] = []
    current: list[str] = []
    for char in str(value or "").strip().lower():
        if char.isalnum():
            current.append(char)
            continue
        if current:
            tokens.append("".join(current))
            current = []
    if current:
        tokens.append("".join(current))
    return tokens


def _normalize(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def _join_nonempty(*parts: str) -> str:
    values = []
    for part in parts:
        text = str(part or "").strip()
        if text and text not in values:
            values.append(text)
    return " | ".join(values)


def _format_education(items: list[dict[str, Any]]) -> str:
    formatted = []
    for item in items[:4]:
        school_value = (
            item.get("school") or item.get("school_name") or item.get("schoolName") or item.get("school_name")
        )
        school = _profile_label_from_value(school_value)
        degree = str(item.get("degree") or item.get("degreeName") or "").strip()
        field = str(item.get("field_of_study") or item.get("fieldOfStudy") or item.get("field") or "").strip()
        if not school:
            continue
        segment = ", ".join(part for part in [degree, school, field] if part)
        if segment:
            formatted.append(segment)
    return " / ".join(formatted)


def _format_experience(items: list[dict[str, Any]]) -> str:
    formatted = []
    for item in items[:6]:
        title = str(item.get("title") or item.get("position") or "").strip()
        companies = _experience_company_labels(item)
        company = companies[0] if companies else ""
        if not title and not company:
            continue
        formatted.append(", ".join(part for part in [company, title] if part))
    return " / ".join(formatted)


def _format_profile_languages(items: list[Any]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for item in list(items or [])[:16]:
        if isinstance(item, str):
            label = item.strip()
        elif isinstance(item, dict):
            label = _profile_label_from_value(
                item.get("name") or item.get("language") or item.get("title") or item.get("value") or item.get("text")
            )
            level = _profile_label_from_value(item.get("proficiency") or item.get("level"))
            if label and level and level.lower() not in label.lower():
                label = f"{label} ({level})"
        else:
            label = ""
        if not label:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append(label)
        if len(results) >= 8:
            break
    return results


def _format_profile_skills(items: list[Any]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for item in list(items or [])[:32]:
        label = _profile_label_from_value(item)
        if not label:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append(label)
        if len(results) >= 16:
            break
    return results


def _profile_label_from_value(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ["name", "title", "value", "text", "label", "schoolName", "school", "companyName", "company"]:
            text = str(value.get(key) or "").strip()
            if text:
                return text
    return str(value or "").strip() if isinstance(value, (int, float, bool)) else ""


def _infer_team_from_text(text: str) -> str:
    lowered = text.lower()
    if "infra" in lowered or "infrastructure" in lowered:
        return "Infrastructure"
    if "research" in lowered:
        return "Research"
    if "training" in lowered:
        return "Training"
    if "inference" in lowered or "serving" in lowered:
        return "Inference"
    return ""


def _looks_like_person_name(name: str) -> bool:
    if len(name.split()) < 2:
        return False
    if any(char.isdigit() for char in name):
        return False
    return True


def _fetch_text(url: str) -> str:
    http_request = request.Request(url, headers={"User-Agent": "Mozilla/5.0"}, method="GET")
    with request.urlopen(http_request, timeout=30) as response:
        return response.read().decode("utf-8", errors="ignore")
