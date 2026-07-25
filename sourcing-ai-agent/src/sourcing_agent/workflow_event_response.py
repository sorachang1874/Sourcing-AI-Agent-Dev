from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

REMOTE_EVENT_WAIT_STAGES = {"waiting_remote_harvest", "waiting_remote_search"}
SEARCH_SEED_DISCOVERY_RECOVERY_KIND = "search_seed_discovery"
SEARCH_SEED_DISCOVERY_QUERY_ITEM_KIND = "search_seed_discovery_query"
LIVE_ROSTER_DISCOVERY_LANE_ITEM_KIND = "live_roster_discovery_lane"
LINKEDIN_STAGE_1_REMOTE_RECOVERY_KINDS = {
    "harvest_company_employees",
    "harvest_profile_batch",
    SEARCH_SEED_DISCOVERY_RECOVERY_KIND,
}
DISCOVERY_LANE_TERMINAL_ITEM_STATUSES = frozenset({"completed", "exhausted", "terminal_failed", "failed"})
DISCOVERY_LANE_TERMINAL_WORKER_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "canceled", "skipped", "superseded"}
)
TARGET_CANDIDATE_PUBLIC_WEB_REMOTE_RECOVERY_KINDS = {
    "target_candidate_public_web_search",
    "crm_public_web_search",
}
DEFAULT_REMOTE_EVENT_FOLLOWUP_LANES = frozenset(
    {
        "linkedin_stage_1",
        "crm_public_web_search",
    }
)


@dataclass(frozen=True)
class WorkflowRemoteEventLane:
    """Reusable registry entry for workflow stages that can react to remote-completion events."""

    event_lane: str
    worker_lanes: frozenset[str]
    recovery_kinds: frozenset[str]
    wait_stages: frozenset[str]

    def matches(self, worker: dict[str, Any]) -> bool:
        lane_id = str(worker.get("lane_id") or "").strip()
        recovery_kind = _worker_recovery_kind(worker)
        if lane_id not in self.worker_lanes:
            return False
        if recovery_kind not in self.recovery_kinds:
            return False
        return _worker_remote_wait_stage(worker) in self.wait_stages


@dataclass(frozen=True)
class WorkflowDiscoveryLaneContract:
    """Durable Stage 1 discovery-lane completion contract.

    Scoped-search and live-roster runs share the same Stage 1 lifecycle
    semantics: discovery lanes first produce candidate documents, then profile
    fetch/materialization consumes that population. The durable source differs
    by strategy, but terminal detection must be registry-driven instead of
    open-coded per workflow shape.
    """

    event_lane: str
    lane_kind: str
    worker_lanes: frozenset[str]
    recovery_kinds: frozenset[str]
    wait_stages: frozenset[str]
    durable_item_kind: str = ""
    terminal_item_statuses: frozenset[str] = DISCOVERY_LANE_TERMINAL_ITEM_STATUSES
    require_any_durable_item: bool = False
    require_any_worker: bool = False
    require_snapshot_apply_marker: bool = False


REMOTE_EVENT_LANE_REGISTRY = (
    WorkflowRemoteEventLane(
        event_lane="linkedin_stage_1",
        worker_lanes=frozenset(
            {
                "acquisition_specialist",
                "enrichment_specialist",
                "search_planner",
                "public_media_specialist",
            }
        ),
        recovery_kinds=frozenset(LINKEDIN_STAGE_1_REMOTE_RECOVERY_KINDS),
        wait_stages=frozenset(REMOTE_EVENT_WAIT_STAGES),
    ),
    WorkflowRemoteEventLane(
        event_lane="crm_public_web_search",
        worker_lanes=frozenset({"exploration_specialist"}),
        recovery_kinds=frozenset({"crm_public_web_search"}),
        wait_stages=frozenset({"waiting_remote_search"}),
    ),
    WorkflowRemoteEventLane(
        event_lane="target_candidate_public_web_search",
        worker_lanes=frozenset({"exploration_specialist"}),
        recovery_kinds=frozenset({"target_candidate_public_web_search"}),
        wait_stages=frozenset({"waiting_remote_search"}),
    ),
)

LINKEDIN_STAGE_1_DISCOVERY_LANE_REGISTRY = {
    "scoped_search": WorkflowDiscoveryLaneContract(
        event_lane="linkedin_stage_1",
        lane_kind="scoped_search",
        worker_lanes=frozenset({"search_planner", "public_media_specialist"}),
        recovery_kinds=frozenset({SEARCH_SEED_DISCOVERY_RECOVERY_KIND}),
        wait_stages=frozenset({"waiting_remote_search"}),
        durable_item_kind=SEARCH_SEED_DISCOVERY_QUERY_ITEM_KIND,
        require_any_durable_item=True,
    ),
    "live_roster": WorkflowDiscoveryLaneContract(
        event_lane="linkedin_stage_1",
        lane_kind="live_roster",
        worker_lanes=frozenset({"acquisition_specialist"}),
        recovery_kinds=frozenset({"harvest_company_employees"}),
        wait_stages=frozenset({"waiting_remote_harvest"}),
        durable_item_kind=LIVE_ROSTER_DISCOVERY_LANE_ITEM_KIND,
        require_any_durable_item=True,
    ),
}


def _worker_recovery_kind(worker: dict[str, Any]) -> str:
    checkpoint = dict(worker.get("checkpoint") or {})
    metadata = dict(worker.get("metadata") or {})
    return str(metadata.get("recovery_kind") or checkpoint.get("recovery_kind") or "").strip()


def _worker_remote_wait_stage(worker: dict[str, Any]) -> str:
    status = str(worker.get("status") or "").strip().lower()
    if status in DISCOVERY_LANE_TERMINAL_WORKER_STATUSES:
        return ""
    checkpoint = dict(worker.get("checkpoint") or {})
    checkpoint_stage = str(checkpoint.get("stage") or "").strip().lower()
    if checkpoint_stage in REMOTE_EVENT_WAIT_STAGES:
        return checkpoint_stage
    wait_stage = str(worker.get("wait_stage") or "").strip().lower()
    if wait_stage in REMOTE_EVENT_WAIT_STAGES:
        return wait_stage
    effective_status = str(worker.get("effective_status") or "").strip().lower()
    if effective_status in REMOTE_EVENT_WAIT_STAGES:
        return effective_status
    status = str(worker.get("status") or "").strip().lower()
    if status in REMOTE_EVENT_WAIT_STAGES:
        return status
    return ""


def linkedin_stage1_discovery_lane_contract(
    execution_preferences: dict[str, Any] | None,
) -> WorkflowDiscoveryLaneContract:
    """Resolve the canonical Stage 1 discovery lane for a workflow request."""

    preferences = dict(execution_preferences or {})
    has_search_seed_lanes = bool(
        str(preferences.get("delta_baseline_snapshot_id") or "").strip()
        or str(preferences.get("linkedin_search_seed_query") or "").strip()
    )
    if has_search_seed_lanes:
        return LINKEDIN_STAGE_1_DISCOVERY_LANE_REGISTRY["scoped_search"]
    return LINKEDIN_STAGE_1_DISCOVERY_LANE_REGISTRY["live_roster"]


def discovery_lane_worker_matches(worker: dict[str, Any], contract: WorkflowDiscoveryLaneContract) -> bool:
    lane_id = str(worker.get("lane_id") or "").strip()
    if lane_id not in contract.worker_lanes:
        return False
    return _worker_recovery_kind(worker) in contract.recovery_kinds


def discovery_lane_has_remote_wait_worker(
    workers: list[dict[str, Any]],
    contract: WorkflowDiscoveryLaneContract,
) -> bool:
    for worker in list(workers or []):
        worker_payload = dict(worker or {})
        if not discovery_lane_worker_matches(worker_payload, contract):
            continue
        if _worker_remote_wait_stage(worker_payload) in contract.wait_stages:
            return True
    return False


def discovery_lane_has_non_terminal_worker(
    workers: list[dict[str, Any]],
    contract: WorkflowDiscoveryLaneContract,
) -> bool:
    """Return true while a discovery-lane worker can still change Stage 1.

    Remote-wait workers are the common long-polling state, but submit/recovery
    workers can briefly be queued/running before entering remote wait. Terminal
    promotion must not race those states.
    """

    for worker in list(workers or []):
        worker_payload = dict(worker or {})
        if not discovery_lane_worker_matches(worker_payload, contract):
            continue
        status = str(worker_payload.get("status") or "").strip().lower()
        checkpoint = dict(worker_payload.get("checkpoint") or {})
        checkpoint_stage = str(checkpoint.get("stage") or "").strip().lower()
        effective_status = str(worker_payload.get("effective_status") or "").strip().lower()
        if status in DISCOVERY_LANE_TERMINAL_WORKER_STATUSES:
            continue
        if checkpoint_stage in DISCOVERY_LANE_TERMINAL_WORKER_STATUSES:
            continue
        if effective_status in DISCOVERY_LANE_TERMINAL_WORKER_STATUSES:
            continue
        return True
    return False


def remote_event_lane_for_worker(worker: dict[str, Any]) -> str:
    """Return the reusable event-response lane a remote-wait worker belongs to."""
    status = str(worker.get("status") or "").strip().lower()
    if status not in {"queued", "running", *REMOTE_EVENT_WAIT_STAGES}:
        return ""
    for entry in REMOTE_EVENT_LANE_REGISTRY:
        if entry.matches(worker):
            return entry.event_lane
    return ""


def remote_event_lane_for_known_provider_worker(worker: dict[str, Any]) -> str:
    """Return the event lane for an already-known provider worker, regardless of wait status."""
    lane_id = str(worker.get("lane_id") or "").strip()
    recovery_kind = _worker_recovery_kind(worker)
    if not lane_id or not recovery_kind:
        return ""
    for entry in REMOTE_EVENT_LANE_REGISTRY:
        if lane_id in entry.worker_lanes and recovery_kind in entry.recovery_kinds:
            return entry.event_lane
    return ""


def collect_remote_event_followup_targets(
    workers: list[dict[str, Any]],
    *,
    enabled_lanes: set[str] | None = None,
) -> dict[str, Any]:
    """Return only workers woken by a durable remote-terminal event.

    This helper is intentionally narrower than a generic recovery selector.
    `remote_event_followup` is the fast path after webhook/local-watcher wakeup;
    broad polling of remote-wait workers belongs to the normal worker recovery
    daemon. Keeping that split prevents one event-response tick from blocking on
    unrelated remote actors that have not emitted terminal provider events yet.
    """

    enabled = set(enabled_lanes or DEFAULT_REMOTE_EVENT_FOLLOWUP_LANES)
    job_ids: list[str] = []
    worker_ids: list[int] = []
    worker_ids_by_job: dict[str, list[int]] = {}
    lane_counts: Counter[str] = Counter()
    recovery_kind_counts: Counter[str] = Counter()
    for worker in list(workers or []):
        worker_payload = dict(worker)
        if not _worker_has_remote_provider_terminal_event_marker(worker_payload):
            continue
        event_lane = remote_event_lane_for_worker(worker_payload)
        if not event_lane or event_lane not in enabled:
            continue
        job_id = str(worker.get("job_id") or "").strip()
        worker_id = int(worker.get("worker_id") or 0)
        metadata = dict(worker.get("metadata") or {})
        checkpoint = dict(worker.get("checkpoint") or {})
        recovery_kind = str(metadata.get("recovery_kind") or checkpoint.get("recovery_kind") or "").strip()
        if job_id and job_id not in job_ids:
            job_ids.append(job_id)
        if worker_id > 0:
            worker_ids.append(worker_id)
            if job_id:
                worker_ids_by_job.setdefault(job_id, []).append(worker_id)
        lane_counts[event_lane] += 1
        if recovery_kind:
            recovery_kind_counts[recovery_kind] += 1
    return {
        "job_ids": job_ids,
        "worker_ids": worker_ids,
        "worker_ids_by_job": worker_ids_by_job,
        "worker_count": len(worker_ids),
        "lane_counts": {key: int(value) for key, value in lane_counts.items()},
        "recovery_kind_counts": {key: int(value) for key, value in recovery_kind_counts.items()},
    }


def worker_has_remote_provider_terminal_event_marker(worker: dict[str, Any]) -> bool:
    """Return whether a worker has durable evidence of a terminal provider event.

    Remote-event follow-up and generic worker recovery must use the same marker
    contract. A submitted remote-wait worker without this marker remains owned
    by the provider-event watcher/webhook path; generic recovery must not poll
    it as regular stale work.
    """

    checkpoint = dict(dict(worker or {}).get("checkpoint") or {})
    if checkpoint.get("remote_provider_terminal_event") or checkpoint.get("remote_provider_terminal_event_seen_at"):
        return True
    metadata = dict(dict(worker or {}).get("metadata") or {})
    return bool(
        metadata.get("remote_provider_terminal_event")
        or metadata.get("remote_provider_terminal_event_seen_at")
    )


def _worker_has_remote_provider_terminal_event_marker(worker: dict[str, Any]) -> bool:
    return worker_has_remote_provider_terminal_event_marker(worker)
