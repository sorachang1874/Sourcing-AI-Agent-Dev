"""Profile-fetch domain owner extracted from ``SourcingOrchestrator`` (Phase 3).

Operation-native LinkedIn profile-fetch command handling (plan/execute for the
activity, provider-fetch, and profile-terminal-admit command types plus their
worker-recovery drain), the background profile-prefetch queue entry points,
the profile-prefetch refill trigger/queue drain, the harvest-profile completion
event handler, terminal-noop ingest recording, the running profile-fetch
activity cancel handler, and the job-scoped profile-refill state vocabularies.
Bodies are moved verbatim from ``orchestrator.py``; the only body edit is
kernel-wrapper calls ``self._x(...)`` -> ``self._kernel._x(...)``.
``SourcingOrchestrator`` keeps signature-identical delegating wrappers for
every moved method, and injects its shared spine helpers as bound callables
stored under the same attribute names so moved bodies stay verbatim.

Deliberate stay-behinds on the orchestrator: the projection/asset-population
profile-progress payload builders and ``_enrich_candidate_records_from_profile_registry``
(consumed only by projection read paths), the projection-admission command
family (which has its own drain/executor), the worker-delta predicates used by
the shared local-apply-closure spine, and the two instance-patched spine
methods ``_queue_background_profile_prefetch_from_available_baselines`` /
``_run_profile_completion_next_submit_opportunity`` (existing tests patch them
on the orchestrator instance; the owner reaches them through re-resolving
injected callables under the same attribute names).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .command_kernel import CommandKernel
from .domain import JobRequest
from .durable_runtime import (
    LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
    LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
    LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
)
from .linkedin_url_normalization import normalize_linkedin_profile_url_key
from .repositories import linkedin_profile_registry_repo
from .runtime_environment import runtime_namespace_ownership_for_path
from .runtime_tuning import resolved_harvest_profile_actor_global_inflight
from .seed_discovery import SearchSeedSnapshot
from .snapshot_materializer import resolve_snapshot_company_identity

# NOTE: the helpers below duplicate small module-level helpers in
# ``orchestrator.py`` (which imports this module — importing them back from
# orchestrator would create a cycle).  The bodies are copied verbatim; several
# other ``sourcing_agent`` modules already carry the same local copies.


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(value)


def _coerce_int(value: Any, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    raw = str(value).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _dedupe_texts(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        text = " ".join(str(item or "").split()).strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(text)
    return result


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _milliseconds_between_iso(start_value: str, end_value: str) -> int | None:
    start = _parse_timestamp(start_value)
    end = _parse_timestamp(end_value)
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds() * 1000))


def _parse_timestamp(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _restore_search_seed_snapshot_from_snapshot_dir(*args: Any, **kwargs: Any):
    """Re-resolving shim for the orchestrator module-level restore helper.

    The function is defined in ``orchestrator.py`` (which imports this module,
    so a top-level import back would be a cycle).  Resolving it through the
    orchestrator module at call time also keeps tests that patch
    ``sourcing_agent.orchestrator._restore_search_seed_snapshot_from_snapshot_dir``
    intercepting calls made from moved bodies.
    """
    from . import orchestrator as _orchestrator_module

    return _orchestrator_module._restore_search_seed_snapshot_from_snapshot_dir(*args, **kwargs)


class ProfileFetchOwner:
    """Owner of the profile-fetch command/worker cluster (Phase 3 extraction)."""

    def __init__(
        self,
        *,
        store: Any,
        command_kernel: CommandKernel,
        durable_runtime_writer: Any,
        runtime_dir: Path | str,
        acquisition_engine: Any,
        upsert_acquisition_run_phase: Callable[..., Any],
        sync_operation_run_from_workflow_command_control: Callable[..., dict[str, Any]],
        workflow_command_downstream_commands: Callable[..., list[dict[str, Any]]],
        restore_roster_snapshot_from_snapshot_dir: Callable[..., Any],
        plan_operation_native_projection_admission_command: Callable[..., Any],
        queue_background_profile_prefetch_from_available_baselines: Callable[..., dict[str, Any]],
        run_profile_completion_next_submit_opportunity: Callable[..., Any],
    ) -> None:
        self.store = store
        self._kernel = command_kernel
        self.durable_runtime_writer = durable_runtime_writer
        self.runtime_dir = runtime_dir
        self.acquisition_engine = acquisition_engine
        # Injected cross-domain/spine callables, stored under the same names the
        # moved bodies already use so the bodies stay verbatim.
        self._upsert_acquisition_run_phase = upsert_acquisition_run_phase
        self._sync_operation_run_from_workflow_command_control = sync_operation_run_from_workflow_command_control
        self._workflow_command_downstream_commands = workflow_command_downstream_commands
        self._restore_roster_snapshot_from_snapshot_dir = restore_roster_snapshot_from_snapshot_dir
        self._plan_operation_native_projection_admission_command = plan_operation_native_projection_admission_command
        # These two spine methods stay orchestrator-side and are instance-patched
        # by existing tests; the orchestrator passes re-resolving lambdas so
        # those patches keep intercepting calls made from moved bodies (see
        # module docstring).
        self._queue_background_profile_prefetch_from_available_baselines = (
            queue_background_profile_prefetch_from_available_baselines
        )
        self._run_profile_completion_next_submit_opportunity = run_profile_completion_next_submit_opportunity

    @staticmethod
    def _operation_native_profile_slug(profile_url: str) -> str:
        match = re.search(r"linkedin\.com/in/([^/?#]+)", str(profile_url or "").strip())
        return str(match.group(1) if match else "").strip()

    def _plan_operation_native_profile_fetch_activity_command(
        self,
        *,
        parent_command: dict[str, Any],
        acquisition_run_id: str,
        lane_id: str,
        source_activity_run_id: str,
        target_company: str,
        profile_urls: list[str],
        source_entity_delta_ids: list[str],
        workspace_id: str = "default",
    ) -> dict[str, Any]:
        parent_payload = dict(parent_command or {})
        parent_command_id = str(parent_payload.get("command_id") or "").strip()
        workflow_run_id = str(parent_payload.get("workflow_run_id") or "").strip()
        parent_body = dict(parent_payload.get("payload") or {})
        parent_causality = dict(parent_body.get("causality") or {})
        operation_id = str(
            parent_payload.get("operation_id")
            or parent_body.get("operation_id")
            or parent_body.get("operation_run_id")
            or parent_causality.get("operation_id")
            or ""
        ).strip()
        normalized_profile_urls = _dedupe_texts(profile_urls)
        normalized_delta_ids = _dedupe_texts(source_entity_delta_ids)
        if not workflow_run_id or not parent_command_id or not normalized_profile_urls:
            return {}
        profile_scope_hash = hashlib.sha1(
            json.dumps(
                {
                    "activity_run_id": str(source_activity_run_id or "").strip(),
                    "profile_urls": normalized_profile_urls,
                    "source_entity_delta_ids": normalized_delta_ids,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:24]
        idempotency_key = f"{LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE}:source:{profile_scope_hash}"
        causal_group_id = (
            str(parent_causality.get("causal_group_id") or "").strip()
            or str(parent_payload.get("causal_group_id") or "").strip()
            or parent_command_id
        )
        command_payload = {
            "runtime_execution_mode": "operation_native_profile_fetch",
            "acquisition_run_id": str(acquisition_run_id or "").strip(),
            "lane_id": str(lane_id or "").strip(),
            "source_discovery_activity_run_id": str(source_activity_run_id or "").strip(),
            "target_company": str(target_company or "").strip(),
            "profile_urls": normalized_profile_urls,
            "profile_url_count": len(normalized_profile_urls),
            "source_entity_delta_ids": normalized_delta_ids,
            "source_entity_delta_count": len(normalized_delta_ids),
            "parent_command_id": parent_command_id,
            "causal_group_id": causal_group_id,
            "operation_run_id": operation_id,
            "action_id": str(parent_body.get("action_id") or "").strip(),
            "workspace_id": str(workspace_id or "default").strip() or "default",
            "legacy_job_shell_created": False,
            "queue_workflow_called": False,
            "normal_path_executes_legacy_profile_refill_owner": False,
            "migration_phase": "W11e_operation_native_profile_fetch_activity",
            "produced_entity_counts": {"profile_url": len(normalized_profile_urls)},
        }
        try:
            self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                command_id=parent_command_id,
                event_family="workflow_event",
                event_type="WorkflowStarted",
                idempotency_key=f"{workflow_run_id}:operation_native_profile_fetch_started:{parent_command_id}",
                actor=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                source="operation_native_discovery_activity_owner",
                payload={
                    "workflow_type": "agent_callable_acquisition",
                    "stage_key": "operation_native_profile_fetch",
                    "parent_command_id": parent_command_id,
                    "causal_group_id": causal_group_id,
                    "migration_phase": "W11e_operation_native_profile_fetch_activity",
                },
            )
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                command_id=parent_command_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{idempotency_key}:plan",
                actor=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                source="operation_native_discovery_activity_owner",
                payload={
                    "workflow_type": "agent_callable_acquisition",
                    "stage_key": "operation_native_profile_fetch",
                    "command_type": LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
                    "idempotency_key": idempotency_key,
                    "parent_command_id": parent_command_id,
                    "causal_group_id": causal_group_id,
                    "payload": command_payload,
                    "max_attempts": 5,
                    "retry_policy": {
                        "kind": "operation_native_profile_fetch_activity",
                        "retry_delay_seconds": 30,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=idempotency_key,
        )

    def _plan_operation_native_profile_fetch_provider_command(
        self,
        *,
        parent_command: dict[str, Any],
        source_profile_activity_run_id: str,
        acquisition_run_id: str,
        target_company: str,
        profile_urls: list[str],
        source_entity_delta_ids: list[str],
        workspace_id: str = "default",
        provider_attempt_scope: str = "normal",
        retry_wave_index: int = 0,
    ) -> dict[str, Any]:
        parent_payload = dict(parent_command or {})
        parent_command_id = str(parent_payload.get("command_id") or "").strip()
        workflow_run_id = str(parent_payload.get("workflow_run_id") or "").strip()
        parent_body = dict(parent_payload.get("payload") or {})
        parent_causality = dict(parent_body.get("causality") or {})
        operation_id = str(
            parent_payload.get("operation_id")
            or parent_body.get("operation_id")
            or parent_body.get("operation_run_id")
            or parent_causality.get("operation_id")
            or ""
        ).strip()
        normalized_profile_urls = _dedupe_texts(profile_urls)
        normalized_delta_ids = _dedupe_texts(source_entity_delta_ids)
        activity_run_id = str(source_profile_activity_run_id or "").strip()
        normalized_attempt_scope = str(provider_attempt_scope or "normal").strip() or "normal"
        normalized_retry_wave_index = max(0, _coerce_int(retry_wave_index, 0))
        if not workflow_run_id or not parent_command_id or not activity_run_id or not normalized_profile_urls:
            return {}
        profile_scope_hash = hashlib.sha1(
            json.dumps(
                {
                    "activity_run_id": activity_run_id,
                    "provider_attempt_scope": normalized_attempt_scope,
                    "retry_wave_index": normalized_retry_wave_index,
                    "profile_urls": normalized_profile_urls,
                    "source_entity_delta_ids": normalized_delta_ids,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:24]
        idempotency_key = f"{LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE}:activity:{profile_scope_hash}"
        causal_group_id = (
            str(parent_causality.get("causal_group_id") or "").strip()
            or str(parent_payload.get("causal_group_id") or "").strip()
            or parent_command_id
        )
        command_payload = {
            "runtime_execution_mode": "operation_native_profile_provider_fetch",
            "source_profile_activity_run_id": activity_run_id,
            "acquisition_run_id": str(acquisition_run_id or "").strip(),
            "target_company": str(target_company or "").strip(),
            "profile_urls": normalized_profile_urls,
            "profile_url_count": len(normalized_profile_urls),
            "source_entity_delta_ids": normalized_delta_ids,
            "source_entity_delta_count": len(normalized_delta_ids),
            "provider_attempt_scope": normalized_attempt_scope,
            "retry_wave_index": normalized_retry_wave_index,
            "profile_retry_budget": 1,
            "retry_unit": "linkedin_url_key",
            "retry_strategy": "bucketed_entity_retry_after_normal_wave",
            "parent_command_id": parent_command_id,
            "causal_group_id": causal_group_id,
            "operation_run_id": operation_id,
            "action_id": str(parent_body.get("action_id") or "").strip(),
            "workspace_id": str(workspace_id or "default").strip() or "default",
            "legacy_job_shell_created": False,
            "queue_workflow_called": False,
            "normal_path_executes_legacy_profile_refill_owner": False,
            "migration_phase": "W11f_operation_native_profile_provider_fetch",
            "produced_entity_counts": {"profile_url": len(normalized_profile_urls)},
        }
        try:
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                command_id=parent_command_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{idempotency_key}:plan",
                actor=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                source="operation_native_profile_fetch_activity_owner",
                payload={
                    "workflow_type": "agent_callable_acquisition",
                    "stage_key": "operation_native_profile_provider_fetch",
                    "command_type": LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
                    "idempotency_key": idempotency_key,
                    "parent_command_id": parent_command_id,
                    "causal_group_id": causal_group_id,
                    "payload": command_payload,
                    "max_attempts": 2 if normalized_attempt_scope != "retry_wave" else 1,
                    "retry_policy": {
                        "kind": "operation_native_profile_provider_fetch",
                        "retry_delay_seconds": 60,
                        "retry_unit": "linkedin_url_key",
                        "retry_strategy": "bucketed_entity_retry_after_normal_wave",
                        "provider_attempt_scope": normalized_attempt_scope,
                        "retry_wave_index": normalized_retry_wave_index,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=idempotency_key,
        )

    def _plan_operation_native_profile_terminal_admit_command(
        self,
        *,
        parent_command: dict[str, Any],
        source_profile_activity_run_id: str,
        acquisition_run_id: str,
        target_company: str,
        source_entity_delta_ids: list[str],
        profile_urls: list[str] | None = None,
        workspace_id: str = "default",
    ) -> dict[str, Any]:
        parent_payload = dict(parent_command or {})
        parent_command_id = str(parent_payload.get("command_id") or "").strip()
        workflow_run_id = str(parent_payload.get("workflow_run_id") or "").strip()
        parent_body = dict(parent_payload.get("payload") or {})
        parent_causality = dict(parent_body.get("causality") or {})
        operation_id = str(
            parent_payload.get("operation_id")
            or parent_body.get("operation_id")
            or parent_body.get("operation_run_id")
            or parent_causality.get("operation_id")
            or ""
        ).strip()
        activity_run_id = str(source_profile_activity_run_id or "").strip()
        normalized_delta_ids = _dedupe_texts(source_entity_delta_ids)
        normalized_profile_urls = _dedupe_texts(profile_urls or [])
        if not workflow_run_id or not parent_command_id or not activity_run_id or not normalized_delta_ids:
            return {}
        terminal_scope_hash = hashlib.sha1(
            json.dumps(
                {
                    "activity_run_id": activity_run_id,
                    "source_entity_delta_ids": normalized_delta_ids,
                    "profile_urls": normalized_profile_urls,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:24]
        idempotency_key = f"{LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE}:activity:{terminal_scope_hash}"
        causal_group_id = (
            str(parent_causality.get("causal_group_id") or "").strip()
            or str(parent_payload.get("causal_group_id") or "").strip()
            or parent_command_id
        )
        command_payload = {
            "runtime_execution_mode": "operation_native_profile_terminal_admission",
            "source_profile_activity_run_id": activity_run_id,
            "acquisition_run_id": str(acquisition_run_id or "").strip(),
            "target_company": str(target_company or "").strip(),
            "profile_urls": normalized_profile_urls,
            "profile_url_count": len(normalized_profile_urls),
            "source_entity_delta_ids": normalized_delta_ids,
            "source_entity_delta_count": len(normalized_delta_ids),
            "parent_command_id": parent_command_id,
            "causal_group_id": causal_group_id,
            "operation_run_id": operation_id,
            "action_id": str(parent_body.get("action_id") or "").strip(),
            "workspace_id": str(workspace_id or "default").strip() or "default",
            "legacy_job_shell_created": False,
            "queue_workflow_called": False,
            "normal_path_executes_legacy_profile_refill_owner": False,
            "normal_path_mutates_projection": False,
            "migration_phase": "W11f_operation_native_profile_terminal_admission",
            "produced_entity_counts": {"profile_url": max(len(normalized_profile_urls), len(normalized_delta_ids))},
        }
        try:
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                command_id=parent_command_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{idempotency_key}:plan",
                actor=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                source="operation_native_profile_terminal_admission_planner",
                payload={
                    "workflow_type": "agent_callable_acquisition",
                    "stage_key": "operation_native_profile_terminal_admission",
                    "command_type": LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
                    "idempotency_key": idempotency_key,
                    "parent_command_id": parent_command_id,
                    "causal_group_id": causal_group_id,
                    "payload": command_payload,
                    "max_attempts": 3,
                    "retry_policy": {
                        "kind": "operation_native_profile_terminal_admission",
                        "retry_delay_seconds": 30,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=idempotency_key,
        )

    def _execute_operation_native_profile_fetch_activity_command_payload(
        self,
        command: dict[str, Any],
        *,
        lease_owner: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        workflow_run_id = str(command_payload.get("workflow_run_id") or payload.get("workflow_run_id") or "").strip()
        operation_run_id = str(
            command_payload.get("operation_id")
            or payload.get("operation_id")
            or payload.get("operation_run_id")
            or dict(payload.get("causality") or {}).get("operation_id")
            or ""
        ).strip()
        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        acquisition_run_id = str(payload.get("acquisition_run_id") or "").strip()
        parent_activity_run_id = str(payload.get("source_discovery_activity_run_id") or "").strip()
        profile_urls = _dedupe_texts(payload.get("profile_urls") or [])
        source_delta_ids = _dedupe_texts(payload.get("source_entity_delta_ids") or [])
        if not command_id or not workflow_run_id or not profile_urls:
            return {
                "status": "failed",
                "reason": "operation_native_profile_fetch_payload_missing",
                "operation_completion_deferred": False,
                "provider_called": False,
                "legacy_job_shell_created": False,
            }
        activity = self.store.upsert_workflow_activity_run(
            {
                "workspace_id": workspace_id,
                "workflow_run_id": workflow_run_id,
                "operation_run_id": operation_run_id,
                "acquisition_run_id": acquisition_run_id,
                "command_id": command_id,
                "parent_activity_run_id": parent_activity_run_id,
                "activity_type": LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
                "owner": LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                "status": "running",
                "phase": "profile_registry_cache_lookup",
                "idempotency_key": f"workflow_activity:{LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE}:{command_id}",
                "input": {
                    "profile_urls": profile_urls,
                    "source_entity_delta_ids": source_delta_ids,
                    "target_company": str(payload.get("target_company") or "").strip(),
                    "runtime_execution_mode": "operation_native_profile_fetch",
                },
                "output": {},
                "artifact_refs": [],
                "entity_counts": {"profile_url_count": len(profile_urls)},
                "metadata": {
                    "legacy_job_shell_created": False,
                    "queue_workflow_called": False,
                    "provider_called": False,
                    "migration_phase": "W11e_operation_native_profile_fetch_activity",
                },
            }
        )
        activity_run_id = str(activity.get("activity_run_id") or "").strip()
        attempt_number = max(1, _coerce_int(command_payload.get("attempt"), 1))
        attempt = self.store.upsert_workflow_activity_attempt(
            {
                "workspace_id": workspace_id,
                "activity_run_id": activity_run_id,
                "workflow_run_id": workflow_run_id,
                "command_id": command_id,
                "attempt_number": attempt_number,
                "status": "running",
                "provider": "linkedin_profile_registry",
                "provider_request_ref": f"operation_native_profile_fetch:{activity_run_id}:{attempt_number}",
                "started_at": _utc_now_iso(),
                "input": {
                    "profile_urls": profile_urls,
                    "source_entity_delta_ids": source_delta_ids,
                    "lookup_mode": "registry_cache_first_no_legacy_job_shell",
                },
                "output": {},
                "artifact_refs": [],
                "error": {},
                "idempotency_key": f"workflow_activity_attempt:{command_id}:{attempt_number}",
                "metadata": {
                    "lease_owner": str(lease_owner or "").strip(),
                    "provider_called": False,
                    "legacy_job_shell_created": False,
                    "migration_phase": "W11e_operation_native_profile_fetch_activity",
                },
            }
        )
        registry_entries = {}
        registry_repo = linkedin_profile_registry_repo(self.store)
        get_registry_bulk = getattr(registry_repo, "get_bulk", None)
        if callable(get_registry_bulk):
            try:
                registry_entries = dict(get_registry_bulk(profile_urls) or {})
            except Exception:
                registry_entries = {}
        cache_hit_urls: list[str] = []
        fetch_required_urls: list[str] = []
        profile_entity_delta_ids: list[str] = []
        cache_hit_delta_ids: list[str] = []
        fetch_required_delta_ids: list[str] = []
        for profile_url in profile_urls:
            profile_key = normalize_linkedin_profile_url_key(profile_url)
            registry_entry = dict(registry_entries.get(profile_key) or {})
            registry_status = str(registry_entry.get("status") or "").strip().lower()
            registry_raw_path = str(registry_entry.get("raw_path") or registry_entry.get("last_raw_path") or "").strip()
            is_cache_hit = registry_status == "fetched" and bool(registry_raw_path)
            if is_cache_hit:
                cache_hit_urls.append(profile_url)
                delta_kind = "profile_cache_hit"
                delta_status = "recorded"
                reason = "operation_native_profile_cache_hit"
                projection_effect_reason = "pending_profile_terminal_or_projection_admission"
            else:
                fetch_required_urls.append(profile_url)
                delta_kind = "profile_fetch_required"
                delta_status = "not_applied"
                reason = "operation_native_profile_fetch_provider_owner_pending"
                projection_effect_reason = "provider_profile_fetch_activity_not_yet_implemented"
            delta = self.store.upsert_workflow_entity_delta(
                {
                    "workspace_id": workspace_id,
                    "workflow_run_id": workflow_run_id,
                    "operation_run_id": operation_run_id,
                    "command_id": command_id,
                    "activity_run_id": activity_run_id,
                    "attempt_id": str(attempt.get("attempt_id") or "").strip(),
                    "acquisition_run_id": acquisition_run_id,
                    "entity_type": "profile",
                    "entity_key": profile_key or profile_url,
                    "delta_kind": delta_kind,
                    "status": delta_status,
                    "reason": reason,
                    "source_ref": {
                        "command_id": command_id,
                        "activity_run_id": activity_run_id,
                        "parent_activity_run_id": parent_activity_run_id,
                        "source_entity_delta_ids": source_delta_ids,
                    },
                    "entity_payload": {
                        "profile_url": profile_url,
                        "profile_url_key": profile_key,
                        "registry_status": registry_status,
                        "raw_path": registry_raw_path,
                    },
                    "projection_effect": {
                        "entered_projection": False,
                        "profile_count_delta": 1 if is_cache_hit else 0,
                        "reason": projection_effect_reason,
                    },
                    "artifact_refs": [],
                    "idempotency_key": (
                        f"workflow_entity_delta:{command_id}:profile:"
                        f"{hashlib.sha1((profile_key or profile_url).encode('utf-8')).hexdigest()[:24]}"
                    ),
                    "metadata": {
                        "provider_called": False,
                        "legacy_job_shell_created": False,
                        "migration_phase": "W11e_operation_native_profile_fetch_activity",
                    },
                }
            )
            if delta:
                delta_id = str(delta.get("delta_id") or "").strip()
                profile_entity_delta_ids.append(delta_id)
                if is_cache_hit:
                    cache_hit_delta_ids.append(delta_id)
                else:
                    fetch_required_delta_ids.append(delta_id)
        fetch_required_count = len(fetch_required_urls)
        cache_hit_count = len(cache_hit_urls)
        final_activity_status = "planned_pending_provider_owner" if fetch_required_count else "succeeded"
        final_activity_phase = "provider_profile_fetch_pending" if fetch_required_count else "profile_cache_resolved"
        completed_attempt = self.store.upsert_workflow_activity_attempt(
            {
                **attempt,
                "status": "succeeded",
                "completed_at": _utc_now_iso(),
                "output": {
                    "profile_url_count": len(profile_urls),
                    "cache_hit_count": cache_hit_count,
                    "fetch_required_count": fetch_required_count,
                    "cache_hit_urls": cache_hit_urls,
                    "fetch_required_urls": fetch_required_urls,
                    "entity_delta_ids": profile_entity_delta_ids,
                    "provider_owner_required": fetch_required_count > 0,
                },
                "error": {},
            }
        )
        downstream_provider_command: dict[str, Any] = {}
        if fetch_required_count > 0:
            downstream_provider_command = self._plan_operation_native_profile_fetch_provider_command(
                parent_command=command_payload,
                source_profile_activity_run_id=activity_run_id,
                acquisition_run_id=acquisition_run_id,
                target_company=str(payload.get("target_company") or "").strip(),
                profile_urls=fetch_required_urls,
                source_entity_delta_ids=fetch_required_delta_ids,
                workspace_id=workspace_id,
            )
            if not downstream_provider_command:
                return {
                    "status": "failed",
                    "reason": "operation_native_profile_fetch_provider_command_planning_failed",
                    "operation_completion_deferred": True,
                    "provider_called": False,
                    "legacy_job_shell_created": False,
                    "queue_workflow_called": False,
                    "activity_run_id": activity_run_id,
                    "profile_url_count": len(profile_urls),
                    "fetch_required_count": fetch_required_count,
                    "migration_phase": "W11e_operation_native_profile_fetch_activity",
                }
        downstream_terminal_command: dict[str, Any] = {}
        if cache_hit_delta_ids:
            downstream_terminal_command = self._plan_operation_native_profile_terminal_admit_command(
                parent_command=command_payload,
                source_profile_activity_run_id=activity_run_id,
                acquisition_run_id=acquisition_run_id,
                target_company=str(payload.get("target_company") or "").strip(),
                profile_urls=cache_hit_urls,
                source_entity_delta_ids=cache_hit_delta_ids,
                workspace_id=workspace_id,
            )
            if not downstream_terminal_command:
                return {
                    "status": "failed",
                    "reason": "operation_native_profile_terminal_command_planning_failed",
                    "operation_completion_deferred": True,
                    "provider_called": False,
                    "legacy_job_shell_created": False,
                    "queue_workflow_called": False,
                    "activity_run_id": activity_run_id,
                    "profile_url_count": len(profile_urls),
                    "cache_hit_count": cache_hit_count,
                    "migration_phase": "W11f_operation_native_profile_terminal_admission",
                }
        downstream_command_ids = [
            str(command.get("command_id") or "").strip()
            for command in (downstream_provider_command, downstream_terminal_command)
            if command
        ]
        final_activity = self.store.upsert_workflow_activity_run(
            {
                **self.store.get_workflow_activity_run(activity_run_id),
                "status": final_activity_status,
                "phase": final_activity_phase,
                "output": {
                    "latest_attempt_id": str(completed_attempt.get("attempt_id") or "").strip(),
                    "cache_hit_urls": cache_hit_urls,
                    "fetch_required_urls": fetch_required_urls,
                    "entity_delta_ids": profile_entity_delta_ids,
                    "cache_hit_delta_ids": cache_hit_delta_ids,
                    "fetch_required_delta_ids": fetch_required_delta_ids,
                    "provider_owner_required": fetch_required_count > 0,
                    "downstream_command_ids": downstream_command_ids,
                },
                "entity_counts": {
                    "profile_url_count": len(profile_urls),
                    "cache_hit_count": cache_hit_count,
                    "fetch_required_count": fetch_required_count,
                },
                "metadata": {
                    **dict((self.store.get_workflow_activity_run(activity_run_id) or {}).get("metadata") or {}),
                    "latest_attempt_id": str(completed_attempt.get("attempt_id") or "").strip(),
                    "provider_owner_required": fetch_required_count > 0,
                    "downstream_command_ids": downstream_command_ids,
                    "provider_called": False,
                    "legacy_job_shell_created": False,
                    "migration_phase": "W11e_operation_native_profile_fetch_activity",
                },
            }
        )
        if acquisition_run_id:
            acquisition_run = self.store.repos.workflow_runtime.get_acquisition_run(acquisition_run_id) or {}
            if acquisition_run:
                self._upsert_acquisition_run_phase(
                    acquisition_run=acquisition_run,
                    command=command_payload,
                    status="profile_fetch_activity_planned",
                    current_phase="profile_fetch_pending",
                    metadata_patch={
                        "latest_profile_activity_run_id": activity_run_id,
                        "profile_fetch_required_count": fetch_required_count,
                        "profile_cache_hit_count": cache_hit_count,
                    },
                )
        return {
            "status": "completed",
            "reason": "operation_native_profile_fetch_activity_planned",
            "operation_completion_deferred": True,
            "operation_phase": "operation_native_profile_fetch_activity_planned",
            "provider_called": False,
            "legacy_job_shell_created": False,
            "queue_workflow_called": False,
            "activity_run_id": activity_run_id,
            "parent_activity_run_id": parent_activity_run_id,
            "activity_attempt_id": str(completed_attempt.get("attempt_id") or "").strip(),
            "profile_url_count": len(profile_urls),
            "cache_hit_count": cache_hit_count,
            "fetch_required_count": fetch_required_count,
            "cache_hit_urls": cache_hit_urls,
            "fetch_required_urls": fetch_required_urls,
            "entity_delta_ids": profile_entity_delta_ids,
            "cache_hit_delta_ids": cache_hit_delta_ids,
            "fetch_required_delta_ids": fetch_required_delta_ids,
            "workflow_activity_run": final_activity,
            "provider_owner_required": fetch_required_count > 0,
            "downstream_command_required": bool(downstream_command_ids),
            "downstream_command_count": len(downstream_command_ids),
            "downstream_command_ids": downstream_command_ids,
            "downstream_commands": [
                command for command in (downstream_provider_command, downstream_terminal_command) if command
            ],
            "next_phase": "W11f_provider_profile_fetch_activity_owner"
            if fetch_required_count
            else "W11f_profile_terminal_and_projection_admission",
            "migration_phase": "W11e_operation_native_profile_fetch_activity",
            "contract": "w11e_operation_native_profile_fetch_activity_plan_v1",
        }

    def _execute_operation_native_profile_fetch_provider_command_payload(
        self,
        command: dict[str, Any],
        *,
        lease_owner: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        workflow_run_id = str(command_payload.get("workflow_run_id") or payload.get("workflow_run_id") or "").strip()
        operation_run_id = str(
            command_payload.get("operation_id") or payload.get("operation_id") or payload.get("operation_run_id") or ""
        ).strip()
        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        acquisition_run_id = str(payload.get("acquisition_run_id") or "").strip()
        activity_run_id = str(
            payload.get("source_profile_activity_run_id") or payload.get("activity_run_id") or ""
        ).strip()
        source_delta_ids = _dedupe_texts(payload.get("source_entity_delta_ids") or [])
        source_delta_rows = [
            self.store.get_workflow_entity_delta(delta_id)
            for delta_id in source_delta_ids
            if str(delta_id or "").strip()
        ]
        source_delta_by_profile_key: dict[str, list[str]] = defaultdict(list)
        for delta in source_delta_rows:
            delta_payload = dict(delta or {})
            entity_payload = dict(delta_payload.get("entity_payload") or {})
            profile_url = str(
                entity_payload.get("profile_url")
                or entity_payload.get("linkedin_url")
                or delta_payload.get("entity_key")
                or ""
            ).strip()
            profile_key = normalize_linkedin_profile_url_key(profile_url) or profile_url
            delta_id = str(delta_payload.get("delta_id") or "").strip()
            if profile_key and delta_id:
                source_delta_by_profile_key[profile_key].append(delta_id)
        profile_urls = _dedupe_texts(payload.get("profile_urls") or [])
        if source_delta_ids and not profile_urls:
            profile_urls = _dedupe_texts(
                [dict(delta.get("entity_payload") or {}).get("profile_url") for delta in source_delta_rows if delta]
            )
        provider_attempt_scope = str(payload.get("provider_attempt_scope") or "normal").strip() or "normal"
        retry_wave_index = max(0, _coerce_int(payload.get("retry_wave_index"), 0))
        retry_budget = max(0, _coerce_int(payload.get("profile_retry_budget"), 1))
        is_retry_wave = provider_attempt_scope == "retry_wave" or retry_wave_index > 0
        if not command_id or not workflow_run_id or not activity_run_id or not profile_urls:
            return {
                "status": "failed",
                "reason": "operation_native_profile_provider_payload_missing",
                "operation_completion_deferred": True,
                "provider_called": False,
                "legacy_job_shell_created": False,
            }
        activity = self.store.get_workflow_activity_run(activity_run_id)
        if not activity:
            return {
                "status": "failed",
                "reason": "operation_native_profile_activity_not_found",
                "operation_completion_deferred": True,
                "provider_called": False,
                "legacy_job_shell_created": False,
            }
        activity_output = dict(activity.get("output") or {})
        activity_metadata = dict(activity.get("metadata") or {})
        started_at = _utc_now_iso()
        self.store.upsert_workflow_activity_run(
            {
                **activity,
                "status": "running",
                "phase": "provider_profile_fetch_running",
                "metadata": {
                    **activity_metadata,
                    "provider_fetch_command_id": command_id,
                    "provider_fetch_started_at": started_at,
                    "provider_called": True,
                    "legacy_job_shell_created": False,
                    "migration_phase": "W11f_operation_native_profile_provider_fetch",
                },
            }
        )
        attempt_number = max(1, _coerce_int(command_payload.get("attempt"), 1))
        activity_root = self.runtime_dir / "operation_activities" / activity_run_id / "profile_fetch"
        attempt = self.store.upsert_workflow_activity_attempt(
            {
                "workspace_id": workspace_id,
                "activity_run_id": activity_run_id,
                "workflow_run_id": workflow_run_id,
                "command_id": command_id,
                "attempt_number": attempt_number,
                "status": "running",
                "provider": "linkedin_profile_detail",
                "provider_request_ref": f"operation_native_profile_provider_fetch:{activity_run_id}:{attempt_number}",
                "started_at": started_at,
                "input": {
                    "profile_urls": profile_urls,
                    "source_entity_delta_ids": source_delta_ids,
                    "artifact_root": str(activity_root),
                },
                "output": {},
                "artifact_refs": [],
                "error": {},
                "idempotency_key": f"workflow_activity_attempt:{command_id}:provider:{attempt_number}",
                "metadata": {
                    "lease_owner": str(lease_owner or "").strip(),
                    "provider_called": True,
                    "legacy_job_shell_created": False,
                    "migration_phase": "W11f_operation_native_profile_provider_fetch",
                },
            }
        )
        profile_connector = getattr(
            getattr(self.acquisition_engine, "multi_source_enricher", None),
            "profile_connector",
            None,
        )
        fetch_profile = getattr(profile_connector, "fetch_profile", None)
        if not callable(fetch_profile):
            completed_attempt = self.store.upsert_workflow_activity_attempt(
                {
                    **attempt,
                    "status": "retry_wait",
                    "completed_at": _utc_now_iso(),
                    "error": {"reason": "profile_detail_connector_unavailable"},
                }
            )
            self.store.upsert_workflow_activity_run(
                {
                    **(self.store.get_workflow_activity_run(activity_run_id) or activity),
                    "status": "retry_wait",
                    "phase": "provider_profile_fetch_retry_wait",
                    "metadata": {
                        **activity_metadata,
                        "latest_attempt_id": str(completed_attempt.get("attempt_id") or "").strip(),
                        "provider_owner_required": True,
                        "provider_called": False,
                        "migration_phase": "W11f_operation_native_profile_provider_fetch",
                    },
                }
            )
            return {
                "status": "deferred",
                "reason": "profile_detail_connector_unavailable",
                "operation_completion_deferred": True,
                "provider_called": False,
                "legacy_job_shell_created": False,
                "retry_delay_seconds": 60,
            }
        fetched_urls: list[str] = []
        failed_urls: list[str] = []
        artifact_refs: list[dict[str, Any]] = []
        fetched_delta_ids: list[str] = []
        errors: list[dict[str, str]] = []
        registry_repo = linkedin_profile_registry_repo(self.store)
        for profile_url in profile_urls:
            profile_key = normalize_linkedin_profile_url_key(profile_url)
            slug = self._operation_native_profile_slug(profile_url)
            if not slug:
                failed_urls.append(profile_url)
                errors.append({"profile_url": profile_url, "reason": "linkedin_profile_slug_missing"})
                continue
            try:
                fetched_profile = fetch_profile(slug, activity_root)
            except Exception as exc:
                fetched_profile = None
                errors.append({"profile_url": profile_url, "reason": f"{type(exc).__name__}: {exc}"[:240]})
            if not fetched_profile:
                failed_urls.append(profile_url)
                if not any(item.get("profile_url") == profile_url for item in errors):
                    errors.append({"profile_url": profile_url, "reason": "profile_detail_fetch_returned_empty"})
                continue
            raw_path = str(dict(fetched_profile).get("raw_path") or "").strip()
            if not raw_path:
                failed_urls.append(profile_url)
                errors.append({"profile_url": profile_url, "reason": "profile_detail_raw_path_missing"})
                continue
            fetched_urls.append(profile_url)
            artifact_refs.append({"profile_url": profile_url, "raw_path": raw_path})
            mark_fetched = getattr(registry_repo, "mark_fetched", None)
            if callable(mark_fetched):
                mark_fetched(
                    profile_url,
                    raw_path=raw_path,
                    source_shards=[f"workflow_activity:{activity_run_id}", "operation_native_profile_fetch"],
                    source_jobs=[],
                    raw_linkedin_url=profile_url,
                    sanity_linkedin_url=profile_key,
                    snapshot_dir=str(activity_root),
                    run_id=f"activity:{activity_run_id}",
                    dataset_id=f"command:{command_id}",
                )
            delta = self.store.upsert_workflow_entity_delta(
                {
                    "workspace_id": workspace_id,
                    "workflow_run_id": workflow_run_id,
                    "operation_run_id": operation_run_id,
                    "command_id": command_id,
                    "activity_run_id": activity_run_id,
                    "attempt_id": str(attempt.get("attempt_id") or "").strip(),
                    "acquisition_run_id": acquisition_run_id,
                    "entity_type": "profile",
                    "entity_key": profile_key or profile_url,
                    "delta_kind": "profile_provider_fetched",
                    "status": "recorded",
                    "reason": "operation_native_profile_provider_fetched",
                    "source_ref": {
                        "command_id": command_id,
                        "activity_run_id": activity_run_id,
                        "source_entity_delta_ids": source_delta_ids,
                    },
                    "entity_payload": {
                        "profile_url": profile_url,
                        "profile_url_key": profile_key,
                        "raw_path": raw_path,
                        "parsed_profile_available": bool(dict(fetched_profile).get("parsed")),
                    },
                    "projection_effect": {
                        "entered_projection": False,
                        "profile_count_delta": 1,
                        "reason": "pending_profile_terminal_or_projection_admission",
                    },
                    "artifact_refs": [{"raw_path": raw_path}],
                    "idempotency_key": (
                        f"workflow_entity_delta:{command_id}:profile_provider_fetched:"
                        f"{hashlib.sha1((profile_key or profile_url).encode('utf-8')).hexdigest()[:24]}"
                    ),
                    "metadata": {
                        "provider_called": True,
                        "legacy_job_shell_created": False,
                        "migration_phase": "W11f_operation_native_profile_provider_fetch",
                    },
                }
            )
            if delta:
                fetched_delta_ids.append(str(delta.get("delta_id") or "").strip())
        fetched_urls = _dedupe_texts(fetched_urls)
        failed_urls = _dedupe_texts(failed_urls)
        error_by_profile_key: dict[str, str] = {}
        for error in errors:
            error_url = str(dict(error).get("profile_url") or "").strip()
            error_key = normalize_linkedin_profile_url_key(error_url) or error_url
            if error_key and error_key not in error_by_profile_key:
                error_by_profile_key[error_key] = str(
                    dict(error).get("reason") or "profile_provider_fetch_failed"
                ).strip()
        failed_delta_ids: list[str] = []
        failed_source_delta_ids: list[str] = []
        retry_exhausted = is_retry_wave or retry_wave_index >= retry_budget
        for failed_url in failed_urls:
            failed_key = normalize_linkedin_profile_url_key(failed_url) or failed_url
            url_source_delta_ids = list(source_delta_by_profile_key.get(failed_key) or [])
            failed_source_delta_ids.extend(url_source_delta_ids)
            failed_reason = error_by_profile_key.get(failed_key) or "profile_provider_fetch_failed"
            delta_kind = "profile_provider_retry_exhausted" if retry_exhausted else "profile_provider_retry_bucketed"
            delta_status = "failed_terminal" if retry_exhausted else "retry_wait"
            delta = self.store.upsert_workflow_entity_delta(
                {
                    "workspace_id": workspace_id,
                    "workflow_run_id": workflow_run_id,
                    "operation_run_id": operation_run_id,
                    "command_id": command_id,
                    "activity_run_id": activity_run_id,
                    "attempt_id": str(attempt.get("attempt_id") or "").strip(),
                    "acquisition_run_id": acquisition_run_id,
                    "entity_type": "profile",
                    "entity_key": failed_key,
                    "delta_kind": delta_kind,
                    "status": delta_status,
                    "reason": failed_reason,
                    "source_ref": {
                        "command_id": command_id,
                        "activity_run_id": activity_run_id,
                        "source_entity_delta_ids": url_source_delta_ids,
                        "provider_attempt_scope": provider_attempt_scope,
                        "retry_wave_index": retry_wave_index,
                    },
                    "entity_payload": {
                        "profile_url": failed_url,
                        "profile_url_key": failed_key,
                        "retryable": not retry_exhausted,
                        "failed_reason": failed_reason,
                    },
                    "projection_effect": {
                        "entered_projection": False,
                        "profile_count_delta": 0,
                        "reason": "profile_provider_retry_bucketed"
                        if not retry_exhausted
                        else "profile_provider_retry_exhausted",
                    },
                    "artifact_refs": [],
                    "idempotency_key": (
                        f"workflow_entity_delta:{command_id}:{delta_kind}:"
                        f"{hashlib.sha1(failed_key.encode('utf-8')).hexdigest()[:24]}"
                    ),
                    "metadata": {
                        "provider_called": True,
                        "legacy_job_shell_created": False,
                        "provider_attempt_scope": provider_attempt_scope,
                        "retry_wave_index": retry_wave_index,
                        "retry_budget": retry_budget,
                        "retry_strategy": "bucketed_entity_retry_after_normal_wave",
                        "migration_phase": "W11f_operation_native_profile_provider_fetch",
                    },
                }
            )
            if delta:
                failed_delta_ids.append(str(delta.get("delta_id") or "").strip())
        fetched_count = len(fetched_urls)
        failed_count = len(failed_urls)
        attempt_status = (
            "succeeded"
            if failed_count == 0
            else "failed_terminal"
            if retry_exhausted and fetched_count == 0
            else "partial_success"
        )
        completed_attempt = self.store.upsert_workflow_activity_attempt(
            {
                **attempt,
                "status": attempt_status,
                "completed_at": _utc_now_iso(),
                "output": {
                    "profile_url_count": len(profile_urls),
                    "fetched_count": fetched_count,
                    "failed_count": failed_count,
                    "fetched_urls": fetched_urls,
                    "failed_urls": failed_urls,
                    "entity_delta_ids": fetched_delta_ids,
                    "failed_entity_delta_ids": failed_delta_ids,
                    "provider_attempt_scope": provider_attempt_scope,
                    "retry_wave_index": retry_wave_index,
                    "retry_exhausted": retry_exhausted,
                },
                "artifact_refs": artifact_refs,
                "error": {"errors": errors} if errors else {},
            }
        )
        final_status = (
            "succeeded"
            if failed_count == 0
            else "failed_terminal"
            if retry_exhausted and fetched_count == 0
            else "partial_success"
        )
        final_phase = (
            "provider_profile_fetch_completed"
            if failed_count == 0
            else "provider_profile_fetch_retry_exhausted"
            if retry_exhausted and fetched_count == 0
            else "provider_profile_fetch_partial_retry_planned"
        )
        final_activity = self.store.upsert_workflow_activity_run(
            {
                **(self.store.get_workflow_activity_run(activity_run_id) or activity),
                "status": final_status,
                "phase": final_phase,
                "output": {
                    **activity_output,
                    "latest_attempt_id": str(completed_attempt.get("attempt_id") or "").strip(),
                    "provider_fetched_urls": fetched_urls,
                    "provider_failed_urls": failed_urls,
                    "provider_entity_delta_ids": fetched_delta_ids,
                    "provider_failed_entity_delta_ids": failed_delta_ids,
                    "provider_attempt_scope": provider_attempt_scope,
                    "retry_wave_index": retry_wave_index,
                    "retry_exhausted": retry_exhausted,
                },
                "artifact_refs": [
                    *list(activity.get("artifact_refs") or []),
                    *artifact_refs,
                ],
                "entity_counts": {
                    **dict(activity.get("entity_counts") or {}),
                    "provider_fetched_count": fetched_count,
                    "provider_failed_count": failed_count,
                },
                "metadata": {
                    **activity_metadata,
                    "latest_attempt_id": str(completed_attempt.get("attempt_id") or "").strip(),
                    "provider_owner_required": failed_count > 0 and not retry_exhausted,
                    "provider_called": True,
                    "legacy_job_shell_created": False,
                    "provider_attempt_scope": provider_attempt_scope,
                    "retry_wave_index": retry_wave_index,
                    "retry_strategy": "bucketed_entity_retry_after_normal_wave",
                    "migration_phase": "W11f_operation_native_profile_provider_fetch",
                },
            }
        )
        downstream_terminal_command: dict[str, Any] = {}
        downstream_retry_command: dict[str, Any] = {}
        downstream_command_ids: list[str] = []
        if fetched_delta_ids:
            downstream_terminal_command = self._plan_operation_native_profile_terminal_admit_command(
                parent_command=command_payload,
                source_profile_activity_run_id=activity_run_id,
                acquisition_run_id=acquisition_run_id,
                target_company=str(payload.get("target_company") or "").strip(),
                profile_urls=fetched_urls,
                source_entity_delta_ids=fetched_delta_ids,
                workspace_id=workspace_id,
            )
            if not downstream_terminal_command:
                return {
                    "status": "failed",
                    "reason": "operation_native_profile_terminal_command_planning_failed",
                    "operation_completion_deferred": True,
                    "operation_phase": "operation_native_profile_terminal_planning_failed",
                    "provider_called": True,
                    "legacy_job_shell_created": False,
                    "queue_workflow_called": False,
                    "activity_run_id": activity_run_id,
                    "profile_url_count": len(profile_urls),
                    "fetched_count": fetched_count,
                    "failed_count": failed_count,
                    "migration_phase": "W11f_operation_native_profile_terminal_admission",
                }
            downstream_command_ids.append(str(downstream_terminal_command.get("command_id") or "").strip())
        if failed_count > 0 and not retry_exhausted:
            downstream_retry_command = self._plan_operation_native_profile_fetch_provider_command(
                parent_command=command_payload,
                source_profile_activity_run_id=activity_run_id,
                acquisition_run_id=acquisition_run_id,
                target_company=str(payload.get("target_company") or "").strip(),
                profile_urls=failed_urls,
                source_entity_delta_ids=failed_delta_ids or failed_source_delta_ids,
                workspace_id=workspace_id,
                provider_attempt_scope="retry_wave",
                retry_wave_index=retry_wave_index + 1,
            )
            if not downstream_retry_command:
                return {
                    "status": "failed",
                    "reason": "operation_native_profile_retry_wave_command_planning_failed",
                    "operation_completion_deferred": True,
                    "operation_phase": "operation_native_profile_retry_wave_planning_failed",
                    "provider_called": True,
                    "legacy_job_shell_created": False,
                    "queue_workflow_called": False,
                    "activity_run_id": activity_run_id,
                    "profile_url_count": len(profile_urls),
                    "fetched_count": fetched_count,
                    "failed_count": failed_count,
                    "migration_phase": "W11f_operation_native_profile_provider_fetch",
                }
            downstream_command_ids.append(str(downstream_retry_command.get("command_id") or "").strip())
        downstream_command_ids = [command_id for command_id in downstream_command_ids if command_id]
        if downstream_command_ids:
            final_activity = self.store.upsert_workflow_activity_run(
                {
                    **final_activity,
                    "output": {
                        **dict(final_activity.get("output") or {}),
                        "downstream_command_ids": downstream_command_ids,
                        "terminal_admission_entity_delta_ids": fetched_delta_ids,
                        "retry_wave_entity_delta_ids": failed_delta_ids,
                    },
                    "metadata": {
                        **dict(final_activity.get("metadata") or {}),
                        "downstream_command_ids": downstream_command_ids,
                        "terminal_admission_planned": bool(downstream_terminal_command),
                        "retry_wave_planned": bool(downstream_retry_command),
                    },
                }
            )
        provider_retry_exhausted = failed_count > 0 and retry_exhausted and fetched_count == 0
        if acquisition_run_id:
            acquisition_run = self.store.repos.workflow_runtime.get_acquisition_run(acquisition_run_id) or {}
            if acquisition_run:
                self._upsert_acquisition_run_phase(
                    acquisition_run=acquisition_run,
                    command=command_payload,
                    status=(
                        "profile_fetch_provider_completed"
                        if failed_count == 0
                        else "failed"
                        if provider_retry_exhausted
                        else "profile_fetch_provider_retry_wait"
                    ),
                    current_phase="profile_terminal_pending"
                    if fetched_delta_ids
                    else "profile_fetch_retry_wait"
                    if not retry_exhausted
                    else "profile_fetch_retry_exhausted",
                    metadata_patch={
                        "latest_profile_activity_run_id": activity_run_id,
                        "profile_provider_fetched_count": fetched_count,
                        "profile_provider_failed_count": failed_count,
                        "profile_provider_retry_wave_planned": bool(downstream_retry_command),
                    },
                )
        if provider_retry_exhausted:
            return {
                "status": "failed",
                "reason": "operation_native_profile_provider_fetch_retry_exhausted",
                "operation_completion_deferred": False,
                "operation_phase": "operation_native_profile_provider_fetch_retry_exhausted",
                "provider_called": True,
                "legacy_job_shell_created": False,
                "queue_workflow_called": False,
                "activity_run_id": activity_run_id,
                "activity_attempt_id": str(completed_attempt.get("attempt_id") or "").strip(),
                "profile_url_count": len(profile_urls),
                "fetched_count": fetched_count,
                "failed_count": failed_count,
                "failed_urls": failed_urls,
                "failed_entity_delta_ids": failed_delta_ids,
                "workflow_activity_run": final_activity,
                "retry_strategy": "bucketed_entity_retry_after_normal_wave",
                "provider_attempt_scope": provider_attempt_scope,
                "retry_wave_index": retry_wave_index,
                "migration_phase": "W11f_operation_native_profile_provider_fetch",
            }
        return {
            "status": "completed",
            "reason": "operation_native_profile_provider_fetch_completed"
            if failed_count == 0
            else "operation_native_profile_provider_fetch_partial_retry_planned"
            if downstream_retry_command
            else "operation_native_profile_provider_fetch_partial_retry_exhausted",
            "operation_completion_deferred": True,
            "operation_phase": "operation_native_profile_provider_fetch_completed"
            if failed_count == 0
            else "operation_native_profile_provider_fetch_partial_retry_planned"
            if downstream_retry_command
            else "operation_native_profile_provider_fetch_partial_retry_exhausted",
            "provider_called": True,
            "legacy_job_shell_created": False,
            "queue_workflow_called": False,
            "activity_run_id": activity_run_id,
            "activity_attempt_id": str(completed_attempt.get("attempt_id") or "").strip(),
            "profile_url_count": len(profile_urls),
            "fetched_count": fetched_count,
            "failed_count": failed_count,
            "fetched_urls": fetched_urls,
            "failed_urls": failed_urls,
            "entity_delta_ids": fetched_delta_ids,
            "failed_entity_delta_ids": failed_delta_ids,
            "workflow_activity_run": final_activity,
            "provider_owner_required": bool(downstream_retry_command),
            "downstream_command_required": bool(downstream_command_ids),
            "downstream_command_count": len(downstream_command_ids),
            "downstream_command_ids": downstream_command_ids,
            "downstream_commands": [
                command for command in (downstream_terminal_command, downstream_retry_command) if command
            ],
            "next_phase": "W11f_profile_terminal_and_projection_admission",
            "retry_strategy": "bucketed_entity_retry_after_normal_wave",
            "provider_attempt_scope": provider_attempt_scope,
            "retry_wave_index": retry_wave_index,
            "retry_exhausted": retry_exhausted,
            "migration_phase": "W11f_operation_native_profile_provider_fetch",
            "contract": "w11f_operation_native_profile_provider_fetch_v1",
        }

    def _execute_operation_native_profile_terminal_admit_command_payload(
        self,
        command: dict[str, Any],
        *,
        lease_owner: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        workflow_run_id = str(command_payload.get("workflow_run_id") or payload.get("workflow_run_id") or "").strip()
        operation_run_id = str(
            command_payload.get("operation_id") or payload.get("operation_id") or payload.get("operation_run_id") or ""
        ).strip()
        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        acquisition_run_id = str(payload.get("acquisition_run_id") or "").strip()
        source_activity_run_id = str(
            payload.get("source_profile_activity_run_id") or payload.get("activity_run_id") or ""
        ).strip()
        source_delta_ids = _dedupe_texts(payload.get("source_entity_delta_ids") or [])
        if source_activity_run_id and not source_delta_ids:
            source_deltas = self.store.list_workflow_entity_deltas(
                activity_run_id=source_activity_run_id,
                entity_type="profile",
                statuses=["recorded"],
                limit=max(1, min(1000, _coerce_int(payload.get("limit"), 250))),
            )
            source_delta_ids = _dedupe_texts(
                [
                    str(delta.get("delta_id") or "").strip()
                    for delta in source_deltas
                    if str(delta.get("delta_kind") or "") in {"profile_cache_hit", "profile_provider_fetched"}
                ]
            )
        if not command_id or not workflow_run_id or not source_activity_run_id or not source_delta_ids:
            return {
                "status": "failed",
                "reason": "operation_native_profile_terminal_payload_missing",
                "operation_completion_deferred": True,
                "provider_called": False,
                "legacy_job_shell_created": False,
            }
        source_activity = self.store.get_workflow_activity_run(source_activity_run_id)
        if not source_activity:
            return {
                "status": "failed",
                "reason": "operation_native_profile_terminal_source_activity_not_found",
                "operation_completion_deferred": True,
                "provider_called": False,
                "legacy_job_shell_created": False,
            }
        activity = self.store.upsert_workflow_activity_run(
            {
                "workspace_id": workspace_id,
                "workflow_run_id": workflow_run_id,
                "operation_run_id": operation_run_id,
                "acquisition_run_id": acquisition_run_id,
                "command_id": command_id,
                "parent_activity_run_id": source_activity_run_id,
                "activity_type": LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
                "owner": LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                "status": "running",
                "phase": "profile_terminal_admission_running",
                "idempotency_key": f"workflow_activity:{LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE}:{command_id}",
                "input": {
                    "source_profile_activity_run_id": source_activity_run_id,
                    "source_entity_delta_ids": source_delta_ids,
                },
                "output": {},
                "artifact_refs": [],
                "entity_counts": {"source_profile_delta_count": len(source_delta_ids)},
                "metadata": {
                    "lease_owner": str(lease_owner or "").strip(),
                    "legacy_job_shell_created": False,
                    "queue_workflow_called": False,
                    "normal_path_mutates_projection": False,
                    "migration_phase": "W11f_operation_native_profile_terminal_admission",
                },
            }
        )
        activity_run_id = str(activity.get("activity_run_id") or "").strip()
        attempt_number = max(1, _coerce_int(command_payload.get("attempt"), 1))
        attempt = self.store.upsert_workflow_activity_attempt(
            {
                "workspace_id": workspace_id,
                "activity_run_id": activity_run_id,
                "workflow_run_id": workflow_run_id,
                "command_id": command_id,
                "attempt_number": attempt_number,
                "status": "running",
                "provider": "local_profile_terminal_admission",
                "provider_request_ref": f"operation_native_profile_terminal_admission:{activity_run_id}:{attempt_number}",
                "started_at": _utc_now_iso(),
                "input": {"source_entity_delta_ids": source_delta_ids},
                "output": {},
                "artifact_refs": [],
                "error": {},
                "idempotency_key": f"workflow_activity_attempt:{command_id}:terminal:{attempt_number}",
                "metadata": {
                    "lease_owner": str(lease_owner or "").strip(),
                    "provider_called": False,
                    "legacy_job_shell_created": False,
                    "migration_phase": "W11f_operation_native_profile_terminal_admission",
                },
            }
        )
        source_deltas = [
            self.store.get_workflow_entity_delta(delta_id)
            for delta_id in source_delta_ids
            if str(delta_id or "").strip()
        ]
        admitted_delta_ids: list[str] = []
        skipped_delta_ids: list[str] = []
        admitted_profile_urls: list[str] = []
        for source_delta in [dict(delta) for delta in source_deltas if delta]:
            source_delta_kind = str(source_delta.get("delta_kind") or "").strip()
            if source_delta_kind not in {"profile_cache_hit", "profile_provider_fetched"}:
                skipped_delta_ids.append(str(source_delta.get("delta_id") or "").strip())
                continue
            entity_payload = dict(source_delta.get("entity_payload") or {})
            profile_url = str(entity_payload.get("profile_url") or "").strip()
            profile_key = str(
                source_delta.get("entity_key") or entity_payload.get("profile_url_key") or profile_url
            ).strip()
            raw_path = str(entity_payload.get("raw_path") or "").strip()
            admitted_profile_urls.append(profile_url)
            terminal_delta = self.store.upsert_workflow_entity_delta(
                {
                    "workspace_id": workspace_id,
                    "workflow_run_id": workflow_run_id,
                    "operation_run_id": operation_run_id,
                    "command_id": command_id,
                    "activity_run_id": activity_run_id,
                    "attempt_id": str(attempt.get("attempt_id") or "").strip(),
                    "acquisition_run_id": acquisition_run_id,
                    "entity_type": "profile",
                    "entity_key": profile_key,
                    "delta_kind": "profile_terminal_recorded",
                    "status": "recorded",
                    "reason": "operation_native_profile_terminal_recorded",
                    "source_ref": {
                        "source_profile_activity_run_id": source_activity_run_id,
                        "source_entity_delta_id": str(source_delta.get("delta_id") or "").strip(),
                        "source_delta_kind": source_delta_kind,
                        "command_id": command_id,
                    },
                    "entity_payload": {
                        "profile_url": profile_url,
                        "profile_url_key": profile_key,
                        "raw_path": raw_path,
                        "source_delta_kind": source_delta_kind,
                    },
                    "projection_effect": {
                        "entered_projection": False,
                        "profile_count_delta": 1,
                        "reason": "projection_admission_pending_owner",
                    },
                    "artifact_refs": list(source_delta.get("artifact_refs") or []),
                    "idempotency_key": (
                        f"workflow_entity_delta:{command_id}:profile_terminal_recorded:"
                        f"{hashlib.sha1((profile_key or profile_url).encode('utf-8')).hexdigest()[:24]}"
                    ),
                    "metadata": {
                        "provider_called": False,
                        "legacy_job_shell_created": False,
                        "queue_workflow_called": False,
                        "normal_path_mutates_projection": False,
                        "migration_phase": "W11f_operation_native_profile_terminal_admission",
                    },
                }
            )
            if terminal_delta:
                admitted_delta_ids.append(str(terminal_delta.get("delta_id") or "").strip())
        completed_attempt = self.store.upsert_workflow_activity_attempt(
            {
                **attempt,
                "status": "succeeded",
                "completed_at": _utc_now_iso(),
                "output": {
                    "source_delta_count": len(source_delta_ids),
                    "admitted_count": len(admitted_delta_ids),
                    "skipped_count": len(skipped_delta_ids),
                    "admitted_profile_urls": admitted_profile_urls,
                    "admitted_entity_delta_ids": admitted_delta_ids,
                    "skipped_source_entity_delta_ids": skipped_delta_ids,
                },
                "error": {},
            }
        )
        final_activity = self.store.upsert_workflow_activity_run(
            {
                **(self.store.get_workflow_activity_run(activity_run_id) or activity),
                "status": "succeeded",
                "phase": "profile_terminal_admitted_pending_projection",
                "output": {
                    "latest_attempt_id": str(completed_attempt.get("attempt_id") or "").strip(),
                    "source_entity_delta_ids": source_delta_ids,
                    "admitted_entity_delta_ids": admitted_delta_ids,
                    "skipped_source_entity_delta_ids": skipped_delta_ids,
                },
                "entity_counts": {
                    "source_profile_delta_count": len(source_delta_ids),
                    "terminal_admitted_count": len(admitted_delta_ids),
                    "skipped_source_delta_count": len(skipped_delta_ids),
                },
                "metadata": {
                    **dict(activity.get("metadata") or {}),
                    "latest_attempt_id": str(completed_attempt.get("attempt_id") or "").strip(),
                    "provider_called": False,
                    "legacy_job_shell_created": False,
                    "queue_workflow_called": False,
                    "normal_path_mutates_projection": False,
                    "next_phase": "projection_admission_pending_owner",
                    "migration_phase": "W11f_operation_native_profile_terminal_admission",
                },
            }
        )
        downstream_projection_command: dict[str, Any] = {}
        downstream_command_ids: list[str] = []
        if admitted_delta_ids:
            downstream_projection_command = self._plan_operation_native_projection_admission_command(
                parent_command=command_payload,
                source_profile_terminal_activity_run_id=activity_run_id,
                acquisition_run_id=acquisition_run_id,
                target_company=str(payload.get("target_company") or "").strip(),
                profile_urls=admitted_profile_urls,
                source_entity_delta_ids=admitted_delta_ids,
                workspace_id=workspace_id,
            )
            if not downstream_projection_command:
                return {
                    "status": "failed",
                    "reason": "operation_native_projection_admission_command_planning_failed",
                    "operation_completion_deferred": True,
                    "operation_phase": "operation_native_projection_admission_planning_failed",
                    "provider_called": False,
                    "legacy_job_shell_created": False,
                    "queue_workflow_called": False,
                    "activity_run_id": activity_run_id,
                    "source_delta_count": len(source_delta_ids),
                    "admitted_count": len(admitted_delta_ids),
                    "migration_phase": "W11g_operation_native_projection_admission",
                }
            downstream_command_ids = [str(downstream_projection_command.get("command_id") or "").strip()]
            final_activity = self.store.upsert_workflow_activity_run(
                {
                    **final_activity,
                    "output": {
                        **dict(final_activity.get("output") or {}),
                        "downstream_command_ids": downstream_command_ids,
                        "projection_admission_entity_delta_ids": admitted_delta_ids,
                    },
                    "metadata": {
                        **dict(final_activity.get("metadata") or {}),
                        "downstream_command_ids": downstream_command_ids,
                        "projection_admission_planned": True,
                    },
                }
            )
        if acquisition_run_id:
            acquisition_run = self.store.repos.workflow_runtime.get_acquisition_run(acquisition_run_id) or {}
            if acquisition_run:
                self._upsert_acquisition_run_phase(
                    acquisition_run=acquisition_run,
                    command=command_payload,
                    status="profile_terminal_recorded",
                    current_phase="projection_admission_pending",
                    metadata_patch={
                        "latest_profile_terminal_activity_run_id": activity_run_id,
                        "profile_terminal_recorded_count": len(admitted_delta_ids),
                        "profile_terminal_skipped_count": len(skipped_delta_ids),
                    },
                )
        return {
            "status": "completed",
            "reason": "operation_native_profile_terminal_admitted",
            "operation_completion_deferred": True,
            "operation_phase": "operation_native_profile_terminal_admitted_pending_projection",
            "provider_called": False,
            "legacy_job_shell_created": False,
            "queue_workflow_called": False,
            "normal_path_mutates_projection": False,
            "activity_run_id": activity_run_id,
            "source_profile_activity_run_id": source_activity_run_id,
            "activity_attempt_id": str(completed_attempt.get("attempt_id") or "").strip(),
            "source_delta_count": len(source_delta_ids),
            "admitted_count": len(admitted_delta_ids),
            "skipped_count": len(skipped_delta_ids),
            "profile_urls": admitted_profile_urls,
            "entity_delta_ids": admitted_delta_ids,
            "workflow_activity_run": final_activity,
            "downstream_command_required": True,
            "downstream_command_count": len(downstream_command_ids),
            "downstream_command_ids": downstream_command_ids,
            "downstream_commands": [downstream_projection_command] if downstream_projection_command else [],
            "next_phase": "projection_admission_pending_owner",
            "migration_phase": "W11f_operation_native_profile_terminal_admission",
            "contract": "w11f_operation_native_profile_terminal_admission_v1",
        }

    def _drain_operation_native_profile_fetch_activity_commands(
        self,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized = dict(payload or {})
        limit = max(
            1,
            _coerce_int(
                normalized.get("operation_native_profile_fetch_command_limit") or normalized.get("command_limit") or 10,
                10,
            ),
        )
        lease_owner = str(
            normalized.get("owner_id") or f"operation-native-profile-fetch-{uuid.uuid4().hex[:8]}"
        ).strip()
        workflow_run_id = str(normalized.get("workflow_run_id") or "").strip()
        ready_commands: list[dict[str, Any]] = []
        for command_type in (
            LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
            LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
            LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
        ):
            remaining_limit = max(0, limit - len(ready_commands))
            if remaining_limit <= 0:
                break
            ready_commands.extend(
                [
                    dict(command)
                    for command in self.store.list_ready_workflow_commands(
                        workflow_run_id=workflow_run_id,
                        owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                        command_type=command_type,
                        limit=remaining_limit,
                    )
                ]
            )
        results: list[dict[str, Any]] = []
        claimed_count = 0
        completed_count = 0
        failed_count = 0
        skipped_count = 0
        for ready_command in ready_commands[:limit]:
            command_id = str(ready_command.get("command_id") or "").strip()
            if not command_id:
                skipped_count += 1
                continue
            current_command = self.store.get_workflow_command(command_id) or ready_command
            current_status = str(current_command.get("status") or "").strip().lower()
            if current_status == "succeeded":
                command_result = dict(current_command.get("result") or {})
                results.append(
                    {
                        **command_result,
                        "status": "completed",
                        "reason": str(
                            command_result.get("reason") or "operation_native_profile_fetch_already_succeeded"
                        ),
                        "workflow_command": self._kernel._workflow_command_observation(
                            current_command,
                            migration_phase="W11e_operation_native_profile_fetch_activity",
                        ),
                    }
                )
                completed_count += 1
                continue
            if current_status in {"claimed", "running", "failed_terminal", "cancelled", "superseded"}:
                observed = self._kernel._workflow_command_observation(
                    current_command,
                    migration_phase="W11e_operation_native_profile_fetch_activity",
                )
                if observed:
                    observed["runtime_command_contention"] = True
                results.append(
                    {
                        "status": "queued",
                        "reason": "operation_native_profile_fetch_command_already_owned",
                        "workflow_command": observed,
                    }
                )
                skipped_count += 1
                continue
            claimed_command = self.store.claim_workflow_command(
                command_id,
                lease_owner=lease_owner,
                lease_seconds=max(30, _env_int("OPERATION_NATIVE_PROFILE_FETCH_COMMAND_LEASE_SECONDS", 300)),
            )
            if not claimed_command:
                refreshed = self.store.get_workflow_command(command_id) or current_command
                observed = self._kernel._workflow_command_observation(
                    refreshed,
                    migration_phase="W11e_operation_native_profile_fetch_activity",
                )
                if observed:
                    observed["runtime_command_contention"] = True
                results.append(
                    {
                        "status": "queued",
                        "reason": "operation_native_profile_fetch_command_claim_contention",
                        "workflow_command": observed,
                    }
                )
                skipped_count += 1
                continue
            running_command = (
                self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner) or claimed_command
            )
            claimed_count += 1
            running_command_type = str(running_command.get("command_type") or "").strip()
            if running_command_type == LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE:
                result_payload = self._execute_operation_native_profile_fetch_provider_command_payload(
                    running_command,
                    lease_owner=lease_owner,
                )
                migration_phase = "W11f_operation_native_profile_provider_fetch"
                sync_source = "operation_native_profile_fetch_provider_owner"
            elif running_command_type == LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE:
                result_payload = self._execute_operation_native_profile_terminal_admit_command_payload(
                    running_command,
                    lease_owner=lease_owner,
                )
                migration_phase = "W11f_operation_native_profile_terminal_admission"
                sync_source = "operation_native_profile_terminal_admission_owner"
            else:
                result_payload = self._execute_operation_native_profile_fetch_activity_command_payload(
                    running_command,
                    lease_owner=lease_owner,
                )
                migration_phase = "W11e_operation_native_profile_fetch_activity"
                sync_source = "operation_native_profile_fetch_activity_owner"
            results.append(result_payload)
            result_status = str(result_payload.get("status") or "").strip()
            if result_status == "completed":
                terminal_command = self.store.mark_workflow_command_succeeded(command_id, result=result_payload)
                result_payload["workflow_command"] = self._kernel._workflow_command_observation(
                    terminal_command or running_command,
                    migration_phase=migration_phase,
                )
                self._kernel._sync_operation_run_from_workflow_command(
                    terminal_command or running_command,
                    actor=lease_owner,
                    source=sync_source,
                )
                completed_count += 1
            elif result_status in {"deferred", "waiting_prerequisite"}:
                retry_command = self.store.mark_workflow_command_failed(
                    command_id,
                    error_text=str(result_payload.get("reason") or "operation_native_profile_fetch_retry_wait"),
                    retryable=True,
                    retry_delay_seconds=max(30, _coerce_int(result_payload.get("retry_delay_seconds"), 60)),
                )
                result_payload["workflow_command"] = self._kernel._workflow_command_observation(
                    retry_command or running_command,
                    migration_phase=migration_phase,
                )
                self._kernel._sync_operation_run_from_workflow_command(
                    retry_command or running_command,
                    actor=lease_owner,
                    source=sync_source,
                )
                failed_count += 1
            else:
                failed_command = self.store.mark_workflow_command_failed(
                    command_id,
                    error_text=str(result_payload.get("reason") or "operation_native_profile_fetch_failed"),
                    retryable=False,
                )
                result_payload["workflow_command"] = self._kernel._workflow_command_observation(
                    failed_command or running_command,
                    migration_phase=migration_phase,
                )
                self._kernel._sync_operation_run_from_workflow_command(
                    failed_command or running_command,
                    actor=lease_owner,
                    source=sync_source,
                )
                failed_count += 1
        return {
            "status": "active" if claimed_count > 0 else "idle",
            "reason": "operation_native_profile_fetch_activity_command_owner"
            if ready_commands
            else "no_ready_operation_native_profile_fetch_commands",
            "workflow_run_id": workflow_run_id,
            "command_count": len(ready_commands),
            "executed_command_count": claimed_count,
            "claimed_count": claimed_count,
            "completed_count": completed_count,
            "failed_count": failed_count,
            "skipped_count": skipped_count,
            "legacy_bridge_used": False,
            "migration_phase": "W11e_operation_native_profile_fetch_activity",
            "items": results,
        }

    @staticmethod
    def _job_scoped_profile_refill_open_states() -> list[str]:
        return [
            "deferred_budget",
            "deferred_coalescing",
            "dispatch_claimed",
            "planned_dispatch",
            "retry_wait",
        ]

    @staticmethod
    def _job_scoped_profile_refill_actionable_states() -> list[str]:
        return [
            "deferred_budget",
            "deferred_coalescing",
            "dispatch_claimed",
            "retry_wait",
        ]

    def _cancel_running_profile_fetch_activity_before_cache_lookup_attempt(
        self,
        command: dict[str, Any],
        *,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        body = dict(command_payload.get("payload") or {})
        actor = str(payload.get("actor") or payload.get("operator") or "api").strip() or "api"
        reason = str(payload.get("reason") or "cancelled_before_profile_fetch_activity_attempt").strip()
        force = _coerce_bool(payload.get("force"), False)
        downstream = self._workflow_command_downstream_commands(command_payload)
        if downstream:
            return {
                "status": "invalid",
                "reason": "profile_fetch_activity_cancel_blocked_after_downstream_planned",
                "downstream_command_count": len(downstream),
                "downstream_command_ids": [
                    str(row.get("command_id") or "").strip()
                    for row in downstream
                    if str(row.get("command_id") or "").strip()
                ],
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                **self._kernel._workflow_command_control_response_policy_records(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        if self._kernel._workflow_command_lease_active(command_payload) and not force:
            return {
                "status": "invalid",
                "reason": "workflow_command_running_cancel_requires_expired_lease_or_force",
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                **self._kernel._workflow_command_control_response_policy_records(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        workflow_run_id = str(command_payload.get("workflow_run_id") or body.get("workflow_run_id") or "").strip()
        acquisition_run_id = str(body.get("acquisition_run_id") or "").strip()
        activities = self.store.list_workflow_activity_runs(
            workflow_run_id=workflow_run_id,
            acquisition_run_id=acquisition_run_id,
            command_id=command_id,
            activity_type=LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
            limit=50,
        )
        activity_ids = [
            str(activity.get("activity_run_id") or "").strip()
            for activity in activities
            if str(activity.get("activity_run_id") or "").strip()
        ]
        attempt_count = 0
        for activity_id in activity_ids:
            attempt_count += len(self.store.list_workflow_activity_attempts(activity_run_id=activity_id, limit=1))
        entity_delta_count = len(self.store.list_workflow_entity_deltas(command_id=command_id, limit=1))
        if attempt_count or entity_delta_count:
            return {
                "status": "invalid",
                "reason": "profile_fetch_activity_cancel_blocked_after_cache_lookup_started",
                "activity_attempt_count": attempt_count,
                "entity_delta_count": entity_delta_count,
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                **self._kernel._workflow_command_control_response_policy_records(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        cancelled_activities: list[dict[str, Any]] = []
        for activity in activities:
            metadata = dict(activity.get("metadata") or {})
            metadata.update(
                {
                    "cancelled_by": actor,
                    "cancel_reason": reason,
                    "control_source": "api.workflow_command_owner_specific_cancel",
                    "activity_attempt_started": False,
                    "profile_entity_delta_recorded": False,
                }
            )
            cancelled_activity = self.store.upsert_workflow_activity_run(
                {
                    **activity,
                    "status": "cancelled_before_cache_lookup",
                    "phase": "cancelled",
                    "metadata": metadata,
                }
            )
            if cancelled_activity:
                cancelled_activities.append(cancelled_activity)
        cancelled_run: dict[str, Any] = {}
        if acquisition_run_id:
            acquisition_run = self.store.repos.workflow_runtime.get_acquisition_run(acquisition_run_id) or {}
            if acquisition_run:
                cancelled_run = self._upsert_acquisition_run_phase(
                    acquisition_run=acquisition_run,
                    command=command_payload,
                    status="cancelled_before_profile_fetch_activity",
                    current_phase="cancelled",
                    metadata_patch={
                        "cancelled_by": actor,
                        "cancel_reason": reason,
                        "control_source": "api.workflow_command_owner_specific_cancel",
                        "activity_attempt_started": False,
                        "profile_entity_delta_recorded": False,
                    },
                )
        updated = self.store.cancel_workflow_command(
            command_id,
            reason=reason,
            actor=actor,
            result={
                "control_source": "api.workflow_command_owner_specific_cancel",
                "control_action": "cancel",
                "owner_specific_control": True,
                "cancel_boundary": "profile_fetch_activity_before_cache_lookup_attempt",
                "activity_run_cancelled_count": len(cancelled_activities),
                "activity_attempt_started": False,
                "profile_entity_delta_recorded": False,
                "acquisition_run_id": acquisition_run_id,
                "acquisition_run_cancelled": bool(cancelled_run),
                "downstream_command_planned": False,
                "force": force,
            },
            from_statuses=("claimed", "running"),
        )
        if not updated:
            latest = self.store.get_workflow_command(command_id) or command_payload
            return {
                "status": "invalid",
                "reason": "workflow_command_owner_specific_cancel_not_applied",
                "workflow_command": self._kernel._workflow_command_api_record(latest),
                **self._kernel._workflow_command_control_response_policy_records(latest),
                "workflow_activity_runs": cancelled_activities,
                "acquisition_run": cancelled_run,
                "module_state_mutated": bool(cancelled_activities or cancelled_run),
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        operation_sync = self._sync_operation_run_from_workflow_command_control(
            updated,
            control_action="cancel",
            actor=actor,
            source="api.workflow_command_owner_specific_cancel",
        )
        return {
            "status": "cancelled",
            "workflow_command": self._kernel._workflow_command_api_record(updated),
            "operation_sync": operation_sync,
            "workflow_activity_runs": cancelled_activities,
            "acquisition_run": cancelled_run,
            **self._kernel._workflow_command_control_response_policy_records(updated),
            "module_state_mutated": bool(cancelled_activities or cancelled_run),
            "owner_specific_control": True,
            "contract": "w11_workflow_command_owner_specific_control_v1",
        }

    def _queue_background_profile_prefetch_from_search_seed_snapshot(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan_payload: dict[str, Any],
        snapshot_dir: Path,
        search_seed_snapshot: SearchSeedSnapshot | None,
        submit_provider: bool = True,
    ) -> dict[str, Any]:
        if not isinstance(snapshot_dir, Path):
            return {"status": "skipped", "reason": "snapshot_dir_missing"}
        if not isinstance(search_seed_snapshot, SearchSeedSnapshot):
            return {"status": "skipped", "reason": "search_seed_snapshot_missing"}
        return self._queue_background_profile_prefetch_from_available_baselines(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            snapshot_dir=snapshot_dir,
            roster_snapshot=None,
            search_seed_snapshot=search_seed_snapshot,
            load_cached_profile_payloads=False,
            submit_provider=submit_provider,
        )

    def _queue_background_profile_prefetch_after_harvest_ingest(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan_payload: dict[str, Any],
        snapshot_dir: Path,
        defer_provider_submit: bool = False,
    ) -> dict[str, Any]:
        if not _env_bool("SOURCING_HARVEST_POST_INGEST_PROFILE_PREFETCH_ENABLED", True):
            return {
                "status": "skipped",
                "reason": "post_ingest_profile_prefetch_disabled",
                "requested_url_count": 0,
                "dispatched_url_count": 0,
                "queued_worker_count": 0,
                "summary_paths": [],
            }
        try:
            return self._queue_background_profile_prefetch_from_available_baselines(
                job_id=job_id,
                request=request,
                plan_payload=plan_payload,
                snapshot_dir=snapshot_dir,
                roster_snapshot=self._restore_roster_snapshot_from_snapshot_dir(
                    snapshot_dir=snapshot_dir,
                    identity=resolve_snapshot_company_identity(
                        snapshot_dir,
                        fallback_target_company=request.target_company,
                    ),
                ),
                search_seed_snapshot=_restore_search_seed_snapshot_from_snapshot_dir(
                    snapshot_dir=snapshot_dir,
                    identity=resolve_snapshot_company_identity(
                        snapshot_dir,
                        fallback_target_company=request.target_company,
                    ),
                ),
                load_cached_profile_payloads=False,
                submit_provider=not bool(defer_provider_submit),
            )
        except Exception as exc:
            return {
                "status": "failed",
                "reason": "post_ingest_profile_prefetch_failed",
                "error": str(exc),
                "retryable": True,
            }

    @staticmethod
    def _profile_prefetch_indicates_pending_provider_work(profile_prefetch: dict[str, Any]) -> bool:
        payload = dict(profile_prefetch or {})
        if str(payload.get("status") or "").strip().lower() != "queued":
            return False
        for key in (
            "queued_worker_count",
            "active_worker_count",
            "already_queued_url_count",
            "deferred_url_count",
            "dispatched_url_count",
        ):
            try:
                if int(payload.get(key) or 0) > 0:
                    return True
            except (TypeError, ValueError):
                continue
        return bool(list(payload.get("queued_urls") or []) or list(payload.get("deferred_urls") or []))

    def _trigger_profile_prefetch_refill(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan_payload: dict[str, Any],
        snapshot_dir: Path,
        source_worker_ids: list[int],
        trigger_source: str,
        trigger_reason: str,
        defer_provider_submit: bool = True,
    ) -> dict[str, Any]:
        """Record a provider-completion refill signal without running the scheduler.

        Provider-completion callbacks are intentionally signal-only. The actual
        registry scan/replan/claim/provider submit work belongs to the profile
        refill daemon so callback latency is bounded and auditable.
        """

        started_at = _utc_now_iso()
        started_monotonic = time.perf_counter()
        finished_at = _utc_now_iso()
        elapsed_ms = int(max(0.0, (time.perf_counter() - started_monotonic) * 1000))
        worker_ids = [int(item) for item in list(source_worker_ids or []) if int(item or 0) > 0]
        profile_prefetch = {
            "status": "queued",
            "reason": "refill_daemon_signal_only",
            "requested_url_count": 0,
            "dispatched_url_count": 0,
            "queued_worker_count": 0,
            "deferred_url_count": 0,
            "provider_submit_deferred_to_refill_daemon": True,
            "direct_refill_enabled": False,
            "direct_refill_async_running": False,
            "refill_daemon_signal_only": True,
            "summary_paths": [],
            "metrics": {
                "prefetch_started_at": started_at,
                "prefetch_finished_at": finished_at,
                "prefetch_elapsed_ms": elapsed_ms,
                "candidate_count": 0,
                "extra_profile_url_count": 0,
                "requested_url_count": 0,
                "refill_queue_item_count": 0,
                "cached_profile_count": 0,
                "registry_cache_marker_count": 0,
                "cached_profile_payload_count": 0,
                "load_cached_profile_payloads": False,
                "retry_isolated_refill": False,
                "signal_only": True,
            },
        }
        trigger_record: dict[str, Any] = {
            "kind": "profile_prefetch_refill_trigger",
            "schema_version": 1,
            "trigger_kind": "provider_completion",
            "trigger_reason": str(trigger_reason or "").strip() or "provider_completion",
            "trigger_source": str(trigger_source or "").strip(),
            "item_store": "linkedin_profile_registry",
            "snapshot_id": snapshot_dir.name,
            "worker_ids": worker_ids,
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_ms": elapsed_ms,
            "status": "queued",
            "reason": "refill_daemon_signal_only",
            "requested_url_count": 0,
            "dispatched_url_count": 0,
            "queued_worker_count": 0,
            "deferred_url_count": 0,
            "provider_submit_deferred_to_refill_daemon": True,
            "direct_refill_enabled": False,
            "direct_refill_async_running": False,
            "refill_daemon_signal_only": True,
            "signal_only": True,
            "next_submit_owner": "profile_refill_daemon",
            "max_sync_work": "record provider-completion signal only; no registry scan/replan/claim/provider submit",
            "profile_prefetch": profile_prefetch,
        }
        return trigger_record

    def _run_profile_prefetch_refill_queue_once(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Wake registry-backed Harvest profile refill work from the service loop.

        Provider completions still trigger immediate refills, but this daemon path
        closes the slot-release gap where no webhook/event arrives after budget
        becomes available. The durable item state remains in linkedin_profile_registry.
        """

        payload = dict(payload or {})
        if "profile_prefetch_refill_enabled" in payload and not _coerce_bool(
            payload.get("profile_prefetch_refill_enabled"),
            True,
        ):
            return {
                "status": "skipped",
                "reason": "profile_prefetch_refill_disabled_by_payload",
                "group_count": 0,
                "dispatched_url_count": 0,
                "queued_worker_count": 0,
                "deferred_url_count": 0,
            }
        if not _env_bool("SOURCING_PROFILE_PREFETCH_REFILL_DAEMON_ENABLED", True):
            return {
                "status": "skipped",
                "reason": "profile_prefetch_refill_daemon_disabled",
                "group_count": 0,
                "dispatched_url_count": 0,
                "queued_worker_count": 0,
            }
        registry_repo = linkedin_profile_registry_repo(self.store)
        list_groups = getattr(registry_repo, "list_refill_queue_groups", None)
        if not callable(list_groups):
            return {
                "status": "skipped",
                "reason": "refill_queue_group_selector_unavailable",
                "group_count": 0,
                "dispatched_url_count": 0,
                "queued_worker_count": 0,
            }
        explicit_job_id = str(payload.get("job_id") or "").strip()
        refill_phase = str(payload.get("profile_prefetch_refill_phase") or "").strip()
        group_limit = max(1, _coerce_int(payload.get("profile_prefetch_refill_group_limit"), 20))
        phase_budget_ms = max(
            0,
            _coerce_int(
                payload.get("profile_prefetch_refill_phase_budget_ms"),
                _env_int("PROFILE_PREFETCH_REFILL_PHASE_BUDGET_MS", 12000),
            ),
        )
        dispatch_worker_limit = max(
            1,
            _coerce_int(
                payload.get("profile_prefetch_refill_dispatch_worker_limit"),
                _env_int(
                    "PROFILE_PREFETCH_REFILL_DISPATCH_WORKER_LIMIT",
                    resolved_harvest_profile_actor_global_inflight({}),
                ),
            ),
        )
        refill_durable_unit_max_urls = max(
            50,
            _coerce_int(
                payload.get("profile_prefetch_refill_durable_unit_max_urls")
                or _env_int("HARVEST_PROFILE_PREFETCH_DURABLE_UNIT_MAX_URLS", 200),
                200,
            ),
        )
        refill_provider_envelope_max_urls = max(
            refill_durable_unit_max_urls,
            _coerce_int(
                payload.get("profile_prefetch_refill_provider_envelope_max_urls")
                or _env_int("HARVEST_PROFILE_PREFETCH_PROVIDER_ENVELOPE_MAX_URLS", 300),
                300,
            ),
        )
        refill_default_item_limit = max(1, refill_provider_envelope_max_urls)
        limiter_request_context: dict[str, Any] = {}
        if explicit_job_id:
            limiter_job = self.store.get_job(explicit_job_id)
            if limiter_job:
                limiter_request_payload = dict(limiter_job.get("request") or {})
                limiter_request_context = {
                    **dict(limiter_request_payload.get("execution_preferences") or {}),
                    **limiter_request_payload,
                }
        limiter_budget = resolved_harvest_profile_actor_global_inflight(limiter_request_context)
        limiter_status_fn = getattr(self.store, "get_runtime_provider_limiter_status", None)
        if callable(limiter_status_fn):
            try:
                limiter_status = dict(
                    limiter_status_fn(
                        "harvest_profile_scraper_actor",
                        budget=limiter_budget,
                    )
                    or {}
                )
            except Exception as exc:
                limiter_status = {
                    "available": True,
                    "reason": f"provider_limiter_status_failed:{type(exc).__name__}",
                    "error": str(exc),
                    "budget": limiter_budget,
                }
            if limiter_status and not bool(limiter_status.get("available", True)):
                return {
                    "status": "idle",
                    "reason": "profile_prefetch_refill_provider_limiter_full",
                    "group_count": 0,
                    "inspected_group_count": 0,
                    "active_group_count": 0,
                    "selected_refill_states": [
                        "deferred_budget",
                        "deferred_coalescing",
                        "dispatch_reserved",
                        "dispatch_claimed",
                    ],
                    "retry_wait_blocked_group_count": 0,
                    "dispatched_url_count": 0,
                    "queued_worker_count": 0,
                    "deferred_url_count": 0,
                    "phase_budget_ms": phase_budget_ms,
                    "dispatch_worker_limit": dispatch_worker_limit,
                    "provider_limiter": limiter_status,
                }
        nonblocking_submit = _coerce_bool(
            payload.get("profile_prefetch_nonblocking_submit"),
            True,
        )
        item_limit_per_group = max(
            1,
            _coerce_int(
                payload.get("profile_prefetch_refill_item_limit")
                or _env_int("SOURCING_PROFILE_PREFETCH_REFILL_ITEM_LIMIT", refill_default_item_limit),
                refill_default_item_limit,
            ),
        )
        try:
            selected_refill_states = [
                "deferred_budget",
                "deferred_coalescing",
                "dispatch_reserved",
                "dispatch_claimed",
            ]
            retry_wait_groups_blocked: dict[tuple[str, str], dict[str, Any]] = {}
            groups = list(
                list_groups(
                    states=selected_refill_states,
                    source_job=explicit_job_id,
                    limit=group_limit,
                    item_limit_per_group=item_limit_per_group,
                )
                or []
            )
            if not groups:
                retry_groups = list(
                    list_groups(
                        states=["retry_wait"],
                        source_job=explicit_job_id,
                        limit=group_limit,
                        item_limit_per_group=item_limit_per_group,
                    )
                    or []
                )
                retry_wait_allowed_groups: list[dict[str, Any]] = []
                enricher = self.acquisition_engine.multi_source_enricher
                for retry_group in retry_groups:
                    retry_payload = dict(retry_group or {})
                    source_job = str(retry_payload.get("source_job") or "").strip()
                    snapshot_dir_value = str(retry_payload.get("snapshot_dir") or "").strip()
                    retry_gate = enricher._profile_refill_retry_gate(
                        job_id=source_job,
                        snapshot_dir=Path(snapshot_dir_value).expanduser(),
                        normal_ready_item_count=0,
                    )
                    if bool(retry_gate.get("retry_allowed")):
                        retry_wait_allowed_groups.append(
                            {
                                **retry_payload,
                                "retry_wait_gate": retry_gate,
                            }
                        )
                    else:
                        retry_wait_groups_blocked[(source_job, snapshot_dir_value)] = {
                            **retry_payload,
                            "retry_wait_gate": retry_gate,
                        }
                groups = retry_wait_allowed_groups
                selected_refill_states = ["retry_wait"]
        except Exception as exc:
            return {
                "status": "failed",
                "reason": "refill_queue_group_selector_failed",
                "error": str(exc),
                "group_count": 0,
                "dispatched_url_count": 0,
                "queued_worker_count": 0,
            }
        if not groups:
            return {
                "status": "idle",
                "reason": (
                    "retry_wait_blocked_by_normal_profile_wave"
                    if retry_wait_groups_blocked
                    else "no_ready_profile_refill_items"
                ),
                "group_count": 0,
                "retry_wait_blocked_group_count": len(retry_wait_groups_blocked),
                "retry_wait_blocked_groups": list(retry_wait_groups_blocked.values())[:10],
                "selected_refill_states": selected_refill_states,
                "dispatched_url_count": 0,
                "queued_worker_count": 0,
            }

        results: list[dict[str, Any]] = []
        dispatched_url_count = 0
        queued_worker_count = 0
        deferred_url_count = 0
        planned_command_count_total = 0
        planned_worker_count_total = 0
        planned_url_count_total = 0
        inspected_group_count = 0
        active_group_count = 0
        elapsed_budget_exhausted = False
        dispatch_worker_budget_exhausted = False
        phase_started_monotonic = time.perf_counter()
        group_queue: list[dict[str, Any]] = [dict(group or {}) for group in list(groups[:group_limit] or [])]
        processed_group_scope_counts: dict[tuple[str, str], int] = {}
        while group_queue:
            group = group_queue.pop(0)
            elapsed_ms_so_far = int(max(0.0, (time.perf_counter() - phase_started_monotonic) * 1000))
            if (
                (queued_worker_count > 0 or dispatched_url_count > 0)
                and phase_budget_ms > 0
                and elapsed_ms_so_far >= phase_budget_ms
            ):
                elapsed_budget_exhausted = True
                break
            if queued_worker_count >= dispatch_worker_limit:
                dispatch_worker_budget_exhausted = True
                break
            source_job = str(dict(group or {}).get("source_job") or "").strip()
            snapshot_dir_value = str(dict(group or {}).get("snapshot_dir") or "").strip()
            group_scope_key = (source_job, snapshot_dir_value)
            processed_group_scope_counts[group_scope_key] = (
                int(processed_group_scope_counts.get(group_scope_key) or 0) + 1
            )
            if not source_job or not snapshot_dir_value:
                results.append(
                    {
                        "status": "skipped",
                        "reason": "group_scope_missing",
                        "source_job": source_job,
                        "snapshot_dir": snapshot_dir_value,
                    }
                )
                continue
            if explicit_job_id and source_job != explicit_job_id:
                continue
            inspected_group_count += 1
            ownership = runtime_namespace_ownership_for_path(
                snapshot_dir_value,
                configured_runtime_dir=self.runtime_dir,
            )
            if not ownership.matches:
                results.append(
                    {
                        "status": "skipped",
                        "reason": "runtime_namespace_mismatch",
                        "source_job": source_job,
                        "snapshot_dir": snapshot_dir_value,
                        "owner_runtime_dir": ownership.owner_runtime_dir,
                        "inferred_runtime_dir": ownership.inferred_runtime_dir,
                        "runtime_namespace": ownership.to_record(),
                        "item_count": int(dict(group or {}).get("item_count") or 0),
                    }
                )
                continue
            job = self.store.get_job(source_job)
            if not job:
                results.append(
                    {
                        "status": "skipped",
                        "reason": "job_missing",
                        "source_job": source_job,
                        "snapshot_dir": snapshot_dir_value,
                    }
                )
                continue
            job_status = str(job.get("status") or "").strip().lower()
            if job_status in {"failed", "superseded", "cancelled", "canceled"}:
                results.append(
                    {
                        "status": "skipped",
                        "reason": "job_terminal_without_refill",
                        "job_status": job_status,
                        "source_job": source_job,
                        "snapshot_dir": snapshot_dir_value,
                    }
                )
                continue
            request_payload = dict(job.get("request") or {})
            plan_payload = dict(job.get("plan") or {})
            request = JobRequest.from_payload(request_payload)
            snapshot_dir = Path(snapshot_dir_value).expanduser()
            if not snapshot_dir.exists():
                results.append(
                    {
                        "status": "skipped",
                        "reason": "snapshot_dir_missing",
                        "source_job": source_job,
                        "snapshot_dir": snapshot_dir_value,
                        "item_count": int(dict(group or {}).get("item_count") or 0),
                    }
                )
                continue
            active_group_count += 1
            started_at = _utc_now_iso()
            started_monotonic = time.perf_counter()
            try:
                profile_prefetch = self.acquisition_engine.multi_source_enricher.queue_background_profile_prefetch(
                    candidates=[],
                    extra_profile_urls=[],
                    snapshot_dir=snapshot_dir,
                    job_id=source_job,
                    request_payload=request.to_record(),
                    plan_payload=plan_payload,
                    runtime_mode="daemon_refill",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                    nonblocking_submit=nonblocking_submit,
                    dispatch_worker_limit=dispatch_worker_limit,
                    refill_item_limit=item_limit_per_group,
                    execute_profile_refill_submit_commands=False,
                )
            except Exception as exc:
                result = {
                    "status": "failed",
                    "reason": "profile_prefetch_refill_failed",
                    "error": str(exc),
                    "source_job": source_job,
                    "snapshot_dir": snapshot_dir_value,
                    "profile_prefetch_refill_phase": refill_phase,
                    "item_count": int(dict(group or {}).get("item_count") or 0),
                }
                results.append(result)
                self.store.append_job_event(
                    source_job,
                    stage=str(job.get("stage") or "acquiring"),
                    status=str(job.get("status") or "running"),
                    detail="Profile prefetch refill daemon failed while dispatching registry deferred items.",
                    payload=result,
                )
                continue
            elapsed_ms = int(max(0.0, (time.perf_counter() - started_monotonic) * 1000))
            prefetch_payload = dict(profile_prefetch or {})
            batch_plan = dict(prefetch_payload.get("batch_plan") or {})
            planned_command_count = _coerce_int(prefetch_payload.get("workflow_command_count"), 0)
            planned_worker_count = _coerce_int(prefetch_payload.get("queued_worker_count"), 0)
            planned_url_count = len(list(prefetch_payload.get("queued_urls") or []))
            result = {
                "kind": "profile_prefetch_refill_daemon_group",
                "schema_version": 1,
                "status": str(prefetch_payload.get("status") or "").strip(),
                "reason": str(prefetch_payload.get("reason") or "").strip(),
                "source_job": source_job,
                "snapshot_dir": snapshot_dir_value,
                "snapshot_id": snapshot_dir.name,
                "profile_prefetch_refill_phase": refill_phase,
                "started_at": started_at,
                "finished_at": _utc_now_iso(),
                "elapsed_ms": elapsed_ms,
                "item_count": int(dict(group or {}).get("item_count") or 0),
                "refill_item_limit": item_limit_per_group,
                "selected_refill_states": selected_refill_states,
                "retry_wait_gate": dict(group.get("retry_wait_gate") or {}),
                "refill_queue_item_count": _coerce_int(prefetch_payload.get("refill_queue_item_count"), 0),
                "requested_url_count": _coerce_int(prefetch_payload.get("requested_url_count"), 0),
                "planned_command_count": planned_command_count,
                "planned_worker_count": planned_worker_count,
                "planned_url_count": planned_url_count,
                "dispatched_url_count": 0,
                "queued_worker_count": 0,
                "deferred_url_count": _coerce_int(prefetch_payload.get("deferred_url_count"), 0),
                "batch_plan": batch_plan,
                "profile_prefetch": prefetch_payload,
            }
            dispatched_url_count += int(result["dispatched_url_count"])
            queued_worker_count += int(result["queued_worker_count"])
            deferred_url_count += int(result["deferred_url_count"])
            planned_command_count_total += planned_command_count
            planned_worker_count_total += planned_worker_count
            planned_url_count_total += planned_url_count
            if int(result["queued_worker_count"]) >= dispatch_worker_limit and int(result["deferred_url_count"]) > 0:
                dispatch_worker_budget_exhausted = True
            results.append(result)
            if planned_command_count > 0 or planned_worker_count > 0 or planned_url_count > 0:
                self.store.append_job_event(
                    source_job,
                    stage=str(job.get("stage") or "acquiring"),
                    status=str(job.get("status") or "running"),
                    detail="Profile prefetch refill daemon planned typed provider-submit commands.",
                    payload=result,
                )
            if (
                selected_refill_states != ["retry_wait"]
                and (planned_command_count > 0 or planned_worker_count > 0)
                and processed_group_scope_counts[group_scope_key] < dispatch_worker_limit
                and (
                    phase_budget_ms <= 0
                    or int(max(0.0, (time.perf_counter() - phase_started_monotonic) * 1000)) < phase_budget_ms
                )
            ):
                followup_groups = list(
                    list_groups(
                        states=selected_refill_states,
                        source_job=source_job,
                        limit=1,
                        item_limit_per_group=item_limit_per_group,
                    )
                    or []
                )
                if followup_groups:
                    group_queue.insert(0, dict(followup_groups[0] or {}))

        planned_work_observed = (
            planned_command_count_total > 0 or planned_worker_count_total > 0 or planned_url_count_total > 0
        )
        return {
            "status": "active"
            if dispatched_url_count > 0 or queued_worker_count > 0 or planned_work_observed
            else "idle",
            "reason": (
                "profile_prefetch_refill_phase_budget_exhausted"
                if elapsed_budget_exhausted
                else "profile_prefetch_refill_dispatch_worker_budget_exhausted"
                if dispatch_worker_budget_exhausted
                else "typed_profile_refill_submit_commands_planned"
                if planned_work_observed and dispatched_url_count <= 0 and queued_worker_count <= 0
                else "registry_profile_refill"
            ),
            "group_count": len(groups),
            "inspected_group_count": inspected_group_count,
            "active_group_count": active_group_count,
            "selected_refill_states": selected_refill_states,
            "retry_wait_blocked_group_count": len(retry_wait_groups_blocked),
            "retry_wait_blocked_groups": list(retry_wait_groups_blocked.values())[:10],
            "dispatched_url_count": dispatched_url_count,
            "queued_worker_count": queued_worker_count,
            "deferred_url_count": deferred_url_count,
            "planned_command_count": planned_command_count_total,
            "planned_worker_count": planned_worker_count_total,
            "planned_url_count": planned_url_count_total,
            "phase_budget_ms": phase_budget_ms,
            "dispatch_worker_limit": dispatch_worker_limit,
            "refill_item_limit": item_limit_per_group,
            "refill_durable_unit_max_urls": refill_durable_unit_max_urls,
            "refill_provider_envelope_max_urls": refill_provider_envelope_max_urls,
            "nonblocking_submit": nonblocking_submit,
            "elapsed_budget_exhausted": elapsed_budget_exhausted,
            "dispatch_worker_budget_exhausted": dispatch_worker_budget_exhausted,
            "groups": results,
        }

    def _handle_harvest_profile_completion_event(
        self,
        *,
        job: dict[str, Any],
        request: JobRequest,
        plan_payload: dict[str, Any],
        snapshot_dir: Path,
        source_worker_ids: list[int],
        source: str,
    ) -> dict[str, Any]:
        job_id = str(job.get("job_id") or "").strip()
        if not job_id:
            return {"status": "skipped", "reason": "job_id_missing"}
        started_at = _utc_now_iso()
        source_worker_id_values = [int(item) for item in list(source_worker_ids or []) if int(item or 0) > 0]
        next_submit = self._run_profile_completion_next_submit_opportunity(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            snapshot_dir=snapshot_dir,
            source_worker_ids=source_worker_id_values,
            source=source,
        )
        finished_at = _utc_now_iso()
        profile_refill_event_record = dict(next_submit.get("profile_refill_trigger") or {})
        profile_prefetch = dict(next_submit.get("profile_prefetch") or {})
        completion_elapsed_ms = _milliseconds_between_iso(started_at, finished_at)
        event_payload = {
            "status": "processed",
            "source": str(source or "").strip(),
            "snapshot_id": snapshot_dir.name,
            "worker_ids": source_worker_id_values,
            "profile_prefetch": dict(profile_prefetch),
            "profile_refill_trigger": profile_refill_event_record,
            "event_metrics": {
                "profile_completion_event_started_at": started_at,
                "profile_completion_event_finished_at": finished_at,
                "profile_completion_event_elapsed_ms": completion_elapsed_ms,
                "local_event_apply_started_at": finished_at,
                "next_submit_attempt_started_at": str(next_submit.get("started_at") or finished_at),
                "next_submit_attempt_finished_at": str(next_submit.get("finished_at") or finished_at),
                "next_submit_attempt_semantics": str(next_submit.get("semantics") or ""),
                "post_ingest_prefetch_elapsed_ms": _coerce_int(next_submit.get("elapsed_ms"), 0),
                "post_ingest_prefetch_candidate_count": _coerce_int(
                    profile_refill_event_record.get("requested_url_count"),
                    0,
                ),
                "post_ingest_prefetch_dispatched_url_count": _coerce_int(
                    profile_refill_event_record.get("dispatched_url_count"),
                    0,
                ),
                "post_ingest_prefetch_deferred_url_count": _coerce_int(
                    profile_refill_event_record.get("deferred_url_count"),
                    0,
                ),
                "registry_cache_marker_count": 0,
                "refill_daemon_signal_started_at": str(next_submit.get("started_at") or started_at),
                "refill_daemon_signal_finished_at": str(next_submit.get("finished_at") or finished_at),
            },
            "pipeline_order": "provider_completed_to_next_submit_before_local_apply",
            "materialization_policy": "local_apply_event_drains_board_visible_after_snapshot_delta",
        }
        self.store.append_job_event(
            job_id,
            stage=str(job.get("stage") or "acquiring"),
            status="running",
            detail="Harvest profile completion event ran the bounded next-submit opportunity before local apply.",
            payload=event_payload,
        )
        return event_payload

    def _record_harvest_profile_terminal_noop_ingest(
        self,
        *,
        worker: dict[str, Any],
        snapshot_id: str,
        source: str,
        reason: str = "harvest_profile_terminal_without_materializable_payload",
    ) -> dict[str, Any]:
        worker_id = int(dict(worker or {}).get("worker_id") or 0)
        if worker_id <= 0:
            return {}
        latest = self.store.get_agent_worker(worker_id=worker_id) or dict(worker or {})
        checkpoint = dict(latest.get("checkpoint") or {})
        output = dict(latest.get("output") or {})
        if dict(output.get("inline_incremental_ingest") or {}):
            return dict(output.get("inline_incremental_ingest") or {})
        summary = dict(output.get("summary") or {})
        requested_count = _coerce_int(
            summary.get("requested_url_count") or summary.get("requested_urls") or output.get("requested_url_count"),
            0,
        )
        unresolved_count = _coerce_int(
            summary.get("unresolved_url_count")
            or output.get("unresolved_url_count")
            or len(list(output.get("unresolved_urls") or [])),
            0,
        )
        marker = {
            "worker_kind": "harvest_prefetch",
            "snapshot_id": str(snapshot_id or "").strip(),
            "applied_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "apply_status": "skipped",
            "sync_status": "skipped",
            "sync_reason": str(reason or "").strip() or "harvest_profile_terminal_without_materializable_payload",
            "materialization_contract": "profile_terminal_no_materializable_delta",
            "full_snapshot_materialization_performed": False,
            "candidate_doc_path": "",
            "candidate_ids": [],
            "candidate_count": 0,
            "applied_worker_ids": [worker_id],
            "applied_worker_count": 1,
            "writer_scope": "job",
            "sync_policy": "terminal_profile_noop_single_writer",
            "terminal_profile_noop": True,
            "requested_url_count": requested_count,
            "unresolved_url_count": unresolved_count,
            "persisted_profile_count": 0,
            "source": str(source or "").strip(),
        }
        output["inline_incremental_ingest"] = marker
        self.store.checkpoint_agent_worker(
            worker_id,
            checkpoint_payload=checkpoint,
            output_payload=output,
            status=str(latest.get("status") or "completed"),
        )
        return marker
