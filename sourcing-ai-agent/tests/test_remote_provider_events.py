import importlib.util
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from urllib import error as urllib_error
from urllib import request as urllib_request

from sourcing_agent.api import create_server
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.remote_provider_events import (
    collect_remote_provider_event_targets,
    normalize_remote_provider_event,
    remote_provider_event_matches_worker,
)
from sourcing_agent.service_daemon import read_service_wakeup_request
from sourcing_agent.workflow_event_response import remote_event_lane_for_worker


def _load_apify_webhook_smoke_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "apify_webhook_roundtrip_smoke.py"
    spec = importlib.util.spec_from_file_location("apify_webhook_roundtrip_smoke", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_apify_webhook_preflight_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "apify_webhook_preflight.py"
    spec = importlib.util.spec_from_file_location("apify_webhook_preflight", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fake_preflight_orchestrator(*, enabled: bool = True, api_token: str = "actor-api-token"):
    return SimpleNamespace(
        acquisition_engine=SimpleNamespace(
            settings=SimpleNamespace(
                harvest=SimpleNamespace(
                    profile_scraper=SimpleNamespace(enabled=enabled, api_token=api_token),
                )
            )
        )
    )


def test_apify_webhook_roundtrip_smoke_requires_public_callback_url_by_default() -> None:
    smoke = _load_apify_webhook_smoke_module()

    for url in (
        "http://127.0.0.1:8765/api/providers/apify/webhook",
        "http://localhost:8765/api/providers/apify/webhook",
        "https://backend.local/api/providers/apify/webhook",
    ):
        try:
            smoke._validate_webhook_url(url, allow_local=False)  # noqa: SLF001
        except SystemExit as exc:
            assert "Apify cannot call" in str(exc)
        else:
            raise AssertionError(f"local webhook URL should be rejected: {url}")

    accepted = smoke._validate_webhook_url(  # noqa: SLF001
        "http://127.0.0.1:8765/api/providers/apify/webhook",
        allow_local=True,
    )
    assert accepted.endswith("/api/providers/apify/webhook")


def test_normalize_apify_webhook_event_extracts_run_and_dataset_ids() -> None:
    event = normalize_remote_provider_event(
        {
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "run-123",
                "actorId": "actor-abc",
                "defaultDatasetId": "dataset-456",
                "finishedAt": "2026-04-27T12:00:01.000Z",
            },
            "createdAt": "2026-04-27T12:00:02.000Z",
        }
    )

    assert event["provider"] == "apify"
    assert event["event_type"] == "ACTOR.RUN.SUCCEEDED"
    assert event["run_id"] == "run-123"
    assert event["dataset_id"] == "dataset-456"
    assert event["status"] == "SUCCEEDED"
    assert event["is_terminal"] is True
    assert event["run_finished_at"] == "2026-04-27T12:00:01.000Z"
    assert event["event_created_at"] == "2026-04-27T12:00:02.000Z"
    assert event["remote_completed_at"] == "2026-04-27T12:00:01.000Z"


def test_remote_provider_event_targets_matching_linkedin_stage_1_worker() -> None:
    event = normalize_remote_provider_event(
        {
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {"actorRunId": "run-123", "defaultDatasetId": "dataset-456"},
        }
    )
    worker = {
        "worker_id": 701,
        "job_id": "job-remote",
        "lane_id": "enrichment_specialist",
        "status": "queued",
        "checkpoint": {
            "stage": "waiting_remote_harvest",
            "run_id": "run-123",
            "dataset_id": "dataset-456",
        },
        "metadata": {"recovery_kind": "harvest_profile_batch"},
    }

    assert remote_provider_event_matches_worker(event, worker)
    targets = collect_remote_provider_event_targets(
        [worker],
        event,
        enabled_lanes={"linkedin_stage_1"},
        lane_resolver=remote_event_lane_for_worker,
    )
    assert targets["job_ids"] == ["job-remote"]
    assert targets["worker_ids"] == [701]
    assert targets["lane_counts"] == {"linkedin_stage_1": 1}


def test_handle_remote_provider_event_triggers_job_scoped_recovery_for_matching_worker(tmp_path: Path) -> None:
    class _FakeStore:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []

        def list_recoverable_agent_workers(self, **kwargs):
            self.list_kwargs = dict(kwargs)
            return [
                {
                    "worker_id": 901,
                    "job_id": "job-linkedin-stage-1",
                    "lane_id": "enrichment_specialist",
                    "status": "queued",
                    "checkpoint": {
                        "stage": "waiting_remote_harvest",
                        "run_id": "run-webhook-1",
                        "dataset_id": "dataset-webhook-1",
                    },
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                }
            ]

        def append_job_event(self, job_id, stage, status, detail, payload=None):
            self.events.append(
                {
                    "job_id": job_id,
                    "stage": stage,
                    "status": status,
                    "detail": detail,
                    "payload": dict(payload or {}),
                }
            )

        def release_agent_worker_lease(self, worker_id, *, lease_owner="", error_text=""):
            self.released = {
                "worker_id": worker_id,
                "lease_owner": lease_owner,
                "error_text": error_text,
            }
            return {"worker_id": worker_id}

    fake_store = _FakeStore()
    dispatch_payloads: list[dict[str, object]] = []

    def _run_worker_recovery_once(payload):
        raise AssertionError("normal remote provider events must not run inline recovery")

    def _ensure_job_scoped_recovery(job_id, payload):
        dispatch_payloads.append({"job_id": job_id, **dict(payload or {})})
        return {"status": "already_running", "scope": "job_scoped", "job_id": job_id}

    fake_orchestrator = SimpleNamespace(
        runtime_dir=tmp_path,
        store=fake_store,
        run_worker_recovery_once=_run_worker_recovery_once,
        ensure_job_scoped_recovery=_ensure_job_scoped_recovery,
    )

    result = SourcingOrchestrator.handle_remote_provider_event(
        fake_orchestrator,
        {
            "provider": "apify",
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "run-webhook-1",
                "defaultDatasetId": "dataset-webhook-1",
            },
            "owner_id": "provider-webhook-test",
        },
    )

    assert result["status"] == "accepted"
    assert result["targets"]["job_ids"] == ["job-linkedin-stage-1"]
    assert result["targets"]["worker_ids"] == [901]
    assert result["recovery_count"] == 0
    assert result["recovery_dispatch_count"] == 1
    assert result["recovery_dispatches"][0]["wakeup"]["service_name"] == "job-recovery-job-linkedin-stage-1"
    assert result["mode"] == "job_scoped_recovery"
    assert result["released_worker_ids"] == [901]
    wakeup = read_service_wakeup_request(tmp_path, "job-recovery-job-linkedin-stage-1")
    assert wakeup["status"] == "requested"
    assert wakeup["reason"] == "remote_provider_event"
    assert wakeup["requested_by"] == "provider-webhook-test"
    assert wakeup["callback_payload"]["explicit_worker_ids"] == [901]
    assert wakeup["callback_payload"]["remote_provider_event_worker_ids"] == [901]
    assert wakeup["callback_payload"]["total_limit"] == 4
    assert wakeup["callback_payload"]["profile_prefetch_refill_before_worker_recovery"] is True
    assert wakeup["callback_payload"]["remote_event_followup_enabled"] is False
    assert wakeup["callback_payload"]["search_seed_discovery_enabled"] is False
    assert fake_store.list_kwargs == {"limit": 500, "stale_after_seconds": 0}
    assert fake_store.events[0]["job_id"] == "job-linkedin-stage-1"
    assert fake_store.events[0]["stage"] == "remote_provider_event"
    event_metrics = fake_store.events[0]["payload"]["event_metrics"]
    assert event_metrics["source"] == "provider_webhook"
    assert event_metrics["local_event_seen_at"]
    assert "remote_to_local_event_lag_ms" in event_metrics
    assert fake_store.events[0]["payload"]["released_worker_ids"] == [901]
    assert dispatch_payloads == [
        {
            "job_id": "job-linkedin-stage-1",
            "source": "remote_provider_event",
            "auto_job_daemon": True,
            "recovery_bootstrap_enabled": False,
            "explicit_worker_ids": [901],
            "force_release_explicit_worker_leases": True,
            "profile_prefetch_nonblocking_submit": True,
            "profile_prefetch_refill_enabled": True,
            "profile_prefetch_refill_before_worker_recovery": True,
            "remote_event_followup_enabled": False,
            "search_seed_discovery_enabled": False,
            "snapshot_full_materialization_enabled": False,
            "excel_intake_recovery_enabled": False,
            "post_recovery_housekeeping_enabled": False,
            "workflow_auto_resume_enabled": True,
            "workflow_queue_auto_takeover_enabled": False,
            "job_recovery_poll_seconds": 0.5,
            "job_recovery_max_ticks": 900,
            "job_recovery_idle_stop_ticks": 3,
            "job_recovery_stale_after_seconds": 0,
            "job_recovery_total_limit": 4,
            "workflow_queue_resume_stale_after_seconds": 0,
            "workflow_stale_scope_job_id": "job-linkedin-stage-1",
            "remote_provider_event_worker_ids": [901],
            "remote_provider_event_owner_id": "provider-webhook-test",
        }
    ]


def test_handle_remote_provider_event_wakes_known_running_worker_without_stale_wait(tmp_path: Path) -> None:
    class _FakeStore:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []

        def list_recoverable_agent_workers(self, **kwargs):
            self.recoverable_kwargs = dict(kwargs)
            return []

        def list_agent_workers_by_remote_provider_identifiers(self, **kwargs):
            self.known_kwargs = dict(kwargs)
            return [
                {
                    "worker_id": 904,
                    "job_id": "job-linkedin-stage-1",
                    "lane_id": "enrichment_specialist",
                    "status": "running",
                    "checkpoint": {
                        "stage": "waiting_remote_harvest",
                        "run_id": "run-webhook-running",
                        "dataset_id": "dataset-webhook-running",
                    },
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                }
            ]

        def append_job_event(self, job_id, stage, status, detail, payload=None):
            self.events.append(
                {
                    "job_id": job_id,
                    "stage": stage,
                    "status": status,
                    "detail": detail,
                    "payload": dict(payload or {}),
                }
            )

        def release_agent_worker_lease(self, worker_id, *, lease_owner="", error_text=""):
            self.released = {
                "worker_id": worker_id,
                "lease_owner": lease_owner,
                "error_text": error_text,
            }
            return {"worker_id": worker_id}

    fake_store = _FakeStore()
    dispatch_payloads: list[dict[str, object]] = []

    def _run_worker_recovery_once(payload):
        raise AssertionError("normal remote provider events must not run inline recovery")

    def _ensure_job_scoped_recovery(job_id, payload):
        dispatch_payloads.append({"job_id": job_id, **dict(payload or {})})
        return {"status": "already_running", "scope": "job_scoped", "job_id": job_id}

    fake_orchestrator = SimpleNamespace(
        runtime_dir=tmp_path,
        store=fake_store,
        run_worker_recovery_once=_run_worker_recovery_once,
        ensure_job_scoped_recovery=_ensure_job_scoped_recovery,
    )

    result = SourcingOrchestrator.handle_remote_provider_event(
        fake_orchestrator,
        {
            "provider": "apify",
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "run-webhook-running",
                "defaultDatasetId": "dataset-webhook-running",
            },
            "owner_id": "provider-webhook-running-test",
        },
    )

    assert result["status"] == "accepted"
    assert result["targets"]["worker_ids"] == [904]
    assert result["targets"]["status_counts"] == {"running": 1}
    assert result["targets"]["source"] == "known_remote_provider_workers"
    assert result["recovery_count"] == 0
    assert result["recovery_dispatch_count"] == 1
    assert result["released_worker_ids"] == [904]
    assert dispatch_payloads[0]["remote_provider_event_worker_ids"] == [904]
    assert dispatch_payloads[0]["job_recovery_stale_after_seconds"] == 0
    assert dispatch_payloads[0]["profile_prefetch_refill_enabled"] is True
    assert dispatch_payloads[0]["profile_prefetch_refill_before_worker_recovery"] is True
    assert dispatch_payloads[0]["remote_event_followup_enabled"] is False
    assert dispatch_payloads[0]["search_seed_discovery_enabled"] is False
    assert dispatch_payloads[0]["snapshot_full_materialization_enabled"] is False
    assert dispatch_payloads[0]["excel_intake_recovery_enabled"] is False
    assert dispatch_payloads[0]["post_recovery_housekeeping_enabled"] is False
    assert dispatch_payloads[0]["workflow_auto_resume_enabled"] is True
    assert dispatch_payloads[0]["workflow_queue_auto_takeover_enabled"] is False
    assert fake_store.events[0]["status"] == "received"
    assert fake_store.events[0]["payload"]["target_source"] == "known_remote_provider_workers"


def test_handle_remote_provider_event_wakes_active_remote_wait_lease_once(tmp_path: Path) -> None:
    class _FakeStore:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []
            self.checkpoints: list[dict[str, object]] = []
            self.worker = {
                "worker_id": 906,
                "job_id": "job-linkedin-stage-1",
                "lane_id": "enrichment_specialist",
                "status": "running",
                "lease_owner": "scripted-browser-webhook-run-in-flight-1",
                "lease_expires_at": "2099-01-01 00:00:00",
                "checkpoint": {
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-webhook-in-flight",
                    "dataset_id": "dataset-webhook-in-flight",
                },
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "output": {},
            }

        def list_recoverable_agent_workers(self, **kwargs):
            self.recoverable_kwargs = dict(kwargs)
            return []

        def list_agent_workers_by_remote_provider_identifiers(self, **kwargs):
            self.known_kwargs = dict(kwargs)
            return [dict(self.worker)]

        def get_agent_worker(self, *, worker_id):
            assert worker_id == 906
            return dict(self.worker)

        def checkpoint_agent_worker(self, worker_id, *, checkpoint_payload, output_payload, status):
            self.checkpoints.append(
                {
                    "worker_id": worker_id,
                    "checkpoint": dict(checkpoint_payload or {}),
                    "output": dict(output_payload or {}),
                    "status": status,
                }
            )
            self.worker["checkpoint"] = dict(checkpoint_payload or {})
            self.worker["output"] = dict(output_payload or {})
            self.worker["status"] = status
            return dict(self.worker)

        def append_job_event(self, job_id, stage, status, detail, payload=None):
            self.events.append(
                {
                    "job_id": job_id,
                    "stage": stage,
                    "status": status,
                    "detail": detail,
                    "payload": dict(payload or {}),
                }
            )

        def release_agent_worker_lease(self, worker_id, *, lease_owner="", error_text=""):
            self.released = {
                "worker_id": worker_id,
                "lease_owner": lease_owner,
                "error_text": error_text,
            }
            return {"worker_id": worker_id}

    fake_store = _FakeStore()
    dispatch_payloads: list[dict[str, object]] = []

    def _run_worker_recovery_once(payload):
        raise AssertionError("normal remote provider events must not run inline recovery")

    def _ensure_job_scoped_recovery(job_id, payload):
        dispatch_payloads.append({"job_id": job_id, **dict(payload or {})})
        return {"status": "already_running", "scope": "job_scoped", "job_id": job_id}

    fake_orchestrator = SimpleNamespace(
        runtime_dir=tmp_path,
        store=fake_store,
        run_worker_recovery_once=_run_worker_recovery_once,
        ensure_job_scoped_recovery=_ensure_job_scoped_recovery,
    )

    result = SourcingOrchestrator.handle_remote_provider_event(
        fake_orchestrator,
        {
            "provider": "apify",
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "run-webhook-in-flight",
                "defaultDatasetId": "dataset-webhook-in-flight",
            },
            "owner_id": "provider-webhook-duplicate-test",
        },
    )

    assert result["status"] == "accepted"
    assert result["recovery_count"] == 0
    assert result["recovery_dispatch_count"] == 1
    assert result["targets"]["worker_ids"] == [906]
    assert result["targets"]["source"] == "known_remote_provider_workers"
    assert result["released_worker_ids"] == [906]
    assert dispatch_payloads[0]["remote_provider_event_worker_ids"] == [906]
    assert fake_store.events[0]["stage"] == "remote_provider_event"
    assert fake_store.events[0]["status"] == "received"
    assert fake_store.checkpoints[0]["worker_id"] == 906
    assert fake_store.checkpoints[0]["checkpoint"]["remote_provider_terminal_event"]["run_id"] == "run-webhook-in-flight"
    assert fake_store.checkpoints[0]["checkpoint"]["remote_provider_terminal_event_metrics"]["source"] == "provider_webhook"


def test_handle_remote_provider_event_does_not_rewake_checkpointed_terminal_event() -> None:
    class _FakeStore:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []
            self.checkpoints: list[dict[str, object]] = []
            self.worker = {
                "worker_id": 906,
                "job_id": "job-linkedin-stage-1",
                "lane_id": "enrichment_specialist",
                "status": "running",
                "lease_owner": "scripted-browser-webhook-run-in-flight-1",
                "lease_expires_at": "2099-01-01 00:00:00",
                "checkpoint": {
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-webhook-in-flight",
                    "dataset_id": "dataset-webhook-in-flight",
                    "provider_limiter_lease": {
                        "lease_token": "provider-lease-906-in-flight",
                        "limiter_key": "harvest_profile_scraper_actor",
                        "lease_owner": "harvest_profile_batch:job-linkedin-stage-1:payload",
                    },
                    "remote_provider_terminal_event": {
                        "run_id": "run-webhook-in-flight",
                        "dataset_id": "dataset-webhook-in-flight",
                        "is_terminal": True,
                    },
                },
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "output": {},
            }
            self.provider_limiter_releases: list[dict[str, object]] = []

        def list_recoverable_agent_workers(self, **kwargs):
            self.recoverable_kwargs = dict(kwargs)
            return []

        def list_agent_workers_by_remote_provider_identifiers(self, **kwargs):
            self.known_kwargs = dict(kwargs)
            return [dict(self.worker)]

        def get_agent_worker(self, *, worker_id):
            assert worker_id == 906
            return dict(self.worker)

        def checkpoint_agent_worker(self, worker_id, *, checkpoint_payload, output_payload, status):
            self.checkpoints.append(
                {
                    "worker_id": worker_id,
                    "checkpoint": dict(checkpoint_payload or {}),
                    "output": dict(output_payload or {}),
                    "status": status,
                }
            )
            self.worker["checkpoint"] = dict(checkpoint_payload or {})
            self.worker["output"] = dict(output_payload or {})
            self.worker["status"] = status
            return dict(self.worker)

        def append_job_event(self, job_id, stage, status, detail, payload=None):
            self.events.append(
                {
                    "job_id": job_id,
                    "stage": stage,
                    "status": status,
                    "detail": detail,
                    "payload": dict(payload or {}),
                }
            )

        def release_runtime_provider_limiter_slot(self, lease_token, *, limiter_key="", lease_owner=""):
            self.provider_limiter_releases.append(
                {
                    "lease_token": lease_token,
                    "limiter_key": limiter_key,
                    "lease_owner": lease_owner,
                }
            )
            return True

    fake_store = _FakeStore()

    def _run_worker_recovery_once(payload):
        raise AssertionError("checkpointed duplicate provider events must not rerun recovery")

    fake_orchestrator = SimpleNamespace(store=fake_store, run_worker_recovery_once=_run_worker_recovery_once)

    result = SourcingOrchestrator.handle_remote_provider_event(
        fake_orchestrator,
        {
            "provider": "apify",
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "run-webhook-in-flight",
                "defaultDatasetId": "dataset-webhook-in-flight",
            },
            "owner_id": "provider-webhook-duplicate-test",
        },
    )

    assert result["status"] == "accepted"
    assert result["reason"] == "remote_provider_event_recovery_already_in_flight"
    assert result["recovery_count"] == 0
    assert result["targets"]["worker_ids"] == [906]
    assert result["targets"]["source"] == "known_remote_provider_workers_in_flight"
    assert result["released_provider_limiter_worker_ids"] == [906]
    assert fake_store.events[0]["stage"] == "remote_provider_event"
    assert fake_store.events[0]["status"] == "received_in_flight"
    assert fake_store.events[0]["payload"]["released_provider_limiter_worker_ids"] == [906]
    assert fake_store.provider_limiter_releases == [
        {
            "lease_token": "provider-lease-906-in-flight",
            "limiter_key": "harvest_profile_scraper_actor",
            "lease_owner": "harvest_profile_batch:job-linkedin-stage-1:payload",
        }
    ]


def test_handle_remote_provider_event_dedupes_recoverable_worker_with_terminal_marker() -> None:
    class _FakeStore:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []
            self.worker = {
                "worker_id": 906,
                "job_id": "job-linkedin-stage-1",
                "lane_id": "enrichment_specialist",
                "status": "running",
                "checkpoint": {
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-webhook-in-flight",
                    "dataset_id": "dataset-webhook-in-flight",
                    "remote_provider_terminal_event": {
                        "run_id": "run-webhook-in-flight",
                        "dataset_id": "dataset-webhook-in-flight",
                        "is_terminal": True,
                    },
                    "remote_provider_terminal_event_seen_at": "2026-05-11T14:48:17+00:00",
                },
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "output": {},
            }

        def list_recoverable_agent_workers(self, **kwargs):
            self.recoverable_kwargs = dict(kwargs)
            return [dict(self.worker)]

        def get_agent_worker(self, *, worker_id):
            assert worker_id == 906
            return dict(self.worker)

        def append_job_event(self, job_id, stage, status, detail, payload=None):
            self.events.append(
                {
                    "job_id": job_id,
                    "stage": stage,
                    "status": status,
                    "detail": detail,
                    "payload": dict(payload or {}),
                }
            )

    fake_store = _FakeStore()

    def _run_worker_recovery_once(payload):
        raise AssertionError("duplicate terminal events must not rerun recovery")

    fake_orchestrator = SimpleNamespace(store=fake_store, run_worker_recovery_once=_run_worker_recovery_once)

    result = SourcingOrchestrator.handle_remote_provider_event(
        fake_orchestrator,
        {
            "provider": "apify",
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "run-webhook-in-flight",
                "defaultDatasetId": "dataset-webhook-in-flight",
                "finishedAt": "2026-05-11T14:48:17.342000+00:00",
            },
            "owner_id": "provider-webhook-duplicate-test",
        },
    )

    assert result["status"] == "accepted"
    assert result["reason"] == "remote_provider_terminal_event_already_recorded"
    assert result["recovery_count"] == 0
    assert result["targets"]["worker_ids"] == [906]
    assert result["targets"]["source"] == "recoverable_workers_with_terminal_event_marker"
    assert fake_store.events[0]["stage"] == "remote_provider_event"
    assert fake_store.events[0]["status"] == "received_in_flight"


def test_handle_remote_provider_event_marks_terminal_checkpoint_before_recovery(tmp_path: Path) -> None:
    class _FakeStore:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []
            self.worker = {
                "worker_id": 907,
                "job_id": "job-linkedin-stage-1",
                "lane_id": "enrichment_specialist",
                "status": "queued",
                "checkpoint": {
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-webhook-terminal",
                    "dataset_id": "dataset-webhook-terminal",
                    "provider_limiter_lease": {
                        "lease_token": "provider-lease-907",
                        "limiter_key": "harvest_profile_scraper_actor",
                        "lease_owner": "harvest_profile_batch:job-linkedin-stage-1:payload",
                    },
                },
                "output": {"summary": {"status": "queued"}},
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            }
            self.checkpoints: list[dict[str, object]] = []
            self.provider_limiter_releases: list[dict[str, object]] = []

        def list_recoverable_agent_workers(self, **kwargs):
            self.list_kwargs = dict(kwargs)
            return [dict(self.worker)]

        def get_agent_worker(self, *, worker_id=None, **kwargs):
            if int(worker_id or 0) == 907:
                return dict(self.worker)
            return None

        def checkpoint_agent_worker(self, worker_id, *, checkpoint_payload=None, output_payload=None, status="running"):
            self.checkpoints.append(
                {
                    "worker_id": worker_id,
                    "checkpoint": dict(checkpoint_payload or {}),
                    "output": dict(output_payload or {}),
                    "status": status,
                }
            )
            self.worker["checkpoint"] = dict(checkpoint_payload or {})
            self.worker["output"] = dict(output_payload or {})
            self.worker["status"] = status
            return dict(self.worker)

        def append_job_event(self, job_id, stage, status, detail, payload=None):
            self.events.append(
                {
                    "job_id": job_id,
                    "stage": stage,
                    "status": status,
                    "detail": detail,
                    "payload": dict(payload or {}),
                }
            )

        def release_agent_worker_lease(self, worker_id, *, lease_owner="", error_text=""):
            self.released = {
                "worker_id": worker_id,
                "lease_owner": lease_owner,
                "error_text": error_text,
            }
            return {"worker_id": worker_id}

        def release_runtime_provider_limiter_slot(self, lease_token, *, limiter_key="", lease_owner=""):
            self.provider_limiter_releases.append(
                {
                    "lease_token": lease_token,
                    "limiter_key": limiter_key,
                    "lease_owner": lease_owner,
                }
            )
            return True

    fake_store = _FakeStore()
    dispatch_payloads: list[dict[str, object]] = []

    def _run_worker_recovery_once(payload):
        raise AssertionError("normal remote provider events must not run inline recovery")

    def _ensure_job_scoped_recovery(job_id, payload):
        dispatch_payloads.append({"job_id": job_id, **dict(payload or {})})
        return {"status": "already_running", "scope": "job_scoped", "job_id": job_id}

    fake_orchestrator = SimpleNamespace(
        runtime_dir=tmp_path,
        store=fake_store,
        run_worker_recovery_once=_run_worker_recovery_once,
        ensure_job_scoped_recovery=_ensure_job_scoped_recovery,
    )

    result = SourcingOrchestrator.handle_remote_provider_event(
        fake_orchestrator,
        {
            "provider": "apify",
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "run-webhook-terminal",
                "defaultDatasetId": "dataset-webhook-terminal",
            },
            "owner_id": "provider-webhook-terminal-test",
        },
    )

    assert result["status"] == "accepted"
    assert result["recovery_count"] == 0
    assert result["recovery_dispatch_count"] == 1
    assert result["released_worker_ids"] == [907]
    assert result["released_provider_limiter_worker_ids"] == [907]
    assert dispatch_payloads[0]["remote_provider_event_worker_ids"] == [907]
    assert dispatch_payloads[0]["explicit_worker_ids"] == [907]
    assert dispatch_payloads[0]["force_release_explicit_worker_leases"] is True
    assert dispatch_payloads[0]["profile_prefetch_nonblocking_submit"] is True
    assert dispatch_payloads[0]["job_recovery_max_ticks"] == 900
    assert dispatch_payloads[0]["job_recovery_idle_stop_ticks"] == 3
    assert fake_store.checkpoints
    checkpoint = fake_store.checkpoints[0]["checkpoint"]
    assert checkpoint["force_scripted_terminal_fetch"] is True
    assert checkpoint["remote_provider_terminal_event"]["event_type"] == "ACTOR.RUN.SUCCEEDED"
    assert checkpoint["remote_provider_terminal_event_metrics"]["source"] == "provider_webhook"
    assert fake_store.provider_limiter_releases == [
        {
            "lease_token": "provider-lease-907",
            "limiter_key": "harvest_profile_scraper_actor",
            "lease_owner": "harvest_profile_batch:job-linkedin-stage-1:payload",
        }
    ]
    release_checkpoint = fake_store.checkpoints[-1]["checkpoint"]
    assert release_checkpoint["provider_limiter_terminal_release"]["released"] is True
    assert release_checkpoint["provider_limiter_terminal_release"]["reason"] == "remote_provider_terminal_event"
    assert release_checkpoint["provider_limiter_lease"]["lease_token"] == "provider-lease-907"
    assert fake_store.events[0]["payload"]["released_provider_limiter_worker_ids"] == [907]


def test_handle_remote_provider_event_records_late_event_for_completed_worker_without_recovery() -> None:
    class _FakeStore:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []

        def list_recoverable_agent_workers(self, **kwargs):
            self.recoverable_kwargs = dict(kwargs)
            return []

        def list_agent_workers_by_remote_provider_identifiers(self, **kwargs):
            self.known_kwargs = dict(kwargs)
            return [
                {
                    "worker_id": 903,
                    "job_id": "job-linkedin-stage-1",
                    "lane_id": "enrichment_specialist",
                    "status": "completed",
                    "checkpoint": {
                        "stage": "completed",
                        "run_id": "run-webhook-late",
                        "dataset_id": "dataset-webhook-late",
                    },
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                }
            ]

        def append_job_event(self, job_id, stage, status, detail, payload=None):
            self.events.append(
                {
                    "job_id": job_id,
                    "stage": stage,
                    "status": status,
                    "detail": detail,
                    "payload": dict(payload or {}),
                }
            )

    fake_store = _FakeStore()

    def _run_worker_recovery_once(payload):
        raise AssertionError("late provider events should not start recovery for completed workers")

    fake_orchestrator = SimpleNamespace(store=fake_store, run_worker_recovery_once=_run_worker_recovery_once)

    result = SourcingOrchestrator.handle_remote_provider_event(
        fake_orchestrator,
        {
            "provider": "apify",
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "run-webhook-late",
                "defaultDatasetId": "dataset-webhook-late",
            },
            "owner_id": "provider-webhook-late-test",
        },
    )

    assert result["status"] == "accepted"
    assert result["reason"] == "matching_remote_provider_workers_not_recoverable"
    assert result["recovery_count"] == 0
    assert result["targets"]["worker_ids"] == [903]
    assert result["targets"]["status_counts"] == {"completed": 1}
    assert fake_store.recoverable_kwargs == {"limit": 500, "stale_after_seconds": 0}
    assert fake_store.known_kwargs == {
        "run_id": "run-webhook-late",
        "dataset_id": "dataset-webhook-late",
        "limit": 500,
    }
    assert fake_store.events[0]["stage"] == "remote_provider_event"
    assert fake_store.events[0]["status"] == "received_late"
    assert fake_store.events[0]["payload"]["target_worker_ids"] == [903]
    assert fake_store.events[0]["payload"]["event_metrics"]["source"] == "provider_webhook"


def test_handle_remote_provider_event_records_actor_run_duration_metric() -> None:
    class _FakeStore:
        def list_recoverable_agent_workers(self, **kwargs):
            return []

        def list_agent_workers_by_remote_provider_identifiers(self, **kwargs):
            return []

    fake_orchestrator = SimpleNamespace(store=_FakeStore(), run_worker_recovery_once=lambda payload: {})

    result = SourcingOrchestrator.handle_remote_provider_event(
        fake_orchestrator,
        {
            "provider": "apify",
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "run-duration",
                "defaultDatasetId": "dataset-duration",
                "startedAt": "2026-05-04T10:00:00.000Z",
                "finishedAt": "2026-05-04T10:00:22.500Z",
            },
        },
    )

    assert result["status"] == "accepted"
    assert result["event_metrics"]["actor_run_duration_ms"] == 22500.0


def test_handle_remote_provider_event_records_late_watcher_event_after_webhook_completion_without_recovery() -> None:
    class _FakeStore:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []

        def list_recoverable_agent_workers(self, **kwargs):
            self.recoverable_kwargs = dict(kwargs)
            return []

        def list_agent_workers_by_remote_provider_identifiers(self, **kwargs):
            self.known_kwargs = dict(kwargs)
            return [
                {
                    "worker_id": 905,
                    "job_id": "job-linkedin-stage-1",
                    "lane_id": "enrichment_specialist",
                    "status": "completed",
                    "checkpoint": {
                        "stage": "completed",
                        "run_id": "run-watcher-late",
                        "dataset_id": "dataset-watcher-late",
                    },
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                }
            ]

        def append_job_event(self, job_id, stage, status, detail, payload=None):
            self.events.append(
                {
                    "job_id": job_id,
                    "stage": stage,
                    "status": status,
                    "detail": detail,
                    "payload": dict(payload or {}),
                }
            )

    fake_store = _FakeStore()

    def _run_worker_recovery_once(payload):
        raise AssertionError("late watcher events should not start recovery for completed workers")

    fake_orchestrator = SimpleNamespace(store=fake_store, run_worker_recovery_once=_run_worker_recovery_once)

    result = SourcingOrchestrator.handle_remote_provider_event(
        fake_orchestrator,
        {
            "provider": "apify",
            "source": "local_provider_event_watcher",
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "run-watcher-late",
                "defaultDatasetId": "dataset-watcher-late",
            },
            "owner_id": "provider-watcher-late-test",
        },
    )

    assert result["status"] == "accepted"
    assert result["reason"] == "matching_remote_provider_workers_not_recoverable"
    assert result["recovery_count"] == 0
    assert result["targets"]["worker_ids"] == [905]
    assert result["targets"]["status_counts"] == {"completed": 1}
    assert fake_store.recoverable_kwargs == {"limit": 500, "stale_after_seconds": 0}
    assert fake_store.known_kwargs == {
        "run_id": "run-watcher-late",
        "dataset_id": "dataset-watcher-late",
        "limit": 500,
    }
    assert fake_store.events[0]["stage"] == "remote_provider_event"
    assert fake_store.events[0]["status"] == "received_late"
    assert fake_store.events[0]["payload"]["target_worker_ids"] == [905]
    assert fake_store.events[0]["payload"]["event_metrics"]["source"] == "local_provider_event_watcher"


def test_handle_remote_provider_event_late_duplicate_does_not_reopen_durable_queues(tmp_path: Path) -> None:
    class _FakeStore:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []
            self.worker = {
                "worker_id": 906,
                "job_id": "job-linkedin-stage-1",
                "lane_id": "enrichment_specialist",
                "status": "queued",
                "checkpoint": {
                    "stage": "waiting_remote_harvest",
                    "run_id": "run-duplicate-order",
                    "dataset_id": "dataset-duplicate-order",
                },
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            }

        def list_recoverable_agent_workers(self, **kwargs):
            self.recoverable_kwargs = dict(kwargs)
            if str(self.worker.get("status") or "") == "completed":
                return []
            return [dict(self.worker)]

        def list_agent_workers_by_remote_provider_identifiers(self, **kwargs):
            self.known_kwargs = dict(kwargs)
            return [dict(self.worker)]

        def append_job_event(self, job_id, stage, status, detail, payload=None):
            self.events.append(
                {
                    "job_id": job_id,
                    "stage": stage,
                    "status": status,
                    "detail": detail,
                    "payload": dict(payload or {}),
                }
            )

        def upsert_job_materialization_item(self, *args, **kwargs):
            raise AssertionError("late provider duplicates must not create materialization items")

        def release_agent_worker_lease(self, worker_id, *, lease_owner="", error_text=""):
            self.released = {
                "worker_id": worker_id,
                "lease_owner": lease_owner,
                "error_text": error_text,
            }
            return {"worker_id": worker_id}

    fake_store = _FakeStore()
    dispatch_payloads: list[dict[str, object]] = []

    def _run_worker_recovery_once(payload):
        raise AssertionError("normal remote provider events must not run inline recovery")

    def _ensure_job_scoped_recovery(job_id, payload):
        dispatch_payloads.append({"job_id": job_id, **dict(payload or {})})
        return {"status": "already_running", "scope": "job_scoped", "job_id": job_id}

    fake_orchestrator = SimpleNamespace(
        runtime_dir=tmp_path,
        store=fake_store,
        run_worker_recovery_once=_run_worker_recovery_once,
        ensure_job_scoped_recovery=_ensure_job_scoped_recovery,
    )

    webhook_result = SourcingOrchestrator.handle_remote_provider_event(
        fake_orchestrator,
        {
            "provider": "apify",
            "source": "provider_webhook",
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "run-duplicate-order",
                "defaultDatasetId": "dataset-duplicate-order",
            },
            "owner_id": "provider-webhook-first",
        },
    )
    fake_store.worker["status"] = "completed"
    fake_store.worker["checkpoint"] = {
        **dict(fake_store.worker.get("checkpoint") or {}),
        "stage": "completed",
    }
    watcher_result = SourcingOrchestrator.handle_remote_provider_event(
        fake_orchestrator,
        {
            "provider": "apify",
            "source": "local_provider_event_watcher",
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "run-duplicate-order",
                "defaultDatasetId": "dataset-duplicate-order",
            },
            "owner_id": "provider-watcher-late",
        },
    )

    assert webhook_result["status"] == "accepted"
    assert webhook_result["recovery_count"] == 0
    assert webhook_result["recovery_dispatch_count"] == 1
    assert watcher_result["status"] == "accepted"
    assert watcher_result["reason"] == "matching_remote_provider_workers_not_recoverable"
    assert watcher_result["recovery_count"] == 0
    assert len(dispatch_payloads) == 1
    assert dispatch_payloads[0]["remote_provider_event_worker_ids"] == [906]
    assert [event["status"] for event in fake_store.events] == ["received", "received_late"]
    assert fake_store.events[1]["payload"]["event_metrics"]["source"] == "local_provider_event_watcher"


def test_failed_apify_event_wakes_matching_worker_without_inline_materialize(tmp_path: Path) -> None:
    class _FakeStore:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []

        def list_recoverable_agent_workers(self, **kwargs):
            self.list_kwargs = dict(kwargs)
            return [
                {
                    "worker_id": 902,
                    "job_id": "job-linkedin-stage-1",
                    "lane_id": "enrichment_specialist",
                    "status": "queued",
                    "checkpoint": {
                        "stage": "waiting_remote_harvest",
                        "run_id": "run-webhook-failed",
                    },
                    "metadata": {"recovery_kind": "harvest_profile_batch"},
                }
            ]

        def append_job_event(self, job_id, stage, status, detail, payload=None):
            self.events.append(
                {
                    "job_id": job_id,
                    "stage": stage,
                    "status": status,
                    "detail": detail,
                    "payload": dict(payload or {}),
                }
            )

        def release_agent_worker_lease(self, worker_id, *, lease_owner="", error_text=""):
            self.released = {
                "worker_id": worker_id,
                "lease_owner": lease_owner,
                "error_text": error_text,
            }
            return {"worker_id": worker_id}

    dispatch_payloads: list[dict[str, object]] = []

    def _run_worker_recovery_once(payload):
        raise AssertionError("normal remote provider events must not run inline recovery")

    def _ensure_job_scoped_recovery(job_id, payload):
        dispatch_payloads.append({"job_id": job_id, **dict(payload or {})})
        return {"status": "already_running", "scope": "job_scoped", "job_id": job_id}

    fake_store = _FakeStore()
    fake_orchestrator = SimpleNamespace(
        runtime_dir=tmp_path,
        store=fake_store,
        run_worker_recovery_once=_run_worker_recovery_once,
        ensure_job_scoped_recovery=_ensure_job_scoped_recovery,
    )

    result = SourcingOrchestrator.handle_remote_provider_event(
        fake_orchestrator,
        {
            "provider": "apify",
            "eventType": "ACTOR.RUN.FAILED",
            "eventData": {
                "actorRunId": "run-webhook-failed",
                "statusMessage": "provider queue backpressure",
            },
            "owner_id": "provider-webhook-failed-test",
        },
    )

    assert result["status"] == "accepted"
    assert result["event"]["status"] == "FAILED"
    assert result["targets"]["worker_ids"] == [902]
    assert result["recovery_count"] == 0
    assert result["recovery_dispatch_count"] == 1
    assert dispatch_payloads[0]["remote_provider_event_worker_ids"] == [902]
    assert fake_store.events[0]["payload"]["event"]["event_type"] == "ACTOR.RUN.FAILED"


def test_apify_webhook_endpoint_requires_token_and_dispatches_valid_event() -> None:
    class _FakeOrchestrator:
        def __init__(self) -> None:
            self.payloads: list[dict[str, object]] = []

        def handle_remote_provider_event(self, payload):
            self.payloads.append(dict(payload or {}))
            return {"status": "accepted", "event": {"run_id": "run-api-1"}}

    orchestrator = _FakeOrchestrator()
    body = json.dumps(
        {
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {"actorRunId": "run-api-1", "defaultDatasetId": "dataset-api-1"},
        }
    ).encode("utf-8")

    with mock.patch.dict("os.environ", {"SOURCING_PROVIDER_WEBHOOK_TOKEN": "secret-token"}, clear=False):
        server = create_server(orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        try:
            forbidden_req = urllib_request.Request(
                f"http://{host}:{port}/api/providers/apify/webhook",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                urllib_request.urlopen(forbidden_req, timeout=5)
                raise AssertionError("unsigned provider webhook should be rejected")
            except urllib_error.HTTPError as exc:
                forbidden_body = json.loads(exc.read().decode("utf-8"))
                assert exc.code == 403
                assert forbidden_body["reason"] == "provider_webhook_token_required"

            signed_req = urllib_request.Request(
                f"http://{host}:{port}/api/providers/apify/webhook?token=secret-token",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib_request.urlopen(signed_req, timeout=5) as response:
                accepted_body = json.loads(response.read().decode("utf-8"))
                accepted_status = response.status

            deadline = time.time() + 2
            while not orchestrator.payloads and time.time() < deadline:
                time.sleep(0.01)

            assert accepted_status == 202
            assert accepted_body["status"] == "accepted"
            assert accepted_body["mode"] == "job_scoped_recovery"
            assert orchestrator.payloads[0]["provider"] == "apify"
            assert orchestrator.payloads[0]["eventType"] == "ACTOR.RUN.SUCCEEDED"
            assert orchestrator.payloads[0]["recovery_mode"] == "job_scoped_recovery"
        finally:
            server.shutdown()
            server.server_close()


def test_apify_webhook_preflight_uses_default_local_live_submit_contract_without_env_url() -> None:
    preflight = _load_apify_webhook_preflight_module()
    args = SimpleNamespace(
        mode="local-live",
        webhook_url="",
        provider_webhook_token="",
        skip_connectivity_probe=True,
        allow_local_webhook_url=False,
    )

    with mock.patch.dict(
        "os.environ",
        {
            "SOURCING_APIFY_WEBHOOK_URL": "",
            "APIFY_WEBHOOK_URL": "",
            "SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED": "",
        },
        clear=False,
    ):
        report = preflight.build_preflight_report(args, orchestrator=_fake_preflight_orchestrator())

    assert report["status"] == "ready"
    assert report["webhook_url"] == "https://api.111874.xyz/local-dev/providers/apify/webhook"
    assert report["webhook_url_source"] == "default_submit_contract"
    assert report["submit_contract"]["would_attach_webhooks"] is True


def test_apify_webhook_preflight_fails_live_mode_when_default_url_is_disabled() -> None:
    preflight = _load_apify_webhook_preflight_module()
    args = SimpleNamespace(
        mode="local-live",
        webhook_url="",
        provider_webhook_token="",
        skip_connectivity_probe=True,
        allow_local_webhook_url=False,
    )

    with mock.patch.dict(
        "os.environ",
        {
            "SOURCING_APIFY_WEBHOOK_URL": "",
            "APIFY_WEBHOOK_URL": "",
            "SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED": "0",
        },
        clear=False,
    ):
        report = preflight.build_preflight_report(args, orchestrator=_fake_preflight_orchestrator())

    assert report["status"] == "failed"
    assert "Harvest submit contract would not attach Apify ad-hoc webhooks." in report["failures"]
    assert "No explicit or default Apify webhook URL is available for this runtime mode." in report["failures"]


def test_apify_webhook_preflight_local_live_checks_submit_contract_and_probe() -> None:
    preflight = _load_apify_webhook_preflight_module()
    args = SimpleNamespace(
        mode="local-live",
        webhook_url="",
        provider_webhook_token="",
        skip_connectivity_probe=False,
        allow_local_webhook_url=False,
    )
    probes: list[dict[str, str]] = []

    def _fake_probe(webhook_url: str, *, token: str):
        probes.append({"webhook_url": webhook_url, "token": token})
        return {"http_status": 202, "body": {"status": "accepted"}}

    with mock.patch.dict(
        "os.environ",
        {
            "SOURCING_APIFY_WEBHOOK_URL": "https://api.111874.xyz/local-dev/providers/apify/webhook",
            "SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN": "1",
        },
        clear=False,
    ):
        report = preflight.build_preflight_report(
            args,
            orchestrator=_fake_preflight_orchestrator(api_token="actor-api-token"),
            probe_func=_fake_probe,
        )

    assert report["status"] == "ready"
    assert report["submit_contract"]["would_attach_webhooks"] is True
    assert report["submit_contract"]["webhook_definition_count"] == 1
    assert report["submit_contract"]["webhooks"][0]["requestUrl"].endswith("/local-dev/providers/apify/webhook")
    assert report["submit_contract"]["webhooks"][0]["headersTemplate"]["X-Sourcing-Provider-Webhook-Token"] == "<redacted>"
    assert probes == [
        {
            "webhook_url": "https://api.111874.xyz/local-dev/providers/apify/webhook",
            "token": "actor-api-token",
        }
    ]


def test_apify_webhook_preflight_hosted_rejects_local_dev_relay() -> None:
    preflight = _load_apify_webhook_preflight_module()
    args = SimpleNamespace(
        mode="hosted",
        webhook_url="",
        provider_webhook_token="",
        skip_connectivity_probe=True,
        allow_local_webhook_url=False,
    )

    with mock.patch.dict(
        "os.environ",
        {"SOURCING_APIFY_WEBHOOK_URL": "https://api.111874.xyz/local-dev/providers/apify/webhook"},
        clear=False,
    ):
        report = preflight.build_preflight_report(args, orchestrator=_fake_preflight_orchestrator())

    assert report["status"] == "failed"
    assert "hosted mode must use /api/providers/apify/webhook, not the local-dev relay path." in report["failures"]


def test_apify_webhook_preflight_scripted_mode_does_not_require_public_callback_url() -> None:
    preflight = _load_apify_webhook_preflight_module()
    args = SimpleNamespace(
        mode="scripted",
        webhook_url="",
        provider_webhook_token="",
        skip_connectivity_probe=False,
        allow_local_webhook_url=False,
    )

    with mock.patch.dict("os.environ", {"SOURCING_APIFY_WEBHOOK_URL": "", "APIFY_WEBHOOK_URL": ""}, clear=False):
        report = preflight.build_preflight_report(args, orchestrator=_fake_preflight_orchestrator(enabled=False, api_token=""))

    assert report["status"] == "ready"
    assert report["scripted_contract"]["external_apify_webhook_required"] is False


def test_apify_webhook_endpoint_sync_mode_returns_recovery_result() -> None:
    class _FakeOrchestrator:
        def __init__(self) -> None:
            self.payloads: list[dict[str, object]] = []

        def handle_remote_provider_event(self, payload):
            self.payloads.append(dict(payload or {}))
            return {
                "status": "accepted",
                "event": {"run_id": "run-sync-1"},
                "targets": {"job_ids": ["job-sync"], "worker_ids": [1001]},
                "recovery_count": 1,
                "recoveries": [{"job_id": "job-sync", "daemon": {"executed_count": 1}}],
            }

    orchestrator = _FakeOrchestrator()
    body = json.dumps(
        {
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {"actorRunId": "run-sync-1", "defaultDatasetId": "dataset-sync-1"},
        }
    ).encode("utf-8")

    with mock.patch.dict("os.environ", {"SOURCING_PROVIDER_WEBHOOK_TOKEN": "secret-token"}, clear=False):
        server = create_server(orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        try:
            signed_req = urllib_request.Request(
                f"http://{host}:{port}/api/providers/apify/webhook?sync=1",
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "X-Sourcing-Provider-Webhook-Token": "secret-token",
                },
                method="POST",
            )
            with urllib_request.urlopen(signed_req, timeout=5) as response:
                accepted_body = json.loads(response.read().decode("utf-8"))
                accepted_status = response.status

            assert accepted_status == 202
            assert accepted_body["status"] == "accepted"
            assert accepted_body["recovery_count"] == 1
            assert accepted_body["recoveries"][0]["daemon"]["executed_count"] == 1
            assert orchestrator.payloads[0]["source"] == "provider_webhook"
        finally:
            server.shutdown()
            server.server_close()


def test_apify_webhook_endpoint_reuses_harvest_api_token_by_default() -> None:
    class _FakeOrchestrator:
        def __init__(self) -> None:
            self.payloads: list[dict[str, object]] = []
            self.acquisition_engine = SimpleNamespace(
                settings=SimpleNamespace(
                    harvest=SimpleNamespace(
                        profile_scraper=SimpleNamespace(api_token="actor-api-token"),
                        profile_search=SimpleNamespace(api_token=""),
                        company_employees=SimpleNamespace(api_token=""),
                    )
                )
            )

        def handle_remote_provider_event(self, payload):
            self.payloads.append(dict(payload or {}))
            return {"status": "accepted", "event": {"run_id": "run-api-token-reuse"}}

    orchestrator = _FakeOrchestrator()
    body = json.dumps(
        {
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {"actorRunId": "run-api-token-reuse", "defaultDatasetId": "dataset-api-token-reuse"},
        }
    ).encode("utf-8")

    with mock.patch.dict(
        "os.environ",
        {
            "SOURCING_PROVIDER_WEBHOOK_TOKEN": "",
            "APIFY_WEBHOOK_TOKEN": "",
            "SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN": "",
        },
        clear=False,
    ):
        server = create_server(orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        try:
            signed_req = urllib_request.Request(
                f"http://{host}:{port}/api/providers/apify/webhook",
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "X-Sourcing-Provider-Webhook-Token": "actor-api-token",
                },
                method="POST",
            )
            with urllib_request.urlopen(signed_req, timeout=5) as response:
                accepted_body = json.loads(response.read().decode("utf-8"))
                accepted_status = response.status

            deadline = time.time() + 2
            while not orchestrator.payloads and time.time() < deadline:
                time.sleep(0.01)

            assert accepted_status == 202
            assert accepted_body["status"] == "accepted"
            assert orchestrator.payloads[0]["eventType"] == "ACTOR.RUN.SUCCEEDED"
        finally:
            server.shutdown()
            server.server_close()
