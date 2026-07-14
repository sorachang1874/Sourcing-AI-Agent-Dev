"""C2.6 canonical-owner race and owner-persistence regressions."""

import threading
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from sourcing_agent.crm_writer import CRMWriter
from sourcing_agent.domain import JobRequest
from sourcing_agent.excel_intake_owner import ExcelIntakeOwner
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.request_ownership import exact_crm_owner_matches, exact_job_owner_matches


class _SequencedJobStore:
    def __init__(self, jobs):
        self.jobs = list(jobs)
        self.get_job_calls = 0
        self.saved = []

    def get_job(self, _job_id):
        index = min(self.get_job_calls, len(self.jobs) - 1)
        self.get_job_calls += 1
        return self.jobs[index]

    def save_job(self, **kwargs):
        self.saved.append(kwargs)


def _owned_job(**patch_payload):
    return {
        "job_id": "job-1",
        "job_type": "workflow",
        "status": "running",
        "stage": "retrieving",
        "requester_id": "alice",
        "tenant_id": "user-alice",
        "request": {},
        "plan": {},
        "summary": {},
        **patch_payload,
    }


def _foreign_job():
    return _owned_job(requester_id="bob", tenant_id="user-bob")


def test_exact_owner_predicates_fail_closed_on_partial_or_legacy_identity() -> None:
    assert exact_job_owner_matches(_owned_job())
    assert not exact_job_owner_matches(_owned_job(), expected_requester_id="alice")
    assert not exact_job_owner_matches(
        _owned_job(requester_id="", tenant_id="default"),
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )
    assert exact_crm_owner_matches(
        {"workspace_id": "user-alice", "owner_user_id": ""},
        expected_workspace_id="user-alice",
        expected_owner_user_id="alice",
    )
    assert not exact_crm_owner_matches(
        {"workspace_id": "default", "owner_user_id": ""},
        expected_workspace_id="user-alice",
        expected_owner_user_id="alice",
    )


def test_cancel_rechecks_owner_before_first_write() -> None:
    orchestrator = object.__new__(SourcingOrchestrator)
    store = _SequencedJobStore([_owned_job(), _foreign_job()])
    orchestrator.store = store

    result = orchestrator.cancel_workflow_job(
        "job-1",
        {},
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == {"status": "not_found", "reason": "job_not_found"}
    assert store.saved == []


def test_concurrent_owner_change_cannot_cross_cancel_canonical_fence() -> None:
    second_read_started = threading.Event()
    allow_second_read = threading.Event()

    class ConcurrentStore:
        def __init__(self):
            self.job = _owned_job()
            self.read_count = 0
            self.saved = []

        def get_job(self, _job_id):
            self.read_count += 1
            if self.read_count == 2:
                second_read_started.set()
                assert allow_second_read.wait(timeout=5)
            return dict(self.job)

        def save_job(self, **kwargs):
            self.saved.append(kwargs)

    orchestrator = object.__new__(SourcingOrchestrator)
    store = ConcurrentStore()
    orchestrator.store = store
    result_holder = []

    thread = threading.Thread(
        target=lambda: result_holder.append(
            orchestrator.cancel_workflow_job(
                "job-1",
                {},
                expected_requester_id="alice",
                expected_tenant_id="user-alice",
            )
        )
    )
    thread.start()
    assert second_read_started.wait(timeout=5)
    store.job = _foreign_job()
    allow_second_read.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert result_holder == [{"status": "not_found", "reason": "job_not_found"}]
    assert store.saved == []


def test_interrupt_rechecks_worker_to_job_link_and_owner_before_side_effect() -> None:
    orchestrator = object.__new__(SourcingOrchestrator)
    store = _SequencedJobStore([_owned_job(), _foreign_job()])
    store.get_agent_worker = lambda **_kwargs: {"worker_id": 7, "job_id": "job-1"}
    orchestrator.store = store
    calls = []
    orchestrator.agent_runtime = SimpleNamespace(interrupt_worker=lambda worker_id: calls.append(worker_id))

    result = orchestrator.interrupt_agent_worker(
        {"worker_id": 7},
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == {"status": "not_found", "reason": "job_not_found"}
    assert calls == []


def test_cleanup_rechecks_job_owner_before_worker_retirement() -> None:
    orchestrator = object.__new__(SourcingOrchestrator)
    store = _SequencedJobStore(
        [
            _owned_job(status="failed", stage="failed"),
            _foreign_job() | {"status": "failed", "stage": "failed"},
        ]
    )
    worker = {
        "worker_id": 7,
        "job_id": "job-1",
        "lane_id": "lane",
        "worker_key": "worker",
        "status": "running",
    }
    store.list_recoverable_agent_workers = lambda **_kwargs: [worker]
    store.get_agent_worker = lambda **_kwargs: worker
    retire_calls = []
    store.retire_agent_workers = lambda **kwargs: retire_calls.append(kwargs) or []
    orchestrator.store = store

    result = orchestrator.cleanup_recoverable_workers(
        {"job_id": "job-1"},
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == {"status": "not_found", "reason": "job_not_found"}
    assert retire_calls == []


def test_cleanup_preview_excludes_worker_outside_explicit_owned_job() -> None:
    orchestrator = object.__new__(SourcingOrchestrator)

    class Store:
        def get_job(self, job_id):
            if job_id == "job-1":
                return _owned_job(status="failed", stage="failed")
            return _foreign_job() | {"job_id": job_id, "status": "failed", "stage": "failed"}

        def list_recoverable_agent_workers(self, **_kwargs):
            return [{"worker_id": 8, "job_id": "job-foreign", "status": "failed"}]

    orchestrator.store = Store()
    result = orchestrator.cleanup_recoverable_workers(
        {"job_id": "job-1", "dry_run": True},
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == {
        "status": "preview",
        "candidate_count": 0,
        "skipped_count": 0,
        "candidates": [],
        "skipped": [],
    }


def test_shutdown_rechecks_owner_before_stop_request() -> None:
    orchestrator = object.__new__(SourcingOrchestrator)
    store = _SequencedJobStore([_owned_job(), _foreign_job()])
    orchestrator.store = store
    calls = []
    with TemporaryDirectory() as temporary_dir:
        orchestrator.runtime_dir = Path(temporary_dir)
        with patch(
            "sourcing_agent.orchestrator.request_service_stop", side_effect=lambda *a, **k: calls.append((a, k))
        ):
            result = orchestrator.request_runtime_service_shutdown(
                {"job_id": "job-1"},
                expected_requester_id="alice",
                expected_tenant_id="user-alice",
            )

    assert result == {"status": "not_found", "reason": "job_not_found"}
    assert calls == []


class _CrmRaceStore:
    def __init__(self):
        self.crm_reads = 0
        self.upserts = []

    def get_crm_record(self, _record_id):
        self.crm_reads += 1
        workspace = "user-alice" if self.crm_reads == 1 else "user-bob"
        return {
            "crm_record_id": "rec-1",
            "workspace_id": workspace,
            "owner_user_id": "alice" if workspace == "user-alice" else "bob",
            "current_engagement_id": "eng-1",
            "metadata": {},
        }

    def get_crm_engagement(self, _engagement_id):
        return {"engagement_id": "eng-1", "stage": "new", "metadata": {}}

    def upsert_crm_record(self, payload):
        self.upserts.append(payload)
        return payload


def test_crm_writer_rechecks_workspace_owner_before_first_write() -> None:
    store = _CrmRaceStore()
    writer = CRMWriter(store)  # type: ignore[arg-type]

    result = writer.update_crm_record(
        crm_record_id="rec-1",
        workspace_id="user-alice",
        expected_workspace_id="user-alice",
        expected_owner_user_id="alice",
        stage="contacted",
    )

    assert result == {"status": "not_found", "reason": "crm_record_not_found"}
    assert store.upserts == []


class _ExcelStore:
    def __init__(self):
        self.job = None
        self.saved = []

    def get_job(self, _job_id):
        return self.job

    def save_job(self, **kwargs):
        self.saved.append(kwargs)
        self.job = {
            "requester_id": kwargs.get("requester_id", ""),
            "tenant_id": kwargs.get("tenant_id", ""),
            "artifact_path": kwargs.get("artifact_path", ""),
        }


def test_excel_job_owner_persists_across_initial_and_followup_saves() -> None:
    owner = object.__new__(ExcelIntakeOwner)
    store = _ExcelStore()
    owner.store = store
    request = JobRequest(raw_user_request="Excel intake")

    owner._save_excel_intake_job_state(
        job_id="excel-1",
        request=request,
        status="queued",
        stage="acquiring",
        summary_payload={},
        requester_id="alice",
        tenant_id="user-alice",
    )
    owner._save_excel_intake_job_state(
        job_id="excel-1",
        request=request,
        status="running",
        stage="acquiring",
        summary_payload={},
        requester_id="bob",
        tenant_id="user-bob",
    )

    assert [item["requester_id"] for item in store.saved] == ["alice", "alice"]
    assert [item["tenant_id"] for item in store.saved] == ["user-alice", "user-alice"]


class _RetrievalOwnerStore:
    def __init__(self):
        self.saved = []

    def save_job(self, **kwargs):
        self.saved.append(kwargs)

    def append_job_event(self, *_args, **_kwargs):
        return None

    def update_agent_runtime_session_status(self, *_args, **_kwargs):
        return None


class _RetrievalRuntime:
    @contextmanager
    def traced_lane(self, **_kwargs):
        yield {}

    def update_agent_runtime_session_status(self, *_args, **_kwargs):
        return None


def test_refinement_retrieval_job_persists_owner_on_running_and_failure_writes() -> None:
    orchestrator = object.__new__(SourcingOrchestrator)
    store = _RetrievalOwnerStore()
    orchestrator.store = store
    orchestrator.agent_runtime = _RetrievalRuntime()
    orchestrator._execute_retrieval = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("stop"))
    orchestrator.mark_job_result_lifecycle_terminal = lambda **_kwargs: None

    with pytest.raises(RuntimeError, match="stop"):
        orchestrator._run_retrieval_job(
            job_id="derived-1",
            request_payload={"raw_user_request": "refined", "target_company": "OpenAI"},
            plan_payload={},
            job_type="retrieval_refinement",
            requester_id="alice",
            tenant_id="user-alice",
        )

    assert len(store.saved) == 2
    assert all(item["requester_id"] == "alice" for item in store.saved)
    assert all(item["tenant_id"] == "user-alice" for item in store.saved)
