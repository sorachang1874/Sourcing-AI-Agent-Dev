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


@pytest.mark.parametrize(
    ("conflict_job", "expected"),
    (
        (
            _owned_job(status="completed", stage="completed"),
            {"status": "already_completed", "job_id": "job-1"},
        ),
        (
            _owned_job(
                status="blocked",
                stage="retrieving",
                summary={"awaiting_user_action": "continue_stage2", "stage2_transition_state": "queued"},
            ),
            {
                "status": "conflict",
                "reason": "stage2_already_requested",
                "job_id": "job-1",
                "stage2_transition_state": "queued",
            },
        ),
    ),
)
def test_stage2_typed_cas_projects_authorized_state_conflict_without_side_effect(
    conflict_job: dict[str, object], expected: dict[str, object]
) -> None:
    waiting = _owned_job(
        status="blocked",
        stage="retrieving",
        summary={"awaiting_user_action": "continue_stage2", "stage2_transition_state": ""},
    )
    writes: list[str] = []
    orchestrator = object.__new__(SourcingOrchestrator)
    orchestrator.store = SimpleNamespace(
        get_job=lambda _job_id: dict(waiting),
        save_job_if_owned=lambda **_kwargs: {
            "status": "state_conflict",
            "job": dict(conflict_job),
        },
        append_job_event=lambda *_args, **_kwargs: writes.append("event"),
    )

    @contextmanager
    def acquired_lock():
        yield {"acquired": True}

    orchestrator._job_run_lock = lambda _job_id: acquired_lock()

    result = orchestrator.continue_workflow_stage2(
        {"job_id": "job-1"},
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == expected
    assert writes == []


def test_cancel_typed_cas_projects_terminal_interleaving_without_cleanup_side_effect() -> None:
    running = _owned_job(status="running", stage="acquiring", summary={"progress": "working"})
    terminal = _owned_job(status="failed", stage="failed", summary={"error": "winner"})
    side_effects: list[str] = []
    orchestrator = object.__new__(SourcingOrchestrator)
    orchestrator.store = SimpleNamespace(
        get_job=lambda _job_id: dict(running),
        save_job_if_owned=lambda **_kwargs: {"status": "state_conflict", "job": dict(terminal)},
        update_agent_runtime_session_status=lambda *_args: side_effects.append("runtime_status"),
        release_workflow_job_lease=lambda *_args: side_effects.append("lease_release"),
    )

    result = orchestrator.cancel_workflow_job(
        "job-1",
        {},
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == {
        "status": "already_terminal",
        "job_id": "job-1",
        "job_status": "failed",
        "job": terminal,
    }
    assert side_effects == []


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

    def apply_owned_crm_record_update(self, **kwargs):
        latest = self.get_crm_record("rec-1")
        if latest.get("workspace_id") != kwargs["expected_workspace_id"]:
            return {"status": "not_found", "reason": "crm_record_not_found"}
        self.upserts.append(kwargs)
        return {"status": "applied"}


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


def test_stage2_lock_contention_rechecks_owner_before_returning_job_state() -> None:
    orchestrator = object.__new__(SourcingOrchestrator)
    store = _SequencedJobStore([_owned_job(job_type="workflow"), _foreign_job()])
    orchestrator.store = store

    @contextmanager
    def busy_lock(_job_id):
        yield None

    orchestrator._job_run_lock = busy_lock
    result = orchestrator.continue_workflow_stage2(
        {"job_id": "job-1"},
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == {"status": "not_found", "reason": "job_not_found"}


def test_authenticated_daemon_status_projects_only_compact_job_scoped_control() -> None:
    orchestrator = object.__new__(SourcingOrchestrator)
    orchestrator.store = SimpleNamespace(
        get_job=lambda _job_id: _owned_job(
            summary={
                "runtime_controls": {
                    "hosted_runtime_watchdog": {"service_status": {"marker": "FOREIGN_HOSTED_DETAIL"}},
                    "shared_recovery": {"service_status": {"marker": "FOREIGN_SHARED_DETAIL"}},
                    # A job-scoped signal may nudge the shared daemon. The
                    # authenticated view must not probe or expose that daemon.
                    "job_recovery": {
                        "status": "signaled",
                        "scope": "job_scoped",
                        "mode": "signal_only",
                        "job_id": "job-1",
                        "service_name": "worker-recovery-daemon",
                        "wakeup": {"marker": "FOREIGN_SHARED_WAKEUP"},
                    },
                    "workflow_runner": {"log_tail": "FOREIGN_LOG"},
                }
            }
        )
    )
    orchestrator._build_live_runtime_controls_payload = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("authenticated status must not build global runtime controls")
    )
    orchestrator._read_progress_service_status = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("shared signal-only service must not be probed")
    )

    result = orchestrator.get_worker_daemon_status(
        {"job_id": "job-1", "include_details": True},
        authenticated_job_scope=True,
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == {
        "job_id": "job-1",
        "status": "ok",
        "runtime_controls": {
            "job_recovery": {
                "status": "signaled",
                "scope": "job_scoped",
                "mode": "signal_only",
                "job_id": "job-1",
            }
        },
        "recovery_services": {"job_scoped": {}},
    }
    assert "FOREIGN" not in str(result)


def test_criteria_rerun_rejects_foreign_explicit_baseline_before_results_read() -> None:
    orchestrator = object.__new__(SourcingOrchestrator)
    store = SimpleNamespace(
        get_job=lambda _job_id: _foreign_job() | {"job_id": "job-bob"},
        get_job_results=lambda _job_id: (_ for _ in ()).throw(AssertionError("foreign results read")),
    )
    orchestrator.store = store

    result = orchestrator._rerun_after_recompile_if_requested(
        {"rerun_retrieval": True, "job_id": "job-bob"},
        {"feedback_id": 1},
        {
            "status": "recompiled",
            "request": {"target_company": "OpenAI"},
            "plan": {},
        },
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == {"status": "not_found", "reason": "job_not_found"}


def test_criteria_feedback_preflights_missing_explicit_baseline_before_any_write() -> None:
    events: list[str] = []
    criteria_repo = SimpleNamespace(
        record_feedback=lambda _payload: events.append("record_feedback"),
    )
    orchestrator = object.__new__(SourcingOrchestrator)
    orchestrator.store = SimpleNamespace(
        repos=SimpleNamespace(criteria_confidence=criteria_repo),
        get_job=lambda _job_id: events.append("get_job") or None,
    )
    orchestrator.criteria_evolution = SimpleNamespace(
        recompile_after_feedback=lambda *_args: events.append("recompile")
    )

    result = orchestrator.record_criteria_feedback(
        {"rerun_retrieval": True, "job_id": "job-missing"},
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == {"status": "not_found", "reason": "job_not_found"}
    assert events == ["get_job"]


def test_criteria_recompile_preflights_foreign_explicit_baseline_before_any_write() -> None:
    events: list[str] = []
    orchestrator = object.__new__(SourcingOrchestrator)
    orchestrator.store = SimpleNamespace(
        get_job=lambda _job_id: events.append("get_job") or _foreign_job(),
    )
    orchestrator.criteria_evolution = SimpleNamespace(
        recompile_after_feedback=lambda *_args: events.append("recompile")
    )

    result = orchestrator.recompile_criteria(
        {"rerun_retrieval": True, "baseline_job_id": "job-bob"},
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == {"status": "not_found", "reason": "job_not_found"}
    assert events == ["get_job"]


def test_criteria_suggestion_preflights_foreign_source_baseline_before_review_write() -> None:
    events: list[str] = []
    criteria_repo = SimpleNamespace(
        get_suggestion=lambda _suggestion_id: (
            events.append("get_suggestion") or {"suggestion_id": 7, "source_feedback_id": 11}
        ),
        get_feedback=lambda _feedback_id: events.append("get_feedback") or {"job_id": "job-bob"},
        review_suggestion=lambda **_kwargs: events.append("review_suggestion"),
    )
    orchestrator = object.__new__(SourcingOrchestrator)
    orchestrator.store = SimpleNamespace(
        repos=SimpleNamespace(criteria_confidence=criteria_repo),
        get_job=lambda _job_id: events.append("get_job") or _foreign_job(),
    )
    orchestrator.criteria_evolution = SimpleNamespace(
        recompile_after_feedback=lambda *_args: events.append("recompile")
    )

    result = orchestrator.review_pattern_suggestion(
        {"suggestion_id": 7, "action": "apply", "rerun_retrieval": True},
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == {"status": "not_found", "reason": "job_not_found"}
    assert events == ["get_suggestion", "get_feedback", "get_job"]


def test_criteria_automatic_baseline_selection_is_exact_owner_scoped() -> None:
    captured: dict[str, object] = {}

    def find_best_completed_job_match(**kwargs):
        captured.update(kwargs)
        return None

    orchestrator = object.__new__(SourcingOrchestrator)
    orchestrator.store = SimpleNamespace(
        find_best_completed_job_match=find_best_completed_job_match,
        get_job_results=lambda _job_id: [],
    )
    with patch(
        "sourcing_agent.orchestrator.decide_rerun_policy",
        return_value={"status": "gated_off", "mode": "none"},
    ):
        result = orchestrator._rerun_after_recompile_if_requested(
            {"rerun_retrieval": True},
            {"feedback_id": 1},
            {
                "status": "recompiled",
                "request": {"target_company": "OpenAI"},
                "plan": {},
            },
            expected_requester_id="alice",
            expected_tenant_id="user-alice",
        )

    assert result["status"] == "gated_off"
    assert captured["requester_id"] == "alice"
    assert captured["tenant_id"] == "user-alice"
