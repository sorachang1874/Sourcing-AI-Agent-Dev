"""C2.5 authenticated request scoping and exact write-authority closure."""

import json
import os
import re
import threading
import unittest
from unittest.mock import patch
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.error import HTTPError

from sourcing_agent.api import (
    AUTHENTICATED_REQUEST_SCOPE_REGISTRY,
    _apply_server_read_scope,
    _build_routes,
    _write_allowed_crm_owner,
    _write_allowed_job_owner,
    create_server,
)


class _FakeState:
    def __init__(self, identity):
        self.identity = identity


class _FakeRequest:
    def __init__(self, identity):
        self.state = _FakeState(identity)


class ApplyServerReadScopeTest(unittest.TestCase):
    def test_open_mode_is_exact_noop(self) -> None:
        payload = {
            "requester_id": "client-user",
            "tenant_id": "client-tenant",
            "workspace_id": "client-workspace",
            "target_company": "OpenAI",
        }
        before = dict(payload)
        result = _apply_server_read_scope(
            payload,
            _FakeRequest(None),
            requester=True,
            tenant=True,
            workspace=True,
        )
        self.assertIs(result, payload)
        self.assertEqual(payload, before)

    def test_authenticated_workspace_defaults_to_server_namespace(self) -> None:
        for payload in ({}, {"workspace_id": "user-bob"}, {"tenant_id": "foreign", "org_id": "foreign"}):
            with self.subTest(payload=payload):
                scoped = dict(payload)
                _apply_server_read_scope(scoped, _FakeRequest({"user_id": "alice"}), workspace=True)
                self.assertEqual(scoped, {"workspace_id": "user-alice"})

    def test_authenticated_workspace_preserves_explicit_legacy_default(self) -> None:
        payload = {"workspace_id": "default", "tenant_id": "foreign", "org_id": "foreign"}
        _apply_server_read_scope(payload, _FakeRequest({"user_id": "alice"}), workspace=True)
        self.assertEqual(payload, {"workspace_id": "default"})

    def test_query_dispatch_scope_strips_spoofable_aliases(self) -> None:
        payload = {
            "requester_id": "bob",
            "user_id": "bob",
            "requester": "bob",
            "tenant_id": "foreign",
            "workspace_id": "foreign",
            "org_id": "foreign",
            "target_company": "OpenAI",
        }
        _apply_server_read_scope(
            payload,
            _FakeRequest({"user_id": "alice"}),
            requester=True,
            tenant=True,
        )
        self.assertEqual(
            payload,
            {"requester_id": "alice", "tenant_id": "user-alice", "target_company": "OpenAI"},
        )

    def test_query_dispatch_legacy_default_does_not_claim_requester(self) -> None:
        payload = {"workspace_id": "default", "requester_id": "bob", "target_company": "OpenAI"}
        _apply_server_read_scope(
            payload,
            _FakeRequest({"user_id": "alice"}),
            requester=True,
            tenant=True,
        )
        self.assertEqual(payload, {"tenant_id": "default", "target_company": "OpenAI"})


class ExactWriteAuthorityTest(unittest.TestCase):
    def test_job_write_requires_exact_requester_and_tenant(self) -> None:
        request = _FakeRequest({"user_id": "alice"})
        self.assertTrue(
            _write_allowed_job_owner(
                request,
                {"requester_id": "alice", "tenant_id": "user-alice"},
            )
        )
        self.assertFalse(
            _write_allowed_job_owner(
                request,
                {"requester_id": "alice", "tenant_id": "default"},
            )
        )
        self.assertFalse(
            _write_allowed_job_owner(
                request,
                {"requester_id": "default", "tenant_id": "default"},
            )
        )

    def test_crm_write_requires_exact_workspace_and_consistent_nonempty_owner(self) -> None:
        request = _FakeRequest({"user_id": "alice"})
        self.assertTrue(_write_allowed_crm_owner(request, {"workspace_id": "user-alice"}))
        self.assertTrue(
            _write_allowed_crm_owner(
                request,
                {"workspace_id": "user-alice", "owner_user_id": "alice"},
            )
        )
        self.assertFalse(_write_allowed_crm_owner(request, {"workspace_id": "default"}))
        self.assertFalse(
            _write_allowed_crm_owner(
                request,
                {"workspace_id": "user-alice", "owner_user_id": "bob"},
            )
        )

    def test_open_mode_write_predicates_are_noop(self) -> None:
        request = _FakeRequest(None)
        self.assertTrue(_write_allowed_job_owner(request, {"requester_id": "bob", "tenant_id": "default"}))
        self.assertTrue(_write_allowed_crm_owner(request, {"workspace_id": "default", "owner_user_id": "bob"}))


class _ScopeStore:
    def __init__(self) -> None:
        self.jobs = {
            "job-alice": {"requester_id": "alice", "tenant_id": "user-alice"},
            "job-alice-wrong-tenant": {"requester_id": "alice", "tenant_id": "default"},
            "job-bob": {"requester_id": "bob", "tenant_id": "user-bob"},
            "job-legacy": {"requester_id": "default", "tenant_id": "default"},
        }
        self.crm_records = {
            "rec-alice": {"workspace_id": "user-alice", "owner_user_id": ""},
            "rec-alice-owned": {"workspace_id": "user-alice", "owner_user_id": "alice"},
            "rec-alice-owner-bob": {"workspace_id": "user-alice", "owner_user_id": "bob"},
            "rec-bob": {"workspace_id": "user-bob", "owner_user_id": "bob"},
            "rec-legacy": {"workspace_id": "default", "owner_user_id": ""},
        }

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def get_crm_record(self, record_id):
        return self.crm_records.get(record_id)


class _ScopeOrchestrator:
    def __init__(self) -> None:
        self.store = _ScopeStore()
        self.captured: dict[str, list[dict]] = {}

    def _capture(self, name: str, payload: dict) -> None:
        self.captured.setdefault(name, []).append(dict(payload))

    def submit_operation_action(self, payload, **owner):
        self._capture("operation_action", {**dict(payload), **owner})
        action_type = str(payload.get("action_type") or "").strip()
        if action_type in {"set_crm_stage", "add_crm_note", "create_crm_task", "enrich_person_public_web"}:
            target_ref = dict(payload.get("target_ref") or {})
            record_ids = target_ref.get("crm_record_ids") or [target_ref.get("crm_record_id")]
            records = [self.store.get_crm_record(str(record_id or "")) for record_id in list(record_ids or [])]
            if owner and (
                not records
                or any(
                    not record
                    or record.get("workspace_id") != owner.get("expected_workspace_id")
                    or str(record.get("owner_user_id") or "")
                    not in {"", str(owner.get("expected_owner_user_id") or "")}
                    for record in records
                )
            ):
                return {"status": "not_found", "reason": "crm_record_not_found"}
        if str(payload.get("idempotency_key") or "").strip() == "completed-replay":
            return {
                "status": "completed",
                "idempotent_replay": True,
                "action": {"action_id": "action-completed", "status": "completed"},
                "operation_run": {"operation_run_id": "run-completed", "status": "completed"},
                "module_state_mutated": False,
            }
        if str(payload.get("idempotency_key") or "").strip() == "invalid-replay-flag":
            return {"status": "invalid", "idempotent_replay": True, "reason": "stub-invalid"}
        if str(payload.get("idempotency_key") or "").strip() == "replay-status-whitespace":
            return {"status": " completed ", "idempotent_replay": True, "module_state_mutated": False}
        if str(payload.get("idempotency_key") or "").strip() == "replay-status-list":
            return {"status": ["completed"], "idempotent_replay": True, "module_state_mutated": False}
        if str(payload.get("idempotency_key") or "").strip() == "replay-status-dict":
            return {
                "status": {"value": "completed"},
                "idempotent_replay": True,
                "module_state_mutated": False,
            }
        if str(payload.get("idempotency_key") or "").strip() == "fresh-replay-flag-missing":
            return {"status": "queued", "module_state_mutated": False}
        if str(payload.get("idempotency_key") or "").strip() == "fresh-replay-flag-null":
            return {"status": "queued", "idempotent_replay": None, "module_state_mutated": False}
        if str(payload.get("idempotency_key") or "").strip() == "fresh-replay-flag-string":
            return {"status": "queued", "idempotent_replay": "false", "module_state_mutated": False}
        if str(payload.get("idempotency_key") or "").strip() == "fresh-status-list":
            return {"status": ["queued"], "idempotent_replay": False, "module_state_mutated": False}
        if str(payload.get("idempotency_key") or "").strip() == "fresh-status-dict":
            return {
                "status": {"value": "queued"},
                "idempotent_replay": False,
                "module_state_mutated": False,
            }
        return {"status": "queued", "idempotent_replay": False, "module_state_mutated": False}

    def get_operation_action_registry(self):
        return {"status": "ok", "actions": {}}

    def list_operation_actions_api(self, payload, **owner):
        self._capture("operation_actions", {**dict(payload), **owner})
        return {"status": "ok", "actions": [], "module_state_mutated": False}

    def list_operation_runs_api(self, payload, **owner):
        self._capture("operation_runs", {**dict(payload), **owner})
        return {"status": "ok", "operation_runs": [], "module_state_mutated": False}

    @staticmethod
    def _operation_owned(resource_id, owner):
        expected = str(owner.get("expected_workspace_id") or "")
        return not expected or str(resource_id).endswith("alice")

    def get_operation_action_api(self, action_id, **owner):
        self._capture("operation_action_get", {"action_id": action_id, **owner})
        if not self._operation_owned(action_id, owner):
            return {"status": "not_found", "action_id": action_id}
        return {"status": "ok", "action": {"action_id": action_id}}

    def get_operation_run_api(self, run_id, **owner):
        self._capture("operation_run_get", {"run_id": run_id, **owner})
        if not self._operation_owned(run_id, owner):
            return {"status": "not_found", "operation_run_id": run_id}
        return {"status": "ok", "operation_run": {"operation_run_id": run_id}}

    def get_operation_run_provenance_api(self, run_id, **owner):
        self._capture("operation_run_provenance", {"run_id": run_id, **owner})
        if not self._operation_owned(run_id, owner):
            return {"status": "not_found", "operation_run_id": run_id}
        return {"status": "ok", "operation_run": {"operation_run_id": run_id}}

    def approve_operation_action_api(self, action_id, payload, **owner):
        self._capture("operation_action_approve", {"action_id": action_id, **dict(payload), **owner})
        if not self._operation_owned(action_id, owner):
            return {"status": "not_found", "action_id": action_id}
        return {"status": "queued"}

    def reject_operation_action_api(self, action_id, payload, **owner):
        self._capture("operation_action_reject", {"action_id": action_id, **dict(payload), **owner})
        if not self._operation_owned(action_id, owner):
            return {"status": "not_found", "action_id": action_id}
        return {"status": "rejected"}

    def cancel_operation_run_api(self, run_id, payload, **owner):
        self._capture("operation_run_cancel", {"run_id": run_id, **dict(payload), **owner})
        if not self._operation_owned(run_id, owner):
            return {"status": "not_found", "operation_run_id": run_id}
        return {"status": "cancelled"}

    def retry_operation_run_api(self, run_id, payload, **owner):
        self._capture("operation_run_retry", {"run_id": run_id, **dict(payload), **owner})
        if not self._operation_owned(run_id, owner):
            return {"status": "not_found", "operation_run_id": run_id}
        return {"status": "queued"}

    def resume_operation_run_api(self, run_id, payload, **owner):
        self._capture("operation_run_resume", {"run_id": run_id, **dict(payload), **owner})
        if not self._operation_owned(run_id, owner):
            return {"status": "not_found", "operation_run_id": run_id}
        return {"status": "queued"}

    def dispatch_operation_run_api(self, run_id, payload, **owner):
        self._capture("operation_run_dispatch", {"run_id": run_id, **dict(payload), **owner})
        if not self._operation_owned(run_id, owner):
            return {"status": "not_found", "operation_run_id": run_id}
        return {"status": "planned"}

    def list_query_dispatches(self, payload):
        self._capture("query_dispatches", payload)
        return {"query_dispatches": [], "scope": dict(payload)}

    def start_excel_intake_workflow(self, payload):
        self._capture("excel_create", payload)
        return {"status": "queued", "job_id": "excel-owned"}

    def compile_post_acquisition_refinement(self, payload, **owner):
        self._capture("refine_compile", {**dict(payload), **owner})
        job = self.store.get_job(str(payload.get("job_id") or ""))
        if (
            not job
            or job.get("requester_id") != owner.get("expected_requester_id")
            or job.get("tenant_id") != owner.get("expected_tenant_id")
        ):
            return {"status": "not_found", "reason": "job_not_found"}
        return {"status": "compiled"}

    def apply_post_acquisition_refinement(self, payload, **owner):
        self._capture("refine_apply", {**dict(payload), **owner})
        job = self.store.get_job(str(payload.get("job_id") or ""))
        if (
            not job
            or job.get("requester_id") != owner.get("expected_requester_id")
            or job.get("tenant_id") != owner.get("expected_tenant_id")
        ):
            return {"status": "not_found", "reason": "job_not_found"}
        return {"status": "completed", "rerun_job_id": "derived-owned"}

    def interrupt_agent_worker(self, payload, **owner):
        self._capture("worker_interrupt", {**dict(payload), **owner})
        if int(payload.get("worker_id") or 0) != 1:
            return {"status": "not_found", "reason": "job_not_found"}
        return {"status": "interrupt_requested"}

    def cleanup_recoverable_workers(self, payload, **owner):
        self._capture("worker_cleanup", {**dict(payload), **owner})
        return {"status": "completed"}

    def request_runtime_service_shutdown(self, payload, **owner):
        self._capture("runtime_shutdown", {**dict(payload), **owner})
        return {"status": "requested"}

    def signal_shared_recovery(self, **payload):
        self._capture("shared_recovery", payload)
        return {"status": "accepted"}

    def write_worker_daemon_systemd_unit(self, payload):
        self._capture("systemd_unit", payload)
        return {"status": "written"}

    def list_recoverable_agent_workers(self, payload=None):
        self._capture("worker_list", dict(payload or {}))
        return {"recoverable_workers": [], "count": 0}

    def get_worker_daemon_status(self, payload=None, **owner):
        self._capture("worker_status", {**dict(payload or {}), **owner})
        return {
            "status": "ok",
            "runtime_controls": {"job_recovery": {"service_status": {"status": "running"}}},
            "recovery_services": {"job_scoped": {"status": "running"}},
        }

    def record_criteria_feedback(self, payload, **owner):
        self._capture("criteria_feedback", {**dict(payload), **owner})
        for raw_job_id in (payload.get("job_id"), payload.get("baseline_job_id")):
            job_id = str(raw_job_id or "").strip()
            job = self.store.get_job(job_id)
            if job_id and (
                not job
                or job.get("requester_id") != owner.get("expected_requester_id")
                or job.get("tenant_id") != owner.get("expected_tenant_id")
            ):
                return {"status": "not_found", "reason": "job_not_found"}
        return {"status": "recorded", "rerun": {"status": "not_requested"}}

    def configure_confidence_policy(self, payload, **owner):
        self._capture("criteria_confidence", {**dict(payload), **owner})
        for raw_job_id in (payload.get("job_id"), payload.get("baseline_job_id"), payload.get("source_job_id")):
            job_id = str(raw_job_id or "").strip()
            job = self.store.get_job(job_id)
            if job_id and (
                not job
                or job.get("requester_id") != owner.get("expected_requester_id")
                or job.get("tenant_id") != owner.get("expected_tenant_id")
            ):
                return {"status": "not_found", "reason": "job_not_found"}
        return {"status": "configured"}

    def review_pattern_suggestion(self, payload, **owner):
        self._capture("criteria_suggestion", {**dict(payload), **owner})
        for raw_job_id in (payload.get("job_id"), payload.get("baseline_job_id")):
            job_id = str(raw_job_id or "").strip()
            job = self.store.get_job(job_id)
            if job_id and (
                not job
                or job.get("requester_id") != owner.get("expected_requester_id")
                or job.get("tenant_id") != owner.get("expected_tenant_id")
            ):
                return {"status": "not_found", "reason": "job_not_found"}
        return {"status": "reviewed", "rerun": {"status": "not_requested"}}

    def recompile_criteria(self, payload, **owner):
        self._capture("criteria_recompile", {**dict(payload), **owner})
        for raw_job_id in (payload.get("job_id"), payload.get("baseline_job_id")):
            job_id = str(raw_job_id or "").strip()
            job = self.store.get_job(job_id)
            if job_id and (
                not job
                or job.get("requester_id") != owner.get("expected_requester_id")
                or job.get("tenant_id") != owner.get("expected_tenant_id")
            ):
                return {"status": "not_found", "reason": "job_not_found"}
        return {"status": "recompiled", "rerun": {"status": "not_requested"}}

    def list_crm_records_api(self, **kwargs):
        self._capture("crm_records", kwargs)
        return {"status": "ready", "workspace_id": kwargs["workspace_id"], "crm_records": []}

    def list_crm_tasks_api(self, **kwargs):
        self._capture("crm_tasks", kwargs)
        return {"status": "ready", "workspace_id": kwargs["workspace_id"], "crm_tasks": []}

    def get_crm_record_api(self, record_id, *, workspace_id="default"):
        self._capture("crm_record", {"record_id": record_id, "workspace_id": workspace_id})
        return {"status": "ready", "workspace_id": workspace_id, "crm_record": {"id": record_id}}

    def get_projection_crm_state_api(self, projection_id, *, candidate_identity_keys=None, workspace_id="default"):
        self._capture(
            "projection_crm_state",
            {
                "projection_id": projection_id,
                "candidate_identity_keys": list(candidate_identity_keys or []),
                "workspace_id": workspace_id,
            },
        )
        return {"status": "ready", "workspace_id": workspace_id}

    def search_projection_person_index_api(
        self,
        projection_id,
        *,
        search_keyword="",
        offset=0,
        limit=120,
        workspace_id="default",
    ):
        self._capture(
            "projection_search",
            {
                "projection_id": projection_id,
                "search_keyword": search_keyword,
                "offset": offset,
                "limit": limit,
                "workspace_id": workspace_id,
            },
        )
        return {"status": "ready", "workspace_id": workspace_id, "candidates": []}

    def get_serving_projection_candidate_page(
        self,
        projection_id,
        *,
        offset=0,
        limit=120,
        candidate_filter=None,
        workspace_id="default",
    ):
        self._capture(
            "projection_candidates",
            {
                "projection_id": projection_id,
                "offset": offset,
                "limit": limit,
                "candidate_filter": dict(candidate_filter or {}),
                "workspace_id": workspace_id,
            },
        )
        return {"status": "ready", "workspace_id": workspace_id, "candidates": []}

    def get_serving_projection_person_detail_api(self, projection_id, person_key, *, workspace_id="default"):
        self._capture(
            "projection_person",
            {"projection_id": projection_id, "person_key": person_key, "workspace_id": workspace_id},
        )
        return {"status": "ready", "workspace_id": workspace_id}

    def get_person_summary_api(self, person_key, *, workspace_id="default"):
        self._capture("person_summary", {"person_key": person_key, "workspace_id": workspace_id})
        return {"status": "ready", "workspace_id": workspace_id}

    def continue_workflow_stage2(self, payload, **owner):
        self._capture("continue_stage2", {**dict(payload), **owner})
        return {"status": "queued"}

    def complete_job_candidate_profiles(self, job_id, payload, **owner):
        self._capture("profile_completion", {"job_id": job_id, **dict(payload), **owner})
        return {"status": "completed"}

    def get_job_candidate_details_batch(self, job_id, candidate_ids):
        self._capture("candidate_batch", {"job_id": job_id, "candidate_ids": list(candidate_ids)})
        return {"status": "ready", "candidates": []}

    def get_job_dashboard(
        self,
        job_id,
        *,
        include_asset_population_preview=True,
        workspace_id="default",
    ):
        self._capture(
            "job_dashboard",
            {
                "job_id": job_id,
                "include_asset_population_preview": include_asset_population_preview,
                "workspace_id": workspace_id,
            },
        )
        return {"status": "ready", "workspace_id": workspace_id}

    def get_job_candidate_page(
        self,
        job_id,
        *,
        offset=0,
        limit=120,
        lightweight=True,
        candidate_filter=None,
        workspace_id="default",
    ):
        self._capture(
            "job_candidates",
            {
                "job_id": job_id,
                "offset": offset,
                "limit": limit,
                "lightweight": lightweight,
                "candidate_filter": dict(candidate_filter or {}),
                "workspace_id": workspace_id,
            },
        )
        return {"status": "ready", "workspace_id": workspace_id, "candidates": []}

    def get_legacy_result_endpoint_retirement_status(self, payload):
        self._capture("legacy_result_retirement", dict(payload))
        return {"status": "ready", "cutover_enforced": False}

    def cancel_workflow_job(self, job_id, payload, **owner):
        self._capture("job_cancel", {"job_id": job_id, **dict(payload), **owner})
        return {"status": "cancelled"}

    def add_projection_candidate_to_crm(self, payload):
        self._capture("crm_create", payload)
        return {"status": "upserted"}

    def import_target_candidates_from_job(self, payload, **owner):
        self._capture("target_import", {**dict(payload), **owner})
        return {"status": "imported"}

    def backfill_serving_projection_for_job(self, payload, **owner):
        self._capture("projection_backfill", {**dict(payload), **owner})
        return {"status": "backfilled"}

    def backfill_crm_from_target_candidates(self, payload):
        self._capture("crm_backfill", payload)
        return {"status": "backfilled"}

    def backfill_public_web_promotions_to_person_assertions(self, payload):
        self._capture("crm_promotion_backfill", payload)
        return {"status": "backfilled"}

    def update_crm_record_api(self, record_id, payload, **owner):
        self._capture("crm_update", {"record_id": record_id, **dict(payload), **owner})
        return {"status": "updated"}

    def promote_crm_record_public_web_signal(self, record_id, payload, **owner):
        self._capture("crm_promotion", {"record_id": record_id, **dict(payload), **owner})
        return {"status": "promoted"}


_TOKENS = json.dumps({"tok-alice": "alice", "tok-bob": "bob"})


class RequestScopeWiringTest(unittest.TestCase):
    def _start_server(self, *, authenticated=True):
        orchestrator = _ScopeOrchestrator()
        env = {"SOURCING_API_BEARER_TOKENS": _TOKENS} if authenticated else {}
        env_patch = patch.dict(os.environ, env)
        env_patch.start()
        if not authenticated:
            os.environ.pop("SOURCING_API_BEARER_TOKENS", None)
        try:
            server = create_server(orchestrator, host="127.0.0.1", port=0)
        finally:
            env_patch.stop()
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        return f"http://{host}:{port}", opener, orchestrator

    def _request(self, opener, url, *, method="GET", body=None, token="tok-alice"):
        headers = {}
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib_request.Request(url, data=data, headers=headers, method=method)
        try:
            with opener.open(request, timeout=10) as response:
                raw = response.read()
                return response.status, json.loads(raw) if raw else {}
        except HTTPError as error:
            raw = error.read()
            return error.code, json.loads(raw) if raw else {}

    def _url(self, base, path, query=None):
        suffix = f"?{urllib_parse.urlencode(query)}" if query else ""
        return f"{base}{path}{suffix}"

    def test_authenticated_crm_and_overlay_reads_use_server_or_explicit_legacy_scope(self) -> None:
        base, opener, _orchestrator = self._start_server()
        paths = (
            "/api/crm/records",
            "/api/crm/tasks",
            "/api/crm/records/rec-alice/tasks",
            "/api/crm/records/rec-alice",
            "/api/projections/proj1/crm-state",
            "/api/projections/proj1/persons/person1",
            "/api/persons/person1",
        )
        for path in paths:
            with self.subTest(path=path, scope="owned"):
                status, result = self._request(
                    opener,
                    self._url(base, path, {"workspace_id": "user-bob"}),
                )
                self.assertEqual(status, 200)
                self.assertEqual(result["workspace_id"], "user-alice")
            with self.subTest(path=path, scope="legacy"):
                status, result = self._request(
                    opener,
                    self._url(base, path, {"workspace_id": "default"}),
                )
                self.assertEqual(status, 200)
                self.assertEqual(result["workspace_id"], "default")

    def test_job_projection_overlay_reads_use_authenticated_and_open_mode_workspace(self) -> None:
        base, opener, orchestrator = self._start_server()
        for path, capture_name in (
            ("/api/jobs/job-alice/dashboard", "job_dashboard"),
            ("/api/jobs/job-alice/candidates", "job_candidates"),
        ):
            with self.subTest(path=path, scope="authenticated"):
                status, result = self._request(
                    opener,
                    self._url(base, path, {"workspace_id": "user-bob"}),
                )
                self.assertEqual(status, 200)
                self.assertEqual(result["workspace_id"], "user-alice")
                self.assertEqual(orchestrator.captured[capture_name][-1]["workspace_id"], "user-alice")
            with self.subTest(path=path, scope="legacy"):
                status, result = self._request(
                    opener,
                    self._url(base, path, {"workspace_id": "default"}),
                )
                self.assertEqual(status, 200)
                self.assertEqual(result["workspace_id"], "default")

        open_base, open_opener, open_orchestrator = self._start_server(authenticated=False)
        for path, capture_name in (
            ("/api/jobs/job-alice/dashboard", "job_dashboard"),
            ("/api/jobs/job-alice/candidates", "job_candidates"),
        ):
            with self.subTest(path=path, scope="open_mode"):
                status, result = self._request(
                    open_opener,
                    self._url(open_base, path, {"workspace_id": "operator-workspace"}),
                    token="",
                )
                self.assertEqual(status, 200)
                self.assertEqual(result["workspace_id"], "operator-workspace")
                self.assertEqual(
                    open_orchestrator.captured[capture_name][-1]["workspace_id"],
                    "operator-workspace",
                )

    def test_direct_projection_candidate_and_search_reads_cover_all_workspace_modes(self) -> None:
        base, opener, orchestrator = self._start_server()
        for path, capture_name in (
            ("/api/projections/proj1/candidates", "projection_candidates"),
            ("/api/projections/proj1/search", "projection_search"),
        ):
            with self.subTest(path=path, scope="authenticated"):
                status, result = self._request(
                    opener,
                    self._url(base, path, {"workspace_id": "user-bob", "search": "Ada"}),
                )
                self.assertEqual(status, 200)
                self.assertEqual(result["workspace_id"], "user-alice")
                self.assertEqual(orchestrator.captured[capture_name][-1]["workspace_id"], "user-alice")
            with self.subTest(path=path, scope="legacy"):
                status, result = self._request(
                    opener,
                    self._url(base, path, {"workspace_id": "default", "search": "Ada"}),
                )
                self.assertEqual(status, 200)
                self.assertEqual(result["workspace_id"], "default")

        open_base, open_opener, open_orchestrator = self._start_server(authenticated=False)
        for path, capture_name in (
            ("/api/projections/proj1/candidates", "projection_candidates"),
            ("/api/projections/proj1/search", "projection_search"),
        ):
            with self.subTest(path=path, scope="open_mode"):
                status, result = self._request(
                    open_opener,
                    self._url(
                        open_base,
                        path,
                        {"workspace_id": "operator-workspace", "search": "Ada"},
                    ),
                    token="",
                )
                self.assertEqual(status, 200)
                self.assertEqual(result["workspace_id"], "operator-workspace")
                self.assertEqual(
                    open_orchestrator.captured[capture_name][-1]["workspace_id"],
                    "operator-workspace",
                )

    def test_query_dispatch_get_and_post_are_server_scoped(self) -> None:
        base, opener, orchestrator = self._start_server()
        status, _ = self._request(
            opener,
            self._url(
                base,
                "/api/query-dispatches",
                {"requester_id": "bob", "tenant_id": "user-bob", "target_company": "OpenAI"},
            ),
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            orchestrator.captured["query_dispatches"][-1],
            {"requester_id": "alice", "tenant_id": "user-alice", "target_company": "OpenAI"},
        )

        status, _ = self._request(
            opener,
            f"{base}/api/query-dispatches/list",
            method="POST",
            body={"workspace_id": "default", "requester_id": "bob", "target_company": "OpenAI"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            orchestrator.captured["query_dispatches"][-1],
            {"tenant_id": "default", "target_company": "OpenAI"},
        )

    def test_authenticated_job_mutations_require_exact_modern_owner(self) -> None:
        base, opener, orchestrator = self._start_server()
        endpoints = (
            ("/api/workflows/{job_id}/continue-stage2", "continue_stage2", 202),
            ("/api/jobs/{job_id}/profile-completion", "profile_completion", 200),
            ("/api/jobs/{job_id}/cancel", "job_cancel", 200),
        )
        for path_template, capture_name, success_status in endpoints:
            for job_id, expected_status in (
                ("job-alice", success_status),
                ("job-alice-wrong-tenant", 404),
                ("job-bob", 404),
                ("job-legacy", 404),
            ):
                with self.subTest(path=path_template, job_id=job_id):
                    status, _ = self._request(
                        opener,
                        f"{base}{path_template.format(job_id=job_id)}",
                        method="POST",
                        body={"requester_id": "bob", "tenant_id": "user-bob"},
                    )
                    self.assertEqual(status, expected_status)
            self.assertEqual(len(orchestrator.captured.get(capture_name, [])), 1)

    def test_authenticated_missing_and_foreign_writes_are_non_enumerating(self) -> None:
        base, opener, _orchestrator = self._start_server()
        job_routes = (
            ("POST", "/api/workflows/{resource}/continue-stage2", {}),
            ("POST", "/api/jobs/{resource}/profile-completion", {"candidate_ids": ["candidate-1"]}),
            ("POST", "/api/jobs/{resource}/cancel", {}),
        )
        for method, path_template, body in job_routes:
            with self.subTest(path=path_template):
                foreign = self._request(
                    opener,
                    f"{base}{path_template.format(resource='job-bob')}",
                    method=method,
                    body=body,
                )
                missing = self._request(
                    opener,
                    f"{base}{path_template.format(resource='job-missing')}",
                    method=method,
                    body=body,
                )
                self.assertEqual(foreign, missing)
                self.assertEqual(foreign, (404, {"status": "not_found", "reason": "job_not_found"}))

        for method, path_template in (
            ("PATCH", "/api/crm/records/{resource}"),
            ("POST", "/api/crm/records/{resource}/public-web-promotions"),
        ):
            with self.subTest(path=path_template):
                foreign = self._request(
                    opener,
                    f"{base}{path_template.format(resource='rec-bob')}",
                    method=method,
                    body={"signal_id": "signal-1"},
                )
                missing = self._request(
                    opener,
                    f"{base}{path_template.format(resource='rec-missing')}",
                    method=method,
                    body={"signal_id": "signal-1"},
                )
                self.assertEqual(foreign, missing)
                self.assertEqual(
                    foreign,
                    (404, {"status": "not_found", "reason": "crm_record_not_found"}),
                )

    def test_excel_create_and_refine_use_server_owned_job_scope(self) -> None:
        base, opener, orchestrator = self._start_server()
        status, _ = self._request(
            opener,
            f"{base}/api/intake/excel/workflow",
            method="POST",
            body={"requester_id": "bob", "tenant_id": "user-bob", "file_content_base64": "eA=="},
        )
        self.assertEqual(status, 200)
        self.assertEqual(orchestrator.captured["excel_create"][-1]["requester_id"], "alice")
        self.assertEqual(orchestrator.captured["excel_create"][-1]["tenant_id"], "user-alice")

        for path, capture_name in (
            ("/api/results/refine/compile-instruction", "refine_compile"),
            ("/api/results/refine", "refine_apply"),
        ):
            for job_id, expected_status in (("job-alice", 200), ("job-bob", 404), ("job-missing", 404)):
                status, result = self._request(
                    opener,
                    f"{base}{path}",
                    method="POST",
                    body={"job_id": job_id, "request_patch": {"query": "refined"}},
                )
                self.assertEqual(status, expected_status)
                if expected_status == 404:
                    self.assertEqual(result, {"status": "not_found", "reason": "job_not_found"})
            self.assertEqual(orchestrator.captured[capture_name][0]["expected_requester_id"], "alice")
            self.assertEqual(orchestrator.captured[capture_name][0]["expected_tenant_id"], "user-alice")

    def test_authenticated_global_worker_controls_fail_closed_but_owned_job_scope_works(self) -> None:
        base, opener, orchestrator = self._start_server()
        for method, path, body in (
            ("GET", "/api/workers/recoverable", None),
            ("GET", "/api/workers/daemon/status", None),
            ("POST", "/api/workers/cleanup", {}),
            ("POST", "/api/runtime/services/shutdown", {}),
            ("POST", "/api/workers/daemon/run-once", {}),
            ("POST", "/api/workers/daemon/systemd-unit", {}),
        ):
            with self.subTest(path=path):
                status, result = self._request(opener, f"{base}{path}", method=method, body=body)
                self.assertEqual(status, 403)
                self.assertEqual(result, {"status": "forbidden", "reason": "admin_scope_required"})

        for path in ("/api/workers/recoverable", "/api/workers/daemon/status"):
            status, _ = self._request(opener, self._url(base, path, {"job_id": "job-alice"}))
            self.assertEqual(status, 200)
        status, daemon_status = self._request(
            opener,
            self._url(base, "/api/workers/daemon/status", {"job_id": "job-alice", "include_details": "true"}),
        )
        self.assertEqual(status, 200)
        self.assertEqual(set(daemon_status["recovery_services"]), {"job_scoped"})
        self.assertTrue(orchestrator.captured["worker_status"][-1]["authenticated_job_scope"])
        self.assertEqual(orchestrator.captured["worker_status"][-1]["expected_requester_id"], "alice")
        status, _ = self._request(
            opener,
            f"{base}/api/workers/cleanup",
            method="POST",
            body={"job_id": "job-alice"},
        )
        self.assertEqual(status, 200)
        status, _ = self._request(
            opener,
            f"{base}/api/runtime/services/shutdown",
            method="POST",
            body={"job_id": "job-alice", "service_name": "foreign-global-service"},
        )
        self.assertEqual(status, 403)
        status, _ = self._request(
            opener,
            f"{base}/api/runtime/services/shutdown",
            method="POST",
            body={"job_id": "job-alice"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(orchestrator.captured["worker_cleanup"][-1]["expected_requester_id"], "alice")
        self.assertEqual(orchestrator.captured["runtime_shutdown"][-1]["expected_tenant_id"], "user-alice")

        missing_worker = self._request(
            opener,
            f"{base}/api/workers/interrupt",
            method="POST",
            body={"worker_id": 99},
        )
        foreign_worker = self._request(
            opener,
            f"{base}/api/workers/interrupt",
            method="POST",
            body={"worker_id": 2},
        )
        self.assertEqual(missing_worker, foreign_worker)
        self.assertEqual(missing_worker, (404, {"status": "not_found", "reason": "job_not_found"}))

    def test_criteria_routes_always_propagate_exact_owner_and_hide_foreign_baselines(self) -> None:
        base, opener, orchestrator = self._start_server()
        endpoints = (
            ("/api/criteria/feedback", "criteria_feedback", 201),
            ("/api/criteria/confidence-policy", "criteria_confidence", 200),
            ("/api/criteria/suggestions/review", "criteria_suggestion", 200),
            ("/api/criteria/recompile", "criteria_recompile", 200),
        )
        for path, capture_name, success_status in endpoints:
            for rerun_payload in ({}, {"rerun_retrieval": False}, {"rerun_retrieval": True}):
                owned = self._request(
                    opener,
                    f"{base}{path}",
                    method="POST",
                    body={**rerun_payload, "job_id": "job-alice"},
                )
                foreign = self._request(
                    opener,
                    f"{base}{path}",
                    method="POST",
                    body={**rerun_payload, "job_id": "job-bob"},
                )
                missing = self._request(
                    opener,
                    f"{base}{path}",
                    method="POST",
                    body={**rerun_payload, "job_id": "job-missing"},
                )
                self.assertEqual(owned[0], success_status)
                self.assertEqual(foreign, missing)
                self.assertEqual(foreign, (404, {"status": "not_found", "reason": "job_not_found"}))
            self.assertTrue(orchestrator.captured[capture_name])
            for captured in orchestrator.captured[capture_name]:
                self.assertEqual(captured["expected_requester_id"], "alice")
                self.assertEqual(captured["expected_tenant_id"], "user-alice")

        for path, _capture_name, _success_status in endpoints:
            foreign = self._request(
                opener,
                f"{base}{path}",
                method="POST",
                body={"job_id": "job-alice", "baseline_job_id": "job-bob"},
            )
            missing = self._request(
                opener,
                f"{base}{path}",
                method="POST",
                body={"job_id": "job-alice", "baseline_job_id": "job-missing"},
            )
            self.assertEqual(foreign, missing)
            self.assertEqual(foreign, (404, {"status": "not_found", "reason": "job_not_found"}))

        source_owned = self._request(
            opener,
            f"{base}/api/criteria/confidence-policy",
            method="POST",
            body={"action": "override", "target_company": "OpenAI", "source_job_id": "job-alice"},
        )
        source_foreign = self._request(
            opener,
            f"{base}/api/criteria/confidence-policy",
            method="POST",
            body={"action": "override", "target_company": "OpenAI", "source_job_id": "job-bob"},
        )
        source_missing = self._request(
            opener,
            f"{base}/api/criteria/confidence-policy",
            method="POST",
            body={"action": "override", "target_company": "OpenAI", "source_job_id": "job-missing"},
        )
        self.assertEqual(source_owned[0], 200)
        self.assertEqual(source_foreign, source_missing)
        self.assertEqual(source_foreign, (404, {"status": "not_found", "reason": "job_not_found"}))

    def test_request_scope_registry_covers_public_job_and_worker_side_effects(self) -> None:
        expected = {
            ("POST", "/api/workflows"),
            ("POST", "/api/intake/excel/workflow"),
            ("POST", "/api/workflows/{job_id}/continue-stage2"),
            ("POST", "/api/jobs/{job_id}/profile-completion"),
            ("POST", "/api/results/refine/compile-instruction"),
            ("POST", "/api/results/refine"),
            ("POST", "/api/criteria/feedback"),
            ("POST", "/api/criteria/confidence-policy"),
            ("POST", "/api/criteria/suggestions/review"),
            ("POST", "/api/criteria/recompile"),
            ("POST", "/api/target-candidates/import-from-job"),
            ("POST", "/api/projections/backfill-from-job"),
            ("POST", "/api/workers/interrupt"),
            ("POST", "/api/workers/cleanup"),
            ("POST", "/api/workers/daemon/run-once"),
            ("POST", "/api/workers/daemon/systemd-unit"),
            ("POST", "/api/runtime/services/shutdown"),
            ("POST", "/api/jobs/{job_id}/cancel"),
            ("GET", "/api/operations/action-registry"),
            ("GET", "/api/operations/actions"),
            ("GET", "/api/operations/actions/{action_id}"),
            ("GET", "/api/operations/runs"),
            ("GET", "/api/operations/runs/{run_id}"),
            ("GET", "/api/operations/runs/{run_id}/provenance"),
            ("POST", "/api/operations/actions"),
            ("POST", "/api/operations/actions/{action_id}/approve"),
            ("POST", "/api/operations/actions/{action_id}/reject"),
            ("POST", "/api/operations/runs/{run_id}/cancel"),
            ("POST", "/api/operations/runs/{run_id}/retry"),
            ("POST", "/api/operations/runs/{run_id}/resume"),
            ("POST", "/api/operations/runs/{run_id}/dispatch"),
        }
        self.assertTrue(expected.issubset(AUTHENTICATED_REQUEST_SCOPE_REGISTRY))
        self.assertNotIn("unclassified", AUTHENTICATED_REQUEST_SCOPE_REGISTRY.values())

        def in_inventory(method: str, path: str) -> bool:
            if path.startswith("/api/workers"):
                return True
            if path.startswith("/api/operations"):
                return True
            if method != "POST":
                return False
            return (
                path.startswith("/api/workflows")
                or path.startswith("/api/jobs/")
                or path.startswith("/api/results/refine")
                or path
                in {
                    "/api/criteria/feedback",
                    "/api/criteria/confidence-policy",
                    "/api/criteria/suggestions/review",
                    "/api/criteria/recompile",
                }
                or path == "/api/intake/excel/workflow"
                or path == "/api/target-candidates/import-from-job"
                or path == "/api/projections/backfill-from-job"
                or path == "/api/runtime/services/shutdown"
            )

        discovered = set()
        for route in _build_routes(_ScopeOrchestrator()):
            normalized_path = re.sub(r"\{([^}:]+):[^}]+\}", r"{\1}", route.path)
            for method in set(route.methods or ()) - {"HEAD", "OPTIONS"}:
                if in_inventory(method, normalized_path):
                    discovered.add((method, normalized_path))
        self.assertEqual(discovered, set(AUTHENTICATED_REQUEST_SCOPE_REGISTRY))

    def test_candidate_batch_is_post_read_and_retains_legacy_read_policy(self) -> None:
        base, opener, orchestrator = self._start_server()
        for job_id, expected_status in (("job-alice", 200), ("job-legacy", 200), ("job-bob", 404)):
            with self.subTest(job_id=job_id):
                status, _ = self._request(
                    opener,
                    f"{base}/api/jobs/{job_id}/candidates/batch",
                    method="POST",
                    body={"candidate_ids": ["candidate-1"]},
                )
                self.assertEqual(status, expected_status)
        self.assertEqual(len(orchestrator.captured["candidate_batch"]), 2)
        foreign = self._request(
            opener,
            f"{base}/api/jobs/job-bob/candidates/batch",
            method="POST",
            body={"candidate_ids": ["candidate-1"]},
        )
        missing = self._request(
            opener,
            f"{base}/api/jobs/job-missing/candidates/batch",
            method="POST",
            body={"candidate_ids": ["candidate-1"]},
        )
        self.assertEqual(foreign, missing)
        self.assertEqual(foreign, (404, {"status": "not_found", "reason": "job_not_found"}))

    def test_job_derived_migration_writes_require_exact_source_job_owner(self) -> None:
        base, opener, orchestrator = self._start_server()
        endpoints = (
            ("/api/target-candidates/import-from-job", "target_import", 201),
            ("/api/projections/backfill-from-job", "projection_backfill", 201),
        )
        for path, capture_name, success_status in endpoints:
            for job_id, expected_status in (
                ("job-alice", success_status),
                ("job-alice-wrong-tenant", 404),
                ("job-bob", 404),
                ("job-legacy", 404),
            ):
                with self.subTest(path=path, job_id=job_id):
                    status, _ = self._request(
                        opener,
                        f"{base}{path}",
                        method="POST",
                        body={"job_id": job_id},
                    )
                    self.assertEqual(status, expected_status)
            self.assertEqual(len(orchestrator.captured.get(capture_name, [])), 1)

    def test_authenticated_crm_writes_are_server_scoped_and_exact_owner_gated(self) -> None:
        base, opener, orchestrator = self._start_server()
        status, _ = self._request(
            opener,
            f"{base}/api/crm/records",
            method="POST",
            body={"workspace_id": "user-bob", "actor_id": "bob", "actor_type": "service"},
        )
        self.assertEqual(status, 201)
        self.assertEqual(orchestrator.captured["crm_create"][-1]["workspace_id"], "user-alice")
        self.assertEqual(orchestrator.captured["crm_create"][-1]["actor_id"], "alice")
        self.assertEqual(orchestrator.captured["crm_create"][-1]["actor_type"], "user")

        endpoints = (
            ("PATCH", "/api/crm/records/{record_id}", "crm_update", 200),
            ("POST", "/api/crm/records/{record_id}/public-web-promotions", "crm_promotion", 201),
        )
        for method, path_template, capture_name, success_status in endpoints:
            for record_id, expected_status in (
                ("rec-alice", success_status),
                ("rec-alice-owned", success_status),
                ("rec-alice-owner-bob", 404),
                ("rec-bob", 404),
                ("rec-legacy", 404),
            ):
                with self.subTest(path=path_template, record_id=record_id):
                    status, _ = self._request(
                        opener,
                        f"{base}{path_template.format(record_id=record_id)}",
                        method=method,
                        body={"workspace_id": "user-bob", "signal_id": "signal-1"},
                    )
                    self.assertEqual(status, expected_status)
            self.assertEqual(len(orchestrator.captured.get(capture_name, [])), 2)

        for captured_update in orchestrator.captured["crm_update"]:
            self.assertEqual(captured_update["workspace_id"], "user-alice")

        for path, capture_name in (
            ("/api/crm/backfill-target-candidates", "crm_backfill"),
            ("/api/crm/backfill-public-web-promotions", "crm_promotion_backfill"),
        ):
            status, _ = self._request(
                opener,
                f"{base}{path}",
                method="POST",
                body={"workspace_id": "user-bob"},
            )
            self.assertEqual(status, 200)
            self.assertEqual(orchestrator.captured[capture_name][-1]["workspace_id"], "user-alice")

    def test_operation_crm_action_transport_derives_owner_and_preserves_open_mode(self) -> None:
        base, opener, orchestrator = self._start_server()
        for action_type in ("set_crm_stage", "add_crm_note", "create_crm_task", "enrich_person_public_web"):
            with self.subTest(action_type=action_type):
                target_ref = (
                    {"crm_record_ids": ["rec-alice-owned"]}
                    if action_type == "enrich_person_public_web"
                    else {"crm_record_id": "rec-alice-owned"}
                )
                status, _ = self._request(
                    opener,
                    f"{base}/api/operations/actions",
                    method="POST",
                    body={
                        "action_type": action_type,
                        "workspace_id": "user-bob",
                        "actor": "bob",
                        "target_ref": target_ref,
                        "input": {},
                    },
                )
                self.assertEqual(status, 202)
                captured = orchestrator.captured["operation_action"][-1]
                self.assertEqual(captured["workspace_id"], "user-alice")
                self.assertEqual(captured["actor"], "alice")
                self.assertEqual(captured["expected_workspace_id"], "user-alice")
                self.assertEqual(captured["expected_owner_user_id"], "alice")

        root_status, _ = self._request(
            opener,
            f"{base}/api/operations/actions",
            method="POST",
            body={
                "action_type": "start_acquisition_run",
                "workspace_id": "user-bob",
                "actor": "bob",
                "input": {
                    "target_company": "Thinking Machines Lab",
                    "query": "pre-training researchers",
                },
            },
        )
        self.assertEqual(root_status, 202)
        captured_root = orchestrator.captured["operation_action"][-1]
        self.assertEqual(captured_root["workspace_id"], "user-alice")
        self.assertEqual(captured_root["actor"], "alice")
        self.assertEqual(captured_root["expected_workspace_id"], "user-alice")
        self.assertEqual(captured_root["expected_owner_user_id"], "alice")
        self.assertNotIn("target_ref", captured_root)

        replay_status, replay = self._request(
            opener,
            f"{base}/api/operations/actions",
            method="POST",
            body={
                "action_type": "add_crm_note",
                "target_ref": {"crm_record_id": "rec-alice-owned"},
                "input": {"note": "already applied"},
                "idempotency_key": "completed-replay",
            },
        )
        self.assertEqual(replay_status, 200)
        self.assertEqual(replay.get("status"), "completed", replay)
        self.assertTrue(replay.get("idempotent_replay"), replay)
        self.assertEqual(replay["action"]["status"], "completed")
        self.assertEqual(replay["operation_run"]["status"], "completed")

        invalid_replay_status, invalid_replay = self._request(
            opener,
            f"{base}/api/operations/actions",
            method="POST",
            body={
                "action_type": "add_crm_note",
                "target_ref": {"crm_record_id": "rec-alice-owned"},
                "input": {"note": "invalid replay flag must not override failure"},
                "idempotency_key": "invalid-replay-flag",
            },
        )
        self.assertEqual(invalid_replay_status, 400)
        self.assertEqual(invalid_replay.get("status"), "invalid", invalid_replay)

        whitespace_replay_status, whitespace_replay = self._request(
            opener,
            f"{base}/api/operations/actions",
            method="POST",
            body={
                "action_type": "add_crm_note",
                "target_ref": {"crm_record_id": "rec-alice-owned"},
                "input": {"note": "malformed replay status"},
                "idempotency_key": "replay-status-whitespace",
            },
        )
        self.assertEqual(whitespace_replay_status, 400)
        self.assertEqual(
            whitespace_replay,
            {"status": " completed ", "idempotent_replay": True, "module_state_mutated": False},
        )

        malformed_submission_results = {
            "replay-status-list": {
                "status": ["completed"],
                "idempotent_replay": True,
                "module_state_mutated": False,
            },
            "replay-status-dict": {
                "status": {"value": "completed"},
                "idempotent_replay": True,
                "module_state_mutated": False,
            },
            "fresh-replay-flag-missing": {"status": "queued", "module_state_mutated": False},
            "fresh-replay-flag-null": {
                "status": "queued",
                "idempotent_replay": None,
                "module_state_mutated": False,
            },
            "fresh-replay-flag-string": {
                "status": "queued",
                "idempotent_replay": "false",
                "module_state_mutated": False,
            },
            "fresh-status-list": {
                "status": ["queued"],
                "idempotent_replay": False,
                "module_state_mutated": False,
            },
            "fresh-status-dict": {
                "status": {"value": "queued"},
                "idempotent_replay": False,
                "module_state_mutated": False,
            },
        }
        for idempotency_key, expected_body in malformed_submission_results.items():
            with self.subTest(idempotency_key=idempotency_key):
                malformed_status, malformed_body = self._request(
                    opener,
                    f"{base}/api/operations/actions",
                    method="POST",
                    body={
                        "action_type": "add_crm_note",
                        "target_ref": {"crm_record_id": "rec-alice-owned"},
                        "input": {"note": "malformed fresh replay marker"},
                        "idempotency_key": idempotency_key,
                    },
                )
                self.assertEqual(malformed_status, 400)
                self.assertEqual(malformed_body, expected_body)

        foreign = self._request(
            opener,
            f"{base}/api/operations/actions",
            method="POST",
            body={
                "action_type": "add_crm_note",
                "target_ref": {"crm_record_id": "rec-bob"},
                "input": {"note": "hidden"},
            },
        )
        missing = self._request(
            opener,
            f"{base}/api/operations/actions",
            method="POST",
            body={
                "action_type": "add_crm_note",
                "target_ref": {"crm_record_id": "rec-missing"},
                "input": {"note": "hidden"},
            },
        )
        self.assertEqual(foreign, missing)
        self.assertEqual(foreign, (404, {"status": "not_found", "reason": "crm_record_not_found"}))

        open_base, open_opener, open_orchestrator = self._start_server(authenticated=False)
        status, _ = self._request(
            open_opener,
            f"{open_base}/api/operations/actions",
            method="POST",
            body={
                "action_type": "create_crm_task",
                "workspace_id": "default",
                "actor": "legacy-operator",
                "target_ref": {"crm_record_id": "rec-legacy"},
                "input": {"title": "Follow up"},
            },
            token=None,
        )
        self.assertEqual(status, 202)
        captured = open_orchestrator.captured["operation_action"][-1]
        self.assertEqual(captured["workspace_id"], "default")
        self.assertEqual(captured["actor"], "legacy-operator")
        self.assertNotIn("expected_workspace_id", captured)
        self.assertNotIn("expected_owner_user_id", captured)

        root_status, _ = self._request(
            open_opener,
            f"{open_base}/api/operations/actions",
            method="POST",
            body={
                "action_type": "start_acquisition_run",
                "workspace_id": "operator-workspace",
                "actor": "legacy-operator",
                "input": {
                    "target_company": "Thinking Machines Lab",
                    "query": "pre-training researchers",
                },
            },
            token=None,
        )
        self.assertEqual(root_status, 202)
        captured_root = open_orchestrator.captured["operation_action"][-1]
        self.assertEqual(captured_root["workspace_id"], "operator-workspace")
        self.assertEqual(captured_root["actor"], "legacy-operator")
        self.assertNotIn("expected_workspace_id", captured_root)
        self.assertNotIn("expected_owner_user_id", captured_root)

    def test_authenticated_operation_reads_and_controls_use_exact_workspace_scope(self) -> None:
        base, opener, orchestrator = self._start_server()

        registry = self._request(opener, f"{base}/api/operations/action-registry")
        self.assertEqual(registry[0], 200)
        for path, capture_name in (
            ("/api/operations/actions?workspace_id=user-bob", "operation_actions"),
            ("/api/operations/runs?workspace_id=user-bob", "operation_runs"),
        ):
            status, _ = self._request(opener, f"{base}{path}")
            self.assertEqual(status, 200)
            self.assertEqual(orchestrator.captured[capture_name][-1]["workspace_id"], "user-alice")
            self.assertEqual(orchestrator.captured[capture_name][-1]["expected_workspace_id"], "user-alice")

        action_not_found = (404, {"status": "not_found", "reason": "operation_action_not_found"})
        for suffix in ("", "/approve", "/reject"):
            method = "GET" if not suffix else "POST"
            foreign = self._request(
                opener,
                f"{base}/api/operations/actions/action-bob{suffix}",
                method=method,
                body={"actor": "bob"} if suffix else None,
            )
            missing = self._request(
                opener,
                f"{base}/api/operations/actions/action-missing{suffix}",
                method=method,
                body={"actor": "bob"} if suffix else None,
            )
            self.assertEqual(foreign, missing)
            self.assertEqual(foreign, action_not_found)

        run_not_found = (404, {"status": "not_found", "reason": "operation_run_not_found"})
        run_suffixes = (
            ("", "GET"),
            ("/provenance", "GET"),
            ("/cancel", "POST"),
            ("/retry", "POST"),
            ("/resume", "POST"),
            ("/dispatch", "POST"),
        )
        for suffix, method in run_suffixes:
            foreign = self._request(
                opener,
                f"{base}/api/operations/runs/run-bob{suffix}",
                method=method,
                body={"actor": "bob"} if method == "POST" else None,
            )
            missing = self._request(
                opener,
                f"{base}/api/operations/runs/run-missing{suffix}",
                method=method,
                body={"actor": "bob"} if method == "POST" else None,
            )
            self.assertEqual(foreign, missing)
            self.assertEqual(foreign, run_not_found)

        for path, method, expected_status in (
            ("/api/operations/actions/action-alice", "GET", 200),
            ("/api/operations/actions/action-alice/approve", "POST", 202),
            ("/api/operations/actions/action-alice/reject", "POST", 200),
            ("/api/operations/runs/run-alice", "GET", 200),
            ("/api/operations/runs/run-alice/provenance", "GET", 200),
            ("/api/operations/runs/run-alice/cancel", "POST", 200),
            ("/api/operations/runs/run-alice/retry", "POST", 202),
            ("/api/operations/runs/run-alice/resume", "POST", 202),
            ("/api/operations/runs/run-alice/dispatch", "POST", 202),
        ):
            status, _ = self._request(
                opener,
                f"{base}{path}",
                method=method,
                body={"actor": "bob"} if method == "POST" else None,
            )
            self.assertEqual(status, expected_status)

        for capture_name in (
            "operation_action_approve",
            "operation_action_reject",
            "operation_run_cancel",
            "operation_run_retry",
            "operation_run_resume",
            "operation_run_dispatch",
        ):
            captured = orchestrator.captured[capture_name][-1]
            self.assertEqual(captured["actor"], "alice")
            self.assertEqual(captured["expected_workspace_id"], "user-alice")

        open_base, open_opener, open_orchestrator = self._start_server(authenticated=False)
        status, _ = self._request(
            open_opener,
            f"{open_base}/api/operations/runs?workspace_id=operator-workspace",
            token=None,
        )
        self.assertEqual(status, 200)
        self.assertEqual(open_orchestrator.captured["operation_runs"][-1]["workspace_id"], "operator-workspace")
        self.assertNotIn("expected_workspace_id", open_orchestrator.captured["operation_runs"][-1])
        status, _ = self._request(
            open_opener,
            f"{open_base}/api/operations/runs/run-bob/resume",
            method="POST",
            body={"actor": "legacy-operator"},
            token=None,
        )
        self.assertEqual(status, 202)
        self.assertEqual(open_orchestrator.captured["operation_run_resume"][-1]["actor"], "legacy-operator")
        self.assertNotIn("expected_workspace_id", open_orchestrator.captured["operation_run_resume"][-1])

    def test_open_mode_preserves_read_and_write_payloads(self) -> None:
        base, opener, orchestrator = self._start_server(authenticated=False)
        status, result = self._request(
            opener,
            self._url(base, "/api/query-dispatches", {"requester_id": "legacy", "tenant_id": "custom"}),
            token=None,
        )
        self.assertEqual(status, 200)
        self.assertEqual(result["scope"], {"requester_id": "legacy", "tenant_id": "custom"})

        status, _ = self._request(
            opener,
            f"{base}/api/crm/records/rec-legacy",
            method="PATCH",
            body={"workspace_id": "custom", "actor_id": "legacy"},
            token=None,
        )
        self.assertEqual(status, 200)
        self.assertEqual(orchestrator.captured["crm_update"][-1]["workspace_id"], "custom")
        self.assertEqual(orchestrator.captured["crm_update"][-1]["actor_id"], "legacy")

        for path, capture_name in (
            ("/api/workers/cleanup", "worker_cleanup"),
            ("/api/runtime/services/shutdown", "runtime_shutdown"),
            ("/api/workers/daemon/run-once", "shared_recovery"),
            ("/api/workers/daemon/systemd-unit", "systemd_unit"),
        ):
            status, _ = self._request(
                opener,
                f"{base}{path}",
                method="POST",
                body={"service_name": "legacy-operator-service"},
                token=None,
            )
            self.assertIn(status, {200, 202})
            self.assertTrue(orchestrator.captured[capture_name])


if __name__ == "__main__":
    unittest.main()
