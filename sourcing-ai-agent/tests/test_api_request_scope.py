"""C2.5 authenticated request scoping and exact write-authority closure."""

import json
import os
import threading
import unittest
from unittest.mock import patch
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.error import HTTPError

from sourcing_agent.api import (
    _apply_server_read_scope,
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

    def list_query_dispatches(self, payload):
        self._capture("query_dispatches", payload)
        return {"query_dispatches": [], "scope": dict(payload)}

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

    def get_serving_projection_person_detail_api(self, projection_id, person_key, *, workspace_id="default"):
        self._capture(
            "projection_person",
            {"projection_id": projection_id, "person_key": person_key, "workspace_id": workspace_id},
        )
        return {"status": "ready", "workspace_id": workspace_id}

    def get_person_summary_api(self, person_key, *, workspace_id="default"):
        self._capture("person_summary", {"person_key": person_key, "workspace_id": workspace_id})
        return {"status": "ready", "workspace_id": workspace_id}

    def continue_workflow_stage2(self, payload):
        self._capture("continue_stage2", payload)
        return {"status": "queued"}

    def complete_job_candidate_profiles(self, job_id, payload):
        self._capture("profile_completion", {"job_id": job_id, **dict(payload)})
        return {"status": "completed"}

    def get_job_candidate_details_batch(self, job_id, candidate_ids):
        self._capture("candidate_batch", {"job_id": job_id, "candidate_ids": list(candidate_ids)})
        return {"status": "ready", "candidates": []}

    def cancel_workflow_job(self, job_id, payload):
        self._capture("job_cancel", {"job_id": job_id, **dict(payload)})
        return {"status": "cancelled"}

    def add_projection_candidate_to_crm(self, payload):
        self._capture("crm_create", payload)
        return {"status": "upserted"}

    def import_target_candidates_from_job(self, payload):
        self._capture("target_import", payload)
        return {"status": "imported"}

    def backfill_serving_projection_for_job(self, payload):
        self._capture("projection_backfill", payload)
        return {"status": "backfilled"}

    def backfill_crm_from_target_candidates(self, payload):
        self._capture("crm_backfill", payload)
        return {"status": "backfilled"}

    def backfill_public_web_promotions_to_person_assertions(self, payload):
        self._capture("crm_promotion_backfill", payload)
        return {"status": "backfilled"}

    def update_crm_record_api(self, record_id, payload):
        self._capture("crm_update", {"record_id": record_id, **dict(payload)})
        return {"status": "updated"}

    def promote_crm_record_public_web_signal(self, record_id, payload):
        self._capture("crm_promotion", {"record_id": record_id, **dict(payload)})
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


if __name__ == "__main__":
    unittest.main()
