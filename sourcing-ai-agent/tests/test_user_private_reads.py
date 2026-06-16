"""C2.3 user-private reads: a non-owner gets 404 (not 403, to deny existence),
the owner reads normally, legacy/pre-auth rows ('' / 'default') stay readable by
anyone, open mode is a strict no-op, and a genuinely-missing resource is
indistinguishable from a forbidden one.

Covers the _read_allowed_* predicates directly, plus end-to-end gating through
create_server for a job route (requester_id) and a CRM record-by-id subresource
(workspace_id) — the two gate kinds.
"""

import json
import os
import threading
import unittest
from unittest.mock import patch
from urllib import request as urllib_request
from urllib.error import HTTPError

from sourcing_agent.api import _read_allowed_namespace, _read_allowed_requester, create_server


class _FakeState:
    def __init__(self, identity):
        self.identity = identity


class _FakeRequest:
    def __init__(self, identity):
        self.state = _FakeState(identity)


class ReadAllowedRequesterTest(unittest.TestCase):
    def test_open_mode_allows_any(self) -> None:
        self.assertTrue(_read_allowed_requester(_FakeRequest(None), "someone-else"))

    def test_legacy_sentinels_readable_by_anyone(self) -> None:
        self.assertTrue(_read_allowed_requester(_FakeRequest({"user_id": "alice"}), ""))
        self.assertTrue(_read_allowed_requester(_FakeRequest({"user_id": "alice"}), "default"))

    def test_owner_match(self) -> None:
        self.assertTrue(_read_allowed_requester(_FakeRequest({"user_id": "alice"}), "alice"))

    def test_owner_mismatch_denied(self) -> None:
        self.assertFalse(_read_allowed_requester(_FakeRequest({"user_id": "bob"}), "alice"))


class ReadAllowedNamespaceTest(unittest.TestCase):
    def test_open_mode_allows_any(self) -> None:
        self.assertTrue(_read_allowed_namespace(_FakeRequest(None), "user-alice"))

    def test_legacy_sentinels_readable(self) -> None:
        self.assertTrue(_read_allowed_namespace(_FakeRequest({"user_id": "alice"}), ""))
        self.assertTrue(_read_allowed_namespace(_FakeRequest({"user_id": "alice"}), "default"))

    def test_namespace_match(self) -> None:
        self.assertTrue(_read_allowed_namespace(_FakeRequest({"user_id": "alice"}), "user-alice"))

    def test_namespace_mismatch_denied(self) -> None:
        self.assertFalse(_read_allowed_namespace(_FakeRequest({"user_id": "bob"}), "user-alice"))
        # the bare user_id is NOT the stored namespace form -> denied
        self.assertFalse(_read_allowed_namespace(_FakeRequest({"user_id": "alice"}), "alice"))


class _StubStore:
    def __init__(self, jobs, crm_records, commands=None, links=None):
        self._jobs = jobs
        self._crm = crm_records
        self._commands = commands or {}
        self._links = links or {}

    def get_job(self, job_id):
        return self._jobs.get(job_id)

    def get_crm_record(self, record_id):
        return self._crm.get(record_id)

    def get_workflow_command(self, command_id):
        return self._commands.get(command_id, {})

    def get_frontend_history_link(self, history_id):
        return self._links.get(history_id)


class _StubOrchestrator:
    def __init__(self, store):
        self.store = store

    def get_runtime_metrics(self, _query):
        return {"status": "ok"}

    def get_job_api(self, job_id, include_details=False):
        if self.store.get_job(job_id) is None:
            return None
        return {"job_id": job_id, "status": "ok"}

    def get_crm_record_public_web_search_detail(self, record_id):
        if self.store.get_crm_record(record_id) is None:
            return {"status": "not_found", "record_id": record_id}
        return {"status": "ok", "record_id": record_id}

    def get_export_command_status(self, command_id):
        return {"status": "succeeded", "task_id": command_id}

    def get_frontend_history_recovery(self, history_id):
        return {"status": "ok", "history_id": history_id}

    def delete_frontend_history(self, history_id):
        return {"status": "ok", "history_id": history_id}

    def list_frontend_history(self, limit=24):
        items = [
            {"history_id": "h1", "job_id": "job-alice"},
            {"history_id": "h2", "job_id": "job-legacy-empty"},
            {"history_id": "h3", "job_id": ""},
        ]
        return {"history": items, "count": len(items)}


_TOKENS = json.dumps({"tok-alice": "alice", "tok-bob": "bob"})


class UserPrivateReadGateTest(unittest.TestCase):
    def _start_server(self, env=None):
        store = _StubStore(
            jobs={
                "job-alice": {"requester_id": "alice"},
                "job-legacy-empty": {"requester_id": ""},
                "job-legacy-default": {"requester_id": "default"},
            },
            crm_records={
                "rec-alice": {"workspace_id": "user-alice"},
                "rec-legacy": {"workspace_id": "default"},
            },
            commands={
                "exp-alice-crm": {"payload": {"workspace_id": "user-alice"}},
                "exp-legacy-crm": {"payload": {"workspace_id": "default"}},
                "exp-projection": {"payload": {}},
            },
            links={
                "hist-alice": {"job_id": "job-alice"},
                "hist-legacy": {"job_id": "job-legacy-empty"},
                "hist-unlinked": {"job_id": ""},
            },
        )
        env_patch = patch.dict(os.environ, dict(env or {}))
        env_patch.start()
        try:
            server = create_server(_StubOrchestrator(store), host="127.0.0.1", port=0)
        finally:
            env_patch.stop()
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        return f"http://{host}:{port}", opener

    def _get(self, opener, url, *, token=None):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        request = urllib_request.Request(url, headers=headers, method="GET")
        try:
            with opener.open(request, timeout=10) as response:
                return response.status
        except HTTPError as error:
            return error.code

    # ---- job route (requester_id gate) ----
    def test_owner_reads_own_job(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._get(opener, f"{base}/api/jobs/job-alice", token="tok-alice"), 200)

    def test_non_owner_job_is_404(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._get(opener, f"{base}/api/jobs/job-alice", token="tok-bob"), 404)

    def test_legacy_jobs_readable_by_anyone(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._get(opener, f"{base}/api/jobs/job-legacy-empty", token="tok-bob"), 200)
        self.assertEqual(self._get(opener, f"{base}/api/jobs/job-legacy-default", token="tok-bob"), 200)

    def test_open_mode_reads_any_job(self) -> None:
        base, opener = self._start_server(env={})
        self.assertEqual(self._get(opener, f"{base}/api/jobs/job-alice"), 200)

    def test_missing_job_is_404_for_owner_too(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._get(opener, f"{base}/api/jobs/job-missing", token="tok-alice"), 404)

    # ---- CRM record-by-id subresource (workspace_id gate) ----
    def test_owner_reads_own_crm_subresource(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(
            self._get(opener, f"{base}/api/crm/records/rec-alice/public-web-search", token="tok-alice"), 200
        )

    def test_non_owner_crm_subresource_is_404(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._get(opener, f"{base}/api/crm/records/rec-alice/public-web-search", token="tok-bob"), 404)

    def test_legacy_crm_record_readable_by_anyone(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(
            self._get(opener, f"{base}/api/crm/records/rec-legacy/public-web-search", token="tok-bob"), 200
        )

    # ---- export commands (workspace_id gate; projection canonical -> open) ----
    def test_owner_reads_own_crm_export(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._get(opener, f"{base}/api/exports/exp-alice-crm", token="tok-alice"), 200)

    def test_non_owner_crm_export_is_404(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._get(opener, f"{base}/api/exports/exp-alice-crm", token="tok-bob"), 404)

    def test_legacy_crm_export_readable_by_anyone(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._get(opener, f"{base}/api/exports/exp-legacy-crm", token="tok-bob"), 200)

    def test_projection_export_is_canonical_and_open(self) -> None:
        # No workspace_id on the command -> canonical-derived -> readable by anyone.
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._get(opener, f"{base}/api/exports/exp-projection", token="tok-bob"), 200)

    # ---- frontend-history (owner derived from the linked job's requester_id) ----
    def _delete(self, opener, url, *, token=None):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        request = urllib_request.Request(url, headers=headers, method="DELETE")
        try:
            with opener.open(request, timeout=10) as response:
                return response.status
        except HTTPError as error:
            return error.code

    def test_owner_reads_own_history(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._get(opener, f"{base}/api/frontend-history/hist-alice", token="tok-alice"), 200)

    def test_non_owner_history_is_404(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._get(opener, f"{base}/api/frontend-history/hist-alice", token="tok-bob"), 404)

    def test_legacy_and_unlinked_history_readable(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._get(opener, f"{base}/api/frontend-history/hist-legacy", token="tok-bob"), 200)
        self.assertEqual(self._get(opener, f"{base}/api/frontend-history/hist-unlinked", token="tok-bob"), 200)

    def test_non_owner_cannot_delete_history(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        self.assertEqual(self._delete(opener, f"{base}/api/frontend-history/hist-alice", token="tok-bob"), 404)

    # ---- frontend-history LIST (owner-filtered) ----
    def _get_json(self, opener, url, *, token=None):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        request = urllib_request.Request(url, headers=headers, method="GET")
        with opener.open(request, timeout=10) as response:
            return json.loads(response.read())

    def test_history_list_filters_out_other_users(self) -> None:
        base, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": _TOKENS})
        alice = self._get_json(opener, f"{base}/api/frontend-history", token="tok-alice")
        self.assertEqual({i["job_id"] for i in alice["history"]}, {"job-alice", "job-legacy-empty", ""})
        bob = self._get_json(opener, f"{base}/api/frontend-history", token="tok-bob")
        # bob does not see alice's entry; legacy + unlinked stay visible.
        self.assertEqual({i["job_id"] for i in bob["history"]}, {"job-legacy-empty", ""})
        self.assertEqual(bob["count"], 2)

    def test_history_list_open_mode_unfiltered(self) -> None:
        base, opener = self._start_server(env={})
        result = self._get_json(opener, f"{base}/api/frontend-history")
        self.assertEqual(result["count"], 3)


if __name__ == "__main__":
    unittest.main()
