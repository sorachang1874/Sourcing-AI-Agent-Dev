"""C2.2 server-derived identity: client-supplied ownership/identity fields are
overridden + stripped from the authenticated identity at submit boundaries, and
open mode (no SOURCING_API_BEARER_TOKENS) is an exact no-op.

Covers _user_namespace + _apply_server_identity directly, plus end-to-end proof
through create_server with a payload-capturing stub orchestrator (spoof ignored
when authed; unchanged when open).
"""

import json
import os
import threading
import unittest
from unittest.mock import patch
from urllib import request as urllib_request
from urllib.error import HTTPError

from sourcing_agent.api import _apply_server_identity, _user_namespace, create_server


class _FakeState:
    def __init__(self, identity):
        self.identity = identity


class _FakeRequest:
    def __init__(self, identity):
        self.state = _FakeState(identity)


class UserNamespaceTest(unittest.TestCase):
    def test_prefixed_and_deterministic(self) -> None:
        self.assertEqual(_user_namespace("alice"), "user-alice")
        self.assertEqual(_user_namespace("alice"), _user_namespace("alice"))

    def test_distinct_per_user(self) -> None:
        self.assertNotEqual(_user_namespace("alice"), _user_namespace("bob"))

    def test_blank_is_empty(self) -> None:
        self.assertEqual(_user_namespace(""), "")
        self.assertEqual(_user_namespace("   "), "")


_SPOOFED = {
    "requester_id": "attacker",
    "user_id": "x",
    "requester": {"id": "y"},
    "tenant_id": "foreign",
    "workspace_id": "foreign",
    "org_id": "z",
    "operator": "evil",
    "actor": "evil",
    "requested_by": "evil",
    "reviewer": "evil",
    "actor_id": "evil",
    "owner_user_id": "evil",
    "actor_type": "service",
}


class ApplyServerIdentityTest(unittest.TestCase):
    def test_open_mode_is_exact_noop(self) -> None:
        for flags in (
            {"requester": True, "tenant": True},
            {"workspace": True, "actor_fields": ("operator", "requested_by"), "owner": True},
            {"actor_fields": ("actor",), "lock_actor_type": True},
        ):
            payload = dict(_SPOOFED)
            before = dict(payload)
            result = _apply_server_identity(payload, _FakeRequest(None), **flags)
            self.assertIs(result, payload)
            self.assertEqual(payload, before)

    def test_missing_user_id_is_noop(self) -> None:
        payload = dict(_SPOOFED)
        before = dict(payload)
        _apply_server_identity(payload, _FakeRequest({"user_id": ""}), requester=True, tenant=True)
        self.assertEqual(payload, before)

    def test_requester_and_tenant_override_and_strip(self) -> None:
        payload = dict(_SPOOFED)
        _apply_server_identity(payload, _FakeRequest({"user_id": "u1"}), requester=True, tenant=True)
        self.assertEqual(payload["requester_id"], "u1")
        self.assertEqual(payload["tenant_id"], "user-u1")
        for stripped in ("user_id", "requester", "workspace_id", "org_id"):
            self.assertNotIn(stripped, payload)

    def test_workspace_override(self) -> None:
        payload = dict(_SPOOFED)
        _apply_server_identity(payload, _FakeRequest({"user_id": "u1"}), workspace=True)
        self.assertEqual(payload["workspace_id"], "user-u1")

    def test_actor_fields_and_owner_and_lock(self) -> None:
        payload = dict(_SPOOFED)
        _apply_server_identity(
            payload,
            _FakeRequest({"user_id": "u1"}),
            actor_fields=("operator", "requested_by", "actor_id"),
            owner=True,
            lock_actor_type=True,
        )
        self.assertEqual(payload["operator"], "u1")
        self.assertEqual(payload["requested_by"], "u1")
        self.assertEqual(payload["actor_id"], "u1")
        self.assertEqual(payload["owner_user_id"], "u1")
        self.assertEqual(payload["actor_type"], "user")


class _CapturingOrchestrator:
    def __init__(self) -> None:
        self.captured: dict = {}

    def get_runtime_metrics(self, _query):
        return {"status": "ok"}

    def explain_workflow(self, payload):
        self.captured["explain"] = dict(payload)
        return {"status": "ok"}

    def start_crm_record_public_web_search(self, payload):
        self.captured["crm_search"] = dict(payload)
        return {"status": "queued"}


class ServerIdentityWiringTest(unittest.TestCase):
    def _start_server(self, orchestrator, env=None):
        env_patch = patch.dict(os.environ, dict(env or {}))
        env_patch.start()
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
        return f"http://{host}:{port}", opener

    def _post(self, opener, url, body, *, headers=None):
        data = json.dumps(body).encode("utf-8")
        request = urllib_request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", **(headers or {})},
            method="POST",
        )
        try:
            with opener.open(request, timeout=10) as response:
                return response.status
        except HTTPError as error:
            return error.code

    _TOKENS = json.dumps({"tok-1": "u1"})

    def test_authed_dispatch_overrides_spoofed_requester_and_tenant(self) -> None:
        orch = _CapturingOrchestrator()
        base_url, opener = self._start_server(orch, env={"SOURCING_API_BEARER_TOKENS": self._TOKENS})
        self._post(
            opener,
            f"{base_url}/api/workflows/explain",
            {"requester_id": "attacker", "tenant_id": "foreign", "workspace_id": "foreign", "user_id": "x"},
            headers={"Authorization": "Bearer tok-1"},
        )
        captured = orch.captured["explain"]
        self.assertEqual(captured["requester_id"], "u1")
        self.assertEqual(captured["tenant_id"], "user-u1")
        for stripped in ("user_id", "workspace_id", "org_id", "requester"):
            self.assertNotIn(stripped, captured)

    def test_authed_crm_search_overrides_spoofed_workspace(self) -> None:
        orch = _CapturingOrchestrator()
        base_url, opener = self._start_server(orch, env={"SOURCING_API_BEARER_TOKENS": self._TOKENS})
        self._post(
            opener,
            f"{base_url}/api/crm/records/public-web-search",
            {"workspace_id": "foreign", "record_id": "r1"},
            headers={"Authorization": "Bearer tok-1"},
        )
        captured = orch.captured["crm_search"]
        self.assertEqual(captured["workspace_id"], "user-u1")
        self.assertEqual(captured["owner_user_id"], "u1")
        self.assertEqual(captured["requested_by"], "u1")

    def test_open_mode_preserves_client_requester(self) -> None:
        orch = _CapturingOrchestrator()
        base_url, opener = self._start_server(orch, env={})
        self._post(
            opener,
            f"{base_url}/api/workflows/explain",
            {"requester_id": "legacy", "tenant_id": "legacy-tenant"},
        )
        captured = orch.captured["explain"]
        self.assertEqual(captured["requester_id"], "legacy")
        self.assertEqual(captured["tenant_id"], "legacy-tenant")


if __name__ == "__main__":
    unittest.main()
