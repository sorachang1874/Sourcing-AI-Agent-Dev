"""C2.1 auth foundation: bearer-token gate, identity injection, public exemptions.

Covers _AuthMiddleware directly at the ASGI layer (precise 401 / identity /
exemption / open-mode behavior), the SOURCING_API_BEARER_TOKENS parser, and
end-to-end wiring through create_server (auth runs before routing, inside CORS).
"""

import asyncio
import json
import os
import threading
import unittest
from http import HTTPStatus
from unittest.mock import patch
from urllib import request as urllib_request
from urllib.error import HTTPError

from sourcing_agent.api import _api_bearer_tokens, _AuthMiddleware, create_server


def _http_scope(path: str = "/api/protected", method: str = "GET", headers: dict | None = None) -> dict:
    raw_headers = [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in (headers or {}).items()]
    return {"type": "http", "method": method, "path": path, "headers": raw_headers, "query_string": b""}


async def _drive(bearer_tokens: dict, scope: dict) -> tuple[dict, list]:
    captured: dict = {}
    sent: list = []

    async def receive() -> dict:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict) -> None:
        sent.append(message)

    async def inner(inner_scope: dict, inner_receive, inner_send) -> None:
        captured["scope"] = inner_scope
        await inner_send({"type": "http.response.start", "status": 200, "headers": []})
        await inner_send({"type": "http.response.body", "body": b"ok"})

    middleware = _AuthMiddleware(inner, bearer_tokens=bearer_tokens)
    await middleware(scope, receive, send)
    return captured, sent


def _status_of(sent: list) -> int | None:
    for message in sent:
        if message.get("type") == "http.response.start":
            return message.get("status")
    return None


class AuthMiddlewareUnitTest(unittest.TestCase):
    def _run(self, tokens: dict, scope: dict) -> tuple[dict, list]:
        return asyncio.run(_drive(tokens, scope))

    def test_open_mode_passes_through_with_null_identity(self) -> None:
        captured, sent = self._run({}, _http_scope())
        self.assertEqual(_status_of(sent), 200)
        self.assertIn("scope", captured)
        self.assertIsNone(captured["scope"]["state"]["identity"])

    def test_configured_missing_token_returns_401_and_never_routes(self) -> None:
        captured, sent = self._run({"tok-1": "user-1"}, _http_scope())
        self.assertEqual(_status_of(sent), HTTPStatus.UNAUTHORIZED.value)
        self.assertNotIn("scope", captured)

    def test_configured_unknown_token_returns_401(self) -> None:
        _captured, sent = self._run({"tok-1": "user-1"}, _http_scope(headers={"Authorization": "Bearer wrong-token"}))
        self.assertEqual(_status_of(sent), HTTPStatus.UNAUTHORIZED.value)

    def test_configured_non_bearer_scheme_returns_401(self) -> None:
        _captured, sent = self._run({"tok-1": "user-1"}, _http_scope(headers={"Authorization": "Basic tok-1"}))
        self.assertEqual(_status_of(sent), HTTPStatus.UNAUTHORIZED.value)

    def test_valid_token_injects_identity_on_state(self) -> None:
        captured, sent = self._run({"tok-1": "user-1"}, _http_scope(headers={"Authorization": "Bearer tok-1"}))
        self.assertEqual(_status_of(sent), 200)
        self.assertEqual(captured["scope"]["state"]["identity"], {"user_id": "user-1"})

    def test_health_path_is_exempt_when_configured(self) -> None:
        captured, sent = self._run({"tok-1": "user-1"}, _http_scope(path="/health"))
        self.assertEqual(_status_of(sent), 200)
        self.assertIsNone(captured["scope"]["state"]["identity"])

    def test_provider_webhook_path_is_exempt_from_bearer(self) -> None:
        # Exempt so the handler's own shared-secret token check runs instead.
        captured, sent = self._run({"tok-1": "user-1"}, _http_scope(path="/api/providers/apify/webhook", method="POST"))
        self.assertEqual(_status_of(sent), 200)
        self.assertIn("scope", captured)


class BearerTokenParsingTest(unittest.TestCase):
    def test_unset_is_empty(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SOURCING_API_BEARER_TOKENS", None)
            self.assertEqual(_api_bearer_tokens(), {})

    def test_valid_json_map(self) -> None:
        with patch.dict(os.environ, {"SOURCING_API_BEARER_TOKENS": json.dumps({"t1": "u1", "t2": "u2"})}):
            self.assertEqual(_api_bearer_tokens(), {"t1": "u1", "t2": "u2"})

    def test_malformed_json_is_empty(self) -> None:
        with patch.dict(os.environ, {"SOURCING_API_BEARER_TOKENS": "not-json{"}):
            self.assertEqual(_api_bearer_tokens(), {})

    def test_non_object_json_is_empty(self) -> None:
        with patch.dict(os.environ, {"SOURCING_API_BEARER_TOKENS": json.dumps(["t1", "t2"])}):
            self.assertEqual(_api_bearer_tokens(), {})

    def test_drops_blank_token_or_user(self) -> None:
        with patch.dict(
            os.environ,
            {"SOURCING_API_BEARER_TOKENS": json.dumps({"t1": "u1", "": "u2", "t3": ""})},
        ):
            self.assertEqual(_api_bearer_tokens(), {"t1": "u1"})


class _StubOrchestrator:
    def get_runtime_metrics(self, _query):
        return {"status": "ok"}


class AuthServerWiringTest(unittest.TestCase):
    def _start_server(self, env=None):
        env_patch = patch.dict(os.environ, dict(env or {}))
        env_patch.start()
        try:
            server = create_server(_StubOrchestrator(), host="127.0.0.1", port=0)
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

    def _request(self, opener, url, *, method="GET", headers=None):
        request = urllib_request.Request(url, headers=dict(headers or {}), method=method)
        try:
            with opener.open(request, timeout=10) as response:
                return response.status
        except HTTPError as error:
            return error.code

    def test_no_tokens_configured_allows_request(self) -> None:
        base_url, opener = self._start_server(env={})
        self.assertEqual(self._request(opener, f"{base_url}/health"), 200)

    def test_configured_rejects_missing_bearer_before_routing(self) -> None:
        # A non-existent path: auth (configured, no bearer) intercepts with 401
        # before routing would 404, proving the gate runs ahead of routing.
        base_url, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": json.dumps({"tok-1": "user-1"})})
        self.assertEqual(self._request(opener, f"{base_url}/api/__auth_probe__"), HTTPStatus.UNAUTHORIZED.value)

    def test_configured_health_stays_exempt(self) -> None:
        base_url, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": json.dumps({"tok-1": "user-1"})})
        self.assertEqual(self._request(opener, f"{base_url}/health"), 200)

    def test_configured_valid_bearer_passes_gate(self) -> None:
        base_url, opener = self._start_server(env={"SOURCING_API_BEARER_TOKENS": json.dumps({"tok-1": "user-1"})})
        # Valid bearer clears auth; /health responds 200 (gate did not 401).
        self.assertEqual(self._request(opener, f"{base_url}/health", headers={"Authorization": "Bearer tok-1"}), 200)


if __name__ == "__main__":
    unittest.main()
