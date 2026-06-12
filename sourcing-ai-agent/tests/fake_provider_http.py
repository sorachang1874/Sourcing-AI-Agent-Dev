from __future__ import annotations

import json
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib import parse as urlparse

FakeProviderRequest = dict[str, Any]
FakeProviderRouteResponse = tuple[int, Any] | tuple[int, Any, dict[str, str]]
FakeProviderRoute = FakeProviderRouteResponse | Callable[[FakeProviderRequest], FakeProviderRouteResponse]


class _RouteHTTPServer(ThreadingHTTPServer):
    routes: dict[tuple[str, str], FakeProviderRoute]
    requests: list[FakeProviderRequest]


class _FakeProviderHTTPHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        self._handle("GET")

    def do_POST(self) -> None:
        self._handle("POST")

    def _handle(self, method: str) -> None:
        parsed = urlparse.urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        raw_body = self.rfile.read(length) if length > 0 else b""
        payload: Any = None
        if raw_body:
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except json.JSONDecodeError:
                payload = None
        record: FakeProviderRequest = {
            "method": method,
            "path": parsed.path,
            "query": urlparse.parse_qs(parsed.query),
            "headers": dict(self.headers.items()),
            "payload": payload,
            "raw_body": raw_body.decode("utf-8", errors="replace"),
        }
        server = self.server
        if not isinstance(server, _RouteHTTPServer):
            self._send_json(500, {"error": "fake provider server is not initialized"})
            return
        server.requests.append(record)
        route = server.routes.get((method, parsed.path))
        if route is None:
            self._send_json(404, {"error": f"unexpected fake provider route: {method} {parsed.path}"})
            return
        response = route(record) if callable(route) else route
        status, response_payload, response_headers = _normalize_route_response(response)
        self._send_json(status, response_payload, headers=response_headers)

    def _send_json(self, status: int, payload: Any, *, headers: dict[str, str] | None = None) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        for key, value in dict(headers or {}).items():
            self.send_header(str(key), str(value))
        self.end_headers()
        self.wfile.write(raw)


def _normalize_route_response(response: FakeProviderRouteResponse) -> tuple[int, Any, dict[str, str]]:
    if len(response) == 2:
        status, payload = response
        return int(status), payload, {}
    status, payload, headers = response
    return int(status), payload, dict(headers or {})


class FakeProviderHTTPServer:
    def __init__(self, routes: dict[tuple[str, str], FakeProviderRoute] | None = None) -> None:
        self.routes: dict[tuple[str, str], FakeProviderRoute] = dict(routes or {})
        self.requests: list[FakeProviderRequest] = []
        self._server: _RouteHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def base_url(self) -> str:
        if self._server is None:
            raise RuntimeError("fake provider server has not started")
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def url(self, path: str) -> str:
        return f"{self.base_url}/{str(path or '').lstrip('/')}"

    def set_route(self, method: str, path: str, route: FakeProviderRoute) -> None:
        self.routes[(str(method or "").upper(), str(path or ""))] = route
        if self._server is not None:
            self._server.routes = self.routes

    def __enter__(self) -> "FakeProviderHTTPServer":
        self._server = _RouteHTTPServer(("127.0.0.1", 0), _FakeProviderHTTPHandler)
        self._server.routes = self.routes
        self._server.requests = self.requests
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
