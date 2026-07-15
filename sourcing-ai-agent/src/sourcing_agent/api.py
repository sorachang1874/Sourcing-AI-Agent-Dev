from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
import re
import socket
import threading
from collections.abc import Iterator
from email.parser import BytesParser
from email.policy import default as default_email_policy
from http import HTTPStatus
from typing import Any, Callable
from urllib.parse import parse_qs, quote, unquote, urlparse, urlsplit

import anyio.to_thread
import uvicorn
from fastapi import FastAPI
from starlette.concurrency import run_in_threadpool
from starlette.convertors import CONVERTOR_TYPES, Convertor, register_url_convertor
from starlette.datastructures import Headers
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from .cohort_selection import (
    CohortSelectionValidationError,
    cohort_selection_options_payload,
    prepare_external_criteria_request_payload,
    validate_external_cohort_selection_payload,
)
from .operation_runtime import (
    CRM_RESOURCE_BOUND_ACTION_TYPES,
    OPERATION_ACTION_FRESH_SUBMISSION_STATUSES,
    OPERATION_ACTION_SUBMISSION_STATUSES,
)
from .orchestrator import SourcingOrchestrator
from .plan_submit_contract import (
    LEGACY_PLAN_SUBMIT_HTTP_STATUS,
    PLAN_SUBMIT_HISTORY_OWNER_UNRESOLVED_HTTP_STATUS,
    PLAN_SUBMIT_HISTORY_OWNER_UNRESOLVED_REASON,
    PLAN_SUBMIT_HISTORY_OWNER_UNRESOLVED_STATUS,
    PLAN_SUBMIT_IDENTITY_PROVENANCE_PAYLOAD_KEY,
    PLAN_SUBMIT_IDENTITY_PROVENANCE_SERVER,
    PLAN_SUBMIT_OWNER_UNAVAILABLE_HTTP_STATUS,
    PLAN_SUBMIT_OWNER_UNAVAILABLE_REASON,
    PLAN_SUBMIT_OWNER_UNAVAILABLE_STATUS,
    authenticated_plan_history_metadata_owned,
    frontend_history_record_is_plan,
)
from .remote_provider_events import normalize_remote_provider_event, shared_recovery_signal_count
from .storage import _json_safe_payload
from .workflow_submission import (
    normalize_workflow_submission_payload,
    workflow_runtime_uses_managed_runner,
)

_RouteHandler = Callable[[Request, dict[str, Any], dict[str, Any]], Response]


class _SourcingIdentConvertor(Convertor):
    """Path segment convertor matching the legacy `[A-Za-z0-9_-]+` route captures."""

    regex = "[A-Za-z0-9_-]+"

    def convert(self, value: str) -> str:
        return value

    def to_string(self, value: str) -> str:
        return str(value)


if "sourcing_ident" not in CONVERTOR_TYPES:
    register_url_convertor("sourcing_ident", _SourcingIdentConvertor())


def create_server(
    orchestrator: SourcingOrchestrator, host: str = "127.0.0.1", port: int = 8765
) -> "SourcingApiHTTPServer":
    app = create_app(orchestrator)
    return SourcingApiHTTPServer(app, host=host, port=port)


def create_app(orchestrator: SourcingOrchestrator) -> FastAPI:
    allowed_origins = _load_allowed_origins()
    max_parallel_requests = max(1, _env_int("SOURCING_API_MAX_PARALLEL_REQUESTS", 8))
    light_request_reserved = max(
        1,
        _env_int(
            "SOURCING_API_LIGHT_REQUEST_RESERVED",
            _default_light_request_reserved(max_parallel_requests),
        ),
    )
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)
    app.router.routes.extend(_build_routes(orchestrator))
    # Middleware stack (outermost first at runtime): raw-path restore -> two-lane
    # request concurrency -> CORS headers/OPTIONS short-circuit -> bearer auth ->
    # routing. add_middleware prepends, so the auth gate (added first) is innermost
    # and its 401 still picks up CORS headers from the outer CORS middleware.
    app.add_middleware(_AuthMiddleware, bearer_tokens=_api_bearer_tokens())
    app.add_middleware(_CorsHeaderMiddleware, allowed_origins=allowed_origins)
    app.add_middleware(
        _RequestConcurrencyMiddleware,
        shared_limit=max_parallel_requests,
        light_reserved_limit=light_request_reserved,
    )
    app.add_middleware(_RawPathTargetMiddleware)
    app.state.request_concurrency_limits = {
        "shared": max_parallel_requests,
        "light_reserved": light_request_reserved,
    }
    return app


class SourcingApiHTTPServer:
    """ThreadingHTTPServer-compatible shim around a uvicorn-served ASGI app.

    Preserves the legacy stdlib server interface used across the codebase:
    `serve_forever()` blocks in the calling thread, `shutdown()` is callable from
    another thread and blocks until the serve loop exits, `server_close()`
    releases the listening socket, and `server_address` reports the real bound
    address (including ephemeral port-0 binds) immediately after construction.
    """

    daemon_threads = True
    block_on_close = False

    def __init__(self, app: FastAPI, host: str = "127.0.0.1", port: int = 8765) -> None:
        self.app = app
        family = socket.AF_INET6 if ":" in str(host or "") else socket.AF_INET
        self._socket = socket.socket(family, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((host, port))
        # Listen immediately so connections issued before serve_forever() starts
        # queue in the kernel backlog (stdlib HTTPServer activates in __init__ too).
        self._socket.listen(2048)
        self.server_address: tuple[str, int] = self._socket.getsockname()[:2]
        self.server_name = str(self.server_address[0])
        self.server_port = int(self.server_address[1])
        config = uvicorn.Config(
            app,
            host=str(host),
            port=self.server_port,
            log_config=None,
            log_level="critical",
            access_log=False,
            lifespan="off",
        )
        self._uvicorn_server = uvicorn.Server(config)
        self._serve_started = threading.Event()
        self._serve_stopped = threading.Event()
        self._closed = False

    def serve_forever(self, poll_interval: float = 0.5) -> None:
        self._serve_started.set()
        try:
            self._uvicorn_server.run(sockets=[self._socket])
        finally:
            self._serve_stopped.set()

    def shutdown(self) -> None:
        self._uvicorn_server.should_exit = True
        if not self._serve_started.is_set():
            return
        if not self._serve_stopped.wait(timeout=10.0):
            self._uvicorn_server.force_exit = True
            self._serve_stopped.wait()

    def server_close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(OSError):
            self._socket.close()

    def fileno(self) -> int:
        return self._socket.fileno()


class _RawPathTargetMiddleware:
    """Route on the raw (still percent-encoded) request target.

    The legacy HTTP server matched route regexes against the undecoded request
    path and only unquoted the captured path params it explicitly decoded. ASGI
    servers pre-decode `scope["path"]`; restoring `raw_path` keeps route
    matching and path-param decoding behavior identical to the old transport.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            raw_path = scope.get("raw_path")
            if raw_path:
                path_bytes = bytes(raw_path).split(b"?", 1)[0]
                scope = dict(scope)
                scope["path"] = path_bytes.decode("latin-1") or "/"
        await self.app(scope, receive, send)


# Public routes exempt from bearer auth: liveness/health probes and the provider
# webhook (which runs its own shared-secret token check in the handler).
_AUTH_EXEMPT_PATHS = frozenset(
    {
        "/health",
        "/api/providers/health",
        "/api/runtime/health",
        "/api/providers/apify/webhook",
    }
)


def _api_bearer_tokens() -> dict[str, str]:
    """Parse SOURCING_API_BEARER_TOKENS into a {token: user_id} map.

    Single-org static per-user bearer tokens (no login UI). An unset/blank env
    keeps the explicit pre-auth/open compatibility mode. Once the operator sets
    the env, malformed, non-object, or filtered-empty configuration fails app
    construction instead of silently disabling authentication.
    """
    raw = str(os.getenv("SOURCING_API_BEARER_TOKENS") or "").strip()
    if not raw:
        return {}

    class _JSONObjectPairs(list[tuple[str, Any]]):
        """Marker that preserves duplicate object keys during JSON parsing."""

    try:
        parsed = json.loads(raw, object_pairs_hook=_JSONObjectPairs)
    except (TypeError, ValueError) as exc:
        raise ValueError("SOURCING_API_BEARER_TOKENS must be a valid JSON object") from exc
    if not isinstance(parsed, _JSONObjectPairs):
        raise ValueError("SOURCING_API_BEARER_TOKENS must be a JSON object")
    tokens: dict[str, str] = {}
    for token, user_id in parsed:
        if not isinstance(token, str) or not isinstance(user_id, str):
            raise ValueError("SOURCING_API_BEARER_TOKENS mappings must use non-empty string tokens and user ids")
        token_text = token.strip()
        user_text = user_id.strip()
        if not token_text or not user_text:
            raise ValueError("SOURCING_API_BEARER_TOKENS mappings must use non-empty string tokens and user ids")
        if token_text in tokens:
            raise ValueError("SOURCING_API_BEARER_TOKENS contains tokens that collide after whitespace normalization")
        tokens[token_text] = user_text
    if not tokens:
        raise ValueError("SOURCING_API_BEARER_TOKENS must contain at least one non-empty token mapping")
    return tokens


def _bearer_token_from_headers(headers: Headers) -> str:
    raw = str(headers.get("authorization") or "").strip()
    if not raw:
        return ""
    scheme, _, credential = raw.partition(" ")
    if scheme.strip().lower() != "bearer":
        return ""
    return credential.strip()


def _resolve_bearer_identity(headers: Headers, bearer_tokens: dict[str, str]) -> dict[str, str] | None:
    token = _bearer_token_from_headers(headers)
    if not token:
        return None
    user_id = bearer_tokens.get(token)
    if not user_id:
        return None
    return {"user_id": user_id}


class _AuthMiddleware:
    """Static per-user bearer-token gate (single org, no login UI).

    Enforcement is active only when SOURCING_API_BEARER_TOKENS is configured (a
    JSON {token: user_id} map, captured at app build time). When active, a
    missing or unknown ``Authorization: Bearer <token>`` yields 401 for every
    route except the public exemptions (health + the provider webhook, which has
    its own token check). On success ``request.state.identity`` is set to
    ``{"user_id": ...}``. When no tokens are configured, requests pass through
    with ``identity = None`` (pre-auth / open) so unconfigured deploys and the
    test/contract lanes keep working before the frontend bearer lands (C2.4).

    Added innermost of the user middleware (runs after CORS has short-circuited
    preflight, before routing) so its 401 still receives CORS headers from the
    outer CORS middleware.
    """

    def __init__(self, app: Any, bearer_tokens: dict[str, str]) -> None:
        self.app = app
        self.bearer_tokens = dict(bearer_tokens or {})

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        identity: dict[str, str] | None = None
        if self.bearer_tokens:
            method = str(scope.get("method") or "").upper()
            path = str(scope.get("path") or "/")
            if method != "OPTIONS" and path not in _AUTH_EXEMPT_PATHS:
                identity = _resolve_bearer_identity(Headers(scope=scope), self.bearer_tokens)
                if identity is None:
                    await _json_response(
                        HTTPStatus.UNAUTHORIZED,
                        {"status": "unauthorized", "reason": "bearer_token_required"},
                    )(scope, receive, send)
                    return
        scope = dict(scope)
        scope["state"] = {**(scope.get("state") or {}), "identity": identity}
        await self.app(scope, receive, send)


# ---------------------------------------------------------------------------
# C2.2: server-derived identity. At submit boundaries the authenticated identity
# (set by _AuthMiddleware on request.state) overrides + strips any client-supplied
# ownership / attribution field, so a caller can never spoof another user's
# requester / tenant / workspace / actor. No-op in open mode (identity None) to
# preserve pre-auth and test-lane behavior until the frontend bearer lands (C2.4).
# ---------------------------------------------------------------------------
_IDENTITY_REQUESTER_KEYS = ("requester_id", "user_id", "requester")
_IDENTITY_TENANT_KEYS = ("tenant_id", "workspace_id", "org_id")


def _user_namespace(user_id: str) -> str:
    """Stable per-user tenant/workspace namespace derived from the user id.

    Prefixed so it never collides with the legacy ``default``/empty workspace and
    stays human-greppable in the jobs / query_dispatches / crm_records columns.
    """
    user = str(user_id or "").strip()
    return f"user-{user}" if user else ""


def _server_identity(request: Request) -> dict[str, str] | None:
    """Authenticated identity from the auth middleware, or None in open mode."""
    state = getattr(request, "state", None)
    identity = getattr(state, "identity", None) if state is not None else None
    if not isinstance(identity, dict):
        return None
    user_id = str(identity.get("user_id") or "").strip()
    return {"user_id": user_id} if user_id else None


def _expected_job_owner_kwargs(request: Request) -> dict[str, str]:
    """Canonical-owner fence arguments for authenticated job operations."""
    identity = _server_identity(request)
    if identity is None:
        return {}
    user_id = identity["user_id"]
    return {
        "expected_requester_id": user_id,
        "expected_tenant_id": _user_namespace(user_id),
    }


def _expected_crm_owner_kwargs(request: Request) -> dict[str, str]:
    """Canonical-owner fence arguments for authenticated CRM operations."""
    identity = _server_identity(request)
    if identity is None:
        return {}
    user_id = identity["user_id"]
    return {
        "expected_workspace_id": _user_namespace(user_id),
        "expected_owner_user_id": user_id,
    }


def _expected_operation_owner_kwargs(request: Request) -> dict[str, str]:
    """Canonical aggregate-owner fence arguments for authenticated Operation APIs."""
    identity = _server_identity(request)
    if identity is None:
        return {}
    return {"expected_workspace_id": _user_namespace(identity["user_id"])}


# C2.6 request-boundary inventory. Every public route that creates, reads via
# POST, or mutates/controls a job/worker is classified here so additions cannot
# silently bypass an ownership decision. ``global_admin`` routes deliberately
# fail closed for authenticated ordinary users until an admin capability exists;
# open mode remains the explicit operator compatibility surface.
AUTHENTICATED_REQUEST_SCOPE_REGISTRY: dict[tuple[str, str], str] = {
    ("POST", "/api/workflows/explain"): "shared_read_via_post",
    ("POST", "/api/workflows"): "server_owned_job_create",
    ("POST", "/api/intake/excel/workflow"): "server_owned_job_create",
    ("POST", "/api/workflows/{job_id}/continue-stage2"): "exact_job_write",
    ("POST", "/api/jobs/{job_id}/profile-completion"): "exact_job_write",
    ("POST", "/api/jobs/{job_id}/candidates/batch"): "owned_job_read_via_post",
    ("POST", "/api/results/refine/compile-instruction"): "exact_job_read_via_post",
    ("POST", "/api/results/refine"): "exact_job_derived_create",
    ("POST", "/api/criteria/feedback"): "criteria_write_with_optional_exact_job_derived_create",
    ("POST", "/api/criteria/confidence-policy"): "criteria_write_with_optional_exact_job_reference",
    ("POST", "/api/criteria/suggestions/review"): "criteria_write_with_optional_exact_job_derived_create",
    ("POST", "/api/criteria/recompile"): "criteria_write_with_optional_exact_job_derived_create",
    ("POST", "/api/target-candidates/import-from-job"): "exact_job_write",
    ("POST", "/api/projections/backfill-from-job"): "exact_job_write",
    ("GET", "/api/operations/action-registry"): "shared_operation_registry_read",
    ("GET", "/api/operations/actions"): "exact_operation_workspace_read",
    ("GET", "/api/operations/actions/{action_id}"): "exact_operation_workspace_read",
    ("GET", "/api/operations/runs"): "exact_operation_workspace_read",
    ("GET", "/api/operations/runs/{run_id}"): "exact_operation_workspace_read",
    ("GET", "/api/operations/runs/{run_id}/provenance"): "exact_operation_workspace_read",
    ("POST", "/api/operations/actions"): "owner_bound_crm_or_schema_less_operation_submit",
    ("POST", "/api/operations/actions/{action_id}/approve"): "exact_operation_workspace_write",
    ("POST", "/api/operations/actions/{action_id}/reject"): "exact_operation_workspace_write",
    ("POST", "/api/operations/runs/{run_id}/cancel"): "exact_operation_workspace_write",
    ("POST", "/api/operations/runs/{run_id}/retry"): "exact_operation_workspace_write",
    ("POST", "/api/operations/runs/{run_id}/resume"): "exact_operation_workspace_write",
    ("POST", "/api/operations/runs/{run_id}/dispatch"): "exact_operation_workspace_write",
    ("GET", "/api/workers/recoverable"): "exact_job_read_or_global_admin",
    ("GET", "/api/workers/daemon/status"): "exact_job_read_or_global_admin",
    ("POST", "/api/workers/interrupt"): "exact_worker_job_write",
    ("POST", "/api/workers/cleanup"): "exact_job_write_or_global_admin",
    ("POST", "/api/workers/daemon/run-once"): "global_admin",
    ("POST", "/api/workers/daemon/systemd-unit"): "global_admin",
    ("POST", "/api/runtime/services/shutdown"): "exact_job_write_or_global_admin",
    ("POST", "/api/jobs/{job_id}/cancel"): "exact_job_write",
}


_JOB_NOT_FOUND_BODY = {"status": "not_found", "reason": "job_not_found"}
_CRM_RECORD_NOT_FOUND_BODY = {"status": "not_found", "reason": "crm_record_not_found"}
_OPERATION_ACTION_NOT_FOUND_BODY = {"status": "not_found", "reason": "operation_action_not_found"}
_OPERATION_RUN_NOT_FOUND_BODY = {"status": "not_found", "reason": "operation_run_not_found"}
_ADMIN_SCOPE_REQUIRED_BODY = {"status": "forbidden", "reason": "admin_scope_required"}


def _mask_authenticated_operation_not_found(
    request: Request,
    result: dict[str, Any],
    *,
    resource: str,
) -> dict[str, Any]:
    """Make authenticated missing/foreign Operation responses byte-identical."""
    if _server_identity(request) is None or result.get("status") != "not_found":
        return result
    if resource == "action":
        return dict(_OPERATION_ACTION_NOT_FOUND_BODY)
    if resource == "run":
        return dict(_OPERATION_RUN_NOT_FOUND_BODY)
    raise ValueError(f"unsupported_operation_resource:{resource}")


def _apply_server_identity(
    payload: dict[str, Any],
    request: Request,
    *,
    requester: bool = False,
    tenant: bool = False,
    workspace: bool = False,
    actor_fields: tuple[str, ...] = (),
    owner: bool = False,
    lock_actor_type: bool = False,
) -> dict[str, Any]:
    """Override + strip client-supplied identity fields with server-derived values.

    No-op when the request is unauthenticated (open mode) so pre-auth deploys and
    the test lanes are unaffected. When authenticated, the server value wins and
    every client alias for the selected field group is stripped first so no
    downstream ``.get()`` fallback can resurrect a spoofed value. Mutates and
    returns ``payload``.
    """
    identity = _server_identity(request)
    if identity is None:
        return payload
    user_id = identity["user_id"]
    namespace = _user_namespace(user_id)
    if requester:
        for key in _IDENTITY_REQUESTER_KEYS:
            payload.pop(key, None)
        payload["requester_id"] = user_id
    if tenant:
        for key in _IDENTITY_TENANT_KEYS:
            payload.pop(key, None)
        payload["tenant_id"] = namespace
    if workspace:
        payload["workspace_id"] = namespace
    for field in actor_fields:
        payload[field] = user_id
    if owner:
        payload["owner_user_id"] = user_id
    if lock_actor_type:
        payload["actor_type"] = "user"
    return payload


def _apply_server_read_scope(
    payload: dict[str, Any],
    request: Request,
    *,
    requester: bool = False,
    tenant: bool = False,
    workspace: bool = False,
) -> dict[str, Any]:
    """Apply authenticated read scope while preserving explicit legacy reads.

    Authenticated reads default to the caller's server-derived namespace and
    ignore spoofed requester/tenant/workspace aliases. An explicit
    ``default`` namespace is the only compatibility selector for pre-auth rows:
    it remains readable, but requester filtering is removed so rows that never
    had a trustworthy requester are not accidentally hidden. Open mode is an
    exact no-op.

    ``requester=True, tenant=True`` is the query-dispatch contract;
    ``workspace=True`` is the CRM/private-overlay contract.
    """
    identity = _server_identity(request)
    if identity is None:
        return payload
    legacy_default_requested = any(str(payload.get(key) or "").strip() == "default" for key in _IDENTITY_TENANT_KEYS)
    namespace = "default" if legacy_default_requested else _user_namespace(identity["user_id"])
    if requester:
        for key in _IDENTITY_REQUESTER_KEYS:
            payload.pop(key, None)
        if not legacy_default_requested:
            payload["requester_id"] = identity["user_id"]
    if tenant or workspace:
        for key in _IDENTITY_TENANT_KEYS:
            payload.pop(key, None)
    if tenant:
        payload["tenant_id"] = namespace
    if workspace:
        payload["workspace_id"] = namespace
    return payload


# ---------------------------------------------------------------------------
# C2.3: tighten user-private reads. A user-private GET serves its normal payload
# when (a) open mode [identity None], (b) the stored owner is a legacy/pre-auth
# sentinel ('' / 'default'), or (c) the stored owner matches the caller. Otherwise
# 404 (never 403 — denies existence to prevent id-enumeration). Shared-canonical
# reads are NOT gated. These are handler-boundary checks (no orchestrator churn):
# the handler pre-fetches the owning row's owner field and gates before serving.
# ---------------------------------------------------------------------------
_LEGACY_OWNER_SENTINELS = ("", "default")


def _read_allowed_for_expected(request: Request, owner_value: str | None, expected: str) -> bool:
    """True => serve; False => 404. ``expected`` is the caller's derived owner."""
    if _server_identity(request) is None:
        return True
    stored = str(owner_value or "").strip()
    if stored in _LEGACY_OWNER_SENTINELS:
        return True
    return stored == expected


def _read_allowed_requester(request: Request, owner_value: str | None) -> bool:
    """owner_field kind: requester_id / owner_user_id (stored = bare user_id)."""
    identity = _server_identity(request)
    expected = identity["user_id"] if identity else ""
    return _read_allowed_for_expected(request, owner_value, expected)


def _read_allowed_namespace(request: Request, owner_value: str | None) -> bool:
    """owner_field kind: workspace_id / tenant_id (stored = 'user-<id>')."""
    identity = _server_identity(request)
    expected = _user_namespace(identity["user_id"]) if identity else ""
    return _read_allowed_for_expected(request, owner_value, expected)


def _write_allowed_job_owner(request: Request, job_row: dict[str, Any]) -> bool:
    """Exact authenticated job write authority; legacy read access is excluded."""
    identity = _server_identity(request)
    if identity is None:
        return True
    user_id = identity["user_id"]
    return str(job_row.get("requester_id") or "").strip() == user_id and str(
        job_row.get("tenant_id") or ""
    ).strip() == _user_namespace(user_id)


def _write_allowed_crm_owner(request: Request, crm_record: dict[str, Any]) -> bool:
    """Exact authenticated CRM workspace write authority.

    ``workspace_id`` is the currently populated owner source of truth. A
    non-empty ``owner_user_id`` is an additional exact-match invariant; blank is
    tolerated for modern rows created before that redundant column was wired.
    Legacy/default workspaces never grant authenticated write authority.
    """
    identity = _server_identity(request)
    if identity is None:
        return True
    user_id = identity["user_id"]
    if str(crm_record.get("workspace_id") or "").strip() != _user_namespace(user_id):
        return False
    owner_user_id = str(crm_record.get("owner_user_id") or "").strip()
    return not owner_user_id or owner_user_id == user_id


class _RequestConcurrencyMiddleware:
    """Two-lane request concurrency gate (asyncio mirror of the legacy semantics).

    Light-lane requests first try a non-blocking claim on the shared limit and
    fall back to a reserved light-lane semaphore; heavy requests block on the
    shared limit. This protects HarvestAPI's hidden ~8-actor concurrency limit
    and must not be removed before M2 provider budgets exist.
    """

    def __init__(self, app: Any, shared_limit: int, light_reserved_limit: int) -> None:
        self.app = app
        self.shared_limit = max(1, int(shared_limit))
        self.light_reserved_limit = max(1, int(light_reserved_limit))
        self._shared: asyncio.Semaphore | None = None
        self._light_reserved: asyncio.Semaphore | None = None

    def _ensure_runtime(self) -> tuple[asyncio.Semaphore, asyncio.Semaphore]:
        if self._shared is None or self._light_reserved is None:
            self._shared = asyncio.Semaphore(self.shared_limit)
            self._light_reserved = asyncio.Semaphore(self.light_reserved_limit)
            # Size the sync-handler threadpool so it never under-cuts the lane
            # budget (handlers run sync in the threadpool while holding a slot).
            limiter = anyio.to_thread.current_default_thread_limiter()
            desired_tokens = max(40, (self.shared_limit + self.light_reserved_limit) * 2)
            if limiter.total_tokens < desired_tokens:
                limiter.total_tokens = desired_tokens
        return self._shared, self._light_reserved

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        shared, light_reserved = self._ensure_runtime()
        lane = _request_priority_lane(str(scope.get("method") or ""), str(scope.get("path") or "/"))
        if lane == "light" and not shared.locked():
            semaphore = shared
        elif lane == "light":
            semaphore = light_reserved
        else:
            semaphore = shared
        await semaphore.acquire()
        try:
            await self.app(scope, receive, send)
        finally:
            semaphore.release()


class _CorsHeaderMiddleware:
    """Legacy CORS behavior: allowlist + localhost auto-allow + header echo."""

    def __init__(self, app: Any, allowed_origins: tuple[str, ...]) -> None:
        self.app = app
        self.allowed_origins = tuple(allowed_origins or ())

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        request_headers = Headers(scope=scope)
        cors_headers = _cors_response_headers(request_headers, self.allowed_origins)
        if str(scope.get("method") or "").upper() == "OPTIONS":
            await send(
                {
                    "type": "http.response.start",
                    "status": HTTPStatus.NO_CONTENT.value,
                    "headers": list(cors_headers),
                }
            )
            await send({"type": "http.response.body", "body": b""})
            return

        async def send_with_cors(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                message = dict(message)
                message["headers"] = list(message.get("headers") or []) + list(cors_headers)
            await send(message)

        await self.app(scope, receive, send_with_cors)


def _cors_response_headers(request_headers: Headers, allowed_origins: tuple[str, ...]) -> list[tuple[bytes, bytes]]:
    headers: list[tuple[bytes, bytes]] = []
    origin = _cors_allow_origin(str(request_headers.get("Origin") or "").strip(), allowed_origins)
    if origin:
        headers.append((b"Access-Control-Allow-Origin", origin.encode("latin-1")))
        headers.append((b"Vary", b"Origin"))
    headers.append((b"Access-Control-Allow-Methods", b"GET, POST, PATCH, DELETE, OPTIONS"))
    requested_headers = str(request_headers.get("Access-Control-Request-Headers") or "").strip()
    allow_headers = requested_headers or "Content-Type"
    headers.append((b"Access-Control-Allow-Headers", allow_headers.encode("latin-1")))
    headers.append(
        (
            b"Access-Control-Expose-Headers",
            b"Content-Disposition, X-Sourcing-Export-Record-Count, "
            b"X-Sourcing-Exported-Record-Count, X-Sourcing-Exported-Signal-Count, "
            b"X-Sourcing-Membership-Revision, X-Sourcing-Source-Candidate-Count, "
            b"X-Sourcing-No-Public-Web-Result-Count, X-Sourcing-No-Exportable-Signal-Count, "
            b"X-Sourcing-Non-Terminal-Run-Count, X-Sourcing-Projection-Id, "
            b"X-Sourcing-Skipped-Assertion-Count",
        )
    )
    headers.append((b"Access-Control-Max-Age", b"600"))
    return headers


def _cors_allow_origin(request_origin: str, allowed_origins: tuple[str, ...]) -> str:
    if not request_origin:
        return ""
    if "*" in allowed_origins:
        return "*"
    if request_origin in allowed_origins:
        return request_origin
    if _is_local_dev_origin(request_origin):
        return request_origin
    return ""


def _decode_path_param(value: str) -> str:
    return unquote(str(value or ""))


def _query_payload_from_scope(scope: dict[str, Any]) -> dict[str, Any]:
    raw_query = scope.get("query_string") or b""
    if isinstance(raw_query, bytes):
        query_text = raw_query.decode("utf-8", errors="replace")
    else:
        query_text = str(raw_query)
    query_payload: dict[str, Any] = {}
    for key, values in parse_qs(query_text, keep_blank_values=False).items():
        if not values:
            continue
        query_payload[key] = values[-1]
    return query_payload


def _raw_header_response(status: HTTPStatus | int, body: bytes, headers: list[tuple[str, str]]) -> Response:
    """Response with exact (canonical-cased) header names like the legacy server.

    Starlette lowercases header names when building responses; the stdlib
    transport emitted them verbatim and several consumers index headers
    case-sensitively, so the raw header list is set directly.
    """
    status_code = status.value if isinstance(status, HTTPStatus) else int(status)
    response = Response(content=body, status_code=status_code)
    response.raw_headers = [(name.encode("latin-1"), value.encode("latin-1")) for name, value in headers]
    return response


def _json_response(status: HTTPStatus | int, payload: dict[str, Any]) -> Response:
    body = json.dumps(_json_safe_payload(payload), ensure_ascii=False, indent=2).encode("utf-8")
    return _raw_header_response(
        status,
        body,
        [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(body))),
        ],
    )


def _bytes_response(
    status: HTTPStatus | int,
    body: bytes,
    *,
    content_type: str,
    filename: str = "",
    extra_headers: dict[str, str] | None = None,
) -> Response:
    headers: list[tuple[str, str]] = [
        ("Content-Type", content_type or "application/octet-stream"),
        ("Content-Length", str(len(body))),
    ]
    if filename:
        headers.append(("Content-Disposition", f'attachment; filename="{filename}"'))
    for header_name, header_value in dict(extra_headers or {}).items():
        if str(header_name or "").strip():
            headers.append((str(header_name), str(header_value)))
    return _raw_header_response(status, body, headers)


def _make_endpoint(handler: _RouteHandler, *, read_body: bool) -> Callable[[Request], Any]:
    async def endpoint(request: Request) -> Response:
        query_payload = _query_payload_from_scope(request.scope)
        body_payload: dict[str, Any] = {}
        if read_body:
            raw = await request.body()
            body_payload = _decode_http_request_body(raw, str(request.headers.get("content-type") or ""))
        return await run_in_threadpool(handler, request, query_payload, body_payload)

    return endpoint


def _build_routes(orchestrator: SourcingOrchestrator) -> list[Route]:
    routes: list[Route] = []

    def add(methods: list[str], path: str, handler: _RouteHandler, *, read_body: bool = False) -> None:
        routes.append(Route(path, _make_endpoint(handler, read_body=read_body), methods=methods))

    def _invalid_external_cohort(payload: dict[str, Any]) -> Response | None:
        try:
            canonical_payload = validate_external_cohort_selection_payload(payload)
        except CohortSelectionValidationError as exc:
            return _json_response(HTTPStatus.BAD_REQUEST, exc.to_result())
        payload.clear()
        payload.update(canonical_payload)
        return None

    def _invalid_external_criteria_request(payload: dict[str, Any]) -> Response | None:
        try:
            canonical_payload = prepare_external_criteria_request_payload(payload)
        except CohortSelectionValidationError as exc:
            return _json_response(HTTPStatus.BAD_REQUEST, exc.to_result())
        payload.clear()
        payload.update(canonical_payload)
        return None

    def _gate_job_owner(request: Request, job_id: str) -> Response | None:
        """C2.3: 404 when the caller is not the job's owner. None => proceed.

        Pre-fetches the job row (a single indexed PK read the downstream getter
        repeats anyway) and gates on jobs.requester_id. Missing and foreign ids
        share the same body. Open mode is a true no-op (no pre-fetch).
        """
        if _server_identity(request) is None:
            return None
        job_row = orchestrator.store.get_job(job_id)
        if job_row is None or not _read_allowed_requester(request, job_row.get("requester_id")):
            return _json_response(HTTPStatus.NOT_FOUND, _JOB_NOT_FOUND_BODY)
        return None

    def _gate_job_write_owner(request: Request, job_id: str) -> Response | None:
        """404 unless an authenticated caller has exact modern job ownership.

        Missing and foreign ids intentionally use the same route-independent
        body. Returning here for both also prevents a missing-at-check id from
        becoming writable if a row appears before the canonical owner runs.
        """
        if _server_identity(request) is None:
            return None
        job_row = orchestrator.store.get_job(job_id)
        if job_row is None or not _write_allowed_job_owner(request, job_row):
            return _json_response(HTTPStatus.NOT_FOUND, _JOB_NOT_FOUND_BODY)
        return None

    def _gate_crm_record_owner(request: Request, record_id: str) -> Response | None:
        """C2.3: 404 when the caller is not the CRM record's workspace owner.

        Gates the record-by-id subresource reads (profile / public-web detail /
        promotions) on crm_records.workspace_id (set to 'user-<id>' by C2.2).
        Missing and foreign ids share the same body. Open mode is a true no-op
        (no pre-fetch).
        """
        if _server_identity(request) is None:
            return None
        crm_record = orchestrator.store.get_crm_record(record_id)
        if crm_record is None or not _read_allowed_namespace(request, crm_record.get("workspace_id")):
            return _json_response(HTTPStatus.NOT_FOUND, _CRM_RECORD_NOT_FOUND_BODY)
        return None

    def _gate_crm_record_write_owner(request: Request, record_id: str) -> Response | None:
        """404 unless an authenticated caller has exact CRM write authority."""
        if _server_identity(request) is None:
            return None
        crm_record = orchestrator.store.get_crm_record(record_id)
        if crm_record is None or not _write_allowed_crm_owner(request, crm_record):
            return _json_response(HTTPStatus.NOT_FOUND, _CRM_RECORD_NOT_FOUND_BODY)
        return None

    def _gate_export_owner(request: Request, command_id: str) -> Response | None:
        """C2.3-followup: 404 when the caller is not the export's workspace owner.

        Export commands are workflow_command rows. CRM exports carry the derived
        workspace_id ('user-<id>') in their command payload (set by C2.2); gate on
        it. Projection exports carry no workspace_id (canonical-derived, not
        user-private) and stay readable. Legacy/'default' workspace stays readable.
        Open mode is a true no-op.
        """
        if _server_identity(request) is None:
            return None
        command = orchestrator.store.get_workflow_command(command_id)
        workspace = str((command or {}).get("payload", {}).get("workspace_id") or "").strip()
        if workspace and not _read_allowed_namespace(request, workspace):
            return _json_response(HTTPStatus.NOT_FOUND, {"status": "not_found", "task_id": command_id})
        return None

    def _frontend_history_is_plan(link: Any) -> bool:
        return frontend_history_record_is_plan(link)

    def _authenticated_unlinked_plan_history_owned(request: Request, link: Any) -> bool:
        identity = _server_identity(request)
        if identity is None:
            return True
        user_id = identity["user_id"]
        return authenticated_plan_history_metadata_owned(
            link,
            requester_id=user_id,
            tenant_id=_user_namespace(user_id),
        )

    def _gate_frontend_history_owner(request: Request, history_id: str) -> Response | None:
        """C2.3-followup: 404 when the caller doesn't own a frontend-history entry.

        Linked rows derive ownership from the job.  An authenticated, unlinked
        Plan row must instead carry the API-authored C1b identity proof in history
        metadata; missing/partial/mismatched proof is quarantined as 404.  Other
        legacy phases retain their prior compatibility behavior. Open mode is a
        true no-op.
        """
        if _server_identity(request) is None:
            return None
        link = orchestrator.store.get_frontend_history_link(history_id)
        job_id = str((link or {}).get("job_id") or "").strip()
        if job_id:
            job_row = orchestrator.store.get_job(job_id)
            if job_row is not None and _read_allowed_requester(request, job_row.get("requester_id")):
                return None
            if job_row is None and not _frontend_history_is_plan(link):
                return None
            return _json_response(HTTPStatus.NOT_FOUND, {"status": "not_found", "history_id": history_id})
        if _frontend_history_is_plan(link) and not _authenticated_unlinked_plan_history_owned(request, link):
            return _json_response(HTTPStatus.NOT_FOUND, {"status": "not_found", "history_id": history_id})
        return None

    def _gate_plan_submit_history_owner(request: Request, history_id: str) -> Response | None:
        """Reject authenticated replacement unless an existing row has exact owner proof.

        Read compatibility for old non-Plan/unlinked rows must not become write
        authority. Authenticated first-create omits ``history_id`` and receives
        a server-generated id; therefore an explicit missing id is an invalid
        replacement handle. An existing linked row uses job ownership, and an
        existing unlinked Plan row uses the C1b metadata proof. Every other
        explicit id fails closed.
        """

        if _server_identity(request) is None:
            return None
        link = orchestrator.store.get_frontend_history_link(history_id)
        if link is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"status": "not_found", "history_id": history_id})
        job_id = str((link or {}).get("job_id") or "").strip()
        if job_id:
            job_row = orchestrator.store.get_job(job_id)
            if job_row is not None and _read_allowed_requester(request, job_row.get("requester_id")):
                return None
        elif _frontend_history_is_plan(link) and _authenticated_unlinked_plan_history_owned(request, link):
            return None
        return _json_response(HTTPStatus.NOT_FOUND, {"status": "not_found", "history_id": history_id})

    def _filter_frontend_history_for_owner(request: Request, result: dict[str, Any]) -> dict[str, Any]:
        """C2.3-followup: drop frontend-history list items the caller doesn't own.

        Uses the same linked-job or provenance-bearing unlinked-Plan matrix as
        ``_gate_frontend_history_owner``. Open mode returns the list unchanged.
        """
        if _server_identity(request) is None:
            return result
        items = result.get("history")
        if not isinstance(items, list):
            return result
        allowed: list[Any] = []
        for item in items:
            job_id = str((item or {}).get("job_id") or "").strip()
            if job_id:
                job_row = orchestrator.store.get_job(job_id)
                if job_row is not None and _read_allowed_requester(request, job_row.get("requester_id")):
                    allowed.append(item)
                elif job_row is None and not _frontend_history_is_plan(item):
                    allowed.append(item)
                continue
            if not _frontend_history_is_plan(item) or _authenticated_unlinked_plan_history_owned(request, item):
                allowed.append(item)
        return {**result, "history": allowed, "count": len(allowed)}

    # ------------------------------------------------------------------ GET
    def get_health(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        health = orchestrator.get_runtime_metrics(query)
        status = HTTPStatus.OK if health.get("status") != "failed" else HTTPStatus.SERVICE_UNAVAILABLE
        return _json_response(status, health)

    add(["GET"], "/health", get_health)

    def get_providers_health(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.healthcheck_model())

    add(["GET"], "/api/providers/health", get_providers_health)

    def get_runtime_health(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        health = orchestrator.get_runtime_health(query)
        status = HTTPStatus.OK if health.get("status") != "failed" else HTTPStatus.SERVICE_UNAVAILABLE
        return _json_response(status, health)

    add(["GET"], "/api/runtime/health", get_runtime_health)

    def get_runtime_metrics(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        health = orchestrator.get_runtime_metrics(query)
        status = HTTPStatus.OK if health.get("status") != "failed" else HTTPStatus.SERVICE_UNAVAILABLE
        return _json_response(status, health)

    add(["GET"], "/api/runtime/metrics", get_runtime_metrics)

    def get_runtime_progress(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        progress = orchestrator.get_system_progress(query)
        status = HTTPStatus.OK if progress.get("status") != "failed" else HTTPStatus.SERVICE_UNAVAILABLE
        return _json_response(status, progress)

    add(["GET"], "/api/runtime/progress", get_runtime_progress)

    def get_cohort_selection_options(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, cohort_selection_options_payload())

    add(["GET"], "/api/cohort-selection/options", get_cohort_selection_options)

    def get_criteria_patterns(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.list_criteria_patterns())

    add(["GET"], "/api/criteria/patterns", get_criteria_patterns)

    def get_plan_reviews(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.list_plan_review_sessions())

    add(["GET"], "/api/plan/reviews", get_plan_reviews)

    def get_query_dispatches(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_read_scope(query, request, requester=True, tenant=True)
        return _json_response(HTTPStatus.OK, orchestrator.list_query_dispatches(query))

    add(["GET"], "/api/query-dispatches", get_query_dispatches)

    def get_workflow_command_registry(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.get_workflow_command_registry_api())

    add(["GET"], "/api/workflow/command-registry", get_workflow_command_registry)

    def get_workflow_commands(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.list_workflow_commands_api(query))

    add(["GET"], "/api/workflow/commands", get_workflow_commands)

    def get_workflow_command(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.get_workflow_command_api(_decode_path_param(request.path_params["command_id"]))
        status = HTTPStatus.OK if result.get("status") == "ok" else HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["GET"], "/api/workflow/commands/{command_id}", get_workflow_command)

    def get_workflow_activities(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.list_workflow_activities_api(query))

    add(["GET"], "/api/workflow/activities", get_workflow_activities)

    def get_workflow_activity(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.get_workflow_activity_api(_decode_path_param(request.path_params["activity_id"]))
        status = HTTPStatus.OK if result.get("status") == "ok" else HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["GET"], "/api/workflow/activities/{activity_id}", get_workflow_activity)

    def get_workflow_activity_attempts(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.list_workflow_activity_attempts_api(query))

    add(["GET"], "/api/workflow/activity-attempts", get_workflow_activity_attempts)

    def get_workflow_activity_attempt(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.get_workflow_activity_attempt_api(_decode_path_param(request.path_params["attempt_id"]))
        status = HTTPStatus.OK if result.get("status") == "ok" else HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["GET"], "/api/workflow/activity-attempts/{attempt_id}", get_workflow_activity_attempt)

    def get_workflow_entity_deltas(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.list_workflow_entity_deltas_api(query))

    add(["GET"], "/api/workflow/entity-deltas", get_workflow_entity_deltas)

    def get_workflow_entity_delta(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.get_workflow_entity_delta_api(_decode_path_param(request.path_params["delta_id"]))
        status = HTTPStatus.OK if result.get("status") == "ok" else HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["GET"], "/api/workflow/entity-deltas/{delta_id}", get_workflow_entity_delta)

    def get_workflow_discovery_lanes(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.list_acquisition_discovery_lanes_api(query))

    add(["GET"], "/api/workflow/discovery-lanes", get_workflow_discovery_lanes)

    def get_workflow_discovery_lane(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.get_acquisition_discovery_lane_api(_decode_path_param(request.path_params["lane_id"]))
        status = HTTPStatus.OK if result.get("status") == "ok" else HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["GET"], "/api/workflow/discovery-lanes/{lane_id}", get_workflow_discovery_lane)

    def get_operation_action_registry(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.get_operation_action_registry())

    add(["GET"], "/api/operations/action-registry", get_operation_action_registry)

    def get_operation_actions(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(query, request, workspace=True)
        return _json_response(
            HTTPStatus.OK,
            orchestrator.list_operation_actions_api(query, **_expected_operation_owner_kwargs(request)),
        )

    add(["GET"], "/api/operations/actions", get_operation_actions)

    def get_operation_runs(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(query, request, workspace=True)
        return _json_response(
            HTTPStatus.OK,
            orchestrator.list_operation_runs_api(query, **_expected_operation_owner_kwargs(request)),
        )

    add(["GET"], "/api/operations/runs", get_operation_runs)

    def get_operation_run_provenance(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.get_operation_run_provenance_api(
            _decode_path_param(request.path_params["run_id"]),
            **_expected_operation_owner_kwargs(request),
        )
        result = _mask_authenticated_operation_not_found(request, result, resource="run")
        status = HTTPStatus.OK if result.get("status") == "ok" else HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["GET"], "/api/operations/runs/{run_id}/provenance", get_operation_run_provenance)

    def get_operation_action(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.get_operation_action_api(
            _decode_path_param(request.path_params["action_id"]),
            **_expected_operation_owner_kwargs(request),
        )
        result = _mask_authenticated_operation_not_found(request, result, resource="action")
        status = HTTPStatus.OK if result.get("status") == "ok" else HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["GET"], "/api/operations/actions/{action_id}", get_operation_action)

    def get_operation_run(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.get_operation_run_api(
            _decode_path_param(request.path_params["run_id"]),
            **_expected_operation_owner_kwargs(request),
        )
        result = _mask_authenticated_operation_not_found(request, result, resource="run")
        status = HTTPStatus.OK if result.get("status") == "ok" else HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["GET"], "/api/operations/runs/{run_id}", get_operation_run)

    def get_company_public_web_assets(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.list_company_public_web_assets(query))

    add(["GET"], "/api/company-assets/public-web", get_company_public_web_assets)

    def get_company_assets(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.list_company_assets_api(query)
        status = HTTPStatus.OK if result.get("status") == "ready" else HTTPStatus.CONFLICT
        return _json_response(status, result)

    add(["GET"], "/api/company-assets", get_company_assets)

    def get_company_evidence(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.list_company_evidence_api(query)
        status = HTTPStatus.OK if result.get("status") == "ready" else HTTPStatus.CONFLICT
        return _json_response(status, result)

    add(["GET"], "/api/company-assets/evidence", get_company_evidence)

    def get_company_assertions(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.list_company_assertions_api(query)
        status = HTTPStatus.OK if result.get("status") == "ready" else HTTPStatus.CONFLICT
        return _json_response(status, result)

    add(["GET"], "/api/company-assets/assertions", get_company_assertions)

    def get_media_asset(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.get_media_asset_content_api(_decode_path_param(request.path_params["asset_id"]))
        payload_status = str(result.get("status") or "").strip()
        if payload_status == "ready":
            return _bytes_response(
                HTTPStatus.OK,
                bytes(result.get("content") or b""),
                content_type=str(result.get("content_type") or "application/octet-stream"),
                extra_headers={
                    "Cache-Control": "public, max-age=86400",
                    "X-Media-Asset-Contract": str(result.get("contract") or "media_asset_read_contract_v1"),
                },
            )
        status = (
            HTTPStatus.NOT_FOUND
            if payload_status in {"not_found", ""}
            else HTTPStatus.FORBIDDEN
            if payload_status == "forbidden"
            else HTTPStatus.CONFLICT
        )
        return _json_response(status, {key: value for key, value in result.items() if key != "content"})

    add(["GET"], "/api/media/assets/{asset_id}", get_media_asset)

    def get_export_command(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        # C1.4b: async export task poll. submit (POST /api/projections/export) -> 202
        # + command_id; the client polls here until status='succeeded', then GETs
        # the artifact handle below.
        command_id = _decode_path_param(request.path_params["command_id"])
        denied = _gate_export_owner(request, command_id)
        if denied is not None:
            return denied
        result = orchestrator.get_export_command_status(command_id)
        payload_status = str(result.get("status") or "").strip()
        if payload_status == "not_found":
            return _json_response(HTTPStatus.NOT_FOUND, result)
        if payload_status == "invalid":
            return _json_response(HTTPStatus.BAD_REQUEST, result)
        return _json_response(HTTPStatus.OK, result)

    add(["GET"], "/api/exports/{command_id}", get_export_command)

    def get_export_command_artifact(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        # C1.4b: stream a succeeded export task's artifact bytes + the command-type's
        # X-Sourcing-* headers (the orchestrator returns a generic 'headers' dict so
        # this one endpoint serves both projection and CRM exports byte/header-parity).
        command_id = _decode_path_param(request.path_params["command_id"])
        denied = _gate_export_owner(request, command_id)
        if denied is not None:
            return denied
        result = orchestrator.get_export_command_artifact(command_id)
        payload_status = str(result.get("status") or "").strip()
        if payload_status == "not_found":
            return _json_response(HTTPStatus.NOT_FOUND, result)
        if payload_status == "invalid":
            return _json_response(HTTPStatus.BAD_REQUEST, result)
        if payload_status == "not_ready":
            return _json_response(HTTPStatus.CONFLICT, result)
        if payload_status != "ok":
            # Fail-closed JSON (never binary) — e.g. a CRM artifact whose stored input
            # watermark no longer matches the owner-computed watermark (stale replay),
            # or a missing artifact. 409 Conflict: the artifact can no longer be served.
            return _json_response(HTTPStatus.CONFLICT, result)
        return _bytes_response(
            HTTPStatus.OK,
            bytes(result.get("body") or b""),
            content_type=str(result.get("content_type") or "application/octet-stream"),
            filename=str(result.get("filename") or "download.bin"),
            extra_headers={str(k): str(v) for k, v in dict(result.get("headers") or {}).items()},
        )

    add(["GET"], "/api/exports/{command_id}/artifact", get_export_command_artifact)

    def get_legacy_result_endpoint_retirement(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.get_legacy_result_endpoint_retirement_status(query))

    add(["GET"], "/api/migrations/legacy-result-endpoints", get_legacy_result_endpoint_retirement)

    def get_legacy_public_web_retirement(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.get_legacy_public_web_retirement_status(query))

    add(["GET"], "/api/migrations/legacy-public-web", get_legacy_public_web_retirement)

    def get_manual_review(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(
            HTTPStatus.OK,
            orchestrator.list_manual_review_items(
                target_company=str(query.get("target_company") or ""),
                job_id=str(query.get("job_id") or ""),
                status=str(query.get("status") or "open"),
            ),
        )

    add(["GET"], "/api/manual-review", get_manual_review)

    def get_candidate_review_registry(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(
            HTTPStatus.OK,
            orchestrator.list_candidate_review_records(
                job_id=str(query.get("job_id") or ""),
                history_id=str(query.get("history_id") or ""),
                candidate_id=str(query.get("candidate_id") or ""),
                status=str(query.get("status") or ""),
            ),
        )

    add(["GET"], "/api/candidate-review-registry", get_candidate_review_registry)

    def get_target_candidates(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(
            HTTPStatus.OK,
            orchestrator.list_target_candidates(
                job_id=str(query.get("job_id") or ""),
                history_id=str(query.get("history_id") or ""),
                candidate_id=str(query.get("candidate_id") or ""),
                follow_up_status=str(query.get("follow_up_status") or ""),
            ),
        )

    add(["GET"], "/api/target-candidates", get_target_candidates)

    def get_target_public_web_search_gone(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(
            HTTPStatus.GONE,
            _legacy_target_public_web_endpoint_payload(
                legacy_endpoint="/api/target-candidates/public-web-search",
                canonical_endpoint="/api/crm/records/public-web-search",
                operation="list",
            ),
        )

    add(["GET"], "/api/target-candidates/public-web-search", get_target_public_web_search_gone)

    def get_crm_record_profile(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        record_id = _decode_path_param(request.path_params["record_id"])
        denied = _gate_crm_record_owner(request, record_id)
        if denied is not None:
            return denied
        result = orchestrator.get_crm_record_profile(record_id)
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["GET"], "/api/crm/records/{record_id}/profile", get_crm_record_profile)

    def get_crm_public_web_detail(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        record_id = _decode_path_param(request.path_params["record_id"])
        denied = _gate_crm_record_owner(request, record_id)
        if denied is not None:
            return denied
        result = orchestrator.get_crm_record_public_web_search_detail(record_id)
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["GET"], "/api/crm/records/{record_id}/public-web-search", get_crm_public_web_detail)

    def get_crm_public_web_promotions(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        record_id = _decode_path_param(request.path_params["record_id"])
        denied = _gate_crm_record_owner(request, record_id)
        if denied is not None:
            return denied
        result = orchestrator.list_crm_record_public_web_promotions(record_id)
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["GET"], "/api/crm/records/{record_id}/public-web-promotions", get_crm_public_web_promotions)

    def get_target_candidate_profile(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.get_target_candidate_profile(_decode_path_param(request.path_params["record_id"]))
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["GET"], "/api/target-candidates/{record_id}/profile", get_target_candidate_profile)

    def get_target_public_web_detail_gone(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(
            HTTPStatus.GONE,
            _legacy_target_public_web_endpoint_payload(
                legacy_endpoint="/api/target-candidates/{record_id}/public-web-search",
                canonical_endpoint="/api/crm/records/{crm_record_id}/public-web-search",
                operation="detail",
                record_id=_decode_path_param(request.path_params["record_id"]),
            ),
        )

    add(["GET"], "/api/target-candidates/{record_id}/public-web-search", get_target_public_web_detail_gone)

    def get_target_public_web_promotions_gone(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        return _json_response(
            HTTPStatus.GONE,
            _legacy_target_public_web_endpoint_payload(
                legacy_endpoint="/api/target-candidates/{record_id}/public-web-promotions",
                canonical_endpoint="/api/crm/records/{crm_record_id}/public-web-promotions",
                operation="promotion_list",
                record_id=_decode_path_param(request.path_params["record_id"]),
            ),
        )

    add(["GET"], "/api/target-candidates/{record_id}/public-web-promotions", get_target_public_web_promotions_gone)

    def get_asset_default_pointers(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.list_asset_default_pointers(query))

    add(["GET"], "/api/assets/governance/default-pointers", get_asset_default_pointers)

    def get_frontend_history(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.list_frontend_history(limit=_env_int_from_payload(query, "limit", 24))
        return _json_response(HTTPStatus.OK, _filter_frontend_history_for_owner(request, result))

    add(["GET"], "/api/frontend-history", get_frontend_history)

    def get_frontend_history_recovery(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        history_id = request.path_params["history_id"]
        denied = _gate_frontend_history_owner(request, history_id)
        if denied is not None:
            return denied
        result = orchestrator.get_frontend_history_recovery(history_id)
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["GET"], "/api/frontend-history/{history_id}", get_frontend_history_recovery)

    def get_crm_records(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_read_scope(query, request, workspace=True)
        return _json_response(
            HTTPStatus.OK,
            orchestrator.list_crm_records_api(
                source_projection_id=str(query.get("source_projection_id") or ""),
                source_collection_id=str(query.get("source_collection_id") or ""),
                workspace_id=str(query.get("workspace_id") or "default"),
                limit=_env_int_from_payload(query, "limit", 250),
            ),
        )

    add(["GET"], "/api/crm/records", get_crm_records)

    def get_crm_tasks(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_read_scope(query, request, workspace=True)
        return _json_response(
            HTTPStatus.OK,
            orchestrator.list_crm_tasks_api(
                workspace_id=str(query.get("workspace_id") or "default"),
                crm_record_id=str(query.get("crm_record_id") or query.get("record_id") or ""),
                status=str(query.get("status") or ""),
                limit=_env_int_from_payload(query, "limit", 100),
            ),
        )

    add(["GET"], "/api/crm/tasks", get_crm_tasks)

    def get_crm_record_tasks(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_read_scope(query, request, workspace=True)
        result = orchestrator.list_crm_tasks_api(
            workspace_id=str(query.get("workspace_id") or "default"),
            crm_record_id=_decode_path_param(request.path_params["record_id"]),
            status=str(query.get("status") or ""),
            limit=_env_int_from_payload(query, "limit", 100),
        )
        if result.get("status") == "not_found":
            return _json_response(HTTPStatus.NOT_FOUND, result)
        return _json_response(HTTPStatus.OK, result)

    add(["GET"], "/api/crm/records/{record_id}/tasks", get_crm_record_tasks)

    def get_crm_record(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_read_scope(query, request, workspace=True)
        result = orchestrator.get_crm_record_api(
            _decode_path_param(request.path_params["record_id"]),
            workspace_id=str(query.get("workspace_id") or "default"),
        )
        if result is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"status": "not_found", "reason": "crm_record_not_found"})
        return _json_response(HTTPStatus.OK, result)

    add(["GET"], "/api/crm/records/{record_id}", get_crm_record)

    def get_recoverable_workers(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        if _server_identity(request) is not None:
            job_id = str(query.get("job_id") or "").strip()
            if not job_id:
                return _json_response(HTTPStatus.FORBIDDEN, _ADMIN_SCOPE_REQUIRED_BODY)
            denied = _gate_job_write_owner(request, job_id)
            if denied is not None:
                return denied
        return _json_response(HTTPStatus.OK, orchestrator.list_recoverable_agent_workers(query))

    add(["GET"], "/api/workers/recoverable", get_recoverable_workers)

    def get_worker_daemon_status(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        authenticated = _server_identity(request) is not None
        if authenticated:
            job_id = str(query.get("job_id") or "").strip()
            if not job_id:
                return _json_response(HTTPStatus.FORBIDDEN, _ADMIN_SCOPE_REQUIRED_BODY)
            denied = _gate_job_write_owner(request, job_id)
            if denied is not None:
                return denied
        result = orchestrator.get_worker_daemon_status(
            query,
            authenticated_job_scope=authenticated,
            **_expected_job_owner_kwargs(request),
        )
        status = HTTPStatus.NOT_FOUND if result.get("status") == "not_found" else HTTPStatus.OK
        return _json_response(status, result)

    add(["GET"], "/api/workers/daemon/status", get_worker_daemon_status)

    def get_run_projection_link(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        link_payload = orchestrator.get_run_projection_link(request.path_params["run_id"])
        if link_payload is None or link_payload.get("status") == "not_found":
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "run projection link not found"})
        if link_payload.get("status") == "invalid":
            return _json_response(HTTPStatus.BAD_REQUEST, link_payload)
        return _json_response(HTTPStatus.OK, link_payload)

    add(["GET"], "/api/runs/{run_id:sourcing_ident}/projection-link", get_run_projection_link)

    def get_collections(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(
            HTTPStatus.OK,
            orchestrator.list_collection_asset_overview(limit=_env_int_from_payload(query, "limit", 250)),
        )

    add(["GET"], "/api/collections", get_collections)

    def get_collection_projection(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        link_payload = orchestrator.get_collection_authoritative_projection_link(
            _decode_path_param(request.path_params["collection_id"])
        )
        if link_payload is None or link_payload.get("status") == "not_found":
            return _json_response(HTTPStatus.NOT_FOUND, link_payload or {"error": "collection projection not found"})
        if link_payload.get("status") == "invalid":
            return _json_response(HTTPStatus.BAD_REQUEST, link_payload)
        if link_payload.get("status") == "not_ready":
            return _json_response(HTTPStatus.CONFLICT, link_payload)
        return _json_response(HTTPStatus.OK, link_payload)

    add(["GET"], "/api/collections/{collection_id}/authoritative-projection", get_collection_projection)

    def get_collection_asset_entry(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        entry_payload = orchestrator.get_collection_authoritative_asset_entry(
            _decode_path_param(request.path_params["collection_id"])
        )
        if entry_payload is None or entry_payload.get("status") == "not_found":
            return _json_response(HTTPStatus.NOT_FOUND, entry_payload or {"error": "collection asset entry not found"})
        if entry_payload.get("status") == "invalid":
            return _json_response(HTTPStatus.BAD_REQUEST, entry_payload)
        if entry_payload.get("status") == "not_ready":
            return _json_response(HTTPStatus.CONFLICT, entry_payload)
        return _json_response(HTTPStatus.OK, entry_payload)

    add(["GET"], "/api/collections/{collection_id}/asset-entry", get_collection_asset_entry)

    def get_collection_coverage(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        coverage_payload = orchestrator.get_collection_asset_coverage(
            _decode_path_param(request.path_params["collection_id"])
        )
        if coverage_payload is None or coverage_payload.get("status") == "not_found":
            return _json_response(HTTPStatus.NOT_FOUND, coverage_payload or {"error": "collection coverage not found"})
        if coverage_payload.get("status") == "invalid":
            return _json_response(HTTPStatus.BAD_REQUEST, coverage_payload)
        if coverage_payload.get("status") == "not_ready":
            return _json_response(HTTPStatus.CONFLICT, coverage_payload)
        return _json_response(HTTPStatus.OK, coverage_payload)

    add(["GET"], "/api/collections/{collection_id}/coverage", get_collection_coverage)

    def get_projection_crm_state(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_read_scope(query, request, workspace=True)
        crm_payload = orchestrator.get_projection_crm_state_api(
            request.path_params["projection_id"],
            candidate_identity_keys=[
                value for value in str(query.get("candidate_identity_keys") or "").split(",") if value.strip()
            ],
            workspace_id=str(query.get("workspace_id") or "default"),
        )
        if crm_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "projection not found"})
        if crm_payload.get("status") == "not_ready":
            return _json_response(HTTPStatus.CONFLICT, crm_payload)
        return _json_response(HTTPStatus.OK, crm_payload)

    add(["GET"], "/api/projections/{projection_id:sourcing_ident}/crm-state", get_projection_crm_state)

    def get_projection_export_policy(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        policy_payload = orchestrator.get_projection_export_policy_api(request.path_params["projection_id"])
        if policy_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "projection not found"})
        if policy_payload.get("status") == "not_ready":
            return _json_response(HTTPStatus.CONFLICT, policy_payload)
        return _json_response(HTTPStatus.OK, policy_payload)

    add(["GET"], "/api/projections/{projection_id:sourcing_ident}/export-policy", get_projection_export_policy)

    def get_projection_search(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        search_payload = orchestrator.search_projection_person_index_api(
            request.path_params["projection_id"],
            search_keyword=str(query.get("search") or query.get("q") or ""),
            offset=_env_int_from_payload(query, "offset", 0),
            limit=_env_int_from_payload(query, "limit", 120),
        )
        if search_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "projection not found"})
        if search_payload.get("status") == "not_ready":
            return _json_response(HTTPStatus.CONFLICT, search_payload)
        return _json_response(HTTPStatus.OK, search_payload)

    add(["GET"], "/api/projections/{projection_id:sourcing_ident}/search", get_projection_search)

    def get_projection_person_detail(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_read_scope(query, request, workspace=True)
        person_payload = orchestrator.get_serving_projection_person_detail_api(
            request.path_params["projection_id"],
            _decode_path_param(request.path_params["person_key"]),
            workspace_id=str(query.get("workspace_id") or "default"),
        )
        if person_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "projection not found"})
        if person_payload.get("status") == "not_ready":
            reason = str(person_payload.get("reason") or "")
            status = HTTPStatus.NOT_FOUND if reason == "projection_member_not_found" else HTTPStatus.CONFLICT
            return _json_response(status, person_payload)
        return _json_response(HTTPStatus.OK, person_payload)

    add(["GET"], "/api/projections/{projection_id:sourcing_ident}/persons/{person_key}", get_projection_person_detail)

    def get_projection_candidates(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        candidate_page_payload = orchestrator.get_serving_projection_candidate_page(
            request.path_params["projection_id"],
            offset=_env_int_from_payload(query, "offset", 0),
            limit=_env_int_from_payload(query, "limit", 120),
            candidate_filter=_candidate_page_filter_from_payload(query),
        )
        if candidate_page_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "projection not found"})
        if candidate_page_payload.get("status") == "not_ready":
            return _json_response(HTTPStatus.CONFLICT, candidate_page_payload)
        return _json_response(HTTPStatus.OK, candidate_page_payload)

    add(["GET"], "/api/projections/{projection_id:sourcing_ident}/candidates", get_projection_candidates)

    def get_projection(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        projection_payload = orchestrator.get_serving_projection_api(request.path_params["projection_id"])
        if projection_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "projection not found"})
        if projection_payload.get("status") == "not_ready":
            return _json_response(HTTPStatus.CONFLICT, projection_payload)
        return _json_response(HTTPStatus.OK, projection_payload)

    add(["GET"], "/api/projections/{projection_id:sourcing_ident}", get_projection)

    def get_person_summary(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_read_scope(query, request, workspace=True)
        person_payload = orchestrator.get_person_summary_api(
            _decode_path_param(request.path_params["person_key"]),
            workspace_id=str(query.get("workspace_id") or "default"),
        )
        if person_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "person summary not found"})
        if person_payload.get("status") == "invalid":
            return _json_response(HTTPStatus.BAD_REQUEST, person_payload)
        if person_payload.get("status") == "not_ready":
            return _json_response(HTTPStatus.CONFLICT, person_payload)
        return _json_response(HTTPStatus.OK, person_payload)

    add(["GET"], "/api/persons/{person_key}", get_person_summary)

    def get_job_progress(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        job_id = request.path_params["job_id"]
        denied = _gate_job_owner(request, job_id)
        if denied is not None:
            return denied
        progress_payload = orchestrator.get_job_progress(job_id)
        if progress_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
        return _json_response(HTTPStatus.OK, progress_payload)

    add(["GET"], "/api/jobs/{job_id:sourcing_ident}/progress", get_job_progress)

    def get_job_board_patches(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        job_id = request.path_params["job_id"]
        denied = _gate_job_owner(request, job_id)
        if denied is not None:
            return denied
        patch_payload = orchestrator.get_job_board_visible_patch_log(
            job_id,
            after_published_at=str(query.get("after_published_at") or ""),
            after_sequence=_env_int_from_payload(query, "after_sequence", 0),
            limit=_env_int_from_payload(query, "limit", 50),
        )
        if patch_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
        return _json_response(HTTPStatus.OK, patch_payload)

    add(["GET"], "/api/jobs/{job_id:sourcing_ident}/board-patches", get_job_board_patches)

    def get_job_dashboard(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        job_id = request.path_params["job_id"]
        if _legacy_job_result_endpoint_retired(orchestrator, job_id):
            return _json_response(
                HTTPStatus.GONE,
                _legacy_job_result_endpoint_payload(
                    orchestrator,
                    job_id,
                    legacy_endpoint="/api/jobs/{job_id}/dashboard",
                ),
            )
        denied = _gate_job_owner(request, job_id)
        if denied is not None:
            return denied
        dashboard_payload = orchestrator.get_job_dashboard(
            job_id,
            include_asset_population_preview=_env_bool_from_payload(query, "include_candidates", True),
        )
        if dashboard_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
        return _json_response(HTTPStatus.OK, dashboard_payload)

    add(["GET"], "/api/jobs/{job_id:sourcing_ident}/dashboard", get_job_dashboard)

    def get_job_candidates(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        job_id = request.path_params["job_id"]
        if _legacy_job_result_endpoint_retired(orchestrator, job_id):
            return _json_response(
                HTTPStatus.GONE,
                _legacy_job_result_endpoint_payload(
                    orchestrator,
                    job_id,
                    legacy_endpoint="/api/jobs/{job_id}/candidates",
                ),
            )
        denied = _gate_job_owner(request, job_id)
        if denied is not None:
            return denied
        candidate_page_payload = orchestrator.get_job_candidate_page(
            job_id,
            offset=_env_int_from_payload(query, "offset", 0),
            limit=_env_int_from_payload(query, "limit", 120),
            lightweight=_env_bool_from_payload(query, "lightweight", True),
            candidate_filter=_candidate_page_filter_from_payload(query),
        )
        if candidate_page_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
        if str(candidate_page_payload.get("status") or "").strip() == "not_ready":
            return _json_response(HTTPStatus.CONFLICT, candidate_page_payload)
        return _json_response(HTTPStatus.OK, candidate_page_payload)

    add(["GET"], "/api/jobs/{job_id:sourcing_ident}/candidates", get_job_candidates)

    def get_job_results(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        job_id = request.path_params["job_id"]
        if _legacy_job_result_endpoint_retired(orchestrator, job_id):
            return _json_response(
                HTTPStatus.GONE,
                _legacy_job_result_endpoint_payload(
                    orchestrator,
                    job_id,
                    legacy_endpoint="/api/jobs/{job_id}/results",
                ),
            )
        denied = _gate_job_owner(request, job_id)
        if denied is not None:
            return denied
        results_payload = orchestrator.get_job_results_api(
            job_id,
            include_candidates=_env_bool_from_payload(query, "include_candidates", False),
            include_runtime_details=_env_bool_from_payload(query, "include_runtime_details", True),
        )
        if results_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
        return _json_response(HTTPStatus.OK, results_payload)

    add(["GET"], "/api/jobs/{job_id:sourcing_ident}/results", get_job_results)

    def get_job_candidate_detail(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        job_id = request.path_params["job_id"]
        candidate_id = request.path_params["candidate_id"]
        if _legacy_job_result_endpoint_retired(orchestrator, job_id):
            return _json_response(
                HTTPStatus.GONE,
                _legacy_job_result_endpoint_payload(
                    orchestrator,
                    job_id,
                    legacy_endpoint="/api/jobs/{job_id}/candidates/{candidate_id}",
                    candidate_identity_key=_decode_path_param(candidate_id),
                ),
            )
        denied = _gate_job_owner(request, job_id)
        if denied is not None:
            return denied
        candidate_detail_payload = orchestrator.get_job_candidate_detail(
            job_id,
            candidate_id,
            allow_legacy_profile_timeline_hydration=_env_bool_from_payload(
                query,
                "hydrate_legacy_timeline",
                False,
            ),
        )
        if candidate_detail_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "candidate not found"})
        return _json_response(HTTPStatus.OK, candidate_detail_payload)

    add(["GET"], "/api/jobs/{job_id:sourcing_ident}/candidates/{candidate_id}", get_job_candidate_detail)

    def get_job_trace(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        job_id = request.path_params["job_id"]
        denied = _gate_job_owner(request, job_id)
        if denied is not None:
            return denied
        trace_payload = orchestrator.get_job_trace(job_id)
        if trace_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
        return _json_response(HTTPStatus.OK, trace_payload)

    add(["GET"], "/api/jobs/{job_id:sourcing_ident}/trace", get_job_trace)

    def get_job_workers(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        job_id = request.path_params["job_id"]
        denied = _gate_job_owner(request, job_id)
        if denied is not None:
            return denied
        worker_payload = orchestrator.get_job_workers(job_id)
        if worker_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
        return _json_response(HTTPStatus.OK, worker_payload)

    add(["GET"], "/api/jobs/{job_id:sourcing_ident}/workers", get_job_workers)

    def get_job_materialization_items(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        job_id = request.path_params["job_id"]
        denied = _gate_job_owner(request, job_id)
        if denied is not None:
            return denied
        materialization_payload = orchestrator.get_job_materialization_items(job_id)
        if materialization_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
        return _json_response(HTTPStatus.OK, materialization_payload)

    add(["GET"], "/api/jobs/{job_id:sourcing_ident}/materialization-items", get_job_materialization_items)

    def get_job_scheduler(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        job_id = request.path_params["job_id"]
        denied = _gate_job_owner(request, job_id)
        if denied is not None:
            return denied
        scheduler_payload = orchestrator.get_job_scheduler(job_id)
        if scheduler_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
        return _json_response(HTTPStatus.OK, scheduler_payload)

    add(["GET"], "/api/jobs/{job_id:sourcing_ident}/scheduler", get_job_scheduler)

    def get_job(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        job_id = request.path_params["job_id"]
        denied = _gate_job_owner(request, job_id)
        if denied is not None:
            return denied
        job_payload = orchestrator.get_job_api(
            job_id,
            include_details=_env_bool_from_payload(query, "include_details", False),
        )
        if job_payload is None:
            return _json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
        return _json_response(HTTPStatus.OK, job_payload)

    add(["GET"], "/api/jobs/{job_id:sourcing_ident}", get_job)

    # --------------------------------------------------------------- DELETE
    def delete_frontend_history(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        history_id = request.path_params["history_id"]
        denied = _gate_frontend_history_owner(request, history_id)
        if denied is not None:
            return denied
        result = orchestrator.delete_frontend_history(history_id)
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["DELETE"], "/api/frontend-history/{history_id}", delete_frontend_history)

    # ----------------------------------------------------------------- POST
    def post_bootstrap(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.OK, orchestrator.bootstrap())

    add(["POST"], "/api/bootstrap", post_bootstrap, read_body=True)

    # C1 (substrate-unify): the synchronous POST /api/plan route is deleted.
    # It ran the full LLM plan-compile inline on a shared request slot and is
    # fully redundant with the async POST /api/plan/submit path the live UI uses
    # (current bridge: submit -> 200/pending -> poll the frontend history link), whose worker
    # literally calls the same orchestrator.plan_workflow. plan_workflow itself
    # is retained as the internal compile function.
    def post_plan_submit(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        # Never accept provenance from the request body.  In authenticated mode
        # the API writes the one marker that submit consumes into history metadata;
        # in open mode the private key stays absent.
        payload.pop(PLAN_SUBMIT_IDENTITY_PROVENANCE_PAYLOAD_KEY, None)
        invalid_cohort = _invalid_external_cohort(payload)
        if invalid_cohort is not None:
            return invalid_cohort
        _apply_server_identity(payload, request, requester=True, tenant=True)
        if _server_identity(request) is not None:
            payload[PLAN_SUBMIT_IDENTITY_PROVENANCE_PAYLOAD_KEY] = PLAN_SUBMIT_IDENTITY_PROVENANCE_SERVER
        history_id = str(payload.get("history_id") or "").strip()
        if history_id:
            denied = _gate_plan_submit_history_owner(request, history_id)
            if denied is not None:
                return denied
        submit_plan = getattr(orchestrator, "submit_plan_workflow", None)
        if not callable(submit_plan):
            return _json_response(
                PLAN_SUBMIT_OWNER_UNAVAILABLE_HTTP_STATUS,
                {
                    "status": PLAN_SUBMIT_OWNER_UNAVAILABLE_STATUS,
                    "reason": PLAN_SUBMIT_OWNER_UNAVAILABLE_REASON,
                    "phase": "plan",
                    "retryable": True,
                    "fallback_used": False,
                },
            )
        result = submit_plan(payload)
        result_status = str(result.get("status") or "").strip()
        result_reason = str(result.get("reason") or "").strip()
        if (
            result_status == PLAN_SUBMIT_HISTORY_OWNER_UNRESOLVED_STATUS
            and result_reason == PLAN_SUBMIT_HISTORY_OWNER_UNRESOLVED_REASON
        ):
            status = PLAN_SUBMIT_HISTORY_OWNER_UNRESOLVED_HTTP_STATUS
        elif (
            result_status == PLAN_SUBMIT_OWNER_UNAVAILABLE_STATUS
            and result_reason == PLAN_SUBMIT_OWNER_UNAVAILABLE_REASON
        ):
            status = PLAN_SUBMIT_OWNER_UNAVAILABLE_HTTP_STATUS
        else:
            status = LEGACY_PLAN_SUBMIT_HTTP_STATUS if result_status != "invalid" else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/plan/submit", post_plan_submit, read_body=True)

    def post_workflows_explain(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        invalid_cohort = _invalid_external_cohort(payload)
        if invalid_cohort is not None:
            return invalid_cohort
        _apply_server_identity(payload, request, requester=True, tenant=True)
        result = orchestrator.explain_workflow(payload)
        status = HTTPStatus.BAD_REQUEST if result.get("status") == "invalid" else HTTPStatus.OK
        return _json_response(status, result)

    add(["POST"], "/api/workflows/explain", post_workflows_explain, read_body=True)

    def post_apify_webhook(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        if not _provider_webhook_token_allowed(request.headers, query, orchestrator=orchestrator):
            return _json_response(
                HTTPStatus.FORBIDDEN,
                {"status": "forbidden", "reason": "provider_webhook_token_required"},
            )
        event_source = "provider_webhook"
        if _env_bool("SOURCING_PROVIDER_WEBHOOK_SOURCE_OVERRIDE_ENABLED", False):
            override_source = str(query.get("source") or payload.get("source") or "").strip()
            if override_source in {"provider_webhook", "local_provider_event_watcher"}:
                event_source = override_source
        event_payload = {**payload, "provider": "apify", "source": event_source}
        event = normalize_remote_provider_event(event_payload, provider="apify")
        if not str(event.get("run_id") or "").strip() and not str(event.get("dataset_id") or "").strip():
            return _json_response(
                HTTPStatus.BAD_REQUEST,
                {
                    "status": "invalid",
                    "reason": "remote_provider_event_missing_run_or_dataset_id",
                    "event": _provider_event_response_payload(event),
                },
            )
        if _provider_webhook_sync_requested(query):
            return _json_response(
                HTTPStatus.GONE,
                {
                    "status": "retired",
                    "reason": "provider_webhook_sync_recovery_retired",
                    "mode": "shared_recovery_signal",
                },
            )
        result = orchestrator.handle_remote_provider_event({**event_payload, "recovery_mode": "shared_recovery_signal"})
        if result.get("status") != "accepted":
            return _json_response(HTTPStatus.BAD_REQUEST, result)
        shared_recovery_signal = dict(result.get("shared_recovery_signal") or {})
        return _json_response(
            HTTPStatus.ACCEPTED,
            {
                "status": "accepted",
                "provider": "apify",
                "mode": "shared_recovery_signal",
                "reason": str(result.get("reason") or ""),
                "event": _provider_event_response_payload(event),
                "targets": dict(result.get("targets") or {}),
                "shared_recovery_signal_count": shared_recovery_signal_count(shared_recovery_signal),
                "shared_recovery_signal": shared_recovery_signal,
                "released_worker_ids": list(result.get("released_worker_ids") or []),
            },
        )

    add(["POST"], "/api/providers/apify/webhook", post_apify_webhook, read_body=True)

    def post_query_dispatches_list(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_read_scope(payload, request, requester=True, tenant=True)
        return _json_response(HTTPStatus.OK, orchestrator.list_query_dispatches(payload))

    add(["POST"], "/api/query-dispatches/list", post_query_dispatches_list, read_body=True)

    # C1 (substrate-unify): the synchronous POST /api/jobs route is deleted.
    # run_job is a retrieval-only sync shim (no acquisition; reads a pre-existing
    # materialized source ~ the tail half of a workflow) that the live frontend
    # never POSTs — it submits exclusively via POST /api/workflows and reads GET
    # /api/jobs/{id}/* subresources keyed by the workflow-created job_id. run_job
    # is retained as a CLI/test one-shot helper (cli.py run-job + tests), demoted
    # off the serving surface. Heavy retrieval reaches the request tier only via
    # the durable workflow path.
    def post_workflows(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        invalid_cohort = _invalid_external_cohort(payload)
        if invalid_cohort is not None:
            return invalid_cohort
        _apply_server_identity(payload, request, requester=True, tenant=True)
        payload = normalize_workflow_submission_payload(payload)
        if workflow_runtime_uses_managed_runner(payload.get("runtime_execution_mode")):
            result = orchestrator.start_workflow_runner_managed(payload)
        else:
            result = orchestrator.start_workflow(payload)
        status = HTTPStatus.BAD_REQUEST if result.get("status") == "invalid" else HTTPStatus.ACCEPTED
        return _json_response(status, result)

    add(["POST"], "/api/workflows", post_workflows, read_body=True)

    def post_continue_stage2(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        denied = _gate_job_write_owner(request, request.path_params["job_id"])
        if denied is not None:
            return denied
        _apply_server_identity(payload, request, requester=True, tenant=True)
        result = orchestrator.continue_workflow_stage2(
            {**payload, "job_id": request.path_params["job_id"]},
            **_expected_job_owner_kwargs(request),
        )
        status = HTTPStatus.ACCEPTED
        if result.get("status") in {"not_found"}:
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") in {"invalid", "conflict"}:
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/workflows/{job_id:sourcing_ident}/continue-stage2", post_continue_stage2, read_body=True)

    def post_job_profile_completion(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        denied = _gate_job_write_owner(request, request.path_params["job_id"])
        if denied is not None:
            return denied
        result = orchestrator.complete_job_candidate_profiles(
            request.path_params["job_id"],
            payload,
            **_expected_job_owner_kwargs(request),
        )
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(
        ["POST"],
        "/api/jobs/{job_id:sourcing_ident}/profile-completion",
        post_job_profile_completion,
        read_body=True,
    )

    def post_job_candidates_batch(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        denied = _gate_job_owner(request, request.path_params["job_id"])
        if denied is not None:
            return denied
        candidate_ids = payload.get("candidate_ids")
        candidate_batch_result = orchestrator.get_job_candidate_details_batch(
            request.path_params["job_id"],
            candidate_ids if isinstance(candidate_ids, list) else [],
        )
        status = HTTPStatus.OK
        if candidate_batch_result is None:
            status = HTTPStatus.NOT_FOUND
            candidate_batch_result = {"error": "job not found"}
        return _json_response(status, candidate_batch_result)

    add(["POST"], "/api/jobs/{job_id:sourcing_ident}/candidates/batch", post_job_candidates_batch, read_body=True)

    def post_company_assets_supplement(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.supplement_company_assets(payload)
        status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/company-assets/supplement", post_company_assets_supplement, read_body=True)

    def post_company_public_web(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, actor_fields=("requested_by",))
        result = orchestrator.refresh_company_public_web_assets(payload)
        status = HTTPStatus.CREATED if result.get("status") in {"completed", "joined"} else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/company-assets/public-web", post_company_public_web, read_body=True)

    def post_operation_actions(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        crm_owner_bound = str(payload.get("action_type") or "").strip() in CRM_RESOURCE_BOUND_ACTION_TYPES
        _apply_server_identity(
            payload,
            request,
            workspace=crm_owner_bound,
            actor_fields=("actor",),
        )
        owner_scope = _expected_crm_owner_kwargs(request) if crm_owner_bound else {}
        result = orchestrator.submit_operation_action(payload, **owner_scope)
        if (
            result.get("idempotent_replay") is True
            and str(result.get("status") or "").strip() in OPERATION_ACTION_SUBMISSION_STATUSES
        ):
            status = HTTPStatus.OK
        elif (
            result.get("idempotent_replay") is False
            and result.get("status") in OPERATION_ACTION_FRESH_SUBMISSION_STATUSES
        ):
            status = HTTPStatus.ACCEPTED
        elif result.get("status") == "not_found" and result.get("reason") == "crm_record_not_found":
            status = HTTPStatus.NOT_FOUND
            result = dict(_CRM_RECORD_NOT_FOUND_BODY)
        elif result.get("status") == "conflict":
            status = HTTPStatus.CONFLICT
        else:
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/operations/actions", post_operation_actions, read_body=True)

    def post_operation_action_approve(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, actor_fields=("actor",))
        result = orchestrator.approve_operation_action_api(
            _decode_path_param(request.path_params["action_id"]),
            payload,
            **_expected_operation_owner_kwargs(request),
        )
        result = _mask_authenticated_operation_not_found(request, result, resource="action")
        status = HTTPStatus.ACCEPTED if result.get("status") == "queued" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "conflict":
            status = HTTPStatus.CONFLICT
        return _json_response(status, result)

    add(["POST"], "/api/operations/actions/{action_id}/approve", post_operation_action_approve, read_body=True)

    def post_operation_action_reject(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, actor_fields=("actor",))
        result = orchestrator.reject_operation_action_api(
            _decode_path_param(request.path_params["action_id"]),
            payload,
            **_expected_operation_owner_kwargs(request),
        )
        result = _mask_authenticated_operation_not_found(request, result, resource="action")
        status = HTTPStatus.OK if result.get("status") == "rejected" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "conflict":
            status = HTTPStatus.CONFLICT
        return _json_response(status, result)

    add(["POST"], "/api/operations/actions/{action_id}/reject", post_operation_action_reject, read_body=True)

    def post_operation_run_cancel(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, actor_fields=("actor",))
        result = orchestrator.cancel_operation_run_api(
            _decode_path_param(request.path_params["run_id"]),
            payload,
            **_expected_operation_owner_kwargs(request),
        )
        result = _mask_authenticated_operation_not_found(request, result, resource="run")
        status = HTTPStatus.OK if result.get("status") == "cancelled" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "conflict":
            status = HTTPStatus.CONFLICT
        return _json_response(status, result)

    add(["POST"], "/api/operations/runs/{run_id}/cancel", post_operation_run_cancel, read_body=True)

    def post_operation_run_retry(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, actor_fields=("actor",))
        result = orchestrator.retry_operation_run_api(
            _decode_path_param(request.path_params["run_id"]),
            payload,
            **_expected_operation_owner_kwargs(request),
        )
        result = _mask_authenticated_operation_not_found(request, result, resource="run")
        status = HTTPStatus.ACCEPTED if result.get("status") == "queued" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "conflict":
            status = HTTPStatus.CONFLICT
        return _json_response(status, result)

    add(["POST"], "/api/operations/runs/{run_id}/retry", post_operation_run_retry, read_body=True)

    def post_operation_run_resume(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, actor_fields=("actor",))
        result = orchestrator.resume_operation_run_api(
            _decode_path_param(request.path_params["run_id"]),
            payload,
            **_expected_operation_owner_kwargs(request),
        )
        result = _mask_authenticated_operation_not_found(request, result, resource="run")
        status = HTTPStatus.ACCEPTED if result.get("status") == "queued" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "conflict":
            status = HTTPStatus.CONFLICT
        return _json_response(status, result)

    add(["POST"], "/api/operations/runs/{run_id}/resume", post_operation_run_resume, read_body=True)

    def post_operation_run_dispatch(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, actor_fields=("actor",))
        result = orchestrator.dispatch_operation_run_api(
            _decode_path_param(request.path_params["run_id"]),
            payload,
            **_expected_operation_owner_kwargs(request),
        )
        result = _mask_authenticated_operation_not_found(request, result, resource="run")
        status = HTTPStatus.ACCEPTED if result.get("status") == "planned" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["POST"], "/api/operations/runs/{run_id}/dispatch", post_operation_run_dispatch, read_body=True)

    def post_workflow_command_cancel(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, actor_fields=("actor",))
        result = orchestrator.cancel_workflow_command_api(
            _decode_path_param(request.path_params["command_id"]), payload
        )
        status = HTTPStatus.OK if result.get("status") == "cancelled" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["POST"], "/api/workflow/commands/{command_id}/cancel", post_workflow_command_cancel, read_body=True)

    def post_workflow_command_retry(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, actor_fields=("actor",))
        result = orchestrator.retry_workflow_command_api(_decode_path_param(request.path_params["command_id"]), payload)
        status = HTTPStatus.ACCEPTED if result.get("status") == "queued" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["POST"], "/api/workflow/commands/{command_id}/retry", post_workflow_command_retry, read_body=True)

    def post_workflow_command_resume(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, actor_fields=("actor",))
        result = orchestrator.resume_workflow_command_api(
            _decode_path_param(request.path_params["command_id"]), payload
        )
        status = HTTPStatus.ACCEPTED if result.get("status") == "queued" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["POST"], "/api/workflow/commands/{command_id}/resume", post_workflow_command_resume, read_body=True)

    def post_intake_excel(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.ingest_excel_contacts(payload)
        status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/intake/excel", post_intake_excel, read_body=True)

    def post_intake_excel_workflow(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, requester=True, tenant=True)
        result = orchestrator.start_excel_intake_workflow(payload)
        status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/intake/excel/workflow", post_intake_excel_workflow, read_body=True)

    def post_intake_excel_continue(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.continue_excel_intake_review(payload)
        status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/intake/excel/continue", post_intake_excel_continue, read_body=True)

    def post_criteria_feedback(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        invalid = _invalid_external_criteria_request(payload)
        if invalid is not None:
            return invalid
        result = orchestrator.record_criteria_feedback(payload, **_expected_job_owner_kwargs(request))
        status = HTTPStatus.CREATED
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/criteria/feedback", post_criteria_feedback, read_body=True)

    def post_plan_review(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, actor_fields=("reviewer",))
        result = orchestrator.review_plan_session(payload)
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        else:
            status = HTTPStatus.OK
        return _json_response(status, result)

    add(["POST"], "/api/plan/review", post_plan_review, read_body=True)

    def post_plan_review_compile_instruction(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        result = orchestrator.compile_plan_review_instruction(payload)
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/plan/review/compile-instruction", post_plan_review_compile_instruction, read_body=True)

    def post_results_refine_compile_instruction(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        result = orchestrator.compile_post_acquisition_refinement(
            payload,
            **_expected_job_owner_kwargs(request),
        )
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/results/refine/compile-instruction", post_results_refine_compile_instruction, read_body=True)

    def post_results_refine(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.apply_post_acquisition_refinement(
            payload,
            **_expected_job_owner_kwargs(request),
        )
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/results/refine", post_results_refine, read_body=True)

    def post_criteria_confidence_policy(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        invalid = _invalid_external_criteria_request(payload)
        if invalid is not None:
            return invalid
        result = orchestrator.configure_confidence_policy(payload, **_expected_job_owner_kwargs(request))
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/criteria/confidence-policy", post_criteria_confidence_policy, read_body=True)

    def post_criteria_suggestions_review(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.review_pattern_suggestion(payload, **_expected_job_owner_kwargs(request))
        status = HTTPStatus.OK if result.get("status") != "not_found" else HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["POST"], "/api/criteria/suggestions/review", post_criteria_suggestions_review, read_body=True)

    def post_manual_review_review(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, actor_fields=("reviewer",))
        result = orchestrator.review_manual_review_item(payload)
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") in {"invalid", "candidate_not_found"}:
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/manual-review/review", post_manual_review_review, read_body=True)

    def post_candidate_review_registry(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, owner=True)
        result = orchestrator.upsert_candidate_review_record(payload)
        status = HTTPStatus.CREATED if result.get("status") == "upserted" else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/candidate-review-registry", post_candidate_review_registry, read_body=True)

    def post_target_candidates_export(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        if not _legacy_target_candidate_export_allowed():
            return _json_response(
                HTTPStatus.GONE,
                {
                    "status": "retired",
                    "reason": "legacy_target_candidate_export_retired",
                    "canonical_export_path": "/api/projections/export",
                    "read_contract": {
                        "source": "projection_export_policy_v1",
                        "fallback_used": False,
                        "fail_closed": True,
                    },
                },
            )
        result = orchestrator.export_target_candidates_archive(payload)
        if result.get("status") == "not_found":
            return _json_response(HTTPStatus.NOT_FOUND, result)
        if result.get("status") == "invalid":
            return _json_response(HTTPStatus.BAD_REQUEST, result)
        return _bytes_response(
            HTTPStatus.OK,
            bytes(result.get("body") or b""),
            content_type=str(result.get("content_type") or "application/octet-stream"),
            filename=str(result.get("filename") or "download.bin"),
            extra_headers={
                "X-Sourcing-Legacy-Export-Path": "target_candidates",
                "X-Sourcing-Canonical-Export-Path": "projections_export",
                "X-Sourcing-Export-Cutover-Status": "legacy_compatibility_path",
            },
        )

    add(["POST"], "/api/target-candidates/export", post_target_candidates_export, read_body=True)

    def post_target_public_web_export_gone(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        return _json_response(
            HTTPStatus.GONE,
            _legacy_target_public_web_endpoint_payload(
                legacy_endpoint="/api/target-candidates/public-web-export",
                canonical_endpoint="/api/crm/records/public-web-export",
                operation="export",
            ),
        )

    add(["POST"], "/api/target-candidates/public-web-export", post_target_public_web_export_gone, read_body=True)

    def post_crm_public_web_export(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        # C1.4 (CRM mirror of projection): submit -> 202 + {task_id} (the worker CRM
        # export drain builds the archive off the request thread); the client polls
        # GET /api/exports/{task_id} then downloads GET /api/exports/{task_id}/artifact.
        # An idempotent hit on an already-succeeded export replays 200 + its handle.
        _apply_server_identity(payload, request, workspace=True)
        result = orchestrator.export_crm_record_public_web_archive(payload)
        status = str(result.get("status") or "").strip()
        if status == "not_found":
            return _json_response(HTTPStatus.NOT_FOUND, result)
        if status == "invalid":
            return _json_response(HTTPStatus.BAD_REQUEST, result)
        if status == "queued":
            return _json_response(HTTPStatus.ACCEPTED, result)
        if status == "succeeded":
            return _json_response(HTTPStatus.OK, result)
        # Fail-closed owner conditions (stale input watermark, contract failure, enqueue
        # failure, ...) keep the legacy 409 — the same status the synchronous route used.
        return _json_response(HTTPStatus.CONFLICT, result)

    add(["POST"], "/api/crm/records/public-web-export", post_crm_public_web_export, read_body=True)

    def post_target_candidates_import_from_job(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        denied = _gate_job_write_owner(request, str(payload.get("job_id") or "").strip())
        if denied is not None:
            return denied
        result = orchestrator.import_target_candidates_from_job(
            payload,
            **_expected_job_owner_kwargs(request),
        )
        status = HTTPStatus.CREATED if result.get("status") == "imported" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["POST"], "/api/target-candidates/import-from-job", post_target_candidates_import_from_job, read_body=True)

    def post_crm_records(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(
            payload,
            request,
            workspace=True,
            actor_fields=("actor_id",),
            lock_actor_type=True,
        )
        result = orchestrator.add_projection_candidate_to_crm(payload)
        status = HTTPStatus.CREATED if result.get("status") in {"upserted", "idempotent"} else HTTPStatus.BAD_REQUEST
        if result.get("status") == "reselected":
            status = HTTPStatus.OK
        if result.get("status") == "partial":
            status = HTTPStatus.MULTI_STATUS
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "not_ready":
            status = HTTPStatus.CONFLICT
        return _json_response(status, result)

    add(["POST"], "/api/crm/records", post_crm_records, read_body=True)

    def post_crm_backfill_target_candidates(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        _apply_server_identity(payload, request, workspace=True)
        result = orchestrator.backfill_crm_from_target_candidates(payload)
        status = HTTPStatus.OK if result.get("status") in {"backfilled", "dry_run"} else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/crm/backfill-target-candidates", post_crm_backfill_target_candidates, read_body=True)

    def post_projections_backfill_from_job(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        source_job_id = str(payload.get("job_id") or payload.get("run_id") or "").strip()
        denied = _gate_job_write_owner(request, source_job_id)
        if denied is not None:
            return denied
        result = orchestrator.backfill_serving_projection_for_job(
            payload,
            **_expected_job_owner_kwargs(request),
        )
        status = HTTPStatus.CREATED if result.get("status") == "backfilled" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "skipped_existing_projection":
            status = HTTPStatus.OK
        elif result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "not_ready":
            status = HTTPStatus.CONFLICT
        return _json_response(status, result)

    add(["POST"], "/api/projections/backfill-from-job", post_projections_backfill_from_job, read_body=True)

    def post_projections_rebuild_person_search_index(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        result = orchestrator.rebuild_projection_person_search_index_api(payload)
        status = HTTPStatus.OK if result.get("status") == "indexed" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(
        ["POST"],
        "/api/projections/rebuild-person-search-index",
        post_projections_rebuild_person_search_index,
        read_body=True,
    )

    def post_projections_backfill_person_summary_views(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        result = orchestrator.backfill_projection_person_summary_views_api(payload)
        status = HTTPStatus.OK if result.get("status") in {"backfilled", "dry_run"} else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(
        ["POST"],
        "/api/projections/backfill-person-summary-views",
        post_projections_backfill_person_summary_views,
        read_body=True,
    )

    def post_projections_backfill_person_search_indexes(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        result = orchestrator.backfill_projection_person_search_indexes_api(payload)
        status = HTTPStatus.OK if result.get("status") in {"backfilled", "dry_run"} else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(
        ["POST"],
        "/api/projections/backfill-person-search-indexes",
        post_projections_backfill_person_search_indexes,
        read_body=True,
    )

    def post_persons_backfill_raw_evidence_indexes(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        result = orchestrator.backfill_person_raw_evidence_indexes_api(payload)
        status = HTTPStatus.OK if result.get("status") in {"backfilled", "dry_run"} else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(
        ["POST"],
        "/api/persons/backfill-raw-evidence-indexes",
        post_persons_backfill_raw_evidence_indexes,
        read_body=True,
    )

    def post_persons_backfill_public_web_signals(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        # Attribution only: workspace_id here scopes a backfill query (canonical
        # layer), not ownership, so it is intentionally left to the caller/default.
        _apply_server_identity(payload, request, actor_fields=("requested_by",))
        result = orchestrator.backfill_public_web_signals_to_person_asset_layer(payload)
        status = HTTPStatus.OK if result.get("status") in {"backfilled", "dry_run"} else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(
        ["POST"],
        "/api/persons/backfill-public-web-signals",
        post_persons_backfill_public_web_signals,
        read_body=True,
    )

    def post_media_backfill_person_avatars(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        result = orchestrator.backfill_person_avatar_media_assets_api(payload)
        status = HTTPStatus.OK if result.get("status") in {"planned", "dry_run"} else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["POST"], "/api/media/backfill-person-avatars", post_media_backfill_person_avatars, read_body=True)

    def post_media_backfill_company_logos(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.backfill_company_logo_media_assets_api(payload)
        status = HTTPStatus.OK if result.get("status") in {"planned", "dry_run"} else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/media/backfill-company-logos", post_media_backfill_company_logos, read_body=True)

    def post_media_ingest_company_logo_from_profile(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        result = orchestrator.ingest_company_logo_from_profile_experience_api(payload)
        status = (
            HTTPStatus.OK
            if result.get("status") in {"planned", "dry_run", "skipped", "source_discovery_required"}
            else HTTPStatus.BAD_REQUEST
        )
        return _json_response(status, result)

    add(
        ["POST"],
        "/api/media/ingest-company-logo-from-profile",
        post_media_ingest_company_logo_from_profile,
        read_body=True,
    )

    def post_company_assets_backfill_public_web_assets(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        result = orchestrator.backfill_company_public_web_assets_to_company_asset_layer_api(payload)
        status = HTTPStatus.OK if result.get("status") in {"backfilled", "dry_run"} else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(
        ["POST"],
        "/api/company-assets/backfill-public-web-assets",
        post_company_assets_backfill_public_web_assets,
        read_body=True,
    )

    def post_crm_backfill_public_web_promotions(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        _apply_server_identity(payload, request, workspace=True)
        result = orchestrator.backfill_public_web_promotions_to_person_assertions(payload)
        status = HTTPStatus.OK if result.get("status") in {"backfilled", "dry_run"} else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/crm/backfill-public-web-promotions", post_crm_backfill_public_web_promotions, read_body=True)

    def post_projections_export(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        # C1.4b: submit -> 202 + {task_id, status:"queued"} — the worker export drain
        # builds the archive off the request thread; the client polls
        # GET /api/exports/{task_id} then downloads GET /api/exports/{task_id}/artifact.
        # An idempotent hit on an already-succeeded export replays 200 + its handle.
        result = orchestrator.export_projection_candidates_archive(payload)
        status = str(result.get("status") or "").strip()
        if status in {"failed", "invalid"}:
            return _json_response(HTTPStatus.BAD_REQUEST, result)
        if status == "not_found":
            return _json_response(HTTPStatus.NOT_FOUND, result)
        if status == "not_ready":
            return _json_response(HTTPStatus.CONFLICT, result)
        if status == "succeeded":
            return _json_response(HTTPStatus.OK, result)
        return _json_response(HTTPStatus.ACCEPTED, result)

    add(["POST"], "/api/projections/export", post_projections_export, read_body=True)

    def post_crm_public_web_search(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, workspace=True, actor_fields=("requested_by",), owner=True)
        result = orchestrator.start_crm_record_public_web_search(payload)
        status = HTTPStatus.ACCEPTED if result.get("status") in {"queued", "joined"} else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["POST"], "/api/crm/records/public-web-search", post_crm_public_web_search, read_body=True)

    def post_crm_public_web_search_poll(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, workspace=True)
        result = orchestrator.list_crm_record_public_web_searches(payload)
        status = HTTPStatus.OK if result.get("status") in {"ok", "ready"} else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["POST"], "/api/crm/records/public-web-search/poll", post_crm_public_web_search_poll, read_body=True)

    def post_crm_public_web_search_cancel(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, workspace=True, actor_fields=("operator", "requested_by"))
        result = orchestrator.cancel_crm_record_public_web_search(payload)
        status = HTTPStatus.OK if result.get("status") in {"cancelled", "skipped"} else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["POST"], "/api/crm/records/public-web-search/cancel", post_crm_public_web_search_cancel, read_body=True)

    def post_crm_public_web_search_retry(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, workspace=True, actor_fields=("operator", "requested_by"))
        result = orchestrator.retry_crm_record_public_web_search(payload)
        status = (
            HTTPStatus.ACCEPTED if result.get("status") in {"retried", "queued", "joined"} else HTTPStatus.BAD_REQUEST
        )
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["POST"], "/api/crm/records/public-web-search/retry", post_crm_public_web_search_retry, read_body=True)

    def post_target_public_web_search_gone(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        return _json_response(
            HTTPStatus.GONE,
            _legacy_target_public_web_endpoint_payload(
                legacy_endpoint="/api/target-candidates/public-web-search",
                canonical_endpoint="/api/crm/records/public-web-search",
                operation="start",
            ),
        )

    add(["POST"], "/api/target-candidates/public-web-search", post_target_public_web_search_gone, read_body=True)

    def post_target_public_web_search_poll_gone(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        return _json_response(
            HTTPStatus.GONE,
            _legacy_target_public_web_endpoint_payload(
                legacy_endpoint="/api/target-candidates/public-web-search/poll",
                canonical_endpoint="/api/crm/records/public-web-search/poll",
                operation="poll",
            ),
        )

    add(
        ["POST"],
        "/api/target-candidates/public-web-search/poll",
        post_target_public_web_search_poll_gone,
        read_body=True,
    )

    def post_target_public_web_search_cancel_gone(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        return _json_response(
            HTTPStatus.GONE,
            _legacy_target_public_web_endpoint_payload(
                legacy_endpoint="/api/target-candidates/public-web-search/cancel",
                canonical_endpoint="/api/crm/records/public-web-search/cancel",
                operation="cancel",
            ),
        )

    add(
        ["POST"],
        "/api/target-candidates/public-web-search/cancel",
        post_target_public_web_search_cancel_gone,
        read_body=True,
    )

    def post_target_public_web_search_retry_gone(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        return _json_response(
            HTTPStatus.GONE,
            _legacy_target_public_web_endpoint_payload(
                legacy_endpoint="/api/target-candidates/public-web-search/retry",
                canonical_endpoint="/api/crm/records/public-web-search/retry",
                operation="retry",
            ),
        )

    add(
        ["POST"],
        "/api/target-candidates/public-web-search/retry",
        post_target_public_web_search_retry_gone,
        read_body=True,
    )

    def post_crm_public_web_promotion(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        record_id = _decode_path_param(request.path_params["record_id"])
        denied = _gate_crm_record_write_owner(request, record_id)
        if denied is not None:
            return denied
        # Attribution only: workspace_id is derived server-side from the CRM record
        # lookup, not the payload, so it is intentionally not forced here.
        _apply_server_identity(payload, request, actor_fields=("operator", "requested_by"))
        result = orchestrator.promote_crm_record_public_web_signal(
            record_id,
            payload,
            **_expected_crm_owner_kwargs(request),
        )
        if result.get("status") == "not_found" and result.get("reason") == "crm_record_not_found":
            result = dict(_CRM_RECORD_NOT_FOUND_BODY)
        status = HTTPStatus.CREATED if result.get("status") in {"promoted", "rejected"} else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "conflict":
            status = HTTPStatus.CONFLICT
        return _json_response(status, result)

    add(["POST"], "/api/crm/records/{record_id}/public-web-promotions", post_crm_public_web_promotion, read_body=True)

    def post_target_public_web_promotion_gone(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        return _json_response(
            HTTPStatus.GONE,
            _legacy_target_public_web_endpoint_payload(
                legacy_endpoint="/api/target-candidates/{record_id}/public-web-promotions",
                canonical_endpoint="/api/crm/records/{crm_record_id}/public-web-promotions",
                operation="promotion",
                record_id=_decode_path_param(request.path_params["record_id"]),
            ),
        )

    add(
        ["POST"],
        "/api/target-candidates/{record_id}/public-web-promotions",
        post_target_public_web_promotion_gone,
        read_body=True,
    )

    def post_target_candidates(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        _apply_server_identity(payload, request, owner=True)
        result = orchestrator.upsert_target_candidate(payload)
        status = HTTPStatus.CREATED if result.get("status") == "upserted" else HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/target-candidates", post_target_candidates, read_body=True)

    def post_assets_governance_promote_default(
        request: Request, query: dict[str, Any], payload: dict[str, Any]
    ) -> Response:
        result = orchestrator.promote_asset_default_pointer(payload)
        status = HTTPStatus.CREATED if result.get("status") == "promoted" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "noop":
            status = HTTPStatus.OK
        return _json_response(status, result)

    add(["POST"], "/api/assets/governance/promote-default", post_assets_governance_promote_default, read_body=True)

    def post_manual_review_synthesize(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.synthesize_manual_review_item(payload)
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/manual-review/synthesize", post_manual_review_synthesize, read_body=True)

    def post_criteria_recompile(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        invalid = _invalid_external_criteria_request(payload)
        if invalid is not None:
            return invalid
        result = orchestrator.recompile_criteria(payload, **_expected_job_owner_kwargs(request))
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/criteria/recompile", post_criteria_recompile, read_body=True)

    def post_workers_interrupt(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        result = orchestrator.interrupt_agent_worker(payload, **_expected_job_owner_kwargs(request))
        status = HTTPStatus.OK
        if result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        elif result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        return _json_response(status, result)

    add(["POST"], "/api/workers/interrupt", post_workers_interrupt, read_body=True)

    def post_workers_cleanup(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        if _server_identity(request) is not None:
            job_id = str(payload.get("job_id") or "").strip()
            if not job_id:
                return _json_response(HTTPStatus.FORBIDDEN, _ADMIN_SCOPE_REQUIRED_BODY)
            denied = _gate_job_write_owner(request, job_id)
            if denied is not None:
                return denied
        result = orchestrator.cleanup_recoverable_workers(payload, **_expected_job_owner_kwargs(request))
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "forbidden":
            status = HTTPStatus.FORBIDDEN
        return _json_response(status, result)

    add(["POST"], "/api/workers/cleanup", post_workers_cleanup, read_body=True)

    def post_workers_daemon_run_once(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        # C3a: the API process is a signaler, never a recovery runner. Client
        # payload fields are deliberately ignored so stale thresholds, limits,
        # job scope, phases, or fallback controls cannot cross this boundary.
        if _server_identity(request) is not None:
            return _json_response(HTTPStatus.FORBIDDEN, _ADMIN_SCOPE_REQUIRED_BODY)
        result = orchestrator.signal_shared_recovery(
            reason="operator_api_recovery_signal",
            requested_by="operator_api",
        )
        status = HTTPStatus.ACCEPTED if result.get("status") == "accepted" else HTTPStatus.SERVICE_UNAVAILABLE
        return _json_response(status, result)

    add(["POST"], "/api/workers/daemon/run-once", post_workers_daemon_run_once, read_body=True)

    def post_runtime_services_shutdown(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        identity = _server_identity(request)
        if identity is not None:
            job_id = str(payload.get("job_id") or "").strip()
            if (
                not job_id
                or payload.get("service_names")
                or payload.get("service_name")
                or bool(payload.get("include_hosted_watchdog"))
                or bool(payload.get("include_shared_recovery"))
            ):
                return _json_response(HTTPStatus.FORBIDDEN, _ADMIN_SCOPE_REQUIRED_BODY)
            denied = _gate_job_write_owner(request, job_id)
            if denied is not None:
                return denied
            payload["requested_by"] = identity["user_id"]
        result = orchestrator.request_runtime_service_shutdown(
            payload,
            **_expected_job_owner_kwargs(request),
        )
        status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "forbidden":
            status = HTTPStatus.FORBIDDEN
        return _json_response(status, result)

    add(["POST"], "/api/runtime/services/shutdown", post_runtime_services_shutdown, read_body=True)

    def post_job_cancel(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        denied = _gate_job_write_owner(request, request.path_params["job_id"])
        if denied is not None:
            return denied
        result = orchestrator.cancel_workflow_job(
            request.path_params["job_id"],
            payload,
            **_expected_job_owner_kwargs(request),
        )
        status = HTTPStatus.OK
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "invalid":
            status = HTTPStatus.BAD_REQUEST
        return _json_response(status, result)

    add(["POST"], "/api/jobs/{job_id:sourcing_ident}/cancel", post_job_cancel, read_body=True)

    def post_workers_daemon_systemd_unit(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        if _server_identity(request) is not None:
            return _json_response(HTTPStatus.FORBIDDEN, _ADMIN_SCOPE_REQUIRED_BODY)
        return _json_response(HTTPStatus.OK, orchestrator.write_worker_daemon_systemd_unit(payload))

    add(["POST"], "/api/workers/daemon/systemd-unit", post_workers_daemon_systemd_unit, read_body=True)

    # ---------------------------------------------------------------- PATCH
    def patch_crm_record(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        record_id = _decode_path_param(request.path_params["record_id"])
        denied = _gate_crm_record_write_owner(request, record_id)
        if denied is not None:
            return denied
        _apply_server_identity(
            payload,
            request,
            workspace=True,
            actor_fields=("actor_id",),
            lock_actor_type=True,
        )
        result = orchestrator.update_crm_record_api(
            record_id,
            payload,
            **_expected_crm_owner_kwargs(request),
        )
        if result.get("status") == "not_found" and result.get("reason") == "crm_record_not_found":
            result = dict(_CRM_RECORD_NOT_FOUND_BODY)
        status = HTTPStatus.OK if result.get("status") == "updated" else HTTPStatus.BAD_REQUEST
        if result.get("status") == "not_found":
            status = HTTPStatus.NOT_FOUND
        elif result.get("status") == "conflict":
            status = HTTPStatus.CONFLICT
        return _json_response(status, result)

    add(["PATCH"], "/api/crm/records/{record_id}", patch_crm_record, read_body=True)

    # --------------------------------------------------------- 404 fallback
    def not_found(request: Request, query: dict[str, Any], payload: dict[str, Any]) -> Response:
        return _json_response(HTTPStatus.NOT_FOUND, {"error": "not found"})

    add(["GET", "POST", "DELETE", "PATCH"], "/{unmatched_path:path}", not_found)

    return routes


def _load_allowed_origins() -> tuple[str, ...]:
    raw = str(
        os.getenv(
            "SOURCING_API_ALLOWED_ORIGINS",
            "http://127.0.0.1:4173,http://localhost:4173",
        )
    ).strip()
    if not raw:
        return ()
    origins = tuple(item.strip() for item in raw.split(",") if item.strip())
    return origins


def _decode_http_request_body(raw: bytes, content_type: str) -> dict[str, Any]:
    if not raw:
        return {}
    normalized_content_type = str(content_type or "").split(";", 1)[0].strip().lower()
    if not normalized_content_type or normalized_content_type == "application/json":
        return json.loads(raw.decode("utf-8"))
    if normalized_content_type == "application/x-www-form-urlencoded":
        decoded = raw.decode("utf-8")
        return {
            key: _coerce_form_scalar(values[-1])
            for key, values in parse_qs(decoded, keep_blank_values=True).items()
            if values
        }
    if normalized_content_type == "multipart/form-data":
        return _decode_multipart_form_data(raw, content_type)
    return json.loads(raw.decode("utf-8"))


def _decode_multipart_form_data(raw: bytes, content_type: str) -> dict[str, Any]:
    message = BytesParser(policy=default_email_policy).parsebytes(
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("utf-8") + raw
    )
    if not message.is_multipart():
        return {}
    payload: dict[str, Any] = {}
    for part in message.iter_parts():
        field_name = str(part.get_param("name", header="content-disposition") or "").strip()
        if not field_name:
            continue
        filename = str(part.get_filename() or "").strip()
        part_bytes = part.get_payload(decode=True) or b""
        if filename:
            payload["filename"] = filename
            payload["file_content_base64"] = base64.b64encode(part_bytes).decode("ascii")
            continue
        payload[field_name] = _coerce_form_scalar(
            part_bytes.decode(part.get_content_charset() or "utf-8", errors="replace")
        )
    return payload


def _coerce_form_scalar(value: str) -> Any:
    lowered = str(value).strip().lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    return value


def _is_local_dev_origin(origin: str) -> bool:
    try:
        parsed = urlparse(origin)
    except ValueError:
        return False
    if parsed.scheme not in {"http", "https"}:
        return False
    hostname = str(parsed.hostname or "").strip().lower()
    if hostname not in {"localhost", "127.0.0.1"}:
        return False
    return bool(parsed.port)


class _RequestConcurrencyController:
    """Legacy thread-based two-lane controller (kept for interface compatibility).

    The live transport now enforces the same semantics in
    `_RequestConcurrencyMiddleware`; this class remains for callers that built
    against the previous stdlib server object.
    """

    def __init__(self, *, shared_limit: int, light_reserved_limit: int) -> None:
        self._shared = threading.BoundedSemaphore(max(1, shared_limit))
        self._light_reserved = threading.BoundedSemaphore(max(1, light_reserved_limit))

    @contextlib.contextmanager
    def claim(self, method: str, path: str) -> Iterator[None]:
        if _request_priority_lane(method, path) == "light" and self._shared.acquire(blocking=False):
            try:
                yield
            finally:
                self._shared.release()
            return

        semaphore = self._light_reserved if _request_priority_lane(method, path) == "light" else self._shared
        semaphore.acquire()
        try:
            yield
        finally:
            semaphore.release()


def _request_priority_lane(method: str, path: str) -> str:
    normalized_method = str(method or "").upper()
    normalized_path = urlsplit(path or "/").path or "/"
    if normalized_method == "OPTIONS":
        return "light"
    if normalized_method == "POST" and normalized_path == "/api/plan/submit":
        return "light"
    if normalized_method == "POST" and normalized_path == "/api/runtime/services/shutdown":
        return "light"
    if normalized_method == "POST" and normalized_path == "/api/crm/records/public-web-search":
        return "light"
    if normalized_method == "POST" and normalized_path == "/api/crm/records":
        return "light"
    if normalized_method == "POST" and (
        normalized_path == "/api/operations/actions"
        or re.fullmatch(r"/api/operations/actions/[^/]+/(approve|reject)", normalized_path)
        or re.fullmatch(r"/api/operations/runs/[^/]+/(cancel|retry|resume|dispatch)", normalized_path)
        or re.fullmatch(r"/api/workflow/commands/[^/]+/(cancel|retry|resume)", normalized_path)
    ):
        return "light"
    if normalized_method == "PATCH" and re.fullmatch(r"/api/crm/records/[^/]+", normalized_path):
        return "light"
    if normalized_method == "POST" and normalized_path == "/api/crm/backfill-target-candidates":
        return "shared"
    if normalized_method == "POST" and normalized_path == "/api/crm/backfill-public-web-promotions":
        return "shared"
    if normalized_method == "POST" and normalized_path == "/api/persons/backfill-public-web-signals":
        return "shared"
    if normalized_method == "POST" and normalized_path == "/api/media/backfill-person-avatars":
        return "shared"
    if normalized_method == "POST" and normalized_path == "/api/media/backfill-company-logos":
        return "shared"
    if normalized_method == "POST" and normalized_path == "/api/company-assets/backfill-public-web-assets":
        return "shared"
    if normalized_method == "POST" and normalized_path == "/api/projections/backfill-from-job":
        return "shared"
    if normalized_method == "POST" and normalized_path == "/api/projections/rebuild-person-search-index":
        return "shared"
    if normalized_path in {
        "/api/projections/export",
        "/api/crm/records/public-web-export",
    }:
        # These are exact POST submit routes. In particular, do not let the
        # generic GET projection-id classifier below reinterpret the literal
        # ``export`` segment as an ordinary projection read.
        return "light" if normalized_method == "POST" else "shared"
    if normalized_method == "POST" and normalized_path in {
        "/api/crm/records/public-web-search/poll",
        "/api/crm/records/public-web-search/cancel",
        "/api/crm/records/public-web-search/retry",
    }:
        return "light"
    if normalized_method == "POST" and re.fullmatch(r"/api/jobs/[A-Za-z0-9_-]+/cancel", normalized_path):
        return "light"
    if normalized_method != "GET":
        return "shared"
    if re.fullmatch(r"/api/exports/[^/]+", normalized_path):
        return "light"
    if normalized_path in {
        "/health",
        "/api/providers/health",
        "/api/runtime/health",
        "/api/runtime/metrics",
        "/api/runtime/progress",
        "/api/criteria/patterns",
        "/api/plan/reviews",
        "/api/query-dispatches",
        "/api/workflow/command-registry",
        "/api/workflow/commands",
        "/api/workflow/activities",
        "/api/workflow/activity-attempts",
        "/api/workflow/entity-deltas",
        "/api/workflow/discovery-lanes",
        "/api/migrations/legacy-public-web",
        "/api/migrations/legacy-result-endpoints",
        "/api/manual-review",
        "/api/candidate-review-registry",
        "/api/target-candidates",
        "/api/crm/records",
        "/api/crm/tasks",
        "/api/assets/governance/default-pointers",
        "/api/frontend-history",
        "/api/workers/recoverable",
        "/api/workers/daemon/status",
    }:
        return "light"
    if re.fullmatch(r"/api/frontend-history/[^/]+", normalized_path):
        return "light"
    if re.fullmatch(r"/api/workflow/commands/[^/]+", normalized_path):
        return "light"
    if re.fullmatch(r"/api/workflow/(activities|activity-attempts)/[^/]+", normalized_path):
        return "light"
    if re.fullmatch(r"/api/workflow/entity-deltas/[^/]+", normalized_path):
        return "light"
    if re.fullmatch(r"/api/workflow/discovery-lanes/[^/]+", normalized_path):
        return "light"
    if re.fullmatch(r"/api/crm/records/[^/]+/tasks", normalized_path):
        return "light"
    if re.fullmatch(r"/api/crm/records/[^/]+/(profile|public-web-search|public-web-promotions)", normalized_path):
        return "light"
    if re.fullmatch(
        r"/api/operations/(action-registry|actions|runs|actions/[^/]+|runs/[^/]+|runs/[^/]+/provenance)",
        normalized_path,
    ):
        return "light"
    if re.fullmatch(r"/api/runs/[A-Za-z0-9_-]+/projection-link", normalized_path):
        return "light"
    if normalized_path == "/api/collections":
        return "light"
    if re.fullmatch(r"/api/collections/[^/]+/(authoritative-projection|asset-entry|coverage)", normalized_path):
        return "light"
    if re.fullmatch(r"/api/projections/[A-Za-z0-9_-]+(/(candidates|crm-state|export-policy|search))?", normalized_path):
        return "light"
    if re.fullmatch(r"/api/projections/[A-Za-z0-9_-]+/persons/[^/]+", normalized_path):
        return "light"
    if re.fullmatch(r"/api/persons/[^/]+", normalized_path):
        return "light"
    if re.fullmatch(r"/api/media/assets/[^/]+", normalized_path):
        return "light"
    if re.fullmatch(
        r"/api/jobs/[A-Za-z0-9_-]+/(progress|dashboard|candidates|board-patches|materialization-items)",
        normalized_path,
    ):
        return "light"
    return "shared"


def _legacy_target_candidate_export_allowed() -> bool:
    return str(os.getenv("SOURCING_ALLOW_LEGACY_TARGET_CANDIDATE_EXPORT") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _legacy_target_public_web_endpoint_payload(
    *,
    legacy_endpoint: str,
    canonical_endpoint: str,
    operation: str,
    record_id: str = "",
) -> dict[str, Any]:
    return {
        "status": "retired",
        "reason": "legacy_target_public_web_endpoint_retired",
        "legacy_endpoint": legacy_endpoint,
        "canonical_endpoint": canonical_endpoint,
        "record_id": str(record_id or "").strip(),
        "operation": str(operation or "").strip(),
        "migration_override_env": "SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS",
        "migration_override_status": "removed",
        "retirement_mode": "permanent_hard_disable",
        "read_contract": {
            "source": "crm_records+person_public_web_assets",
            "fallback_used": False,
            "fail_closed": True,
            "legacy_target_candidates_used": False,
            "public_web_storage_bridge": "",
            "bridge_removal_phase": "retired",
        },
    }


def _legacy_job_result_endpoint_retired(orchestrator: SourcingOrchestrator, run_id: str) -> bool:
    if _env_bool("SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS", False):
        return False
    explicit_cutover_value = os.getenv("SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS")
    if explicit_cutover_value is not None:
        return _env_bool("SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS", False)
    status_payload = orchestrator.get_legacy_result_endpoint_retirement_status({"run_id": str(run_id or "").strip()})
    return bool(status_payload.get("cutover_enforced"))


def _legacy_job_result_endpoint_payload(
    orchestrator: SourcingOrchestrator,
    run_id: str,
    *,
    legacy_endpoint: str,
    candidate_identity_key: str = "",
) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    status_payload = orchestrator.get_legacy_result_endpoint_retirement_status({"run_id": normalized_run_id})
    projection_id = str(status_payload.get("projection_id") or "").strip()
    projection_url = f"/projections/{projection_id}" if projection_id else ""
    if projection_id and candidate_identity_key:
        projection_url = f"{projection_url}?candidate={quote(candidate_identity_key, safe='')}"
    return {
        "status": "retired",
        "reason": (
            "legacy_job_result_endpoint_retired" if projection_id else "legacy_job_result_endpoint_migration_required"
        ),
        "run_id": normalized_run_id,
        "legacy_endpoint": legacy_endpoint,
        "projection_id": projection_id,
        "projection_url": projection_url,
        "candidate_identity_key": str(candidate_identity_key or "").strip(),
        "retirement": status_payload,
        "read_contract": {
            "source": "run_projection_links+serving_projections",
            "fallback_used": False,
            "fail_closed": True,
        },
    }


def _default_light_request_reserved(max_parallel_requests: int) -> int:
    if max_parallel_requests <= 2:
        return 1
    return min(4, max(2, max_parallel_requests // 4))


def _provider_webhook_token_allowed(
    headers: Any,
    query_payload: dict[str, Any],
    *,
    orchestrator: SourcingOrchestrator | None = None,
) -> bool:
    expected_tokens = _provider_webhook_expected_tokens(orchestrator)
    if not expected_tokens:
        return _env_bool("SOURCING_ALLOW_UNSIGNED_PROVIDER_WEBHOOKS", False)
    observed = (
        str(dict(query_payload or {}).get("token") or "").strip()
        or str(headers.get("X-Sourcing-Provider-Webhook-Token") or "").strip()
        or str(headers.get("X-Apify-Webhook-Token") or "").strip()
    )
    return bool(observed and observed in expected_tokens)


def _provider_webhook_expected_tokens(orchestrator: SourcingOrchestrator | None = None) -> set[str]:
    tokens = {
        str(os.getenv("SOURCING_PROVIDER_WEBHOOK_TOKEN") or "").strip(),
        str(os.getenv("APIFY_WEBHOOK_TOKEN") or "").strip(),
    }
    if _env_bool("SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN", True):
        acquisition_engine = getattr(orchestrator, "acquisition_engine", None)
        settings = getattr(acquisition_engine, "settings", None)
        harvest_settings = getattr(settings, "harvest", None)
        for actor_name in ("profile_scraper", "profile_search", "company_employees"):
            actor_settings = getattr(harvest_settings, actor_name, None)
            tokens.add(str(getattr(actor_settings, "api_token", "") or "").strip())
    return {token for token in tokens if token}


def _provider_webhook_sync_requested(query_payload: dict[str, Any]) -> bool:
    observed = str(dict(query_payload or {}).get("sync") or "").strip().lower()
    if observed in {"1", "true", "yes", "on"}:
        return True
    return _env_bool("SOURCING_PROVIDER_WEBHOOK_SYNC_RECOVERY", False)


def _provider_event_response_payload(event: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in dict(event or {}).items() if key != "raw_payload"}


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_int_from_payload(payload: dict[str, Any], key: str, default: int) -> int:
    raw = str(dict(payload or {}).get(key) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_bool_from_payload(payload: dict[str, Any], key: str, default: bool) -> bool:
    raw = str(dict(payload or {}).get(key) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _candidate_page_filter_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    source = dict(payload or {})
    return {
        "search_keyword": str(source.get("search") or source.get("search_keyword") or "").strip(),
        "recall_buckets": _csv_query_values(source.get("recall_buckets")),
        "employment_statuses": _csv_query_values(source.get("employment_statuses")),
        "locations": _csv_query_values(source.get("locations")),
        "function_buckets": _csv_query_values(source.get("function_buckets")),
        "layer_includes": _csv_query_values(source.get("layer_includes") or source.get("layers")),
        "layer_excludes": _csv_query_values(source.get("layer_excludes")),
        "audit_statuses": _csv_query_values(source.get("audit_statuses")),
    }


def _csv_query_values(value: Any) -> list[str]:
    values: list[str] = []
    for item in str(value or "").split(","):
        normalized = item.strip()
        if normalized:
            values.append(normalized)
    return values
