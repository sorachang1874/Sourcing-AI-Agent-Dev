"""Transport-parity gate for the FastAPI rewrite of sourcing_agent.api.

Locks the HTTP transport surface to the legacy stdlib-server behavior:
route inventory, CORS allowlist + preflight echo, two-lane request
concurrency, webhook token rejection, byte/streaming responses, JSON body
formatting, port-0 ephemeral binding, and shutdown semantics.
"""

import json
import os
import socket
import threading
import time
import unittest
from unittest.mock import patch
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

from sourcing_agent.api import create_app, create_server

# Machine-readable inventory of the legacy route table (registration order).
# Methods exclude the implicit HEAD that Starlette adds to GET routes.
EXPECTED_ROUTES = [
    ("GET", "/health"),
    ("GET", "/api/providers/health"),
    ("GET", "/api/runtime/health"),
    ("GET", "/api/runtime/metrics"),
    ("GET", "/api/runtime/progress"),
    ("GET", "/api/criteria/patterns"),
    ("GET", "/api/plan/reviews"),
    ("GET", "/api/query-dispatches"),
    ("GET", "/api/workflow/command-registry"),
    ("GET", "/api/workflow/commands"),
    ("GET", "/api/workflow/commands/{command_id}"),
    ("GET", "/api/workflow/activities"),
    ("GET", "/api/workflow/activities/{activity_id}"),
    ("GET", "/api/workflow/activity-attempts"),
    ("GET", "/api/workflow/activity-attempts/{attempt_id}"),
    ("GET", "/api/workflow/entity-deltas"),
    ("GET", "/api/workflow/entity-deltas/{delta_id}"),
    ("GET", "/api/workflow/discovery-lanes"),
    ("GET", "/api/workflow/discovery-lanes/{lane_id}"),
    ("GET", "/api/operations/action-registry"),
    ("GET", "/api/operations/actions"),
    ("GET", "/api/operations/runs"),
    ("GET", "/api/operations/runs/{run_id}/provenance"),
    ("GET", "/api/operations/actions/{action_id}"),
    ("GET", "/api/operations/runs/{run_id}"),
    ("GET", "/api/company-assets/public-web"),
    ("GET", "/api/company-assets"),
    ("GET", "/api/company-assets/evidence"),
    ("GET", "/api/company-assets/assertions"),
    ("GET", "/api/media/assets/{asset_id}"),
    ("GET", "/api/exports/{command_id}"),
    ("GET", "/api/exports/{command_id}/artifact"),
    ("GET", "/api/migrations/legacy-result-endpoints"),
    ("GET", "/api/migrations/legacy-public-web"),
    ("GET", "/api/manual-review"),
    ("GET", "/api/candidate-review-registry"),
    ("GET", "/api/target-candidates"),
    ("GET", "/api/target-candidates/public-web-search"),
    ("GET", "/api/crm/records/{record_id}/profile"),
    ("GET", "/api/crm/records/{record_id}/public-web-search"),
    ("GET", "/api/crm/records/{record_id}/public-web-promotions"),
    ("GET", "/api/target-candidates/{record_id}/profile"),
    ("GET", "/api/target-candidates/{record_id}/public-web-search"),
    ("GET", "/api/target-candidates/{record_id}/public-web-promotions"),
    ("GET", "/api/assets/governance/default-pointers"),
    ("GET", "/api/frontend-history"),
    ("GET", "/api/frontend-history/{history_id}"),
    ("GET", "/api/crm/records"),
    ("GET", "/api/crm/tasks"),
    ("GET", "/api/crm/records/{record_id}/tasks"),
    ("GET", "/api/crm/records/{record_id}"),
    ("GET", "/api/workers/recoverable"),
    ("GET", "/api/workers/daemon/status"),
    ("GET", "/api/runs/{run_id:sourcing_ident}/projection-link"),
    ("GET", "/api/collections"),
    ("GET", "/api/collections/{collection_id}/authoritative-projection"),
    ("GET", "/api/collections/{collection_id}/asset-entry"),
    ("GET", "/api/collections/{collection_id}/coverage"),
    ("GET", "/api/projections/{projection_id:sourcing_ident}/crm-state"),
    ("GET", "/api/projections/{projection_id:sourcing_ident}/export-policy"),
    ("GET", "/api/projections/{projection_id:sourcing_ident}/search"),
    ("GET", "/api/projections/{projection_id:sourcing_ident}/persons/{person_key}"),
    ("GET", "/api/projections/{projection_id:sourcing_ident}/candidates"),
    ("GET", "/api/projections/{projection_id:sourcing_ident}"),
    ("GET", "/api/persons/{person_key}"),
    ("GET", "/api/jobs/{job_id:sourcing_ident}/progress"),
    ("GET", "/api/jobs/{job_id:sourcing_ident}/board-patches"),
    ("GET", "/api/jobs/{job_id:sourcing_ident}/dashboard"),
    ("GET", "/api/jobs/{job_id:sourcing_ident}/candidates"),
    ("GET", "/api/jobs/{job_id:sourcing_ident}/results"),
    ("GET", "/api/jobs/{job_id:sourcing_ident}/candidates/{candidate_id}"),
    ("GET", "/api/jobs/{job_id:sourcing_ident}/trace"),
    ("GET", "/api/jobs/{job_id:sourcing_ident}/workers"),
    ("GET", "/api/jobs/{job_id:sourcing_ident}/materialization-items"),
    ("GET", "/api/jobs/{job_id:sourcing_ident}/scheduler"),
    ("GET", "/api/jobs/{job_id:sourcing_ident}"),
    ("DELETE", "/api/frontend-history/{history_id}"),
    ("POST", "/api/bootstrap"),
    # POST /api/plan deleted in C1 (substrate-unify) — unified on /api/plan/submit.
    ("POST", "/api/plan/submit"),
    ("POST", "/api/workflows/explain"),
    ("POST", "/api/providers/apify/webhook"),
    ("POST", "/api/query-dispatches/list"),
    # POST /api/jobs deleted in C1 (substrate-unify) — run_job demoted to CLI/test.
    ("POST", "/api/workflows"),
    ("POST", "/api/workflows/{job_id:sourcing_ident}/continue-stage2"),
    ("POST", "/api/jobs/{job_id:sourcing_ident}/profile-completion"),
    ("POST", "/api/jobs/{job_id:sourcing_ident}/candidates/batch"),
    ("POST", "/api/company-assets/supplement"),
    ("POST", "/api/company-assets/public-web"),
    ("POST", "/api/operations/actions"),
    ("POST", "/api/operations/actions/{action_id}/approve"),
    ("POST", "/api/operations/actions/{action_id}/reject"),
    ("POST", "/api/operations/runs/{run_id}/cancel"),
    ("POST", "/api/operations/runs/{run_id}/retry"),
    ("POST", "/api/operations/runs/{run_id}/resume"),
    ("POST", "/api/operations/runs/{run_id}/dispatch"),
    ("POST", "/api/workflow/commands/{command_id}/cancel"),
    ("POST", "/api/workflow/commands/{command_id}/retry"),
    ("POST", "/api/workflow/commands/{command_id}/resume"),
    ("POST", "/api/intake/excel"),
    ("POST", "/api/intake/excel/workflow"),
    ("POST", "/api/intake/excel/continue"),
    ("POST", "/api/criteria/feedback"),
    ("POST", "/api/plan/review"),
    ("POST", "/api/plan/review/compile-instruction"),
    ("POST", "/api/results/refine/compile-instruction"),
    ("POST", "/api/results/refine"),
    ("POST", "/api/criteria/confidence-policy"),
    ("POST", "/api/criteria/suggestions/review"),
    ("POST", "/api/manual-review/review"),
    ("POST", "/api/candidate-review-registry"),
    ("POST", "/api/target-candidates/export"),
    ("POST", "/api/target-candidates/public-web-export"),
    ("POST", "/api/crm/records/public-web-export"),
    ("POST", "/api/target-candidates/import-from-job"),
    ("POST", "/api/crm/records"),
    ("POST", "/api/crm/backfill-target-candidates"),
    ("POST", "/api/projections/backfill-from-job"),
    ("POST", "/api/projections/rebuild-person-search-index"),
    ("POST", "/api/projections/backfill-person-summary-views"),
    ("POST", "/api/projections/backfill-person-search-indexes"),
    ("POST", "/api/persons/backfill-raw-evidence-indexes"),
    ("POST", "/api/persons/backfill-public-web-signals"),
    ("POST", "/api/media/backfill-person-avatars"),
    ("POST", "/api/media/backfill-company-logos"),
    ("POST", "/api/media/ingest-company-logo-from-profile"),
    ("POST", "/api/company-assets/backfill-public-web-assets"),
    ("POST", "/api/crm/backfill-public-web-promotions"),
    ("POST", "/api/projections/export"),
    ("POST", "/api/crm/records/public-web-search"),
    ("POST", "/api/crm/records/public-web-search/poll"),
    ("POST", "/api/crm/records/public-web-search/cancel"),
    ("POST", "/api/crm/records/public-web-search/retry"),
    ("POST", "/api/target-candidates/public-web-search"),
    ("POST", "/api/target-candidates/public-web-search/poll"),
    ("POST", "/api/target-candidates/public-web-search/cancel"),
    ("POST", "/api/target-candidates/public-web-search/retry"),
    ("POST", "/api/crm/records/{record_id}/public-web-promotions"),
    ("POST", "/api/target-candidates/{record_id}/public-web-promotions"),
    ("POST", "/api/target-candidates"),
    ("POST", "/api/assets/governance/promote-default"),
    ("POST", "/api/manual-review/synthesize"),
    ("POST", "/api/criteria/recompile"),
    ("POST", "/api/workers/interrupt"),
    ("POST", "/api/workers/cleanup"),
    ("POST", "/api/workers/daemon/run-once"),
    ("POST", "/api/runtime/services/shutdown"),
    ("POST", "/api/jobs/{job_id:sourcing_ident}/cancel"),
    ("POST", "/api/workers/daemon/systemd-unit"),
    ("PATCH", "/api/crm/records/{record_id}"),
]
EXPECTED_FALLBACK_ROUTE = ("/{unmatched_path:path}", frozenset({"GET", "POST", "DELETE", "PATCH"}))


class _StubOrchestrator:
    """Minimal orchestrator exposing only the endpoints exercised here."""

    def __init__(self) -> None:
        self.trace_gate = threading.Event()
        self.trace_entered: list[str] = []
        self.webhook_events: list[dict] = []
        self.recovery_signal_requests: list[dict[str, str]] = []

    def get_runtime_metrics(self, _query):
        return {"status": "ok"}

    def get_job_trace(self, job_id):
        self.trace_entered.append(job_id)
        self.trace_gate.wait(timeout=15)
        return {"job_id": job_id}

    def handle_remote_provider_event(self, event_payload):
        self.webhook_events.append(dict(event_payload))
        return {
            "status": "accepted",
            "targets": {},
            "shared_recovery_signal_count": 1,
            "shared_recovery_signal": {
                "status": "signaled",
                "scope": "shared",
                "mode": "signal_only",
                "service_name": "worker-recovery-daemon",
            },
        }

    def signal_shared_recovery(self, *, reason, requested_by):
        self.recovery_signal_requests.append({"reason": reason, "requested_by": requested_by})
        return {
            "status": "accepted",
            "mode": "shared_recovery_signal",
            "shared_recovery_signal": {
                "status": "signaled",
                "scope": "shared",
                "mode": "signal_only",
                "service_name": "worker-recovery-daemon",
            },
        }

    def get_media_asset_content_api(self, asset_id):
        if asset_id == "asset-ready":
            return {
                "status": "ready",
                "content": b"\x89PNG-fake-bytes",
                "content_type": "image/png",
                "contract": "media_asset_read_contract_v1",
            }
        return {"status": "not_found", "asset_id": asset_id, "content": b""}

    def export_projection_candidates_archive(self, _payload):
        # C1.4b: submit returns a 202 async-task envelope (no inline bytes).
        return {
            "task_id": "cmd-export-1",
            "task_type": "export.projection.generate",
            "status": "queued",
            "domain_status": "queued",
            "idempotency_key": "export.projection.generate:abc",
        }

    def get_export_command_status(self, command_id):
        return {
            "task_id": command_id,
            "task_type": "export.projection.generate",
            "status": "succeeded",
            "domain_status": "succeeded",
            "error": None,
            "artifact": {
                "handle": f"/api/exports/{command_id}/artifact",
                "content_type": "application/zip",
                "filename": "projection-export.zip",
                "byte_size": 17,
                "headers": {"X-Sourcing-Projection-Id": "proj-1"},
            },
        }

    def get_export_command_artifact(self, _command_id):
        # C1.4: the orchestrator returns a generic command-type-aware 'headers' dict
        # that the API download handler emits verbatim.
        return {
            "status": "ok",
            "body": b"ZIP-ARCHIVE-BYTES",
            "content_type": "application/zip",
            "filename": "projection-export.zip",
            "headers": {
                "X-Sourcing-Projection-Id": "proj-1",
                "X-Sourcing-Export-Record-Count": "3",
                "X-Sourcing-Exported-Record-Count": "2",
                "X-Sourcing-Skipped-Assertion-Count": "1",
            },
        }

    def export_crm_record_public_web_archive(self, _payload):
        # C1.4: CRM export submit returns a 202 async-task envelope (no inline bytes).
        return {
            "task_id": "cmd-crm-export-1",
            "task_type": "export.crm_public_web.generate",
            "status": "queued",
            "domain_status": "queued",
            "idempotency_key": "export.crm_public_web.generate:abc",
        }


class ApiTransportParityTest(unittest.TestCase):
    def _start_server(self, orchestrator=None, env=None):
        orchestrator = orchestrator or _StubOrchestrator()
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
        return server, thread, f"http://{host}:{port}", opener, orchestrator

    def _request(self, opener, url, *, method="GET", data=None, headers=None, timeout=10):
        request = urllib_request.Request(url, data=data, headers=dict(headers or {}), method=method)
        try:
            with opener.open(request, timeout=timeout) as response:
                # email.message.Message: case-insensitive header lookup like the clients we serve.
                return response.status, response.headers, response.read()
        except HTTPError as error:
            return error.code, error.headers, error.read()

    def test_route_inventory_matches_legacy_route_table(self) -> None:
        app = create_app(_StubOrchestrator())
        observed = [(route.path, frozenset(set(route.methods or ()) - {"HEAD"})) for route in app.routes]
        expected = [(path, frozenset({method})) for method, path in EXPECTED_ROUTES]
        expected.append(EXPECTED_FALLBACK_ROUTE)
        self.assertEqual(observed, expected)

    def test_port_zero_binding_reports_real_port_before_serving(self) -> None:
        server = create_server(_StubOrchestrator(), host="127.0.0.1", port=0)
        try:
            host, port = server.server_address
            self.assertEqual(host, "127.0.0.1")
            self.assertGreater(port, 0)
            self.assertEqual(server.server_port, port)
            self.assertTrue(server.daemon_threads)
            self.assertFalse(server.block_on_close)
        finally:
            server.server_close()

    def test_json_body_formatting_matches_legacy_send_json(self) -> None:
        _server, _thread, base_url, opener, _orchestrator = self._start_server()
        status, headers, body = self._request(opener, f"{base_url}/health")
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("Content-Type"), "application/json; charset=utf-8")
        self.assertEqual(body, json.dumps({"status": "ok"}, ensure_ascii=False, indent=2).encode("utf-8"))

    def test_unknown_paths_and_method_mismatches_return_legacy_404_body(self) -> None:
        _server, _thread, base_url, opener, _orchestrator = self._start_server()
        for method, path in [
            ("GET", "/definitely/not/a/route"),
            ("POST", "/definitely/not/a/route"),
            ("DELETE", "/api/plan"),
            ("PATCH", "/health"),
            ("GET", "/api/jobs/has.invalid.chars"),
        ]:
            status, _headers, body = self._request(opener, f"{base_url}{path}", method=method)
            self.assertEqual(status, 404, msg=f"{method} {path}")
            self.assertEqual(json.loads(body), {"error": "not found"}, msg=f"{method} {path}")

    def test_cors_preflight_and_response_headers(self) -> None:
        _server, _thread, base_url, opener, _orchestrator = self._start_server()
        # Preflight: allowlisted default origin + request-header echo.
        status, headers, body = self._request(
            opener,
            f"{base_url}/api/plan",
            method="OPTIONS",
            headers={
                "Origin": "http://127.0.0.1:4173",
                "Access-Control-Request-Headers": "X-Custom-Header, Content-Type",
            },
        )
        self.assertEqual(status, 204)
        self.assertEqual(body, b"")
        self.assertEqual(headers.get("Access-Control-Allow-Origin"), "http://127.0.0.1:4173")
        self.assertEqual(headers.get("Vary"), "Origin")
        self.assertEqual(headers.get("Access-Control-Allow-Methods"), "GET, POST, PATCH, DELETE, OPTIONS")
        self.assertEqual(headers.get("Access-Control-Allow-Headers"), "X-Custom-Header, Content-Type")
        self.assertEqual(headers.get("Access-Control-Max-Age"), "600")
        self.assertIn("Content-Disposition", str(headers.get("Access-Control-Expose-Headers")))
        # Localhost origins outside the allowlist are auto-allowed.
        status, headers, _body = self._request(
            opener,
            f"{base_url}/health",
            headers={"Origin": "http://localhost:5173"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("Access-Control-Allow-Origin"), "http://localhost:5173")
        # Non-local origins outside the allowlist get no allow-origin echo.
        status, headers, _body = self._request(
            opener,
            f"{base_url}/health",
            headers={"Origin": "https://evil.example.com"},
        )
        self.assertEqual(status, 200)
        self.assertIsNone(headers.get("Access-Control-Allow-Origin"))
        self.assertEqual(headers.get("Access-Control-Allow-Methods"), "GET, POST, PATCH, DELETE, OPTIONS")
        # No Origin header: no allow-origin, but shared CORS headers still present.
        status, headers, _body = self._request(opener, f"{base_url}/health")
        self.assertEqual(status, 200)
        self.assertIsNone(headers.get("Access-Control-Allow-Origin"))
        self.assertEqual(headers.get("Access-Control-Allow-Headers"), "Content-Type")

    def test_two_lane_concurrency_queues_heavy_and_preserves_light_reserve(self) -> None:
        orchestrator = _StubOrchestrator()
        _server, _thread, base_url, opener, orchestrator = self._start_server(
            orchestrator,
            env={
                "SOURCING_API_MAX_PARALLEL_REQUESTS": "2",
                "SOURCING_API_LIGHT_REQUEST_RESERVED": "1",
            },
        )
        results: dict[str, int] = {}

        def heavy(job_id: str) -> None:
            status, _headers, _body = self._request(opener, f"{base_url}/api/jobs/{job_id}/trace", timeout=30)
            results[job_id] = status

        self.addCleanup(orchestrator.trace_gate.set)
        # Saturate the shared lane (limit 2) with blocking heavy GETs.
        workers = [threading.Thread(target=heavy, args=(job_id,), daemon=True) for job_id in ("j1", "j2")]
        for worker in workers:
            worker.start()
        deadline = time.monotonic() + 10
        while len(orchestrator.trace_entered) < 2 and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual(sorted(orchestrator.trace_entered), ["j1", "j2"])
        # A third heavy request must queue at the transport, not reach the orchestrator.
        queued = threading.Thread(target=heavy, args=("j3",), daemon=True)
        queued.start()
        workers.append(queued)
        time.sleep(0.4)
        self.assertEqual(sorted(orchestrator.trace_entered), ["j1", "j2"])
        # Light-lane requests still get through via the reserved slot.
        status, _headers, body = self._request(opener, f"{base_url}/health", timeout=10)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"status": "ok"})
        # Release the gate: queued heavy request runs to completion.
        orchestrator.trace_gate.set()
        for worker in workers:
            worker.join(timeout=15)
        self.assertEqual(results, {"j1": 200, "j2": 200, "j3": 200})
        self.assertEqual(sorted(orchestrator.trace_entered), ["j1", "j2", "j3"])

    def test_apify_webhook_token_enforcement(self) -> None:
        _server, _thread, base_url, opener, orchestrator = self._start_server()
        webhook_url = f"{base_url}/api/providers/apify/webhook"
        body = json.dumps({"run_id": "run-1"}).encode("utf-8")
        json_headers = {"Content-Type": "application/json"}
        with patch.dict(os.environ, {"SOURCING_PROVIDER_WEBHOOK_TOKEN": "expected-token"}):
            os.environ.pop("APIFY_WEBHOOK_TOKEN", None)
            os.environ.pop("SOURCING_ALLOW_UNSIGNED_PROVIDER_WEBHOOKS", None)
            # Missing token -> forbidden.
            status, _headers, response_body = self._request(
                opener, webhook_url, method="POST", data=body, headers=json_headers
            )
            self.assertEqual(status, 403)
            self.assertEqual(
                json.loads(response_body),
                {"status": "forbidden", "reason": "provider_webhook_token_required"},
            )
            # Wrong token -> forbidden.
            status, _headers, _response_body = self._request(
                opener,
                webhook_url,
                method="POST",
                data=body,
                headers={**json_headers, "X-Sourcing-Provider-Webhook-Token": "wrong"},
            )
            self.assertEqual(status, 403)
            self.assertEqual(orchestrator.webhook_events, [])
            # Valid token but no run/dataset id -> invalid.
            status, _headers, response_body = self._request(
                opener,
                webhook_url,
                method="POST",
                data=json.dumps({}).encode("utf-8"),
                headers={**json_headers, "X-Sourcing-Provider-Webhook-Token": "expected-token"},
            )
            self.assertEqual(status, 400)
            self.assertEqual(
                json.loads(response_body).get("reason"),
                "remote_provider_event_missing_run_or_dataset_id",
            )
            # Valid token with run id -> durable handoff plus shared-daemon signal.
            status, _headers, response_body = self._request(
                opener,
                webhook_url,
                method="POST",
                data=body,
                headers={**json_headers, "X-Sourcing-Provider-Webhook-Token": "expected-token"},
            )
            self.assertEqual(status, 202)
            payload = json.loads(response_body)
            self.assertEqual(payload.get("status"), "accepted")
            self.assertEqual(payload.get("mode"), "shared_recovery_signal")
            self.assertEqual(payload.get("shared_recovery_signal_count"), 1)
            self.assertEqual(len(orchestrator.webhook_events), 1)
            self.assertEqual(orchestrator.webhook_events[0].get("recovery_mode"), "shared_recovery_signal")

            # sync=1 is retired fail-closed and never reaches the orchestrator.
            status, _headers, response_body = self._request(
                opener,
                f"{webhook_url}?sync=1",
                method="POST",
                data=body,
                headers={**json_headers, "X-Sourcing-Provider-Webhook-Token": "expected-token"},
            )
            self.assertEqual(status, 410)
            retired = json.loads(response_body)
            self.assertEqual(retired.get("reason"), "provider_webhook_sync_recovery_retired")
            self.assertEqual(len(orchestrator.webhook_events), 1)

    def test_apify_webhook_keeps_202_handoff_but_does_not_count_failed_signal(self) -> None:
        class _FailedWebhookSignalOrchestrator(_StubOrchestrator):
            def handle_remote_provider_event(self, event_payload):
                self.webhook_events.append(dict(event_payload))
                return {
                    "status": "accepted",
                    # Deliberately inconsistent legacy field: the API must derive
                    # evidence from the complete signal status instead.
                    "shared_recovery_signal_count": 1,
                    "shared_recovery_signal": {
                        "status": "signal_failed",
                        "scope": "shared",
                        "mode": "signal_only",
                        "service_name": "worker-recovery-daemon",
                        "error": "wake-file-unavailable",
                    },
                }

        orchestrator = _FailedWebhookSignalOrchestrator()
        _server, _thread, base_url, opener, _orchestrator = self._start_server(orchestrator)
        with patch.dict(os.environ, {"SOURCING_PROVIDER_WEBHOOK_TOKEN": "expected-token"}):
            status, _headers, response_body = self._request(
                opener,
                f"{base_url}/api/providers/apify/webhook",
                method="POST",
                data=json.dumps({"run_id": "run-failed-signal"}).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "X-Sourcing-Provider-Webhook-Token": "expected-token",
                },
            )

        self.assertEqual(status, 202)
        payload = json.loads(response_body)
        self.assertEqual(payload.get("status"), "accepted")
        self.assertEqual(payload.get("shared_recovery_signal_count"), 0)
        self.assertEqual(payload.get("shared_recovery_signal", {}).get("status"), "signal_failed")
        self.assertEqual(payload.get("shared_recovery_signal", {}).get("error"), "wake-file-unavailable")
        self.assertEqual(len(orchestrator.webhook_events), 1)

    def test_worker_daemon_run_once_route_is_signal_only_and_ignores_control_payload(self) -> None:
        _server, _thread, base_url, opener, orchestrator = self._start_server()
        status, _headers, response_body = self._request(
            opener,
            f"{base_url}/api/workers/daemon/run-once",
            method="POST",
            data=json.dumps(
                {
                    "job_id": "must-not-cross-api-boundary",
                    "stale_after_seconds": 0,
                    "total_limit": 999,
                    "workflow_auto_resume_enabled": True,
                }
            ).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(status, 202)
        payload = json.loads(response_body)
        self.assertEqual(payload.get("status"), "accepted")
        self.assertEqual(payload.get("mode"), "shared_recovery_signal")
        self.assertEqual(payload.get("shared_recovery_signal", {}).get("service_name"), "worker-recovery-daemon")
        self.assertEqual(
            orchestrator.recovery_signal_requests,
            [{"reason": "operator_api_recovery_signal", "requested_by": "operator_api"}],
        )

    def test_worker_daemon_run_once_route_fails_closed_when_signal_is_unavailable(self) -> None:
        class _UnavailableSignalOrchestrator(_StubOrchestrator):
            def signal_shared_recovery(self, *, reason, requested_by):
                self.recovery_signal_requests.append({"reason": reason, "requested_by": requested_by})
                return {
                    "status": "unavailable",
                    "reason": "shared_recovery_signal_unavailable",
                    "mode": "shared_recovery_signal",
                    "shared_recovery_signal": {
                        "status": "signal_skipped",
                        "scope": "shared",
                        "mode": "signal_only",
                        "reason": "runtime_dir_unset",
                    },
                }

        orchestrator = _UnavailableSignalOrchestrator()
        _server, _thread, base_url, opener, _orchestrator = self._start_server(orchestrator)
        status, _headers, response_body = self._request(
            opener,
            f"{base_url}/api/workers/daemon/run-once",
            method="POST",
            data=b"{}",
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(status, 503)
        self.assertEqual(json.loads(response_body).get("reason"), "shared_recovery_signal_unavailable")

    def test_workflow_job_cas_business_projections_preserve_transport_contract(self) -> None:
        class _JobCasProjectionOrchestrator(_StubOrchestrator):
            def continue_workflow_stage2(self, payload, **_owner):
                if payload.get("job_id") == "job-completed":
                    return {"status": "already_completed", "job_id": "job-completed"}
                return {
                    "status": "conflict",
                    "reason": "stage2_already_requested",
                    "job_id": str(payload.get("job_id") or ""),
                    "stage2_transition_state": "queued",
                }

            def cancel_workflow_job(self, job_id, _payload, **_owner):
                return {
                    "status": "already_terminal",
                    "job_id": job_id,
                    "job_status": "failed",
                    "job": {"job_id": job_id, "status": "failed", "stage": "failed"},
                }

        _server, _thread, base_url, opener, _orchestrator = self._start_server(_JobCasProjectionOrchestrator())
        status, _headers, body = self._request(
            opener,
            f"{base_url}/api/workflows/job-completed/continue-stage2",
            method="POST",
            data=b"{}",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(status, 202)
        self.assertEqual(json.loads(body), {"status": "already_completed", "job_id": "job-completed"})

        status, _headers, body = self._request(
            opener,
            f"{base_url}/api/workflows/job-queued/continue-stage2",
            method="POST",
            data=b"{}",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body).get("reason"), "stage2_already_requested")

        status, _headers, body = self._request(
            opener,
            f"{base_url}/api/jobs/job-failed/cancel",
            method="POST",
            data=b"{}",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body).get("status"), "already_terminal")
        self.assertEqual(json.loads(body).get("job_status"), "failed")

    def test_crm_typed_conflicts_map_to_http_409(self) -> None:
        class _StaleCrmOrchestrator(_StubOrchestrator):
            def update_crm_record_api(self, record_id, _payload, **_owner):
                return {
                    "status": "conflict",
                    "reason": "crm_record_stale",
                    "crm_record_id": record_id,
                }

            def promote_crm_record_public_web_signal(self, record_id, _payload, **_owner):
                return {
                    "status": "conflict",
                    "reason": "crm_public_web_promotion_idempotency_conflict",
                    "crm_record_id": record_id,
                }

        _server, _thread, base_url, opener, _orchestrator = self._start_server(_StaleCrmOrchestrator())
        status, _headers, body = self._request(
            opener,
            f"{base_url}/api/crm/records/crm-stale",
            method="PATCH",
            data=json.dumps({"stage": "contacted_waiting"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(status, 409)
        self.assertEqual(
            json.loads(body),
            {
                "status": "conflict",
                "reason": "crm_record_stale",
                "crm_record_id": "crm-stale",
            },
        )

        status, _headers, body = self._request(
            opener,
            f"{base_url}/api/crm/records/crm-stale/public-web-promotions",
            method="POST",
            data=json.dumps({"signal_id": "signal-drift", "action": "promote"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(status, 409)
        self.assertEqual(
            json.loads(body),
            {
                "status": "conflict",
                "reason": "crm_public_web_promotion_idempotency_conflict",
                "crm_record_id": "crm-stale",
            },
        )

    def test_bytes_endpoints_preserve_payload_and_headers(self) -> None:
        _server, _thread, base_url, opener, _orchestrator = self._start_server()
        # Media asset read path (GET bytes).
        status, headers, body = self._request(opener, f"{base_url}/api/media/assets/asset-ready")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"\x89PNG-fake-bytes")
        self.assertEqual(headers.get("Content-Type"), "image/png")
        self.assertEqual(headers.get("Cache-Control"), "public, max-age=86400")
        self.assertEqual(headers.get("X-Media-Asset-Contract"), "media_asset_read_contract_v1")
        # Missing asset returns JSON without the content key.
        status, _headers, body = self._request(opener, f"{base_url}/api/media/assets/missing")
        self.assertEqual(status, 404)
        self.assertNotIn("content", json.loads(body))
        # C1.4b projection export — async task contract:
        # 1) POST submit -> 202 + envelope (no inline bytes).
        status, _headers, body = self._request(
            opener,
            f"{base_url}/api/projections/export",
            method="POST",
            data=json.dumps({"projection_id": "proj-1"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(status, 202)
        envelope = json.loads(body)
        self.assertEqual(envelope.get("status"), "queued")
        self.assertEqual(envelope.get("task_type"), "export.projection.generate")
        task_id = str(envelope.get("task_id") or "")
        self.assertTrue(task_id)
        # 2) GET poll -> 200 + succeeded envelope with an artifact handle.
        status, _headers, body = self._request(opener, f"{base_url}/api/exports/{task_id}")
        self.assertEqual(status, 200)
        poll = json.loads(body)
        self.assertEqual(poll.get("status"), "succeeded")
        self.assertEqual(poll.get("artifact", {}).get("handle"), f"/api/exports/{task_id}/artifact")
        # 3) GET artifact download -> 200 + zip bytes + X-Sourcing-* headers.
        status, headers, body = self._request(opener, f"{base_url}/api/exports/{task_id}/artifact")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"ZIP-ARCHIVE-BYTES")
        self.assertEqual(headers.get("Content-Type"), "application/zip")
        self.assertEqual(headers.get("Content-Disposition"), 'attachment; filename="projection-export.zip"')
        self.assertEqual(headers.get("X-Sourcing-Projection-Id"), "proj-1")
        self.assertEqual(headers.get("X-Sourcing-Export-Record-Count"), "3")
        self.assertEqual(headers.get("X-Sourcing-Exported-Record-Count"), "2")
        self.assertEqual(headers.get("X-Sourcing-Skipped-Assertion-Count"), "1")
        # Header names must keep the legacy canonical casing on the wire:
        # consumers (and several tests) index them case-sensitively.
        exact_cased = dict(headers)
        for header_name in (
            "Content-Type",
            "Content-Length",
            "Content-Disposition",
            "X-Sourcing-Projection-Id",
            "X-Sourcing-Export-Record-Count",
            "X-Sourcing-Exported-Record-Count",
            "X-Sourcing-Skipped-Assertion-Count",
        ):
            self.assertIn(header_name, exact_cased)

    def test_c1_heavy_op_route_contracts(self) -> None:
        """Transport contracts of the heavy-op routes across the C1 substrate-unify
        migration. Each assertion tracks the CURRENT reality and flips as its route
        lands, so every commit's diff is a visible, intentional contract change:
          - POST /api/plan        : DELETED (404) — unified on /api/plan/submit
          - POST /api/jobs        : 201 + run_job artifact passthrough        -> DELETED in C1.3b (run_job demoted to CLI/test)
          - POST /api/crm/.../public-web-export : DELETED inline blob -> 202 async task (worker drain builds; poll+download via shared /api/exports/{id})
          - POST /api/target-candidates/export  : 410 GONE retirement payload  (legacy track retired -> pure tombstone)
        The CRM public-web export headers are the byte/header baseline the async
        artifact-download handle (C1.4/C1.5) must replicate exactly.
        """
        _server, _thread, base_url, opener, _orchestrator = self._start_server()
        json_headers = {"Content-Type": "application/json"}

        # POST /api/plan -> 404: route deleted in C1, plan unified on /api/plan/submit.
        status, _headers, body = self._request(
            opener,
            f"{base_url}/api/plan",
            method="POST",
            data=json.dumps({"raw_user_request": "find me people"}).encode("utf-8"),
            headers=json_headers,
        )
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body), {"error": "not found"})

        # POST /api/jobs -> 404: route deleted in C1, run_job demoted to CLI/test helper.
        status, _headers, body = self._request(
            opener,
            f"{base_url}/api/jobs",
            method="POST",
            data=json.dumps({"raw_user_request": "find me people"}).encode("utf-8"),
            headers=json_headers,
        )
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body), {"error": "not found"})

        # POST /api/crm/records/public-web-export -> 202 + async-task envelope (C1.4:
        # flipped off the request thread; the worker CRM export drain builds it, then
        # the client polls + downloads via the shared /api/exports/{id}[/artifact]).
        status, _headers, body = self._request(
            opener,
            f"{base_url}/api/crm/records/public-web-export",
            method="POST",
            data=json.dumps({}).encode("utf-8"),
            headers=json_headers,
        )
        self.assertEqual(status, 202)
        crm_envelope = json.loads(body)
        self.assertEqual(crm_envelope.get("status"), "queued")
        self.assertEqual(crm_envelope.get("task_type"), "export.crm_public_web.generate")
        self.assertTrue(crm_envelope.get("task_id"))

        # POST /api/target-candidates/export -> 410 GONE (legacy retired by default).
        status, _headers, body = self._request(
            opener,
            f"{base_url}/api/target-candidates/export",
            method="POST",
            data=json.dumps({}).encode("utf-8"),
            headers=json_headers,
        )
        self.assertEqual(status, 410)
        retired = json.loads(body)
        self.assertEqual(retired.get("status"), "retired")
        self.assertEqual(retired.get("reason"), "legacy_target_candidate_export_retired")
        self.assertEqual(retired.get("canonical_export_path"), "/api/projections/export")

    def test_shutdown_stops_serve_forever_and_releases_port(self) -> None:
        server = create_server(_StubOrchestrator(), host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        with opener.open(f"http://{host}:{port}/health", timeout=10) as response:
            self.assertEqual(response.status, 200)
        server.shutdown()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        server.server_close()
        with self.assertRaises((URLError, ConnectionError, socket.timeout, OSError)):
            opener.open(f"http://{host}:{port}/health", timeout=2)


if __name__ == "__main__":
    unittest.main()
