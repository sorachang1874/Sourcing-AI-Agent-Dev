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
    ("POST", "/api/plan"),
    ("POST", "/api/plan/submit"),
    ("POST", "/api/workflows/explain"),
    ("POST", "/api/providers/apify/webhook"),
    ("POST", "/api/query-dispatches/list"),
    ("POST", "/api/jobs"),
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

    def get_runtime_metrics(self, _query):
        return {"status": "ok"}

    def get_job_trace(self, job_id):
        self.trace_entered.append(job_id)
        self.trace_gate.wait(timeout=15)
        return {"job_id": job_id}

    def handle_remote_provider_event(self, event_payload):
        self.webhook_events.append(dict(event_payload))
        return {"status": "accepted", "targets": {}, "recovery_dispatch_count": 0}

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
        return {
            "status": "ok",
            "body": b"ZIP-ARCHIVE-BYTES",
            "content_type": "application/zip",
            "filename": "projection-export.zip",
            "projection_id": "proj-1",
            "record_count": 3,
            "exported_record_count": 2,
            "skipped_assertion_count": 1,
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
        observed = [
            (route.path, frozenset(set(route.methods or ()) - {"HEAD"}))
            for route in app.routes
        ]
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
            # Valid token with run id -> accepted job-scoped recovery.
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
            self.assertEqual(payload.get("mode"), "job_scoped_recovery")
            self.assertEqual(len(orchestrator.webhook_events), 1)
            self.assertEqual(orchestrator.webhook_events[0].get("recovery_mode"), "job_scoped_recovery")

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
        # Projection export path (POST bytes + attachment headers).
        status, headers, body = self._request(
            opener,
            f"{base_url}/api/projections/export",
            method="POST",
            data=json.dumps({"projection_id": "proj-1"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
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
