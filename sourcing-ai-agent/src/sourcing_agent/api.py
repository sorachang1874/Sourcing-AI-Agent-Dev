from __future__ import annotations

import base64
import contextlib
import json
import os
import re
import threading
from collections.abc import Iterator
from email.parser import BytesParser
from email.policy import default as default_email_policy
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse, urlsplit

from .orchestrator import SourcingOrchestrator
from .storage import _json_safe_payload
from .workflow_submission import (
    normalize_workflow_submission_payload,
    workflow_runtime_uses_managed_runner,
)


def create_server(orchestrator: SourcingOrchestrator, host: str = "127.0.0.1", port: int = 8765) -> ThreadingHTTPServer:
    handler = _build_handler(orchestrator)
    max_parallel_requests = max(1, _env_int("SOURCING_API_MAX_PARALLEL_REQUESTS", 8))
    light_request_reserved = max(
        1,
        _env_int(
            "SOURCING_API_LIGHT_REQUEST_RESERVED",
            _default_light_request_reserved(max_parallel_requests),
        ),
    )

    class BoundedThreadingHTTPServer(ThreadingHTTPServer):
        def __init__(self, server_address: tuple[str, int], request_handler_class: type[BaseHTTPRequestHandler]) -> None:
            super().__init__(server_address, request_handler_class)
            self.request_concurrency = _RequestConcurrencyController(
                shared_limit=max_parallel_requests,
                light_reserved_limit=light_request_reserved,
            )

    return BoundedThreadingHTTPServer((host, port), handler)


def _build_handler(orchestrator: SourcingOrchestrator):
    allowed_origins = _load_allowed_origins()

    class Handler(BaseHTTPRequestHandler):
        def do_OPTIONS(self) -> None:  # noqa: N802
            with self._request_slot("OPTIONS", self.path):
                self.send_response(HTTPStatus.NO_CONTENT.value)
                self._write_cors_headers()
                self.send_header("Content-Length", "0")
                self.end_headers()

        def do_GET(self) -> None:  # noqa: N802
            path, query_payload = self._request_target_payload()
            with self._request_slot("GET", path):
                if path == "/health":
                    health = orchestrator.get_runtime_metrics(query_payload)
                    status = HTTPStatus.OK if health.get("status") != "failed" else HTTPStatus.SERVICE_UNAVAILABLE
                    return self._send_json(status, health)
                if path == "/api/providers/health":
                    return self._send_json(HTTPStatus.OK, orchestrator.healthcheck_model())
                if path == "/api/runtime/health":
                    health = orchestrator.get_runtime_health(query_payload)
                    status = HTTPStatus.OK if health.get("status") != "failed" else HTTPStatus.SERVICE_UNAVAILABLE
                    return self._send_json(status, health)
                if path == "/api/runtime/metrics":
                    health = orchestrator.get_runtime_metrics(query_payload)
                    status = HTTPStatus.OK if health.get("status") != "failed" else HTTPStatus.SERVICE_UNAVAILABLE
                    return self._send_json(status, health)
                if path == "/api/runtime/progress":
                    progress = orchestrator.get_system_progress(query_payload)
                    status = HTTPStatus.OK if progress.get("status") != "failed" else HTTPStatus.SERVICE_UNAVAILABLE
                    return self._send_json(status, progress)
                if path == "/api/criteria/patterns":
                    return self._send_json(HTTPStatus.OK, orchestrator.list_criteria_patterns())
                if path == "/api/plan/reviews":
                    return self._send_json(HTTPStatus.OK, orchestrator.list_plan_review_sessions())
                if path == "/api/query-dispatches":
                    return self._send_json(HTTPStatus.OK, orchestrator.list_query_dispatches(query_payload))
                if path == "/api/manual-review":
                    return self._send_json(
                        HTTPStatus.OK,
                        orchestrator.list_manual_review_items(
                            target_company=str(query_payload.get("target_company") or ""),
                            job_id=str(query_payload.get("job_id") or ""),
                            status=str(query_payload.get("status") or "open"),
                        ),
                    )
                if path == "/api/candidate-review-registry":
                    return self._send_json(
                        HTTPStatus.OK,
                        orchestrator.list_candidate_review_records(
                            job_id=str(query_payload.get("job_id") or ""),
                            history_id=str(query_payload.get("history_id") or ""),
                            candidate_id=str(query_payload.get("candidate_id") or ""),
                            status=str(query_payload.get("status") or ""),
                        ),
                    )
                if path == "/api/target-candidates":
                    return self._send_json(
                        HTTPStatus.OK,
                        orchestrator.list_target_candidates(
                            job_id=str(query_payload.get("job_id") or ""),
                            history_id=str(query_payload.get("history_id") or ""),
                            candidate_id=str(query_payload.get("candidate_id") or ""),
                            follow_up_status=str(query_payload.get("follow_up_status") or ""),
                        ),
                    )
                if path == "/api/target-candidates/public-web-search":
                    return self._send_json(
                        HTTPStatus.OK,
                        orchestrator.list_target_candidate_public_web_searches(query_payload),
                    )
                public_web_detail_match = re.fullmatch(r"/api/target-candidates/([^/]+)/public-web-search", path)
                if public_web_detail_match:
                    detail = orchestrator.get_target_candidate_public_web_search_detail(
                        public_web_detail_match.group(1)
                    )
                    status = HTTPStatus.OK
                    if detail.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    elif detail.get("status") == "invalid":
                        status = HTTPStatus.BAD_REQUEST
                    return self._send_json(status, detail)
                public_web_promotions_match = re.fullmatch(
                    r"/api/target-candidates/([^/]+)/public-web-promotions",
                    path,
                )
                if public_web_promotions_match:
                    result = orchestrator.list_target_candidate_public_web_promotions(
                        public_web_promotions_match.group(1)
                    )
                    status = HTTPStatus.OK
                    if result.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    elif result.get("status") == "invalid":
                        status = HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/assets/governance/default-pointers":
                    return self._send_json(HTTPStatus.OK, orchestrator.list_asset_default_pointers(query_payload))
                if path == "/api/frontend-history":
                    return self._send_json(
                        HTTPStatus.OK,
                        orchestrator.list_frontend_history(limit=_env_int_from_payload(query_payload, "limit", 24)),
                    )
                frontend_history_match = re.fullmatch(r"/api/frontend-history/([^/]+)", path)
                if frontend_history_match:
                    payload = orchestrator.get_frontend_history_recovery(frontend_history_match.group(1))
                    status = HTTPStatus.OK
                    if payload.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    elif payload.get("status") == "invalid":
                        status = HTTPStatus.BAD_REQUEST
                    return self._send_json(status, payload)
                if path == "/api/workers/recoverable":
                    return self._send_json(HTTPStatus.OK, orchestrator.list_recoverable_agent_workers())
                if path == "/api/workers/daemon/status":
                    return self._send_json(HTTPStatus.OK, orchestrator.get_worker_daemon_status())
                progress_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9_-]+)/progress", path)
                if progress_match:
                    progress_payload = orchestrator.get_job_progress(progress_match.group(1))
                    if progress_payload is None:
                        return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                    return self._send_json(HTTPStatus.OK, progress_payload)
                dashboard_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9_-]+)/dashboard", path)
                if dashboard_match:
                    dashboard_payload = orchestrator.get_job_dashboard(dashboard_match.group(1))
                    if dashboard_payload is None:
                        return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                    return self._send_json(HTTPStatus.OK, dashboard_payload)
                candidate_page_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9_-]+)/candidates", path)
                if candidate_page_match:
                    candidate_page_payload = orchestrator.get_job_candidate_page(
                        candidate_page_match.group(1),
                        offset=_env_int_from_payload(query_payload, "offset", 0),
                        limit=_env_int_from_payload(query_payload, "limit", 120),
                        lightweight=_env_bool_from_payload(query_payload, "lightweight", False),
                    )
                    if candidate_page_payload is None:
                        return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                    return self._send_json(HTTPStatus.OK, candidate_page_payload)
                job_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9_-]+)/results", path)
                if job_match:
                    results_payload = orchestrator.get_job_results_api(
                        job_match.group(1),
                        include_candidates=_env_bool_from_payload(query_payload, "include_candidates", False),
                    )
                    if results_payload is None:
                        return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                    return self._send_json(HTTPStatus.OK, results_payload)
                candidate_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9_-]+)/candidates/([^/]+)", path)
                if candidate_match:
                    candidate_detail_payload = orchestrator.get_job_candidate_detail(
                        candidate_match.group(1), candidate_match.group(2)
                    )
                    if candidate_detail_payload is None:
                        return self._send_json(HTTPStatus.NOT_FOUND, {"error": "candidate not found"})
                    return self._send_json(HTTPStatus.OK, candidate_detail_payload)
                trace_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9_-]+)/trace", path)
                if trace_match:
                    trace_payload = orchestrator.get_job_trace(trace_match.group(1))
                    if trace_payload is None:
                        return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                    return self._send_json(HTTPStatus.OK, trace_payload)
                worker_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9_-]+)/workers", path)
                if worker_match:
                    worker_payload = orchestrator.get_job_workers(worker_match.group(1))
                    if worker_payload is None:
                        return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                    return self._send_json(HTTPStatus.OK, worker_payload)
                scheduler_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9_-]+)/scheduler", path)
                if scheduler_match:
                    scheduler_payload = orchestrator.get_job_scheduler(scheduler_match.group(1))
                    if scheduler_payload is None:
                        return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                    return self._send_json(HTTPStatus.OK, scheduler_payload)
                job_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9_-]+)", path)
                if job_match:
                    job_payload = orchestrator.get_job_api(
                        job_match.group(1),
                        include_details=_env_bool_from_payload(query_payload, "include_details", False),
                    )
                    if job_payload is None:
                        return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                    return self._send_json(HTTPStatus.OK, job_payload)
                return self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

        def do_DELETE(self) -> None:  # noqa: N802
            path, _query_payload = self._request_target_payload()
            with self._request_slot("DELETE", path):
                frontend_history_match = re.fullmatch(r"/api/frontend-history/([^/]+)", path)
                if frontend_history_match:
                    payload = orchestrator.delete_frontend_history(frontend_history_match.group(1))
                    status = HTTPStatus.OK
                    if payload.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    elif payload.get("status") == "invalid":
                        status = HTTPStatus.BAD_REQUEST
                    return self._send_json(status, payload)
                return self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            path, _query_payload = self._request_target_payload()
            with self._request_slot("POST", path):
                payload = self._read_payload()
                if path == "/api/bootstrap":
                    result = orchestrator.bootstrap()
                    return self._send_json(HTTPStatus.OK, result)
                if path == "/api/plan":
                    result = orchestrator.plan_workflow(payload)
                    return self._send_json(HTTPStatus.OK, result)
                if path == "/api/plan/submit":
                    submit_plan = getattr(orchestrator, "submit_plan_workflow", None)
                    result = submit_plan(payload) if callable(submit_plan) else orchestrator.plan_workflow(payload)
                    status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/workflows/explain":
                    result = orchestrator.explain_workflow(payload)
                    return self._send_json(HTTPStatus.OK, result)
                if path == "/api/query-dispatches/list":
                    result = orchestrator.list_query_dispatches(payload)
                    return self._send_json(HTTPStatus.OK, result)
                if path == "/api/jobs":
                    result = orchestrator.run_job(payload)
                    return self._send_json(HTTPStatus.CREATED, result)
                if path == "/api/workflows":
                    payload = normalize_workflow_submission_payload(payload)
                    if workflow_runtime_uses_managed_runner(payload.get("runtime_execution_mode")):
                        result = orchestrator.start_workflow_runner_managed(payload)
                    else:
                        result = orchestrator.start_workflow(payload)
                    return self._send_json(HTTPStatus.ACCEPTED, result)
                continue_stage2_match = re.fullmatch(r"/api/workflows/([A-Za-z0-9_-]+)/continue-stage2", path)
                if continue_stage2_match:
                    result = orchestrator.continue_workflow_stage2(
                        {
                            **payload,
                            "job_id": continue_stage2_match.group(1),
                        }
                    )
                    status = HTTPStatus.ACCEPTED
                    if result.get("status") in {"not_found"}:
                        status = HTTPStatus.NOT_FOUND
                    elif result.get("status") in {"invalid", "conflict"}:
                        status = HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                job_profile_completion_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9_-]+)/profile-completion", path)
                if job_profile_completion_match:
                    result = orchestrator.complete_job_candidate_profiles(job_profile_completion_match.group(1), payload)
                    status = HTTPStatus.OK
                    if result.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    elif result.get("status") == "invalid":
                        status = HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                candidate_batch_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9_-]+)/candidates/batch", path)
                if candidate_batch_match:
                    candidate_ids = payload.get("candidate_ids")
                    candidate_batch_result = orchestrator.get_job_candidate_details_batch(
                        candidate_batch_match.group(1),
                        candidate_ids if isinstance(candidate_ids, list) else [],
                    )
                    status = HTTPStatus.OK
                    if candidate_batch_result is None:
                        status = HTTPStatus.NOT_FOUND
                        candidate_batch_result = {"error": "job not found"}
                    return self._send_json(status, candidate_batch_result)
                if path == "/api/company-assets/supplement":
                    result = orchestrator.supplement_company_assets(payload)
                    status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/intake/excel":
                    result = orchestrator.ingest_excel_contacts(payload)
                    status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/intake/excel/workflow":
                    result = orchestrator.start_excel_intake_workflow(payload)
                    status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/intake/excel/continue":
                    result = orchestrator.continue_excel_intake_review(payload)
                    status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/criteria/feedback":
                    result = orchestrator.record_criteria_feedback(payload)
                    return self._send_json(HTTPStatus.CREATED, result)
                if path == "/api/plan/review":
                    result = orchestrator.review_plan_session(payload)
                    status = HTTPStatus.OK if result.get("status") != "not_found" else HTTPStatus.NOT_FOUND
                    return self._send_json(status, result)
                if path == "/api/plan/review/compile-instruction":
                    result = orchestrator.compile_plan_review_instruction(payload)
                    status = HTTPStatus.OK
                    if result.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    elif result.get("status") == "invalid":
                        status = HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/results/refine/compile-instruction":
                    result = orchestrator.compile_post_acquisition_refinement(payload)
                    status = HTTPStatus.OK
                    if result.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    elif result.get("status") == "invalid":
                        status = HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/results/refine":
                    result = orchestrator.apply_post_acquisition_refinement(payload)
                    status = HTTPStatus.OK
                    if result.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    elif result.get("status") == "invalid":
                        status = HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/criteria/confidence-policy":
                    result = orchestrator.configure_confidence_policy(payload)
                    status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/criteria/suggestions/review":
                    result = orchestrator.review_pattern_suggestion(payload)
                    status = HTTPStatus.OK if result.get("status") != "not_found" else HTTPStatus.NOT_FOUND
                    return self._send_json(status, result)
                if path == "/api/manual-review/review":
                    result = orchestrator.review_manual_review_item(payload)
                    status = HTTPStatus.OK
                    if result.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    elif result.get("status") in {"invalid", "candidate_not_found"}:
                        status = HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/candidate-review-registry":
                    result = orchestrator.upsert_candidate_review_record(payload)
                    status = HTTPStatus.CREATED if result.get("status") == "upserted" else HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/target-candidates/export":
                    result = orchestrator.export_target_candidates_archive(payload)
                    if result.get("status") == "not_found":
                        return self._send_json(HTTPStatus.NOT_FOUND, result)
                    if result.get("status") == "invalid":
                        return self._send_json(HTTPStatus.BAD_REQUEST, result)
                    return self._send_bytes(
                        HTTPStatus.OK,
                        bytes(result.get("body") or b""),
                        content_type=str(result.get("content_type") or "application/octet-stream"),
                        filename=str(result.get("filename") or "download.bin"),
                    )
                if path == "/api/target-candidates/public-web-export":
                    result = orchestrator.export_target_candidate_public_web_archive(payload)
                    if result.get("status") == "not_found":
                        return self._send_json(HTTPStatus.NOT_FOUND, result)
                    if result.get("status") == "invalid":
                        return self._send_json(HTTPStatus.BAD_REQUEST, result)
                    return self._send_bytes(
                        HTTPStatus.OK,
                        bytes(result.get("body") or b""),
                        content_type=str(result.get("content_type") or "application/octet-stream"),
                        filename=str(result.get("filename") or "download.bin"),
                    )
                if path == "/api/target-candidates/import-from-job":
                    result = orchestrator.import_target_candidates_from_job(payload)
                    status = HTTPStatus.CREATED if result.get("status") == "imported" else HTTPStatus.BAD_REQUEST
                    if result.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    return self._send_json(status, result)
                if path == "/api/target-candidates/public-web-search":
                    result = orchestrator.start_target_candidate_public_web_search(payload)
                    status = HTTPStatus.ACCEPTED if result.get("status") in {"queued", "joined"} else HTTPStatus.BAD_REQUEST
                    if result.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    return self._send_json(status, result)
                public_web_promotion_match = re.fullmatch(
                    r"/api/target-candidates/([^/]+)/public-web-promotions",
                    path,
                )
                if public_web_promotion_match:
                    result = orchestrator.promote_target_candidate_public_web_signal(
                        public_web_promotion_match.group(1),
                        payload,
                    )
                    status = HTTPStatus.CREATED if result.get("status") in {"promoted", "rejected"} else HTTPStatus.BAD_REQUEST
                    if result.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    return self._send_json(status, result)
                if path == "/api/target-candidates":
                    result = orchestrator.upsert_target_candidate(payload)
                    status = HTTPStatus.CREATED if result.get("status") == "upserted" else HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/assets/governance/promote-default":
                    result = orchestrator.promote_asset_default_pointer(payload)
                    status = HTTPStatus.CREATED if result.get("status") == "promoted" else HTTPStatus.BAD_REQUEST
                    if result.get("status") == "noop":
                        status = HTTPStatus.OK
                    return self._send_json(status, result)
                if path == "/api/manual-review/synthesize":
                    result = orchestrator.synthesize_manual_review_item(payload)
                    status = HTTPStatus.OK
                    if result.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    elif result.get("status") == "invalid":
                        status = HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/criteria/recompile":
                    result = orchestrator.recompile_criteria(payload)
                    return self._send_json(HTTPStatus.OK, result)
                if path == "/api/workers/interrupt":
                    result = orchestrator.interrupt_agent_worker(payload)
                    status = HTTPStatus.OK
                    if result.get("status") == "invalid":
                        status = HTTPStatus.BAD_REQUEST
                    elif result.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    return self._send_json(status, result)
                if path == "/api/workers/cleanup":
                    result = orchestrator.cleanup_recoverable_workers(payload)
                    return self._send_json(HTTPStatus.OK, result)
                if path == "/api/workers/daemon/run-once":
                    result = orchestrator.run_worker_recovery_once(payload)
                    return self._send_json(HTTPStatus.OK, result)
                if path == "/api/runtime/services/shutdown":
                    result = orchestrator.request_runtime_service_shutdown(payload)
                    status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                cancel_job_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9_-]+)/cancel", path)
                if cancel_job_match:
                    result = orchestrator.cancel_workflow_job(cancel_job_match.group(1), payload)
                    status = HTTPStatus.OK
                    if result.get("status") == "not_found":
                        status = HTTPStatus.NOT_FOUND
                    elif result.get("status") == "invalid":
                        status = HTTPStatus.BAD_REQUEST
                    return self._send_json(status, result)
                if path == "/api/workers/daemon/systemd-unit":
                    result = orchestrator.write_worker_daemon_systemd_unit(payload)
                    return self._send_json(HTTPStatus.OK, result)
                return self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

        def _request_target_payload(self) -> tuple[str, dict[str, Any]]:
            parsed = urlsplit(self.path)
            path = parsed.path or "/"
            query_payload: dict[str, Any] = {}
            for key, values in parse_qs(parsed.query, keep_blank_values=False).items():
                if not values:
                    continue
                query_payload[key] = values[-1]
            return path, query_payload

        def _read_payload(self) -> dict[str, Any]:
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length == 0:
                return {}
            raw = self.rfile.read(content_length)
            return _decode_http_request_body(raw, str(self.headers.get("Content-Type") or ""))

        def _request_slot(self, method: str, path: str) -> contextlib.AbstractContextManager[None]:
            concurrency = getattr(self.server, "request_concurrency", None)
            if concurrency is None:
                return contextlib.nullcontext()
            return concurrency.claim(method, path)

        def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            body = json.dumps(_json_safe_payload(payload), ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status.value)
            self._write_cors_headers()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except BrokenPipeError:
                return

        def _send_bytes(
            self,
            status: HTTPStatus,
            body: bytes,
            *,
            content_type: str,
            filename: str = "",
        ) -> None:
            self.send_response(status.value)
            self._write_cors_headers()
            self.send_header("Content-Type", content_type or "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            if filename:
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.end_headers()
            try:
                self.wfile.write(body)
            except BrokenPipeError:
                return

        def _write_cors_headers(self) -> None:
            origin = self._cors_origin()
            if origin:
                self.send_header("Access-Control-Allow-Origin", origin)
                self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            request_headers = self.headers.get("Access-Control-Request-Headers", "").strip()
            allow_headers = request_headers or "Content-Type"
            self.send_header("Access-Control-Allow-Headers", allow_headers)
            self.send_header("Access-Control-Max-Age", "600")

        def _cors_origin(self) -> str:
            request_origin = str(self.headers.get("Origin") or "").strip()
            if not request_origin:
                return ""
            if "*" in allowed_origins:
                return "*"
            if request_origin in allowed_origins:
                return request_origin
            if _is_local_dev_origin(request_origin):
                return request_origin
            return ""

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    return Handler


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
    if normalized_method == "POST" and normalized_path == "/api/target-candidates/public-web-search":
        return "light"
    if normalized_method == "POST" and re.fullmatch(r"/api/jobs/[A-Za-z0-9_-]+/cancel", normalized_path):
        return "light"
    if normalized_method != "GET":
        return "shared"
    if normalized_path in {
        "/health",
        "/api/providers/health",
        "/api/runtime/health",
        "/api/runtime/metrics",
        "/api/runtime/progress",
        "/api/criteria/patterns",
        "/api/plan/reviews",
        "/api/query-dispatches",
        "/api/manual-review",
            "/api/candidate-review-registry",
            "/api/target-candidates",
            "/api/target-candidates/public-web-search",
            "/api/assets/governance/default-pointers",
        "/api/frontend-history",
        "/api/workers/recoverable",
        "/api/workers/daemon/status",
    }:
        return "light"
    if re.fullmatch(r"/api/frontend-history/[^/]+", normalized_path):
        return "light"
    if re.fullmatch(r"/api/target-candidates/[^/]+/public-web-search", normalized_path):
        return "light"
    if re.fullmatch(r"/api/jobs/[A-Za-z0-9]+/(progress|dashboard)", normalized_path):
        return "light"
    return "shared"


def _default_light_request_reserved(max_parallel_requests: int) -> int:
    if max_parallel_requests <= 2:
        return 1
    return min(4, max(2, max_parallel_requests // 4))


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
