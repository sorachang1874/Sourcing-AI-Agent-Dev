from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
from typing import Any

from .orchestrator import SourcingOrchestrator


def create_server(orchestrator: SourcingOrchestrator, host: str = "127.0.0.1", port: int = 8765) -> ThreadingHTTPServer:
    handler = _build_handler(orchestrator)
    return ThreadingHTTPServer((host, port), handler)


def _build_handler(orchestrator: SourcingOrchestrator):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                return self._send_json(HTTPStatus.OK, {"status": "ok"})
            if self.path == "/api/providers/health":
                return self._send_json(HTTPStatus.OK, orchestrator.healthcheck_model())
            if self.path == "/api/criteria/patterns":
                return self._send_json(HTTPStatus.OK, orchestrator.list_criteria_patterns())
            if self.path == "/api/plan/reviews":
                return self._send_json(HTTPStatus.OK, orchestrator.list_plan_review_sessions())
            if self.path == "/api/manual-review":
                return self._send_json(HTTPStatus.OK, orchestrator.list_manual_review_items())
            if self.path == "/api/workers/recoverable":
                return self._send_json(HTTPStatus.OK, orchestrator.list_recoverable_agent_workers())
            if self.path == "/api/workers/daemon/status":
                return self._send_json(HTTPStatus.OK, orchestrator.get_worker_daemon_status())
            progress_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9]+)/progress", self.path)
            if progress_match:
                payload = orchestrator.get_job_progress(progress_match.group(1))
                if payload is None:
                    return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return self._send_json(HTTPStatus.OK, payload)
            job_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9]+)/results", self.path)
            if job_match:
                payload = orchestrator.get_job_results(job_match.group(1))
                if payload is None:
                    return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return self._send_json(HTTPStatus.OK, payload)
            trace_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9]+)/trace", self.path)
            if trace_match:
                payload = orchestrator.get_job_trace(trace_match.group(1))
                if payload is None:
                    return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return self._send_json(HTTPStatus.OK, payload)
            worker_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9]+)/workers", self.path)
            if worker_match:
                payload = orchestrator.get_job_workers(worker_match.group(1))
                if payload is None:
                    return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return self._send_json(HTTPStatus.OK, payload)
            scheduler_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9]+)/scheduler", self.path)
            if scheduler_match:
                payload = orchestrator.get_job_scheduler(scheduler_match.group(1))
                if payload is None:
                    return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return self._send_json(HTTPStatus.OK, payload)
            job_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9]+)", self.path)
            if job_match:
                payload = orchestrator.get_job(job_match.group(1))
                if payload is None:
                    return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return self._send_json(HTTPStatus.OK, payload)
            return self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            payload = self._read_json()
            if self.path == "/api/bootstrap":
                result = orchestrator.bootstrap()
                return self._send_json(HTTPStatus.OK, result)
            if self.path == "/api/plan":
                result = orchestrator.plan_workflow(payload)
                return self._send_json(HTTPStatus.OK, result)
            if self.path == "/api/jobs":
                result = orchestrator.run_job(payload)
                return self._send_json(HTTPStatus.CREATED, result)
            if self.path == "/api/workflows":
                payload.setdefault("auto_job_daemon", True)
                result = orchestrator.start_workflow(payload)
                return self._send_json(HTTPStatus.ACCEPTED, result)
            if self.path == "/api/company-assets/supplement":
                result = orchestrator.supplement_company_assets(payload)
                status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
                return self._send_json(status, result)
            if self.path == "/api/criteria/feedback":
                result = orchestrator.record_criteria_feedback(payload)
                return self._send_json(HTTPStatus.CREATED, result)
            if self.path == "/api/plan/review":
                result = orchestrator.review_plan_session(payload)
                status = HTTPStatus.OK if result.get("status") != "not_found" else HTTPStatus.NOT_FOUND
                return self._send_json(status, result)
            if self.path == "/api/plan/review/compile-instruction":
                result = orchestrator.compile_plan_review_instruction(payload)
                status = HTTPStatus.OK
                if result.get("status") == "not_found":
                    status = HTTPStatus.NOT_FOUND
                elif result.get("status") == "invalid":
                    status = HTTPStatus.BAD_REQUEST
                return self._send_json(status, result)
            if self.path == "/api/results/refine/compile-instruction":
                result = orchestrator.compile_post_acquisition_refinement(payload)
                status = HTTPStatus.OK
                if result.get("status") == "not_found":
                    status = HTTPStatus.NOT_FOUND
                elif result.get("status") == "invalid":
                    status = HTTPStatus.BAD_REQUEST
                return self._send_json(status, result)
            if self.path == "/api/results/refine":
                result = orchestrator.apply_post_acquisition_refinement(payload)
                status = HTTPStatus.OK
                if result.get("status") == "not_found":
                    status = HTTPStatus.NOT_FOUND
                elif result.get("status") == "invalid":
                    status = HTTPStatus.BAD_REQUEST
                return self._send_json(status, result)
            if self.path == "/api/criteria/confidence-policy":
                result = orchestrator.configure_confidence_policy(payload)
                status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
                return self._send_json(status, result)
            if self.path == "/api/criteria/suggestions/review":
                result = orchestrator.review_pattern_suggestion(payload)
                status = HTTPStatus.OK if result.get("status") != "not_found" else HTTPStatus.NOT_FOUND
                return self._send_json(status, result)
            if self.path == "/api/manual-review/review":
                result = orchestrator.review_manual_review_item(payload)
                status = HTTPStatus.OK
                if result.get("status") == "not_found":
                    status = HTTPStatus.NOT_FOUND
                elif result.get("status") in {"invalid", "candidate_not_found"}:
                    status = HTTPStatus.BAD_REQUEST
                return self._send_json(status, result)
            if self.path == "/api/manual-review/synthesize":
                result = orchestrator.synthesize_manual_review_item(payload)
                status = HTTPStatus.OK
                if result.get("status") == "not_found":
                    status = HTTPStatus.NOT_FOUND
                elif result.get("status") == "invalid":
                    status = HTTPStatus.BAD_REQUEST
                return self._send_json(status, result)
            if self.path == "/api/criteria/recompile":
                result = orchestrator.recompile_criteria(payload)
                return self._send_json(HTTPStatus.OK, result)
            if self.path == "/api/workers/interrupt":
                result = orchestrator.interrupt_agent_worker(payload)
                status = HTTPStatus.OK
                if result.get("status") == "invalid":
                    status = HTTPStatus.BAD_REQUEST
                elif result.get("status") == "not_found":
                    status = HTTPStatus.NOT_FOUND
                return self._send_json(status, result)
            if self.path == "/api/workers/daemon/run-once":
                result = orchestrator.run_worker_recovery_once(payload)
                return self._send_json(HTTPStatus.OK, result)
            if self.path == "/api/workers/daemon/systemd-unit":
                result = orchestrator.write_worker_daemon_systemd_unit(payload)
                return self._send_json(HTTPStatus.OK, result)
            return self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

        def _read_json(self) -> dict[str, Any]:
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length == 0:
                return {}
            raw = self.rfile.read(content_length)
            return json.loads(raw.decode("utf-8"))

        def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status.value)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    return Handler
