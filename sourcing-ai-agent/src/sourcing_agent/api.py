from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .orchestrator import SourcingOrchestrator


def create_server(orchestrator: SourcingOrchestrator, host: str = "127.0.0.1", port: int = 8765) -> ThreadingHTTPServer:
    handler = _build_handler(orchestrator)
    return ThreadingHTTPServer((host, port), handler)


def _build_handler(orchestrator: SourcingOrchestrator):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            path, query_payload = self._request_target_payload()
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
                return self._send_json(HTTPStatus.OK, orchestrator.list_manual_review_items())
            if path == "/api/workers/recoverable":
                return self._send_json(HTTPStatus.OK, orchestrator.list_recoverable_agent_workers())
            if path == "/api/workers/daemon/status":
                return self._send_json(HTTPStatus.OK, orchestrator.get_worker_daemon_status())
            progress_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9]+)/progress", path)
            if progress_match:
                payload = orchestrator.get_job_progress(progress_match.group(1))
                if payload is None:
                    return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return self._send_json(HTTPStatus.OK, payload)
            job_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9]+)/results", path)
            if job_match:
                payload = orchestrator.get_job_results(job_match.group(1))
                if payload is None:
                    return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return self._send_json(HTTPStatus.OK, payload)
            trace_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9]+)/trace", path)
            if trace_match:
                payload = orchestrator.get_job_trace(trace_match.group(1))
                if payload is None:
                    return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return self._send_json(HTTPStatus.OK, payload)
            worker_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9]+)/workers", path)
            if worker_match:
                payload = orchestrator.get_job_workers(worker_match.group(1))
                if payload is None:
                    return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return self._send_json(HTTPStatus.OK, payload)
            scheduler_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9]+)/scheduler", path)
            if scheduler_match:
                payload = orchestrator.get_job_scheduler(scheduler_match.group(1))
                if payload is None:
                    return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return self._send_json(HTTPStatus.OK, payload)
            job_match = re.fullmatch(r"/api/jobs/([A-Za-z0-9]+)", path)
            if job_match:
                payload = orchestrator.get_job(job_match.group(1))
                if payload is None:
                    return self._send_json(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return self._send_json(HTTPStatus.OK, payload)
            return self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            path, _query_payload = self._request_target_payload()
            payload = self._read_json()
            if path == "/api/bootstrap":
                result = orchestrator.bootstrap()
                return self._send_json(HTTPStatus.OK, result)
            if path == "/api/plan":
                result = orchestrator.plan_workflow(payload)
                return self._send_json(HTTPStatus.OK, result)
            if path == "/api/query-dispatches/list":
                result = orchestrator.list_query_dispatches(payload)
                return self._send_json(HTTPStatus.OK, result)
            if path == "/api/jobs":
                result = orchestrator.run_job(payload)
                return self._send_json(HTTPStatus.CREATED, result)
            if path == "/api/workflows":
                execution_mode = str(
                    payload.get("runtime_execution_mode")
                    or payload.get("workflow_runner_mode")
                    or "hosted"
                ).strip().lower()
                payload.setdefault("analysis_stage_mode", "two_stage")
                if execution_mode in {"managed_subprocess", "runner_managed", "detached_sidecar"}:
                    payload["runtime_execution_mode"] = execution_mode
                    payload.setdefault("auto_job_daemon", True)
                    result = orchestrator.start_workflow_runner_managed(payload)
                else:
                    payload["runtime_execution_mode"] = "hosted"
                    payload.setdefault("auto_job_daemon", False)
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
            if path == "/api/company-assets/supplement":
                result = orchestrator.supplement_company_assets(payload)
                status = HTTPStatus.OK if result.get("status") != "invalid" else HTTPStatus.BAD_REQUEST
                return self._send_json(status, result)
            if path == "/api/intake/excel":
                result = orchestrator.ingest_excel_contacts(payload)
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
