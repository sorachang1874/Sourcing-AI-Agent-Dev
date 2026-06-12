from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from typing import Any
from urllib import parse
from urllib import request as urlrequest

from tests.fake_provider_http import FakeProviderHTTPServer, FakeProviderRequest, FakeProviderRouteResponse


@dataclass
class FakeApifyActorRun:
    actor_id: str
    run_id: str
    dataset_id: str
    dataset_items: list[Any] = field(default_factory=list)
    submit_status: str = "RUNNING"
    status_sequence: list[str] = field(default_factory=lambda: ["SUCCEEDED"])
    dataset_failures: list[FakeProviderRouteResponse] = field(default_factory=list)
    log_text: str = ""
    poll_count: int = 0

    def next_status(self) -> str:
        if not self.status_sequence:
            return self.submit_status
        index = min(self.poll_count, len(self.status_sequence) - 1)
        self.poll_count += 1
        return self.status_sequence[index]


class FakeApifyProvider:
    def __init__(self) -> None:
        self.http = FakeProviderHTTPServer()
        self.runs: dict[str, FakeApifyActorRun] = {}
        self.submissions: list[FakeProviderRequest] = []

    @property
    def base_url(self) -> str:
        return self.http.base_url

    @property
    def requests(self) -> list[FakeProviderRequest]:
        return self.http.requests

    def add_actor_run(
        self,
        *,
        actor_id: str,
        run_id: str,
        dataset_id: str,
        dataset_items: list[Any] | None = None,
        submit_status: str = "RUNNING",
        status_sequence: list[str] | None = None,
        dataset_failures: list[FakeProviderRouteResponse] | None = None,
        log_text: str = "",
    ) -> FakeApifyActorRun:
        run = FakeApifyActorRun(
            actor_id=actor_id,
            run_id=run_id,
            dataset_id=dataset_id,
            dataset_items=list(dataset_items or []),
            submit_status=submit_status,
            status_sequence=list(status_sequence or ["SUCCEEDED"]),
            dataset_failures=list(dataset_failures or []),
            log_text=log_text,
        )
        self.runs[run_id] = run
        self.http.set_route("POST", _actor_runs_path(actor_id), lambda request: self._submit_actor_run(request, run))
        self.http.set_route("POST", _actor_sync_path(actor_id), lambda request: self._sync_actor_run(request, run))
        self.http.set_route("GET", _actor_run_path(run_id), lambda request: self._get_actor_run(request, run))
        self.http.set_route("GET", _actor_run_log_path(run_id), lambda request: self._get_actor_run_log(request, run))
        self.http.set_route("GET", _dataset_items_path(dataset_id), lambda request: self._get_dataset_items(request, run))
        return run

    def submitted_webhooks(self) -> list[dict[str, Any]]:
        webhooks: list[dict[str, Any]] = []
        for request in self.submissions:
            for raw_value in request.get("query", {}).get("webhooks", []):
                try:
                    decoded = base64.b64decode(str(raw_value)).decode("utf-8")
                    payload = json.loads(decoded)
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                if isinstance(payload, list):
                    webhooks.extend(item for item in payload if isinstance(item, dict))
                elif isinstance(payload, dict):
                    webhooks.append(payload)
        return webhooks

    def deliver_webhooks(
        self,
        *,
        run_id: str,
        event_type: str = "ACTOR.RUN.SUCCEEDED",
        extra_payload: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        run = self.runs[str(run_id)]
        payload = {
            "eventType": event_type,
            "eventData": {
                "actorRunId": run.run_id,
                "actorId": run.actor_id,
                "defaultDatasetId": run.dataset_id,
            },
            "resource": _run_record(run, status=_status_for_event_type(event_type)),
            **dict(extra_payload or {}),
        }
        delivered: list[dict[str, Any]] = []
        for webhook in self.submitted_webhooks():
            event_types = set(webhook.get("eventTypes") or [])
            if event_types and event_type not in event_types:
                continue
            request_url = str(webhook.get("requestUrl") or "").strip()
            if not request_url:
                continue
            headers = _webhook_headers(webhook)
            headers.setdefault("Content-Type", "application/json")
            raw_payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            http_request = urlrequest.Request(request_url, data=raw_payload, headers=headers, method="POST")
            with urlrequest.urlopen(http_request, timeout=15) as response:
                response_body = response.read().decode("utf-8", errors="replace")
                delivered.append(
                    {
                        "request_url": request_url,
                        "status": int(response.status),
                        "body": response_body,
                    }
                )
        return delivered

    def __enter__(self) -> "FakeApifyProvider":
        self.http.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.http.__exit__(exc_type, exc, tb)

    def _submit_actor_run(self, request: FakeProviderRequest, run: FakeApifyActorRun) -> FakeProviderRouteResponse:
        self.submissions.append(request)
        return 200, {"data": _run_record(run, status=run.submit_status)}

    def _sync_actor_run(self, request: FakeProviderRequest, run: FakeApifyActorRun) -> FakeProviderRouteResponse:
        self.submissions.append(request)
        return 200, list(run.dataset_items)

    def _get_actor_run(self, request: FakeProviderRequest, run: FakeApifyActorRun) -> FakeProviderRouteResponse:
        return 200, {"data": _run_record(run, status=run.next_status())}

    def _get_actor_run_log(self, request: FakeProviderRequest, run: FakeApifyActorRun) -> FakeProviderRouteResponse:
        return 200, {"log": run.log_text}

    def _get_dataset_items(self, request: FakeProviderRequest, run: FakeApifyActorRun) -> FakeProviderRouteResponse:
        if run.dataset_failures:
            return run.dataset_failures.pop(0)
        query = request.get("query", {})
        offset = _query_int(query, "offset", 0)
        limit = _query_int(query, "limit", len(run.dataset_items) or 1)
        return 200, run.dataset_items[offset : offset + limit]


def _run_record(run: FakeApifyActorRun, *, status: str) -> dict[str, Any]:
    return {
        "id": run.run_id,
        "status": status,
        "defaultDatasetId": run.dataset_id,
    }


def _query_int(query: dict[str, list[str]], key: str, default: int) -> int:
    try:
        return int(query.get(key, [str(default)])[0])
    except (TypeError, ValueError):
        return default


def _webhook_headers(webhook: dict[str, Any]) -> dict[str, str]:
    raw_template = str(webhook.get("headersTemplate") or "").strip()
    if not raw_template:
        return {}
    try:
        parsed = json.loads(raw_template)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(key): str(value) for key, value in parsed.items()}


def _status_for_event_type(event_type: str) -> str:
    normalized = str(event_type or "").strip().upper()
    if normalized.endswith(".SUCCEEDED"):
        return "SUCCEEDED"
    if normalized.endswith(".FAILED"):
        return "FAILED"
    if normalized.endswith(".TIMED_OUT"):
        return "TIMED-OUT"
    if normalized.endswith(".ABORTED"):
        return "ABORTED"
    return "RUNNING"


def _actor_runs_path(actor_id: str) -> str:
    return f"/v2/acts/{parse.quote(str(actor_id or '').strip(), safe='')}/runs"


def _actor_sync_path(actor_id: str) -> str:
    return f"/v2/acts/{parse.quote(str(actor_id or '').strip(), safe='')}/run-sync-get-dataset-items"


def _actor_run_path(run_id: str) -> str:
    return f"/v2/actor-runs/{parse.quote(str(run_id or '').strip(), safe='')}"


def _actor_run_log_path(run_id: str) -> str:
    return f"/v2/actor-runs/{parse.quote(str(run_id or '').strip(), safe='')}/log"


def _dataset_items_path(dataset_id: str) -> str:
    return f"/v2/datasets/{parse.quote(str(dataset_id or '').strip(), safe='')}/items"
