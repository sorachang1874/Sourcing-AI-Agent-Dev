#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import parse, request

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Submit one live Harvest LinkedIn profile actor with an Apify ad-hoc webhook "
            "and wait for the provider callback to complete the local worker."
        )
    )
    parser.add_argument("--webhook-url", required=True, help="Public backend callback URL ending in /api/providers/apify/webhook")
    parser.add_argument("--profile-url", required=True, help="LinkedIn profile URL to fetch for the one-profile smoke")
    parser.add_argument("--job-id", default="", help="Optional smoke job id")
    parser.add_argument("--target-company", default="Webhook Roundtrip Smoke", help="Synthetic target company label")
    parser.add_argument("--wait-seconds", type=int, default=180, help="Maximum seconds to wait for worker completion")
    parser.add_argument("--poll-seconds", type=float, default=3.0, help="Local worker status poll interval")
    parser.add_argument(
        "--post-completion-event-grace-seconds",
        type=float,
        default=15.0,
        help=(
            "After a fast worker completes, keep polling job events for this many seconds before "
            "treating the run as a missing webhook. This avoids false negatives when local polling beats dispatch."
        ),
    )
    parser.add_argument(
        "--provider-webhook-token",
        default="",
        help=(
            "Optional inbound shared secret. If omitted, current short-term defaults reuse the Harvest Apify API token; "
            "the running backend must accept the same token."
        ),
    )
    parser.add_argument(
        "--skip-connectivity-probe",
        action="store_true",
        help="Skip the unsigned-work-free POST probe to the webhook URL before submitting the live actor.",
    )
    parser.add_argument(
        "--allow-local-webhook-url",
        action="store_true",
        help="Allow localhost/private webhook URLs. This is usually wrong because Apify must reach the URL.",
    )
    parser.add_argument(
        "--allow-existing-registry-entry",
        action="store_true",
        help="Allow a URL that is already fetched/queued in the local registry; the script may not submit a live run.",
    )
    parser.add_argument(
        "--i-understand-this-submits-live-run",
        action="store_true",
        help="Required safety acknowledgement because this can call a paid live provider.",
    )
    return parser


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _validate_webhook_url(value: str, *, allow_local: bool) -> str:
    webhook_url = str(value or "").strip()
    parsed = parse.urlparse(webhook_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise SystemExit("--webhook-url must be an absolute http(s) URL")
    host = (parsed.hostname or "").lower()
    local_hosts = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}
    if not allow_local and (host in local_hosts or host.endswith(".local")):
        raise SystemExit("Apify cannot call a local/private webhook URL; use a public tunnel or hosted ECS URL.")
    normalized_path = parsed.path.rstrip("/")
    if not (
        normalized_path.endswith("/api/providers/apify/webhook")
        or normalized_path.endswith("/local-dev/providers/apify/webhook")
    ):
        raise SystemExit("--webhook-url should point to /api/providers/apify/webhook or the ECS local-dev relay")
    return webhook_url


def _json_line(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)


def _expected_probe_token(orchestrator: Any, explicit_token: str) -> str:
    if explicit_token.strip():
        return explicit_token.strip()
    env_token = str(os.getenv("SOURCING_PROVIDER_WEBHOOK_TOKEN") or os.getenv("APIFY_WEBHOOK_TOKEN") or "").strip()
    if env_token:
        return env_token
    if not _env_bool("SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN", True):
        return ""
    harvest = getattr(getattr(orchestrator.acquisition_engine, "settings", None), "harvest", None)
    profile_scraper = getattr(harvest, "profile_scraper", None)
    return str(getattr(profile_scraper, "api_token", "") or "").strip()


def _probe_webhook_url(webhook_url: str, *, token: str) -> dict[str, Any]:
    body = json.dumps(
        {
            "eventType": "ACTOR.RUN.SUCCEEDED",
            "eventData": {
                "actorRunId": "apify-webhook-smoke-connectivity-probe",
                "defaultDatasetId": "apify-webhook-smoke-connectivity-probe-dataset",
            },
        }
    ).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": os.getenv("SOURCING_PROVIDER_WEBHOOK_USER_AGENT", "SourcingAgentApifyWebhook/1.0"),
    }
    if token:
        headers["X-Sourcing-Provider-Webhook-Token"] = token
    req = request.Request(webhook_url, data=body, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=15) as response:
            response_body = response.read().decode("utf-8", errors="replace")
            parsed = json.loads(response_body) if response_body.strip().startswith("{") else {"body": response_body}
            return {"http_status": int(response.status), "body": parsed}
    except urllib_error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        parsed = json.loads(response_body) if response_body.strip().startswith("{") else {"body": response_body}
        return {"http_status": int(exc.code), "body": parsed}


def _registry_entry(orchestrator: Any, profile_url: str) -> dict[str, Any]:
    from sourcing_agent.linkedin_url_normalization import normalize_linkedin_profile_url_key

    store = getattr(orchestrator, "store", None)
    if store is None:
        return {}
    key = normalize_linkedin_profile_url_key(profile_url)
    entries = store.get_linkedin_profile_registry_bulk([profile_url])
    return dict(entries.get(key) or {})


def _worker_for_smoke(orchestrator: Any, *, job_id: str, profile_url: str) -> dict[str, Any]:
    payload_hash = sha1(json.dumps(sorted([profile_url]), ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
    return dict(
        orchestrator.store.get_agent_worker(
            job_id=job_id,
            lane_id="enrichment_specialist",
            worker_key=f"harvest_profile_batch::{payload_hash}",
        )
        or {}
    )


def main() -> int:
    from sourcing_agent.cli import build_orchestrator
    from sourcing_agent.domain import JobRequest

    args = _build_parser().parse_args()
    if not args.i_understand_this_submits_live_run:
        raise SystemExit("Refusing to submit a live provider run without --i-understand-this-submits-live-run.")

    webhook_url = _validate_webhook_url(args.webhook_url, allow_local=bool(args.allow_local_webhook_url))
    profile_url = str(args.profile_url or "").strip()
    if not profile_url:
        raise SystemExit("--profile-url is required")

    os.environ["SOURCING_EXTERNAL_PROVIDER_MODE"] = "live"
    os.environ["SOURCING_APIFY_WEBHOOK_URL"] = webhook_url
    os.environ["SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED"] = "0"
    os.environ.setdefault("SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN", "1")
    if str(args.provider_webhook_token or "").strip():
        os.environ["SOURCING_PROVIDER_WEBHOOK_TOKEN"] = str(args.provider_webhook_token).strip()

    orchestrator = build_orchestrator()
    profile_scraper = orchestrator.acquisition_engine.settings.harvest.profile_scraper
    if not profile_scraper.enabled or not str(profile_scraper.api_token or "").strip():
        raise SystemExit("Harvest profile_scraper is not enabled; check runtime/secrets/providers.local.json.")

    registry_entry = _registry_entry(orchestrator, profile_url)
    registry_status = str(registry_entry.get("status") or "").strip().lower()
    if registry_status in {"fetched", "queued"} and not args.allow_existing_registry_entry:
        raise SystemExit(
            f"Profile URL is already {registry_status} in the local registry; choose a never-used URL "
            "or pass --allow-existing-registry-entry if you only want to exercise the local path."
        )

    token = _expected_probe_token(orchestrator, str(args.provider_webhook_token or ""))
    if not args.skip_connectivity_probe:
        probe = _probe_webhook_url(webhook_url, token=token)
        _json_line({"phase": "connectivity_probe", **probe})
        if int(probe.get("http_status") or 0) != 202:
            raise SystemExit("Webhook connectivity probe did not return HTTP 202; fix URL/token before submitting live actor.")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    job_id = str(args.job_id or f"webhook_roundtrip_{timestamp}").strip()
    settings = orchestrator.acquisition_engine.settings
    snapshot_dir = settings.runtime_dir / "company_assets" / "webhook_roundtrip_smoke" / job_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    request_payload = JobRequest(
        raw_user_request="Provider webhook round-trip smoke",
        query="Provider webhook round-trip smoke",
        target_company=str(args.target_company or "Webhook Roundtrip Smoke").strip(),
        profile_detail_limit=1,
    ).to_record()
    plan_payload = {"plan_id": "apify_webhook_roundtrip_smoke", "tasks": []}
    orchestrator.store.save_job(
        job_id=job_id,
        job_type="workflow",
        status="running",
        stage="linkedin_stage_1",
        request_payload=request_payload,
        plan_payload=plan_payload,
        summary_payload={"snapshot_id": snapshot_dir.name, "smoke": "apify_webhook_roundtrip"},
    )

    result = orchestrator.acquisition_engine.multi_source_enricher._execute_harvest_profile_batch_worker(
        profile_urls=[profile_url],
        snapshot_dir=snapshot_dir,
        job_id=job_id,
        request_payload=request_payload,
        plan_payload=plan_payload,
        runtime_mode="workflow",
        allow_shared_provider_cache=False,
    )
    summary = dict(result.get("summary") or {})
    worker = _worker_for_smoke(orchestrator, job_id=job_id, profile_url=profile_url)
    run_id = str(summary.get("run_id") or dict(worker.get("checkpoint") or {}).get("run_id") or "").strip()
    dataset_id = str(summary.get("dataset_id") or dict(worker.get("checkpoint") or {}).get("dataset_id") or "").strip()
    worker_id = int(worker.get("worker_id") or 0)
    _json_line(
        {
            "phase": "submitted",
            "job_id": job_id,
            "worker_id": worker_id,
            "worker_status": result.get("worker_status"),
            "run_id": run_id,
            "dataset_id": dataset_id,
            "summary_path": summary.get("summary_path"),
        }
    )
    if not run_id or worker_id <= 0:
        raise SystemExit("No live Apify run/worker was created; the profile URL may have been served from cache.")

    deadline = time.time() + max(1, int(args.wait_seconds or 180))
    last_status = ""
    remote_event_seen = False
    while time.time() < deadline:
        worker = dict(orchestrator.store.get_agent_worker(worker_id=worker_id) or {})
        status = str(worker.get("status") or "").strip()
        checkpoint = dict(worker.get("checkpoint") or {})
        if status != last_status:
            _json_line(
                {
                    "phase": "worker_status",
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "status": status,
                    "checkpoint_stage": checkpoint.get("stage"),
                }
            )
            last_status = status
        events = orchestrator.store.list_job_events(job_id)
        remote_event_seen = remote_event_seen or any(
            str(event.get("stage") or "") == "remote_provider_event" for event in events
        )
        if status == "completed":
            output = dict(worker.get("output") or {})
            result_summary = dict(output.get("summary") or {})
            if not remote_event_seen:
                grace_deadline = time.time() + max(0.0, float(args.post_completion_event_grace_seconds or 0.0))
                while time.time() < grace_deadline and not remote_event_seen:
                    time.sleep(max(0.5, min(float(args.poll_seconds or 3.0), 2.0)))
                    events = orchestrator.store.list_job_events(job_id)
                    remote_event_seen = any(
                        str(event.get("stage") or "") == "remote_provider_event" for event in events
                    )
            _json_line(
                {
                    "phase": "completed",
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "remote_event_seen": remote_event_seen,
                    "persisted_profile_count": result_summary.get("profile_count")
                    or result_summary.get("persisted_profile_count"),
                    "summary_path": result_summary.get("summary_path") or summary.get("summary_path"),
                }
            )
            return 0 if remote_event_seen else 3
        time.sleep(max(0.5, float(args.poll_seconds or 3.0)))

    _json_line(
        {
            "phase": "timeout",
            "job_id": job_id,
            "worker_id": worker_id,
            "run_id": run_id,
            "dataset_id": dataset_id,
            "remote_event_seen": remote_event_seen,
            "next_check": f"PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli show-workers --job-id {job_id}",
        }
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
