#!/usr/bin/env python3
"""No-cost readiness checks for Apify provider webhook workflows."""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from apify_webhook_roundtrip_smoke import (  # noqa: E402
    _expected_probe_token,
    _probe_webhook_url,
    _validate_webhook_url,
)

from sourcing_agent.harvest_connectors import _apify_actor_run_webhooks_query_value  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run no-cost Apify webhook readiness checks before starting live workflows. "
            "This does not submit Harvest actors."
        )
    )
    parser.add_argument(
        "--mode",
        choices=("hosted", "local-live", "scripted"),
        default="local-live",
        help="Runtime mode being checked.",
    )
    parser.add_argument(
        "--webhook-url",
        default="",
        help="Public webhook URL. Defaults to SOURCING_APIFY_WEBHOOK_URL / APIFY_WEBHOOK_URL.",
    )
    parser.add_argument(
        "--provider-webhook-token",
        default="",
        help="Optional inbound shared secret for the probe. Defaults to runtime token settings.",
    )
    parser.add_argument(
        "--skip-connectivity-probe",
        action="store_true",
        help="Skip HTTP POST probe and only validate env/submit contract.",
    )
    parser.add_argument(
        "--allow-local-webhook-url",
        action="store_true",
        help="Allow localhost/private URLs. This is only appropriate for direct local endpoint checks.",
    )
    parser.add_argument(
        "--json-only",
        action="store_true",
        help="Only print the final JSON report.",
    )
    return parser


def _env_webhook_url() -> str:
    return str(os.getenv("SOURCING_APIFY_WEBHOOK_URL") or os.getenv("APIFY_WEBHOOK_URL") or "").strip()


def _redact_webhook_definitions(encoded: str) -> list[dict[str, Any]]:
    if not encoded:
        return []
    decoded = json.loads(base64.b64decode(encoded).decode("utf-8"))
    redacted: list[dict[str, Any]] = []
    for item in decoded if isinstance(decoded, list) else []:
        webhook = dict(item or {})
        headers_raw = str(webhook.get("headersTemplate") or "")
        if headers_raw:
            try:
                headers = json.loads(headers_raw)
            except Exception:
                headers = {"unparsed": "<redacted>"}
            if "X-Sourcing-Provider-Webhook-Token" in headers:
                headers["X-Sourcing-Provider-Webhook-Token"] = "<redacted>"
            webhook["headersTemplate"] = headers
        redacted.append(webhook)
    return redacted


def _profile_scraper_settings(orchestrator: Any) -> Any:
    harvest = getattr(getattr(orchestrator.acquisition_engine, "settings", None), "harvest", None)
    return getattr(harvest, "profile_scraper", None)


def _submit_context_for_mode(mode: str, webhook_url: str) -> dict[str, Any]:
    context: dict[str, Any] = {"external_provider_mode": "live"}
    if mode == "hosted":
        context["runtime_environment"] = "production"
    elif mode == "local-live":
        context["runtime_environment"] = "local_dev"
    if webhook_url:
        context["provider_webhook_url"] = webhook_url
    return context


def build_preflight_report(
    args: argparse.Namespace,
    *,
    orchestrator: Any,
    probe_func: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    mode = str(args.mode or "local-live")
    env_url = _env_webhook_url()
    webhook_url = str(args.webhook_url or env_url or "").strip()
    failures: list[str] = []
    warnings: list[str] = []

    report: dict[str, Any] = {
        "status": "pending",
        "mode": mode,
        "provider_mode": str(os.getenv("SOURCING_EXTERNAL_PROVIDER_MODE") or "").strip() or "live",
        "webhook_url": webhook_url,
        "env": {
            "SOURCING_APIFY_WEBHOOK_URL_present": bool(str(os.getenv("SOURCING_APIFY_WEBHOOK_URL") or "").strip()),
            "APIFY_WEBHOOK_URL_present": bool(str(os.getenv("APIFY_WEBHOOK_URL") or "").strip()),
            "SOURCING_PROVIDER_WEBHOOK_TOKEN_present": bool(
                str(os.getenv("SOURCING_PROVIDER_WEBHOOK_TOKEN") or "").strip()
            ),
            "APIFY_WEBHOOK_TOKEN_present": bool(str(os.getenv("APIFY_WEBHOOK_TOKEN") or "").strip()),
            "SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN": str(
                os.getenv("SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN") or ""
            ).strip(),
            "SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED": str(
                os.getenv("SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED") or ""
            ).strip(),
            "SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED": str(
                os.getenv("SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED") or ""
            ).strip(),
        },
    }

    if mode == "scripted":
        report["scripted_contract"] = {
            "external_apify_webhook_required": False,
            "reason": (
                "Scripted/browser tests use synthetic provider completion events and may drive the local "
                "webhook endpoint with a test token; they must not require a public Apify callback URL."
            ),
        }
        report["status"] = "ready"
        return report

    if webhook_url and args.webhook_url and not env_url:
        warnings.append(
            "--webhook-url was provided to preflight, but the current process environment does not contain it. "
            "Start the actual backend and worker with the same URL before submitting live actors."
        )

    settings = _profile_scraper_settings(orchestrator)
    if settings is None:
        failures.append("Harvest profile_scraper settings are unavailable.")
    else:
        enabled = bool(getattr(settings, "enabled", False))
        api_token_present = bool(str(getattr(settings, "api_token", "") or "").strip())
        report["harvest_profile_scraper"] = {"enabled": enabled, "api_token_present": api_token_present}
        if not enabled:
            failures.append("Harvest profile_scraper is disabled.")
        if not api_token_present:
            failures.append("Harvest profile_scraper api_token is missing.")

    if settings is not None:
        submit_context = _submit_context_for_mode(mode, webhook_url)
        encoded = _apify_actor_run_webhooks_query_value(submit_context, settings=settings)
        webhook_definitions = _redact_webhook_definitions(encoded)
        if not webhook_url and webhook_definitions:
            webhook_url = str(webhook_definitions[0].get("requestUrl") or "").strip()
            report["webhook_url"] = webhook_url
            report["webhook_url_source"] = "default_submit_contract"
        report["submit_contract"] = {
            "would_attach_webhooks": bool(encoded),
            "webhook_definition_count": len(webhook_definitions),
            "webhooks": webhook_definitions,
        }
        if not encoded:
            failures.append("Harvest submit contract would not attach Apify ad-hoc webhooks.")

    if not webhook_url:
        failures.append("No explicit or default Apify webhook URL is available for this runtime mode.")
    else:
        try:
            normalized_url = _validate_webhook_url(webhook_url, allow_local=bool(args.allow_local_webhook_url))
            report["webhook_url"] = normalized_url
            if mode == "hosted" and "/local-dev/" in normalized_url:
                failures.append("hosted mode must use /api/providers/apify/webhook, not the local-dev relay path.")
        except SystemExit as exc:
            failures.append(str(exc))

    if webhook_url and not bool(args.skip_connectivity_probe):
        token = _expected_probe_token(orchestrator, str(args.provider_webhook_token or ""))
        if not token:
            failures.append("No provider webhook token is available for connectivity probe.")
        else:
            probe = (probe_func or _probe_webhook_url)(webhook_url, token=token)
            report["connectivity_probe"] = probe
            if int(probe.get("http_status") or 0) != 202:
                failures.append(f"Webhook connectivity probe returned HTTP {probe.get('http_status')}.")
    elif bool(args.skip_connectivity_probe):
        warnings.append("Connectivity probe skipped; public route/token readiness was not verified.")

    report["warnings"] = warnings
    report["failures"] = failures
    report["status"] = "ready" if not failures else "failed"
    return report


def main() -> int:
    from sourcing_agent.cli import build_orchestrator

    args = _build_parser().parse_args()
    orchestrator = build_orchestrator()
    report = build_preflight_report(args, orchestrator=orchestrator)
    if not bool(args.json_only):
        print(
            f"Apify webhook preflight: {report['status']} "
            f"(mode={report['mode']}, webhook_url={report.get('webhook_url') or '<missing>'})",
            file=sys.stderr,
        )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report.get("status") == "ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
