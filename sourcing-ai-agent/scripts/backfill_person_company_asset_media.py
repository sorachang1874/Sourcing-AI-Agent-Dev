#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Explicit Phase 5c/7c historical asset repair runner. Defaults to dry-run. "
            "It backfills Public Web signals into PersonAsset/PersonEvidence and plans media.asset.cache "
            "commands for historical person avatars / company logos without request-path fetching, and "
            "syncs historical company Public Web source rows into CompanyAsset/CompanyEvidence."
        )
    )
    parser.add_argument("--runtime-dir", default="", help="Runtime directory. Defaults to configured runtime.")
    parser.add_argument(
        "--collection-id",
        action="append",
        default=[],
        help="Collection id to inspect, for example company:openai. Repeatable.",
    )
    parser.add_argument(
        "--projection-id",
        action="append",
        default=[],
        help="Projection id to use for person avatar backfill. Repeatable.",
    )
    parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Company name/key to use for company logo backfill when no collection id is available. Repeatable.",
    )
    parser.add_argument(
        "--public-web-run-id",
        action="append",
        default=[],
        help="Optional Public Web run id to limit signal backfill. Repeatable.",
    )
    parser.add_argument(
        "--company-logo-source-url",
        action="append",
        default=[],
        help="Explicit company logo URL mapping as company_or_collection=url. Repeatable.",
    )
    parser.add_argument("--limit", type=int, default=1000, help="Per-endpoint bounded row/member limit.")
    parser.add_argument(
        "--skip-active-collections",
        action="store_true",
        help="Do not automatically include active collection authoritative pointers.",
    )
    parser.add_argument("--skip-public-web-signals", action="store_true", help="Skip Public Web signal repair.")
    parser.add_argument(
        "--skip-company-public-web-facts",
        action="store_true",
        help="Skip historical company Public Web fact sync into CompanyAsset/CompanyEvidence.",
    )
    parser.add_argument("--skip-person-avatars", action="store_true", help="Skip person avatar media repair.")
    parser.add_argument("--skip-company-logos", action="store_true", help="Skip company logo media repair.")
    parser.add_argument(
        "--include-homepage-favicon",
        action="store_true",
        help="Allow company logo backfill to derive homepage favicon candidates. Default is off because it is heuristic.",
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="After planning media.asset.cache commands, drain the media owner immediately.",
    )
    parser.add_argument("--apply", action="store_true", help="Mutate canonical assets or plan workflow commands.")
    parser.add_argument(
        "--reviewed",
        action="store_true",
        help="Required with --apply. Records that the operator reviewed the dry-run target scope.",
    )
    parser.add_argument("--output-json", default="", help="Optional JSON report path.")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when any endpoint returns an error status.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.runtime_dir:
        runtime_dir = Path(str(args.runtime_dir)).expanduser()
        if not runtime_dir.is_absolute():
            runtime_dir = (PROJECT_ROOT / runtime_dir).resolve()
        os.environ["SOURCING_RUNTIME_DIR"] = str(runtime_dir)
    if args.apply and not args.reviewed:
        raise SystemExit("--apply requires --reviewed after inspecting a dry-run report")
    _require_pg_only()

    from sourcing_agent.cli import build_orchestrator  # noqa: WPS433

    orchestrator = build_orchestrator()
    store = orchestrator.store
    limit = max(1, int(args.limit or 1000))
    dry_run = not bool(args.apply)
    active_pointers = []
    if not args.skip_active_collections:
        active_pointers = store.repos.serving_projection.list_authoritative_pointers(state="active", limit=limit)

    collection_ids = _dedupe(
        [
            *[str(item or "").strip() for item in list(args.collection_id or [])],
            *[str(pointer.get("collection_id") or "").strip() for pointer in active_pointers],
        ]
    )
    projection_ids = _dedupe(
        [
            *[str(item or "").strip() for item in list(args.projection_id or [])],
            *[
                str(pointer.get("active_projection_id") or "").strip()
                for pointer in active_pointers
                if str(pointer.get("active_projection_id") or "").strip()
            ],
        ]
    )
    logo_source_urls = _parse_key_value_args(args.company_logo_source_url)
    report: dict[str, Any] = {
        "status": "dry_run_ready" if dry_run else "applied",
        "apply": bool(args.apply),
        "reviewed": bool(args.reviewed),
        "limit": limit,
        "runtime_contract": {
            "pg_only_required": True,
            "normal_reader_repair": False,
            "request_path_fetch": False,
        },
        "targets": {
            "collection_ids": collection_ids,
            "projection_ids": projection_ids,
            "companies": _dedupe([str(item or "").strip() for item in list(args.company or [])]),
        },
        "public_web_signal_backfills": [],
        "person_avatar_backfills": [],
        "company_logo_backfills": [],
        "company_public_web_fact_backfills": [],
        "errors": [],
    }

    if not args.skip_public_web_signals:
        run_ids = _dedupe([str(item or "").strip() for item in list(args.public_web_run_id or [])])
        public_web_payload: dict[str, Any] = {"dry_run": dry_run, "reviewed": bool(args.reviewed), "limit": limit}
        if run_ids:
            public_web_payload["run_ids"] = run_ids
        result = orchestrator.backfill_public_web_signals_to_person_asset_layer(public_web_payload)
        report["public_web_signal_backfills"].append(result)
        _record_error_if_needed(report, "public_web_signals", result, allowed={"dry_run", "backfilled"})

    if not args.skip_person_avatars:
        for projection_id in projection_ids:
            result = orchestrator.backfill_person_avatar_media_assets_api(
                {
                    "projection_id": projection_id,
                    "dry_run": dry_run,
                    "reviewed": bool(args.reviewed),
                    "limit": limit,
                    "run_now": bool(args.run_now),
                }
            )
            report["person_avatar_backfills"].append(result)
            _record_error_if_needed(report, f"person_avatars:{projection_id}", result, allowed={"dry_run", "planned"})

    if not args.skip_company_public_web_facts:
        fact_targets = collection_ids or _dedupe([str(item or "").strip() for item in list(args.company or [])])
        if not fact_targets:
            result = orchestrator.backfill_company_public_web_assets_to_company_asset_layer_api(
                {"dry_run": dry_run, "reviewed": bool(args.reviewed), "limit": limit}
            )
            report["company_public_web_fact_backfills"].append(result)
            _record_error_if_needed(report, "company_public_web_facts", result, allowed={"dry_run", "backfilled"})
        else:
            for target in fact_targets:
                payload = {"dry_run": dry_run, "reviewed": bool(args.reviewed), "limit": limit}
                if str(target or "").strip().startswith("company:"):
                    payload["collection_id"] = str(target or "").strip()
                else:
                    payload["target_company"] = str(target or "").strip()
                result = orchestrator.backfill_company_public_web_assets_to_company_asset_layer_api(payload)
                report["company_public_web_fact_backfills"].append(result)
                _record_error_if_needed(
                    report,
                    f"company_public_web_facts:{target}",
                    result,
                    allowed={"dry_run", "backfilled"},
                )

    if not args.skip_company_logos:
        company_targets = _dedupe([str(item or "").strip() for item in list(args.company or [])])
        for collection_id in collection_ids:
            source_url = _lookup_logo_source_url(logo_source_urls, collection_id)
            payload: dict[str, Any] = {
                "collection_id": collection_id,
                "dry_run": dry_run,
                "reviewed": bool(args.reviewed),
                "limit": limit,
                "run_now": bool(args.run_now),
                "include_homepage_favicon": bool(args.include_homepage_favicon),
            }
            if source_url:
                payload["source_url"] = source_url
            result = orchestrator.backfill_company_logo_media_assets_api(payload)
            report["company_logo_backfills"].append(result)
            _record_error_if_needed(report, f"company_logos:{collection_id}", result, allowed={"dry_run", "planned"})
        for company in company_targets:
            source_url = _lookup_logo_source_url(logo_source_urls, company)
            payload = {
                "target_company": company,
                "dry_run": dry_run,
                "reviewed": bool(args.reviewed),
                "limit": limit,
                "run_now": bool(args.run_now),
                "include_homepage_favicon": bool(args.include_homepage_favicon),
            }
            if source_url:
                payload["source_url"] = source_url
            result = orchestrator.backfill_company_logo_media_assets_api(payload)
            report["company_logo_backfills"].append(result)
            _record_error_if_needed(report, f"company_logos:{company}", result, allowed={"dry_run", "planned"})

    report["summary"] = _summarize(report)
    if report["errors"]:
        report["status"] = "completed_with_errors"
    output = json.dumps(report, ensure_ascii=False, indent=2)
    if str(args.output_json or "").strip():
        output_path = Path(str(args.output_json)).expanduser()
        if not output_path.is_absolute():
            output_path = (PROJECT_ROOT / output_path).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output + "\n", encoding="utf-8")
    else:
        sys.stdout.write(output + "\n")
    if args.strict and report["errors"]:
        return 1
    return 0


def _require_pg_only() -> None:
    from sourcing_agent.control_plane_live_postgres import resolve_control_plane_postgres_live_mode  # noqa: WPS433
    from sourcing_agent.local_postgres import resolve_control_plane_postgres_dsn  # noqa: WPS433

    dsn = str(resolve_control_plane_postgres_dsn(PROJECT_ROOT) or "").strip()
    live_mode = str(resolve_control_plane_postgres_live_mode(os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE")) or "").strip()
    require_pg = os.getenv("SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES") == "1"
    shadow_backend = str(os.getenv("SOURCING_PG_ONLY_SQLITE_BACKEND") or "").strip()
    if not dsn or live_mode != "postgres_only" or not require_pg or shadow_backend != "shared_memory":
        raise SystemExit(
            "Asset media backfill requires PG-only control plane. Set "
            "SOURCING_CONTROL_PLANE_POSTGRES_DSN, SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only, "
            "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1, and SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory."
        )


def _parse_key_value_args(values: list[str] | tuple[str, ...]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for raw in list(values or []):
        text = str(raw or "").strip()
        if not text:
            continue
        if "=" not in text:
            raise SystemExit(f"Invalid --company-logo-source-url value {raw!r}; expected company_or_collection=url")
        key, value = text.split("=", 1)
        key = key.strip().lower()
        value = value.strip()
        if not key or not value:
            raise SystemExit(f"Invalid --company-logo-source-url value {raw!r}; key and URL must be non-empty")
        parsed[key] = value
    return parsed


def _lookup_logo_source_url(mapping: dict[str, str], key: str) -> str:
    normalized = str(key or "").strip().lower()
    if not normalized:
        return ""
    if normalized in mapping:
        return mapping[normalized]
    if normalized.startswith("company:"):
        return mapping.get(normalized.removeprefix("company:"), "")
    return mapping.get(f"company:{normalized}", "")


def _record_error_if_needed(report: dict[str, Any], target: str, result: dict[str, Any], *, allowed: set[str]) -> None:
    status = str(dict(result or {}).get("status") or "").strip()
    if status not in allowed:
        report.setdefault("errors", []).append(
            {
                "target": target,
                "status": status,
                "reason": str(dict(result or {}).get("reason") or "").strip(),
            }
        )


def _summarize(report: dict[str, Any]) -> dict[str, Any]:
    public_web_results = [dict(item or {}) for item in list(report.get("public_web_signal_backfills") or [])]
    avatar_results = [dict(item or {}) for item in list(report.get("person_avatar_backfills") or [])]
    logo_results = [dict(item or {}) for item in list(report.get("company_logo_backfills") or [])]
    fact_results = [dict(item or {}) for item in list(report.get("company_public_web_fact_backfills") or [])]
    return {
        "public_web_signal_eligible_count": sum(int(item.get("eligible_signal_count") or 0) for item in public_web_results),
        "public_web_signal_asset_count": sum(int(item.get("asset_count") or 0) for item in public_web_results),
        "person_avatar_eligible_command_count": sum(int(item.get("eligible_command_count") or 0) for item in avatar_results),
        "person_avatar_planned_command_count": sum(int(item.get("planned_command_count") or 0) for item in avatar_results),
        "company_public_web_fact_eligible_asset_count": sum(int(item.get("eligible_asset_count") or 0) for item in fact_results),
        "company_public_web_fact_synced_asset_count": sum(int(item.get("synced_asset_count") or 0) for item in fact_results),
        "company_public_web_fact_synced_evidence_count": sum(int(item.get("synced_evidence_count") or 0) for item in fact_results),
        "company_logo_eligible_command_count": sum(int(item.get("eligible_command_count") or 0) for item in logo_results),
        "company_logo_planned_command_count": sum(int(item.get("planned_command_count") or 0) for item in logo_results),
        "error_count": len(list(report.get("errors") or [])),
    }


def _dedupe(values: list[str] | tuple[str, ...]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
