#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sourcing_agent.dataforseo_client import DataForSeoGoogleOrganicClient
from sourcing_agent.settings import SearchProviderSettings, load_settings


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DataForSEO Google Organic helper")
    parser.add_argument("--project-root", default=str(PROJECT_ROOT), help="Project root containing runtime/secrets/providers.local.json")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command_name in ("live", "task-post"):
        subparser = subparsers.add_parser(command_name)
        subparser.add_argument("--keyword", required=True)
        subparser.add_argument("--location-name", default="")
        subparser.add_argument("--language-name", default="")
        subparser.add_argument("--device", default="")
        subparser.add_argument("--os", default="")
        subparser.add_argument("--depth", type=int, default=0)
        subparser.add_argument("--tag", default="")

    subparsers.add_parser("tasks-ready")
    task_get_parser = subparsers.add_parser("task-get")
    task_get_parser.add_argument("--task-id", required=True)
    return parser


def _load_client(project_root: Path) -> tuple[DataForSeoGoogleOrganicClient, SearchProviderSettings]:
    settings = load_settings(project_root)
    search_settings = settings.search
    if not search_settings.dataforseo_login or not search_settings.dataforseo_password:
        raise SystemExit("DataForSEO credentials are missing in runtime/secrets/providers.local.json or env.")
    client = DataForSeoGoogleOrganicClient(
        login=search_settings.dataforseo_login,
        password=search_settings.dataforseo_password,
        base_url=search_settings.dataforseo_base_url,
        timeout_seconds=search_settings.timeout_seconds,
    )
    return client, search_settings


def _resolve_task_defaults(args: argparse.Namespace, settings: SearchProviderSettings) -> dict[str, object]:
    return {
        "keyword": str(args.keyword).strip(),
        "location_name": str(args.location_name or settings.dataforseo_default_location_name).strip() or "United States",
        "language_name": str(args.language_name or settings.dataforseo_default_language_name).strip() or "English",
        "device": str(args.device or settings.dataforseo_default_device).strip() or "desktop",
        "os": str(args.os or settings.dataforseo_default_os).strip() or "windows",
        "depth": int(args.depth or settings.dataforseo_default_depth or 10),
        "tag": str(args.tag or "").strip(),
    }


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    client, search_settings = _load_client(Path(args.project_root))

    if args.command == "live":
        payload = client.live_regular(**_resolve_task_defaults(args, search_settings))
    elif args.command == "task-post":
        payload = client.task_post(**_resolve_task_defaults(args, search_settings))
    elif args.command == "tasks-ready":
        payload = client.tasks_ready()
    elif args.command == "task-get":
        payload = client.task_get_regular(args.task_id)
    else:
        raise SystemExit(f"Unsupported command: {args.command}")

    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
