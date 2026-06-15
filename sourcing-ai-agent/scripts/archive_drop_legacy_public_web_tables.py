#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sourcing_agent.legacy_public_web_storage import (  # noqa: E402
    archive_legacy_target_public_web_tables,
    drop_legacy_target_public_web_tables,
)
from sourcing_agent.settings import load_settings  # noqa: E402
from sourcing_agent.storage import ControlPlaneStore  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "W7e migration-only archive/drop utility for retired target-candidate Public Web tables. "
            "Normal CRM Public Web execution must not use this path."
        )
    )
    parser.add_argument("--runtime-dir", default="", help="Runtime directory. Defaults to repo runtime settings.")
    parser.add_argument(
        "--db-path",
        default="",
        help="Control-plane DB path. Defaults to <runtime-dir>/sourcing_agent.db.",
    )
    parser.add_argument("--row-limit", type=int, default=10000, help="Maximum rows to archive per legacy table.")
    parser.add_argument("--sample-limit", type=int, default=25, help="Maximum sample rows per table.")
    parser.add_argument(
        "--archive-json",
        default="",
        help="Cold archive manifest path. Required for dropping non-empty legacy tables unless explicitly overridden.",
    )
    parser.add_argument("--drop", action="store_true", help="Drop retired legacy tables after archive checks.")
    parser.add_argument(
        "--allow-non-empty-without-archive",
        action="store_true",
        help="Dangerous migration override: allow dropping non-empty legacy tables without writing archive-json.",
    )
    parser.add_argument("--reason", default="W7e_legacy_public_web_physical_deletion", help="Audit reason.")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if archive/drop is blocked.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime_dir = _resolve_runtime_dir(args.runtime_dir)
    db_path = Path(str(args.db_path)).expanduser() if str(args.db_path or "").strip() else runtime_dir / "sourcing_agent.db"
    if not db_path.is_absolute():
        db_path = (PROJECT_ROOT / db_path).resolve()
    store = ControlPlaneStore(db_path)
    archive_path = _resolve_optional_path(args.archive_json)
    if args.drop:
        result = drop_legacy_target_public_web_tables(
            store,
            archive_path=archive_path,
            row_limit=max(1, int(args.row_limit or 10000)),
            sample_limit=max(1, int(args.sample_limit or 25)),
            allow_non_empty_without_archive=bool(args.allow_non_empty_without_archive),
            reason=str(args.reason or "").strip(),
        )
    elif archive_path:
        result = archive_legacy_target_public_web_tables(
            store,
            archive_path,
            row_limit=max(1, int(args.row_limit or 10000)),
            sample_limit=max(1, int(args.sample_limit or 25)),
        )
    else:
        result = {
            "status": "noop",
            "reason": "pass --archive-json to write a cold archive or --drop to drop retired tables",
        }
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    if args.strict and str(result.get("status") or "").strip() == "blocked":
        return 1
    return 0


def _resolve_optional_path(value: str) -> Path | None:
    if not str(value or "").strip():
        return None
    path = Path(str(value)).expanduser()
    return path if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def _resolve_runtime_dir(value: str) -> Path:
    if str(value or "").strip():
        path = Path(str(value)).expanduser()
        return path if path.is_absolute() else (PROJECT_ROOT / path).resolve()
    settings = load_settings(PROJECT_ROOT)
    return settings.runtime_dir


if __name__ == "__main__":
    raise SystemExit(main())
