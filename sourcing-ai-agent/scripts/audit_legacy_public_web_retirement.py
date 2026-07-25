#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sourcing_agent.legacy_public_web_retirement_audit import audit_legacy_public_web_retirement  # noqa: E402
from sourcing_agent.settings import load_settings  # noqa: E402
from sourcing_agent.storage import ControlPlaneStore  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only W7e pre-delete audit for retired target-candidate Public Web state. "
            "It reports legacy target-candidate rows, CRM-owned Public Web rows, CRM records, "
            "and collection pointers before physical helper/table deletion."
        )
    )
    parser.add_argument("--runtime-dir", default="", help="Runtime directory. Defaults to repo runtime settings.")
    parser.add_argument(
        "--db-path",
        default="",
        help="Control-plane DB path. Defaults to <runtime-dir>/sourcing_agent.db.",
    )
    parser.add_argument("--workspace-id", default="default", help="CRM workspace id to audit.")
    parser.add_argument("--row-limit", type=int, default=10000, help="Maximum rows to inspect per surface.")
    parser.add_argument("--sample-limit", type=int, default=25, help="Maximum sample rows per surface.")
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if legacy rows remain or the audit is row-limit truncated.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime_dir = _resolve_runtime_dir(args.runtime_dir)
    db_path = Path(str(args.db_path)).expanduser() if str(args.db_path or "").strip() else runtime_dir / "sourcing_agent.db"
    if not db_path.is_absolute():
        db_path = (PROJECT_ROOT / db_path).resolve()
    store = ControlPlaneStore(db_path)
    report = audit_legacy_public_web_retirement(
        store=store,
        workspace_id=str(args.workspace_id or "default").strip() or "default",
        row_limit=max(1, int(args.row_limit or 10000)),
        sample_limit=max(1, int(args.sample_limit or 25)),
    )
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if str(args.output_json or "").strip():
        output_path = Path(str(args.output_json)).expanduser()
        if not output_path.is_absolute():
            output_path = (PROJECT_ROOT / output_path).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload + "\n", encoding="utf-8")
    sys.stdout.write(payload + "\n")
    if args.strict and not bool(report.get("deletion_allowed")):
        return 1
    return 0


def _resolve_runtime_dir(value: str) -> Path:
    if str(value or "").strip():
        path = Path(str(value)).expanduser()
        return path if path.is_absolute() else (PROJECT_ROOT / path).resolve()
    settings = load_settings(PROJECT_ROOT)
    return settings.runtime_dir


if __name__ == "__main__":
    raise SystemExit(main())
