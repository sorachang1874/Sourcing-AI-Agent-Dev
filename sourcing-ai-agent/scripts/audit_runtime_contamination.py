#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sourcing_agent.runtime_contamination_audit import (
    apply_runtime_contamination_quarantine_plan,
    build_runtime_contamination_quarantine_plan,
    build_runtime_contamination_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only audit for test-runtime contamination in root/local-dev state. "
            "Default mode does not delete files, mutate Postgres, or call providers. "
            "--apply-quarantine-plan is the explicit repair mode."
        )
    )
    parser.add_argument(
        "--workspace-root",
        default=".",
        help="Repository/workspace root used to resolve default runtime paths.",
    )
    parser.add_argument(
        "--target-runtime-dir",
        default="",
        help="Isolated runtime dir to search for, for example runtime/test_env/<case>.",
    )
    parser.add_argument(
        "--dsn",
        default="",
        help="Optional Postgres DSN override. Defaults to local control-plane resolution.",
    )
    parser.add_argument(
        "--schema",
        default="",
        help="Optional Postgres schema override. Defaults to the resolved control-plane schema.",
    )
    parser.add_argument(
        "--skip-postgres",
        action="store_true",
        help="Only scan provider cache files; do not connect to Postgres.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=25,
        help="Maximum samples to include per scanned surface.",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional path to write the JSON report.",
    )
    parser.add_argument(
        "--quarantine-plan",
        action="store_true",
        help=(
            "Emit a dry-run quarantine/repair plan with counts, sample SQL, and file-move commands. "
            "No state is mutated."
        ),
    )
    parser.add_argument(
        "--apply-quarantine-plan",
        action="store_true",
        help=(
            "Apply the reviewed quarantine/repair plan: move contaminated live-cache artifacts, "
            "copy matching PG rows into quarantine tables, then delete them from the active schema."
        ),
    )
    parser.add_argument(
        "--quarantine-root",
        default="runtime/quarantine/runtime_contamination",
        help="Destination root used in generated file-move commands for --quarantine-plan.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when contamination findings are present or the PG audit errors.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.apply_quarantine_plan:
        report = apply_runtime_contamination_quarantine_plan(
            workspace_root=Path(str(args.workspace_root)).expanduser(),
            target_runtime_dir=str(args.target_runtime_dir or "").strip() or None,
            dsn=str(args.dsn or ""),
            schema=str(args.schema or ""),
            quarantine_root=str(args.quarantine_root or "runtime/quarantine/runtime_contamination"),
            sample_limit=max(1, int(args.sample_limit or 25)),
            include_postgres=not bool(args.skip_postgres),
        )
    elif args.quarantine_plan:
        report = build_runtime_contamination_quarantine_plan(
            workspace_root=Path(str(args.workspace_root)).expanduser(),
            target_runtime_dir=str(args.target_runtime_dir or "").strip() or None,
            dsn=str(args.dsn or ""),
            schema=str(args.schema or ""),
            quarantine_root=str(args.quarantine_root or "runtime/quarantine/runtime_contamination"),
            sample_limit=max(1, int(args.sample_limit or 25)),
        )
    else:
        report = build_runtime_contamination_report(
            workspace_root=Path(str(args.workspace_root)).expanduser(),
            target_runtime_dir=str(args.target_runtime_dir or "").strip() or None,
            dsn=str(args.dsn or ""),
            schema=str(args.schema or ""),
            include_postgres=not bool(args.skip_postgres),
            sample_limit=max(1, int(args.sample_limit or 25)),
        )
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if str(args.output_json or "").strip():
        output_path = Path(str(args.output_json)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload + "\n", encoding="utf-8")
    sys.stdout.write(payload + "\n")
    if args.strict and (
        int(report.get("finding_count") or 0) > 0
        or str(dict(report.get("postgres") or {}).get("status") or "") == "error"
        or str(dict(dict(report.get("audit_report") or {}).get("postgres") or {}).get("status") or "") == "error"
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
