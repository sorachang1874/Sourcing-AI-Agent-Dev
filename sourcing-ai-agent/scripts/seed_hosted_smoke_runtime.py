#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from contextlib import contextmanager
from pathlib import Path

from sourcing_agent.local_postgres import resolve_default_control_plane_db_path
from sourcing_agent.smoke_runtime_seed import seed_reference_smoke_runtime
from sourcing_agent.storage import SQLiteStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed a runtime with the baseline assets required by standalone hosted simulate smoke flows.",
    )
    parser.add_argument(
        "--runtime-dir",
        default="runtime/simulate_smoke",
        help="Target runtime root that should receive smoke baseline assets.",
    )
    parser.add_argument(
        "--db-path",
        default="",
        help="Optional sqlite/shadow path override. Defaults to the runtime's resolved control-plane db path.",
    )
    return parser.parse_args()


@contextmanager
def _disabled_live_postgres():
    original = os.getenv("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE")
    os.environ["SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE"] = "disabled"
    try:
        yield
    finally:
        if original is None:
            os.environ.pop("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE", None)
        else:
            os.environ["SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE"] = original


def main() -> int:
    args = parse_args()
    runtime_dir = Path(str(args.runtime_dir or "")).expanduser().resolve()
    db_path = (
        Path(str(args.db_path or "")).expanduser().resolve()
        if str(args.db_path or "").strip()
        else resolve_default_control_plane_db_path(runtime_dir, base_dir=runtime_dir).resolve()
    )
    runtime_dir.mkdir(parents=True, exist_ok=True)
    with _disabled_live_postgres():
        store = SQLiteStore(db_path)
    result = seed_reference_smoke_runtime(runtime_dir=runtime_dir, store=store)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
