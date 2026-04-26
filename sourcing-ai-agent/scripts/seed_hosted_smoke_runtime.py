#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from contextlib import contextmanager
from pathlib import Path

from sourcing_agent.local_postgres import resolve_default_control_plane_db_path
from sourcing_agent.smoke_runtime_seed import seed_reference_smoke_runtime
from sourcing_agent.storage import ControlPlaneStore


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
        help="Optional compatibility-shadow path override. Defaults to the runtime's resolved control-plane db path.",
    )
    return parser.parse_args()


@contextmanager
def _postgres_simulate_environment():
    keys = {
        "SOURCING_RUNTIME_ENVIRONMENT": "simulate",
        "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
        "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "1",
        "SOURCING_PG_ONLY_SQLITE_BACKEND": "shared_memory",
    }
    previous = {key: os.getenv(key) for key in keys}
    os.environ.update(keys)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def main() -> int:
    args = parse_args()
    runtime_dir = Path(str(args.runtime_dir or "")).expanduser().resolve()
    db_path = (
        Path(str(args.db_path or "")).expanduser().resolve()
        if str(args.db_path or "").strip()
        else resolve_default_control_plane_db_path(runtime_dir, base_dir=runtime_dir).resolve()
    )
    runtime_dir.mkdir(parents=True, exist_ok=True)
    with _postgres_simulate_environment():
        store = ControlPlaneStore(db_path)
    result = seed_reference_smoke_runtime(runtime_dir=runtime_dir, store=store)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
