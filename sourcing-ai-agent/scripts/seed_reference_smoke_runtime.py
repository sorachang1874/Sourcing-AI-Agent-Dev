#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from sourcing_agent.scripted_test_runtime import (
    FAST_HOSTED_TEST_ENV,
    build_isolated_runtime_env,
    patched_environment,
    prepare_workflow_confidence_postgres_schema,
)
from sourcing_agent.smoke_runtime_seed import seed_reference_smoke_runtime


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Seed the isolated reference smoke runtime used by simulate/scripted "
            "interactive tests. This writes only into the requested runtime dir."
        )
    )
    parser.add_argument("--runtime-dir", default="runtime/test_env", help="Isolated runtime dir to seed.")
    parser.add_argument(
        "--provider-mode",
        default="scripted",
        choices=("simulate", "scripted", "replay", "live"),
        help="Provider mode for the isolated runtime environment.",
    )
    parser.add_argument(
        "--scripted-scenario",
        default="",
        help="Optional scripted provider scenario JSON path.",
    )
    parser.add_argument(
        "--runtime-environment",
        default="",
        help="Optional SOURCING_RUNTIME_ENVIRONMENT override. Defaults to provider-mode-derived test env.",
    )
    parser.add_argument(
        "--runtime-env-file",
        default="",
        help="Optional runtime-scoped local-postgres env file. Omit to generate a PG-only env file.",
    )
    parser.add_argument(
        "--fast-runtime",
        action="store_true",
        help="Apply fast scripted smoke cooldown/sleep settings while seeding.",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional path to write the seed result JSON.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    runtime_dir = Path(args.runtime_dir).expanduser().resolve()
    scenario = str(args.scripted_scenario or "").strip()
    extra_env: dict[str, str] = {}
    if args.fast_runtime:
        extra_env.update(FAST_HOSTED_TEST_ENV)
    if str(args.runtime_environment or "").strip():
        extra_env["SOURCING_RUNTIME_ENVIRONMENT"] = str(args.runtime_environment).strip()
    env_payload, _env_file = build_isolated_runtime_env(
        runtime_dir=runtime_dir,
        runtime_env_file=args.runtime_env_file,
        provider_mode=args.provider_mode,
        scripted_scenario=scenario,
        extra_env=extra_env,
    )
    prepare_workflow_confidence_postgres_schema(env_payload)
    with patched_environment(env_payload):
        result = seed_reference_smoke_runtime(runtime_dir=runtime_dir)
    payload = {
        "status": "seeded",
        "runtime_dir": str(runtime_dir),
        "provider_mode": args.provider_mode,
        "scripted_scenario": str(Path(scenario).expanduser().resolve()) if scenario else "",
        "seed": result,
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if str(args.output_json or "").strip():
        output_path = Path(args.output_json).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
