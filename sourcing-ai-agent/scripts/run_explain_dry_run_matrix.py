#!/usr/bin/env python3
from __future__ import annotations

import argparse
from contextlib import nullcontext
import json
import sys

from sourcing_agent.scripted_test_runtime import (
    FAST_HOSTED_TEST_ENV,
    isolated_hosted_test_runtime,
)
from sourcing_agent.workflow_explain_matrix import (
    load_explain_cases,
    run_hosted_explain_matrix,
)
from sourcing_agent.workflow_smoke import HostedWorkflowSmokeClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a lightweight explain/dry-run matrix against the workflow API.",
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8765", help="Hosted API base URL.")
    parser.add_argument(
        "--runtime-dir",
        default="",
        help="Optional isolated runtime dir. When set, the script starts its own in-process backend against this runtime.",
    )
    parser.add_argument(
        "--runtime-env-file",
        default="",
        help=(
            "Optional local-postgres env file for the isolated runtime. "
            "When omitted, the script writes an empty sentinel env file so it does not inherit repo-level PG config."
        ),
    )
    parser.add_argument(
        "--seed-reference-runtime",
        action="store_true",
        help="Seed the isolated runtime with deterministic reference smoke assets before starting the backend.",
    )
    parser.add_argument(
        "--provider-mode",
        default="simulate",
        choices=("simulate", "scripted", "replay", "live"),
        help="External provider mode used only when --runtime-dir starts an isolated backend.",
    )
    parser.add_argument(
        "--scripted-scenario",
        default="",
        help="Optional scripted provider scenario JSON used when --runtime-dir is active.",
    )
    parser.add_argument(
        "--fast-runtime",
        action="store_true",
        help="Inject zero-cooldown runtime knobs into the isolated backend for faster scripted testing.",
    )
    parser.add_argument("--matrix-file", default="", help="Optional JSON file with top-level `cases`.")
    parser.add_argument("--case", action="append", default=[], help="Run only the named case; repeatable.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if any case returns mismatches or raises an exception.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.seed_reference_runtime and not str(args.runtime_dir or "").strip():
        raise SystemExit("--seed-reference-runtime requires --runtime-dir")
    selected_cases = {str(item).strip() for item in args.case if str(item).strip()}
    try:
        cases = load_explain_cases(args.matrix_file, selected_cases)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    runtime_context = nullcontext()
    if str(args.runtime_dir or "").strip():
        runtime_context = isolated_hosted_test_runtime(
            runtime_dir=str(args.runtime_dir),
            runtime_env_file=str(args.runtime_env_file or ""),
            provider_mode=str(args.provider_mode or "simulate"),
            scripted_scenario=str(args.scripted_scenario or ""),
            seed_reference_runtime=bool(args.seed_reference_runtime),
            extra_env=FAST_HOSTED_TEST_ENV if args.fast_runtime else {},
        )
    with runtime_context as isolated_runtime:
        client = HostedWorkflowSmokeClient(
            isolated_runtime.base_url if isolated_runtime is not None else str(args.base_url)
        )
        summaries, failures = run_hosted_explain_matrix(client, cases=cases)
    json.dump(summaries, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    if args.strict and failures:
        sys.stderr.write(f"explain dry-run failures: {', '.join(failures)}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
