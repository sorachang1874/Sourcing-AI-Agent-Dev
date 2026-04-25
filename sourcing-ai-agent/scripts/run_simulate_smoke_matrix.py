#!/usr/bin/env python3
from __future__ import annotations

import argparse
from contextlib import nullcontext
import json
from pathlib import Path
import sys

from sourcing_agent.scripted_test_runtime import (
    FAST_HOSTED_TEST_ENV,
    isolated_hosted_test_runtime,
)
from sourcing_agent.workflow_smoke import (
    HostedWorkflowSmokeClient,
    load_smoke_cases,
    run_hosted_smoke_matrix,
    summarize_smoke_timings,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a hosted simulate-mode smoke matrix against the workflow API. "
            "For standalone servers, seed baseline assets first with scripts/seed_hosted_smoke_runtime.py, "
            "or use --runtime-dir to spin up an isolated in-process backend."
        ),
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
    parser.add_argument("--reviewer", default="simulate-smoke", help="Reviewer name used for auto-approval.")
    parser.add_argument("--poll-seconds", type=float, default=1.0, help="Polling interval for progress.")
    parser.add_argument("--max-poll-seconds", type=float, default=90.0, help="Per-case timeout window.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if any case ends non-completed or raises an exception.",
    )
    parser.add_argument(
        "--no-auto-continue-stage2",
        action="store_true",
        help="Do not auto-call continue-stage2 when the workflow pauses after stage 1 preview.",
    )
    parser.add_argument(
        "--runtime-tuning-profile",
        default="",
        help="Optional job-scoped runtime tuning profile, for example `fast_smoke`.",
    )
    parser.add_argument(
        "--timing-summary",
        action="store_true",
        help="Also print an aggregate timings summary to stderr.",
    )
    parser.add_argument(
        "--report-json",
        default="",
        help="Optional path to persist the full per-case smoke report JSON.",
    )
    parser.add_argument(
        "--summary-json",
        default="",
        help="Optional path to persist the aggregate timing/observability summary JSON.",
    )
    parser.add_argument(
        "--max-total-ms",
        type=float,
        default=0.0,
        help="Optional regression guard for aggregate p95 total runtime; 0 disables the check.",
    )
    parser.add_argument(
        "--max-wait-ms",
        type=float,
        default=0.0,
        help="Optional regression guard for aggregate p95 wait_for_completion runtime; 0 disables the check.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.seed_reference_runtime and not str(args.runtime_dir or "").strip():
        raise SystemExit("--seed-reference-runtime requires --runtime-dir")
    selected_cases = {str(item).strip() for item in args.case if str(item).strip()}
    try:
        cases = load_smoke_cases(args.matrix_file, selected_cases)
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
        summaries, failures = run_hosted_smoke_matrix(
            client=client,
            cases=cases,
            reviewer=str(args.reviewer),
            poll_seconds=float(args.poll_seconds),
            max_poll_seconds=float(args.max_poll_seconds),
            auto_continue_stage2=not bool(args.no_auto_continue_stage2),
            runtime_tuning_profile=str(args.runtime_tuning_profile or ""),
        )
    json.dump(summaries, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    timing_summary = summarize_smoke_timings(summaries)
    if str(args.report_json or "").strip():
        report_path = Path(str(args.report_json)).expanduser()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(summaries, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if str(args.summary_json or "").strip():
        summary_path = Path(str(args.summary_json)).expanduser()
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(timing_summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.timing_summary:
        json.dump(timing_summary, sys.stderr, ensure_ascii=False, indent=2)
        sys.stderr.write("\n")
    total_p95 = float(dict(dict(timing_summary.get("timings_ms") or {}).get("total") or {}).get("p95") or 0.0)
    wait_p95 = float(
        dict(dict(timing_summary.get("timings_ms") or {}).get("wait_for_completion") or {}).get("p95") or 0.0
    )
    if args.strict and failures:
        sys.stderr.write(f"simulate smoke failures: {', '.join(failures)}\n")
        return 1
    if float(args.max_total_ms or 0.0) > 0 and total_p95 > float(args.max_total_ms):
        sys.stderr.write(
            f"simulate smoke total p95 regression: {round(total_p95, 2)}ms > {float(args.max_total_ms):.2f}ms\n"
        )
        return 1
    if float(args.max_wait_ms or 0.0) > 0 and wait_p95 > float(args.max_wait_ms):
        sys.stderr.write(
            f"simulate smoke wait_for_completion p95 regression: {round(wait_p95, 2)}ms > {float(args.max_wait_ms):.2f}ms\n"
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
