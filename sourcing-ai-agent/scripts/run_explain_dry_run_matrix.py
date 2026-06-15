#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from sourcing_agent.scripted_test_runtime import (
    FAST_HOSTED_TEST_ENV,
    isolated_hosted_test_runtime,
)
from sourcing_agent.workflow_explain_matrix import (
    load_explain_cases,
    run_hosted_explain_matrix,
)
from sourcing_agent.workflow_smoke import HostedWorkflowSmokeClient


def _normalized_runtime_env(case: dict[str, object]) -> dict[str, str]:
    return {
        str(key).strip(): str(value)
        for key, value in dict(dict(case).get("runtime_env") or {}).items()
        if str(key).strip()
    }


def _normalized_case_scripted_scenario(case: dict[str, object], *, explicit_scripted_scenario: str = "") -> str:
    if str(explicit_scripted_scenario or "").strip():
        return str(Path(str(explicit_scripted_scenario)).expanduser().resolve())
    scenario = str(dict(case).get("scripted_scenario") or "").strip()
    if not scenario:
        return ""
    return str(Path(scenario).expanduser().resolve())


def _isolated_runtime_groups(
    cases: list[dict[str, object]],
    *,
    explicit_scripted_scenario: str = "",
) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    group_indexes: dict[tuple[str, tuple[tuple[str, str], ...]], int] = {}
    for case in list(cases or []):
        runtime_env = _normalized_runtime_env(case)
        scripted_scenario = _normalized_case_scripted_scenario(
            case,
            explicit_scripted_scenario=explicit_scripted_scenario,
        )
        signature = (scripted_scenario, tuple(sorted(runtime_env.items())))
        group_index = group_indexes.get(signature)
        if group_index is None:
            group_index = len(groups)
            group_indexes[signature] = group_index
            groups.append({"scripted_scenario": scripted_scenario, "runtime_env": runtime_env, "cases": []})
        groups[group_index]["cases"].append(dict(case))
    return groups


def _isolated_group_runtime_dir(
    base_runtime_dir: str,
    *,
    group_index: int,
    total_groups: int,
    group: dict[str, Any],
) -> str:
    if total_groups <= 1:
        return str(base_runtime_dir)
    base_path = Path(str(base_runtime_dir)).expanduser()
    case_names = [
        str(dict(item).get("case") or "").strip()
        for item in list(group.get("cases") or [])
        if str(dict(item).get("case") or "").strip()
    ]
    scenario_label = (
        Path(str(group.get("scripted_scenario") or "")).stem
        if str(group.get("scripted_scenario") or "").strip()
        else "default"
    )
    case_label = case_names[0] if case_names else f"group-{group_index + 1}"
    safe_label = "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in f"{group_index + 1:02d}_{case_label}_{scenario_label}"
    ).strip("_")
    return str(base_path / safe_label)


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
            "When omitted, the script generates a runtime-scoped PG-only env file from local Postgres config."
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
    if str(args.runtime_dir or "").strip():
        summaries: list[dict[str, Any]] = []
        failures: list[str] = []
        groups = _isolated_runtime_groups(
            cases,
            explicit_scripted_scenario=str(args.scripted_scenario or ""),
        )
        for group_index, group in enumerate(groups):
            runtime_extra_env = dict(FAST_HOSTED_TEST_ENV) if args.fast_runtime else {}
            runtime_extra_env.update(dict(group.get("runtime_env") or {}))
            runtime_dir = _isolated_group_runtime_dir(
                str(args.runtime_dir),
                group_index=group_index,
                total_groups=len(groups),
                group=group,
            )
            with isolated_hosted_test_runtime(
                runtime_dir=runtime_dir,
                runtime_env_file=str(args.runtime_env_file or ""),
                provider_mode=str(args.provider_mode or "simulate"),
                scripted_scenario=str(group.get("scripted_scenario") or ""),
                seed_reference_runtime=bool(args.seed_reference_runtime),
                extra_env=runtime_extra_env,
            ) as isolated_runtime:
                client = HostedWorkflowSmokeClient(isolated_runtime.base_url)
                group_summaries, group_failures = run_hosted_explain_matrix(
                    client,
                    cases=list(group.get("cases") or []),
                )
                summaries.extend(group_summaries)
                failures.extend(group_failures)
    else:
        with nullcontext() as _:
            client = HostedWorkflowSmokeClient(str(args.base_url))
            summaries, failures = run_hosted_explain_matrix(client, cases=cases)
    json.dump(summaries, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    if args.strict and failures:
        sys.stderr.write(f"explain dry-run failures: {', '.join(failures)}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
