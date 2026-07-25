#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from sourcing_agent.regression_matrix import (
    PytestInvocation,
    durations_pytest_args,
    infer_pytest_invocations,
    normalize_changed_paths,
    smoke_pytest_invocations,
)


def _repo_root() -> Path:
    return REPO_ROOT


def _pytest_bin_is_usable(candidate: Path | str) -> bool:
    command = [str(candidate), "--version"]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return False
    return int(completed.returncode or 0) == 0


def _resolve_pytest_bin(repo_root: Path) -> str:
    bundled = repo_root / ".venv-tests" / "bin" / "pytest"
    if bundled.is_file() and _pytest_bin_is_usable(bundled):
        return str(bundled)
    resolved = shutil.which("pytest")
    if resolved and _pytest_bin_is_usable(resolved):
        return resolved
    raise RuntimeError("pytest not found")


def _git_changed_paths(repo_root: Path) -> list[str]:
    tracked = subprocess.run(
        ["git", "-C", str(repo_root), "diff", "--name-only", "--relative", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    untracked = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files", "--others", "--exclude-standard"],
        check=False,
        capture_output=True,
        text=True,
    )
    paths: list[str] = []
    for payload in (tracked.stdout, untracked.stdout):
        for line in payload.splitlines():
            text = str(line or "").strip()
            if text:
                paths.append(text)
    return normalize_changed_paths(paths)


def _run_pytest_invocation(
    repo_root: Path,
    pytest_bin: str,
    invocation: PytestInvocation,
    *,
    extra_pytest_args: list[str],
    dry_run: bool,
) -> dict[str, Any]:
    if invocation.runner == "command":
        command = [*invocation.args]
    else:
        command = [pytest_bin, "-q", *invocation.args, *extra_pytest_args]
    sys.stderr.write(f"[pytest-matrix] {invocation.label}: {invocation.reason}\n")
    sys.stderr.write(f"[pytest-matrix] command: {' '.join(command)}\n")
    if dry_run:
        return {
            "label": invocation.label,
            "reason": invocation.reason,
            "command": command,
            "runner": invocation.runner,
            "cwd": invocation.cwd,
            "exit_code": 0,
            "elapsed_seconds": 0.0,
            "dry_run": True,
        }
    env = dict(os.environ)
    if invocation.runner == "pytest":
        existing_pythonpath = str(env.get("PYTHONPATH") or "").strip()
        env["PYTHONPATH"] = f"src{os.pathsep}{existing_pythonpath}" if existing_pythonpath else "src"
    started_at = time.perf_counter()
    invocation_cwd = repo_root / invocation.cwd if invocation.cwd else repo_root
    completed = subprocess.run(command, cwd=invocation_cwd, env=env, check=False)
    return {
        "label": invocation.label,
        "reason": invocation.reason,
        "command": command,
        "runner": invocation.runner,
        "cwd": str(invocation_cwd),
        "exit_code": int(completed.returncode or 0),
        "elapsed_seconds": round(time.perf_counter() - started_at, 3),
        "dry_run": False,
    }


def _write_report(report_path: str, payload: dict[str, Any]) -> None:
    destination = Path(report_path).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a layered pytest matrix with changed-path selection.")
    parser.add_argument(
        "--mode",
        choices=["changed", "smoke", "full", "durations"],
        default="changed",
        help="Regression mode to run.",
    )
    parser.add_argument(
        "--changed-path",
        action="append",
        default=[],
        help="Explicit changed path override; repeatable. Defaults to git diff/untracked.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print selected commands without executing them.",
    )
    parser.add_argument(
        "--durations-top",
        type=int,
        default=25,
        help="Top N tests to print in durations mode.",
    )
    parser.add_argument(
        "--durations-min-seconds",
        type=float,
        default=0.5,
        help="Minimum test duration reported in durations mode.",
    )
    parser.add_argument(
        "--extra-pytest-arg",
        action="append",
        default=[],
        help="Additional raw pytest arg appended to each invocation; repeatable.",
    )
    parser.add_argument(
        "--report-json",
        default="",
        help="Optional JSON report path capturing selected suites, commands, exit codes, and elapsed seconds.",
    )
    parser.add_argument(
        "--max-parallel",
        type=int,
        default=1,
        help="Maximum number of changed/smoke suites to run in parallel. Values <= 1 keep sequential execution.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = _repo_root()
    pytest_bin = _resolve_pytest_bin(repo_root)
    report_payload: dict[str, Any] = {
        "mode": args.mode,
        "repo_root": str(repo_root),
        "dry_run": bool(args.dry_run),
        "max_parallel": max(1, int(args.max_parallel or 1)),
        "invocations": [],
    }

    if args.mode == "full":
        command = [pytest_bin, "-q", *list(args.extra_pytest_arg or [])]
        sys.stderr.write(f"[pytest-matrix] command: {' '.join(command)}\n")
        report_payload["invocations"] = [
            {
                "label": "full",
                "reason": "full pytest suite",
                "command": command,
            }
        ]
        if args.dry_run:
            if args.report_json:
                _write_report(str(args.report_json), report_payload)
            return 0
        env = dict(os.environ)
        existing_pythonpath = str(env.get("PYTHONPATH") or "").strip()
        env["PYTHONPATH"] = f"src{os.pathsep}{existing_pythonpath}" if existing_pythonpath else "src"
        started_at = time.perf_counter()
        exit_code = int(subprocess.run(command, cwd=repo_root, env=env, check=False).returncode or 0)
        report_payload["invocations"][0]["exit_code"] = exit_code
        report_payload["invocations"][0]["elapsed_seconds"] = round(time.perf_counter() - started_at, 3)
        if args.report_json:
            _write_report(str(args.report_json), report_payload)
        return exit_code

    if args.mode == "durations":
        command = [
            pytest_bin,
            *durations_pytest_args(top=int(args.durations_top), min_seconds=float(args.durations_min_seconds)),
            *list(args.extra_pytest_arg or []),
        ]
        sys.stderr.write(f"[pytest-matrix] command: {' '.join(command)}\n")
        report_payload["invocations"] = [
            {
                "label": "durations",
                "reason": "full pytest durations profile",
                "command": command,
            }
        ]
        if args.dry_run:
            if args.report_json:
                _write_report(str(args.report_json), report_payload)
            return 0
        env = dict(os.environ)
        existing_pythonpath = str(env.get("PYTHONPATH") or "").strip()
        env["PYTHONPATH"] = f"src{os.pathsep}{existing_pythonpath}" if existing_pythonpath else "src"
        started_at = time.perf_counter()
        exit_code = int(subprocess.run(command, cwd=repo_root, env=env, check=False).returncode or 0)
        report_payload["invocations"][0]["exit_code"] = exit_code
        report_payload["invocations"][0]["elapsed_seconds"] = round(time.perf_counter() - started_at, 3)
        if args.report_json:
            _write_report(str(args.report_json), report_payload)
        return exit_code

    if args.mode == "smoke":
        invocations = smoke_pytest_invocations()
    else:
        changed_paths = (
            normalize_changed_paths(args.changed_path) if args.changed_path else _git_changed_paths(repo_root)
        )
        sys.stderr.write(f"[pytest-matrix] changed-paths: {', '.join(changed_paths) if changed_paths else '<none>'}\n")
        report_payload["changed_paths"] = changed_paths
        invocations = infer_pytest_invocations(changed_paths, repo_root=repo_root)

    exit_code = 0
    max_parallel = max(1, int(args.max_parallel or 1))
    if max_parallel == 1 or len(invocations) <= 1 or args.dry_run:
        for invocation in invocations:
            result = _run_pytest_invocation(
                repo_root,
                pytest_bin,
                invocation,
                extra_pytest_args=list(args.extra_pytest_arg or []),
                dry_run=bool(args.dry_run),
            )
            report_payload["invocations"].append(result)
            exit_code = int(result.get("exit_code") or 0)
            if exit_code != 0:
                break
    else:
        ordered_results: list[dict[str, Any] | None] = [None] * len(invocations)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as executor:
            future_map = {
                executor.submit(
                    _run_pytest_invocation,
                    repo_root,
                    pytest_bin,
                    invocation,
                    extra_pytest_args=list(args.extra_pytest_arg or []),
                    dry_run=False,
                ): index
                for index, invocation in enumerate(invocations)
            }
            for future in concurrent.futures.as_completed(future_map):
                index = future_map[future]
                result = future.result()
                ordered_results[index] = result
                if exit_code == 0 and int(result.get("exit_code") or 0) != 0:
                    exit_code = int(result.get("exit_code") or 0)
        report_payload["invocations"] = [result for result in ordered_results if result is not None]
    if args.report_json:
        _write_report(str(args.report_json), report_payload)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
