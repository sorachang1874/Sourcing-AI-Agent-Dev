#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path
from typing import Any

from sourcing_agent.local_postgres import resolve_control_plane_postgres_dsn, resolve_control_plane_postgres_schema
from sourcing_agent.runtime_contamination_audit import (
    audit_active_runtime_daemons,
    build_runtime_contamination_report,
    outer_runtime_root_for_path,
)
from sourcing_agent.runtime_environment import NON_LIVE_PROVIDER_MODES, normalize_provider_mode
from sourcing_agent.scripted_test_runtime import (
    FAST_HOSTED_TEST_ENV,
    isolated_hosted_test_runtime,
)
from sourcing_agent.settings import load_settings
from sourcing_agent.workflow_smoke import (
    HostedWorkflowSmokeClient,
    load_smoke_cases,
    run_hosted_smoke_matrix,
    summarize_smoke_timings,
)


class SmokeMatrixShutdownRequested(Exception):
    """Raised from signal handlers so isolated runtime cleanup can run."""


_MAX_BACKGROUND_RECONCILE_REPORT_BYTES = 100_000
_BACKGROUND_RECONCILE_SUMMARY_KEYS = {
    "applied_worker_count",
    "candidate_count",
    "completed_count",
    "deferred_url_count",
    "dispatched_url_count",
    "failed_count",
    "fetched_profile_count",
    "missing_visible_count",
    "pending_worker_count",
    "profile_materialized_candidate_record_count",
    "queued_worker_count",
    "ready_url_count",
    "reason",
    "requested_url_count",
    "snapshot_id",
    "source",
    "status",
    "target_visible_count",
    "total_url_count",
    "visible_count",
}


def _install_shutdown_signal_handlers() -> None:
    def _handle_shutdown_signal(signum: int, _frame: Any) -> None:
        signal_name = signal.Signals(signum).name
        raise SmokeMatrixShutdownRequested(f"received {signal_name}")

    for signal_name in ("SIGTERM", "SIGINT"):
        signum = getattr(signal, signal_name, None)
        if signum is not None:
            signal.signal(signum, _handle_shutdown_signal)


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
    parser.add_argument(
        "--live-model-planning",
        action="store_true",
        help=(
            "When used with --runtime-dir --provider-mode scripted, allow the front-door planning model "
            "request-normalization path to call the configured live LLM while external data providers remain scripted."
        ),
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


def _live_model_planning_env_from_current_settings() -> dict[str, str]:
    settings = load_settings(Path.cwd())
    env_payload: dict[str, str] = {}
    if settings.qwen.enabled and settings.qwen.api_key:
        env_payload.update(
            {
                "DASHSCOPE_API_KEY": settings.qwen.api_key,
                "DASHSCOPE_BASE_URL": settings.qwen.base_url,
                "DASHSCOPE_MODEL": settings.qwen.model,
                "DASHSCOPE_TIMEOUT_SECONDS": str(settings.qwen.timeout_seconds),
            }
        )
    if settings.model_provider.enabled and settings.model_provider.api_key:
        env_payload.update(
            {
                "MODEL_PROVIDER_API_KEY": settings.model_provider.api_key,
                "MODEL_PROVIDER_BASE_URL": settings.model_provider.base_url,
                "MODEL_PROVIDER_MODEL": settings.model_provider.model,
                "MODEL_PROVIDER_NAME": settings.model_provider.provider_name,
                "MODEL_PROVIDER_API_STYLE": settings.model_provider.api_style,
                "MODEL_PROVIDER_TIMEOUT_SECONDS": str(settings.model_provider.timeout_seconds),
            }
        )
    return {key: value for key, value in env_payload.items() if str(value or "").strip()}


def _runtime_env_from_cases(cases: list[dict[str, object]]) -> dict[str, str]:
    env_payload: dict[str, str] = {}
    conflicts: list[str] = []
    for case in list(cases or []):
        case_name = str(dict(case).get("case") or "").strip()
        for key, value in dict(dict(case).get("runtime_env") or {}).items():
            normalized_key = str(key or "").strip()
            if not normalized_key:
                continue
            normalized_value = str(value)
            existing = env_payload.get(normalized_key)
            if existing is not None and existing != normalized_value:
                conflicts.append(f"{normalized_key}:{case_name}")
                continue
            env_payload[normalized_key] = normalized_value
    if conflicts:
        raise SystemExit(
            "conflicting runtime_env values across selected smoke cases: " + ", ".join(sorted(conflicts))
        )
    return env_payload


def _normalized_runtime_env(case: dict[str, object]) -> dict[str, str]:
    payload = dict(dict(case).get("runtime_env") or {})
    return {
        str(key).strip(): str(value)
        for key, value in payload.items()
        if str(key).strip()
    }


def _normalized_case_scripted_scenario(case: dict[str, object], *, explicit_scripted_scenario: str = "") -> str:
    if str(explicit_scripted_scenario or "").strip():
        return str(Path(str(explicit_scripted_scenario)).expanduser().resolve())
    scenario = str(dict(case).get("scripted_scenario") or "").strip()
    if not scenario:
        return ""
    return str(Path(scenario).expanduser().resolve())


def _runtime_env_requests_reference_seed(runtime_env: dict[str, str]) -> bool:
    return any(
        str(key or "").strip().startswith("SOURCING_SEED_")
        and str(value or "").strip().lower() in {"1", "true", "yes", "on"}
        for key, value in dict(runtime_env or {}).items()
    )


def _case_requests_reference_seed(case: dict[str, object]) -> bool:
    return bool(dict(case or {}).get("seed_reference_runtime")) or _runtime_env_requests_reference_seed(
        _normalized_runtime_env(case)
    )


def _case_requests_runtime_isolation(case: dict[str, object]) -> bool:
    payload = dict(dict(case or {}).get("payload") or {})
    execution_preferences = dict(payload.get("execution_preferences") or {})
    review_decision = dict(dict(case or {}).get("review_decision") or {})
    isolation = str(dict(case or {}).get("runtime_isolation") or "").strip().lower()
    return bool(
        isolation in {"case", "isolated", "force"}
        or payload.get("force_fresh_run")
        or execution_preferences.get("force_fresh_run")
        or review_decision.get("force_fresh_run")
    )


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
        signature = (
            scripted_scenario,
            tuple(sorted(runtime_env.items())),
            _case_requests_reference_seed(case),
        )
        if _case_requests_runtime_isolation(case):
            signature = (
                scripted_scenario,
                tuple(sorted(runtime_env.items())),
                str(dict(case).get("case") or "").strip(),
            )
        group_index = group_indexes.get(signature)
        if group_index is None:
            group_index = len(groups)
            group_indexes[signature] = group_index
            groups.append(
                {
                    "scripted_scenario": scripted_scenario,
                    "runtime_env": runtime_env,
                    "seed_reference_runtime": _case_requests_reference_seed(case),
                    "cases": [],
                }
            )
        if _case_requests_reference_seed(case):
            groups[group_index]["seed_reference_runtime"] = True
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


def _run_smoke_cases(
    args: argparse.Namespace,
    *,
    cases: list[dict[str, object]],
) -> tuple[list[dict[str, Any]], list[str]]:
    summaries: list[dict[str, Any]] = []
    failures: list[str] = []
    if str(args.runtime_dir or "").strip():
        groups = _isolated_runtime_groups(
            cases,
            explicit_scripted_scenario=str(getattr(args, "scripted_scenario", "") or ""),
        )
        for group_index, group in enumerate(groups):
            runtime_extra_env = dict(FAST_HOSTED_TEST_ENV) if args.fast_runtime else {}
            runtime_extra_env.update(dict(group.get("runtime_env") or {}))
            if args.live_model_planning:
                runtime_extra_env.update(_live_model_planning_env_from_current_settings())
                runtime_extra_env["SOURCING_SCRIPTED_LIVE_MODEL_PLANNING"] = "1"
            runtime_dir = _isolated_group_runtime_dir(
                str(args.runtime_dir),
                group_index=group_index,
                total_groups=len(groups),
                group=group,
            )
            with isolated_hosted_test_runtime(
                runtime_dir=runtime_dir,
                runtime_env_file=str(getattr(args, "runtime_env_file", "") or ""),
                provider_mode=str(getattr(args, "provider_mode", "simulate") or "simulate"),
                scripted_scenario=str(group.get("scripted_scenario") or ""),
                seed_reference_runtime=bool(args.seed_reference_runtime)
                or bool(group.get("seed_reference_runtime")),
                extra_env=runtime_extra_env,
            ) as isolated_runtime:
                client = HostedWorkflowSmokeClient(isolated_runtime.base_url)
                group_summaries, group_failures = run_hosted_smoke_matrix(
                    client=client,
                    cases=list(group.get("cases") or []),
                    reviewer=str(args.reviewer),
                    poll_seconds=float(args.poll_seconds),
                    max_poll_seconds=float(args.max_poll_seconds),
                    auto_continue_stage2=not bool(args.no_auto_continue_stage2),
                    runtime_tuning_profile=str(args.runtime_tuning_profile or ""),
                )
                summaries.extend(group_summaries)
                failures.extend(group_failures)
        return summaries, failures
    client = HostedWorkflowSmokeClient(str(args.base_url))
    return run_hosted_smoke_matrix(
        client=client,
        cases=cases,
        reviewer=str(args.reviewer),
        poll_seconds=float(args.poll_seconds),
        max_poll_seconds=float(args.max_poll_seconds),
        auto_continue_stage2=not bool(args.no_auto_continue_stage2),
        runtime_tuning_profile=str(args.runtime_tuning_profile or ""),
    )


def _contamination_runtime_scope(
    *,
    runtime_dir: str | Path,
    runtime_env_file: str | Path,
    provider_mode: str,
) -> tuple[Path, str, str]:
    del runtime_env_file, provider_mode
    resolved_runtime_dir = Path(runtime_dir).expanduser()
    return (
        resolved_runtime_dir,
        str(resolve_control_plane_postgres_dsn(Path.cwd()) or "").strip(),
        str(resolve_control_plane_postgres_schema(Path.cwd()) or "").strip(),
    )


def _runtime_contamination_groups(
    args: argparse.Namespace,
    *,
    cases: list[dict[str, object]] | None,
) -> list[dict[str, Any]]:
    groups = _isolated_runtime_groups(
        list(cases or []),
        explicit_scripted_scenario=str(getattr(args, "scripted_scenario", "") or ""),
    )
    if groups:
        return groups
    return [
        {
            "scripted_scenario": str(getattr(args, "scripted_scenario", "") or "").strip(),
            "runtime_env": {},
            "cases": [],
        }
    ]


def _runtime_contamination_preflight(
    args: argparse.Namespace,
    *,
    cases: list[dict[str, object]] | None = None,
) -> None:
    if not str(args.runtime_dir or "").strip():
        return
    groups = _runtime_contamination_groups(args, cases=cases)
    for group_index, group in enumerate(groups):
        runtime_dir = _isolated_group_runtime_dir(
            str(args.runtime_dir),
            group_index=group_index,
            total_groups=len(groups),
            group=group,
        )
        runtime_root = Path(runtime_dir).expanduser()
        root_runtime = outer_runtime_root_for_path(runtime_root)
        daemon_report = audit_active_runtime_daemons(runtime_root=root_runtime)
        if str(daemon_report.get("status") or "") == "blocked":
            raise SystemExit(
                "runtime contamination preflight failed: active runtime daemons detected: "
                + json.dumps(daemon_report.get("findings") or [], ensure_ascii=False)
            )
        runtime_dir_path, dsn, schema = _contamination_runtime_scope(
            runtime_dir=runtime_root,
            runtime_env_file=str(getattr(args, "runtime_env_file", "") or ""),
            provider_mode=str(getattr(args, "provider_mode", "simulate") or "simulate"),
        )
        contamination_report = build_runtime_contamination_report(
            workspace_root=Path.cwd(),
            target_runtime_dir=runtime_dir_path,
            provider_cache_runtime_root=runtime_dir_path,
            dsn=dsn,
            schema=schema,
            include_postgres=True,
            sample_limit=3,
        )
        if str(contamination_report.get("status") or "") != "clean":
            raise SystemExit(
                "runtime contamination preflight failed: "
                f"{contamination_report.get('finding_count') or 0} findings in "
                f"{contamination_report.get('runtime_root')}: "
                + json.dumps(contamination_report, ensure_ascii=False)
            )


def _runtime_contamination_postflight(
    args: argparse.Namespace,
    *,
    cases: list[dict[str, object]] | None = None,
) -> None:
    if not str(args.runtime_dir or "").strip():
        return
    groups = _runtime_contamination_groups(args, cases=cases)
    for group_index, group in enumerate(groups):
        runtime_dir = _isolated_group_runtime_dir(
            str(args.runtime_dir),
            group_index=group_index,
            total_groups=len(groups),
            group=group,
        )
        runtime_root = Path(runtime_dir).expanduser()
        root_runtime = outer_runtime_root_for_path(runtime_root)
        daemon_report = audit_active_runtime_daemons(runtime_root=root_runtime)
        if str(daemon_report.get("status") or "") == "blocked":
            raise SystemExit(
                "runtime contamination postflight failed: active runtime daemons detected: "
                + json.dumps(daemon_report.get("findings") or [], ensure_ascii=False)
            )
        runtime_dir_path, dsn, schema = _contamination_runtime_scope(
            runtime_dir=runtime_root,
            runtime_env_file=str(getattr(args, "runtime_env_file", "") or ""),
            provider_mode=str(getattr(args, "provider_mode", "simulate") or "simulate"),
        )
        contamination_report = build_runtime_contamination_report(
            workspace_root=Path.cwd(),
            target_runtime_dir=runtime_dir_path,
            provider_cache_runtime_root=runtime_dir_path,
            dsn=dsn,
            schema=schema,
            include_postgres=True,
            sample_limit=3,
        )
        if str(contamination_report.get("status") or "") != "clean":
            raise SystemExit(
                "runtime contamination postflight failed: "
                f"{contamination_report.get('finding_count') or 0} findings in "
                f"{contamination_report.get('runtime_root')}: "
                + json.dumps(contamination_report, ensure_ascii=False)
            )


def _validate_smoke_provider_invocation_modes(
    summaries: list[dict[str, Any]],
    *,
    provider_mode: str,
) -> list[str]:
    normalized_provider_mode = normalize_provider_mode(provider_mode)
    if normalized_provider_mode not in NON_LIVE_PROVIDER_MODES:
        return []
    failures: list[str] = []
    for record in list(summaries or []):
        case_name = str(dict(record).get("case") or "<unknown>")
        for index, invocation in enumerate(list(dict(record).get("provider_invocations") or [])):
            if not isinstance(invocation, dict):
                continue
            invocation_provider_mode = str(invocation.get("provider_mode") or "").strip().lower()
            if not invocation_provider_mode:
                failures.append(f"{case_name}: provider_invocations[{index}].provider_mode missing")
            elif normalize_provider_mode(invocation_provider_mode) == "live":
                failures.append(f"{case_name}: provider_invocations[{index}].provider_mode=live")
    return failures


def _write_json_file(path: str | Path, payload: Any) -> Path:
    output_path = Path(str(path)).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return output_path


def _json_report_size(value: Any) -> int:
    try:
        return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
    except (TypeError, ValueError):
        return len(str(value))


def _summarize_background_reconcile_branch(value: Any, *, depth: int = 0) -> Any:
    if isinstance(value, list):
        summary: dict[str, Any] = {"count": len(value)}
        if value and depth < 2:
            summary["sample"] = [
                _summarize_background_reconcile_branch(item, depth=depth + 1)
                for item in value[:3]
            ]
        return summary
    if not isinstance(value, dict):
        return value
    source = dict(value)
    summary: dict[str, Any] = {}
    for key in sorted(_BACKGROUND_RECONCILE_SUMMARY_KEYS):
        if key in source:
            summary[key] = source.get(key)
    for key, nested in source.items():
        if key in summary:
            continue
        if isinstance(nested, dict):
            nested_summary = _summarize_background_reconcile_branch(nested, depth=depth + 1)
            if isinstance(nested_summary, dict) and nested_summary:
                summary[str(key)] = nested_summary
        elif isinstance(nested, list):
            summary[str(key)] = {"count": len(nested)}
        if depth >= 2 and len(summary) >= 20:
            break
    return summary


def _compact_background_reconcile_for_report(value: Any) -> Any:
    raw_size = _json_report_size(value)
    if raw_size <= _MAX_BACKGROUND_RECONCILE_REPORT_BYTES:
        return value
    return {
        "status": "report_compacted",
        "reason": "background_reconcile_raw_payload_omitted_from_smoke_report",
        "original_bytes": raw_size,
        "summary": _summarize_background_reconcile_branch(value),
    }


def _compact_smoke_report_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    for record in list(records or []):
        cloned = dict(record or {})
        job_summary = dict(cloned.get("job_summary") or {})
        if "background_reconcile" in job_summary:
            job_summary["background_reconcile"] = _compact_background_reconcile_for_report(
                job_summary.get("background_reconcile")
            )
            cloned["job_summary"] = job_summary
        final = dict(cloned.get("final") or {})
        if "background_reconcile" in final:
            final["background_reconcile"] = _compact_background_reconcile_for_report(
                final.get("background_reconcile")
            )
            cloned["final"] = final
        compacted.append(cloned)
    return compacted


def _compact_case_stdout_record(record: dict[str, Any]) -> dict[str, Any]:
    final = dict(record.get("final") or {})
    start = dict(record.get("start") or {})
    return {
        "case": str(record.get("case") or ""),
        "job_id": str(record.get("job_id") or final.get("job_id") or start.get("job_id") or ""),
        "status": str(
            record.get("status")
            or final.get("status")
            or final.get("job_status")
            or start.get("status")
            or ""
        ),
        "expectation_failures": list(record.get("expectation_failures") or []),
    }


def _write_stdout_report(
    *,
    summaries: list[dict[str, Any]],
    timing_summary: dict[str, Any],
    report_json: str = "",
    summary_json: str = "",
) -> None:
    if str(report_json or "").strip():
        payload = {
            "case_count": len(summaries),
            "report_json": str(Path(str(report_json)).expanduser()),
            "summary_json": str(Path(str(summary_json)).expanduser()) if str(summary_json or "").strip() else "",
            "cases": [_compact_case_stdout_record(dict(record)) for record in list(summaries or [])],
            "timings_ms": dict(timing_summary.get("timings_ms") or {}),
        }
        json.dump(payload, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
        return
    json.dump(summaries, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")


def main() -> int:
    _install_shutdown_signal_handlers()
    args = parse_args()
    try:
        if args.seed_reference_runtime and not str(args.runtime_dir or "").strip():
            raise SystemExit("--seed-reference-runtime requires --runtime-dir")
        if args.live_model_planning and not str(args.runtime_dir or "").strip():
            raise SystemExit("--live-model-planning requires --runtime-dir so the isolated backend receives the env flag")
        if args.live_model_planning and str(args.provider_mode or "").strip().lower() != "scripted":
            raise SystemExit("--live-model-planning is only supported with --provider-mode scripted")
        selected_cases = {str(item).strip() for item in args.case if str(item).strip()}
        try:
            cases = load_smoke_cases(args.matrix_file, selected_cases)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
        _runtime_contamination_preflight(args, cases=cases)
        if not str(args.runtime_dir or "").strip():
            _runtime_env_from_cases(cases)
        summaries, failures = _run_smoke_cases(args, cases=cases)
        provider_mode_failures = _validate_smoke_provider_invocation_modes(
            summaries,
            provider_mode=str(args.provider_mode or "simulate"),
        )
        if provider_mode_failures:
            failures.extend(provider_mode_failures)
        timing_summary = summarize_smoke_timings(summaries)
        if str(args.report_json or "").strip():
            _write_json_file(args.report_json, _compact_smoke_report_records(summaries))
        if str(args.summary_json or "").strip():
            _write_json_file(args.summary_json, timing_summary)
        _runtime_contamination_postflight(args, cases=cases)
        _write_stdout_report(
            summaries=summaries,
            timing_summary=timing_summary,
            report_json=str(args.report_json or ""),
            summary_json=str(args.summary_json or ""),
        )
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
    except SmokeMatrixShutdownRequested as exc:
        sys.stderr.write(f"simulate smoke interrupted: {exc}\n")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
