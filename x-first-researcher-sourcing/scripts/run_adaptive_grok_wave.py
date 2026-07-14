from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.adaptive_grok_wave_runner import (  # noqa: E402
    issue_live_grant,
    purge_expired_adaptive_runs,
    recover_incomplete_run,
    run_adaptive_grok_wave_fixture,
    run_adaptive_grok_wave_live,
)


def _public_receipt_summary(receipt: dict[str, object]) -> dict[str, object]:
    process = receipt.get("process")
    spawned = isinstance(process, dict) and process.get("process_spawn_attempted") is True
    return {"external_execution": spawned, "status": receipt["status"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a fail-closed adaptive Grok native-X recall wave")
    parser.add_argument("--request", type=Path, help="strict SHA-bound operator request JSON")
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="consume the one-shot approval and perform the live Grok CLI execution",
    )
    parser.add_argument(
        "--issue-live-grant",
        action="store_true",
        help="preissue one request-bound owner-only grant without executing Grok",
    )
    parser.add_argument(
        "--grant-ttl-seconds",
        type=int,
        default=900,
        help="grant validity in seconds (60..3600); used only with --issue-live-grant",
    )
    parser.add_argument(
        "--fixture-output-root",
        type=Path,
        help="owner-only fixture artifact root; not accepted by the live lane",
    )
    parser.add_argument(
        "--recover-incomplete-run",
        type=Path,
        help="seal an interrupted private run after proving its recorded PID is dead",
    )
    parser.add_argument(
        "--terminate-orphan",
        action="store_true",
        help="during recovery only, explicitly TERM then KILL the recorded live process group",
    )
    parser.add_argument(
        "--purge-expired",
        action="store_true",
        help="delete expired private run bundles after durable deletion journaling",
    )
    args = parser.parse_args(argv)
    try:
        if args.purge_expired:
            if (
                args.request is not None
                or args.execute_live
                or args.issue_live_grant
                or args.fixture_output_root is not None
                or args.recover_incomplete_run is not None
                or args.terminate_orphan
                or args.grant_ttl_seconds != 900
            ):
                raise ValueError("purge_mode_must_be_exclusive")
            receipts = purge_expired_adaptive_runs()
            print(json.dumps({"deleted_run_count": len(receipts), "status": "purge_complete"}, sort_keys=True))
            return 0
        if args.recover_incomplete_run is not None:
            if (
                args.request is not None
                or args.execute_live
                or args.issue_live_grant
                or args.fixture_output_root is not None
                or args.grant_ttl_seconds != 900
            ):
                raise ValueError("recovery_mode_must_be_exclusive")
            receipt = recover_incomplete_run(
                args.recover_incomplete_run,
                terminate_orphan=args.terminate_orphan,
            )
        else:
            if args.terminate_orphan:
                raise ValueError("terminate_orphan_requires_recovery")
            if args.request is None:
                raise ValueError("request_required")
            if args.issue_live_grant:
                if args.execute_live or args.fixture_output_root is not None:
                    raise ValueError("grant_issue_mode_must_be_exclusive")
                issue_live_grant(request_path=args.request, ttl_seconds=args.grant_ttl_seconds)
                print(json.dumps({"grant_issued": True, "status": "ready"}, sort_keys=True))
                return 0
            if args.grant_ttl_seconds != 900:
                raise ValueError("grant_ttl_requires_issue_mode")
            if args.execute_live:
                if args.fixture_output_root is not None:
                    raise ValueError("live_output_root_is_module_owned")
                receipt, _ = run_adaptive_grok_wave_live(execute_live=True, request_path=args.request)
            else:
                options = {"request_path": args.request}
                if args.fixture_output_root is not None:
                    options["runtime_root"] = args.fixture_output_root
                receipt, _ = run_adaptive_grok_wave_fixture(**options)
    except Exception:  # noqa: BLE001 - never emit paths, handles, prompts, or provider content
        print(json.dumps({"error": "ADAPTIVE_GROK_WAVE_FAILED", "status": "failed"}, sort_keys=True))
        return 1
    print(json.dumps(_public_receipt_summary(receipt), sort_keys=True))
    return 0 if receipt["status"] in {"fixture_complete", "completed", "crash_recovered"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
