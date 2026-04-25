#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from sourcing_agent.workflow_smoke import (
    HostedWorkflowSmokeClient,
    load_smoke_cases,
    run_hosted_smoke_case,
)


DEFAULT_CASES = ["xai_full_roster", "xai_coding_all_members_scoped"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a manual live-provider large-org regression against a hosted backend. "
            "This command is intentionally not part of fast regression."
        ),
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8777", help="Hosted API base URL.")
    parser.add_argument("--case", action="append", default=[], help="Run only the named case; repeatable.")
    parser.add_argument("--reviewer", default="live-large-org-manual", help="Reviewer name used for auto-approval.")
    parser.add_argument("--poll-seconds", type=float, default=2.0, help="Polling interval for progress.")
    parser.add_argument("--max-poll-seconds", type=float, default=180.0, help="Per-case timeout window.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if any case fails its manual live acceptance criteria.",
    )
    parser.add_argument(
        "--confirm-live-provider-cost",
        action="store_true",
        help="Required acknowledgement that this run may trigger real paid provider calls.",
    )
    return parser.parse_args()


def _accept_live_large_org_case(record: dict[str, Any]) -> dict[str, Any]:
    case_name = str(record.get("case") or "").strip()
    explain = dict(record.get("explain") or {})
    final = dict(record.get("final") or {})
    timeline = list(record.get("timeline") or [])
    job_status = str(final.get("job_status") or "").strip().lower()
    stages = {str(item.get("stage") or "").strip().lower() for item in timeline}
    current_keywords = list(explain.get("plan_current_filter_keywords") or [])
    current_queries = list(explain.get("plan_current_search_seed_queries") or [])

    if case_name == "xai_full_roster":
        strategy_ok = str(explain.get("plan_primary_strategy_type") or "") == "full_company_roster"
        shard_ok = str(explain.get("plan_company_employee_shard_strategy") or "") == "adaptive_us_eng_research_partition"
        progress_ok = "acquiring" in stages or job_status == "completed"
        accepted = bool(strategy_ok and shard_ok and progress_ok)
        status = "accepted_completed" if accepted and job_status == "completed" else ("accepted_nonterminal" if accepted else "failed")
        return {
            "status": status,
            "reason": (
                "full roster path entered with expected shard policy"
                if accepted
                else "expected full_company_roster + adaptive_us_eng_research_partition + acquiring/completed progress"
            ),
        }

    if case_name == "xai_coding_all_members_scoped":
        strategy_ok = str(explain.get("plan_primary_strategy_type") or "") == "scoped_search_roster"
        keyword_ok = "coding" in {str(item).strip().lower() for item in current_keywords + current_queries}
        progress_ok = "acquiring" in stages or job_status == "completed"
        accepted = bool(strategy_ok and keyword_ok and progress_ok)
        status = "accepted_completed" if accepted and job_status == "completed" else ("accepted_nonterminal" if accepted else "failed")
        return {
            "status": status,
            "reason": (
                "directional scoped-search path entered with Coding keyword preserved"
                if accepted
                else "expected scoped_search_roster + Coding keyword + acquiring/completed progress"
            ),
        }

    accepted = job_status == "completed"
    return {
        "status": "accepted_completed" if accepted else "failed",
        "reason": "completed" if accepted else "job did not complete",
    }


def main() -> int:
    args = parse_args()
    confirmed = bool(args.confirm_live_provider_cost) or str(
        os.getenv("SOURCING_LIVE_PROVIDER_CONFIRM") or ""
    ).strip().lower() in {"1", "true", "yes", "on"}
    if not confirmed:
        sys.stderr.write(
            "Refusing to run live large-org regression without --confirm-live-provider-cost "
            "or SOURCING_LIVE_PROVIDER_CONFIRM=1.\n"
        )
        return 2

    selected_cases = [str(item).strip() for item in args.case if str(item).strip()]
    if not selected_cases:
        selected_cases = list(DEFAULT_CASES)
    cases = load_smoke_cases(selected_cases=set(selected_cases))
    client = HostedWorkflowSmokeClient(args.base_url)

    summaries: list[dict[str, Any]] = []
    failures: list[str] = []
    for case in cases:
        record = run_hosted_smoke_case(
            client=client,
            case_name=str(case.get("case") or ""),
            payload=dict(case.get("payload") or {}),
            reviewer=str(args.reviewer),
            poll_seconds=float(args.poll_seconds),
            max_poll_seconds=float(args.max_poll_seconds),
        )
        acceptance = _accept_live_large_org_case(record)
        record["acceptance"] = acceptance
        summaries.append(record)
        if str(acceptance.get("status") or "") == "failed":
            failures.append(str(case.get("case") or ""))

    json.dump(summaries, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    if args.strict and failures:
        sys.stderr.write(f"live large-org regression failures: {', '.join(failures)}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
