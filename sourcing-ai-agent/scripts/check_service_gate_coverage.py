from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sourcing_agent.service_gate_coverage import validate_service_gate_coverage


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate scripted/browser service-gate coverage tags and SLO guardrails."
    )
    parser.add_argument(
        "--config-dir",
        default="configs/scripted",
        help="Directory containing service_gate_coverage_manifest.json and *smoke_matrix.json files.",
    )
    parser.add_argument(
        "--require-before-ecs-sync",
        action="store_true",
        help="Also fail while required_before_ecs_sync tags are not covered by smoke matrices.",
    )
    parser.add_argument("--json", action="store_true", help="Print the full machine-readable report.")
    args = parser.parse_args()

    report = validate_service_gate_coverage(
        Path(args.config_dir),
        require_before_ecs_sync=bool(args.require_before_ecs_sync),
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(f"status={report['status']} errors={report['error_count']}")
        print(
            "coverage="
            f"{len(report['covered_tags'])}/{report['tag_count']} tags, "
            f"{report['matrix_count']} matrices, {report['case_count']} cases"
        )
        if report["missing_required_now_tags"]:
            print("missing required_now tags:")
            for tag in report["missing_required_now_tags"]:
                print(f"  - {tag}")
        if report["missing_required_before_ecs_sync_tags"]:
            print("missing required_before_ecs_sync tags:")
            for tag in report["missing_required_before_ecs_sync_tags"]:
                print(f"  - {tag}")
        if report["coverage_errors"]:
            print("coverage errors:")
            for error in report["coverage_errors"]:
                print(f"  - {error}")
        if report["metadata_errors"]:
            print("metadata errors:")
            for error in report["metadata_errors"]:
                print(f"  - {error}")
        if report["planned_gaps"] and not args.require_before_ecs_sync:
            print("planned gaps before ECS sync:")
            for item in report["planned_gaps"]:
                print(f"  - {item['tag']}: {item['owner_next_step']}")
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
