from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.luna_live_canary import validate_artifact_directory as validate_v1_directory  # noqa: E402
from x_first.luna_live_canary_v2 import (  # noqa: E402
    purge_expired_artifact_directory_v2,
    run_luna_live_canary_v2,
    validate_artifact_directory_v2,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the receipt-first v2 chshapi Luna semantic canary or validate a v1/v2 bundle"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--execute-live-v2",
        action="store_true",
        help="consume the distinct v2 approval and execute the receipt-first bounded catalog/model canary",
    )
    mode.add_argument(
        "--validate-directory",
        type=Path,
        help="validate an existing private canary bundle without credentials or network",
    )
    mode.add_argument(
        "--purge-expired-v2-directory",
        type=Path,
        help="atomically purge one expired production v2 bundle and retain a private deletion tombstone",
    )
    args = parser.parse_args(argv)
    if args.validate_directory is not None:
        v2_errors = validate_artifact_directory_v2(args.validate_directory)
        v1_errors = validate_v1_directory(args.validate_directory) if v2_errors else ["not_checked"]
        errors = [] if not v2_errors or not v1_errors else ["LUNA_LIVE_CANARY_ARTIFACT_INVALID"]
        version = "v2" if not v2_errors else "v1" if not v1_errors else None
        print(
            json.dumps(
                {"artifact_version": version, "errors": errors, "status": "valid" if not errors else "invalid"},
                sort_keys=True,
            )
        )
        return 0 if not errors else 1
    if args.purge_expired_v2_directory is not None:
        try:
            receipt, receipt_path = purge_expired_artifact_directory_v2(args.purge_expired_v2_directory)
        except Exception:  # noqa: BLE001 - never echo private paths or retained evidence on failure
            print(json.dumps({"error": "LUNA_LIVE_CANARY_V2_PURGE_FAILED", "status": "failed"}, sort_keys=True))
            return 1
        print(
            json.dumps(
                {
                    "deletion_receipt": str(receipt_path),
                    "run_id": receipt["run_id"],
                    "status": "deleted",
                },
                sort_keys=True,
            )
        )
        return 0
    try:
        result, artifact_root = run_luna_live_canary_v2(execute_live=True)
    except Exception:  # noqa: BLE001 - never echo credentials, provider bodies, or transport details
        print(json.dumps({"error": "LUNA_LIVE_CANARY_EXECUTION_FAILED", "status": "failed"}, sort_keys=True))
        return 1
    print(
        json.dumps(
            {
                "artifact_directory": str(artifact_root),
                "calls": result["calls"],
                "error_codes": result["error_codes"],
                "status": result["status"],
            },
            sort_keys=True,
        )
    )
    return 0 if result["status"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
