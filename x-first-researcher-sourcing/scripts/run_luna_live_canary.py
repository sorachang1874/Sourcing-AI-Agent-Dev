from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.luna_live_canary import run_luna_live_canary, validate_artifact_directory  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run or validate the one-shot chshapi Luna semantic canary")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--execute-live",
        action="store_true",
        help="consume the fixed approval and execute the bounded catalog/model canary",
    )
    mode.add_argument(
        "--validate-directory",
        type=Path,
        help="validate an existing private canary bundle without credentials or network",
    )
    args = parser.parse_args(argv)
    if args.validate_directory is not None:
        errors = validate_artifact_directory(args.validate_directory)
        print(json.dumps({"errors": errors, "status": "valid" if not errors else "invalid"}, sort_keys=True))
        return 0 if not errors else 1
    try:
        result, artifact_root = run_luna_live_canary(execute_live=True)
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
