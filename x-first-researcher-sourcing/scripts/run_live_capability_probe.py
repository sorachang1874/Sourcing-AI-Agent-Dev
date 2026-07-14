from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.live_probe import purge_expired_live_artifacts, run_live_probe, validate_artifact_pair  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run or validate the bounded Grok/X capability probe")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--execute-live",
        action="store_true",
        help="perform the single owner-approved Grok/X live capability execution",
    )
    mode.add_argument(
        "--validate-directory",
        type=Path,
        help="validate an existing private request.json/result.json pair without external execution",
    )
    mode.add_argument(
        "--purge-expired",
        action="store_true",
        help="delete expired private live artifacts and write non-sensitive deletion receipts",
    )
    args = parser.parse_args(argv)
    if args.purge_expired:
        receipts = purge_expired_live_artifacts()
        print(json.dumps({"deleted": receipts, "status": "complete"}, indent=2, sort_keys=True))
        return 0
    if args.validate_directory is not None:
        errors = validate_artifact_pair(
            args.validate_directory / "request.json",
            args.validate_directory / "result.json",
        )
        print(json.dumps({"errors": errors, "status": "valid" if not errors else "invalid"}, indent=2))
        return 0 if not errors else 1
    try:
        result, artifact_root = run_live_probe(execute_live=True)
    except Exception:
        print(
            json.dumps(
                {
                    "error": "XCAP_LIVE_EXECUTION_FAILED",
                    "status": "failed",
                },
                indent=2,
            )
        )
        return 1
    print(
        json.dumps(
            {
                "artifact_directory": str(artifact_root),
                "capability": result["capability"],
                "errors": result["errors"],
                "status": result["run"]["status"],
                "usage": result["usage"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if result["run"]["status"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
