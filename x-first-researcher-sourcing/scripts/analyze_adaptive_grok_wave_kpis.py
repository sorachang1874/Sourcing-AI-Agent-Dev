from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.adaptive_grok_wave_kpis import (  # noqa: E402
    analyze_operator_bundle,
    analyze_sanitized_pair_paths,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Emit de-identified KPIs for one verified adaptive Grok live wave")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--bundle", type=Path, help="Private adaptive Grok run bundle")
    source.add_argument("--result", type=Path, help="Extracted owner-only sanitized.json")
    parser.add_argument("--receipt", type=Path, help="Extracted owner-only operator-receipt.json")
    parser.add_argument(
        "--approval-root",
        type=Path,
        help="Approval ledger root used only when replaying --bundle",
    )
    args = parser.parse_args(argv)
    try:
        if args.bundle is not None:
            if args.receipt is not None:
                raise ValueError("receipt_not_allowed_with_bundle")
            kwargs = {"approval_root": args.approval_root} if args.approval_root is not None else {}
            summary = analyze_operator_bundle(args.bundle, **kwargs)
        else:
            if args.receipt is None:
                raise ValueError("receipt_required_with_result")
            if args.approval_root is not None:
                raise ValueError("approval_root_not_allowed_with_pair")
            summary = analyze_sanitized_pair_paths(args.result, args.receipt)
    except Exception:  # noqa: BLE001 - never echo candidate text, handles, paths, or private receipt fields
        print(json.dumps({"error": "ADAPTIVE_GROK_WAVE_KPI_ANALYSIS_FAILED", "status": "failed"}, sort_keys=True))
        return 1
    print(json.dumps(summary, ensure_ascii=True, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
