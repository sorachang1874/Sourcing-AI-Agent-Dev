from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.grok_transport_health import evaluate_grok_transport_health  # noqa: E402

MAX_MODELS_OUTPUT_BYTES = 2_000_000


def _read_text(path: Path) -> str:
    if not path.is_file() or path.is_symlink() or path.stat().st_size > MAX_MODELS_OUTPUT_BYTES:
        raise ValueError("models_output_file_invalid")
    return path.read_text(encoding="utf-8")


def _run_grok_models(timeout_seconds: float) -> str:
    result = subprocess.run(
        ["grok", "models"],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    return f"{result.stdout}\n{result.stderr}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate Grok CLI transport health before native-X live waves")
    parser.add_argument("--requested-model", required=True, help="model id intended for the next local live attempt")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--models-output", type=Path, help="captured output from `grok models`")
    source.add_argument(
        "--run-grok-models",
        action="store_true",
        help="run `grok models`; this does not call native-X tools or start a research wave",
    )
    parser.add_argument(
        "--grok-models-timeout-seconds",
        type=float,
        default=180.0,
        help="timeout used only with --run-grok-models",
    )
    parser.add_argument("--canary-exit-code", type=int, help="optional one-call native-X canary process exit code")
    parser.add_argument("--canary-output", type=Path, help="optional captured one-call native-X canary stdout/stderr")
    args = parser.parse_args(argv)

    try:
        if args.grok_models_timeout_seconds <= 0 or args.grok_models_timeout_seconds > 600:
            raise ValueError("grok_models_timeout_invalid")
        if args.run_grok_models:
            models_output = _run_grok_models(args.grok_models_timeout_seconds)
        elif args.models_output is not None:
            models_output = _read_text(args.models_output)
        else:
            models_output = sys.stdin.read(MAX_MODELS_OUTPUT_BYTES + 1)
            if len(models_output.encode("utf-8")) > MAX_MODELS_OUTPUT_BYTES:
                raise ValueError("models_output_stdin_too_large")
        canary_output = _read_text(args.canary_output) if args.canary_output is not None else ""
        health = evaluate_grok_transport_health(
            requested_model=args.requested_model,
            models_output=models_output,
            canary_exit_code=args.canary_exit_code,
            canary_output=canary_output,
        )
    except Exception:  # noqa: BLE001 - do not echo paths, auth state, or provider logs
        print(json.dumps({"error": "GROK_TRANSPORT_HEALTH_CHECK_FAILED", "status": "failed"}, sort_keys=True))
        return 1
    print(json.dumps(health.as_dict(), ensure_ascii=True, separators=(",", ":"), sort_keys=True))
    return 0 if health.large_wave_allowed else 2


if __name__ == "__main__":
    raise SystemExit(main())
