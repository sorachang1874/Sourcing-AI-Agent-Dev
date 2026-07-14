from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.grok_cli_exploration import canonical_json, evaluate_exploration  # noqa: E402

MAX_INPUT_BYTES = 64 * 1024 * 1024


def _reject_constant(value: str) -> None:
    raise ValueError(f"non_finite_number:{value}")


def _closed_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def _load_json(path: Path) -> Any:
    if not path.is_file() or path.is_symlink() or path.stat().st_size > MAX_INPUT_BYTES:
        raise ValueError("input_file_invalid")
    return json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=_closed_object,
        parse_constant=_reject_constant,
    )


def _write_private(path: Path, payload: bytes) -> None:
    unresolved_parent = path.parent
    if unresolved_parent.is_symlink():
        raise ValueError("output_parent_not_owner_only")
    parent = unresolved_parent.resolve()
    if not parent.is_dir() or stat.S_IMODE(parent.stat().st_mode) & 0o077:
        raise ValueError("output_parent_not_owner_only")
    destination = parent / path.name
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(destination, flags, 0o600)
    try:
        if stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600:
            raise ValueError("output_file_not_owner_only")
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        destination.unlink(missing_ok=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate one sanitized Grok CLI X-search exploration")
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--raw-session-directory", type=Path)
    parser.add_argument(
        "--query-policy-version",
        help=(
            "Select one enabled policy from the repository-approved registry; "
            "defaults to exact session/request binding."
        ),
    )
    parser.add_argument(
        "--query-policy-registry-version",
        help="Select one immutable reviewed registry snapshot; defaults to the canonical v2 snapshot.",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        evaluation = evaluate_exploration(
            _load_json(args.result),
            _load_json(args.receipt),
            raw_session_directory=args.raw_session_directory,
            query_policy_version=args.query_policy_version,
            query_policy_registry_version=args.query_policy_registry_version,
        )
        payload = (canonical_json(evaluation) + "\n").encode()
        _write_private(args.output, payload)
        rendered = {
            "evaluation_sha256": hashlib.sha256(payload).hexdigest(),
            "native_x_call_proof": evaluation["native_x_call_proof"],
            "redacted_summary": True,
            "scale_verdict": evaluation["scale_verdict"],
            "status": evaluation["status"],
        }
    except Exception:  # noqa: BLE001 - do not echo private paths, excerpts, or provider/session data
        print(json.dumps({"error": "GROK_CLI_EXPLORATION_EVALUATION_FAILED", "status": "failed"}, sort_keys=True))
        return 1
    print(json.dumps(rendered, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
