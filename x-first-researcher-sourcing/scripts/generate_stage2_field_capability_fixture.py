from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

from x_first.stage2_field_capability import build_fixture_bundle, load_json, validate_fixture_bundle

ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "configs/stage2_field_registry.v1.json"
FIXTURE_PATH = ROOT / "fixtures/stage2_field_capability_fixture_v1.json"


def serialized_fixture() -> str:
    registry = load_json(REGISTRY_PATH)
    bundle = build_fixture_bundle(registry)
    if bundle != build_fixture_bundle(registry):
        raise ValueError("generated Stage 2A fixture is nondeterministic")
    errors = validate_fixture_bundle(bundle, registry=registry)
    if errors:
        raise ValueError("generated Stage 2A fixture is invalid: " + "; ".join(errors))
    return json.dumps(bundle, allow_nan=False, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def check_fixture() -> bool:
    if FIXTURE_PATH.is_symlink() or not FIXTURE_PATH.is_file():
        return False
    try:
        return FIXTURE_PATH.read_text(encoding="utf-8") == serialized_fixture()
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
        return False


def write_fixture() -> None:
    if FIXTURE_PATH.is_symlink():
        raise ValueError(f"refusing to replace symlink: {FIXTURE_PATH}")
    FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = serialized_fixture().encode("utf-8")
    descriptor, temp_name = tempfile.mkstemp(prefix=".stage2-field-capability-", suffix=".tmp", dir=FIXTURE_PATH.parent)
    temp_path = Path(temp_name)
    try:
        os.fchmod(descriptor, 0o644)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, FIXTURE_PATH)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate or verify the deterministic Stage 2A offline fixture")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if args.check:
        if check_fixture():
            print(f"current: {FIXTURE_PATH}")
            return 0
        print(f"stale or missing: {FIXTURE_PATH}")
        return 1
    write_fixture()
    print(f"wrote: {FIXTURE_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
