#!/usr/bin/env python3
"""Grok collection driver for X-First seeds (live, xAI subscription).

Committed successor of the proven OpenAI /tmp driver: runs
``x_first.luna_batch_runner.run_grok_collection`` over a seeds file with the
CWD-pinned subprocess transport (cwd=~/.grok — required, otherwise the grok
CLI dies with "Device not configured (os error 6)").  Concurrency is
operator-directed aggressive (default 48 workers, measured OK on this 16GB
machine for the OpenAI 947 batch); tune with --workers.

Smoke first with --limit 2 before any full run.  No Apify cost; grok CLI
calls go through the operator's xAI OAuth.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

GROK_HOME = Path.home() / ".grok"


class CwdGrokTransport:
    def __init__(self, total: int, progress_path: Path | None = None) -> None:
        self._total = total
        self._done = 0
        self._lock = threading.Lock()
        self._progress_path = progress_path

    def run(self, *, argv, prompt, session_id, timeout_ms):
        from x_first import luna_batch_runner as lbr

        completed = subprocess.run(
            list(argv), capture_output=True, timeout=max(1, timeout_ms) / 1000,
            check=False, cwd=GROK_HOME,
        )
        if completed.returncode != 0:
            raise lbr.LunaBatchRunnerError("grok_cli_exit_nonzero")
        envelope = json.loads(completed.stdout.decode("utf-8"))
        if not isinstance(envelope, dict):
            raise lbr.LunaBatchRunnerError("grok_cli_envelope_invalid")
        with self._lock:
            self._done += 1
            if self._progress_path and (self._done % 25 == 0 or self._done == self._total):
                self._progress_path.write_text(f"{self._done}/{self._total} @ {time.strftime('%H:%M:%S')}\n")
        return envelope


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seeds", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--workers", type=int, default=48)
    parser.add_argument("--timeout-ms", type=int, default=300_000)
    parser.add_argument("--target-direction", default="pre-training")
    parser.add_argument("--limit", type=int, default=0, help="smoke-test only the first N seeds")
    parser.add_argument("--xfirst-src", default="/Users/changyuyi/projects/Sourcing AI Agent Dev/x-first-researcher-sourcing/src")
    args = parser.parse_args()

    sys.path.insert(0, args.xfirst_src)
    from x_first import luna_batch_runner as lbr  # noqa: E402

    seeds = json.load(open(args.seeds))["seeds"]
    if args.limit:
        seeds = seeds[: args.limit]
    print(f"[grok] {len(seeds)} seeds, {args.workers} workers, target={args.target_direction}", flush=True)
    t0 = time.monotonic()
    collection = lbr.run_grok_collection(
        seeds=seeds,
        transport=CwdGrokTransport(len(seeds), Path("/tmp/gdm_grok_progress.txt")),
        worker_count=args.workers,
        target_direction=args.target_direction,
        timeout_ms=args.timeout_ms,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    json.dump(collection, open(args.out, "w"), ensure_ascii=False, indent=1)
    found = sum(
        1 for r in collection["results"]
        if r["status"] == "completed"
        and r["bundle"]["account_resolution"]["resolution_confidence"] != "not_found"
    )
    failed = sum(1 for r in collection["results"] if r["status"] != "completed")
    print(
        f"[grok] done {collection['completed_count']}/{collection['candidate_count']} "
        f"in {time.monotonic() - t0:.0f}s; accounts resolved: {found}; failed: {failed}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
