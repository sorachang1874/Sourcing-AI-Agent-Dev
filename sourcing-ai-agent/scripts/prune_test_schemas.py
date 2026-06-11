#!/usr/bin/env python3
"""Orphan test-schema janitor for the local control-plane Postgres.

Test-environment contract v2 pairs every ephemeral test runtime dir with a
per-runtime PG schema. `isolated_hosted_test_runtime(...)` drops its schema on
teardown, but crashed runs (SIGKILL, power loss, pre-v2 runtimes) leave orphan
schemas behind. This janitor:

1. snapshots the PG schemas matching `sourcing_(test|scripted|simulate|replay)_*`
   FIRST (a runtime created after the snapshot is simply not a candidate, which
   closes the list-then-pair race where a brand-new runtime would be listed but
   not yet paired);
2. then collects the set of schemas still referenced by runtime dirs under
   `runtime/test_env` via, in order of authority:
   - `.ephemeral-test-env.json` markers (contract v2),
   - legacy `.scripted-local-postgres.env` files
     (`SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA=...`),
   - the deterministic schema derivation from the runtime dir path
     (`sourcing_<provider_mode>_<dirname[:24]>_<sha1(path)[:8]>`, reproduced via
     `scripted_test_runtime._default_isolated_postgres_schema`);
3. classifies the rest as orphan — unless an active backend (other pid, state
   != 'idle') has query text referencing the schema, in which case the schema
   is skipped with reason `active_query_reference` (override with --force).

LIMITATION of the active-query guard: pg_stat_activity only exposes each
backend's current/most-recent query text. A live session that is idle between
statements, or that uses a schema purely via search_path without naming it,
is NOT detected. Do not run --apply while test suites are executing — tmp-dir
pytest fixtures (e.g. tests/pg_durable_runtime.py) create sourcing_test_*
schemas with no runtime/test_env pairing.

Dry-run by default: prints schemas + verdicts and never drops anything.
Pass --apply to DROP orphan schemas (CASCADE). Refuses non-local DSNs — this
tool must never run against a remote control plane.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sourcing_agent.local_postgres import (  # noqa: E402
    normalize_control_plane_postgres_connect_dsn,
    normalize_control_plane_postgres_schema,
    quote_control_plane_postgres_identifier,
    resolve_control_plane_postgres_dsn,
)
from sourcing_agent.scripted_test_runtime import (  # noqa: E402
    EPHEMERAL_TEST_ENV_MARKER_NAME,
    _default_isolated_postgres_schema,
    _runtime_env_file_assignments,
    _WORKFLOW_CONFIDENCE_ENV_FILE_NAME,
)

# Safety boundary: the janitor only ever considers (and only ever drops)
# schemas matching this pattern, regardless of any user-supplied --match.
TEST_SCHEMA_SAFETY_PATTERN = re.compile(r"^sourcing_(test|scripted|simulate|replay)_")

_LOCAL_DSN_HOSTS = {"localhost", "127.0.0.1", "::1"}
_DERIVED_PAIRING_PROVIDER_MODES = ("simulate", "scripted", "replay")

DEFAULT_RUNTIME_ROOT = PROJECT_ROOT / "runtime" / "test_env"


def dsn_is_local(dsn: str) -> bool:
    """True only when the DSN host is loopback (localhost/127.0.0.1/::1)."""

    normalized = str(dsn or "").strip()
    if not normalized:
        return False
    if "://" in normalized:
        try:
            parsed = urlsplit(normalized)
        except ValueError:
            return False
        if str(parsed.scheme or "").strip().lower() not in {"postgres", "postgresql"}:
            return False
        try:
            hostname = str(parsed.hostname or "").strip().lower()
        except ValueError:
            return False
        return hostname in _LOCAL_DSN_HOSTS
    # libpq keyword/value form: host=... port=...
    if "=" not in normalized:
        return False
    host = ""
    for token in normalized.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        if key.strip().lower() == "host":
            host = value.strip().strip("'\"").lower()
    return host in _LOCAL_DSN_HOSTS


def _candidate_runtime_dirs(runtime_root: Path) -> list[Path]:
    root = Path(runtime_root).expanduser()
    if not root.is_dir():
        return []
    dirs = [root]
    try:
        children = sorted(root.iterdir())
    except OSError:
        children = []
    for child in children:
        if child.is_dir():
            dirs.append(child)
    return dirs


def collect_referenced_schemas(runtime_roots: list[Path]) -> dict[str, list[str]]:
    """Map schema name -> list of pairing evidence strings."""

    referenced: dict[str, list[str]] = {}

    def _record(schema_value: Any, source: str) -> None:
        schema = normalize_control_plane_postgres_schema(str(schema_value or ""), default="")
        if not schema or not TEST_SCHEMA_SAFETY_PATTERN.match(schema):
            return
        referenced.setdefault(schema, []).append(source)

    for runtime_root in runtime_roots:
        for runtime_dir in _candidate_runtime_dirs(runtime_root):
            marker_path = runtime_dir / EPHEMERAL_TEST_ENV_MARKER_NAME
            if marker_path.is_file():
                try:
                    payload = json.loads(marker_path.read_text(encoding="utf-8", errors="replace"))
                except (OSError, ValueError):
                    payload = {}
                if isinstance(payload, dict):
                    _record(payload.get("schema"), f"marker:{marker_path}")
            env_file = runtime_dir / _WORKFLOW_CONFIDENCE_ENV_FILE_NAME
            if env_file.is_file():
                assignments = _runtime_env_file_assignments(env_file)
                _record(
                    assignments.get("SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA"),
                    f"legacy-env:{env_file}",
                )
            # Pre-marker runtimes may have lost their env file; reproduce the
            # deterministic dir->schema derivation for every non-live mode.
            for provider_mode in _DERIVED_PAIRING_PROVIDER_MODES:
                _record(
                    _default_isolated_postgres_schema(runtime_dir, provider_mode=provider_mode),
                    f"derived:{provider_mode}:{runtime_dir}",
                )
    return referenced


def list_candidate_test_schemas(dsn: str, *, match: re.Pattern[str] | None = None) -> list[str]:
    import psycopg

    connect_dsn = normalize_control_plane_postgres_connect_dsn(dsn)
    with psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT nspname FROM pg_namespace ORDER BY nspname")
            names = [str(row[0]) for row in cursor.fetchall()]
    candidates = [name for name in names if TEST_SCHEMA_SAFETY_PATTERN.match(name)]
    if match is not None:
        candidates = [name for name in candidates if match.search(name)]
    return candidates


def _match_active_query_schemas(
    rows: list[tuple[Any, ...]], candidates: list[str]
) -> dict[str, list[str]]:
    """Pure matching half of the active-query guard (unit-testable without PG).

    `rows` are (pid, state, query) tuples from pg_stat_activity; a candidate
    schema matches when its name appears (case-insensitively, ILIKE-style
    containment) in any active backend's query text.
    """

    matched: dict[str, list[str]] = {}
    lowered_candidates = [(schema, schema.lower()) for schema in candidates if str(schema).strip()]
    for row in rows:
        pid = row[0] if len(row) > 0 else ""
        state = row[1] if len(row) > 1 else ""
        query_text = str(row[2] or "").lower() if len(row) > 2 else ""
        if not query_text:
            continue
        for schema, lowered_schema in lowered_candidates:
            if lowered_schema in query_text:
                matched.setdefault(schema, []).append(f"pid={pid},state={state}")
    return matched


def list_active_query_schema_references(dsn: str, candidates: list[str]) -> dict[str, list[str]]:
    """Map candidate schema -> evidence of active backends whose query names it.

    Practical guard, not a proof: pg_stat_activity only shows each backend's
    current/most-recent query text, so sessions that are idle between
    statements (excluded by state != 'idle') or that reach a schema purely via
    search_path without naming it are NOT detected. See module docstring.
    """

    normalized_candidates = [str(item).strip() for item in candidates if str(item).strip()]
    if not normalized_candidates:
        return {}
    import psycopg

    connect_dsn = normalize_control_plane_postgres_connect_dsn(dsn)
    with psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pid, state, COALESCE(query, '') FROM pg_stat_activity "
                "WHERE datname = current_database() "
                "  AND pid <> pg_backend_pid() "
                "  AND state IS NOT NULL AND state <> 'idle'"
            )
            rows = [tuple(row) for row in cursor.fetchall()]
    return _match_active_query_schemas(rows, normalized_candidates)


def _drop_schema(dsn: str, schema: str) -> None:
    import psycopg

    if not TEST_SCHEMA_SAFETY_PATTERN.match(schema):  # defense in depth
        raise ValueError(f"refusing to drop non-test schema: {schema}")
    connect_dsn = normalize_control_plane_postgres_connect_dsn(dsn)
    with psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA IF EXISTS {quote_control_plane_postgres_identifier(schema)} CASCADE")


def prune_test_schemas(
    *,
    dsn: str = "",
    runtime_roots: list[Path] | None = None,
    apply: bool = False,
    match: re.Pattern[str] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    resolved_dsn = str(dsn or "").strip() or str(resolve_control_plane_postgres_dsn(PROJECT_ROOT) or "").strip()
    if not resolved_dsn:
        raise ValueError("no control-plane Postgres DSN resolved; pass --dsn or configure .local-postgres.env")
    if not dsn_is_local(resolved_dsn):
        raise ValueError(
            "refusing to prune test schemas against a non-local DSN "
            f"(host must be one of {sorted(_LOCAL_DSN_HOSTS)}): {resolved_dsn}"
        )
    roots = [Path(item) for item in (runtime_roots if runtime_roots is not None else [DEFAULT_RUNTIME_ROOT])]
    # Ordering matters: snapshot the PG schema list FIRST, then scan runtime
    # dirs. A runtime created between the two steps is simply not a candidate;
    # the reverse order would list it while its pairing was not yet visible
    # and (under --apply) drop a live schema.
    candidates = list_candidate_test_schemas(resolved_dsn, match=match)
    referenced = collect_referenced_schemas(roots)
    unpaired = [schema for schema in candidates if not referenced.get(schema)]
    active_references: dict[str, list[str]] = (
        {} if force else list_active_query_schema_references(resolved_dsn, unpaired)
    )
    entries: list[dict[str, Any]] = []
    orphans: list[str] = []
    skipped_active: list[str] = []
    for schema in candidates:
        sources = referenced.get(schema) or []
        if sources:
            verdict = "referenced"
        elif schema in active_references:
            verdict = "skipped_active"
            skipped_active.append(schema)
        else:
            verdict = "orphan"
            orphans.append(schema)
        entry: dict[str, Any] = {"schema": schema, "verdict": verdict, "sources": sources}
        if verdict == "skipped_active":
            entry["reason"] = "active_query_reference"
            entry["active_backends"] = active_references[schema]
        entries.append(entry)
    dropped: list[str] = []
    drop_failures: list[dict[str, str]] = []
    if apply:
        for schema in orphans:
            try:
                _drop_schema(resolved_dsn, schema)
            except Exception as exc:
                drop_failures.append({"schema": schema, "error": f"{type(exc).__name__}: {exc}"})
            else:
                dropped.append(schema)
    return {
        "status": "applied" if apply else "dry_run",
        "dsn": resolved_dsn,
        "runtime_roots": [str(root) for root in roots],
        "schemas": entries,
        "referenced_count": sum(1 for entry in entries if entry["verdict"] == "referenced"),
        "orphan_count": len(orphans),
        "orphans": orphans,
        "skipped_active": skipped_active,
        "skipped_active_count": len(skipped_active),
        "force": bool(force),
        "dropped": dropped,
        "drop_failures": drop_failures,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "List sourcing_(test|scripted|simulate|replay)_* schemas in the local control-plane "
            "Postgres and classify them as referenced (paired with a runtime dir) or orphan. "
            "Dry-run by default; --apply drops orphans."
        ),
        epilog=(
            "WARNING: do not run --apply while test suites are executing. Tmp-dir pytest "
            "fixtures (e.g. tests/pg_durable_runtime.py) create sourcing_test_* schemas with "
            "no runtime/test_env pairing, so a concurrent --apply can classify their live "
            "schemas as orphans. The active-query guard (skip reason: active_query_reference) "
            "reduces but does not eliminate this exposure: pg_stat_activity only shows each "
            "backend's current/most-recent query text, so sessions idle between statements or "
            "using a schema purely via search_path are not detected. --force disables the guard."
        ),
    )
    parser.add_argument("--dsn", default="", help="control-plane DSN (default: resolve .local-postgres.env)")
    parser.add_argument(
        "--runtime-root",
        action="append",
        default=[],
        help=f"runtime root(s) to scan for pairings (default: {DEFAULT_RUNTIME_ROOT})",
    )
    parser.add_argument(
        "--match",
        default="",
        help="optional extra regex (re.search) narrowing candidate schemas; the sourcing_* safety pattern always applies",
    )
    parser.add_argument("--apply", action="store_true", help="drop orphan schemas (default: dry-run)")
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "disable the active-query guard: classify unpaired schemas as orphans even when "
            "active backends have query text referencing them"
        ),
    )
    parser.add_argument("--json", action="store_true", help="emit the full report as JSON")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    match = re.compile(args.match) if str(args.match or "").strip() else None
    runtime_roots = [Path(item) for item in args.runtime_root] if args.runtime_root else None
    try:
        report = prune_test_schemas(
            dsn=args.dsn,
            runtime_roots=runtime_roots,
            apply=bool(args.apply),
            match=match,
            force=bool(args.force),
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    for entry in report["schemas"]:
        evidence = entry["sources"][0] if entry["sources"] else "-"
        if entry["verdict"] == "skipped_active":
            evidence = ";".join(entry.get("active_backends") or []) or entry.get("reason") or "-"
        print(f"{entry['verdict']:<14} {entry['schema']}  {evidence}")
    print(
        f"{report['status']}: {len(report['schemas'])} candidate schema(s), "
        f"{report['referenced_count']} referenced, {report['orphan_count']} orphan, "
        f"{report['skipped_active_count']} skipped (active_query_reference), "
        f"{len(report['dropped'])} dropped"
    )
    if report["drop_failures"]:
        for failure in report["drop_failures"]:
            print(f"drop failed: {failure['schema']}: {failure['error']}", file=sys.stderr)
        return 1
    if report["status"] == "dry_run" and report["orphan_count"]:
        print("dry-run only; re-run with --apply to drop orphan schemas")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
