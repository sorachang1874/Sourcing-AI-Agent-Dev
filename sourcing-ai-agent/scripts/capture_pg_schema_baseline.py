#!/usr/bin/env python3
"""Regenerate the Track B PG-native schema baseline migration.

The baseline (``src/sourcing_agent/migrations/0001_baseline.sql``) is GENERATED, not
hand-written: it is the schema a fresh ``ControlPlaneStore`` bootstrap produces via the
real code path (``init_schema`` + ``ensure_bootstrapped`` + the writer/coordination
ensures), captured with ``pg_dump --schema-only`` and normalized to be schema-agnostic
(unqualified object names; the migration runner sets ``search_path`` to the target schema).

This script is the single regeneration path so the baseline can never silently drift from
what the code produces — re-run it and ``git diff`` after any schema-affecting change, and
keep it byte-stable until a deliberate, reviewed migration adds ``0002_*.sql`` instead.

Usage:
    PYTHONPATH=src python scripts/capture_pg_schema_baseline.py [--out PATH] [--keep-schema]

DSN comes from the standard control-plane resolution (``SOURCING_CONTROL_PLANE_POSTGRES_DSN``
or the docker ``.local-postgres.env``). ``pg_dump`` is located via ``PG_DUMP_BIN`` or PATH.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

CAPTURE_SCHEMA = "track_b_schema_baseline_capture"
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_OUT = _REPO_ROOT / "src" / "sourcing_agent" / "migrations" / "0001_baseline.sql"


def _locate_pg_dump() -> str:
    explicit = os.getenv("PG_DUMP_BIN")
    if explicit and Path(explicit).exists():
        return explicit
    found = shutil.which("pg_dump")
    if found:
        return found
    for candidate in (
        "/opt/homebrew/opt/postgresql@16/bin/pg_dump",
        "/usr/lib/postgresql/16/bin/pg_dump",
        "/usr/bin/pg_dump",
    ):
        if Path(candidate).exists():
            return candidate
    raise SystemExit("pg_dump not found — set PG_DUMP_BIN or add postgresql client to PATH")


def _normalize(raw: str) -> str:
    raw = raw.replace(f"{CAPTURE_SCHEMA}.", "")
    raw = raw.replace(f"; Schema: {CAPTURE_SCHEMA};", "; Schema: -;")
    raw = raw.replace(CAPTURE_SCHEMA, "-")
    skip_prefixes = (
        "\\restrict",
        "\\unrestrict",
        "SET statement_timeout",
        "SET lock_timeout",
        "SET idle_in_transaction_session_timeout",
        "SET client_encoding",
        "SET standard_conforming_strings",
        "SET check_function_bodies",
        "SET xmloption",
        "SET client_min_messages",
        "SET row_security",
        "SET default_tablespace",
        "SET default_table_access_method",
        "SELECT pg_catalog.set_config",
    )
    out: list[str] = []
    for line in raw.splitlines():
        s = line.strip()
        if s.startswith(skip_prefixes):
            continue
        if s.startswith("CREATE SCHEMA "):
            continue
        if s.startswith("-- PostgreSQL database dump") or s.startswith("-- Dumped"):
            continue
        if "; Type: SCHEMA;" in s:
            continue
        if s == "--":
            continue
        out.append(line.rstrip())
    body = re.sub(r"\n{3,}", "\n\n", "\n".join(out)).strip() + "\n"
    header = (
        "-- migrations/0001_baseline.sql\n"
        "-- Track B B1.1 — PG-native control-plane schema baseline (single source of truth).\n"
        "--\n"
        "-- GENERATED, not hand-written: captured from a fresh ControlPlaneStore bootstrap via\n"
        "-- the real code path (init_schema + ensure_bootstrapped + writer/coordination ensures),\n"
        "-- pg_dump --schema-only, normalized to be schema-agnostic (table set verified == live\n"
        "-- `public`, 83 tables, 0 diff). Regenerate with scripts/capture_pg_schema_baseline.py\n"
        "-- and review the git diff; keep byte-stable until a reviewed 0002_*.sql migration.\n"
        "--\n"
        "-- The migration runner sets search_path to the target schema before applying this file,\n"
        "-- so all object names here are intentionally UNQUALIFIED. Do not add a schema prefix.\n"
        "--\n\n"
    )
    return header + body


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(_DEFAULT_OUT), help="baseline output path")
    parser.add_argument("--keep-schema", action="store_true", help="do not drop the throwaway capture schema")
    args = parser.parse_args()

    sys.path.insert(0, str(_REPO_ROOT / "src"))
    from sourcing_agent.local_postgres import resolve_control_plane_postgres_dsn

    dsn = resolve_control_plane_postgres_dsn()
    if not dsn:
        raise SystemExit("no control-plane Postgres DSN resolved (set SOURCING_CONTROL_PLANE_POSTGRES_DSN)")

    os.environ.update(
        {
            "SOURCING_CONTROL_PLANE_POSTGRES_DSN": dsn,
            "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": CAPTURE_SCHEMA,
            "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
            "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "1",
            "SOURCING_PG_ONLY_SQLITE_BACKEND": "shared_memory",
            "SOURCING_RUNTIME_ENVIRONMENT": "test",
            "SOURCING_EXTERNAL_PROVIDER_MODE": "simulate",
        }
    )

    import psycopg

    from sourcing_agent.storage import ControlPlaneStore

    with psycopg.connect(dsn, autocommit=True, client_encoding="utf8") as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{CAPTURE_SCHEMA}" CASCADE')
            cur.execute(f'CREATE SCHEMA "{CAPTURE_SCHEMA}"')

    with tempfile.TemporaryDirectory() as tmp:
        store = ControlPlaneStore(Path(tmp) / "capture.db")
        adapter = store._control_plane_postgres
        adapter.ensure_bootstrapped()
        adapter._ensure_control_plane_writer_schema()
        adapter._ensure_runtime_coordination_schema()
        store.close()

    raw = subprocess.run(
        [
            _locate_pg_dump(),
            dsn,
            f"--schema={CAPTURE_SCHEMA}",
            "--schema-only",
            "--no-owner",
            "--no-privileges",
            "--no-comments",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(_normalize(raw), encoding="utf-8")

    if not args.keep_schema:
        with psycopg.connect(dsn, autocommit=True, client_encoding="utf8") as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{CAPTURE_SCHEMA}" CASCADE')

    table_count = raw.count("\nCREATE TABLE ")
    print(f"wrote {out_path} ({table_count} tables)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
