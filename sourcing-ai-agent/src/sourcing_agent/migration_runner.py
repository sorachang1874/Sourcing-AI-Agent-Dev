"""Track B B1.2 — tiny idempotent PostgreSQL schema migration runner.

Applies ``migrations/000N_*.sql`` in filename order against a target schema, recording
each applied file in a ``schema_migrations`` ledger. Properties:

- **Idempotent** — a version already in the ledger is skipped; re-running is a no-op.
- **Serialized** — a transaction-scoped advisory lock (same key shape as
  ``LiveControlPlanePostgresAdapter._advisory_lock_key``) keeps concurrently-booting
  processes from racing to apply the same migration.
- **Atomic** — all pending migrations + their ledger rows commit in ONE transaction
  (PostgreSQL DDL is transactional), so a crash mid-apply leaves the schema untouched.
- **Brownfield-safe** — a schema that already carries the baseline tables but has no
  ledger (every DB created by the pre-runner ``sqlite_master``-derived bootstrap: prod
  ``public``, local dev, every already-bootstrapped test schema) is STAMPED at the
  baseline rather than re-running ``0001_baseline`` (whose bare ``CREATE TABLE`` would
  collide). This is what makes the B1.3 cutover safe on live databases.
- **Tamper-evident** — each ledger row stores the file's sha256; a previously-applied
  migration whose file content changed fails closed (the baseline must stay byte-stable;
  real changes go in a new ``0002_*.sql``).

The runner does NOT manage connection lifecycle — the caller passes a live psycopg
connection (non-autocommit) and the runner commits/rolls back the migration transaction.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .local_postgres import quote_control_plane_postgres_identifier

_MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
_MIGRATION_FILE_RE = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")
_BASELINE_VERSION = "0001_baseline"
# Presence of this table => the baseline application schema is already materialized.
# `jobs` is a core baseline table created by every bootstrap path.
_BASELINE_SENTINEL_TABLE = "jobs"

_SCHEMA_MIGRATIONS_DDL = (
    "CREATE TABLE IF NOT EXISTS schema_migrations ("
    "version text PRIMARY KEY, "
    "checksum text NOT NULL, "
    "applied_at timestamptz NOT NULL DEFAULT now())"
)


class MigrationChecksumError(RuntimeError):
    """A previously-applied migration's file content no longer matches its ledger checksum."""


@dataclass
class MigrationResult:
    schema: str
    applied: list[str] = field(default_factory=list)  # migrations actually executed this run
    stamped: list[str] = field(default_factory=list)  # baseline adopted (recorded, not executed)
    already_applied: list[str] = field(default_factory=list)  # present in ledger before this run


def discover_migrations(migrations_dir: Path | None = None) -> list[tuple[str, Path]]:
    """Return (version, path) for every well-formed migration file, in version order.

    Raises if versions are not unique or not contiguous from 0001 — a malformed
    migration set is a deploy-time error, not something to silently apply partially.
    """

    directory = migrations_dir or _MIGRATIONS_DIR
    found: list[tuple[str, Path]] = []
    for path in sorted(directory.glob("*.sql")):
        match = _MIGRATION_FILE_RE.match(path.name)
        if match:
            found.append((path.stem, path))
    found.sort(key=lambda item: item[0])
    numbers = [int(version[:4]) for version, _ in found]
    if len(set(numbers)) != len(numbers):
        raise ValueError(f"duplicate migration version numbers in {directory}: {numbers}")
    if numbers and numbers != list(range(1, len(numbers) + 1)):
        raise ValueError(f"migration versions must be contiguous from 0001 in {directory}: {numbers}")
    return found


def _checksum(sql_text: str) -> str:
    return hashlib.sha256(sql_text.encode("utf-8")).hexdigest()


def _table_exists(cursor: Any, schema: str, table_name: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s LIMIT 1",
        (schema, table_name),
    )
    return cursor.fetchone() is not None


def apply_pending_migrations(
    connection: Any,
    *,
    schema: str,
    migrations_dir: Path | None = None,
) -> MigrationResult:
    """Apply every pending migration to ``schema`` on ``connection`` (one atomic transaction)."""

    normalized_schema = str(schema or "").strip() or "public"
    quoted_schema = quote_control_plane_postgres_identifier(normalized_schema)
    lock_key = f"{normalized_schema}:schema_migrations"
    files = discover_migrations(migrations_dir)
    result = MigrationResult(schema=normalized_schema)

    try:
        with connection.cursor() as cursor:
            # Serialize concurrent runners for the lifetime of this transaction.
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lock_key,))
            cursor.execute(f"SET LOCAL search_path TO {quoted_schema}")
            cursor.execute(_SCHEMA_MIGRATIONS_DDL)
            cursor.execute("SELECT version, checksum FROM schema_migrations")
            ledger = {str(row[0]): str(row[1]) for row in cursor.fetchall()}
            result.already_applied = sorted(ledger)

            # Brownfield adoption: a pre-runner schema already has the baseline tables but
            # no ledger. Record the baseline as applied without re-running its CREATE TABLEs.
            if not ledger and _table_exists(cursor, normalized_schema, _BASELINE_SENTINEL_TABLE):
                baseline_path = next((path for version, path in files if version == _BASELINE_VERSION), None)
                if baseline_path is not None:
                    checksum = _checksum(baseline_path.read_text(encoding="utf-8"))
                    cursor.execute(
                        "INSERT INTO schema_migrations (version, checksum) VALUES (%s, %s) "
                        "ON CONFLICT (version) DO NOTHING",
                        (_BASELINE_VERSION, checksum),
                    )
                    ledger[_BASELINE_VERSION] = checksum
                    result.stamped.append(_BASELINE_VERSION)

            for version, path in files:
                sql_text = path.read_text(encoding="utf-8")
                checksum = _checksum(sql_text)
                if version in ledger:
                    if ledger[version] != checksum and version not in result.stamped:
                        raise MigrationChecksumError(
                            f"migration {version} was already applied with a different checksum "
                            f"(ledger={ledger[version]!r}, file={checksum!r}); applied migrations "
                            f"must be byte-stable — add a new migration instead of editing this one."
                        )
                    continue
                cursor.execute(sql_text)
                cursor.execute(
                    "INSERT INTO schema_migrations (version, checksum) VALUES (%s, %s)",
                    (version, checksum),
                )
                result.applied.append(version)
    except BaseException:
        connection.rollback()
        raise
    connection.commit()
    return result
