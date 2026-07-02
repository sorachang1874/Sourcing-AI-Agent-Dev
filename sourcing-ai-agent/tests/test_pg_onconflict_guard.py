"""Guard: every ON CONFLICT target must be backed by the PG bootstrap.

``InvalidColumnReference`` in postgres_only mode (e.g. the historical
``criteria_patterns`` defect: SQLite declared
``UNIQUE(target_company, pattern_type, subject, value)`` but the Postgres
bootstrap never created a matching unique index) happens whenever code issues
``INSERT ... ON CONFLICT (<columns>)`` against a Postgres table that has no
unique index / primary key on exactly those columns.

This module mechanically re-derives the complete ON CONFLICT target list from

- ``sourcing_agent/storage.py`` (literal SQL, ``upsert_row_with_generated_id``
  call sites, ``_upsert_simple_control_plane_row`` call sites),
- ``sourcing_agent/control_plane_postgres.py`` and
  ``sourcing_agent/control_plane_live_postgres.py`` (literal SQL and the
  ``_PRIMARY_KEY_COLUMNS`` conflict-target map used by ``upsert_row`` /
  ``bulk_upsert_rows``),

and asserts every target set has a matching unique constraint in the Postgres
bootstrap, where bootstrap uniqueness comes from

1. primary keys carried over from the SQLite DDL by ``_build_create_table_sql``
   (the snapshot/direct sync paths derive PG PKs from SQLite ``pk`` columns —
   SQLite ``UNIQUE(...)`` constraints are NOT carried over),
2. ``_CONTROL_PLANE_UNIQUE_INDEXES`` (non-partial entries only: a partial
   unique index cannot serve a plain ``ON CONFLICT (cols)`` inference),
3. literal non-partial ``CREATE UNIQUE INDEX`` statements in the two
   control-plane modules,
4. literal ``CREATE TABLE`` primary keys in the live module — counted only
   when they cannot be pre-empted by a SQLite-synced table of the same name
   with a different primary key,
5. the acquisition shard registry split schema (``shard_key``).

If you add a new ``ON CONFLICT (<columns>)`` without a matching unique index
in the bootstrap, this test fails and tells you what to add.

A second test class exercises the bootstrap dedupe path end-to-end against a
real Postgres schema: duplicates that pre-date a unique index must be removed
deterministically (newest row kept) before the index is created, and an index
without a configured recency policy must fail loudly instead of guessing.
"""

from __future__ import annotations

import ast
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import sourcing_agent.control_plane_live_postgres as control_plane_live_postgres
import sourcing_agent.control_plane_postgres as control_plane_postgres
import sourcing_agent.storage as storage_module
from sourcing_agent.control_plane_postgres import (
    ACQUISITION_SHARD_REGISTRY_COLUMNS,
    ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE,
    ACQUISITION_SHARD_REGISTRY_SPLIT_TABLES,
    _CONTROL_PLANE_UNIQUE_INDEXES,
    _CONTROL_PLANE_UNIQUE_INDEX_DEDUPE_RECENCY_SQL,
    _ensure_control_plane_unique_indexes,
)
from sourcing_agent.control_plane_live_postgres import _PRIMARY_KEY_COLUMNS

from tests.pg_store_fixture import enter_pg_store_fixture
from tests.pg_durable_runtime import psycopg

STORAGE_PATH = Path(storage_module.__file__)
CONTROL_PLANE_PATH = Path(control_plane_postgres.__file__)
CONTROL_PLANE_LIVE_PATH = Path(control_plane_live_postgres.__file__)

_ON_CONFLICT_RE = re.compile(r"ON CONFLICT\s*\(([^)]*)\)", re.IGNORECASE)
_INSERT_INTO_RE = re.compile(r"INSERT (?:OR \w+ )?INTO\s+([A-Za-z_{][\w{}]*)", re.IGNORECASE)
_CREATE_UNIQUE_INDEX_RE = re.compile(
    r"CREATE UNIQUE INDEX(?:\s+IF NOT EXISTS)?\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)(\s*WHERE)?",
    re.IGNORECASE,
)
_CREATE_TABLE_RE = re.compile(r"CREATE TABLE(?:\s+IF NOT EXISTS)?\s+(\w+)\s*\(", re.IGNORECASE)
_PK_CLAUSE_RE = re.compile(r"PRIMARY KEY\s*\(([^)]+)\)", re.IGNORECASE)
_PK_INLINE_RE = re.compile(r"^\s*\"?(\w+)\"?\s+\w+[^,()]*?PRIMARY KEY", re.IGNORECASE | re.MULTILINE)

# How far back (characters) we look for the INSERT INTO that owns an
# ON CONFLICT clause. Large enough for the longest column list in the repo.
_INSERT_LOOKBACK_CHARS = 6000


def _split_columns(raw: str) -> tuple[str, ...]:
    return tuple(part.strip().strip('"') for part in raw.split(",") if part.strip())


def _extract_balanced_create_table_blocks(source: str) -> list[tuple[str, str]]:
    """Return (table_name, paren_body) for every literal CREATE TABLE."""

    blocks: list[tuple[str, str]] = []
    for match in _CREATE_TABLE_RE.finditer(source):
        depth = 1
        position = match.end()
        while position < len(source) and depth > 0:
            char = source[position]
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            position += 1
        blocks.append((match.group(1), source[match.end() : position - 1]))
    return blocks


_BASELINE_SQL_PATH = Path(control_plane_live_postgres.__file__).parent / "migrations" / "0001_baseline.sql"
_BASELINE_PK_RE = re.compile(
    r"ALTER TABLE ONLY\s+\"?(\w+)\"?\s+ADD CONSTRAINT\s+\w+\s+PRIMARY KEY\s*\(([^)]+)\)",
    re.IGNORECASE | re.DOTALL,
)
_BASELINE_UNIQUE_INDEX_RE = re.compile(
    r"CREATE UNIQUE INDEX\s+\w+\s+ON\s+\"?(\w+)\"?\s+USING\s+\w+\s*\(([^)]+)\)(\s*WHERE)?",
    re.IGNORECASE,
)


def derive_baseline_unique_sets() -> dict[str, set[frozenset[str]]]:
    """table -> unique column sets from migrations/0001_baseline.sql.

    B4.3f: storage.py carries no SQLite DDL anymore — the versioned migration
    baseline is the sole schema source for the normal runtime tables, so the
    guard derives primary keys and (non-partial) unique indexes directly from
    it. Legacy migration-only tables live as literal CREATE TABLE DDL in
    control_plane_live_postgres.py and are picked up separately.
    """

    source = _BASELINE_SQL_PATH.read_text(encoding="utf-8")
    unique_sets: dict[str, set[frozenset[str]]] = {}
    for match in _BASELINE_PK_RE.finditer(source):
        unique_sets.setdefault(match.group(1), set()).add(frozenset(_split_columns(match.group(2))))
    for match in _BASELINE_UNIQUE_INDEX_RE.finditer(source):
        if match.group(3):
            continue  # partial index cannot serve a plain ON CONFLICT (cols)
        unique_sets.setdefault(match.group(1), set()).add(frozenset(_split_columns(match.group(2))))
    return unique_sets


def extract_literal_on_conflict_targets(source: str, label: str) -> list[tuple[str, int, str, tuple[str, ...]]]:
    """(label, line, table, columns) for every literal ON CONFLICT (cols).

    Dynamic targets (f-string table names or column lists) are skipped here;
    they are covered by the call-site/AST extractions below.
    """

    targets: list[tuple[str, int, str, tuple[str, ...]]] = []
    for match in _ON_CONFLICT_RE.finditer(source):
        raw_columns = match.group(1)
        line_number = source[: match.start()].count("\n") + 1
        window = source[max(0, match.start() - _INSERT_LOOKBACK_CHARS) : match.start()]
        inserts = list(_INSERT_INTO_RE.finditer(window))
        table_name = inserts[-1].group(1).strip() if inserts else ""
        if "{" in raw_columns or "{" in table_name or not raw_columns.strip() or not table_name:
            continue
        targets.append((label, line_number, table_name, _split_columns(raw_columns)))
    return targets


def extract_generated_id_upsert_targets() -> list[tuple[int, str, tuple[str, ...]]]:
    """(line, table, conflict_columns) for every upsert_row_with_generated_id call."""

    tree = ast.parse(STORAGE_PATH.read_text(encoding="utf-8"))
    targets: list[tuple[int, str, tuple[str, ...]]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        first = node.args[0]
        if not (isinstance(first, ast.Constant) and first.value == "upsert_row_with_generated_id"):
            continue
        keywords = {keyword.arg: keyword.value for keyword in node.keywords}
        table_node = keywords.get("table_name")
        columns_node = keywords.get("conflict_columns")
        if not isinstance(table_node, ast.Constant) or not isinstance(columns_node, (ast.List, ast.Tuple)):
            raise AssertionError(
                f"storage.py line {node.lineno}: upsert_row_with_generated_id must pass literal "
                "table_name and conflict_columns so this guard can verify the unique index."
            )
        columns = tuple(
            element.value for element in columns_node.elts if isinstance(element, ast.Constant)
        )
        targets.append((node.lineno, str(table_node.value), columns))
    return targets


def extract_simple_upsert_targets() -> list[tuple[int, str, tuple[str, ...]]]:
    """(line, table, (id_column,)) for every _upsert_simple_control_plane_row call."""

    tree = ast.parse(STORAGE_PATH.read_text(encoding="utf-8"))
    targets: list[tuple[int, str, tuple[str, ...]]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if name != "_upsert_simple_control_plane_row":
            continue
        keywords = {keyword.arg: keyword.value for keyword in node.keywords}
        table_node = keywords.get("table_name") or (node.args[0] if node.args else None)
        id_node = keywords.get("id_column") or (node.args[1] if len(node.args) > 1 else None)
        if not isinstance(table_node, ast.Constant) or not isinstance(id_node, ast.Constant):
            raise AssertionError(
                f"storage.py line {node.lineno}: _upsert_simple_control_plane_row must pass literal "
                "table_name and id_column so this guard can verify the unique index."
            )
        targets.append((node.lineno, str(table_node.value), (str(id_node.value),)))
    return targets


def derive_pg_bootstrap_unique_sets() -> dict[str, set[frozenset[str]]]:
    """table -> set of column sets with a unique constraint after PG bootstrap."""

    unique_sets: dict[str, set[frozenset[str]]] = {}

    def add(table_name: str, columns: tuple[str, ...] | frozenset[str]) -> None:
        if columns:
            unique_sets.setdefault(table_name, set()).add(frozenset(columns))

    # 1. Primary keys + non-partial unique indexes from the versioned migration
    #    baseline (B4.3f: the sole schema source for the normal runtime tables).
    for table_name, column_sets in derive_baseline_unique_sets().items():
        for columns in column_sets:
            add(table_name, columns)

    # 2. Bootstrap unique indexes (non-partial only).
    for table_name, entries in _CONTROL_PLANE_UNIQUE_INDEXES.items():
        for _index_name, columns, where_sql in entries:
            if not str(where_sql or "").strip():
                add(table_name, tuple(columns))

    # 3. Literal non-partial CREATE UNIQUE INDEX in the control-plane modules.
    for source_path in (CONTROL_PLANE_PATH, CONTROL_PLANE_LIVE_PATH):
        source = source_path.read_text(encoding="utf-8")
        for match in _CREATE_UNIQUE_INDEX_RE.finditer(source):
            if match.group(4):
                continue  # partial index cannot serve a plain ON CONFLICT (cols)
            add(match.group(2), _split_columns(match.group(3)))

    # 4. Literal CREATE TABLE primary keys in the live module (runtime
    #    coordination + writer schema + the legacy migration-only tables,
    #    all created by native PG DDL post-B4.3f).
    live_source = CONTROL_PLANE_LIVE_PATH.read_text(encoding="utf-8")
    for table_name, body in _extract_balanced_create_table_blocks(live_source):
        pk_clause = _PK_CLAUSE_RE.search(body)
        if pk_clause:
            pk_columns = _split_columns(pk_clause.group(1))
        else:
            inline = _PK_INLINE_RE.search(body)
            pk_columns = (inline.group(1),) if inline else ()
        if not pk_columns:
            continue
        add(table_name, pk_columns)

    # 5. Acquisition shard registry split schema (logical view + split tables).
    shard_pk = tuple(
        str(column.get("name") or "")
        for column in ACQUISITION_SHARD_REGISTRY_COLUMNS
        if int(column.get("pk_position") or 0) > 0
    )
    for table_name in (ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE, *ACQUISITION_SHARD_REGISTRY_SPLIT_TABLES):
        add(table_name, shard_pk)

    return unique_sets


def collect_all_conflict_targets() -> list[tuple[str, str, tuple[str, ...]]]:
    """(origin, table, columns) for every ON CONFLICT target that can hit PG."""

    targets: list[tuple[str, str, tuple[str, ...]]] = []
    for path, label in (
        (STORAGE_PATH, "storage.py"),
        (CONTROL_PLANE_PATH, "control_plane_postgres.py"),
        (CONTROL_PLANE_LIVE_PATH, "control_plane_live_postgres.py"),
    ):
        for file_label, line_number, table_name, columns in extract_literal_on_conflict_targets(
            path.read_text(encoding="utf-8"), label
        ):
            targets.append((f"{file_label}:{line_number}", table_name, columns))
    for line_number, table_name, columns in extract_generated_id_upsert_targets():
        targets.append((f"storage.py:{line_number} (upsert_row_with_generated_id)", table_name, columns))
    for line_number, table_name, columns in extract_simple_upsert_targets():
        targets.append((f"storage.py:{line_number} (_upsert_simple_control_plane_row)", table_name, columns))
    for table_name, pk_columns in _PRIMARY_KEY_COLUMNS.items():
        targets.append(("_PRIMARY_KEY_COLUMNS (upsert_row/bulk_upsert_rows)", table_name, tuple(pk_columns)))
    return targets


class OnConflictBootstrapGuardTest(unittest.TestCase):
    maxDiff = None

    def test_every_on_conflict_target_has_a_pg_unique_constraint(self) -> None:
        targets = collect_all_conflict_targets()
        unique_sets = derive_pg_bootstrap_unique_sets()

        # Extraction sentinels: if these disappear the extraction itself broke.
        target_keys = {(table, frozenset(columns)) for _origin, table, columns in targets}
        self.assertIn(
            ("criteria_patterns", frozenset({"target_company", "pattern_type", "subject", "value"})),
            target_keys,
            "Extraction no longer sees the criteria_patterns ON CONFLICT target; "
            "fix the extraction before trusting this guard.",
        )
        self.assertGreaterEqual(
            len(target_keys),
            50,
            f"Suspiciously few ON CONFLICT targets extracted ({len(target_keys)}); "
            "the extraction in this guard has likely rotted.",
        )

        known_tables = set(unique_sets)
        unknown_tables = sorted(
            {
                f"{table} (from {origin})"
                for origin, table, _columns in targets
                if table not in known_tables
            }
        )
        self.assertEqual(
            unknown_tables,
            [],
            "ON CONFLICT extraction attributed statements to tables unknown to the PG bootstrap "
            "schema derivation. Either the INSERT lookback heuristic misfired or a new table is "
            f"missing bootstrap DDL: {unknown_tables}",
        )

        failures: list[str] = []
        for origin, table_name, columns in sorted(set(targets)):
            if frozenset(columns) not in unique_sets.get(table_name, set()):
                available = sorted(sorted(column_set) for column_set in unique_sets.get(table_name, set()))
                failures.append(
                    f"- {origin}: ON CONFLICT ({', '.join(columns)}) on {table_name} has no matching "
                    f"unique index/primary key in the PG bootstrap (available unique sets: {available})"
                )
        self.assertEqual(
            failures,
            [],
            "ON CONFLICT targets without a PG bootstrap unique constraint — add a matching entry to "
            "_CONTROL_PLANE_UNIQUE_INDEXES (with a dedupe recency policy in "
            "_CONTROL_PLANE_UNIQUE_INDEX_DEDUPE_RECENCY_SQL) in sourcing_agent/control_plane_postgres.py:\n"
            + "\n".join(failures),
        )

    def test_every_bootstrap_unique_index_has_a_dedupe_recency_policy(self) -> None:
        missing = [
            index_name
            for entries in _CONTROL_PLANE_UNIQUE_INDEXES.values()
            for index_name, _columns, _where_sql in entries
            if not str(_CONTROL_PLANE_UNIQUE_INDEX_DEDUPE_RECENCY_SQL.get(index_name) or "").strip()
        ]
        self.assertEqual(
            missing,
            [],
            "Every unique index in _CONTROL_PLANE_UNIQUE_INDEXES needs a deterministic dedupe "
            "recency policy so bootstrap can clean pre-existing duplicates instead of failing "
            f"CREATE UNIQUE INDEX. Missing: {missing}",
        )


class UniqueIndexDedupeBootstrapTest(unittest.TestCase):
    """Exercises the dedupe-then-index bootstrap path against real Postgres."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.fixture = enter_pg_store_fixture(
            runtime_dir=self.tempdir.name, schema_label="onconflict_guard"
        )
        self.addCleanup(self.fixture.__exit__, None, None, None)

    def _connect(self):
        connection = psycopg.connect(
            self.fixture.dsn, autocommit=True, connect_timeout=5, client_encoding="utf8"
        )
        connection.execute(f"SET search_path TO {self.fixture.schema}")
        return connection

    def test_bootstrap_dedupes_keeping_newest_then_creates_unique_index(self) -> None:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE criteria_patterns (
                    pattern_id BIGINT PRIMARY KEY,
                    target_company TEXT,
                    pattern_type TEXT NOT NULL,
                    subject TEXT,
                    value TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT,
                    updated_at TEXT
                )
                """
            )
            cursor.executemany(
                "INSERT INTO criteria_patterns "
                "(pattern_id, target_company, pattern_type, subject, value, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [
                    (1, "xAI", "alias", "RL", "reinforcement learning", "2026-01-01", "2026-01-01"),
                    (2, "xAI", "alias", "RL", "reinforcement learning", "2026-01-02", "2026-06-01"),
                    (3, "xAI", "alias", "RL", "reinforcement learning", "2026-01-03", "2026-03-01"),
                    (4, "xAI", "must_signal", "research", "research engineer", "2026-01-04", "2026-01-04"),
                ],
            )
            summary = _ensure_control_plane_unique_indexes(cursor, "criteria_patterns")

            self.assertIn("idx_criteria_patterns_identity_unique", summary)
            dedupe = summary["idx_criteria_patterns_identity_unique"]
            self.assertEqual(dedupe["removed_row_count"], 2)
            self.assertEqual(dedupe["duplicate_group_count"], 1)
            self.assertEqual(
                dedupe["conflict_columns"], ["target_company", "pattern_type", "subject", "value"]
            )
            self.assertTrue(dedupe["sample_groups"])

            cursor.execute("SELECT pattern_id FROM criteria_patterns ORDER BY pattern_id")
            surviving_ids = [row[0] for row in cursor.fetchall()]
            self.assertEqual(surviving_ids, [2, 4], "newest row per duplicate group must survive")

            cursor.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = %s",
                (self.fixture.schema, "criteria_patterns"),
            )
            index_names = {row[0] for row in cursor.fetchall()}
            self.assertIn("idx_criteria_patterns_identity_unique", index_names)

            # The unique contract is now enforced by Postgres itself.
            cursor.execute(
                """
                INSERT INTO criteria_patterns
                    (pattern_id, target_company, pattern_type, subject, value, created_at, updated_at)
                VALUES (9, 'xAI', 'alias', 'RL', 'reinforcement learning', '2026-06-11', '2026-06-11')
                ON CONFLICT (target_company, pattern_type, subject, value)
                DO UPDATE SET updated_at = EXCLUDED.updated_at
                """
            )
            cursor.execute("SELECT COUNT(*) FROM criteria_patterns WHERE pattern_type = 'alias'")
            self.assertEqual(cursor.fetchone()[0], 1)

            # Idempotent: a second bootstrap pass has nothing left to dedupe.
            self.assertEqual(_ensure_control_plane_unique_indexes(cursor, "criteria_patterns"), {})

    def test_bootstrap_fails_loudly_when_duplicates_have_no_recency_policy(self) -> None:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                "CREATE TABLE guard_no_recency (row_id BIGINT PRIMARY KEY, dedupe_key TEXT)"
            )
            cursor.executemany(
                "INSERT INTO guard_no_recency (row_id, dedupe_key) VALUES (%s, %s)",
                [(1, "dup"), (2, "dup")],
            )
            with mock.patch.dict(
                _CONTROL_PLANE_UNIQUE_INDEXES,
                {"guard_no_recency": (("idx_guard_no_recency_key_unique", ("dedupe_key",), ""),)},
            ):
                with self.assertRaises(RuntimeError) as raised:
                    _ensure_control_plane_unique_indexes(cursor, "guard_no_recency")
            message = str(raised.exception)
            self.assertIn("guard_no_recency", message)
            self.assertIn("_CONTROL_PLANE_UNIQUE_INDEX_DEDUPE_RECENCY_SQL", message)
            # No rows were deleted and the index was not created.
            cursor.execute("SELECT COUNT(*) FROM guard_no_recency")
            self.assertEqual(cursor.fetchone()[0], 2)
            cursor.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = %s",
                (self.fixture.schema, "guard_no_recency"),
            )
            index_names = {row[0] for row in cursor.fetchall()}
            self.assertNotIn("idx_guard_no_recency_key_unique", index_names)


if __name__ == "__main__":
    unittest.main()
