import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Literal
from unittest import mock

from sourcing_agent.control_plane_postgres import (
    ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE,
    ACQUISITION_SHARD_REGISTRY_FORMER_TABLE,
    ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE,
    DEFAULT_CONTROL_PLANE_TABLES,
    GENERIC_POSTGRES_IMPORT_EXCLUDED_TABLES,
    NONPORTABLE_RUNTIME_COORDINATION_TABLES,
    _connect_postgres,
    control_plane_postgres_sync_state_path,
    ensure_acquisition_shard_registry_split_schema,
    export_control_plane_snapshot,
    restore_control_plane_snapshot_to_sqlite,
    sync_control_plane_snapshot_to_postgres,
    sync_runtime_control_plane_to_postgres,
    upsert_acquisition_shard_registry_rows,
)
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin, psycopg

_PG_ONLY_CAUSAL_AGGREGATE_TABLES = (
    "workflow_commands",
    "workflow_events",
    "workflow_current_state",
    "runtime_outbox",
    "agent_actions",
    "operation_runs",
    "agent_tool_result_slots",
    "agent_tool_result_attempts",
    "agent_tool_result_journal",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    "operation_events",
)
_NONPORTABLE_COORDINATION_TABLES = (
    "workflow_job_leases",
    "workflow_recovery_intents",
    "runtime_provider_limiter_leases",
)
_EXPECTED_EXCLUSION_GAP = sorted(GENERIC_POSTGRES_IMPORT_EXCLUDED_TABLES)


class _FakeCursor:
    def __init__(self, *, row_counts: dict[str, int] | None = None) -> None:
        self.executed: list[str] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.row_counts = dict(row_counts or {})
        self._fetchone_result: tuple[object, ...] | None = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        return False

    def execute(self, sql: str) -> None:
        self.executed.append(sql)
        if sql.startswith('SELECT COUNT(*) AS row_count FROM "'):
            table_name = str(sql.split('"')[1] or "")
            self._fetchone_result = (int(self.row_counts.get(table_name, 0)),)
        else:
            self._fetchone_result = None

    def executemany(self, sql: str, values: list[tuple[object, ...]]) -> None:
        self.executemany_calls.append((sql, values))

    def fetchone(self) -> tuple[object, ...] | None:
        return self._fetchone_result


class _FakeConnection:
    def __init__(self, dsn: str, *, row_counts: dict[str, int] | None = None) -> None:
        self.dsn = dsn
        self.cursor_instance = _FakeCursor(row_counts=row_counts)
        self.committed = False
        self.commit_calls = 0

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        return False

    def cursor(self) -> _FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.committed = True
        self.commit_calls += 1


class _FakePsycopg:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def connect(self, dsn: str, **kwargs: object) -> _FakeConnection:
        self.calls.append((dsn, dict(kwargs)))
        return _FakeConnection(dsn)


class _FakeSnapshotPostgresCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []
        self.description: list[tuple[str]] = []
        self._rows: list[object] = []
        self._offset = 0

    def __enter__(self) -> "_FakeSnapshotPostgresCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        return False

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.executed.append((sql, params))
        self._offset = 0
        normalized = " ".join(sql.split())
        if "FROM information_schema.tables" in normalized and "LIMIT 1" in normalized:
            table_name = str((params or ("",))[0] or "")
            self.description = [("present",)]
            self._rows = [(1,)] if table_name == "jobs" else []
            return
        if "FROM pg_attribute AS attribute" in normalized:
            self.description = [
                ("name",),
                ("type",),
                ("notnull",),
                ("default",),
                ("pk_position",),
            ]
            self._rows = [
                {
                    "name": "job_id",
                    "type": "text",
                    "notnull": 1,
                    "default": None,
                    "pk_position": 1,
                },
                {
                    "name": "status",
                    "type": "text",
                    "notnull": 1,
                    "default": None,
                    "pk_position": 0,
                },
            ]
            return
        if normalized.startswith('SELECT COUNT(*) AS row_count FROM "jobs"'):
            self.description = [("row_count",)]
            self._rows = [(1,)]
            return
        if normalized.startswith('SELECT * FROM "jobs"'):
            self.description = [("job_id",), ("status",)]
            self._rows = [("job-pg", "completed")]
            return
        if "FROM information_schema.tables" in normalized:
            self.description = [("table_name",)]
            self._rows = [("jobs",)]
            return
        self.description = []
        self._rows = []

    def fetchall(self) -> list[object]:
        return list(self._rows)

    def fetchmany(self, size: int) -> list[object]:
        if self._offset >= len(self._rows):
            return []
        next_offset = min(len(self._rows), self._offset + max(1, size))
        chunk = self._rows[self._offset : next_offset]
        self._offset = next_offset
        return list(chunk)

    def fetchone(self) -> object | None:
        return self._rows[0] if self._rows else None


class _FakeSnapshotPostgresConnection:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.cursor_instance = _FakeSnapshotPostgresCursor()

    def __enter__(self) -> "_FakeSnapshotPostgresConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        return False

    def cursor(self) -> _FakeSnapshotPostgresCursor:
        return self.cursor_instance


class ControlPlanePostgresTest(unittest.TestCase):
    def test_default_generic_import_inventory_excludes_workflow_commands(self) -> None:
        self.assertNotIn("workflow_commands", DEFAULT_CONTROL_PLANE_TABLES)

    def test_generic_import_exclusion_covers_the_complete_pg_only_causal_aggregate(self) -> None:
        self.assertEqual(
            GENERIC_POSTGRES_IMPORT_EXCLUDED_TABLES,
            frozenset(_PG_ONLY_CAUSAL_AGGREGATE_TABLES) | frozenset(_NONPORTABLE_COORDINATION_TABLES),
        )
        for table_name in _PG_ONLY_CAUSAL_AGGREGATE_TABLES:
            self.assertNotIn(table_name, DEFAULT_CONTROL_PLANE_TABLES)

    def test_portability_registry_excludes_nonportable_coordination_tables_from_default_inventory(self) -> None:
        self.assertEqual(
            NONPORTABLE_RUNTIME_COORDINATION_TABLES,
            frozenset(_NONPORTABLE_COORDINATION_TABLES),
        )
        self.assertTrue(NONPORTABLE_RUNTIME_COORDINATION_TABLES.issubset(GENERIC_POSTGRES_IMPORT_EXCLUDED_TABLES))
        for table_name in _NONPORTABLE_COORDINATION_TABLES:
            self.assertNotIn(table_name, DEFAULT_CONTROL_PLANE_TABLES)

    def _snapshot_payload(
        self,
        table_rows: dict[str, list[dict[str, object]]],
        *,
        declaration: object = "default",
        schema_version: object = 1,
    ) -> dict[str, object]:
        tables: dict[str, object] = {}
        for table_name, rows in table_rows.items():
            columns = [
                {"name": column, "type": "TEXT", "notnull": 0, "default": "", "pk_position": index == 0}
                for index, column in enumerate(rows[0] if rows else ("placeholder",))
            ]
            tables[table_name] = {"columns": columns, "row_count": len(rows), "rows": rows}
        payload: dict[str, object] = {"tables": tables}
        if schema_version != "omit":
            payload["schema_version"] = schema_version
        if declaration == "default":
            payload["excluded_pg_only_durable_runtime_tables"] = _EXPECTED_EXCLUSION_GAP
        elif declaration != "omit":
            payload["excluded_pg_only_durable_runtime_tables"] = declaration
        return payload

    def test_default_export_excludes_aggregate_and_records_typed_explicit_gap(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("CREATE TABLE workflow_events (event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL)")
            connection.execute("CREATE TABLE operation_runs (operation_run_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", ("job-1", "completed"))
            connection.execute(
                "INSERT INTO workflow_events (event_id, event_type) VALUES (?, ?)",
                ("evt-1", "CommandPlanRequested"),
            )
            connection.execute(
                "INSERT INTO operation_runs (operation_run_id, status) VALUES (?, ?)",
                ("op-1", "queued"),
            )
            connection.commit()
            connection.close()
            output_path = Path(tempdir) / "snapshot.json"

            result = export_control_plane_snapshot(
                runtime_dir=runtime_dir,
                output_path=output_path,
                source_backend="sqlite",
            )

            expected_gap = _EXPECTED_EXCLUSION_GAP
            self.assertEqual(result["status"], "exported")
            self.assertEqual(result["excluded_pg_only_durable_runtime_tables"], expected_gap)
            self.assertIn("jobs", result["tables"])
            for table_name in _PG_ONLY_CAUSAL_AGGREGATE_TABLES:
                self.assertNotIn(table_name, result["tables"])
            snapshot_payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(snapshot_payload["excluded_pg_only_durable_runtime_tables"], expected_gap)
            self.assertIn("jobs", snapshot_payload["tables"])
            for table_name in _PG_ONLY_CAUSAL_AGGREGATE_TABLES:
                self.assertNotIn(table_name, snapshot_payload["tables"])

    def test_include_all_export_filters_aggregate_out_of_inventory_driven_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("CREATE TABLE workflow_commands (command_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("CREATE TABLE operation_events (event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL)")
            connection.commit()
            connection.close()
            output_path = Path(tempdir) / "snapshot.json"

            result = export_control_plane_snapshot(
                runtime_dir=runtime_dir,
                output_path=output_path,
                source_backend="sqlite",
                include_all_sqlite_tables=True,
            )

            self.assertEqual(result["status"], "exported")
            self.assertEqual(set(result["tables"]), {"jobs"})
            snapshot_payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(set(snapshot_payload["tables"]), {"jobs"})
            self.assertEqual(
                snapshot_payload["excluded_pg_only_durable_runtime_tables"],
                _EXPECTED_EXCLUSION_GAP,
            )

    def test_explicit_export_of_durable_runtime_tables_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("CREATE TABLE workflow_events (event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL)")
            connection.commit()
            connection.close()

            for table_name in (*_PG_ONLY_CAUSAL_AGGREGATE_TABLES, *_NONPORTABLE_COORDINATION_TABLES):
                with self.subTest(table_name=table_name):
                    with self.assertRaisesRegex(
                        ValueError,
                        f"cannot restore PG-only durable runtime tables: {table_name}",
                    ):
                        export_control_plane_snapshot(
                            runtime_dir=runtime_dir,
                            output_path=Path(tempdir) / f"snapshot-{table_name}.json",
                            source_backend="sqlite",
                            tables=[table_name],
                        )

    def test_include_all_export_filters_nonportable_coordination_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            for table_name in _NONPORTABLE_COORDINATION_TABLES:
                connection.execute(f'CREATE TABLE "{table_name}" (lease_key TEXT PRIMARY KEY, lease_owner TEXT)')
                connection.execute(
                    f'INSERT INTO "{table_name}" (lease_key, lease_owner) VALUES (?, ?)',
                    ("held-lease", "daemon-A"),
                )
            connection.commit()
            connection.close()
            output_path = Path(tempdir) / "snapshot.json"

            result = export_control_plane_snapshot(
                runtime_dir=runtime_dir,
                output_path=output_path,
                source_backend="sqlite",
                include_all_sqlite_tables=True,
            )

            self.assertEqual(result["status"], "exported")
            self.assertEqual(set(result["tables"]), {"jobs"})
            snapshot_payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(set(snapshot_payload["tables"]), {"jobs"})
            for table_name in _NONPORTABLE_COORDINATION_TABLES:
                self.assertNotIn(table_name, snapshot_payload["tables"])

    def test_snapshot_import_rejects_each_durable_runtime_table_before_postgres(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            for table_name in (*_PG_ONLY_CAUSAL_AGGREGATE_TABLES, *_NONPORTABLE_COORDINATION_TABLES):
                snapshot_path = Path(tempdir) / f"snapshot-{table_name}.json"
                snapshot_path.write_text(
                    json.dumps(
                        self._snapshot_payload({table_name: [{"aggregate_key": "held-1", "status": "held"}]}),
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                with self.subTest(table_name=table_name):
                    with (
                        mock.patch("sourcing_agent.control_plane_postgres._import_psycopg") as import_psycopg,
                        mock.patch("sourcing_agent.control_plane_postgres._connect_postgres") as connect_postgres,
                        self.assertRaisesRegex(
                            ValueError,
                            f"cannot restore PG-only durable runtime tables: {table_name}",
                        ),
                    ):
                        sync_control_plane_snapshot_to_postgres(
                            snapshot_path=snapshot_path,
                            dsn="postgresql://user:pass@localhost:5432/sourcing",
                            truncate_first=True,
                        )
                    import_psycopg.assert_not_called()
                    connect_postgres.assert_not_called()

    def test_sqlite_restore_rejects_partial_aggregate_that_cannot_pass_as_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            # A snapshot carrying command children without their commands is a
            # partial aggregate; the SQLite restore boundary must reject it
            # rather than materialize an incoherent mirror.
            partial_snapshot_path = Path(tempdir) / "partial.json"
            partial_snapshot_path.write_text(
                json.dumps(
                    self._snapshot_payload(
                        {
                            "jobs": [{"job_id": "job-1", "status": "completed"}],
                            "operation_events": [{"event_id": "evt-1", "event_type": "ActionApproved"}],
                        }
                    ),
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ValueError,
                "cannot restore PG-only durable runtime tables: operation_events",
            ):
                restore_control_plane_snapshot_to_sqlite(
                    snapshot_path=partial_snapshot_path,
                    runtime_dir=runtime_dir,
                )

            portable_snapshot_path = Path(tempdir) / "portable.json"
            portable_snapshot_path.write_text(
                json.dumps(
                    self._snapshot_payload({"jobs": [{"job_id": "job-1", "status": "completed"}]}),
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            restored = restore_control_plane_snapshot_to_sqlite(
                snapshot_path=portable_snapshot_path,
                runtime_dir=runtime_dir,
            )
            self.assertEqual(restored["status"], "restored")
            self.assertEqual(restored["tables"]["jobs"]["row_count"], 1)
            self.assertEqual(restored["excluded_pg_only_durable_runtime_tables"], _EXPECTED_EXCLUSION_GAP)

    def test_generic_import_requires_exact_schema_versioned_exclusion_declaration(self) -> None:
        declaration_cases = (
            ("missing", "omit", 1),
            ("subset", sorted(_PG_ONLY_CAUSAL_AGGREGATE_TABLES), 1),
            ("superset", [*_EXPECTED_EXCLUSION_GAP, "extra_table"], 1),
            ("drifted", ["not_a_registry_table"], 1),
            ("non_list", "workflow_commands", 1),
            ("non_string_items", [*_EXPECTED_EXCLUSION_GAP[:-1], 7], 1),
            ("missing_schema_version", "default", "omit"),
            ("wrong_schema_version", "default", 2),
            ("bool_schema_version", "default", True),
        )
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            for label, declaration, schema_version in declaration_cases:
                snapshot_path = Path(tempdir) / f"snapshot-{label}.json"
                snapshot_path.write_text(
                    json.dumps(
                        self._snapshot_payload(
                            {"jobs": [{"job_id": "job-1", "status": "completed"}]},
                            declaration=declaration,
                            schema_version=schema_version,
                        ),
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                with self.subTest(declaration=label):
                    with (
                        mock.patch("sourcing_agent.control_plane_postgres._import_psycopg") as import_psycopg,
                        mock.patch("sourcing_agent.control_plane_postgres._connect_postgres") as connect_postgres,
                        self.assertRaisesRegex(
                            ValueError,
                            "requires an exact schema-versioned exclusion declaration",
                        ),
                    ):
                        sync_control_plane_snapshot_to_postgres(
                            snapshot_path=snapshot_path,
                            dsn="postgresql://user:pass@localhost:5432/sourcing",
                            truncate_first=True,
                        )
                    import_psycopg.assert_not_called()
                    connect_postgres.assert_not_called()
                    with self.assertRaisesRegex(
                        ValueError,
                        "requires an exact schema-versioned exclusion declaration",
                    ):
                        restore_control_plane_snapshot_to_sqlite(
                            snapshot_path=snapshot_path,
                            runtime_dir=runtime_dir,
                        )
                    self.assertFalse((runtime_dir / "sourcing_agent.db").exists())

    def test_sync_summary_propagates_the_verified_exclusion_gap(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_path = Path(tempdir) / "control-plane.json"
            snapshot_path.write_text(
                json.dumps(
                    self._snapshot_payload({"jobs": [{"job_id": "job-1", "status": "completed"}]}),
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            fake_connections: list[_FakeConnection] = []

            def _connect(dsn: str) -> _FakeConnection:
                connection = _FakeConnection(dsn, row_counts={"jobs": 1})
                fake_connections.append(connection)
                return connection

            fake_psycopg = SimpleNamespace(connect=_connect)
            with mock.patch(
                "sourcing_agent.control_plane_postgres._import_psycopg",
                return_value=fake_psycopg,
            ):
                result = sync_control_plane_snapshot_to_postgres(
                    snapshot_path=snapshot_path,
                    dsn="postgresql://user:pass@localhost:5432/sourcing",
                    truncate_first=True,
                )

            self.assertEqual(result["status"], "synced")
            self.assertEqual(result["excluded_pg_only_durable_runtime_tables"], _EXPECTED_EXCLUSION_GAP)

    def test_runtime_mirror_default_selection_skips_aggregate_but_explicit_selection_rejects(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("CREATE TABLE operation_runs (operation_run_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", ("job-1", "completed"))
            connection.commit()
            connection.close()

            fake_connections: list[_FakeConnection] = []

            def _connect(dsn: str) -> _FakeConnection:
                connection = _FakeConnection(dsn, row_counts={"jobs": 1})
                fake_connections.append(connection)
                return connection

            fake_psycopg = SimpleNamespace(connect=_connect)
            with mock.patch(
                "sourcing_agent.control_plane_postgres._import_psycopg",
                return_value=fake_psycopg,
            ):
                result = sync_runtime_control_plane_to_postgres(
                    runtime_dir=runtime_dir,
                    sqlite_path=db_path,
                    dsn="postgresql://user:pass@localhost:5432/sourcing",
                    force=True,
                )
            self.assertEqual(result["status"], "synced")
            self.assertIn("jobs", result["tables"])
            for table_name in _PG_ONLY_CAUSAL_AGGREGATE_TABLES:
                self.assertNotIn(table_name, result["tables"])
            self.assertEqual(result["postgres"]["tables"]["jobs"]["row_count"], 1)

            for table_name in _PG_ONLY_CAUSAL_AGGREGATE_TABLES:
                with self.subTest(table_name=table_name):
                    with (
                        mock.patch("sourcing_agent.control_plane_postgres._import_psycopg") as import_psycopg,
                        mock.patch("sourcing_agent.control_plane_postgres._connect_postgres") as connect_postgres,
                        self.assertRaisesRegex(
                            ValueError,
                            f"cannot restore PG-only durable runtime tables: {table_name}",
                        ),
                    ):
                        sync_runtime_control_plane_to_postgres(
                            runtime_dir=runtime_dir,
                            sqlite_path=db_path,
                            dsn="postgresql://user:pass@localhost:5432/sourcing",
                            tables=[table_name],
                            force=True,
                        )
                    import_psycopg.assert_not_called()
                    connect_postgres.assert_not_called()

    def test_ensure_acquisition_shard_registry_split_schema_normalizes_legacy_provider_cap_hit(self) -> None:
        class _LegacyCursor:
            def __init__(self) -> None:
                self.executed: list[tuple[str, tuple[object, ...] | None]] = []
                self._fetchone_result: tuple[object, ...] | None = None

            def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
                self.executed.append((sql, params))
                normalized = " ".join(sql.split())
                if "SELECT class_rel.relkind" in normalized:
                    relation_name = str((params or ("",))[0] or "")
                    self._fetchone_result = (
                        ("r",) if relation_name == ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE else None
                    )
                    return
                self._fetchone_result = None

            def fetchone(self) -> tuple[object, ...] | None:
                return self._fetchone_result

        cursor = _LegacyCursor()
        ensure_acquisition_shard_registry_split_schema(cursor)

        insert_sqls = [
            sql
            for sql, _params in cursor.executed
            if sql.startswith(f'INSERT INTO "{ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE}"')
            or sql.startswith(f'INSERT INTO "{ACQUISITION_SHARD_REGISTRY_FORMER_TABLE}"')
        ]
        self.assertEqual(len(insert_sqls), 2)
        self.assertTrue(
            all('lower(trim("provider_cap_hit"::text))' in sql for sql in insert_sqls),
            insert_sqls,
        )
        self.assertTrue(
            any(
                sql == f'DROP TABLE IF EXISTS "{ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE}" CASCADE'
                for sql, _params in cursor.executed
            )
        )

    def test_upsert_acquisition_shard_registry_rows_splits_current_and_former_tables(self) -> None:
        class _SplitCursor:
            def __init__(self) -> None:
                self.executed: list[tuple[str, tuple[object, ...] | None]] = []
                self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
                self._fetchone_result: tuple[object, ...] | None = None

            def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
                self.executed.append((sql, params))
                normalized = " ".join(sql.split())
                if "SELECT class_rel.relkind" in normalized:
                    relation_name = str((params or ("",))[0] or "")
                    self._fetchone_result = ()
                    if relation_name in {
                        ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE,
                        ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE,
                        ACQUISITION_SHARD_REGISTRY_FORMER_TABLE,
                    }:
                        self._fetchone_result = None
                    return
                self._fetchone_result = None

            def executemany(self, sql: str, values: list[tuple[object, ...]]) -> None:
                self.executemany_calls.append((sql, values))

            def fetchone(self) -> tuple[object, ...] | None:
                return self._fetchone_result

        cursor = _SplitCursor()
        summary = upsert_acquisition_shard_registry_rows(
            cursor,
            [
                {
                    "shard_key": "openai_current",
                    "target_company": "OpenAI",
                    "snapshot_id": "20260422T150000",
                    "asset_view": "canonical_merged",
                    "lane": "profile_search",
                    "status": "completed",
                    "employment_scope": "current",
                    "provider_cap_hit": 1,
                },
                {
                    "shard_key": "openai_former",
                    "target_company": "OpenAI",
                    "snapshot_id": "20260422T150000",
                    "asset_view": "canonical_merged",
                    "lane": "profile_search",
                    "status": "completed",
                    "employment_scope": "former",
                    "provider_cap_hit": 0,
                },
            ],
        )

        self.assertEqual(summary["row_count"], 2)
        self.assertEqual(len(cursor.executemany_calls), 2)
        self.assertIn(ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE, cursor.executemany_calls[0][0])
        self.assertIn(ACQUISITION_SHARD_REGISTRY_FORMER_TABLE, cursor.executemany_calls[1][0])
        delete_sqls = [sql for sql, _params in cursor.executed if sql.startswith("DELETE FROM")]
        self.assertTrue(any(ACQUISITION_SHARD_REGISTRY_FORMER_TABLE in sql for sql in delete_sqls))
        self.assertTrue(any(ACQUISITION_SHARD_REGISTRY_CURRENT_TABLE in sql for sql in delete_sqls))
        current_payload = cursor.executemany_calls[0][1][0]
        former_payload = cursor.executemany_calls[1][1][0]
        self.assertIs(current_payload[18], True)
        self.assertIs(former_payload[18], False)

    def test_connect_postgres_disables_gss_for_loopback_dsn(self) -> None:
        fake_psycopg = _FakePsycopg()

        with _connect_postgres("postgresql://tester@127.0.0.1:5432/sourcing_agent", psycopg=fake_psycopg):
            pass

        self.assertEqual(len(fake_psycopg.calls), 1)
        self.assertEqual(
            fake_psycopg.calls[0][0],
            "postgresql://tester@127.0.0.1:5432/sourcing_agent?gssencmode=disable",
        )
        self.assertEqual(fake_psycopg.calls[0][1], {"client_encoding": "utf8"})

    def test_export_control_plane_snapshot_includes_generation_index_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            runtime_dir = root / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute(
                """
                CREATE TABLE jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "INSERT INTO jobs (job_id, status) VALUES (?, ?)",
                ("job-1", "completed"),
            )
            connection.commit()
            connection.close()

            generation_index_path = runtime_dir / "object_sync" / "generation_index.json"
            generation_index_path.parent.mkdir(parents=True, exist_ok=True)
            generation_index_path.write_text(
                json.dumps(
                    {
                        "generations": [
                            {
                                "generation_key": "gen-1",
                                "generation_sequence": 7,
                                "target_company": "Acme",
                                "company_key": "acme",
                                "snapshot_id": "20260419T120000",
                                "asset_view": "canonical_merged",
                            }
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            output_path = runtime_dir / "exports" / "control-plane.json"
            result = export_control_plane_snapshot(
                runtime_dir=runtime_dir,
                output_path=output_path,
                tables=["jobs", "generation_index_entries"],
            )

            self.assertEqual(result["status"], "exported")
            self.assertEqual(result["table_count"], 2)
            snapshot_payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(snapshot_payload["tables"]["jobs"]["row_count"], 1)
            self.assertEqual(snapshot_payload["tables"]["generation_index_entries"]["row_count"], 1)
            self.assertEqual(
                snapshot_payload["tables"]["generation_index_entries"]["rows"][0]["generation_key"],
                "gen-1",
            )

    def test_export_control_plane_snapshot_supports_shared_memory_sqlite_uri(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            sqlite_uri = "file:sourcing-agent-shadow-test?mode=memory&cache=shared"
            keeper = sqlite3.connect(sqlite_uri, uri=True)
            try:
                keeper.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
                keeper.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", ("job-shadow", "running"))
                keeper.commit()

                output_path = runtime_dir / "exports" / "control-plane-shadow.json"
                result = export_control_plane_snapshot(
                    runtime_dir=runtime_dir,
                    output_path=output_path,
                    sqlite_path=sqlite_uri,
                    tables=["jobs"],
                )
            finally:
                keeper.close()

            self.assertEqual(result["status"], "exported")
            snapshot_payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(snapshot_payload["sqlite_path"], sqlite_uri)
            self.assertEqual(snapshot_payload["tables"]["jobs"]["row_count"], 1)
            self.assertEqual(snapshot_payload["tables"]["jobs"]["rows"][0]["job_id"], "job-shadow")

    def test_export_control_plane_snapshot_can_include_all_sqlite_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            runtime_dir = root / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("CREATE TABLE candidates (candidate_id TEXT PRIMARY KEY, name_en TEXT NOT NULL)")
            connection.execute("CREATE TABLE evidence (evidence_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL)")
            connection.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", ("job-1", "completed"))
            connection.execute("INSERT INTO candidates (candidate_id, name_en) VALUES (?, ?)", ("cand-1", "Alice"))
            connection.execute("INSERT INTO evidence (evidence_id, candidate_id) VALUES (?, ?)", ("ev-1", "cand-1"))
            connection.commit()
            connection.close()

            output_path = runtime_dir / "exports" / "all-sqlite.json"
            result = export_control_plane_snapshot(
                runtime_dir=runtime_dir,
                output_path=output_path,
                sqlite_path=db_path,
                include_all_sqlite_tables=True,
            )

            self.assertEqual(result["status"], "exported")
            snapshot_payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertIn("candidates", snapshot_payload["tables"])
            self.assertIn("evidence", snapshot_payload["tables"])
            self.assertEqual(snapshot_payload["tables"]["candidates"]["row_count"], 1)
            self.assertEqual(snapshot_payload["tables"]["evidence"]["row_count"], 1)

    def test_export_control_plane_snapshot_can_fall_back_to_postgres_when_sqlite_file_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            output_path = runtime_dir / "exports" / "control-plane-from-postgres.json"

            connections: list[_FakeSnapshotPostgresConnection] = []

            def _connect(dsn: str, **_kwargs: object) -> _FakeSnapshotPostgresConnection:
                connection = _FakeSnapshotPostgresConnection(dsn)
                connections.append(connection)
                return connection

            fake_psycopg = SimpleNamespace(connect=_connect)
            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "postgresql://local/test",
                    "SOURCING_RUNTIME_ENVIRONMENT": "scripted",
                },
                clear=True,
            ):
                with mock.patch(
                    "sourcing_agent.control_plane_postgres._import_psycopg",
                    return_value=fake_psycopg,
                ):
                    result = export_control_plane_snapshot(
                        runtime_dir=runtime_dir,
                        output_path=output_path,
                        tables=["jobs"],
                    )

            self.assertEqual(result["status"], "exported")
            self.assertEqual(result["source_backend"], "postgres")
            snapshot_payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(snapshot_payload["source_backend"], "postgres")
            self.assertEqual(snapshot_payload["tables"]["jobs"]["row_count"], 1)
            self.assertEqual(snapshot_payload["tables"]["jobs"]["rows"][0]["job_id"], "job-pg")
            self.assertTrue(connections)
            executed_sql = [sql for sql, _params in connections[0].cursor_instance.executed]
            self.assertIn('SET search_path TO "sourcing_scripted", public', executed_sql)

    def test_sync_control_plane_snapshot_to_postgres_requires_dsn(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_path = Path(tempdir) / "control-plane.json"
            snapshot_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "excluded_pg_only_durable_runtime_tables": _EXPECTED_EXCLUSION_GAP,
                        "tables": {
                            "jobs": {
                                "columns": [
                                    {"name": "job_id", "type": "TEXT", "notnull": 1, "default": "", "pk_position": 1},
                                    {"name": "status", "type": "TEXT", "notnull": 1, "default": "", "pk_position": 0},
                                ],
                                "row_count": 1,
                                "rows": [{"job_id": "job-1", "status": "completed"}],
                            },
                            "generation_index_entries": {
                                "columns": [
                                    {
                                        "name": "generation_key",
                                        "type": "TEXT",
                                        "notnull": 1,
                                        "default": "",
                                        "pk_position": 1,
                                    },
                                    {
                                        "name": "generation_sequence",
                                        "type": "INTEGER",
                                        "notnull": 1,
                                        "default": "0",
                                        "pk_position": 0,
                                    },
                                ],
                                "row_count": 1,
                                "rows": [{"generation_key": "gen-1", "generation_sequence": 7}],
                            },
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            with mock.patch.dict("os.environ", {}, clear=True):
                with self.assertRaisesRegex(RuntimeError, "Postgres DSN is required"):
                    sync_control_plane_snapshot_to_postgres(
                        snapshot_path=snapshot_path,
                        tables=["jobs", "generation_index_entries"],
                        truncate_first=True,
                    )

    def test_snapshot_import_rejects_explicit_and_auto_selected_workflow_commands_before_postgres(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_path = Path(tempdir) / "control-plane.json"
            snapshot_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "excluded_pg_only_durable_runtime_tables": _EXPECTED_EXCLUSION_GAP,
                        "tables": {
                            "workflow_commands": {
                                "columns": [
                                    {
                                        "name": "command_id",
                                        "type": "TEXT",
                                        "notnull": 1,
                                        "default": "",
                                        "pk_position": 1,
                                    },
                                    {
                                        "name": "status",
                                        "type": "TEXT",
                                        "notnull": 1,
                                        "default": "",
                                        "pk_position": 0,
                                    },
                                ],
                                "row_count": 1,
                                "rows": [{"command_id": "held-1", "status": "held"}],
                            }
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            for tables, truncate_first in [
                (["workflow_commands"], False),
                (["workflow_commands"], True),
                (None, False),
                (None, True),
            ]:
                with self.subTest(tables=tables, truncate_first=truncate_first):
                    with (
                        mock.patch("sourcing_agent.control_plane_postgres._import_psycopg") as import_psycopg,
                        mock.patch("sourcing_agent.control_plane_postgres._connect_postgres") as connect_postgres,
                        self.assertRaisesRegex(
                            ValueError,
                            "cannot restore PG-only durable runtime tables: workflow_commands",
                        ),
                    ):
                        sync_control_plane_snapshot_to_postgres(
                            snapshot_path=snapshot_path,
                            dsn="postgresql://user:pass@localhost:5432/sourcing",
                            tables=tables,
                            truncate_first=truncate_first,
                        )
                    import_psycopg.assert_not_called()
                    connect_postgres.assert_not_called()

    def test_sync_control_plane_snapshot_to_postgres_uses_psycopg_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_path = Path(tempdir) / "control-plane.json"
            snapshot_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "excluded_pg_only_durable_runtime_tables": _EXPECTED_EXCLUSION_GAP,
                        "tables": {
                            "jobs": {
                                "columns": [
                                    {"name": "job_id", "type": "TEXT", "notnull": 1, "default": "", "pk_position": 1},
                                    {"name": "status", "type": "TEXT", "notnull": 1, "default": "", "pk_position": 0},
                                ],
                                "row_count": 1,
                                "rows": [{"job_id": "job-1", "status": "completed"}],
                            }
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            fake_connections: list[_FakeConnection] = []

            def _connect(dsn: str) -> _FakeConnection:
                connection = _FakeConnection(dsn)
                fake_connections.append(connection)
                return connection

            fake_psycopg = SimpleNamespace(connect=_connect)

            with mock.patch(
                "sourcing_agent.control_plane_postgres._import_psycopg",
                return_value=fake_psycopg,
            ):
                result = sync_control_plane_snapshot_to_postgres(
                    snapshot_path=snapshot_path,
                    dsn="postgresql://user:pass@localhost:5432/sourcing",
                    tables=["jobs"],
                    truncate_first=True,
                )

            self.assertEqual(result["status"], "synced")
            self.assertEqual(len(fake_connections), 1)
            fake_connection = fake_connections[0]
            self.assertEqual(
                fake_connection.dsn,
                "postgresql://user:pass@localhost:5432/sourcing?gssencmode=disable",
            )
            self.assertTrue(fake_connection.committed)
            self.assertTrue(
                any(
                    sql.startswith('CREATE TABLE IF NOT EXISTS "jobs"')
                    for sql in fake_connection.cursor_instance.executed
                )
            )
            self.assertIn('TRUNCATE TABLE "jobs"', fake_connection.cursor_instance.executed)
            self.assertEqual(len(fake_connection.cursor_instance.executemany_calls), 1)
            insert_sql, values = fake_connection.cursor_instance.executemany_calls[0]
            self.assertIn('INSERT INTO "jobs"', insert_sql)
            self.assertIn('ON CONFLICT ("job_id") DO UPDATE SET', insert_sql)
            self.assertEqual(values, [("job-1", "completed")])

    def test_sync_control_plane_snapshot_to_postgres_can_validate_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_path = Path(tempdir) / "control-plane.json"
            snapshot_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "excluded_pg_only_durable_runtime_tables": _EXPECTED_EXCLUSION_GAP,
                        "tables": {
                            "jobs": {
                                "columns": [
                                    {"name": "job_id", "type": "TEXT", "notnull": 1, "default": "", "pk_position": 1},
                                    {"name": "status", "type": "TEXT", "notnull": 1, "default": "", "pk_position": 0},
                                ],
                                "row_count": 1,
                                "rows": [{"job_id": "job-1", "status": "completed"}],
                            }
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            fake_connections: list[_FakeConnection] = []

            def _connect(dsn: str) -> _FakeConnection:
                connection = _FakeConnection(dsn, row_counts={"jobs": 1})
                fake_connections.append(connection)
                return connection

            fake_psycopg = SimpleNamespace(connect=_connect)

            with mock.patch(
                "sourcing_agent.control_plane_postgres._import_psycopg",
                return_value=fake_psycopg,
            ):
                result = sync_control_plane_snapshot_to_postgres(
                    snapshot_path=snapshot_path,
                    dsn="postgresql://user:pass@localhost:5432/sourcing",
                    validate_postgres=True,
                    truncate_first=True,
                )

            self.assertEqual(result["status"], "synced")
            self.assertTrue(bool(result["validated_postgres"]))
            self.assertEqual(result["validation"]["tables"]["jobs"]["actual_row_count"], 1)

    def test_sync_runtime_control_plane_to_postgres_writes_state_and_skips_when_source_is_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute(
                """
                CREATE TABLE jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL
                )
                """
            )
            connection.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", ("job-1", "completed"))
            connection.commit()
            connection.close()

            fake_connections: list[_FakeConnection] = []

            def _connect(dsn: str) -> _FakeConnection:
                connection = _FakeConnection(dsn)
                fake_connections.append(connection)
                return connection

            fake_psycopg = SimpleNamespace(connect=_connect)
            with mock.patch(
                "sourcing_agent.control_plane_postgres._import_psycopg",
                return_value=fake_psycopg,
            ):
                first = sync_runtime_control_plane_to_postgres(
                    runtime_dir=runtime_dir,
                    sqlite_path=db_path,
                    dsn="postgresql://user:pass@localhost:5432/sourcing",
                    tables=["jobs"],
                    force=True,
                )
                second = sync_runtime_control_plane_to_postgres(
                    runtime_dir=runtime_dir,
                    sqlite_path=db_path,
                    dsn="postgresql://user:pass@localhost:5432/sourcing",
                    tables=["jobs"],
                )

            self.assertEqual(first["status"], "synced")
            self.assertEqual(second["status"], "skipped")
            self.assertEqual(second["reason"], "unchanged_source")
            self.assertEqual(len(fake_connections), 1)
            state_path = control_plane_postgres_sync_state_path(runtime_dir)
            self.assertTrue(state_path.exists())
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state_payload["status"], "skipped")
            self.assertEqual(state_payload["last_sync_status"], "skipped")
            self.assertEqual(
                str(dict(state_payload.get("last_success_summary") or {}).get("status") or ""),
                "synced",
            )
            self.assertTrue(str(state_payload.get("last_synced_fingerprint") or ""))

    def test_sync_runtime_control_plane_to_postgres_can_include_all_sqlite_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("CREATE TABLE candidates (candidate_id TEXT PRIMARY KEY, name_en TEXT NOT NULL)")
            connection.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", ("job-1", "completed"))
            connection.execute("INSERT INTO candidates (candidate_id, name_en) VALUES (?, ?)", ("cand-1", "Alice"))
            connection.commit()
            connection.close()

            fake_connections: list[_FakeConnection] = []

            def _connect(dsn: str) -> _FakeConnection:
                connection = _FakeConnection(dsn, row_counts={"jobs": 1, "candidates": 1})
                fake_connections.append(connection)
                return connection

            fake_psycopg = SimpleNamespace(connect=_connect)
            with mock.patch(
                "sourcing_agent.control_plane_postgres._import_psycopg",
                return_value=fake_psycopg,
            ):
                result = sync_runtime_control_plane_to_postgres(
                    runtime_dir=runtime_dir,
                    sqlite_path=db_path,
                    dsn="postgresql://user:pass@localhost:5432/sourcing",
                    include_all_sqlite_tables=True,
                    validate_postgres=True,
                    truncate_first=True,
                    force=True,
                )

            self.assertEqual(result["status"], "synced")
            self.assertIn("candidates", result["tables"])
            self.assertEqual(result["postgres"]["validation"]["tables"]["candidates"]["actual_row_count"], 1)

    def test_runtime_import_rejects_workflow_commands_across_direct_all_and_truncate_before_postgres(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("CREATE TABLE workflow_commands (command_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", ("job-1", "completed"))
            connection.execute(
                "INSERT INTO workflow_commands (command_id, status) VALUES (?, ?)",
                ("held-1", "held"),
            )
            connection.commit()
            connection.close()

            for direct_stream, include_all, tables, truncate_first in [
                (False, False, ["workflow_commands"], False),
                (True, False, ["workflow_commands"], True),
            ]:
                with self.subTest(
                    direct_stream=direct_stream,
                    include_all=include_all,
                    tables=tables,
                    truncate_first=truncate_first,
                ):
                    with (
                        mock.patch("sourcing_agent.control_plane_postgres._import_psycopg") as import_psycopg,
                        mock.patch("sourcing_agent.control_plane_postgres._connect_postgres") as connect_postgres,
                        self.assertRaisesRegex(
                            ValueError,
                            "cannot restore PG-only durable runtime tables: workflow_commands",
                        ),
                    ):
                        sync_runtime_control_plane_to_postgres(
                            runtime_dir=runtime_dir,
                            sqlite_path=db_path,
                            dsn="postgresql://user:pass@localhost:5432/sourcing",
                            tables=tables,
                            truncate_first=truncate_first,
                            include_all_sqlite_tables=include_all,
                            direct_stream=direct_stream,
                            force=True,
                        )
                    import_psycopg.assert_not_called()
                    connect_postgres.assert_not_called()
                    self.assertFalse(control_plane_postgres_sync_state_path(runtime_dir).exists())

            source_connection = sqlite3.connect(db_path)
            try:
                held_row = source_connection.execute(
                    "SELECT command_id, status FROM workflow_commands WHERE command_id = ?",
                    ("held-1",),
                ).fetchone()
            finally:
                source_connection.close()
            self.assertEqual(held_row, ("held-1", "held"))

    def test_runtime_import_include_all_skips_causal_aggregate_with_typed_gap_before_postgres(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("CREATE TABLE workflow_commands (command_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", ("job-1", "completed"))
            connection.execute(
                "INSERT INTO workflow_commands (command_id, status) VALUES (?, ?)",
                ("held-1", "held"),
            )
            connection.commit()
            connection.close()

            for direct_stream in (False, True):
                with self.subTest(direct_stream=direct_stream):
                    fake_connections: list[_FakeConnection] = []

                    def _connect(dsn: str) -> _FakeConnection:
                        connection = _FakeConnection(dsn, row_counts={"jobs": 1})
                        fake_connections.append(connection)
                        return connection

                    fake_psycopg = SimpleNamespace(connect=_connect)
                    with mock.patch(
                        "sourcing_agent.control_plane_postgres._import_psycopg",
                        return_value=fake_psycopg,
                    ):
                        result = sync_runtime_control_plane_to_postgres(
                            runtime_dir=runtime_dir,
                            sqlite_path=db_path,
                            dsn="postgresql://user:pass@localhost:5432/sourcing",
                            truncate_first=True,
                            include_all_sqlite_tables=True,
                            direct_stream=direct_stream,
                            force=True,
                        )
                    self.assertEqual(result["status"], "synced")
                    self.assertEqual(set(result["tables"]), {"jobs"})
                    self.assertEqual(
                        result["excluded_pg_only_durable_runtime_tables"],
                        sorted(GENERIC_POSTGRES_IMPORT_EXCLUDED_TABLES),
                    )

            source_connection = sqlite3.connect(db_path)
            try:
                held_row = source_connection.execute(
                    "SELECT command_id, status FROM workflow_commands WHERE command_id = ?",
                    ("held-1",),
                ).fetchone()
            finally:
                source_connection.close()
            self.assertEqual(held_row, ("held-1", "held"))

    def test_sync_runtime_control_plane_to_postgres_can_stream_direct_in_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", ("job-1", "completed"))
            connection.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", ("job-2", "running"))
            connection.commit()
            connection.close()

            fake_connections: list[_FakeConnection] = []

            def _connect(dsn: str) -> _FakeConnection:
                connection = _FakeConnection(dsn, row_counts={"jobs": 2})
                fake_connections.append(connection)
                return connection

            fake_psycopg = SimpleNamespace(connect=_connect)
            with mock.patch(
                "sourcing_agent.control_plane_postgres._import_psycopg",
                return_value=fake_psycopg,
            ):
                result = sync_runtime_control_plane_to_postgres(
                    runtime_dir=runtime_dir,
                    sqlite_path=db_path,
                    dsn="postgresql://user:pass@localhost:5432/sourcing",
                    tables=["jobs"],
                    truncate_first=True,
                    validate_postgres=True,
                    direct_stream=True,
                    chunk_size=1,
                    force=True,
                )

            self.assertEqual(result["status"], "synced")
            self.assertEqual(result["postgres"]["mode"], "direct_stream")
            self.assertEqual(result["postgres"]["tables"]["jobs"]["row_count"], 2)
            self.assertEqual(result["postgres"]["tables"]["jobs"]["chunk_count"], 2)
            self.assertEqual(len(fake_connections[0].cursor_instance.executemany_calls), 2)

    def test_sync_runtime_control_plane_to_postgres_direct_stream_flushes_progress_by_chunk(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            for index in range(5):
                connection.execute(
                    "INSERT INTO jobs (job_id, status) VALUES (?, ?)",
                    (f"job-{index}", "completed"),
                )
            connection.commit()
            connection.close()

            fake_connections: list[_FakeConnection] = []

            def _connect(dsn: str) -> _FakeConnection:
                connection = _FakeConnection(dsn, row_counts={"jobs": 5})
                fake_connections.append(connection)
                return connection

            fake_psycopg = SimpleNamespace(connect=_connect)
            state_path = runtime_dir / "object_sync" / "control_plane" / "postgres_sync_state.json"
            with mock.patch(
                "sourcing_agent.control_plane_postgres._import_psycopg",
                return_value=fake_psycopg,
            ):
                result = sync_runtime_control_plane_to_postgres(
                    runtime_dir=runtime_dir,
                    sqlite_path=db_path,
                    dsn="postgresql://user:pass@localhost:5432/sourcing",
                    tables=["jobs"],
                    truncate_first=True,
                    validate_postgres=True,
                    direct_stream=True,
                    chunk_size=1,
                    commit_every_chunks=2,
                    progress_every_chunks=2,
                    force=True,
                )

            self.assertEqual(result["status"], "synced")
            self.assertEqual(result["postgres"]["tables"]["jobs"]["row_count"], 5)
            self.assertGreaterEqual(fake_connections[0].commit_calls, 3)
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state_payload["last_sync_status"], "synced")
            last_summary = dict(state_payload["last_summary"] or {})
            self.assertEqual(last_summary["postgres"]["tables"]["jobs"]["chunk_count"], 5)

    def test_sync_runtime_control_plane_to_postgres_direct_stream_sanitizes_text_nul_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", ("job-1", "needs\x00review"))
            connection.commit()
            connection.close()

            fake_connections: list[_FakeConnection] = []

            def _connect(dsn: str) -> _FakeConnection:
                connection = _FakeConnection(dsn, row_counts={"jobs": 1})
                fake_connections.append(connection)
                return connection

            fake_psycopg = SimpleNamespace(connect=_connect)
            with mock.patch(
                "sourcing_agent.control_plane_postgres._import_psycopg",
                return_value=fake_psycopg,
            ):
                result = sync_runtime_control_plane_to_postgres(
                    runtime_dir=runtime_dir,
                    sqlite_path=db_path,
                    dsn="postgresql://user:pass@localhost:5432/sourcing",
                    tables=["jobs"],
                    truncate_first=True,
                    validate_postgres=True,
                    direct_stream=True,
                    chunk_size=1,
                    force=True,
                )

            self.assertEqual(result["status"], "synced")
            inserted_values = fake_connections[0].cursor_instance.executemany_calls[0][1][0]
            self.assertEqual(inserted_values[1], "needs\\u0000review")
            self.assertEqual(result["postgres"]["tables"]["jobs"]["nul_text_replacements"], 1)

    def test_sync_runtime_control_plane_to_postgres_resumes_direct_stream_after_stale_running_state(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            connection.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", ("job-1", "completed"))
            connection.commit()
            connection.close()

            stale_state_path = control_plane_postgres_sync_state_path(runtime_dir)
            source_fingerprint = "resume-fingerprint"
            stale_state_path.parent.mkdir(parents=True, exist_ok=True)
            stale_state_path.write_text(
                json.dumps(
                    {
                        "last_sync_status": "running",
                        "last_synced_fingerprint": source_fingerprint,
                        "last_summary": {
                            "table_progress": {
                                "jobs": {
                                    "status": "synced",
                                    "row_count": 1,
                                    "column_count": 2,
                                }
                            }
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            fake_connections: list[_FakeConnection] = []

            def _connect(dsn: str) -> _FakeConnection:
                connection = _FakeConnection(dsn, row_counts={"jobs": 1})
                fake_connections.append(connection)
                return connection

            fake_psycopg = SimpleNamespace(connect=_connect)
            with (
                mock.patch(
                    "sourcing_agent.control_plane_postgres._import_psycopg",
                    return_value=fake_psycopg,
                ),
                mock.patch(
                    "sourcing_agent.control_plane_postgres.build_control_plane_source_fingerprint",
                    return_value=source_fingerprint,
                ),
            ):
                result = sync_runtime_control_plane_to_postgres(
                    runtime_dir=runtime_dir,
                    sqlite_path=db_path,
                    dsn="postgresql://user:pass@localhost:5432/sourcing",
                    tables=["jobs"],
                    direct_stream=True,
                )

            self.assertEqual(result["status"], "synced")
            self.assertEqual(result["postgres"]["mode"], "direct_stream")
            self.assertEqual(result["postgres"]["skipped_tables"], ["jobs"])

    def test_sync_runtime_control_plane_to_postgres_returns_disabled_without_dsn(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            db_path = runtime_dir / "sourcing_agent.db"
            sqlite3.connect(db_path).close()
            with mock.patch.dict("os.environ", {}, clear=True):
                result = sync_runtime_control_plane_to_postgres(
                    runtime_dir=runtime_dir,
                    sqlite_path=db_path,
                    tables=["jobs"],
                )
            self.assertEqual(result["status"], "disabled")
            self.assertEqual(result["reason"], "missing_dsn")


class ControlPlanePostgresPortabilityPGTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    """Prove generic export/import can neither observe nor mutate live coordination state."""

    pg_store_schema_label = "control_plane_portability"

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.runtime_dir = Path(self.tempdir.name) / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.store = self.make_pg_store(Path(self.tempdir.name) / "portability.db")
        self.adapter = self.store._control_plane_postgres  # noqa: SLF001

    def _coordination_rows(self, table_name: str) -> tuple[dict[str, object], ...]:
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None and psycopg is not None
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        quoted_table = quote_control_plane_postgres_identifier(table_name)
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT row_to_json(row_value) FROM {quoted_schema}.{quoted_table} AS row_value "
                    "ORDER BY row_to_json(row_value)::text"
                )
                return tuple(dict(row[0]) for row in cursor.fetchall())

    def test_generic_export_import_ignores_active_leases_and_pending_intents(self) -> None:
        self.adapter._execute_non_query(  # noqa: SLF001
            "INSERT INTO workflow_job_leases (job_id, lease_owner, lease_token, lease_expires_at, created_at, "
            "updated_at) VALUES (%s, %s, %s, %s, %s, %s)",
            (
                "job-active-lease",
                "daemon-A",
                "lease-token-1",
                "2099-01-01 00:00:00",
                "2026-07-19T00:00:00Z",
                "2026-07-19T00:00:00Z",
            ),
        )
        self.adapter._execute_non_query(  # noqa: SLF001
            "INSERT INTO workflow_recovery_intents (job_id, classification, status, requested_at, requested_by, "
            "params_json, lease_owner, lease_expires_at, claimed_at, schema_version, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                "job-pending-intent",
                "stale_runner",
                "pending",
                "2026-07-19T00:00:00Z",
                "read-path",
                "{}",
                "",
                "",
                "",
                "workflow_recovery_intent_v1",
                "2026-07-19T00:00:00Z",
                "2026-07-19T00:00:00Z",
            ),
        )
        self.adapter._execute_non_query(  # noqa: SLF001
            "INSERT INTO runtime_provider_limiter_leases (lease_token, limiter_key, lease_owner, lease_expires_at, "
            "metadata_json, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                "limiter-lease-1",
                "harvest_profile_scraper_actor",
                "worker-1",
                "2099-01-01 00:00:00",
                "{}",
                "2026-07-19T00:00:00Z",
                "2026-07-19T00:00:00Z",
            ),
        )
        coordination_baseline = {
            table_name: self._coordination_rows(table_name) for table_name in _NONPORTABLE_COORDINATION_TABLES
        }
        self.assertTrue(all(coordination_baseline.values()))

        output_path = Path(self.tempdir.name) / "snapshot.json"
        exported = export_control_plane_snapshot(
            runtime_dir=self.runtime_dir,
            output_path=output_path,
            source_backend="postgres",
        )

        self.assertEqual(exported["status"], "exported")
        self.assertEqual(exported["excluded_pg_only_durable_runtime_tables"], _EXPECTED_EXCLUSION_GAP)
        snapshot_payload = json.loads(output_path.read_text(encoding="utf-8"))
        self.assertEqual(snapshot_payload["excluded_pg_only_durable_runtime_tables"], _EXPECTED_EXCLUSION_GAP)
        for table_name in _NONPORTABLE_COORDINATION_TABLES:
            self.assertNotIn(table_name, snapshot_payload["tables"])
        self.assertNotIn("job-active-lease", output_path.read_text(encoding="utf-8"))

        synced = sync_control_plane_snapshot_to_postgres(
            snapshot_path=output_path,
            truncate_first=True,
        )
        self.assertEqual(synced["status"], "synced")
        self.assertEqual(synced["excluded_pg_only_durable_runtime_tables"], _EXPECTED_EXCLUSION_GAP)
        for table_name in _NONPORTABLE_COORDINATION_TABLES:
            self.assertEqual(self._coordination_rows(table_name), coordination_baseline[table_name])

        for table_name in _NONPORTABLE_COORDINATION_TABLES:
            with self.subTest(boundary="export", table_name=table_name):
                with self.assertRaisesRegex(
                    ValueError,
                    f"cannot restore PG-only durable runtime tables: {table_name}",
                ):
                    export_control_plane_snapshot(
                        runtime_dir=self.runtime_dir,
                        output_path=Path(self.tempdir.name) / f"snapshot-{table_name}.json",
                        source_backend="postgres",
                        tables=[table_name],
                    )
            with self.subTest(boundary="sync", table_name=table_name):
                with self.assertRaisesRegex(
                    ValueError,
                    f"cannot restore PG-only durable runtime tables: {table_name}",
                ):
                    sync_control_plane_snapshot_to_postgres(
                        snapshot_path=output_path,
                        tables=[table_name],
                    )
            self.assertEqual(self._coordination_rows(table_name), coordination_baseline[table_name])


if __name__ == "__main__":
    unittest.main()
