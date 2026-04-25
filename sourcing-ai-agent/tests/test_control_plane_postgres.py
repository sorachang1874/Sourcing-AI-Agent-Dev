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
    _connect_postgres,
    control_plane_postgres_sync_state_path,
    ensure_acquisition_shard_registry_split_schema,
    export_control_plane_snapshot,
    sync_control_plane_snapshot_to_postgres,
    sync_runtime_control_plane_to_postgres,
    upsert_acquisition_shard_registry_rows,
)


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
        self.description: list[tuple[str]] = []
        self._rows: list[object] = []
        self._offset = 0

    def __enter__(self) -> "_FakeSnapshotPostgresCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        return False

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
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
        chunk = self._rows[self._offset:next_offset]
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
                    self._fetchone_result = ("r",) if relation_name == ACQUISITION_SHARD_REGISTRY_LOGICAL_TABLE else None
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

            fake_psycopg = SimpleNamespace(connect=lambda dsn: _FakeSnapshotPostgresConnection(dsn))
            with mock.patch.dict(
                "os.environ",
                {"SOURCING_CONTROL_PLANE_POSTGRES_DSN": "postgresql://local/test"},
                clear=False,
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

    def test_sync_control_plane_snapshot_to_postgres_requires_dsn(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_path = Path(tempdir) / "control-plane.json"
            snapshot_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
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

    def test_sync_control_plane_snapshot_to_postgres_uses_psycopg_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            snapshot_path = Path(tempdir) / "control-plane.json"
            snapshot_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
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


if __name__ == "__main__":
    unittest.main()
