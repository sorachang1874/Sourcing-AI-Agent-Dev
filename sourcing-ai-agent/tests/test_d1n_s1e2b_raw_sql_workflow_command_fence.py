from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin, psycopg

_READ_ONLY_MESSAGE = "plainly read-only single statements"
_READ_ONLY_TRANSACTION_MESSAGE = "database-enforced read-only transaction"


class D1nS1e2bRawSqlWorkflowCommandFenceUnitTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        runtime_dir = Path(self.tempdir.name)
        self.adapter = LiveControlPlanePostgresAdapter(
            runtime_dir=runtime_dir,
            sqlite_path=runtime_dir / "shadow.db",
            dsn="postgresql://example/test",
            mode="disabled",
        )
        self.addCleanup(self.adapter.close)

    def _assert_public_rejects_before_delegate(self, rejected_sql: tuple[str, ...]) -> None:
        public_methods = (
            ("execute_non_query", 1, False),
            ("execute_returning_one", {"command_id": "cmd_held"}, True),
        )
        for public_name, delegate_result, fetch_one in public_methods:
            for sql in rejected_sql:
                with self.subTest(public_method=public_name, sql=sql):
                    with mock.patch.object(
                        self.adapter, "_execute_public_probe", return_value=delegate_result
                    ) as delegate:
                        with self.assertRaisesRegex(ValueError, _READ_ONLY_MESSAGE):
                            getattr(self.adapter, public_name)(sql, ())
                        delegate.assert_not_called()

    def _assert_public_allows_read_only(self, allowed_sql: tuple[str, ...]) -> None:
        public_methods = (
            ("execute_non_query", 7, False),
            ("execute_returning_one", {"marker": 1}, True),
        )
        for public_name, delegate_result, fetch_one in public_methods:
            for sql in allowed_sql:
                with self.subTest(public_method=public_name, sql=sql):
                    with mock.patch.object(
                        self.adapter, "_execute_public_probe", return_value=delegate_result
                    ) as delegate:
                        result = getattr(self.adapter, public_name)(sql, ("parameter",))
                        self.assertEqual(result, delegate_result)
                        delegate.assert_called_once_with(
                            sql,
                            ("parameter",),
                            fetch_one=fetch_one,
                            method=public_name,
                        )

    def test_public_raw_sql_rejects_dml_ddl_and_target_smuggling_before_delegate(self) -> None:
        rejected_sql = (
            "UPDATE workflow_commands SET status = 'cancelled'",
            "DELETE FROM public.workflow_commands WHERE command_id = 'cmd_held'",
            "INSERT INTO workflow_commands (command_id) VALUES ('cmd_new')",
            "MERGE INTO workflow_commands AS target USING source ON false WHEN NOT MATCHED THEN DO NOTHING",
            "MERGE workflow_commands AS target USING source ON false WHEN NOT MATCHED THEN DO NOTHING",
            # The MERGE branch used to skip the optional ONLY marker.
            "MERGE INTO ONLY workflow_commands AS target USING source ON false WHEN NOT MATCHED THEN DO NOTHING",
            'COPY/* fence */"public"."workflow_commands" (command_id, status) FROM STDIN',
            "COPY workflow_commands TO STDOUT",
            "TRUNCATE TABLE other_table, ONLY public.workflow_commands RESTART IDENTITY",
            'ALTER TABLE IF EXISTS "public"."workflow_commands" ADD COLUMN bypass TEXT',
            "ALTER TABLE other_table RENAME TO workflow_commands",
            "ALTER TABLE other_table ADD COLUMN workflow_commands TEXT",
            "DROP TABLE IF EXISTS other_table, workflow_commands CASCADE",
            # DROP handled only OWNED and TABLE; schema destruction cascades.
            "DROP SCHEMA control_plane CASCADE",
            "DROP SCHEMA IF EXISTS public CASCADE",
            "CREATE TABLE IF NOT EXISTS workflow_commands (command_id TEXT)",
            "CREATE TABLE workflow_commands_archive (command_id TEXT)",
            "CREATE UNIQUE INDEX command_probe ON workflow_commands (command_id)",
            "CREATE TRIGGER command_probe AFTER UPDATE ON workflow_commands EXECUTE FUNCTION probe()",
            "CREATE POLICY command_probe ON workflow_commands USING (true)",
            # A rule on another table whose action updates the command table.
            "CREATE RULE probe AS ON INSERT TO other_table DO ALSO UPDATE workflow_commands SET not_before_at = ''",
            "WITH held AS (SELECT 'cmd_held' AS command_id) "
            "UPDATE/* fence */public./* target */workflow_commands SET status = 'cancelled'",
            "WITH removed AS (DELETE FROM workflow_commands RETURNING command_id) SELECT * FROM removed",
            "WITH removed AS (UPDATE workflow_commands_archive SET status = 'x' RETURNING *) SELECT * FROM removed",
            "SELECT 1; DELETE FROM workflow_commands; SELECT 2",
            "SELECT 1; SELECT 2",
            # CTE column-list spellings fail closed (alias inside the body).
            "WITH walk(level) AS (SELECT 1) SELECT level FROM walk",
            # Mutations of other tables no longer pass either: the public
            # helpers are a read-only allowlist, not a command-table denylist.
            "UPDATE workflow_commands_archive SET status = 'cancelled'",
            "DELETE FROM public.workflow_command WHERE command_id = 'cmd_other'",
            "UPDATE \"WORKFLOW_COMMANDS\" SET status = 'cancelled'",
            "UPDATE ONLY (workflow_commands) SET not_before_at=''",
            "DELETE FROM ONLY (public.workflow_commands) WHERE command_id = 'cmd_held'",
            "TRUNCATE ONLY (workflow_commands)",
            "UPDATE u&\"workflow_commands\" SET not_before_at=''",
            "UPDATE u&\"workflow\\005fcommands\" SET not_before_at=''",
            "UPDATE u&\"workflow!005fcommands\" UESCAPE '!' SET not_before_at=''",
        )
        self._assert_public_rejects_before_delegate(rejected_sql)

    def test_public_raw_sql_rejects_opaque_executing_and_utility_bodies_before_delegate(self) -> None:
        rejected_sql = (
            "DO $$BEGIN UPDATE workflow_commands SET not_before_at=''; END$$",
            "DO $body$BEGIN DELETE FROM workflow_commands; END$body$",
            "DO $$BEGIN RAISE NOTICE 'probe'; END$$",
            "EXPLAIN ANALYZE UPDATE workflow_commands SET not_before_at='' RETURNING *",
            "EXPLAIN (ANALYZE, FORMAT JSON) DELETE FROM workflow_commands",
            "EXPLAIN ANALYZE WITH removed AS (DELETE FROM workflow_commands RETURNING *) SELECT * FROM removed",
            "EXPLAIN VERBOSE ANALYZE INSERT INTO workflow_commands (command_id) VALUES ('cmd_new')",
            "EXPLAIN ANALYZE UPDATE workflow_commands_archive SET status = 'x'",
            "EXPLAIN ANALYZE SELECT existing_mutator()",
            "CALL release_held_commands()",
            "PREPARE release_hold AS UPDATE workflow_commands SET not_before_at=''",
            "PREPARE release_hold(text) AS DELETE FROM workflow_commands WHERE command_id = $1",
            "EXECUTE release_hold",
            "PREPARE diagnostic AS SELECT 1; EXECUTE diagnostic",
            # Session-state wrappers are utility execution, not read probes;
            # the private migration/test interface covers them.
            "PREPARE read_probe AS SELECT * FROM workflow_commands",
            "DEALLOCATE read_probe",
            "DEALLOCATE ALL",
            "SELECT * INTO workflow_commands FROM workflow_commands_archive",
            "SELECT command_id INTO TEMP TABLE workflow_commands FROM staging_commands",
            "SELECT * INTO workflow_commands_archive FROM workflow_commands",
            "CREATE FUNCTION release_holds() RETURNS void AS $$ BEGIN UPDATE workflow_commands SET not_before_at=''; END $$ LANGUAGE plpgsql",
            "CREATE OR REPLACE PROCEDURE drop_holds() AS $$ BEGIN DELETE FROM workflow_commands; END $$ LANGUAGE plpgsql",
            "DROP OWNED BY current_user",
            # Side-effecting or unlisted function calls are never proven
            # read-only, even inside an otherwise plain SELECT.
            "SELECT existing_mutator()",
            "SELECT public.existing_mutator()",
            "SELECT nextval('agent_trace_spans_span_id_seq')",
            "SELECT setval('agent_trace_spans_span_id_seq', 7)",
            "SELECT pg_advisory_lock(1)",
            "SELECT set_config('statement_timeout', '5s', false)",
            "SELECT pg_notify('probe', 'payload')",
            # Locking reads are not plainly read-only.
            "SELECT * FROM workflow_commands WHERE command_id = 'cmd_held' FOR UPDATE",
            "SELECT * FROM workflow_commands FOR NO KEY UPDATE",
            "SELECT * FROM workflow_commands FOR SHARE",
            "SELECT * FROM workflow_commands FOR KEY SHARE",
            # Session/utility statements belong to the private interface.
            "SET statement_timeout = '5s'",
            "RESET statement_timeout",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "LISTEN control_plane_probe",
            "UNLISTEN control_plane_probe",
            "NOTIFY control_plane_probe",
            "DISCARD ALL",
            "VACUUM workflow_commands",
            "ANALYZE workflow_commands",
            "REINDEX TABLE workflow_commands",
            "CLUSTER workflow_commands USING workflow_commands_pkey",
            "LOCK TABLE workflow_commands IN ACCESS EXCLUSIVE MODE",
            "COMMENT ON TABLE workflow_commands IS 'probe'",
            "GRANT SELECT ON workflow_commands TO PUBLIC",
            "REVOKE SELECT ON workflow_commands FROM PUBLIC",
            "SECURITY LABEL ON TABLE workflow_commands IS 'probe'",
            "CHECKPOINT",
        )
        self._assert_public_rejects_before_delegate(rejected_sql)

    def test_public_raw_sql_preserves_plainly_read_only_probes(self) -> None:
        allowed_sql = (
            "SELECT * FROM workflow_commands WHERE command_id = %s",
            "SELECT 'UPDATE workflow_commands SET status = cancelled' AS diagnostic",
            "SELECT $$DELETE FROM workflow_commands$$ AS diagnostic",
            "SELECT 1 /* UPDATE workflow_commands SET status = 'cancelled' */",
            "SELECT update workflow_commands FROM diagnostic_columns",
            "WITH workflow_commands AS (SELECT 1 AS marker) SELECT marker FROM workflow_commands",
            "WITH first AS (SELECT 1 AS marker), second AS (SELECT marker FROM first) SELECT * FROM second",
            "WITH RECURSIVE walk AS (SELECT 1 AS level) SELECT level FROM walk",
            "SELECT * FROM workflow_commands WHERE command_id IN (SELECT command_id FROM workflow_commands)",
            "SELECT EXISTS (SELECT 1 FROM workflow_commands)",
            "SELECT count(*), max(command_id) FROM workflow_commands",
            "SELECT row_to_json(command_row) FROM workflow_commands AS command_row",
            "SELECT json_agg(column_name ORDER BY ordinal_position) FROM information_schema.columns",
            "SELECT json_object_agg(conname, pg_get_constraintdef(oid)) FROM pg_constraint",
            "SELECT to_regclass(%s)",
            "SELECT pg_backend_pid() AS backend_pid",
            "SELECT coalesce(nullif(%s, ''), 'fallback')",
            "SELECT * FROM generate_series(1, 3)",
            "SELECT * FROM workflow_commands ORDER BY command_id LIMIT 10 OFFSET 0",
            "SELECT 1 AS one;",
            "VALUES (1), (2)",
            "TABLE workflow_commands",
            "EXPLAIN SELECT * FROM workflow_commands",
            "EXPLAIN (COSTS OFF) SELECT command_id FROM workflow_commands",
            "EXPLAIN ANALYZE SELECT count(*) FROM workflow_commands",
            "EXPLAIN ANALYZE WITH held AS (SELECT 'cmd_held' AS command_id) SELECT * FROM held",
            "SHOW search_path",
            "SHOW ALL",
            # The bitwise-and operator keeps word/quoted-identifier adjacency
            # from being misread as a U& unicode identifier.
            'SELECT u & "workflow_commands" FROM diagnostics',
            "SELECT u&'d\\0061ta' AS diagnostic",
        )
        self._assert_public_allows_read_only(allowed_sql)


class D1nS1e2bRawSqlWorkflowCommandFencePGTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    pg_store_schema_label = "d1n_s1e2b_raw_sql_fence"

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.store = self.make_pg_store(Path(self.tempdir.name) / "raw-sql-fence.db")
        self.adapter = self.store._control_plane_postgres  # noqa: SLF001
        self.command_id = "cmd_s1e2b_raw_sql_held"
        inserted = self.adapter.upsert_workflow_command(
            {
                "command_id": self.command_id,
                "workflow_run_id": "workflow_s1e2b_raw_sql_held",
                "operation_id": "operation_s1e2b_raw_sql_held",
                "command_type": "acquisition.run.create",
                "owner": "acquisition_engine",
                "status": "queued",
                "idempotency_key": "s1e2b_raw_sql_held",
                "not_before_at": "9999-12-31 23:59:59",
            }
        )
        self.assertIsNotNone(inserted)

    def _quoted_schema(self) -> str:
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        assert fixture is not None
        return quote_control_plane_postgres_identifier(fixture.schema)

    def _qualified_command_table(self) -> str:
        return f"{self._quoted_schema()}.{quote_control_plane_postgres_identifier('workflow_commands')}"

    def _workflow_command_snapshot(self) -> tuple[str, ...]:
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        quoted_table = quote_control_plane_postgres_identifier("workflow_commands")
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT row_to_json(command_row)::text "
                    f"FROM {quoted_schema}.{quoted_table} AS command_row ORDER BY command_id"
                )
                return tuple(str(row[0]) for row in cursor.fetchall())

    def _held_not_before_at(self) -> str:
        row = self.adapter.execute_returning_one(
            "SELECT not_before_at FROM workflow_commands WHERE command_id = %s",
            (self.command_id,),
        )
        self.assertIsNotNone(row)
        assert row is not None
        return str(row["not_before_at"])

    def _restore_held_row(self) -> None:
        self.adapter._execute_non_query(  # noqa: SLF001
            "UPDATE workflow_commands SET not_before_at = '9999-12-31 23:59:59' WHERE command_id = %s",
            (self.command_id,),
        )

    def test_public_raw_update_delete_matrix_preserves_full_held_command_table(self) -> None:
        qualified_table = self._qualified_command_table()
        baseline = self._workflow_command_snapshot()
        attempts = (
            (
                "execute_returning_one",
                "UPDATE workflow_commands SET not_before_at = '' WHERE command_id = %s RETURNING *",
            ),
            (
                "execute_returning_one",
                f"DELETE FROM {qualified_table} WHERE command_id = %s RETURNING *",
            ),
            (
                "execute_non_query",
                f"WITH held AS (SELECT %s::text AS command_id) "
                f"UPDATE/* owner-fence */{qualified_table} SET not_before_at = '' "
                "WHERE command_id IN (SELECT command_id FROM held)",
            ),
            (
                "execute_non_query",
                f"WITH removed AS (DELETE FROM {qualified_table} WHERE command_id = %s RETURNING command_id) "
                "SELECT count(*) FROM removed",
            ),
        )
        for public_method, sql in attempts:
            with self.subTest(public_method=public_method, sql=sql):
                with self.assertRaisesRegex(ValueError, _READ_ONLY_MESSAGE):
                    getattr(self.adapter, public_method)(sql, (self.command_id,))
                self.assertEqual(self._workflow_command_snapshot(), baseline)

        row = self.adapter.execute_returning_one(
            "SELECT command_id, status, not_before_at FROM workflow_commands WHERE command_id = %s",
            (self.command_id,),
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["command_id"], self.command_id)
        self.assertEqual(row["status"], "queued")
        self.assertEqual(str(row["not_before_at"]), "9999-12-31 23:59:59")

    def test_public_raw_sql_opaque_and_executing_wrappers_preserve_held_command_table(self) -> None:
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        assert psycopg is not None
        quoted_schema = self._quoted_schema()
        qualified_table = self._qualified_command_table()
        quoted_procedure = quote_control_plane_postgres_identifier("s1e2b_fence_release_holds")
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE PROCEDURE {quoted_schema}.{quoted_procedure}() "
                    "LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'must never run'; END $$"
                )
            connection.commit()

        baseline = self._workflow_command_snapshot()
        attempts = (
            (
                "execute_non_query",
                f"DO $$BEGIN UPDATE {qualified_table} SET not_before_at = ''; END$$",
            ),
            (
                "execute_returning_one",
                f"EXPLAIN ANALYZE UPDATE {qualified_table} SET not_before_at = '' WHERE command_id = %s RETURNING *",
            ),
            (
                "execute_non_query",
                f"CALL {quoted_schema}.{quoted_procedure}()",
            ),
            (
                "execute_non_query",
                f"PREPARE s1e2b_release AS UPDATE {qualified_table} SET not_before_at = ''",
            ),
            (
                "execute_non_query",
                "EXECUTE s1e2b_release",
            ),
            (
                "execute_non_query",
                f"SELECT * INTO {qualified_table} FROM {qualified_table}",
            ),
            (
                "execute_non_query",
                f"UPDATE ONLY ({qualified_table}) SET not_before_at = ''",
            ),
            (
                "execute_non_query",
                f"UPDATE {quoted_schema}.u&\"workflow_commands\" SET not_before_at = ''",
            ),
            (
                "execute_non_query",
                f"CREATE PROCEDURE {quoted_schema}.{quoted_procedure}() "
                f"LANGUAGE plpgsql AS $$ BEGIN UPDATE {qualified_table} SET not_before_at = ''; END $$",
            ),
            (
                "execute_non_query",
                "DROP OWNED BY current_user",
            ),
            # Read-only-shape session wrappers now fail the allowlist too.
            (
                "execute_non_query",
                f"PREPARE s1e2b_read_probe AS SELECT command_id FROM {qualified_table}",
            ),
        )
        for public_method, sql in attempts:
            with self.subTest(public_method=public_method, sql=sql):
                params = (self.command_id,) if "%s" in sql else ()
                with self.assertRaisesRegex(ValueError, _READ_ONLY_MESSAGE):
                    getattr(self.adapter, public_method)(sql, params)
                self.assertEqual(self._workflow_command_snapshot(), baseline)

        read_only = (
            f"EXPLAIN ANALYZE SELECT count(*) FROM {qualified_table}",
            "SHOW search_path",
            "WITH held AS (SELECT %s::text AS command_id) SELECT command_id FROM held",
        )
        for sql in read_only:
            with self.subTest(sql=sql):
                params = (self.command_id,) if "%s" in sql else ()
                result = (
                    self.adapter.execute_returning_one(sql, params)
                    if sql.startswith("SHOW") or "%s" in sql
                    else self.adapter.execute_non_query(sql, params)
                )
                self.assertIsNotNone(result)
                self.assertEqual(self._workflow_command_snapshot(), baseline)

    def test_public_raw_sql_rejects_merge_into_only_on_real_pg(self) -> None:
        qualified_table = self._qualified_command_table()
        baseline = self._workflow_command_snapshot()
        attempts = (
            f"MERGE INTO ONLY {qualified_table} AS target USING (SELECT 1 AS one) AS source "
            "ON false WHEN NOT MATCHED THEN DO NOTHING",
            f"MERGE INTO {qualified_table} AS target USING (SELECT 1 AS one) AS source "
            "ON false WHEN NOT MATCHED THEN DO NOTHING",
        )
        for sql in attempts:
            with self.subTest(sql=sql):
                with self.assertRaisesRegex(ValueError, _READ_ONLY_MESSAGE):
                    self.adapter.execute_non_query(sql, ())
        self.assertEqual(self._workflow_command_snapshot(), baseline)

    def test_public_raw_sql_never_invokes_side_effecting_function_on_real_pg(self) -> None:
        quoted_schema = self._quoted_schema()
        qualified_table = self._qualified_command_table()
        quoted_function = quote_control_plane_postgres_identifier("s1e2b_existing_mutator")
        self.adapter._execute_non_query(  # noqa: SLF001
            f"CREATE FUNCTION {quoted_schema}.{quoted_function}() RETURNS integer "
            "LANGUAGE plpgsql AS $$ "
            f"BEGIN UPDATE {qualified_table} SET not_before_at = '2000-01-01 00:00:00' "
            f"WHERE command_id = '{self.command_id}'; RETURN 1; END $$",
            (),
        )
        self.addCleanup(
            lambda: self.adapter._execute_non_query(  # noqa: SLF001
                f"DROP FUNCTION IF EXISTS {quoted_schema}.{quoted_function}()",
                (),
            )
        )

        invoked = self.adapter._execute_returning_one(  # noqa: SLF001
            f"SELECT {quoted_schema}.{quoted_function}() AS probe",
            (),
        )
        self.assertIsNotNone(invoked)
        self.assertEqual(self._held_not_before_at(), "2000-01-01 00:00:00")
        self._restore_held_row()
        baseline = self._workflow_command_snapshot()

        attempts = (
            f"SELECT {quoted_schema}.{quoted_function}()",
            f"SELECT public.{quote_control_plane_postgres_identifier('s1e2b_existing_mutator')}()",
            f"WITH invoked AS (SELECT {quoted_schema}.{quoted_function}() AS probe) SELECT * FROM invoked",
            f"EXPLAIN ANALYZE SELECT {quoted_schema}.{quoted_function}()",
        )
        for sql in attempts:
            with self.subTest(sql=sql):
                with self.assertRaisesRegex(ValueError, _READ_ONLY_MESSAGE):
                    self.adapter.execute_returning_one(sql, ())
                with self.assertRaisesRegex(ValueError, _READ_ONLY_MESSAGE):
                    self.adapter.execute_non_query(sql, ())
                self.assertEqual(self._workflow_command_snapshot(), baseline)

        with self.assertRaisesRegex(ValueError, _READ_ONLY_MESSAGE):
            self.adapter.execute_non_query(f"DROP FUNCTION {quoted_schema}.{quoted_function}()", ())
        self.assertEqual(self._workflow_command_snapshot(), baseline)

    def test_public_raw_sql_rejects_drop_schema_cascade_on_real_pg(self) -> None:
        quoted_schema = self._quoted_schema()
        baseline = self._workflow_command_snapshot()
        attempts = (
            f"DROP SCHEMA {quoted_schema} CASCADE",
            f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE",
            "DROP SCHEMA public CASCADE",
        )
        for sql in attempts:
            with self.subTest(sql=sql):
                with self.assertRaisesRegex(ValueError, _READ_ONLY_MESSAGE):
                    self.adapter.execute_non_query(sql, ())
        self.assertEqual(self._workflow_command_snapshot(), baseline)
        self.assertEqual(self._held_not_before_at(), "9999-12-31 23:59:59")

    def test_public_raw_sql_rejects_cross_table_rule_firing_on_real_pg(self) -> None:
        quoted_schema = self._quoted_schema()
        qualified_table = self._qualified_command_table()
        quoted_probe = quote_control_plane_postgres_identifier("s1e2b_rule_probe")
        quoted_rule = quote_control_plane_postgres_identifier("s1e2b_rule_probe_fire")
        self.adapter._execute_non_query(  # noqa: SLF001
            f"CREATE TABLE {quoted_schema}.{quoted_probe} (marker TEXT NOT NULL)",
            (),
        )
        create_rule_sql = (
            f"CREATE RULE {quoted_rule} AS ON INSERT TO {quoted_schema}.{quoted_probe} "
            f"DO ALSO UPDATE {qualified_table} SET not_before_at = '2000-01-01 00:00:00' "
            f"WHERE command_id = '{self.command_id}'"
        )
        baseline = self._workflow_command_snapshot()

        with self.assertRaisesRegex(ValueError, _READ_ONLY_MESSAGE):
            self.adapter.execute_non_query(create_rule_sql, ())
        self.assertEqual(self._workflow_command_snapshot(), baseline)

        # Control: the same rule, installed through the private migration/test
        # interface, genuinely mutates the command table when the probe table
        # receives an insert — the public allowlist is what stops it.
        self.adapter._execute_non_query(create_rule_sql, ())  # noqa: SLF001
        self.adapter._execute_non_query(  # noqa: SLF001
            f"INSERT INTO {quoted_schema}.{quoted_probe} (marker) VALUES (%s)",
            ("fire",),
        )
        self.assertEqual(self._held_not_before_at(), "2000-01-01 00:00:00")
        self._restore_held_row()
        self.assertEqual(self._workflow_command_snapshot(), baseline)

    def test_public_probe_rejects_mutating_view_on_real_pg(self) -> None:
        # A plainly shaped ``SELECT * FROM view`` hides a proven mutator behind
        # the view definition: the lexical allowlist cannot see it, so the
        # database-enforced read-only transaction is the boundary that stops it.
        quoted_schema = self._quoted_schema()
        qualified_table = self._qualified_command_table()
        quoted_function = quote_control_plane_postgres_identifier("s1e2b_view_mutator")
        quoted_view = quote_control_plane_postgres_identifier("s1e2b_mutating_view")
        self.adapter._execute_non_query(  # noqa: SLF001
            f"CREATE FUNCTION {quoted_schema}.{quoted_function}() RETURNS integer "
            "LANGUAGE plpgsql AS $$ "
            f"BEGIN UPDATE {qualified_table} SET not_before_at = '2000-01-01 00:00:00' "
            f"WHERE command_id = '{self.command_id}'; RETURN 1; END $$",
            (),
        )
        self.adapter._execute_non_query(  # noqa: SLF001
            f"CREATE VIEW {quoted_schema}.{quoted_view} AS SELECT {quoted_schema}.{quoted_function}() AS probe",
            (),
        )
        self.addCleanup(
            lambda: self.adapter._execute_non_query(  # noqa: SLF001
                f"DROP FUNCTION IF EXISTS {quoted_schema}.{quoted_function}()",
                (),
            )
        )
        self.addCleanup(
            lambda: self.adapter._execute_non_query(  # noqa: SLF001
                f"DROP VIEW IF EXISTS {quoted_schema}.{quoted_view}",
                (),
            )
        )

        # Control: reading the view through the private interface fires the
        # hidden mutator — the catalog indirection is real, not hypothetical.
        invoked = self.adapter._execute_returning_one(  # noqa: SLF001
            f"SELECT * FROM {quoted_schema}.{quoted_view}",
            (),
        )
        self.assertIsNotNone(invoked)
        self.assertEqual(self._held_not_before_at(), "2000-01-01 00:00:00")
        self._restore_held_row()
        baseline = self._workflow_command_snapshot()

        attempts = (
            f"SELECT * FROM {quoted_view}",
            f"SELECT probe FROM {quoted_schema}.{quoted_view}",
            f"EXPLAIN ANALYZE SELECT * FROM {quoted_schema}.{quoted_view}",
        )
        for sql in attempts:
            for public_method in ("execute_returning_one", "execute_non_query"):
                with self.subTest(public_method=public_method, sql=sql):
                    with self.assertRaisesRegex(ValueError, _READ_ONLY_TRANSACTION_MESSAGE):
                        getattr(self.adapter, public_method)(sql, ())
                    self.assertEqual(self._workflow_command_snapshot(), baseline)

    def test_public_probe_rejects_allowlisted_function_overload_on_real_pg(self) -> None:
        # A schema-local ``lower(integer)`` overload shadows the allowlisted
        # ``pg_catalog.lower`` for ``SELECT lower(1)``; overload resolution is a
        # database decision, so the read-only transaction is the boundary.
        quoted_schema = self._quoted_schema()
        qualified_table = self._qualified_command_table()
        quoted_function = quote_control_plane_postgres_identifier("lower")
        self.adapter._execute_non_query(  # noqa: SLF001
            f"CREATE FUNCTION {quoted_schema}.{quoted_function}(integer) RETURNS integer "
            "LANGUAGE plpgsql AS $$ "
            f"BEGIN UPDATE {qualified_table} SET not_before_at = '2000-01-01 00:00:00' "
            f"WHERE command_id = '{self.command_id}'; RETURN 1; END $$",
            (),
        )
        self.addCleanup(
            lambda: self.adapter._execute_non_query(  # noqa: SLF001
                f"DROP FUNCTION IF EXISTS {quoted_schema}.{quoted_function}(integer)",
                (),
            )
        )

        # Control: the overload resolves to the mutator through the private
        # interface, proving the shadowing is live.
        invoked = self.adapter._execute_returning_one("SELECT lower(1) AS probe", ())  # noqa: SLF001
        self.assertIsNotNone(invoked)
        self.assertEqual(self._held_not_before_at(), "2000-01-01 00:00:00")
        self._restore_held_row()
        baseline = self._workflow_command_snapshot()

        for public_method in ("execute_returning_one", "execute_non_query"):
            with self.subTest(public_method=public_method):
                with self.assertRaisesRegex(ValueError, _READ_ONLY_TRANSACTION_MESSAGE):
                    getattr(self.adapter, public_method)("SELECT lower(1)", ())
                self.assertEqual(self._workflow_command_snapshot(), baseline)

    def test_public_probe_rejects_user_operator_hiding_mutator_on_real_pg(self) -> None:
        # A user operator whose procedure mutates is another catalog object the
        # lexer cannot resolve behind the ``OPERATOR(schema.##)`` construct.
        quoted_schema = self._quoted_schema()
        qualified_table = self._qualified_command_table()
        quoted_function = quote_control_plane_postgres_identifier("s1e2b_operator_mutator")
        self.adapter._execute_non_query(  # noqa: SLF001
            f"CREATE FUNCTION {quoted_schema}.{quoted_function}(integer, integer) RETURNS integer "
            "LANGUAGE plpgsql AS $$ "
            f"BEGIN UPDATE {qualified_table} SET not_before_at = '2000-01-01 00:00:00' "
            f"WHERE command_id = '{self.command_id}'; RETURN 1; END $$",
            (),
        )
        self.adapter._execute_non_query(  # noqa: SLF001
            f"CREATE OPERATOR {quoted_schema}.## "
            f"(PROCEDURE = {quoted_schema}.{quoted_function}, LEFTARG = integer, RIGHTARG = integer)",
            (),
        )
        self.addCleanup(
            lambda: self.adapter._execute_non_query(  # noqa: SLF001
                f"DROP FUNCTION IF EXISTS {quoted_schema}.{quoted_function}(integer, integer)",
                (),
            )
        )
        self.addCleanup(
            lambda: self.adapter._execute_non_query(  # noqa: SLF001
                f"DROP OPERATOR IF EXISTS {quoted_schema}.## (integer, integer)",
                (),
            )
        )

        probe_sql = f"SELECT 1 OPERATOR({quoted_schema}.##) 2"
        # Control: the operator fires the mutator through the private interface.
        invoked = self.adapter._execute_returning_one(probe_sql, ())  # noqa: SLF001
        self.assertIsNotNone(invoked)
        self.assertEqual(self._held_not_before_at(), "2000-01-01 00:00:00")
        self._restore_held_row()
        baseline = self._workflow_command_snapshot()

        for public_method in ("execute_returning_one", "execute_non_query"):
            with self.subTest(public_method=public_method):
                with self.assertRaisesRegex(ValueError, _READ_ONLY_TRANSACTION_MESSAGE):
                    getattr(self.adapter, public_method)(probe_sql, ())
                self.assertEqual(self._workflow_command_snapshot(), baseline)

    def _install_lower_overload(self, body: str, *, name: str = "lower") -> str:
        quoted_schema = self._quoted_schema()
        quoted_function = quote_control_plane_postgres_identifier(name)
        self.adapter._execute_non_query(  # noqa: SLF001
            f"CREATE FUNCTION {quoted_schema}.{quoted_function}(integer) RETURNS integer "
            f"LANGUAGE plpgsql AS $$ BEGIN {body} RETURN 1; END $$",
            (),
        )
        self.addCleanup(
            lambda: self.adapter._execute_non_query(  # noqa: SLF001
                f"DROP FUNCTION IF EXISTS {quoted_schema}.{quoted_function}(integer)",
                (),
            )
        )
        return quoted_function

    def _direct_connection(self, *, autocommit: bool = False) -> Any:
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None and psycopg is not None
        connection = psycopg.connect(fixture.dsn, client_encoding="utf8", autocommit=autocommit)
        self.addCleanup(connection.close)
        return connection

    def test_public_probe_hidden_set_config_dies_with_disposable_connection(self) -> None:
        # ``set_config(..., false)`` is not a table write, so no sqlstate 25006
        # fires: only the disposable connection keeps the poisoned GUC from
        # reaching later pool users.
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        assert fixture is not None
        self._install_lower_overload("PERFORM set_config('search_path', 'pg_catalog', false);")

        # Control: on one persistent session the overload really poisons
        # search_path — the hidden side effect is live, not hypothetical.
        control = self._direct_connection()
        quoted_schema = self._quoted_schema()
        with control.cursor() as cursor:
            cursor.execute(f"SET search_path TO {quoted_schema}")
            cursor.execute("SELECT lower(1)")
            self.assertEqual(cursor.fetchone()[0], 1)
            cursor.execute("SHOW search_path")
            self.assertNotIn(fixture.schema, str(cursor.fetchone()[0]))

        baseline = self._workflow_command_snapshot()
        invoked = self.adapter.execute_returning_one("SELECT lower(1)", ())
        self.assertIsNotNone(invoked)
        # Later probes get fresh, correctly configured sessions.
        self.assertEqual(self._held_not_before_at(), "9999-12-31 23:59:59")
        search_path = self.adapter.execute_returning_one("SHOW search_path", ())
        self.assertIsNotNone(search_path)
        assert search_path is not None
        self.assertIn(fixture.schema, str(search_path["search_path"]))
        self.assertEqual(self._workflow_command_snapshot(), baseline)

    def test_public_probe_hidden_advisory_lock_dies_with_disposable_connection(self) -> None:
        # A session advisory lock taken inside an allowlisted overload must not
        # survive the probe: the disposable connection closes and releases it.
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        assert fixture is not None
        lock_key = f"{fixture.schema}:s1e2b_hidden_probe_lock"
        self._install_lower_overload(
            "PERFORM pg_advisory_lock(hashtext(current_schema() || ':s1e2b_hidden_probe_lock'));"
        )

        invoked = self.adapter.execute_returning_one("SELECT lower(1)", ())
        self.assertIsNotNone(invoked)
        direct = self._direct_connection()
        with direct.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (lock_key,))
            self.assertTrue(cursor.fetchone()[0], "probe advisory lock leaked past the disposable connection")
            cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_key,))

        # Control: run through the private interface, which keeps a persistent
        # session — the same hidden lock then really is held against others.
        held = self.adapter._execute_returning_one("SELECT lower(1) AS probe", ())  # noqa: SLF001
        self.assertIsNotNone(held)
        with direct.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (lock_key,))
            self.assertFalse(cursor.fetchone()[0], "control lock was not held by the persistent session")

    def test_public_probe_hidden_notification_is_never_delivered(self) -> None:
        # Notifications are delivered at COMMIT; the probe rolls back, so a
        # hidden ``pg_notify`` inside an allowlisted overload never fires.
        self._install_lower_overload("PERFORM pg_notify('s1e2b_probe_notify', 'x');")
        listener = self._direct_connection(autocommit=True)
        with listener.cursor() as cursor:
            cursor.execute("LISTEN s1e2b_probe_notify")

            # Control: the private interface commits, so the same hidden
            # notification really is deliverable.
            held = self.adapter._execute_returning_one("SELECT lower(1) AS probe", ())  # noqa: SLF001
            self.assertIsNotNone(held)
            control_notifies = list(listener.notifies(timeout=5.0, stop_after=1))
            self.assertEqual(len(control_notifies), 1, "control notification was not delivered on commit")

            invoked = self.adapter.execute_returning_one("SELECT lower(1)", ())
            self.assertIsNotNone(invoked)
            self.assertEqual(
                list(listener.notifies(timeout=0.5)),
                [],
                "probe notification escaped the rollback",
            )

    def test_public_probe_rejects_hidden_temporary_state_on_real_pg(self) -> None:
        # Temporary DDL inside an allowlisted overload is still DDL: the
        # read-only transaction rejects it and nothing persists.
        qualified_table = self._qualified_command_table()
        self._install_lower_overload(
            "CREATE TEMP TABLE s1e2b_probe_poison (marker int); "
            f"INSERT INTO s1e2b_probe_poison VALUES (1); "
            f"UPDATE {qualified_table} SET not_before_at = '2000-01-01 00:00:00' "
            f"WHERE command_id = '{self.command_id}';"
        )
        baseline = self._workflow_command_snapshot()
        for public_method in ("execute_returning_one", "execute_non_query"):
            with self.subTest(public_method=public_method):
                with self.assertRaisesRegex(ValueError, _READ_ONLY_TRANSACTION_MESSAGE):
                    getattr(self.adapter, public_method)("SELECT lower(1)", ())
                self.assertEqual(self._workflow_command_snapshot(), baseline)


if __name__ == "__main__":
    unittest.main()
