from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin, psycopg


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

    def test_public_raw_sql_methods_reject_exact_command_table_mutations_before_delegate(self) -> None:
        mutations = (
            "UPDATE workflow_commands SET status = 'cancelled'",
            "DELETE FROM public.workflow_commands WHERE command_id = 'cmd_held'",
            "INSERT INTO workflow_commands (command_id) VALUES ('cmd_new')",
            "MERGE INTO workflow_commands AS target USING source ON false WHEN NOT MATCHED THEN DO NOTHING",
            "MERGE workflow_commands AS target USING source ON false WHEN NOT MATCHED THEN DO NOTHING",
            'COPY/* fence */"public"."workflow_commands" (command_id, status) FROM STDIN',
            "TRUNCATE TABLE other_table, ONLY public.workflow_commands RESTART IDENTITY",
            'ALTER TABLE IF EXISTS "public"."workflow_commands" ADD COLUMN bypass TEXT',
            "ALTER TABLE other_table RENAME TO workflow_commands",
            "DROP TABLE IF EXISTS other_table, workflow_commands CASCADE",
            "CREATE TABLE IF NOT EXISTS workflow_commands (command_id TEXT)",
            "CREATE UNIQUE INDEX command_probe ON workflow_commands (command_id)",
            "CREATE TRIGGER command_probe AFTER UPDATE ON workflow_commands EXECUTE FUNCTION probe()",
            "CREATE POLICY command_probe ON workflow_commands USING (true)",
            "WITH held AS (SELECT 'cmd_held' AS command_id) "
            "UPDATE/* fence */public./* target */workflow_commands SET status = 'cancelled'",
            "WITH removed AS (DELETE FROM workflow_commands RETURNING command_id) SELECT * FROM removed",
            "SELECT 1; DELETE FROM workflow_commands; SELECT 2",
        )
        public_methods = (
            ("execute_non_query", "_execute_non_query", 1),
            ("execute_returning_one", "_execute_returning_one", {"command_id": "cmd_held"}),
        )
        for public_name, private_name, delegate_result in public_methods:
            for sql in mutations:
                with self.subTest(public_method=public_name, sql=sql):
                    with mock.patch.object(self.adapter, private_name, return_value=delegate_result) as delegate:
                        with self.assertRaisesRegex(ValueError, "dedicated workflow command writer"):
                            getattr(self.adapter, public_name)(sql, ())
                        delegate.assert_not_called()

    def test_public_raw_sql_methods_preserve_read_probes_literals_and_other_tables(self) -> None:
        allowed_sql = (
            "SELECT * FROM workflow_commands WHERE command_id = %s",
            "SELECT 'UPDATE workflow_commands SET status = cancelled' AS diagnostic",
            "SELECT $$DELETE FROM workflow_commands$$ AS diagnostic",
            "SELECT 1 /* UPDATE workflow_commands SET status = 'cancelled' */",
            "SELECT update workflow_commands FROM diagnostic_columns",
            "WITH workflow_commands AS (SELECT 1 AS marker) SELECT marker FROM workflow_commands",
            "UPDATE workflow_commands_archive SET status = 'cancelled'",
            "DELETE FROM public.workflow_command WHERE command_id = 'cmd_other'",
            "CREATE TABLE workflow_commands_archive (command_id TEXT)",
            "ALTER TABLE other_table ADD COLUMN workflow_commands TEXT",
            "ALTER TABLE other_table RENAME COLUMN command_id TO workflow_commands",
            "COPY workflow_commands TO STDOUT",
            "UPDATE \"WORKFLOW_COMMANDS\" SET status = 'cancelled'",
        )
        public_methods = (
            ("execute_non_query", "_execute_non_query", 7),
            ("execute_returning_one", "_execute_returning_one", {"marker": 1}),
        )
        for public_name, private_name, delegate_result in public_methods:
            for sql in allowed_sql:
                with self.subTest(public_method=public_name, sql=sql):
                    with mock.patch.object(self.adapter, private_name, return_value=delegate_result) as delegate:
                        result = getattr(self.adapter, public_name)(sql, ("parameter",))
                        self.assertEqual(result, delegate_result)
                        delegate.assert_called_once_with(sql, ("parameter",))


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

    def test_public_raw_update_delete_matrix_preserves_full_held_command_table(self) -> None:
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        assert fixture is not None
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        quoted_table = quote_control_plane_postgres_identifier("workflow_commands")
        qualified_table = f"{quoted_schema}.{quoted_table}"
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
                with self.assertRaisesRegex(ValueError, "dedicated workflow command writer"):
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


if __name__ == "__main__":
    unittest.main()
