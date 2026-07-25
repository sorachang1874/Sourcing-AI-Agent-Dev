from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from sourcing_agent.migration_runner import discover_migrations
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import psycopg
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class PGControlPlaneStoreFixtureTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    """Pin per-class schema ownership across repeated test-method setup."""

    _bootstrap_tempdir: tempfile.TemporaryDirectory[str] | None = None
    _bootstrap_migration_versions: tuple[str, ...] = ()
    _written_job_ids: list[str] = []

    @classmethod
    def _migration_versions(cls) -> tuple[str, ...]:
        fixture = cls._pg_store_fixture
        assert fixture is not None
        assert psycopg is not None
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        with psycopg.connect(
            fixture.dsn,
            autocommit=True,
            connect_timeout=5,
            client_encoding="utf8",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT version FROM {quoted_schema}.schema_migrations ORDER BY version")
                return tuple(str(row[0]) for row in cursor.fetchall())

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._written_job_ids = []
        bootstrap_tempdir = tempfile.TemporaryDirectory()
        cls._bootstrap_tempdir = bootstrap_tempdir
        try:
            store = ControlPlaneStore(Path(bootstrap_tempdir.name) / "bootstrap.db")
            try:
                store._control_plane_postgres.ensure_bootstrapped()  # noqa: SLF001
                store.save_job(
                    job_id="fixture-bootstrap-job",
                    job_type="workflow",
                    status="running",
                    stage="planning",
                    request_payload={"raw_user_request": "fixture reset contract"},
                )
            finally:
                store.close()
            cls._bootstrap_migration_versions = cls._migration_versions()
            expected_versions = tuple(version for version, _ in discover_migrations())
            if cls._bootstrap_migration_versions != expected_versions:
                raise AssertionError(
                    "fixture bootstrap did not record the complete migration history: "
                    f"actual={cls._bootstrap_migration_versions!r} expected={expected_versions!r}"
                )
        except BaseException:
            bootstrap_tempdir.cleanup()
            cls._bootstrap_tempdir = None
            super().tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls) -> None:
        bootstrap_tempdir = cls._bootstrap_tempdir
        cls._bootstrap_tempdir = None
        if bootstrap_tempdir is not None:
            bootstrap_tempdir.cleanup()
        super().tearDownClass()

    def setUp(self) -> None:
        super().setUp()
        self._test_tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._test_tempdir.cleanup)
        self.store = self.make_pg_store(Path(self._test_tempdir.name) / "test.db")

    def _assert_reset_contract_then_write(self, job_id: str) -> None:
        self.assertEqual(self._migration_versions(), self._bootstrap_migration_versions)
        self.assertIsNone(self.store.get_job("fixture-bootstrap-job"))
        for previous_job_id in self._written_job_ids:
            self.assertIsNone(self.store.get_job(previous_job_id))

        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="planning",
            request_payload={"raw_user_request": "fixture reset contract"},
        )
        self.assertIsNotNone(self.store.get_job(job_id))
        self._written_job_ids.append(job_id)

    def test_first_method_setup_preserves_history_and_resets_domain_rows(self) -> None:
        self._assert_reset_contract_then_write("fixture-method-one-job")

    def test_repeated_method_setup_preserves_history_and_resets_domain_rows(self) -> None:
        self._assert_reset_contract_then_write("fixture-method-two-job")


if __name__ == "__main__":
    unittest.main()
