"""Test-environment contract v2: ephemeral PG schema + runtime dir pairs.

`isolated_hosted_test_runtime(...)` must drop its per-runtime PG schema on
teardown (unless keep_schema=True) and must write an `.ephemeral-test-env.json`
marker pairing the runtime dir with the schema for the orphan janitor.

PG-backed tests skip cleanly when no local control-plane Postgres DSN resolves.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

from sourcing_agent.local_postgres import (
    normalize_control_plane_postgres_connect_dsn,
    quote_control_plane_postgres_identifier,
    resolve_control_plane_postgres_dsn,
)
from sourcing_agent.scripted_test_runtime import (
    EPHEMERAL_TEST_ENV_MARKER_NAME,
    _close_store_connection,
    build_isolated_runtime_env,
    drop_workflow_confidence_postgres_schema,
    ensure_isolated_runtime_env_file,
    isolated_hosted_test_runtime,
    prepare_workflow_confidence_postgres_schema,
    write_ephemeral_test_env_marker,
)

try:
    import psycopg
except Exception:  # pragma: no cover - depends on optional local test env.
    psycopg = None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _resolve_local_dsn() -> str:
    return str(resolve_control_plane_postgres_dsn(_repo_root()) or "").strip()


def _schema_exists(dsn: str, schema: str) -> bool:
    assert psycopg is not None
    connect_dsn = normalize_control_plane_postgres_connect_dsn(dsn)
    with psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,))
            return cursor.fetchone() is not None


def _drop_schema(dsn: str, schema: str) -> None:
    assert psycopg is not None
    connect_dsn = normalize_control_plane_postgres_connect_dsn(dsn)
    with psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA IF EXISTS {quote_control_plane_postgres_identifier(schema)} CASCADE")


def _create_schema(dsn: str, schema: str) -> None:
    assert psycopg is not None
    connect_dsn = normalize_control_plane_postgres_connect_dsn(dsn)
    with psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_control_plane_postgres_identifier(schema)}")


class DropWorkflowConfidenceSchemaGuardTest(unittest.TestCase):
    """Safety contract: no PG access needed, must hold even without a DSN."""

    def test_refuses_schema_outside_workflow_confidence_prefixes(self) -> None:
        result = drop_workflow_confidence_postgres_schema(
            {
                "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "postgresql://sourcing@127.0.0.1:55432/sourcing_agent",
                "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "public",
            }
        )
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "unsafe_schema")

    def test_refuses_empty_schema(self) -> None:
        result = drop_workflow_confidence_postgres_schema(
            {"SOURCING_CONTROL_PLANE_POSTGRES_DSN": "postgresql://sourcing@127.0.0.1:55432/sourcing_agent"}
        )
        self.assertEqual(result["status"], "skipped")

    def test_missing_dsn_is_fail_soft(self) -> None:
        result = drop_workflow_confidence_postgres_schema(
            {"SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "sourcing_test_anything"}
        )
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "postgres_dsn_missing")

    def test_unreachable_pg_never_raises(self) -> None:
        result = drop_workflow_confidence_postgres_schema(
            {
                "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "postgresql://nobody@127.0.0.1:1/nope",
                "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "sourcing_test_unreachable",
            }
        )
        self.assertEqual(result["status"], "failed")


class EphemeralMarkerTest(unittest.TestCase):
    def test_marker_pairs_dir_and_schema(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ephemeral-marker-") as tmp:
            marker = write_ephemeral_test_env_marker(
                tmp,
                schema="sourcing_simulate_demo_12345678",
                provider_mode="simulate",
                extra={"keep_schema": False},
            )
            self.assertIsNotNone(marker)
            assert marker is not None
            self.assertEqual(marker.name, EPHEMERAL_TEST_ENV_MARKER_NAME)
            payload = json.loads(marker.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema"], "sourcing_simulate_demo_12345678")
            self.assertEqual(payload["runtime_dir"], str(Path(tmp).resolve()))
            self.assertIn("created_at", payload)
            self.assertFalse(payload["keep_schema"])


class CloseStoreConnectionWiringTest(unittest.TestCase):
    """Teardown must dispose the full store (PG adapter pool + sqlite), not just sqlite."""

    def test_prefers_store_level_close(self) -> None:
        calls: list[str] = []

        class _Connection:
            def close(self) -> None:
                calls.append("raw_connection_close")

        class _Store:
            _connection = _Connection()

            def close(self) -> None:
                calls.append("store_close")

        class _Orchestrator:
            store = _Store()

        _close_store_connection(_Orchestrator())
        self.assertEqual(calls, ["store_close"])

    def test_falls_back_to_raw_sqlite_connection_close(self) -> None:
        calls: list[str] = []

        class _Connection:
            def close(self) -> None:
                calls.append("raw_connection_close")

        class _LegacyStore:
            _connection = _Connection()

        class _Orchestrator:
            store = _LegacyStore()

        _close_store_connection(_Orchestrator())
        self.assertEqual(calls, ["raw_connection_close"])

    def test_missing_store_is_a_noop(self) -> None:
        class _Orchestrator:
            store = None

        _close_store_connection(_Orchestrator())  # must not raise


class ScriptedRuntimeSchemaTeardownPGTest(unittest.TestCase):
    """Runs against the live local docker control-plane PG; skips without a DSN."""

    def setUp(self) -> None:
        if psycopg is None:
            self.skipTest("psycopg not installed in this test environment")
        self.dsn = _resolve_local_dsn()
        if not self.dsn:
            self.skipTest("no local control-plane Postgres DSN resolved (make local-pg-up)")
        self._env_patch = mock.patch.dict(
            os.environ,
            {"SOURCING_CONTROL_PLANE_POSTGRES_DSN": self.dsn},
            clear=False,
        )
        self._env_patch.start()
        self.addCleanup(self._env_patch.stop)
        self._cleanup_schemas: list[str] = []

    def tearDown(self) -> None:
        for schema in self._cleanup_schemas:
            try:
                _drop_schema(self.dsn, schema)
            except Exception:
                pass

    def test_prepare_and_drop_helpers_pair(self) -> None:
        with tempfile.TemporaryDirectory(prefix="schema-pair-") as tmp:
            env_payload, _ = build_isolated_runtime_env(runtime_dir=tmp)
            prepare = prepare_workflow_confidence_postgres_schema(env_payload)
            schema = str(prepare["schema"])
            self._cleanup_schemas.append(schema)
            self.assertTrue(_schema_exists(self.dsn, schema))
            result = drop_workflow_confidence_postgres_schema(env_payload)
            self.assertEqual(result, {"status": "dropped", "schema": schema})
            self.assertFalse(_schema_exists(self.dsn, schema))

    def test_prepare_applies_migrations_before_seeding(self) -> None:
        with tempfile.TemporaryDirectory(prefix="schema-migrate-") as tmp:
            env_payload, _ = build_isolated_runtime_env(runtime_dir=tmp)
            prepare = prepare_workflow_confidence_postgres_schema(env_payload)
            schema = str(prepare["schema"])
            self._cleanup_schemas.append(schema)
            self.assertIn("0001_baseline", prepare["migrations_applied"])
            connect_dsn = normalize_control_plane_postgres_connect_dsn(self.dsn)
            with psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = %s AND table_name = %s",
                        (schema, "organization_asset_registry"),
                    )
                    self.assertIsNotNone(cursor.fetchone())
                    cursor.execute(
                        f"SELECT count(*) FROM {quote_control_plane_postgres_identifier(schema)}.schema_migrations"
                    )
                    row = cursor.fetchone()
                    self.assertIsNotNone(row)
                    self.assertGreaterEqual(int(row[0]), 14)
            second = prepare_workflow_confidence_postgres_schema(env_payload)
            self.assertEqual(second["migrations_applied"], [])

    def test_isolated_hosted_test_runtime_seeds_fresh_schema(self) -> None:
        with tempfile.TemporaryDirectory(prefix="seed-fresh-") as tmp:
            with isolated_hosted_test_runtime(runtime_dir=tmp, seed_reference_runtime=True) as runtime:
                schema = str(runtime.postgres_prepare_result["schema"])
                self._cleanup_schemas.append(schema)
                self.assertTrue(runtime.seed_result)

    def test_cli_build_runtime_store_bootstraps_fresh_schema(self) -> None:
        from sourcing_agent import cli as cli_module

        schema = f"sourcing_test_cli_bootstrap_{uuid.uuid4().hex[:12]}"
        self._cleanup_schemas.append(schema)
        with tempfile.TemporaryDirectory(prefix="cli-store-") as tmp:
            with mock.patch.dict(
                os.environ,
                {"SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": schema},
                clear=False,
            ):
                settings = type("FakeSettings", (), {"db_path": Path(tmp) / "test.db"})()
                store = cli_module.build_runtime_store(settings)
                try:
                    connect_dsn = normalize_control_plane_postgres_connect_dsn(self.dsn)
                    with psycopg.connect(connect_dsn, autocommit=True, connect_timeout=5, client_encoding="utf8") as connection:
                        with connection.cursor() as cursor:
                            cursor.execute(
                                f"SELECT count(*) FROM {quote_control_plane_postgres_identifier(schema)}.schema_migrations"
                            )
                            row = cursor.fetchone()
                            self.assertIsNotNone(row)
                            self.assertGreaterEqual(int(row[0]), 14)
                            cursor.execute(
                                "SELECT 1 FROM information_schema.tables "
                                "WHERE table_schema = %s AND table_name = %s",
                                (schema, "jobs"),
                            )
                            self.assertIsNotNone(cursor.fetchone())
                finally:
                    store.close()

    def test_isolated_hosted_test_runtime_drops_schema_and_writes_marker(self) -> None:
        with tempfile.TemporaryDirectory(prefix="teardown-drop-") as tmp:
            with isolated_hosted_test_runtime(runtime_dir=tmp) as runtime:
                schema = str(runtime.postgres_prepare_result["schema"])
                self._cleanup_schemas.append(schema)
                self.assertTrue(schema.startswith("sourcing_simulate_"))
                self.assertTrue(_schema_exists(self.dsn, schema))
                # Self-created schema: prepare must record that we own it.
                self.assertFalse(bool(runtime.postgres_prepare_result["pre_existing"]))
                marker_path = runtime.runtime_dir / EPHEMERAL_TEST_ENV_MARKER_NAME
                self.assertTrue(marker_path.is_file())
                payload = json.loads(marker_path.read_text(encoding="utf-8"))
                self.assertEqual(payload["schema"], schema)
                self.assertEqual(payload["runtime_dir"], str(runtime.runtime_dir))
                self.assertIn("created_at", payload)
                self.assertFalse(payload["pre_existing"])
            self.assertFalse(_schema_exists(self.dsn, schema))

    def test_pre_existing_schema_from_supplied_env_file_survives_teardown(self) -> None:
        pre_existing_schema = f"sourcing_test_preexisting_{uuid.uuid4().hex[:8]}"
        self._cleanup_schemas.append(pre_existing_schema)
        _create_schema(self.dsn, pre_existing_schema)
        with tempfile.TemporaryDirectory(prefix="teardown-preexisting-") as tmp:
            runtime_root = Path(tmp)
            # Simulate a user-supplied --runtime-env-file naming a schema the
            # caller owns (run_explain_dry_run_matrix / run_simulate_smoke_matrix
            # forward such files verbatim).
            env_file = ensure_isolated_runtime_env_file(runtime_root)
            rewritten = re.sub(
                r"^export SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA=.*$",
                f"export SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA='{pre_existing_schema}'",
                env_file.read_text(encoding="utf-8"),
                flags=re.MULTILINE,
            )
            env_file.write_text(rewritten, encoding="utf-8")
            with isolated_hosted_test_runtime(
                runtime_dir=runtime_root, runtime_env_file=env_file
            ) as runtime:
                self.assertEqual(
                    str(runtime.postgres_prepare_result["schema"]), pre_existing_schema
                )
                self.assertTrue(bool(runtime.postgres_prepare_result["pre_existing"]))
                marker_payload = json.loads(
                    (runtime.runtime_dir / EPHEMERAL_TEST_ENV_MARKER_NAME).read_text(encoding="utf-8")
                )
                self.assertTrue(marker_payload["pre_existing"])
            # We did not create the schema, so teardown must not drop it.
            self.assertTrue(_schema_exists(self.dsn, pre_existing_schema))

    def test_isolated_hosted_test_runtime_keep_schema_opts_out(self) -> None:
        with tempfile.TemporaryDirectory(prefix="teardown-keep-") as tmp:
            with isolated_hosted_test_runtime(runtime_dir=tmp, keep_schema=True) as runtime:
                schema = str(runtime.postgres_prepare_result["schema"])
                self._cleanup_schemas.append(schema)
                marker_payload = json.loads(
                    (runtime.runtime_dir / EPHEMERAL_TEST_ENV_MARKER_NAME).read_text(encoding="utf-8")
                )
                self.assertTrue(marker_payload["keep_schema"])
            self.assertTrue(_schema_exists(self.dsn, schema))


if __name__ == "__main__":
    unittest.main()
