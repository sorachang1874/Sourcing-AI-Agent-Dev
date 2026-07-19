"""Connection-pool semantics for LiveControlPlanePostgresAdapter.

These tests run against a real Postgres (local docker control-plane PG or
testcontainers) and skip cleanly when no DSN is resolvable. They validate the
psycopg_pool refactor of the adapter:

- pooled connections are reused (bounded backend count under sequential ops)
- correctness under an 8-thread mixed read/write hammer
- search_path isolation across adapter instances bound to different schemas
- commit-on-clean-exit / rollback-on-exception / discard-on-close parity with
  the previous one-connection-per-call behavior
"""

from __future__ import annotations

import os
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock
from uuid import uuid4

from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter
from sourcing_agent.local_postgres import (
    normalize_control_plane_postgres_connect_dsn,
    quote_control_plane_postgres_identifier,
)
from tests.pg_durable_runtime import _pg_required, _resolve_test_postgres_dsn

try:
    import psycopg
except Exception as exc:  # pragma: no cover - depends on optional local test env.
    psycopg = None
    _PSYCOPG_IMPORT_ERROR = exc
else:
    _PSYCOPG_IMPORT_ERROR = None


class ControlPlanePostgresPoolTest(unittest.TestCase):
    dsn = ""
    _container: object | None = None

    @classmethod
    def setUpClass(cls) -> None:
        if psycopg is None:
            message = f"control-plane pool tests require psycopg: {_PSYCOPG_IMPORT_ERROR}"
            if _pg_required():
                raise AssertionError(message)
            raise unittest.SkipTest(message)
        dsn, container = _resolve_test_postgres_dsn()
        if not dsn:
            message = "control-plane pool tests require a local Postgres DSN or SOURCING_RUN_TESTCONTAINERS=1"
            if _pg_required():
                raise AssertionError(message)
            raise unittest.SkipTest(message)
        cls.dsn = normalize_control_plane_postgres_connect_dsn(dsn)
        cls._container = container

    @classmethod
    def tearDownClass(cls) -> None:
        stop = getattr(cls._container, "stop", None)
        if callable(stop):
            try:
                stop()
            except Exception:
                pass
        cls._container = None

    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._schemas: list[str] = []
        self.addCleanup(self._drop_schemas)
        # Explicit DSN env: keep _ensure_pool() on the configured endpoint and
        # away from the local-postgres bootstrap path during tests.
        env_patch = mock.patch.dict(
            os.environ, {"SOURCING_CONTROL_PLANE_POSTGRES_DSN": self.dsn}, clear=False
        )
        env_patch.start()
        self.addCleanup(env_patch.stop)

    def _drop_schemas(self) -> None:
        if not self._schemas or psycopg is None:
            return
        try:
            with psycopg.connect(self.dsn, autocommit=True, connect_timeout=5) as connection:
                with connection.cursor() as cursor:
                    for schema in self._schemas:
                        cursor.execute(
                            f"DROP SCHEMA IF EXISTS {quote_control_plane_postgres_identifier(schema)} CASCADE"
                        )
        except Exception:
            pass

    def _new_schema(self, label: str) -> str:
        schema = f"sourcing_test_pool_{label}_{uuid4().hex[:8]}"
        self._schemas.append(schema)
        return schema

    def _make_adapter(self, schema: str) -> LiveControlPlanePostgresAdapter:
        runtime_dir = Path(self._tmp.name) / schema
        runtime_dir.mkdir(parents=True, exist_ok=True)
        with mock.patch.dict(
            os.environ, {"SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": schema}, clear=False
        ):
            adapter = LiveControlPlanePostgresAdapter(
                runtime_dir=runtime_dir,
                sqlite_path=runtime_dir / "control_plane.shadow.db",
                dsn=self.dsn,
                mode="disabled",
            )
        self.assertEqual(adapter.schema, schema)
        self.addCleanup(adapter.close)
        return adapter

    def _fetch_one_fresh(self, sql: str, params: tuple = ()) -> tuple | None:
        assert psycopg is not None
        with psycopg.connect(self.dsn, autocommit=True, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchone()

    def test_sequential_ops_reuse_pooled_connections(self) -> None:
        schema = self._new_schema("reuse")
        with mock.patch.dict(
            os.environ,
            {
                "SOURCING_CONTROL_PLANE_PG_POOL_MIN": "1",
                "SOURCING_CONTROL_PLANE_PG_POOL_MAX": "2",
            },
            clear=False,
        ):
            adapter = self._make_adapter(schema)
            adapter._execute_non_query(
                "CREATE TABLE pool_probe (id INT PRIMARY KEY, value TEXT NOT NULL)", ()
            )
            backend_pids: set[int] = set()
            for index in range(40):
                adapter._execute_non_query(
                    "INSERT INTO pool_probe (id, value) VALUES (%s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
                    (index % 5, f"value-{index}"),
                )
                row = adapter.execute_returning_one("SELECT pg_backend_pid() AS backend_pid", ())
                self.assertIsNotNone(row)
                backend_pids.add(int(row["backend_pid"]))
        # 80 statements should ride at most max_size pooled connections, not
        # one fresh connection per statement like the pre-pool adapter.
        self.assertLessEqual(len(backend_pids), 2)
        live = self._fetch_one_fresh(
            "SELECT count(*) FROM pg_stat_activity WHERE pid = ANY(%s)",
            (sorted(backend_pids),),
        )
        self.assertLessEqual(int(live[0]), 2)
        # close() disposes the pool and its server backends drain.
        adapter.close()
        deadline = time.monotonic() + 10.0
        remaining = -1
        while time.monotonic() < deadline:
            remaining = int(
                self._fetch_one_fresh(
                    "SELECT count(*) FROM pg_stat_activity WHERE pid = ANY(%s)",
                    (sorted(backend_pids),),
                )[0]
            )
            if remaining == 0:
                break
            time.sleep(0.2)
        self.assertEqual(remaining, 0)

    def test_concurrent_mixed_reads_writes_stay_correct(self) -> None:
        schema = self._new_schema("hammer")
        adapter = self._make_adapter(schema)
        adapter._execute_non_query(
            "CREATE TABLE pool_hammer (id INT PRIMARY KEY, value TEXT NOT NULL, hits INT NOT NULL)",
            (),
        )

        def worker(worker_id: int) -> None:
            for step in range(12):
                row_id = (worker_id * 12 + step) % 24
                affected = adapter._execute_non_query(
                    "INSERT INTO pool_hammer (id, value, hits) VALUES (%s, %s, 1) "
                    "ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value, "
                    "hits = pool_hammer.hits + 1",
                    (row_id, f"worker-{worker_id}-step-{step}"),
                )
                if affected != 1:
                    raise AssertionError(f"unexpected rowcount {affected}")
                row = adapter.execute_returning_one(
                    "SELECT id, value, hits FROM pool_hammer WHERE id = %s", (row_id,)
                )
                if row is None or int(row["id"]) != row_id:
                    raise AssertionError(f"read-back failed for id {row_id}: {row}")

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(worker, worker_id) for worker_id in range(8)]
            for future in futures:
                future.result(timeout=120)

        quoted_schema = quote_control_plane_postgres_identifier(schema)
        totals = self._fetch_one_fresh(
            f"SELECT count(*), sum(hits) FROM {quoted_schema}.pool_hammer"
        )
        # 8 workers x 12 upserts over 24 ids: every write must have landed.
        self.assertEqual(int(totals[0]), 24)
        self.assertEqual(int(totals[1]), 96)

    def test_search_path_isolation_across_adapters(self) -> None:
        schema_a = self._new_schema("iso_a")
        schema_b = self._new_schema("iso_b")
        adapter_a = self._make_adapter(schema_a)
        adapter_b = self._make_adapter(schema_b)
        for adapter, marker in ((adapter_a, "alpha"), (adapter_b, "beta")):
            adapter._execute_non_query("CREATE TABLE pool_iso (marker TEXT NOT NULL)", ())
            adapter._execute_non_query("INSERT INTO pool_iso (marker) VALUES (%s)", (marker,))
        # Unqualified reads resolve through each adapter's own search_path.
        row_a = adapter_a.execute_returning_one("SELECT marker FROM pool_iso", ())
        row_b = adapter_b.execute_returning_one("SELECT marker FROM pool_iso", ())
        self.assertEqual(row_a["marker"], "alpha")
        self.assertEqual(row_b["marker"], "beta")
        path_a = adapter_a.execute_returning_one("SHOW search_path", ())
        path_b = adapter_b.execute_returning_one("SHOW search_path", ())
        self.assertIn(schema_a, str(path_a))
        self.assertNotIn(schema_b, str(path_a))
        self.assertIn(schema_b, str(path_b))
        self.assertNotIn(schema_a, str(path_b))
        # Schema-qualified verification from a fresh connection.
        for schema, marker in ((schema_a, "alpha"), (schema_b, "beta")):
            quoted_schema = quote_control_plane_postgres_identifier(schema)
            row = self._fetch_one_fresh(
                f"SELECT count(*), min(marker) FROM {quoted_schema}.pool_iso"
            )
            self.assertEqual((int(row[0]), row[1]), (1, marker))

    def test_advisory_lock_keys_are_schema_namespaced(self) -> None:
        # Advisory locks are database-global; without schema namespacing,
        # schema-isolated runs (per-test schemas, dev vs tests on the shared
        # local PG) contend on identical logical keys.
        schema_a = self._new_schema("lock_a")
        schema_b = self._new_schema("lock_b")
        adapter_a = self._make_adapter(schema_a)
        adapter_b = self._make_adapter(schema_b)

        logical_key = "advisory_namespace_probe:job-1"
        self.assertEqual(adapter_a._advisory_lock_key(logical_key), f"{schema_a}:{logical_key}")
        self.assertEqual(adapter_b._advisory_lock_key(logical_key), f"{schema_b}:{logical_key}")
        # Empty schema must namespace deterministically so every process in a
        # default-schema deployment computes the same lock identity.
        class _DefaultSchema:
            schema = ""

        self.assertEqual(
            LiveControlPlanePostgresAdapter._advisory_lock_key(_DefaultSchema(), logical_key),
            f"public:{logical_key}",
        )

        assert psycopg is not None
        with psycopg.connect(self.dsn, connect_timeout=5) as holder:
            with holder.cursor() as holder_cursor:
                holder_cursor.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                    (adapter_a._advisory_lock_key(logical_key),),
                )
                held = holder_cursor.fetchone()
                self.assertTrue(held and held[0])

                with psycopg.connect(self.dsn, connect_timeout=5) as probe:
                    with probe.cursor() as probe_cursor:
                        # A different schema must NOT contend on the same
                        # logical key...
                        probe_cursor.execute(
                            "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                            (adapter_b._advisory_lock_key(logical_key),),
                        )
                        cross_schema = probe_cursor.fetchone()
                        self.assertTrue(cross_schema and cross_schema[0])

                        # ...while the SAME schema keeps mutual exclusion.
                        probe_cursor.execute(
                            "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                            (adapter_a._advisory_lock_key(logical_key),),
                        )
                        same_schema = probe_cursor.fetchone()
                        self.assertFalse(same_schema and same_schema[0])

    def test_commit_rollback_and_close_semantics(self) -> None:
        schema = self._new_schema("tx")
        adapter = self._make_adapter(schema)
        quoted_schema = quote_control_plane_postgres_identifier(schema)
        adapter._execute_non_query(
            "CREATE TABLE pool_tx (id INT PRIMARY KEY, value TEXT NOT NULL)", ()
        )

        def fresh_value(row_id: int) -> tuple | None:
            return self._fetch_one_fresh(
                f"SELECT value FROM {quoted_schema}.pool_tx WHERE id = %s", (row_id,)
            )

        # (1) helper write is committed and visible from a fresh connection.
        adapter._execute_non_query(
            "INSERT INTO pool_tx (id, value) VALUES (%s, %s)", (1, "helper-commit")
        )
        self.assertEqual(fresh_value(1), ("helper-commit",))

        # (2) `with adapter._connect()` commits on clean exit without an
        # explicit commit() call (psycopg `with connect()` parity).
        with adapter._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("INSERT INTO pool_tx (id, value) VALUES (2, 'ctx-commit')")
        self.assertEqual(fresh_value(2), ("ctx-commit",))

        # (3) an exception inside the block rolls the write back.
        with self.assertRaises(RuntimeError):
            with adapter._connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("INSERT INTO pool_tx (id, value) VALUES (3, 'never')")
                raise RuntimeError("boom")
        self.assertIsNone(fresh_value(3))

        # (4) bare handle close() discards uncommitted work, matching the old
        # behavior of closing a dedicated connection mid-transaction.
        handle = adapter._connect()
        try:
            with handle.cursor() as cursor:
                cursor.execute("INSERT INTO pool_tx (id, value) VALUES (4, 'never')")
        finally:
            handle.close()
        self.assertIsNone(fresh_value(4))

        # (5) bare handle commit() then close() persists (advisory-lock flows).
        handle = adapter._connect()
        try:
            with handle.cursor() as cursor:
                cursor.execute("INSERT INTO pool_tx (id, value) VALUES (5, 'bare-commit')")
            handle.commit()
        finally:
            handle.close()
        self.assertEqual(fresh_value(5), ("bare-commit",))

    def test_pool_timeout_from_getconn_is_retried(self) -> None:
        import psycopg_pool

        schema = self._new_schema("timeout")
        adapter = self._make_adapter(schema)
        adapter._execute_non_query("CREATE TABLE pool_timeout_probe (id INT PRIMARY KEY)", ())
        pool = adapter._ensure_pool()
        real_getconn = pool.getconn
        calls = {"count": 0}

        def flaky_getconn(*args: object, **kwargs: object):
            calls["count"] += 1
            if calls["count"] == 1:
                raise psycopg_pool.PoolTimeout("simulated pool exhaustion")
            return real_getconn(*args, **kwargs)

        with mock.patch.object(pool, "getconn", side_effect=flaky_getconn):
            row = adapter.execute_returning_one("SELECT 1 AS one", ())
        self.assertIsNotNone(row)
        self.assertEqual(int(row["one"]), 1)
        self.assertGreaterEqual(calls["count"], 2)

    def test_control_plane_store_close_disposes_pool(self) -> None:
        from sourcing_agent.storage import ControlPlaneStore

        schema = self._new_schema("store_close")
        runtime_dir = Path(self._tmp.name) / "store-close-runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        with mock.patch.dict(
            os.environ,
            {
                "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": schema,
                "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
            },
            clear=False,
        ):
            store = ControlPlaneStore(runtime_dir / "sourcing_agent.db")
        adapter = store._control_plane_postgres
        pool = adapter._ensure_pool()
        self.assertFalse(getattr(pool, "closed", True))

        store.close()

        self.assertTrue(getattr(pool, "closed", False))
        self.assertIsNone(adapter._pool)
        # B4.3f: the SQLite compatibility shadow no longer exists on the store.
        self.assertFalse(hasattr(store, "_connection"))
        # Idempotent: a second close (and adapter close) must not raise.
        store.close()
        adapter.close()


class PoolTimeoutRetryClassificationTest(unittest.TestCase):
    """PoolTimeout must be in the adapter's retryable-exception classification."""

    def test_pool_timeout_is_classified_retryable(self) -> None:
        try:
            import psycopg_pool
        except Exception as exc:  # pragma: no cover - optional dependency missing
            self.skipTest(f"psycopg_pool not installed: {exc}")
        from sourcing_agent.control_plane_live_postgres import _is_retryable_postgres_exception

        self.assertTrue(_is_retryable_postgres_exception(psycopg_pool.PoolTimeout("pool exhausted")))
        self.assertFalse(_is_retryable_postgres_exception(RuntimeError("not transient")))


if __name__ == "__main__":
    unittest.main()
