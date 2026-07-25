import subprocess
import tempfile
import threading
import time
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.local_postgres import (
    normalize_control_plane_postgres_connect_dsn,
    normalize_control_plane_postgres_schema,
    quote_control_plane_postgres_identifier,
    resolve_control_plane_postgres_dsn,
)
from sourcing_agent.process_supervision import build_subprocess_env
from sourcing_agent.scripted_test_runtime import (
    FAST_HOSTED_TEST_ENV,
    _cleanup_runtime_sidecar_processes,
    _join_runtime_threads,
    build_isolated_runtime_env,
    isolated_hosted_test_runtime,
    isolated_runtime_state_paths,
    patched_environment,
    validate_isolated_runtime_env_contract,
)
from sourcing_agent.hosted_smoke_surface import HostedWorkflowSmokeClient, load_smoke_cases
from sourcing_agent.workflow_smoke import run_hosted_smoke_matrix


def _connectable_postgres_dsn() -> str:
    dsn = resolve_control_plane_postgres_dsn(Path(__file__).resolve().parents[1])
    if not str(dsn or "").strip():
        return ""
    try:
        import psycopg
    except ImportError:
        return ""
    try:
        with psycopg.connect(
            normalize_control_plane_postgres_connect_dsn(dsn),
            connect_timeout=2,
            client_encoding="utf8",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
    except Exception:
        return ""
    return str(dsn).strip()


def _reset_test_postgres_schema(schema: str) -> None:
    dsn = _connectable_postgres_dsn()
    if not dsn:
        raise unittest.SkipTest("PG-only hosted workflow runtime requires a connectable Postgres DSN")
    normalized_schema = normalize_control_plane_postgres_schema(schema)
    if not normalized_schema.startswith(("sourcing_test", "sourcing_simulate", "sourcing_scripted", "sourcing_replay")):
        raise AssertionError(f"refusing to reset unsafe test schema: {normalized_schema}")
    import psycopg

    with psycopg.connect(
        normalize_control_plane_postgres_connect_dsn(dsn),
        autocommit=True,
        connect_timeout=2,
        client_encoding="utf8",
    ) as connection:
        with connection.cursor() as cursor:
            quoted = quote_control_plane_postgres_identifier(normalized_schema)
            cursor.execute(f"DROP SCHEMA IF EXISTS {quoted} CASCADE")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted}")


class ScriptedTestRuntimeTest(unittest.TestCase):
    def _write_pg_runtime_env_file(
        self,
        path: Path,
        *,
        schema: str = "sourcing_test_scripted",
        dsn: str = "postgresql://isolated@127.0.0.1:55432/isolated_runtime",
    ) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "\n".join(
                [
                    f"SOURCING_CONTROL_PLANE_POSTGRES_DSN={dsn}",
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only",
                    f"SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA={schema}",
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1",
                    "SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return path

    def test_manual_scripted_launcher_defaults_to_pg_control_plane_and_event_watcher(self) -> None:
        completed = subprocess.run(
            ["bash", "./scripts/dev_scripted_openai_agent_delta.sh", "--print-config"],
            cwd=Path(__file__).resolve().parents[1],
            check=True,
            text=True,
            capture_output=True,
        )

        self.assertIn("control_plane=postgres", completed.stdout)
        self.assertIn("postgres_schema=sourcing_scripted_openai_agent_delta", completed.stdout)
        self.assertIn("live_provider_access_disabled=enabled", completed.stdout)
        self.assertIn("scripted_local_provider_event_watcher=enabled", completed.stdout)

    def test_manual_scripted_launcher_rejects_sqlite_control_plane_option(self) -> None:
        completed = subprocess.run(
            ["bash", "./scripts/dev_scripted_openai_agent_delta.sh", "--sqlite-control-plane", "--print-config"],
            cwd=Path(__file__).resolve().parents[1],
            check=False,
            text=True,
            capture_output=True,
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("Unknown option: --sqlite-control-plane", completed.stderr)

    def test_build_isolated_runtime_env_requires_pg_dsn_when_env_file_omitted(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "explain_matrix"
            with unittest.mock.patch(
                "sourcing_agent.scripted_test_runtime.resolve_control_plane_postgres_dsn",
                return_value="",
            ):
                with self.assertRaisesRegex(RuntimeError, "PG-only workflow confidence runtime requires"):
                    build_isolated_runtime_env(runtime_dir=runtime_dir)

    def test_build_isolated_runtime_env_generates_pg_only_env_file_when_dsn_available(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "explain_matrix"
            with unittest.mock.patch(
                "sourcing_agent.scripted_test_runtime.resolve_control_plane_postgres_dsn",
                return_value="postgresql://ambient@127.0.0.1:55432/sourcing_agent",
            ):
                env_payload, env_file = build_isolated_runtime_env(
                    runtime_dir=runtime_dir,
                    provider_mode="scripted",
                )

            self.assertTrue(env_file.exists())
            self.assertEqual(env_file.name, ".scripted-local-postgres.env")
            self.assertEqual(env_payload["SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE"], "postgres_only")
            self.assertEqual(env_payload["SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES"], "1")
            self.assertEqual(env_payload["SOURCING_PG_ONLY_SQLITE_BACKEND"], "shared_memory")
            self.assertTrue(env_payload["SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA"].startswith("sourcing_scripted_"))
            env_file_text = env_file.read_text(encoding="utf-8")
            self.assertIn("SOURCING_EXTERNAL_PROVIDER_MODE='scripted'", env_file_text)
            self.assertIn("SOURCING_RUNTIME_ENVIRONMENT='scripted'", env_file_text)
            self.assertIn("SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1", env_file_text)
            self.assertIn("SOURCING_RUNTIME_DIR=", env_file_text)
            self.assertIn("SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED=1", env_file_text)
            self.assertEqual(env_payload["SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED"], "1")
            with patched_environment(env_payload):
                self.assertEqual(
                    resolve_control_plane_postgres_dsn(runtime_dir),
                    "postgresql://ambient@127.0.0.1:55432/sourcing_agent",
                )

    def test_build_isolated_runtime_env_schema_is_unique_per_runtime_root(self) -> None:
        with tempfile.TemporaryDirectory() as left, tempfile.TemporaryDirectory() as right:
            suffix = Path("runtime") / "test_env" / "01_openai_agent_scoped_delta_streaming"
            with unittest.mock.patch(
                "sourcing_agent.scripted_test_runtime.resolve_control_plane_postgres_dsn",
                return_value="postgresql://ambient@127.0.0.1:55432/sourcing_agent",
            ):
                left_env, _left_file = build_isolated_runtime_env(
                    runtime_dir=Path(left) / suffix,
                    provider_mode="scripted",
                )
                right_env, _right_file = build_isolated_runtime_env(
                    runtime_dir=Path(right) / suffix,
                    provider_mode="scripted",
                )

            self.assertTrue(left_env["SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA"].startswith("sourcing_scripted_"))
            self.assertTrue(right_env["SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA"].startswith("sourcing_scripted_"))
            self.assertNotEqual(
                left_env["SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA"],
                right_env["SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA"],
            )

    def test_build_isolated_runtime_env_pins_stateful_paths_into_runtime_root(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "smoke"
            env_file = self._write_pg_runtime_env_file(runtime_dir / ".scripted-local-postgres.env")
            state_paths = isolated_runtime_state_paths(runtime_dir)
            env_payload, _env_file = build_isolated_runtime_env(
                runtime_dir=runtime_dir,
                runtime_env_file=env_file,
            )

            self.assertEqual(env_payload["SOURCING_RUNTIME_DIR"], str(state_paths["runtime_dir"]))
            self.assertEqual(env_payload["SOURCING_RUNTIME_ENVIRONMENT"], "simulate")
            self.assertEqual(env_payload["SOURCING_JOBS_DIR"], str(state_paths["jobs_dir"]))
            self.assertEqual(env_payload["SOURCING_COMPANY_ASSETS_DIR"], str(state_paths["company_assets_dir"]))
            self.assertEqual(env_payload["SOURCING_CANONICAL_ASSETS_DIR"], str(state_paths["canonical_assets_dir"]))
            self.assertEqual(env_payload["SOURCING_HOT_CACHE_ASSETS_DIR"], str(state_paths["hot_cache_assets_dir"]))
            self.assertEqual(env_payload["SOURCING_DB_PATH"], str(state_paths["db_path"]))
            self.assertEqual(env_payload["SOURCING_SECRETS_FILE"], str(state_paths["secrets_file"]))
            self.assertEqual(env_payload["OBJECT_STORAGE_LOCAL_DIR"], str(state_paths["object_storage_dir"]))

    def test_build_isolated_runtime_env_uses_explicit_runtime_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir)
            runtime_dir = project_root / "runtime" / "test_env" / "simulate_smoke"
            env_file = project_root / "test-runtime.env"
            self._write_pg_runtime_env_file(env_file, schema="sourcing_test_explicit_runtime")
            env_payload, _resolved_env_file = build_isolated_runtime_env(
                runtime_dir=runtime_dir,
                runtime_env_file=env_file,
                provider_mode="live",
                extra_env={"SOURCING_RUNTIME_ENVIRONMENT": "test"},
            )

            with patched_environment(env_payload):
                self.assertEqual(
                    resolve_control_plane_postgres_dsn(runtime_dir),
                    "postgresql://isolated@127.0.0.1:55432/isolated_runtime",
                )

    def test_build_isolated_runtime_env_can_opt_into_scripted_live_model_planning(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "scripted_smoke"
            env_file = self._write_pg_runtime_env_file(runtime_dir / ".scripted-local-postgres.env")
            env_payload, _env_file = build_isolated_runtime_env(
                runtime_dir=runtime_dir,
                runtime_env_file=env_file,
                provider_mode="scripted",
                extra_env={"SOURCING_SCRIPTED_LIVE_MODEL_PLANNING": "1"},
            )

            self.assertEqual(env_payload["SOURCING_EXTERNAL_PROVIDER_MODE"], "scripted")
            self.assertEqual(env_payload["SOURCING_SCRIPTED_LIVE_MODEL_PLANNING"], "1")

    def test_build_isolated_runtime_env_configures_scripted_provider_webhook_token(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "scripted_smoke"
            env_file = self._write_pg_runtime_env_file(runtime_dir / ".scripted-local-postgres.env")
            env_payload, _env_file = build_isolated_runtime_env(
                runtime_dir=runtime_dir,
                runtime_env_file=env_file,
                provider_mode="scripted",
            )

            self.assertEqual(env_payload["SOURCING_LIVE_PROVIDER_ACCESS_DISABLED"], "1")
            self.assertEqual(env_payload["APIFY_API_TOKEN"], "")
            self.assertEqual(env_payload["APIFY_WEBHOOK_TOKEN"], "")
            self.assertEqual(env_payload["DATAFORSEO_LOGIN"], "")
            self.assertEqual(env_payload["DATAFORSEO_PASSWORD"], "")
            self.assertEqual(env_payload["SERPER_API_KEY"], "")
            self.assertEqual(
                env_payload["SOURCING_SCRIPTED_PROVIDER_WEBHOOK_TOKEN"],
                "local-scripted-provider-webhook-token",
            )
            self.assertEqual(
                env_payload["SOURCING_PROVIDER_WEBHOOK_TOKEN"],
                "local-scripted-provider-webhook-token",
            )
            self.assertEqual(env_payload["SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN"], "0")

    def test_build_isolated_runtime_env_non_live_contract_overrides_ambient_provider_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "scripted_smoke"
            env_file = self._write_pg_runtime_env_file(runtime_dir / ".scripted-local-postgres.env")
            env_payload, _env_file = build_isolated_runtime_env(
                runtime_dir=runtime_dir,
                runtime_env_file=env_file,
                provider_mode="scripted",
                extra_env={
                    "APIFY_API_TOKEN": "ambient-live-apify-token",
                    "APIFY_WEBHOOK_TOKEN": "ambient-live-webhook-token",
                    "DATAFORSEO_LOGIN": "ambient-dataforseo-login",
                    "DATAFORSEO_PASSWORD": "ambient-dataforseo-password",
                    "SERPER_API_KEY": "ambient-serper-key",
                    "SOURCING_PROVIDER_WEBHOOK_TOKEN": "explicit-scripted-token",
                },
            )

            self.assertEqual(env_payload["SOURCING_LIVE_PROVIDER_ACCESS_DISABLED"], "1")
            self.assertEqual(env_payload["APIFY_API_TOKEN"], "")
            self.assertEqual(env_payload["APIFY_WEBHOOK_TOKEN"], "")
            self.assertEqual(env_payload["DATAFORSEO_LOGIN"], "")
            self.assertEqual(env_payload["DATAFORSEO_PASSWORD"], "")
            self.assertEqual(env_payload["SERPER_API_KEY"], "")
            self.assertEqual(env_payload["SOURCING_PROVIDER_WEBHOOK_TOKEN"], "explicit-scripted-token")

    def test_validate_isolated_runtime_env_contract_rejects_non_live_provider_secret_leak(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "scripted_smoke"
            env_file = self._write_pg_runtime_env_file(runtime_dir / ".scripted-local-postgres.env")
            validation = validate_isolated_runtime_env_contract(
                env_payload={
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_RUNTIME_ENVIRONMENT": "scripted",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "postgresql://isolated@127.0.0.1:55432/isolated_runtime",
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
                    "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "sourcing_test_scripted",
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "1",
                    "SOURCING_PG_ONLY_SQLITE_BACKEND": "shared_memory",
                    "SOURCING_LIVE_PROVIDER_ACCESS_DISABLED": "1",
                    "APIFY_API_TOKEN": "leaked-live-token",
                },
                runtime_dir=runtime_dir,
                runtime_env_file=env_file,
                provider_mode="scripted",
            )

            self.assertEqual(validation["status"], "failed")
            self.assertIn("live_provider_secret_not_blank:APIFY_API_TOKEN", validation["violations"])

    def test_validate_isolated_runtime_env_contract_rejects_runtime_env_file_live_provider_leak(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "scripted_smoke"
            env_file = runtime_dir / ".scripted-local-postgres.env"
            env_file.parent.mkdir(parents=True)
            env_file.write_text(
                "\n".join(
                    [
                        "SOURCING_CONTROL_PLANE_POSTGRES_DSN=postgresql://isolated@127.0.0.1:55432/isolated_runtime",
                        "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only",
                        "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA=sourcing_test_scripted",
                        "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1",
                        "SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory",
                        "SOURCING_EXTERNAL_PROVIDER_MODE=live",
                        "SOURCING_RUNTIME_ENVIRONMENT=local_dev",
                        "SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=0",
                        "export APIFY_API_TOKEN='leaked-live-token'",
                    ]
                ),
                encoding="utf-8",
            )

            validation = validate_isolated_runtime_env_contract(
                env_payload={
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_RUNTIME_ENVIRONMENT": "scripted",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "postgresql://isolated@127.0.0.1:55432/isolated_runtime",
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
                    "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "sourcing_test_scripted",
                    "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "1",
                    "SOURCING_PG_ONLY_SQLITE_BACKEND": "shared_memory",
                    "SOURCING_LIVE_PROVIDER_ACCESS_DISABLED": "1",
                    "APIFY_API_TOKEN": "",
                },
                runtime_dir=runtime_dir,
                runtime_env_file=env_file,
                provider_mode="scripted",
            )

            self.assertEqual(validation["status"], "failed")
            self.assertIn("runtime_env_file_provider_mode_live", validation["violations"])
            self.assertIn("runtime_env_file_environment_not_isolated:local_dev", validation["violations"])
            self.assertIn("runtime_env_file_live_provider_secret:APIFY_API_TOKEN", validation["violations"])
            self.assertIn(
                "runtime_env_file_SOURCING_LIVE_PROVIDER_ACCESS_DISABLED_not_disabled",
                validation["violations"],
            )

    def test_validate_isolated_runtime_env_contract_rejects_missing_pg_control_plane(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "scripted_smoke"
            env_file = runtime_dir / ".scripted-local-postgres.env"
            env_file.parent.mkdir(parents=True)
            env_file.write_text("", encoding="utf-8")

            validation = validate_isolated_runtime_env_contract(
                env_payload={
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_RUNTIME_ENVIRONMENT": "scripted",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_LIVE_PROVIDER_ACCESS_DISABLED": "1",
                    "APIFY_API_TOKEN": "",
                },
                runtime_dir=runtime_dir,
                runtime_env_file=env_file,
                provider_mode="scripted",
            )

            self.assertEqual(validation["status"], "failed")
            self.assertIn("SOURCING_CONTROL_PLANE_POSTGRES_DSN_missing", validation["violations"])
            self.assertIn("runtime_env_file_postgres_dsn_missing", validation["violations"])
            self.assertIn("SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE_not_postgres_only", validation["violations"])

    def test_build_subprocess_env_blocks_unconfirmed_isolated_live_provider_env(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir)
            (project_root / "src").mkdir(parents=True, exist_ok=True)
            runtime_dir = project_root / "runtime" / "test_env" / "scripted_case"

            env_payload = build_subprocess_env(
                project_root,
                base_env={
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_RUNTIME_ENVIRONMENT": "test",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                    "APIFY_API_TOKEN": "ambient-live-token",
                },
            )

            self.assertEqual(env_payload["SOURCING_LIVE_PROVIDER_ACCESS_DISABLED"], "1")
            self.assertEqual(env_payload["APIFY_API_TOKEN"], "")

    def test_build_subprocess_env_preserves_runtime_isolation_keys_for_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir)
            (project_root / "src").mkdir(parents=True, exist_ok=True)
            runtime_dir = project_root / "runtime" / "test_env" / "scripted_case"
            runtime_env_file = runtime_dir / ".scripted-local-postgres.env"

            env_payload = build_subprocess_env(
                project_root,
                base_env={
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_RUNTIME_ENVIRONMENT": "test",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "SOURCING_LOCAL_POSTGRES_ENV_FILE": str(runtime_env_file),
                    "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "sourcing_scripted_case",
                    "SOURCING_CONTROL_PLANE_POSTGRES_ONLY": "1",
                    "APIFY_API_TOKEN": "ambient-live-token",
                },
            )

            self.assertEqual(env_payload["SOURCING_RUNTIME_DIR"], str(runtime_dir))
            self.assertEqual(env_payload["SOURCING_RUNTIME_ENVIRONMENT"], "test")
            self.assertEqual(env_payload["SOURCING_EXTERNAL_PROVIDER_MODE"], "scripted")
            self.assertEqual(env_payload["SOURCING_LOCAL_POSTGRES_ENV_FILE"], str(runtime_env_file))
            self.assertEqual(env_payload["SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA"], "sourcing_scripted_case")
            self.assertEqual(env_payload["SOURCING_CONTROL_PLANE_POSTGRES_ONLY"], "1")
            self.assertEqual(env_payload["SOURCING_LIVE_PROVIDER_ACCESS_DISABLED"], "1")
            self.assertEqual(env_payload["APIFY_API_TOKEN"], "")

    def test_build_subprocess_env_keeps_dual_confirmed_isolated_live_provider_env(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir)
            (project_root / "src").mkdir(parents=True, exist_ok=True)
            runtime_dir = project_root / "runtime" / "test_env_live"

            env_payload = build_subprocess_env(
                project_root,
                base_env={
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "SOURCING_RUNTIME_ENVIRONMENT": "test",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                    "SOURCING_LIVE_PROVIDER_CONFIRM": "1",
                    "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS": "1",
                    "APIFY_API_TOKEN": "confirmed-live-token",
                },
            )

            self.assertNotEqual(env_payload.get("SOURCING_LIVE_PROVIDER_ACCESS_DISABLED"), "1")
            self.assertEqual(env_payload["APIFY_API_TOKEN"], "confirmed-live-token")

    def test_build_isolated_runtime_env_respects_explicit_scripted_provider_webhook_token(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "scripted_smoke"
            env_file = self._write_pg_runtime_env_file(runtime_dir / ".scripted-local-postgres.env")
            env_payload, _env_file = build_isolated_runtime_env(
                runtime_dir=runtime_dir,
                runtime_env_file=env_file,
                provider_mode="scripted",
                extra_env={"SOURCING_SCRIPTED_PROVIDER_WEBHOOK_TOKEN": "explicit-smoke-token"},
            )

            self.assertEqual(env_payload["SOURCING_SCRIPTED_PROVIDER_WEBHOOK_TOKEN"], "explicit-smoke-token")
            self.assertEqual(env_payload["SOURCING_PROVIDER_WEBHOOK_TOKEN"], "explicit-smoke-token")

    def test_isolated_runtime_cleanup_waits_for_provider_webhook_event_threads(self) -> None:
        completed: list[bool] = []

        def _run() -> None:
            time.sleep(0.05)
            completed.append(True)

        thread = threading.Thread(target=_run, name="provider-webhook-event-unit-test", daemon=True)
        thread.start()

        _join_runtime_threads()

        self.assertFalse(thread.is_alive())
        self.assertEqual(completed, [True])

    def test_isolated_runtime_cleanup_waits_for_profile_completion_refill_threads(self) -> None:
        completed: list[bool] = []

        def _run() -> None:
            time.sleep(0.05)
            completed.append(True)

        thread = threading.Thread(target=_run, name="profile-completion-refill-unit-test", daemon=True)
        thread.start()

        _join_runtime_threads()

        self.assertFalse(thread.is_alive())
        self.assertEqual(completed, [True])

    def test_isolated_runtime_cleanup_stops_detached_sidecars_for_current_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "matrix" / "01_case"
            service_dir = runtime_dir / "services" / "job-recovery-abc123"
            service_dir.mkdir(parents=True)
            pid = 23456
            (service_dir / "status.json").write_text(
                "\n".join(
                    [
                        "{",
                        '  "service_name": "job-recovery-abc123",',
                        '  "status": "running",',
                        f'  "pid": {pid},',
                        '  "owner_id": "unit-test",',
                        '  "runtime_dir": "' + str(runtime_dir) + '",',
                        '  "lock_path": "' + str(service_dir / "service.lock") + '",',
                        '  "updated_at": "2026-05-08T00:00:00+00:00",',
                        '  "poll_seconds": 5',
                        "}",
                    ]
                ),
                encoding="utf-8",
            )

            alive = {pid: True}
            signals: list[tuple[int, int]] = []

            def _process_alive(candidate_pid: int) -> bool:
                return bool(alive.get(candidate_pid, False))

            def _kill(candidate_pid: int, signum: int) -> None:
                signals.append((candidate_pid, signum))
                if signum != 0:
                    alive[candidate_pid] = False

            with unittest.mock.patch(
                "sourcing_agent.scripted_test_runtime.process_alive",
                side_effect=_process_alive,
            ), unittest.mock.patch(
                "sourcing_agent.scripted_test_runtime.os.kill",
                side_effect=_kill,
            ):
                report = _cleanup_runtime_sidecar_processes(runtime_dir, grace_seconds=0.01)

            self.assertEqual(report["cleaned_count"], 1)
            self.assertIn((pid, 15), signals)
            self.assertFalse((service_dir / "stop_request.json").exists())
            self.assertEqual(
                report["findings"][0]["stop_request"].get("target_scope"),
                "service_shutdown_fence",
            )

    def test_isolated_runtime_cleanup_after_error_leaves_no_runtime_threads(self) -> None:
        completed: list[str] = []
        dsn = _connectable_postgres_dsn()
        if not dsn:
            self.skipTest("PG-only hosted workflow runtime requires a connectable Postgres DSN")

        def _run(label: str) -> None:
            time.sleep(0.05)
            completed.append(label)

        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "scripted_matrix"
            schema = "sourcing_test_scripted_runtime_cleanup"
            _reset_test_postgres_schema(schema)
            env_file = self._write_pg_runtime_env_file(
                runtime_dir / ".scripted-local-postgres.env",
                schema=schema,
                dsn=dsn,
            )
            with self.assertRaisesRegex(RuntimeError, "simulate timeout"):
                with isolated_hosted_test_runtime(
                    runtime_dir=runtime_dir,
                    runtime_env_file=env_file,
                    provider_mode="simulate",
                ):
                    threading.Thread(
                        target=_run,
                        args=("provider",),
                        name="provider-webhook-event-cleanup-test",
                        daemon=True,
                    ).start()
                    threading.Thread(
                        target=_run,
                        args=("recovery",),
                        name="job-recovery-cleanup-test",
                        daemon=True,
                    ).start()
                    threading.Thread(
                        target=_run,
                        args=("shared",),
                        name="shared-recovery-deferred-cleanup-test",
                        daemon=True,
                    ).start()
                    threading.Thread(
                        target=_run,
                        args=("profile-refill",),
                        name="profile-completion-refill-cleanup-test",
                        daemon=True,
                    ).start()
                    raise RuntimeError("simulate timeout")

        active_names = {thread.name for thread in threading.enumerate() if thread.is_alive()}
        self.assertFalse(
            {
                "provider-webhook-event-cleanup-test",
                "job-recovery-cleanup-test",
                "shared-recovery-deferred-cleanup-test",
                "profile-completion-refill-cleanup-test",
            }
            & active_names
        )
        self.assertEqual(sorted(completed), ["profile-refill", "provider", "recovery", "shared"])

    def test_isolated_hosted_test_runtime_serves_seeded_reference_explain(self) -> None:
        dsn = _connectable_postgres_dsn()
        if not dsn:
            self.skipTest("PG-only hosted workflow runtime requires a connectable Postgres DSN")
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "scripted_matrix"
            schema = "sourcing_test_scripted_runtime_explain"
            _reset_test_postgres_schema(schema)
            env_file = self._write_pg_runtime_env_file(
                runtime_dir / ".scripted-local-postgres.env",
                schema=schema,
                dsn=dsn,
            )
            with isolated_hosted_test_runtime(
                runtime_dir=runtime_dir,
                runtime_env_file=env_file,
                seed_reference_runtime=True,
                provider_mode="simulate",
                extra_env=FAST_HOSTED_TEST_ENV,
            ) as runtime:
                self.assertEqual(runtime.postgres_prepare_result["schema"], schema)
                client = HostedWorkflowSmokeClient(runtime.base_url)
                explain = client.post(
                    "/api/workflows/explain",
                    {"raw_user_request": "我想要OpenAI做Reasoning方向的人", "top_k": 10},
                    timeout=30.0,
                )

                self.assertEqual(dict(explain.get("plan") or {}).get("target_company"), "OpenAI")
                self.assertEqual(dict(explain.get("dispatch_preview") or {}).get("strategy"), "reuse_snapshot")
                self.assertEqual(
                    dict(dict(explain.get("dispatch_preview") or {}).get("request_family_match_explanation") or {}).get(
                        "planner_mode"
                    ),
                    "reuse_snapshot_only",
                )
                health = client.get("/health", timeout=30.0)
                self.assertNotEqual(str(health.get("status") or "").strip().lower(), "failed")

    def test_full_local_reuse_smoke_matrix_enforces_progress_contract(self) -> None:
        dsn = _connectable_postgres_dsn()
        if not dsn:
            self.skipTest("PG-only hosted workflow runtime requires a connectable Postgres DSN")
        matrix_path = (
            Path(__file__).resolve().parents[1]
            / "configs"
            / "scripted"
            / "full_local_reuse_smoke_matrix.json"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "full_local_reuse_matrix"
            schema = "sourcing_test_scripted_runtime_reuse"
            _reset_test_postgres_schema(schema)
            env_file = self._write_pg_runtime_env_file(
                runtime_dir / ".scripted-local-postgres.env",
                schema=schema,
                dsn=dsn,
            )
            with isolated_hosted_test_runtime(
                runtime_dir=runtime_dir,
                runtime_env_file=env_file,
                seed_reference_runtime=True,
                provider_mode="simulate",
                extra_env=FAST_HOSTED_TEST_ENV,
            ) as runtime:
                client = HostedWorkflowSmokeClient(runtime.base_url)
                cases = load_smoke_cases(
                    str(matrix_path),
                    selected_cases={"openai_reasoning_reuse_snapshot_only"},
                )
                summaries, failures = run_hosted_smoke_matrix(
                    client=client,
                    cases=cases,
                    reviewer="scripted-runtime-test",
                    poll_seconds=0.05,
                    max_poll_seconds=20.0,
                )

        self.assertEqual(failures, [])
        self.assertEqual(len(summaries), 1)
        report = dict(summaries[0].get("provider_case_report") or {})
        self.assertEqual(dict(report.get("execution") or {}).get("planner_mode"), "reuse_snapshot_only")
        progress_observability = dict(report.get("progress_observability") or {})
        self.assertLessEqual(int(progress_observability.get("max_payload_bytes") or 0), 50000)
        latest_contract = dict(progress_observability.get("latest_execution_phase_contract") or {})
        latest_lifecycle = dict(progress_observability.get("latest_result_view_lifecycle") or {})
        self.assertFalse(bool(latest_contract.get("profile_work_pending")))
        self.assertFalse(bool(latest_lifecycle.get("delta_profile_progress_applicable")))


if __name__ == "__main__":
    unittest.main()
