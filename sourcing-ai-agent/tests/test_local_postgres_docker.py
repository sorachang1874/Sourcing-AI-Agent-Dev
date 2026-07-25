import os
import socket
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent import local_postgres_docker
from sourcing_agent.local_postgres import (
    resolve_control_plane_postgres_dsn,
    resolve_default_control_plane_postgres_live_mode,
)

_EXPECTED_DEFAULT_DSN = "postgresql://sourcing@127.0.0.1:55432/sourcing_agent"


def _free_loopback_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


class DockerLocalPostgresEnvFileTest(unittest.TestCase):
    """Env-file mechanics; no Docker daemon required."""

    def test_default_settings_match_existing_local_postgres_dsn_convention(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            settings = local_postgres_docker.resolve_docker_local_postgres_settings()

        self.assertEqual(settings["container"], "sourcing-local-postgres")
        self.assertEqual(settings["volume"], "sourcing-local-postgres-data")
        self.assertEqual(settings["image"], "postgres:16-alpine")
        self.assertEqual(settings["port"], 55432)
        self.assertEqual(settings["user"], "sourcing")
        self.assertEqual(settings["database"], "sourcing_agent")
        self.assertEqual(settings["dsn"], _EXPECTED_DEFAULT_DSN)

    def test_written_env_file_is_discovered_by_existing_dsn_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)

            with mock.patch.dict(os.environ, {}, clear=True):
                result = local_postgres_docker.write_docker_local_postgres_env_file(repo_root)
                dsn = resolve_control_plane_postgres_dsn(repo_root)
                live_mode = resolve_default_control_plane_postgres_live_mode(base_dir=repo_root)

            self.assertEqual(result["status"], "written")
            self.assertEqual(
                Path(result["path"]).resolve(), (repo_root / ".local-postgres.env").resolve()
            )
            self.assertEqual(dsn, _EXPECTED_DEFAULT_DSN)
            self.assertEqual(live_mode, "postgres_only")

    def test_write_env_file_is_idempotent_for_managed_content(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)

            with mock.patch.dict(os.environ, {}, clear=True):
                first = local_postgres_docker.write_docker_local_postgres_env_file(repo_root)
                second = local_postgres_docker.write_docker_local_postgres_env_file(repo_root)

            self.assertEqual(first["status"], "written")
            self.assertEqual(second["status"], "unchanged")

    def test_write_env_file_keeps_unmanaged_file_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            env_file = repo_root / ".local-postgres.env"
            unmanaged_content = (
                "SOURCING_CONTROL_PLANE_POSTGRES_DSN=postgresql://someone@127.0.0.1:5432/other\n"
            )
            env_file.write_text(unmanaged_content, encoding="utf-8")

            with mock.patch.dict(os.environ, {}, clear=True):
                result = local_postgres_docker.write_docker_local_postgres_env_file(repo_root)

            self.assertEqual(result["status"], "kept_unmanaged")
            self.assertEqual(env_file.read_text(encoding="utf-8"), unmanaged_content)

    def test_write_env_file_force_preserves_previous_unmanaged_lines_as_comments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            env_file = repo_root / ".local-postgres.env"
            previous_dsn_line = (
                "SOURCING_CONTROL_PLANE_POSTGRES_DSN=postgresql://someone@127.0.0.1:5432/other"
            )
            env_file.write_text(previous_dsn_line + "\n", encoding="utf-8")

            with mock.patch.dict(os.environ, {}, clear=True):
                result = local_postgres_docker.write_docker_local_postgres_env_file(repo_root, force=True)
                dsn = resolve_control_plane_postgres_dsn(repo_root)

            content = env_file.read_text(encoding="utf-8")
            self.assertEqual(result["status"], "written")
            self.assertEqual(dsn, _EXPECTED_DEFAULT_DSN)
            self.assertIn(f"# previous-unmanaged: {previous_dsn_line}", content)

    def test_dsn_targets_docker_local_postgres_matches_loopback_port_only(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertTrue(
                local_postgres_docker.dsn_targets_docker_local_postgres(_EXPECTED_DEFAULT_DSN)
            )
            self.assertTrue(
                local_postgres_docker.dsn_targets_docker_local_postgres(
                    "postgresql://other_user@localhost:55432/other_db"
                )
            )
            self.assertFalse(
                local_postgres_docker.dsn_targets_docker_local_postgres(
                    "postgresql://sourcing@127.0.0.1:5432/sourcing_agent"
                )
            )
            self.assertFalse(
                local_postgres_docker.dsn_targets_docker_local_postgres(
                    "postgresql://sourcing@db.example.com:55432/sourcing_agent"
                )
            )
            self.assertFalse(local_postgres_docker.dsn_targets_docker_local_postgres(""))


class DockerLocalPostgresLifecycleTest(unittest.TestCase):
    """Container lifecycle against a throwaway container; skips when Docker is absent."""

    def setUp(self) -> None:
        if not local_postgres_docker.docker_runtime_available(force_refresh=True):
            self.skipTest("docker daemon unavailable; skipping docker local postgres lifecycle test")

    def test_ensure_status_and_stop_lifecycle_with_throwaway_container(self) -> None:
        suffix = f"unittest-{os.getpid()}"
        container = f"sourcing-local-postgres-{suffix}"
        volume = f"sourcing-local-postgres-data-{suffix}"
        port = _free_loopback_port()
        overrides = {
            "SOURCING_LOCAL_POSTGRES_DOCKER_CONTAINER": container,
            "SOURCING_LOCAL_POSTGRES_DOCKER_VOLUME": volume,
            "SOURCING_LOCAL_POSTGRES_DOCKER_PORT": str(port),
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            with mock.patch.dict(os.environ, overrides, clear=False):
                os.environ.pop("SOURCING_LOCAL_POSTGRES_ENV_FILE", None)
                try:
                    result = local_postgres_docker.ensure_docker_local_postgres_started(
                        repo_root,
                        wait_timeout_seconds=120.0,
                        write_env_file=True,
                        force_env_file=True,
                    )
                    self.assertIn(
                        result["status"],
                        {"running", "started"},
                        msg=f"unexpected ensure result: {result}",
                    )
                    expected_dsn = f"postgresql://sourcing@127.0.0.1:{port}/sourcing_agent"
                    self.assertEqual(result["dsn"], expected_dsn)
                    self.assertEqual(
                        Path(result["env_file"]).resolve(),
                        (repo_root / ".local-postgres.env").resolve(),
                    )

                    with mock.patch.dict(os.environ, {}, clear=True):
                        with mock.patch.dict(os.environ, overrides, clear=False):
                            resolved = resolve_control_plane_postgres_dsn(repo_root)
                    self.assertEqual(resolved, expected_dsn)

                    status = local_postgres_docker.docker_local_postgres_status(repo_root)
                    self.assertTrue(status["docker_available"])
                    self.assertTrue(status["container_exists"])
                    self.assertTrue(status["running"])
                    self.assertTrue(status["ready"])
                    self.assertTrue(status["port_open"])
                    self.assertTrue(status["env_file_managed"])
                    self.assertTrue(status["env_file_dsn_matches"])

                    # Idempotent re-ensure short-circuits to "running".
                    second = local_postgres_docker.ensure_docker_local_postgres_started(
                        repo_root, wait_timeout_seconds=30.0, write_env_file=False
                    )
                    self.assertEqual(second["status"], "running")

                    try:
                        import psycopg

                        with psycopg.connect(
                            expected_dsn + "?gssencmode=disable", connect_timeout=5
                        ) as connection:
                            with connection.cursor() as cursor:
                                cursor.execute("SELECT current_user, current_database()")
                                row = cursor.fetchone()
                        self.assertEqual(tuple(row), ("sourcing", "sourcing_agent"))
                    except ImportError:
                        pass
                finally:
                    stop_result = local_postgres_docker.stop_docker_local_postgres(
                        repo_root, remove_container=True, remove_volume=True
                    )
                self.assertIn(stop_result["status"], {"removed", "stopped", "not_found"})
                after = local_postgres_docker.docker_local_postgres_status(repo_root)
                self.assertFalse(after["container_exists"])

    def test_stop_missing_container_reports_not_found(self) -> None:
        overrides = {
            "SOURCING_LOCAL_POSTGRES_DOCKER_CONTAINER": f"sourcing-local-postgres-missing-{os.getpid()}",
        }
        with mock.patch.dict(os.environ, overrides, clear=False):
            result = local_postgres_docker.stop_docker_local_postgres()
        self.assertEqual(result["status"], "not_found")


if __name__ == "__main__":
    unittest.main()
