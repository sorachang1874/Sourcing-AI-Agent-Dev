import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.local_postgres import (
    configure_control_plane_postgres_session,
    describe_control_plane_runtime,
    discover_local_postgres_root,
    ensure_local_postgres_started,
    local_postgres_is_running,
    normalize_control_plane_postgres_connect_dsn,
    normalize_control_plane_postgres_schema,
    resolve_control_plane_postgres_dsn,
    resolve_default_control_plane_db_path,
    resolve_default_control_plane_postgres_live_mode,
    resolve_control_plane_postgres_schema,
    resolve_local_postgres_settings,
)


def _resolved_path_text(value: object) -> str:
    return str(Path(str(value)).resolve())


def _create_fake_local_postgres_root(root: Path, *, with_pg_ctl: bool = False) -> Path:
    extract_dir = root / "extract" / "usr/lib/postgresql/16/bin"
    data_dir = root / "data"
    run_dir = root / "run"
    extract_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(parents=True, exist_ok=True)
    if with_pg_ctl:
        pg_ctl = extract_dir / "pg_ctl"
        pg_ctl.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        pg_ctl.chmod(0o755)
    return root


class LocalPostgresResolverTest(unittest.TestCase):
    def test_resolver_discovers_workspace_local_postgres_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir)
            repo_root = workspace_root / "nested" / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            local_root = _create_fake_local_postgres_root(workspace_root / ".local-postgres")

            with mock.patch.dict(os.environ, {}, clear=True):
                discovered = discover_local_postgres_root(repo_root)
                settings = resolve_local_postgres_settings(repo_root)
                dsn = resolve_control_plane_postgres_dsn(repo_root)
                live_mode = resolve_default_control_plane_postgres_live_mode(base_dir=repo_root)

            self.assertEqual(_resolved_path_text(discovered), _resolved_path_text(local_root))
            self.assertTrue(settings["available"])
            self.assertEqual(_resolved_path_text(settings["root"]), _resolved_path_text(local_root))
            self.assertEqual(dsn, "postgresql://sourcing@127.0.0.1:55432/sourcing_agent")
            self.assertEqual(live_mode, "postgres_only")

    def test_explicit_env_dsn_wins_over_local_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            _create_fake_local_postgres_root(repo_root / ".local-postgres")

            with mock.patch.dict(
                os.environ,
                {"SOURCING_CONTROL_PLANE_POSTGRES_DSN": "postgresql://remote:5432/custom"},
                clear=True,
            ):
                dsn = resolve_control_plane_postgres_dsn(repo_root)

            self.assertEqual(dsn, "postgresql://remote:5432/custom")

    def test_env_file_dsn_enables_postgres_only_without_linux_local_postgres_assets(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            runtime_dir = repo_root / "runtime"
            repo_root.mkdir(parents=True, exist_ok=True)
            runtime_dir.mkdir(parents=True, exist_ok=True)
            (repo_root / ".local-postgres.env").write_text(
                "\n".join(
                    (
                        "SOURCING_CONTROL_PLANE_POSTGRES_DSN=postgresql://mac@127.0.0.1:55432/sourcing_agent",
                        "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only",
                    )
                )
                + "\n",
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {}, clear=True):
                dsn = resolve_control_plane_postgres_dsn(repo_root)
                live_mode = resolve_default_control_plane_postgres_live_mode(base_dir=repo_root)
                summary = describe_control_plane_runtime(base_dir=repo_root, runtime_dir=runtime_dir)

            self.assertEqual(dsn, "postgresql://mac@127.0.0.1:55432/sourcing_agent")
            self.assertEqual(live_mode, "postgres_only")
            self.assertEqual(summary["resolved_control_plane_postgres_dsn"], dsn)
            self.assertEqual(
                _resolved_path_text(summary["default_db_path"]),
                _resolved_path_text(runtime_dir / "control_plane.shadow.db"),
            )
            self.assertFalse(summary["local_postgres"]["available"])
            self.assertEqual(
                _resolved_path_text(summary["local_postgres"]["env_file"]),
                _resolved_path_text(repo_root / ".local-postgres.env"),
            )

    def test_resolve_default_control_plane_db_path_switches_to_shadow_when_pg_is_discoverable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir) / "repo"
            runtime_dir = project_root / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            _create_fake_local_postgres_root(project_root / ".local-postgres")

            with mock.patch.dict(os.environ, {}, clear=True):
                resolved_path = resolve_default_control_plane_db_path(runtime_dir, base_dir=project_root)

            self.assertEqual(_resolved_path_text(resolved_path), _resolved_path_text(runtime_dir / "control_plane.shadow.db"))

    def test_describe_control_plane_runtime_reports_local_pg_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir) / "repo"
            runtime_dir = project_root / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            local_root = _create_fake_local_postgres_root(project_root / ".local-postgres")

            with mock.patch.dict(os.environ, {}, clear=True):
                summary = describe_control_plane_runtime(base_dir=project_root, runtime_dir=runtime_dir)

            self.assertEqual(summary["resolved_control_plane_postgres_live_mode"], "postgres_only")
            self.assertEqual(
                _resolved_path_text(summary["default_db_path"]),
                _resolved_path_text(runtime_dir / "control_plane.shadow.db"),
            )
            local_postgres = dict(summary["local_postgres"])
            self.assertTrue(local_postgres["available"])
            self.assertEqual(_resolved_path_text(local_postgres["root"]), _resolved_path_text(local_root))

    def test_ensure_local_postgres_started_reports_missing_pg_ctl_without_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            local_root = _create_fake_local_postgres_root(repo_root / ".local-postgres", with_pg_ctl=False)

            with mock.patch.dict(os.environ, {"LOCAL_PG_PORT": "65432"}, clear=True):
                self.assertFalse(local_postgres_is_running(repo_root))
                result = ensure_local_postgres_started(repo_root)

            self.assertEqual(result["status"], "missing_pg_ctl")
            self.assertEqual(_resolved_path_text(result["root"]), _resolved_path_text(local_root))

    def test_normalize_control_plane_postgres_connect_dsn_disables_gss_for_loopback_urls(self) -> None:
        normalized = normalize_control_plane_postgres_connect_dsn(
            "postgresql://tester@127.0.0.1:5432/sourcing_agent"
        )

        self.assertEqual(normalized, "postgresql://tester@127.0.0.1:5432/sourcing_agent?gssencmode=disable")

    def test_normalize_control_plane_postgres_connect_dsn_preserves_explicit_gssencmode(self) -> None:
        normalized = normalize_control_plane_postgres_connect_dsn(
            "postgresql://tester@127.0.0.1:5432/sourcing_agent?sslmode=disable&gssencmode=prefer"
        )

        self.assertEqual(
            normalized,
            "postgresql://tester@127.0.0.1:5432/sourcing_agent?sslmode=disable&gssencmode=prefer",
        )

    def test_runtime_isolated_postgres_schema_defaults_to_runtime_namespace(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            with mock.patch.dict(os.environ, {"SOURCING_RUNTIME_ENVIRONMENT": "scripted"}, clear=True):
                self.assertEqual(resolve_control_plane_postgres_schema(repo_root), "sourcing_scripted")
            with mock.patch.dict(
                os.environ,
                {
                    "SOURCING_RUNTIME_ENVIRONMENT": "scripted",
                    "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "Team Smoke-01!",
                },
                clear=True,
            ):
                self.assertEqual(resolve_control_plane_postgres_schema(repo_root), "team_smoke_01")

    def test_configure_control_plane_postgres_session_bootstraps_schema_search_path(self) -> None:
        executed: list[str] = []

        class FakeCursor:
            def __enter__(self) -> "FakeCursor":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def execute(self, sql: str) -> None:
                executed.append(sql)

        class FakeConnection:
            def cursor(self) -> FakeCursor:
                return FakeCursor()

        configure_control_plane_postgres_session(FakeConnection(), schema="Team Smoke-01!")

        self.assertEqual(normalize_control_plane_postgres_schema("Team Smoke-01!"), "team_smoke_01")
        self.assertIn('CREATE SCHEMA IF NOT EXISTS "team_smoke_01"', executed)
        self.assertIn('SET search_path TO "team_smoke_01", public', executed)


if __name__ == "__main__":
    unittest.main()
