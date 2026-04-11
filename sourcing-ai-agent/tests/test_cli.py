import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
import json

from sourcing_agent import cli


class CliWorkflowRunnerTest(unittest.TestCase):
    def test_runner_environment_prepends_src_to_pythonpath(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir)
            with mock.patch.dict(os.environ, {"PYTHONPATH": "/existing/path"}, clear=False):
                env = cli._runner_environment(project_root)

        self.assertEqual(
            env["PYTHONPATH"],
            os.pathsep.join([str((project_root / "src").resolve()), "/existing/path"]),
        )
        self.assertEqual(env["PYTHONUNBUFFERED"], "1")

    def test_spawn_workflow_runner_uses_project_src_pythonpath(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir) / "project"
            runtime_dir = project_root / "runtime"
            log_path = runtime_dir / "service_logs" / "workflow-runner-job-123.log"
            project_root.mkdir(parents=True, exist_ok=True)

            catalog = SimpleNamespace(project_root=project_root)
            settings = SimpleNamespace(runtime_dir=runtime_dir)

            with mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog), mock.patch.object(
                cli, "load_settings", return_value=settings
            ), mock.patch.object(
                cli,
                "_spawn_detached_process",
                return_value={
                    "status": "started",
                    "pid": 43210,
                    "log_path": str(log_path),
                    "command": [
                        cli.sys.executable,
                        "-m",
                        "sourcing_agent.cli",
                        "supervise-workflow",
                        "--job-id",
                        "job-123",
                        "--auto-job-daemon",
                    ],
                },
            ) as spawn_detached:
                result = cli.spawn_workflow_runner("job-123", auto_job_daemon=True)

            self.assertEqual(result["status"], "started")
            self.assertEqual(result["job_id"], "job-123")
            self.assertEqual(result["pid"], 43210)
            spawn_detached.assert_called_once()
            kwargs = spawn_detached.call_args.kwargs
            self.assertEqual(kwargs["cwd"], project_root)
            self.assertEqual(kwargs["log_path"], log_path)
            self.assertEqual(
                kwargs["command"],
                [
                    cli.sys.executable,
                    "-m",
                    "sourcing_agent.cli",
                    "supervise-workflow",
                    "--job-id",
                    "job-123",
                    "--auto-job-daemon",
                ],
            )
            self.assertIn("PYTHONPATH", kwargs["env"])
            self.assertTrue(str((project_root / "src").resolve()) in kwargs["env"]["PYTHONPATH"])

    def test_spawn_workflow_runner_reports_early_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir) / "project"
            runtime_dir = project_root / "runtime"
            project_root.mkdir(parents=True, exist_ok=True)

            catalog = SimpleNamespace(project_root=project_root)
            settings = SimpleNamespace(runtime_dir=runtime_dir)

            with mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog), mock.patch.object(
                cli, "load_settings", return_value=settings
            ), mock.patch.object(
                cli,
                "_spawn_detached_process",
                return_value={
                    "status": "failed_to_start",
                    "pid": 54321,
                    "exit_code": 1,
                    "log_path": str(runtime_dir / "service_logs" / "workflow-runner-job-early-exit.log"),
                    "log_tail": "runner boot failed",
                    "command": [cli.sys.executable],
                },
            ):
                result = cli.spawn_workflow_runner("job-early-exit", auto_job_daemon=False)

            self.assertEqual(result["status"], "failed_to_start")
            self.assertEqual(result["pid"], 54321)
            self.assertEqual(result["exit_code"], 1)
            self.assertIn("runner boot failed", result["log_tail"])

    def test_start_workflow_runner_with_handshake_delegates_to_orchestrator(self) -> None:
        orchestrator = mock.Mock()
        orchestrator._start_workflow_runner_with_handshake = mock.Mock(  # noqa: SLF001
            return_value={"status": "started_deferred", "runner": {"pid": 3001}}
        )

        result = cli.start_workflow_runner_with_handshake(
            orchestrator,
            job_id="job-3",
            auto_job_daemon=True,
            handshake_timeout_seconds=0.2,
            poll_seconds=0.15,
            max_attempts=4,
        )

        self.assertEqual(result["status"], "started_deferred")
        orchestrator._start_workflow_runner_with_handshake.assert_called_once_with(  # noqa: SLF001
            job_id="job-3",
            auto_job_daemon=True,
            handshake_timeout_seconds=0.2,
            poll_seconds=0.15,
            max_attempts=4,
        )

    def test_start_workflow_command_delegates_to_runner_managed_entrypoint(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            request_path = Path(tempdir) / "workflow.json"
            request_path.write_text('{"target_company":"Reflection AI"}', encoding="utf-8")
            orchestrator = mock.Mock()
            orchestrator.start_workflow_runner_managed = mock.Mock(return_value={"status": "queued", "job_id": "job-123"})

            with mock.patch.object(cli, "build_orchestrator", return_value=orchestrator), mock.patch.object(
                cli.sys,
                "argv",
                ["cli", "start-workflow", "--file", str(request_path)],
            ), mock.patch("builtins.print") as print_mock:
                cli.main()

        orchestrator.start_workflow_runner_managed.assert_called_once()
        payload = orchestrator.start_workflow_runner_managed.call_args[0][0]
        self.assertEqual(payload["target_company"], "Reflection AI")
        self.assertTrue(bool(payload["auto_job_daemon"]))
        print_mock.assert_called_once()

    def test_show_system_progress_command_delegates_to_orchestrator(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.get_system_progress = mock.Mock(return_value={"status": "ok", "workflow_jobs": {"count": 0}})

        with mock.patch.object(cli, "build_orchestrator", return_value=orchestrator), mock.patch.object(
            cli.sys,
            "argv",
            ["cli", "show-system-progress", "--active-limit", "7", "--object-sync-limit", "4", "--profile-registry-lookback-hours", "12", "--force-refresh"],
        ), mock.patch("builtins.print") as print_mock:
            cli.main()

        orchestrator.get_system_progress.assert_called_once_with(
            {
                "active_limit": 7,
                "object_sync_limit": 4,
                "profile_registry_lookback_hours": 12,
                "force_refresh": True,
            }
        )
        print_mock.assert_called_once()

    def test_serve_command_starts_watchdog_without_blocking_bootstrap_pass(self) -> None:
        orchestrator = mock.Mock()
        server = mock.Mock()
        server.serve_forever.side_effect = KeyboardInterrupt()
        watchdog_stop = mock.Mock()
        watchdog_thread = mock.Mock()

        with mock.patch.object(cli, "build_orchestrator", return_value=orchestrator), mock.patch.object(
            cli,
            "start_server_runtime_watchdog",
            return_value=(watchdog_stop, watchdog_thread),
        ) as start_watchdog_mock, mock.patch.object(
            cli,
            "create_server",
            return_value=server,
        ), mock.patch.object(
            cli,
            "run_server_runtime_watchdog_once",
        ) as bootstrap_once_mock, mock.patch.object(
            cli.sys,
            "argv",
            ["cli", "serve", "--host", "127.0.0.1", "--port", "8765"],
        ), mock.patch("builtins.print"):
            cli.main()

        start_watchdog_mock.assert_called_once()
        bootstrap_once_mock.assert_not_called()
        server.server_close.assert_called_once()
        watchdog_stop.set.assert_called_once()
        watchdog_thread.join.assert_called_once()

    def test_upload_asset_bundle_command_defaults_to_auto_archive_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "bundle_manifest.json"
            manifest_path.write_text(json.dumps({"bundle_id": "bundle_1", "bundle_kind": "company_snapshot"}), encoding="utf-8")
            manager = mock.Mock()
            manager.upload_bundle = mock.Mock(return_value={"status": "uploaded", "bundle_id": "bundle_1"})
            storage_client = object()

            with mock.patch.object(cli, "build_asset_bundle_manager", return_value=manager), mock.patch.object(
                cli,
                "build_object_storage",
                return_value=storage_client,
            ), mock.patch.object(
                cli.sys,
                "argv",
                ["cli", "upload-asset-bundle", "--manifest", str(manifest_path)],
            ), mock.patch("builtins.print") as print_mock:
                cli.main()

        manager.upload_bundle.assert_called_once_with(
            str(manifest_path),
            storage_client,
            max_workers=None,
            resume=True,
            archive_mode="auto",
        )
        print_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
