import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

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
            captured: dict[str, object] = {}

            class FakeProcess:
                pid = 43210

            def fake_popen(command, **kwargs):  # type: ignore[no-untyped-def]
                captured["command"] = command
                captured["kwargs"] = kwargs
                return FakeProcess()

            with mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog), mock.patch.object(
                cli, "load_settings", return_value=settings
            ), mock.patch.object(cli.subprocess, "Popen", side_effect=fake_popen):
                result = cli.spawn_workflow_runner("job-123", auto_job_daemon=True)

            self.assertEqual(result["status"], "started")
            self.assertEqual(result["job_id"], "job-123")
            self.assertEqual(result["pid"], 43210)
            self.assertEqual(
                captured["command"],
                [
                    cli.sys.executable,
                    "-m",
                    "sourcing_agent.cli",
                    "execute-workflow",
                    "--job-id",
                    "job-123",
                    "--auto-job-daemon",
                ],
            )
            kwargs = captured["kwargs"]
            self.assertEqual(kwargs["cwd"], str(project_root))
            self.assertEqual(kwargs["stdin"], cli.subprocess.DEVNULL)
            self.assertEqual(kwargs["stderr"], cli.subprocess.STDOUT)
            self.assertTrue(kwargs["start_new_session"])
            self.assertIn("PYTHONPATH", kwargs["env"])
            self.assertTrue(str((project_root / "src").resolve()) in kwargs["env"]["PYTHONPATH"])
            self.assertTrue(log_path.exists())

    def test_spawn_workflow_runner_reports_early_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir) / "project"
            runtime_dir = project_root / "runtime"
            project_root.mkdir(parents=True, exist_ok=True)

            catalog = SimpleNamespace(project_root=project_root)
            settings = SimpleNamespace(runtime_dir=runtime_dir)

            class FakeProcess:
                pid = 54321

                def poll(self):  # type: ignore[no-untyped-def]
                    return 1

            def fake_popen(command, **kwargs):  # type: ignore[no-untyped-def]
                kwargs["stdout"].write(b"runner boot failed\n")
                kwargs["stdout"].flush()
                return FakeProcess()

            with mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog), mock.patch.object(
                cli, "load_settings", return_value=settings
            ), mock.patch.object(cli.subprocess, "Popen", side_effect=fake_popen):
                result = cli.spawn_workflow_runner("job-early-exit", auto_job_daemon=False)

            self.assertEqual(result["status"], "failed_to_start")
            self.assertEqual(result["pid"], 54321)
            self.assertEqual(result["exit_code"], 1)
            self.assertIn("runner boot failed", result["log_tail"])


if __name__ == "__main__":
    unittest.main()
