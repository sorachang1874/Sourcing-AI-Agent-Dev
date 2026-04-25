import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from sourcing_agent import cli


class CliWorkflowRunnerTest(unittest.TestCase):
    def test_legacy_sqlite_runtime_banner_marks_non_pg_runtime_as_warning(self) -> None:
        banner = cli._legacy_sqlite_runtime_banner(  # noqa: SLF001
            {
                "control_plane_postgres_live_mode": "disabled",
                "sqlite_shadow_backend": "disk",
            }
        )

        self.assertEqual(banner["status"], "legacy_sqlite_runtime")
        self.assertEqual(banner["severity"], "warning")
        self.assertIn("production/live hosted traffic must use Postgres", banner["message"])

    def test_legacy_sqlite_runtime_banner_accepts_pg_only_ephemeral_shadow(self) -> None:
        banner = cli._legacy_sqlite_runtime_banner(  # noqa: SLF001
            {
                "control_plane_postgres_live_mode": "postgres_only",
                "sqlite_shadow_backend": "shared_memory",
            }
        )

        self.assertEqual(banner["status"], "pg_only")
        self.assertEqual(banner["severity"], "ok")

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

            with (
                mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
                mock.patch.object(cli, "load_settings", return_value=settings),
                mock.patch.object(
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
                ) as spawn_detached,
            ):
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

            with (
                mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
                mock.patch.object(cli, "load_settings", return_value=settings),
                mock.patch.object(
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
                ),
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

    def test_launch_detached_command_resolves_relative_paths_and_writes_status_file(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir) / "project"
            project_root.mkdir(parents=True, exist_ok=True)
            catalog = SimpleNamespace(project_root=project_root)
            spawned_payload = {
                "status": "started",
                "pid": 321,
                "log_path": str(project_root / "runtime" / "service_logs" / "detached.log"),
                "command": ["python3", "-m", "http.server"],
            }

            with (
                mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
                mock.patch.object(cli, "_spawn_detached_process", return_value=spawned_payload) as spawn_mock,
            ):
                result = cli.launch_detached_command(
                    command=["python3", "-m", "http.server"],
                    log_path="runtime/service_logs/detached.log",
                    cwd="src",
                    description="test server",
                    status_path="runtime/service_logs/detached.status.json",
                    startup_wait_seconds=0.5,
                )
                written_status = json.loads(
                    (project_root / "runtime" / "service_logs" / "detached.status.json").read_text(encoding="utf-8")
                )

        expected_cwd = (project_root / "src").resolve()
        expected_log_path = (project_root / "runtime" / "service_logs" / "detached.log").resolve()
        expected_status_path = (project_root / "runtime" / "service_logs" / "detached.status.json").resolve()
        spawn_mock.assert_called_once_with(
            command=["python3", "-m", "http.server"],
            cwd=expected_cwd,
            log_path=expected_log_path,
            env=mock.ANY,
            startup_wait_seconds=0.5,
        )
        self.assertEqual(result["status"], "started")
        self.assertEqual(result["description"], "test server")
        self.assertEqual(result["cwd"], str(expected_cwd))
        self.assertEqual(result["status_path"], str(expected_status_path))
        self.assertEqual(written_status["pid"], 321)
        self.assertEqual(written_status["description"], "test server")

    def test_launch_detached_command_requires_non_empty_command(self) -> None:
        with self.assertRaises(ValueError):
            cli.launch_detached_command(command=[], log_path="runtime/service_logs/test.log")

    def test_launch_detached_main_command_strips_separator(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir) / "project"
            project_root.mkdir(parents=True, exist_ok=True)
            catalog = SimpleNamespace(project_root=project_root)

            with (
                mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
                mock.patch.object(
                    cli,
                    "launch_detached_command",
                    return_value={"status": "started", "pid": 654, "log_path": "runtime/service_logs/task.log"},
                ) as launch_mock,
                mock.patch.object(
                    cli.sys,
                    "argv",
                    [
                        "cli",
                        "launch-detached",
                        "--log-path",
                        "runtime/service_logs/task.log",
                        "--",
                        "python3",
                        "-m",
                        "http.server",
                    ],
                ),
                mock.patch("builtins.print") as print_mock,
            ):
                cli.main()

        launch_mock.assert_called_once_with(
            command=["python3", "-m", "http.server"],
            log_path="runtime/service_logs/task.log",
            cwd="",
            description="",
            startup_wait_seconds=0.2,
            status_path="",
        )
        print_mock.assert_called_once()

    def test_start_workflow_command_defaults_to_hosted_entrypoint(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            request_path = Path(tempdir) / "workflow.json"
            request_path.write_text('{"target_company":"Reflection AI"}', encoding="utf-8")
            orchestrator = mock.Mock()

            with (
                mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
                mock.patch.object(
                    cli,
                    "_submit_hosted_workflow_request",
                    return_value={"status": "queued", "job_id": "job-123"},
                ) as hosted_submit_mock,
                mock.patch.object(
                    cli.sys,
                    "argv",
                    ["cli", "start-workflow", "--file", str(request_path)],
                ),
                mock.patch("builtins.print") as print_mock,
            ):
                cli.main()

        hosted_submit_mock.assert_called_once()
        payload = hosted_submit_mock.call_args.args[0]
        self.assertEqual(payload["target_company"], "Reflection AI")
        self.assertEqual(hosted_submit_mock.call_args.kwargs["base_url"], "http://127.0.0.1:8765")
        print_mock.assert_called_once()

    def test_start_workflow_command_hosted_mode_uses_configured_api_base_url(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            request_path = Path(tempdir) / "workflow.json"
            request_path.write_text('{"target_company":"Google"}', encoding="utf-8")
            orchestrator = mock.Mock()

            with (
                mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
                mock.patch.object(
                    cli,
                    "_submit_hosted_workflow_request",
                    return_value={"status": "queued", "job_id": "job-789"},
                ) as hosted_submit_mock,
                mock.patch.object(
                    cli.sys,
                    "argv",
                    [
                        "cli",
                        "start-workflow",
                        "--file",
                        str(request_path),
                        "--hosted-api-base-url",
                        "http://127.0.0.1:9999",
                        "--hosted-api-timeout-seconds",
                        "9",
                    ],
                ),
                mock.patch("builtins.print"),
            ):
                cli.main()

        hosted_submit_mock.assert_called_once()
        self.assertEqual(hosted_submit_mock.call_args.kwargs["base_url"], "http://127.0.0.1:9999")
        self.assertEqual(hosted_submit_mock.call_args.kwargs["timeout_seconds"], 9.0)

    def test_start_workflow_command_can_request_managed_subprocess_entrypoint(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            request_path = Path(tempdir) / "workflow.json"
            request_path.write_text('{"target_company":"Google"}', encoding="utf-8")
            orchestrator = mock.Mock()
            orchestrator.start_workflow_runner_managed = mock.Mock(
                return_value={"status": "queued", "job_id": "job-456"}
            )

            with (
                mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
                mock.patch.object(
                    cli.sys,
                    "argv",
                    [
                        "cli",
                        "start-workflow",
                        "--file",
                        str(request_path),
                        "--runtime-execution-mode",
                        "managed_subprocess",
                    ],
                ),
                mock.patch("builtins.print") as print_mock,
            ):
                cli.main()

        orchestrator.start_workflow_runner_managed.assert_called_once()
        payload = orchestrator.start_workflow_runner_managed.call_args[0][0]
        self.assertEqual(payload["target_company"], "Google")
        self.assertEqual(payload["runtime_execution_mode"], "managed_subprocess")
        self.assertTrue(bool(payload["auto_job_daemon"]))
        print_mock.assert_called_once()

    def test_show_system_progress_command_delegates_to_orchestrator(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.get_system_progress = mock.Mock(return_value={"status": "ok", "workflow_jobs": {"count": 0}})

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "show-system-progress",
                    "--active-limit",
                    "7",
                    "--object-sync-limit",
                    "4",
                    "--profile-registry-lookback-hours",
                    "12",
                    "--force-refresh",
                ],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
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

    def test_intake_excel_command_delegates_to_orchestrator(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workbook_path = Path(tempdir) / "contacts.xlsx"
            workbook_path.write_bytes(b"placeholder")
            orchestrator = mock.Mock()
            orchestrator.ingest_excel_contacts = mock.Mock(return_value={"status": "completed", "intake_id": "excel-1"})

            with (
                mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
                mock.patch.object(
                    cli.sys,
                    "argv",
                    ["cli", "intake-excel", "--file", str(workbook_path)],
                ),
                mock.patch("builtins.print") as print_mock,
            ):
                cli.main()

        orchestrator.ingest_excel_contacts.assert_called_once_with(
            {
                "file_path": str(workbook_path),
                "target_company": "",
                "snapshot_id": "",
                "attach_to_snapshot": False,
            }
        )
        print_mock.assert_called_once()

    def test_repair_company_candidate_artifacts_command_delegates_to_repair_helper(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"), db_path=Path("/tmp/project/runtime/sourcing_agent.db")
        )
        store = mock.Mock()

        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(
                cli,
                "load_settings",
                return_value=settings,
            ),
            mock.patch.object(
                cli,
                "SQLiteStore",
                return_value=store,
            ),
            mock.patch.object(
                cli,
                "repair_missing_company_candidate_artifacts",
                return_value={"status": "completed", "repaired_snapshot_count": 1},
            ) as repair_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                ["cli", "repair-company-candidate-artifacts", "--company", "Acme", "--snapshot-id", "20260406T120000"],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        repair_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            store=store,
            companies=["Acme"],
            snapshot_id="20260406T120000",
            force_rebuild_artifacts=False,
        )
        print_mock.assert_called_once()

    def test_backfill_structured_timeline_command_delegates_to_helper(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"), db_path=Path("/tmp/project/runtime/sourcing_agent.db")
        )
        store = mock.Mock()

        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(
                cli,
                "load_settings",
                return_value=settings,
            ),
            mock.patch.object(
                cli,
                "SQLiteStore",
                return_value=store,
            ),
            mock.patch.object(
                cli,
                "backfill_structured_timeline_for_company_assets",
                return_value={"status": "completed", "artifact_backfill": {"force_rebuilt_snapshot_count": 2}},
            ) as backfill_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "backfill-structured-timeline",
                    "--company",
                    "Acme",
                    "--snapshot-id",
                    "20260406T120000",
                    "--skip-profile-registry-backfill",
                    "--skip-registry-refresh",
                    "--profile-no-resume",
                    "--profile-progress-interval",
                    "25",
                ],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        backfill_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            store=store,
            companies=["Acme"],
            snapshot_id="20260406T120000",
            backfill_profile_registry=False,
            profile_resume=False,
            profile_progress_interval=25,
            refresh_registry=False,
        )
        print_mock.assert_called_once()

    def test_rebuild_runtime_control_plane_command_delegates_to_helper(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"), db_path=Path("/tmp/project/runtime/sourcing_agent.db")
        )
        store = mock.Mock()

        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(cli, "load_settings", return_value=settings),
            mock.patch.object(cli, "SQLiteStore", return_value=store),
            mock.patch.object(
                cli,
                "rebuild_runtime_control_plane",
                return_value={"status": "completed", "company_assets": {}, "jobs": {}},
            ) as rebuild_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "rebuild-runtime-control-plane",
                    "--company",
                    "Acme",
                    "--snapshot-id",
                    "20260406T120000",
                    "--skip-jobs",
                ],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        rebuild_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            store=store,
            companies=["Acme"],
            snapshot_id="20260406T120000",
            rebuild_missing_artifacts=True,
            rebuild_company_assets=True,
            rebuild_jobs=False,
        )
        print_mock.assert_called_once()

    def test_continue_excel_intake_command_delegates_to_orchestrator(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            payload_path = Path(tempdir) / "continue.json"
            payload = {
                "intake_id": "excel-1",
                "decisions": [
                    {
                        "row_key": "Contacts#2",
                        "action": "select_local_candidate",
                        "selected_candidate_id": "cand-2",
                    }
                ],
            }
            payload_path.write_text(json.dumps(payload), encoding="utf-8")
            orchestrator = mock.Mock()
            orchestrator.continue_excel_intake_review = mock.Mock(
                return_value={"status": "completed", "intake_id": "excel-1"}
            )

            with (
                mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
                mock.patch.object(
                    cli.sys,
                    "argv",
                    ["cli", "continue-excel-intake", "--file", str(payload_path)],
                ),
                mock.patch("builtins.print") as print_mock,
            ):
                cli.main()

        orchestrator.continue_excel_intake_review.assert_called_once_with(payload)
        print_mock.assert_called_once()

    def test_serve_command_starts_watchdog_without_blocking_bootstrap_pass(self) -> None:
        orchestrator = mock.Mock()
        server = mock.Mock()
        server.serve_forever.side_effect = KeyboardInterrupt()
        shared_recovery_stop = mock.Mock()
        shared_recovery_thread = mock.Mock()
        watchdog_stop = mock.Mock()
        watchdog_thread = mock.Mock()

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(
                cli,
                "start_shared_recovery_service",
                return_value=(shared_recovery_stop, shared_recovery_thread),
            ) as start_shared_recovery_mock,
            mock.patch.object(
                cli,
                "start_server_runtime_watchdog",
                return_value=(watchdog_stop, watchdog_thread),
            ) as start_watchdog_mock,
            mock.patch.object(
                cli,
                "create_server",
                return_value=server,
            ),
            mock.patch.object(
                cli,
                "run_server_runtime_watchdog_once",
            ) as bootstrap_once_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                ["cli", "serve", "--host", "127.0.0.1", "--port", "8765"],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        start_shared_recovery_mock.assert_called_once_with(orchestrator)
        start_watchdog_mock.assert_called_once()
        bootstrap_once_mock.assert_not_called()
        server.server_close.assert_called_once()
        watchdog_stop.set.assert_called_once()
        shared_recovery_stop.set.assert_called_once()
        watchdog_thread.join.assert_called_once()
        shared_recovery_thread.join.assert_called_once()

    def test_upload_asset_bundle_command_defaults_to_auto_archive_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "bundle_manifest.json"
            manifest_path.write_text(
                json.dumps({"bundle_id": "bundle_1", "bundle_kind": "company_snapshot"}), encoding="utf-8"
            )
            manager = mock.Mock()
            manager.upload_bundle = mock.Mock(return_value={"status": "uploaded", "bundle_id": "bundle_1"})
            storage_client = object()

            with (
                mock.patch.object(cli, "build_asset_bundle_manager", return_value=manager),
                mock.patch.object(
                    cli,
                    "build_object_storage",
                    return_value=storage_client,
                ),
                mock.patch.object(
                    cli.sys,
                    "argv",
                    ["cli", "upload-asset-bundle", "--manifest", str(manifest_path)],
                ),
                mock.patch("builtins.print") as print_mock,
            ):
                cli.main()

        manager.upload_bundle.assert_called_once_with(
            str(manifest_path),
            storage_client,
            max_workers=None,
            resume=True,
            archive_mode="auto",
        )
        print_mock.assert_called_once()

    def test_delete_asset_bundle_command_records_gc_ledger(self) -> None:
        manager = mock.Mock()
        manager.runtime_dir = Path("/tmp/runtime")
        manager.delete_bundle = mock.Mock(
            return_value={
                "status": "deleted",
                "bundle_kind": "company_snapshot",
                "bundle_id": "bundle_1",
                "sync_run_id": "delete_run_1",
            }
        )
        storage_client = object()
        store = mock.Mock()
        store.record_cloud_asset_operation.return_value = {
            "ledger_id": 1,
            "operation_type": "gc_delete_bundle",
        }

        with (
            mock.patch.object(cli, "build_asset_bundle_manager", return_value=manager),
            mock.patch.object(
                cli,
                "build_object_storage",
                return_value=storage_client,
            ),
            mock.patch.object(
                cli,
                "SQLiteStore",
                return_value=store,
            ),
            mock.patch.object(
                cli.sys,
                "argv",
                ["cli", "delete-asset-bundle", "--bundle-kind", "company_snapshot", "--bundle-id", "bundle_1"],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        manager.delete_bundle.assert_called_once_with(
            bundle_kind="company_snapshot",
            bundle_id="bundle_1",
            client=storage_client,
            max_workers=None,
            prune_local_index=True,
        )
        store.record_cloud_asset_operation.assert_called_once()
        printed_payload = json.loads(print_mock.call_args.args[0])
        self.assertEqual(printed_payload["ledger"]["operation_type"], "gc_delete_bundle")

    def test_export_control_plane_snapshot_command_uses_runtime_defaults(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"),
            db_path=Path("/tmp/project/runtime/sourcing_agent.db"),
        )

        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(
                cli,
                "load_settings",
                return_value=settings,
            ),
            mock.patch.object(
                cli,
                "export_control_plane_snapshot",
                return_value={
                    "status": "exported",
                    "output_path": "/tmp/project/runtime/object_sync/control_plane/control_plane_snapshot.json",
                },
            ) as export_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                ["cli", "export-control-plane-snapshot", "--table", "jobs", "--table", "job_result_views"],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        export_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            output_path=settings.runtime_dir / "object_sync" / "control_plane" / "control_plane_snapshot.json",
            sqlite_path=settings.db_path,
            tables=["jobs", "job_result_views"],
            include_all_sqlite_tables=False,
        )
        print_mock.assert_called_once()

    def test_export_control_plane_snapshot_command_can_include_all_sqlite_tables(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"),
            db_path=Path("/tmp/project/runtime/sourcing_agent.db"),
        )

        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(cli, "load_settings", return_value=settings),
            mock.patch.object(
                cli,
                "export_control_plane_snapshot",
                return_value={"status": "exported"},
            ) as export_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                ["cli", "export-control-plane-snapshot", "--all-sqlite-tables"],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        export_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            output_path=settings.runtime_dir / "object_sync" / "control_plane" / "control_plane_snapshot.json",
            sqlite_path=settings.db_path,
            tables=[],
            include_all_sqlite_tables=True,
        )

    def test_sync_control_plane_postgres_command_delegates_to_helper(self) -> None:
        snapshot_path = "/tmp/control-plane.json"
        with (
            mock.patch.object(
                cli,
                "sync_control_plane_snapshot_to_postgres",
                return_value={"status": "synced", "table_count": 2},
            ) as sync_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "sync-control-plane-postgres",
                    "--snapshot",
                    snapshot_path,
                    "--dsn",
                    "postgresql://user:pass@localhost:5432/sourcing",
                    "--table",
                    "jobs",
                    "--truncate-first",
                ],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        sync_mock.assert_called_once_with(
            snapshot_path=snapshot_path,
            dsn="postgresql://user:pass@localhost:5432/sourcing",
            tables=["jobs"],
            truncate_first=True,
            validate_postgres=False,
        )
        print_mock.assert_called_once()

    def test_sync_control_plane_postgres_snapshot_command_can_validate_all_tables(self) -> None:
        snapshot_path = "/tmp/control-plane.json"
        with (
            mock.patch.object(
                cli,
                "sync_control_plane_snapshot_to_postgres",
                return_value={"status": "synced", "table_count": 2},
            ) as sync_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "sync-control-plane-postgres",
                    "--snapshot",
                    snapshot_path,
                    "--dsn",
                    "postgresql://user:pass@localhost:5432/sourcing",
                    "--validate-postgres",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        sync_mock.assert_called_once_with(
            snapshot_path=snapshot_path,
            dsn="postgresql://user:pass@localhost:5432/sourcing",
            tables=None,
            truncate_first=False,
            validate_postgres=True,
        )

    def test_sync_control_plane_postgres_command_can_mirror_runtime_directly(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"),
            db_path=Path("/tmp/project/runtime/sourcing_agent.db"),
        )
        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(cli, "load_settings", return_value=settings),
            mock.patch.object(
                cli,
                "sync_runtime_control_plane_to_postgres",
                return_value={"status": "synced", "table_count_synced": 2},
            ) as mirror_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "sync-control-plane-postgres",
                    "--dsn",
                    "postgresql://user:pass@localhost:5432/sourcing",
                    "--table",
                    "jobs",
                    "--min-interval-seconds",
                    "30",
                    "--force",
                ],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        mirror_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            sqlite_path=settings.db_path,
            dsn="postgresql://user:pass@localhost:5432/sourcing",
            tables=["jobs"],
            truncate_first=False,
            state_path=None,
            min_interval_seconds=30.0,
            force=True,
            include_all_sqlite_tables=False,
            validate_postgres=False,
            direct_stream=False,
            chunk_size=0,
            commit_every_chunks=0,
            progress_every_chunks=0,
            chunk_pause_seconds=0.0,
        )
        print_mock.assert_called_once()

    def test_sync_control_plane_postgres_runtime_command_can_include_all_sqlite_tables_and_validate(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"),
            db_path=Path("/tmp/project/runtime/sourcing_agent.db"),
        )
        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(cli, "load_settings", return_value=settings),
            mock.patch.object(
                cli,
                "sync_runtime_control_plane_to_postgres",
                return_value={"status": "synced", "table_count_synced": 2},
            ) as mirror_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "sync-control-plane-postgres",
                    "--dsn",
                    "postgresql://user:pass@localhost:5432/sourcing",
                    "--all-sqlite-tables",
                    "--validate-postgres",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        mirror_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            sqlite_path=settings.db_path,
            dsn="postgresql://user:pass@localhost:5432/sourcing",
            tables=[],
            truncate_first=False,
            state_path=None,
            min_interval_seconds=0.0,
            force=False,
            include_all_sqlite_tables=True,
            validate_postgres=True,
            direct_stream=False,
            chunk_size=0,
            commit_every_chunks=0,
            progress_every_chunks=0,
            chunk_pause_seconds=0.0,
        )

    def test_sync_control_plane_postgres_runtime_command_can_direct_stream(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"),
            db_path=Path("/tmp/project/runtime/sourcing_agent.db"),
        )
        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(cli, "load_settings", return_value=settings),
            mock.patch.object(
                cli,
                "sync_runtime_control_plane_to_postgres",
                return_value={"status": "synced", "table_count_synced": 2},
            ) as mirror_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "sync-control-plane-postgres",
                    "--dsn",
                    "postgresql://user:pass@localhost:5432/sourcing",
                    "--all-sqlite-tables",
                    "--validate-postgres",
                    "--direct-stream",
                    "--chunk-size",
                    "25",
                    "--commit-every-chunks",
                    "10",
                    "--progress-every-chunks",
                    "5",
                    "--chunk-pause-seconds",
                    "0.01",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        mirror_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            sqlite_path=settings.db_path,
            dsn="postgresql://user:pass@localhost:5432/sourcing",
            tables=[],
            truncate_first=False,
            state_path=None,
            min_interval_seconds=0.0,
            force=False,
            include_all_sqlite_tables=True,
            validate_postgres=True,
            direct_stream=True,
            chunk_size=25,
            commit_every_chunks=10,
            progress_every_chunks=5,
            chunk_pause_seconds=0.01,
        )


if __name__ == "__main__":
    unittest.main()
