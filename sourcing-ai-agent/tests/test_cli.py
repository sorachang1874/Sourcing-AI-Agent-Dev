import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from sourcing_agent import cli
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class CliWorkflowRunnerTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def test_control_plane_storage_banner_marks_non_pg_runtime_as_error(self) -> None:
        banner = cli._control_plane_storage_banner(  # noqa: SLF001
            {
                "control_plane_postgres_live_mode": "disabled",
                "sqlite_shadow_backend": "disk",
            }
        )

        self.assertEqual(banner["status"], "non_pg_control_plane")
        self.assertEqual(banner["severity"], "error")
        self.assertIn("Disk-backed live SQLite control-plane mode is retired", banner["message"])

    def test_control_plane_storage_banner_accepts_pg_only_ephemeral_shadow(self) -> None:
        banner = cli._control_plane_storage_banner(  # noqa: SLF001
            {
                "control_plane_postgres_live_mode": "postgres_only",
                "compatibility_shadow_backend": "shared_memory",
            }
        )

        self.assertEqual(banner["status"], "pg_only")
        self.assertEqual(banner["severity"], "ok")
        self.assertIn("ephemeral compatibility shadow", banner["message"])

    def test_retired_sqlite_tool_confirmation_helper_is_removed(self) -> None:
        self.assertFalse(hasattr(cli, "_require_legacy_sqlite_tool_confirmation"))

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

    def test_run_worker_daemon_once_prints_json_safe_recovery_payload(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.run_worker_recovery_once = mock.Mock(
            return_value={
                "status": "completed",
                "runtime_path": Path("/tmp/runtime/projection-state.json"),
                "candidate_ids": [f"candidate-{index}" for index in range(30)],
                "items": [
                    {
                        "status": "partial",
                        "profile_url_batch": [
                            f"https://www.linkedin.com/in/profile-{index}/" for index in range(29)
                        ],
                        "queued_urls": [f"https://www.linkedin.com/in/member-{index}/" for index in range(28)],
                    }
                ],
            }
        )

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "run-worker-daemon-once",
                    "--job-id",
                    "job-123",
                    "--disable-profile-prefetch-refill",
                ],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        orchestrator.run_worker_recovery_once.assert_called_once()
        printed_payload = json.loads(print_mock.call_args.args[0])
        self.assertEqual(printed_payload["runtime_path"], "/tmp/runtime/projection-state.json")
        self.assertEqual(printed_payload["candidate_ids"]["count"], 30)
        self.assertEqual(len(printed_payload["candidate_ids"]["sample"]), 24)
        self.assertTrue(printed_payload["candidate_ids"]["truncated"])
        self.assertEqual(printed_payload["items"][0]["queued_urls"]["count"], 28)
        self.assertEqual(printed_payload["items"][0]["profile_url_batch"]["count"], 29)

    def test_refresh_company_public_web_assets_command_delegates_to_api_cli_lane(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.refresh_company_public_web_assets = mock.Mock(
            return_value={"status": "completed", "summary": {"asset_count": 1}}
        )

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "refresh-company-public-web-assets",
                    "--target-company",
                    "OpenAI",
                    "--source-family",
                    "company_research",
                    "--seed-url",
                    "https://openai.com/research",
                    "--max-assets",
                    "5",
                ],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        orchestrator.refresh_company_public_web_assets.assert_called_once()
        payload = orchestrator.refresh_company_public_web_assets.call_args.args[0]
        self.assertEqual(payload["target_company"], "OpenAI")
        self.assertEqual(payload["source_families"], ["company_research"])
        self.assertEqual(payload["seed_urls"], ["https://openai.com/research"])
        self.assertEqual(payload["options"]["max_assets"], 5)
        self.assertEqual(payload["options"]["collection_mode"], "seed_url_only")
        print_mock.assert_called_once()

    def test_refresh_company_public_web_assets_command_passes_provider_search_options(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.refresh_company_public_web_assets = mock.Mock(
            return_value={"status": "completed", "summary": {"asset_count": 2}}
        )

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "refresh-company-public-web-assets",
                    "--target-company",
                    "OpenAI",
                    "--collection-mode",
                    "provider_search",
                    "--max-queries",
                    "2",
                    "--max-results-per-query",
                    "3",
                ],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        orchestrator.refresh_company_public_web_assets.assert_called_once()
        payload = orchestrator.refresh_company_public_web_assets.call_args.args[0]
        self.assertEqual(payload["options"]["collection_mode"], "provider_search")
        self.assertEqual(payload["options"]["max_queries"], 2)
        self.assertEqual(payload["options"]["max_results_per_query"], 3)
        print_mock.assert_called_once()

    def test_refresh_company_public_web_assets_command_passes_collector_bundle_json(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.refresh_company_public_web_assets = mock.Mock(
            return_value={"status": "completed", "summary": {"asset_count": 1}}
        )
        with tempfile.TemporaryDirectory() as tempdir:
            collector_path = Path(tempdir) / "collector_inputs.json"
            collector_path.write_text(
                json.dumps(
                    {
                        "collector_inputs": {
                            "arxiv_publications": [
                                {"title": "Inference Systems", "url": "https://arxiv.org/abs/2601.00001"}
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )

            with (
                mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
                mock.patch.object(
                    cli.sys,
                    "argv",
                    [
                        "cli",
                        "refresh-company-public-web-assets",
                        "--target-company",
                        "OpenAI",
                        "--collection-mode",
                        "collector_bundle",
                        "--collector-input-json",
                        str(collector_path),
                    ],
                ),
                mock.patch("builtins.print") as print_mock,
            ):
                cli.main()

        orchestrator.refresh_company_public_web_assets.assert_called_once()
        payload = orchestrator.refresh_company_public_web_assets.call_args.args[0]
        self.assertEqual(payload["options"]["collection_mode"], "collector_bundle")
        self.assertEqual(payload["collector_inputs"]["arxiv_publications"][0]["title"], "Inference Systems")
        print_mock.assert_called_once()

    def test_refresh_company_public_web_assets_command_passes_live_collector_sources(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.refresh_company_public_web_assets = mock.Mock(
            return_value={"status": "completed", "summary": {"asset_count": 1}}
        )
        with tempfile.TemporaryDirectory() as tempdir:
            collector_source_path = Path(tempdir) / "collector_sources.json"
            collector_source_path.write_text(
                json.dumps(
                    {
                        "collector_sources": [
                            {"url": "https://openai.com/research/rss.xml", "collector_type": "rss_items"}
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with (
                mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
                mock.patch.object(
                    cli.sys,
                    "argv",
                    [
                        "cli",
                        "refresh-company-public-web-assets",
                        "--target-company",
                        "OpenAI",
                        "--collection-mode",
                        "collector_bundle",
                        "--collector-source-json",
                        str(collector_source_path),
                        "--collector-source-url",
                        "https://openai.com/engineering",
                        "--discover-collector-sources",
                        "--max-discovered-collector-sources",
                        "7",
                    ],
                ),
                mock.patch("builtins.print") as print_mock,
            ):
                cli.main()

        orchestrator.refresh_company_public_web_assets.assert_called_once()
        payload = orchestrator.refresh_company_public_web_assets.call_args.args[0]
        self.assertEqual(payload["options"]["collection_mode"], "collector_bundle")
        self.assertTrue(payload["options"]["discover_collector_sources"])
        self.assertEqual(payload["options"]["max_discovered_collector_sources"], 7)
        self.assertEqual(payload["collector_sources"][0]["collector_type"], "rss_items")
        self.assertEqual(payload["collector_sources"][1], "https://openai.com/engineering")
        print_mock.assert_called_once()

    def test_list_company_public_web_assets_command_does_not_trigger_refresh(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.list_company_public_web_assets = mock.Mock(return_value={"status": "ok", "assets": []})

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(
                cli.sys,
                "argv",
                ["cli", "list-company-public-web-assets", "--target-company", "OpenAI", "--limit", "25"],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        orchestrator.list_company_public_web_assets.assert_called_once()
        self.assertEqual(orchestrator.list_company_public_web_assets.call_args.args[0]["target_company"], "OpenAI")
        self.assertEqual(orchestrator.list_company_public_web_assets.call_args.args[0]["limit"], 25)
        self.assertFalse(orchestrator.refresh_company_public_web_assets.called)
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

    def test_backfill_job_result_lifecycle_cli_dry_run_delegates_with_progress_callback(self) -> None:
        orchestrator = mock.Mock()

        def _fake_backfill(*, orchestrator, dry_run, batch_size, progress_callback):
            self.assertIsNotNone(orchestrator)
            self.assertTrue(dry_run)
            self.assertEqual(batch_size, 2)

            class _Stats:
                def to_dict(self) -> dict[str, int]:
                    return {
                        "total_jobs": 1,
                        "jobs_with_lifecycle": 0,
                        "jobs_backfilled": 1,
                        "jobs_skipped": 0,
                        "errors": 0,
                    }

            progress_callback("job-123", _Stats())
            return {"total_jobs": 1, "jobs_backfilled": 1, "schema_preflight": {"status": "synced"}}

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator) as build_mock,
            mock.patch.object(cli, "backfill_job_result_lifecycle", side_effect=_fake_backfill) as backfill_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "backfill-job-result-lifecycle",
                    "--dry-run",
                    "--batch-size",
                    "2",
                    "--verbose",
                ],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        build_mock.assert_called_once()
        backfill_mock.assert_called_once()
        self.assertGreaterEqual(print_mock.call_count, 2)

    def test_backfill_snapshot_full_materialization_items_cli_defaults_to_dry_run(self) -> None:
        orchestrator = mock.Mock()

        def _fake_backfill(*, orchestrator, dry_run, limit, batch_size, progress_callback):
            self.assertIsNotNone(orchestrator)
            self.assertTrue(dry_run)
            self.assertEqual(limit, 3)
            self.assertEqual(batch_size, 2)

            class _Stats:
                def to_dict(self) -> dict[str, int]:
                    return {
                        "total_jobs": 1,
                        "eligible_jobs": 1,
                        "existing_items": 0,
                        "items_enqueued": 1,
                        "jobs_skipped": 0,
                        "errors": 0,
                    }

            progress_callback("job-123", _Stats())
            return {"dry_run": True, "eligible_jobs": 1, "items_enqueued": 1}

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator) as build_mock,
            mock.patch.object(
                cli,
                "backfill_snapshot_full_materialization_items",
                side_effect=_fake_backfill,
            ) as backfill_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "backfill-snapshot-full-materialization-items",
                    "--limit",
                    "3",
                    "--batch-size",
                    "2",
                    "--verbose",
                ],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        build_mock.assert_called_once()
        backfill_mock.assert_called_once()
        self.assertGreaterEqual(print_mock.call_count, 2)

    def test_backfill_snapshot_full_materialization_items_cli_apply_persists(self) -> None:
        orchestrator = mock.Mock()

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(
                cli,
                "backfill_snapshot_full_materialization_items",
                return_value={"dry_run": False, "items_enqueued": 1},
            ) as backfill_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "backfill-snapshot-full-materialization-items",
                    "--apply",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        self.assertFalse(backfill_mock.call_args.kwargs["dry_run"])

    def test_backfill_local_apply_closure_items_cli_defaults_to_dry_run(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.backfill_local_apply_closure_items.return_value = {
            "status": "dry_run",
            "dry_run": True,
            "candidate_worker_count": 1,
        }

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator) as build_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "backfill-local-apply-closure-items",
                    "--job-id",
                    "job-123",
                    "--limit",
                    "7",
                    "--job-scan-limit",
                    "11",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        build_mock.assert_called_once()
        orchestrator.backfill_local_apply_closure_items.assert_called_once_with(
            {
                "dry_run": True,
                "job_id": "job-123",
                "local_apply_backlog_worker_limit": 7,
                "local_apply_backlog_job_scan_limit": 11,
            }
        )

    def test_backfill_local_apply_closure_items_cli_apply_persists(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.backfill_local_apply_closure_items.return_value = {
            "status": "completed",
            "dry_run": False,
            "enqueued_count": 1,
        }

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "backfill-local-apply-closure-items",
                    "--apply",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        self.assertFalse(orchestrator.backfill_local_apply_closure_items.call_args.args[0]["dry_run"])

    def test_backfill_search_seed_discovery_items_cli_defaults_to_dry_run(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.backfill_search_seed_discovery_query_items.return_value = {
            "status": "dry_run",
            "dry_run": True,
            "candidate_worker_count": 1,
        }

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator) as build_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "backfill-search-seed-discovery-items",
                    "--job-id",
                    "job-123",
                    "--limit",
                    "7",
                    "--job-scan-limit",
                    "11",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        build_mock.assert_called_once()
        orchestrator.backfill_search_seed_discovery_query_items.assert_called_once_with(
            {
                "dry_run": True,
                "job_id": "job-123",
                "search_seed_discovery_worker_limit": 7,
                "search_seed_discovery_job_scan_limit": 11,
            }
        )

    def test_backfill_search_seed_discovery_items_cli_apply_persists(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.backfill_search_seed_discovery_query_items.return_value = {
            "status": "completed",
            "dry_run": False,
            "backfilled_count": 1,
        }

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "backfill-search-seed-discovery-items",
                    "--apply",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        self.assertFalse(orchestrator.backfill_search_seed_discovery_query_items.call_args.args[0]["dry_run"])

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
                "ControlPlaneStore",
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
                "ControlPlaneStore",
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
            mock.patch.object(cli, "ControlPlaneStore", return_value=store),
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

    def test_repair_excel_intake_artifacts_command_delegates_to_orchestrator(self) -> None:
        orchestrator = mock.Mock()
        orchestrator.repair_excel_intake_artifacts = mock.Mock(
            return_value={"status": "completed", "job_id": "excel-job-1"}
        )

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(
                cli.sys,
                "argv",
                ["cli", "repair-excel-intake-artifacts", "--job-id", "excel-job-1", "--apply", "--run-now"],
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            cli.main()

        orchestrator.repair_excel_intake_artifacts.assert_called_once_with(
            {
                "job_id": "excel-job-1",
                "dry_run": False,
                "run_now": True,
                "source": "repair_excel_intake_artifacts_cli",
            }
        )
        print_mock.assert_called_once()


class ServeTopologyCliTest(unittest.TestCase):
    def _fake_python_executable(self, tempdir: str) -> tuple[Path, Path]:
        fake_python = Path(tempdir) / "fake-python"
        argv_log = Path(tempdir) / "python-argv.log"
        fake_python.write_text(
            """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "${FAKE_PYTHON_ARGV_LOG:?}"
if [[ "$*" == *"run-worker-daemon-service"* ]]; then
  trap 'exit 0' TERM INT
  while true; do sleep 0.1; done
fi
exit 0
""",
            encoding="utf-8",
        )
        fake_python.chmod(0o755)
        return fake_python, argv_log

    def _wrapper_env(
        self,
        *,
        tempdir: str,
        fake_python: Path,
        argv_log: Path,
        hosted: bool,
    ) -> dict[str, str]:
        empty_pg_env = Path(tempdir) / "empty-postgres.env"
        empty_pg_env.write_text("", encoding="utf-8")
        env = {
            **os.environ,
            "FAKE_PYTHON_ARGV_LOG": str(argv_log),
            "SOURCING_LOCAL_POSTGRES_ENV_FILE": str(empty_pg_env),
            "SOURCING_RUNTIME_ENVIRONMENT": "test",
            "SOURCING_EXTERNAL_PROVIDER_MODE": "simulate",
            "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "test_schema",
        }
        for key in (
            "SOURCING_CONTROL_PLANE_POSTGRES_DSN",
            "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE",
            "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES",
        ):
            env.pop(key, None)
        if hosted:
            env.update(
                {
                    "HOSTED_PYTHON_BIN": str(fake_python),
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN": (
                        "postgresql://sourcing@127.0.0.1:55432/no_connection_is_attempted"
                    ),
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "postgres_only",
                }
            )
        else:
            env["DEV_PYTHON_BIN"] = str(fake_python)
        return env

    def test_serve_defaults_to_external_recovery_without_starting_in_process_threads(self) -> None:
        orchestrator = mock.Mock()
        server = mock.Mock()
        server.serve_forever.side_effect = KeyboardInterrupt()

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(cli, "start_shared_recovery_service") as start_shared_recovery_mock,
            mock.patch.object(cli, "start_server_runtime_watchdog") as start_watchdog_mock,
            mock.patch.object(
                cli,
                "assert_recovery_coverage_or_fail_closed",
                return_value={"coverage": "external_recovery_daemon"},
            ) as coverage_mock,
            mock.patch.object(cli, "create_server", return_value=server),
            mock.patch.object(
                cli.sys,
                "argv",
                ["cli", "serve", "--host", "127.0.0.1", "--port", "8765"],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        start_shared_recovery_mock.assert_not_called()
        start_watchdog_mock.assert_not_called()
        coverage_mock.assert_called_once_with(
            orchestrator,
            shared_recovery_thread=None,
            watchdog_disabled=True,
            allow_uncovered_recovery=False,
        )
        orchestrator.start_background_organization_asset_warmup.assert_called_once_with()
        server.server_close.assert_called_once_with()

    def test_serve_default_recovery_coverage_failure_precedes_server_creation(self) -> None:
        orchestrator = mock.Mock()

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(cli, "start_shared_recovery_service") as start_shared_recovery_mock,
            mock.patch.object(cli, "start_server_runtime_watchdog") as start_watchdog_mock,
            mock.patch.object(
                cli,
                "assert_recovery_coverage_or_fail_closed",
                side_effect=cli.RecoveryCoverageError("external recovery daemon is unavailable"),
            ) as coverage_mock,
            mock.patch.object(cli, "create_server") as create_server_mock,
            mock.patch.object(cli.sys, "argv", ["cli", "serve"]),
        ):
            with self.assertRaisesRegex(cli.RecoveryCoverageError, "external recovery daemon is unavailable"):
                cli.main()

        start_shared_recovery_mock.assert_not_called()
        start_watchdog_mock.assert_not_called()
        coverage_mock.assert_called_once_with(
            orchestrator,
            shared_recovery_thread=None,
            watchdog_disabled=True,
            allow_uncovered_recovery=False,
        )
        orchestrator.start_background_organization_asset_warmup.assert_not_called()
        create_server_mock.assert_not_called()

    def test_serve_explicit_dev_opt_in_starts_and_joins_in_process_recovery(self) -> None:
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
            mock.patch.object(cli, "assert_recovery_coverage_or_fail_closed") as coverage_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "serve",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "8765",
                    "--enable-runtime-watchdog",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        start_shared_recovery_mock.assert_called_once_with(orchestrator)
        start_watchdog_mock.assert_called_once()
        coverage_mock.assert_called_once_with(
            orchestrator,
            shared_recovery_thread=shared_recovery_thread,
            watchdog_disabled=False,
            allow_uncovered_recovery=False,
        )
        server.server_close.assert_called_once_with()
        watchdog_stop.set.assert_called_once_with()
        shared_recovery_stop.set.assert_called_once_with()
        watchdog_thread.join.assert_called_once_with(timeout=15.0)
        shared_recovery_thread.join.assert_called_once_with(timeout=5.0)

    def test_serve_explicit_dev_opt_in_joins_threads_when_coverage_check_fails(self) -> None:
        orchestrator = mock.Mock()
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
            ),
            mock.patch.object(
                cli,
                "start_server_runtime_watchdog",
                return_value=(watchdog_stop, watchdog_thread),
            ),
            mock.patch.object(
                cli,
                "assert_recovery_coverage_or_fail_closed",
                side_effect=cli.RecoveryCoverageError("recovery unavailable"),
            ),
            mock.patch.object(cli, "create_server") as create_server_mock,
            mock.patch.object(cli.sys, "argv", ["cli", "serve", "--enable-runtime-watchdog"]),
        ):
            with self.assertRaisesRegex(cli.RecoveryCoverageError, "recovery unavailable"):
                cli.main()

        create_server_mock.assert_not_called()
        watchdog_stop.set.assert_called_once_with()
        shared_recovery_stop.set.assert_called_once_with()
        watchdog_thread.join.assert_called_once_with(timeout=15.0)
        shared_recovery_thread.join.assert_called_once_with(timeout=5.0)

    def test_serve_allow_uncovered_does_not_silently_start_in_process_recovery(self) -> None:
        orchestrator = mock.Mock()
        server = mock.Mock()
        server.serve_forever.side_effect = KeyboardInterrupt()

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(cli, "start_shared_recovery_service") as start_shared_recovery_mock,
            mock.patch.object(cli, "start_server_runtime_watchdog") as start_watchdog_mock,
            mock.patch.object(
                cli,
                "assert_recovery_coverage_or_fail_closed",
                return_value={"coverage": "opt_out_allow_uncovered_recovery", "severity": "warning"},
            ) as coverage_mock,
            mock.patch.object(cli, "create_server", return_value=server),
            mock.patch.object(cli.sys, "argv", ["cli", "serve", "--allow-uncovered-recovery"]),
            mock.patch("builtins.print"),
        ):
            cli.main()

        start_shared_recovery_mock.assert_not_called()
        start_watchdog_mock.assert_not_called()
        coverage_mock.assert_called_once_with(
            orchestrator,
            shared_recovery_thread=None,
            watchdog_disabled=True,
            allow_uncovered_recovery=True,
        )

    def test_serve_deprecated_disable_flag_preserves_external_only_behavior(self) -> None:
        orchestrator = mock.Mock()
        server = mock.Mock()
        server.serve_forever.side_effect = KeyboardInterrupt()

        with (
            mock.patch.object(cli, "build_orchestrator", return_value=orchestrator),
            mock.patch.object(cli, "start_shared_recovery_service") as start_shared_recovery_mock,
            mock.patch.object(cli, "start_server_runtime_watchdog") as start_watchdog_mock,
            mock.patch.object(cli, "assert_recovery_coverage_or_fail_closed") as coverage_mock,
            mock.patch.object(cli, "create_server", return_value=server),
            mock.patch.object(cli.sys, "argv", ["cli", "serve", "--disable-runtime-watchdog"]),
            mock.patch("builtins.print"),
        ):
            cli.main()

        start_shared_recovery_mock.assert_not_called()
        start_watchdog_mock.assert_not_called()
        coverage_mock.assert_called_once_with(
            orchestrator,
            shared_recovery_thread=None,
            watchdog_disabled=True,
            allow_uncovered_recovery=False,
        )

    def test_serve_rejects_conflicting_runtime_watchdog_flags_before_building_runtime(self) -> None:
        with (
            mock.patch.object(cli, "build_orchestrator") as build_orchestrator_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                ["cli", "serve", "--enable-runtime-watchdog", "--disable-runtime-watchdog"],
            ),
            mock.patch.object(cli.sys, "stderr"),
        ):
            with self.assertRaises(SystemExit) as raised:
                cli.main()

        self.assertEqual(raised.exception.code, 2)
        build_orchestrator_mock.assert_not_called()

    def test_dev_backend_rejects_conflicting_runtime_watchdog_flags_in_either_order(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        script_path = repo_root / "scripts" / "dev_backend.sh"
        flag_orders = (
            ("--enable-runtime-watchdog", "--disable-runtime-watchdog"),
            ("--disable-runtime-watchdog", "--enable-runtime-watchdog"),
        )

        for flag_order in flag_orders:
            with self.subTest(flag_order=flag_order):
                completed = subprocess.run(
                    ["bash", str(script_path), *flag_order, "--print-config"],
                    cwd=repo_root,
                    check=False,
                    capture_output=True,
                    text=True,
                )

                self.assertEqual(completed.returncode, 2)
                self.assertIn("Conflicting runtime watchdog flags", completed.stderr)

    def test_hosted_backend_real_argv_starts_daemon_before_external_only_serve(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        script_path = repo_root / "scripts" / "run_hosted_trial_backend.sh"
        with tempfile.TemporaryDirectory() as tempdir:
            fake_python, argv_log = self._fake_python_executable(tempdir)
            env = self._wrapper_env(
                tempdir=tempdir,
                fake_python=fake_python,
                argv_log=argv_log,
                hosted=True,
            )
            completed = subprocess.run(
                [
                    "bash",
                    str(script_path),
                    "--runtime-dir",
                    str(Path(tempdir) / "hosted-runtime"),
                ],
                cwd=repo_root,
                env=env,
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            argv = argv_log.read_text(encoding="utf-8").splitlines()
            daemon_index = next(index for index, item in enumerate(argv) if "run-worker-daemon-service" in item)
            serve_index = next(index for index, item in enumerate(argv) if "sourcing_agent.cli serve" in item)
            self.assertLess(daemon_index, serve_index)
            self.assertNotIn("--enable-runtime-watchdog", argv[serve_index])
            self.assertNotIn("--allow-uncovered-recovery", argv[serve_index])

    def test_hosted_backend_real_argv_forwards_only_explicit_uncovered_opt_out(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        script_path = repo_root / "scripts" / "run_hosted_trial_backend.sh"
        with tempfile.TemporaryDirectory() as tempdir:
            fake_python, argv_log = self._fake_python_executable(tempdir)
            env = self._wrapper_env(
                tempdir=tempdir,
                fake_python=fake_python,
                argv_log=argv_log,
                hosted=True,
            )
            completed = subprocess.run(
                [
                    "bash",
                    str(script_path),
                    "--runtime-dir",
                    str(Path(tempdir) / "hosted-runtime"),
                    "--no-daemon",
                    "--allow-uncovered-recovery",
                ],
                cwd=repo_root,
                env=env,
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            argv = argv_log.read_text(encoding="utf-8").splitlines()
            self.assertFalse(any("run-worker-daemon-service" in item for item in argv))
            serve_argv = next(item for item in argv if "sourcing_agent.cli serve" in item)
            self.assertIn("--allow-uncovered-recovery", serve_argv)
            self.assertNotIn("--enable-runtime-watchdog", serve_argv)

    def test_dev_backend_real_argv_forwards_explicit_dev_watchdog_without_daemon(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        script_path = repo_root / "scripts" / "dev_backend.sh"
        with tempfile.TemporaryDirectory() as tempdir:
            fake_python, argv_log = self._fake_python_executable(tempdir)
            env = self._wrapper_env(
                tempdir=tempdir,
                fake_python=fake_python,
                argv_log=argv_log,
                hosted=False,
            )
            completed = subprocess.run(
                [
                    "bash",
                    str(script_path),
                    "--runtime-dir",
                    str(Path(tempdir) / "dev-runtime"),
                    "--no-daemon",
                    "--enable-runtime-watchdog",
                ],
                cwd=repo_root,
                env=env,
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            argv = argv_log.read_text(encoding="utf-8").splitlines()
            self.assertFalse(any("run-worker-daemon-service" in item for item in argv))
            serve_argv = next(item for item in argv if "sourcing_agent.cli serve" in item)
            self.assertIn("--enable-runtime-watchdog", serve_argv)
            self.assertNotIn("--allow-uncovered-recovery", serve_argv)

    def test_dev_backend_real_argv_starts_daemon_before_external_only_serve(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        script_path = repo_root / "scripts" / "dev_backend.sh"
        with tempfile.TemporaryDirectory() as tempdir:
            fake_python, argv_log = self._fake_python_executable(tempdir)
            env = self._wrapper_env(
                tempdir=tempdir,
                fake_python=fake_python,
                argv_log=argv_log,
                hosted=False,
            )
            completed = subprocess.run(
                [
                    "bash",
                    str(script_path),
                    "--runtime-dir",
                    str(Path(tempdir) / "dev-runtime"),
                ],
                cwd=repo_root,
                env=env,
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            argv = argv_log.read_text(encoding="utf-8").splitlines()
            daemon_index = next(index for index, item in enumerate(argv) if "run-worker-daemon-service" in item)
            serve_index = next(index for index, item in enumerate(argv) if "sourcing_agent.cli serve" in item)
            self.assertLess(daemon_index, serve_index)
            self.assertNotIn("--enable-runtime-watchdog", argv[serve_index])
            self.assertNotIn("--allow-uncovered-recovery", argv[serve_index])

    def test_local_proxy_raw_serve_example_uses_explicit_dev_only_recovery_opt_in(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        script_path = repo_root / "scripts" / "local_dev_proxy_guard.sh"
        completed = subprocess.run(
            [
                "bash",
                str(script_path),
                "/bin/echo",
                "sourcing_agent.cli",
                "serve",
                "--host",
                "0.0.0.0",
                "--port",
                "8765",
                "--enable-runtime-watchdog",
            ],
            cwd=repo_root,
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(
            completed.stdout.strip(),
            "sourcing_agent.cli serve --host 0.0.0.0 --port 8765 --enable-runtime-watchdog",
        )


class CliWorkflowRunnerContinuationTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
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
                "ControlPlaneStore",
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
            sqlite_path=None,
            tables=["jobs", "job_result_views"],
            include_all_sqlite_tables=False,
            source_backend="postgres",
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
                ["cli", "export-control-plane-snapshot", "--source-backend", "sqlite", "--all-sqlite-tables"],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        export_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            output_path=settings.runtime_dir / "object_sync" / "control_plane" / "control_plane_snapshot.json",
            sqlite_path=None,
            tables=[],
            include_all_sqlite_tables=True,
            source_backend="sqlite",
        )

    def test_audit_company_serving_view_reports_projection_and_job_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            artifact_dir = runtime_dir / "company_assets" / "acme" / "snap-new" / "normalized_artifacts"
            pages_dir = artifact_dir / "pages"
            pages_dir.mkdir(parents=True, exist_ok=True)
            (artifact_dir / "artifact_summary.json").write_text(
                json.dumps(
                    {
                        "target_company": "Acme",
                        "company_key": "acme",
                        "snapshot_id": "snap-new",
                        "asset_view": "canonical_merged",
                        "candidate_count": 1,
                        "candidate_shard_count": 1,
                        "build_profile": "foreground_fast",
                        "projection_version": "candidate_artifact_projection_v20260427_source_matches",
                        "materialization_generation_key": "gen-new",
                        "source_snapshot_selection": {
                            "mode": "current_snapshot_only_large_org",
                            "selected_snapshot_ids": ["snap-new"],
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (artifact_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "target_company": "Acme",
                        "company_key": "acme",
                        "snapshot_id": "snap-new",
                        "asset_view": "canonical_merged",
                        "candidate_count": 1,
                        "build_profile": "foreground_fast",
                        "projection_version": "candidate_artifact_projection_v20260427_source_matches",
                        "pagination": {"page_count": 1, "page_size": 50},
                        "candidate_shards": [{"candidate_id": "c1", "path": "candidates/c1.json"}],
                        "pages": [{"page": 1, "path": "pages/page-0001.json", "candidate_count": 1}],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (pages_dir / "page-0001.json").write_text(
                json.dumps(
                    {
                        "candidates": [
                            {
                                "candidate_id": "c1",
                                "display_name": "Ada Example",
                                "matched_keywords": ["Gemini"],
                                "source_matches": [
                                    {
                                        "source_type": "harvest_profile_search",
                                        "source_query": "Gemini",
                                        "matched_keywords": ["Gemini"],
                                    }
                                ],
                            }
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            store = self.make_pg_store(runtime_dir / "sourcing_agent.db")
            store.upsert_organization_asset_registry(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": "snap-new",
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "evidence_count": 0,
                    "source_path": str(artifact_dir / "artifact_summary.json"),
                    "materialization_generation_key": "gen-new",
                    "summary": {"candidate_count": 1},
                },
                authoritative=True,
            )
            store.upsert_job_result_view(
                job_id="job-1",
                target_company="Acme",
                source_kind="company_snapshot",
                view_kind="asset_population",
                snapshot_id="snap-old",
                asset_view="canonical_merged",
            )

            result = cli.audit_company_serving_view(
                runtime_dir=runtime_dir,
                store=store,
                company="Acme",
                job_id="job-1",
                sample_pages=0,
            )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["artifact"]["build_profile"], "foreground_fast")
        self.assertEqual(
            result["artifact"]["projection_version"],
            "candidate_artifact_projection_v20260427_source_matches",
        )
        self.assertEqual(result["artifact"]["projection_version_source"], "artifact_summary")
        self.assertEqual(result["source_provenance"]["source_matches_records"], 1)
        self.assertEqual(result["source_provenance"]["matched_keywords_records"], 1)
        self.assertTrue(result["drift"]["policy_required_for_repoint"])

    def test_audit_job_result_view_consistency_only_auto_repairs_full_reuse_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            artifact_dir = runtime_dir / "company_assets" / "acme" / "snap-new" / "normalized_artifacts"
            artifact_dir.mkdir(parents=True, exist_ok=True)
            (artifact_dir / "artifact_summary.json").write_text(
                json.dumps(
                    {
                        "target_company": "Acme",
                        "company_key": "acme",
                        "snapshot_id": "snap-new",
                        "asset_view": "canonical_merged",
                        "candidate_count": 42,
                        "projection_version": "candidate_artifact_projection_v20260427_source_matches",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (artifact_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "target_company": "Acme",
                        "company_key": "acme",
                        "snapshot_id": "snap-new",
                        "asset_view": "canonical_merged",
                        "candidate_count": 42,
                        "pagination": {"page_count": 1},
                        "pages": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            store = self.make_pg_store(runtime_dir / "sourcing_agent.db")
            store.upsert_organization_asset_registry(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": "snap-new",
                    "asset_view": "canonical_merged",
                    "candidate_count": 42,
                    "source_path": str(artifact_dir / "artifact_summary.json"),
                    "summary": {"candidate_count": 42},
                },
                authoritative=True,
            )
            store.save_job(
                job_id="job-full-reuse",
                job_type="workflow",
                status="completed",
                stage="completed",
                request_payload={
                    "raw_user_request": "给我 Acme 的全部成员",
                    "target_company": "Acme",
                    "target_scope": "full_company_asset",
                },
                plan_payload={"asset_reuse_plan": {"planner_mode": "reuse_snapshot_only"}},
                summary_payload={
                    "candidate_source": {
                        "source_kind": "company_snapshot",
                        "snapshot_id": "snap-old",
                        "asset_view": "canonical_merged",
                        "candidate_count": 2,
                    }
                },
            )
            store.upsert_job_result_view(
                job_id="job-full-reuse",
                target_company="Acme",
                source_kind="company_snapshot",
                view_kind="asset_population",
                snapshot_id="snap-old",
                asset_view="canonical_merged",
                summary={"candidate_count": 2},
            )
            store.save_job(
                job_id="job-scoped-overlay",
                job_type="workflow",
                status="completed",
                stage="completed",
                request_payload={
                    "raw_user_request": "给我 Acme 做 Agent 的人",
                    "target_company": "Acme",
                    "target_scope": "full_company_asset",
                    "keywords": ["Agent"],
                },
                plan_payload={"asset_reuse_plan": {"planner_mode": "delta_from_snapshot"}},
                summary_payload={
                    "candidate_source": {
                        "source_kind": "company_snapshot",
                        "snapshot_id": "snap-old",
                        "asset_view": "canonical_merged",
                        "candidate_count": 2,
                        "asset_population_overlay_path": str(runtime_dir / "overlay.json"),
                    }
                },
            )
            store.upsert_job_result_view(
                job_id="job-scoped-overlay",
                target_company="Acme",
                source_kind="company_snapshot",
                view_kind="asset_population",
                snapshot_id="snap-old",
                asset_view="canonical_merged",
                summary={"candidate_count": 2},
                metadata={"asset_population_overlay_path": str(runtime_dir / "overlay.json")},
            )

            dry_run = cli.audit_job_result_view_consistency(
                runtime_dir=runtime_dir,
                store=store,
                company="Acme",
                limit=10,
                apply=False,
            )
            applied = cli.audit_job_result_view_consistency(
                runtime_dir=runtime_dir,
                store=store,
                company="Acme",
                limit=10,
                apply=True,
            )

        dry_run_by_job = {record["job_id"]: record for record in dry_run["jobs"]}
        self.assertEqual(dry_run["summary"]["auto_repoint_candidate_count"], 1)
        self.assertEqual(dry_run["summary"]["manual_review_count"], 1)
        self.assertEqual(
            dry_run_by_job["job-full-reuse"]["recommended_action"],
            "dry_run_repoint_to_authoritative",
        )
        self.assertEqual(
            dry_run_by_job["job-scoped-overlay"]["recommended_action"],
            "manual_review_required",
        )
        self.assertTrue(dry_run_by_job["job-full-reuse"]["eligibility"]["eligible"])
        self.assertFalse(dry_run_by_job["job-scoped-overlay"]["eligibility"]["eligible"])
        self.assertEqual(applied["summary"]["applied_repoint_count"], 1)
        self.assertEqual(store.get_job_result_view(job_id="job-full-reuse")["snapshot_id"], "snap-new")
        self.assertEqual(store.get_job_result_view(job_id="job-scoped-overlay")["snapshot_id"], "snap-old")

    def test_audit_hot_cache_serving_artifacts_command_is_read_only_report(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir)
            output_path = project_root / "reports" / "hot-cache-audit.json"
            catalog = SimpleNamespace(project_root=project_root)
            settings = SimpleNamespace(
                runtime_dir=project_root / "runtime",
                db_path=project_root / "runtime" / "sourcing_agent.db",
            )
            with (
                mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
                mock.patch.object(cli, "load_settings", return_value=settings),
                mock.patch.object(
                    cli,
                    "audit_candidate_artifact_hot_cache",
                    return_value={"status": "needs_rehydrate", "summary": {"rehydrate_candidate_count": 1}},
                ) as audit_mock,
                mock.patch.object(
                    cli.sys,
                    "argv",
                    [
                        "cli",
                        "audit-hot-cache-serving-artifacts",
                        "--company",
                        "Acme",
                        "--snapshot-id",
                        "snap-1",
                        "--limit",
                        "5",
                        "--output",
                        "reports/hot-cache-audit.json",
                    ],
                ),
                mock.patch("builtins.print"),
            ):
                cli.main()

            audit_mock.assert_called_once_with(
                runtime_dir=settings.runtime_dir,
                companies=["Acme"],
                snapshot_id="snap-1",
                asset_view="canonical_merged",
                limit=5,
            )
            self.assertTrue(output_path.exists())
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "needs_rehydrate")

    def test_cleanup_hot_cache_serving_artifacts_command_defaults_to_dry_run(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"),
            db_path=Path("/tmp/project/runtime/sourcing_agent.db"),
        )
        store = object()
        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(cli, "load_settings", return_value=settings),
            mock.patch.object(cli, "build_runtime_store", return_value=store),
            mock.patch.object(
                cli,
                "cleanup_candidate_artifact_hot_cache",
                return_value={"status": "completed", "dry_run": True},
            ) as cleanup_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "cleanup-hot-cache-serving-artifacts",
                    "--company",
                    "Acme",
                    "--snapshot-id",
                    "snap-1",
                    "--ttl-seconds",
                    "3600",
                    "--keep-compatibility-exports",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        cleanup_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            store=store,
            companies=["Acme"],
            snapshot_id="snap-1",
            dry_run=True,
            drop_compatibility_exports=False,
            ttl_seconds=3600,
            size_budget_bytes=0,
            max_bytes_per_company=0,
            keep_latest_snapshots_per_company=1,
            max_generations_per_scope=0,
        )

    def test_publish_candidate_generation_runs_post_publish_hot_cache_governance(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"),
            db_path=Path("/tmp/project/runtime/sourcing_agent.db"),
        )
        store = object()
        storage_client = object()
        bundle_manager = mock.Mock()
        bundle_manager.publish_candidate_generation.return_value = {
            "status": "uploaded",
            "generation_key": "gen-1",
        }
        with (
            mock.patch.object(cli, "build_asset_bundle_manager", return_value=bundle_manager),
            mock.patch.object(cli, "build_object_storage", return_value=storage_client),
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(cli, "load_settings", return_value=settings),
            mock.patch.object(cli, "build_runtime_store", return_value=store),
            mock.patch.object(
                cli,
                "run_hot_cache_governance_cycle",
                return_value={"status": "completed"},
            ) as governance_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "publish-candidate-generation",
                    "--company",
                    "Acme",
                    "--snapshot-id",
                    "snap-1",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        bundle_manager.publish_candidate_generation.assert_called_once_with(
            target_company="Acme",
            snapshot_id="snap-1",
            asset_view="canonical_merged",
            client=storage_client,
            max_workers=None,
            resume=True,
            include_compatibility_exports=False,
        )
        governance_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            store=store,
            min_interval_seconds=0.0,
            force=True,
        )

    def test_audit_authoritative_reuse_planning_command_is_read_only_report(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"),
            db_path=Path("/tmp/project/runtime/sourcing_agent.db"),
        )
        store = object()
        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(cli, "load_settings", return_value=settings),
            mock.patch.object(cli, "build_runtime_store", return_value=store),
            mock.patch.object(
                cli,
                "audit_authoritative_reuse_planning_many",
                return_value={"status": "ok", "read_only": True, "audits": []},
            ) as audit_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "audit-authoritative-reuse-planning",
                    "--company",
                    "OpenAI",
                    "--query",
                    "帮我找OpenAI做Agent方向的人",
                    "--query",
                    "我想要OpenAI在health组的人",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        audit_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            store=store,
            company="OpenAI",
            queries=["帮我找OpenAI做Agent方向的人", "我想要OpenAI在health组的人"],
            asset_view="canonical_merged",
        )

    def test_repair_authoritative_serving_generation_command_defaults_to_dry_run(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"),
            db_path=Path("/tmp/project/runtime/sourcing_agent.db"),
        )
        store = object()
        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(cli, "load_settings", return_value=settings),
            mock.patch.object(cli, "build_runtime_store", return_value=store),
            mock.patch.object(
                cli,
                "repair_authoritative_serving_generation",
                return_value={"status": "dry_run", "applied": False},
            ) as repair_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "repair-authoritative-serving-generation",
                    "--company",
                    "OpenAI",
                    "--query",
                    "我想要OpenAI在health组的人",
                    "--repair-snapshot-id",
                    "repair-health",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        repair_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            store=store,
            company="OpenAI",
            queries=["我想要OpenAI在health组的人"],
            asset_view="canonical_merged",
            snapshot_id="",
            repair_snapshot_id="repair-health",
            build_profile="foreground_fast",
            output_dir=None,
            apply=False,
        )

    def test_normalize_authoritative_source_provenance_command_defaults_to_dry_run(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"),
            db_path=Path("/tmp/project/runtime/sourcing_agent.db"),
        )
        store = object()
        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(cli, "load_settings", return_value=settings),
            mock.patch.object(cli, "build_runtime_store", return_value=store),
            mock.patch.object(
                cli,
                "normalize_authoritative_source_provenance",
                return_value={"status": "dry_run", "applied": False},
            ) as normalize_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "normalize-authoritative-source-provenance",
                    "--company",
                    "OpenAI",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        normalize_mock.assert_called_once_with(
            store=store,
            company="OpenAI",
            asset_view="canonical_merged",
            apply=False,
        )

    def test_audit_authoritative_reuse_planning_matrix_command_loads_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            matrix_path = Path(tempdir) / "matrix.json"
            matrix_payload = {"matrix_version": 1, "cases": []}
            matrix_path.write_text(json.dumps(matrix_payload), encoding="utf-8")
            catalog = SimpleNamespace(project_root=Path(tempdir))
            settings = SimpleNamespace(
                runtime_dir=Path(tempdir) / "runtime",
                db_path=Path(tempdir) / "runtime" / "sourcing_agent.db",
            )
            store = object()
            with (
                mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
                mock.patch.object(cli, "load_settings", return_value=settings),
                mock.patch.object(cli, "build_runtime_store", return_value=store),
                mock.patch.object(
                    cli,
                    "audit_authoritative_reuse_planning_matrix",
                    return_value={"status": "ok", "read_only": True, "cases": []},
                ) as matrix_mock,
                mock.patch.object(
                    cli.sys,
                    "argv",
                    [
                        "cli",
                        "audit-authoritative-reuse-planning-matrix",
                        "--matrix",
                        str(matrix_path),
                        "--summary-only",
                    ],
                ),
                mock.patch("builtins.print"),
            ):
                cli.main()

        matrix_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            store=store,
            matrix=matrix_payload,
            default_asset_view="canonical_merged",
            include_full_audit=False,
        )

    def test_compare_authoritative_reuse_planning_matrix_command_reports_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            left_path = Path(tempdir) / "left.json"
            right_path = Path(tempdir) / "right.json"
            left_payload = {"cases": []}
            right_payload = {"cases": []}
            left_path.write_text(json.dumps(left_payload), encoding="utf-8")
            right_path.write_text(json.dumps(right_payload), encoding="utf-8")
            with (
                mock.patch.object(
                    cli,
                    "compare_authoritative_reuse_planning_matrix_reports",
                    return_value={"status": "match", "cases": []},
                ) as compare_mock,
                mock.patch.object(
                    cli.sys,
                    "argv",
                    [
                        "cli",
                        "compare-authoritative-reuse-planning-matrix",
                        "--left",
                        str(left_path),
                        "--right",
                        str(right_path),
                    ],
                ),
                mock.patch("builtins.print"),
            ):
                cli.main()

        compare_mock.assert_called_once_with(
            left=left_payload,
            right=right_payload,
            compare_fields=None,
        )

    def test_backfill_authoritative_population_coverage_command_defaults_to_dry_run(self) -> None:
        catalog = SimpleNamespace(project_root=Path("/tmp/project"))
        settings = SimpleNamespace(
            runtime_dir=Path("/tmp/project/runtime"),
            db_path=Path("/tmp/project/runtime/sourcing_agent.db"),
        )
        store = object()
        with (
            mock.patch.object(cli.AssetCatalog, "discover", return_value=catalog),
            mock.patch.object(cli, "load_settings", return_value=settings),
            mock.patch.object(cli, "build_runtime_store", return_value=store),
            mock.patch.object(
                cli,
                "backfill_authoritative_population_coverage",
                return_value={"status": "dry_run", "changed_count": 0},
            ) as backfill_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "backfill-authoritative-population-coverage",
                    "--company",
                    "OpenAI",
                    "--company",
                    "Meta",
                    "--limit",
                    "25",
                ],
            ),
            mock.patch("builtins.print"),
        ):
            cli.main()

        backfill_mock.assert_called_once_with(
            runtime_dir=settings.runtime_dir,
            store=store,
            companies=["OpenAI", "Meta"],
            asset_view="canonical_merged",
            include_non_authoritative=False,
            dry_run=True,
            force=False,
            limit=25,
        )

    def test_repoint_job_result_view_requires_explicit_apply(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            artifact_dir = runtime_dir / "company_assets" / "acme" / "snap-new" / "normalized_artifacts"
            artifact_dir.mkdir(parents=True, exist_ok=True)
            (artifact_dir / "artifact_summary.json").write_text(
                json.dumps(
                    {
                        "target_company": "Acme",
                        "company_key": "acme",
                        "snapshot_id": "snap-new",
                        "asset_view": "canonical_merged",
                        "candidate_count": 2,
                        "build_profile": "foreground_fast",
                        "projection_version": "candidate_artifact_projection_v20260427_source_matches",
                        "materialization_generation_key": "gen-new",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (artifact_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "target_company": "Acme",
                        "company_key": "acme",
                        "snapshot_id": "snap-new",
                        "asset_view": "canonical_merged",
                        "candidate_count": 2,
                        "pagination": {"page_count": 0},
                        "pages": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            store = self.make_pg_store(runtime_dir / "sourcing_agent.db")
            store.upsert_organization_asset_registry(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": "snap-new",
                    "asset_view": "canonical_merged",
                    "candidate_count": 2,
                    "evidence_count": 0,
                    "source_path": str(artifact_dir / "artifact_summary.json"),
                    "materialization_generation_key": "gen-new",
                    "summary": {"candidate_count": 2},
                },
                authoritative=True,
            )
            store.upsert_job_result_view(
                job_id="job-2",
                target_company="Acme",
                source_kind="company_snapshot",
                view_kind="asset_population",
                snapshot_id="snap-old",
                asset_view="canonical_merged",
            )

            dry_run = cli.repoint_job_result_view(
                runtime_dir=runtime_dir,
                store=store,
                job_id="job-2",
                policy="serve_latest_company_asset",
            )
            self.assertEqual(store.get_job_result_view(job_id="job-2")["snapshot_id"], "snap-old")

            applied = cli.repoint_job_result_view(
                runtime_dir=runtime_dir,
                store=store,
                job_id="job-2",
                policy="serve_latest_company_asset",
                apply=True,
                reason="manual scoped serving repair",
            )

        self.assertEqual(dry_run["status"], "dry_run")
        self.assertEqual(applied["status"], "updated")
        self.assertEqual(applied["updated_view"]["snapshot_id"], "snap-new")
        self.assertEqual(applied["updated_view"]["metadata"]["repoint_policy"], "serve_latest_company_asset")

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

    def test_sync_control_plane_postgres_command_surfaces_durable_table_rejection(self) -> None:
        rejection = (
            "Generic control-plane snapshot/SQLite import cannot restore PG-only durable runtime tables: "
            "workflow_commands."
        )
        with (
            mock.patch.object(
                cli,
                "sync_control_plane_snapshot_to_postgres",
                side_effect=ValueError(rejection),
            ) as sync_mock,
            mock.patch.object(
                cli.sys,
                "argv",
                [
                    "cli",
                    "sync-control-plane-postgres",
                    "--snapshot",
                    "/tmp/control-plane.json",
                    "--dsn",
                    "postgresql://user:pass@localhost:5432/sourcing",
                    "--table",
                    "workflow_commands",
                    "--truncate-first",
                ],
            ),
            self.assertRaises(SystemExit) as raised,
        ):
            cli.main()

        self.assertEqual(str(raised.exception), rejection)
        sync_mock.assert_called_once_with(
            snapshot_path="/tmp/control-plane.json",
            dsn="postgresql://user:pass@localhost:5432/sourcing",
            tables=["workflow_commands"],
            truncate_first=True,
            validate_postgres=False,
        )

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
