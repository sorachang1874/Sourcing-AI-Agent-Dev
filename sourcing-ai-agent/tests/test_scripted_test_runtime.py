import tempfile
import unittest
from pathlib import Path

from sourcing_agent.local_postgres import resolve_control_plane_postgres_dsn
from sourcing_agent.scripted_test_runtime import (
    FAST_HOSTED_TEST_ENV,
    build_isolated_runtime_env,
    isolated_runtime_state_paths,
    isolated_hosted_test_runtime,
    patched_environment,
)
from sourcing_agent.workflow_smoke import HostedWorkflowSmokeClient


class ScriptedTestRuntimeTest(unittest.TestCase):
    def test_build_isolated_runtime_env_blocks_parent_repo_postgres_env(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            project_root = Path(tempdir)
            runtime_dir = project_root / "runtime" / "test_env" / "explain_matrix"
            (project_root / ".local-postgres.env").write_text(
                "SOURCING_CONTROL_PLANE_POSTGRES_DSN=postgresql://ambient@127.0.0.1:5432/ambient_db\n",
                encoding="utf-8",
            )
            env_payload, env_file = build_isolated_runtime_env(runtime_dir=runtime_dir)

            self.assertTrue(env_file.exists())
            with patched_environment(env_payload):
                self.assertEqual(resolve_control_plane_postgres_dsn(runtime_dir), "")

    def test_build_isolated_runtime_env_pins_stateful_paths_into_runtime_root(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "smoke"
            state_paths = isolated_runtime_state_paths(runtime_dir)
            env_payload, _env_file = build_isolated_runtime_env(runtime_dir=runtime_dir)

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
            env_file.write_text(
                (
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN="
                    "postgresql://isolated@127.0.0.1:55432/isolated_runtime\n"
                ),
                encoding="utf-8",
            )
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

    def test_isolated_hosted_test_runtime_serves_seeded_reference_explain(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "scripted_matrix"
            with isolated_hosted_test_runtime(
                runtime_dir=runtime_dir,
                seed_reference_runtime=True,
                provider_mode="simulate",
                extra_env=FAST_HOSTED_TEST_ENV,
            ) as runtime:
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


if __name__ == "__main__":
    unittest.main()
