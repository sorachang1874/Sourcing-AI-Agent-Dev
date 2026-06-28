import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sourcing_agent.runtime_environment import (
    ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS_ENV,
    LIVE_PROVIDER_ACCESS_DISABLED_ENV,
    LIVE_PROVIDER_CONFIRM_ENV,
    NON_LIVE_PROVIDER_MODES,
    SAFE_DEFAULT_PROVIDER_MODE,
    LiveProviderAccessError,
    assert_live_provider_access_allowed,
    current_runtime_environment,
    iter_runtime_namespace_path_values,
    normalize_provider_mode,
    normalize_runtime_environment,
    provider_isolation_contract,
    provider_isolation_env_overrides,
    runtime_namespace_ownership_for_path,
    runtime_requires_isolated_state,
    shared_provider_cache_dir,
    validate_runtime_environment,
)


class RuntimeEnvironmentTest(unittest.TestCase):
    def test_provider_mode_normalization_fails_closed_to_non_live_by_default(self) -> None:
        # Fail-closed: an unset/blank/unknown provider mode must NOT default to live,
        # so a bare process cannot make billed external calls without explicit opt-in.
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(normalize_provider_mode(), SAFE_DEFAULT_PROVIDER_MODE)
            self.assertIn(normalize_provider_mode(), NON_LIVE_PROVIDER_MODES)
        self.assertEqual(normalize_provider_mode(""), SAFE_DEFAULT_PROVIDER_MODE)
        self.assertEqual(normalize_provider_mode("totally-unknown-mode"), SAFE_DEFAULT_PROVIDER_MODE)
        # Live is still reachable, but only by explicit opt-in.
        self.assertEqual(normalize_provider_mode("live"), "live")
        self.assertEqual(normalize_provider_mode("production"), "live")
        self.assertEqual(normalize_provider_mode("fixture"), "scripted")
        self.assertEqual(normalize_provider_mode("offline"), "replay")

    def test_runtime_environment_infers_non_live_provider_namespaces(self) -> None:
        self.assertEqual(normalize_runtime_environment(provider_mode="scripted"), "scripted")
        self.assertEqual(normalize_runtime_environment(provider_mode="simulate"), "simulate")
        self.assertEqual(normalize_runtime_environment(provider_mode="replay"), "replay")

    def test_runtime_environment_infers_test_runtime_from_path(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "matrix"
            env = current_runtime_environment(runtime_dir=runtime_dir, provider_mode="live")
            self.assertEqual(env.name, "test")
            self.assertTrue(env.requires_isolated_state)

    def test_runtime_environment_prefers_runtime_scoped_provider_mode_over_ambient_env(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "openai_agent_scripted_case"
            runtime_dir.mkdir(parents=True)
            (runtime_dir / ".scripted-local-postgres.env").write_text(
                "\n".join(
                    [
                        "SOURCING_RUNTIME_ENVIRONMENT=scripted",
                        "SOURCING_EXTERNAL_PROVIDER_MODE=scripted",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "SOURCING_RUNTIME_ENVIRONMENT": "local_dev",
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                },
                clear=True,
            ):
                env = current_runtime_environment(runtime_dir=runtime_dir)

            self.assertEqual(env.provider_mode, "scripted")
            self.assertEqual(env.name, "scripted")
            self.assertIsNone(shared_provider_cache_dir(runtime_dir, "harvest_profile_scraper_batch"))

    def test_runtime_namespace_ownership_detects_nested_test_runtime_under_root_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            owner_runtime = Path(tempdir) / "runtime"
            owner_path = owner_runtime / "company_assets" / "openai" / "snapshot-a" / "candidate_documents.json"
            nested_path = (
                owner_runtime
                / "test_env"
                / "scripted_case"
                / "company_assets"
                / "openai"
                / "snapshot-b"
                / "candidate_documents.json"
            )

            owner = runtime_namespace_ownership_for_path(owner_path, configured_runtime_dir=owner_runtime)
            nested = runtime_namespace_ownership_for_path(nested_path, configured_runtime_dir=owner_runtime)

            self.assertTrue(owner.matches)
            self.assertFalse(nested.matches)
            self.assertEqual(nested.reason, "runtime_namespace_mismatch")
            self.assertEqual(nested.inferred_runtime_dir, str((owner_runtime / "test_env" / "scripted_case").resolve()))

    def test_runtime_namespace_ownership_prefers_configured_nested_group_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            group_runtime = Path(tempdir) / "runtime" / "test_env" / "matrix_run" / "01_openai_agent"
            snapshot_path = (
                group_runtime
                / "company_assets"
                / "openai"
                / "snapshot-a"
                / "candidate_documents.json"
            )

            ownership = runtime_namespace_ownership_for_path(snapshot_path, configured_runtime_dir=group_runtime)

            self.assertTrue(ownership.matches)
            self.assertEqual(ownership.reason, "runtime_namespace_match")
            self.assertEqual(ownership.inferred_runtime_dir, str(group_runtime.resolve()))

    def test_runtime_namespace_path_iterator_extracts_artifact_paths_without_linkedin_urls(self) -> None:
        payload = {
            "metadata": {
                "snapshot_dir": "/tmp/runtime/test_env/case/company_assets/openai/snapshot-a",
                "profile_url": "https://www.linkedin.com/in/openai-agent-current-0189/",
                "artifact_paths": {
                    "run_get": "/tmp/runtime/test_env/case/company_assets/openai/snapshot-a/run_get.json",
                },
            }
        }

        paths = dict(iter_runtime_namespace_path_values(payload))

        self.assertEqual(
            paths["snapshot_dir"],
            "/tmp/runtime/test_env/case/company_assets/openai/snapshot-a",
        )
        self.assertEqual(
            paths["run_get"],
            "/tmp/runtime/test_env/case/company_assets/openai/snapshot-a/run_get.json",
        )
        self.assertNotIn("profile_url", paths)

    def test_shared_provider_cache_is_live_only_and_namespaced(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            self.assertIsNone(
                shared_provider_cache_dir(runtime_dir, "harvest_company_employees", provider_mode="replay")
            )
            cache_dir = shared_provider_cache_dir(
                runtime_dir,
                "harvest_company_employees",
                provider_mode="live",
                runtime_environment="local_dev",
            )
            self.assertEqual(
                cache_dir,
                runtime_dir / "provider_cache" / "local_dev" / "live" / "harvest_company_employees",
            )

    def test_production_rejects_non_live_provider_mode_without_explicit_override(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                validate_runtime_environment(runtime_environment="production", provider_mode="replay")
        with patch.dict(os.environ, {"SOURCING_ALLOW_PRODUCTION_REPLAY": "1"}, clear=True):
            validate_runtime_environment(runtime_environment="production", provider_mode="replay")

    def test_runtime_requires_isolated_state_for_test_and_non_live_modes(self) -> None:
        self.assertTrue(runtime_requires_isolated_state(runtime_environment="test", provider_mode="live"))
        self.assertTrue(runtime_requires_isolated_state(runtime_environment="local_dev", provider_mode="scripted"))
        self.assertFalse(runtime_requires_isolated_state(runtime_environment="local_dev", provider_mode="live"))

    def test_provider_isolation_contract_blocks_live_in_test_runtime_without_dual_confirm(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "scripted_case"
            contract = provider_isolation_contract(provider_mode="live", runtime_dir=runtime_dir)

            self.assertFalse(contract.live_provider_access_allowed)
            overrides = provider_isolation_env_overrides(provider_mode="live", runtime_dir=runtime_dir)
            self.assertEqual(overrides[LIVE_PROVIDER_ACCESS_DISABLED_ENV], "1")
            self.assertEqual(overrides["APIFY_API_TOKEN"], "")

    def test_live_provider_boundary_allows_isolated_live_only_with_dual_confirm(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env_live"
            with patch.dict(
                os.environ,
                {
                    LIVE_PROVIDER_CONFIRM_ENV: "1",
                    ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS_ENV: "1",
                },
                clear=True,
            ):
                assert_live_provider_access_allowed(
                    provider_name="harvest_apify",
                    operation="submit_actor_run",
                    provider_mode="live",
                    runtime_dir=runtime_dir,
                    payload={"urls": ["https://www.linkedin.com/in/real-person-example/"]},
                )

    def test_live_provider_boundary_rejects_isolated_live_without_dual_confirm(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime" / "test_env" / "scripted_case"
            with patch.dict(os.environ, {LIVE_PROVIDER_CONFIRM_ENV: "1"}, clear=True):
                with self.assertRaises(LiveProviderAccessError):
                    assert_live_provider_access_allowed(
                        provider_name="harvest_apify",
                        operation="submit_actor_run",
                        provider_mode="live",
                        runtime_dir=runtime_dir,
                        payload={"urls": ["https://www.linkedin.com/in/real-person-example/"]},
                    )

    def test_provider_isolation_contract_disables_live_access_for_non_live_modes(self) -> None:
        contract = provider_isolation_contract(provider_mode="scripted", runtime_environment="scripted")

        self.assertFalse(contract.live_provider_access_allowed)
        self.assertTrue(contract.live_provider_access_disabled)
        overrides = provider_isolation_env_overrides(provider_mode="scripted", runtime_environment="scripted")
        self.assertEqual(overrides[LIVE_PROVIDER_ACCESS_DISABLED_ENV], "1")
        self.assertEqual(overrides["APIFY_API_TOKEN"], "")
        self.assertEqual(overrides["DATAFORSEO_LOGIN"], "")
        self.assertEqual(overrides["SERPER_API_KEY"], "")

    def test_default_unset_environment_isolation_is_fail_closed(self) -> None:
        # Regression guard for the 2026-06-27 incident: a bare process / detached
        # subprocess with NO provider env set must resolve to a fail-closed isolation
        # contract (live access disabled + every paid token blanked), so an
        # auto-spawned workflow runner inheriting this env cannot make billed calls.
        with patch.dict(os.environ, {}, clear=True):
            overrides = provider_isolation_env_overrides()
            contract = provider_isolation_contract()
        self.assertTrue(contract.live_provider_access_disabled)
        self.assertFalse(contract.live_provider_access_allowed)
        self.assertEqual(overrides[LIVE_PROVIDER_ACCESS_DISABLED_ENV], "1")
        self.assertEqual(overrides["APIFY_API_TOKEN"], "")
        self.assertEqual(overrides["HARVEST_PROFILE_SEARCH_API_TOKEN"], "")
        self.assertEqual(overrides["DATAFORSEO_LOGIN"], "")
        self.assertEqual(overrides["SERPER_API_KEY"], "")

    def test_live_provider_boundary_rejects_non_live_mode(self) -> None:
        with self.assertRaises(LiveProviderAccessError):
            assert_live_provider_access_allowed(
                provider_name="harvest_apify",
                operation="submit_actor_run",
                provider_mode="scripted",
                payload={"urls": ["https://www.linkedin.com/in/example/"]},
            )

    def test_live_provider_boundary_rejects_global_disable_flag(self) -> None:
        with patch.dict(os.environ, {LIVE_PROVIDER_ACCESS_DISABLED_ENV: "1"}, clear=False):
            with self.assertRaises(LiveProviderAccessError):
                assert_live_provider_access_allowed(
                    provider_name="dataforseo",
                    operation="task_post",
                    provider_mode="live",
                    payload={"keyword": "OpenAI Agent"},
                )

    def test_live_provider_boundary_rejects_synthetic_fixture_inputs_even_in_live_mode(self) -> None:
        # Dual-confirm set so we get past the confirm gate and specifically exercise
        # the synthetic/scripted-fixture marker rejection.
        with patch.dict(
            os.environ,
            {LIVE_PROVIDER_CONFIRM_ENV: "1", ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS_ENV: "1"},
            clear=True,
        ):
            with self.assertRaises(LiveProviderAccessError):
                assert_live_provider_access_allowed(
                    provider_name="harvest_apify",
                    operation="submit_actor_run",
                    provider_mode="live",
                    payload={"urls": ["https://www.linkedin.com/in/openai-agent-current-0087/"]},
                )

    def test_live_provider_boundary_rejects_local_dev_live_without_confirm(self) -> None:
        # Regression guard for the 2026-06-27 accidental-billing incident: a default
        # local_dev process in live mode (e.g. a detached workflow runner) must NOT be
        # able to reach a real provider call without the explicit dual-confirm.
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(LiveProviderAccessError):
                assert_live_provider_access_allowed(
                    provider_name="harvest_apify",
                    operation="submit_actor_run",
                    provider_mode="live",
                    runtime_environment="local_dev",
                    payload={"urls": ["https://www.linkedin.com/in/real-person-example/"]},
                )


if __name__ == "__main__":
    unittest.main()
