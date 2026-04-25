import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sourcing_agent.runtime_environment import (
    current_runtime_environment,
    normalize_provider_mode,
    normalize_runtime_environment,
    runtime_requires_isolated_state,
    shared_provider_cache_dir,
    validate_runtime_environment,
)


class RuntimeEnvironmentTest(unittest.TestCase):
    def test_provider_mode_normalization_uses_live_by_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(normalize_provider_mode(), "live")
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


if __name__ == "__main__":
    unittest.main()
