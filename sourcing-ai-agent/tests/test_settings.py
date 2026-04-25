import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.settings import load_settings


def _resolved(path: Path) -> Path:
    return path.resolve()


class SettingsRuntimeOverrideTest(unittest.TestCase):
    def test_load_settings_uses_runtime_dir_override_for_stateful_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            runtime_dir = project_root / "runtime" / "test_env"
            secrets_dir = project_root / "runtime" / "secrets"
            secrets_dir.mkdir(parents=True, exist_ok=True)
            (secrets_dir / "providers.local.json").write_text("{}", encoding="utf-8")

            with mock.patch.dict(
                os.environ,
                {
                    "SOURCING_RUNTIME_DIR": str(runtime_dir),
                    "OBJECT_STORAGE_PROVIDER": "filesystem",
                },
                clear=False,
            ):
                settings = load_settings(project_root)

            self.assertEqual(_resolved(settings.runtime_dir), _resolved(runtime_dir))
            self.assertEqual(_resolved(settings.jobs_dir), _resolved(runtime_dir / "jobs"))
            self.assertEqual(_resolved(settings.company_assets_dir), _resolved(runtime_dir / "company_assets"))
            self.assertEqual(_resolved(settings.db_path), _resolved(runtime_dir / "sourcing_agent.db"))
            self.assertEqual(_resolved(Path(settings.object_storage.local_dir)), _resolved(runtime_dir / "object_store"))

    def test_load_settings_uses_control_plane_shadow_db_when_local_postgres_is_discoverable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            runtime_dir = project_root / "runtime"
            secrets_dir = runtime_dir / "secrets"
            local_pg_root = project_root / ".local-postgres"
            (local_pg_root / "extract/usr/lib/postgresql/16/bin").mkdir(parents=True, exist_ok=True)
            (local_pg_root / "data").mkdir(parents=True, exist_ok=True)
            secrets_dir.mkdir(parents=True, exist_ok=True)
            (secrets_dir / "providers.local.json").write_text("{}", encoding="utf-8")

            with mock.patch.dict(os.environ, {"OBJECT_STORAGE_PROVIDER": "filesystem"}, clear=True):
                settings = load_settings(project_root)

            self.assertEqual(_resolved(settings.db_path), _resolved(runtime_dir / "control_plane.shadow.db"))

    def test_load_settings_uses_control_plane_shadow_db_when_env_file_declares_postgres_dsn(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            runtime_dir = project_root / "runtime"
            secrets_dir = runtime_dir / "secrets"
            secrets_dir.mkdir(parents=True, exist_ok=True)
            (secrets_dir / "providers.local.json").write_text("{}", encoding="utf-8")
            (project_root / ".local-postgres.env").write_text(
                "SOURCING_CONTROL_PLANE_POSTGRES_DSN=postgresql://mac@127.0.0.1:55432/sourcing_agent\n",
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {"OBJECT_STORAGE_PROVIDER": "filesystem"}, clear=True):
                settings = load_settings(project_root)

            self.assertEqual(_resolved(settings.db_path), _resolved(runtime_dir / "control_plane.shadow.db"))

    def test_load_settings_falls_back_to_default_runtime_secrets_when_override_runtime_has_none(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            default_secrets_dir = project_root / "runtime" / "secrets"
            default_secrets_dir.mkdir(parents=True, exist_ok=True)
            default_secret_file = default_secrets_dir / "providers.local.json"
            default_secret_file.write_text("{}", encoding="utf-8")

            with mock.patch.dict(
                os.environ,
                {
                    "SOURCING_RUNTIME_DIR": str(project_root / "runtime" / "test_env"),
                    "OBJECT_STORAGE_PROVIDER": "filesystem",
                },
                clear=False,
            ):
                settings = load_settings(project_root)

            self.assertEqual(_resolved(settings.secrets_file), _resolved(default_secret_file))

    def test_load_settings_keeps_semantic_provider_disabled_by_default_even_with_qwen_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            secrets_dir = project_root / "runtime" / "secrets"
            secrets_dir.mkdir(parents=True, exist_ok=True)
            (secrets_dir / "providers.local.json").write_text(
                '{"qwen":{"api_key":"qwen-key"},"semantic":{}}',
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {}, clear=True):
                settings = load_settings(project_root)

            self.assertTrue(settings.qwen.enabled)
            self.assertFalse(settings.semantic.enabled)
            self.assertEqual(settings.semantic.api_key, "")

    def test_load_settings_allows_semantic_provider_to_explicitly_reuse_qwen_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            secrets_dir = project_root / "runtime" / "secrets"
            secrets_dir.mkdir(parents=True, exist_ok=True)
            (secrets_dir / "providers.local.json").write_text(
                '{"qwen":{"api_key":"qwen-key"},"semantic":{"enabled":true}}',
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {}, clear=True):
                settings = load_settings(project_root)

            self.assertTrue(settings.qwen.enabled)
            self.assertTrue(settings.semantic.enabled)
            self.assertEqual(settings.semantic.api_key, "qwen-key")

    def test_load_settings_env_can_disable_semantic_provider_even_when_secret_enables_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            secrets_dir = project_root / "runtime" / "secrets"
            secrets_dir.mkdir(parents=True, exist_ok=True)
            (secrets_dir / "providers.local.json").write_text(
                '{"qwen":{"api_key":"qwen-key"},"semantic":{"enabled":true,"api_key":"semantic-key"}}',
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {"SOURCING_SEMANTIC_PROVIDER_ENABLED": "false"},
                clear=True,
            ):
                settings = load_settings(project_root)

            self.assertFalse(settings.semantic.enabled)
            self.assertEqual(settings.semantic.api_key, "semantic-key")


if __name__ == "__main__":
    unittest.main()
