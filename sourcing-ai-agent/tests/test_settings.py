import json
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
            self.assertEqual(
                _resolved(Path(settings.object_storage.local_dir)), _resolved(runtime_dir / "object_store")
            )

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

    def test_qwen_model_default_and_override_order(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            secrets_dir = project_root / "runtime" / "secrets"
            secrets_dir.mkdir(parents=True, exist_ok=True)
            secret_file = secrets_dir / "providers.local.json"

            secret_file.write_text('{"qwen":{"api_key":"qwen-key"}}', encoding="utf-8")
            with mock.patch.dict(os.environ, {}, clear=True):
                settings = load_settings(project_root)
            self.assertEqual(settings.qwen.model, "qwen3.5-plus-2026-04-20")

            secret_file.write_text(
                '{"qwen":{"api_key":"qwen-key","model":"qwen-custom-secret"}}',
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {}, clear=True):
                settings = load_settings(project_root)
            self.assertEqual(settings.qwen.model, "qwen-custom-secret")

            with mock.patch.dict(os.environ, {"DASHSCOPE_MODEL": "qwen-env-override"}, clear=True):
                settings = load_settings(project_root)
            self.assertEqual(settings.qwen.model, "qwen-env-override")

    def test_model_provider_api_key_file_overrides_inline_secret(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            secrets_dir = project_root / "runtime" / "secrets"
            secrets_dir.mkdir(parents=True, exist_ok=True)
            key_file = project_root / "relay-key.md"
            key_file.write_text("Base URL: https://relay.example.test/v1\nAPI Key: sk-file-key\n", encoding="utf-8")
            (secrets_dir / "providers.local.json").write_text(
                json.dumps(
                    {
                        "model_provider": {
                            "provider_name": "relay",
                            "api_key": "sk-inline-key",
                            "api_key_file": "relay-key.md",
                            "base_url": "https://api.example.test/v1",
                            "model": "gpt-5.5",
                        }
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {}, clear=True):
                settings = load_settings(project_root)

            self.assertTrue(settings.model_provider.enabled)
            self.assertEqual(settings.model_provider.api_key, "sk-file-key")
            self.assertEqual(settings.model_provider.model, "gpt-5.5")

    def test_model_native_search_settings_are_explicit_and_disabled_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            secrets_dir = project_root / "runtime" / "secrets"
            secrets_dir.mkdir(parents=True, exist_ok=True)
            (secrets_dir / "providers.local.json").write_text("{}", encoding="utf-8")

            with mock.patch.dict(os.environ, {}, clear=True):
                settings = load_settings(project_root)

            self.assertFalse(settings.search.enable_model_native_search)
            self.assertEqual(settings.search.model_native_search_mode, "disabled")
            self.assertEqual(settings.search.model_native_search_max_queries_per_operation, 0)
            self.assertEqual(settings.search.model_native_search_cost_budget_usd, 0.0)

    def test_model_native_search_settings_require_explicit_env_or_secret(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            secrets_dir = project_root / "runtime" / "secrets"
            secrets_dir.mkdir(parents=True, exist_ok=True)
            (secrets_dir / "providers.local.json").write_text("{}", encoding="utf-8")

            with mock.patch.dict(
                os.environ,
                {
                    "SEARCH_PROVIDER_ENABLE_MODEL_NATIVE_SEARCH": "1",
                    "SEARCH_PROVIDER_MODEL_NATIVE_SEARCH_MODE": "experimental_evidence_only",
                    "SEARCH_PROVIDER_MODEL_NATIVE_SEARCH_MAX_QUERIES": "12",
                    "SEARCH_PROVIDER_MODEL_NATIVE_SEARCH_COST_BUDGET_USD": "2.50",
                },
                clear=True,
            ):
                settings = load_settings(project_root)

            self.assertTrue(settings.search.enable_model_native_search)
            self.assertEqual(settings.search.model_native_search_mode, "experimental_evidence_only")
            self.assertEqual(settings.search.model_native_search_max_queries_per_operation, 12)
            self.assertEqual(settings.search.model_native_search_cost_budget_usd, 2.5)

    def test_load_settings_clears_live_provider_credentials_in_non_live_provider_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            secrets_dir = project_root / "runtime" / "test_env" / "secrets"
            secrets_dir.mkdir(parents=True, exist_ok=True)
            secret_file = secrets_dir / "providers.local.json"
            secret_file.write_text(
                json.dumps(
                    {
                        "search_provider": {
                            "dataforseo_login": "secret-dataforseo-login",
                            "dataforseo_password": "secret-dataforseo-password",
                            "serper_api_key": "secret-serper-key",
                        },
                        "harvest": {
                            "profile_scraper": {"api_token": "secret-apify-token"},
                            "profile_search": {"api_token": "secret-apify-token"},
                            "company_employees": {"api_token": "secret-apify-token"},
                        },
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "SOURCING_RUNTIME_DIR": str(project_root / "runtime" / "test_env"),
                    "SOURCING_SECRETS_FILE": str(secret_file),
                    "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                    "APIFY_API_TOKEN": "ambient-apify-token",
                    "DATAFORSEO_LOGIN": "ambient-dataforseo-login",
                    "DATAFORSEO_PASSWORD": "ambient-dataforseo-password",
                    "SERPER_API_KEY": "ambient-serper-key",
                },
                clear=False,
            ):
                settings = load_settings(project_root)

            self.assertFalse(settings.harvest.profile_scraper.enabled)
            self.assertEqual(settings.harvest.profile_scraper.api_token, "")
            self.assertEqual(settings.harvest.profile_search.api_token, "")
            self.assertEqual(settings.harvest.company_employees.api_token, "")
            self.assertEqual(settings.search.dataforseo_login, "")
            self.assertEqual(settings.search.dataforseo_password, "")
            self.assertEqual(settings.search.serper_api_key, "")

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
