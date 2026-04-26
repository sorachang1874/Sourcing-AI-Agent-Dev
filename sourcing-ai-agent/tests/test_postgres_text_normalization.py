import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.control_plane_live_postgres import _fetch_one_dict_row
from sourcing_agent.local_postgres import describe_control_plane_runtime
from sourcing_agent.storage import ControlPlaneStore


class _FakeCursor:
    description = [
        ("target_company",),
        ("snapshot_id",),
        ("default_acquisition_mode",),
    ]


class PostgresTextNormalizationTest(unittest.TestCase):
    def test_fetch_one_dict_row_decodes_bytes_and_byte_literal_strings(self) -> None:
        row = _fetch_one_dict_row(
            _FakeCursor(),
            (
                b"OpenAI",
                "b'20260417T041804'",
                b"full_company_roster",
            ),
        )
        self.assertEqual(
            row,
            {
                "target_company": "OpenAI",
                "snapshot_id": "20260417T041804",
                "default_acquisition_mode": "full_company_roster",
            },
        )

    def test_storage_upserts_normalize_byte_literal_text_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            db_path = runtime_dir / "sourcing_agent.db"
            with mock.patch.dict(
                "os.environ",
                {
                    "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "",
                    "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "disabled",
                },
                clear=False,
            ):
                store = ControlPlaneStore(db_path)
                registry = store.upsert_organization_asset_registry(
                    {
                        "target_company": "b'OpenAI'",
                        "company_key": "b'openai'",
                        "snapshot_id": "b'20260417T041804'",
                        "asset_view": "b'canonical_merged'",
                        "status": "b'ready'",
                        "completeness_band": "b'high'",
                        "source_path": "b'runtime/company_assets/openai/20260417T041804'",
                        "source_job_id": "b'job-1'",
                        "materialization_generation_key": "b'openai:20260417T041804'",
                        "materialization_watermark": "b'2026-04-21T10:00:00+00:00'",
                        "selected_snapshot_ids": ["b'20260417T041804'"],
                    },
                    authoritative=True,
                )
                profile = store.upsert_organization_execution_profile(
                    {
                        "target_company": "b'OpenAI'",
                        "company_key": "b'openai'",
                        "asset_view": "b'canonical_merged'",
                        "source_snapshot_id": "b'20260417T041804'",
                        "status": "b'ready'",
                        "org_scale_band": "b'large'",
                        "default_acquisition_mode": "b'full_company_roster'",
                        "current_lane_default": "b'local_reuse'",
                        "former_lane_default": "b'local_reuse'",
                        "completeness_band": "b'high'",
                    }
                )
                shard = store.upsert_acquisition_shard_registry(
                    {
                        "shard_key": "b'openai:20260417T041804:all:all'",
                        "target_company": "b'OpenAI'",
                        "company_key": "b'openai'",
                        "snapshot_id": "b'20260417T041804'",
                        "asset_view": "b'canonical_merged'",
                        "status": "b'completed'",
                        "employment_scope": "b'all'",
                        "search_query": "b'Reasoning'",
                        "query_signature": "b'reasoning'",
                    }
                )

            self.assertEqual(registry["target_company"], "OpenAI")
            self.assertEqual(registry["snapshot_id"], "20260417T041804")
            self.assertEqual(profile["default_acquisition_mode"], "full_company_roster")
            self.assertEqual(profile["current_lane_default"], "local_reuse")
            self.assertEqual(shard["search_query"], "Reasoning")
            self.assertEqual(shard["query_signature"], "reasoning")

    def test_runtime_summary_surfaces_sql_ascii_probe(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            runtime_dir = project_root / "runtime"
            local_pg_root = project_root / ".local-postgres"
            (local_pg_root / "extract/usr/lib/postgresql/16/bin").mkdir(parents=True, exist_ok=True)
            (local_pg_root / "data").mkdir(parents=True, exist_ok=True)

            with mock.patch(
                "sourcing_agent.local_postgres.inspect_local_postgres_encoding",
                return_value={
                    "available": True,
                    "running": True,
                    "status": "ok",
                    "server_encoding": "SQL_ASCII",
                    "client_encoding": "UTF8",
                    "database_encoding": "SQL_ASCII",
                    "encoding_warning": True,
                },
            ), mock.patch(
                "sourcing_agent.local_postgres.local_postgres_is_running",
                return_value=True,
            ), mock.patch.dict("os.environ", {"OBJECT_STORAGE_PROVIDER": "filesystem"}, clear=False):
                summary = describe_control_plane_runtime(base_dir=project_root, runtime_dir=runtime_dir)

            probe = summary["local_postgres"]["encoding_probe"]
            self.assertTrue(probe["encoding_warning"])
            self.assertEqual(probe["server_encoding"], "SQL_ASCII")


if __name__ == "__main__":
    unittest.main()
