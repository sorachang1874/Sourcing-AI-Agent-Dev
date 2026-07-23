"""Regression pin: the backfill script's --dry-run is fully read-only.

2026-07-23 defect: `--rebuild-from-assets` ran the writing snapshot->registry
ingestion (`ensure_acquisition_shard_registry_for_snapshot`) unconditionally —
`--dry-run` only guarded the later metadata upserts, so the documented
"--rebuild-from-assets --dry-run first" inventory step would have written
live registry rows. Dry-run must skip the rebuild and report it as planned.
"""

import unittest
from unittest import mock

import scripts.backfill_acquisition_shard_query_families as backfill_script


class _FakeStore:
    def __init__(self) -> None:
        self.upserts: list[dict] = []

    def list_acquisition_shard_registry(self, **_kwargs) -> list[dict]:
        return []

    def upsert_acquisition_shard_registry(self, row: dict) -> None:
        self.upserts.append(dict(row))


class BackfillDryRunReadOnlyTest(unittest.TestCase):
    def _run_main(self, argv: list[str]) -> tuple[_FakeStore, mock.MagicMock]:
        fake_store = _FakeStore()
        fake_settings = mock.Mock(db_path="unused.db", runtime_dir="runtime")
        rebuild = mock.MagicMock(return_value={"organization_records": 1, "shard_records": 2})
        with (
            mock.patch.object(backfill_script.sys, "argv", ["backfill", *argv]),
            mock.patch.object(backfill_script, "load_settings", return_value=fake_settings),
            mock.patch.object(backfill_script, "ControlPlaneStore", return_value=fake_store),
            mock.patch.object(backfill_script, "ensure_acquisition_shard_registry_for_snapshot", rebuild),
            mock.patch("builtins.print"),
        ):
            backfill_script.main()
        return fake_store, rebuild

    def test_dry_run_rebuild_never_writes(self) -> None:
        store, rebuild = self._run_main(
            ["--company", "Skild AI", "--snapshot-id", "snap-1", "--rebuild-from-assets", "--dry-run"]
        )
        rebuild.assert_not_called()
        self.assertEqual(store.upserts, [])

    def test_live_rebuild_still_ingests(self) -> None:
        _store, rebuild = self._run_main(
            ["--company", "Skild AI", "--snapshot-id", "snap-1", "--rebuild-from-assets"]
        )
        rebuild.assert_called_once()


if __name__ == "__main__":
    unittest.main()
