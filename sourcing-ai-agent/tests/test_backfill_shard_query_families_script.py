"""Regression pin: the backfill script's --dry-run is fully read-only.

2026-07-23 defect: `--rebuild-from-assets` ran the writing snapshot->registry
ingestion (`ensure_acquisition_shard_registry_for_snapshot`) unconditionally —
`--dry-run` only guarded the later metadata upserts, so the documented
"--rebuild-from-assets --dry-run first" inventory step would have written
live registry rows. Dry-run must skip the rebuild and report it as planned.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import scripts.backfill_acquisition_shard_query_families as backfill_script
from scripts.backfill_profile_fetched_flags import reconcile_candidate_documents
from sourcing_agent.asset_reuse_planning import build_salvage_manifest_shard_registry_records


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


class SalvageManifestRecordBuilderTest(unittest.TestCase):
    """Adopted-paid-dataset receipts map onto the company_employees lane;
    profile batches and passthrough docs are skipped with explicit reasons."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_root = Path(self.tempdir.name) / "runtime"
        (self.runtime_root / "salvage").mkdir(parents=True)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _write_dataset(self, name: str, entries: int) -> str:
        path = self.runtime_root / "salvage" / name
        path.write_text(json.dumps([{"id": i} for i in range(entries)]))
        return str(path)

    def test_function_paired_datasets_yield_per_function_shards(self) -> None:
        fn8 = self._write_dataset("fn8.json", 3)
        fn24 = self._write_dataset("fn24.json", 2)
        records, skipped = build_salvage_manifest_shard_registry_records(
            target_company="Google",
            company_key="google",
            snapshot_id="20260722T113432",
            manifest={
                "salvage_note": "Built from adopted paid Apify datasets",
                "created_at": "2026-07-22T11:34:33Z",
                "inputs": {
                    "current_roster_dataset": f"[PosixPath('{fn8}'), PosixPath('{fn24}')]",
                    "current_roster_functions": ["8", "24"],
                    "current_roster_run_ids": ["runA", "runB"],
                },
            },
            manifest_path="manifest.json",
            runtime_root=self.runtime_root,
        )
        self.assertEqual(skipped, [])
        self.assertEqual(len(records), 2)
        first, second = records
        self.assertEqual(first["lane"], "company_employees")
        self.assertEqual(first["shard_id"], "salvage_fn8")
        self.assertEqual(first["result_count"], 3)
        self.assertEqual(first["function_ids"], ["8"])
        self.assertEqual(dict(first["metadata"])["apify_run_id"], "runA")
        self.assertTrue(dict(first["metadata"])["adopted_paid_dataset"])
        self.assertEqual(
            dict(dict(first["metadata"])["request_filters"]).get("function_ids"), ["8"]
        )
        self.assertEqual(second["shard_id"], "salvage_fn24")
        self.assertEqual(second["result_count"], 2)

    def test_full_mode_roster_yields_single_root_shard_and_skips_non_receipts(self) -> None:
        roster = self._write_dataset("tml_roster.json", 5)
        records, skipped = build_salvage_manifest_shard_registry_records(
            target_company="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            snapshot_id="20260722T113432",
            manifest={
                "inputs": {
                    "current_roster_dataset": f"[PosixPath('{roster}')]",
                    "current_roster_functions": [],
                    "current_roster_run_ids": ["runC"],
                    "former_candidate_documents": "old/candidate_documents.json",
                    "profile_datasets": ["b1.json", "b2.json"],
                },
            },
            manifest_path="manifest.json",
            runtime_root=self.runtime_root,
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["shard_id"], "salvage_root")
        self.assertEqual(records[0]["result_count"], 5)
        self.assertEqual(records[0]["function_ids"], [])
        reasons = {row["reason"] for row in skipped}
        self.assertEqual(
            reasons,
            {
                "profile_detail_batches_have_no_registry_lane_contract",
                "passthrough_candidate_documents_are_not_provider_receipts",
            },
        )

    def test_profile_fetched_reconciliation_is_evidence_only(self) -> None:
        import hashlib

        snap = Path(self.tempdir.name) / "snap"
        (snap / "harvest_profiles").mkdir(parents=True)
        url = "https://www.linkedin.com/in/with-envelope"
        sha_key = hashlib.sha1(url.encode()).hexdigest()[:16]
        (snap / "harvest_profiles" / f"{sha_key}.json").write_text(json.dumps({"item": {"emails": []}}))
        (snap / "harvest_profiles" / "cid42.json").write_text(json.dumps({"item": {"emails": ["x@y.z"]}}))
        (snap / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        {"candidate_id": "a1", "display_name": "NoEvidence", "linkedin_url": "https://x/no"},
                        {"candidate_id": "b2", "display_name": "ShaMatch", "linkedin_url": url},
                        {"candidate_id": "cid42", "display_name": "IdMatch", "profile_fetched": True},
                    ]
                }
            )
        )
        dry = reconcile_candidate_documents(snap, dry_run=True)
        self.assertEqual((dry["flag_flips"], dry["mode_stamps"]), (1, 1))
        self.assertFalse(dry["written"])
        real = reconcile_candidate_documents(snap, dry_run=False)
        self.assertTrue(real["written"])
        rows = json.loads((snap / "candidate_documents.json").read_text())["candidates"]
        by_name = {r["display_name"]: r for r in rows}
        self.assertNotIn("profile_fetched", by_name["NoEvidence"])  # evidence-only: untouched
        self.assertTrue(by_name["ShaMatch"]["profile_fetched"])
        self.assertEqual(by_name["ShaMatch"]["profile_mode"], "full_details_no_email")
        self.assertEqual(by_name["ShaMatch"]["profile_merge_path"], "salvage_hash_joined")
        # envelope with emails: flag already true stays; mode NOT stamped as no_email
        self.assertNotIn("profile_mode", by_name["IdMatch"])
        self.assertTrue((snap / "candidate_documents.json.pre_flag_backfill_bak").exists())

    def test_profile_only_manifest_registers_nothing(self) -> None:
        records, skipped = build_salvage_manifest_shard_registry_records(
            target_company="OpenAI",
            company_key="openai",
            snapshot_id="20260722T113432",
            manifest={
                "inputs": {
                    "current_roster_dataset": "",
                    "profile_datasets": ["batch1.json"],
                },
            },
            manifest_path="manifest.json",
            runtime_root=self.runtime_root,
        )
        self.assertEqual(records, [])
        self.assertEqual(
            [row["reason"] for row in skipped],
            ["profile_detail_batches_have_no_registry_lane_contract"],
        )


if __name__ == "__main__":
    unittest.main()
