import json
import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.connectors import CompanyIdentity


class LatestSnapshotPointerContractTest(unittest.TestCase):
    """latest_snapshot.json contract: `snapshot_id` is authoritative and the
    recorded snapshot_dir is derived from the company_dir being written —
    a pointer must never aim outside its own assets root (the 2026-07 drift:
    canonical pointers into hot-cache, test_env_live, and a dead /home path)."""

    def _identity(self) -> CompanyIdentity:
        return CompanyIdentity(
            requested_name="xAI",
            canonical_name="xAI",
            company_key="xai",
            linkedin_slug="xai",
            linkedin_company_url="https://www.linkedin.com/company/xai/",
        )

    def _write(self, company_dir: Path, snapshot_dir: Path) -> dict:
        AcquisitionEngine._write_latest_snapshot_pointer_to_dir(
            None,  # method touches no engine state
            company_dir=company_dir,
            identity=self._identity(),
            snapshot_id="20260722T000000",
            snapshot_dir=snapshot_dir,
        )
        return json.loads((company_dir / "latest_snapshot.json").read_text(encoding="utf-8"))

    def test_recorded_dir_is_always_inside_the_company_dir(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            company_dir = Path(root) / "company_assets" / "xai"
            company_dir.mkdir(parents=True)
            foreign_dir = Path(root) / "hot_cache_company_assets" / "xai" / "20260722T000000"

            payload = self._write(company_dir, foreign_dir)

            self.assertEqual(payload["snapshot_id"], "20260722T000000")
            self.assertEqual(payload["snapshot_dir"], str(company_dir / "20260722T000000"))
            self.assertEqual(payload["pointer_contract"], "snapshot_id")
            self.assertEqual(payload["source_snapshot_dir_provenance"], str(foreign_dir))

    def test_consistent_dir_records_no_provenance_field(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            company_dir = Path(root) / "company_assets" / "xai"
            company_dir.mkdir(parents=True)

            payload = self._write(company_dir, company_dir / "20260722T000000")

            self.assertEqual(payload["snapshot_dir"], str(company_dir / "20260722T000000"))
            self.assertNotIn("source_snapshot_dir_provenance", payload)


class CanonicalizeCompanyAssetPathTest(unittest.TestCase):
    """Registry source_path must record the canonical home of an asset, not the
    hot-cache build location (regenerating hot-cache registrations re-armed the
    2026-07-22 source==destination incident on every reconcile)."""

    def _runtime(self, root: str) -> Path:
        runtime = Path(root) / "runtime"
        (runtime / "company_assets" / "xai" / "snap").mkdir(parents=True)
        (runtime / "hot_cache_company_assets" / "xai" / "snap").mkdir(parents=True)
        return runtime

    def test_hot_cache_path_with_existing_canonical_twin_is_rewritten(self) -> None:
        from unittest.mock import patch

        from sourcing_agent.asset_paths import canonicalize_company_asset_path

        with tempfile.TemporaryDirectory() as root, patch.dict(
            "os.environ", {}, clear=False
        ):
            runtime = self._runtime(root)
            (runtime / "company_assets" / "xai" / "snap" / "summary.json").write_text("{}")
            hot = runtime / "hot_cache_company_assets" / "xai" / "snap" / "summary.json"
            hot.write_text("{}")

            result = canonicalize_company_asset_path(runtime, hot)

            expected = (runtime / "company_assets").resolve() / "xai" / "snap" / "summary.json"
            self.assertEqual(result, str(expected))

    def test_hot_cache_path_without_canonical_twin_is_kept(self) -> None:
        from sourcing_agent.asset_paths import canonicalize_company_asset_path

        with tempfile.TemporaryDirectory() as root:
            runtime = self._runtime(root)
            hot = runtime / "hot_cache_company_assets" / "xai" / "snap" / "only_here.json"
            hot.write_text("{}")

            self.assertEqual(canonicalize_company_asset_path(runtime, hot), str(hot))

    def test_non_hot_cache_paths_pass_through(self) -> None:
        from sourcing_agent.asset_paths import canonicalize_company_asset_path

        with tempfile.TemporaryDirectory() as root:
            runtime = self._runtime(root)
            canonical = runtime / "company_assets" / "xai" / "snap" / "summary.json"
            canonical.write_text("{}")

            self.assertEqual(canonicalize_company_asset_path(runtime, canonical), str(canonical))
            self.assertEqual(canonicalize_company_asset_path(runtime, ""), "")


if __name__ == "__main__":
    unittest.main()
