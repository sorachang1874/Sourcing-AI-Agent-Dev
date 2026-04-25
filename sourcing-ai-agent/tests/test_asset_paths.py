import json
import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.asset_paths import (
    hot_cache_company_assets_dir,
    resolve_company_snapshot_dir,
    resolve_company_snapshot_match_selection,
    resolve_company_snapshot_selection,
    resolve_snapshot_dir_from_source_path,
    resolve_source_path_in_runtime,
)


class AssetPathsSnapshotResolutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.tempdir.name)
        self.runtime_dir = self.project_root / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_resolve_company_snapshot_dir_discovers_hot_cache_only_company(self) -> None:
        snapshot_id = "20260409T101500"
        canonical_root = self.project_root / "canonical_assets"
        hot_cache_root = self.project_root / "hot_cache_assets"
        unrelated_snapshot_dir = canonical_root / "otherco" / "20260401T090000"
        unrelated_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (canonical_root / "otherco" / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": "20260401T090000"}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        hot_cache_company_dir = hot_cache_root / "acme"
        hot_cache_snapshot_dir = hot_cache_company_dir / snapshot_id
        hot_cache_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (hot_cache_company_dir / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            selection = resolve_company_snapshot_selection(self.runtime_dir, company_keys=["Acme"])
            snapshot_dir = resolve_company_snapshot_dir(self.runtime_dir, target_company="Acme")

        self.assertIsNotNone(selection)
        assert selection is not None
        self.assertEqual(selection["snapshot_id"], snapshot_id)
        self.assertEqual(Path(selection["company_dir"]).resolve(), hot_cache_company_dir.resolve())
        self.assertEqual(Path(selection["snapshot_dir"]).resolve(), hot_cache_snapshot_dir.resolve())
        self.assertEqual(snapshot_dir.resolve(), hot_cache_snapshot_dir.resolve())

    def test_resolve_company_snapshot_dir_keeps_canonical_latest_pointer_but_prefers_hot_cache_snapshot(self) -> None:
        canonical_snapshot_id = "20260408T204924"
        hot_cache_newer_snapshot_id = "20260410T103000"
        canonical_root = self.project_root / "canonical_assets"
        hot_cache_root = self.project_root / "hot_cache_assets"
        canonical_company_dir = canonical_root / "acme"
        hot_cache_company_dir = hot_cache_root / "acme"
        canonical_snapshot_dir = canonical_company_dir / canonical_snapshot_id
        hot_cache_canonical_snapshot_dir = hot_cache_company_dir / canonical_snapshot_id
        hot_cache_newer_snapshot_dir = hot_cache_company_dir / hot_cache_newer_snapshot_id
        canonical_snapshot_dir.mkdir(parents=True, exist_ok=True)
        hot_cache_canonical_snapshot_dir.mkdir(parents=True, exist_ok=True)
        hot_cache_newer_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (canonical_company_dir / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": canonical_snapshot_id}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (hot_cache_company_dir / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": hot_cache_newer_snapshot_id}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            selection = resolve_company_snapshot_selection(self.runtime_dir, company_keys=["Acme"])
            snapshot_dir = resolve_company_snapshot_dir(self.runtime_dir, target_company="Acme")

        self.assertIsNotNone(selection)
        assert selection is not None
        self.assertEqual(selection["snapshot_id"], canonical_snapshot_id)
        self.assertEqual(Path(selection["company_dir"]).resolve(), canonical_company_dir.resolve())
        self.assertEqual(Path(selection["snapshot_dir"]).resolve(), hot_cache_canonical_snapshot_dir.resolve())
        self.assertEqual(snapshot_dir.resolve(), hot_cache_canonical_snapshot_dir.resolve())

    def test_resolve_company_snapshot_dir_prefers_complete_canonical_snapshot_over_incomplete_hot_cache_shadow(self) -> None:
        snapshot_id = "20260412T090000"
        canonical_root = self.project_root / "canonical_assets"
        hot_cache_root = self.project_root / "hot_cache_assets"
        canonical_company_dir = canonical_root / "google"
        hot_cache_company_dir = hot_cache_root / "google"
        canonical_snapshot_dir = canonical_company_dir / snapshot_id
        hot_cache_snapshot_dir = hot_cache_company_dir / snapshot_id
        canonical_snapshot_dir.mkdir(parents=True, exist_ok=True)
        hot_cache_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (canonical_company_dir / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (hot_cache_company_dir / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (canonical_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_id,
                        "company_identity": {
                            "requested_name": "Google",
                            "canonical_name": "Google",
                            "company_key": "google",
                        },
                    },
                    "candidates": [],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            selection = resolve_company_snapshot_selection(self.runtime_dir, company_keys=["Google"], snapshot_id=snapshot_id)
            snapshot_dir = resolve_company_snapshot_dir(self.runtime_dir, target_company="Google", snapshot_id=snapshot_id)

        self.assertIsNotNone(selection)
        assert selection is not None
        self.assertEqual(Path(selection["snapshot_dir"]).resolve(), canonical_snapshot_dir.resolve())
        assert snapshot_dir is not None
        self.assertEqual(snapshot_dir.resolve(), canonical_snapshot_dir.resolve())

    def test_resolve_company_snapshot_match_selection_groups_alias_company_dirs(self) -> None:
        canonical_snapshot_id = "20260412T090000"
        hot_cache_newer_snapshot_id = "20260413T120000"
        canonical_root = self.project_root / "canonical_assets"
        hot_cache_root = self.project_root / "hot_cache_assets"
        canonical_company_dir = canonical_root / "ssiai"
        hot_cache_company_dir = hot_cache_root / "safesuperintelligenceinc"
        canonical_snapshot_dir = canonical_company_dir / canonical_snapshot_id
        hot_cache_canonical_snapshot_dir = hot_cache_company_dir / canonical_snapshot_id
        hot_cache_newer_snapshot_dir = hot_cache_company_dir / hot_cache_newer_snapshot_id
        canonical_snapshot_dir.mkdir(parents=True, exist_ok=True)
        hot_cache_canonical_snapshot_dir.mkdir(parents=True, exist_ok=True)
        hot_cache_newer_snapshot_dir.mkdir(parents=True, exist_ok=True)
        canonical_identity = {
            "requested_name": "Safe Superintelligence Inc",
            "canonical_name": "Safe Superintelligence Inc",
            "company_key": "ssiai",
            "linkedin_slug": "ssi-ai",
            "aliases": ["Safe Superintelligence"],
        }
        hot_cache_identity = {
            "requested_name": "Safe Superintelligence",
            "canonical_name": "Safe Superintelligence Inc",
            "company_key": "ssiai",
            "linkedin_slug": "ssi-ai",
            "aliases": ["Safe Superintelligence Inc"],
        }
        (canonical_company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {"snapshot_id": canonical_snapshot_id, "company_identity": canonical_identity},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (hot_cache_company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {"snapshot_id": hot_cache_newer_snapshot_id, "company_identity": hot_cache_identity},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (canonical_snapshot_dir / "identity.json").write_text(
            json.dumps(canonical_identity, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (hot_cache_canonical_snapshot_dir / "identity.json").write_text(
            json.dumps(hot_cache_identity, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (hot_cache_newer_snapshot_dir / "identity.json").write_text(
            json.dumps(hot_cache_identity, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            selection = resolve_company_snapshot_match_selection(
                self.runtime_dir,
                target_company="Safe Superintelligence",
            )

        self.assertIsNotNone(selection)
        assert selection is not None
        self.assertEqual(selection["snapshot_id"], canonical_snapshot_id)
        self.assertEqual(Path(selection["company_dir"]).resolve(), canonical_company_dir.resolve())
        self.assertEqual(Path(selection["snapshot_dir"]).resolve(), hot_cache_canonical_snapshot_dir.resolve())
        self.assertEqual(selection["identity_payload"]["company_key"], "ssiai")

    def test_resolve_company_snapshot_dir_uses_canonical_alias_key(self) -> None:
        canonical_root = self.project_root / "canonical_assets"
        cases = [
            ("google", "20260415T020102", "Google", "Google DeepMind"),
            ("humansand", "20260414T040101", "Humans&", "Humans&"),
        ]
        for company_dir_name, snapshot_id, canonical_name, query_name in cases:
            snapshot_dir = canonical_root / company_dir_name / snapshot_id
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            identity = {
                "requested_name": canonical_name,
                "canonical_name": canonical_name,
                "company_key": company_dir_name,
                "linkedin_slug": company_dir_name,
                "aliases": [canonical_name],
            }
            (snapshot_dir.parent / "latest_snapshot.json").write_text(
                json.dumps(
                    {"snapshot_id": snapshot_id, "company_identity": identity},
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (snapshot_dir / "identity.json").write_text(
                json.dumps(identity, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        with unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root)},
            clear=False,
        ):
            google_snapshot_dir = resolve_company_snapshot_dir(self.runtime_dir, target_company="Google DeepMind")
            humans_snapshot_dir = resolve_company_snapshot_dir(self.runtime_dir, target_company="Humans&")

        assert google_snapshot_dir is not None
        assert humans_snapshot_dir is not None
        self.assertEqual(google_snapshot_dir.resolve(), (canonical_root / "google" / "20260415T020102").resolve())
        self.assertEqual(humans_snapshot_dir.resolve(), (canonical_root / "humansand" / "20260414T040101").resolve())

    def test_hot_cache_company_assets_dir_defaults_under_runtime_dir(self) -> None:
        with unittest.mock.patch.dict(os.environ, {}, clear=False):
            resolved = hot_cache_company_assets_dir(self.runtime_dir)

        self.assertEqual(resolved, (self.runtime_dir / "hot_cache_company_assets").resolve())

    def test_hot_cache_company_assets_dir_can_be_explicitly_disabled(self) -> None:
        with unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_HOT_CACHE_ASSETS_DIR": "off"},
            clear=False,
        ):
            resolved = hot_cache_company_assets_dir(self.runtime_dir)

        self.assertIsNone(resolved)

    def test_resolve_snapshot_dir_from_legacy_linux_source_path_remaps_to_current_runtime(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260409T101500"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        source_path = (
            "/home/sorachang/projects/Sourcing AI Agent Dev/"
            "sourcing-ai-agent/runtime/company_assets/acme/20260409T101500/normalized_artifacts/manifest.json"
        )

        resolved = resolve_snapshot_dir_from_source_path(source_path, runtime_dir=self.runtime_dir)

        self.assertEqual(resolved, snapshot_dir.resolve())

    def test_resolve_source_path_in_runtime_remaps_legacy_linux_file_path(self) -> None:
        source_file = (
            self.runtime_dir
            / "company_assets"
            / "acme"
            / "20260409T101500"
            / "harvest_profiles"
            / "alice.json"
        )
        source_file.parent.mkdir(parents=True, exist_ok=True)
        source_file.write_text("{}", encoding="utf-8")
        source_path = (
            "/home/sorachang/projects/Sourcing AI Agent Dev/"
            "sourcing-ai-agent/runtime/company_assets/acme/20260409T101500/harvest_profiles/alice.json"
        )

        resolved = resolve_source_path_in_runtime(source_path, runtime_dir=self.runtime_dir)

        self.assertEqual(resolved, source_file.resolve())

    def test_resolve_company_snapshot_dir_prefers_stable_company_identity_over_requested_name_collision(self) -> None:
        snapshot_id = "20260408T154820"
        humans_dir = self.runtime_dir / "company_assets" / "humans"
        humansand_dir = self.runtime_dir / "company_assets" / "humansand"
        humans_snapshot = humans_dir / snapshot_id
        humansand_snapshot = humansand_dir / snapshot_id
        humans_snapshot.mkdir(parents=True, exist_ok=True)
        humansand_snapshot.mkdir(parents=True, exist_ok=True)
        humans_identity = {
            "requested_name": "Humans",
            "canonical_name": "Humans",
            "company_key": "humans",
            "aliases": ["humans"],
        }
        humansand_identity = {
            "requested_name": "Humans&",
            "canonical_name": "humans&",
            "company_key": "humansand",
            "linkedin_slug": "humansand",
            "aliases": ["humansand"],
        }
        (humans_snapshot / "identity.json").write_text(
            json.dumps(humans_identity, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (humansand_snapshot / "identity.json").write_text(
            json.dumps(humansand_identity, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (humans_dir / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id, "company_identity": humans_identity}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (humansand_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {"snapshot_id": snapshot_id, "company_identity": humansand_identity},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        humans_resolved = resolve_company_snapshot_dir(
            self.runtime_dir,
            target_company="humans",
            snapshot_id=snapshot_id,
        )
        humansand_resolved = resolve_company_snapshot_dir(
            self.runtime_dir,
            target_company="humansand",
            snapshot_id=snapshot_id,
        )

        self.assertEqual(humans_resolved.resolve(), humans_snapshot.resolve())
        self.assertEqual(humansand_resolved.resolve(), humansand_snapshot.resolve())


if __name__ == "__main__":
    unittest.main()
