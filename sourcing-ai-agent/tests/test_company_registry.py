import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sourcing_agent.company_registry import (
    builtin_company_identity,
    infer_target_company_from_text,
    refresh_company_identity_registry,
)
from sourcing_agent.domain import JobRequest


class CompanyRegistryTest(unittest.TestCase):
    def _write_seed_catalog(
        self,
        *,
        runtime_dir: Path,
        records: list[dict[str, object]],
    ) -> None:
        seed_path = runtime_dir / "company_identity_seed_catalog.json"
        seed_path.parent.mkdir(parents=True, exist_ok=True)
        seed_path.write_text(
            json.dumps(
                {
                    "updated_at": "2026-04-15T00:00:00+00:00",
                    "company_count": len(records),
                    "alias_count": 0,
                    "records": records,
                    "alias_index": {},
                },
                ensure_ascii=False,
            )
        )

    def _write_runtime_identity(
        self,
        *,
        runtime_dir: Path,
        company_key: str,
        snapshot_id: str,
        requested_name: str,
        canonical_name: str,
        linkedin_slug: str,
        aliases: list[str],
        confidence: str = "high",
        assets_dir_name: str = "company_assets",
    ) -> None:
        identity_path = runtime_dir / assets_dir_name / company_key / snapshot_id / "identity.json"
        identity_path.parent.mkdir(parents=True, exist_ok=True)
        identity_path.write_text(
            json.dumps(
                {
                    "requested_name": requested_name,
                    "canonical_name": canonical_name,
                    "company_key": company_key,
                    "linkedin_slug": linkedin_slug,
                    "aliases": aliases,
                    "confidence": confidence,
                },
                ensure_ascii=False,
            )
        )

    def test_infer_target_company_from_text_detects_anthropic(self) -> None:
        inferred = infer_target_company_from_text("帮我找出Anthropic的所有华人成员")

        self.assertEqual(inferred["company_key"], "anthropic")
        self.assertEqual(inferred["canonical_name"], "Anthropic")

    def test_infer_target_company_from_text_detects_humansand(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            self._write_seed_catalog(
                runtime_dir=runtime_dir,
                records=[
                    {
                        "company_key": "humansand",
                        "canonical_name": "Humans&",
                        "linkedin_slug": "humansand",
                        "aliases": ["humans and", "humansand"],
                        "confidence": "high",
                    }
                ],
            )
            with patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                inferred = infer_target_company_from_text("我想了解 Humans& 的 Coding 方向 Researcher")

        self.assertEqual(inferred["company_key"], "humansand")
        self.assertEqual(inferred["canonical_name"], "Humans&")

    def test_infer_target_company_from_text_detects_safe_superintelligence_inc_from_bundled_seed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            with patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                inferred = infer_target_company_from_text("给我'Safe Superintelligence Inc'这家公司的人")
                company_key, builtin = builtin_company_identity("Safe Superintelligence Inc")

        self.assertEqual(inferred["company_key"], "safesuperintelligence")
        self.assertEqual(inferred["canonical_name"], "Safe Superintelligence")
        self.assertEqual(company_key, "safesuperintelligence")
        self.assertIsNotNone(builtin)
        assert builtin is not None
        self.assertEqual(builtin["linkedin_slug"], "ssi-ai")
        self.assertEqual(builtin["resolver"], "seed_catalog")

    def test_infer_target_company_from_text_heuristically_detects_physical_intelligence_without_runtime_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            with patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                inferred = infer_target_company_from_text("给我Physical Intelligence的所有成员")

            self.assertEqual(inferred["company_key"], "physicalintelligence")
            self.assertEqual(inferred["canonical_name"], "Physical Intelligence")
            self.assertEqual(inferred["matched_alias"], "heuristic_company_span")

    def test_infer_target_company_from_text_heuristic_does_not_promote_generic_geo_phrase(self) -> None:
        inferred = infer_target_company_from_text("帮我找有 Greater China 经验的人")

        self.assertEqual(inferred, {})

    def test_job_request_uses_company_inference_when_target_company_missing(self) -> None:
        request = JobRequest.from_payload({"raw_user_request": "帮我找出Anthropic的所有华人成员"})

        self.assertEqual(request.target_company, "Anthropic")
        self.assertEqual(request.keywords, ["Greater China experience", "Chinese bilingual outreach"])

    def test_builtin_company_identity_contains_humansand(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            self._write_seed_catalog(
                runtime_dir=runtime_dir,
                records=[
                    {
                        "company_key": "humansand",
                        "canonical_name": "Humans&",
                        "linkedin_slug": "humansand",
                        "aliases": ["humans and", "humansand"],
                        "confidence": "high",
                    }
                ],
            )
            with patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                company_key, builtin = builtin_company_identity("Humans&")

        self.assertEqual(company_key, "humansand")
        self.assertIsNotNone(builtin)
        assert builtin is not None
        self.assertEqual(builtin["linkedin_slug"], "humansand")

    def test_local_runtime_identity_supports_skildai_without_builtin(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            self._write_runtime_identity(
                runtime_dir=runtime_dir,
                company_key="skildai",
                snapshot_id="20260415T010101",
                requested_name="Skild AI",
                canonical_name="Skild AI",
                linkedin_slug="skildai",
                aliases=["skild ai", "skild"],
                confidence="medium",
            )
            with patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                inferred = infer_target_company_from_text("帮我找Skild AI做Pre-train方向的成员")
                company_key, builtin = builtin_company_identity("Skild")

            self.assertEqual(inferred["company_key"], "skildai")
            self.assertEqual(inferred["canonical_name"], "Skild AI")
            self.assertEqual(company_key, "skildai")
            self.assertIsNotNone(builtin)
            assert builtin is not None
            self.assertEqual(builtin["linkedin_slug"], "skildai")
            self.assertEqual(builtin["resolver"], "local_asset_identity")
            self.assertTrue(bool(builtin.get("local_asset_available")))

    def test_local_runtime_identity_supports_reflectionai_without_builtin(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            self._write_runtime_identity(
                runtime_dir=runtime_dir,
                company_key="reflectionai",
                snapshot_id="20260415T010102",
                requested_name="Reflection AI",
                canonical_name="Reflection AI",
                linkedin_slug="reflectionai",
                aliases=["reflection ai"],
            )
            with patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                inferred = infer_target_company_from_text("帮我找Reflection AI做Infra方向的人")
                company_key, builtin = builtin_company_identity("Reflection AI")

            self.assertEqual(inferred["company_key"], "reflectionai")
            self.assertEqual(inferred["canonical_name"], "Reflection AI")
            self.assertEqual(company_key, "reflectionai")
            self.assertIsNotNone(builtin)
            assert builtin is not None
            self.assertEqual(builtin["linkedin_slug"], "reflectionai")
            self.assertEqual(builtin["resolver"], "local_asset_identity")
            self.assertTrue(bool(builtin.get("local_asset_available")))

    def test_local_runtime_identity_supports_physical_intelligence_without_builtin(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            self._write_runtime_identity(
                runtime_dir=runtime_dir,
                company_key="physicalintelligence",
                snapshot_id="20260415T010203",
                requested_name="Physical Intelligence",
                canonical_name="Physical Intelligence",
                linkedin_slug="physical-intelligence",
                aliases=["physical intelligence", "pi"],
            )
            with patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                inferred = infer_target_company_from_text("帮我找Physical Intelligence做机器人方向的人")
                company_key, builtin = builtin_company_identity("Physical Intelligence")

            self.assertEqual(inferred["company_key"], "physicalintelligence")
            self.assertEqual(inferred["canonical_name"], "Physical Intelligence")
            self.assertEqual(company_key, "physicalintelligence")
            self.assertIsNotNone(builtin)
            assert builtin is not None
            self.assertEqual(builtin["linkedin_slug"], "physical-intelligence")
            self.assertEqual(builtin["resolver"], "local_asset_identity")
            self.assertTrue(bool(builtin.get("local_asset_available")))

    def test_refresh_company_identity_registry_keeps_seed_catalog_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            self._write_seed_catalog(
                runtime_dir=runtime_dir,
                records=[
                    {
                        "company_key": "humansand",
                        "canonical_name": "Humans&",
                        "linkedin_slug": "humansand",
                        "aliases": ["humans and", "humansand"],
                        "confidence": "high",
                    }
                ],
            )
            summary = refresh_company_identity_registry(runtime_dir)
            registry_payload = json.loads((runtime_dir / "company_identity_registry.json").read_text())

        self.assertEqual(summary["seed_company_count"], 1)
        self.assertEqual(registry_payload["company_count"], 1)
        self.assertEqual(registry_payload["records"][0]["company_key"], "humansand")

    def test_refresh_company_identity_registry_scans_hot_cache_company_assets(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            hot_cache_dir = runtime_dir / "hot_cache_company_assets"
            self._write_runtime_identity(
                runtime_dir=runtime_dir,
                assets_dir_name="hot_cache_company_assets",
                company_key="reflectionai",
                snapshot_id="20260418T010101",
                requested_name="Reflection AI",
                canonical_name="Reflection AI",
                linkedin_slug="reflectionai",
                aliases=["reflection ai"],
            )
            with patch.dict(os.environ, {"SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_dir)}, clear=False):
                summary = refresh_company_identity_registry(runtime_dir)
                registry_payload = json.loads((runtime_dir / "company_identity_registry.json").read_text())

        self.assertEqual(summary["scanned_identity_count"], 1)
        reflection_record = next(item for item in registry_payload["records"] if item["company_key"] == "reflectionai")
        self.assertGreaterEqual(summary["company_count"], 1)
        self.assertEqual(reflection_record["linkedin_slug"], "reflectionai")

    def test_cached_registry_records_do_not_hide_seed_catalog_identities(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            self._write_seed_catalog(
                runtime_dir=runtime_dir,
                records=[
                    {
                        "company_key": "humansand",
                        "canonical_name": "Humans&",
                        "linkedin_slug": "humansand",
                        "aliases": ["humans and", "humansand"],
                        "confidence": "high",
                    }
                ],
            )
            (runtime_dir / "company_identity_registry.json").write_text(
                json.dumps(
                    {
                        "updated_at": "2026-04-17T00:00:00+00:00",
                        "company_count": 1,
                        "alias_count": 0,
                        "records": [
                            {
                                "company_key": "skildai",
                                "canonical_name": "Skild AI",
                                "linkedin_slug": "skildai",
                                "aliases": ["skild ai"],
                                "resolver": "local_asset_identity",
                                "confidence": "medium",
                                "local_asset_available": True,
                            }
                        ],
                        "alias_index": {},
                    },
                    ensure_ascii=False,
                )
            )
            with patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                inferred = infer_target_company_from_text("我想了解 Humans& 的 Coding 方向 Researcher")

        self.assertEqual(inferred["company_key"], "humansand")
        self.assertEqual(inferred["canonical_name"], "Humans&")

    def test_local_runtime_identity_extends_company_inference(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime"
            self._write_runtime_identity(
                runtime_dir=runtime_dir,
                company_key="acmelabs",
                snapshot_id="20260414T010203",
                requested_name="Acme Labs",
                canonical_name="Acme Labs",
                linkedin_slug="acme-labs",
                aliases=["AcmeLab"],
                confidence="medium",
            )
            with patch.dict(os.environ, {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(runtime_dir)}):
                inferred = infer_target_company_from_text("帮我找Acme Labs做Infra方向的人")
                company_key, builtin = builtin_company_identity("AcmeLab")

            self.assertEqual(inferred["company_key"], "acmelabs")
            self.assertEqual(inferred["canonical_name"], "Acme Labs")
            self.assertEqual(company_key, "acmelabs")
            self.assertIsNotNone(builtin)
            assert builtin is not None
            self.assertEqual(builtin["linkedin_slug"], "acme-labs")
            self.assertTrue(bool(builtin.get("local_asset_available")))


if __name__ == "__main__":
    unittest.main()
