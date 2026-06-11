import json
import tempfile
import unittest
from pathlib import Path

from sourcing_agent.asset_registration import sync_company_asset_registration
from sourcing_agent.asset_reuse_audit import audit_authoritative_reuse_planning, summarize_authoritative_reuse_audit
from sourcing_agent.asset_reuse_planning import build_acquisition_shard_registry_record
from sourcing_agent.authoritative_serving_repair import repair_authoritative_serving_generation
from sourcing_agent.domain import Candidate
from sourcing_agent.storage import _build_asset_membership_row

from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class AuthoritativeServingRepairTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self.store = self.make_pg_store(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _candidate(self, candidate_id: str, name: str, *, status: str) -> dict:
        return Candidate(
            candidate_id=candidate_id,
            name_en=name,
            display_name=name,
            category="former_employee" if status == "former" else "employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status=status,
            role="Health Researcher",
            focus_areas="Health",
            linkedin_url=f"https://www.linkedin.com/in/{candidate_id}/",
        ).to_record()

    def _write_snapshot(self, snapshot_id: str, candidates: list[dict]) -> Path:
        snapshot_dir = self.runtime_dir / "company_assets" / "openai" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "snapshot": {
                "target_company": "OpenAI",
                "snapshot_id": snapshot_id,
                "company_identity": {
                    "requested_name": "OpenAI",
                    "canonical_name": "OpenAI",
                    "company_key": "openai",
                },
            },
            "candidates": candidates,
            "evidence": [],
        }
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(payload["snapshot"]["company_identity"], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (snapshot_dir.parent / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": payload["snapshot"]["company_identity"],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return snapshot_dir

    def _register_generation(
        self,
        *,
        snapshot_id: str,
        artifact_kind: str,
        artifact_key: str,
        candidates: list[dict],
        lane: str = "baseline",
        employment_scope: str = "",
    ) -> dict:
        return self.store.register_asset_materialization(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            asset_view="canonical_merged",
            artifact_kind=artifact_kind,
            artifact_key=artifact_key,
            summary={"candidate_count": len(candidates)},
            members=[
                _build_asset_membership_row(
                    target_company="OpenAI",
                    snapshot_id=snapshot_id,
                    asset_view="canonical_merged",
                    artifact_kind=artifact_kind,
                    artifact_key=artifact_key,
                    candidate_record=candidate,
                    lane=lane,
                    employment_scope=employment_scope or str(candidate.get("employment_status") or ""),
                )
                for candidate in candidates
            ],
        )

    def _seed_health_lag_fixture(self) -> str:
        snapshot_id = "20260430T090520"
        baseline_current = self._candidate("openai-health-current-1", "Health Current One", status="current")
        baseline_former = self._candidate("openai-health-former-1", "Health Former One", status="former")
        missing_current = self._candidate("openai-health-current-2", "Health Current Two", status="current")
        missing_former = self._candidate("openai-health-former-2", "Health Former Two", status="former")
        snapshot_dir = self._write_snapshot(snapshot_id, [baseline_current, baseline_former])
        baseline_generation = self._register_generation(
            snapshot_id=snapshot_id,
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
            candidates=[baseline_current, baseline_former],
        )
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": "openai",
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": 2,
                "profile_detail_count": 2,
                "current_lane_effective_candidate_count": 1,
                "former_lane_effective_candidate_count": 1,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "selected_snapshot_ids": [snapshot_id, "20260401T000000"],
                "source_snapshot_selection": {
                    "mode": "full_company_roster",
                    "selected_snapshot_ids": [snapshot_id, "20260401T000000"],
                    "population_coverage": {
                        "coverage_kind": "full_company_roster",
                        "coverage_status": "verified",
                        "full_company_coverage_proven": True,
                    },
                },
                "source_path": str(snapshot_dir / "candidate_documents.json"),
                "materialization_generation_key": str(baseline_generation.get("generation_key") or ""),
                "materialization_generation_sequence": int(baseline_generation.get("generation_sequence") or 0),
                "materialization_watermark": str(baseline_generation.get("generation_watermark") or ""),
                "summary": {
                    "candidate_count": 2,
                    "population_coverage": {
                        "coverage_kind": "full_company_roster",
                        "coverage_status": "verified",
                        "full_company_coverage_proven": True,
                    },
                },
            },
            authoritative=True,
        )
        raw_dir = snapshot_dir / "search_seed_discovery" / "harvest_profile_search"
        raw_dir.mkdir(parents=True, exist_ok=True)
        self._upsert_health_shard(
            snapshot_id=snapshot_id,
            employment_scope="current",
            candidates=[baseline_current, missing_current],
            raw_path=raw_dir / "health-current.json",
        )
        self._upsert_health_shard(
            snapshot_id=snapshot_id,
            employment_scope="former",
            candidates=[baseline_former, missing_former],
            raw_path=raw_dir / "health-former.json",
        )
        return snapshot_id

    def _seed_health_scope_mismatch_fixture(self) -> str:
        snapshot_id = "20260430T090520"
        misclassified_current = self._candidate(
            "openai-health-current-misclassified",
            "Health Current Misclassified",
            status="former",
        )
        snapshot_dir = self._write_snapshot(snapshot_id, [misclassified_current])
        baseline_generation = self._register_generation(
            snapshot_id=snapshot_id,
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
            candidates=[misclassified_current],
        )
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": "openai",
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": 1,
                "profile_detail_count": 1,
                "current_lane_effective_candidate_count": 0,
                "former_lane_effective_candidate_count": 1,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "selected_snapshot_ids": [snapshot_id],
                "source_snapshot_selection": {
                    "mode": "full_company_roster",
                    "selected_snapshot_ids": [snapshot_id],
                    "population_coverage": {
                        "coverage_kind": "full_company_roster",
                        "coverage_status": "verified",
                        "full_company_coverage_proven": True,
                    },
                },
                "source_path": str(snapshot_dir / "candidate_documents.json"),
                "materialization_generation_key": str(baseline_generation.get("generation_key") or ""),
                "materialization_generation_sequence": int(baseline_generation.get("generation_sequence") or 0),
                "materialization_watermark": str(baseline_generation.get("generation_watermark") or ""),
                "summary": {
                    "candidate_count": 1,
                    "population_coverage": {
                        "coverage_kind": "full_company_roster",
                        "coverage_status": "verified",
                        "full_company_coverage_proven": True,
                    },
                },
            },
            authoritative=True,
        )
        raw_dir = snapshot_dir / "search_seed_discovery" / "harvest_profile_search"
        raw_dir.mkdir(parents=True, exist_ok=True)
        self._upsert_health_shard(
            snapshot_id=snapshot_id,
            employment_scope="current",
            candidates=[misclassified_current],
            raw_path=raw_dir / "health-current.json",
        )
        return snapshot_id

    def _upsert_health_shard(
        self,
        *,
        snapshot_id: str,
        employment_scope: str,
        candidates: list[dict],
        raw_path: Path,
    ) -> None:
        raw_path.write_text(
            json.dumps(
                [
                    {
                        "full_name": candidate["display_name"],
                        "linkedin_url": candidate["linkedin_url"],
                        "headline": candidate["role"],
                    }
                    for candidate in candidates
                ],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        row = build_acquisition_shard_registry_record(
            target_company="OpenAI",
            company_key="openai",
            snapshot_id=snapshot_id,
            lane="profile_search",
            employment_scope=employment_scope,
            strategy_type="scoped_search_roster" if employment_scope == "current" else "former_employee_search",
            shard_id=f"health-{employment_scope}",
            shard_title="health",
            search_query="Health",
            company_filters={
                "companies": ["https://www.linkedin.com/company/openai/"],
                "function_ids": ["24", "8"],
                "search_query": "Health",
            },
            result_count=len(candidates),
            source_path=str(raw_path),
            status="completed",
        )
        generation = self._register_generation(
            snapshot_id=snapshot_id,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=str(row.get("shard_key") or ""),
            candidates=candidates,
            lane="profile_search",
            employment_scope=employment_scope,
        )
        row["materialization_generation_key"] = str(generation.get("generation_key") or "")
        row["materialization_generation_sequence"] = int(generation.get("generation_sequence") or 0)
        row["materialization_watermark"] = str(generation.get("generation_watermark") or "")
        self.store.upsert_acquisition_shard_registry(row)

    def test_repair_authoritative_serving_generation_dry_run_reports_same_snapshot_gap(self) -> None:
        snapshot_id = self._seed_health_lag_fixture()

        result = repair_authoritative_serving_generation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
            queries=["我想要OpenAI在health组的人"],
            repair_snapshot_id="repair-dry-run",
            apply=False,
        )

        self.assertEqual(result["status"], "dry_run")
        self.assertFalse(result["applied"])
        self.assertEqual(result["baseline_snapshot_id"], snapshot_id)
        self.assertEqual(result["gap_count"], 2)
        self.assertEqual(result["selected_shard_count"], 2)
        self.assertFalse((self.runtime_dir / "company_assets" / "openai" / "repair-dry-run").exists())

    def test_repair_authoritative_serving_generation_republishes_overlay_and_clears_gap(self) -> None:
        self._seed_health_lag_fixture()

        before = summarize_authoritative_reuse_audit(
            audit_authoritative_reuse_planning(
                runtime_dir=self.runtime_dir,
                store=self.store,
                company="OpenAI",
                query="我想要OpenAI在health组的人",
            ),
            case_id="before",
        )
        self.assertTrue(before["baseline_generation_lags_same_snapshot_shard_materialization"])

        result = repair_authoritative_serving_generation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
            queries=["我想要OpenAI在health组的人"],
            repair_snapshot_id="20260502T010203",
            apply=True,
        )

        self.assertEqual(result["status"], "repaired")
        self.assertTrue(result["applied"])
        self.assertTrue(all(item["baseline_subsumes_row"] for item in result["generation_checks"]))
        authoritative = self.store.get_authoritative_organization_asset_registry(
            target_company="OpenAI",
            asset_view="canonical_merged",
        )
        self.assertEqual(authoritative["snapshot_id"], "20260502T010203")
        self.assertIn("20260430T090520", authoritative["selected_snapshot_ids"])
        self.assertNotIn("20260401T000000", authoritative["selected_snapshot_ids"])
        repair_payload = json.loads(
            (
                self.runtime_dir
                / "company_assets"
                / "openai"
                / "20260502T010203"
                / "candidate_documents.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(len(repair_payload["candidates"]), 4)
        after = summarize_authoritative_reuse_audit(
            audit_authoritative_reuse_planning(
                runtime_dir=self.runtime_dir,
                store=self.store,
                company="OpenAI",
                query="我想要OpenAI在health组的人",
            ),
            case_id="after",
        )
        self.assertFalse(after["baseline_generation_lags_same_snapshot_shard_materialization"])
        self.assertEqual(after["missing_current_profile_search_query_count"], 0)
        self.assertEqual(after["missing_former_profile_search_query_count"], 0)

    def test_repair_authoritative_serving_generation_reports_scope_mismatch_as_quality_warning(self) -> None:
        self._seed_health_scope_mismatch_fixture()

        result = repair_authoritative_serving_generation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
            queries=["我想要OpenAI在health组的人"],
            repair_snapshot_id="20260502T020304",
            apply=True,
        )

        self.assertEqual(result["status"], "repaired_with_scope_mismatch")
        self.assertTrue(result["planner_lag_cleared"])
        self.assertTrue(result["generation_subsumes_selected_shards"])
        self.assertEqual(result["scope_mismatch_count"], 1)
        self.assertIn("repaired_generation_member_scope_mismatch", result["quality_warnings"])
        self.assertEqual(result["generation_checks"][0]["scope_coherence_status"], "member_present_with_scope_mismatch")

    def test_authoritative_publication_repairs_same_snapshot_shard_gap_before_promotion(self) -> None:
        snapshot_id = self._seed_health_lag_fixture()
        bad_authoritative = self.store.get_authoritative_organization_asset_registry(
            target_company="OpenAI",
            asset_view="canonical_merged",
        )

        result = sync_company_asset_registration(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            asset_view="canonical_merged",
            company_key="openai",
            registry_summary={
                **dict(bad_authoritative.get("summary") or {}),
                "candidate_count": bad_authoritative["candidate_count"],
                "profile_detail_count": bad_authoritative["profile_detail_count"],
                "current_lane_coverage": bad_authoritative["current_lane_coverage"],
                "former_lane_coverage": bad_authoritative["former_lane_coverage"],
                "source_snapshot_selection": bad_authoritative["source_snapshot_selection"],
                "selected_snapshot_ids": bad_authoritative["selected_snapshot_ids"],
                "materialization_generation_key": bad_authoritative["materialization_generation_key"],
                "materialization_generation_sequence": bad_authoritative["materialization_generation_sequence"],
                "materialization_watermark": bad_authoritative["materialization_watermark"],
            },
            source_path=str(self.runtime_dir / "company_assets" / "openai" / snapshot_id / "candidate_documents.json"),
            authoritative=True,
            serving_generation_repair_snapshot_id="20260503T010203",
        )

        sync_status = dict(result["sync_status"])
        publication_check = dict(sync_status["authoritative_serving_generation_publication_check"])
        self.assertEqual(publication_check["status"], "repaired")
        self.assertEqual(publication_check["baseline_snapshot_id"], snapshot_id)
        self.assertEqual(publication_check["repair_snapshot_id"], "20260503T010203")
        self.assertEqual(sync_status["organization_asset_registry_refresh"]["status"], "completed")

        authoritative = self.store.get_authoritative_organization_asset_registry(
            target_company="OpenAI",
            asset_view="canonical_merged",
        )
        self.assertEqual(authoritative["snapshot_id"], "20260503T010203")
        self.assertIn(snapshot_id, authoritative["selected_snapshot_ids"])
        self.assertNotIn("20260401T000000", authoritative["selected_snapshot_ids"])
        self.assertFalse(
            summarize_authoritative_reuse_audit(
                audit_authoritative_reuse_planning(
                    runtime_dir=self.runtime_dir,
                    store=self.store,
                    company="OpenAI",
                    query="我想要OpenAI在health组的人",
                ),
                case_id="after_publication",
            )["baseline_generation_lags_same_snapshot_shard_materialization"]
        )


if __name__ == "__main__":
    unittest.main()
