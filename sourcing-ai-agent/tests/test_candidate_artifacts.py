import json
import os
import tempfile
import time
import unittest
import unittest.mock
from collections import namedtuple
from pathlib import Path

from sourcing_agent.artifact_cache import (
    build_hot_cache_governance_policy,
    configured_hot_cache_retention_policy,
    load_hot_cache_governance_state,
    run_hot_cache_governance_cycle,
)
from sourcing_agent.authoritative_candidates import load_authoritative_candidate_snapshot
from sourcing_agent.candidate_artifacts import (
    CandidateArtifactError,
    backfill_structured_timeline_for_company_assets,
    build_company_candidate_artifacts,
    cleanup_candidate_artifact_hot_cache,
    load_company_snapshot_candidate_documents,
    materialize_company_candidate_view,
    repair_missing_company_candidate_artifacts,
    repair_projected_profile_signals_in_company_candidate_artifacts,
    rewrite_structured_timeline_in_company_candidate_artifacts,
)
from sourcing_agent.domain import Candidate, EvidenceRecord, make_evidence_id
from sourcing_agent.storage import ControlPlaneStore


class CandidateArtifactsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.tempdir.name)
        self.runtime_dir = self.project_root / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (self.runtime_dir / "company_assets" / "acme" / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260406T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                        "aliases": ["acme ai"],
                    },
                }
            )
        )
        self.store = ControlPlaneStore(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _load_artifact_view(
        self,
        *,
        target_company: str = "Acme",
        snapshot_id: str = "20260406T120000",
        view: str = "canonical_merged",
        allow_candidate_documents_fallback: bool | None = None,
    ) -> dict[str, object]:
        return load_company_snapshot_candidate_documents(
            runtime_dir=self.runtime_dir,
            target_company=target_company,
            snapshot_id=snapshot_id,
            view=view,
            allow_candidate_documents_fallback=allow_candidate_documents_fallback,
        )

    def _write_snapshot_candidate_documents(
        self,
        *,
        snapshot_id: str = "20260406T120000",
        candidates: list[Candidate] | None = None,
        evidence: list[object] | None = None,
        extra_payload: dict[str, object] | None = None,
    ) -> Path:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        payload = dict(extra_payload or {})
        payload["candidates"] = [candidate.to_record() for candidate in list(candidates or [])]
        payload["evidence"] = [
            item.to_record() if hasattr(item, "to_record") else dict(item) for item in list(evidence or [])
        ]
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return candidate_doc_path

    def _write_hot_cache_snapshot_view(
        self,
        *,
        hot_cache_root: Path,
        snapshot_id: str,
        generation_key: str,
        generation_sequence: int,
        candidate_id: str,
        target_company: str = "Acme",
        asset_view: str = "canonical_merged",
    ) -> Path:
        snapshot_dir = hot_cache_root / "acme" / snapshot_id
        artifact_dir = snapshot_dir / "normalized_artifacts"
        if asset_view != "canonical_merged":
            artifact_dir = artifact_dir / asset_view
        (artifact_dir / "candidate_shards").mkdir(parents=True, exist_ok=True)
        manifest_payload = {
            "target_company": target_company,
            "company_key": "acme",
            "snapshot_id": snapshot_id,
            "asset_view": asset_view,
            "candidate_count": 1,
            "candidate_shards": [
                {
                    "candidate_id": candidate_id,
                    "path": f"candidate_shards/{candidate_id}.json",
                }
            ],
            "pages": [],
            "backlogs": {},
        }
        artifact_summary = {
            "target_company": target_company,
            "company_key": "acme",
            "snapshot_id": snapshot_id,
            "asset_view": asset_view,
            "candidate_count": 1,
            "materialization_generation_key": generation_key,
            "materialization_generation_sequence": generation_sequence,
            "materialization_watermark": f"{generation_sequence}:{generation_key[:12]}",
        }
        snapshot_manifest = {
            "target_company": target_company,
            "company_key": "acme",
            "snapshot_id": snapshot_id,
            "asset_view": asset_view,
            "materialization_generation_key": generation_key,
            "materialization_generation_sequence": generation_sequence,
            "materialization_watermark": f"{generation_sequence}:{generation_key[:12]}",
        }
        (artifact_dir / "manifest.json").write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2))
        (artifact_dir / "artifact_summary.json").write_text(json.dumps(artifact_summary, ensure_ascii=False, indent=2))
        (artifact_dir / "snapshot_manifest.json").write_text(
            json.dumps(snapshot_manifest, ensure_ascii=False, indent=2)
        )
        (artifact_dir / "candidate_shards" / f"{candidate_id}.json").write_text(
            json.dumps({"candidate_id": candidate_id, "display_name": candidate_id}, ensure_ascii=False, indent=2)
        )
        identity_payload = {
            "requested_name": target_company,
            "canonical_name": target_company,
            "company_key": "acme",
            "linkedin_slug": "acme",
            "aliases": ["acme ai"],
        }
        (snapshot_dir / "identity.json").write_text(json.dumps(identity_payload, ensure_ascii=False, indent=2))
        (hot_cache_root / "acme" / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "snapshot_dir": str(snapshot_dir),
                    "company_identity": identity_payload,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return snapshot_dir

    def _write_generation_cache_dir(
        self,
        *,
        generation_root: Path,
        generation_key: str,
        snapshot_id: str,
        generation_sequence: int,
        asset_view: str = "canonical_merged",
    ) -> Path:
        generation_dir = generation_root / generation_key
        generation_dir.mkdir(parents=True, exist_ok=True)
        (generation_dir / "generation_manifest.json").write_text(
            json.dumps(
                {
                    "generation_key": generation_key,
                    "generation_sequence": generation_sequence,
                    "generation_watermark": f"{generation_sequence}:{generation_key[:12]}",
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": asset_view,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (generation_dir / "hydrate_summary.json").write_text(
            json.dumps({"generation_key": generation_key, "snapshot_id": snapshot_id}, ensure_ascii=False, indent=2)
        )
        return generation_dir

    def test_build_company_candidate_artifacts_materializes_backlog_and_reusable_docs(self) -> None:
        snapshot_candidate = Candidate(
            candidate_id="c0",
            name_en="Snapshot Current",
            display_name="Snapshot Current",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Member of Technical Staff",
            linkedin_url="https://www.linkedin.com/in/snapshot-current",
            source_dataset="acme_roster_snapshot",
        )
        snapshot_evidence = {
            "evidence_id": make_evidence_id(
                "c0", "acme_roster_snapshot", "Roster row", "https://www.linkedin.com/in/snapshot-current"
            ),
            "candidate_id": "c0",
            "source_type": "company_roster",
            "title": "Roster row",
            "url": "https://www.linkedin.com/in/snapshot-current",
            "summary": "Recovered from Acme current roster snapshot.",
            "source_dataset": "acme_roster_snapshot",
            "source_path": str(
                self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json"
            ),
            "metadata": {},
        }
        (self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [snapshot_candidate.to_record()],
                    "evidence": [snapshot_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        current = Candidate(
            candidate_id="c1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice",
            education="MIT",
            work_history="Acme",
            source_dataset="acme_roster",
        )
        lead = Candidate(
            candidate_id="c2",
            name_en="Bob Lead",
            display_name="Bob Lead",
            category="lead",
            target_company="Acme",
            employment_status="",
            role="Unknown",
            source_dataset="publication_feed",
        )
        roster_baseline = Candidate(
            candidate_id="c3",
            name_en="Carol Baseline",
            display_name="Carol Baseline",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Member of Technical Staff at Acme",
            work_history="Member of Technical Staff at Acme",
            notes="LinkedIn company roster baseline.",
            linkedin_url="https://www.linkedin.com/in/carol-baseline/",
            source_dataset="acme_linkedin_company_people",
        )
        profile_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "harvest_profiles"
        profile_dir.mkdir(parents=True, exist_ok=True)
        carol_profile_path = profile_dir / "carol-baseline.json"
        carol_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "headline": "Member of Technical Staff | Post-training at Acme",
                        "about": "Works on post-training systems and model evaluation.",
                        "skills": [
                            {"name": "PyTorch"},
                            {"name": "Model Evaluation"},
                        ],
                        "experience": [
                            {
                                "title": "Member of Technical Staff",
                                "companyName": "Acme",
                                "startDate": {"year": 2022},
                                "endDate": {"text": "Present"},
                            }
                        ],
                        "educations": [
                            {
                                "degreeName": "Bachelor",
                                "schoolName": "MIT",
                                "fieldOfStudy": "Computer Science",
                                "startDate": {"year": 2018},
                                "endDate": {"year": 2022},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.mark_linkedin_profile_registry_fetched(
            "https://www.linkedin.com/in/carol-baseline/",
            raw_path=str(carol_profile_path),
            snapshot_dir=str(self.runtime_dir / "company_assets" / "acme" / "20260406T120000"),
        )
        duplicate_alice = Candidate(
            candidate_id="c4",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Research Engineer",
            media_url="https://alice.example.com/about",
            source_dataset="rocketreach_seed",
        )
        alice_manual_review = EvidenceRecord(
            evidence_id=make_evidence_id("c1", "manual_review_link", "Homepage", "https://alice.example.com"),
            candidate_id="c1",
            source_type="manual_review_link",
            title="Homepage",
            url="https://alice.example.com",
            summary="Manual review confirmed Alice Example.",
            source_dataset="manual_review",
            source_path="/tmp/manual_review.json",
        )
        rocketreach_evidence = EvidenceRecord(
            evidence_id=make_evidence_id("c4", "rocketreach_profile", "RocketReach", "https://rocketreach.co/alice"),
            candidate_id="c4",
            source_type="rocketreach_profile",
            title="RocketReach",
            url="https://www.linkedin.com/in/alice/",
            summary="RocketReach found the same LinkedIn identity.",
            source_dataset="rocketreach_profile",
            source_path="/tmp/rocketreach.json",
            metadata={"profile_url": "https://www.linkedin.com/in/alice/", "public_identifier": "alice"},
        )
        self._write_snapshot_candidate_documents(
            candidates=[snapshot_candidate, current, lead, roster_baseline, duplicate_alice],
            evidence=[snapshot_evidence, alice_manual_review, rocketreach_evidence],
        )

        materialized_view = materialize_company_candidate_view(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(len(materialized_view["candidates"]), 4)
        self.assertEqual(len(materialized_view["source_snapshots"]), 1)

        result = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(result["status"], "built")
        summary = result["summary"]
        sync_status = result["sync_status"]
        self.assertEqual(summary["candidate_count"], 4)
        self.assertEqual(summary["source_snapshot_count"], 1)
        self.assertEqual(summary["manual_review_backlog_count"], 1)
        self.assertEqual(summary["profile_completion_backlog_count"], 1)
        self.assertEqual(summary["explicit_profile_capture_count"], 0)
        self.assertTrue(str(summary.get("materialization_generation_key") or ""))
        self.assertEqual(int(summary.get("materialization_generation_sequence") or 0), 1)
        self.assertEqual(int(dict(summary.get("membership_summary") or {}).get("member_count") or 0), 4)
        self.assertEqual(sync_status["organization_asset_registry_refresh"]["status"], "completed")
        self.assertEqual(sync_status["company_identity_registry_refresh"]["status"], "completed")
        self.assertEqual(sync_status["organization_execution_profile_refresh"]["status"], "completed")
        self.assertTrue((self.runtime_dir / "company_identity_registry.json").exists())
        authoritative = self.store.get_authoritative_organization_asset_registry(
            target_company="Acme",
            asset_view="canonical_merged",
        )
        self.assertEqual(
            str(authoritative.get("materialization_generation_key") or ""),
            str(summary.get("materialization_generation_key") or ""),
        )

        loaded = self._load_artifact_view()
        strict_loaded = self._load_artifact_view(view="strict_roster_only")
        materialized = dict(loaded["source_payload"])
        normalized = list(loaded["normalized_candidates"])
        reusable = list(loaded["reusable_documents"])
        backlog = list(loaded["manual_review_backlog"])
        profile_completion_backlog = list(loaded["profile_completion_backlog"])
        manifest = json.loads(Path(result["artifact_paths"]["manifest"]).read_text())
        snapshot_manifest = json.loads(Path(result["artifact_paths"]["snapshot_manifest"]).read_text())
        strict_summary = result["views"]["strict_roster_only"]["summary"]
        strict_normalized = list(strict_loaded["normalized_candidates"])
        self.assertEqual(len(normalized), 4)
        self.assertEqual(len(reusable), 4)
        self.assertEqual(len(backlog), 1)
        self.assertEqual(len(profile_completion_backlog), 1)
        self.assertEqual(manifest["candidate_count"], 4)
        self.assertEqual(len(manifest["candidate_shards"]), 4)
        self.assertEqual(manifest["pagination"]["page_count"], 1)
        self.assertEqual(summary["candidate_shard_count"], 4)
        self.assertEqual(summary["dirty_candidate_count"], 4)
        self.assertEqual(snapshot_manifest["snapshot_id"], "20260406T120000")
        self.assertEqual(snapshot_manifest["asset_view"], "canonical_merged")
        self.assertEqual(snapshot_manifest["materialization_generation_key"], summary["materialization_generation_key"])
        self.assertEqual(strict_summary["candidate_count"], 3)
        self.assertEqual(strict_summary["manual_review_backlog_count"], 0)
        self.assertEqual(
            str(materialized["snapshot"].get("materialization_generation_key") or ""),
            str(summary.get("materialization_generation_key") or ""),
        )
        self.assertTrue(str(strict_summary.get("materialization_generation_key") or ""))
        self.assertEqual(int(dict(strict_summary.get("membership_summary") or {}).get("member_count") or 0), 3)
        self.assertEqual(summary["structured_timeline_count"], 1)
        self.assertEqual(summary["structured_experience_count"], 1)
        self.assertEqual(summary["structured_education_count"], 1)
        self.assertEqual(summary["profile_detail_count"], 2)
        snapshot_current = next(item for item in normalized if item["candidate_id"] == "c0")
        alice = next(item for item in normalized if item["candidate_id"] == "c1")
        bob = next(item for item in normalized if item["candidate_id"] == "c2")
        carol = next(item for item in normalized if item["candidate_id"] == "c3")
        carol_materialized = next(item for item in materialized["candidates"] if item["candidate_id"] == "c3")
        carol_reusable = next(item for item in reusable if item["candidate_id"] == "c3")
        strict_ids = {item["candidate_id"] for item in strict_normalized}
        self.assertTrue(snapshot_current["has_linkedin_url"])
        self.assertTrue(snapshot_current["needs_profile_completion"])
        self.assertTrue(alice["manual_review_confirmed"])
        self.assertTrue(alice["has_profile_detail"])
        self.assertEqual(alice["role_bucket"], "research")
        self.assertIn("engineering", alice["functional_facets"])
        self.assertEqual(alice["evidence_count"], 2)
        self.assertIn("rocketreach_profile", alice["source_datasets"])
        self.assertTrue(bob["needs_manual_review"])
        self.assertEqual(bob["manual_review_reason"], "unresolved_lead")
        self.assertTrue(carol["has_profile_detail"])
        self.assertFalse(carol["needs_profile_completion"])
        self.assertFalse(carol["has_explicit_profile_capture"])
        self.assertEqual(carol["role_bucket"], "engineering")
        self.assertIn("training", carol["functional_facets"])
        self.assertIn("post_train", carol["functional_facets"])
        self.assertEqual(carol["experience_lines"], ["2022~Present, Acme, Member of Technical Staff"])
        self.assertEqual(carol["education_lines"], ["2018~2022, Bachelor, MIT, Computer Science"])
        self.assertEqual(carol["profile_timeline_source"], "profile_registry")
        self.assertEqual(carol_materialized["experience_lines"], ["2022~Present, Acme, Member of Technical Staff"])
        self.assertEqual(carol_materialized["education_lines"], ["2018~2022, Bachelor, MIT, Computer Science"])
        self.assertEqual(
            carol_materialized["metadata"]["headline"], "Member of Technical Staff | Post-training at Acme"
        )
        self.assertEqual(
            carol_materialized["metadata"]["about"], "Works on post-training systems and model evaluation."
        )
        self.assertEqual(carol_materialized["metadata"]["skills"], ["PyTorch", "Model Evaluation"])
        self.assertEqual(carol_reusable["experience_lines"], ["2022~Present, Acme, Member of Technical Staff"])
        self.assertEqual(carol_reusable["education_lines"], ["2018~2022, Bachelor, MIT, Computer Science"])
        self.assertIn("2022~Present, Acme, Member of Technical Staff", carol_reusable["profile_document"])
        self.assertIn("post-training at acme", carol_reusable["profile_document"].lower())
        self.assertNotIn("c2", strict_ids)
        states = {
            item["candidate_id"]: item
            for item in self.store.list_candidate_materialization_states(
                target_company="Acme",
                snapshot_id="20260406T120000",
                asset_view="canonical_merged",
            )
        }
        self.assertEqual(len(states), 4)
        self.assertEqual({item["company_key"] for item in states.values()}, {"acme"})
        alias_states = self.store.list_candidate_materialization_states(
            target_company="Acme!!!",
            snapshot_id="20260406T120000",
            asset_view="canonical_merged",
        )
        self.assertEqual(len(alias_states), 4)
        generation = self.store.get_asset_materialization_generation(
            target_company="Acme!!!",
            snapshot_id="20260406T120000",
            asset_view="canonical_merged",
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
        )
        self.assertEqual(generation["company_key"], "acme")
        self.assertEqual(
            str(generation.get("generation_key") or ""),
            str(summary.get("materialization_generation_key") or ""),
        )
        self.assertTrue(
            (
                self.runtime_dir
                / "company_assets"
                / "acme"
                / "20260406T120000"
                / "normalized_artifacts"
                / states["c3"]["shard_path"]
            ).exists()
        )

    def test_build_company_candidate_artifacts_foreground_fast_skips_compatibility_exports_and_hot_cache(self) -> None:
        candidate = Candidate(
            candidate_id="fast_1",
            name_en="Fast Candidate",
            display_name="Fast Candidate",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Engineer",
            linkedin_url="https://www.linkedin.com/in/fast-candidate/",
            source_dataset="acme_roster_snapshot",
        )
        self._write_snapshot_candidate_documents(
            candidates=[candidate],
            extra_payload={
                "snapshot": {
                    "snapshot_id": "20260406T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                }
            },
        )
        hot_cache_root = self.runtime_dir / "hot_cache_company_assets"
        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_WRITE_COMPATIBILITY_ARTIFACTS": "1",
                "SOURCING_CANONICAL_ASSETS_DIR": str(self.runtime_dir / "company_assets"),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            result = build_company_candidate_artifacts(
                runtime_dir=self.runtime_dir,
                store=self.store,
                target_company="Acme",
                snapshot_id="20260406T120000",
                build_profile="foreground_fast",
            )

        normalized_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "normalized_artifacts"
        strict_dir = normalized_dir / "strict_roster_only"
        self.assertEqual(result["build_profile"], "foreground_fast")
        self.assertEqual(result["hot_cache_sync"]["status"], "skipped")
        self.assertEqual(result["views"]["strict_roster_only"]["hot_cache_sync"]["status"], "skipped")
        self.assertEqual(result["sync_status"]["hot_cache_retention"]["status"], "skipped")
        self.assertTrue((normalized_dir / "manifest.json").exists())
        self.assertTrue((strict_dir / "manifest.json").exists())
        self.assertFalse((normalized_dir / "materialized_candidate_documents.json").exists())
        self.assertFalse((strict_dir / "materialized_candidate_documents.json").exists())
        self.assertFalse((hot_cache_root / "acme" / "20260406T120000").exists())

    def test_build_company_candidate_artifacts_aliases_strict_view_when_it_matches_canonical(self) -> None:
        candidates = [
            Candidate(
                candidate_id=f"alias_{index}",
                name_en=f"Alias Candidate {index}",
                display_name=f"Alias Candidate {index}",
                category="employee",
                target_company="Acme",
                employment_status="current",
                role="Engineer",
                linkedin_url=f"https://www.linkedin.com/in/alias-candidate-{index}/",
                source_dataset="acme_company_roster",
            )
            for index in range(3)
        ]
        evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    candidate.candidate_id,
                    "acme_company_roster",
                    f"Roster row {candidate.candidate_id}",
                    candidate.linkedin_url,
                ),
                candidate_id=candidate.candidate_id,
                source_type="company_roster",
                title=f"Roster row {candidate.candidate_id}",
                url=candidate.linkedin_url,
                summary=f"Evidence for {candidate.display_name}",
                source_dataset="acme_company_roster",
                source_path=f"synthetic/{candidate.candidate_id}",
            )
            for candidate in candidates
        ]
        self._write_snapshot_candidate_documents(candidates=candidates, evidence=evidence)

        result = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
            build_profile="foreground_fast",
        )

        strict_summary = dict(result["views"]["strict_roster_only"]["summary"] or {})
        strict_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "normalized_artifacts" / "strict_roster_only"
        self.assertEqual(strict_summary.get("storage_mode"), "alias")
        self.assertEqual(strict_summary.get("alias_asset_view"), "canonical_merged")
        self.assertEqual(int(strict_summary.get("state_upsert_candidate_count") or 0), 0)
        self.assertTrue((strict_dir / "manifest.json").exists())
        self.assertTrue((strict_dir / "artifact_summary.json").exists())
        self.assertFalse((strict_dir / "candidates").exists())

        loaded = self._load_artifact_view(view="strict_roster_only")
        self.assertEqual(len(list(loaded["normalized_candidates"])), 3)
        self.assertEqual(str(dict(loaded.get("artifact_summary") or {}).get("alias_asset_view") or ""), "canonical_merged")

    def test_build_company_candidate_artifacts_records_phase_timings(self) -> None:
        candidates = [
            Candidate(
                candidate_id=f"timed_{index}",
                name_en=f"Timed Candidate {index}",
                display_name=f"Timed Candidate {index}",
                category="employee",
                target_company="Acme",
                employment_status="current",
                role="Engineer",
                linkedin_url=f"https://www.linkedin.com/in/timed-candidate-{index}/",
                source_dataset="acme_roster_snapshot",
                metadata={"function_ids": ["research", "engineering"]},
            )
            for index in range(4)
        ]
        evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    candidate.candidate_id,
                    "acme_roster_snapshot",
                    f"Roster row {candidate.candidate_id}",
                    candidate.linkedin_url,
                ),
                candidate_id=candidate.candidate_id,
                source_type="company_roster",
                title=f"Roster row {candidate.candidate_id}",
                url=candidate.linkedin_url,
                summary=f"Evidence for {candidate.display_name}",
                source_dataset="acme_roster_snapshot",
                source_path=f"synthetic/{candidate.candidate_id}",
            )
            for candidate in candidates
        ]
        self._write_snapshot_candidate_documents(
            candidates=candidates,
            evidence=evidence,
            extra_payload={
                "snapshot": {
                    "snapshot_id": "20260406T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                }
            },
        )

        result = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
            snapshot_id="20260406T120000",
            build_profile="foreground_fast",
        )

        timings_ms = dict(result["summary"].get("timings_ms") or {})
        for key in (
            "profile_registry_lookup",
            "existing_state_load",
            "prepare_candidates",
            "assemble_candidate_outputs",
            "page_manifest_build",
            "payload_build_total",
            "shard_write",
            "page_write",
            "auxiliary_write",
            "state_upsert",
            "prune_states",
            "stale_cleanup",
            "view_write_total",
            "generation_register",
            "membership_summary",
            "snapshot_manifest_write",
            "artifact_summary_write",
            "compatibility_write",
            "finalize_total",
        ):
            self.assertIn(key, timings_ms)
            self.assertGreaterEqual(float(timings_ms[key]), 0.0)
        build_execution = dict(result["summary"].get("build_execution") or {})
        self.assertIn("parallel_workers", build_execution)
        self.assertIn("batch_json_writes_enabled", build_execution)
        self.assertIn("bulk_state_upsert_enabled", build_execution)

    def test_build_company_candidate_artifacts_legacy_like_benchmark_flags_disable_fast_paths(self) -> None:
        candidates = [
            Candidate(
                candidate_id=f"legacy_{index}",
                name_en=f"Legacy Candidate {index}",
                display_name=f"Legacy Candidate {index}",
                category="employee",
                target_company="Acme",
                employment_status="current",
                role="Engineer",
                linkedin_url=f"https://www.linkedin.com/in/legacy-candidate-{index}/",
                source_dataset="acme_roster_snapshot",
            )
            for index in range(32)
        ]
        self._write_snapshot_candidate_documents(
            candidates=candidates,
            extra_payload={
                "snapshot": {
                    "snapshot_id": "20260406T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                }
            },
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANDIDATE_ARTIFACT_FORCE_SERIAL": "1",
                "SOURCING_CANDIDATE_ARTIFACT_DISABLE_BATCH_JSON_WRITES": "1",
                "SOURCING_CANDIDATE_ARTIFACT_DISABLE_BULK_STATE_UPSERT": "1",
            },
            clear=False,
        ):
            result = build_company_candidate_artifacts(
                runtime_dir=self.runtime_dir,
                store=self.store,
                target_company="Acme",
                snapshot_id="20260406T120000",
                build_profile="foreground_fast",
            )

        build_execution = dict(result["summary"].get("build_execution") or {})
        self.assertEqual(build_execution["parallel_workers"], 1)
        self.assertFalse(build_execution["batch_json_writes_enabled"])
        self.assertFalse(build_execution["bulk_state_upsert_enabled"])

    def test_materialize_company_candidate_view_prefers_canonical_snapshot_for_write_path(self) -> None:
        canonical_root = self.project_root / "canonical_company_assets"
        hot_cache_root = self.project_root / "hot_cache_company_assets"
        canonical_snapshot_id = "20260406T120000"
        hot_cache_snapshot_id = "20260410T103000"
        canonical_snapshot_dir = canonical_root / "acme" / canonical_snapshot_id
        hot_cache_snapshot_dir = hot_cache_root / "acme" / hot_cache_snapshot_id
        canonical_snapshot_dir.mkdir(parents=True, exist_ok=True)
        hot_cache_snapshot_dir.mkdir(parents=True, exist_ok=True)
        company_identity = {
            "requested_name": "Acme",
            "canonical_name": "Acme",
            "company_key": "acme",
            "aliases": ["acme ai"],
        }
        (canonical_root / "acme" / "latest_snapshot.json").write_text(
            json.dumps(
                {"snapshot_id": canonical_snapshot_id, "company_identity": company_identity},
                ensure_ascii=False,
                indent=2,
            )
        )
        (hot_cache_root / "acme" / "latest_snapshot.json").write_text(
            json.dumps(
                {"snapshot_id": hot_cache_snapshot_id, "company_identity": company_identity},
                ensure_ascii=False,
                indent=2,
            )
        )
        (canonical_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": canonical_snapshot_id, "company_identity": company_identity},
                    "candidates": [
                        Candidate(
                            candidate_id="canonical_candidate",
                            name_en="Canonical Candidate",
                            display_name="Canonical Candidate",
                            category="employee",
                            target_company="Acme",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url="https://www.linkedin.com/in/canonical-candidate/",
                        ).to_record()
                    ],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (hot_cache_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": hot_cache_snapshot_id, "company_identity": company_identity},
                    "candidates": [
                        Candidate(
                            candidate_id="hot_cache_candidate",
                            name_en="Hot Cache Candidate",
                            display_name="Hot Cache Candidate",
                            category="employee",
                            target_company="Acme",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url="https://www.linkedin.com/in/hot-cache-candidate/",
                        ).to_record()
                    ],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            materialized_view = materialize_company_candidate_view(
                runtime_dir=self.runtime_dir,
                store=self.store,
                target_company="Acme",
            )

        self.assertEqual(materialized_view["snapshot_id"], canonical_snapshot_id)
        self.assertEqual(
            [candidate.candidate_id for candidate in materialized_view["candidates"]], ["canonical_candidate"]
        )

    def test_cleanup_candidate_artifact_hot_cache_removes_orphan_files_and_stale_states(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        candidate = Candidate(
            candidate_id="cleanup_1",
            name_en="Cleanup Candidate",
            display_name="Cleanup Candidate",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Engineer",
            linkedin_url="https://www.linkedin.com/in/cleanup-candidate/",
            source_dataset="acme_roster_snapshot",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260406T120000",
                        "company_identity": {
                            "requested_name": "Acme",
                            "canonical_name": "Acme",
                            "company_key": "acme",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
            snapshot_id="20260406T120000",
        )

        normalized_dir = self.runtime_dir / "hot_cache_company_assets" / "acme" / "20260406T120000" / "normalized_artifacts"
        orphan_shard = normalized_dir / "candidate_shards" / "orphan.json"
        orphan_page = normalized_dir / "pages" / "page-9999.json"
        compatibility_export = normalized_dir / "materialized_candidate_documents.json"
        orphan_shard.parent.mkdir(parents=True, exist_ok=True)
        orphan_page.parent.mkdir(parents=True, exist_ok=True)
        orphan_shard.write_text(json.dumps({"orphan": True}, ensure_ascii=False, indent=2), encoding="utf-8")
        orphan_page.write_text(json.dumps({"orphan": True}, ensure_ascii=False, indent=2), encoding="utf-8")
        compatibility_export.write_text(
            json.dumps({"snapshot": {}, "candidates": [], "evidence": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.store.upsert_candidate_materialization_state(
            target_company="Acme",
            snapshot_id="20260406T120000",
            asset_view="canonical_merged",
            candidate_id="orphan_state",
            fingerprint="fp_orphan_state",
            shard_path="candidate_shards/orphan-state.json",
            list_page=99,
        )

        dry_run = cleanup_candidate_artifact_hot_cache(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["Acme"],
            snapshot_id="20260406T120000",
            dry_run=True,
        )
        self.assertEqual(dry_run["status"], "completed")
        self.assertEqual(dry_run["orphan_file_count"], 3)
        self.assertEqual(dry_run["pruned_state_count"], 1)
        self.assertTrue(orphan_shard.exists())
        self.assertTrue(orphan_page.exists())
        self.assertTrue(compatibility_export.exists())

        cleanup = cleanup_candidate_artifact_hot_cache(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["Acme"],
            snapshot_id="20260406T120000",
            dry_run=False,
        )
        self.assertEqual(cleanup["status"], "completed")
        self.assertEqual(cleanup["deleted_file_count"], 3)
        self.assertEqual(cleanup["pruned_state_count"], 1)
        self.assertFalse(orphan_shard.exists())
        self.assertFalse(orphan_page.exists())
        self.assertFalse(compatibility_export.exists())
        state_ids = {
            item["candidate_id"]
            for item in self.store.list_candidate_materialization_states(
                target_company="Acme",
                snapshot_id="20260406T120000",
                asset_view="canonical_merged",
            )
        }
        self.assertNotIn("orphan_state", state_ids)
        self.assertIn("cleanup_1", state_ids)

    def test_cleanup_candidate_artifact_hot_cache_applies_ttl_retention_to_old_snapshots_and_generations(self) -> None:
        hot_cache_root = self.project_root / "hot_cache_company_assets"
        generation_root = self.runtime_dir / "object_sync" / "generations"
        old_snapshot_dir = self._write_hot_cache_snapshot_view(
            hot_cache_root=hot_cache_root,
            snapshot_id="20260405T120000",
            generation_key="gen-old",
            generation_sequence=1,
            candidate_id="old_candidate",
        )
        new_snapshot_dir = self._write_hot_cache_snapshot_view(
            hot_cache_root=hot_cache_root,
            snapshot_id="20260406T120000",
            generation_key="gen-new",
            generation_sequence=2,
            candidate_id="new_candidate",
        )
        old_generation_dir = self._write_generation_cache_dir(
            generation_root=generation_root,
            generation_key="gen-old",
            snapshot_id="20260405T120000",
            generation_sequence=1,
        )
        new_generation_dir = self._write_generation_cache_dir(
            generation_root=generation_root,
            generation_key="gen-new",
            snapshot_id="20260406T120000",
            generation_sequence=2,
        )
        stale_epoch = time.time() - 7200
        for root in [old_snapshot_dir, old_generation_dir]:
            for path in [root, *root.rglob("*")]:
                os.utime(path, (stale_epoch, stale_epoch))

        self.store.upsert_candidate_materialization_state(
            target_company="Acme",
            snapshot_id="20260405T120000",
            asset_view="canonical_merged",
            candidate_id="old_candidate",
            fingerprint="fp-old",
            shard_path="candidate_shards/old_candidate.json",
            list_page=1,
        )
        self.store.upsert_candidate_materialization_state(
            target_company="Acme",
            snapshot_id="20260406T120000",
            asset_view="canonical_merged",
            candidate_id="new_candidate",
            fingerprint="fp-new",
            shard_path="candidate_shards/new_candidate.json",
            list_page=1,
        )

        with unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root)},
            clear=False,
        ):
            cleanup = cleanup_candidate_artifact_hot_cache(
                runtime_dir=self.runtime_dir,
                store=self.store,
                companies=["Acme"],
                dry_run=False,
                ttl_seconds=3600,
            )

        retention = dict(cleanup.get("retention") or {})
        self.assertEqual(retention.get("status"), "completed")
        self.assertEqual(retention.get("planned_snapshot_eviction_count"), 1)
        self.assertEqual(retention.get("planned_generation_eviction_count"), 1)
        self.assertFalse(old_snapshot_dir.exists())
        self.assertFalse(old_generation_dir.exists())
        self.assertTrue(new_snapshot_dir.exists())
        self.assertTrue(new_generation_dir.exists())
        latest_payload = json.loads((hot_cache_root / "acme" / "latest_snapshot.json").read_text(encoding="utf-8"))
        self.assertEqual(latest_payload["snapshot_id"], "20260406T120000")
        retained_states = self.store.list_candidate_materialization_states(
            target_company="Acme",
            snapshot_id="20260406T120000",
            asset_view="canonical_merged",
        )
        evicted_states = self.store.list_candidate_materialization_states(
            target_company="Acme",
            snapshot_id="20260405T120000",
            asset_view="canonical_merged",
        )
        self.assertEqual([item["candidate_id"] for item in retained_states], ["new_candidate"])
        self.assertEqual(evicted_states, [])

    def test_cleanup_candidate_artifact_hot_cache_applies_size_budget_retention(self) -> None:
        hot_cache_root = self.project_root / "hot_cache_company_assets"
        generation_root = self.runtime_dir / "object_sync" / "generations"
        old_snapshot_dir = self._write_hot_cache_snapshot_view(
            hot_cache_root=hot_cache_root,
            snapshot_id="20260405T120000",
            generation_key="gen-budget-old",
            generation_sequence=1,
            candidate_id="old_budget_candidate",
        )
        new_snapshot_dir = self._write_hot_cache_snapshot_view(
            hot_cache_root=hot_cache_root,
            snapshot_id="20260406T120000",
            generation_key="gen-budget-new",
            generation_sequence=2,
            candidate_id="new_budget_candidate",
        )
        old_generation_dir = self._write_generation_cache_dir(
            generation_root=generation_root,
            generation_key="gen-budget-old",
            snapshot_id="20260405T120000",
            generation_sequence=1,
        )
        new_generation_dir = self._write_generation_cache_dir(
            generation_root=generation_root,
            generation_key="gen-budget-new",
            snapshot_id="20260406T120000",
            generation_sequence=2,
        )
        budget_floor = sum(path.stat().st_size for path in new_snapshot_dir.rglob("*") if path.is_file()) + sum(
            path.stat().st_size for path in new_generation_dir.rglob("*") if path.is_file()
        )

        with unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root)},
            clear=False,
        ):
            cleanup = cleanup_candidate_artifact_hot_cache(
                runtime_dir=self.runtime_dir,
                store=self.store,
                companies=["Acme"],
                dry_run=False,
                size_budget_bytes=budget_floor,
                keep_latest_snapshots_per_company=1,
            )

        retention = dict(cleanup.get("retention") or {})
        self.assertEqual(retention.get("status"), "completed")
        self.assertGreaterEqual(int(retention.get("planned_bytes_to_free") or 0), 1)
        self.assertEqual(retention.get("planned_snapshot_eviction_count"), 1)
        self.assertEqual(retention.get("planned_generation_eviction_count"), 1)
        self.assertFalse(old_snapshot_dir.exists())
        self.assertFalse(old_generation_dir.exists())
        self.assertTrue(new_snapshot_dir.exists())
        self.assertTrue(new_generation_dir.exists())

    def test_cleanup_candidate_artifact_hot_cache_applies_per_company_budget_retention(self) -> None:
        hot_cache_root = self.project_root / "hot_cache_company_assets"
        old_snapshot_dir = self._write_hot_cache_snapshot_view(
            hot_cache_root=hot_cache_root,
            snapshot_id="20260405T120000",
            generation_key="gen-company-budget-old",
            generation_sequence=1,
            candidate_id="old_company_budget_candidate",
        )
        new_snapshot_dir = self._write_hot_cache_snapshot_view(
            hot_cache_root=hot_cache_root,
            snapshot_id="20260406T120000",
            generation_key="gen-company-budget-new",
            generation_sequence=2,
            candidate_id="new_company_budget_candidate",
        )
        new_snapshot_bytes = sum(path.stat().st_size for path in new_snapshot_dir.rglob("*") if path.is_file())

        with unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root)},
            clear=False,
        ):
            cleanup = cleanup_candidate_artifact_hot_cache(
                runtime_dir=self.runtime_dir,
                store=self.store,
                companies=["Acme"],
                dry_run=False,
                max_bytes_per_company=new_snapshot_bytes + 1,
                keep_latest_snapshots_per_company=1,
            )

        retention = dict(cleanup.get("retention") or {})
        self.assertEqual(retention.get("status"), "completed")
        self.assertEqual(int(retention.get("planned_snapshot_eviction_count") or 0), 1)
        self.assertFalse(old_snapshot_dir.exists())
        self.assertTrue(new_snapshot_dir.exists())

    def test_cleanup_candidate_artifact_hot_cache_compacts_superseded_generations_per_scope(self) -> None:
        hot_cache_root = self.project_root / "hot_cache_company_assets"
        generation_root = self.runtime_dir / "object_sync" / "generations"
        self._write_hot_cache_snapshot_view(
            hot_cache_root=hot_cache_root,
            snapshot_id="20260406T120000",
            generation_key="gen-compact-new",
            generation_sequence=2,
            candidate_id="compact_candidate",
        )
        old_generation_dir = self._write_generation_cache_dir(
            generation_root=generation_root,
            generation_key="gen-compact-old",
            snapshot_id="20260406T120000",
            generation_sequence=1,
        )
        new_generation_dir = self._write_generation_cache_dir(
            generation_root=generation_root,
            generation_key="gen-compact-new",
            snapshot_id="20260406T120000",
            generation_sequence=2,
        )

        with unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root)},
            clear=False,
        ):
            cleanup = cleanup_candidate_artifact_hot_cache(
                runtime_dir=self.runtime_dir,
                store=self.store,
                companies=["Acme"],
                dry_run=False,
                max_generations_per_scope=1,
            )

        retention = dict(cleanup.get("retention") or {})
        self.assertEqual(retention.get("status"), "completed")
        self.assertEqual(int(retention.get("planned_generation_eviction_count") or 0), 1)
        self.assertFalse(old_generation_dir.exists())
        self.assertTrue(new_generation_dir.exists())

    def test_configured_hot_cache_retention_policy_computes_auto_target_budget_when_disk_is_healthy(self) -> None:
        hot_cache_root = self.project_root / "hot_cache_company_assets"
        hot_cache_root.mkdir(parents=True, exist_ok=True)
        disk_usage = namedtuple("DiskUsage", "total used free")(10_000, 9_000, 1_000)

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
                "SOURCING_HOT_CACHE_MIN_FREE_RATIO": "0",
                "SOURCING_HOT_CACHE_MIN_FREE_BYTES": "800",
                "SOURCING_HOT_CACHE_TARGET_BUDGET_RATIO": "0.1",
                "SOURCING_HOT_CACHE_TARGET_BUDGET_FLOOR_BYTES": "400",
                "SOURCING_HOT_CACHE_TARGET_BUDGET_CAP_BYTES": "900",
            },
            clear=False,
        ):
            with unittest.mock.patch("sourcing_agent.artifact_cache.shutil.disk_usage", return_value=disk_usage):
                policy = configured_hot_cache_retention_policy(
                    runtime_dir=self.runtime_dir,
                    inventory={"status": "completed", "total_bytes": 700, "company_records": []},
                )

        self.assertEqual(str(policy.get("size_budget_source") or ""), "auto_target_budget")
        self.assertEqual(int(policy.get("effective_size_budget_bytes") or 0), 900)
        self.assertEqual(int(policy.get("free_space_reserve_bytes") or 0), 800)
        self.assertEqual(int(policy.get("auto_target_budget_bytes") or 0), 900)

    def test_configured_hot_cache_retention_policy_computes_auto_budget_under_disk_pressure(self) -> None:
        hot_cache_root = self.project_root / "hot_cache_company_assets"
        hot_cache_root.mkdir(parents=True, exist_ok=True)
        disk_usage = namedtuple("DiskUsage", "total used free")(10_000, 9_500, 500)

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
                "SOURCING_HOT_CACHE_MIN_FREE_RATIO": "0",
                "SOURCING_HOT_CACHE_MIN_FREE_BYTES": "800",
                "SOURCING_HOT_CACHE_TARGET_BUDGET_RATIO": "0.1",
                "SOURCING_HOT_CACHE_TARGET_BUDGET_FLOOR_BYTES": "400",
                "SOURCING_HOT_CACHE_TARGET_BUDGET_CAP_BYTES": "900",
            },
            clear=False,
        ):
            with unittest.mock.patch("sourcing_agent.artifact_cache.shutil.disk_usage", return_value=disk_usage):
                policy = configured_hot_cache_retention_policy(
                    runtime_dir=self.runtime_dir,
                    inventory={"status": "completed", "total_bytes": 700},
                )

        self.assertEqual(str(policy.get("size_budget_source") or ""), "auto_target_budget_under_pressure")
        self.assertEqual(int(policy.get("free_space_deficit_bytes") or 0), 300)
        self.assertEqual(int(policy.get("effective_size_budget_bytes") or 0), 400)

    def test_run_hot_cache_governance_cycle_persists_state_and_self_tunes_under_pressure(self) -> None:
        hot_cache_root = self.project_root / "hot_cache_company_assets"
        self._write_hot_cache_snapshot_view(
            hot_cache_root=hot_cache_root,
            snapshot_id="20260409T090000",
            generation_key="gen-acme-old",
            generation_sequence=1,
            candidate_id="cand-old",
        )
        disk_usage = namedtuple("DiskUsage", "total used free")(10_000, 9_700, 300)

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
                "SOURCING_HOT_CACHE_MIN_FREE_RATIO": "0",
                "SOURCING_HOT_CACHE_MIN_FREE_BYTES": "800",
            },
            clear=False,
        ):
            with unittest.mock.patch("sourcing_agent.artifact_cache.shutil.disk_usage", return_value=disk_usage):
                policy = build_hot_cache_governance_policy(runtime_dir=self.runtime_dir)
                first = run_hot_cache_governance_cycle(
                    runtime_dir=self.runtime_dir,
                    store=self.store,
                    force=True,
                )
                second = run_hot_cache_governance_cycle(
                    runtime_dir=self.runtime_dir,
                    store=self.store,
                )

        self.assertEqual(str(policy.get("pressure_level") or ""), "severe")
        self.assertGreater(int(policy.get("effective_ttl_seconds") or 0), 0)
        self.assertGreater(int(policy.get("governance_interval_seconds") or 0), 0)
        self.assertEqual(first["status"], "completed")
        self.assertEqual(second["status"], "skipped")
        self.assertEqual(second["reason"], "throttled")
        governance_state = load_hot_cache_governance_state(self.runtime_dir)
        self.assertEqual(str(governance_state.get("status") or ""), "skipped")
        self.assertEqual(
            str(dict(governance_state.get("last_success_summary") or {}).get("status") or ""),
            "completed",
        )

    def test_build_company_candidate_artifacts_reuses_candidate_shards_and_rebuilds_only_dirty_candidates(self) -> None:
        candidate_one = Candidate(
            candidate_id="c10",
            name_en="Candidate One",
            display_name="Candidate One",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/candidate-one/",
            source_dataset="acme_roster",
        )
        candidate_two = Candidate(
            candidate_id="c20",
            name_en="Candidate Two",
            display_name="Candidate Two",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Research Scientist",
            linkedin_url="https://www.linkedin.com/in/candidate-two/",
            source_dataset="acme_roster",
        )
        self._write_snapshot_candidate_documents(candidates=[candidate_one, candidate_two])

        first_result = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(first_result["summary"]["dirty_candidate_count"], 2)
        self.assertEqual(first_result["summary"]["reused_candidate_count"], 0)
        self.assertEqual(first_result["summary"]["state_upsert_candidate_count"], 2)

        first_states = {
            item["candidate_id"]: item
            for item in self.store.list_candidate_materialization_states(
                target_company="Acme",
                snapshot_id="20260406T120000",
                asset_view="canonical_merged",
            )
        }
        self.assertEqual(set(first_states), {"c10", "c20"})
        first_manifest = json.loads(Path(first_result["artifact_paths"]["manifest"]).read_text(encoding="utf-8"))
        self.assertEqual(first_manifest["candidate_count"], 2)
        self.assertEqual(len(first_manifest["candidate_shards"]), 2)

        second_result = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(second_result["summary"]["dirty_candidate_count"], 0)
        self.assertEqual(second_result["summary"]["reused_candidate_count"], 2)
        self.assertEqual(second_result["summary"]["state_upsert_candidate_count"], 0)
        second_states = {
            item["candidate_id"]: item
            for item in self.store.list_candidate_materialization_states(
                target_company="Acme",
                snapshot_id="20260406T120000",
                asset_view="canonical_merged",
            )
        }
        self.assertEqual(
            {candidate_id: state["fingerprint"] for candidate_id, state in second_states.items()},
            {candidate_id: state["fingerprint"] for candidate_id, state in first_states.items()},
        )

        old_candidate_one_shard = (
            self.runtime_dir
            / "company_assets"
            / "acme"
            / "20260406T120000"
            / "normalized_artifacts"
            / second_states["c10"]["shard_path"]
        )
        self.assertTrue(old_candidate_one_shard.exists())

        updated_candidate_one = Candidate(
            candidate_id="c10",
            name_en="Candidate One",
            display_name="Candidate One",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Staff Research Engineer",
            linkedin_url="https://www.linkedin.com/in/candidate-one/",
            source_dataset="acme_roster",
        )
        self._write_snapshot_candidate_documents(candidates=[updated_candidate_one, candidate_two])
        third_result = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(third_result["summary"]["dirty_candidate_count"], 1)
        self.assertEqual(third_result["summary"]["reused_candidate_count"], 1)
        self.assertEqual(third_result["summary"]["state_upsert_candidate_count"], 1)
        third_states = {
            item["candidate_id"]: item
            for item in self.store.list_candidate_materialization_states(
                target_company="Acme",
                snapshot_id="20260406T120000",
                asset_view="canonical_merged",
            )
        }
        self.assertNotEqual(third_states["c10"]["fingerprint"], second_states["c10"]["fingerprint"])
        self.assertEqual(third_states["c20"]["fingerprint"], second_states["c20"]["fingerprint"])
        self.assertFalse(old_candidate_one_shard.exists())

    def test_build_company_candidate_artifacts_uses_scope_replace_when_all_states_are_dirty(self) -> None:
        candidate_one = Candidate(
            candidate_id="c10",
            name_en="Candidate One",
            display_name="Candidate One",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/candidate-one/",
            source_dataset="acme_roster",
        )
        candidate_two = Candidate(
            candidate_id="c20",
            name_en="Candidate Two",
            display_name="Candidate Two",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Research Scientist",
            linkedin_url="https://www.linkedin.com/in/candidate-two/",
            source_dataset="acme_roster",
        )
        self._write_snapshot_candidate_documents(candidates=[candidate_one, candidate_two])

        with unittest.mock.patch.object(
            self.store,
            "replace_candidate_materialization_state_scope",
            wraps=self.store.replace_candidate_materialization_state_scope,
        ) as replace_scope, unittest.mock.patch.object(
            self.store,
            "prune_candidate_materialization_states",
            wraps=self.store.prune_candidate_materialization_states,
        ) as _prune_scope:
            result = build_company_candidate_artifacts(
                runtime_dir=self.runtime_dir,
                store=self.store,
                target_company="Acme",
            )

        self.assertEqual(result["summary"]["state_upsert_candidate_count"], 2)
        self.assertTrue(result["summary"]["build_execution"]["full_scope_state_replace_eligible"])
        replace_scope.assert_called_once()
        self.assertEqual(str(replace_scope.call_args.kwargs.get("asset_view") or ""), "canonical_merged")

    def test_build_company_candidate_artifacts_surfaces_suspicious_membership_reviews(self) -> None:
        suspicious = Candidate(
            candidate_id="c5",
            name_en="Suspicious Example",
            display_name="Suspicious Example",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/suspicious-example/",
            source_dataset="acme_roster",
            metadata={
                "membership_review_required": True,
                "membership_review_reason": "suspicious_membership",
                "membership_review_decision": "suspicious_member",
                "membership_review_rationale": "Profile content does not look like a plausible Acme employee profile.",
                "membership_review_triggers": ["suspicious_profile_content"],
                "membership_review_trigger_keywords": ["spiritual", "healer"],
            },
        )
        suspicious_evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                "c5", "linkedin_profile_detail", "Profile", "https://www.linkedin.com/in/suspicious-example/"
            ),
            candidate_id="c5",
            source_type="linkedin_profile_detail",
            title="Profile",
            url="https://www.linkedin.com/in/suspicious-example/",
            summary="LinkedIn profile detail captured for suspicious review.",
            source_dataset="linkedin_profile_detail",
            source_path="/tmp/profile.json",
        )
        self._write_snapshot_candidate_documents(candidates=[suspicious], evidence=[suspicious_evidence])

        build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        loaded = self._load_artifact_view()
        normalized = list(loaded["normalized_candidates"])
        backlog = list(loaded["manual_review_backlog"])
        suspicious_normalized = next(item for item in normalized if item["candidate_id"] == "c5")
        suspicious_backlog = next(item for item in backlog if item["candidate_id"] == "c5")
        self.assertEqual(suspicious_normalized["status_bucket"], "lead")
        self.assertTrue(suspicious_normalized["needs_manual_review"])
        self.assertEqual(suspicious_normalized["manual_review_reason"], "suspicious_membership")
        self.assertEqual(suspicious_normalized["membership_review_decision"], "suspicious_member")
        self.assertEqual(suspicious_backlog["reason"], "suspicious_membership")

    def test_build_company_candidate_artifacts_persists_profile_signals_from_registry_timeline(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        roster_dir = snapshot_dir / "harvest_company_employees"
        profile_dir = snapshot_dir / "harvest_profiles"
        roster_dir.mkdir(parents=True, exist_ok=True)
        profile_dir.mkdir(parents=True, exist_ok=True)
        visible_path = roster_dir / "harvest_company_employees_visible.json"
        visible_path.write_text(
            json.dumps(
                {
                    "items": [
                        {
                            "linkedin_url": "https://www.linkedin.com/in/signal-candidate/",
                            "profilePicture": {
                                "url": "https://cdn.example.com/roster-signal-candidate.jpg",
                            },
                            "experience": [
                                {
                                    "title": "Member of Technical Staff",
                                    "companyName": "Acme",
                                    "startDate": {"year": 2024},
                                    "endDate": {"text": "Present"},
                                }
                            ],
                        }
                    ]
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        raw_profile_path = profile_dir / "signal-candidate.json"
        raw_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "profilePicture": {
                            "url": "https://cdn.example.com/signal-candidate.jpg",
                        },
                        "emails": [
                            {
                                "email": "signal.candidate@acme.com",
                                "foundInLinkedInProfile": True,
                            }
                        ],
                        "headline": "Member of Technical Staff | Post-training at Acme",
                        "experience": [
                            {
                                "title": "Member of Technical Staff",
                                "companyName": "Acme",
                                "startDate": {"year": 2024},
                                "endDate": {"text": "Present"},
                            }
                        ],
                        "educations": [
                            {
                                "degreeName": "Bachelor",
                                "schoolName": "MIT",
                                "fieldOfStudy": "Computer Science",
                                "startDate": {"year": 2020},
                                "endDate": {"year": 2024},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate = Candidate(
            candidate_id="signal_candidate",
            name_en="Signal Candidate",
            display_name="Signal Candidate",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Member of Technical Staff at Acme",
            work_history="Member of Technical Staff at Acme",
            notes="LinkedIn company roster baseline.",
            linkedin_url="https://www.linkedin.com/in/signal-candidate/",
            source_dataset="acme_linkedin_company_people",
            source_path=str(visible_path),
        )
        self._write_snapshot_candidate_documents(
            candidates=[candidate],
            extra_payload={
                "snapshot": {
                    "snapshot_id": "20260406T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                }
            },
        )
        self.store.mark_linkedin_profile_registry_fetched(
            "https://www.linkedin.com/in/signal-candidate/",
            raw_path=str(raw_profile_path),
            snapshot_dir=str(snapshot_dir),
        )

        build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
            snapshot_id="20260406T120000",
        )

        materialized_payload = dict(self._load_artifact_view()["source_payload"])
        candidate_record = next(
            item
            for item in list(materialized_payload.get("candidates") or [])
            if item.get("candidate_id") == "signal_candidate"
        )
        self.assertEqual(
            candidate_record.get("experience_lines"),
            ["2024~Present, Acme, Member of Technical Staff"],
        )
        self.assertEqual(
            candidate_record.get("education_lines"),
            ["2020~2024, Bachelor, MIT, Computer Science"],
        )
        self.assertEqual(candidate_record.get("media_url"), "https://cdn.example.com/signal-candidate.jpg")
        self.assertEqual(candidate_record.get("avatar_url"), "https://cdn.example.com/signal-candidate.jpg")
        self.assertEqual(candidate_record.get("primary_email"), "signal.candidate@acme.com")
        self.assertTrue(dict(candidate_record.get("primary_email_metadata") or {}).get("foundInLinkedInProfile"))
        self.assertEqual(
            dict(candidate_record.get("metadata") or {}).get("avatar_url"),
            "https://cdn.example.com/signal-candidate.jpg",
        )
        self.assertEqual(
            dict(candidate_record.get("metadata") or {}).get("primary_email"),
            "signal.candidate@acme.com",
        )
        self.assertTrue(
            dict(dict(candidate_record.get("metadata") or {}).get("primary_email_metadata") or {}).get(
                "foundInLinkedInProfile"
            )
        )
        self.assertTrue(candidate_record.get("has_profile_detail"))
        self.assertFalse(candidate_record.get("needs_profile_completion"))

        loaded = load_company_snapshot_candidate_documents(
            runtime_dir=self.runtime_dir,
            target_company="Acme",
            snapshot_id="20260406T120000",
        )
        loaded_candidate = next(
            item for item in list(loaded.get("candidates") or []) if item.candidate_id == "signal_candidate"
        )
        self.assertEqual(
            str(dict(loaded_candidate.metadata or {}).get("avatar_url") or ""),
            "https://cdn.example.com/signal-candidate.jpg",
        )
        self.assertEqual(
            str(dict(loaded_candidate.metadata or {}).get("primary_email") or ""),
            "signal.candidate@acme.com",
        )
        self.assertTrue(
            bool(
                dict(dict(loaded_candidate.metadata or {}).get("primary_email_metadata") or {}).get(
                    "foundInLinkedInProfile"
                )
            )
        )

    def test_build_company_candidate_artifacts_keeps_search_seed_preview_in_completion_backlog(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        search_dir = discovery_dir / "harvest_profile_search"
        search_dir.mkdir(parents=True, exist_ok=True)

        raw_path = search_dir / "preview.json"
        raw_path.write_text(
            json.dumps(
                [
                    {
                        "id": "preview-member",
                        "linkedinUrl": "https://www.linkedin.com/in/preview-candidate/",
                        "fullName": "Preview Candidate",
                        "headline": "Researcher at Acme",
                        "photoUrl": "https://cdn.example.com/preview.jpg",
                        "currentPosition": [
                            {
                                "title": "Researcher",
                                "companyName": "Acme",
                                "startDate": {"year": 2025},
                                "current": True,
                            }
                        ],
                    }
                ],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        summary_path = discovery_dir / "summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "query_summaries": [
                        {
                            "mode": "harvest_profile_search",
                            "query": "Reasoning",
                            "effective_query_text": "Reasoning",
                            "raw_path": str(raw_path),
                        }
                    ]
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate = Candidate(
            candidate_id="c_preview",
            name_en="Preview Candidate",
            display_name="Preview Candidate",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Researcher",
            linkedin_url="https://www.linkedin.com/in/preview-candidate/",
            source_dataset="harvest_profile_search",
            source_path=str(summary_path),
            metadata={
                "source_path": str(summary_path),
                "seed_source_type": "harvest_profile_search",
                "seed_query": "Reasoning",
            },
        )
        self._write_snapshot_candidate_documents(candidates=[candidate])

        result = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
            snapshot_id="20260406T120000",
        )

        summary = result["summary"]
        loaded = self._load_artifact_view()
        normalized = list(loaded["normalized_candidates"])
        materialized = dict(loaded["source_payload"])
        candidate_record = next(item for item in normalized if item["candidate_id"] == "c_preview")
        materialized_record = next(item for item in materialized["candidates"] if item["candidate_id"] == "c_preview")

        self.assertEqual(summary["candidate_count"], 1)
        self.assertEqual(summary["profile_detail_count"], 0)
        self.assertEqual(summary["profile_completion_backlog_count"], 1)
        self.assertEqual(summary["structured_experience_count"], 1)
        self.assertFalse(candidate_record["has_profile_detail"])
        self.assertTrue(candidate_record["needs_profile_completion"])
        self.assertEqual(candidate_record["profile_capture_kind"], "search_seed_preview")
        self.assertEqual(candidate_record["experience_lines"], ["2025~Present, Acme, Researcher"])
        self.assertEqual(materialized_record["profile_capture_kind"], "search_seed_preview")
        self.assertEqual(
            dict(materialized_record.get("metadata") or {}).get("profile_capture_kind"),
            "search_seed_preview",
        )

    def test_build_company_candidate_artifacts_keeps_sparse_provider_profile_detail_in_completion_backlog(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        profile_dir = snapshot_dir / "harvest_profiles"
        profile_dir.mkdir(parents=True, exist_ok=True)
        sparse_profile_path = profile_dir / "sparse-provider-detail.json"
        sparse_profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "headline": "Research Engineer at Acme",
                        "experience": [],
                        "education": [],
                        "currentPosition": [],
                        "profileTopEducation": [],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        candidate = Candidate(
            candidate_id="c_sparse_detail",
            name_en="Sparse Detail Candidate",
            display_name="Sparse Detail Candidate",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/sparse-detail-candidate/",
            source_dataset="acme_roster",
            source_path=str(sparse_profile_path),
            metadata={
                "profile_capture_kind": "provider_profile_detail",
                "profile_capture_source_path": str(sparse_profile_path),
                "source_path": str(sparse_profile_path),
            },
        )
        sparse_evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                "c_sparse_detail",
                "linkedin_profile_detail",
                "Sparse provider detail",
                "https://www.linkedin.com/in/sparse-detail-candidate/",
            ),
            candidate_id="c_sparse_detail",
            source_type="linkedin_profile_detail",
            title="Sparse provider detail",
            url="https://www.linkedin.com/in/sparse-detail-candidate/",
            summary="LinkedIn profile detail was captured, but the provider returned no structured timeline.",
            source_dataset="linkedin_profile_detail",
            source_path=str(sparse_profile_path),
        )
        self._write_snapshot_candidate_documents(candidates=[candidate], evidence=[sparse_evidence])

        result = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
            snapshot_id="20260406T120000",
        )

        summary = result["summary"]
        loaded = self._load_artifact_view()
        normalized = list(loaded["normalized_candidates"])
        profile_completion_backlog = list(loaded["profile_completion_backlog"])
        candidate_record = next(item for item in normalized if item["candidate_id"] == "c_sparse_detail")

        self.assertEqual(summary["profile_detail_count"], 0)
        self.assertEqual(summary["explicit_profile_capture_count"], 1)
        self.assertEqual(summary["profile_completion_backlog_count"], 1)
        self.assertFalse(candidate_record["has_profile_detail"])
        self.assertTrue(candidate_record["has_explicit_profile_capture"])
        self.assertTrue(candidate_record["needs_profile_completion"])
        self.assertEqual(candidate_record["profile_capture_kind"], "provider_profile_detail")
        self.assertEqual(
            [item["candidate_id"] for item in profile_completion_backlog],
            ["c_sparse_detail"],
        )

    def test_rewrite_structured_timeline_does_not_promote_bundle_function_filters_to_candidate_function_ids(
        self,
    ) -> None:
        company_dir = self.runtime_dir / "company_assets" / "acme"
        current_snapshot_dir = company_dir / "20260406T120000"
        prior_snapshot_dir = company_dir / "20260405T120000"
        prior_snapshot_dir.mkdir(parents=True, exist_ok=True)

        candidate = Candidate(
            candidate_id="c_function_backfill",
            name_en="Function Backfill",
            display_name="Function Backfill",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Scientist",
            linkedin_url="https://www.linkedin.com/in/function-backfill/",
            source_dataset="acme_linkedin_company_people",
        )
        (prior_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260405T120000",
                        "company_identity": {
                            "requested_name": "Acme",
                            "canonical_name": "Acme",
                            "company_key": "acme",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        prior_bundle_dir = prior_snapshot_dir / "normalized_artifacts" / "acquisition_shard_bundles" / "research"
        prior_bundle_dir.mkdir(parents=True, exist_ok=True)
        (prior_snapshot_dir / "normalized_artifacts" / "acquisition_shard_bundles" / "manifest.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "snapshot_id": "20260405T120000",
                    "asset_view": "canonical_merged",
                    "entries": [
                        {
                            "shard_key": "research",
                            "bundle_path": str(prior_bundle_dir / "bundle.json"),
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (prior_bundle_dir / "bundle.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "target_company": "Acme",
                        "snapshot_id": "20260405T120000",
                        "asset_view": "canonical_merged",
                    },
                    "shard": {
                        "shard_key": "research",
                        "function_ids": ["24"],
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        (current_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260406T120000",
                        "company_identity": {
                            "requested_name": "Acme",
                            "canonical_name": "Acme",
                            "company_key": "acme",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
            snapshot_id="20260406T120000",
        )

        rewrite_result = rewrite_structured_timeline_in_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["Acme"],
            snapshot_id="20260406T120000",
            refresh_registry=False,
        )

        self.assertEqual(rewrite_result["status"], "completed")
        loaded_payload = self._load_artifact_view()
        materialized_payload = dict(loaded_payload["source_payload"])
        normalized_payload = list(loaded_payload["normalized_candidates"])
        materialized_candidate = next(
            item
            for item in list(materialized_payload.get("candidates") or [])
            if item.get("candidate_id") == "c_function_backfill"
        )
        normalized_candidate = next(
            item for item in list(normalized_payload or []) if item.get("candidate_id") == "c_function_backfill"
        )

        self.assertIsNone(materialized_candidate.get("function_ids"))
        self.assertIsNone(dict(materialized_candidate.get("metadata") or {}).get("function_ids"))
        self.assertEqual(normalized_candidate.get("function_ids"), [])
        self.assertEqual(
            int(self._load_artifact_view().get("artifact_summary", {}).get("function_id_candidate_count") or 0),
            0,
        )

    def test_build_company_candidate_artifacts_reports_sync_failures(self) -> None:
        snapshot_candidate = Candidate(
            candidate_id="sync-c1",
            name_en="Sync Example",
            display_name="Sync Example",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Engineer",
            linkedin_url="https://www.linkedin.com/in/sync-example/",
            source_dataset="acme_roster_snapshot",
        )
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "target_company": "Acme",
                        "snapshot_id": "20260406T120000",
                    },
                    "candidates": [snapshot_candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        with unittest.mock.patch(
            "sourcing_agent.organization_assets.ensure_acquisition_shard_bundles_for_snapshot",
            side_effect=RuntimeError("bundle sync boom"),
        ):
            result = build_company_candidate_artifacts(
                runtime_dir=self.runtime_dir,
                store=self.store,
                target_company="Acme",
            )

        self.assertEqual(result["status"], "built")
        sync_status = dict(result.get("sync_status") or {})
        self.assertEqual(sync_status.get("overall_status"), "partial_failure")
        self.assertEqual(
            dict(sync_status.get("acquisition_shard_bundle_refresh") or {}).get("status"),
            "failed",
        )
        self.assertIn(
            "bundle sync boom",
            str(dict(sync_status.get("acquisition_shard_bundle_refresh") or {}).get("error") or ""),
        )

    def test_load_company_snapshot_candidate_documents_normalizes_investor_roles(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "normalized_artifacts"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        strict_dir = snapshot_dir / "strict_roster_only"
        strict_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "candidates": [
                Candidate(
                    candidate_id="investor_1",
                    name_en="Ivy Investor",
                    display_name="Ivy Investor",
                    category="employee",
                    target_company="Acme",
                    organization="Acme",
                    employment_status="current",
                    role="Investor at Acme",
                    focus_areas="Investor at Acme | Seed investor",
                    source_dataset="acme_materialized",
                ).to_record(),
                Candidate(
                    candidate_id="other_1",
                    name_en="Other Company",
                    display_name="Other Company",
                    category="employee",
                    target_company="OtherCo",
                    organization="OtherCo",
                    employment_status="current",
                    role="Engineer",
                    source_dataset="other_materialized",
                ).to_record(),
            ],
            "evidence": [],
        }
        (snapshot_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2)
        )
        (strict_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2)
        )

        result = load_company_snapshot_candidate_documents(
            runtime_dir=self.runtime_dir,
            target_company="Acme",
        )
        self.assertEqual(result["snapshot_id"], "20260406T120000")
        self.assertEqual(len(result["candidates"]), 1)
        self.assertEqual(result["candidates"][0].display_name, "Ivy Investor")
        self.assertEqual(result["candidates"][0].category, "investor")
        self.assertEqual(result["candidates"][0].employment_status, "current")

        strict_result = load_company_snapshot_candidate_documents(
            runtime_dir=self.runtime_dir,
            target_company="Acme",
            view="strict_roster_only",
        )
        self.assertEqual(strict_result["asset_view"], "strict_roster_only")
        self.assertEqual(len(strict_result["candidates"]), 1)

    def test_load_company_snapshot_candidate_documents_matches_snapshot_via_identity_names(self) -> None:
        company_dir = self.runtime_dir / "company_assets" / "humansand"
        snapshot_dir = company_dir / "20260408T204924"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260408T204924",
                    "company_identity": {
                        "requested_name": "Humans&",
                        "canonical_name": "humans&",
                        "company_key": "humansand",
                        "linkedin_slug": "humansand",
                        "aliases": ["humansand"],
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(
                {
                    "requested_name": "Humans&",
                    "canonical_name": "humans&",
                    "company_key": "humansand",
                    "linkedin_slug": "humansand",
                    "aliases": ["humansand"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        candidate = Candidate(
            candidate_id="humansand_1",
            name_en="Jeremy Berman",
            display_name="Jeremy Berman",
            category="employee",
            target_company="humans&",
            organization="humans&",
            employment_status="current",
            role="Member of Technical Staff",
            source_dataset="humansand_roster",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260408T204924",
                        "company_identity": {
                            "requested_name": "Humans&",
                            "canonical_name": "humans&",
                            "company_key": "humansand",
                            "linkedin_slug": "humansand",
                            "aliases": ["humansand"],
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        with self.assertRaises(CandidateArtifactError):
            load_company_snapshot_candidate_documents(
                runtime_dir=self.runtime_dir,
                target_company="Humans&",
                snapshot_id="20260408T204924",
            )

        result = load_company_snapshot_candidate_documents(
            runtime_dir=self.runtime_dir,
            target_company="Humans&",
            snapshot_id="20260408T204924",
            allow_candidate_documents_fallback=True,
        )

        self.assertEqual(result["company_key"], "humansand")
        self.assertEqual(result["snapshot_id"], "20260408T204924")
        self.assertEqual(result["source_kind"], "candidate_documents")
        self.assertEqual(len(result["candidates"]), 1)
        self.assertEqual(result["candidates"][0].display_name, "Jeremy Berman")

    def test_load_company_snapshot_candidate_documents_prefers_hot_cache_snapshot_when_configured(self) -> None:
        snapshot_id = "20260408T204924"
        canonical_root = self.project_root / "canonical_assets"
        hot_cache_root = self.project_root / "hot_cache_assets"
        canonical_snapshot_dir = canonical_root / "acme" / snapshot_id
        hot_cache_snapshot_dir = hot_cache_root / "acme" / snapshot_id
        canonical_snapshot_dir.mkdir(parents=True, exist_ok=True)
        hot_cache_snapshot_dir.mkdir(parents=True, exist_ok=True)
        latest_payload = {
            "snapshot_id": snapshot_id,
            "company_identity": {
                "requested_name": "Acme",
                "canonical_name": "Acme",
                "company_key": "acme",
                "aliases": ["acme ai"],
            },
        }
        for company_dir in (canonical_root / "acme", hot_cache_root / "acme"):
            company_dir.mkdir(parents=True, exist_ok=True)
            (company_dir / "latest_snapshot.json").write_text(
                json.dumps(latest_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        (canonical_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        Candidate(
                            candidate_id="canonical_1",
                            name_en="Canonical Lead",
                            display_name="Canonical Lead",
                            category="employee",
                            target_company="Acme",
                            employment_status="current",
                            role="Research Engineer",
                        ).to_record()
                    ],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (hot_cache_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        Candidate(
                            candidate_id="hot_cache_1",
                            name_en="Hot Cache Lead",
                            display_name="Hot Cache Lead",
                            category="employee",
                            target_company="Acme",
                            employment_status="current",
                            role="Research Engineer",
                        ).to_record()
                    ],
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
            result = load_company_snapshot_candidate_documents(
                runtime_dir=self.runtime_dir,
                target_company="Acme",
                snapshot_id=snapshot_id,
                allow_candidate_documents_fallback=True,
            )

        self.assertEqual(result["snapshot_id"], snapshot_id)
        self.assertEqual(result["candidates"][0].display_name, "Hot Cache Lead")
        self.assertEqual(Path(result["source_path"]).parent.resolve(), hot_cache_snapshot_dir.resolve())

    def test_load_company_snapshot_candidate_documents_without_snapshot_id_discovers_hot_cache_only_company(
        self,
    ) -> None:
        snapshot_id = "20260409T101500"
        canonical_root = self.project_root / "canonical_assets"
        hot_cache_root = self.project_root / "hot_cache_assets"
        unrelated_canonical_snapshot_dir = canonical_root / "otherco" / "20260401T090000"
        unrelated_canonical_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (canonical_root / "otherco" / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": "20260401T090000"}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        hot_cache_company_dir = hot_cache_root / "acme"
        hot_cache_snapshot_dir = hot_cache_company_dir / snapshot_id
        hot_cache_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (hot_cache_company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                        "aliases": ["acme ai"],
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (hot_cache_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        Candidate(
                            candidate_id="hot_cache_only_1",
                            name_en="Hot Cache Only Lead",
                            display_name="Hot Cache Only Lead",
                            category="employee",
                            target_company="Acme",
                            employment_status="current",
                            role="Research Engineer",
                        ).to_record()
                    ],
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
            result = load_company_snapshot_candidate_documents(
                runtime_dir=self.runtime_dir,
                target_company="Acme",
                allow_candidate_documents_fallback=True,
            )

        self.assertEqual(result["snapshot_id"], snapshot_id)
        self.assertEqual(result["candidates"][0].display_name, "Hot Cache Only Lead")
        self.assertEqual(Path(result["source_path"]).parent.resolve(), hot_cache_snapshot_dir.resolve())

    def test_load_company_snapshot_candidate_documents_without_snapshot_id_keeps_canonical_latest_pointer(self) -> None:
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
            json.dumps(
                {
                    "snapshot_id": canonical_snapshot_id,
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                        "aliases": ["acme ai"],
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (hot_cache_company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": hot_cache_newer_snapshot_id,
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                        "aliases": ["acme ai"],
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (canonical_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (hot_cache_canonical_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        Candidate(
                            candidate_id="hot_cache_canonical_pointer_1",
                            name_en="Hot Cache Canonical Pointer Lead",
                            display_name="Hot Cache Canonical Pointer Lead",
                            category="employee",
                            target_company="Acme",
                            employment_status="current",
                            role="Research Engineer",
                        ).to_record()
                    ],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (hot_cache_newer_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        Candidate(
                            candidate_id="hot_cache_newer_1",
                            name_en="Hot Cache Newer Lead",
                            display_name="Hot Cache Newer Lead",
                            category="employee",
                            target_company="Acme",
                            employment_status="current",
                            role="Research Engineer",
                        ).to_record()
                    ],
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
            result = load_company_snapshot_candidate_documents(
                runtime_dir=self.runtime_dir,
                target_company="Acme",
                allow_candidate_documents_fallback=True,
            )

        self.assertEqual(result["snapshot_id"], canonical_snapshot_id)
        self.assertEqual(result["candidates"][0].display_name, "Hot Cache Canonical Pointer Lead")
        self.assertEqual(
            Path(result["source_path"]).parent.resolve(),
            hot_cache_canonical_snapshot_dir.resolve(),
        )

    def test_load_company_snapshot_candidate_documents_matches_alias_via_shared_identity_resolver(self) -> None:
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
        (canonical_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (hot_cache_canonical_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        Candidate(
                            candidate_id="ssi_alias_1",
                            name_en="SSI Alias Lead",
                            display_name="SSI Alias Lead",
                            category="employee",
                            target_company="Safe Superintelligence Inc",
                            employment_status="current",
                            role="Research Engineer",
                        ).to_record()
                    ],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (hot_cache_newer_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False, indent=2),
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
            result = load_company_snapshot_candidate_documents(
                runtime_dir=self.runtime_dir,
                target_company="Safe Superintelligence",
                allow_candidate_documents_fallback=True,
            )

        self.assertEqual(result["snapshot_id"], canonical_snapshot_id)
        self.assertEqual(result["company_key"], "ssiai")
        self.assertEqual(result["candidates"][0].display_name, "SSI Alias Lead")
        self.assertEqual(
            Path(result["source_path"]).parent.resolve(),
            hot_cache_canonical_snapshot_dir.resolve(),
        )

    def test_load_authoritative_candidate_snapshot_materializes_from_root_candidate_documents(self) -> None:
        candidate = Candidate(
            candidate_id="cand_root_only",
            name_en="Snapshot Only",
            display_name="Snapshot Only",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Researcher",
            linkedin_url="https://www.linkedin.com/in/snapshot-only/",
        )
        evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                candidate.candidate_id,
                "linkedin_profile_detail",
                "Snapshot Only",
                "https://www.linkedin.com/in/snapshot-only/",
            ),
            candidate_id=candidate.candidate_id,
            source_type="linkedin_profile_detail",
            title="Snapshot Only",
            url="https://www.linkedin.com/in/snapshot-only/",
            summary="Profile captured from root candidate documents only.",
            source_dataset="linkedin_profile_detail",
            source_path=str(
                self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json"
            ),
            metadata={"headline": "Researcher at Acme"},
        )
        self._write_snapshot_candidate_documents(candidates=[candidate], evidence=[evidence])

        snapshot = load_authoritative_candidate_snapshot(
            runtime_dir=str(self.runtime_dir),
            target_company="Acme",
            snapshot_id="20260406T120000",
            store=self.store,
            allow_candidate_documents_fallback=True,
        )

        self.assertEqual(snapshot.snapshot_id, "20260406T120000")
        self.assertEqual(len(snapshot.candidates), 1)
        self.assertEqual(snapshot.candidates[0].candidate_id, "cand_root_only")
        self.assertEqual(len(snapshot.evidence_records), 1)
        self.assertNotEqual(snapshot.source_kind, "candidate_documents")
        self.assertTrue(snapshot.source_path.endswith("manifest.json"), snapshot.source_path)
        self.assertTrue(
            (
                self.runtime_dir
                / "company_assets"
                / "acme"
                / "20260406T120000"
                / "normalized_artifacts"
                / "manifest.json"
            ).exists()
        )

    def test_materialized_view_canonicalizes_historical_snapshot_duplicates(self) -> None:
        previous_snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T110000"
        previous_snapshot_dir.mkdir(parents=True, exist_ok=True)
        previous_snapshot_candidate = Candidate(
            candidate_id="lead_kevin",
            name_en="Kevin Example",
            display_name="Kevin Example",
            category="lead",
            target_company="Acme",
            organization="Acme",
            employment_status="",
            role="Publication author lead",
            source_dataset="publication_match",
        )
        previous_snapshot_evidence = {
            "evidence_id": make_evidence_id("lead_kevin", "publication_match", "Paper", "https://example.com/paper"),
            "candidate_id": "lead_kevin",
            "source_type": "publication_match",
            "title": "Paper",
            "url": "https://example.com/paper",
            "summary": "Lead from publication.",
            "source_dataset": "publication_match",
            "source_path": str(previous_snapshot_dir / "candidate_documents.json"),
            "metadata": {},
        }
        (previous_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [previous_snapshot_candidate.to_record()],
                    "evidence": [previous_snapshot_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        current_snapshot_candidate = Candidate(
            candidate_id="emp_kevin",
            name_en="Kevin Example",
            display_name="Kevin Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/kevin-example/",
            education="MIT",
            work_history="Acme",
            source_dataset="acme_linkedin_company_people",
            metadata={"public_identifier": "kevin-example"},
        )
        current_snapshot_evidence = {
            "evidence_id": make_evidence_id(
                "emp_kevin",
                "linkedin_profile_detail",
                "Research Engineer",
                "https://www.linkedin.com/in/kevin-example/",
            ),
            "candidate_id": "emp_kevin",
            "source_type": "linkedin_profile_detail",
            "title": "Research Engineer",
            "url": "https://www.linkedin.com/in/kevin-example/",
            "summary": "Profile detail for Kevin Example.",
            "source_dataset": "linkedin_profile_detail",
            "source_path": "/tmp/kevin-example.json",
        }
        (self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [current_snapshot_candidate.to_record()],
                    "evidence": [current_snapshot_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        materialized_view = materialize_company_candidate_view(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(len(materialized_view["candidates"]), 1)
        self.assertEqual(materialized_view["candidates"][0].candidate_id, "emp_kevin")
        self.assertEqual(materialized_view["canonicalization"]["name_merge_count"], 1)
        source_types = {item["source_type"] for item in materialized_view["evidence"]}
        self.assertIn("publication_match", source_types)
        self.assertIn("linkedin_profile_detail", source_types)

    def test_materialized_view_preserves_manual_non_member_resolution(self) -> None:
        snapshot_candidate = Candidate(
            candidate_id="seed_rabia",
            name_en="Rabia Example",
            display_name="Rabia Example",
            category="former_employee",
            target_company="Acme",
            organization="Acme",
            employment_status="former",
            role="OpenAI at Acme",
            linkedin_url="https://www.linkedin.com/in/rabia-example/",
            source_dataset="acme_search_seed_candidates",
        )
        snapshot_evidence = {
            "evidence_id": make_evidence_id(
                "seed_rabia",
                "acme_search_seed_candidates",
                "Search seed",
                "https://www.linkedin.com/in/rabia-example/",
            ),
            "candidate_id": "seed_rabia",
            "source_type": "harvest_profile_search",
            "title": "Search seed",
            "url": "https://www.linkedin.com/in/rabia-example/",
            "summary": "Suspicious search-seed candidate.",
            "source_dataset": "acme_search_seed_candidates",
            "source_path": str(
                self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json"
            ),
            "metadata": {},
        }
        (self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [snapshot_candidate.to_record()],
                    "evidence": [snapshot_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        previous_snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T110000"
        previous_snapshot_dir.mkdir(parents=True, exist_ok=True)
        previous_snapshot_candidate = Candidate(
            candidate_id="manual_rabia",
            name_en="Rabia Example",
            display_name="Rabia Example",
            category="non_member",
            target_company="Acme",
            organization="Other Org",
            employment_status="",
            role="Data Analyst",
            linkedin_url="https://www.linkedin.com/in/rabia-example/",
            source_dataset="acme_search_seed_candidates",
            source_path="/tmp/manual_review/rabia.json",
            metadata={
                "manual_review_artifact_root": "/tmp/manual_review/rabia",
                "manual_review_links": [{"label": "LinkedIn", "url": "https://www.linkedin.com/in/rabia-example/"}],
                "target_company_mismatch": True,
                "membership_review_required": False,
                "membership_review_decision": "manual_non_member",
            },
        )
        previous_snapshot_evidence = {
            "evidence_id": make_evidence_id(
                "manual_rabia",
                "manual_review",
                "LinkedIn",
                "https://www.linkedin.com/in/rabia-example/",
            ),
            "candidate_id": "manual_rabia",
            "source_type": "manual_review_link",
            "title": "LinkedIn",
            "url": "https://www.linkedin.com/in/rabia-example/",
            "summary": "Manual review rejected this profile as unrelated to Acme.",
            "source_dataset": "manual_review",
            "source_path": "/tmp/manual_review/rabia_source.json",
        }
        (previous_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [previous_snapshot_candidate.to_record()],
                    "evidence": [previous_snapshot_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        materialized_view = materialize_company_candidate_view(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(len(materialized_view["candidates"]), 1)
        candidate = materialized_view["candidates"][0]
        self.assertEqual(candidate.category, "non_member")
        self.assertEqual(candidate.organization, "Other Org")
        self.assertEqual(candidate.metadata.get("membership_review_decision"), "manual_non_member")
        self.assertTrue(candidate.metadata.get("target_company_mismatch"))

    def test_materialized_view_prefers_profile_enriched_current_duplicate_over_raw_former_seed(self) -> None:
        snapshot_candidate = Candidate(
            candidate_id="seed_deanna",
            name_en="Deanna Graham",
            display_name="Deanna Graham",
            category="former_employee",
            target_company="Acme",
            organization="Acme",
            employment_status="former",
            role="Head of Marketing Insights & Research at Acme",
            linkedin_url="https://www.linkedin.com/in/ACwAAAAmToEBWFfDWPTIqJTWLTI_dvQ-qmyXPGw",
            source_dataset="acme_search_seed_candidates",
            metadata={"seed_slug": "ACwAAAAmToEBWFfDWPTIqJTWLTI_dvQ-qmyXPGw"},
        )
        (self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [snapshot_candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        previous_snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T110000"
        previous_snapshot_dir.mkdir(parents=True, exist_ok=True)
        previous_snapshot_candidate = Candidate(
            candidate_id="profile_deanna",
            name_en="Deanna Graham",
            display_name="Deanna Graham",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Head of Marketing Insights & Research at Acme",
            linkedin_url="https://www.linkedin.com/in/deannagraham2023",
            source_dataset="acme_search_seed_candidates",
            source_path="/tmp/deanna-profile.json",
            metadata={
                "seed_slug": "ACwAAAAmToEBWFfDWPTIqJTWLTI_dvQ-qmyXPGw",
                "profile_url": "https://www.linkedin.com/in/ACwAAAAmToEBWFfDWPTIqJTWLTI_dvQ-qmyXPGw",
                "public_identifier": "deannagraham2023",
                "membership_claim_category": "employee",
                "membership_claim_employment_status": "current",
            },
        )
        (previous_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [previous_snapshot_candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        materialized_view = materialize_company_candidate_view(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(len(materialized_view["candidates"]), 1)
        candidate = materialized_view["candidates"][0]
        self.assertEqual(candidate.category, "employee")
        self.assertEqual(candidate.employment_status, "current")
        self.assertEqual(candidate.linkedin_url, "https://www.linkedin.com/in/deannagraham2023")

    def test_materialized_view_limits_large_org_history_to_current_snapshot(self) -> None:
        company_dir = self.runtime_dir / "company_assets" / "megacorp"
        current_snapshot_dir = company_dir / "20260406T130000"
        old_snapshot_dir = company_dir / "20260406T120000"
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)
        old_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260406T130000",
                    "company_identity": {
                        "requested_name": "MegaCorp",
                        "canonical_name": "MegaCorp",
                        "company_key": "megacorp",
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        old_candidates = [
            Candidate(
                candidate_id=f"old_{index}",
                name_en=f"Old Person {index}",
                display_name=f"Old Person {index}",
                category="employee",
                target_company="MegaCorp",
                employment_status="current",
                linkedin_url=f"https://www.linkedin.com/in/old-person-{index}",
                source_dataset="megacorp_old_snapshot",
            ).to_record()
            for index in range(1100)
        ]
        current_candidates = [
            Candidate(
                candidate_id=f"current_{index}",
                name_en=f"Current Person {index}",
                display_name=f"Current Person {index}",
                category="employee",
                target_company="MegaCorp",
                employment_status="current",
                linkedin_url=f"https://www.linkedin.com/in/current-person-{index}",
                source_dataset="megacorp_current_snapshot",
            ).to_record()
            for index in range(1200)
        ]
        (old_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": old_candidates, "evidence": []}, ensure_ascii=False, indent=2)
        )
        (current_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": current_candidates, "evidence": []}, ensure_ascii=False, indent=2)
        )

        materialized_view = materialize_company_candidate_view(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="MegaCorp",
        )

        self.assertEqual(len(materialized_view["source_snapshots"]), 1)
        self.assertEqual(materialized_view["source_snapshots"][0]["snapshot_id"], "20260406T130000")
        self.assertEqual(materialized_view["source_snapshot_selection"]["mode"], "current_snapshot_only_large_org")
        self.assertEqual(len(materialized_view["candidates"]), 1200)

    def test_replace_company_data_tolerates_duplicate_evidence_ids_in_same_batch(self) -> None:
        candidate = Candidate(
            candidate_id="dup_evidence_candidate",
            name_en="Dana Duplicate",
            display_name="Dana Duplicate",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            source_dataset="acme_roster",
        )
        duplicate_evidence_id = make_evidence_id(
            candidate.candidate_id,
            "linkedin_profile_detail",
            "Research Engineer",
            "https://www.linkedin.com/in/dana-duplicate/",
        )
        evidence_primary = EvidenceRecord(
            evidence_id=duplicate_evidence_id,
            candidate_id=candidate.candidate_id,
            source_type="linkedin_profile_detail",
            title="Research Engineer",
            url="https://www.linkedin.com/in/dana-duplicate/",
            summary="Primary profile detail capture.",
            source_dataset="linkedin_profile_detail",
            source_path="/tmp/dana-primary.json",
        )
        evidence_duplicate = EvidenceRecord(
            evidence_id=duplicate_evidence_id,
            candidate_id=candidate.candidate_id,
            source_type="linkedin_profile_detail",
            title="Research Engineer",
            url="https://www.linkedin.com/in/dana-duplicate/",
            summary="Duplicate profile detail capture from resume replay.",
            source_dataset="linkedin_profile_detail",
            source_path="/tmp/dana-duplicate.json",
        )

        self.store.replace_company_data("Acme", [candidate], [evidence_primary, evidence_duplicate])

        stored_evidence = self.store.list_evidence(candidate.candidate_id)
        self.assertEqual(len(stored_evidence), 1)
        self.assertEqual(stored_evidence[0]["evidence_id"], duplicate_evidence_id)
        self.assertEqual(stored_evidence[0]["summary"], "Duplicate profile detail capture from resume replay.")

    def test_repair_missing_company_candidate_artifacts_builds_normalized_payload_and_refreshes_registry(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        candidate = Candidate(
            candidate_id="legacy_1",
            name_en="Legacy Former",
            display_name="Legacy Former",
            category="former_employee",
            target_company="Acme",
            organization="Acme",
            employment_status="former",
            role="Infra Engineer",
            linkedin_url="https://www.linkedin.com/in/legacy-former/",
            education="Stanford",
            work_history="Acme | AnotherCo",
            source_dataset="legacy_snapshot",
        )
        evidence = {
            "evidence_id": make_evidence_id(
                "legacy_1",
                "legacy_snapshot",
                "Legacy root snapshot",
                "https://www.linkedin.com/in/legacy-former/",
            ),
            "candidate_id": "legacy_1",
            "source_type": "linkedin_profile_detail",
            "title": "Legacy root snapshot",
            "url": "https://www.linkedin.com/in/legacy-former/",
            "summary": "Recovered from a legacy root candidate snapshot.",
            "source_dataset": "legacy_snapshot",
            "source_path": str(snapshot_dir / "candidate_documents.json"),
            "metadata": {"profile_url": "https://www.linkedin.com/in/legacy-former/"},
        }
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260406T120000",
                        "company_identity": {
                            "requested_name": "Acme",
                            "canonical_name": "Acme",
                            "company_key": "acme",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [evidence],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        result = repair_missing_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["Acme"],
            snapshot_id="20260406T120000",
        )

        normalized_dir = snapshot_dir / "normalized_artifacts"
        self.assertEqual(result["status"], "completed")
        repaired_payload = self._load_artifact_view()
        self.assertEqual(result["repaired_snapshot_count"], 1)
        self.assertTrue((normalized_dir / "manifest.json").exists())
        self.assertTrue((normalized_dir / "artifact_summary.json").exists())
        ledger_path = Path(result["companies"][0]["repaired_snapshots"][0]["ledger_path"])
        self.assertTrue(ledger_path.exists())
        self.assertEqual(len(repaired_payload["candidates"]), 1)
        authoritative = self.store.get_authoritative_organization_asset_registry(
            target_company="Acme",
            asset_view="canonical_merged",
        )
        self.assertEqual(str(authoritative.get("snapshot_id") or ""), "20260406T120000")
        self.assertGreaterEqual(int(authoritative.get("candidate_count") or 0), 1)

    def test_repair_missing_company_candidate_artifacts_uses_company_key_for_alias_directories(self) -> None:
        company_dir = self.runtime_dir / "company_assets" / "humansand"
        snapshot_dir = company_dir / "20260408T204924"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260408T204924",
                    "company_identity": {
                        "requested_name": "Humans&",
                        "canonical_name": "humans&",
                        "company_key": "humansand",
                        "aliases": ["humansand"],
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(
                {
                    "requested_name": "Humans&",
                    "canonical_name": "humans&",
                    "company_key": "humansand",
                    "aliases": ["humansand"],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate = Candidate(
            candidate_id="humansand_legacy_1",
            name_en="Casey Example",
            display_name="Casey Example",
            category="employee",
            target_company="humans&",
            organization="humans&",
            employment_status="current",
            role="Infra Engineer",
            linkedin_url="https://www.linkedin.com/in/casey-example/",
            source_dataset="humansand_legacy_snapshot",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260408T204924",
                        "company_identity": {
                            "requested_name": "Humans&",
                            "canonical_name": "humans&",
                            "company_key": "humansand",
                            "aliases": ["humansand"],
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        result = repair_missing_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["Humans&"],
            snapshot_id="20260408T204924",
        )

        normalized_dir = snapshot_dir / "normalized_artifacts"
        self.assertEqual(result["status"], "completed")
        self.assertTrue((normalized_dir / "manifest.json").exists())
        self.assertTrue((normalized_dir / "artifact_summary.json").exists())
        ledger_path = Path(result["companies"][0]["repaired_snapshots"][0]["ledger_path"])
        self.assertTrue(ledger_path.exists())

    def test_repair_missing_company_candidate_artifacts_force_rebuilds_existing_artifacts(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        normalized_dir = snapshot_dir / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        materialized_path = normalized_dir / "materialized_candidate_documents.json"
        artifact_summary_path = normalized_dir / "artifact_summary.json"
        ledger_path = normalized_dir / "organization_completeness_ledger.json"

        candidate = Candidate(
            candidate_id="legacy_2",
            name_en="Timeline Candidate",
            display_name="Timeline Candidate",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/timeline-candidate/",
            source_dataset="legacy_snapshot",
        )
        evidence = {
            "evidence_id": make_evidence_id(
                "legacy_2",
                "legacy_snapshot",
                "Legacy timeline candidate",
                "https://www.linkedin.com/in/timeline-candidate/",
            ),
            "candidate_id": "legacy_2",
            "source_type": "linkedin_profile_detail",
            "title": "Legacy timeline candidate",
            "url": "https://www.linkedin.com/in/timeline-candidate/",
            "summary": "Recovered from a legacy root candidate snapshot.",
            "source_dataset": "legacy_snapshot",
            "source_path": str(snapshot_dir / "candidate_documents.json"),
            "metadata": {"profile_url": "https://www.linkedin.com/in/timeline-candidate/"},
        }
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260406T120000",
                        "company_identity": {
                            "requested_name": "Acme",
                            "canonical_name": "Acme",
                            "company_key": "acme",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [evidence],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        materialized_path.write_text(
            json.dumps({"candidates": [candidate.to_record()], "evidence": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        artifact_summary_path.write_text(
            json.dumps({"candidate_count": 1}, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        ledger_path.write_text(json.dumps({"status": "stale"}, ensure_ascii=False, indent=2), encoding="utf-8")

        profile_dir = snapshot_dir / "harvest_profiles"
        profile_dir.mkdir(parents=True, exist_ok=True)
        profile_path = profile_dir / "timeline-candidate.json"
        profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "picture": "https://cdn.example.com/rewrite-candidate.jpg",
                        "email": "rewrite@acme.com",
                        "experience": [
                            {
                                "title": "Research Engineer",
                                "companyName": "Acme",
                                "startDate": {"year": 2022},
                                "endDate": {"text": "Present"},
                            }
                        ],
                        "educations": [
                            {
                                "degreeName": "Bachelor",
                                "schoolName": "MIT",
                                "fieldOfStudy": "Computer Science",
                                "startDate": {"year": 2018},
                                "endDate": {"year": 2022},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.mark_linkedin_profile_registry_fetched(
            "https://www.linkedin.com/in/timeline-candidate/",
            raw_path=str(profile_path),
            snapshot_dir=str(snapshot_dir),
        )

        result = repair_missing_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["Acme"],
            snapshot_id="20260406T120000",
            force_rebuild_artifacts=True,
        )

        rebuilt_payload = dict(self._load_artifact_view()["source_payload"])
        rebuilt_candidate = dict((rebuilt_payload.get("candidates") or [])[0] or {})
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["repaired_snapshot_count"], 0)
        self.assertEqual(result["force_rebuilt_snapshot_count"], 1)
        self.assertEqual(rebuilt_candidate.get("experience_lines"), ["2022~Present, Acme, Research Engineer"])
        self.assertEqual(
            rebuilt_candidate.get("education_lines"),
            ["2018~2022, Bachelor, MIT, Computer Science"],
        )

    def test_backfill_structured_timeline_for_company_assets_runs_profile_registry_and_force_repair(self) -> None:
        with (
            unittest.mock.patch(
                "sourcing_agent.profile_registry_backfill.backfill_linkedin_profile_registry",
                side_effect=[
                    {"status": "completed", "files_processed_this_run": 3},
                    {"status": "completed", "files_processed_this_run": 5},
                ],
            ) as profile_backfill,
            unittest.mock.patch(
                "sourcing_agent.candidate_artifacts.rewrite_structured_timeline_in_company_candidate_artifacts",
                return_value={"status": "completed", "rewritten_snapshot_count": 2},
            ) as rewrite_mock,
        ):
            result = backfill_structured_timeline_for_company_assets(
                runtime_dir=self.runtime_dir,
                store=self.store,
                companies=["Acme", "Beta"],
                snapshot_id="20260406T120000",
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["profile_registry_backfill"]["company_run_count"], 2)
        self.assertEqual(result["profile_registry_backfill"]["files_processed_this_run"], 8)
        self.assertEqual(profile_backfill.call_count, 2)
        rewrite_mock.assert_called_once_with(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["Acme", "Beta"],
            snapshot_id="20260406T120000",
            refresh_registry=True,
        )

    def test_backfill_structured_timeline_for_company_assets_can_skip_registry_refresh(self) -> None:
        with (
            unittest.mock.patch(
                "sourcing_agent.profile_registry_backfill.backfill_linkedin_profile_registry",
                return_value={"status": "completed", "files_processed_this_run": 3},
            ) as profile_backfill,
            unittest.mock.patch(
                "sourcing_agent.candidate_artifacts.rewrite_structured_timeline_in_company_candidate_artifacts",
                return_value={"status": "completed", "rewritten_snapshot_count": 1},
            ) as rewrite_mock,
        ):
            result = backfill_structured_timeline_for_company_assets(
                runtime_dir=self.runtime_dir,
                store=self.store,
                companies=["Acme"],
                snapshot_id="20260406T120000",
                refresh_registry=False,
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(profile_backfill.call_count, 1)
        rewrite_mock.assert_called_once_with(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["Acme"],
            snapshot_id="20260406T120000",
            refresh_registry=False,
        )

    def test_rewrite_structured_timeline_in_company_candidate_artifacts_rewrites_existing_materialized_payload(
        self,
    ) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        candidate = Candidate(
            candidate_id="rewrite_1",
            name_en="Rewrite Candidate",
            display_name="Rewrite Candidate",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/rewrite-candidate/",
            source_dataset="legacy_snapshot",
        )
        evidence = {
            "evidence_id": make_evidence_id(
                "rewrite_1",
                "legacy_snapshot",
                "Rewrite candidate",
                "https://www.linkedin.com/in/rewrite-candidate/",
            ),
            "candidate_id": "rewrite_1",
            "source_type": "linkedin_profile_detail",
            "title": "Rewrite candidate",
            "url": "https://www.linkedin.com/in/rewrite-candidate/",
            "summary": "Recovered from a legacy root candidate snapshot.",
            "source_dataset": "legacy_snapshot",
            "source_path": str(snapshot_dir / "candidate_documents.json"),
            "metadata": {"profile_url": "https://www.linkedin.com/in/rewrite-candidate/"},
        }
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260406T120000",
                        "company_identity": {
                            "requested_name": "Acme",
                            "canonical_name": "Acme",
                            "company_key": "acme",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [evidence],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        profile_dir = snapshot_dir / "harvest_profiles"
        profile_dir.mkdir(parents=True, exist_ok=True)
        profile_path = profile_dir / "rewrite-candidate.json"
        profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "picture": "https://cdn.example.com/rewrite-candidate.jpg",
                        "email": "rewrite@acme.com",
                        "experience": [
                            {
                                "title": "Research Engineer",
                                "companyName": "Acme",
                                "startDate": {"year": 2023},
                                "endDate": {"text": "Present"},
                            }
                        ],
                        "educations": [
                            {
                                "degreeName": "Master",
                                "schoolName": "Stanford University",
                                "fieldOfStudy": "Computer Science",
                                "startDate": {"year": 2021},
                                "endDate": {"year": 2023},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.mark_linkedin_profile_registry_fetched(
            "https://www.linkedin.com/in/rewrite-candidate/",
            raw_path=str(profile_path),
            snapshot_dir=str(snapshot_dir),
        )

        build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
            snapshot_id="20260406T120000",
        )

        normalized_dir = snapshot_dir / "normalized_artifacts"
        manifest_path = normalized_dir / "manifest.json"
        summary_path = normalized_dir / "artifact_summary.json"
        manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
        for entry in list(manifest_payload.get("candidate_shards") or []):
            shard_path = normalized_dir / str(entry.get("path") or "")
            shard_payload = json.loads(shard_path.read_text(encoding="utf-8"))
            materialized_candidate = dict(shard_payload.get("materialized_candidate") or {})
            normalized_candidate = dict(shard_payload.get("normalized_candidate") or {})
            reusable_document = dict(shard_payload.get("reusable_document") or {})
            for record in (materialized_candidate, normalized_candidate):
                record.pop("experience_lines", None)
                record.pop("education_lines", None)
                record.pop("profile_timeline_source", None)
                record.pop("profile_timeline_source_path", None)
            materialized_candidate["metadata"] = {}
            reusable_document["experience_lines"] = []
            reusable_document["education_lines"] = []
            reusable_document["profile_timeline_source"] = ""
            reusable_document["profile_timeline_source_path"] = ""
            shard_payload["materialized_candidate"] = materialized_candidate
            shard_payload["normalized_candidate"] = normalized_candidate
            shard_payload["reusable_document"] = reusable_document
            shard_path.write_text(json.dumps(shard_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        summary_payload["structured_timeline_count"] = 0
        summary_payload["structured_experience_count"] = 0
        summary_payload["structured_education_count"] = 0
        summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        result = rewrite_structured_timeline_in_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["Acme"],
            snapshot_id="20260406T120000",
        )

        rebuilt_materialized = dict(self._load_artifact_view()["source_payload"])
        rebuilt_candidate = dict((rebuilt_materialized.get("candidates") or [])[0] or {})
        rebuilt_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertEqual(result["status"], "completed")
        self.assertEqual(
            int(result["rewritten_snapshot_count"] or 0) + int(result["rebuilt_missing_snapshot_count"] or 0),
            1,
        )
        self.assertEqual(result["rewritten_view_count"], 2)
        self.assertEqual(rebuilt_candidate.get("experience_lines"), ["2023~Present, Acme, Research Engineer"])
        self.assertEqual(
            rebuilt_candidate.get("education_lines"),
            ["2021~2023, Master, Stanford University, Computer Science"],
        )
        self.assertEqual(rebuilt_candidate.get("avatar_url"), "https://cdn.example.com/rewrite-candidate.jpg")
        self.assertIsNone(rebuilt_candidate.get("primary_email"))
        self.assertEqual(rebuilt_candidate.get("media_url"), "https://cdn.example.com/rewrite-candidate.jpg")
        self.assertEqual(int(rebuilt_summary.get("structured_timeline_count") or 0), 1)

    def test_repair_projected_profile_signals_in_company_candidate_artifacts_projects_metadata_to_top_level(
        self,
    ) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        normalized_dir = snapshot_dir / "normalized_artifacts"
        strict_dir = normalized_dir / "strict_roster_only"
        strict_dir.mkdir(parents=True, exist_ok=True)
        materialized_payload = {
            "snapshot": {
                "snapshot_id": "20260406T120000",
                "target_company": "Acme",
            },
            "candidates": [
                {
                    "candidate_id": "projection_1",
                    "display_name": "Projection Candidate",
                    "media_url": "",
                    "metadata": {
                        "experience_lines": ["2023~Present, Acme, Research Engineer"],
                        "education_lines": ["2019~2023, Bachelor, CMU, Computer Science"],
                        "avatar_url": "https://cdn.example.com/projection-candidate.jpg",
                        "primary_email": "projection@acme.com",
                        "headline": "Research Engineer at Acme",
                    },
                }
            ],
            "evidence": [],
        }
        normalized_payload = [
            {
                "candidate_id": "projection_1",
                "display_name": "Projection Candidate",
                "media_url": "",
            }
        ]
        reusable_payload = [
            {
                "candidate_id": "projection_1",
                "display_name": "Projection Candidate",
                "media_url": "",
            }
        ]
        for artifact_dir in (normalized_dir, strict_dir):
            artifact_dir.mkdir(parents=True, exist_ok=True)
            (artifact_dir / "materialized_candidate_documents.json").write_text(
                json.dumps(materialized_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (artifact_dir / "normalized_candidates.json").write_text(
                json.dumps(normalized_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (artifact_dir / "reusable_candidate_documents.json").write_text(
                json.dumps(reusable_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        result = repair_projected_profile_signals_in_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            companies=["Acme"],
            snapshot_id="20260406T120000",
        )

        repaired_materialized = json.loads(
            (normalized_dir / "materialized_candidate_documents.json").read_text(encoding="utf-8")
        )
        repaired_normalized = json.loads((normalized_dir / "normalized_candidates.json").read_text(encoding="utf-8"))
        repaired_reusable = json.loads(
            (normalized_dir / "reusable_candidate_documents.json").read_text(encoding="utf-8")
        )
        repaired_candidate = dict((repaired_materialized.get("candidates") or [])[0] or {})
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["updated_snapshot_count"], 1)
        self.assertEqual(result["updated_view_count"], 2)
        self.assertEqual(repaired_candidate.get("experience_lines"), ["2023~Present, Acme, Research Engineer"])
        self.assertEqual(repaired_candidate.get("education_lines"), ["2019~2023, Bachelor, CMU, Computer Science"])
        self.assertEqual(repaired_candidate.get("avatar_url"), "https://cdn.example.com/projection-candidate.jpg")
        self.assertEqual(repaired_candidate.get("primary_email"), "projection@acme.com")
        self.assertEqual(repaired_candidate.get("headline"), "Research Engineer at Acme")
        self.assertEqual(repaired_candidate.get("media_url"), "https://cdn.example.com/projection-candidate.jpg")
        self.assertEqual(repaired_normalized[0].get("avatar_url"), "https://cdn.example.com/projection-candidate.jpg")
        self.assertEqual(repaired_normalized[0].get("primary_email"), "projection@acme.com")
        self.assertEqual(repaired_reusable[0].get("avatar_url"), "https://cdn.example.com/projection-candidate.jpg")

    def test_repair_projected_profile_signals_backfills_publishable_harvest_email_metadata(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        normalized_dir = snapshot_dir / "normalized_artifacts"
        strict_dir = normalized_dir / "strict_roster_only"
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        strict_dir.mkdir(parents=True, exist_ok=True)
        profile_path = harvest_dir / "repair-email-candidate.json"
        profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "linkedinUrl": "https://www.linkedin.com/in/repair-email-candidate/",
                        "emails": [
                            {
                                "email": "repair-candidate@example.org",
                                "foundInLinkedInProfile": True,
                                "qualityScore": 100,
                                "status": "valid",
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps({"items": []}, ensure_ascii=False, indent=2), encoding="utf-8")
        materialized_payload = {
            "snapshot": {
                "snapshot_id": "20260406T120000",
                "target_company": "Acme",
            },
            "candidates": [
                {
                    "candidate_id": "projection_email_1",
                    "display_name": "Projection Email Candidate",
                    "target_company": "Acme",
                    "source_path": str(summary_path),
                    "primary_email": "repair-candidate@example.org",
                    "metadata": {
                        "profile_timeline_source_path": str(profile_path),
                        "primary_email": "repair-candidate@example.org",
                    },
                }
            ],
            "evidence": [],
        }
        normalized_payload = [
            {
                "candidate_id": "projection_email_1",
                "display_name": "Projection Email Candidate",
                "primary_email": "repair-candidate@example.org",
            }
        ]
        reusable_payload = [
            {
                "candidate_id": "projection_email_1",
                "display_name": "Projection Email Candidate",
                "primary_email": "repair-candidate@example.org",
            }
        ]
        for artifact_dir in (normalized_dir, strict_dir):
            artifact_dir.mkdir(parents=True, exist_ok=True)
            (artifact_dir / "materialized_candidate_documents.json").write_text(
                json.dumps(materialized_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (artifact_dir / "normalized_candidates.json").write_text(
                json.dumps(normalized_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (artifact_dir / "reusable_candidate_documents.json").write_text(
                json.dumps(reusable_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        result = repair_projected_profile_signals_in_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            companies=["Acme"],
            snapshot_id="20260406T120000",
        )

        repaired_materialized = json.loads(
            (normalized_dir / "materialized_candidate_documents.json").read_text(encoding="utf-8")
        )
        repaired_normalized = json.loads((normalized_dir / "normalized_candidates.json").read_text(encoding="utf-8"))
        repaired_reusable = json.loads(
            (normalized_dir / "reusable_candidate_documents.json").read_text(encoding="utf-8")
        )
        repaired_candidate = dict((repaired_materialized.get("candidates") or [])[0] or {})
        expected_metadata = {
            "source": "harvestapi",
            "status": "valid",
            "qualityScore": 100,
            "foundInLinkedInProfile": True,
        }
        self.assertEqual(result["status"], "completed")
        self.assertEqual(repaired_candidate.get("primary_email_metadata"), expected_metadata)
        self.assertEqual(repaired_normalized[0].get("primary_email_metadata"), expected_metadata)
        self.assertEqual(repaired_reusable[0].get("primary_email_metadata"), expected_metadata)
        self.assertEqual(repaired_reusable[0].get("primary_email"), "repair-candidate@example.org")

    def test_rewrite_structured_timeline_in_company_candidate_artifacts_bootstraps_missing_artifacts(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        candidate = Candidate(
            candidate_id="bootstrap_1",
            name_en="Bootstrap Candidate",
            display_name="Bootstrap Candidate",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/bootstrap-candidate/",
            source_dataset="legacy_snapshot",
        )
        evidence = {
            "evidence_id": make_evidence_id(
                "bootstrap_1",
                "legacy_snapshot",
                "Bootstrap candidate",
                "https://www.linkedin.com/in/bootstrap-candidate/",
            ),
            "candidate_id": "bootstrap_1",
            "source_type": "linkedin_profile_detail",
            "title": "Bootstrap candidate",
            "url": "https://www.linkedin.com/in/bootstrap-candidate/",
            "summary": "Recovered from a legacy root candidate snapshot.",
            "source_dataset": "legacy_snapshot",
            "source_path": str(snapshot_dir / "candidate_documents.json"),
            "metadata": {"profile_url": "https://www.linkedin.com/in/bootstrap-candidate/"},
        }
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260406T120000",
                        "company_identity": {
                            "requested_name": "Acme",
                            "canonical_name": "Acme",
                            "company_key": "acme",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [evidence],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        profile_dir = snapshot_dir / "harvest_profiles"
        profile_dir.mkdir(parents=True, exist_ok=True)
        profile_path = profile_dir / "bootstrap-candidate.json"
        profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "experience": [
                            {
                                "title": "Research Engineer",
                                "companyName": "Acme",
                                "startDate": {"year": 2020},
                                "endDate": {"text": "Present"},
                            }
                        ],
                        "educations": [
                            {
                                "degreeName": "BS",
                                "schoolName": "CMU",
                                "fieldOfStudy": "Computer Science",
                                "startDate": {"year": 2016},
                                "endDate": {"year": 2020},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.mark_linkedin_profile_registry_fetched(
            "https://www.linkedin.com/in/bootstrap-candidate/",
            raw_path=str(profile_path),
            snapshot_dir=str(snapshot_dir),
        )

        result = rewrite_structured_timeline_in_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["Acme"],
            snapshot_id="20260406T120000",
        )

        normalized_dir = snapshot_dir / "normalized_artifacts"
        strict_manifest_path = normalized_dir / "strict_roster_only" / "manifest.json"
        summary_path = normalized_dir / "artifact_summary.json"
        rebuilt_payload = dict(self._load_artifact_view()["source_payload"])
        rebuilt_candidate = dict((rebuilt_payload.get("candidates") or [])[0] or {})
        rebuilt_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["rewritten_snapshot_count"], 0)
        self.assertEqual(result["rebuilt_missing_snapshot_count"], 1)
        self.assertTrue(strict_manifest_path.exists())
        self.assertEqual(rebuilt_candidate.get("experience_lines"), ["2020~Present, Acme, Research Engineer"])
        self.assertEqual(rebuilt_candidate.get("education_lines"), ["2016~2020, BS, CMU, Computer Science"])
        self.assertEqual(int(rebuilt_summary.get("structured_timeline_count") or 0), 1)
        rebuilt_company = result["companies"][0]["rebuilt_missing_snapshots"][0]
        self.assertEqual(rebuilt_company.get("rewrite_mode"), "candidate_documents_bootstrap")

    def test_rewrite_structured_timeline_in_company_candidate_artifacts_can_skip_registry_refresh(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        candidate = Candidate(
            candidate_id="skip_registry_1",
            name_en="Skip Registry Candidate",
            display_name="Skip Registry Candidate",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/skip-registry-candidate/",
            source_dataset="legacy_snapshot",
        )
        evidence = {
            "evidence_id": make_evidence_id(
                "skip_registry_1",
                "legacy_snapshot",
                "Skip registry candidate",
                "https://www.linkedin.com/in/skip-registry-candidate/",
            ),
            "candidate_id": "skip_registry_1",
            "source_type": "linkedin_profile_detail",
            "title": "Skip registry candidate",
            "url": "https://www.linkedin.com/in/skip-registry-candidate/",
            "summary": "Recovered from a legacy root candidate snapshot.",
            "source_dataset": "legacy_snapshot",
            "source_path": str(snapshot_dir / "candidate_documents.json"),
            "metadata": {"profile_url": "https://www.linkedin.com/in/skip-registry-candidate/"},
        }
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260406T120000",
                        "company_identity": {
                            "requested_name": "Acme",
                            "canonical_name": "Acme",
                            "company_key": "acme",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [evidence],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        profile_dir = snapshot_dir / "harvest_profiles"
        profile_dir.mkdir(parents=True, exist_ok=True)
        profile_path = profile_dir / "skip-registry-candidate.json"
        profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "experience": [
                            {
                                "title": "Research Engineer",
                                "companyName": "Acme",
                                "startDate": {"year": 2020},
                                "endDate": {"text": "Present"},
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.mark_linkedin_profile_registry_fetched(
            "https://www.linkedin.com/in/skip-registry-candidate/",
            raw_path=str(profile_path),
            snapshot_dir=str(snapshot_dir),
        )

        with unittest.mock.patch(
            "sourcing_agent.asset_reuse_planning.backfill_organization_asset_registry_for_company"
        ) as refresh_mock:
            result = rewrite_structured_timeline_in_company_candidate_artifacts(
                runtime_dir=self.runtime_dir,
                store=self.store,
                companies=["Acme"],
                snapshot_id="20260406T120000",
                refresh_registry=False,
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["rewritten_snapshot_count"], 0)
        self.assertEqual(result["rebuilt_missing_snapshot_count"], 1)
        self.assertEqual(result["registry_refresh_count"], 0)
        self.assertEqual(result["companies"][0]["registry_refresh"]["status"], "skipped")
        refresh_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
