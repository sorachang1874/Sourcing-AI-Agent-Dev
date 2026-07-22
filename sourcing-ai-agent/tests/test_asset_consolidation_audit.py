import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.audit_asset_consolidation import _safe_output_name, render_asset_consolidation_markdown
from sourcing_agent.asset_consolidation_audit import audit_asset_consolidation
from sourcing_agent.asset_consolidation_plan import (
    build_asset_consolidation_plan,
    render_asset_consolidation_plan_markdown,
)
from sourcing_agent.asset_consolidation_repair_proposal import (
    build_asset_consolidation_repair_proposal,
    render_asset_consolidation_repair_proposal_markdown,
)
from sourcing_agent.asset_reuse_planning import build_acquisition_shard_registry_record

from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class AssetConsolidationAuditTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self.store = self.make_pg_store(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()
        super().tearDown()

    def _write_snapshot_dir(
        self,
        company_key: str,
        snapshot_id: str,
        *,
        latest: bool = False,
        candidates: list[dict[str, object]] | None = None,
    ) -> None:
        company_dir = self.runtime_dir / "company_assets" / company_key
        snapshot_dir = company_dir / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "manifest.json").write_text(
            json.dumps({"snapshot_id": snapshot_id}, ensure_ascii=False),
            encoding="utf-8",
        )
        if candidates is not None:
            (snapshot_dir / "candidate_documents.json").write_text(
                json.dumps({"candidates": candidates, "evidence": []}, ensure_ascii=False),
                encoding="utf-8",
            )
        if latest:
            (company_dir / "latest_snapshot.json").write_text(
                json.dumps({"snapshot_id": snapshot_id}, ensure_ascii=False),
                encoding="utf-8",
            )

    def _snapshot(self, company: str, snapshot_id: str, **overrides: object) -> dict[str, object]:
        payload: dict[str, object] = {
            "target_company": company,
            "company_key": company.lower(),
            "snapshot_id": snapshot_id,
            "asset_view": "canonical_merged",
            "status": "ready",
            "authoritative": False,
            "candidate_count": 10,
            "profile_detail_count": 8,
            "summary": {"candidate_count": 10},
        }
        payload.update(overrides)
        return payload

    def _snapshots_by_id(self, report: dict[str, object]) -> dict[str, dict[str, object]]:
        company = dict(report["companies"][0])
        return {str(snapshot["snapshot_id"]): snapshot for snapshot in company["snapshots"]}  # type: ignore[index]

    def test_audit_classifies_authoritative_shard_projection_and_archive_candidates(self) -> None:
        for snapshot_id in ("snap-authoritative", "snap-reusable", "snap-projection", "snap-archive"):
            self._write_snapshot_dir("openai", snapshot_id, latest=snapshot_id == "snap-authoritative")
        self.store.upsert_organization_asset_registry(
            self._snapshot(
                "OpenAI",
                "snap-authoritative",
                authoritative=True,
                candidate_count=100,
                selected_snapshot_ids=["snap-authoritative", "snap-reusable"],
                source_snapshot_selection={
                    "selected_snapshot_ids": ["snap-authoritative", "snap-reusable"],
                },
            ),
            authoritative=True,
        )
        self.store.upsert_organization_asset_registry(self._snapshot("OpenAI", "snap-reusable"))
        self.store.upsert_organization_asset_registry(self._snapshot("OpenAI", "snap-projection"))
        self.store.upsert_organization_asset_registry(self._snapshot("OpenAI", "snap-archive"))
        self.store.upsert_acquisition_shard_registry(
            build_acquisition_shard_registry_record(
                target_company="OpenAI",
                company_key="openai",
                snapshot_id="snap-reusable",
                lane="profile_search",
                employment_scope="current",
                strategy_type="scoped_search_roster",
                shard_id="agent",
                shard_title="Agent",
                search_query="Agent",
                company_filters={"companies": ["OpenAI"], "search_query": "Agent"},
                result_count=12,
                status="completed",
            )
        )
        self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj_openai_scope",
                "projection_type": "run_scope_projection",
                "collection_id": "company:openai",
                "source_run_id": "job-openai-agent",
                "state": "serving",
                "provenance": {"snapshot_id": "snap-projection"},
            }
        )
        self.store.repos.serving_projection.upsert_authoritative_pointer(
            {
                "collection_id": "company:openai",
                "active_projection_id": "proj_openai_scope",
                "active_collection_version": "snap-projection",
            }
        )

        report = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
        )

        snapshots = self._snapshots_by_id(report)
        self.assertEqual(snapshots["snap-authoritative"]["classification"], "keep_authoritative_serving")
        self.assertIn(
            "authoritative_registry_pointer",
            {blocker["type"] for blocker in snapshots["snap-authoritative"]["deletion_blockers"]},
        )
        self.assertIn(
            "latest_snapshot_pointer",
            {blocker["type"] for blocker in snapshots["snap-authoritative"]["deletion_blockers"]},
        )
        self.assertEqual(snapshots["snap-reusable"]["classification"], "keep_reusable_shard_source")
        self.assertEqual(snapshots["snap-reusable"]["shards"]["reusable_shard_count"], 1)
        self.assertIn(
            "reusable_acquisition_shard",
            {blocker["type"] for blocker in snapshots["snap-reusable"]["deletion_blockers"]},
        )
        self.assertEqual(snapshots["snap-projection"]["classification"], "keep_projection_dependency")
        self.assertIn(
            "active_serving_projection_dependency",
            {blocker["type"] for blocker in snapshots["snap-projection"]["deletion_blockers"]},
        )
        self.assertEqual(snapshots["snap-archive"]["classification"], "archive_candidate_no_increment_duplicate")
        self.assertTrue(snapshots["snap-archive"]["archive_ready"])
        self.assertEqual(snapshots["snap-archive"]["deletion_blockers"], [])
        self.assertEqual(report["summary"]["archive_candidate_count"], 1)

    def test_crm_and_person_asset_dependencies_block_snapshot_archive(self) -> None:
        self._write_snapshot_dir("google", "snap-google-scope")
        self.store.upsert_organization_asset_registry(self._snapshot("Google", "snap-google-scope", company_key="google"))
        self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj_google_scope",
                "projection_type": "run_scope_projection",
                "collection_id": "company:google",
                "source_run_id": "job-google-veo",
                "state": "archived",
                "provenance": {"snapshot_id": "snap-google-scope"},
            }
        )
        self.store.upsert_crm_record(
            {
                "crm_record_id": "crm-google-ada",
                "person_identity_key": "linkedin:google-ada",
                "source_projection_id": "proj_google_scope",
                "source_collection_id": "company:google",
            }
        )
        self.store.upsert_person_asset(
            {
                "asset_id": "pa-google-ada-profile",
                "person_identity_key": "linkedin:google-ada",
                "asset_type": "raw_profile",
                "source_projection_id": "proj_google_scope",
                "content_ref": "runtime/company_assets/google/snap-google-scope/profile.json",
            }
        )

        report = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="Google",
        )

        snapshot = self._snapshots_by_id(report)["snap-google-scope"]
        blocker_types = {blocker["type"] for blocker in snapshot["deletion_blockers"]}
        self.assertEqual(snapshot["classification"], "keep_projection_dependency")
        self.assertIn("crm_record_source_projection_dependency", blocker_types)
        self.assertIn("person_asset_source_projection_dependency", blocker_types)
        self.assertEqual(snapshot["crm_dependency_count"], 1)
        self.assertEqual(snapshot["person_asset_dependency_count"], 1)
        self.assertFalse(snapshot["archive_ready"])

    def test_local_only_snapshot_requires_review_instead_of_silent_archive(self) -> None:
        self._write_snapshot_dir("anthropic", "local-only-smoke")

        report = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="Anthropic",
        )

        snapshot = self._snapshots_by_id(report)["local-only-smoke"]
        self.assertEqual(snapshot["classification"], "review_local_only_snapshot")
        self.assertFalse(snapshot["archive_ready"])
        self.assertEqual(snapshot["registry"], {"present": False})
        self.assertTrue(snapshot["local_snapshot"]["present"])

    def test_historical_row_selected_ids_do_not_block_archive_when_not_current_authoritative_source(self) -> None:
        self.store.upsert_organization_asset_registry(
            self._snapshot(
                "OpenAI",
                "snap-current",
                authoritative=True,
                selected_snapshot_ids=["snap-current"],
                source_snapshot_selection={"selected_snapshot_ids": ["snap-current"]},
            ),
            authoritative=True,
        )
        self.store.upsert_organization_asset_registry(
            self._snapshot(
                "OpenAI",
                "snap-old-self-selected",
                selected_snapshot_ids=["snap-old-self-selected"],
                source_snapshot_selection={
                    "selected_snapshot_ids": ["snap-old-self-selected"],
                    "population_coverage": {
                        "reason_codes": ["legacy_standard_bundle_full_company_proof"],
                    },
                },
            )
        )

        report = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
        )

        snapshots = self._snapshots_by_id(report)
        old_snapshot = snapshots["snap-old-self-selected"]
        self.assertEqual(old_snapshot["classification"], "archive_candidate_no_increment_duplicate")
        self.assertEqual(old_snapshot["deletion_blockers"], [])
        company = dict(report["companies"][0])
        self.assertEqual(company["selected_snapshot_ids"], ["snap-current"])

    def test_overlap_subsumption_marks_archive_candidate_ready_only_when_identity_set_is_subsumed(self) -> None:
        self._write_snapshot_dir(
            "openai",
            "snap-current",
            candidates=[
                {
                    "candidate_identity_key": "legacy:ada-current",
                    "candidate_id": "ada",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-current/",
                },
                {
                    "candidate_id": "grace",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/grace-current/",
                },
                {
                    "candidate_id": "linus",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/linus-current/",
                },
            ],
        )
        self._write_snapshot_dir(
            "openai",
            "snap-duplicate",
            candidates=[
                {
                    "candidate_identity_key": "legacy:ada-older",
                    "candidate_id": "ada-older",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-current/",
                },
                {
                    "candidate_id": "grace-older",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/grace-current/",
                },
            ],
        )
        self._write_snapshot_dir(
            "openai",
            "snap-unique",
            candidates=[
                {
                    "candidate_id": "ada-older",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-current/",
                },
                {
                    "candidate_id": "unique-person",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/unique-person/",
                },
            ],
        )
        self.store.upsert_organization_asset_registry(
            self._snapshot(
                "OpenAI",
                "snap-current",
                authoritative=True,
                selected_snapshot_ids=["snap-current"],
            ),
            authoritative=True,
        )
        self.store.upsert_organization_asset_registry(self._snapshot("OpenAI", "snap-duplicate"))
        self.store.upsert_organization_asset_registry(self._snapshot("OpenAI", "snap-unique"))

        report = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
            include_overlap=True,
        )

        snapshots = self._snapshots_by_id(report)
        duplicate_overlap = snapshots["snap-duplicate"]["overlap_subsumption"]
        unique_overlap = snapshots["snap-unique"]["overlap_subsumption"]
        self.assertEqual(duplicate_overlap["status"], "subsumed_by_reference")
        self.assertEqual(duplicate_overlap["unique_count"], 0)
        self.assertTrue(snapshots["snap-duplicate"]["archive_ready"])
        self.assertEqual(unique_overlap["status"], "review_unique_candidates_present")
        self.assertEqual(unique_overlap["unique_count"], 1)
        self.assertFalse(snapshots["snap-unique"]["archive_ready"])
        self.assertTrue(report["summary"]["overlap_enabled"])
        self.assertEqual(report["summary"]["overlap_subsumed_archive_candidate_count"], 1)
        self.assertEqual(report["summary"]["overlap_review_archive_candidate_count"], 1)

    def test_overlap_is_opt_in_and_not_in_default_audit(self) -> None:
        self._write_snapshot_dir(
            "openai",
            "snap-current",
            candidates=[
                {
                    "candidate_id": "ada",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-current/",
                }
            ],
        )
        self._write_snapshot_dir(
            "openai",
            "snap-duplicate",
            candidates=[
                {
                    "candidate_id": "ada-older",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-current/",
                }
            ],
        )
        self.store.upsert_organization_asset_registry(
            self._snapshot("OpenAI", "snap-current", authoritative=True),
            authoritative=True,
        )
        self.store.upsert_organization_asset_registry(self._snapshot("OpenAI", "snap-duplicate"))

        report = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
        )

        duplicate = self._snapshots_by_id(report)["snap-duplicate"]
        self.assertNotIn("overlap_subsumption", duplicate)
        self.assertTrue(duplicate["archive_ready"])
        self.assertFalse(report["summary"]["overlap_enabled"])

    def test_overlap_fails_closed_without_scanning_archive_payload_when_reference_missing(self) -> None:
        self._write_snapshot_dir(
            "openai",
            "snap-archive",
            candidates=[
                {
                    "candidate_id": "ada-archive",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-archive/",
                }
            ],
        )
        self.store.upsert_organization_asset_registry(
            self._snapshot(
                "OpenAI",
                "snap-current-missing-local-payload",
                authoritative=True,
                selected_snapshot_ids=["snap-current-missing-local-payload"],
            ),
            authoritative=True,
        )
        self.store.upsert_organization_asset_registry(self._snapshot("OpenAI", "snap-archive"))

        report = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
            include_overlap=True,
        )

        archive = self._snapshots_by_id(report)["snap-archive"]
        overlap = archive["overlap_subsumption"]
        self.assertEqual(overlap["status"], "review_reference_identity_missing")
        self.assertEqual(overlap["candidate_count"], 0)
        self.assertEqual(overlap["identity_count"], 0)
        self.assertEqual(overlap["source_path"], "")
        self.assertFalse(archive["archive_ready"])
        self.assertEqual(report["summary"]["overlap_review_archive_candidate_count"], 1)

    def test_overlap_prefers_canonical_collection_projection_over_stale_registry_reference(self) -> None:
        self._write_snapshot_dir(
            "openai",
            "snap-duplicate",
            candidates=[
                {
                    "candidate_id": "ada-older",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-current/",
                },
                {
                    "candidate_id": "grace-older",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/grace-current/",
                },
            ],
        )
        self.store.upsert_organization_asset_registry(
            self._snapshot(
                "OpenAI",
                "snap-missing-registry-reference",
                authoritative=True,
                selected_snapshot_ids=["snap-missing-registry-reference"],
            ),
            authoritative=True,
        )
        self.store.upsert_organization_asset_registry(self._snapshot("OpenAI", "snap-repair-reference"))
        self.store.upsert_organization_asset_registry(self._snapshot("OpenAI", "snap-duplicate"))
        self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj_openai_repair_reference",
                "projection_type": "collection_authoritative_projection",
                "collection_id": "company:openai",
                "state": "serving",
                "provenance": {"source_snapshot_id": "snap-repair-reference"},
            }
        )
        self.store.repos.serving_projection.upsert_members(
            "proj_openai_repair_reference",
            [
                {
                    "candidate_identity_key": "linkedin:https://www.linkedin.com/in/ada-current",
                    "person_identity_key": "linkedin:https://www.linkedin.com/in/ada-current",
                    "rank_index": 1,
                    "public_summary": {"linkedin_url": "https://www.linkedin.com/in/ada-current/"},
                },
                {
                    "candidate_identity_key": "linkedin:https://www.linkedin.com/in/grace-current",
                    "person_identity_key": "linkedin:https://www.linkedin.com/in/grace-current",
                    "rank_index": 2,
                    "public_summary": {"linkedin_url": "https://www.linkedin.com/in/grace-current/"},
                },
            ],
        )
        self.store.repos.serving_projection.upsert_authoritative_pointer(
            {
                "collection_id": "company:openai",
                "active_projection_id": "proj_openai_repair_reference",
                "active_collection_version": "snap-repair-reference",
            }
        )

        report = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
            include_overlap=True,
        )

        company = dict(report["companies"][0])
        overlap = dict(company["overlap_subsumption"])
        duplicate = self._snapshots_by_id(report)["snap-duplicate"]

        self.assertEqual(overlap["reference_source"], "collection_authoritative_pointer")
        self.assertEqual(overlap["reference_projection_id"], "proj_openai_repair_reference")
        self.assertEqual(overlap["reference_snapshot_ids"], ["snap-repair-reference"])
        self.assertEqual(overlap["legacy_registry_reference_snapshot_ids"], ["snap-missing-registry-reference"])
        self.assertEqual(overlap["reference_identity_count"], 2)
        self.assertEqual(duplicate["overlap_subsumption"]["status"], "subsumed_by_reference")
        self.assertTrue(duplicate["archive_ready"])

    def test_overlap_identity_loader_prefers_compact_candidate_documents(self) -> None:
        self._write_snapshot_dir(
            "openai",
            "snap-current",
            candidates=[
                {
                    "candidate_id": "ada",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-compact/",
                }
            ],
        )
        materialized_dir = self.runtime_dir / "company_assets" / "openai" / "snap-current" / "normalized_artifacts"
        materialized_dir.mkdir(parents=True, exist_ok=True)
        (materialized_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        {
                            "candidate_id": "wrong-large-payload",
                            "target_company": "OpenAI",
                            "linkedin_url": "https://www.linkedin.com/in/wrong-large-payload/",
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        self._write_snapshot_dir(
            "openai",
            "snap-duplicate",
            candidates=[
                {
                    "candidate_id": "ada-duplicate",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-compact/",
                }
            ],
        )
        self.store.upsert_organization_asset_registry(
            self._snapshot("OpenAI", "snap-current", authoritative=True, selected_snapshot_ids=["snap-current"]),
            authoritative=True,
        )
        self.store.upsert_organization_asset_registry(self._snapshot("OpenAI", "snap-duplicate"))

        report = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
            include_overlap=True,
        )

        overlap = dict(report["companies"][0]["overlap_subsumption"])
        duplicate = self._snapshots_by_id(report)["snap-duplicate"]
        self.assertEqual(overlap["reference_loads"][0]["source_path"].split("/")[-1], "candidate_documents.json")
        self.assertEqual(duplicate["overlap_subsumption"]["status"], "subsumed_by_reference")

    def test_company_filtered_audit_does_not_enumerate_all_snapshot_dirs(self) -> None:
        self._write_snapshot_dir("openai", "snap-openai")
        self.store.upsert_organization_asset_registry(
            self._snapshot("OpenAI", "snap-openai", authoritative=True),
            authoritative=True,
        )

        with mock.patch(
            "sourcing_agent.asset_consolidation_audit.iter_company_asset_snapshot_dirs",
            side_effect=AssertionError("company-filtered audit must not scan every company snapshot dir"),
        ):
            report = audit_asset_consolidation(
                runtime_dir=self.runtime_dir,
                store=self.store,
                company="OpenAI",
            )

        self.assertEqual(report["summary"]["company_count"], 1)
        self.assertEqual(report["companies"][0]["company_key"], "openai")

    def test_cli_company_evidence_output_names_are_stable_and_markdown_renderable(self) -> None:
        self.assertEqual(_safe_output_name("OpenAI"), "openai")
        self.assertEqual(_safe_output_name("Surge AI"), "surge_ai")
        self.assertEqual(_safe_output_name("  Google/Veo  "), "google_veo")

        self.store.upsert_organization_asset_registry(
            self._snapshot("OpenAI", "snap-current", authoritative=True),
            authoritative=True,
        )
        report = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
        )

        markdown = render_asset_consolidation_markdown(report)
        self.assertIn("# Asset Consolidation Audit", markdown)
        self.assertIn("## OpenAI", markdown)
        self.assertIn("snap-current", markdown)

    def test_consolidation_plan_blocks_archive_when_reference_identity_is_missing(self) -> None:
        self._write_snapshot_dir(
            "openai",
            "snap-archive",
            candidates=[
                {
                    "candidate_id": "ada-archive",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-archive/",
                }
            ],
        )
        self.store.upsert_organization_asset_registry(
            self._snapshot(
                "OpenAI",
                "snap-current-missing-local-payload",
                authoritative=True,
                selected_snapshot_ids=["snap-current-missing-local-payload"],
            ),
            authoritative=True,
        )
        self.store.upsert_organization_asset_registry(self._snapshot("OpenAI", "snap-archive"))
        report = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
            include_overlap=True,
        )

        plan = build_asset_consolidation_plan(audit_report=report, runtime_dir=self.runtime_dir)

        self.assertEqual(plan["status"], "blocked_missing_reference_identity")
        self.assertEqual(plan["summary"]["archive_ready_snapshot_count"], 0)
        company = dict(plan["companies"][0])
        self.assertEqual(company["status"], "blocked_missing_reference_identity")
        self.assertEqual(
            company["reference_state"]["missing_reference_snapshot_ids"],
            ["snap-current-missing-local-payload"],
        )
        self.assertEqual(company["archive_plan"]["decisions"]["blocked_missing_reference_identity"], 1)
        self.assertIn(
            "restore_or_rebuild_reference_snapshots",
            {action["action_type"] for action in company["required_actions"]},
        )
        markdown = render_asset_consolidation_plan_markdown(plan)
        self.assertIn("# Asset Consolidation Plan", markdown)
        self.assertIn("restore_or_rebuild_reference_snapshots", markdown)

    def test_consolidation_plan_exposes_payload_backed_candidates_as_review_only(self) -> None:
        self._write_snapshot_dir(
            "openai",
            "snap-current",
            candidates=[
                {
                    "candidate_id": "ada-current",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-current/",
                }
            ],
        )
        self._write_snapshot_dir(
            "openai",
            "snap-reusable",
            latest=True,
            candidates=[
                {
                    "candidate_id": "ada-current",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-current/",
                }
            ],
        )
        self.store.upsert_organization_asset_registry(
            self._snapshot("OpenAI", "snap-current", authoritative=True, selected_snapshot_ids=["snap-current"]),
            authoritative=True,
        )
        self.store.upsert_organization_asset_registry(
            self._snapshot("OpenAI", "snap-reusable", candidate_count=20, profile_detail_count=19)
        )
        self.store.upsert_acquisition_shard_registry(
            build_acquisition_shard_registry_record(
                target_company="OpenAI",
                company_key="openai",
                snapshot_id="snap-reusable",
                lane="profile_search",
                employment_scope="current",
                strategy_type="scoped_search_roster",
                shard_id="agent",
                shard_title="Agent",
                search_query="Agent",
                company_filters={"companies": ["OpenAI"], "search_query": "Agent"},
                result_count=12,
                status="completed",
            )
        )
        report = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
            include_overlap=True,
        )

        plan = build_asset_consolidation_plan(audit_report=report, runtime_dir=self.runtime_dir)

        company = dict(plan["companies"][0])
        replacements = list(company["candidate_authoritative_replacements"])
        self.assertTrue(any(item["snapshot_id"] == "snap-reusable" for item in replacements))
        self.assertIn(
            "review_payload_backed_authoritative_source_candidates",
            {action["action_type"] for action in company["required_actions"]},
        )
        self.assertIn("snap-reusable", company["preserve_snapshots"]["reusable_shard_sources"])

    def test_repair_proposal_verifies_payload_backed_candidate_without_mutating(self) -> None:
        self._write_snapshot_dir(
            "openai",
            "snap-repair-candidate",
            latest=True,
            candidates=[
                {
                    "candidate_id": "ada",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/ada-current/",
                },
                {
                    "candidate_id": "grace",
                    "target_company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/grace-current/",
                },
            ],
        )
        self.store.upsert_organization_asset_registry(
            self._snapshot(
                "OpenAI",
                "snap-missing-reference",
                authoritative=True,
                selected_snapshot_ids=["snap-missing-reference"],
            ),
            authoritative=True,
        )
        self.store.upsert_organization_asset_registry(
            self._snapshot("OpenAI", "snap-repair-candidate", candidate_count=2, profile_detail_count=2)
        )
        self.store.upsert_acquisition_shard_registry(
            build_acquisition_shard_registry_record(
                target_company="OpenAI",
                company_key="openai",
                snapshot_id="snap-repair-candidate",
                lane="profile_search",
                employment_scope="current",
                strategy_type="scoped_search_roster",
                shard_id="agent",
                shard_title="Agent",
                search_query="Agent",
                company_filters={"companies": ["OpenAI"], "search_query": "Agent"},
                result_count=2,
                status="completed",
            )
        )
        audit = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
            include_overlap=True,
        )
        plan = build_asset_consolidation_plan(audit_report=audit, runtime_dir=self.runtime_dir)

        proposal = build_asset_consolidation_repair_proposal(plan=plan, runtime_dir=self.runtime_dir)

        self.assertEqual(proposal["status"], "ready_for_manual_authoritative_repair_review")
        company = dict(proposal["companies"][0])
        self.assertEqual(company["recommended_candidate"]["snapshot_id"], "snap-repair-candidate")
        self.assertEqual(company["recommended_candidate"]["verification_status"], "verified_payload_available")
        self.assertEqual(company["recommended_candidate"]["payload_candidate_count"], 2)
        self.assertEqual(company["recommended_candidate"]["identity_count"], 2)
        self.assertEqual(
            company["recommended_action"],
            "publish_new_payload_backed_authoritative_reference_after_manual_review",
        )
        markdown = render_asset_consolidation_repair_proposal_markdown(proposal)
        self.assertIn("# Asset Consolidation Repair Proposal", markdown)
        self.assertIn("snap-repair-candidate", markdown)

    def test_repair_proposal_blocks_missing_payload_candidate(self) -> None:
        self._write_snapshot_dir("openai", "snap-missing-payload", latest=True)
        self.store.upsert_organization_asset_registry(
            self._snapshot(
                "OpenAI",
                "snap-missing-reference",
                authoritative=True,
                selected_snapshot_ids=["snap-missing-reference"],
            ),
            authoritative=True,
        )
        self.store.upsert_organization_asset_registry(
            self._snapshot("OpenAI", "snap-missing-payload", candidate_count=2, profile_detail_count=2)
        )
        audit = audit_asset_consolidation(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="OpenAI",
            include_overlap=True,
        )
        plan = build_asset_consolidation_plan(audit_report=audit, runtime_dir=self.runtime_dir)

        proposal = build_asset_consolidation_repair_proposal(plan=plan, runtime_dir=self.runtime_dir)

        self.assertEqual(proposal["status"], "blocked_no_verified_payload_candidate")
        company = dict(proposal["companies"][0])
        self.assertEqual(company["recommended_candidate"], {})
        self.assertEqual(company["verified_candidates"][0]["verification_status"], "payload_missing")

    def test_repair_proposal_uses_source_path_payload_when_hot_cache_payload_is_missing(self) -> None:
        hot_snapshot_dir = self.runtime_dir / "hot_cache_company_assets" / "openai" / "snap-repair"
        hot_snapshot_dir.mkdir(parents=True)
        canonical_snapshot_dir = self.runtime_dir / "company_assets" / "openai" / "snap-repair"
        canonical_snapshot_dir.mkdir(parents=True)
        (canonical_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        {
                            "candidate_id": "ada",
                            "linkedin_url": "https://www.linkedin.com/in/ada-current/",
                        },
                        {
                            "candidate_id": "grace",
                            "linkedin_url": "https://www.linkedin.com/in/grace-current/",
                        },
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        artifact_dir = canonical_snapshot_dir / "normalized_artifacts"
        artifact_dir.mkdir()
        source_path = artifact_dir / "artifact_summary.json"
        source_path.write_text(json.dumps({"candidate_count": 2}), encoding="utf-8")
        plan = {
            "contract_version": "asset_consolidation_plan_v1",
            "companies": [
                {
                    "company_key": "openai",
                    "target_company": "OpenAI",
                    "collection_id": "company:openai",
                    "reference_state": {"missing_reference_snapshot_ids": ["snap-missing-reference"]},
                    "candidate_authoritative_replacements": [
                        {
                            "snapshot_id": "snap-repair",
                            "classification": "keep_reusable_shard_source",
                            "candidate_count": 2,
                            "profile_detail_count": 2,
                            "reusable_shard_count": 1,
                            "local_path": str(hot_snapshot_dir),
                            "local_path_exists": True,
                            "source_path": str(source_path),
                            "source_path_exists": True,
                        }
                    ],
                }
            ],
        }

        proposal = build_asset_consolidation_repair_proposal(plan=plan, runtime_dir=self.runtime_dir)

        candidate = proposal["companies"][0]["recommended_candidate"]
        self.assertEqual(candidate["verification_status"], "verified_payload_available")
        self.assertEqual(candidate["payload_candidate_count"], 2)
        self.assertEqual(candidate["candidate_payload_source"], "source_path")
        self.assertEqual(candidate["source_snapshot_dir"], str(canonical_snapshot_dir))

    def test_repair_proposal_does_not_recommend_count_mismatch_candidate(self) -> None:
        self._write_snapshot_dir(
            "openai",
            "snap-count-mismatch",
            candidates=[
                {
                    "candidate_id": "ada",
                    "linkedin_url": "https://www.linkedin.com/in/ada-current/",
                },
            ],
        )
        plan = {
            "contract_version": "asset_consolidation_plan_v1",
            "companies": [
                {
                    "company_key": "openai",
                    "target_company": "OpenAI",
                    "collection_id": "company:openai",
                    "reference_state": {"missing_reference_snapshot_ids": ["snap-missing-reference"]},
                    "candidate_authoritative_replacements": [
                        {
                            "snapshot_id": "snap-count-mismatch",
                            "classification": "keep_reusable_shard_source",
                            "candidate_count": 2,
                            "profile_detail_count": 2,
                            "reusable_shard_count": 1,
                            "local_path": str(self.runtime_dir / "company_assets" / "openai" / "snap-count-mismatch"),
                            "local_path_exists": True,
                            "source_path": "",
                            "source_path_exists": False,
                        }
                    ],
                }
            ],
        }

        proposal = build_asset_consolidation_repair_proposal(plan=plan, runtime_dir=self.runtime_dir)

        company = dict(proposal["companies"][0])
        self.assertEqual(company["status"], "blocked_no_verified_payload_candidate")
        self.assertEqual(company["recommended_candidate"], {})
        candidate = company["verified_candidates"][0]
        self.assertIn("registry_payload_count_mismatch", candidate["promotion_risks"])


class OrganizationAssetRegistryGenerationGuardTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    """Authoritative promotion must never demote a HIGHER materialization
    generation (2026-07-22 incident: a stale-job recovery reconcile
    re-materialized an old snapshot and flipped authoritative seq 6 → 3)."""

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = self.make_pg_store(Path(self.tempdir.name) / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()
        super().tearDown()

    def _payload(
        self,
        snapshot_id: str,
        *,
        sequence: int,
        generation_key: str = "",
        selected: list[str] | None = None,
        **overrides: object,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "target_company": "OpenAI",
            "company_key": "openai",
            "snapshot_id": snapshot_id,
            "asset_view": "canonical_merged",
            "status": "ready",
            "candidate_count": 10,
            "materialization_generation_key": generation_key or f"gen-{snapshot_id}",
            "materialization_generation_sequence": sequence,
            "selected_snapshot_ids": list(selected if selected is not None else [snapshot_id]),
        }
        payload.update(overrides)
        return payload

    def _authoritative_map(self) -> dict[str, int]:
        rows = self.store.list_organization_asset_registry(target_company="OpenAI")
        return {str(r["snapshot_id"]): int(bool(r["authoritative"])) for r in rows}

    def test_same_lineage_stale_sequence_replay_is_refused(self) -> None:
        self.store.upsert_organization_asset_registry(
            self._payload("snap-current", sequence=6, generation_key="lineage-a"), authoritative=True
        )

        result = self.store.upsert_organization_asset_registry(
            self._payload("snap-stale", sequence=3, generation_key="lineage-a"), authoritative=True
        )

        refusal = result.get("authoritative_promotion_refused")
        self.assertIsNotNone(refusal)
        self.assertEqual(refusal["reason"], "stale_generation_sequence_replay")
        self.assertEqual(refusal["blocking_snapshot_id"], "snap-current")
        self.assertEqual(refusal["blocking_generation_sequence"], 6)
        self.assertEqual(self._authoritative_map(), {"snap-current": 1, "snap-stale": 0})

    def test_source_coverage_regression_is_refused_across_lineages(self) -> None:
        # The 2026-07-22 incident shape: incumbent merged {104157, 041551};
        # a stale-job recovery re-materialized 041551 selecting only itself.
        self.store.upsert_organization_asset_registry(
            self._payload("snap-104157", sequence=6, selected=["snap-104157", "snap-041551"]),
            authoritative=True,
        )

        result = self.store.upsert_organization_asset_registry(
            self._payload("snap-041551", sequence=3, selected=["snap-041551"]), authoritative=True
        )

        refusal = result.get("authoritative_promotion_refused")
        self.assertIsNotNone(refusal)
        self.assertEqual(refusal["reason"], "source_snapshot_coverage_regression")
        self.assertEqual(refusal["blocking_snapshot_id"], "snap-104157")
        self.assertEqual(self._authoritative_map(), {"snap-104157": 1, "snap-041551": 0})

    def test_new_lineage_with_equal_or_wider_coverage_promotes(self) -> None:
        self.store.upsert_organization_asset_registry(
            self._payload("snap-old", sequence=6, selected=["snap-old", "snap-extra"]), authoritative=True
        )

        result = self.store.upsert_organization_asset_registry(
            self._payload("snap-repair", sequence=1, selected=["snap-old", "snap-extra", "snap-repair"]),
            authoritative=True,
        )

        self.assertNotIn("authoritative_promotion_refused", result)
        self.assertEqual(self._authoritative_map(), {"snap-old": 0, "snap-repair": 1})

    def test_same_lineage_higher_sequence_promotes(self) -> None:
        self.store.upsert_organization_asset_registry(
            self._payload("snap-gen6", sequence=6, generation_key="lineage-a", selected=["snap-gen6"]),
            authoritative=True,
        )

        result = self.store.upsert_organization_asset_registry(
            self._payload("snap-gen7", sequence=7, generation_key="lineage-a", selected=["snap-gen6", "snap-gen7"]),
            authoritative=True,
        )

        self.assertNotIn("authoritative_promotion_refused", result)
        self.assertEqual(self._authoritative_map(), {"snap-gen6": 0, "snap-gen7": 1})

    def test_repromoting_the_current_authoritative_snapshot_is_allowed(self) -> None:
        self.store.upsert_organization_asset_registry(
            self._payload("snap-gen6", sequence=6, generation_key="lineage-a"), authoritative=True
        )

        result = self.store.upsert_organization_asset_registry(
            self._payload("snap-gen6", sequence=6, generation_key="lineage-a", candidate_count=11),
            authoritative=True,
        )

        self.assertNotIn("authoritative_promotion_refused", result)
        self.assertEqual(self._authoritative_map(), {"snap-gen6": 1})
        self.assertEqual(int(result["candidate_count"]), 11)

    def test_selection_untracked_writers_keep_previous_promotion_behavior(self) -> None:
        # Escape hatch, documented: writers that record neither a shared
        # lineage key nor source selections cannot be classified as
        # regressions and promote as before the guard existed.
        self.store.upsert_organization_asset_registry(
            self._payload("snap-tracked", sequence=6, selected=["snap-tracked", "snap-extra"]),
            authoritative=True,
        )

        result = self.store.upsert_organization_asset_registry(
            self._payload("snap-legacy", sequence=0, selected=[]), authoritative=True
        )

        self.assertNotIn("authoritative_promotion_refused", result)
        self.assertEqual(self._authoritative_map(), {"snap-tracked": 0, "snap-legacy": 1})


if __name__ == "__main__":
    unittest.main()
