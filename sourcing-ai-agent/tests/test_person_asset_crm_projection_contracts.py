import json
import tempfile
import unittest
from pathlib import Path

from sourcing_agent.crm_migration import CRMTargetCandidateMigrationBackfill
from sourcing_agent.crm_writer import CRMWriter
from sourcing_agent.legacy_public_web_storage import seed_legacy_target_public_web_promotion
from sourcing_agent.person_asset_writer import PersonAssetWriter
from sourcing_agent.serving_projection_migration import ServingProjectionMigrationBackfill
from sourcing_agent.serving_projection_reader import ServingProjectionReader
from sourcing_agent.serving_projection_writer import ServingProjectionWriter

from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class PersonAssetCrmProjectionContractTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self.store = self.make_pg_store(self.runtime_dir / "sourcing_agent.db")
        self.projection_writer = ServingProjectionWriter(self.store)
        self.projection_reader = ServingProjectionReader(self.store)
        self.crm_writer = CRMWriter(self.store)
        self.person_asset_writer = PersonAssetWriter(self.store)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_projection_members_use_shared_person_identity_and_summary_view(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-openai-agent",
            collection_id="company:openai",
            projection_id="proj_identity",
            members=[
                {
                    "candidate_id": "ada",
                    "rank_index": 1,
                    "public_summary": {
                        "display_name": "Ada Example",
                        "linkedin_url": "https://www.linkedin.com/in/ada-example/",
                        "headline": "Agent systems engineer",
                    },
                }
            ],
            replace_members=True,
        )

        page = self.projection_reader.get_projection_candidates("proj_identity", limit=10)
        row = page["candidates"][0]

        self.assertEqual(row["person_identity_key"], "linkedin:https://www.linkedin.com/in/ada-example")
        self.assertEqual(row["profile_url_key"], "https://www.linkedin.com/in/ada-example")
        self.assertEqual(row["public_summary"]["person_identity_key"], "linkedin:https://www.linkedin.com/in/ada-example")
        self.assertEqual(row["public_summary"]["source_projection_id"], "proj_identity")

    def test_projection_search_index_publishes_canonical_public_facet_counts(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-google-vision",
            collection_id="company:google",
            projection_id="proj_google_facets",
            members=[
                {
                    "candidate_id": "google-current",
                    "rank_index": 1,
                    "employment_scope": "current",
                    "public_summary": {
                        "display_name": "Current Vision",
                        "linkedin_url": "https://www.linkedin.com/in/current-vision/",
                        "headline": "Vision language engineer",
                        "location": "Mountain View, California",
                        "matched_keywords": ["Vision Language"],
                    },
                },
                {
                    "candidate_id": "google-former",
                    "rank_index": 2,
                    "employment_scope": "former",
                    "public_summary": {
                        "display_name": "Former Vision",
                        "linkedin_url": "https://www.linkedin.com/in/former-vision/",
                        "headline": "Former ML researcher",
                        "location": "Shanghai, China",
                        "matched_keywords": ["Vision Language"],
                    },
                },
            ],
            replace_members=True,
            counts={"result_count": 2, "candidate_count": 2, "count_scope": "exact_projection"},
        )

        before = self.projection_reader.get_projection_candidates("proj_google_facets", limit=10)
        self.assertEqual(before["facet_summary"]["status"], "unavailable")
        self.assertEqual(before["filter_contract"]["facet_count_scope"], "unavailable")
        self.assertEqual(
            before["filter_contract"]["facet_unavailable_reason"],
            "projection_facet_build_product_missing",
        )
        index_result = self.person_asset_writer.rebuild_projection_person_search_index(
            projection_id="proj_google_facets",
            count_scope="exact_projection",
            member_page_size=1,
        )
        after = self.projection_reader.get_projection_candidates("proj_google_facets", limit=10)
        facet_summary = after["facet_summary"]
        employment_counts = {str(item["id"]): int(item["count"]) for item in facet_summary["employment"]}
        recall_counts = {str(item["id"]): int(item["count"]) for item in facet_summary["recall"]}
        projection = self.store.get_serving_projection("proj_google_facets")
        facet_counts = dict(dict(projection.get("counts") or {}).get("public_facet_counts") or {})

        self.assertEqual(index_result["status"], "indexed")
        self.assertEqual(index_result["indexed_count"], 2)
        self.assertEqual(facet_summary["status"], "complete")
        self.assertEqual(facet_summary["count_scope"], "exact_projection")
        self.assertEqual(after["filter_contract"]["facet_count_scope"], "exact_projection")
        self.assertEqual(
            after["filter_contract"]["facet_summary_source"],
            "serving_projection_public_facet_counts",
        )
        self.assertEqual(after["filter_contract"]["facet_summary_projection_id"], "proj_google_facets")
        self.assertEqual(employment_counts["current"], 1)
        self.assertEqual(employment_counts["former"], 1)
        self.assertEqual(recall_counts["all"], 2)
        self.assertEqual(facet_counts["source"], "projection_person_search_index")
        self.assertFalse(facet_counts["truncated"])

    def test_person_asset_evidence_assertion_are_separate_from_projection_rows(self) -> None:
        asset = self.person_asset_writer.record_asset(
            {
                "asset_id": "pa_avatar",
                "person_identity_key": "linkedin:ada-example",
                "asset_type": "avatar_media",
                "source_kind": "harvest",
                "content_ref": "s3://assets/avatar.png",
                "visibility_scope": "public_summary",
            }
        )
        evidence = self.person_asset_writer.record_evidence(
            {
                "evidence_id": "pe_homepage",
                "person_identity_key": "linkedin:ada-example",
                "asset_id": asset["asset_id"],
                "evidence_type": "profile_link",
                "value": "https://ada.example/",
                "publishable": True,
            }
        )
        assertion = self.person_asset_writer.record_assertion(
            {
                "assertion_id": "pass_homepage",
                "person_identity_key": "linkedin:ada-example",
                "assertion_type": "homepage_url",
                "value": "https://ada.example/",
                "authority": "operator_confirmed",
                "verification_status": "active",
                "source_evidence_id": evidence["evidence_id"],
            }
        )

        self.assertEqual(self.store.list_person_assets(person_identity_key="linkedin:ada-example")[0]["asset_id"], "pa_avatar")
        self.assertEqual(self.store.list_person_evidence(asset_id="pa_avatar")[0]["evidence_id"], "pe_homepage")
        self.assertEqual(
            self.store.list_person_assertions(person_identity_key="linkedin:ada-example")[0]["assertion_id"],
            assertion["assertion_id"],
        )
        self.assertEqual(assertion["metadata"]["writer_id"], "person_asset_writer_v1")

    def test_crm_writer_adds_projection_member_without_public_reader_mutation(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-lovable",
            collection_id="company:lovable-dev",
            projection_id="proj_crm",
            members=[
                {
                    "candidate_id": "lovable-ada",
                    "candidate_identity_key": "linkedin:lovable-ada",
                    "public_summary": {
                        "display_name": "Lovable Ada",
                        "linkedin_url": "https://www.linkedin.com/in/lovable-ada/",
                    },
                }
            ],
            replace_members=True,
        )

        result = self.crm_writer.add_projection_member_to_crm(
            projection_id="proj_crm",
            candidate_identity_key="linkedin:lovable-ada",
            actor_type="user",
            actor_id="operator",
            idempotency_key="add-lovable-ada",
            stage="researching",
        )
        idempotent = self.crm_writer.add_projection_member_to_crm(
            projection_id="proj_crm",
            candidate_identity_key="linkedin:lovable-ada",
            actor_type="user",
            actor_id="operator",
            idempotency_key="add-lovable-ada",
            stage="researching",
        )
        page = self.projection_reader.get_projection_candidates("proj_crm", limit=10)
        row = page["candidates"][0]

        self.assertEqual(result["status"], "upserted")
        self.assertEqual(idempotent["status"], "idempotent")
        self.assertTrue(row["crm_overlay_summary"]["in_crm"])
        self.assertEqual(row["crm_overlay_summary"]["stage"], "researching")
        self.assertEqual(len(self.store.list_crm_engagements(crm_record_id=result["crm_record"]["crm_record_id"])), 1)

    def test_migration_backfills_legacy_job_pages_into_run_scope_projection(self) -> None:
        pages = {
            0: {
                "candidates": [
                    {
                        "candidate_id": "one",
                        "display_name": "One",
                        "linkedin_url": "https://www.linkedin.com/in/one/",
                    }
                ],
                "has_more": True,
                "next_offset": 1,
            },
            1: {
                "candidates": [
                    {
                        "candidate_id": "two",
                        "display_name": "Two",
                        "linkedin_url": "https://www.linkedin.com/in/two/",
                    }
                ],
                "has_more": False,
            },
        }
        backfill = ServingProjectionMigrationBackfill(self.store)

        result = backfill.backfill_run_scope_projection(
            run_id="job-legacy",
            collection_id="company:legacy",
            candidate_page_loader=lambda offset, _limit: pages[offset],
            scope_label="Legacy job",
        )

        self.assertEqual(result["status"], "backfilled")
        self.assertEqual(result["member_count"], 2)
        self.assertEqual(self.store.get_run_projection_link("job-legacy")["projection_id"], result["projection_id"])
        self.assertEqual(self.store.count_serving_projection_members(result["projection_id"]), 2)

    def test_person_summary_backfill_repairs_legacy_projection_rows_without_reader_fallback(self) -> None:
        payload_path = self.runtime_dir / "company_assets" / "legacy" / "snap" / "normalized_artifacts"
        payload_path.mkdir(parents=True)
        candidate_payload_path = payload_path / "materialized_candidate_documents.json"
        candidate_payload_path.write_text(
            json.dumps(
                {
                    "candidates": [
                        {
                            "candidate_id": "ada",
                            "display_name": "Legacy Ada",
                            "linkedin_url": "https://www.linkedin.com/in/legacy-summary-ada/",
                            "metadata": {
                                "experience_lines": ["2025~Present, OpenAI, Agent Engineer"],
                                "education_lines": ["MS, Example University"],
                                "about": "Builds durable agent systems.",
                            },
                            "has_profile_detail": True,
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        self.store.upsert_serving_projection(
            {
                "projection_id": "proj_legacy_summary",
                "projection_type": "run_scope_projection",
                "source_run_id": "job-legacy-summary",
                "state": "serving",
                "provenance": {"candidate_payload_path": str(candidate_payload_path)},
            }
        )
        self.store.upsert_serving_projection_members(
            "proj_legacy_summary",
            [
                {
                    "candidate_identity_key": "legacy-row-ada",
                    "candidate_id": "ada",
                    "public_summary": {
                        "display_name": "Legacy Ada",
                        "linkedin_url": "https://www.linkedin.com/in/legacy-summary-ada/",
                    },
                }
            ],
        )
        before = self.store.get_serving_projection_member("proj_legacy_summary", "legacy-row-ada")
        self.assertEqual(before["person_identity_key"], "linkedin:https://www.linkedin.com/in/legacy-summary-ada")
        legacy_summary = dict(before["public_summary"])
        legacy_summary.pop("source_projection_id", None)
        legacy_summary.pop("source_run_id", None)
        with self.store._lock, self.store._connection:  # noqa: SLF001 - migration regression fixture
            self.store._connection.execute(  # noqa: SLF001
                """
                UPDATE serving_projection_members
                SET public_summary_json = ?
                WHERE projection_id = ? AND candidate_identity_key = ?
                """,
                (json.dumps(legacy_summary), "proj_legacy_summary", "legacy-row-ada"),
            )
        backfill = ServingProjectionMigrationBackfill(self.store)

        result = backfill.backfill_person_summary_views(projection_ids=["proj_legacy_summary"])
        row = self.store.get_serving_projection_member("proj_legacy_summary", "legacy-row-ada")

        self.assertEqual(result["status"], "backfilled")
        self.assertEqual(result["changed_member_count"], 1)
        self.assertEqual(row["candidate_identity_key"], "legacy-row-ada")
        self.assertEqual(row["profile_url_key"], "https://www.linkedin.com/in/legacy-summary-ada")
        self.assertEqual(row["public_summary"]["source_projection_id"], "proj_legacy_summary")
        self.assertEqual(row["public_summary"]["source_run_id"], "job-legacy-summary")
        self.assertEqual(row["public_summary"]["experience_lines"], ["2025~Present, OpenAI, Agent Engineer"])
        self.assertEqual(row["public_summary"]["education_lines"], ["MS, Example University"])
        self.assertEqual(row["public_summary"]["summary"], "Builds durable agent systems.")
        self.assertEqual(row["profile_readiness"], "ready")
        self.assertTrue(row["public_summary"]["has_profile_detail"])
        self.assertFalse(row["public_summary"]["needs_profile_completion"])
        self.assertEqual(result["projections"][0]["source_payload_match_count"], 1)

    def test_local_asset_collection_backfill_publishes_missing_company_pointer_only(self) -> None:
        existing = self.projection_writer.publish_collection_authoritative_projection(
            collection_id="company:existingco",
            active_collection_version="snap-existing",
            projection_id="proj_existing",
            members=[
                {
                    "candidate_identity_key": "linkedin:existing-ada",
                    "public_summary": {
                        "display_name": "Existing Ada",
                        "linkedin_url": "https://www.linkedin.com/in/existing-ada/",
                    },
                }
            ],
            replace_members=True,
        )
        self.assertEqual(existing["pointer"]["active_projection_id"], "proj_existing")
        new_snapshot_dir = self.runtime_dir / "company_assets" / "newco" / "20260501T000000"
        new_artifact_dir = new_snapshot_dir / "normalized_artifacts"
        new_artifact_dir.mkdir(parents=True)
        (new_artifact_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        {
                            "candidate_id": "new-ada",
                            "display_name": "New Ada",
                            "linkedin_url": "https://www.linkedin.com/in/new-ada/",
                            "metadata": {
                                "experience_lines": ["2026~Present, NewCo, Research Engineer"],
                                "education_lines": ["PhD, Example University"],
                            },
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        newer_shell_snapshot_dir = self.runtime_dir / "company_assets" / "newco" / "20260502T000000"
        newer_shell_snapshot_dir.mkdir(parents=True)
        (newer_shell_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        {
                            "candidate_id": "new-ada",
                            "display_name": "New Ada Shell",
                            "linkedin_url": "https://www.linkedin.com/in/new-ada/",
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        existing_snapshot_dir = self.runtime_dir / "company_assets" / "existingco" / "20260501T000000"
        existing_snapshot_dir.mkdir(parents=True)
        (existing_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": [{"candidate_id": "other"}]}, ensure_ascii=False),
            encoding="utf-8",
        )
        backfill = ServingProjectionMigrationBackfill(self.store, writer_id="local_asset_collection_backfill_test")

        result = backfill.backfill_collection_authoritative_local_asset_snapshots(
            runtime_dir=self.runtime_dir,
            dry_run=False,
        )
        new_pointer = self.store.get_collection_authoritative_pointer("company:newco")
        existing_pointer = self.store.get_collection_authoritative_pointer("company:existingco")
        new_members = self.store.list_serving_projection_members(new_pointer["active_projection_id"], limit=10)

        self.assertEqual(result["status"], "backfilled")
        self.assertEqual(result["planned_count"], 1)
        self.assertEqual(result["applied_count"], 1)
        self.assertEqual(result["skipped_existing_count"], 1)
        self.assertEqual(existing_pointer["active_projection_id"], "proj_existing")
        self.assertTrue(new_pointer["active_projection_id"].startswith("proj_localasset_"))
        self.assertEqual(new_pointer["active_collection_version"], "20260501T000000")
        self.assertEqual(new_members[0]["profile_readiness"], "ready")
        self.assertEqual(new_members[0]["public_summary"]["experience_lines"], ["2026~Present, NewCo, Research Engineer"])
        self.assertEqual(new_members[0]["public_summary"]["source_snapshot_id"], "20260501T000000")

    def test_collection_projection_layer_backfill_writes_canonical_member_fields(self) -> None:
        self.projection_writer.publish_collection_authoritative_projection(
            collection_id="company:anthropic",
            active_collection_version="snap-layer",
            projection_id="proj_collection_layer",
            members=[
                {
                    "candidate_identity_key": "linkedin:zhang-wei-layer",
                    "candidate_id": "zhang-wei-layer",
                    "public_summary": {
                        "display_name": "Zhang Wei",
                        "linkedin_url": "https://www.linkedin.com/in/zhang-wei-layer/",
                        "headline": "Research Engineer",
                        "experience_lines": ["2024~Present, Anthropic, Research Engineer"],
                        "education_lines": ["PhD, Tsinghua University"],
                        "has_profile_detail": True,
                    },
                }
            ],
            replace_members=True,
        )
        backfill = ServingProjectionMigrationBackfill(self.store, writer_id="collection_layer_backfill_test")

        result = backfill.backfill_projection_layer_assignments(projection_ids=["proj_collection_layer"])
        row = self.store.get_serving_projection_member("proj_collection_layer", "linkedin:zhang-wei-layer")
        projection = self.store.get_serving_projection("proj_collection_layer")

        self.assertEqual(result["status"], "backfilled")
        self.assertEqual(result["processed_member_count"], 1)
        self.assertEqual(row["public_summary"]["outreach_layer"], 3)
        self.assertEqual(row["public_summary"]["outreach_layer_key"], "layer_3_mainland_china_experience_or_chinese_language")
        self.assertEqual(row["public_summary"]["outreach_layer_key"], row["metadata"]["outreach_layer_key"])
        self.assertEqual(projection["readiness"]["layering"], "complete")
        self.assertEqual(projection["metadata"]["layer_assignment_source"], "collection_projection_layer_backfill")

    def test_target_candidate_migration_creates_person_first_crm_and_email_assertion(self) -> None:
        self.store.upsert_target_candidate(
            {
                "candidate_id": "legacy-ada",
                "candidate_name": "Legacy Ada",
                "headline": "Founder",
                "current_company": "Example",
                "linkedin_url": "https://www.linkedin.com/in/legacy-ada/",
                "primary_email": "ada@example.com",
                "follow_up_status": "pending_outreach",
                "source_projection_id": "proj_legacy",
                "source_run_id": "job-legacy",
            }
        )
        backfill = CRMTargetCandidateMigrationBackfill(self.store)

        result = backfill.backfill()
        crm_records = self.store.list_crm_records(source_projection_id="proj_legacy")
        assertions = self.store.list_person_assertions(
            person_identity_key="linkedin:https://www.linkedin.com/in/legacy-ada",
            assertion_type="primary_email",
        )

        self.assertEqual(result["status"], "backfilled")
        self.assertEqual(result["migrated_count"], 1)
        self.assertEqual(result["assertions_created"], 1)
        self.assertEqual(crm_records[0]["metadata"]["current_stage"], "outreach_ready")
        self.assertEqual(assertions[0]["value"], "ada@example.com")
        self.assertEqual(assertions[0]["authority"], "legacy_migrated")
        self.assertEqual(assertions[0]["verification_status"], "needs_review")

    def test_public_web_promotion_migration_creates_human_promoted_assertion_and_event(self) -> None:
        seed_legacy_target_public_web_promotion(
            self.store,
            {
                "promotion_id": "promotion-ada-homepage",
                "signal_id": "signal-ada-homepage",
                "run_id": "public-web-run-ada",
                "person_identity_key": "linkedin:ada-public-web",
                "record_id": "target-ada",
                "candidate_id": "ada",
                "candidate_name": "Ada Public Web",
                "signal_kind": "profile_link",
                "signal_type": "personal_homepage",
                "url": "https://ada.example/",
                "normalized_value": "https://ada.example/",
                "new_value": "https://ada.example/",
                "source_url": "https://ada.example/about",
                "source_domain": "ada.example",
                "source_family": "profile_web_presence",
                "confidence_score": 0.92,
                "publishable": True,
                "action": "promote",
                "promoted_field": "public_web_profile_link",
                "operator": "unit-test",
            }
        )
        backfill = CRMTargetCandidateMigrationBackfill(
            self.store,
            crm_writer=CRMWriter(self.store, writer_id="public_web_promotion_migration_v1"),
            person_asset_writer=PersonAssetWriter(self.store, writer_id="public_web_promotion_migration_v1"),
            writer_id="public_web_promotion_migration_v1",
        )

        result = backfill.backfill_public_web_promotions()
        assertions = self.store.list_person_assertions(
            person_identity_key="linkedin:ada-public-web",
            assertion_type="homepage_url",
        )
        event = self.store.get_crm_event_by_idempotency(
            "crm:migrate-public-web-promotion:promotion-ada-homepage"
        )

        self.assertEqual(result["status"], "backfilled")
        self.assertEqual(result["migrated_count"], 1)
        self.assertEqual(result["assertions_created"], 1)
        self.assertEqual(result["crm_events_created"], 1)
        self.assertEqual(assertions[0]["value"], "https://ada.example/")
        self.assertEqual(assertions[0]["authority"], "operator_confirmed")
        self.assertEqual(assertions[0]["verification_status"], "active")
        self.assertEqual(assertions[0]["metadata"]["source"], "legacy_target_candidate_public_web_promotion")
        self.assertEqual(event["event_type"], "person_assertion_linked")
        self.assertEqual(event["payload"]["assertion_type"], "homepage_url")

    def test_projection_reader_person_detail_and_summary_are_public_safe(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-person-detail",
            collection_id="company:openai",
            projection_id="proj_person_detail",
            members=[
                {
                    "candidate_identity_key": "linkedin:detail-ada",
                    "person_identity_key": "linkedin:detail-ada",
                    "candidate_id": "detail-ada",
                    "public_summary": {
                        "display_name": "Detail Ada",
                        "linkedin_url": "https://www.linkedin.com/in/detail-ada/",
                        "primary_email": "must-not-leak@example.com",
                        "raw_profile": {"hidden": True},
                    },
                }
            ],
            replace_members=True,
        )
        self.person_asset_writer.record_assertion(
            {
                "assertion_id": "assertion-detail-active-email",
                "person_identity_key": "linkedin:detail-ada",
                "assertion_type": "primary_email",
                "value": "active@example.com",
                "authority": "operator_confirmed",
                "verification_status": "active",
            }
        )
        self.person_asset_writer.record_assertion(
            {
                "assertion_id": "assertion-detail-review-email",
                "person_identity_key": "linkedin:detail-ada",
                "assertion_type": "primary_email",
                "value": "review@example.com",
                "authority": "agent_suggested",
                "verification_status": "needs_review",
            }
        )
        self.person_asset_writer.record_asset(
            {
                "asset_id": "avatar-detail-ada",
                "person_identity_key": "linkedin:detail-ada",
                "asset_type": "avatar_media",
                "source_kind": "media_cache",
                "content_ref": "https://static.example.com/avatar-detail-ada.png",
                "source_url": "https://provider.example.com/avatar-detail-ada",
                "visibility_scope": "public_summary",
                "status": "available",
            }
        )

        detail = self.projection_reader.get_projection_person_detail(
            "proj_person_detail",
            "linkedin:detail-ada",
        )
        summary = self.projection_reader.get_person_summary("linkedin:detail-ada")
        page = self.projection_reader.get_projection_candidates("proj_person_detail", limit=10)

        self.assertEqual(detail["status"], "ready")
        self.assertEqual(detail["read_contract"]["source"], "serving_projection_members+person_assets+crm_records")
        self.assertNotIn("primary_email", detail["public_summary"])
        self.assertNotIn("raw_profile", detail["public_summary"])
        active_assertion = next(item for item in detail["assertions"] if item["assertion_id"] == "assertion-detail-active-email")
        review_assertion = next(item for item in detail["assertions"] if item["assertion_id"] == "assertion-detail-review-email")
        self.assertEqual(active_assertion["value"], "active@example.com")
        self.assertTrue(review_assertion["value_redacted"])
        self.assertNotIn("value", review_assertion)
        self.assertEqual(detail["media_summary"]["avatar_status"], "available")
        self.assertEqual(detail["media_summary"]["avatar_asset_id"], "avatar-detail-ada")
        self.assertEqual(detail["media_summary"]["avatar_url"], "https://static.example.com/avatar-detail-ada.png")
        self.assertEqual(detail["media_summary"]["media_contract"]["source"], "PersonAsset.avatar_media")
        self.assertFalse(detail["media_summary"]["media_contract"]["fallback_used"])
        self.assertEqual(summary["status"], "ready")
        self.assertEqual(summary["projection_membership_count"], 1)
        self.assertEqual(summary["assertion_summary"]["assertion_count"], 2)
        self.assertEqual(summary["media_summary"]["avatar_asset_id"], "avatar-detail-ada")
        self.assertEqual(page["status"], "ready")
        self.assertEqual(page["candidates"][0]["media_summary"]["avatar_asset_id"], "avatar-detail-ada")
        self.assertEqual(page["candidates"][0]["media_summary"]["avatar_url"], "https://static.example.com/avatar-detail-ada.png")

    def test_person_media_summary_does_not_promote_provider_hotlink_without_asset(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-person-hotlink",
            collection_id="company:openai",
            projection_id="proj_person_hotlink",
            members=[
                {
                    "candidate_identity_key": "linkedin:hotlink-ada",
                    "person_identity_key": "linkedin:hotlink-ada",
                    "candidate_id": "hotlink-ada",
                    "public_summary": {
                        "display_name": "Hotlink Ada",
                        "linkedin_url": "https://www.linkedin.com/in/hotlink-ada/",
                        "avatar_url": "https://provider.example.com/hotlink-ada.jpg",
                    },
                }
            ],
            replace_members=True,
        )

        detail = self.projection_reader.get_projection_person_detail(
            "proj_person_hotlink",
            "linkedin:hotlink-ada",
        )
        summary = self.projection_reader.get_person_summary("linkedin:hotlink-ada")
        page = self.projection_reader.get_projection_candidates("proj_person_hotlink", limit=10)

        self.assertEqual(detail["media_summary"]["avatar_status"], "avatar_unavailable")
        self.assertEqual(detail["media_summary"]["avatar_url"], "")
        self.assertEqual(detail["media_summary"]["media_contract"]["source"], "PersonAsset.avatar_media")
        self.assertFalse(detail["media_summary"]["media_contract"]["fallback_used"])
        self.assertEqual(summary["media_summary"]["avatar_status"], "avatar_unavailable")
        self.assertEqual(page["candidates"][0]["media_summary"]["avatar_status"], "avatar_unavailable")
        self.assertEqual(page["candidates"][0]["media_summary"]["avatar_url"], "")

    def test_projection_readiness_and_index_filter_contract_are_served_from_membership(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-readiness",
            collection_id="company:openai",
            projection_id="proj_readiness",
            members=[
                {
                    "candidate_identity_key": "linkedin:ready",
                    "person_identity_key": "linkedin:ready",
                    "profile_readiness": "ready",
                    "card_readiness": "ready",
                    "projection_metrics": {"profile_required": True},
                },
                {
                    "candidate_identity_key": "linkedin:pending",
                    "person_identity_key": "linkedin:pending",
                    "profile_readiness": "pending",
                    "card_readiness": "unknown",
                    "projection_metrics": {"profile_required": True},
                },
                {
                    "candidate_identity_key": "linkedin:not-required",
                    "person_identity_key": "linkedin:not-required",
                    "profile_readiness": "not_required",
                    "card_readiness": "ready",
                },
            ],
            readiness={"index_count_scope": "index_partial", "profile_indexed_at": "2026-05-19T10:00:00+08:00"},
            replace_members=True,
        )

        projection = self.projection_reader.get_projection("proj_readiness")["projection"]
        page = self.projection_reader.get_projection_candidates("proj_readiness", limit=10)

        self.assertEqual(projection["readiness"]["row_count"], 3)
        self.assertEqual(projection["readiness"]["profile_required_count"], 2)
        self.assertEqual(projection["readiness"]["profile_ready_count"], 1)
        self.assertEqual(projection["readiness"]["card_ready_count"], 2)
        self.assertEqual(page["index_filter_readiness"]["count_scope"], "index_partial")
        self.assertEqual(page["index_filter_readiness"]["freshness_timezone"], "Asia/Shanghai")

    def test_projection_person_search_index_serves_keyword_filter_without_raw_payload_leak(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-index",
            collection_id="company:openai",
            projection_id="proj_index",
            members=[
                {
                    "candidate_identity_key": "linkedin:agent-ada",
                    "person_identity_key": "linkedin:agent-ada",
                    "candidate_id": "agent-ada",
                    "public_summary": {
                        "display_name": "Agent Ada",
                        "headline": "Systems engineer",
                        "linkedin_url": "https://www.linkedin.com/in/agent-ada/",
                    },
                },
                {
                    "candidate_identity_key": "linkedin:policy-ben",
                    "person_identity_key": "linkedin:policy-ben",
                    "candidate_id": "policy-ben",
                    "public_summary": {
                        "display_name": "Policy Ben",
                        "headline": "Policy lead",
                        "linkedin_url": "https://www.linkedin.com/in/policy-ben/",
                    },
                },
            ],
            replace_members=True,
        )
        self.person_asset_writer.record_asset(
            {
                "asset_id": "pa_agent_raw",
                "person_identity_key": "linkedin:agent-ada",
                "asset_type": "raw_profile",
                "visibility_scope": "internal",
                "metadata": {"indexed_terms": ["langgraph orchestration", "agent runtime"]},
            }
        )
        self.person_asset_writer.record_evidence(
            {
                "evidence_id": "pe_agent_homepage",
                "person_identity_key": "linkedin:agent-ada",
                "evidence_type": "homepage",
                "value": "LangGraph systems notes",
                "source_domain": "ada.example",
                "publishable": True,
            }
        )

        result = self.person_asset_writer.rebuild_projection_person_search_index(
            projection_id="proj_index",
            count_scope="exact_projection",
            raw_profile_index_watermark="raw-wm-1",
            evidence_index_watermark="evidence-wm-1",
        )
        search = self.projection_reader.search_projection_person_index(
            "proj_index",
            search_keyword="LangGraph",
        )
        filtered_page = self.projection_reader.get_projection_candidates(
            "proj_index",
            candidate_filter={"search_keyword": "LangGraph"},
            limit=10,
        )
        empty_search = self.projection_reader.search_projection_person_index(
            "proj_index",
            search_keyword="nonexistent-token",
        )

        self.assertEqual(result["status"], "indexed")
        self.assertEqual(result["indexed_count"], 2)
        self.assertEqual(search["status"], "ready")
        self.assertEqual(search["filtered_candidate_count"], 1)
        self.assertEqual(search["candidates"][0]["candidate_identity_key"], "linkedin:agent-ada")
        self.assertEqual(search["read_contract"]["source"], "projection_person_search_index+serving_projection_members")
        self.assertNotIn("raw_profile", search["candidates"][0]["public_summary"])
        self.assertEqual(filtered_page["filtered_candidate_count"], 1)
        self.assertEqual(filtered_page["filter_contract"]["source"], "projection_person_search_index")
        self.assertEqual(filtered_page["index_filter_readiness"]["count_scope"], "exact_projection")
        self.assertEqual(filtered_page["index_filter_readiness"]["raw_profile_index_watermark"], "raw-wm-1")
        self.assertEqual(empty_search["filtered_candidate_count"], 0)
        self.assertEqual(empty_search["index_filter_readiness"]["count_scope"], "exact_projection")
        self.assertEqual(empty_search["index_filter_readiness"]["raw_profile_index_watermark"], "raw-wm-1")

    def test_projection_person_search_index_uses_person_level_raw_and_evidence_indexes(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-person-index",
            collection_id="company:openai",
            projection_id="proj_person_index",
            members=[
                {
                    "candidate_identity_key": "linkedin:person-index-ada",
                    "person_identity_key": "linkedin:person-index-ada",
                    "candidate_id": "person-index-ada",
                    "public_summary": {
                        "display_name": "Person Index Ada",
                        "headline": "Retrieval engineer",
                        "linkedin_url": "https://www.linkedin.com/in/person-index-ada/",
                    },
                }
            ],
            replace_members=True,
        )
        self.person_asset_writer.record_asset(
            {
                "asset_id": "pa_person_index_raw",
                "person_identity_key": "linkedin:person-index-ada",
                "asset_type": "raw_profile",
                "metadata": {"indexed_terms": ["vector database retrieval"]},
            }
        )
        self.person_asset_writer.record_evidence(
            {
                "evidence_id": "pe_person_index_blog",
                "person_identity_key": "linkedin:person-index-ada",
                "evidence_type": "public_web_profile",
                "value": "Substack notes about retrieval agents",
                "source_domain": "substack.com",
                "publishable": True,
            }
        )

        person_index = self.person_asset_writer.rebuild_person_indexes_for_projection(
            projection_id="proj_person_index",
            raw_profile_index_watermark="raw-person-wm-1",
            evidence_index_watermark="evidence-person-wm-1",
        )
        projection_index = self.person_asset_writer.rebuild_projection_person_search_index(
            projection_id="proj_person_index",
            count_scope="exact_projection",
            raw_profile_index_watermark="raw-person-wm-1",
            evidence_index_watermark="evidence-person-wm-1",
        )
        raw_index = self.store.get_raw_profile_index("linkedin:person-index-ada")
        evidence_index = self.store.get_candidate_evidence_index("linkedin:person-index-ada")
        vector_search = self.projection_reader.search_projection_person_index("proj_person_index", search_keyword="vector")
        substack_search = self.projection_reader.search_projection_person_index("proj_person_index", search_keyword="Substack")
        rows = self.store._list_projection_person_search_index_rows("proj_person_index", limit=10)  # noqa: SLF001

        self.assertEqual(person_index["status"], "indexed")
        self.assertEqual(person_index["raw_profile_indexed_count"], 1)
        self.assertEqual(person_index["candidate_evidence_indexed_count"], 1)
        self.assertEqual(projection_index["status"], "indexed")
        self.assertIn("vector database retrieval", raw_index["raw_profile_terms"])
        self.assertIn("substack notes about retrieval agents", evidence_index["evidence_terms"])
        self.assertEqual(vector_search["filtered_candidate_count"], 1)
        self.assertEqual(substack_search["filtered_candidate_count"], 1)
        self.assertTrue(rows[0]["indexed_field_sources"]["raw_profile_index"])
        self.assertTrue(rows[0]["indexed_field_sources"]["candidate_evidence_index"])
        self.assertNotIn("person_assets", rows[0]["indexed_field_sources"])

    def test_projection_person_search_index_backfill_rebuilds_stale_projection_indexes(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-index-backfill",
            collection_id="company:openai",
            projection_id="proj_index_backfill",
            members=[
                {
                    "candidate_identity_key": "linkedin:index-backfill-ada",
                    "person_identity_key": "linkedin:index-backfill-ada",
                    "candidate_id": "index-backfill-ada",
                    "public_summary": {
                        "display_name": "Index Backfill Ada",
                        "headline": "Raw profile indexing engineer",
                        "linkedin_url": "https://www.linkedin.com/in/index-backfill-ada/",
                    },
                }
            ],
            replace_members=True,
        )
        backfill = ServingProjectionMigrationBackfill(self.store)

        result = backfill.backfill_projection_person_search_indexes(projection_ids=["proj_index_backfill"])
        search = self.projection_reader.search_projection_person_index("proj_index_backfill", search_keyword="indexing")

        self.assertEqual(result["status"], "backfilled")
        self.assertEqual(result["rebuilt_count"], 1)
        self.assertEqual(result["projections"][0]["indexed_count"], 1)
        self.assertEqual(search["status"], "ready")
        self.assertEqual(search["filtered_candidate_count"], 1)
        self.assertEqual(search["index_filter_readiness"]["count_scope"], "exact_projection")

    def test_projection_person_search_index_backfill_can_skip_person_index_rebuild(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-index-backfill-fast",
            collection_id="company:openai",
            projection_id="proj_index_backfill_fast",
            members=[
                {
                    "candidate_identity_key": "linkedin:index-backfill-fast-ada",
                    "person_identity_key": "linkedin:index-backfill-fast-ada",
                    "candidate_id": "index-backfill-fast-ada",
                    "public_summary": {
                        "display_name": "Fast Index Ada",
                        "headline": "Projection filter engineer",
                        "linkedin_url": "https://www.linkedin.com/in/index-backfill-fast-ada/",
                    },
                }
            ],
            replace_members=True,
        )
        self.person_asset_writer.record_asset(
            {
                "asset_id": "raw_fast_ada",
                "person_identity_key": "linkedin:index-backfill-fast-ada",
                "asset_type": "raw_profile",
                "source_kind": "test",
                "content_ref": "memory://raw-fast-ada",
                "metadata": {"indexed_terms": ["raw-only-nebula-term"]},
            }
        )
        backfill = ServingProjectionMigrationBackfill(self.store)

        result = backfill.backfill_projection_person_search_indexes(
            projection_ids=["proj_index_backfill_fast"],
            rebuild_person_indexes=False,
        )
        public_search = self.projection_reader.search_projection_person_index(
            "proj_index_backfill_fast",
            search_keyword="filter engineer",
        )
        raw_search = self.projection_reader.search_projection_person_index(
            "proj_index_backfill_fast",
            search_keyword="raw-only-nebula-term",
        )
        row = self.store._list_projection_person_search_index_rows("proj_index_backfill_fast", limit=1)[0]  # noqa: SLF001

        self.assertEqual(result["status"], "backfilled")
        self.assertFalse(result["rebuild_person_indexes"])
        self.assertEqual(result["projections"][0]["indexed_count"], 1)
        self.assertEqual(result["projections"][0]["person_index"]["status"], "skipped")
        self.assertEqual(public_search["filtered_candidate_count"], 1)
        self.assertEqual(raw_search["filtered_candidate_count"], 0)
        self.assertFalse(row["indexed_field_sources"]["raw_profile_index"])


if __name__ == "__main__":
    unittest.main()
