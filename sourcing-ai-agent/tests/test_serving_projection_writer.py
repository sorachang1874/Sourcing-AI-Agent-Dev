import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.person_asset_writer import PersonAssetWriter
from sourcing_agent.serving_projection_reader import ServingProjectionReader
from sourcing_agent.serving_projection_writer import ServingProjectionWriter

from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class ServingProjectionWriterTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self.store = self.make_pg_store(self.runtime_dir / "sourcing_agent.db")
        self.writer = ServingProjectionWriter(self.store)
        self.reader = ServingProjectionReader(self.store)
        self.person_asset_writer = PersonAssetWriter(self.store)

    def tearDown(self) -> None:
        self.tempdir.cleanup()
        super().tearDown()

    def test_publish_run_scope_projection_creates_projection_members_and_link(self) -> None:
        result = self.writer.publish_run_scope_projection(
            run_id="job-openai-agent",
            collection_id="company:openai",
            members=[
                {
                    "candidate_identity_key": "linkedin:ada",
                    "person_identity_key": "linkedin:ada",
                    "rank_index": 1,
                    "public_summary": {"name": "Ada"},
                },
                {
                    "candidate_identity_key": "linkedin:grace",
                    "person_identity_key": "linkedin:grace",
                    "rank_index": 2,
                    "public_summary": {"name": "Grace"},
                },
            ],
            scope_spec={"keywords": ["agent"], "baseline_policy": "delta_only"},
            replace_members=True,
        )

        projection = result["projection"]
        link = result["link"]
        members = self.store.list_serving_projection_members(projection["projection_id"])

        self.assertEqual(projection["projection_type"], "run_scope_projection")
        self.assertEqual(projection["source_run_id"], "job-openai-agent")
        self.assertEqual(projection["counts"]["result_count"], 2)
        self.assertEqual(projection["counts"]["count_scope"], "exact_projection")
        self.assertEqual(link["projection_id"], projection["projection_id"])
        self.assertEqual(self.store.get_run_projection_link("job-openai-agent")["projection_id"], projection["projection_id"])
        self.assertEqual([member["candidate_identity_key"] for member in members], ["linkedin:ada", "linkedin:grace"])

    def test_publish_collection_authoritative_projection_switches_pointer_with_previous_projection(self) -> None:
        first = self.writer.publish_collection_authoritative_projection(
            collection_id="company:google",
            active_collection_version="v1",
            members=[
                {
                    "candidate_identity_key": "linkedin:first",
                    "person_identity_key": "linkedin:first",
                }
            ],
            replace_members=True,
        )
        second = self.writer.publish_collection_authoritative_projection(
            collection_id="company:google",
            active_collection_version="v2",
            members=[
                {
                    "candidate_identity_key": "linkedin:second",
                    "person_identity_key": "linkedin:second",
                }
            ],
            replace_members=True,
        )
        pointer = self.store.get_collection_authoritative_pointer("company:google")

        self.assertEqual(pointer["active_projection_id"], second["projection"]["projection_id"])
        self.assertEqual(pointer["previous_projection_id"], first["projection"]["projection_id"])
        self.assertEqual(pointer["active_collection_version"], "v2")
        self.assertEqual(second["projection"]["projection_type"], "collection_authoritative_projection")

    def test_publish_run_scope_projection_reuses_existing_link_when_projection_id_is_omitted(self) -> None:
        first = self.writer.publish_run_scope_projection(
            run_id="job-idempotent",
            collection_id="company:lovable-dev",
            projection_id="proj_existing_run_scope",
            members=[],
        )
        second = self.writer.publish_run_scope_projection(
            run_id="job-idempotent",
            collection_id="company:lovable-dev",
            members=[
                {
                    "candidate_identity_key": "linkedin:lovable",
                    "person_identity_key": "linkedin:lovable",
                }
            ],
        )

        self.assertEqual(first["projection"]["projection_id"], "proj_existing_run_scope")
        self.assertEqual(second["projection"]["projection_id"], "proj_existing_run_scope")
        self.assertEqual(self.store.count_serving_projection_members("proj_existing_run_scope"), 1)

    def test_reader_fails_closed_and_serves_only_public_projection_fields(self) -> None:
        missing = self.reader.get_projection("proj_missing")
        self.assertEqual(missing["status"], "not_ready")
        self.assertTrue(missing["read_contract"]["fail_closed"])
        self.assertFalse(missing["read_contract"]["fallback_used"])

        self.store.upsert_serving_projection(
            {
                "projection_id": "proj_draft",
                "projection_type": "run_scope_projection",
                "state": "draft",
            }
        )
        draft = self.reader.get_projection("proj_draft")
        self.assertEqual(draft["status"], "not_ready")
        self.assertEqual(draft["reason"], "projection_not_servable")

        self.writer.publish_run_scope_projection(
            run_id="job-reader",
            collection_id="company:openai",
            projection_id="proj_reader",
            members=[
                {
                    "candidate_identity_key": "linkedin:reader",
                    "person_identity_key": "linkedin:reader",
                    "rank_index": 1,
                    "public_summary": {
                        "name": "Reader Candidate",
                        "primary_email": "restricted@example.com",
                        "raw_profile": {"payload": True},
                        "nested": {"debug": {"hidden": True}, "headline": "Engineer"},
                    },
                    "projection_metrics": {"row_readiness": "ready", "raw_payload": {"hidden": True}},
                    "crm_overlay_summary": {"in_crm": False, "crm_notes": "hidden"},
                }
            ],
            replace_members=True,
        )

        page = self.reader.get_projection_candidates("proj_reader", offset=0, limit=10)
        row = page["candidates"][0]

        self.assertEqual(page["status"], "ready")
        self.assertEqual(page["read_contract"]["source"], "serving_projection_members")
        self.assertFalse(page["read_contract"]["fallback_used"])
        self.assertEqual(row["public_summary"]["name"], "Reader Candidate")
        self.assertEqual(row["public_summary"]["nested"], {"headline": "Engineer"})
        self.assertNotIn("primary_email", row["public_summary"])
        self.assertNotIn("raw_profile", row["public_summary"])
        self.assertNotIn("raw_payload", row["projection_metrics"])
        self.assertNotIn("crm_notes", row["crm_overlay_summary"])
        self.assertNotIn("restricted_contact", page["field_visibility"]["included"])

    def test_reader_public_counts_exclude_hidden_members(self) -> None:
        self.writer.publish_run_scope_projection(
            run_id="job-hidden-public-count",
            collection_id="company:google",
            projection_id="proj_hidden_public_count",
            members=[
                {
                    "candidate_identity_key": "linkedin:visible",
                    "person_identity_key": "linkedin:visible",
                    "rank_index": 1,
                    "visibility_state": "visible",
                    "public_summary": {"name": "Visible Member"},
                },
                {
                    "candidate_identity_key": "linkedin:hidden",
                    "person_identity_key": "linkedin:hidden",
                    "rank_index": 2,
                    "visibility_state": "hidden",
                    "public_summary": {"name": "Hidden Member"},
                },
            ],
            counts={"result_count": 2, "candidate_count": 2, "count_scope": "exact_projection"},
            replace_members=True,
        )

        projection_payload = self.reader.get_projection("proj_hidden_public_count")
        projection = projection_payload["projection"]
        page = self.reader.get_projection_candidates("proj_hidden_public_count", offset=0, limit=10)

        self.assertEqual(projection_payload["status"], "ready")
        self.assertEqual(projection["visible_member_count"], 1)
        self.assertEqual(projection["counts"]["result_count"], 1)
        self.assertEqual(projection["counts"]["candidate_count"], 1)
        self.assertEqual(projection["counts"]["visible_member_count"], 1)
        self.assertEqual(projection["counts"]["member_count"], 2)
        self.assertEqual(page["candidate_count"], 1)
        self.assertEqual(page["total_candidates"], 1)
        self.assertEqual([row["candidate_identity_key"] for row in page["candidates"]], ["linkedin:visible"])

    def test_reader_filters_projection_members_without_public_reader_fallback(self) -> None:
        self.writer.publish_run_scope_projection(
            run_id="job-reader-filter",
            collection_id="company:openai",
            projection_id="proj_reader_filter",
            members=[
                {
                    "candidate_identity_key": "linkedin:agent-current",
                    "person_identity_key": "linkedin:agent-current",
                    "candidate_id": "agent-current",
                    "rank_index": 1,
                    "employment_scope": "current",
                    "public_summary": {
                        "candidate_id": "agent-current",
                        "display_name": "Agent Current",
                        "headline": "Agent systems engineer",
                        "employment_status": "current",
                        "matched_keywords": ["Agent"],
                        "function_ids": ["8"],
                    },
                },
                {
                    "candidate_identity_key": "linkedin:agent-former",
                    "person_identity_key": "linkedin:agent-former",
                    "candidate_id": "agent-former",
                    "rank_index": 2,
                    "employment_scope": "former",
                    "public_summary": {
                        "candidate_id": "agent-former",
                        "display_name": "Agent Former",
                        "headline": "Agent researcher",
                        "employment_status": "former",
                        "matched_keywords": ["Agent"],
                        "function_ids": ["24"],
                    },
                },
                {
                    "candidate_identity_key": "linkedin:infra-current",
                    "person_identity_key": "linkedin:infra-current",
                    "candidate_id": "infra-current",
                    "rank_index": 3,
                    "employment_scope": "current",
                    "public_summary": {
                        "candidate_id": "infra-current",
                        "display_name": "Infra Current",
                        "headline": "Infrastructure engineer",
                        "employment_status": "current",
                        "matched_keywords": ["Infra"],
                        "function_ids": ["8"],
                    },
                },
            ],
            scope_spec={"keywords": ["Agent"]},
            replace_members=True,
        )
        self.person_asset_writer.rebuild_projection_person_search_index(
            projection_id="proj_reader_filter",
            count_scope="exact_projection",
            raw_profile_index_watermark="public-summary-filter-v1",
        )

        page = self.reader.get_projection_candidates(
            "proj_reader_filter",
            offset=0,
            limit=10,
            candidate_filter={
                "recall_buckets": ["keyword:agent"],
                "employment_statuses": ["former"],
                "function_buckets": ["research"],
            },
        )

        self.assertEqual(page["status"], "ready")
        self.assertEqual(page["candidate_count"], 3)
        self.assertEqual(page["total_candidates"], 3)
        self.assertEqual(page["filtered_candidate_count"], 1)
        self.assertEqual([row["candidate_id"] for row in page["candidates"]], ["agent-former"])
        self.assertEqual(page["filter_contract"]["source"], "projection_person_search_index")
        self.assertFalse(page["filter_contract"]["fallback_used"])
        self.assertEqual(page["filter_contract"]["row_filter_scope"], "projection_membership")
        self.assertEqual(page["filter_contract"]["facet_count_scope"], "exact_projection")
        self.assertEqual(
            page["filter_contract"]["facet_summary_source"],
            "serving_projection_public_facet_counts",
        )
        self.assertTrue(page["filter_contract"]["backend_filtered_paging_supported"])
        self.assertTrue(page["filter_contract"]["filter_active"])
        self.assertFalse(page["read_contract"]["fallback_used"])

    def test_reader_active_filter_fails_closed_when_projection_search_index_is_missing(self) -> None:
        self.writer.publish_run_scope_projection(
            run_id="job-reader-filter-missing-index",
            collection_id="company:openai",
            projection_id="proj_reader_filter_missing_index",
            members=[
                {
                    "candidate_identity_key": "linkedin:agent-current",
                    "person_identity_key": "linkedin:agent-current",
                    "candidate_id": "agent-current",
                    "rank_index": 1,
                    "employment_scope": "current",
                    "public_summary": {
                        "candidate_id": "agent-current",
                        "display_name": "Agent Current",
                        "headline": "Agent systems engineer",
                    },
                }
            ],
            replace_members=True,
        )

        with mock.patch.object(
            self.store,
            "list_serving_projection_members",
            side_effect=AssertionError("active projection filters must not scan membership rows without an index"),
        ):
            page = self.reader.get_projection_candidates(
                "proj_reader_filter_missing_index",
                offset=0,
                limit=10,
                candidate_filter={"search_keyword": "Agent"},
            )

        self.assertEqual(page["status"], "not_ready")
        self.assertEqual(page["reason"], "projection_person_search_index_unavailable")
        self.assertEqual(page["candidate_count"], 1)
        self.assertEqual(page["filter_contract"]["source"], "projection_person_search_index")
        self.assertFalse(page["filter_contract"]["fallback_used"])
        self.assertEqual(page["filter_contract"]["facet_count_scope"], "unavailable")
        self.assertEqual(page["read_contract"]["source"], "projection_person_search_index")
        self.assertFalse(page["read_contract"]["fallback_used"])


if __name__ == "__main__":
    unittest.main()
