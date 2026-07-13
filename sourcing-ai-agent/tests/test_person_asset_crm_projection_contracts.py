import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.control_plane_repository import ControlPlaneAuthoritativeReadError
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

    def test_projection_member_count_fault_does_not_delete_or_finalize_index(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-index-count-fault",
            projection_id="proj_index_count_fault",
            members=[
                {
                    "candidate_identity_key": "linkedin:index-count-fault",
                    "person_identity_key": "linkedin:index-count-fault",
                }
            ],
            replace_members=True,
        )

        with (
            mock.patch.object(
                self.store._control_plane_postgres,  # noqa: SLF001
                "count_rows",
                side_effect=RuntimeError("postgres unavailable"),
            ),
            mock.patch.object(
                self.store.repos.serving_projection,
                "delete_person_search_index",
            ) as delete_index,
            mock.patch.object(
                self.person_asset_writer,
                "_finalize_projection_person_search_index",
            ) as finalize_index,
            self.assertRaises(ControlPlaneAuthoritativeReadError),
        ):
            self.person_asset_writer.rebuild_projection_person_search_index_page(
                projection_id="proj_index_count_fault",
                reset_index=True,
            )

        delete_index.assert_not_called()
        finalize_index.assert_not_called()

    def test_projection_index_count_fault_raises_typed_authoritative_error(self) -> None:
        with (
            mock.patch.object(
                self.store._control_plane_postgres,  # noqa: SLF001
                "count_rows",
                side_effect=RuntimeError("projection index count unavailable"),
            ),
            self.assertRaisesRegex(
                ControlPlaneAuthoritativeReadError,
                "projection_person_search_index via count_rows",
            ),
        ):
            self.store.repos.serving_projection.count_person_search_index("proj_index_count_fault")

    def test_projection_index_reset_preserves_old_rows_when_atomic_replace_fails(self) -> None:
        projection_id = "proj_index_atomic_reset_failure"
        candidate_key = "linkedin:index-atomic-reset"
        self.projection_writer.publish_run_scope_projection(
            run_id="job-index-atomic-reset-failure",
            projection_id=projection_id,
            members=[
                {
                    "candidate_identity_key": candidate_key,
                    "person_identity_key": candidate_key,
                    "public_summary": {"display_name": "Atomic Reset"},
                }
            ],
            replace_members=True,
        )
        repository = self.store.repos.serving_projection
        projection = repository.get(projection_id)
        repository.replace_person_search_index(
            projection_id,
            [
                {
                    "candidate_identity_key": candidate_key,
                    "person_identity_key": candidate_key,
                    "indexed_text": "old searchable row",
                }
            ],
            build_generation="generation-before-reset-failure",
            expected_build_generation="",
            expected_input_revision=str(
                dict(projection.get("metadata") or {}).get("projection_person_search_index_input_revision") or ""
            ),
        )

        with (
            mock.patch.object(
                repository,
                "replace_person_search_index",
                side_effect=RuntimeError("atomic replace unavailable"),
            ),
            mock.patch.object(repository, "delete_person_search_index") as delete_index,
            mock.patch.object(self.person_asset_writer, "_mark_projection_person_search_index_partial") as mark_partial,
            self.assertRaisesRegex(RuntimeError, "atomic replace unavailable"),
        ):
            self.person_asset_writer.rebuild_projection_person_search_index_page(
                projection_id=projection_id,
                reset_index=True,
                rebuild_person_indexes=False,
            )

        delete_index.assert_not_called()
        mark_partial.assert_not_called()
        self.assertEqual(repository.count_person_search_index(projection_id), 1)
        self.assertEqual(
            repository.search_person_index(projection_id, search_keyword="old")["candidate_identity_keys"],
            [candidate_key],
        )

    def test_projection_index_filter_fails_closed_for_mixed_missing_filter_records(self) -> None:
        projection_id = "proj_index_mixed_filter_record"
        first_key = "linkedin:mixed-filter-first"
        second_key = "linkedin:mixed-filter-second"
        self.projection_writer.publish_run_scope_projection(
            run_id="job-index-mixed-filter-record",
            projection_id=projection_id,
            members=[
                {
                    "candidate_identity_key": first_key,
                    "person_identity_key": first_key,
                    "public_summary": {"display_name": "First", "location": "San Francisco, CA"},
                },
                {
                    "candidate_identity_key": second_key,
                    "person_identity_key": second_key,
                    "public_summary": {"display_name": "Second", "location": "New York, NY"},
                },
            ],
            replace_members=True,
        )
        self.person_asset_writer.rebuild_projection_person_search_index(
            projection_id=projection_id,
            count_scope="exact_projection",
            rebuild_person_indexes=False,
        )
        repository = self.store.repos.serving_projection
        rows = repository.list_person_search_index_rows(projection_id, limit=10)
        damaged_row = next(row for row in rows if row["candidate_identity_key"] == second_key)
        repository.upsert_person_search_index_rows(
            projection_id,
            [{**damaged_row, "metadata": {"writer_id": "damaged-test-row"}}],
        )

        repository_result = repository.filter_person_search_index(
            projection_id,
            candidate_filter={"locations": ["us"]},
            limit=10,
        )
        public_result = self.projection_reader.get_projection_candidates(
            projection_id,
            candidate_filter={"locations": ["us"]},
            limit=10,
        )

        self.assertEqual(repository_result["status"], "unavailable")
        self.assertEqual(repository_result["missing_filter_record_count"], 1)
        self.assertEqual(repository_result["candidate_identity_keys"], [])
        self.assertEqual(public_result["status"], "not_ready")
        self.assertEqual(public_result["reason"], "projection_person_search_index_unavailable")
        self.assertEqual(public_result["candidate_count"], 2)
        self.assertEqual(public_result["filtered_candidate_count"], 0)
        self.assertEqual(public_result["candidates"], [])

    def test_projection_index_generation_fence_rejects_delayed_old_continuation(self) -> None:
        projection_id = "proj_index_generation_fence"
        repository = self.store.repos.serving_projection
        self.projection_writer.publish_run_scope_projection(
            run_id="job-index-generation-fence",
            projection_id=projection_id,
            members=[
                {
                    "candidate_identity_key": "linkedin:generation-member",
                    "person_identity_key": "linkedin:generation-member",
                }
            ],
            replace_members=True,
        )
        v1_projection = repository.get(projection_id)
        repository.replace_person_search_index(
            projection_id,
            [
                {
                    "candidate_identity_key": "linkedin:v1-first",
                    "person_identity_key": "linkedin:v1-first",
                    "indexed_text": "v1 first",
                }
            ],
            build_generation="generation-v1",
            expected_build_generation="",
            expected_input_revision=str(
                dict(v1_projection.get("metadata") or {}).get("projection_person_search_index_input_revision") or ""
            ),
        )
        delayed_v1_projection_metadata = repository.get(projection_id)

        continuation_entered = threading.Event()
        release_continuation = threading.Event()
        continuation_result: dict[str, object] = {}
        original_write = self.store._control_plane_postgres.write_projection_person_search_index_generation  # noqa: SLF001

        def delayed_write(**kwargs: object) -> dict[str, object] | None:
            if kwargs.get("build_generation") == "generation-v1" and not bool(kwargs.get("reset_index")):
                continuation_entered.set()
                self.assertTrue(release_continuation.wait(timeout=10))
            return original_write(**kwargs)

        def write_old_continuation() -> None:
            continuation_result.update(
                repository.upsert_person_search_index_rows(
                    projection_id,
                    [
                        {
                            "candidate_identity_key": "linkedin:v1-second",
                            "person_identity_key": "linkedin:v1-second",
                            "indexed_text": "v1 second",
                        }
                    ],
                    build_generation="generation-v1",
                )
            )

        with mock.patch.object(
            self.store._control_plane_postgres,  # noqa: SLF001
            "write_projection_person_search_index_generation",
            side_effect=delayed_write,
        ):
            continuation_thread = threading.Thread(target=write_old_continuation, daemon=True)
            continuation_thread.start()
            self.assertTrue(continuation_entered.wait(timeout=10))
            v2_projection = repository.get(projection_id)
            v2_result = repository.replace_person_search_index(
                projection_id,
                [
                    {
                        "candidate_identity_key": "linkedin:v2-only",
                        "person_identity_key": "linkedin:v2-only",
                        "indexed_text": "v2 only",
                    }
                ],
                build_generation="generation-v2",
                expected_build_generation="generation-v1",
                expected_input_revision=str(
                    dict(v2_projection.get("metadata") or {}).get("projection_person_search_index_input_revision") or ""
                ),
            )
            repository.upsert(
                {
                    **delayed_v1_projection_metadata,
                    "metadata": {
                        **dict(delayed_v1_projection_metadata.get("metadata") or {}),
                        "search_index_build_status": "partial",
                    },
                }
            )
            release_continuation.set()
            continuation_thread.join(timeout=10)

        self.assertFalse(continuation_thread.is_alive())
        self.assertEqual(v2_result["status"], "indexed")
        self.assertEqual(continuation_result["status"], "obsolete")
        self.assertEqual(continuation_result["current_build_generation"], "generation-v2")
        projection = repository.get(projection_id)
        self.assertEqual(
            projection["metadata"]["projection_person_search_index_build_generation"],
            "generation-v2",
        )
        rows = repository.list_person_search_index_rows(projection_id, limit=10)
        self.assertEqual([row["candidate_identity_key"] for row in rows], ["linkedin:v2-only"])

    def test_projection_index_generation_fence_rejects_delayed_old_reset(self) -> None:
        projection_id = "proj_index_generation_reset_fence"
        self.projection_writer.publish_run_scope_projection(
            run_id="job-index-generation-reset-fence",
            projection_id=projection_id,
            members=[
                {
                    "candidate_identity_key": "linkedin:generation-reset-member",
                    "person_identity_key": "linkedin:generation-reset-member",
                }
            ],
            replace_members=True,
        )
        reset_entered = threading.Event()
        release_reset = threading.Event()
        v1_result: dict[str, object] = {}
        original_write = self.store._control_plane_postgres.write_projection_person_search_index_generation  # noqa: SLF001

        def delayed_write(**kwargs: object) -> dict[str, object] | None:
            if kwargs.get("build_generation") == "generation-reset-v1" and bool(kwargs.get("reset_index")):
                reset_entered.set()
                self.assertTrue(release_reset.wait(timeout=10))
            return original_write(**kwargs)

        def write_old_reset() -> None:
            v1_result.update(
                self.person_asset_writer.rebuild_projection_person_search_index_page(
                    projection_id=projection_id,
                    member_page_size=1,
                    reset_index=True,
                    rebuild_person_indexes=False,
                    build_generation="generation-reset-v1",
                )
            )

        with mock.patch.object(
            self.store._control_plane_postgres,  # noqa: SLF001
            "write_projection_person_search_index_generation",
            side_effect=delayed_write,
        ):
            v1_thread = threading.Thread(target=write_old_reset, daemon=True)
            v1_thread.start()
            self.assertTrue(reset_entered.wait(timeout=10))
            v2_result = self.person_asset_writer.rebuild_projection_person_search_index_page(
                projection_id=projection_id,
                member_page_size=1,
                reset_index=True,
                rebuild_person_indexes=False,
                build_generation="generation-reset-v2",
            )
            release_reset.set()
            v1_thread.join(timeout=10)

        self.assertFalse(v1_thread.is_alive())
        self.assertEqual(v2_result["status"], "indexed")
        self.assertEqual(v1_result["status"], "obsolete")
        self.assertEqual(v1_result["current_build_generation"], "generation-reset-v2")
        projection = self.store.repos.serving_projection.get(projection_id)
        self.assertEqual(
            projection["metadata"]["projection_person_search_index_build_generation"],
            "generation-reset-v2",
        )

    def test_projection_input_revision_preserves_noop_and_invalidates_same_count_replacement(self) -> None:
        projection_id = "proj_index_semantic_revision"
        run_id = "job-index-semantic-revision"
        first_member = {
            "candidate_identity_key": "linkedin:semantic-first",
            "person_identity_key": "linkedin:semantic-first",
            "public_summary": {"display_name": "Semantic First"},
        }
        self.projection_writer.publish_run_scope_projection(
            run_id=run_id,
            projection_id=projection_id,
            members=[first_member],
            replace_members=True,
        )
        repository = self.store.repos.serving_projection
        initial_projection = repository.get(projection_id)
        initial_revision = str(initial_projection["metadata"]["projection_person_search_index_input_revision"])
        first_build = self.person_asset_writer.rebuild_projection_person_search_index_page(
            projection_id=projection_id,
            member_page_size=1,
            reset_index=True,
            rebuild_person_indexes=False,
            build_generation="generation-semantic-first",
        )
        self.assertEqual(first_build["status"], "indexed")
        self.assertEqual(repository.search_person_index(projection_id, search_keyword="semantic")["status"], "ready")

        self.projection_writer.publish_run_scope_projection(
            run_id=run_id,
            projection_id=projection_id,
            members=[first_member],
            replace_members=True,
        )
        replayed_projection = repository.get(projection_id)
        self.assertEqual(
            replayed_projection["metadata"]["projection_person_search_index_input_revision"],
            initial_revision,
        )
        self.assertEqual(repository.search_person_index(projection_id, search_keyword="semantic")["status"], "ready")

        replacement_member = {
            "candidate_identity_key": "linkedin:semantic-second",
            "person_identity_key": "linkedin:semantic-second",
            "public_summary": {"display_name": "Semantic Second"},
        }
        self.projection_writer.publish_run_scope_projection(
            run_id=run_id,
            projection_id=projection_id,
            members=[replacement_member],
            replace_members=True,
        )
        replaced_projection = repository.get(projection_id)
        self.assertNotEqual(
            replaced_projection["metadata"]["projection_person_search_index_input_revision"],
            initial_revision,
        )
        self.assertEqual(
            replaced_projection["metadata"]["projection_person_search_index_build_input_revision"],
            initial_revision,
        )
        self.assertEqual(
            repository.search_person_index(projection_id, search_keyword="semantic")["status"], "unavailable"
        )
        self.assertEqual(
            self.projection_reader.search_projection_person_index(projection_id, search_keyword="semantic")["status"],
            "not_ready",
        )

    def test_projection_input_revision_rejects_same_timestamp_delayed_reset(self) -> None:
        projection_id = "proj_index_same_timestamp_reset"
        run_id = "job-index-same-timestamp-reset"
        initial_member = {
            "candidate_identity_key": "linkedin:same-timestamp",
            "person_identity_key": "linkedin:same-timestamp",
            "public_summary": {"display_name": "Same Timestamp Initial"},
        }
        reset_entered = threading.Event()
        release_reset = threading.Event()
        reset_result: dict[str, object] = {}
        adapter = self.store._control_plane_postgres  # noqa: SLF001
        original_write = adapter.write_projection_person_search_index_generation

        def delayed_write(**kwargs: object) -> dict[str, object] | None:
            if kwargs.get("build_generation") == "generation-same-timestamp-v1":
                reset_entered.set()
                self.assertTrue(release_reset.wait(timeout=10))
            return original_write(**kwargs)

        def run_delayed_reset() -> None:
            reset_result.update(
                self.person_asset_writer.rebuild_projection_person_search_index_page(
                    projection_id=projection_id,
                    member_page_size=1,
                    reset_index=True,
                    rebuild_person_indexes=False,
                    build_generation="generation-same-timestamp-v1",
                )
            )

        with mock.patch(
            "sourcing_agent.control_plane_live_postgres._utc_now_sql_timestamp",
            return_value="2026-07-10 00:00:00",
        ):
            self.projection_writer.publish_run_scope_projection(
                run_id=run_id,
                projection_id=projection_id,
                members=[initial_member],
                replace_members=True,
            )
            initial_revision = str(
                self.store.repos.serving_projection.get(projection_id)["metadata"][
                    "projection_person_search_index_input_revision"
                ]
            )
            with mock.patch.object(
                adapter, "write_projection_person_search_index_generation", side_effect=delayed_write
            ):
                reset_thread = threading.Thread(target=run_delayed_reset, daemon=True)
                reset_thread.start()
                self.assertTrue(reset_entered.wait(timeout=10))
                self.projection_writer.publish_run_scope_projection(
                    run_id=run_id,
                    projection_id=projection_id,
                    members=[
                        {
                            **initial_member,
                            "public_summary": {"display_name": "Same Timestamp Changed"},
                        }
                    ],
                    replace_members=True,
                )
                release_reset.set()
                reset_thread.join(timeout=10)

        self.assertFalse(reset_thread.is_alive())
        self.assertEqual(reset_result["status"], "obsolete")
        self.assertNotEqual(
            self.store.repos.serving_projection.get(projection_id)["metadata"][
                "projection_person_search_index_input_revision"
            ],
            initial_revision,
        )

    def test_public_facet_and_index_readiness_follow_completed_three_key_build(self) -> None:
        projection_id = "proj_index_publication_fence"
        run_id = "job-index-publication-fence"
        members = [
            {
                "candidate_identity_key": "linkedin:publication-current",
                "person_identity_key": "linkedin:publication-current",
                "employment_scope": "current",
                "public_summary": {
                    "display_name": "Publication Current",
                    "headline": "Search infrastructure engineer",
                },
            },
            {
                "candidate_identity_key": "linkedin:publication-former",
                "person_identity_key": "linkedin:publication-former",
                "employment_scope": "former",
                "public_summary": {
                    "display_name": "Publication Former",
                    "headline": "Former search engineer",
                },
            },
        ]
        self.projection_writer.publish_run_scope_projection(
            run_id=run_id,
            projection_id=projection_id,
            members=members,
            replace_members=True,
        )
        initial_build = self.person_asset_writer.rebuild_projection_person_search_index_page(
            projection_id=projection_id,
            count_scope="exact_projection",
            member_page_size=2,
            reset_index=True,
            rebuild_person_indexes=False,
            build_generation="generation-publication-fence-v1",
        )
        self.assertTrue(initial_build["completed"])
        initially_ready = self.projection_reader.get_projection_candidates(projection_id, limit=10)
        self.assertEqual(initially_ready["facet_summary"]["status"], "complete")
        self.assertEqual(initially_ready["filter_contract"]["facet_count_scope"], "exact_projection")
        self.assertEqual(initially_ready["index_filter_readiness"]["count_scope"], "exact_projection")

        replacement_members = [
            members[0],
            {
                "candidate_identity_key": "linkedin:publication-replacement",
                "person_identity_key": "linkedin:publication-replacement",
                "employment_scope": "former",
                "public_summary": {
                    "display_name": "Publication Replacement",
                    "headline": "Replacement search engineer",
                },
            },
        ]
        repository = self.store.repos.serving_projection
        original_list_members = repository.list_members
        replacement_applied = False

        def replace_members_before_public_member_read(*args: object, **kwargs: object) -> list[dict[str, object]]:
            nonlocal replacement_applied
            if not replacement_applied:
                replacement_applied = True
                self.projection_writer.publish_run_scope_projection(
                    run_id=run_id,
                    projection_id=projection_id,
                    members=replacement_members,
                    replace_members=True,
                )
            return original_list_members(*args, **kwargs)

        with mock.patch.object(repository, "list_members", side_effect=replace_members_before_public_member_read):
            stale_page = self.projection_reader.get_projection_candidates(projection_id, limit=10)
        stale_projection = repository.get(projection_id)
        self.assertTrue(replacement_applied)
        self.assertEqual(stale_projection["metadata"]["search_index_build_status"], "stale")
        self.assertNotIn("public_facet_counts", stale_projection["counts"])
        self.assertEqual(stale_page["facet_summary"]["status"], "unavailable")
        self.assertEqual(stale_page["filter_contract"]["facet_count_scope"], "unavailable")
        self.assertEqual(stale_page["index_filter_readiness"]["count_scope"], "unavailable")

        reset_page = self.person_asset_writer.rebuild_projection_person_search_index_page(
            projection_id=projection_id,
            count_scope="exact_projection",
            member_page_size=1,
            reset_index=True,
            rebuild_person_indexes=False,
            build_generation="generation-publication-fence-v2",
        )
        self.assertFalse(reset_page["completed"])
        reset_projection = self.store.repos.serving_projection.get(projection_id)
        reset_public_page = self.projection_reader.get_projection_candidates(projection_id, limit=10)
        self.assertEqual(reset_projection["metadata"]["search_index_build_status"], "partial")
        self.assertNotIn("public_facet_counts", reset_projection["counts"])
        self.assertEqual(reset_public_page["facet_summary"]["status"], "unavailable")
        self.assertEqual(reset_public_page["index_filter_readiness"]["count_scope"], "unavailable")

        final_page = self.person_asset_writer.rebuild_projection_person_search_index_page(
            projection_id=projection_id,
            count_scope="exact_projection",
            member_page_size=1,
            offset=1,
            rebuild_person_indexes=False,
            build_generation="generation-publication-fence-v2",
        )
        self.assertTrue(final_page["completed"])
        finalized_projection = self.store.repos.serving_projection.get(projection_id)
        finalized_public_page = self.projection_reader.get_projection_candidates(projection_id, limit=10)
        facet_product = dict(finalized_projection["counts"]["public_facet_counts"])
        for key in (
            "projection_person_search_index_build_generation",
            "projection_person_search_index_build_input_revision",
            "projection_person_search_index_input_revision",
        ):
            self.assertEqual(facet_product[key], finalized_projection["metadata"][key])
            self.assertEqual(finalized_projection["readiness"][key], finalized_projection["metadata"][key])
        self.assertEqual(finalized_projection["metadata"]["search_index_build_status"], "completed")
        self.assertEqual(finalized_public_page["facet_summary"]["status"], "complete")
        self.assertEqual(finalized_public_page["filter_contract"]["facet_count_scope"], "exact_projection")
        self.assertEqual(finalized_public_page["index_filter_readiness"]["count_scope"], "exact_projection")

    def test_empty_projection_finalizes_zero_facet_product_under_three_key_fence(self) -> None:
        projection_id = "proj_index_empty_publication_fence"
        self.projection_writer.publish_run_scope_projection(
            run_id="job-index-empty-publication-fence",
            projection_id=projection_id,
            members=[],
            replace_members=True,
        )

        result = self.person_asset_writer.rebuild_projection_person_search_index_page(
            projection_id=projection_id,
            count_scope="exact_projection",
            reset_index=True,
            rebuild_person_indexes=False,
            build_generation="generation-empty-publication-fence",
        )
        page = self.projection_reader.get_projection_candidates(projection_id, limit=10)

        self.assertTrue(result["completed"])
        self.assertEqual(result["indexed_count"], 0)
        self.assertEqual(page["candidate_count"], 0)
        self.assertEqual(page["facet_summary"]["status"], "complete")
        self.assertEqual(page["facet_summary"]["candidate_count"], 0)
        self.assertEqual(page["filter_contract"]["facet_count_scope"], "exact_projection")
        self.assertEqual(page["index_filter_readiness"]["count_scope"], "exact_projection")

    def test_projection_revision_fence_rejects_old_continuation_and_completed_downgrade(self) -> None:
        projection_id = "proj_index_revision_continuation"
        run_id = "job-index-revision-continuation"
        members = [
            {
                "candidate_identity_key": f"linkedin:revision-{index}",
                "person_identity_key": f"linkedin:revision-{index}",
                "public_summary": {"display_name": f"Revision {index}"},
            }
            for index in range(2)
        ]
        self.projection_writer.publish_run_scope_projection(
            run_id=run_id,
            projection_id=projection_id,
            members=members,
            replace_members=True,
        )
        first_page = self.person_asset_writer.rebuild_projection_person_search_index_page(
            projection_id=projection_id,
            member_page_size=1,
            reset_index=True,
            rebuild_person_indexes=False,
            build_generation="generation-revision-v1",
        )
        self.assertEqual(first_page["status"], "indexed")
        self.assertFalse(first_page["completed"])

        changed_members = [
            members[0],
            {
                **members[1],
                "public_summary": {"display_name": "Revision Changed"},
            },
        ]
        self.projection_writer.publish_run_scope_projection(
            run_id=run_id,
            projection_id=projection_id,
            members=changed_members,
            replace_members=True,
        )
        stale_continuation = self.person_asset_writer.rebuild_projection_person_search_index_page(
            projection_id=projection_id,
            member_page_size=1,
            offset=1,
            rebuild_person_indexes=False,
            build_generation="generation-revision-v1",
        )
        self.assertEqual(stale_continuation["status"], "obsolete")

        current_projection = self.store.repos.serving_projection.get(projection_id)
        current_revision = str(current_projection["metadata"]["projection_person_search_index_input_revision"])
        rebuilt = self.person_asset_writer.rebuild_projection_person_search_index_page(
            projection_id=projection_id,
            count_scope="exact_projection",
            member_page_size=2,
            reset_index=True,
            rebuild_person_indexes=False,
            build_generation="generation-revision-v2",
        )
        self.assertEqual(rebuilt["status"], "indexed")
        completed_projection = self.store.repos.serving_projection.get(projection_id)
        self.assertEqual(
            completed_projection["metadata"]["projection_person_search_index_build_input_revision"],
            current_revision,
        )

        stale_state = self.store.repos.serving_projection.update_person_search_index_build_state(
            projection_id,
            build_generation="generation-revision-v1",
            readiness_patch={"index_count_scope": "index_partial"},
            metadata_patch={"search_index_build_status": "partial"},
        )
        delayed_same_generation_partial = self.person_asset_writer._mark_projection_person_search_index_partial(  # noqa: SLF001
            projection_id=projection_id,
            build_generation="generation-revision-v2",
            total_member_count=2,
            truncated=False,
        )
        self.assertEqual(stale_state["status"], "obsolete")
        self.assertEqual(delayed_same_generation_partial["status"], "obsolete")
        final_projection = self.store.repos.serving_projection.get(projection_id)
        self.assertEqual(final_projection["metadata"]["search_index_build_status"], "completed")
        self.assertEqual(final_projection["readiness"]["index_count_scope"], "exact_projection")

    def test_projection_member_uow_advances_input_revision_only_for_semantic_change(self) -> None:
        projection_id = "proj_index_member_revision"
        member = {
            "candidate_identity_key": "linkedin:member-revision",
            "person_identity_key": "linkedin:member-revision",
            "public_summary": {"display_name": "Member Revision"},
        }
        self.projection_writer.publish_run_scope_projection(
            run_id="job-index-member-revision",
            projection_id=projection_id,
            members=[member],
            replace_members=True,
        )
        repository = self.store.repos.serving_projection
        initial_revision = str(
            repository.get(projection_id)["metadata"]["projection_person_search_index_input_revision"]
        )
        completed_build = self.person_asset_writer.rebuild_projection_person_search_index_page(
            projection_id=projection_id,
            count_scope="exact_projection",
            reset_index=True,
            rebuild_person_indexes=False,
            build_generation="generation-member-revision",
        )
        self.assertTrue(completed_build["completed"])

        self.assertEqual(repository.upsert_members(projection_id, [member]), 1)
        replayed_projection = repository.get(projection_id)
        self.assertEqual(
            replayed_projection["metadata"]["projection_person_search_index_input_revision"], initial_revision
        )
        self.assertIn("public_facet_counts", replayed_projection["counts"])

        self.assertEqual(
            repository.upsert_members(
                projection_id,
                [
                    {
                        **member,
                        "public_summary": {"display_name": "Member Revision Changed"},
                    }
                ],
            ),
            1,
        )
        changed_projection = repository.get(projection_id)
        self.assertNotEqual(
            changed_projection["metadata"]["projection_person_search_index_input_revision"], initial_revision
        )
        self.assertEqual(changed_projection["metadata"]["search_index_build_status"], "stale")
        self.assertNotIn("public_facet_counts", changed_projection["counts"])
        self.assertEqual(changed_projection["readiness"]["index_count_scope"], "unavailable")

    def test_projection_publication_bulk_upsert_preserves_index_generation_marker(self) -> None:
        projection_id = "proj_index_generation_publication"
        run_id = "job-index-generation-publication"
        member = {
            "candidate_identity_key": "linkedin:generation-publication-member",
            "person_identity_key": "linkedin:generation-publication-member",
        }
        self.projection_writer.publish_run_scope_projection(
            run_id=run_id,
            projection_id=projection_id,
            members=[member],
            replace_members=True,
        )
        build_result = self.person_asset_writer.rebuild_projection_person_search_index_page(
            projection_id=projection_id,
            member_page_size=1,
            reset_index=True,
            rebuild_person_indexes=False,
            build_generation="generation-publication-v1",
        )
        self.assertEqual(build_result["status"], "indexed")

        self.projection_writer.publish_run_scope_projection(
            run_id=run_id,
            projection_id=projection_id,
            members=[member],
            replace_members=False,
            metadata={"publication_reason": "marker-preservation-regression"},
        )

        projection = self.store.repos.serving_projection.get(projection_id)
        self.assertEqual(
            projection["metadata"]["projection_person_search_index_build_generation"],
            "generation-publication-v1",
        )

    def test_public_projection_search_normalizes_missing_index_reason(self) -> None:
        projection_id = "proj_search_missing_index_reason"
        self.projection_writer.publish_run_scope_projection(
            run_id="job-search-missing-index-reason",
            projection_id=projection_id,
            members=[
                {
                    "candidate_identity_key": "linkedin:missing-index",
                    "person_identity_key": "linkedin:missing-index",
                }
            ],
            replace_members=True,
        )

        result = self.projection_reader.search_projection_person_index(
            projection_id,
            search_keyword="missing",
        )

        self.assertEqual(result["status"], "not_ready")
        self.assertEqual(result["reason"], "projection_person_search_index_unavailable")
        self.assertEqual(result["candidate_count"], 1)

    def test_public_projection_member_consumers_fail_closed_after_count_succeeds(self) -> None:
        projection_id = "proj_member_second_read_fault"
        candidate_key = "linkedin:member-second-read-fault"
        self.projection_writer.publish_run_scope_projection(
            run_id="job-member-second-read-fault",
            projection_id=projection_id,
            members=[
                {
                    "candidate_identity_key": candidate_key,
                    "person_identity_key": candidate_key,
                }
            ],
            replace_members=True,
        )
        repository = self.store.repos.serving_projection
        failure = ControlPlaneAuthoritativeReadError("postgres unavailable after count")

        with mock.patch.object(repository, "get_member", side_effect=failure):
            detail = self.projection_reader.get_projection_person_detail(projection_id, candidate_key)
        with (
            mock.patch.object(
                repository,
                "search_person_index",
                return_value={
                    "status": "ready",
                    "candidate_identity_keys": [candidate_key],
                    "matched_count": 1,
                    "offset": 0,
                    "has_more": False,
                    "next_offset": None,
                    "index_filter_readiness": {"count_scope": "exact_projection"},
                },
            ),
            mock.patch.object(repository, "list_members_by_identity_keys", side_effect=failure),
        ):
            search = self.projection_reader.search_projection_person_index(
                projection_id,
                search_keyword="fault",
            )
        with mock.patch.object(repository, "list_members_by_person_identity", side_effect=failure):
            person = self.projection_reader.get_person_summary(candidate_key)

        for payload in (detail, search, person):
            self.assertEqual(payload["status"], "not_ready")
            self.assertEqual(payload["reason"], "projection_members_unavailable")
            self.assertTrue(payload["read_contract"]["fail_closed"])
            self.assertFalse(payload["read_contract"]["fallback_used"])

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
        self.assertEqual(
            row["public_summary"]["person_identity_key"], "linkedin:https://www.linkedin.com/in/ada-example"
        )
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
        projection = self.store.repos.serving_projection.get("proj_google_facets")
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

        self.assertEqual(
            self.store.list_person_assets(person_identity_key="linkedin:ada-example")[0]["asset_id"], "pa_avatar"
        )
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
        self.assertEqual(
            self.store.repos.serving_projection.get_run_link("job-legacy")["projection_id"], result["projection_id"]
        )
        self.assertEqual(self.store.repos.serving_projection.count_members(result["projection_id"]), 2)

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
        self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj_legacy_summary",
                "projection_type": "run_scope_projection",
                "source_run_id": "job-legacy-summary",
                "state": "serving",
                "provenance": {"candidate_payload_path": str(candidate_payload_path)},
            }
        )
        self.store.repos.serving_projection.upsert_members(
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
        before = self.store.repos.serving_projection.get_member("proj_legacy_summary", "legacy-row-ada")
        self.assertEqual(before["person_identity_key"], "linkedin:https://www.linkedin.com/in/legacy-summary-ada")
        legacy_summary = dict(before["public_summary"])
        legacy_summary.pop("source_projection_id", None)
        legacy_summary.pop("source_run_id", None)
        # Downgrade the member to the pre-enrichment "legacy" shape directly in Postgres (the SQLite
        # shadow schema was retired in Track B B4.1), simulating a row written by an older serving
        # writer that did not stamp source_projection_id / source_run_id.
        self.store._control_plane_postgres._execute_non_query(  # noqa: SLF001 - migration regression fixture
            """
            UPDATE serving_projection_members
            SET public_summary_json = %s
            WHERE projection_id = %s AND candidate_identity_key = %s
            """,
            (json.dumps(legacy_summary), "proj_legacy_summary", "legacy-row-ada"),
        )
        backfill = ServingProjectionMigrationBackfill(self.store)

        result = backfill.backfill_person_summary_views(projection_ids=["proj_legacy_summary"])
        row = self.store.repos.serving_projection.get_member("proj_legacy_summary", "legacy-row-ada")

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
        new_pointer = self.store.repos.serving_projection.get_authoritative_pointer("company:newco")
        existing_pointer = self.store.repos.serving_projection.get_authoritative_pointer("company:existingco")
        new_members = self.store.repos.serving_projection.list_members(new_pointer["active_projection_id"], limit=10)

        self.assertEqual(result["status"], "backfilled")
        self.assertEqual(result["planned_count"], 1)
        self.assertEqual(result["applied_count"], 1)
        self.assertEqual(result["skipped_existing_count"], 1)
        self.assertEqual(existing_pointer["active_projection_id"], "proj_existing")
        self.assertTrue(new_pointer["active_projection_id"].startswith("proj_localasset_"))
        self.assertEqual(new_pointer["active_collection_version"], "20260501T000000")
        self.assertEqual(new_members[0]["profile_readiness"], "ready")
        self.assertEqual(
            new_members[0]["public_summary"]["experience_lines"], ["2026~Present, NewCo, Research Engineer"]
        )
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
        row = self.store.repos.serving_projection.get_member("proj_collection_layer", "linkedin:zhang-wei-layer")
        projection = self.store.repos.serving_projection.get("proj_collection_layer")

        self.assertEqual(result["status"], "backfilled")
        self.assertEqual(result["processed_member_count"], 1)
        self.assertEqual(row["public_summary"]["outreach_layer"], 3)
        self.assertEqual(
            row["public_summary"]["outreach_layer_key"], "layer_3_mainland_china_experience_or_chinese_language"
        )
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
            },
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
        event = self.store.get_crm_event_by_idempotency("crm:migrate-public-web-promotion:promotion-ada-homepage")

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
        active_assertion = next(
            item for item in detail["assertions"] if item["assertion_id"] == "assertion-detail-active-email"
        )
        review_assertion = next(
            item for item in detail["assertions"] if item["assertion_id"] == "assertion-detail-review-email"
        )
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
        self.assertEqual(
            page["candidates"][0]["media_summary"]["avatar_url"], "https://static.example.com/avatar-detail-ada.png"
        )

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
        self.assertEqual(page["index_filter_readiness"]["count_scope"], "unavailable")
        self.assertEqual(page["index_filter_readiness"]["profile_indexed_at"], "")
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
        vector_search = self.projection_reader.search_projection_person_index(
            "proj_person_index", search_keyword="vector"
        )
        substack_search = self.projection_reader.search_projection_person_index(
            "proj_person_index", search_keyword="Substack"
        )
        rows = self.store.repos.serving_projection._list_person_search_index_rows(  # noqa: SLF001
            "proj_person_index", limit=10
        )

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
        row = self.store.repos.serving_projection._list_person_search_index_rows(  # noqa: SLF001
            "proj_index_backfill_fast", limit=1
        )[0]

        self.assertEqual(result["status"], "backfilled")
        self.assertFalse(result["rebuild_person_indexes"])
        self.assertEqual(result["projections"][0]["indexed_count"], 1)
        self.assertEqual(result["projections"][0]["person_index"]["status"], "skipped")
        self.assertEqual(public_search["filtered_candidate_count"], 1)
        self.assertEqual(raw_search["filtered_candidate_count"], 0)
        self.assertFalse(row["indexed_field_sources"]["raw_profile_index"])


if __name__ == "__main__":
    unittest.main()
