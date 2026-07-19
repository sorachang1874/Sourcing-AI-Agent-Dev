import ast
import json
import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

from sourcing_agent.control_plane_repository import ControlPlaneAuthoritativeReadError
from sourcing_agent.person_asset_writer import PersonAssetWriter
from sourcing_agent.projection_search_index_contract import PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY
from sourcing_agent.repositories.serving_projection import _build_projection_id
from sourcing_agent.serving_projection_reader import ServingProjectionReader
from sourcing_agent.serving_projection_writer import ServingProjectionWriter
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin, pg_backed_control_plane_store


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

    def test_production_callers_do_not_bypass_projection_publication_owners_with_generic_upsert(self) -> None:
        source_root = Path(__file__).resolve().parents[1] / "src" / "sourcing_agent"
        violations: list[str] = []
        for source_path in sorted(source_root.rglob("*.py")):
            tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                    continue
                repository = node.func.value
                if node.func.attr != "upsert" or not isinstance(repository, ast.Attribute):
                    continue
                repos = repository.value
                if (
                    repository.attr == "serving_projection"
                    and isinstance(repos, ast.Attribute)
                    and repos.attr == "repos"
                ):
                    violations.append(f"{source_path.relative_to(source_root)}:{node.lineno}")
        self.assertEqual(
            violations,
            [],
            "production projection publication must use a lock-owning publish/upsert_with_* method or "
            "patch_publication_fields_under_lock",
        )

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
        members = self.store.repos.serving_projection.list_members(projection["projection_id"])

        self.assertEqual(projection["projection_type"], "run_scope_projection")
        self.assertEqual(projection["source_run_id"], "job-openai-agent")
        self.assertEqual(projection["counts"]["result_count"], 2)
        self.assertEqual(projection["counts"]["count_scope"], "exact_projection")
        self.assertEqual(link["projection_id"], projection["projection_id"])
        self.assertEqual(
            self.store.repos.serving_projection.get_run_link("job-openai-agent")["projection_id"],
            projection["projection_id"],
        )
        self.assertEqual([member["candidate_identity_key"] for member in members], ["linkedin:ada", "linkedin:grace"])

    def test_publication_field_patch_waits_for_lock_and_merges_authoritative_row(self) -> None:
        publication = self.writer.publish_run_scope_projection(
            run_id="job-publication-field-lock",
            collection_id="company:test",
            members=[{"candidate_identity_key": "linkedin:ada"}],
            replace_members=True,
        )
        projection_id = publication["projection"]["projection_id"]
        with self.assertRaisesRegex(ValueError, "cannot overwrite search-index binding metadata"):
            self.store.repos.serving_projection.patch_publication_fields_under_lock(
                projection_id,
                metadata_patch={PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY: "forged-revision"},
            )
        current = self.store.repos.serving_projection.get(projection_id)
        self.store.repos.serving_projection.upsert(
            {
                **current,
                "metadata": {
                    **dict(current.get("metadata") or {}),
                    "authoritative_marker": "preserve-me",
                },
            }
        )
        patch_started = threading.Event()
        patch_finished = threading.Event()
        patch_result: dict[str, object] = {}
        thread_errors: list[BaseException] = []

        def _patch_publication_fields() -> None:
            try:
                patch_started.set()
                patch_result.update(
                    self.store.repos.serving_projection.patch_publication_fields_under_lock(
                        projection_id,
                        counts_patch={"visible_member_count": 1},
                        readiness_patch={"layering": "complete"},
                        metadata_patch={"patch_marker": "applied"},
                    )
                )
            except BaseException as exc:  # pragma: no cover - surfaced below
                thread_errors.append(exc)
            finally:
                patch_finished.set()

        with self.store.repos.serving_projection.hold_publication_lock(projection_id):
            patch_thread = threading.Thread(target=_patch_publication_fields, daemon=True)
            patch_thread.start()
            self.assertTrue(patch_started.wait(timeout=1))
            self.assertFalse(patch_finished.wait(timeout=0.1))
        patch_thread.join(timeout=2)

        self.assertFalse(patch_thread.is_alive())
        self.assertEqual(thread_errors, [])
        self.assertEqual(patch_result["counts"]["visible_member_count"], 1)
        self.assertEqual(patch_result["readiness"]["layering"], "complete")
        self.assertEqual(patch_result["metadata"]["authoritative_marker"], "preserve-me")
        self.assertEqual(patch_result["metadata"]["patch_marker"], "applied")

        row_patch_started = threading.Event()
        row_patch_finished = threading.Event()
        row_patch_result: dict[str, object] = {}
        row_patch_errors: list[BaseException] = []

        def _patch_after_concurrent_row_writer() -> None:
            try:
                row_patch_started.set()
                row_patch_result.update(
                    self.store.repos.serving_projection.patch_publication_fields_under_lock(
                        projection_id,
                        counts_patch={"visible_member_count": 2},
                        metadata_patch={"row_patch_marker": "applied"},
                    )
                )
            except BaseException as exc:  # pragma: no cover - surfaced below
                row_patch_errors.append(exc)
            finally:
                row_patch_finished.set()

        with self.store._control_plane_postgres._connect() as row_lock_connection:
            with row_lock_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT projection_id FROM serving_projections WHERE projection_id = %s FOR UPDATE",
                    (projection_id,),
                )
                self.assertIsNotNone(cursor.fetchone())
                row_patch_thread = threading.Thread(target=_patch_after_concurrent_row_writer, daemon=True)
                row_patch_thread.start()
                self.assertTrue(row_patch_started.wait(timeout=1))
                self.assertFalse(row_patch_finished.wait(timeout=0.1))
                cursor.execute(
                    """
                    UPDATE serving_projections
                    SET counts_json = %s,
                        readiness_json = %s,
                        metadata_json = %s
                    WHERE projection_id = %s
                    """,
                    (
                        json.dumps({"search_index_candidate_count": 7}),
                        json.dumps({"search_index": "ready"}),
                        json.dumps({"search_index_generation": "generation-concurrent"}),
                        projection_id,
                    ),
                )
            row_lock_connection.commit()
        row_patch_thread.join(timeout=2)

        self.assertFalse(row_patch_thread.is_alive())
        self.assertEqual(row_patch_errors, [])
        self.assertEqual(row_patch_result["counts"]["search_index_candidate_count"], 7)
        self.assertEqual(row_patch_result["counts"]["visible_member_count"], 2)
        self.assertEqual(row_patch_result["readiness"]["search_index"], "ready")
        self.assertEqual(row_patch_result["metadata"]["search_index_generation"], "generation-concurrent")
        self.assertEqual(row_patch_result["metadata"]["row_patch_marker"], "applied")

    def test_projection_reader_aggregates_only_explicit_member_quality_evidence(self) -> None:
        exact = self.writer.publish_run_scope_projection(
            run_id="job-owned-quality-counts",
            collection_id="company:lovable",
            members=[
                {
                    "candidate_identity_key": "linkedin:owned-ready",
                    "profile_readiness": "ready",
                    "card_readiness": "ready",
                    "projection_metrics": {
                        "has_explicit_profile_capture": True,
                        "needs_profile_completion": False,
                        "low_profile_richness": True,
                    },
                },
                {
                    "candidate_identity_key": "linkedin:owned-shell",
                    "profile_readiness": "required",
                    "card_readiness": "row_shell",
                    "projection_metrics": {
                        "has_explicit_profile_capture": False,
                        "needs_profile_completion": True,
                        "low_profile_richness": False,
                    },
                },
            ],
            replace_members=True,
        )

        exact_payload = self.reader.get_projection(exact["projection"]["projection_id"])
        exact_readiness = exact_payload["projection"]["readiness"]
        self.assertEqual(exact_readiness["explicit_profile_capture_candidate_count"], 1)
        self.assertEqual(exact_readiness["needs_profile_completion_candidate_count"], 1)
        self.assertEqual(exact_readiness["low_profile_richness_candidate_count"], 1)

        legacy_integer = self.writer.publish_run_scope_projection(
            run_id="job-owned-quality-integer-compatibility",
            collection_id="company:lovable",
            members=[
                {
                    "candidate_identity_key": "linkedin:legacy-integer-true",
                    "profile_readiness": "not_required",
                    "projection_metrics": {
                        "profile_required": 0,
                        "has_explicit_profile_capture": 1,
                        "needs_profile_completion": 0,
                        "low_profile_richness": 1,
                    },
                    "public_summary": {"needs_profile_completion": 0},
                },
                {
                    "candidate_identity_key": "linkedin:legacy-integer-false",
                    "profile_readiness": "skipped",
                    "projection_metrics": {
                        "profile_required": 0,
                        "has_explicit_profile_capture": 0,
                        "needs_profile_completion": 1,
                        "low_profile_richness": 0,
                    },
                    "public_summary": {"needs_profile_completion": 0},
                },
                {
                    "candidate_identity_key": "linkedin:hidden-poison",
                    "visibility_state": "hidden",
                    "projection_metrics": {
                        "has_explicit_profile_capture": "garbage",
                        "needs_profile_completion": None,
                        "low_profile_richness": 2,
                    },
                },
            ],
            replace_members=True,
        )
        legacy_integer_payload = self.reader.get_projection(legacy_integer["projection"]["projection_id"])
        legacy_integer_readiness = legacy_integer_payload["projection"]["readiness"]
        self.assertEqual(legacy_integer_readiness["profile_required_count"], 1)
        self.assertEqual(legacy_integer_readiness["explicit_profile_capture_candidate_count"], 1)
        self.assertEqual(legacy_integer_readiness["needs_profile_completion_candidate_count"], 1)
        self.assertEqual(legacy_integer_readiness["low_profile_richness_candidate_count"], 1)

        poisoned = self.writer.publish_run_scope_projection(
            run_id="job-owned-quality-poisoned-booleans",
            collection_id="company:lovable",
            members=[
                {
                    "candidate_identity_key": "linkedin:poison-none-number-string",
                    "profile_readiness": "not_required",
                    "projection_metrics": {
                        "profile_required": "false",
                        "has_explicit_profile_capture": None,
                        "needs_profile_completion": 2,
                        "low_profile_richness": "garbage",
                    },
                    "public_summary": {"needs_profile_completion": "garbage"},
                },
                {
                    "candidate_identity_key": "linkedin:poison-string-number-float",
                    "profile_readiness": "skipped",
                    "projection_metrics": {
                        "profile_required": "garbage",
                        "has_explicit_profile_capture": "false",
                        "needs_profile_completion": -1,
                        "low_profile_richness": 1.0,
                    },
                    "public_summary": {"needs_profile_completion": "false"},
                },
                {
                    "candidate_identity_key": "linkedin:readiness-still-required",
                    "profile_readiness": "required",
                    "projection_metrics": {
                        "profile_required": False,
                        "has_explicit_profile_capture": False,
                        "needs_profile_completion": 0,
                        "low_profile_richness": False,
                    },
                    "public_summary": {"needs_profile_completion": False},
                },
            ],
            replace_members=True,
        )
        poisoned_payload = self.reader.get_projection(poisoned["projection"]["projection_id"])
        poisoned_readiness = poisoned_payload["projection"]["readiness"]
        self.assertEqual(poisoned_readiness["profile_required_count"], 1)
        self.assertNotIn("explicit_profile_capture_candidate_count", poisoned_readiness)
        self.assertNotIn("needs_profile_completion_candidate_count", poisoned_readiness)
        self.assertNotIn("low_profile_richness_candidate_count", poisoned_readiness)

        stored_projection = self.store.repos.serving_projection.get(exact["projection"]["projection_id"])
        missing_revision_projection = {
            **stored_projection,
            "metadata": {
                key: value
                for key, value in dict(stored_projection.get("metadata") or {}).items()
                if key != PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY
            },
        }
        with mock.patch.object(
            self.store.repos.serving_projection,
            "get",
            side_effect=[missing_revision_projection],
        ):
            missing_revision = self.reader.get_projection(exact["projection"]["projection_id"])
        self.assertEqual(missing_revision["status"], "not_ready")
        self.assertEqual(missing_revision["reason"], "projection_membership_revision_missing")

        changed_revision_projection = {
            **stored_projection,
            "metadata": {
                **dict(stored_projection.get("metadata") or {}),
                PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY: "projidxinput_changed_during_read",
            },
        }
        with mock.patch.object(
            self.store.repos.serving_projection,
            "get",
            side_effect=[stored_projection, changed_revision_projection],
        ):
            changed_revision = self.reader.get_projection(exact["projection"]["projection_id"])
        self.assertEqual(changed_revision["status"], "not_ready")
        self.assertEqual(changed_revision["reason"], "projection_membership_revision_changed_during_read")

        legacy = self.writer.publish_run_scope_projection(
            run_id="job-missing-quality-evidence",
            collection_id="company:legacy",
            members=[
                {
                    "candidate_identity_key": "linkedin:legacy",
                    "profile_readiness": "ready",
                    "card_readiness": "ready",
                }
            ],
            readiness={
                "explicit_profile_capture_candidate_count": 1,
                "needs_profile_completion_candidate_count": 1,
                "low_profile_richness_candidate_count": 1,
            },
            replace_members=True,
        )
        legacy_payload = self.reader.get_projection(legacy["projection"]["projection_id"])
        legacy_readiness = legacy_payload["projection"]["readiness"]
        self.assertNotIn("explicit_profile_capture_candidate_count", legacy_readiness)
        self.assertNotIn("needs_profile_completion_candidate_count", legacy_readiness)
        self.assertNotIn("low_profile_richness_candidate_count", legacy_readiness)

    def test_candidate_page_fails_closed_when_membership_changes_during_read(self) -> None:
        first = self.writer.publish_run_scope_projection(
            run_id="job-page-revision-fence",
            collection_id="company:lovable",
            members=[
                {
                    "candidate_identity_key": "linkedin:old-member",
                    "projection_metrics": {
                        "has_explicit_profile_capture": False,
                        "needs_profile_completion": False,
                        "low_profile_richness": False,
                    },
                }
            ],
            replace_members=True,
        )
        projection_id = first["projection"]["projection_id"]
        repository = self.store.repos.serving_projection
        original_list_members = repository.list_members
        replacement_published = False

        def list_then_replace(*args, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal replacement_published
            rows = original_list_members(*args, **kwargs)
            if not replacement_published:
                replacement_published = True
                self.writer.publish_run_scope_projection(
                    run_id="job-page-revision-fence",
                    collection_id="company:lovable",
                    members=[
                        {
                            "candidate_identity_key": "linkedin:new-member",
                            "projection_metrics": {
                                "has_explicit_profile_capture": True,
                                "needs_profile_completion": False,
                                "low_profile_richness": False,
                            },
                        }
                    ],
                    replace_members=True,
                )
            return rows

        with mock.patch.object(repository, "list_members", side_effect=list_then_replace):
            page = self.reader.get_projection_candidates(projection_id, offset=0, limit=10)

        self.assertEqual(page["status"], "not_ready")
        self.assertEqual(page["reason"], "projection_membership_revision_changed_during_page_read")
        self.assertTrue(page["membership_revision"])

    def test_person_detail_fails_closed_when_membership_changes_during_read(self) -> None:
        first = self.writer.publish_run_scope_projection(
            run_id="job-detail-revision-fence",
            projection_id="proj_detail_revision_fence",
            members=[
                {
                    "candidate_identity_key": "linkedin:detail-old",
                    "person_identity_key": "linkedin:detail-old",
                }
            ],
            replace_members=True,
        )
        projection_id = first["projection"]["projection_id"]
        repository = self.store.repos.serving_projection
        original_list = repository.list_members_by_identity_keys

        def list_then_replace(*args, **kwargs):  # type: ignore[no-untyped-def]
            rows = original_list(*args, **kwargs)
            self.writer.publish_run_scope_projection(
                run_id="job-detail-revision-fence",
                projection_id=projection_id,
                members=[
                    {
                        "candidate_identity_key": "linkedin:detail-new",
                        "person_identity_key": "linkedin:detail-new",
                    }
                ],
                replace_members=True,
            )
            return rows

        with mock.patch.object(repository, "list_members_by_identity_keys", side_effect=list_then_replace):
            detail = self.reader.get_projection_person_detail(projection_id, "linkedin:detail-old")

        self.assertEqual(detail["status"], "not_ready")
        self.assertEqual(
            detail["reason"],
            "projection_membership_revision_changed_during_member_snapshot",
        )

    def test_search_fails_closed_when_membership_changes_after_index_hydration(self) -> None:
        projection_id = "proj_search_revision_fence"
        self.writer.publish_run_scope_projection(
            run_id="job-search-revision-fence",
            projection_id=projection_id,
            members=[
                {
                    "candidate_identity_key": "linkedin:search-old",
                    "person_identity_key": "linkedin:search-old",
                    "public_summary": {"display_name": "Old Search Engineer"},
                }
            ],
            replace_members=True,
        )
        self.person_asset_writer.rebuild_projection_person_search_index(
            projection_id=projection_id,
            count_scope="exact_projection",
        )
        original_hydrate = self.reader._hydrate_index_page_members  # noqa: SLF001

        def hydrate_then_replace(*args, **kwargs):  # type: ignore[no-untyped-def]
            members = original_hydrate(*args, **kwargs)
            self.writer.publish_run_scope_projection(
                run_id="job-search-revision-fence",
                projection_id=projection_id,
                members=[
                    {
                        "candidate_identity_key": "linkedin:search-new",
                        "person_identity_key": "linkedin:search-new",
                        "public_summary": {"display_name": "New Search Engineer"},
                    }
                ],
                replace_members=True,
            )
            return members

        with mock.patch.object(self.reader, "_hydrate_index_page_members", side_effect=hydrate_then_replace):
            search = self.reader.search_projection_person_index(
                projection_id,
                search_keyword="Old Search",
            )

        self.assertEqual(search["status"], "not_ready")
        self.assertEqual(search["reason"], "projection_membership_revision_changed_during_search_read")

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
        pointer = self.store.repos.serving_projection.get_authoritative_pointer("company:google")

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
        self.assertEqual(second["projection"]["created_at"], first["projection"]["created_at"])
        self.assertEqual(second["link"]["created_at"], first["link"]["created_at"])
        self.assertEqual(self.store.repos.serving_projection.count_members("proj_existing_run_scope"), 1)

    def test_explicit_projection_id_wins_over_existing_run_link(self) -> None:
        first = self.writer.publish_run_scope_projection(
            run_id="job-explicit-projection-wins",
            members=[],
            replace_members=True,
        )
        second = self.writer.publish_run_scope_projection(
            run_id="job-explicit-projection-wins",
            projection_id="proj_explicit_winner",
            members=[
                {
                    "candidate_identity_key": "linkedin:explicit-winner",
                    "person_identity_key": "linkedin:explicit-winner",
                }
            ],
            replace_members=True,
        )

        self.assertNotEqual(first["projection"]["projection_id"], "proj_explicit_winner")
        self.assertEqual(second["projection"]["projection_id"], "proj_explicit_winner")
        self.assertEqual(second["link"]["projection_id"], "proj_explicit_winner")
        members = self.store.repos.serving_projection.list_members(
            "proj_explicit_winner",
            visible_only=False,
        )
        self.assertEqual(members[0]["public_summary"]["source_projection_id"], "proj_explicit_winner")

    def test_replace_members_can_atomically_clear_an_existing_projection(self) -> None:
        first = self.writer.publish_run_scope_projection(
            run_id="job-atomic-clear",
            collection_id="company:openai",
            projection_id="proj_atomic_clear",
            members=[
                {
                    "candidate_identity_key": "linkedin:old-member",
                    "person_identity_key": "linkedin:old-member",
                }
            ],
            replace_members=True,
        )

        second = self.writer.publish_run_scope_projection(
            run_id="job-atomic-clear",
            collection_id="company:openai",
            members=[],
            replace_members=True,
            metadata={"replacement": "empty"},
        )

        self.assertEqual(second["projection"]["projection_id"], first["projection"]["projection_id"])
        self.assertEqual(second["member_count"], 0)
        self.assertEqual(self.store.repos.serving_projection.count_members("proj_atomic_clear"), 0)
        self.assertEqual(second["projection"]["metadata"]["replacement"], "empty")

    def test_collection_replace_failure_rolls_back_metadata_members_and_pointer(self) -> None:
        first = self.writer.publish_collection_authoritative_projection(
            collection_id="company:atomic-rollback",
            active_collection_version="v1",
            members=[
                {
                    "candidate_identity_key": "linkedin:preserved",
                    "person_identity_key": "linkedin:preserved",
                    "public_summary": {"name": "Preserved"},
                }
            ],
            replace_members=True,
            metadata={"publication": "preserved"},
        )
        projection_id = first["projection"]["projection_id"]
        before_projection = self.store.repos.serving_projection.get(projection_id)
        before_members = self.store.repos.serving_projection.list_members(
            projection_id,
            visible_only=False,
        )
        before_pointer = self.store.repos.serving_projection.get_authoritative_pointer("company:atomic-rollback")
        adapter = self.store._control_plane_postgres  # noqa: SLF001
        constraint_name = "test_serving_projection_member_atomic_rollback"
        with adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    f"ALTER TABLE serving_projection_members ADD CONSTRAINT {constraint_name} "
                    "CHECK (candidate_identity_key <> 'linkedin:reject-atomic')"
                )
            connection.commit()
        try:
            with mock.patch(
                "sourcing_agent.control_plane_live_postgres._BULK_UPSERT_DIRECT_PARAM_LIMIT",
                23,
            ):
                with self.assertRaisesRegex(RuntimeError, "publish_serving_projection"):
                    self.writer.publish_collection_authoritative_projection(
                        collection_id="company:atomic-rollback",
                        active_collection_version="v1",
                        members=[
                            {
                                "candidate_identity_key": "linkedin:first-new-chunk",
                                "person_identity_key": "linkedin:first-new-chunk",
                            },
                            {
                                "candidate_identity_key": "linkedin:reject-atomic",
                                "person_identity_key": "linkedin:reject-atomic",
                            },
                        ],
                        replace_members=True,
                        metadata={"publication": "must-roll-back"},
                    )
        finally:
            with adapter._connect() as connection:  # noqa: SLF001
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"ALTER TABLE serving_projection_members DROP CONSTRAINT IF EXISTS {constraint_name}"
                    )
                connection.commit()

        self.assertEqual(self.store.repos.serving_projection.get(projection_id), before_projection)
        self.assertEqual(
            self.store.repos.serving_projection.list_members(projection_id, visible_only=False),
            before_members,
        )
        self.assertEqual(
            self.store.repos.serving_projection.get_authoritative_pointer("company:atomic-rollback"),
            before_pointer,
        )

    def test_run_link_failure_rolls_back_parent_and_members(self) -> None:
        adapter = self.store._control_plane_postgres  # noqa: SLF001
        constraint_name = "test_run_projection_link_atomic_rollback"
        with adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    f"ALTER TABLE run_projection_links ADD CONSTRAINT {constraint_name} "
                    "CHECK (run_id <> 'job-route-rollback')"
                )
            connection.commit()
        try:
            with self.assertRaisesRegex(RuntimeError, "publish_serving_projection"):
                self.writer.publish_run_scope_projection(
                    run_id="job-route-rollback",
                    members=[
                        {
                            "candidate_identity_key": "linkedin:must-roll-back",
                            "person_identity_key": "linkedin:must-roll-back",
                        }
                    ],
                    replace_members=True,
                )
        finally:
            with adapter._connect() as connection:  # noqa: SLF001
                with connection.cursor() as cursor:
                    cursor.execute(f"ALTER TABLE run_projection_links DROP CONSTRAINT IF EXISTS {constraint_name}")
                connection.commit()

        self.assertEqual(
            self.store.repos.serving_projection.list(source_run_id="job-route-rollback", limit=10),
            [],
        )
        self.assertEqual(self.store.repos.serving_projection.get_run_link("job-route-rollback"), {})
        self.assertEqual(adapter.count_rows("serving_projection_members"), 0)

    def test_collection_pointer_failure_rolls_back_parent_and_members(self) -> None:
        adapter = self.store._control_plane_postgres  # noqa: SLF001
        constraint_name = "test_collection_projection_pointer_atomic_rollback"
        with adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    f"ALTER TABLE collection_authoritative_pointers ADD CONSTRAINT {constraint_name} "
                    "CHECK (collection_id <> 'company:route-rollback')"
                )
            connection.commit()
        try:
            with self.assertRaisesRegex(RuntimeError, "publish_serving_projection"):
                self.writer.publish_collection_authoritative_projection(
                    collection_id="company:route-rollback",
                    active_collection_version="v1",
                    members=[
                        {
                            "candidate_identity_key": "linkedin:pointer-must-roll-back",
                            "person_identity_key": "linkedin:pointer-must-roll-back",
                        }
                    ],
                    replace_members=True,
                )
        finally:
            with adapter._connect() as connection:  # noqa: SLF001
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"ALTER TABLE collection_authoritative_pointers DROP CONSTRAINT IF EXISTS {constraint_name}"
                    )
                connection.commit()

        self.assertEqual(
            self.store.repos.serving_projection.list(collection_id="company:route-rollback", limit=10),
            [],
        )
        self.assertEqual(
            self.store.repos.serving_projection.get_authoritative_pointer("company:route-rollback"),
            {},
        )
        self.assertEqual(adapter.count_rows("serving_projection_members"), 0)

    def test_reader_fails_closed_when_authoritative_member_count_is_unavailable(self) -> None:
        self.writer.publish_run_scope_projection(
            run_id="job-member-count-unavailable",
            projection_id="proj_member_count_unavailable",
            members=[
                {
                    "candidate_identity_key": "linkedin:counted",
                    "person_identity_key": "linkedin:counted",
                }
            ],
            replace_members=True,
        )

        with mock.patch.object(
            self.store._control_plane_postgres,  # noqa: SLF001
            "count_rows",
            side_effect=RuntimeError("postgres unavailable"),
        ):
            payload = self.reader.get_projection("proj_member_count_unavailable")

        self.assertEqual(payload["status"], "not_ready")
        self.assertEqual(payload["reason"], "projection_members_unavailable")
        self.assertTrue(payload["read_contract"]["fail_closed"])
        self.assertFalse(payload["read_contract"]["fallback_used"])

    def test_reader_fails_closed_when_authoritative_member_table_is_missing(self) -> None:
        with pg_backed_control_plane_store(schema_label="serving_projection_missing_member_table") as store:
            writer = ServingProjectionWriter(store)
            reader = ServingProjectionReader(store)
            writer.publish_run_scope_projection(
                run_id="job-member-table-missing",
                projection_id="proj_member_table_missing",
                members=[],
                replace_members=True,
            )
            adapter = store._control_plane_postgres  # noqa: SLF001
            schema = str(adapter.schema or "public").replace('"', '""')
            with adapter._connect() as connection:  # noqa: SLF001
                with connection.cursor() as cursor:
                    cursor.execute(f'DROP TABLE "{schema}"."serving_projection_members"')
                connection.commit()

            payload = reader.get_projection("proj_member_table_missing")

        self.assertEqual(payload["status"], "not_ready")
        self.assertEqual(payload["reason"], "projection_members_unavailable")
        self.assertTrue(payload["read_contract"]["fail_closed"])
        self.assertFalse(payload["read_contract"]["fallback_used"])

    def test_concurrent_first_run_publications_share_one_random_identity_without_orphans(self) -> None:
        barrier = threading.Barrier(2)
        adapter = self.store._control_plane_postgres  # noqa: SLF001
        repository = self.store.repos.serving_projection
        original_publish = repository._publish_projection_with_route  # noqa: SLF001
        adapter.close()
        with mock.patch.dict(
            os.environ,
            {
                "SOURCING_CONTROL_PLANE_PG_POOL_MIN": "1",
                "SOURCING_CONTROL_PLANE_PG_POOL_MAX": "1",
            },
        ):
            pool = adapter._ensure_pool()  # noqa: SLF001
        self.assertEqual(pool.max_size, 1)

        def synchronized_publish(**kwargs: object) -> dict[str, object]:
            barrier.wait(timeout=5)
            return original_publish(**kwargs)

        def publish(candidate_key: str) -> dict[str, object]:
            return ServingProjectionWriter(self.store).publish_run_scope_projection(
                run_id="job-concurrent-first-publication",
                members=[
                    {
                        "candidate_identity_key": candidate_key,
                        "person_identity_key": candidate_key,
                    }
                ],
                replace_members=True,
            )

        with mock.patch.object(repository, "_publish_projection_with_route", side_effect=synchronized_publish):
            with mock.patch(
                "sourcing_agent.repositories.serving_projection._build_projection_id",
                wraps=_build_projection_id,
            ) as projection_id_factory:
                with mock.patch(
                    "sourcing_agent.control_plane_live_postgres._utc_now_sql_timestamp",
                    side_effect=["2026-07-10 12:00:01", "2026-07-10 12:00:02"],
                ):
                    with ThreadPoolExecutor(max_workers=2) as executor:
                        futures = [
                            executor.submit(publish, candidate_key)
                            for candidate_key in ("linkedin:concurrent-a", "linkedin:concurrent-b")
                        ]
                        results = [future.result(timeout=10) for future in futures]

        self.assertEqual(projection_id_factory.call_count, 1)

        projection_ids = {str(dict(result.get("projection") or {}).get("projection_id") or "") for result in results}
        self.assertEqual(len(projection_ids), 1)
        projection_id = next(iter(projection_ids))
        self.assertRegex(projection_id, r"^proj_[0-9a-f]{32}$")
        projections = self.store.repos.serving_projection.list(
            source_run_id="job-concurrent-first-publication",
            limit=10,
        )
        members = self.store.repos.serving_projection.list_members(projection_id, visible_only=False)
        self.assertEqual(len(projections), 1)
        self.assertEqual(projections[0]["created_at"], "2026-07-10 12:00:01")
        self.assertEqual(projections[0]["updated_at"], "2026-07-10 12:00:02")
        self.assertEqual(len(members), 1)
        self.assertEqual(members[0]["public_summary"]["source_projection_id"], projection_id)
        self.assertIn(
            members[0]["candidate_identity_key"],
            {"linkedin:concurrent-a", "linkedin:concurrent-b"},
        )

    def test_concurrent_first_collection_publications_share_one_random_identity_without_orphans(self) -> None:
        barrier = threading.Barrier(2)
        adapter = self.store._control_plane_postgres  # noqa: SLF001
        repository = self.store.repos.serving_projection
        original_publish = repository._publish_projection_with_route  # noqa: SLF001
        adapter.close()
        with mock.patch.dict(
            os.environ,
            {
                "SOURCING_CONTROL_PLANE_PG_POOL_MIN": "1",
                "SOURCING_CONTROL_PLANE_PG_POOL_MAX": "1",
            },
        ):
            pool = adapter._ensure_pool()  # noqa: SLF001
        self.assertEqual(pool.max_size, 1)

        def synchronized_publish(**kwargs: object) -> dict[str, object]:
            barrier.wait(timeout=5)
            return original_publish(**kwargs)

        def publish(candidate_key: str) -> dict[str, object]:
            return ServingProjectionWriter(self.store).publish_collection_authoritative_projection(
                collection_id="company:concurrent",
                active_collection_version="v1",
                members=[
                    {
                        "candidate_identity_key": candidate_key,
                        "person_identity_key": candidate_key,
                    }
                ],
                replace_members=True,
            )

        with mock.patch.object(repository, "_publish_projection_with_route", side_effect=synchronized_publish):
            with mock.patch(
                "sourcing_agent.repositories.serving_projection._build_projection_id",
                wraps=_build_projection_id,
            ) as projection_id_factory:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = [
                        executor.submit(publish, candidate_key)
                        for candidate_key in ("linkedin:collection-a", "linkedin:collection-b")
                    ]
                    results = [future.result(timeout=10) for future in futures]

        self.assertEqual(projection_id_factory.call_count, 1)
        projection_ids = {str(dict(result.get("projection") or {}).get("projection_id") or "") for result in results}
        self.assertEqual(len(projection_ids), 1)
        projection_id = next(iter(projection_ids))
        self.assertRegex(projection_id, r"^proj_[0-9a-f]{32}$")
        pointer = self.store.repos.serving_projection.get_authoritative_pointer("company:concurrent")
        projections = self.store.repos.serving_projection.list(
            collection_id="company:concurrent",
            projection_type="collection_authoritative_projection",
            limit=10,
        )
        members = self.store.repos.serving_projection.list_members(projection_id, visible_only=False)
        self.assertEqual(pointer["active_projection_id"], projection_id)
        self.assertEqual(len(projections), 1)
        self.assertEqual(len(members), 1)
        self.assertEqual(members[0]["public_summary"]["source_projection_id"], projection_id)

    def test_reader_does_not_hide_non_authoritative_programming_errors(self) -> None:
        self.writer.publish_run_scope_projection(
            run_id="job-member-programming-error",
            projection_id="proj_member_programming_error",
            members=[],
            replace_members=True,
        )

        with mock.patch.object(
            self.store.repos.serving_projection,
            "count_members",
            side_effect=RuntimeError("programming invariant"),
        ):
            with self.assertRaisesRegex(RuntimeError, "programming invariant"):
                self.reader.get_projection("proj_member_programming_error")

    def test_candidate_page_fails_closed_when_authoritative_member_page_is_unavailable(self) -> None:
        self.writer.publish_run_scope_projection(
            run_id="job-member-page-unavailable",
            projection_id="proj_member_page_unavailable",
            members=[
                {
                    "candidate_identity_key": "linkedin:paged",
                    "person_identity_key": "linkedin:paged",
                }
            ],
            replace_members=True,
        )

        with mock.patch.object(
            self.store.repos.serving_projection,
            "list_members",
            side_effect=ControlPlaneAuthoritativeReadError("postgres unavailable"),
        ):
            payload = self.reader.get_projection_candidates("proj_member_page_unavailable")

        self.assertEqual(payload["status"], "not_ready")
        self.assertEqual(payload["reason"], "projection_members_unavailable")
        self.assertTrue(payload["read_contract"]["fail_closed"])
        self.assertFalse(payload["read_contract"]["fallback_used"])

    def test_reader_fails_closed_and_serves_only_public_projection_fields(self) -> None:
        missing = self.reader.get_projection("proj_missing")
        self.assertEqual(missing["status"], "not_ready")
        self.assertEqual(missing["reason"], "projection_not_found")
        self.assertTrue(missing["read_contract"]["fail_closed"])
        self.assertFalse(missing["read_contract"]["fallback_used"])

        with mock.patch.object(
            self.store.repos.serving_projection,
            "get",
            return_value={
                "projection_id": "proj_non_shared",
                "projection_type": "tenant_private_projection",
                "state": "serving",
            },
        ):
            non_shared = self.reader.get_projection("proj_non_shared")
        self.assertEqual(non_shared["status"], "not_ready")
        self.assertEqual(non_shared["reason"], "projection_not_found")
        self.assertNotIn("projection_state", non_shared)

        self.store.repos.serving_projection.upsert(
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

        projection_payload = self.reader.get_projection("proj_reader")
        page = self.reader.get_projection_candidates("proj_reader", offset=0, limit=10)
        row = page["candidates"][0]

        self.assertEqual(projection_payload["status"], "ready")
        membership_revision = projection_payload["projection"]["membership_revision"]
        self.assertTrue(membership_revision.startswith("projidxinput_"))
        self.assertEqual(page["projection"]["membership_revision"], membership_revision)
        self.assertNotIn("metadata", projection_payload["projection"])
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
            self.store.repos.serving_projection,
            "list_members",
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

    def test_index_hydration_preserves_index_order_and_rejects_membership_drift(self) -> None:
        projection_id = "proj_index_hydration_integrity"
        self.writer.publish_run_scope_projection(
            run_id="job-index-hydration-integrity",
            projection_id=projection_id,
            members=[
                {
                    "candidate_identity_key": "linkedin:zulu",
                    "person_identity_key": "linkedin:zulu",
                    "rank_index": 1,
                    "visibility_state": "visible",
                    "public_summary": {"display_name": "Zulu Engineer", "headline": "Engineer"},
                },
                {
                    "candidate_identity_key": "linkedin:alpha",
                    "person_identity_key": "linkedin:alpha",
                    "rank_index": 2,
                    "visibility_state": "visible",
                    "public_summary": {"display_name": "Alpha Engineer", "headline": "Engineer"},
                },
                {
                    "candidate_identity_key": "linkedin:hidden",
                    "person_identity_key": "linkedin:hidden",
                    "rank_index": 3,
                    "visibility_state": "hidden",
                    "public_summary": {"display_name": "Hidden Engineer", "headline": "Engineer"},
                },
            ],
            replace_members=True,
        )
        self.person_asset_writer.rebuild_projection_person_search_index(
            projection_id=projection_id,
            count_scope="exact_projection",
        )
        repository = self.store.repos.serving_projection

        ordered = self.reader.search_projection_person_index(projection_id, search_keyword="Engineer")
        self.assertEqual(ordered["status"], "ready")
        self.assertEqual(
            [row["candidate_identity_key"] for row in ordered["candidates"]],
            ["linkedin:alpha", "linkedin:zulu"],
        )

        ready_index_result = repository.search_person_index(
            projection_id,
            search_keyword="Engineer",
            offset=0,
            limit=10,
        )
        alpha_member = repository.get_member(projection_id, "linkedin:alpha")
        hidden_member = repository.get_member(projection_id, "linkedin:hidden")
        drift_cases = {
            "missing_member": {
                "index_result": ready_index_result,
                "members": [alpha_member],
            },
            "hidden_member": {
                "index_result": {
                    **ready_index_result,
                    "candidate_identity_keys": ["linkedin:hidden"],
                    "matched_count": 1,
                },
                "members": [hidden_member],
            },
            "duplicate_index_key": {
                "index_result": {
                    **ready_index_result,
                    "candidate_identity_keys": ["linkedin:alpha", "linkedin:alpha"],
                    "matched_count": 2,
                },
                "members": [alpha_member],
            },
        }
        for label, drift_case in drift_cases.items():
            with (
                self.subTest(label=label),
                mock.patch.object(repository, "search_person_index", return_value=drift_case["index_result"]),
                mock.patch.object(
                    repository,
                    "list_members_by_identity_keys",
                    return_value=drift_case["members"],
                ),
            ):
                payload = self.reader.search_projection_person_index(
                    projection_id,
                    search_keyword="Engineer",
                    limit=10,
                )
                self.assertEqual(payload["status"], "not_ready")
                self.assertEqual(payload["reason"], "projection_person_search_index_unavailable")
                self.assertEqual(payload["filtered_candidate_count"], 0)
                self.assertEqual(payload["candidates"], [])
                self.assertFalse(payload["has_more"])
                self.assertIsNone(payload["next_offset"])

        detail = self.reader.get_projection_person_detail(projection_id, "linkedin:hidden")
        self.assertEqual(detail["status"], "not_ready")
        self.assertEqual(detail["reason"], "projection_member_not_found")

    def test_projection_index_read_fault_fails_closed_before_member_hydration(self) -> None:
        projection_id = "proj_index_read_fault"
        self.writer.publish_run_scope_projection(
            run_id="job-index-read-fault",
            projection_id=projection_id,
            members=[
                {
                    "candidate_identity_key": "linkedin:index-read-fault",
                    "person_identity_key": "linkedin:index-read-fault",
                }
            ],
            replace_members=True,
        )
        repository = self.store.repos.serving_projection
        with (
            mock.patch.object(
                repository,
                "search_person_index",
                side_effect=ControlPlaneAuthoritativeReadError("projection index unavailable"),
            ),
            mock.patch.object(repository, "list_members_by_identity_keys") as hydrate_members,
        ):
            search = self.reader.search_projection_person_index(projection_id, search_keyword="fault")
            filtered = self.reader.get_projection_candidates(
                projection_id,
                candidate_filter={"search_keyword": "fault"},
            )

        for payload in (search, filtered):
            self.assertEqual(payload["status"], "not_ready")
            self.assertEqual(payload["reason"], "projection_person_search_index_unavailable")
            self.assertEqual(payload["candidates"], [])
            self.assertFalse(payload["read_contract"]["fallback_used"])
        hydrate_members.assert_not_called()


if __name__ == "__main__":
    unittest.main()


class ServingProjectionFacetOwnershipTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    """FT1-FF (finding 5): the canonical function-bucket pair and the
    authoritative employment status set persist through projection members and
    the person search index, and count/filter consumers use the owned values."""

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

    @staticmethod
    def _members_from_records(records):
        from sourcing_agent.orchestrator import SourcingOrchestrator

        orchestrator = object.__new__(SourcingOrchestrator)
        return orchestrator._serving_projection_members_from_records(  # noqa: SLF001
            records,
            source_run_id="job-facet-ownership",
        )

    def test_owned_facet_projection_persists_through_members_index_and_counts(self) -> None:
        cohort_record = {
            "candidate_id": "c-cohort",
            "display_name": "Dual Role",
            "employment_status": "current",
            "linkedin_url": "https://www.linkedin.com/in/dual-role/",
            "metadata": {
                "cohort_lane_membership": [
                    {
                        "lane_id": "cohort_current_research_d",
                        "employment_status": "current",
                        "role_bucket_id": "research",
                    },
                    {
                        "lane_id": "cohort_former_engineering_d",
                        "employment_status": "former",
                        "role_bucket_id": "engineering",
                    },
                ],
                "cohort_role_bucket_ids": ["research", "engineering"],
                "cohort_employment_statuses": ["current", "former"],
            },
        }
        members = self._members_from_records([cohort_record])
        self.assertEqual(len(members), 1)
        public_summary = members[0]["public_summary"]
        self.assertEqual(public_summary["function_bucket_ids"], ["research", "engineering"])
        self.assertEqual(public_summary["function_bucket_source"], "lane_membership")
        self.assertEqual(public_summary["employment_statuses"], ["current", "former"])

        published = self.writer.publish_run_scope_projection(
            run_id="job-facet-ownership",
            projection_id="proj_facet_ownership",
            members=members,
            replace_members=True,
        )
        projection_id = published["projection"]["projection_id"]
        persisted_members = self.store.repos.serving_projection.list_members(projection_id)
        persisted_summary = persisted_members[0]["public_summary"]
        # The owned pair and the authoritative status set survive persistence;
        # the lossy scalar employment_scope stays a display-only field.
        self.assertEqual(persisted_summary["function_bucket_ids"], ["research", "engineering"])
        self.assertEqual(persisted_summary["function_bucket_source"], "lane_membership")
        self.assertEqual(persisted_summary["employment_statuses"], ["current", "former"])
        self.assertEqual(persisted_members[0]["employment_scope"], "current")

        indexed = self.person_asset_writer.rebuild_projection_person_search_index(
            projection_id=projection_id,
            count_scope="exact_projection",
        )
        self.assertEqual(indexed["status"], "indexed")
        index_rows = self.store.repos.serving_projection.list_person_search_index_rows(
            projection_id,
            offset=0,
            limit=10,
        )
        self.assertEqual(len(index_rows), 1)
        filter_record = dict(dict(index_rows[0].get("metadata") or {}).get("filter_record") or {})
        self.assertEqual(filter_record["function_bucket_ids"], ["research", "engineering"])
        self.assertEqual(filter_record["function_bucket_source"], "lane_membership")
        self.assertEqual(filter_record["employment_statuses"], ["current", "former"])

        # Facet counts consume the owned values: the dual-status candidate is
        # counted under BOTH statuses and BOTH function buckets, even though the
        # display scalar shows only one status.
        projection = self.store.repos.serving_projection.get(projection_id)
        counts = dict(dict(projection.get("counts") or {}).get("public_facet_counts") or {})
        self.assertEqual(counts["employment_counts"], {"current": 1, "former": 1})
        self.assertEqual(counts["function_counts"], {"research": 1, "engineering": 1})

        # Filter reads consume the owned values through the index path.
        former_filter = self.reader.get_projection_candidates(
            projection_id,
            candidate_filter={"employment_statuses": ["former"]},
        )
        self.assertEqual(former_filter["status"], "ready")
        self.assertEqual(former_filter["filtered_candidate_count"], 1)
        infra_filter = self.reader.get_projection_candidates(
            projection_id,
            candidate_filter={"function_buckets": ["infra_systems"]},
        )
        self.assertEqual(infra_filter["status"], "ready")
        self.assertEqual(infra_filter["filtered_candidate_count"], 0)
        engineering_filter = self.reader.get_projection_candidates(
            projection_id,
            candidate_filter={"function_buckets": ["engineering"]},
        )
        self.assertEqual(engineering_filter["filtered_candidate_count"], 1)

    def test_member_projection_change_bumps_index_input_revision(self) -> None:
        record_v1 = {
            "candidate_id": "c-rev",
            "display_name": "Revision Candidate",
            "employment_status": "current",
            "linkedin_url": "https://www.linkedin.com/in/revision-candidate/",
            "metadata": {
                "cohort_lane_membership": [
                    {
                        "lane_id": "cohort_current_research_d",
                        "employment_status": "current",
                        "role_bucket_id": "research",
                    },
                ],
                "cohort_role_bucket_ids": ["research"],
                "cohort_employment_statuses": ["current"],
            },
        }
        first = self.writer.publish_run_scope_projection(
            run_id="job-facet-revision",
            projection_id="proj_facet_revision",
            members=self._members_from_records([record_v1]),
            replace_members=True,
        )
        projection_id = first["projection"]["projection_id"]
        first_revision = str(
            dict(first["projection"].get("metadata") or {}).get(PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY) or ""
        )
        self.assertTrue(first_revision)
        self.person_asset_writer.rebuild_projection_person_search_index(
            projection_id=projection_id,
            count_scope="exact_projection",
        )

        record_v2 = {
            **record_v1,
            "metadata": {
                "cohort_lane_membership": [
                    {
                        "lane_id": "cohort_current_engineering_d",
                        "employment_status": "current",
                        "role_bucket_id": "engineering",
                    },
                ],
                "cohort_role_bucket_ids": ["engineering"],
                "cohort_employment_statuses": ["current"],
            },
        }
        second = self.writer.publish_run_scope_projection(
            run_id="job-facet-revision",
            projection_id="proj_facet_revision",
            members=self._members_from_records([record_v2]),
            replace_members=True,
        )
        second_revision = str(
            dict(second["projection"].get("metadata") or {}).get(PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY) or ""
        )
        # The changed bucket pair is a semantic member change: the input
        # revision bumps so the pre-change index reads stale (fail closed).
        self.assertTrue(second_revision)
        self.assertNotEqual(first_revision, second_revision)
        stale_read = self.reader.get_projection_candidates(
            projection_id,
            candidate_filter={"function_buckets": ["engineering"]},
        )
        self.assertEqual(stale_read["status"], "not_ready")

        # Control: republishing identical members keeps the revision stable.
        third = self.writer.publish_run_scope_projection(
            run_id="job-facet-revision",
            projection_id="proj_facet_revision",
            members=self._members_from_records([record_v2]),
            replace_members=True,
        )
        third_revision = str(
            dict(third["projection"].get("metadata") or {}).get(PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY) or ""
        )
        self.assertEqual(second_revision, third_revision)

    def test_malformed_provenance_blocks_member_publication(self) -> None:
        malformed = {
            "candidate_id": "c-malformed",
            "display_name": "Malformed",
            "metadata": {
                "cohort_lane_membership": [
                    {"lane_id": "l1", "employment_status": "current", "role_bucket_id": "research"},
                ],
                "cohort_role_bucket_ids": ["engineering"],
            },
        }
        from sourcing_agent.public_candidate_facets import CohortFacetProvenanceError

        with self.assertRaises(CohortFacetProvenanceError):
            self._members_from_records([malformed])


class CanonicalPublicSummaryAdapterTest(unittest.TestCase):
    """FT1-FF2 (finding 8): build_person_summary_view is the ONE canonical
    public-summary adapter — migration/repair/consolidation can never silently
    downgrade owned facets to unmarked legacy inference."""

    @staticmethod
    def _dual_status_metadata() -> dict:
        return {
            "cohort_lane_membership": [
                {
                    "lane_id": "cohort_current_infra_d",
                    "employment_status": "current",
                    "role_bucket_id": "infra_systems",
                },
                {"lane_id": "cohort_former_infra_d", "employment_status": "former", "role_bucket_id": "infra_systems"},
            ],
            "cohort_role_bucket_ids": ["infra_systems"],
            "cohort_employment_statuses": ["current", "former"],
        }

    def test_owned_pair_and_status_set_are_preserved_and_validated(self) -> None:
        from sourcing_agent.person_identity import build_person_summary_view

        summary = build_person_summary_view(
            {
                "candidate_id": "c1",
                "display_name": "Owned",
                "function_bucket_ids": ["infra_systems"],
                "function_bucket_source": "lane_membership",
                "employment_statuses": ["current", "former"],
            }
        )
        self.assertEqual(summary["function_bucket_ids"], ["infra_systems"])
        self.assertEqual(summary["function_bucket_source"], "lane_membership")
        self.assertEqual(summary["employment_statuses"], ["current", "former"])

    def test_malformed_owned_pair_fails_closed(self) -> None:
        from sourcing_agent.person_identity import build_person_summary_view
        from sourcing_agent.public_candidate_facets import CohortFacetProvenanceError

        with self.assertRaises(CohortFacetProvenanceError):
            build_person_summary_view({"candidate_id": "c1", "function_bucket_ids": ["engineering"]})
        with self.assertRaises(CohortFacetProvenanceError):
            build_person_summary_view(
                {
                    "candidate_id": "c1",
                    "function_bucket_ids": ["engineering"],
                    "function_bucket_source": "lane_membership",
                    "employment_statuses": [],
                }
            )

    def test_explicit_infra_systems_never_downgrades_to_bare_id_engineering(self) -> None:
        from sourcing_agent.person_identity import build_person_summary_view

        summary = build_person_summary_view(
            {
                "candidate_id": "c1",
                "display_name": "Infra",
                "role_bucket": "infra_systems",
                "function_ids": ["8"],
            }
        )
        self.assertEqual(summary["function_bucket_ids"], ["engineering", "infra_systems"])
        self.assertEqual(summary["function_bucket_source"], "registry_evidence")

    def test_bare_function_id_8_maps_to_engineering_only(self) -> None:
        from sourcing_agent.person_identity import build_person_summary_view

        summary = build_person_summary_view({"candidate_id": "c1", "display_name": "Bare", "function_ids": ["8"]})
        self.assertEqual(summary["function_bucket_ids"], ["engineering"])
        self.assertEqual(summary["function_bucket_source"], "registry_evidence")

    def test_dual_status_membership_survives_the_adapter(self) -> None:
        from sourcing_agent.person_identity import build_person_summary_view

        summary = build_person_summary_view(
            {
                "candidate_id": "c1",
                "display_name": "Dual",
                "employment_status": "current",
                "metadata": self._dual_status_metadata(),
            }
        )
        self.assertEqual(summary["function_bucket_ids"], ["infra_systems"])
        self.assertEqual(summary["function_bucket_source"], "lane_membership")
        self.assertEqual(summary["employment_statuses"], ["current", "former"])

    def test_genuinely_legacy_rows_are_explicitly_marked(self) -> None:
        from sourcing_agent.person_identity import build_person_summary_view

        summary = build_person_summary_view(
            {"candidate_id": "c1", "display_name": "Legacy", "headline": "Software Engineer"}
        )
        self.assertEqual(summary["function_bucket_ids"], ["engineering"])
        self.assertEqual(summary["function_bucket_source"], "legacy_inference")
        self.assertNotIn("employment_statuses", summary)

    def test_migration_member_builder_preserves_infra_and_dual_status(self) -> None:
        from sourcing_agent.serving_projection_migration import _projection_member_from_candidate

        member = _projection_member_from_candidate(  # noqa: SLF001
            {
                "candidate_id": "c1",
                "display_name": "Migrated",
                "employment_status": "current",
                "metadata": self._dual_status_metadata(),
            },
            run_id="run-1",
            rank_index=1,
        )
        public_summary = dict(member.get("public_summary") or {})
        self.assertEqual(public_summary["function_bucket_ids"], ["infra_systems"])
        self.assertEqual(public_summary["function_bucket_source"], "lane_membership")
        self.assertEqual(public_summary["employment_statuses"], ["current", "former"])

    def test_consolidation_member_builder_preserves_infra_and_dual_status(self) -> None:
        from sourcing_agent.asset_consolidation_repair_apply import _projection_members_from_payload

        members = _projection_members_from_payload(  # noqa: SLF001
            payload={
                "candidates": [
                    {
                        "candidate_id": "c1",
                        "display_name": "Consolidated",
                        "employment_status": "current",
                        "function_bucket_ids": ["infra_systems"],
                        "function_bucket_source": "lane_membership",
                        "employment_statuses": ["current", "former"],
                        "metadata": self._dual_status_metadata(),
                    }
                ]
            },
            source_snapshot_id="snap-1",
            target_company="Acme",
            collection_id="company:acme",
        )
        self.assertEqual(len(members), 1)
        public_summary = dict(members[0].get("public_summary") or {})
        self.assertEqual(public_summary["function_bucket_ids"], ["infra_systems"])
        self.assertEqual(public_summary["function_bucket_source"], "lane_membership")
        self.assertEqual(public_summary["employment_statuses"], ["current", "former"])

    def test_index_filter_record_inherits_the_canonical_fields(self) -> None:
        from sourcing_agent.person_asset_writer import _projection_filter_record
        from sourcing_agent.serving_projection_migration import _projection_member_from_candidate

        member = _projection_member_from_candidate(  # noqa: SLF001
            {
                "candidate_id": "c1",
                "display_name": "Indexed",
                "employment_status": "current",
                "metadata": self._dual_status_metadata(),
            },
            run_id="run-1",
            rank_index=1,
        )
        filter_record = _projection_filter_record(  # noqa: SLF001
            member=member,
            public_summary=dict(member.get("public_summary") or {}),
            projection_metrics=dict(member.get("projection_metrics") or {}),
            member_metadata={},
        )
        self.assertEqual(filter_record["function_bucket_ids"], ["infra_systems"])
        self.assertEqual(filter_record["function_bucket_source"], "lane_membership")
        self.assertEqual(filter_record["employment_statuses"], ["current", "former"])


class AdapterCohortProvenancePreservationTest(unittest.TestCase):
    """FT1-FF3 (finding 6): the canonical adapter copies the three
    server-owned Cohort provenance keys BY PRESENCE into summary metadata —
    including the legitimate present-empty role mirror — so a claimed
    lane_membership source stays auditable after migration/consolidation."""

    @staticmethod
    def _dual_status_metadata() -> dict:
        return {
            "cohort_lane_membership": [
                {
                    "lane_id": "cohort_current_infra_d",
                    "employment_status": "current",
                    "role_bucket_id": "infra_systems",
                },
                {"lane_id": "cohort_former_infra_d", "employment_status": "former", "role_bucket_id": "infra_systems"},
            ],
            "cohort_role_bucket_ids": ["infra_systems"],
            "cohort_employment_statuses": ["current", "former"],
        }

    def test_adapter_preserves_provenance_by_presence(self) -> None:
        from sourcing_agent.person_identity import build_person_summary_view

        source_metadata = self._dual_status_metadata()
        summary = build_person_summary_view({"candidate_id": "c1", "display_name": "Dual", "metadata": source_metadata})
        metadata = dict(summary.get("metadata") or {})
        self.assertEqual(metadata.get("cohort_lane_membership"), source_metadata["cohort_lane_membership"])
        self.assertEqual(metadata.get("cohort_role_bucket_ids"), ["infra_systems"])
        self.assertEqual(metadata.get("cohort_employment_statuses"), ["current", "former"])
        self.assertEqual(summary["function_bucket_source"], "lane_membership")

    def test_adapter_preserves_legitimate_present_empty_role_mirror(self) -> None:
        from sourcing_agent.person_identity import build_person_summary_view

        all_roles_metadata = {
            "cohort_lane_membership": [
                {"lane_id": "cohort_current_all_d", "employment_status": "current", "role_bucket_id": ""},
            ],
            "cohort_role_bucket_ids": [],
            "cohort_employment_statuses": ["current"],
        }
        summary = build_person_summary_view(
            {"candidate_id": "c1", "display_name": "All Roles", "metadata": all_roles_metadata}
        )
        metadata = dict(summary.get("metadata") or {})
        self.assertIn("cohort_role_bucket_ids", metadata)
        self.assertEqual(metadata["cohort_role_bucket_ids"], [])
        self.assertEqual(metadata["cohort_employment_statuses"], ["current"])

    def test_legacy_source_stays_byte_identical_without_provenance(self) -> None:
        from sourcing_agent.person_identity import build_person_summary_view

        summary = build_person_summary_view({"candidate_id": "c1", "display_name": "Legacy"})
        self.assertNotIn("metadata", summary)

    def test_migration_and_consolidation_members_carry_provenance(self) -> None:
        from sourcing_agent.asset_consolidation_repair_apply import _projection_members_from_payload
        from sourcing_agent.serving_projection_migration import _projection_member_from_candidate

        member = _projection_member_from_candidate(  # noqa: SLF001
            {
                "candidate_id": "c1",
                "display_name": "Migrated",
                "employment_status": "current",
                "metadata": self._dual_status_metadata(),
            },
            run_id="run-1",
            rank_index=1,
        )
        migrated_metadata = dict(dict(member.get("public_summary") or {}).get("metadata") or {})
        self.assertEqual(len(migrated_metadata.get("cohort_lane_membership") or []), 2)
        self.assertEqual(migrated_metadata.get("cohort_employment_statuses"), ["current", "former"])

        members = _projection_members_from_payload(  # noqa: SLF001
            payload={
                "candidates": [
                    {
                        "candidate_id": "c1",
                        "display_name": "Consolidated",
                        "employment_status": "current",
                        "metadata": self._dual_status_metadata(),
                    }
                ]
            },
            source_snapshot_id="snap-1",
            target_company="Acme",
            collection_id="company:acme",
        )
        consolidated_metadata = dict(dict(members[0].get("public_summary") or {}).get("metadata") or {})
        self.assertEqual(len(consolidated_metadata.get("cohort_lane_membership") or []), 2)
        self.assertEqual(consolidated_metadata.get("cohort_role_bucket_ids"), ["infra_systems"])

    def test_index_filter_record_metadata_carries_provenance(self) -> None:
        from sourcing_agent.person_asset_writer import _projection_filter_record
        from sourcing_agent.serving_projection_migration import _projection_member_from_candidate

        member = _projection_member_from_candidate(  # noqa: SLF001
            {
                "candidate_id": "c1",
                "display_name": "Indexed",
                "employment_status": "current",
                "metadata": self._dual_status_metadata(),
            },
            run_id="run-1",
            rank_index=1,
        )
        filter_record = _projection_filter_record(  # noqa: SLF001
            member=member,
            public_summary=dict(member.get("public_summary") or {}),
            projection_metrics=dict(member.get("projection_metrics") or {}),
            member_metadata={},
        )
        record_metadata = dict(filter_record.get("metadata") or {})
        self.assertEqual(len(record_metadata.get("cohort_lane_membership") or []), 2)
        self.assertEqual(record_metadata.get("cohort_employment_statuses"), ["current", "former"])
