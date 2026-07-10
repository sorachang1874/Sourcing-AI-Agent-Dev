import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

from sourcing_agent.control_plane_repository import ControlPlaneAuthoritativeReadError
from sourcing_agent.person_asset_writer import PersonAssetWriter
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
        self.assertTrue(missing["read_contract"]["fail_closed"])
        self.assertFalse(missing["read_contract"]["fallback_used"])

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
