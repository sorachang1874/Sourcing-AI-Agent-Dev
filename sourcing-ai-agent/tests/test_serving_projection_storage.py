import tempfile
import unittest
from pathlib import Path
from unittest import mock

import sourcing_agent.repositories.serving_projection as serving_projection_repository

from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class ServingProjectionStorageTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self.store = self.make_pg_store(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_serving_projection_writer_persists_contract_fields(self) -> None:
        projection = self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj_test_1",
                "projection_type": "run_scope_projection",
                "collection_id": "company:openai",
                "source_run_id": "job-openai-agent",
                "state": "serving",
                "scope_label": "OpenAI Agent delta",
                "scope_spec": {"keywords": ["agent"], "baseline_policy": "delta_only"},
                "counts": {"candidate_count": 2, "count_scope": "deduped_projection_members"},
                "readiness": {"row": "complete", "profile": "partial"},
                "provenance": {"source_runs": ["job-openai-agent"]},
                "metadata": {"owner": "projection_writer_v1"},
            }
        )

        self.assertEqual(projection["projection_id"], "proj_test_1")
        self.assertEqual(projection["projection_type"], "run_scope_projection")
        self.assertEqual(projection["collection_id"], "company:openai")
        self.assertEqual(projection["state"], "serving")
        self.assertEqual(projection["scope_spec"]["baseline_policy"], "delta_only")
        self.assertEqual(projection["counts"]["candidate_count"], 2)
        self.assertEqual(projection["readiness"]["profile"], "partial")

    def test_catalog_lists_filter_order_limit_and_pointer_rollover(self) -> None:
        repository = self.store.repos.serving_projection
        with mock.patch.object(
            serving_projection_repository,
            "utc_now_timestamp",
            return_value="2026-07-10 05:00:00",
        ):
            for payload in (
                {
                    "projection_id": "proj-a",
                    "projection_type": "run_scope_projection",
                    "collection_id": "company:alpha",
                    "source_run_id": "run-a",
                    "state": "serving",
                },
                {
                    "projection_id": "proj-z",
                    "projection_type": "collection_authoritative_projection",
                    "collection_id": "company:alpha",
                    "source_run_id": "run-z",
                    "state": "serving",
                },
                {
                    "projection_id": "proj-mid",
                    "projection_type": "run_scope_projection",
                    "collection_id": "company:beta",
                    "source_run_id": "run-mid",
                    "state": "archived",
                },
            ):
                repository.upsert(payload)

            repository.upsert_run_link(
                {"run_id": "run-a", "projection_id": "proj-a", "link_type": "result"}
            )
            repository.upsert_run_link(
                {"run_id": "run-a", "projection_id": "proj-z", "link_type": "audit"}
            )
            repository.upsert_authoritative_pointer(
                {"collection_id": "company:alpha", "active_projection_id": "proj-a"}
            )
            repository.upsert_authoritative_pointer(
                {
                    "collection_id": "company:beta",
                    "active_projection_id": "proj-mid",
                    "state": "archived",
                }
            )
            repository.upsert_authoritative_pointer(
                {"collection_id": "company:alpha", "active_projection_id": "proj-z"}
            )

        self.assertEqual(
            [row["projection_id"] for row in repository.list(collection_id="company:alpha")],
            ["proj-z", "proj-a"],
        )
        self.assertEqual(
            [row["projection_id"] for row in repository.list(source_run_id="run-a")],
            ["proj-a"],
        )
        self.assertEqual(
            [
                row["projection_id"]
                for row in repository.list(
                    projection_type="collection_authoritative_projection",
                    state="serving",
                    limit=1,
                )
            ],
            ["proj-z"],
        )
        self.assertEqual(
            [row["link_type"] for row in repository.list_run_links("run-a")],
            ["audit", "result"],
        )
        self.assertEqual(len(repository.list_run_links("run-a", limit=1)), 1)
        pointer = repository.get_authoritative_pointer("company:alpha")
        self.assertEqual(pointer["previous_projection_id"], "proj-a")
        self.assertEqual(
            [row["collection_id"] for row in repository.list_authoritative_pointers()],
            ["company:alpha"],
        )
        self.assertEqual(
            [row["collection_id"] for row in repository.list_authoritative_pointers(state="")],
            ["company:alpha", "company:beta"],
        )
        self.assertEqual(len(repository.list_authoritative_pointers(state="", limit=1)), 1)

    def test_serving_projection_members_dedupe_and_page_by_identity_key(self) -> None:
        self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj_members",
                "projection_type": "run_scope_projection",
                "collection_id": "company:lovable-dev",
                "source_run_id": "job-lovable",
                "state": "serving",
            }
        )

        written = self.store.upsert_serving_projection_members(
            "proj_members",
            [
                {
                    "candidate_identity_key": "linkedin:ada",
                    "person_identity_key": "linkedin:ada",
                    "profile_url_key": "ada",
                    "rank_index": 2,
                    "public_summary": {"name": "Ada"},
                    "projection_metrics": {"row_readiness": "ready"},
                    "crm_overlay_summary": {"in_crm": False},
                },
                {
                    "candidate_identity_key": "linkedin:grace",
                    "person_identity_key": "linkedin:grace",
                    "profile_url_key": "grace",
                    "rank_index": 1,
                    "public_summary": {"name": "Grace"},
                },
                {
                    "candidate_identity_key": "linkedin:ada",
                    "person_identity_key": "linkedin:ada",
                    "profile_url_key": "ada",
                    "rank_index": 0,
                    "public_summary": {"name": "Ada Updated"},
                    "metadata": {"raw_profile_json_is_not_served": True},
                },
            ],
        )

        members = self.store.list_serving_projection_members("proj_members", limit=10)

        self.assertEqual(written, 2)
        self.assertEqual(self.store.count_serving_projection_members("proj_members"), 2)
        self.assertEqual([member["candidate_identity_key"] for member in members], ["linkedin:ada", "linkedin:grace"])
        self.assertEqual(members[0]["public_summary"]["name"], "Ada Updated")
        self.assertEqual(members[0]["public_summary"]["person_identity_key"], "linkedin:ada")
        self.assertEqual(members[0]["projection_metrics"], {})
        self.assertEqual(members[0]["crm_overlay_summary"], {})
        self.assertNotIn("restricted_contact", members[0])
        self.assertNotIn("raw_profile", members[0])

    def test_list_serving_projection_members_by_identity_keys_bulk_fetches_requested_members(self) -> None:
        self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj_member_keys",
                "projection_type": "run_scope_projection",
                "collection_id": "company:google",
                "source_run_id": "job-google",
                "state": "serving",
            }
        )
        self.store.upsert_serving_projection_members(
            "proj_member_keys",
            [
                {
                    "candidate_identity_key": f"linkedin:person-{index}",
                    "person_identity_key": f"linkedin:person-{index}",
                    "rank_index": index,
                    "public_summary": {"name": f"Person {index}"},
                }
                for index in range(8)
            ],
        )

        members = self.store.list_serving_projection_members_by_identity_keys(
            "proj_member_keys",
            ["linkedin:person-7", "linkedin:person-2", "linkedin:missing", "linkedin:person-2"],
        )

        self.assertEqual(
            [member["candidate_identity_key"] for member in members],
            ["linkedin:person-2", "linkedin:person-7"],
        )

    def test_manifest_shards_are_audit_refs_not_member_source(self) -> None:
        self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj_manifest",
                "projection_type": "collection_authoritative_projection",
                "collection_id": "company:google",
                "state": "serving",
            }
        )

        shard = self.store.repos.serving_projection.upsert_manifest_shard(
            {
                "projection_id": "proj_manifest",
                "shard_kind": "candidate_identity_manifest",
                "shard_index": 0,
                "manifest_ref": "s3://bucket/projections/proj_manifest/shard-000.json",
                "row_count": 7384,
                "content_signature": "sha256:test",
            }
        )

        self.assertEqual(shard["projection_id"], "proj_manifest")
        self.assertEqual(shard["row_count"], 7384)
        self.assertEqual(
            self.store.repos.serving_projection.list_manifest_shards("proj_manifest")[0]["manifest_ref"],
            shard["manifest_ref"],
        )
        self.assertEqual(self.store.count_serving_projection_members("proj_manifest"), 0)

    def test_manifest_shard_repository_preserves_normalization_order_and_update_contract(self) -> None:
        repository = self.store.repos.serving_projection
        self.assertEqual(repository.upsert_manifest_shard({}), {})
        self.assertEqual(repository.get_manifest_shard(""), {})
        self.assertEqual(repository.list_manifest_shards(""), [])

        with mock.patch.object(
            serving_projection_repository,
            "utc_now_timestamp",
            return_value="2026-07-10 07:00:00",
        ):
            defaulted = repository.upsert_manifest_shard(
                {
                    "projection_id": "proj-contract",
                    "shard_index": -7,
                    "manifest_ref": "s3://bucket/default.json",
                    "row_count": "bad",
                    "metadata_json": '{"source":"contract"}',
                }
            )
            explicit = repository.upsert_manifest_shard(
                {
                    "shard_id": " explicit-shard ",
                    "projection_id": "proj-contract",
                    "shard_kind": "zeta",
                    "shard_index": "2",
                    "manifest_ref": "s3://bucket/zeta.json",
                    "row_count": "4",
                    "metadata": {"nested": ["x"]},
                    "created_at": "2026-01-01 00:00:00",
                }
            )
            malformed = repository.upsert_manifest_shard(
                {
                    "projection_id": "proj-contract",
                    "shard_kind": "alpha",
                    "shard_index": 1,
                    "manifest_ref": "s3://bucket/alpha.json",
                    "row_count": 3,
                    "metadata_json": "[",
                }
            )

        with mock.patch.object(
            serving_projection_repository,
            "utc_now_timestamp",
            return_value="2026-07-10 07:30:00",
        ):
            updated = repository.upsert_manifest_shard(
                {
                    "projection_id": "proj-contract",
                    "shard_index": 0,
                    "manifest_ref": "s3://bucket/default-v2.json",
                    "row_count": 9,
                }
            )

        self.assertEqual(defaulted["shard_id"], "proj-contract::candidate_identity_manifest::0")
        self.assertEqual(defaulted["shard_index"], 0)
        self.assertEqual(defaulted["row_count"], 0)
        self.assertEqual(defaulted["metadata"], {"source": "contract"})
        self.assertEqual(explicit["shard_id"], "explicit-shard")
        self.assertEqual(explicit["shard_index"], 2)
        self.assertEqual(explicit["row_count"], 4)
        self.assertEqual(malformed["metadata"], {})
        self.assertEqual(updated["created_at"], defaulted["created_at"])
        self.assertEqual(updated["updated_at"], "2026-07-10 07:30:00")
        self.assertEqual(
            [row["shard_kind"] for row in repository.list_manifest_shards("proj-contract")],
            ["alpha", "candidate_identity_manifest", "zeta"],
        )
        self.assertEqual(repository.list_manifest_shards("proj-contract", shard_kind="zeta"), [explicit])
        self.assertEqual(len(repository.list_manifest_shards("proj-contract", limit=1)), 1)
        self.assertEqual(len(repository.list_manifest_shards("proj-contract", limit=0)), 3)

    def test_manifest_shard_reader_clamps_raw_postgres_negative_values(self) -> None:
        adapter = self.store._control_plane_postgres
        adapter.ensure_bootstrapped()
        adapter.upsert_row(
            "projection_manifest_shards",
            {
                "shard_id": "raw-negative",
                "projection_id": "proj-raw",
                "shard_kind": "candidate_identity_manifest",
                "shard_index": -7,
                "manifest_ref": "s3://bucket/raw.json",
                "row_count": -3,
                "content_signature": "sha256:raw",
                "metadata_json": "{",
                "created_at": "2026-07-10 08:00:00",
                "updated_at": "2026-07-10 08:00:00",
            },
        )

        raw = adapter.select_one(
            "projection_manifest_shards",
            where_sql="shard_id = %s",
            params=["raw-negative"],
        )
        public = self.store.repos.serving_projection.get_manifest_shard("raw-negative")
        listed = self.store.repos.serving_projection.list_manifest_shards("proj-raw")

        self.assertEqual(int(raw["shard_index"]), -7)
        self.assertEqual(int(raw["row_count"]), -3)
        self.assertEqual(public["shard_index"], 0)
        self.assertEqual(public["row_count"], 0)
        self.assertEqual(public["metadata"], {})
        self.assertEqual(listed, [public])

    def test_run_projection_link_and_collection_pointer_are_explicit_foundation_records(self) -> None:
        self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj_run_scope",
                "projection_type": "run_scope_projection",
                "collection_id": "company:google",
                "source_run_id": "job-google-vision",
                "state": "serving",
            }
        )
        self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj_google_authoritative_v1",
                "projection_type": "collection_authoritative_projection",
                "collection_id": "company:google",
                "state": "serving",
            }
        )

        link = self.store.repos.serving_projection.upsert_run_link(
            {
                "run_id": "job-google-vision",
                "projection_id": "proj_run_scope",
                "projection_type": "run_scope_projection",
                "collection_id": "company:google",
                "created_by": "run_projection_writer_v1",
                "metadata": {"scope": "vision-language"},
            }
        )
        pointer = self.store.repos.serving_projection.upsert_authoritative_pointer(
            {
                "collection_id": "company:google",
                "active_projection_id": "proj_google_authoritative_v1",
                "active_collection_version": "v1",
                "writer_id": "collection_writer_v1",
                "metadata": {"source": "collection-writer"},
            }
        )

        self.assertEqual(link["run_id"], "job-google-vision")
        self.assertEqual(link["projection_id"], "proj_run_scope")
        self.assertEqual(link["metadata"], {"scope": "vision-language"})
        self.assertEqual(self.store.repos.serving_projection.get_run_link("job-google-vision")["projection_id"], "proj_run_scope")
        self.assertEqual(len(self.store.repos.serving_projection.list_run_links("job-google-vision")), 1)
        self.assertEqual(pointer["active_projection_id"], "proj_google_authoritative_v1")
        self.assertEqual(pointer["previous_projection_id"], "")

        self.store.repos.serving_projection.upsert_authoritative_pointer(
            {
                "collection_id": "company:google",
                "active_projection_id": "proj_google_authoritative_v2",
                "active_collection_version": "v2",
            }
        )
        updated_pointer = self.store.repos.serving_projection.get_authoritative_pointer("company:google")

        self.assertEqual(updated_pointer["active_projection_id"], "proj_google_authoritative_v2")
        self.assertEqual(updated_pointer["previous_projection_id"], "proj_google_authoritative_v1")

    def test_collection_authoritative_pointers_can_be_listed_for_asset_overview(self) -> None:
        self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj_google_authoritative_v1",
                "projection_type": "collection_authoritative_projection",
                "collection_id": "company:google",
                "state": "serving",
            }
        )
        self.store.repos.serving_projection.upsert_authoritative_pointer(
            {
                "collection_id": "company:google",
                "active_projection_id": "proj_google_authoritative_v1",
                "active_collection_version": "v1",
            }
        )

        pointers = self.store.repos.serving_projection.list_authoritative_pointers()

        self.assertEqual(len(pointers), 1)
        self.assertEqual(pointers[0]["collection_id"], "company:google")
        self.assertEqual(pointers[0]["active_projection_id"], "proj_google_authoritative_v1")


if __name__ == "__main__":
    unittest.main()
