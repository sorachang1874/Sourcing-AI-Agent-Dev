from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

import pytest

from sourcing_agent.filter_projection_publication_owner import (
    FILTER_PROJECTION_FOUNDATION_CANDIDATE,
    FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY,
    FILTER_PROJECTION_FOUNDATION_METADATA_KEY,
    FILTER_PROJECTION_FOUNDATION_PROJECTION_STATE,
    FILTER_PROJECTION_FOUNDATION_ROUTE_TYPE,
    FILTER_PROJECTION_FOUNDATION_STATUS,
    FilterProjectionPublicationFoundation,
    FilterProjectionPublicationFoundationError,
    build_filter_projection_publication_foundation,
)
from sourcing_agent.projection_search_index_contract import (
    PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY,
)
from sourcing_agent.serving_projection_reader import ServingProjectionReader
from sourcing_agent.serving_projection_writer import ServingProjectionWriter
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _members() -> list[dict[str, Any]]:
    return [
        {
            "candidate_identity_key": "linkedin:ada",
            "person_identity_key": "linkedin:ada",
            "rank_index": 1,
            "visibility_state": "visible",
            "public_summary": {"name": "Ada"},
            "provenance": {"source": "scripted"},
        },
        {
            "candidate_identity_key": "linkedin:grace",
            "person_identity_key": "linkedin:grace",
            "rank_index": 2,
            "visibility_state": "visible",
            "public_summary": {"name": "Grace"},
            "provenance": {"source": "scripted"},
        },
    ]


def _foundation(source_run_id: str = "run-s1f0b-foundation") -> FilterProjectionPublicationFoundation:
    return build_filter_projection_publication_foundation(
        source_run_id=source_run_id,
        members=_members(),
    )


def test_foundation_carrier_is_closed_distinct_and_non_product() -> None:
    foundation = _foundation()
    record = foundation.foundation_record

    assert record == {
        "schema_version": "filter_projection_publication_foundation.v1",
        "status": FILTER_PROJECTION_FOUNDATION_STATUS,
        "candidate": FILTER_PROJECTION_FOUNDATION_CANDIDATE,
        "candidate_revision": "filter_projection_publication_candidate_v1",
        "source_run_id": "run-s1f0b-foundation",
    }
    assert foundation.foundation_record_digest == _digest(record)
    assert set(foundation.foundation_wrapper) == {
        "schema_version",
        "foundation_record",
        "foundation_record_digest",
    }
    assert not {
        "receipt_id",
        "result_view_id",
        "snapshot_id",
        "terminal_owner_ref",
        "freshness",
        "readiness",
        "member_set_digest",
    } & set(record)

    decorated_members = foundation.members
    for member in decorated_members:
        carrier = member["provenance"][FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY]
        assert set(carrier) == {
            "schema_version",
            "status",
            "candidate",
            "candidate_revision",
            "parent_foundation_digest",
            "candidate_identity_key",
        }
        assert carrier["status"] == FILTER_PROJECTION_FOUNDATION_STATUS
        assert carrier["parent_foundation_digest"] == foundation.foundation_record_digest

    decorated_members[0]["provenance"].clear()
    assert FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY in foundation.members[0]["provenance"]


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("empty", "filter_projection_foundation_members_invalid"),
        ("duplicate", "filter_projection_foundation_candidate_duplicate"),
        ("reserved", "filter_projection_foundation_reserved_key"),
        ("path_shaped_run", "filter_projection_foundation_identity_invalid"),
    ],
)
def test_foundation_rejects_ambiguous_or_forged_inputs(
    mutation: str,
    expected_code: str,
) -> None:
    members = _members()
    source_run_id = "run-s1f0b-foundation"
    if mutation == "empty":
        members = []
    elif mutation == "duplicate":
        members[1]["candidate_identity_key"] = members[0]["candidate_identity_key"]
    elif mutation == "reserved":
        members[0]["provenance"][FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY] = {"forged": True}
    elif mutation == "path_shaped_run":
        source_run_id = "/private/runtime/run-s1f0b"

    with pytest.raises(FilterProjectionPublicationFoundationError, match=expected_code):
        build_filter_projection_publication_foundation(
            source_run_id=source_run_id,
            members=members,
        )


class FilterProjectionPublicationFoundationPostgresTest(
    PGControlPlaneStoreTestMixin,
    unittest.TestCase,
):
    pg_store_schema_label = "d1n_s1f0b_filter_projection_foundation"

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.runtime_dir = Path(self.tempdir.name)
        self.store = self.make_pg_store(self.runtime_dir / "control-plane.db")
        self.writer = ServingProjectionWriter(self.store)

    def _publish_foundation(self, source_run_id: str) -> tuple[FilterProjectionPublicationFoundation, dict[str, Any]]:
        foundation = _foundation(source_run_id)
        publication = self.writer.publish_filter_projection_foundation_run_scope_projection(
            foundation=foundation,
            collection_id="company:thinkingmachineslab",
            scope_label="foundation-only",
            metadata={"request_kind": "bounded-scripted-foundation"},
        )
        return foundation, publication

    def _assert_foundation_present(
        self,
        projection_id: str,
        foundation: FilterProjectionPublicationFoundation,
    ) -> None:
        projection = self.store.repos.serving_projection.get(projection_id)
        members = self.store.repos.serving_projection.list_members(
            projection_id,
            visible_only=False,
        )
        self.assertEqual(
            projection["metadata"][FILTER_PROJECTION_FOUNDATION_METADATA_KEY],
            foundation.foundation_wrapper,
        )
        self.assertEqual(len(members), foundation.member_count)
        for member in members:
            self.assertIn(
                FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY,
                member["provenance"],
            )

    def _assert_foundation_absent(self, projection_id: str) -> None:
        projection = self.store.repos.serving_projection.get(projection_id)
        members = self.store.repos.serving_projection.list_members(
            projection_id,
            visible_only=False,
        )
        self.assertNotIn(FILTER_PROJECTION_FOUNDATION_METADATA_KEY, projection["metadata"])
        for member in members:
            self.assertNotIn(
                FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY,
                member["provenance"],
            )
        source_run_id = str(projection.get("source_run_id") or "").strip()
        if source_run_id:
            self.assertEqual(
                self.store.repos.serving_projection.get_run_link(
                    source_run_id,
                    link_type=FILTER_PROJECTION_FOUNDATION_ROUTE_TYPE,
                ),
                {},
            )

    @staticmethod
    def _generic_projection_payload(projection: dict[str, Any]) -> dict[str, Any]:
        payload = copy.deepcopy(projection)
        payload["metadata"].pop(FILTER_PROJECTION_FOUNDATION_METADATA_KEY, None)
        return payload

    @staticmethod
    def _plain_members(foundation: FilterProjectionPublicationFoundation) -> list[dict[str, Any]]:
        members = foundation.members
        for member in members:
            member["provenance"].pop(
                FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY,
                None,
            )
        return members

    def test_foundation_publication_atomically_persists_parent_members_route_and_replays(self) -> None:
        foundation, first = self._publish_foundation("run-foundation-atomic")
        projection = first["projection"]
        projection_id = projection["projection_id"]
        input_revision = projection["metadata"][PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY]

        self._assert_foundation_present(projection_id, foundation)
        self.assertEqual(first["link"]["run_id"], foundation.source_run_id)
        self.assertEqual(first["member_count"], foundation.member_count)

        replay = self.writer.publish_filter_projection_foundation_run_scope_projection(
            foundation=foundation,
            collection_id="company:thinkingmachineslab",
            scope_label="foundation-only",
            metadata={"request_kind": "bounded-scripted-foundation"},
        )
        self.assertEqual(replay["projection"]["projection_id"], projection_id)
        self.assertEqual(
            replay["projection"]["metadata"][PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY],
            input_revision,
        )
        self._assert_foundation_present(projection_id, foundation)

    def test_foundation_is_not_servable_or_product_routed(self) -> None:
        foundation, publication = self._publish_foundation("run-foundation-shadow-route")
        projection = publication["projection"]

        self.assertEqual(projection["state"], FILTER_PROJECTION_FOUNDATION_PROJECTION_STATE)
        self.assertEqual(publication["link"]["link_type"], FILTER_PROJECTION_FOUNDATION_ROUTE_TYPE)
        self.assertEqual(
            self.store.repos.serving_projection.get_run_link(foundation.source_run_id),
            {},
        )
        self.assertEqual(
            self.store.repos.serving_projection.get_run_link(
                foundation.source_run_id,
                link_type=FILTER_PROJECTION_FOUNDATION_ROUTE_TYPE,
            )["projection_id"],
            projection["projection_id"],
        )
        read = ServingProjectionReader(self.store).get_projection(projection["projection_id"])
        self.assertEqual(read["status"], "not_ready")
        self.assertEqual(read["reason"], "projection_not_servable")

    def test_foundation_rejects_existing_product_route_collision(self) -> None:
        run_id = "run-foundation-product-route-collision"
        product = self.writer.publish_run_scope_projection(
            run_id=run_id,
            members=[{"candidate_identity_key": "linkedin:product"}],
            replace_members=True,
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "filter_projection_foundation_product_route_collision",
        ):
            self.writer.publish_filter_projection_foundation_run_scope_projection(
                foundation=_foundation(run_id),
            )

        self.assertEqual(
            self.store.repos.serving_projection.get_run_link(run_id)["projection_id"],
            product["projection"]["projection_id"],
        )

    def test_direct_foundation_constructor_cannot_publish_unvalidated_bytes(self) -> None:
        forged = FilterProjectionPublicationFoundation(
            _foundation_wrapper_json=json.dumps(
                {
                    "schema_version": "filter_projection_publication_foundation_wrapper.v1",
                    "foundation_record": {
                        "schema_version": "filter_projection_publication_foundation.v1",
                        "status": FILTER_PROJECTION_FOUNDATION_STATUS,
                        "candidate": FILTER_PROJECTION_FOUNDATION_CANDIDATE,
                        "candidate_revision": "filter_projection_publication_candidate_v1",
                        "source_run_id": "run-forged-typed-foundation",
                    },
                    "foundation_record_digest": "forged",
                }
            ),
            _members_json="[]",
        )

        with self.assertRaisesRegex(
            FilterProjectionPublicationFoundationError,
            "filter_projection_foundation_value_invalid",
        ):
            self.writer.publish_filter_projection_foundation_run_scope_projection(
                foundation=forged,
            )

        adapter = self.store._control_plane_postgres  # noqa: SLF001
        self.assertEqual(adapter.count_rows("serving_projections"), 0)
        self.assertEqual(adapter.count_rows("serving_projection_members"), 0)
        self.assertEqual(adapter.count_rows("run_projection_links"), 0)

    def test_generic_reserved_parent_member_and_patch_inputs_write_zero_rows(self) -> None:
        adapter = self.store._control_plane_postgres  # noqa: SLF001
        cases: tuple[Callable[[], object], ...] = (
            lambda: self.writer.publish_run_scope_projection(
                run_id="run-forged-parent",
                metadata={FILTER_PROJECTION_FOUNDATION_METADATA_KEY: {"forged": True}},
                members=[{"candidate_identity_key": "linkedin:parent"}],
                replace_members=True,
            ),
            lambda: self.writer.publish_run_scope_projection(
                run_id="run-forged-member",
                members=[
                    {
                        "candidate_identity_key": "linkedin:member",
                        "provenance": {FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY: {"forged": True}},
                    }
                ],
                replace_members=True,
            ),
        )
        for case in cases:
            with self.subTest(case=case):
                with self.assertRaisesRegex(
                    FilterProjectionPublicationFoundationError,
                    "filter_projection_foundation_reserved_key",
                ):
                    case()

        forged_parent = json.dumps({FILTER_PROJECTION_FOUNDATION_METADATA_KEY: {"forged": True}})
        forged_member = json.dumps({FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY: {"forged": True}})
        native_cases: tuple[Callable[[], object], ...] = (
            lambda: adapter.upsert_row(
                "serving_projections",
                {
                    "projection_id": "proj-native-upsert",
                    "metadata_json": forged_parent,
                },
            ),
            lambda: adapter.bulk_upsert_rows(
                table_name="serving_projections",
                rows=[
                    {
                        "projection_id": "proj-native-bulk",
                        "metadata_json": forged_parent,
                    }
                ],
            ),
            lambda: adapter.replace_rows(
                table_name="serving_projection_members",
                where_sql="projection_id = %s",
                params=["proj-native-replace"],
                rows=[
                    {
                        "projection_id": "proj-native-replace",
                        "candidate_identity_key": "linkedin:native-replace",
                        "provenance_json": forged_member,
                    }
                ],
            ),
            lambda: adapter.write_serving_projection_members_with_input_revision(
                table_name="serving_projection_members",
                projection_id="proj-native-members",
                rows=[
                    {
                        "projection_id": "proj-native-members",
                        "candidate_identity_key": "linkedin:native-members",
                        "provenance_json": forged_member,
                    }
                ],
                replace_members=False,
                input_revision="projidxinput_native",
            ),
            lambda: adapter.upsert_row_and_upsert_rows(
                table_name="serving_projections",
                row={
                    "projection_id": "proj-native-combined-upsert",
                    "metadata_json": forged_parent,
                },
                upsert_table_name="serving_projection_members",
                upsert_rows=[],
            ),
            lambda: adapter.upsert_row_and_replace_rows(
                table_name="serving_projections",
                row={
                    "projection_id": "proj-native-combined-replace",
                    "metadata_json": forged_parent,
                },
                replace_table_name="serving_projection_members",
                replace_where_sql="projection_id = %s",
                replace_params=["proj-native-combined-replace"],
                replace_rows=[],
            ),
            lambda: adapter.publish_serving_projection(
                scope_kind="run_scope",
                scope_key="run-native-publish",
                replace_members=True,
                projection_id_factory=lambda: "proj-native-publish",
                payload_builder=lambda **_: {
                    "projection_row": {
                        "projection_id": "proj-native-publish",
                        "projection_type": "run_scope_projection",
                        "source_run_id": "run-native-publish",
                        "metadata_json": forged_parent,
                    },
                    "member_rows": [],
                    "routing_row": {
                        "run_id": "run-native-publish",
                        "link_type": "result",
                        "projection_id": "proj-native-publish",
                    },
                },
            ),
        )
        for case in native_cases:
            with self.subTest(native_case=case):
                with self.assertRaisesRegex(
                    ValueError,
                    "filter_projection_foundation_reserved_key",
                ):
                    case()

        self.assertEqual(adapter.count_rows("serving_projections"), 0)
        self.assertEqual(adapter.count_rows("serving_projection_members"), 0)
        self.assertEqual(adapter.count_rows("run_projection_links"), 0)

        foundation, publication = self._publish_foundation("run-reserved-patch")
        projection_id = publication["projection"]["projection_id"]
        before = self.store.repos.serving_projection.get(projection_id)
        with self.assertRaisesRegex(ValueError, "reserved publication metadata"):
            self.store.repos.serving_projection.patch_publication_fields_under_lock(
                projection_id,
                metadata_patch={FILTER_PROJECTION_FOUNDATION_METADATA_KEY: {"forged": True}},
            )
        with self.assertRaisesRegex(
            FilterProjectionPublicationFoundationError,
            "filter_projection_foundation_reserved_key",
        ):
            self.store.repos.serving_projection.update_person_search_index_build_state(
                projection_id,
                build_generation="projidxgen_forged",
                metadata_patch={FILTER_PROJECTION_FOUNDATION_METADATA_KEY: {"forged": True}},
            )
        self.assertEqual(self.store.repos.serving_projection.get(projection_id), before)
        self._assert_foundation_present(projection_id, foundation)

    def test_native_foundation_scope_and_source_bindings_are_exact_and_write_zero_on_rejection(self) -> None:
        adapter = self.store._control_plane_postgres  # noqa: SLF001
        foundation = _foundation("run-native-exact-binding")

        with self.assertRaisesRegex(ValueError, "requires a typed foundation"):
            adapter.publish_serving_projection(
                scope_kind="filter_projection_foundation",
                scope_key=foundation.source_run_id,
                replace_members=True,
                projection_id_factory=lambda: "proj-native-missing-foundation",
                payload_builder=lambda **_: {},
            )
        with self.assertRaisesRegex(ValueError, "source_run_mismatch"):
            adapter.publish_serving_projection(
                scope_kind="filter_projection_foundation",
                scope_key="run-native-wrong-scope",
                replace_members=True,
                projection_id_factory=lambda: "proj-native-wrong-scope",
                payload_builder=lambda **_: {},
                filter_projection_foundation=foundation,
            )

        for route_run_id, link_type in (
            (foundation.source_run_id, FILTER_PROJECTION_FOUNDATION_ROUTE_TYPE),
            ("run-native-lock-a-write-b", "result"),
        ):
            with self.subTest(route_run_id=route_run_id, link_type=link_type):
                product_projection_id = f"proj-native-product-binding-{link_type}-{route_run_id}"
                with self.assertRaisesRegex(ValueError, "run scope payload binding mismatch"):
                    adapter.publish_serving_projection(
                        scope_kind="run_scope",
                        scope_key=foundation.source_run_id,
                        explicit_projection_id=product_projection_id,
                        replace_members=True,
                        projection_id_factory=lambda: product_projection_id,
                        payload_builder=lambda **_: {
                            "projection_row": {
                                "projection_id": product_projection_id,
                                "projection_type": "run_scope_projection",
                                "source_run_id": foundation.source_run_id,
                                "state": "serving",
                                "metadata_json": "{}",
                            },
                            "member_rows": [],
                            "routing_row": {
                                "run_id": route_run_id,
                                "link_type": link_type,
                                "projection_id": product_projection_id,
                            },
                        },
                    )

        for projection_type, source_version, route_collection_id in (
            ("collection_authoritative_projection", "v1", "collection:written-b"),
            ("run_scope_projection", "v1", "collection:locked-a"),
            ("collection_authoritative_projection", "v2", "collection:locked-a"),
        ):
            with self.subTest(
                projection_type=projection_type,
                source_version=source_version,
                route_collection_id=route_collection_id,
            ):
                collection_projection_id = (
                    f"proj-native-collection-binding-{projection_type}-{source_version}-{route_collection_id}"
                )
                with self.assertRaisesRegex(ValueError, "collection scope payload binding mismatch"):
                    adapter.publish_serving_projection(
                        scope_kind="collection_authoritative",
                        scope_key="collection:locked-a",
                        active_collection_version="v1",
                        explicit_projection_id=collection_projection_id,
                        replace_members=True,
                        projection_id_factory=lambda: collection_projection_id,
                        payload_builder=lambda **_: {
                            "projection_row": {
                                "projection_id": collection_projection_id,
                                "projection_type": projection_type,
                                "collection_id": "collection:locked-a",
                                "source_collection_version": source_version,
                                "metadata_json": "{}",
                            },
                            "member_rows": [],
                            "routing_row": {
                                "collection_id": route_collection_id,
                                "active_collection_version": "v1",
                                "active_projection_id": collection_projection_id,
                            },
                        },
                    )

        wrong_type_projection_id = "proj-native-wrong-foundation-projection-type"
        wrong_type_member_rows = [
            {
                "projection_id": wrong_type_projection_id,
                "candidate_identity_key": member["candidate_identity_key"],
                "provenance_json": json.dumps(member["provenance"], ensure_ascii=False),
            }
            for member in foundation.members
        ]
        with self.assertRaisesRegex(ValueError, "run scope payload binding mismatch"):
            adapter.publish_serving_projection(
                scope_kind="filter_projection_foundation",
                scope_key=foundation.source_run_id,
                explicit_projection_id=wrong_type_projection_id,
                replace_members=True,
                member_identity_keys=[member["candidate_identity_key"] for member in foundation.members],
                projection_id_factory=lambda: wrong_type_projection_id,
                payload_builder=lambda **_: {
                    "projection_row": {
                        "projection_id": wrong_type_projection_id,
                        "projection_type": "collection_projection",
                        "source_run_id": foundation.source_run_id,
                        "state": FILTER_PROJECTION_FOUNDATION_PROJECTION_STATE,
                        "metadata_json": json.dumps(
                            {
                                FILTER_PROJECTION_FOUNDATION_METADATA_KEY: foundation.foundation_wrapper,
                            },
                            ensure_ascii=False,
                        ),
                    },
                    "member_rows": wrong_type_member_rows,
                    "routing_row": {
                        "run_id": foundation.source_run_id,
                        "link_type": FILTER_PROJECTION_FOUNDATION_ROUTE_TYPE,
                        "projection_id": wrong_type_projection_id,
                    },
                },
                filter_projection_foundation=foundation,
            )

        projection_id = "proj-native-wrong-payload-source"
        member_rows = [
            {
                "projection_id": projection_id,
                "candidate_identity_key": member["candidate_identity_key"],
                "provenance_json": json.dumps(member["provenance"], ensure_ascii=False),
            }
            for member in foundation.members
        ]
        with self.assertRaisesRegex(ValueError, "run scope payload binding mismatch"):
            adapter.publish_serving_projection(
                scope_kind="filter_projection_foundation",
                scope_key=foundation.source_run_id,
                explicit_projection_id=projection_id,
                replace_members=True,
                member_identity_keys=[member["candidate_identity_key"] for member in foundation.members],
                projection_id_factory=lambda: projection_id,
                payload_builder=lambda **_: {
                    "projection_row": {
                        "projection_id": projection_id,
                        "projection_type": "run_scope_projection",
                        "source_run_id": "run-native-forged-payload-source",
                        "state": FILTER_PROJECTION_FOUNDATION_PROJECTION_STATE,
                        "metadata_json": json.dumps(
                            {
                                FILTER_PROJECTION_FOUNDATION_METADATA_KEY: foundation.foundation_wrapper,
                            },
                            ensure_ascii=False,
                        ),
                    },
                    "member_rows": member_rows,
                    "routing_row": {
                        "run_id": foundation.source_run_id,
                        "link_type": FILTER_PROJECTION_FOUNDATION_ROUTE_TYPE,
                        "projection_id": projection_id,
                    },
                },
                filter_projection_foundation=foundation,
            )

        self.assertEqual(adapter.count_rows("serving_projections"), 0)
        self.assertEqual(adapter.count_rows("serving_projection_members"), 0)
        self.assertEqual(adapter.count_rows("run_projection_links"), 0)

    def test_every_generic_membership_surface_preserves_foundation_on_semantic_noop(self) -> None:
        operations = (
            "upsert_members",
            "replace_members",
            "upsert_with_members",
            "upsert_with_replaced_members",
        )
        for operation in operations:
            with self.subTest(operation=operation):
                foundation, publication = self._publish_foundation(f"run-noop-{operation.replace('_', '-')}")
                projection = publication["projection"]
                projection_id = projection["projection_id"]
                before = self.store.repos.serving_projection.patch_publication_fields_under_lock(
                    projection_id,
                    counts_patch={
                        "public_facet_counts": {"role": {"researcher": 2}},
                        "facet_count_scope": "exact_projection",
                        "facet_build_status": "completed",
                    },
                    readiness_patch={
                        "index_count_scope": "exact_projection",
                        "profile_indexed_at": "2026-07-18T00:00:00+00:00",
                    },
                    metadata_patch={
                        "search_index_build_status": "completed",
                        "search_index_writer_id": "test-s1f0b-derived-product",
                        "search_indexed_member_count": 2,
                    },
                )
                members = self._plain_members(foundation)
                if operation == "upsert_members":
                    self.store.repos.serving_projection.upsert_members(projection_id, members)
                elif operation == "replace_members":
                    self.store.repos.serving_projection.replace_members(projection_id, members)
                elif operation == "upsert_with_members":
                    self.store.repos.serving_projection.upsert_with_members(
                        self._generic_projection_payload(projection),
                        members,
                    )
                else:
                    self.store.repos.serving_projection.upsert_with_replaced_members(
                        self._generic_projection_payload(projection),
                        members,
                    )
                self._assert_foundation_present(projection_id, foundation)
                after = self.store.repos.serving_projection.get(projection_id)
                self.assertEqual(
                    after["metadata"][PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY],
                    before["metadata"][PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY],
                )
                self.assertEqual(after["counts"], before["counts"])
                self.assertEqual(after["readiness"], before["readiness"])
                self.assertEqual(after["raw_profile_index_watermark"], before["raw_profile_index_watermark"])
                self.assertEqual(after["evidence_index_watermark"], before["evidence_index_watermark"])

    def test_generic_run_publication_cannot_product_route_foundation_even_on_member_noop(self) -> None:
        foundation, publication = self._publish_foundation("run-generic-route-noop")
        projection = publication["projection"]

        for mutation in ("noop", "changed"):
            with self.subTest(mutation=mutation):
                members = self._plain_members(foundation)
                if mutation == "changed":
                    members[0]["public_summary"] = {"name": "Changed but rejected"}
                with self.assertRaisesRegex(
                    RuntimeError,
                    "filter_projection_foundation_requires_candidate_aware_publication",
                ):
                    self.writer.publish_run_scope_projection(
                        run_id=foundation.source_run_id,
                        projection_id=projection["projection_id"],
                        members=members,
                        replace_members=True,
                    )

        self.assertEqual(
            self.store.repos.serving_projection.get_run_link(foundation.source_run_id),
            {},
        )
        self._assert_foundation_present(projection["projection_id"], foundation)

    def test_every_generic_membership_surface_invalidates_parent_and_all_members_on_change(self) -> None:
        operations = (
            "upsert_members",
            "replace_members",
            "upsert_with_members",
            "upsert_with_replaced_members",
        )
        for operation in operations:
            with self.subTest(operation=operation):
                foundation, publication = self._publish_foundation(f"run-change-{operation.replace('_', '-')}")
                projection = publication["projection"]
                projection_id = projection["projection_id"]
                members = self._plain_members(foundation)
                members[0]["public_summary"] = {"name": "Ada Changed"}
                if operation == "upsert_members":
                    self.store.repos.serving_projection.upsert_members(
                        projection_id,
                        members[:1],
                    )
                elif operation == "replace_members":
                    self.store.repos.serving_projection.replace_members(
                        projection_id,
                        members,
                    )
                elif operation == "upsert_with_members":
                    self.store.repos.serving_projection.upsert_with_members(
                        self._generic_projection_payload(projection),
                        members[:1],
                    )
                else:
                    self.store.repos.serving_projection.upsert_with_replaced_members(
                        self._generic_projection_payload(projection),
                        members,
                    )
                self._assert_foundation_absent(projection_id)

    def test_real_invalidation_deletes_candidate_route_and_allows_rebuild_or_product_publication(self) -> None:
        rebuild_foundation, rebuild_publication = self._publish_foundation("run-invalidation-rebuild")
        rebuild_projection_id = rebuild_publication["projection"]["projection_id"]
        changed_rebuild_members = self._plain_members(rebuild_foundation)
        changed_rebuild_members[0]["public_summary"] = {"name": "Changed before rebuild"}
        self.store.repos.serving_projection.upsert_members(
            rebuild_projection_id,
            changed_rebuild_members[:1],
        )
        self._assert_foundation_absent(rebuild_projection_id)

        rebuilt = self.writer.publish_filter_projection_foundation_run_scope_projection(
            foundation=rebuild_foundation,
        )
        self.assertNotEqual(rebuilt["projection"]["projection_id"], rebuild_projection_id)
        self._assert_foundation_present(rebuilt["projection"]["projection_id"], rebuild_foundation)

        product_foundation, product_publication = self._publish_foundation("run-invalidation-product")
        product_projection_id = product_publication["projection"]["projection_id"]
        changed_product_members = self._plain_members(product_foundation)
        changed_product_members[0]["public_summary"] = {"name": "Changed before product"}
        self.store.repos.serving_projection.upsert_members(
            product_projection_id,
            changed_product_members[:1],
        )
        self._assert_foundation_absent(product_projection_id)

        product = self.writer.publish_run_scope_projection(
            run_id=product_foundation.source_run_id,
            members=[{"candidate_identity_key": "linkedin:product-after-invalidation"}],
            replace_members=True,
        )
        self.assertEqual(product["link"]["link_type"], "result")
        self.assertEqual(
            self.store.repos.serving_projection.get_run_link(product_foundation.source_run_id)["projection_id"],
            product["projection"]["projection_id"],
        )

    def test_generic_parent_upsert_cannot_silently_remove_foundation(self) -> None:
        foundation, publication = self._publish_foundation("run-foundation-parent-upsert")
        projection_id = publication["projection"]["projection_id"]
        generic_payload = self._generic_projection_payload(publication["projection"])
        generic_payload["metadata"]["generic_mutation"] = True

        with self.assertRaisesRegex(
            FilterProjectionPublicationFoundationError,
            "filter_projection_foundation_requires_candidate_aware_publication",
        ):
            self.store.repos.serving_projection.upsert(generic_payload)

        self._assert_foundation_present(projection_id, foundation)

    def test_native_standalone_mutations_cannot_orphan_foundation_carriers(self) -> None:
        foundation, publication = self._publish_foundation("run-native-standalone-guard")
        projection_id = publication["projection"]["projection_id"]
        adapter = self.store._control_plane_postgres  # noqa: SLF001
        prohibited_unscoped_cases: tuple[Callable[[], object], ...] = (
            lambda: adapter.insert_row_with_generated_id(
                table_name="serving_projections",
                row={"projection_id": projection_id},
            ),
            lambda: adapter.upsert_row_with_generated_id(
                table_name="serving_projection_members",
                row={"projection_id": projection_id},
                conflict_columns=["projection_id", "candidate_identity_key"],
            ),
            lambda: adapter.update_row_returning(
                table_name="serving_projections",
                id_column="projection_id",
                id_value=projection_id,
                row={"metadata_json": "{}"},
            ),
            lambda: adapter.update_rows(
                table_name="serving_projection_members",
                where_sql="projection_id = %s",
                params=[projection_id],
                values={"provenance_json": "{}"},
            ),
            lambda: adapter.delete_rows(
                table_name="serving_projections",
                where_sql="projection_id = %s",
                params=[projection_id],
            ),
        )
        for case in prohibited_unscoped_cases:
            with self.subTest(unscoped_case=case):
                with self.assertRaisesRegex(ValueError, "dedicated atomic publication"):
                    case()
                self._assert_foundation_present(projection_id, foundation)

        cases: tuple[Callable[[], object], ...] = (
            lambda: adapter.upsert_row(
                "serving_projections",
                {"projection_id": projection_id, "metadata_json": "{}"},
            ),
            lambda: adapter.bulk_upsert_rows(
                "serving_projection_members",
                [
                    {
                        "projection_id": projection_id,
                        "candidate_identity_key": "linkedin:ada",
                        "public_summary_json": json.dumps({"name": "Native changed"}),
                    }
                ],
            ),
            lambda: adapter.replace_rows(
                table_name="serving_projection_members",
                where_sql="projection_id = %s",
                params=[projection_id],
                rows=[],
            ),
            lambda: adapter.replace_rows(
                table_name="serving_projections",
                where_sql="projection_id = %s",
                params=[projection_id],
                rows=[],
            ),
        )
        for case in cases:
            with self.subTest(case=case):
                with self.assertRaisesRegex(
                    ValueError,
                    "filter_projection_foundation_requires_candidate_aware_publication",
                ):
                    case()
                self._assert_foundation_present(projection_id, foundation)

    def test_native_combined_mutation_cannot_opt_out_of_foundation_invalidation(self) -> None:
        foundation, publication = self._publish_foundation("run-native-combined-invalidation")
        projection_id = publication["projection"]["projection_id"]
        adapter = self.store._control_plane_postgres  # noqa: SLF001
        repository = self.store.repos.serving_projection
        _, parent_row = repository._projection_row_payload(  # noqa: SLF001
            self._generic_projection_payload(publication["projection"]),
            selected_projection_id=projection_id,
            existing=repository.get(projection_id),
        )
        members = self._plain_members(foundation)
        members[0]["public_summary"] = {"name": "Native changed"}
        member_rows = repository._member_row_payloads(projection_id, members[:1])  # noqa: SLF001

        adapter.upsert_row_and_upsert_rows(
            table_name="serving_projections",
            row=parent_row,
            upsert_table_name="serving_projection_members",
            upsert_rows=member_rows,
        )

        self._assert_foundation_absent(projection_id)

    def test_native_combined_and_member_writers_reject_lock_a_write_b_scopes(self) -> None:
        foundation_a, publication_a = self._publish_foundation("run-native-scope-binding-a")
        foundation_b, publication_b = self._publish_foundation("run-native-scope-binding-b")
        projection_a = publication_a["projection"]
        projection_b = publication_b["projection"]
        projection_a_id = projection_a["projection_id"]
        projection_b_id = projection_b["projection_id"]
        adapter = self.store._control_plane_postgres  # noqa: SLF001
        repository = self.store.repos.serving_projection
        _, parent_a_row = repository._projection_row_payload(  # noqa: SLF001
            self._generic_projection_payload(projection_a),
            selected_projection_id=projection_a_id,
            existing=repository.get(projection_a_id),
        )
        member_b_rows = repository._member_row_payloads(  # noqa: SLF001
            projection_b_id,
            self._plain_members(foundation_b)[:1],
        )
        before = {
            projection_id: (
                repository.get(projection_id),
                repository.list_members(projection_id, visible_only=False),
            )
            for projection_id in (projection_a_id, projection_b_id)
        }

        with self.assertRaisesRegex(ValueError, "member rows must match parent projection_id"):
            adapter.write_serving_projection_members_with_input_revision(
                table_name="serving_projection_members",
                projection_id=projection_a_id,
                rows=member_b_rows,
                replace_members=False,
                input_revision="projidxinput_lock_a_write_b",
            )
        with self.assertRaisesRegex(ValueError, "member rows must match parent projection_id"):
            adapter.upsert_row_and_upsert_rows(
                table_name="serving_projections",
                row=parent_a_row,
                upsert_table_name="serving_projection_members",
                upsert_rows=member_b_rows,
            )
        with self.assertRaisesRegex(ValueError, "replacement scope must match parent projection_id"):
            adapter.upsert_row_and_replace_rows(
                table_name="serving_projections",
                row=parent_a_row,
                replace_table_name="serving_projection_members",
                replace_where_sql="projection_id = %s",
                replace_params=[projection_b_id],
                replace_rows=[],
            )
        with self.assertRaisesRegex(ValueError, "requires serving_projections parent"):
            adapter.upsert_row_and_upsert_rows(
                table_name="serving_projections",
                row=parent_a_row,
                upsert_table_name="serving_projections",
                upsert_rows=[],
            )

        for projection_id in (projection_a_id, projection_b_id):
            self.assertEqual(repository.get(projection_id), before[projection_id][0])
            self.assertEqual(
                repository.list_members(projection_id, visible_only=False),
                before[projection_id][1],
            )
        self._assert_foundation_present(projection_a_id, foundation_a)
        self._assert_foundation_present(projection_b_id, foundation_b)

    def test_identical_foundation_publications_serialize_without_revision_churn(self) -> None:
        foundation = _foundation("run-foundation-identical-concurrency")
        barrier = threading.Barrier(2)

        def publish() -> dict[str, Any]:
            barrier.wait(timeout=5)
            return self.writer.publish_filter_projection_foundation_run_scope_projection(
                foundation=foundation,
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = [future.result(timeout=15) for future in (executor.submit(publish), executor.submit(publish))]

        self.assertEqual(
            {result["projection"]["projection_id"] for result in results},
            {results[0]["projection"]["projection_id"]},
        )
        self.assertEqual(
            {result["projection"]["metadata"][PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY] for result in results},
            {results[0]["projection"]["metadata"][PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY]},
        )
        self._assert_foundation_present(results[0]["projection"]["projection_id"], foundation)

    def test_foundation_and_product_first_publications_share_one_run_lock_and_one_route(self) -> None:
        run_id = "run-foundation-product-route-concurrency"
        foundation = _foundation(run_id)
        barrier = threading.Barrier(2)

        def publish_foundation() -> str:
            barrier.wait(timeout=5)
            self.writer.publish_filter_projection_foundation_run_scope_projection(foundation=foundation)
            return "foundation"

        def publish_product() -> str:
            barrier.wait(timeout=5)
            self.writer.publish_run_scope_projection(
                run_id=run_id,
                members=[{"candidate_identity_key": "linkedin:product"}],
                replace_members=True,
            )
            return "product"

        outcomes: list[str] = []
        errors: list[Exception] = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            for future in (executor.submit(publish_foundation), executor.submit(publish_product)):
                try:
                    outcomes.append(future.result(timeout=15))
                except Exception as exc:  # noqa: BLE001 - the losing route must fail closed
                    errors.append(exc)

        self.assertEqual(len(outcomes), 1)
        self.assertEqual(len(errors), 1)
        product_route = self.store.repos.serving_projection.get_run_link(run_id)
        foundation_route = self.store.repos.serving_projection.get_run_link(
            run_id,
            link_type=FILTER_PROJECTION_FOUNDATION_ROUTE_TYPE,
        )
        self.assertNotEqual(bool(product_route), bool(foundation_route))
        self.assertIn(
            "filter_projection_foundation",
            str(errors[0]),
        )

    def test_divergent_foundation_publications_never_commit_a_mixed_member_scope(self) -> None:
        run_id = "run-foundation-divergent-concurrency"
        first = build_filter_projection_publication_foundation(
            source_run_id=run_id,
            members=[_members()[0]],
        )
        second = build_filter_projection_publication_foundation(
            source_run_id=run_id,
            members=[_members()[1]],
        )
        barrier = threading.Barrier(2)

        def publish(foundation: FilterProjectionPublicationFoundation) -> dict[str, Any]:
            barrier.wait(timeout=5)
            return self.writer.publish_filter_projection_foundation_run_scope_projection(
                foundation=foundation,
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = [
                future.result(timeout=15)
                for future in (
                    executor.submit(publish, first),
                    executor.submit(publish, second),
                )
            ]

        projection_id = results[-1]["projection"]["projection_id"]
        member_keys = {
            member["candidate_identity_key"]
            for member in self.store.repos.serving_projection.list_members(
                projection_id,
                visible_only=False,
            )
        }
        self.assertIn(member_keys, ({"linkedin:ada"}, {"linkedin:grace"}))
        projection = self.store.repos.serving_projection.get(projection_id)
        self.assertIn(FILTER_PROJECTION_FOUNDATION_METADATA_KEY, projection["metadata"])
        for member in self.store.repos.serving_projection.list_members(projection_id, visible_only=False):
            self.assertIn(FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY, member["provenance"])

    def test_foundation_and_generic_parent_race_never_commits_mixed_carriers(self) -> None:
        run_id = "run-foundation-generic-race"
        projection_id = "proj-foundation-generic-race"
        foundation = _foundation(run_id)
        barrier = threading.Barrier(2)

        def publish_foundation() -> str:
            barrier.wait(timeout=5)
            self.writer.publish_filter_projection_foundation_run_scope_projection(
                foundation=foundation,
                projection_id=projection_id,
            )
            return "foundation"

        def publish_generic() -> str:
            barrier.wait(timeout=5)
            self.store.repos.serving_projection.upsert(
                {
                    "projection_id": projection_id,
                    "projection_type": "run_scope_projection",
                    "source_run_id": run_id,
                    "state": "draft",
                    "metadata": {"writer_id": "generic-race"},
                }
            )
            return "generic"

        outcomes: list[str] = []
        errors: list[Exception] = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = (executor.submit(publish_foundation), executor.submit(publish_generic))
            for future in futures:
                try:
                    outcomes.append(future.result(timeout=15))
                except Exception as exc:  # noqa: BLE001 - the losing writer must fail closed
                    errors.append(exc)

        self.assertEqual(len(outcomes), 1)
        self.assertEqual(len(errors), 1)
        projection = self.store.repos.serving_projection.get(projection_id)
        members = self.store.repos.serving_projection.list_members(projection_id, visible_only=False)
        if FILTER_PROJECTION_FOUNDATION_METADATA_KEY in projection["metadata"]:
            self.assertEqual(len(members), foundation.member_count)
            for member in members:
                self.assertIn(FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY, member["provenance"])
        else:
            self.assertEqual(members, [])

    def test_route_failure_rolls_back_foundation_parent_and_members(self) -> None:
        foundation = _foundation("run-foundation-route-rollback")
        adapter = self.store._control_plane_postgres  # noqa: SLF001
        constraint_name = "test_s1f0b_foundation_route_atomic_rollback"
        with adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    f"ALTER TABLE run_projection_links ADD CONSTRAINT {constraint_name} "
                    f"CHECK (run_id <> '{foundation.source_run_id}')"
                )
            connection.commit()
        try:
            with self.assertRaisesRegex(RuntimeError, "publish_serving_projection"):
                self.writer.publish_filter_projection_foundation_run_scope_projection(foundation=foundation)
        finally:
            with adapter._connect() as connection:  # noqa: SLF001
                with connection.cursor() as cursor:
                    cursor.execute(f"ALTER TABLE run_projection_links DROP CONSTRAINT IF EXISTS {constraint_name}")
                connection.commit()

        self.assertEqual(
            self.store.repos.serving_projection.list(
                source_run_id=foundation.source_run_id,
                limit=10,
            ),
            [],
        )
        self.assertEqual(
            self.store.repos.serving_projection.get_run_link(foundation.source_run_id),
            {},
        )
        self.assertEqual(adapter.count_rows("serving_projection_members"), 0)

    def test_collection_publication_always_strips_foundation_carriers(self) -> None:
        publication = self.writer.publish_collection_authoritative_projection(
            collection_id="company:thinkingmachineslab",
            active_collection_version="v1",
            metadata={
                "collection_owner": "company_asset",
                FILTER_PROJECTION_FOUNDATION_METADATA_KEY: {"forged": True},
            },
            members=[
                {
                    "candidate_identity_key": "linkedin:collection-member",
                    "provenance": {
                        "source": "company_asset",
                        FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY: {"forged": True},
                    },
                }
            ],
            replace_members=True,
        )
        projection = publication["projection"]
        member = self.store.repos.serving_projection.list_members(
            projection["projection_id"],
            visible_only=False,
        )[0]

        self.assertEqual(projection["metadata"]["collection_owner"], "company_asset")
        self.assertNotIn(
            FILTER_PROJECTION_FOUNDATION_METADATA_KEY,
            projection["metadata"],
        )
        self.assertEqual(member["provenance"]["source"], "company_asset")
        self.assertNotIn(
            FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY,
            member["provenance"],
        )

        foundation, foundation_publication = self._publish_foundation("run-collection-route-invalidation")
        foundation_projection_id = foundation_publication["projection"]["projection_id"]
        self.writer.publish_collection_authoritative_projection(
            collection_id="company:thinkingmachineslab",
            active_collection_version="v2",
            projection_id=foundation_projection_id,
            members=self._plain_members(foundation),
            replace_members=True,
        )
        self._assert_foundation_absent(foundation_projection_id)
