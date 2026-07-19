"""FT1 result-facet contract tests (FT0 v2 §4-§6; §10.1 matrix rows 1-5, 9).

Covers the single registry-derived facet option source, the multi-valued
membership-first bucket derivation with exact provenance markers, the
function-id "8" collision rule, membership-based employment facets/filters,
and the retained legacy-inference fallback byte-parity.  All fake/scripted:
zero provider/model/network/PG calls.
"""

from __future__ import annotations

import unittest

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.candidate_artifacts import _build_artifact_view_payloads
from sourcing_agent.cohort_selection import cohort_selection_options_payload
from sourcing_agent.domain import Candidate
from sourcing_agent.operation_runtime import _PROJECTION_READ_FILTER_PROPERTIES
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.public_candidate_facets import (
    FUNCTION_BUCKET_SOURCE_LANE_MEMBERSHIP,
    FUNCTION_BUCKET_SOURCE_LEGACY_INFERENCE,
    FUNCTION_BUCKET_SOURCE_REGISTRY_EVIDENCE,
    FUNCTION_BUCKET_SOURCES,
    CohortFacetProvenanceError,
    candidate_employment_statuses_for_public_facets,
    candidate_function_bucket_projection_for_public_facets,
    candidate_function_buckets_for_public_facets,
    candidate_matches_candidate_page_filter,
    candidate_page_filter_active,
    normalize_candidate_page_filter,
    public_facet_counts_from_records,
    public_facet_summary_from_counts,
    public_function_facet_option_spec,
)
from sourcing_agent.query_signal_knowledge import ROLE_BUCKET_KNOWLEDGE
from sourcing_agent.serving_projection_reader import _candidate_filter_is_keyword_only


def _pre_ft1_function_buckets(record: dict) -> list[str]:
    """Byte-exact oracle of the pre-FT1 single-winner derivation.

    Guards the retained legacy fallback (FT0 §5.3): records with neither lane
    membership nor registry-mappable function evidence must keep producing
    exactly these buckets, marked ``legacy_inference``.
    """

    metadata = dict(record.get("metadata") or {})
    function_ids = [
        str(item or "").strip()
        for item in list(record.get("function_ids") or metadata.get("function_ids") or [])
        if str(item or "").strip()
    ]
    if len(function_ids) == 1:
        buckets: set[str] = set()
        if "24" in function_ids:
            buckets.add("research")
        if "8" in function_ids:
            buckets.add("engineering")
        if "19" in function_ids:
            buckets.add("product_management")
        if any(item not in {"24", "8", "19"} for item in function_ids):
            buckets.add("other")
        return sorted(buckets or {"other"})

    role_bucket = str(record.get("role_bucket") or metadata.get("role_bucket") or "").strip().lower()
    role_mapping = {
        "research": "research",
        "engineering": "engineering",
        "infra_systems": "engineering",
        "product_management": "product_management",
    }
    if role_bucket in role_mapping:
        return [role_mapping[role_bucket]]

    corpus = " ".join(
        str(value or "")
        for value in (
            record.get("headline"),
            record.get("summary"),
            record.get("role"),
            record.get("team"),
            metadata.get("headline"),
            metadata.get("summary"),
            metadata.get("role"),
        )
    ).lower()
    if any(token in corpus for token in ("research scientist", "research engineer", "researcher", "scientist")):
        return ["research"]
    if any(
        token in corpus
        for token in (
            "software engineer",
            "machine learning engineer",
            "systems engineer",
            "platform engineer",
            "engineer",
            "engineering",
            "infrastructure",
            "backend",
            "frontend",
            "developer",
        )
    ):
        return ["engineering"]
    if "product manager" in corpus or "product management" in corpus:
        return ["product_management"]
    return ["unknown"]


def _membership(*entries: tuple[str, str]) -> dict:
    return {
        "metadata": {
            "cohort_lane_membership": [
                {
                    "lane_id": f"cohort_{status}_{role}_digest",
                    "employment_status": status,
                    "role_bucket_id": role,
                }
                for status, role in entries
            ]
        }
    }


class FunctionFacetOptionParityTest(unittest.TestCase):
    """Matrix row 1: facet named roles == options-endpoint roles == registry."""

    def test_named_roles_match_options_endpoint_and_registry_projection(self) -> None:
        options = cohort_selection_options_payload()
        endpoint_roles = [(str(item["id"]), str(item["label"])) for item in options["role_buckets"]]

        registry_projection = [
            (role_id, str(ROLE_BUCKET_KNOWLEDGE[role_id]["selectable_label"]).strip())
            for role_id in sorted(
                ROLE_BUCKET_KNOWLEDGE,
                key=lambda role_id: (
                    int(ROLE_BUCKET_KNOWLEDGE[role_id]["selectable_order"]),
                    role_id,
                ),
            )
        ]
        self.assertEqual(endpoint_roles, registry_projection)

        spec = public_function_facet_option_spec()
        self.assertEqual(spec[: len(endpoint_roles)], endpoint_roles)
        named_ids = [item_id for item_id, _label in spec[: len(endpoint_roles)]]
        self.assertIn("infra_systems", named_ids)
        self.assertIn("founding", named_ids)

    def test_other_and_unknown_present_but_never_selectable(self) -> None:
        spec = public_function_facet_option_spec()
        self.assertEqual(spec[-2:], [("other", "其他"), ("unknown", "未提供职能信息")])
        selectable_ids = {str(item["id"]) for item in cohort_selection_options_payload()["role_buckets"]}
        self.assertNotIn("other", selectable_ids)
        self.assertNotIn("unknown", selectable_ids)

    def test_summary_projects_the_same_option_spec_with_counts(self) -> None:
        counts = {
            "candidate_count": 9,
            "function_counts": {
                "research": 2,
                "engineering": 3,
                "product_management": 1,
                "infra_systems": 1,
                "founding": 1,
                "other": 1,
                "unknown": 1,
            },
        }
        summary = public_facet_summary_from_counts(counts)
        spec = public_function_facet_option_spec()
        self.assertEqual(
            [(option["id"], option["label"]) for option in summary["functions"]],
            spec,
        )
        counts_by_id = {option["id"]: option["count"] for option in summary["functions"]}
        self.assertEqual(counts_by_id["engineering"], 3)
        self.assertEqual(counts_by_id["infra_systems"], 1)
        self.assertEqual(counts_by_id["founding"], 1)

    def test_zero_count_facets_stay_hidden_but_options_stay_ordered(self) -> None:
        summary = public_facet_summary_from_counts(
            {"candidate_count": 2, "function_counts": {"founding": 1, "research": 1}}
        )
        self.assertEqual(
            [option["id"] for option in summary["functions"]],
            ["research", "founding"],
        )


class MultiRoleMembershipDerivationTest(unittest.TestCase):
    """Matrix row 2: multi-valued membership-first derivation + exact sources."""

    def test_source_enum_is_exact_and_closed(self) -> None:
        self.assertEqual(
            FUNCTION_BUCKET_SOURCES,
            ("lane_membership", "registry_evidence", "legacy_inference"),
        )
        self.assertEqual(FUNCTION_BUCKET_SOURCE_LANE_MEMBERSHIP, "lane_membership")
        self.assertEqual(FUNCTION_BUCKET_SOURCE_REGISTRY_EVIDENCE, "registry_evidence")
        self.assertEqual(FUNCTION_BUCKET_SOURCE_LEGACY_INFERENCE, "legacy_inference")

    def test_multi_role_candidate_counts_in_every_membership_bucket(self) -> None:
        record = _membership(("current", "research"), ("former", "engineering"))
        projection = candidate_function_bucket_projection_for_public_facets(record)
        self.assertEqual(projection["function_bucket_ids"], ["research", "engineering"])
        self.assertEqual(projection["function_bucket_source"], "lane_membership")

        counts = public_facet_counts_from_records([record])
        self.assertEqual(counts["function_counts"], {"research": 1, "engineering": 1})
        # One candidate, two buckets: multi-valued counting replaces single-winner.
        self.assertEqual(counts["candidate_count"], 1)

    def test_membership_beats_function_evidence(self) -> None:
        record = _membership(("current", "research"))
        record["function_ids"] = ["8"]
        record["headline"] = "Software Engineer"
        projection = candidate_function_bucket_projection_for_public_facets(record)
        self.assertEqual(projection["function_bucket_ids"], ["research"])
        self.assertEqual(projection["function_bucket_source"], "lane_membership")

    def test_registry_evidence_beats_legacy_inference(self) -> None:
        record = {"function_ids": ["24"], "headline": "Software Engineer"}
        projection = candidate_function_bucket_projection_for_public_facets(record)
        self.assertEqual(projection["function_bucket_ids"], ["research"])
        self.assertEqual(projection["function_bucket_source"], "registry_evidence")

    def test_membership_role_mirror_alone_is_accepted(self) -> None:
        record = {"metadata": {"cohort_role_bucket_ids": ["infra_systems", "research"]}}
        projection = candidate_function_bucket_projection_for_public_facets(record)
        self.assertEqual(projection["function_bucket_ids"], ["research", "infra_systems"])
        self.assertEqual(projection["function_bucket_source"], "lane_membership")

    def test_wrapper_returns_ids_for_existing_consumers(self) -> None:
        record = _membership(("current", "founding"))
        self.assertEqual(candidate_function_buckets_for_public_facets(record), ["founding"])


class FunctionIdCollisionTest(unittest.TestCase):
    """Matrix row 3: engineering/infra_systems share function id "8"."""

    def test_bare_8_legacy_record_maps_to_engineering_only(self) -> None:
        projection = candidate_function_bucket_projection_for_public_facets({"function_ids": ["8"]})
        self.assertEqual(projection["function_bucket_ids"], ["engineering"])
        self.assertEqual(projection["function_bucket_source"], "registry_evidence")

    def test_explicit_infra_evidence_additionally_attributes_infra_systems(self) -> None:
        projection = candidate_function_bucket_projection_for_public_facets(
            {"function_ids": ["8"], "role_bucket": "infra_systems"}
        )
        self.assertEqual(projection["function_bucket_ids"], ["engineering", "infra_systems"])
        self.assertEqual(projection["function_bucket_source"], "registry_evidence")

    def test_cohort_infra_lane_member_uses_membership(self) -> None:
        record = _membership(("current", "infra_systems"))
        record["function_ids"] = ["8"]
        projection = candidate_function_bucket_projection_for_public_facets(record)
        self.assertEqual(projection["function_bucket_ids"], ["infra_systems"])
        self.assertEqual(projection["function_bucket_source"], "lane_membership")

    def test_headline_keyword_overlap_never_attributes_infra_systems(self) -> None:
        for headline in ("Platform Engineer", "Systems Engineer", "Infrastructure Engineer"):
            with self.subTest(headline=headline):
                projection = candidate_function_bucket_projection_for_public_facets({"headline": headline})
                self.assertEqual(projection["function_bucket_source"], "legacy_inference")
                self.assertNotIn("infra_systems", projection["function_bucket_ids"])


class EmploymentFacetFromMembershipTest(unittest.TestCase):
    """Matrix row 4: membership status set drives counts/filters; lead pinned."""

    def test_dual_status_candidate_counts_under_current_and_former(self) -> None:
        record = _membership(("current", "research"), ("former", "research"))
        record["employment_status"] = "current"  # lossy display-only top-level
        counts = public_facet_counts_from_records([record])
        self.assertEqual(counts["employment_counts"], {"current": 1, "former": 1})

    def test_dual_status_candidate_matches_both_employment_filters(self) -> None:
        record = _membership(("current", "research"), ("former", "research"))
        record["employment_status"] = "current"
        for selected in (["current"], ["former"], ["current", "former"]):
            with self.subTest(selected=selected):
                self.assertTrue(
                    candidate_matches_candidate_page_filter(
                        record=record,
                        candidate_filter=normalize_candidate_page_filter({"employment_statuses": selected}),
                    )
                )

    def test_membership_overrides_lossy_top_level_for_facets(self) -> None:
        record = _membership(("former", "engineering"))
        record["employment_status"] = "current"
        counts = public_facet_counts_from_records([record])
        self.assertEqual(counts["employment_counts"], {"former": 1})
        self.assertFalse(
            candidate_matches_candidate_page_filter(
                record=record,
                candidate_filter=normalize_candidate_page_filter({"employment_statuses": ["current"]}),
            )
        )
        self.assertTrue(
            candidate_matches_candidate_page_filter(
                record=record,
                candidate_filter=normalize_candidate_page_filter({"employment_statuses": ["former"]}),
            )
        )

    def test_top_level_display_derivation_is_unchanged(self) -> None:
        row = {
            "username": "dual-status",
            "cohort_lane_membership": [
                {"lane_id": "cohort_current_research_d", "employment_status": "current", "role_bucket_id": "research"},
                {"lane_id": "cohort_former_research_d", "employment_status": "former", "role_bucket_id": "research"},
            ],
        }
        entry = AcquisitionEngine._cohort_provider_row_to_search_seed_entry(row, manifest_digest="d" * 64)
        self.assertEqual(entry["employment_status"], "current")
        self.assertEqual(entry["metadata"]["cohort_employment_statuses"], ["current", "former"])

    def test_lead_semantics_regression_pinned(self) -> None:
        lead = {"headline": "No status evidence"}
        counts = public_facet_counts_from_records([lead])
        self.assertEqual(counts["employment_counts"], {"lead": 1})
        self.assertFalse(
            candidate_matches_candidate_page_filter(
                record=lead,
                candidate_filter=normalize_candidate_page_filter({"employment_statuses": ["current"]}),
            )
        )
        self.assertTrue(
            candidate_matches_candidate_page_filter(
                record=lead,
                candidate_filter=normalize_candidate_page_filter({"employment_statuses": ["current", "former"]}),
            )
        )
        legacy_current = {"employment_status": "current"}
        self.assertFalse(
            candidate_matches_candidate_page_filter(
                record=legacy_current,
                candidate_filter=normalize_candidate_page_filter({"employment_statuses": ["former"]}),
            )
        )


class LegacyFallbackParityTest(unittest.TestCase):
    """Matrix row 5: legacy-shaped records keep pre-FT1 results via fallback."""

    def test_legacy_shaped_records_match_pre_ft1_oracle_byte_for_byte(self) -> None:
        legacy_records = [
            {"headline": "Research Scientist"},
            {"headline": "Software Engineer"},
            {"headline": "Platform Engineer"},
            {"headline": "Product Manager"},
            {"headline": "Sales Director"},
            {"summary": "machine learning engineer"},
            {"role": "backend developer"},
            {"team": "infrastructure"},
            {"metadata": {"headline": "researcher"}},
            {"metadata": {"role": "product management"}},
            {"role_bucket": "sales"},
            {"role_bucket": "sales", "headline": "research scientist"},
            {"role_bucket": "operations", "team": "engineering"},
            {"display_name": "No Evidence At All"},
        ]
        for record in legacy_records:
            with self.subTest(record=record):
                projection = candidate_function_bucket_projection_for_public_facets(record)
                self.assertEqual(projection["function_bucket_source"], "legacy_inference")
                self.assertEqual(projection["function_bucket_ids"], _pre_ft1_function_buckets(record))

    def test_ft0_named_changes_over_pre_ft1_are_exact(self) -> None:
        # infra_systems is a named facet now (was collapsed into engineering).
        projection = candidate_function_bucket_projection_for_public_facets({"role_bucket": "infra_systems"})
        self.assertEqual(projection["function_bucket_ids"], ["infra_systems"])
        self.assertEqual(projection["function_bucket_source"], "registry_evidence")
        # founding is a named facet now (bare id "9" was "other").
        projection = candidate_function_bucket_projection_for_public_facets({"function_ids": ["9"]})
        self.assertEqual(projection["function_bucket_ids"], ["founding"])
        # Multi-id evidence is multi-valued now (was text re-inference).
        projection = candidate_function_bucket_projection_for_public_facets({"function_ids": ["8", "24"]})
        self.assertEqual(projection["function_bucket_ids"], ["research", "engineering"])
        self.assertEqual(projection["function_bucket_source"], "registry_evidence")
        # Unchanged values: single mapped ids and unmapped ids.
        self.assertEqual(
            candidate_function_buckets_for_public_facets({"function_ids": ["19"]}),
            ["product_management"],
        )
        self.assertEqual(
            candidate_function_buckets_for_public_facets({"function_ids": ["99"]}),
            ["other"],
        )


class FunctionFilterEnumTest(unittest.TestCase):
    """Matrix row 9: every backend filter consumer accepts the named facets."""

    def test_page_filter_normalization_accepts_all_facet_ids_and_drops_garbage(self) -> None:
        normalized = normalize_candidate_page_filter(
            {"function_buckets": ["infra_systems", "founding", "research", "bogus", "unknown", "other"]}
        )
        self.assertEqual(
            normalized["function_buckets"],
            ["infra_systems", "founding", "research", "unknown", "other"],
        )

    def test_previously_accepted_values_are_still_accepted(self) -> None:
        normalized = normalize_candidate_page_filter(
            {"function_buckets": ["research", "engineering", "product_management", "other", "unknown"]}
        )
        self.assertEqual(
            set(normalized["function_buckets"]),
            {"research", "engineering", "product_management", "other", "unknown"},
        )

    def test_filter_active_boundary_follows_the_full_option_set(self) -> None:
        all_ids = [item_id for item_id, _label in public_function_facet_option_spec()]
        self.assertFalse(candidate_page_filter_active({"function_buckets": all_ids}))
        self.assertTrue(candidate_page_filter_active({"function_buckets": ["infra_systems"]}))
        # Selecting only the five legacy ids is now an active (narrowing) filter.
        self.assertTrue(
            candidate_page_filter_active(
                {"function_buckets": ["research", "engineering", "product_management", "other", "unknown"]}
            )
        )

    def test_operation_runtime_v1_schema_keeps_exact_historical_enum(self) -> None:
        # FT1-FF (finding 7): the published projection_filter_request_v1
        # schema is immutable — the FT1 in-place enum mutation was reverted,
        # so v1 keeps its exact historical five-value enum and the canonical
        # Cohort mapping (with the FT1 named roles) lives in the distinct
        # projection_filter_request_v2 contract.
        schema = _PROJECTION_READ_FILTER_PROPERTIES["function_buckets"]
        self.assertEqual(
            schema["items"]["enum"],
            ["research", "engineering", "product_management", "other", "unknown"],
        )
        self.assertEqual(schema["maxItems"], 5)

    def test_serving_projection_keyword_only_check_keeps_active_facet_filters(self) -> None:
        # FT1-FF (finding 6): keyword-only iff removing search_keyword leaves
        # NO active normalized filter.  A keyword combined with a single
        # function/location/employment value is NOT keyword-only — the facet
        # filter must reach the filtered index path instead of being silently
        # discarded.
        self.assertFalse(
            _candidate_filter_is_keyword_only({"search_keyword": "agent", "function_buckets": ["infra_systems"]})
        )
        self.assertFalse(
            _candidate_filter_is_keyword_only({"search_keyword": "agent", "function_buckets": ["founding"]})
        )
        self.assertFalse(_candidate_filter_is_keyword_only({"search_keyword": "agent", "locations": ["us"]}))
        self.assertFalse(
            _candidate_filter_is_keyword_only({"search_keyword": "agent", "employment_statuses": ["current"]})
        )
        # Unrecognized values are not full-domain no-ops either: the safe
        # answer stays "not keyword-only" (normalized filters drop them
        # upstream; a raw caller gets the conservative filtered path).
        self.assertFalse(_candidate_filter_is_keyword_only({"search_keyword": "agent", "function_buckets": ["bogus"]}))
        # The genuine all-values no-op selection IS keyword-only.
        all_ids = [item_id for item_id, _label in public_function_facet_option_spec()]
        self.assertTrue(_candidate_filter_is_keyword_only({"search_keyword": "agent", "function_buckets": all_ids}))
        self.assertTrue(
            _candidate_filter_is_keyword_only({"search_keyword": "agent", "employment_statuses": ["current", "former"]})
        )
        self.assertTrue(
            _candidate_filter_is_keyword_only({"search_keyword": "agent", "locations": ["us", "other", "unknown"]})
        )
        self.assertTrue(_candidate_filter_is_keyword_only({"search_keyword": "agent"}))
        self.assertFalse(_candidate_filter_is_keyword_only({"function_buckets": ["infra_systems"]}))

    def test_orchestrator_projection_filter_accepts_new_ids_and_rejects_garbage(self) -> None:
        normalized = SourcingOrchestrator._normalize_operation_projection_filter(
            {"function_buckets": ["infra_systems", "founding"]}
        )
        self.assertEqual(normalized["function_buckets"], ["founding", "infra_systems"])
        for legacy_value in ("research", "engineering", "product_management", "other", "unknown"):
            with self.subTest(legacy_value=legacy_value):
                accepted = SourcingOrchestrator._normalize_operation_projection_filter(
                    {"function_buckets": [legacy_value]}
                )
                self.assertEqual(accepted["function_buckets"], [legacy_value])
        with self.assertRaises(ValueError):
            SourcingOrchestrator._normalize_operation_projection_filter({"function_buckets": ["bogus"]})


class ServedFunctionBucketProjectionTest(unittest.TestCase):
    """FT0 §5.2: served rows carry the projection at the real build point."""

    def _build(self, candidates: list[Candidate]) -> dict:
        import tempfile
        from pathlib import Path

        class _Registry:
            def get_bulk(self, _urls):
                return {}

        class _Repos:
            linkedin_profile_registry = _Registry()

        class _FakeStore:
            repos = _Repos()

            def list_candidate_materialization_states(self, **_kwargs):
                return []

        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        snapshot_dir = root / "snap-1"
        snapshot_dir.mkdir()
        artifact_dir = root / "artifacts"
        artifact_dir.mkdir()
        materialized_view = {
            "target_company": "Acme",
            "snapshot_dir": str(snapshot_dir),
            "company_key": "acme",
            "company_identity": {"canonical_name": "Acme"},
            "source_snapshots": [],
            "source_snapshot_selection": {},
            "control_plane_candidate_count": len(candidates),
            "control_plane_evidence_count": 0,
        }
        return _build_artifact_view_payloads(
            store=_FakeStore(),
            artifact_dir=artifact_dir,
            materialized_view=materialized_view,
            candidates=candidates,
            evidence=[],
            evidence_by_candidate={},
            asset_view="canonical_merged",
            build_profile="default",
        )

    def test_served_rows_carry_membership_first_projection_at_build_point(self) -> None:
        cohort_candidate = Candidate(
            candidate_id="c1",
            name_en="Dual Role",
            employment_status="current",
            metadata={
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
        )
        legacy_engineer = Candidate(
            candidate_id="c2", name_en="Legacy Dev", role="Software Engineer", employment_status="current"
        )
        legacy_asset_only = Candidate(
            candidate_id="c3", name_en="Legacy Other", role="Sales Director", employment_status="former"
        )

        result = self._build([cohort_candidate, legacy_engineer, legacy_asset_only])
        served = [
            row for page in result["incremental_artifacts"]["page_payloads"] for row in page["payload"]["candidates"]
        ]
        self.assertEqual(len(served), 3)
        by_id = {row["candidate_id"]: row for row in served}
        self.assertEqual(by_id["c1"]["function_bucket_ids"], ["research", "engineering"])
        self.assertEqual(by_id["c1"]["function_bucket_source"], "lane_membership")
        self.assertEqual(by_id["c2"]["function_bucket_ids"], ["engineering"])
        self.assertEqual(by_id["c2"]["function_bucket_source"], "registry_evidence")
        # FT1-FF (finding 10): the artifact normalization derives the TML
        # asset-only ``leadership`` role bucket for "Sales Director"; asset-only
        # buckets are structured ``other`` evidence, never legacy inference.
        self.assertEqual(by_id["c3"]["function_bucket_ids"], ["other"])
        self.assertEqual(by_id["c3"]["function_bucket_source"], "registry_evidence")

        # FT1-FF (findings 5/8): page rows also carry the authoritative
        # membership employment status set exactly when non-empty.
        self.assertEqual(by_id["c1"]["employment_statuses"], ["current", "former"])
        self.assertNotIn("employment_statuses", by_id["c2"])
        self.assertNotIn("employment_statuses", by_id["c3"])

        # Per-row ids always agree with the served facet counts at the same build point.
        counts = result["artifact_summary"]["public_facet_counts"]
        self.assertEqual(counts["function_counts"], {"research": 1, "engineering": 2, "other": 1})
        # Dual-status membership is counted under both statuses.
        self.assertEqual(counts["employment_counts"], {"current": 2, "former": 2})

        # FT1-FF (finding 8): materialized candidate documents receive the
        # identical centralized row projection, not just page payloads.
        materialized_by_id = {row["candidate_id"]: row for row in result["materialized_documents"]["candidates"]}
        self.assertEqual(materialized_by_id["c1"]["function_bucket_ids"], ["research", "engineering"])
        self.assertEqual(materialized_by_id["c1"]["function_bucket_source"], "lane_membership")
        self.assertEqual(materialized_by_id["c1"]["employment_statuses"], ["current", "former"])
        self.assertEqual(materialized_by_id["c2"]["function_bucket_ids"], ["engineering"])
        self.assertEqual(materialized_by_id["c3"]["function_bucket_ids"], ["other"])
        self.assertNotIn("employment_statuses", materialized_by_id["c2"])
        # The artifact carries the bumped projection version (finding 8).
        from sourcing_agent.candidate_artifacts import _CANDIDATE_ARTIFACT_PROJECTION_VERSION

        self.assertEqual(result["artifact_summary"]["projection_version"], _CANDIDATE_ARTIFACT_PROJECTION_VERSION)


class TmlAssetOnlyRoleMappingTest(unittest.TestCase):
    """FT1-FF (finding 10): TML asset-only roles map to structured ``other``."""

    def test_all_three_asset_only_roles_produce_structured_other(self) -> None:
        for role_bucket in ("leadership", "ops", "investor"):
            with self.subTest(role_bucket=role_bucket):
                projection = candidate_function_bucket_projection_for_public_facets({"role_bucket": role_bucket})
                self.assertEqual(projection["function_bucket_ids"], ["other"])
                self.assertEqual(projection["function_bucket_source"], "registry_evidence")
                metadata_projection = candidate_function_bucket_projection_for_public_facets(
                    {"metadata": {"role_bucket": role_bucket}}
                )
                self.assertEqual(metadata_projection["function_bucket_ids"], ["other"])
                self.assertEqual(metadata_projection["function_bucket_source"], "registry_evidence")

    def test_conflicting_headline_never_converts_asset_only_evidence_into_a_named_role(self) -> None:
        for role_bucket in ("leadership", "ops", "investor"):
            with self.subTest(role_bucket=role_bucket):
                projection = candidate_function_bucket_projection_for_public_facets(
                    {"role_bucket": role_bucket, "headline": "Software Engineer"}
                )
                self.assertEqual(projection["function_bucket_ids"], ["other"])
                self.assertEqual(projection["function_bucket_source"], "registry_evidence")

    def test_unrelated_legacy_parity_is_retained(self) -> None:
        # Non-registry, non-asset-only buckets still fall to legacy inference.
        projection = candidate_function_bucket_projection_for_public_facets({"role_bucket": "sales"})
        self.assertEqual(projection["function_bucket_source"], "legacy_inference")
        # Asset-only evidence combines with registry function evidence.
        projection = candidate_function_bucket_projection_for_public_facets(
            {"role_bucket": "ops", "function_ids": ["24"]}
        )
        self.assertEqual(projection["function_bucket_ids"], ["research", "other"])
        self.assertEqual(projection["function_bucket_source"], "registry_evidence")


class CohortProvenanceValidatorTest(unittest.TestCase):
    """FT1-FF (finding 9): malformed Cohort provenance blocks, never falls back."""

    def test_invalid_membership_shapes_raise(self) -> None:
        bad_records = [
            {"metadata": {"cohort_lane_membership": "research"}},
            {"metadata": {"cohort_lane_membership": ["research"]}},
            {"metadata": {"cohort_lane_membership": [{"employment_status": "current", "role_bucket_id": "research"}]}},
            {
                "metadata": {
                    "cohort_lane_membership": [
                        {"lane_id": "l1", "employment_status": "current", "role_bucket_id": "sales"}
                    ]
                }
            },
            {
                "metadata": {
                    "cohort_lane_membership": [
                        {"lane_id": "l1", "employment_status": "lead", "role_bucket_id": "research"}
                    ]
                }
            },
            {
                "metadata": {
                    "cohort_lane_membership": [{"lane_id": "l1", "employment_status": "current", "role_bucket_id": 7}]
                }
            },
        ]
        for record in bad_records:
            with self.subTest(record=record):
                with self.assertRaises(CohortFacetProvenanceError):
                    candidate_function_bucket_projection_for_public_facets(record)

    def test_invalid_mirror_shapes_raise(self) -> None:
        for mirror in ("research", ["research", "sales"], [None], ["leadership"]):
            with self.subTest(mirror=mirror):
                with self.assertRaises(CohortFacetProvenanceError):
                    candidate_function_bucket_projection_for_public_facets(
                        {"metadata": {"cohort_role_bucket_ids": mirror}}
                    )

    def test_lane_mirror_disagreement_raises(self) -> None:
        record = {
            "metadata": {
                "cohort_lane_membership": [
                    {"lane_id": "l1", "employment_status": "current", "role_bucket_id": "research"}
                ],
                "cohort_role_bucket_ids": ["research", "engineering"],
            }
        }
        with self.assertRaises(CohortFacetProvenanceError) as captured:
            candidate_function_bucket_projection_for_public_facets(record)
        self.assertEqual(captured.exception.code, "cohort_facet_provenance_role_disagreement")

    def test_employment_disagreement_and_empty_mirror_raise(self) -> None:
        disagreement = {
            "metadata": {
                "cohort_lane_membership": [
                    {"lane_id": "l1", "employment_status": "current", "role_bucket_id": "research"}
                ],
                "cohort_employment_statuses": ["current", "former"],
            }
        }
        with self.assertRaises(CohortFacetProvenanceError) as captured:
            candidate_employment_statuses_for_public_facets(disagreement)
        self.assertEqual(captured.exception.code, "cohort_facet_provenance_employment_disagreement")

        empty_mirror = {"metadata": {"cohort_employment_statuses": []}}
        with self.assertRaises(CohortFacetProvenanceError) as captured_empty:
            candidate_employment_statuses_for_public_facets(empty_mirror)
        self.assertEqual(captured_empty.exception.code, "cohort_facet_provenance_empty_employment_statuses")

        invalid_status = {"metadata": {"cohort_employment_statuses": ["lead"]}}
        with self.assertRaises(CohortFacetProvenanceError):
            candidate_employment_statuses_for_public_facets(invalid_status)

    def test_legitimate_all_roles_blank_role_case_passes_through(self) -> None:
        # Status-only lanes carry a blank role_bucket_id and the role mirror is
        # an empty list: well-formed provenance with no role evidence, so the
        # record passes through to the registry/legacy tiers.
        record = {
            "function_ids": ["8"],
            "metadata": {
                "cohort_lane_membership": [
                    {"lane_id": "cohort_current_all_roles_d", "employment_status": "current", "role_bucket_id": ""},
                    {"lane_id": "cohort_former_all_roles_d", "employment_status": "former", "role_bucket_id": ""},
                ],
                "cohort_role_bucket_ids": [],
                "cohort_employment_statuses": ["current", "former"],
            },
        }
        projection = candidate_function_bucket_projection_for_public_facets(record)
        self.assertEqual(projection["function_bucket_ids"], ["engineering"])
        self.assertEqual(projection["function_bucket_source"], "registry_evidence")
        self.assertEqual(
            candidate_employment_statuses_for_public_facets(record),
            ["current", "former"],
        )
        legacy_shaped = {
            "headline": "Software Engineer",
            "metadata": {
                "cohort_lane_membership": [
                    {"lane_id": "cohort_current_all_roles_d", "employment_status": "current", "role_bucket_id": ""}
                ],
                "cohort_role_bucket_ids": [],
                "cohort_employment_statuses": ["current"],
            },
        }
        legacy_projection = candidate_function_bucket_projection_for_public_facets(legacy_shaped)
        self.assertEqual(legacy_projection["function_bucket_ids"], ["engineering"])
        self.assertEqual(legacy_projection["function_bucket_source"], "legacy_inference")

    def test_mirror_only_historical_path_is_validated(self) -> None:
        accepted = candidate_function_bucket_projection_for_public_facets(
            {"metadata": {"cohort_role_bucket_ids": ["infra_systems"]}}
        )
        self.assertEqual(accepted["function_bucket_ids"], ["infra_systems"])
        self.assertEqual(accepted["function_bucket_source"], "lane_membership")
        status_only = candidate_employment_statuses_for_public_facets(
            {"metadata": {"cohort_employment_statuses": ["former"]}}
        )
        self.assertEqual(status_only, ["former"])

    def test_malformed_provenance_blocks_facet_count_publication(self) -> None:
        malformed = {
            "metadata": {
                "cohort_lane_membership": [
                    {"lane_id": "l1", "employment_status": "current", "role_bucket_id": "research"}
                ],
                "cohort_role_bucket_ids": ["engineering"],
            }
        }
        with self.assertRaises(CohortFacetProvenanceError):
            public_facet_counts_from_records([malformed])

    def test_persisted_pair_is_the_owned_value_for_read_consumers(self) -> None:
        record = {
            "function_bucket_ids": ["infra_systems"],
            "function_bucket_source": "lane_membership",
            "headline": "Software Engineer",
        }
        projection = candidate_function_bucket_projection_for_public_facets(record)
        self.assertEqual(projection["function_bucket_ids"], ["infra_systems"])
        self.assertEqual(projection["function_bucket_source"], "lane_membership")

    def test_malformed_persisted_pair_or_status_set_raises(self) -> None:
        for record in (
            {"function_bucket_ids": ["research"]},
            {"function_bucket_source": "lane_membership"},
            {"function_bucket_ids": [], "function_bucket_source": "lane_membership"},
            {"function_bucket_ids": ["bogus"], "function_bucket_source": "lane_membership"},
            {"function_bucket_ids": ["research"], "function_bucket_source": "bogus"},
        ):
            with self.subTest(record=record):
                with self.assertRaises(CohortFacetProvenanceError):
                    candidate_function_bucket_projection_for_public_facets(record)
        for record in (
            {"employment_statuses": []},
            {"employment_statuses": ["lead"]},
            {"employment_statuses": "current"},
        ):
            with self.subTest(record=record):
                with self.assertRaises(CohortFacetProvenanceError):
                    candidate_employment_statuses_for_public_facets(record)

    def test_persisted_status_set_is_the_owned_value_for_filters(self) -> None:
        record = {"employment_statuses": ["current", "former"], "employment_status": "current"}
        self.assertEqual(candidate_employment_statuses_for_public_facets(record), ["current", "former"])
        for selected in (["current"], ["former"]):
            with self.subTest(selected=selected):
                self.assertTrue(
                    candidate_matches_candidate_page_filter(
                        record=record,
                        candidate_filter=normalize_candidate_page_filter({"employment_statuses": selected}),
                    )
                )


class ServedPublicSummaryProjectionTest(unittest.TestCase):
    """FT1-FF (finding 5): public summaries carry the owned projection pair,
    the authoritative employment status set, and Cohort provenance metadata."""

    def _serialize(self, record: dict) -> dict:
        orchestrator = object.__new__(SourcingOrchestrator)
        return orchestrator._serialize_asset_population_candidate_api_record(
            dict(record),
            load_profile_timeline=False,
            publishable_email_lookup=None,
        )

    def test_compact_serializer_carries_owned_projection_and_cohort_metadata(self) -> None:
        record = {
            "candidate_id": "c1",
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
        compact = self._serialize(record)
        self.assertEqual(compact["function_bucket_ids"], ["research", "engineering"])
        self.assertEqual(compact["function_bucket_source"], "lane_membership")
        self.assertEqual(compact["employment_statuses"], ["current", "former"])
        metadata = compact["metadata"]
        self.assertEqual(metadata["cohort_role_bucket_ids"], ["research", "engineering"])
        self.assertEqual(metadata["cohort_employment_statuses"], ["current", "former"])
        self.assertEqual(len(metadata["cohort_lane_membership"]), 2)

    def test_compact_serializer_honors_the_persisted_pair_and_omits_empty_status_sets(self) -> None:
        persisted = {
            "candidate_id": "c2",
            "display_name": "Persisted Infra",
            "function_bucket_ids": ["infra_systems"],
            "function_bucket_source": "lane_membership",
        }
        compact = self._serialize(persisted)
        self.assertEqual(compact["function_bucket_ids"], ["infra_systems"])
        self.assertEqual(compact["function_bucket_source"], "lane_membership")
        self.assertNotIn("employment_statuses", compact)
        self.assertNotIn("metadata", compact)
        # Legacy rows keep derived legacy_inference values rather than
        # silently dropping the pair.
        legacy = self._serialize({"candidate_id": "c3", "headline": "Software Engineer"})
        self.assertEqual(legacy["function_bucket_ids"], ["engineering"])
        self.assertEqual(legacy["function_bucket_source"], "legacy_inference")

    def test_compact_serializer_fails_closed_on_malformed_provenance(self) -> None:
        malformed = {
            "candidate_id": "c4",
            "metadata": {
                "cohort_lane_membership": [
                    {"lane_id": "l1", "employment_status": "current", "role_bucket_id": "research"}
                ],
                "cohort_role_bucket_ids": ["engineering"],
            },
        }
        with self.assertRaises(CohortFacetProvenanceError):
            self._serialize(malformed)


if __name__ == "__main__":
    unittest.main()


class AllSelectableRolesNoOpAcrossAxesTest(unittest.TestCase):
    """FT1-FF2 (finding 3): an all-selectable-role (default-complete) function
    selection stays an INACTIVE no-op even when another facet axis is active —
    it never becomes a narrowing predicate that hides other/unknown candidates."""

    _ALL_ROLES = ["research", "engineering", "product_management", "infra_systems", "founding"]

    @staticmethod
    def _other_candidate() -> dict:
        # TML asset-only role evidence → structured ``other`` bucket.
        return {
            "candidate_id": "c-other",
            "display_name": "Other Candidate",
            "role_bucket": "leadership",
            "employment_status": "current",
            "profile_location": "Berlin, Germany",
        }

    @staticmethod
    def _unknown_candidate() -> dict:
        # No role evidence at all → ``unknown`` bucket.
        return {
            "candidate_id": "c-unknown",
            "display_name": "Unknown Candidate",
            "employment_status": "current",
            "profile_location": "New York, NY",
        }

    @staticmethod
    def _research_candidate() -> dict:
        return {
            "candidate_id": "c-research",
            "display_name": "Research Candidate",
            "role_bucket": "research",
            "employment_status": "current",
            "profile_location": "San Francisco, CA",
        }

    def test_all_roles_plus_active_axis_keeps_other_and_unknown_visible(self) -> None:
        from sourcing_agent.public_candidate_facets import apply_candidate_page_filter

        for name, extra in (
            ("employment", {"employment_statuses": ["current"]}),
            ("locations", {"locations": ["us", "other"]}),
            ("keyword", {"search_keyword": "candidate"}),
            ("recall_keyword", {"recall_buckets": ["keyword:candidate"]}),
        ):
            with self.subTest(name=name):
                candidate_filter = {"function_buckets": list(self._ALL_ROLES), **extra}
                # Another axis makes the filter active overall...
                self.assertTrue(candidate_page_filter_active(candidate_filter))
                for record in (self._other_candidate(), self._unknown_candidate(), self._research_candidate()):
                    with self.subTest(candidate=record["candidate_id"]):
                        self.assertTrue(
                            candidate_matches_candidate_page_filter(
                                record=record,
                                candidate_filter=candidate_filter,
                            )
                        )
                filtered = apply_candidate_page_filter(
                    candidates=[self._other_candidate(), self._unknown_candidate(), self._research_candidate()],
                    candidate_filter=candidate_filter,
                )
                self.assertEqual(len(filtered), 3)

    def test_all_roles_with_result_only_ids_is_still_a_no_op(self) -> None:
        candidate_filter = {"function_buckets": [*self._ALL_ROLES, "other", "unknown"]}
        self.assertFalse(candidate_page_filter_active(candidate_filter))
        for record in (self._other_candidate(), self._unknown_candidate(), self._research_candidate()):
            with self.subTest(candidate=record["candidate_id"]):
                self.assertTrue(
                    candidate_matches_candidate_page_filter(record=record, candidate_filter=candidate_filter)
                )

    def test_proper_subset_still_narrows(self) -> None:
        candidate_filter = {"function_buckets": ["research"], "employment_statuses": ["current"]}
        self.assertTrue(candidate_page_filter_active(candidate_filter))
        self.assertTrue(
            candidate_matches_candidate_page_filter(
                record=self._research_candidate(), candidate_filter=candidate_filter
            )
        )
        self.assertFalse(
            candidate_matches_candidate_page_filter(record=self._other_candidate(), candidate_filter=candidate_filter)
        )
        self.assertFalse(
            candidate_matches_candidate_page_filter(record=self._unknown_candidate(), candidate_filter=candidate_filter)
        )


class ProjectionFilterV1HistoricalNoOpTest(unittest.TestCase):
    """FT1-FF2 (finding 4): the immutable projection_filter_request_v1 contract
    keeps its HISTORICAL complete-enum no-op execution semantics — dispatch
    normalization is contract-version-aware, v2 keeps the canonical predicate."""

    _V1_COMPLETE_ENUM = ["research", "engineering", "product_management", "other", "unknown"]

    def test_complete_v1_enum_normalizes_to_no_predicate(self) -> None:
        normalized = SourcingOrchestrator._normalize_operation_projection_filter(  # noqa: SLF001
            {"function_buckets": list(self._V1_COMPLETE_ENUM)}
        )
        self.assertNotIn("function_buckets", normalized)

    def test_v1_selectable_cover_with_any_result_only_subset_is_a_no_op(self) -> None:
        for selection in (
            ["research", "engineering", "product_management"],
            ["research", "engineering", "product_management", "other"],
            ["engineering", "research", "product_management", "unknown"],
        ):
            with self.subTest(selection=selection):
                normalized = SourcingOrchestrator._normalize_operation_projection_filter(  # noqa: SLF001
                    {"function_buckets": selection}
                )
                self.assertNotIn("function_buckets", normalized)

    def test_v1_proper_subsets_still_narrow(self) -> None:
        for selection, expected in (
            (["research", "engineering"], ["engineering", "research"]),
            (["other"], ["other"]),
            (["research", "other", "unknown"], ["other", "research", "unknown"]),
        ):
            with self.subTest(selection=selection):
                normalized = SourcingOrchestrator._normalize_operation_projection_filter(  # noqa: SLF001
                    {"function_buckets": selection}
                )
                self.assertEqual(normalized["function_buckets"], expected)

    def test_replayed_v1_complete_enum_returns_newly_classified_candidates(self) -> None:
        # Execution-result replay (not merely schema validation): a replayed v1
        # action carrying the recorded complete-enum selection must match every
        # served row, including FT1-classified infra_systems/founding/other rows.
        from sourcing_agent.public_candidate_facets import apply_candidate_page_filter

        normalized = SourcingOrchestrator._normalize_operation_projection_filter(  # noqa: SLF001
            {"function_buckets": list(self._V1_COMPLETE_ENUM)}
        )
        served_rows = [
            {"candidate_id": "c-infra", "display_name": "Infra Candidate", "role_bucket": "infra_systems"},
            {"candidate_id": "c-founding", "display_name": "Founding Candidate", "role_bucket": "founding"},
            {"candidate_id": "c-research", "display_name": "Research Candidate", "role_bucket": "research"},
            {"candidate_id": "c-other", "display_name": "Other Candidate", "role_bucket": "leadership"},
            {"candidate_id": "c-unknown", "display_name": "Unknown Candidate"},
        ]
        self.assertFalse(candidate_page_filter_active(normalized))
        filtered = apply_candidate_page_filter(candidates=served_rows, candidate_filter=normalized)
        self.assertEqual({row["candidate_id"] for row in filtered}, {row["candidate_id"] for row in served_rows})

    def test_v2_canonical_predicate_still_requires_current_selectable_cover(self) -> None:
        # The version-blind (v2/board) predicate is unchanged: the five legacy
        # ids remain an ACTIVE narrowing selection there because they do not
        # cover infra_systems/founding.
        self.assertTrue(candidate_page_filter_active({"function_buckets": list(self._V1_COMPLETE_ENUM)}))
        all_current_ids = [item_id for item_id, _label in public_function_facet_option_spec()]
        self.assertFalse(candidate_page_filter_active({"function_buckets": all_current_ids}))


class PresentEmptyMembershipValidatorTest(unittest.TestCase):
    """FT1-FF2 (finding 7): a present-empty lane membership is malformed
    provenance and fails closed; the legitimate all-roles status-only lane
    shape stays valid."""

    def test_present_empty_lane_membership_raises(self) -> None:
        record = {"candidate_id": "c1", "metadata": {"cohort_lane_membership": []}}
        with self.assertRaises(CohortFacetProvenanceError) as captured:
            candidate_function_bucket_projection_for_public_facets(record)
        self.assertEqual(captured.exception.code, "cohort_facet_provenance_empty_lane_membership")
        with self.assertRaises(CohortFacetProvenanceError):
            candidate_employment_statuses_for_public_facets(record)

    def test_legitimate_status_only_all_roles_lane_still_passes_through(self) -> None:
        record = {
            "candidate_id": "c1",
            "metadata": {
                "cohort_lane_membership": [
                    {"lane_id": "cohort_current_all_d", "employment_status": "current", "role_bucket_id": ""},
                    {"lane_id": "cohort_former_all_d", "employment_status": "former", "role_bucket_id": ""},
                ],
                "cohort_role_bucket_ids": [],
                "cohort_employment_statuses": ["current", "former"],
            },
        }
        projection = candidate_function_bucket_projection_for_public_facets(record)
        self.assertEqual(projection["function_bucket_source"], FUNCTION_BUCKET_SOURCE_LEGACY_INFERENCE)
        self.assertEqual(
            candidate_employment_statuses_for_public_facets(record),
            ["current", "former"],
        )
