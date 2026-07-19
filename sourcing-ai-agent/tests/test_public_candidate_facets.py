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
                )
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
                        candidate_filter=normalize_candidate_page_filter(
                            {"employment_statuses": selected}
                        ),
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

    def test_operation_runtime_schema_enum_derives_from_the_one_helper(self) -> None:
        schema = _PROJECTION_READ_FILTER_PROPERTIES["function_buckets"]
        expected = [item_id for item_id, _label in public_function_facet_option_spec()]
        self.assertEqual(schema["items"]["enum"], expected)
        self.assertEqual(schema["maxItems"], len(expected))
        self.assertIn("infra_systems", schema["items"]["enum"])
        self.assertIn("founding", schema["items"]["enum"])

    def test_serving_projection_keyword_only_check_accepts_new_ids(self) -> None:
        self.assertTrue(
            _candidate_filter_is_keyword_only(
                {"search_keyword": "agent", "function_buckets": ["infra_systems", "founding"]}
            )
        )
        self.assertFalse(
            _candidate_filter_is_keyword_only({"search_keyword": "agent", "function_buckets": ["bogus"]})
        )

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
                    {"lane_id": "cohort_current_research_d", "employment_status": "current", "role_bucket_id": "research"},
                    {"lane_id": "cohort_former_engineering_d", "employment_status": "former", "role_bucket_id": "engineering"},
                ],
                "cohort_role_bucket_ids": ["research", "engineering"],
                "cohort_employment_statuses": ["current", "former"],
            },
        )
        legacy_engineer = Candidate(candidate_id="c2", name_en="Legacy Dev", role="Software Engineer", employment_status="current")
        legacy_unknown = Candidate(candidate_id="c3", name_en="Legacy Other", role="Sales Director", employment_status="former")

        result = self._build([cohort_candidate, legacy_engineer, legacy_unknown])
        served = [
            row
            for page in result["incremental_artifacts"]["page_payloads"]
            for row in page["payload"]["candidates"]
        ]
        self.assertEqual(len(served), 3)
        by_id = {row["candidate_id"]: row for row in served}
        self.assertEqual(by_id["c1"]["function_bucket_ids"], ["research", "engineering"])
        self.assertEqual(by_id["c1"]["function_bucket_source"], "lane_membership")
        self.assertEqual(by_id["c2"]["function_bucket_ids"], ["engineering"])
        self.assertEqual(by_id["c2"]["function_bucket_source"], "registry_evidence")
        self.assertEqual(by_id["c3"]["function_bucket_ids"], ["unknown"])
        self.assertEqual(by_id["c3"]["function_bucket_source"], "legacy_inference")

        # Per-row ids always agree with the served facet counts at the same build point.
        counts = result["artifact_summary"]["public_facet_counts"]
        self.assertEqual(counts["function_counts"], {"research": 1, "engineering": 2, "unknown": 1})
        # Dual-status membership is counted under both statuses.
        self.assertEqual(counts["employment_counts"], {"current": 2, "former": 2})


if __name__ == "__main__":
    unittest.main()
