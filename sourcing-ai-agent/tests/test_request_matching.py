import unittest

from sourcing_agent.confidence_policy import _family_relevance_weight
from sourcing_agent.domain import JobRequest
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.request_matching import (
    build_request_matching_bundle,
    matching_bundle_payload,
    matching_request_family_signature,
    matching_request_signature,
    request_family_score,
    request_family_signature,
    request_signature,
)
from sourcing_agent.storage import ControlPlaneStore


def _explicit_cohort_request(
    *,
    roles: list[str] | None = None,
    statuses: list[str] | None = None,
    role_match: str = "any",
) -> dict[str, object]:
    return {
        "target_company": "Acme",
        "cohort_selection": {
            "schema_version": "cohort_selection.v1",
            "role_bucket_ids": list(roles if roles is not None else ["research"]),
            "employment_statuses": list(statuses if statuses is not None else ["current"]),
            "role_match": role_match,
            "source": "user_explicit",
        },
    }


class RequestMatchingTest(unittest.TestCase):
    def test_family_signature_ignores_runtime_limits(self) -> None:
        left = {
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["RL"],
            "top_k": 5,
            "semantic_rerank_limit": 8,
        }
        right = {
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["RL"],
            "top_k": 20,
            "semantic_rerank_limit": 20,
        }
        self.assertNotEqual(request_signature(left), request_signature(right))
        self.assertEqual(request_family_signature(left), request_family_signature(right))
        score = request_family_score(left, right)
        self.assertTrue(score["exact_family_match"])
        self.assertFalse(score["exact_request_match"])

    def test_family_score_penalizes_different_query_family(self) -> None:
        left = {
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["RL"],
        }
        right = {
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["inference"],
        }
        score = request_family_score(left, right)
        self.assertFalse(score["exact_family_match"])
        self.assertLess(score["score"], 50.0)

    def test_family_signature_distinguishes_asset_view(self) -> None:
        left = {
            "target_company": "xAI",
            "asset_view": "canonical_merged",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["systems"],
        }
        right = {
            "target_company": "xAI",
            "asset_view": "strict_roster_only",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["systems"],
        }
        self.assertNotEqual(request_signature(left), request_signature(right))
        self.assertNotEqual(request_family_signature(left), request_family_signature(right))
        score = request_family_score(left, right)
        self.assertFalse(score["exact_family_match"])
        self.assertLess(score["score"], 70.0)

    def test_family_signature_normalizes_must_have_facet_aliases(self) -> None:
        left = {
            "target_company": "xAI",
            "must_have_facet": "multimodality",
            "categories": ["employee"],
        }
        right = {
            "target_company": "xAI",
            "must_have_facets": ["multimodal"],
            "categories": ["employee"],
        }
        self.assertEqual(request_signature(left), request_signature(right))
        self.assertEqual(request_family_signature(left), request_family_signature(right))

    def test_family_signature_normalizes_explicit_primary_role_bucket_aliases(self) -> None:
        left = {
            "target_company": "xAI",
            "must_have_primary_role_bucket": "infrastructure engineer",
            "categories": ["employee"],
        }
        right = {
            "target_company": "xAI",
            "must_have_primary_role_buckets": ["infra_systems"],
            "categories": ["employee"],
        }
        self.assertEqual(request_signature(left), request_signature(right))
        self.assertEqual(request_family_signature(left), request_family_signature(right))

    def test_family_signature_does_not_treat_infra_theme_as_role_bucket(self) -> None:
        left = {
            "target_company": "xAI",
            "keywords": ["Infra"],
            "categories": ["employee"],
        }
        right = {
            "target_company": "xAI",
            "must_have_primary_role_buckets": ["infra_systems"],
            "categories": ["employee"],
        }
        self.assertNotEqual(request_signature(left), request_signature(right))

    def test_matching_signature_uses_effective_request_normalization(self) -> None:
        raw_query_payload = {
            "raw_user_request": "我想找 Google Gemini 的产品经理",
            "target_company": "Google",
        }
        structured_payload = {
            "target_company": "Google",
            "organization_keywords": ["Gemini", "Google DeepMind"],
            "keywords": ["Gemini"],
            "must_have_primary_role_buckets": ["product_management"],
        }

        self.assertNotEqual(request_signature(raw_query_payload), request_signature(structured_payload))
        self.assertEqual(
            matching_request_signature(raw_query_payload),
            matching_request_signature(structured_payload),
        )
        self.assertEqual(
            matching_request_family_signature(raw_query_payload),
            matching_request_family_signature(structured_payload),
        )

        bundle = build_request_matching_bundle(raw_query_payload)
        self.assertEqual(bundle["matching_family_request"]["target_company"], "google")
        self.assertIn("gemini", bundle["matching_family_request"]["organization_keywords"])
        self.assertIn("product_management", bundle["matching_family_request"]["must_have_primary_role_buckets"])

    def test_explicit_cohort_identity_is_a_hard_family_fence(self) -> None:
        request = _explicit_cohort_request()
        mismatches = {
            "role_match": _explicit_cohort_request(role_match="all"),
            "roles": _explicit_cohort_request(roles=["engineering"]),
            "statuses": _explicit_cohort_request(statuses=["former"]),
            "legacy": {
                "target_company": "Acme",
                "must_have_primary_role_buckets": ["research"],
                "employment_statuses": ["current"],
            },
        }

        for name, candidate in mismatches.items():
            with self.subTest(name=name):
                match = request_family_score(request, candidate)
                self.assertEqual(match["score"], 0.0)
                self.assertTrue(match["hard_family_mismatch"])
                self.assertEqual(match["reasons"], ["cohort_selection_identity_mismatch"])
                self.assertIn("cohort_selection_digest", match["explanation"]["mismatched_fields"])

        exact = request_family_score(request, dict(request))
        self.assertNotIn("hard_family_mismatch", exact)
        self.assertTrue(exact["exact_request_match"])

    def test_stale_matching_bundle_cannot_erase_explicit_cohort_identity(self) -> None:
        request = _explicit_cohort_request()
        stale_bundle = build_request_matching_bundle(
            {
                "target_company": "Acme",
                "must_have_primary_role_buckets": ["research"],
                "employment_statuses": ["current"],
            }
        )

        match = request_family_score(
            request,
            dict(request),
            left_bundle=stale_bundle,
            right_bundle=build_request_matching_bundle(request),
        )

        self.assertTrue(match["exact_request_match"])
        self.assertNotIn("hard_family_mismatch", match)

    def test_malformed_persisted_cohort_is_a_stable_hard_mismatch(self) -> None:
        valid = _explicit_cohort_request()
        malformed = {
            "target_company": "Acme",
            "cohort_selection": {},
        }

        for name, left, right in (
            ("malformed_source", valid, malformed),
            ("malformed_current", malformed, valid),
        ):
            with self.subTest(name=name):
                match = request_family_score(left, right)
                self.assertEqual(match["score"], 0.0)
                self.assertTrue(match["hard_family_mismatch"])
                self.assertEqual(match["reasons"], ["cohort_selection_invalid"])
                self.assertIn("cohort_selection_digest", match["explanation"]["mismatched_fields"])

    def test_legacy_family_score_shape_does_not_gain_cohort_fence_fields(self) -> None:
        left = {
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["RL"],
        }
        right = {**left, "top_k": 20}

        match = request_family_score(left, right)

        self.assertNotIn("hard_family_mismatch", match)
        self.assertNotIn("hard_family_mismatch", match["explanation"])
        self.assertFalse(
            any(detail.get("field") == "cohort_selection_digest" for detail in match["explanation"]["field_details"])
        )

    def test_completed_job_fallback_cannot_cross_explicit_cohort_identity(self) -> None:
        request = {
            **_explicit_cohort_request(roles=[]),
            "asset_view": "canonical_merged",
            "categories": ["employee"],
            "keywords": ["pretraining"],
        }
        same_cohort_low_score = {
            **_explicit_cohort_request(roles=[]),
            "asset_view": "strict_roster_only",
            "categories": ["investor"],
            "keywords": ["sales"],
        }
        legacy_fallback = {
            "job_id": "legacy-fallback",
            "job_type": "workflow",
            "request": {
                "target_company": "Acme",
                "employment_statuses": ["current"],
            },
        }
        row = {
            "job_id": "same-cohort-low-score",
            "job_type": "workflow",
            "request": same_cohort_low_score,
            "request_matching": build_request_matching_bundle(same_cohort_low_score),
            "updated_at": "2026-07-15T00:00:00+00:00",
            "created_at": "2026-07-15T00:00:00+00:00",
        }
        store = object.__new__(ControlPlaneStore)
        store._select_control_plane_job_rows = lambda **_kwargs: [row]
        store.find_latest_completed_job = lambda **_kwargs: dict(legacy_fallback)

        selected = ControlPlaneStore.find_best_completed_job_match(
            store,
            target_company="Acme",
            request_payload=request,
        )

        self.assertIsNone(selected)


def _location_request(
    locations: list[str] | None = None,
    *,
    exclude: list[str] | None = None,
    cohort: bool = False,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "target_company": "Acme",
        "categories": ["employee"],
        "employment_statuses": ["current"],
        "keywords": ["rl"],
    }
    if locations is not None:
        payload["target_locations"] = list(locations)
    if exclude is not None:
        payload["exclude_target_locations"] = list(exclude)
    if cohort:
        payload["cohort_selection"] = {
            "schema_version": "cohort_selection.v1",
            "role_bucket_ids": ["research"],
            "employment_statuses": ["current"],
            "role_match": "any",
            "source": "user_explicit",
        }
    return JobRequest.from_payload(payload).to_record()


class LocationHardFamilyFenceTest(unittest.TestCase):
    """FT1-FF (finding 1): location is a hard request-family boundary."""

    def test_location_mismatch_scores_zero_with_hard_family_mismatch(self) -> None:
        for name, left, right in (
            ("target_values", _location_request(["United States"]), _location_request(["Germany"])),
            ("absent_vs_explicit_empty", _location_request(), _location_request([])),
            ("explicit_empty_vs_absent", _location_request([]), _location_request()),
            ("exclusion_values", _location_request(exclude=["France"]), _location_request(exclude=["Germany"])),
            ("exclusion_absent_vs_empty", _location_request(), _location_request(exclude=[])),
            (
                "cohort_same_digest_different_location",
                _location_request(["United States"], cohort=True),
                _location_request(["Germany"], cohort=True),
            ),
        ):
            with self.subTest(name=name):
                match = request_family_score(left, right)
                self.assertEqual(match["score"], 0.0)
                self.assertTrue(match["hard_family_mismatch"])
                self.assertEqual(match["reasons"], ["location_identity_mismatch"])
                self.assertFalse(match["exact_request_match"])
                self.assertFalse(match["exact_family_match"])

    def test_identical_location_identity_still_matches_exactly(self) -> None:
        for name, left in (
            ("present", _location_request(["United States"])),
            ("absent", _location_request()),
            ("explicit_empty", _location_request([])),
            ("with_exclusion", _location_request(["United States"], exclude=["France"])),
        ):
            with self.subTest(name=name):
                match = request_family_score(left, dict(left))
                self.assertTrue(match["exact_request_match"])
                self.assertNotIn("hard_family_mismatch", match)

    def test_otherwise_identical_high_similarity_cannot_exceed_threshold_across_locations(self) -> None:
        left = _location_request(["United States"])
        right = _location_request(["Germany"])
        # Sanity: without the location fields the payloads are identical.
        self.assertEqual(
            {key: value for key, value in left.items() if not key.endswith("locations")},
            {key: value for key, value in right.items() if not key.endswith("locations")},
        )
        match = request_family_score(left, right)
        self.assertEqual(match["score"], 0.0)
        self.assertTrue(match["hard_family_mismatch"])

    def test_explanation_marks_location_as_hard_identity_fields(self) -> None:
        match = request_family_score(_location_request(["United States"]), _location_request(["Germany"]))
        explanation = match["explanation"]
        self.assertIn("target_locations", explanation["mismatched_fields"])
        location_details = [detail for detail in explanation["field_details"] if detail["field"] == "target_locations"]
        self.assertEqual(len(location_details), 1)
        self.assertEqual(location_details[0]["kind"], "hard_identity")
        self.assertEqual(location_details[0]["status"], "hard_mismatch")
        self.assertTrue(location_details[0]["left_present"])
        self.assertTrue(location_details[0]["right_present"])

    def test_automatic_baseline_selection_cannot_cross_location_identity(self) -> None:
        request = _location_request(["United States"])
        mismatched_request = _location_request(["Germany"])
        row = {
            "job_id": "germany-job",
            "job_type": "workflow",
            "request": mismatched_request,
            "request_matching": build_request_matching_bundle(mismatched_request),
            "updated_at": "2026-07-15T00:00:00+00:00",
            "created_at": "2026-07-15T00:00:00+00:00",
        }
        store = object.__new__(ControlPlaneStore)
        store._select_control_plane_job_rows = lambda **_kwargs: [row]
        store.find_latest_completed_job = lambda **_kwargs: None

        selected = ControlPlaneStore.find_best_completed_job_match(
            store,
            target_company="Acme",
            request_payload=request,
        )
        self.assertIsNone(selected)

        # Control: a same-location candidate IS selected.
        matched_request = _location_request(["United States"])
        matched_row = {
            **row,
            "job_id": "us-job",
            "request": matched_request,
            "request_matching": build_request_matching_bundle(matched_request),
        }
        store._select_control_plane_job_rows = lambda **_kwargs: [matched_row]
        selected = ControlPlaneStore.find_best_completed_job_match(
            store,
            target_company="Acme",
            request_payload=request,
        )
        self.assertIsNotNone(selected)
        self.assertEqual(selected["job_id"], "us-job")

    def test_latest_company_fallback_cannot_cross_location_identity(self) -> None:
        request = _location_request(["United States"], exclude=["France"])
        # Different asset_view keeps the similarity below MATCH_THRESHOLD so the
        # latest-company fallback path engages.
        fallback_request = {
            **_location_request(["Germany"]),
            "asset_view": "strict_roster_only",
            "categories": ["investor"],
            "keywords": ["sales"],
        }
        fallback_job = {
            "job_id": "fallback-germany",
            "job_type": "workflow",
            "request": fallback_request,
            "updated_at": "2026-07-15T00:00:00+00:00",
            "created_at": "2026-07-15T00:00:00+00:00",
        }
        store = object.__new__(ControlPlaneStore)
        store._select_control_plane_job_rows = lambda **_kwargs: [fallback_job]
        store.find_latest_completed_job = lambda **_kwargs: dict(fallback_job)

        selected = ControlPlaneStore.find_best_completed_job_match(
            store,
            target_company="Acme",
            request_payload=request,
        )
        self.assertIsNone(selected)

    def test_snapshot_reuse_cannot_cross_location_identity(self) -> None:
        orchestrator = object.__new__(SourcingOrchestrator)
        mismatched_request = _location_request(["Germany"])
        candidate_job = {
            "job_id": "germany-snapshot",
            "job_type": "workflow",
            "request": mismatched_request,
            "request_matching": build_request_matching_bundle(mismatched_request),
            "updated_at": "2026-07-15T00:00:00+00:00",
            "created_at": "2026-07-15T00:00:00+00:00",
        }
        orchestrator.store = type(
            "_Store",
            (),
            {"list_jobs": lambda self, **_kwargs: [candidate_job]},
        )()
        orchestrator._load_snapshot_reuse_context_from_job = lambda _job, **_kwargs: {
            "snapshot_id": "snap-1",
            "snapshot_dir": "/tmp/snap-1",
            "source_path": "/tmp/snap-1/snapshot.json",
        }
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["rl"],
                "target_locations": ["United States"],
            }
        )
        match = orchestrator._resolve_snapshot_reuse_job_match(request, {"scope": "global"})
        self.assertEqual(match, {})

        # Control: the same-location snapshot candidate IS reused.
        matched_request = _location_request(["United States"])
        candidate_job["request"] = matched_request
        candidate_job["request_matching"] = build_request_matching_bundle(matched_request)
        match = orchestrator._resolve_snapshot_reuse_job_match(request, {"scope": "global"})
        self.assertEqual(match.get("strategy"), "reuse_snapshot")
        self.assertEqual(match.get("matched_snapshot_id"), "snap-1")

    def test_feedback_reuse_cannot_cross_location_identity(self) -> None:
        request = _location_request(["United States"])
        feedback_request = _location_request(["Germany"])
        matching_bundle = build_request_matching_bundle(request)
        item = {"metadata": {"request_payload": feedback_request}}
        weight, reason, family = _family_relevance_weight(
            item,
            request_payload=request,
            request_sig=request_signature(request),
            request_family_sig=request_family_signature(request),
            matching_request_sig=str(matching_bundle.get("matching_request_signature") or ""),
            matching_request_family_sig=str(matching_bundle.get("matching_request_family_signature") or ""),
        )
        self.assertEqual(weight, 0.0)
        self.assertEqual(family, "mismatch")
        self.assertTrue(reason.startswith("family_mismatch="))

        same_item = {"metadata": {"request_payload": _location_request(["United States"])}}
        weight, reason, family = _family_relevance_weight(
            same_item,
            request_payload=request,
            request_sig=request_signature(request),
            request_family_sig=request_family_signature(request),
            matching_request_sig=str(matching_bundle.get("matching_request_signature") or ""),
            matching_request_family_sig=str(matching_bundle.get("matching_request_family_signature") or ""),
        )
        self.assertEqual(weight, 1.0)
        self.assertEqual(family, "exact_family")


class StaleMatchingBundleTest(unittest.TestCase):
    """FT1-FF (finding 3): persisted bundles are trusted only on full canonical match."""

    def test_stale_bundle_missing_location_is_rebuilt(self) -> None:
        request = _location_request(["United States"])
        stale_bundle = build_request_matching_bundle(_location_request())
        rebuilt = matching_bundle_payload(
            request,
            execution_bundle_payload={"request_matching": stale_bundle},
        )
        self.assertEqual(rebuilt["matching_request"].get("target_locations"), ["united states"])
        self.assertNotEqual(
            rebuilt["matching_request_signature"],
            stale_bundle["matching_request_signature"],
        )

    def test_stale_bundle_with_changed_location_is_rebuilt(self) -> None:
        request = _location_request(["United States"])
        stale_bundle = build_request_matching_bundle(_location_request(["Germany"]))
        rebuilt = matching_bundle_payload(
            request,
            execution_bundle_payload={"request_matching": stale_bundle},
        )
        self.assertEqual(rebuilt["matching_request"].get("target_locations"), ["united states"])

    def test_stale_bundle_absent_versus_empty_is_rebuilt(self) -> None:
        request = _location_request([])
        stale_bundle = build_request_matching_bundle(_location_request())
        rebuilt = matching_bundle_payload(
            request,
            execution_bundle_payload={"request_matching": stale_bundle},
        )
        self.assertEqual(rebuilt["matching_request"].get("target_locations"), [])

    def test_stale_bundle_with_exclusion_drift_is_rebuilt(self) -> None:
        request = _location_request(["United States"], exclude=["France"])
        stale_bundle = build_request_matching_bundle(_location_request(["United States"], exclude=["Germany"]))
        rebuilt = matching_bundle_payload(
            request,
            execution_bundle_payload={"request_matching": stale_bundle},
        )
        self.assertEqual(rebuilt["matching_request"].get("exclude_target_locations"), ["france"])

    def test_signature_mismatch_is_rebuilt_even_when_payloads_match(self) -> None:
        request = _location_request(["United States"])
        canonical = build_request_matching_bundle(request)
        tampered = {**canonical, "matching_request_signature": "0" * 16}
        rebuilt = matching_bundle_payload(
            request,
            execution_bundle_payload={"request_matching": tampered},
        )
        self.assertEqual(rebuilt["matching_request_signature"], canonical["matching_request_signature"])

    def test_matching_persisted_bundle_is_trusted_and_signatures_backfilled(self) -> None:
        request = _location_request(["United States"])
        canonical = build_request_matching_bundle(request)
        persisted = {
            "matching_request": dict(canonical["matching_request"]),
            "matching_family_request": dict(canonical["matching_family_request"]),
        }
        trusted = matching_bundle_payload(
            request,
            execution_bundle_payload={"request_matching": persisted},
        )
        self.assertEqual(trusted["matching_request"], canonical["matching_request"])
        self.assertEqual(trusted["matching_request_signature"], canonical["matching_request_signature"])
        self.assertEqual(
            trusted["matching_request_family_signature"],
            canonical["matching_request_family_signature"],
        )

    def test_stale_bundle_cannot_produce_exact_match_across_location_identity(self) -> None:
        # Persisted bundle claims United States while the raw request payload
        # says Germany: trusting the bundle would fabricate an exact match and
        # reuse the wrong snapshot family.
        left = _location_request(["United States"])
        right_raw = _location_request(["Germany"])
        stale_right = build_request_matching_bundle(_location_request(["United States"]))
        match = request_family_score(
            left,
            right_raw,
            left_bundle=build_request_matching_bundle(left),
            right_bundle=stale_right,
        )
        self.assertEqual(match["score"], 0.0)
        self.assertTrue(match["hard_family_mismatch"])
        self.assertEqual(match["reasons"], ["location_identity_mismatch"])

        # Control: stale bundle that merely OMITS location is rebuilt from the
        # raw payload, so a genuinely identical request still matches exactly.
        stale_missing = build_request_matching_bundle(_location_request())
        match = request_family_score(
            left,
            dict(left),
            left_bundle=build_request_matching_bundle(left),
            right_bundle=stale_missing,
        )
        self.assertTrue(match["exact_request_match"])


class LocationPresenceSemanticsTest(unittest.TestCase):
    """FT1-FF (finding 4, matching side): present JSON null is not field absence."""

    def test_present_null_raises_in_signature_builders(self) -> None:
        from sourcing_agent.cohort_selection import CohortSelectionValidationError

        for field in ("target_locations", "exclude_target_locations"):
            with self.subTest(field=field):
                with self.assertRaises(CohortSelectionValidationError) as captured:
                    request_signature({"target_company": "Acme", field: None})
                self.assertEqual(captured.exception.code, "request_location_invalid_type")
                with self.assertRaises(CohortSelectionValidationError):
                    build_request_matching_bundle({"target_company": "Acme", field: None})

    def test_present_null_is_a_stable_hard_mismatch_in_family_scoring(self) -> None:
        for field in ("target_locations", "exclude_target_locations"):
            with self.subTest(field=field):
                match = request_family_score(
                    {"target_company": "Acme", field: None},
                    _location_request(["United States"]),
                )
                self.assertEqual(match["score"], 0.0)
                self.assertTrue(match["hard_family_mismatch"])
                self.assertEqual(match["reasons"], ["request_location_invalid_type"])


class HardIdentityReuseGuardTest(unittest.TestCase):
    """FT1-FF2 (finding 1): one presence-aware reuse guard covers Cohort digest
    plus BOTH location sibling fields, for Cohort and legacy requests alike."""

    def test_cohort_requests_require_matching_cohort_and_location_identity(self) -> None:
        from sourcing_agent.request_matching import source_request_matches_hard_identity

        request = _location_request(["United States"], exclude=["France"], cohort=True)
        for name, source, expected in (
            ("same", _location_request(["United States"], exclude=["France"], cohort=True), True),
            ("different_region", _location_request(["Germany"], exclude=["France"], cohort=True), False),
            ("different_exclusion", _location_request(["United States"], exclude=["Germany"], cohort=True), False),
            ("absent_vs_explicit_empty", _location_request([], exclude=["France"], cohort=True), False),
            ("missing_location", _location_request(cohort=True), False),
            ("legacy_source", _location_request(["United States"], exclude=["France"]), False),
            ("missing_source", None, False),
            ("non_dict_source", "invalid", False),
        ):
            with self.subTest(name=name):
                self.assertIs(
                    source_request_matches_hard_identity(request, source),
                    expected,
                )

    def test_legacy_requests_never_reuse_location_carrying_sources(self) -> None:
        from sourcing_agent.request_matching import source_request_matches_hard_identity

        for name, request, source, expected in (
            ("fully_legacy_pair", _location_request(), _location_request(), True),
            ("legacy_missing_source", _location_request(), None, True),
            ("legacy_vs_located_source", _location_request(), _location_request(["Germany"]), False),
            (
                "legacy_vs_explicit_empty_source",
                _location_request(),
                _location_request([], exclude=[]),
                False,
            ),
            ("located_vs_absent_source", _location_request(["United States"]), _location_request(), False),
            ("located_vs_missing_source", _location_request(["United States"]), None, False),
            ("explicit_empty_vs_absent", _location_request([]), _location_request(), False),
            ("same_explicit_empty", _location_request([]), _location_request([]), True),
            (
                "same_values_case_and_order_insensitive",
                _location_request(["United States", "Germany"]),
                _location_request(["germany", "united states"]),
                True,
            ),
        ):
            with self.subTest(name=name):
                self.assertIs(
                    source_request_matches_hard_identity(request, source),
                    expected,
                )

    def test_malformed_location_values_fail_closed_without_raising(self) -> None:
        from sourcing_agent.request_matching import source_request_matches_hard_identity

        for name, request, source in (
            ("null_on_request", {**_location_request(), "target_locations": None}, _location_request()),
            ("null_on_source", _location_request(), {**_location_request(), "target_locations": None}),
            (
                "string_on_source",
                _location_request(["Germany"]),
                {**_location_request(), "target_locations": "Germany"},
            ),
            ("integer_item", _location_request(), {**_location_request(), "target_locations": [5]}),
        ):
            with self.subTest(name=name):
                self.assertIs(source_request_matches_hard_identity(request, source), False)


class LocationSignatureClosedValidationTest(unittest.TestCase):
    """FT1-FF2 (finding 2): matching closed-validates non-null malformed
    location values with the same ingress semantics as JobRequest."""

    def test_malformed_values_raise_stable_location_errors_in_signature_builders(self) -> None:
        from sourcing_agent.cohort_selection import CohortSelectionValidationError

        cases = (
            ("bare_string", "Germany", "request_location_invalid_type"),
            ("mapping", {"region": "Germany"}, "request_location_invalid_type"),
            ("number", 5, "request_location_invalid_type"),
            ("null_item", [None, "Germany"], "request_location_invalid_item"),
            ("non_string_item", [5], "request_location_invalid_item"),
            ("blank_item", ["   "], "request_location_item_length_invalid"),
            ("too_many_items", [f"loc-{index}" for index in range(17)], "request_location_too_many_items"),
        )
        for field in ("target_locations", "exclude_target_locations"):
            for name, value, expected_code in cases:
                with self.subTest(field=field, case=name):
                    with self.assertRaises(CohortSelectionValidationError) as captured:
                        request_signature({"target_company": "Acme", field: value})
                    self.assertEqual(captured.exception.code, expected_code)
                    with self.assertRaises(CohortSelectionValidationError):
                        build_request_matching_bundle({"target_company": "Acme", field: value})

    def test_malformed_values_are_stable_hard_family_mismatches_not_exceptions(self) -> None:
        for name, value, expected_reason in (
            ("bare_string", "Germany", "request_location_invalid_type"),
            ("null_item", [None, "Germany"], "request_location_invalid_item"),
            ("number", 5, "request_location_invalid_type"),
        ):
            with self.subTest(name=name):
                match = request_family_score(
                    {"target_company": "Acme", "target_locations": value},
                    _location_request(["United States"]),
                )
                self.assertEqual(match["score"], 0.0)
                self.assertTrue(match["hard_family_mismatch"])
                self.assertEqual(match["reasons"], [expected_reason])

    def test_well_formed_payloads_keep_byte_identical_signatures(self) -> None:
        # Canonical signature identity is unchanged for valid values:
        # trimmed/lowercased/deduped/sorted, absent stays absent, [] stays [].
        self.assertEqual(
            request_signature(_location_request(["United States", "Germany"])),
            request_signature(_location_request(["germany", "united states", "Germany"])),
        )
        self.assertEqual(
            request_family_signature(_location_request([])),
            request_family_signature(_location_request([])),
        )
        self.assertNotEqual(
            request_signature(_location_request()),
            request_signature(_location_request([])),
        )

    def test_malformed_persisted_bundle_rebuilds_or_mismatches_closed(self) -> None:
        from sourcing_agent.cohort_selection import CohortSelectionValidationError

        # A persisted bundle computed from a bare-string location can never be
        # trusted: canonical regeneration now fails closed, so the malformed
        # identity can never alias a valid family.
        malformed_payload = {"target_company": "Acme", "target_locations": "Germany"}
        with self.assertRaises(CohortSelectionValidationError):
            matching_bundle_payload(
                malformed_payload,
                execution_bundle_payload={
                    "request_matching": {
                        "matching_request": {"target_company": "acme", "target_locations": ["germany"]},
                        "matching_family_request": {"target_company": "acme", "target_locations": ["germany"]},
                    }
                },
            )
