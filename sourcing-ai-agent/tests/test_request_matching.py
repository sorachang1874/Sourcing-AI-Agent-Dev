import unittest

from sourcing_agent.request_matching import (
    build_request_matching_bundle,
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
