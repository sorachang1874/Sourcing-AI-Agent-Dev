from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from sourcing_agent.acquisition_strategy import compile_acquisition_strategy
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.cohort_provider_compiler import (
    COHORT_EXECUTION_NOT_READY,
    CohortExecutionCapability,
    CohortProviderCompilationError,
    CohortProviderCompiler,
    CohortProviderExecutionError,
    VerifiedCohortRoleProof,
    _sha256_json,
    resolve_effective_role_targeting,
)
from sourcing_agent.domain import JobRequest, RetrievalPlan
from sourcing_agent.harvest_connectors import (
    HarvestProfileSearchConnector,
    HarvestProfileSearchResultError,
    _get_harvest_dataset_items,
    _run_harvest_actor,
    parse_harvest_search_rows,
)
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.planning import build_sourcing_plan, hydrate_sourcing_plan
from sourcing_agent.settings import HarvestActorSettings


def _cohort(
    *,
    roles: list[str],
    statuses: list[str],
    role_match: str = "any",
) -> dict[str, object]:
    return {
        "schema_version": "cohort_selection.v1",
        "role_bucket_ids": roles,
        "employment_statuses": statuses,
        "role_match": role_match,
        "source": "user_explicit",
    }


def _request_payload(
    *,
    roles: list[str],
    statuses: list[str],
    role_match: str = "any",
) -> dict[str, object]:
    return {
        "raw_user_request": "Find product managers and engineers at Acme",
        "target_company": "Acme",
        "cohort_selection": _cohort(
            roles=roles,
            statuses=statuses,
            role_match=role_match,
        ),
    }


class CohortRoleAuthorityTest(unittest.TestCase):
    def test_explicit_roles_are_the_complete_authority_and_empty_means_all(self) -> None:
        resolved = resolve_effective_role_targeting(
            _request_payload(roles=["research"], statuses=["current"]),
            legacy_resolved_role_buckets=["product_management", "engineering"],
            legacy_function_target_groups=[{"role_bucket_id": "engineering"}],
        )
        all_roles = resolve_effective_role_targeting(
            _request_payload(roles=[], statuses=["current"]),
            legacy_resolved_role_buckets=["product_management", "engineering"],
            legacy_function_target_groups=[{"role_bucket_id": "engineering"}],
        )

        self.assertEqual(resolved["resolved_role_buckets"], ["research"])
        self.assertEqual(
            [group["role_bucket_id"] for group in resolved["function_target_groups"]],
            ["research"],
        )
        self.assertFalse(resolved["inference_allowed"])
        self.assertEqual(all_roles["resolved_role_buckets"], [])
        self.assertEqual(all_roles["function_target_groups"], [])
        self.assertTrue(all_roles["all_roles"])

    def test_acquisition_strategy_does_not_add_raw_category_or_facet_roles(self) -> None:
        request = JobRequest.from_payload(
            {
                **_request_payload(roles=["research"], statuses=["current"]),
                "categories": ["product_management", "engineering"],
                "must_have_facets": ["product_management", "infra_systems"],
            }
        )
        strategy = compile_acquisition_strategy(
            request,
            list(request.categories),
            list(request.employment_statuses),
            RetrievalPlan(strategy="hybrid", reason="test"),
        )

        targeting = strategy.strategy_decision_explanation["effective_role_targeting"]
        self.assertEqual(targeting["resolved_role_buckets"], ["research"])
        self.assertEqual(strategy.filter_hints.get("function_ids"), ["24"])
        self.assertNotIn("19", strategy.filter_hints.get("function_ids") or [])
        self.assertNotIn("8", strategy.filter_hints.get("function_ids") or [])

    def test_empty_explicit_roles_do_not_reinfer_provider_role_filters(self) -> None:
        request = JobRequest.from_payload(_request_payload(roles=[], statuses=["current"]))
        strategy = compile_acquisition_strategy(
            request,
            list(request.categories),
            list(request.employment_statuses),
            RetrievalPlan(strategy="hybrid", reason="test"),
        )

        self.assertEqual(
            strategy.strategy_decision_explanation["effective_role_targeting"]["resolved_role_buckets"],
            [],
        )
        self.assertNotIn("function_ids", strategy.filter_hints)
        self.assertNotIn("job_titles", strategy.filter_hints)


class CohortProviderCompilerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.compiler = CohortProviderCompiler()
        self.capability = CohortExecutionCapability(policy_revision="test.cohort-runtime.v1")
        self.base_filters = {
            "current_companies": ["Acme"],
            "function_ids": ["999"],
            "job_titles": ["Injected Role"],
            "keywords": ["Product Manager", "Pre-train"],
            "scope_keywords": ["Platform Engineering", "Foundation Models"],
        }

    def test_manifest_has_one_physical_lane_per_status_role_and_stable_payloads(self) -> None:
        manifest = self.compiler.compile(
            _request_payload(
                roles=["product_management", "research"],
                statuses=["former", "current"],
            ),
            base_filter_hints=self.base_filters,
            execution_capability=self.capability,
        )

        lanes = list(manifest["lanes"])
        self.assertEqual(
            [
                (
                    lane["employment_status"],
                    lane["role_bucket_id"],
                    lane["provider_payload"]["filter_hints"].get("function_ids"),
                )
                for lane in lanes
            ],
            [
                ("current", "research", ["24"]),
                ("current", "product_management", ["19"]),
                ("former", "research", ["24"]),
                ("former", "product_management", ["19"]),
            ],
        )
        self.assertTrue(manifest["execution_ready"])
        self.assertEqual(
            [lane["lane_id"] for lane in lanes],
            [
                "cohort_current_research_bf10eb4528d2",
                "cohort_current_product_management_bf10eb4528d2",
                "cohort_former_research_bf10eb4528d2",
                "cohort_former_product_management_bf10eb4528d2",
            ],
        )
        self.assertEqual(
            [lane["lane_digest"] for lane in lanes],
            [
                "f9e1ce4b7186cf81c856ecf661fa0feb81cbac5e6d50e2db618f6d0c7f332007",
                "608a3ab6e553fad8b89ad23c4cf3faa594ea81ff8990659f00a0ee43b915408a",
                "feec4093be91e77195551548a5191c356baf87c899d7a1bd81d26a035ea5e060",
                "61252aff9cbe1c372a41c18fd1fb1414e16b283f5528a525977015c95c831e2a",
            ],
        )
        self.assertEqual(
            manifest["manifest_digest"],
            "1e0a10c1c361d0198f3a0d3a9d65a48d11fad1d08e04985e33ee0759f43bdc07",
        )
        self.assertEqual(manifest["budget"]["lane_item_limits"], [7, 6, 6, 6])
        self.assertNotIn(
            "999",
            [
                function_id
                for lane in lanes
                for function_id in lane["provider_payload"]["filter_hints"].get("function_ids", [])
            ],
        )
        self.assertTrue(
            all(lane["provider_payload"]["filter_hints"].get("keywords") == ["Pre-train"] for lane in lanes)
        )
        self.assertTrue(
            all(
                lane["provider_payload"]["filter_hints"].get("scope_keywords") == ["Foundation Models"]
                for lane in lanes
            )
        )

    def test_manifest_digest_is_order_invariant_after_canonicalization(self) -> None:
        first = self.compiler.compile(
            _request_payload(
                roles=["product_management", "research"],
                statuses=["former", "current"],
            ),
            execution_capability=self.capability,
        )
        second = self.compiler.compile(
            _request_payload(
                roles=["research", "product_management"],
                statuses=["current", "former"],
            ),
            execution_capability=self.capability,
        )

        self.assertEqual(first["manifest_digest"], second["manifest_digest"])
        self.assertEqual(first["lanes"], second["lanes"])

    def test_manifest_exact_recompile_rejects_rehashed_lane_and_readiness_tampering(self) -> None:
        preview = self.compiler.compile(
            _request_payload(roles=["research"], statuses=["current"]),
        )
        tampered = copy.deepcopy(preview)
        tampered["execution_ready"] = True
        tampered["execution_blocker"] = ""
        tampered["lanes"][0]["provider_payload"]["filter_hints"]["function_ids"] = ["19"]
        lane_without_digest = dict(tampered["lanes"][0])
        lane_without_digest.pop("lane_digest")
        tampered["lanes"][0]["lane_digest"] = _sha256_json(lane_without_digest)
        manifest_without_digest = dict(tampered)
        manifest_without_digest.pop("manifest_digest")
        tampered["manifest_digest"] = _sha256_json(manifest_without_digest)

        with self.assertRaisesRegex(
            CohortProviderCompilationError,
            "cohort_provider_manifest_semantic_mismatch",
        ):
            self.compiler.assert_execution_ready(
                tampered,
                execution_capability=self.capability,
            )

    def test_manifest_requires_the_exact_execution_owner_capability(self) -> None:
        manifest = self.compiler.compile(
            _request_payload(roles=["research"], statuses=["current"]),
            execution_capability=self.capability,
        )
        other = CohortExecutionCapability(policy_revision="test.other-policy.v1")

        with self.assertRaisesRegex(
            CohortProviderCompilationError,
            "cohort_execution_capability_mismatch",
        ):
            self.compiler.assert_execution_ready(
                manifest,
                execution_capability=other,
            )

    def test_empty_roles_compile_status_only_lanes(self) -> None:
        manifest = self.compiler.compile(
            _request_payload(roles=[], statuses=["current", "former"]),
            base_filter_hints=self.base_filters,
            execution_capability=self.capability,
        )

        self.assertEqual(
            [(lane["employment_status"], lane["role_bucket_id"]) for lane in manifest["lanes"]],
            [("current", ""), ("former", "")],
        )
        for lane in manifest["lanes"]:
            filters = lane["provider_payload"]["filter_hints"]
            self.assertNotIn("function_ids", filters)
            self.assertNotIn("job_titles", filters)

    def test_any_combination_is_ordered_union_with_dedupe(self) -> None:
        manifest = self.compiler.compile(
            _request_payload(roles=["research", "engineering"], statuses=["current"]),
            execution_capability=self.capability,
        )
        first_lane, second_lane = manifest["lanes"]
        combined = self.compiler.combine_lane_results(
            manifest,
            {
                first_lane["lane_id"]: [
                    {"candidate_id": "a", "full_name": "A"},
                    {"candidate_id": "shared", "full_name": "Shared first"},
                ],
                second_lane["lane_id"]: [
                    {"candidate_id": "shared", "full_name": "Shared second"},
                    {"candidate_id": "b", "full_name": "B"},
                ],
            },
            execution_capability=self.capability,
        )

        self.assertEqual([row["candidate_id"] for row in combined["rows"]], ["a", "shared", "b"])
        self.assertEqual(combined["candidate_count"], 3)
        self.assertEqual(
            [membership["role_bucket_id"] for membership in combined["rows"][1]["cohort_lane_membership"]],
            ["research", "engineering"],
        )

    def test_any_combination_uses_one_canonical_profile_identity(self) -> None:
        manifest = self.compiler.compile(
            _request_payload(roles=["research", "engineering"], statuses=["current"]),
            execution_capability=self.capability,
        )
        first_lane, second_lane = manifest["lanes"]
        combined = self.compiler.combine_lane_results(
            manifest,
            {
                first_lane["lane_id"]: [
                    {
                        "candidate_id": "provider-a",
                        "profile_url": "https://www.linkedin.com/in/shared-person/",
                    }
                ],
                second_lane["lane_id"]: [
                    {
                        "candidate_id": "provider-b",
                        "linkedin_url": "https://linkedin.com/in/shared-person",
                    }
                ],
            },
            execution_capability=self.capability,
        )

        self.assertEqual(combined["candidate_count"], 1)
        self.assertEqual(combined["rows"][0]["candidate_id"], "provider-a")

    def test_harvest_public_identifier_only_uses_canonical_linkedin_identity(self) -> None:
        manifest = self.compiler.compile(
            _request_payload(roles=["research", "engineering"], statuses=["current"]),
            execution_capability=self.capability,
        )
        first_lane, second_lane = manifest["lanes"]
        url_row = parse_harvest_search_rows(
            [{"linkedinUrl": "https://www.linkedin.com/in/jane-doe/", "fullName": "Jane Doe"}]
        )
        identifier_row = parse_harvest_search_rows(
            [
                {
                    "url": "https://example.com/profile/jane",
                    "publicIdentifier": "jane-doe",
                    "fullName": "Jane Doe",
                }
            ]
        )

        combined = self.compiler.combine_lane_results(
            manifest,
            {
                first_lane["lane_id"]: url_row,
                second_lane["lane_id"]: identifier_row,
            },
            execution_capability=self.capability,
        )

        self.assertEqual(combined["candidate_count"], 1)
        self.assertEqual(len(combined["rows"][0]["cohort_lane_membership"]), 2)

        non_linkedin_only = parse_harvest_search_rows(
            [{"url": "https://example.com/profile/no-linkedin-identity", "fullName": "No Identity"}]
        )
        with self.assertRaisesRegex(
            CohortProviderExecutionError,
            "cohort_provider_candidate_identity_missing",
        ):
            self.compiler.combine_lane_results(
                manifest,
                {
                    first_lane["lane_id"]: non_linkedin_only,
                    second_lane["lane_id"]: [],
                },
                execution_capability=self.capability,
            )

    def test_any_combination_dedupes_across_statuses_and_preserves_membership(self) -> None:
        manifest = self.compiler.compile(
            _request_payload(roles=[], statuses=["current", "former"]),
            execution_capability=self.capability,
        )
        current_lane, former_lane = manifest["lanes"]
        combined = self.compiler.combine_lane_results(
            manifest,
            {
                current_lane["lane_id"]: [{"candidate_id": "shared", "full_name": "Current view"}],
                former_lane["lane_id"]: [{"candidate_id": "shared", "full_name": "Former view"}],
            },
            execution_capability=self.capability,
        )

        self.assertEqual(combined["candidate_count"], 1)
        self.assertEqual(
            [item["employment_status"] for item in combined["rows"][0]["cohort_lane_membership"]],
            ["current", "former"],
        )

    def test_combine_requires_exact_complete_lane_result_set_and_stable_identity(self) -> None:
        manifest = self.compiler.compile(
            _request_payload(roles=["research", "engineering"], statuses=["current"]),
            execution_capability=self.capability,
        )
        first_lane, second_lane = manifest["lanes"]
        with self.assertRaisesRegex(
            CohortProviderExecutionError,
            "cohort_provider_lane_results_incomplete",
        ):
            self.compiler.combine_lane_results(
                manifest,
                {first_lane["lane_id"]: []},
                execution_capability=self.capability,
            )
        with self.assertRaisesRegex(
            CohortProviderExecutionError,
            "cohort_provider_candidate_identity_missing",
        ):
            self.compiler.combine_lane_results(
                manifest,
                {
                    first_lane["lane_id"]: [{"full_name": "Identity-free"}],
                    second_lane["lane_id"]: [],
                },
                execution_capability=self.capability,
            )

    def test_harvest_boundary_dispatches_exact_single_role_payloads(self) -> None:
        manifest = self.compiler.compile(
            _request_payload(
                roles=["research", "product_management"],
                statuses=["current"],
            ),
            base_filter_hints=self.base_filters,
            execution_capability=self.capability,
        )
        captured: list[dict[str, Any]] = []

        class _FakeConnector(HarvestProfileSearchConnector):
            def search_profiles(self, **kwargs):
                captured.append(dict(kwargs))
                return {
                    "rows": [
                        {
                            "candidate_id": "shared",
                            "full_name": "Shared Candidate",
                        }
                    ]
                }

        connector = _FakeConnector(HarvestActorSettings())
        with tempfile.TemporaryDirectory() as tempdir:
            result = connector.search_profiles_for_cohort_manifest(
                manifest=manifest,
                execution_capability=self.capability,
                discovery_dir=Path(tempdir),
            )

        self.assertEqual(len(captured), 2)
        self.assertEqual([call["query_text"] for call in captured], ["", ""])
        self.assertEqual(
            [
                (
                    call["employment_status"],
                    call["filter_hints"]["function_ids"],
                    call["filter_hints"]["job_titles"],
                )
                for call in captured
            ],
            [
                (
                    "current",
                    ["24"],
                    ["Researcher", "Research Scientist", "Applied Scientist"],
                ),
                (
                    "current",
                    ["19"],
                    ["Product Manager", "Senior Product Manager", "Group Product Manager"],
                ),
            ],
        )
        self.assertEqual(result["candidate_count"], 1)
        self.assertEqual(len(result["lane_summaries"]), 2)
        self.assertEqual([call["limit"] for call in captured], [13, 12])
        self.assertEqual([call["pages"] for call in captured], [1, 1])
        self.assertEqual([call["dispatch_mode"] for call in captured], ["async_only", "async_only"])
        self.assertEqual(
            [summary["role_bucket_id"] for summary in result["lane_summaries"]],
            ["research", "product_management"],
        )

    def test_harvest_boundary_derives_exact_page_count_from_lane_budget(self) -> None:
        capability = CohortExecutionCapability(
            policy_revision="test.cohort-runtime.large-budget.v1",
            max_provider_items=50,
            max_output_candidates=50,
        )
        manifest = self.compiler.compile(
            _request_payload(roles=["research"], statuses=["current"]),
            execution_capability=capability,
            requested_result_limit=40,
        )
        captured: list[dict[str, Any]] = []

        class _FakeConnector(HarvestProfileSearchConnector):
            def search_profiles(self, **kwargs):
                captured.append(dict(kwargs))
                return {"rows": []}

        with tempfile.TemporaryDirectory() as tempdir:
            _FakeConnector(HarvestActorSettings()).search_profiles_for_cohort_manifest(
                manifest=manifest,
                execution_capability=capability,
                discovery_dir=Path(tempdir),
            )

        self.assertEqual([(call["limit"], call["pages"]) for call in captured], [(40, 2)])

    def test_cohort_async_only_dispatch_never_falls_back_to_a_second_submission(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="test", actor_id="actor")
        payload = {"maxItems": 13, "takePages": 1}

        with (
            patch("sourcing_agent.harvest_connectors._runtime_scoped_provider_mode", return_value="live"),
            patch("sourcing_agent.harvest_connectors._run_harvest_actor_sync_request") as sync_run,
            patch(
                "sourcing_agent.harvest_connectors._run_harvest_actor_via_async_dataset",
                return_value=[{"id": "candidate"}],
            ) as async_run,
        ):
            result = _run_harvest_actor(
                settings,
                payload,
                request_context={"harvest_dispatch_mode": "async_only"},
            )

        self.assertEqual(result, [{"id": "candidate"}])
        sync_run.assert_not_called()
        async_run.assert_called_once()

    def test_harvest_boundary_fails_stably_on_unavailable_partial_or_malformed_lane(self) -> None:
        manifest = self.compiler.compile(
            _request_payload(roles=["research", "engineering"], statuses=["current"]),
            execution_capability=self.capability,
        )

        class _PartialConnector(HarvestProfileSearchConnector):
            calls = 0

            def search_profiles(self, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    return {"rows": [{"candidate_id": "first"}]}
                return None

        connector = _PartialConnector(HarvestActorSettings())
        with tempfile.TemporaryDirectory() as tempdir:
            with self.assertRaises(CohortProviderExecutionError) as caught:
                connector.search_profiles_for_cohort_manifest(
                    manifest=manifest,
                    execution_capability=self.capability,
                    discovery_dir=Path(tempdir),
                )
        self.assertEqual(caught.exception.code, "cohort_provider_lane_result_unavailable")
        self.assertEqual(caught.exception.completed_lane_ids, (manifest["lanes"][0]["lane_id"],))

        class _MalformedConnector(HarvestProfileSearchConnector):
            def search_profiles(self, **kwargs):
                return {"rows": [None]}

        with tempfile.TemporaryDirectory() as tempdir:
            with self.assertRaisesRegex(
                CohortProviderExecutionError,
                "cohort_provider_lane_result_invalid",
            ):
                _MalformedConnector(HarvestActorSettings()).search_profiles_for_cohort_manifest(
                    manifest=manifest,
                    execution_capability=self.capability,
                    discovery_dir=Path(tempdir),
                )

        single_lane_manifest = self.compiler.compile(
            _request_payload(roles=["research"], statuses=["current"]),
            execution_capability=self.capability,
        )
        real_connector = HarvestProfileSearchConnector(
            HarvestActorSettings(enabled=True, api_token="test", actor_id="actor")
        )
        for malformed_body in ([None], [{"publicIdentifier": "valid-person"}, None]):
            with self.subTest(malformed_body=malformed_body), tempfile.TemporaryDirectory() as tempdir:
                with (
                    patch("sourcing_agent.harvest_connectors._harvest_connector_available", return_value=True),
                    patch("sourcing_agent.harvest_connectors._run_harvest_actor", return_value=malformed_body),
                    self.assertRaises(CohortProviderExecutionError) as malformed,
                ):
                    real_connector.search_profiles_for_cohort_manifest(
                        manifest=single_lane_manifest,
                        execution_capability=self.capability,
                        discovery_dir=Path(tempdir),
                        allow_shared_provider_cache=False,
                    )
                self.assertEqual(malformed.exception.code, "cohort_provider_lane_result_invalid")

        with tempfile.TemporaryDirectory() as tempdir:
            with (
                patch("sourcing_agent.harvest_connectors._harvest_connector_available", return_value=True),
                patch("sourcing_agent.harvest_connectors._run_harvest_actor", return_value=[]),
            ):
                empty = real_connector.search_profiles_for_cohort_manifest(
                    manifest=single_lane_manifest,
                    execution_capability=self.capability,
                    discovery_dir=Path(tempdir),
                    allow_shared_provider_cache=False,
                )
        self.assertEqual(empty["candidate_count"], 0)

    def test_strict_cohort_dataset_fetch_distinguishes_null_from_empty_page(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="test", actor_id="actor")
        with patch("sourcing_agent.harvest_connectors._get_harvest_dataset_items_page", return_value=None):
            with self.assertRaises(HarvestProfileSearchResultError):
                _get_harvest_dataset_items(
                    settings,
                    "dataset",
                    logical_name="harvest_profile_search",
                    request_context={"strict_harvest_profile_search_result_envelope": True},
                )
        with patch("sourcing_agent.harvest_connectors._get_harvest_dataset_items_page", return_value=[]):
            self.assertEqual(
                _get_harvest_dataset_items(
                    settings,
                    "dataset",
                    logical_name="harvest_profile_search",
                    request_context={"strict_harvest_profile_search_result_envelope": True},
                ),
                [],
            )

    def test_all_requires_predeclared_proof_before_provider_and_verified_postfilter(self) -> None:
        unready = self.compiler.compile(
            _request_payload(
                roles=["research", "engineering"],
                statuses=["current"],
                role_match="all",
            ),
            execution_capability=self.capability,
        )

        class _CountingConnector(HarvestProfileSearchConnector):
            calls = 0

            def search_profiles(self, **kwargs):
                self.calls += 1
                return {"rows": []}

        connector = _CountingConnector(HarvestActorSettings(enabled=False, api_token="", actor_id="actor"))
        with tempfile.TemporaryDirectory() as tempdir:
            not_integrated = self.compiler.compile(
                _request_payload(roles=["research"], statuses=["current"]),
            )
            with self.assertRaisesRegex(
                CohortProviderCompilationError,
                "cohort_selection_execution_not_ready",
            ):
                connector.search_profiles_for_cohort_manifest(
                    manifest=not_integrated,
                    execution_capability=None,
                    discovery_dir=Path(tempdir),
                )
            with self.assertRaisesRegex(
                CohortProviderCompilationError,
                "cohort_selection_all_role_proof_unavailable",
            ):
                connector.search_profiles_for_cohort_manifest(
                    manifest=unready,
                    execution_capability=self.capability,
                    discovery_dir=Path(tempdir),
                )
        self.assertEqual(connector.calls, 0)

        proof_capability = CohortExecutionCapability(
            policy_revision="test.cohort-runtime.v1",
            role_proof_verifier_id="test_role_verifier",
            role_proof_verifier_revision="v1",
        )
        ready = self.compiler.compile(
            _request_payload(
                roles=["research", "engineering"],
                statuses=["current"],
                role_match="all",
            ),
            execution_capability=proof_capability,
        )
        with tempfile.TemporaryDirectory() as tempdir:
            with self.assertRaisesRegex(
                CohortProviderCompilationError,
                "cohort_selection_all_role_proof_unavailable",
            ):
                connector.search_profiles_for_cohort_manifest(
                    manifest=ready,
                    execution_capability=proof_capability,
                    role_proof_verifier=None,
                    discovery_dir=Path(tempdir),
                )
        self.assertEqual(connector.calls, 0)

        class _NonCallableVerifier:
            verifier_id = "test_role_verifier"
            verifier_revision = "v1"

        with tempfile.TemporaryDirectory() as tempdir:
            with self.assertRaisesRegex(
                CohortProviderCompilationError,
                "cohort_selection_all_role_proof_unavailable",
            ):
                connector.search_profiles_for_cohort_manifest(
                    manifest=ready,
                    execution_capability=proof_capability,
                    role_proof_verifier=_NonCallableVerifier(),
                    discovery_dir=Path(tempdir),
                )
        self.assertEqual(connector.calls, 0)

        class _TestRoleVerifier:
            verifier_id = "test_role_verifier"
            verifier_revision = "v1"

            @staticmethod
            def verify(row):
                roles = tuple(str(item) for item in list(row.get("evidence_role_bucket_ids") or []))
                if not roles:
                    return None
                return VerifiedCohortRoleProof(
                    role_bucket_ids=roles,
                    evidence_digest=f"proof:{row.get('candidate_id')}:{','.join(roles)}",
                )

        research_lane, engineering_lane = ready["lanes"]
        combined = self.compiler.combine_lane_results(
            ready,
            {
                research_lane["lane_id"]: [
                    {
                        "candidate_id": "verified",
                        "evidence_role_bucket_ids": ["research"],
                    },
                    {
                        "candidate_id": "unverified",
                        "normalized_role_bucket_ids": ["research", "engineering"],
                    },
                ],
                engineering_lane["lane_id"]: [
                    {
                        "candidate_id": "verified",
                        "evidence_role_bucket_ids": ["engineering"],
                    },
                    {
                        "candidate_id": "unverified",
                        "normalized_role_bucket_ids": ["research", "engineering"],
                    },
                ],
            },
            execution_capability=proof_capability,
            role_proof_verifier=_TestRoleVerifier(),
        )

        self.assertEqual([row["candidate_id"] for row in combined["rows"]], ["verified"])
        self.assertEqual(
            combined["rows"][0]["normalized_role_bucket_ids"],
            ["research", "engineering"],
        )
        self.assertEqual(combined["rejected_unverified_count"], 1)
        self.assertEqual(combined["missing_required_lane_count"], 0)

        class _FailingRoleVerifier:
            verifier_id = "test_role_verifier"
            verifier_revision = "v1"

            @staticmethod
            def verify(_row):
                raise RuntimeError("synthetic verifier failure")

        with self.assertRaises(CohortProviderExecutionError) as verification_failure:
            self.compiler.combine_lane_results(
                ready,
                {
                    research_lane["lane_id"]: [{"candidate_id": "verified"}],
                    engineering_lane["lane_id"]: [{"candidate_id": "verified"}],
                },
                execution_capability=proof_capability,
                role_proof_verifier=_FailingRoleVerifier(),
            )
        self.assertEqual(verification_failure.exception.code, "cohort_role_proof_verification_failed")
        self.assertEqual(verification_failure.exception.detail, "RuntimeError")

        multi_status = self.compiler.compile(
            _request_payload(
                roles=["research", "engineering"],
                statuses=["current", "former"],
                role_match="all",
            ),
            execution_capability=proof_capability,
        )
        lane_results = {
            lane["lane_id"]: [
                {
                    "candidate_id": "verified",
                    "evidence_role_bucket_ids": [lane["role_bucket_id"]],
                }
            ]
            for lane in multi_status["lanes"]
        }
        multi_status_combined = self.compiler.combine_lane_results(
            multi_status,
            lane_results,
            execution_capability=proof_capability,
            role_proof_verifier=_TestRoleVerifier(),
        )

        self.assertEqual(multi_status_combined["candidate_count"], 1)
        self.assertEqual(len(multi_status_combined["rows"][0]["cohort_lane_membership"]), 4)
        self.assertEqual(
            {item["employment_status"] for item in multi_status_combined["rows"][0]["cohort_lane_membership"]},
            {"current", "former"},
        )

    def test_planning_round_trip_retains_compiler_lane_and_digest_semantics(self) -> None:
        request = JobRequest.from_payload(
            _request_payload(
                roles=["research", "product_management"],
                statuses=["current", "former"],
            )
        )
        plan = build_sourcing_plan(
            request,
            AssetCatalog.discover(),
            DeterministicModelClient(),
        )
        hydrated = hydrate_sourcing_plan(plan.to_record())
        manifest = plan.acquisition_strategy.provider_execution_manifest

        self.assertEqual(
            hydrated.acquisition_strategy.provider_execution_manifest,
            manifest,
        )
        self.assertEqual(manifest["execution_blocker"], COHORT_EXECUTION_NOT_READY)
        self.assertEqual(
            [(lane["employment_status"], lane["role_bucket_id"]) for lane in manifest["lanes"]],
            [
                ("current", "research"),
                ("current", "product_management"),
                ("former", "research"),
                ("former", "product_management"),
            ],
        )
        self.assertEqual(len(manifest["manifest_digest"]), 64)


if __name__ == "__main__":
    unittest.main()
