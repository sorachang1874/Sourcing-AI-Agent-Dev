from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from sourcing_agent.acquisition import AcquisitionEngine, AcquisitionExecution
from sourcing_agent.acquisition_strategy import compile_acquisition_strategy
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.cohort_provider_compiler import (
    COHORT_CANONICAL_PROFILE_URL_FIELD,
    COHORT_EXECUTION_NOT_READY,
    COHORT_PUBLIC_HEADLINE_SOURCE,
    CohortExecutionCapability,
    CohortHeadlineRoleProofVerifier,
    CohortProviderCompilationError,
    CohortProviderCompiler,
    CohortProviderExecutionError,
    VerifiedCohortRoleProof,
    _sha256_json,
    cohort_execution_capability_for_runtime,
    cohort_execution_not_ready_result,
    resolve_effective_role_targeting,
)
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import AcquisitionTask, JobRequest, RetrievalPlan
from sourcing_agent.harvest_connectors import (
    HarvestProfileSearchConnector,
    HarvestProfileSearchResultError,
    _get_harvest_dataset_items,
    _run_harvest_actor,
    parse_harvest_search_rows,
)
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import _restore_search_seed_snapshot_from_snapshot_dir
from sourcing_agent.planning import build_sourcing_plan, hydrate_sourcing_plan
from sourcing_agent.runtime_environment import RuntimeEnvironment
from sourcing_agent.search_seed_registry import load_search_seed_snapshot_from_snapshot_dir
from sourcing_agent.settings import HarvestActorSettings
from sourcing_agent.snapshot_materializer import SnapshotMaterializer


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


def _write_isolated_runtime_contract(runtime_dir: Path) -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / ".scripted-local-postgres.env").write_text(
        "SOURCING_EXTERNAL_PROVIDER_MODE=scripted\nSOURCING_RUNTIME_ENVIRONMENT=scripted\n",
        encoding="utf-8",
    )


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
        runtime_home = tempfile.TemporaryDirectory()
        self.addCleanup(runtime_home.cleanup)
        self.runtime_dir = Path(runtime_home.name)
        _write_isolated_runtime_contract(self.runtime_dir)
        self.capability = CohortExecutionCapability(
            policy_revision="test.cohort-runtime.v1",
            provider_mode="scripted",
            runtime_namespace=str(self.runtime_dir),
        )
        self.base_filters = {
            "current_companies": ["Acme"],
            "function_ids": ["999"],
            "job_titles": ["Injected Role"],
            "keywords": ["Product Manager", "Pre-train"],
            "scope_keywords": ["Platform Engineering", "Foundation Models"],
        }

    def test_runtime_capability_is_server_owned_and_non_live_only(self) -> None:
        scripted_runtime = RuntimeEnvironment(
            name="scripted",
            provider_mode="scripted",
            runtime_dir=self.runtime_dir,
        )
        live_runtime = RuntimeEnvironment(
            name="production",
            provider_mode="live",
            runtime_dir=Path("/tmp/live-runtime"),
        )
        payload = _request_payload(roles=["research"], statuses=["current"])

        with patch(
            "sourcing_agent.cohort_provider_compiler.current_runtime_environment",
            return_value=scripted_runtime,
        ):
            capability = cohort_execution_capability_for_runtime(runtime_dir=self.runtime_dir)
            gate = cohort_execution_not_ready_result(
                payload,
                runtime_dir=self.runtime_dir,
            )

        self.assertIsNotNone(capability)
        assert capability is not None
        self.assertEqual(capability.owner, "cohort_runtime")
        self.assertEqual(
            capability.policy_revision,
            "cohort_non_live_runtime.v1:scripted",
        )
        self.assertIsNone(gate)

        with patch(
            "sourcing_agent.cohort_provider_compiler.current_runtime_environment",
            return_value=live_runtime,
        ):
            self.assertIsNone(cohort_execution_capability_for_runtime(runtime_dir="/tmp/live-runtime"))
            live_gate = cohort_execution_not_ready_result(
                payload,
                runtime_dir="/tmp/live-runtime",
            )

        self.assertEqual(live_gate["reason"], "cohort_selection_execution_not_ready")
        self.assertFalse(live_gate["cohort_provider_manifest"]["execution_ready"])

    def test_non_live_runtime_installs_exact_all_role_proof_owner(self) -> None:
        scripted_runtime = RuntimeEnvironment(
            name="scripted",
            provider_mode="scripted",
            runtime_dir=self.runtime_dir,
        )
        with patch(
            "sourcing_agent.cohort_provider_compiler.current_runtime_environment",
            return_value=scripted_runtime,
        ):
            capability = cohort_execution_capability_for_runtime(runtime_dir=self.runtime_dir)
            gate = cohort_execution_not_ready_result(
                _request_payload(
                    roles=["research", "engineering"],
                    statuses=["current"],
                    role_match="all",
                ),
                runtime_dir=self.runtime_dir,
            )

        self.assertIsNone(gate)
        assert capability is not None
        self.assertEqual(capability.role_proof_verifier_id, CohortHeadlineRoleProofVerifier.verifier_id)
        self.assertEqual(
            capability.role_proof_verifier_revision,
            CohortHeadlineRoleProofVerifier.verifier_revision,
        )
        proof = CohortHeadlineRoleProofVerifier().verify(
            {
                "public_headline": "Research Scientist and Software Engineer",
                "public_headline_source": COHORT_PUBLIC_HEADLINE_SOURCE,
            }
        )
        self.assertIsNotNone(proof)
        assert proof is not None
        self.assertEqual(proof.role_bucket_ids, ("research", "engineering"))
        self.assertEqual(len(proof.evidence_digest), 64)

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
        manifest_without_digest = dict(manifest)
        manifest_without_digest.pop("manifest_digest")
        self.assertEqual(manifest["manifest_digest"], _sha256_json(manifest_without_digest))
        self.assertEqual(
            manifest["compiler_inputs"]["execution_capability"]["runtime_namespace"],
            str(self.runtime_dir.resolve()),
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
        other = CohortExecutionCapability(
            policy_revision="test.other-policy.v1",
            provider_mode="scripted",
            runtime_namespace=str(self.runtime_dir),
        )

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
        with tempfile.TemporaryDirectory(dir=self.runtime_dir) as tempdir:
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
            provider_mode="scripted",
            runtime_namespace=str(self.runtime_dir),
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

        with tempfile.TemporaryDirectory(dir=self.runtime_dir) as tempdir:
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
        with tempfile.TemporaryDirectory(dir=self.runtime_dir) as tempdir:
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

        with tempfile.TemporaryDirectory(dir=self.runtime_dir) as tempdir:
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
            with (
                self.subTest(malformed_body=malformed_body),
                tempfile.TemporaryDirectory(dir=self.runtime_dir) as tempdir,
            ):
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

        with tempfile.TemporaryDirectory(dir=self.runtime_dir) as tempdir:
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

    def test_harvest_boundary_stops_after_invalid_identity_and_binds_attempt_evidence(self) -> None:
        manifest = self.compiler.compile(
            _request_payload(roles=["research", "engineering"], statuses=["current"]),
            execution_capability=self.capability,
        )

        class _InvalidFirstLaneConnector(HarvestProfileSearchConnector):
            calls = 0

            def search_profiles(self, **_kwargs):
                self.calls += 1
                if self.calls > 1:
                    raise AssertionError("invalid first-lane identity must stop later provider calls")
                return {"rows": [{"full_name": "Identity-free"}]}

        connector = _InvalidFirstLaneConnector(HarvestActorSettings())
        with tempfile.TemporaryDirectory(dir=self.runtime_dir) as tempdir:
            with self.assertRaises(CohortProviderExecutionError) as caught:
                connector.search_profiles_for_cohort_manifest(
                    manifest=manifest,
                    execution_capability=self.capability,
                    discovery_dir=Path(tempdir),
                )

        self.assertEqual(connector.calls, 1)
        self.assertEqual(caught.exception.code, "cohort_provider_candidate_identity_missing")
        self.assertEqual(caught.exception.lane_id, manifest["lanes"][0]["lane_id"])
        self.assertEqual(caught.exception.completed_lane_ids, ())

    def test_harvest_boundary_binds_completed_lanes_to_role_verifier_failure(self) -> None:
        capability = CohortExecutionCapability(
            policy_revision="test.proof-attempt-evidence.v1",
            provider_mode="scripted",
            runtime_namespace=str(self.runtime_dir),
            role_proof_verifier_id="failing_role_verifier",
            role_proof_verifier_revision="v1",
        )
        manifest = self.compiler.compile(
            _request_payload(
                roles=["research", "engineering"],
                statuses=["current"],
                role_match="all",
            ),
            execution_capability=capability,
        )

        class _Connector(HarvestProfileSearchConnector):
            def search_profiles(self, **_kwargs):
                return {"rows": [{"candidate_id": "same-person", "headline": "Research Engineer"}]}

        class _FailingVerifier:
            verifier_id = "failing_role_verifier"
            verifier_revision = "v1"

            @staticmethod
            def verify(_row):
                raise RuntimeError("proof unavailable")

        with tempfile.TemporaryDirectory(dir=self.runtime_dir) as tempdir:
            with self.assertRaises(CohortProviderExecutionError) as caught:
                _Connector(HarvestActorSettings()).search_profiles_for_cohort_manifest(
                    manifest=manifest,
                    execution_capability=capability,
                    role_proof_verifier=_FailingVerifier(),
                    discovery_dir=Path(tempdir),
                )

        self.assertEqual(caught.exception.code, "cohort_role_proof_verification_failed")
        self.assertEqual(
            caught.exception.completed_lane_ids,
            tuple(lane["lane_id"] for lane in manifest["lanes"]),
        )

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
        with tempfile.TemporaryDirectory(dir=self.runtime_dir) as tempdir:
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
            provider_mode="scripted",
            runtime_namespace=str(self.runtime_dir),
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
        with tempfile.TemporaryDirectory(dir=self.runtime_dir) as tempdir:
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

        with tempfile.TemporaryDirectory(dir=self.runtime_dir) as tempdir:
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
        self.assertNotIn(
            "acquire_former_search_seed",
            [task.task_type for task in plan.acquisition_tasks],
        )
        full_roster_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")
        self.assertFalse(full_roster_task.metadata["include_former_search_seed"])


class CohortAcquisitionRuntimeTest(unittest.TestCase):
    def test_explicit_cohort_fence_precedes_every_legacy_strategy_route(self) -> None:
        engine = object.__new__(AcquisitionEngine)
        engine._should_use_local_anthropic_assets = lambda _request: False
        routed: list[str] = []

        def _cohort_route(task, _state, _request):
            routed.append(str(task.metadata.get("strategy_type") or ""))
            return AcquisitionExecution(
                task_id=task.task_id,
                status="blocked",
                detail="cohort fence reached",
                payload={"reason": "cohort_fence_reached"},
            )

        engine._acquire_search_seed_pool = _cohort_route
        engine._acquire_full_roster = lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("legacy full-roster route must not run")
        )
        engine._acquire_investor_firm_roster = lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("legacy investor route must not run")
        )
        request = JobRequest.from_payload(
            _request_payload(
                roles=["research", "engineering"],
                statuses=["current", "former"],
            )
        )
        strategy_types = [
            "full_company_roster",
            "scoped_search_roster",
            "former_employee_search",
            "investor_firm_roster",
            "caller_override_unknown",
        ]

        for strategy_type in strategy_types:
            with self.subTest(strategy_type=strategy_type):
                execution = engine.execute_task(
                    AcquisitionTask(
                        task_id=f"cohort-{strategy_type}",
                        task_type="acquire_full_roster",
                        title="Acquire explicit cohort",
                        description="strategy route fence",
                        status="ready",
                        blocking=True,
                        metadata={"strategy_type": strategy_type},
                    ),
                    request,
                    "Acme",
                    {},
                )
                self.assertEqual(execution.payload["reason"], "cohort_fence_reached")

        self.assertEqual(routed, strategy_types)

    def test_zero_result_normalizer_never_converts_manifest_mismatch_to_success(self) -> None:
        engine = object.__new__(AcquisitionEngine)
        blocked = AcquisitionExecution(
            task_id="cohort-former-compatibility",
            status="blocked",
            detail="Stored cohort provider manifest no longer matches canonical inputs.",
            payload={"reason": "cohort_provider_manifest_semantic_mismatch"},
        )

        normalized = engine._normalize_zero_result_search_seed_execution(
            blocked,
            detail="Former-member search completed but did not add any new candidates.",
        )

        self.assertIs(normalized, blocked)
        self.assertEqual(normalized.status, "blocked")
        self.assertEqual(normalized.payload["reason"], "cohort_provider_manifest_semantic_mismatch")

    def test_non_live_manifest_flows_into_one_durable_search_seed_contract(self) -> None:
        captured: list[dict[str, Any]] = []
        emit_rows = [True]
        rejected_unverified_count = [0]

        class _Connector:
            @staticmethod
            def search_profiles_for_cohort_manifest(**kwargs):
                manifest = dict(kwargs["manifest"])
                captured.append(dict(kwargs))
                memberships = [
                    {
                        "lane_id": str(lane["lane_id"]),
                        "employment_status": str(lane["employment_status"]),
                        "role_bucket_id": str(lane["role_bucket_id"]),
                    }
                    for lane in manifest["lanes"]
                ]
                rows = (
                    [
                        {
                            "full_name": "Ada Researcher",
                            "headline": "Research Engineer",
                            "profile_url": "https://www.linkedin.com/in/ada-researcher/",
                            "username": "ada-researcher",
                            COHORT_CANONICAL_PROFILE_URL_FIELD: "https://linkedin.com/in/ada-researcher",
                            "cohort_lane_membership": memberships,
                        }
                    ]
                    if emit_rows[0]
                    else []
                )
                return {
                    "rows": rows,
                    "candidate_count": len(rows),
                    "truncated_count": 0,
                    "rejected_unverified_count": rejected_unverified_count[0],
                    "missing_required_lane_count": 0,
                    "result_digest": "result-digest",
                    "cohort_provider_manifest": manifest,
                    "lane_summaries": [
                        {
                            "lane_id": lane["lane_id"],
                            "lane_digest": lane["lane_digest"],
                            "employment_status": lane["employment_status"],
                            "role_bucket_id": lane["role_bucket_id"],
                            "provider_item_limit": lane["provider_item_limit"],
                            "row_count": len(rows),
                            "raw_path": f"/tmp/{lane['lane_id']}.json",
                        }
                        for lane in manifest["lanes"]
                    ],
                }

        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "simulate_runtime"
            snapshot_dir = runtime_dir / "company_assets" / "acme" / "snap-test"
            snapshot_dir.mkdir(parents=True)
            engine = object.__new__(AcquisitionEngine)
            engine.settings = SimpleNamespace(runtime_dir=runtime_dir)
            engine.store = SimpleNamespace()
            engine.worker_runtime = None
            engine.harvest_profile_search_connector = _Connector()
            engine._queue_background_profile_prefetch_for_search_seed_entries = lambda **_kwargs: {
                "status": "completed",
                "requested_url_count": 1,
                "dispatched_url_count": 1,
            }
            identity = CompanyIdentity(
                requested_name="Acme",
                canonical_name="Acme",
                company_key="acme",
                linkedin_slug="acme",
                linkedin_company_url="https://www.linkedin.com/company/acme/",
            )
            request = JobRequest.from_payload(
                _request_payload(
                    roles=["research", "engineering"],
                    statuses=["current", "former"],
                )
            )
            task = AcquisitionTask(
                task_id="cohort-search",
                task_type="acquire_full_roster",
                title="Acquire cohort",
                description="test",
                status="ready",
                blocking=True,
                metadata={
                    "strategy_type": "scoped_search_roster",
                    "filter_hints": {"past_companies": ["Injected task-local company"]},
                    "cost_policy": {"allow_shared_provider_cache": False},
                },
            )
            preview_manifest = CohortProviderCompiler().compile(
                request.to_record(),
                base_filter_hints={"current_companies": ["Acme"], "past_companies": ["Acme"]},
            )

            runtime_state = {
                "company_identity": identity,
                "snapshot_dir": snapshot_dir,
                "job_id": "job-cohort",
                "plan_payload": {
                    "acquisition_strategy": {
                        "filter_hints": {"current_companies": ["Acme"], "past_companies": ["Acme"]},
                        "provider_execution_manifest": preview_manifest,
                    }
                },
                "runtime_mode": "workflow",
            }
            execution = engine.execute_task(
                task,
                request,
                "Acme",
                runtime_state,
            )

            self.assertEqual(execution.status, "completed")
            self.assertEqual(execution.payload["entry_count"], 1)
            self.assertEqual(execution.payload["cohort_execution_result"]["candidate_count"], 1)
            self.assertEqual(len(captured), 1)
            manifest = captured[0]["manifest"]
            self.assertTrue(manifest["execution_ready"])
            self.assertEqual(
                manifest["compiler_inputs"]["base_filter_hints"],
                {"current_companies": ["Acme"], "past_companies": ["Acme"]},
            )
            self.assertEqual(
                {(lane["employment_status"], lane["role_bucket_id"]) for lane in manifest["lanes"]},
                {
                    ("current", "research"),
                    ("current", "engineering"),
                    ("former", "research"),
                    ("former", "engineering"),
                },
            )
            entries = json.loads((snapshot_dir / "search_seed_discovery" / "entries.json").read_text(encoding="utf-8"))
            self.assertEqual(entries[0]["full_name"], "Ada Researcher")
            self.assertEqual(
                entries[0]["metadata"]["cohort_role_bucket_ids"],
                ["research", "engineering"],
            )
            candidate_documents = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
            self.assertEqual(candidate_documents["candidate_count"], 1)
            result_summary_path = snapshot_dir / "cohort_provider_discovery" / "cohort_execution_result.json"
            result_summary = json.loads(result_summary_path.read_text(encoding="utf-8"))
            self.assertEqual(result_summary["result_digest"], "result-digest")
            self.assertEqual(result_summary["artifact_path"], str(result_summary_path))
            publication_digest = result_summary["cohort_publication_digest"]
            self.assertEqual(len(publication_digest), 64)
            self.assertEqual(
                candidate_documents["acquisition_sources"]["search_seed_snapshot"]["cohort_publication_digest"],
                publication_digest,
            )
            self.assertIsNotNone(load_search_seed_snapshot_from_snapshot_dir(snapshot_dir, identity=identity))

            runtime_state.update(execution.state_updates)
            compatibility_former_task = AcquisitionTask(
                task_id="cohort-former-compatibility",
                task_type="acquire_former_search_seed",
                title="Acquire former cohort",
                description="legacy hydrated plan compatibility",
                status="ready",
                blocking=False,
                metadata={
                    "strategy_type": "former_employee_search",
                    "employment_statuses": ["former"],
                    "search_seed_queries": ["legacy former query"],
                    "filter_hints": {"past_companies": ["Acme"]},
                },
            )
            committed_files_before_compatibility_reuse = {
                str(path.relative_to(snapshot_dir)): path.read_bytes()
                for path in snapshot_dir.rglob("*")
                if path.is_file()
            }
            compatibility_execution = engine._acquire_former_search_seed(
                compatibility_former_task,
                runtime_state,
                request,
            )
            self.assertEqual(compatibility_execution.status, "completed")
            self.assertTrue(compatibility_execution.payload["reused_existing_full_manifest"])
            self.assertEqual(len(captured), 1)
            self.assertEqual(
                {
                    str(path.relative_to(snapshot_dir)): path.read_bytes()
                    for path in snapshot_dir.rglob("*")
                    if path.is_file()
                },
                committed_files_before_compatibility_reuse,
            )

            committed_files_before_invalid_compatibility = {
                str(path.relative_to(snapshot_dir)): path.read_bytes()
                for path in snapshot_dir.rglob("*")
                if path.is_file()
            }
            invalid_compatibility_plans = {
                "missing": {
                    "acquisition_strategy": {"filter_hints": preview_manifest["compiler_inputs"]["base_filter_hints"]}
                },
                "forged": {
                    "acquisition_strategy": {
                        "filter_hints": preview_manifest["compiler_inputs"]["base_filter_hints"],
                        "provider_execution_manifest": {"forged": True},
                    }
                },
            }
            for label, invalid_plan_payload in invalid_compatibility_plans.items():
                with (
                    self.subTest(compatibility_manifest=label),
                    patch("sourcing_agent.acquisition._registry_load_search_seed_snapshot_from_snapshot_dir") as load,
                    patch("sourcing_agent.acquisition._merge_search_seed_snapshots") as merge,
                ):
                    blocked_compatibility = engine._acquire_former_search_seed(
                        compatibility_former_task,
                        {
                            **runtime_state,
                            "plan_payload": invalid_plan_payload,
                        },
                        request,
                    )
                    self.assertEqual(blocked_compatibility.status, "blocked")
                    self.assertEqual(
                        blocked_compatibility.payload["reason"],
                        "cohort_provider_manifest_semantic_mismatch",
                    )
                    load.assert_not_called()
                    merge.assert_not_called()
                    self.assertEqual(
                        {
                            str(path.relative_to(snapshot_dir)): path.read_bytes()
                            for path in snapshot_dir.rglob("*")
                            if path.is_file()
                        },
                        committed_files_before_invalid_compatibility,
                    )

            invalid_plan_payloads = {
                "missing": {},
                "forged": {
                    "acquisition_strategy": {
                        "provider_execution_manifest": {"forged": True},
                    }
                },
            }
            for label, plan_payload in invalid_plan_payloads.items():
                with self.subTest(plan_manifest=label):
                    invalid_snapshot_dir = runtime_dir / "company_assets" / "acme" / f"snap-invalid-{label}"
                    invalid_snapshot_dir.mkdir(parents=True)
                    blocked = engine.execute_task(
                        task,
                        request,
                        "Acme",
                        {
                            "company_identity": identity,
                            "snapshot_dir": invalid_snapshot_dir,
                            "job_id": f"job-{label}-plan",
                            "plan_payload": {
                                **plan_payload,
                                **(
                                    {
                                        "acquisition_strategy": {
                                            "filter_hints": {
                                                "current_companies": ["Acme"],
                                                "past_companies": ["Acme"],
                                            },
                                            **dict(plan_payload.get("acquisition_strategy") or {}),
                                        }
                                    }
                                    if plan_payload.get("acquisition_strategy")
                                    else {}
                                ),
                            },
                            "runtime_mode": "workflow",
                        },
                    )
                    self.assertEqual(blocked.status, "blocked")
                    self.assertEqual(blocked.payload["reason"], "cohort_provider_manifest_semantic_mismatch")
                    self.assertFalse((invalid_snapshot_dir / "candidate_documents.json").exists())
                    self.assertFalse(
                        (invalid_snapshot_dir / "cohort_provider_discovery" / "cohort_execution_result.json").exists()
                    )
                    self.assertFalse((invalid_snapshot_dir / "search_seed_discovery" / "summary.json").exists())
            self.assertEqual(len(captured), 1)

            emit_rows[0] = False
            zero_snapshot_dir = runtime_dir / "company_assets" / "acme" / "snap-zero"
            zero_snapshot_dir.mkdir(parents=True)
            stale_candidate_path = zero_snapshot_dir / "candidate_documents.json"
            stale_result_path = zero_snapshot_dir / "cohort_provider_discovery" / "cohort_execution_result.json"
            stale_result_path.parent.mkdir(parents=True)
            stale_candidate_path.write_text('{"sentinel":"stale-candidates"}', encoding="utf-8")
            stale_result_path.write_text('{"sentinel":"stale-result"}', encoding="utf-8")
            mutated_route_task = AcquisitionTask(
                task_id="cohort-mutated-route",
                task_type="acquire_full_roster",
                title="Acquire explicit cohort",
                description="task-local strategy must not redirect the cohort route",
                status="ready",
                blocking=True,
                metadata={
                    "strategy_type": "investor_firm_roster",
                    "filter_hints": {"past_companies": ["Injected task-local company"]},
                    "cost_policy": {"allow_shared_provider_cache": False},
                },
            )
            zero_execution = engine.execute_task(
                mutated_route_task,
                request,
                "Acme",
                {
                    "company_identity": identity,
                    "snapshot_dir": zero_snapshot_dir,
                    "job_id": "job-cohort-zero",
                    "plan_payload": {
                        "acquisition_strategy": {
                            "filter_hints": {
                                "current_companies": ["Acme"],
                                "past_companies": ["Acme"],
                            },
                            "provider_execution_manifest": preview_manifest,
                        }
                    },
                    "runtime_mode": "workflow",
                },
            )
            self.assertEqual(zero_execution.status, "blocked")
            self.assertEqual(zero_execution.payload["reason"], "cohort_provider_no_results")
            self.assertEqual(zero_execution.state_updates, {})
            self.assertEqual(len(captured), 2)
            self.assertEqual(
                json.loads(stale_candidate_path.read_text(encoding="utf-8")),
                {"sentinel": "stale-candidates"},
            )
            self.assertEqual(
                json.loads(stale_result_path.read_text(encoding="utf-8")),
                {"sentinel": "stale-result"},
            )
            self.assertFalse((zero_snapshot_dir / "search_seed_discovery" / "summary.json").exists())

            rejected_unverified_count[0] = 2
            rejected_snapshot_dir = runtime_dir / "company_assets" / "acme" / "snap-all-rejected"
            rejected_snapshot_dir.mkdir(parents=True)
            rejected_execution = engine.execute_task(
                mutated_route_task,
                request,
                "Acme",
                {
                    "company_identity": identity,
                    "snapshot_dir": rejected_snapshot_dir,
                    "job_id": "job-cohort-all-rejected",
                    "plan_payload": runtime_state["plan_payload"],
                    "runtime_mode": "workflow",
                },
            )
            self.assertEqual(rejected_execution.status, "blocked")
            self.assertEqual(rejected_execution.payload["reason"], "cohort_provider_all_rows_rejected")
            self.assertEqual(rejected_execution.state_updates, {})
            self.assertFalse(
                (rejected_snapshot_dir / "cohort_provider_discovery" / "cohort_execution_result.json").exists()
            )
            self.assertFalse((rejected_snapshot_dir / "search_seed_discovery" / "summary.json").exists())

            emit_rows[0] = True
            rejected_unverified_count[0] = 0
            partial_snapshot_dir = runtime_dir / "company_assets" / "acme" / "snap-partial-publication"
            partial_snapshot_dir.mkdir(parents=True)
            with (
                patch(
                    "sourcing_agent.search_seed_registry.project_search_seed_snapshot_to_candidate_documents",
                    side_effect=OSError("synthetic candidate projection failure"),
                ),
                self.assertRaisesRegex(OSError, "synthetic candidate projection failure"),
            ):
                engine.execute_task(
                    mutated_route_task,
                    request,
                    "Acme",
                    {
                        "company_identity": identity,
                        "snapshot_dir": partial_snapshot_dir,
                        "job_id": "job-cohort-partial-publication",
                        "plan_payload": runtime_state["plan_payload"],
                        "runtime_mode": "workflow",
                    },
                )
            self.assertTrue(
                (partial_snapshot_dir / "cohort_provider_discovery" / "cohort_execution_result.json").exists()
            )
            self.assertTrue((partial_snapshot_dir / "search_seed_discovery" / "summary.json").exists())
            self.assertFalse((partial_snapshot_dir / "candidate_documents.json").exists())
            self.assertIsNone(load_search_seed_snapshot_from_snapshot_dir(partial_snapshot_dir, identity=identity))
            self.assertIsNone(
                _restore_search_seed_snapshot_from_snapshot_dir(
                    snapshot_dir=partial_snapshot_dir,
                    identity=identity,
                )
            )
            partial_files_before_recovery = {
                str(path.relative_to(partial_snapshot_dir)): path.read_bytes()
                for path in partial_snapshot_dir.rglob("*")
                if path.is_file()
            }
            recovery_result = object.__new__(SnapshotMaterializer).apply_search_seed_workers_to_snapshot(
                snapshot_dir=partial_snapshot_dir,
                pending_workers=[
                    {
                        "worker_id": 1,
                        "output": {
                            "entries": [
                                {
                                    "full_name": "Must Not Recover",
                                    "profile_url": "https://www.linkedin.com/in/must-not-recover/",
                                }
                            ]
                        },
                    }
                ],
            )
            self.assertEqual(recovery_result, {"status": "skipped", "reason": "cohort_publication_uncommitted"})
            self.assertEqual(
                {
                    str(path.relative_to(partial_snapshot_dir)): path.read_bytes()
                    for path in partial_snapshot_dir.rglob("*")
                    if path.is_file()
                },
                partial_files_before_recovery,
            )

    def test_scripted_runtime_executes_real_harvest_boundary_without_live_submission(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            prefetch_calls: list[dict[str, Any]] = []
            runtime_dir = Path(tempdir) / "scripted_runtime"
            snapshot_dir = runtime_dir / "company_assets" / "acme" / "snap-scripted"
            snapshot_dir.mkdir(parents=True)
            scenario_path = Path(tempdir) / "cohort_scenario.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "cohort_profile_search",
                                    "match": {"logical_name": "harvest_profile_search"},
                                    "body": [
                                        {
                                            "linkedinUrl": "https://example.com/not-the-linkedin-identity",
                                            "publicIdentifier": "scripted-researcher",
                                            "fullName": "Scripted Researcher",
                                            "headline": "Research Scientist and Software Engineer",
                                            "currentCompany": "Acme",
                                        }
                                    ],
                                }
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )
            request = JobRequest.from_payload(
                _request_payload(
                    roles=["research", "engineering"],
                    statuses=["current"],
                    role_match="all",
                )
            )
            task = AcquisitionTask(
                task_id="cohort-scripted",
                task_type="acquire_full_roster",
                title="Acquire scripted cohort",
                description="test",
                status="ready",
                blocking=True,
                metadata={
                    "strategy_type": "scoped_search_roster",
                    "filter_hints": {"current_companies": ["Acme"]},
                    "cost_policy": {"allow_shared_provider_cache": False},
                },
            )
            preview_manifest = CohortProviderCompiler().compile(
                request.to_record(),
                base_filter_hints={"current_companies": ["Acme"]},
            )
            engine = object.__new__(AcquisitionEngine)
            engine.settings = SimpleNamespace(runtime_dir=runtime_dir)
            engine.store = SimpleNamespace()
            engine.worker_runtime = None
            engine.harvest_profile_search_connector = HarvestProfileSearchConnector(HarvestActorSettings(enabled=False))

            def _capture_prefetch(**kwargs):
                prefetch_calls.append(copy.deepcopy(kwargs))
                return {
                    "status": "completed",
                    "requested_url_count": 1,
                    "dispatched_url_count": 0,
                }

            engine._queue_background_profile_prefetch_for_search_seed_entries = _capture_prefetch
            identity = CompanyIdentity(
                requested_name="Acme",
                canonical_name="Acme",
                company_key="acme",
                linkedin_slug="acme",
                linkedin_company_url="https://www.linkedin.com/company/acme/",
            )
            with (
                patch.dict(
                    "os.environ",
                    {
                        "SOURCING_RUNTIME_DIR": str(runtime_dir),
                        "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                        "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                        "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
                    },
                ),
                patch(
                    "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                    side_effect=AssertionError("scripted cohort runtime must not submit a live Harvest run"),
                ),
            ):
                execution = engine.execute_task(
                    task,
                    request,
                    "Acme",
                    {
                        "company_identity": identity,
                        "snapshot_dir": snapshot_dir,
                        "job_id": "job-scripted-cohort",
                        "plan_payload": {
                            "acquisition_strategy": {
                                "filter_hints": {"current_companies": ["Acme"]},
                                "provider_execution_manifest": preview_manifest,
                            }
                        },
                        "runtime_mode": "workflow",
                    },
                )

            self.assertEqual(execution.status, "completed")
            self.assertEqual(execution.payload["cohort_execution_result"]["candidate_count"], 1)
            self.assertEqual(len(execution.payload["cohort_execution_result"]["lane_summaries"]), 2)
            self.assertTrue(
                all(
                    summary["row_count"] == 1
                    for summary in execution.payload["cohort_execution_result"]["lane_summaries"]
                )
            )
            persisted = json.loads(
                (snapshot_dir / "search_seed_discovery" / "entries.json").read_text(encoding="utf-8")
            )
            self.assertEqual(persisted[0]["full_name"], "Scripted Researcher")
            self.assertEqual(persisted[0]["profile_url"], "https://linkedin.com/in/scripted-researcher")
            self.assertEqual(
                persisted[0]["metadata"]["cohort_role_bucket_ids"],
                ["research", "engineering"],
            )
            self.assertEqual(
                persisted[0]["metadata"]["cohort_role_proof"]["verifier_id"],
                CohortHeadlineRoleProofVerifier.verifier_id,
            )
            candidate_documents = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
            self.assertEqual(candidate_documents["candidate_count"], 1)
            self.assertEqual(
                candidate_documents["candidates"][0]["linkedin_url"],
                "https://linkedin.com/in/scripted-researcher",
            )
            self.assertEqual(len(prefetch_calls), 1)
            self.assertEqual(
                prefetch_calls[0]["entries"][0]["profile_url"],
                "https://linkedin.com/in/scripted-researcher",
            )


if __name__ == "__main__":
    unittest.main()
