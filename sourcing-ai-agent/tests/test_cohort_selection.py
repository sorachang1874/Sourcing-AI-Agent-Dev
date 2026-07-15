from __future__ import annotations

import hashlib
import json
import os
import threading
import unittest
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from urllib import request as urllib_request
from urllib.error import HTTPError

from sourcing_agent.acquisition_strategy import compile_acquisition_strategy
from sourcing_agent.api import create_server
from sourcing_agent.asset_reuse_planning import compile_asset_reuse_plan
from sourcing_agent.cohort_provider_compiler import (
    CohortExecutionCapability,
    CohortProviderCompilationError,
    CohortProviderCompiler,
)
from sourcing_agent.cohort_selection import (
    COHORT_SELECTION_REGISTRY_VERSION,
    COHORT_SELECTION_SCHEMA_VERSION,
    CohortSelectionValidationError,
    canonicalize_cohort_selection_request_payload,
    cohort_selection_digest,
    cohort_selection_options_payload,
    cohort_selection_registry_digest,
    effective_cohort_selection,
    merge_plan_review_cohort_selection,
    prepare_external_criteria_request_payload,
    validate_external_cohort_selection_payload,
)
from sourcing_agent.domain import JobRequest, RetrievalPlan
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.plan_review import apply_plan_review_decision
from sourcing_agent.post_acquisition_refinement import compile_refinement_patch_from_instruction
from sourcing_agent.query_signal_knowledge import ROLE_BUCKET_KNOWLEDGE
from sourcing_agent.request_matching import (
    build_request_matching_bundle,
    matching_request_signature,
    request_signature,
)
from sourcing_agent.request_normalization import build_effective_request_payload


def _cohort(
    *,
    roles: list[str] | None = None,
    statuses: list[str] | None = None,
    role_match: str = "any",
    source: str = "user_explicit",
) -> dict[str, object]:
    return {
        "schema_version": COHORT_SELECTION_SCHEMA_VERSION,
        "role_bucket_ids": list(roles if roles is not None else ["research"]),
        "employment_statuses": list(statuses if statuses is not None else ["current"]),
        "role_match": role_match,
        "source": source,
    }


class CohortSelectionContractTest(unittest.TestCase):
    def test_options_are_registry_derived_and_stably_ordered(self) -> None:
        options = cohort_selection_options_payload()

        self.assertEqual(options["schema_version"], COHORT_SELECTION_SCHEMA_VERSION)
        self.assertEqual(options["registry_version"], COHORT_SELECTION_REGISTRY_VERSION)
        self.assertEqual(options["registry_digest"], cohort_selection_registry_digest())
        self.assertEqual(
            options["registry_digest"],
            "b8613f09fa7eb8cf016d45c4a2ece4ead1d562043b897ed2f335c9c3ce6a8e41",
        )
        self.assertTrue(all(set(item) == {"id", "label", "order"} for item in options["role_buckets"]))
        self.assertEqual(
            [item["id"] for item in options["role_buckets"]],
            ["research", "engineering", "product_management", "infra_systems", "founding"],
        )
        self.assertEqual(
            {item["id"]: item["label"] for item in options["role_buckets"]},
            {
                role_id: spec["selectable_label"]
                for role_id, spec in ROLE_BUCKET_KNOWLEDGE.items()
                if spec.get("selectable_label")
            },
        )
        self.assertEqual(
            [item["id"] for item in options["employment_statuses"]],
            ["current", "former"],
        )

    def test_selection_digest_pins_registry_and_semantics_but_not_source_or_order(self) -> None:
        explicit = _cohort(
            roles=["engineering", "research"],
            statuses=["former", "current"],
        )
        same_server_selection = _cohort(
            roles=["research", "engineering"],
            statuses=["current", "former"],
            source="inferred",
        )

        self.assertEqual(
            cohort_selection_digest(explicit),
            cohort_selection_digest(same_server_selection),
        )
        self.assertNotEqual(
            cohort_selection_digest(explicit),
            cohort_selection_digest({**explicit, "role_match": "all"}),
        )

    def test_registry_digest_binds_execution_affecting_role_mapping(self) -> None:
        selection = _cohort(roles=["research"], statuses=["current"])
        compiler = CohortProviderCompiler()
        capability = CohortExecutionCapability(policy_revision="test.registry-binding.v1")
        before_registry = cohort_selection_registry_digest()
        before_selection = cohort_selection_digest(selection)
        before_manifest = compiler.compile(
            {"cohort_selection": selection},
            execution_capability=capability,
        )
        research_spec = ROLE_BUCKET_KNOWLEDGE["research"]
        original_hints = tuple(research_spec.get("role_hints") or ())
        try:
            research_spec["role_hints"] = (*original_hints, "Research Fellow")
            after_registry = cohort_selection_registry_digest()
            after_selection = cohort_selection_digest(selection)
            after_manifest = compiler.compile(
                {"cohort_selection": selection},
                execution_capability=capability,
            )
        finally:
            research_spec["role_hints"] = original_hints

        self.assertNotEqual(before_registry, after_registry)
        self.assertNotEqual(before_selection, after_selection)
        self.assertNotEqual(before_manifest["manifest_digest"], after_manifest["manifest_digest"])
        self.assertNotEqual(
            before_manifest["lanes"][0]["provider_payload"],
            after_manifest["lanes"][0]["provider_payload"],
        )

    def test_explicit_object_canonicalizes_order_and_installs_missing_mirrors(self) -> None:
        canonical = canonicalize_cohort_selection_request_payload(
            {
                "target_company": "Thinking Machines Lab",
                "cohort_selection": _cohort(
                    roles=["product_management", "research", "engineering"],
                    statuses=["former", "current"],
                ),
            }
        )

        self.assertEqual(
            canonical["cohort_selection"]["role_bucket_ids"],
            ["research", "engineering", "product_management"],
        )
        self.assertEqual(canonical["cohort_selection"]["employment_statuses"], ["current", "former"])
        self.assertEqual(
            canonical["must_have_primary_role_buckets"],
            ["research", "engineering", "product_management"],
        )
        self.assertEqual(canonical["employment_statuses"], ["current", "former"])

    def test_role_list_may_be_empty_but_status_list_must_not_be_empty(self) -> None:
        canonical = canonicalize_cohort_selection_request_payload(
            {"cohort_selection": _cohort(roles=[], statuses=["former"])}
        )
        self.assertEqual(canonical["must_have_primary_role_buckets"], [])

        with self.assertRaisesRegex(
            CohortSelectionValidationError,
            "cohort_selection_empty_employment_statuses",
        ):
            canonicalize_cohort_selection_request_payload({"cohort_selection": _cohort(roles=[], statuses=[])})

    def test_unknown_values_fields_and_external_server_sources_fail_closed(self) -> None:
        invalid_payloads: list[dict[str, Any]] = [
            {"cohort_selection": {**_cohort(), "unknown": True}},
            {"cohort_selection": _cohort(roles=["sales"])},
            {"cohort_selection": _cohort(statuses=["contractor"])},
            {"cohort_selection": _cohort(source="legacy_adapter")},
            {"cohort_selection": _cohort(source="inferred")},
        ]
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                with self.assertRaises(CohortSelectionValidationError):
                    validate_external_cohort_selection_payload(payload)

    def test_external_criteria_request_aliases_share_one_canonical_owner(self) -> None:
        request_payload = {
            "target_company": "OpenAI",
            "cohort_selection": _cohort(
                roles=["engineering", "research"],
                statuses=["former", "current"],
            ),
        }

        prepared = prepare_external_criteria_request_payload(
            {
                "request": request_payload,
                "request_payload": request_payload,
                "metadata": {"request_payload": request_payload, "note": "keep"},
            }
        )

        self.assertNotIn("request", prepared)
        self.assertEqual(
            prepared["request_payload"]["must_have_primary_role_buckets"],
            ["research", "engineering"],
        )
        self.assertEqual(prepared["request_payload"]["employment_statuses"], ["current", "former"])
        self.assertEqual(prepared["metadata"], {"note": "keep"})

    def test_external_criteria_request_rejects_conflicting_aliases(self) -> None:
        with self.assertRaises(CohortSelectionValidationError) as captured:
            prepare_external_criteria_request_payload(
                {
                    "request": {
                        "target_company": "OpenAI",
                        "cohort_selection": _cohort(roles=["research"]),
                    },
                    "request_payload": {
                        "target_company": "OpenAI",
                        "cohort_selection": _cohort(roles=["engineering"]),
                    },
                }
            )

        self.assertEqual(captured.exception.code, "criteria_request_alias_conflict")

    def test_external_criteria_request_rejects_server_owned_source_in_metadata_alias(self) -> None:
        with self.assertRaises(CohortSelectionValidationError) as captured:
            prepare_external_criteria_request_payload(
                {
                    "metadata": {
                        "request_payload": {
                            "target_company": "OpenAI",
                            "cohort_selection": _cohort(source="inferred"),
                        }
                    }
                }
            )

        self.assertEqual(captured.exception.code, "cohort_selection_invalid_source")

    def test_present_flat_or_intent_axis_mirror_conflicts_fail_closed(self) -> None:
        conflicts: list[dict[str, Any]] = [
            {
                "cohort_selection": _cohort(),
                "employment_statuses": ["former"],
            },
            {
                "cohort_selection": _cohort(),
                "must_have_primary_role_buckets": ["engineering"],
            },
            {
                "cohort_selection": _cohort(),
                "intent_axes": {
                    "population_boundary": {"employment_statuses": ["former"]},
                },
            },
            {
                "cohort_selection": _cohort(role_match="all"),
                "intent_axes": {"thematic_constraints": {"role_match": "any"}},
            },
        ]
        for payload in conflicts:
            with self.subTest(payload=payload):
                with self.assertRaisesRegex(
                    CohortSelectionValidationError,
                    "cohort_selection_mirror_conflict",
                ):
                    validate_external_cohort_selection_payload(payload)

    def test_null_wrong_shape_and_alias_duplicate_mirrors_fail_closed(self) -> None:
        conflicts: list[dict[str, Any]] = [
            {"cohort_selection": _cohort(), "employment_statuses": None},
            {"cohort_selection": _cohort(), "must_have_primary_role_buckets": None},
            {"cohort_selection": _cohort(), "must_have_primary_role_bucket": None},
            {
                "cohort_selection": _cohort(),
                "must_have_primary_role_buckets": ["research"],
                "must_have_primary_role_bucket": ["research"],
            },
            {"cohort_selection": _cohort(), "intent_axes": None},
            {"cohort_selection": _cohort(), "intent_axes": []},
            {
                "cohort_selection": _cohort(),
                "intent_axes": {"population_boundary": None},
            },
            {
                "cohort_selection": _cohort(),
                "intent_axes": {"thematic_constraints": "research"},
            },
            {
                "cohort_selection": _cohort(),
                "intent_axes": {
                    "population_boundary": {"employment_statuses": None},
                },
            },
            {
                "cohort_selection": _cohort(),
                "intent_axes": {
                    "thematic_constraints": {"must_have_primary_role_buckets": None},
                },
            },
        ]
        for payload in conflicts:
            with self.subTest(payload=payload):
                with self.assertRaises(CohortSelectionValidationError):
                    validate_external_cohort_selection_payload(payload)

    def test_plan_review_merge_owner_accepts_exact_replay_and_upgrades_legacy(self) -> None:
        stored_explicit = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(
                    roles=["research", "engineering"],
                    statuses=["current"],
                ),
            }
        ).to_record()
        exact = merge_plan_review_cohort_selection(
            stored_explicit,
            {"cohort_selection": dict(stored_explicit["cohort_selection"])},
        )
        self.assertEqual(exact["cohort_selection"], stored_explicit["cohort_selection"])

        with self.assertRaisesRegex(
            CohortSelectionValidationError,
            "cohort_selection_plan_review_conflict",
        ):
            merge_plan_review_cohort_selection(
                stored_explicit,
                {"cohort_selection": _cohort(roles=["product_management"])},
            )

        upgraded = merge_plan_review_cohort_selection(
            {
                "target_company": "Acme",
                "employment_statuses": ["current", "former"],
                "must_have_primary_role_buckets": ["research"],
            },
            {
                "cohort_selection": _cohort(
                    roles=["product_management", "engineering"],
                    statuses=["former", "current"],
                    role_match="all",
                )
            },
        )
        self.assertEqual(upgraded["cohort_selection"]["role_match"], "all")
        self.assertEqual(
            upgraded["must_have_primary_role_buckets"],
            ["engineering", "product_management"],
        )
        self.assertEqual(upgraded["employment_statuses"], ["current", "former"])

    def test_plan_review_mirror_validation_does_not_replace_stored_intent_axes(self) -> None:
        stored = canonicalize_cohort_selection_request_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(roles=["research"], statuses=["current"]),
                "intent_axes": {
                    "population_boundary": {"employment_statuses": ["current"]},
                    "scope_boundary": {
                        "organization_keywords": ["Pre-training"],
                        "confirmed_company_scope": ["Acme Research"],
                    },
                    "thematic_constraints": {
                        "must_have_primary_role_buckets": ["research"],
                        "role_match": "any",
                    },
                },
            }
        )

        merged = merge_plan_review_cohort_selection(
            stored,
            {
                "intent_axes": {
                    "population_boundary": {"employment_statuses": ["current"]},
                }
            },
        )

        self.assertEqual(merged, stored)
        self.assertEqual(
            merged["intent_axes"]["scope_boundary"]["confirmed_company_scope"],
            ["Acme Research"],
        )

    def test_absent_object_preserves_legacy_job_request_shape_and_lazy_adapter(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Acme researchers",
                "target_company": "Acme",
                "employment_statuses": ["current", "former"],
                "must_have_primary_role_buckets": ["research"],
            }
        )
        record = request.to_record()

        self.assertNotIn("cohort_selection", record)
        self.assertEqual(
            effective_cohort_selection(record),
            {
                "schema_version": COHORT_SELECTION_SCHEMA_VERSION,
                "role_bucket_ids": ["research"],
                "employment_statuses": ["current", "former"],
                "role_match": "any",
                "source": "legacy_adapter",
            },
        )
        self.assertNotIn("cohort_selection", record)

    def test_user_explicit_selection_is_signature_bound_while_server_inferred_remains_legacy(self) -> None:
        legacy = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "employment_statuses": ["current"],
                "must_have_primary_role_buckets": ["research", "engineering"],
            }
        ).to_record()
        explicit = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(
                    roles=["engineering", "research"],
                    statuses=["current"],
                ),
            }
        ).to_record()
        server_record = {
            **explicit,
            "cohort_selection": {**explicit["cohort_selection"], "source": "inferred"},
        }
        all_match = {
            **explicit,
            "cohort_selection": {**explicit["cohort_selection"], "role_match": "all"},
        }

        self.assertNotEqual(request_signature(legacy), request_signature(explicit))
        self.assertNotEqual(matching_request_signature(legacy), matching_request_signature(explicit))
        self.assertNotEqual(request_signature(explicit), request_signature(server_record))
        self.assertEqual(request_signature(legacy), request_signature(server_record))
        self.assertNotEqual(request_signature(explicit), request_signature(all_match))

    def test_explicit_cohort_strips_role_like_constraints_but_keeps_thematic_values(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Find engineers at Acme",
                "target_company": "Acme",
                "cohort_selection": _cohort(roles=["research"]),
                "categories": ["investor", "engineering"],
                "must_have_facets": ["training", "safety", "product_management"],
                "intent_axes": {
                    "population_boundary": {
                        "categories": ["investor", "engineering"],
                        "employment_statuses": ["current"],
                    },
                    "thematic_constraints": {
                        "must_have_facets": ["training", "safety", "engineering"],
                        "must_have_primary_role_buckets": ["research"],
                        "role_match": "any",
                    },
                },
            }
        )
        matching = build_request_matching_bundle(request.to_record())["matching_request"]

        self.assertEqual(request.categories, ["investor"])
        self.assertEqual(request.must_have_facets, ["training", "safety"])
        self.assertEqual(request.must_have_primary_role_buckets, ["research"])
        self.assertEqual(matching["categories"], ["investor"])
        self.assertEqual(matching["must_have_facets"], ["training", "safety"])
        self.assertEqual(matching["must_have_primary_role_buckets"], ["research"])
        self.assertEqual(
            matching["cohort_selection_digest"],
            cohort_selection_digest(request.cohort_selection),
        )

    def test_explicit_all_roles_does_not_share_legacy_inferred_role_identity(self) -> None:
        legacy = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "employment_statuses": ["current"],
                "categories": ["engineering"],
            }
        )
        explicit = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(roles=[], statuses=["current"]),
                "categories": ["engineering"],
            }
        )
        legacy_strategy = compile_acquisition_strategy(
            legacy,
            list(legacy.categories),
            list(legacy.employment_statuses),
            RetrievalPlan(strategy="hybrid", reason="test"),
        )
        explicit_strategy = compile_acquisition_strategy(
            explicit,
            list(explicit.categories),
            list(explicit.employment_statuses),
            RetrievalPlan(strategy="hybrid", reason="test"),
        )

        self.assertEqual(legacy_strategy.filter_hints.get("function_ids"), ["8"])
        self.assertNotIn("function_ids", explicit_strategy.filter_hints)
        self.assertNotEqual(request_signature(legacy.to_record()), request_signature(explicit.to_record()))
        self.assertNotEqual(
            matching_request_signature(legacy.to_record()),
            matching_request_signature(explicit.to_record()),
        )

    def test_no_cohort_does_not_add_role_targeting_explanation_field(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "employment_statuses": ["current"],
                "must_have_primary_role_buckets": ["research"],
            }
        )
        strategy = compile_acquisition_strategy(
            request,
            list(request.categories),
            list(request.employment_statuses),
            RetrievalPlan(strategy="hybrid", reason="test"),
        )

        self.assertNotIn("effective_role_targeting", strategy.strategy_decision_explanation)

    def test_no_cohort_execution_payloads_are_byte_compatible_with_pinned_baseline(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Find current AI infrastructure engineers at Acme",
                "target_company": "Acme",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "must_have_primary_role_buckets": ["engineering", "infra_systems"],
                "must_have_facets": ["infrastructure"],
                "top_k": 25,
            }
        )
        strategy = compile_acquisition_strategy(
            request,
            list(request.categories),
            list(request.employment_statuses),
            RetrievalPlan(strategy="hybrid", reason="compatibility"),
        )
        payloads = {
            "record": request.to_record(),
            "effective": build_effective_request_payload(request),
            "matching": build_request_matching_bundle(request.to_record()),
            "strategy": strategy.to_record(),
        }
        expected_sha256 = {
            # Exact compact-JSON bytes from clean pre-Cohort HEAD e04faf8.
            "record": "3ec594d3a9d20acd0df1252d3a16e8230110cd1cbfdd6aaa348cf4ab4d8e76df",
            "effective": "af5ad8c1f45c9a0606444172bd425554d7d2a2e18990589a5b315ef57e9a3e76",
            "matching": "f612b022f2935f8e6eaf4f8c7aa78838b01060ae6b8172a947ac7e86cf1f4eef",
            "strategy": "68106cecfaa0660a9b7228fd2293d5d01691fbfd336d8864cabcbd2313b6bd99",
        }

        self.assertEqual(
            {
                name: hashlib.sha256(
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                for name, payload in payloads.items()
            },
            expected_sha256,
        )

    def test_plan_review_materialization_exact_copies_user_selection(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(
                    roles=["research", "product_management"],
                    statuses=["current", "former"],
                    role_match="all",
                ),
            }
        ).to_record()

        reviewed_request, _ = apply_plan_review_decision(
            request,
            {},
            {"precision_recall_bias": "precision"},
        )

        self.assertEqual(reviewed_request["cohort_selection"], request["cohort_selection"])
        self.assertEqual(
            reviewed_request["must_have_primary_role_buckets"],
            request["cohort_selection"]["role_bucket_ids"],
        )
        self.assertEqual(
            reviewed_request["employment_statuses"],
            request["cohort_selection"]["employment_statuses"],
        )


class CohortSelectionIngressTest(unittest.TestCase):
    def test_model_normalization_cannot_override_user_explicit_selection(self) -> None:
        class _ConflictingModel(DeterministicModelClient):
            calls = 0

            def normalize_request(self, payload):
                self.calls += 1
                return {
                    "employment_statuses": ["former"],
                    "must_have_primary_role_buckets": ["product_management"],
                }

        class _Store:
            @staticmethod
            def bootstrap_candidate_store_loaded() -> bool:
                return False

        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.model_client = _ConflictingModel()
        orchestrator.store = _Store()
        prepared, _ = orchestrator._prepare_request_payload_with_diagnostics(
            {
                "raw_user_request": "Acme technical team",
                "target_company": "Acme",
                "cohort_selection": _cohort(
                    roles=["research", "engineering"],
                    statuses=["current"],
                ),
            }
        )

        self.assertEqual(orchestrator.model_client.calls, 1)
        self.assertEqual(prepared["employment_statuses"], ["current"])
        self.assertEqual(prepared["must_have_primary_role_buckets"], ["research", "engineering"])
        self.assertEqual(prepared["cohort_selection"]["source"], "user_explicit")

    def test_server_inferred_selection_is_not_a_user_authority_lock(self) -> None:
        class _ConflictingModel(DeterministicModelClient):
            def normalize_request(self, payload):
                return {
                    "employment_statuses": ["former"],
                    "must_have_primary_role_buckets": ["product_management"],
                }

        class _Store:
            @staticmethod
            def bootstrap_candidate_store_loaded() -> bool:
                return False

        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.model_client = _ConflictingModel()
        orchestrator.store = _Store()
        prepared, _ = orchestrator._prepare_request_payload_with_diagnostics(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(source="inferred"),
            }
        )

        self.assertEqual(prepared["employment_statuses"], ["former"])
        self.assertIn("product_management", prepared["must_have_primary_role_buckets"])
        self.assertNotIn("cohort_selection", prepared)

    def test_invalid_ingress_returns_before_model_history_review_or_job_writes(self) -> None:
        class _Exploding:
            def __getattr__(self, name):
                raise AssertionError(f"unexpected side effect through {name}")

        payload = {
            "raw_user_request": "Acme",
            "cohort_selection": _cohort(statuses=["contractor"]),
        }
        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.model_client = _Exploding()
        orchestrator.store = _Exploding()

        results = {
            "plan": orchestrator.plan_workflow(payload),
            "explain": orchestrator.explain_workflow(payload),
            "submit": orchestrator.submit_plan_workflow(payload),
            "queue": orchestrator.queue_workflow(payload),
            "hosted": orchestrator.start_workflow(payload),
            "managed": orchestrator.start_workflow_runner_managed(payload),
            "blocking": orchestrator.run_workflow_blocking(payload),
            "run_job": orchestrator.run_job(payload),
        }

        self.assertEqual(
            {name: result["reason"] for name, result in results.items()},
            {name: "cohort_selection_unknown_employment_status" for name in results},
        )

    def test_valid_cohort_execution_gate_precedes_model_store_and_provider_work(self) -> None:
        class _Exploding:
            def __getattr__(self, name):
                raise AssertionError(f"unexpected side effect through {name}")

        payload = {
            "raw_user_request": "Acme researchers",
            "target_company": "Acme",
            "cohort_selection": _cohort(),
        }
        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.model_client = _Exploding()
        orchestrator.store = _Exploding()
        orchestrator.acquisition_engine = _Exploding()

        with patch(
            "sourcing_agent.cohort_provider_compiler.cohort_execution_capability_for_runtime",
            return_value=None,
        ):
            results = {
                "queue": orchestrator.queue_workflow(payload),
                "hosted": orchestrator.start_workflow(payload),
                "managed": orchestrator.start_workflow_runner_managed(payload),
                "blocking": orchestrator.run_workflow_blocking(payload),
                "run_job": orchestrator.run_job(payload),
            }
        self.assertEqual(
            {name: result["reason"] for name, result in results.items()},
            {name: "cohort_selection_execution_not_ready" for name in results},
        )

        class _Strategy:
            filter_hints: dict[str, object] = {}

        class _Plan:
            acquisition_strategy = _Strategy()

        with (
            patch(
                "sourcing_agent.cohort_provider_compiler.cohort_execution_capability_for_runtime",
                return_value=None,
            ),
            self.assertRaisesRegex(
                CohortProviderCompilationError,
                "cohort_selection_execution_not_ready",
            ),
        ):
            orchestrator._run_workflow_from_acquisition(
                "existing-job",
                JobRequest.from_payload(payload),
                _Plan(),
            )

    def test_plan_review_cohort_conflict_returns_before_review_write(self) -> None:
        stored_request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(roles=["research"]),
            }
        ).to_record()

        class _Store:
            @staticmethod
            def get_plan_review_session(review_id):
                return {
                    "review_id": review_id,
                    "status": "pending",
                    "request": stored_request,
                    "plan": {},
                    "execution_bundle": {},
                }

            def __getattr__(self, name):
                raise AssertionError(f"unexpected write through {name}")

        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.store = _Store()
        result = orchestrator.review_plan_session(
            {
                "review_id": 1,
                "action": "approved",
                "decision": {
                    "cohort_selection": _cohort(roles=["engineering"]),
                },
            }
        )

        self.assertEqual(result["status"], "invalid")
        self.assertEqual(result["reason"], "cohort_selection_plan_review_conflict")

        queued = orchestrator.queue_workflow(
            {
                "plan_review_id": 1,
                "cohort_selection": _cohort(roles=["engineering"]),
            }
        )
        self.assertEqual(queued["status"], "invalid")
        self.assertEqual(queued["reason"], "cohort_selection_plan_review_conflict")

    def test_non_approved_review_validates_cohort_before_write(self) -> None:
        class _Store:
            writes = 0

            @staticmethod
            def get_plan_review_session(review_id):
                return {
                    "review_id": review_id,
                    "status": "pending",
                    "request": JobRequest.from_payload(
                        {
                            "target_company": "Acme",
                            "cohort_selection": _cohort(
                                roles=["research"],
                                statuses=["current"],
                            ),
                        }
                    ).to_record(),
                    "plan": {},
                    "execution_bundle": {},
                }

            def review_plan_session(self, **kwargs):
                self.writes += 1
                return kwargs

        store = _Store()
        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.store = store

        result = orchestrator.review_plan_session(
            {
                "review_id": 1,
                "action": "rejected",
                "decision": {
                    "cohort_selection": _cohort(source="inferred"),
                },
            }
        )

        self.assertEqual(result["status"], "invalid")
        self.assertEqual(result["reason"], "cohort_selection_invalid_source")
        self.assertEqual(store.writes, 0)

        conflict_cases = (
            ("rejected", {"employment_statuses": ["former"]}),
            ("needs_changes", {"must_have_primary_role_buckets": ["engineering"]}),
            (
                "rejected",
                {
                    "intent_axes": {
                        "population_boundary": {"employment_statuses": ["former"]},
                    }
                },
            ),
        )
        for action, decision in conflict_cases:
            with self.subTest(action=action, decision=decision):
                conflict = orchestrator.review_plan_session(
                    {
                        "review_id": 1,
                        "action": action,
                        "decision": decision,
                    }
                )
                self.assertEqual(conflict["status"], "invalid")
                self.assertEqual(conflict["reason"], "cohort_selection_plan_review_conflict")
                self.assertEqual(store.writes, 0)

    def test_plan_review_legacy_upgrade_rebuilds_plan_before_atomic_write(self) -> None:
        legacy_request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "employment_statuses": ["current"],
                "must_have_primary_role_buckets": ["research"],
            }
        ).to_record()

        class _Store:
            captured: dict[str, Any] = {}

            @staticmethod
            def get_plan_review_session(review_id):
                return {
                    "review_id": review_id,
                    "status": "pending",
                    "request": legacy_request,
                    "plan": {
                        "acquisition_strategy": {
                            "provider_execution_manifest": {"schema_version": "legacy"},
                        }
                    },
                    "execution_bundle": {},
                }

            def review_plan_session(self, **kwargs):
                self.captured = dict(kwargs)
                return {
                    "review_id": kwargs["review_id"],
                    "status": kwargs["status"],
                    "request": dict(kwargs["request_payload"]),
                    "plan": dict(kwargs["plan_payload"]),
                }

        store = _Store()
        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.store = store
        rebuild_requests: list[dict[str, Any]] = []

        def _rebuild(request):
            rebuild_requests.append(request.to_record())
            return SimpleNamespace(
                to_record=lambda: {
                    "acquisition_strategy": {
                        "company_scope": ["Acme"],
                        "provider_execution_manifest": {
                            "schema_version": "cohort_provider_manifest.v1",
                            "cohort_selection_digest": cohort_selection_digest(request.cohort_selection),
                        },
                    },
                    "acquisition_tasks": [],
                }
            )

        orchestrator._build_augmented_sourcing_plan = _rebuild
        orchestrator._build_execution_bundle = lambda **kwargs: {
            "request": dict(kwargs.get("request") or {}),
            "plan": dict(kwargs.get("plan") or {}),
        }
        orchestrator._plan_acquisition_plan_commit_command_from_review = lambda **kwargs: {}

        result = orchestrator.review_plan_session(
            {
                "review_id": 1,
                "action": "approved",
                "decision": {
                    "cohort_selection": _cohort(
                        roles=["research", "engineering"],
                        statuses=["current", "former"],
                    )
                },
            }
        )

        self.assertEqual(result["status"], "reviewed")
        self.assertEqual(len(rebuild_requests), 1)
        self.assertEqual(
            store.captured["request_payload"]["cohort_selection"]["role_bucket_ids"],
            ["research", "engineering"],
        )
        manifest = store.captured["plan_payload"]["acquisition_strategy"]["provider_execution_manifest"]
        self.assertEqual(manifest["schema_version"], "cohort_provider_manifest.v1")
        self.assertEqual(
            manifest["cohort_selection_digest"],
            cohort_selection_digest(store.captured["request_payload"]["cohort_selection"]),
        )

    def test_plan_review_cohort_rebuild_failure_has_zero_write(self) -> None:
        class _Store:
            writes = 0

            @staticmethod
            def get_plan_review_session(review_id):
                return {
                    "review_id": review_id,
                    "status": "pending",
                    "request": {"target_company": "Acme"},
                    "plan": {},
                    "execution_bundle": {},
                }

            def review_plan_session(self, **kwargs):
                self.writes += 1
                return kwargs

        def _fail_rebuild(_request):
            raise RuntimeError("synthetic rebuild failure")

        store = _Store()
        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.store = store
        orchestrator._build_augmented_sourcing_plan = _fail_rebuild

        result = orchestrator.review_plan_session(
            {
                "review_id": 1,
                "action": "approved",
                "decision": {"cohort_selection": _cohort(roles=["engineering"])},
            }
        )

        self.assertEqual(
            result,
            {
                "status": "invalid",
                "reason": "cohort_selection_plan_rebuild_failed",
            },
        )
        self.assertEqual(store.writes, 0)

    def test_plan_review_bundle_failure_cannot_persist_stale_legacy_bundle(self) -> None:
        class _Store:
            writes = 0

            @staticmethod
            def get_plan_review_session(review_id):
                return {
                    "review_id": review_id,
                    "status": "pending",
                    "request": {
                        "target_company": "Acme",
                        "employment_statuses": ["former"],
                    },
                    "plan": {},
                    "execution_bundle": {
                        "request": {
                            "target_company": "Acme",
                            "employment_statuses": ["former"],
                        },
                        "plan": {"legacy": True},
                    },
                }

            def review_plan_session(self, **kwargs):
                self.writes += 1
                return kwargs

        store = _Store()
        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.store = store
        orchestrator._build_augmented_sourcing_plan = lambda request: SimpleNamespace(
            to_record=lambda: {
                "acquisition_strategy": {
                    "company_scope": [request.target_company],
                    "provider_execution_manifest": {
                        "schema_version": "cohort_provider_manifest.v1",
                        "cohort_selection_digest": cohort_selection_digest(request.cohort_selection),
                    },
                },
                "acquisition_tasks": [],
            }
        )
        orchestrator._build_execution_bundle = lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("synthetic bundle failure")
        )

        result = orchestrator.review_plan_session(
            {
                "review_id": 1,
                "action": "approved",
                "decision": {
                    "cohort_selection": _cohort(
                        roles=["research"],
                        statuses=["current"],
                    )
                },
            }
        )

        self.assertEqual(
            result,
            {
                "status": "invalid",
                "reason": "plan_review_execution_bundle_rebuild_failed",
            },
        )
        self.assertEqual(store.writes, 0)

    def test_run_helpers_reject_external_server_owned_sources_before_side_effects(self) -> None:
        class _Exploding:
            def __getattr__(self, name):
                raise AssertionError(f"unexpected side effect through {name}")

        for source in ("inferred", "legacy_adapter"):
            with self.subTest(source=source):
                orchestrator = object.__new__(SourcingOrchestrator)
                orchestrator.model_client = _Exploding()
                orchestrator.store = _Exploding()
                payload = {
                    "target_company": "Acme",
                    "cohort_selection": _cohort(source=source),
                }
                self.assertEqual(
                    orchestrator.run_job(payload)["reason"],
                    "cohort_selection_invalid_source",
                )
                self.assertEqual(
                    orchestrator.run_workflow_blocking(payload)["reason"],
                    "cohort_selection_invalid_source",
                )

    def test_authoritative_reuse_source_requires_exact_explicit_cohort_identity(self) -> None:
        for roles in ([], ["research"]):
            request = JobRequest.from_payload(
                {
                    "target_company": "Acme",
                    "cohort_selection": _cohort(roles=roles, statuses=["current"]),
                }
            )
            different_request = JobRequest.from_payload(
                {
                    "target_company": "Acme",
                    "cohort_selection": _cohort(
                        roles=["engineering"] if roles != ["engineering"] else ["research"],
                        statuses=["current"],
                    ),
                }
            )
            cases = (
                ("missing", {}, False),
                ("legacy", {"request": {"target_company": "Acme"}}, False),
                ("non_object", {"request": "invalid"}, False),
                (
                    "malformed",
                    {
                        "request": {
                            "target_company": "Acme",
                            "cohort_selection": {},
                        }
                    },
                    False,
                ),
                ("different", {"request": different_request.to_record()}, False),
                ("same", {"request": request.to_record()}, True),
            )
            for name, matched_job, expected in cases:
                with self.subTest(roles=roles, source=name):
                    self.assertEqual(
                        SourcingOrchestrator._matched_job_covers_explicit_cohort(  # noqa: SLF001
                            request_payload=request.to_record(),
                            matched_job=matched_job,
                        ),
                        expected,
                    )

    def test_registry_reuse_rejects_legacy_source_for_nonempty_explicit_cohort(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(roles=["research"], statuses=["current"]),
            }
        )

        class _Store:
            source_job = {"job_id": "legacy", "request": {"target_company": "Acme"}}

            @staticmethod
            def get_authoritative_organization_asset_registry(**_kwargs):
                return {
                    "registry_id": 1,
                    "snapshot_id": "snap",
                    "source_job_id": "legacy",
                    "candidate_count": 2,
                    "current_lane_effective_ready": True,
                    "current_lane_effective_candidate_count": 2,
                    "former_lane_effective_ready": False,
                    "former_lane_effective_candidate_count": 0,
                }

            @staticmethod
            def get_organization_execution_profile(**_kwargs):
                return {}

            def get_job(self, _job_id):
                return dict(self.source_job)

        store = _Store()
        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.store = store
        orchestrator._load_snapshot_reuse_context_from_snapshot = lambda **_kwargs: {
            "snapshot_id": "snap",
            "snapshot_dir": "/tmp/snap",
            "source_path": "/tmp/snap/candidate_documents.json",
        }
        context = {
            "asset_reuse_plan": {
                "baseline_reuse_available": True,
                "baseline_snapshot_id": "snap",
                "baseline_current_effective_ready": True,
                "baseline_current_effective_candidate_count": 2,
                "baseline_former_effective_ready": False,
                "baseline_former_effective_candidate_count": 0,
            }
        }

        self.assertEqual(
            orchestrator._resolve_organization_asset_registry_snapshot_reuse_match(  # noqa: SLF001
                request,
                context,
            ),
            {},
        )
        store.source_job = {
            "job_id": "same",
            "status": "completed",
            "request": request.to_record(),
        }
        exact_match = orchestrator._resolve_organization_asset_registry_snapshot_reuse_match(  # noqa: SLF001
            request,
            context,
        )
        self.assertEqual(exact_match["strategy"], "reuse_snapshot")

    def test_projection_reuse_rejects_legacy_source_for_all_role_explicit_cohort(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(roles=[], statuses=["current"]),
            }
        )
        projection = {
            "projection_id": "proj",
            "projection_type": "run_scope_projection",
            "state": "serving",
            "source_run_id": "legacy",
            "scope_spec": {
                "target_scope": "full_company_asset",
                "keywords": [],
                "snapshot_id": "snap",
            },
            "counts": {"candidate_count": 2},
            "readiness": {"row": "complete", "profile": "complete", "card": "complete"},
            "metadata": {"source_path": "/tmp/snap/candidate_documents.json"},
        }

        class _ServingProjectionRepo:
            @staticmethod
            def get_authoritative_pointer(_collection_id):
                return {"state": "active", "active_projection_id": "proj"}

            @staticmethod
            def get(_projection_id):
                return dict(projection)

            @staticmethod
            def list(**_kwargs):
                return []

        class _Store:
            source_job = {"job_id": "legacy", "request": {"target_company": "Acme"}}
            repos = SimpleNamespace(serving_projection=_ServingProjectionRepo())

            def get_job(self, _job_id):
                return dict(self.source_job)

        store = _Store()
        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.store = store
        orchestrator._projection_collection_id_for_request = lambda _request: "company:acme"
        orchestrator._load_snapshot_reuse_context_from_snapshot = lambda **_kwargs: {
            "snapshot_id": "snap",
            "snapshot_dir": "/tmp/snap",
            "source_path": "/tmp/snap/candidate_documents.json",
        }

        self.assertEqual(
            orchestrator._resolve_collection_authoritative_projection_reuse_match(  # noqa: SLF001
                request,
                {},
            ),
            {},
        )
        store.source_job = {
            "job_id": "same",
            "status": "completed",
            "request": request.to_record(),
        }
        exact_match = orchestrator._resolve_collection_authoritative_projection_reuse_match(  # noqa: SLF001
            request,
            {},
        )
        self.assertEqual(exact_match["strategy"], "reuse_completed")

    def test_idempotency_candidate_cannot_cross_explicit_cohort_identity(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(roles=["research"], statuses=["current"]),
            }
        )
        different_request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(roles=["engineering"], statuses=["current"]),
            }
        ).to_record()

        class _Store:
            matched_job: dict[str, Any] = {}

            def find_latest_job_by_idempotency_key(self, **_kwargs):
                return dict(self.matched_job)

        store = _Store()
        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.store = store
        context = {
            "dispatch_enabled": True,
            "idempotency_key": "same-key",
            "allow_join_inflight": False,
            "allow_result_reuse": False,
        }
        candidates = {
            "missing": {},
            "legacy": {"target_company": "Acme"},
            "different": different_request,
            "malformed": {"target_company": "Acme", "cohort_selection": {}},
            "same": request.to_record(),
        }
        for name, source_request in candidates.items():
            with self.subTest(source=name):
                store.matched_job = {
                    "job_id": f"job-{name}",
                    "status": "completed",
                    "request": source_request,
                }
                decision = orchestrator._resolve_query_dispatch_decision(  # noqa: SLF001
                    request,
                    context,
                )
                self.assertEqual(
                    decision["strategy"],
                    "reuse_completed" if name == "same" else "new_job",
                )

        legacy_request = JobRequest.from_payload({"target_company": "Acme"})
        store.matched_job = {
            "job_id": "job-legacy-contract",
            "status": "completed",
            "request": {"target_company": "Acme", "keywords": ["different"]},
        }
        legacy_decision = orchestrator._resolve_query_dispatch_decision(  # noqa: SLF001
            legacy_request,
            context,
        )
        self.assertEqual(legacy_decision["strategy"], "reuse_completed")

    def test_asset_reuse_plan_requires_exact_source_job_cohort_identity(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(roles=["research"], statuses=["current"]),
            }
        )
        different_request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(roles=["engineering"], statuses=["current"]),
            }
        ).to_record()
        source_requests = {
            "legacy": {"target_company": "Acme"},
            "different": different_request,
            "malformed": {"target_company": "Acme", "cohort_selection": {}},
            "same": request.to_record(),
        }

        for source_kind in ("missing", "legacy", "different", "malformed", "same"):
            with self.subTest(source=source_kind):
                source_job_id = "" if source_kind == "missing" else "source-job"
                registry_row = {
                    "registry_id": 1,
                    "snapshot_id": "snap",
                    "source_job_id": source_job_id,
                    "authoritative": True,
                }
                reads: list[str] = []
                store = SimpleNamespace(
                    get_job=lambda job_id: (
                        reads.append(job_id)
                        or {
                            "job_id": job_id,
                            "request": source_requests.get(source_kind, {}),
                        }
                    )
                )
                compiled = {
                    "baseline_reuse_available": True,
                    "baseline_snapshot_id": "snap",
                }
                with (
                    patch(
                        "sourcing_agent.asset_reuse_planning.build_organization_asset_registry_candidate_inventory",
                        return_value={
                            "authoritative_row": registry_row,
                            "ordered_candidate_rows": [registry_row],
                        },
                    ),
                    patch(
                        "sourcing_agent.asset_reuse_planning._compile_asset_reuse_plan_for_baseline",
                        return_value=compiled,
                    ) as compile_baseline,
                ):
                    result = compile_asset_reuse_plan(
                        runtime_dir="/tmp",
                        store=store,
                        request=request,
                        plan=SimpleNamespace(),
                    )

                if source_kind == "same":
                    self.assertEqual(result, compiled)
                    compile_baseline.assert_called_once()
                    self.assertEqual(reads, ["source-job"])
                else:
                    self.assertEqual(
                        result,
                        {
                            "baseline_reuse_available": False,
                            "reason": "no_cohort_compatible_authoritative_baseline",
                        },
                    )
                    compile_baseline.assert_not_called()
                    self.assertEqual(reads, [] if source_kind == "missing" else ["source-job"])

    def test_inherited_force_fresh_is_not_suppressed_by_legacy_cohort_registry(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "execution_preferences": {"force_fresh_run": True},
                "cohort_selection": _cohort(roles=["research"], statuses=["current"]),
            }
        )

        class _Store:
            source_job = {
                "job_id": "source-job",
                "request": {"target_company": "Acme"},
            }

            @staticmethod
            def get_authoritative_organization_asset_registry(**_kwargs):
                return {
                    "snapshot_id": "snap",
                    "source_job_id": "source-job",
                    "current_lane_effective_ready": True,
                    "current_lane_effective_candidate_count": 2,
                }

            def get_job(self, _job_id):
                return dict(self.source_job)

        store = _Store()
        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.store = store
        orchestrator._requested_dispatch_lane_requirements = lambda _request: {
            "need_current": True,
            "need_former": False,
            "employment_statuses": ["current"],
        }

        stale_plan_request, stale_plan_reason = orchestrator._maybe_suppress_inherited_force_fresh_run(  # noqa: SLF001
            payload={},
            request=request,
            asset_reuse_plan={
                "baseline_reuse_available": True,
                "baseline_snapshot_id": "stale-snap",
                "baseline_current_effective_ready": True,
                "baseline_current_effective_candidate_count": 2,
            },
            runtime_execution_mode="hosted",
        )
        self.assertTrue(stale_plan_request.execution_preferences["force_fresh_run"])
        self.assertEqual(stale_plan_reason, {})

        preserved_request, preserved_reason = orchestrator._maybe_suppress_inherited_force_fresh_run(  # noqa: SLF001
            payload={},
            request=request,
            asset_reuse_plan={},
            runtime_execution_mode="hosted",
        )
        self.assertTrue(preserved_request.execution_preferences["force_fresh_run"])
        self.assertEqual(preserved_reason, {})

        store.source_job = {
            "job_id": "source-job",
            "request": request.to_record(),
        }
        suppressed_request, suppressed_reason = orchestrator._maybe_suppress_inherited_force_fresh_run(  # noqa: SLF001
            payload={},
            request=request,
            asset_reuse_plan={
                "baseline_reuse_available": True,
                "baseline_snapshot_id": "snap",
                "baseline_source_job_id": "source-job",
                "baseline_current_effective_ready": True,
                "baseline_current_effective_candidate_count": 2,
            },
            runtime_execution_mode="hosted",
        )
        self.assertNotIn("force_fresh_run", suppressed_request.execution_preferences)
        self.assertEqual(suppressed_reason["baseline_snapshot_id"], "snap")

    def test_refinement_model_cannot_mutate_explicit_cohort(self) -> None:
        class _ConflictingRefinementModel(DeterministicModelClient):
            def normalize_refinement_instruction(self, payload):
                return {
                    "patch": {
                        "employment_statuses": ["former"],
                        "must_have_primary_role_buckets": ["product_management"],
                        "categories": ["engineering"],
                        "must_have_facets": ["product_management", "training"],
                        "top_k": 3,
                    }
                }

        base_request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(
                    roles=["research", "engineering"],
                    statuses=["current"],
                ),
            }
        ).to_record()
        compiled = compile_refinement_patch_from_instruction(
            instruction="只看 former product managers，top 3",
            base_request=base_request,
            model_client=_ConflictingRefinementModel(),
        )

        self.assertEqual(compiled["request_patch"]["top_k"], 3)
        self.assertNotIn("employment_statuses", compiled["request_patch"])
        self.assertNotIn("must_have_primary_role_buckets", compiled["request_patch"])
        self.assertNotIn("categories", compiled["request_patch"])
        self.assertEqual(compiled["request_patch"]["must_have_facets"], ["training"])
        self.assertEqual(compiled["merged_request"]["employment_statuses"], ["current"])
        self.assertEqual(
            compiled["merged_request"]["must_have_primary_role_buckets"],
            ["research", "engineering"],
        )
        self.assertEqual(
            compiled["instruction_compiler"]["protected_cohort_fields"],
            [
                "categories",
                "employment_statuses",
                "must_have_facets",
                "must_have_primary_role_buckets",
            ],
        )

    def test_explicit_flat_refinement_conflict_is_rejected_before_writes(self) -> None:
        base_request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(statuses=["current"]),
            }
        ).to_record()

        class _Store:
            def get_job(self, job_id):
                return {
                    "job_id": job_id,
                    "status": "completed",
                    "request": base_request,
                }

            def __getattr__(self, name):
                raise AssertionError(f"unexpected write through {name}")

        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.store = _Store()
        result = orchestrator.compile_post_acquisition_refinement(
            {
                "job_id": "job-1",
                "request_patch": {"employment_statuses": ["former"]},
            }
        )

        self.assertEqual(result["status"], "invalid")
        self.assertEqual(result["reason"], "cohort_selection_mirror_conflict")
        self.assertEqual(result["field"], "employment_statuses")

        for field, value in (
            ("categories", ["engineering"]),
            ("must_have_facets", ["product_management"]),
        ):
            with self.subTest(field=field):
                role_conflict = orchestrator.compile_post_acquisition_refinement(
                    {
                        "job_id": "job-1",
                        "request_patch": {field: value},
                    }
                )
                self.assertEqual(role_conflict["status"], "invalid")
                self.assertEqual(role_conflict["reason"], "cohort_selection_mirror_conflict")
                self.assertEqual(role_conflict["field"], field)


class CohortSelectionApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.previous_tokens = os.environ.pop("SOURCING_API_BEARER_TOKENS", None)

        class _ApiOrchestrator:
            calls = {
                "plan": 0,
                "explain": 0,
                "workflow": 0,
                "criteria_feedback": 0,
                "criteria_confidence": 0,
                "criteria_recompile": 0,
            }
            received_payloads = {}

            def submit_plan_workflow(self, payload):
                self.calls["plan"] += 1
                self.received_payloads["plan"] = dict(payload)
                return {"status": "queued"}

            def explain_workflow(self, payload):
                self.calls["explain"] += 1
                self.received_payloads["explain"] = dict(payload)
                return {"status": "ready"}

            def start_workflow(self, payload):
                self.calls["workflow"] += 1
                self.received_payloads["workflow"] = dict(payload)
                return {"status": "queued", "job_id": "job-1"}

            def review_plan_session(self, payload):
                return {
                    "status": "invalid",
                    "reason": "cohort_selection_plan_review_conflict",
                }

            def record_criteria_feedback(self, payload, **_owner):
                self.calls["criteria_feedback"] += 1
                self.received_payloads["criteria_feedback"] = dict(payload)
                return {"status": "recorded"}

            def configure_confidence_policy(self, payload):
                self.calls["criteria_confidence"] += 1
                self.received_payloads["criteria_confidence"] = dict(payload)
                return {"status": "configured"}

            def recompile_criteria(self, payload, **_owner):
                self.calls["criteria_recompile"] += 1
                self.received_payloads["criteria_recompile"] = dict(payload)
                return {"status": "recompiled"}

        self.orchestrator = _ApiOrchestrator()
        self.server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"
        self.opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)
        if self.previous_tokens is not None:
            os.environ["SOURCING_API_BEARER_TOKENS"] = self.previous_tokens

    def _request(self, path: str, *, method: str = "GET", body: dict | None = None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Content-Type": "application/json"} if body is not None else {}
        request = urllib_request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with self.opener.open(request, timeout=10) as response:
                return response.status, json.loads(response.read())
        except HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_options_endpoint_and_all_public_ingress_conflicts_are_pre_handler_400(self) -> None:
        status, options = self._request("/api/cohort-selection/options")
        self.assertEqual(status, 200)
        self.assertEqual(options, cohort_selection_options_payload())

        for path in ("/api/plan/submit", "/api/workflows/explain", "/api/workflows"):
            with self.subTest(path=path):
                status, result = self._request(
                    path,
                    method="POST",
                    body={
                        "raw_user_request": "Acme",
                        "cohort_selection": _cohort(statuses=["current"]),
                        "employment_statuses": ["former"],
                    },
                )
                self.assertEqual(status, 400)
                self.assertEqual(result["reason"], "cohort_selection_mirror_conflict")
        self.assertEqual(
            self.orchestrator.calls,
            {
                "plan": 0,
                "explain": 0,
                "workflow": 0,
                "criteria_feedback": 0,
                "criteria_confidence": 0,
                "criteria_recompile": 0,
            },
        )

    def test_criteria_ingress_rejects_invalid_nested_cohort_before_handlers(self) -> None:
        endpoints = (
            ("/api/criteria/feedback", "criteria_feedback"),
            ("/api/criteria/confidence-policy", "criteria_confidence"),
            ("/api/criteria/recompile", "criteria_recompile"),
        )
        invalid_requests = (
            (
                {
                    "target_company": "OpenAI",
                    "cohort_selection": _cohort(source="inferred"),
                },
                "cohort_selection_invalid_source",
            ),
            (
                {
                    "target_company": "OpenAI",
                    "cohort_selection": _cohort(statuses=["current"]),
                    "employment_statuses": ["former"],
                },
                "cohort_selection_mirror_conflict",
            ),
        )

        for path, call_name in endpoints:
            for request_payload, expected_reason in invalid_requests:
                with self.subTest(path=path, reason=expected_reason):
                    status, result = self._request(
                        path,
                        method="POST",
                        body={"request_payload": request_payload},
                    )
                    self.assertEqual(status, 400)
                    self.assertEqual(result["reason"], expected_reason)
            self.assertEqual(self.orchestrator.calls[call_name], 0)

    def test_criteria_ingress_canonicalizes_valid_nested_cohort_before_handlers(self) -> None:
        endpoints = (
            ("/api/criteria/feedback", "criteria_feedback", 201),
            ("/api/criteria/confidence-policy", "criteria_confidence", 200),
            ("/api/criteria/recompile", "criteria_recompile", 200),
        )
        for path, call_name, expected_status in endpoints:
            with self.subTest(path=path):
                status, _ = self._request(
                    path,
                    method="POST",
                    body={
                        "request": {
                            "target_company": "OpenAI",
                            "cohort_selection": _cohort(
                                roles=["engineering", "research"],
                                statuses=["former", "current"],
                            ),
                        }
                    },
                )
                self.assertEqual(status, expected_status)
                received = self.orchestrator.received_payloads[call_name]
                self.assertNotIn("request", received)
                self.assertEqual(
                    received["request_payload"]["must_have_primary_role_buckets"],
                    ["research", "engineering"],
                )
                self.assertEqual(
                    received["request_payload"]["employment_statuses"],
                    ["current", "former"],
                )

    def test_valid_public_ingress_installs_canonical_mirrors_before_handler(self) -> None:
        status, _ = self._request(
            "/api/workflows/explain",
            method="POST",
            body={
                "raw_user_request": "Acme",
                "cohort_selection": _cohort(
                    roles=["engineering", "research"],
                    statuses=["former", "current"],
                ),
            },
        )

        self.assertEqual(status, 200)
        received = self.orchestrator.received_payloads["explain"]
        self.assertEqual(
            received["must_have_primary_role_buckets"],
            ["research", "engineering"],
        )
        self.assertEqual(received["employment_statuses"], ["current", "former"])

    def test_plan_review_invalid_result_maps_to_http_400(self) -> None:
        status, result = self._request(
            "/api/plan/review",
            method="POST",
            body={"review_id": 1, "action": "approved"},
        )

        self.assertEqual(status, 400)
        self.assertEqual(result["reason"], "cohort_selection_plan_review_conflict")
