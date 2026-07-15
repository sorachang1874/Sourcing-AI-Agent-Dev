from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sourcing_agent.cohort_provider_compiler import (
    COHORT_CANONICAL_PROFILE_URL_FIELD,
    COHORT_PUBLIC_HEADLINE_SOURCE,
    CohortExecutionCapability,
    CohortHeadlineRoleProofVerifier,
    CohortProviderCompilationError,
    CohortProviderCompiler,
    canonical_cohort_profile_url,
    cohort_execution_capability_for_runtime,
)
from sourcing_agent.harvest_connectors import (
    HarvestProfileSearchConnector,
    parse_harvest_search_rows,
)
from sourcing_agent.runtime_environment import RuntimeEnvironment
from sourcing_agent.settings import HarvestActorSettings


def _cohort_request(*, role_match: str = "any") -> dict[str, object]:
    return {
        "target_company": "Acme",
        "cohort_selection": {
            "schema_version": "cohort_selection.v1",
            "role_bucket_ids": ["research", "engineering"],
            "employment_statuses": ["current"],
            "role_match": role_match,
            "source": "user_explicit",
        },
    }


def _write_runtime_environment(runtime_dir: Path, *, provider_mode: str, environment: str) -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / ".scripted-local-postgres.env").write_text(
        "\n".join(
            [
                f"SOURCING_EXTERNAL_PROVIDER_MODE={provider_mode}",
                f"SOURCING_RUNTIME_ENVIRONMENT={environment}",
                "",
            ]
        ),
        encoding="utf-8",
    )


class CohortProviderRuntimeSafetyTest(unittest.TestCase):
    def test_capability_rejects_empty_runtime_namespace(self) -> None:
        with self.assertRaisesRegex(
            CohortProviderCompilationError,
            r"cohort_execution_capability_invalid:\s*runtime_namespace",
        ):
            CohortExecutionCapability(
                policy_revision="test.unbound-runtime.v1",
                provider_mode="simulate",
                runtime_namespace="",
            )

    def test_capability_binds_validated_nonlive_mode_and_runtime_namespace(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            _write_runtime_environment(runtime_dir, provider_mode="scripted", environment="scripted")

            capability = cohort_execution_capability_for_runtime(runtime_dir=runtime_dir)

            self.assertIsNotNone(capability)
            assert capability is not None
            self.assertEqual(capability.provider_mode, "scripted")
            self.assertEqual(capability.runtime_namespace, str(runtime_dir.resolve()))
            self.assertEqual(
                CohortExecutionCapability.from_record(capability.to_record()),
                capability,
            )

    def test_capability_is_not_issued_for_missing_runtime_namespace(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            missing_runtime = Path(tempdir) / "missing-scripted-runtime"
            scripted_runtime = RuntimeEnvironment(
                name="scripted",
                provider_mode="scripted",
                runtime_dir=missing_runtime,
            )
            with patch(
                "sourcing_agent.cohort_provider_compiler.current_runtime_environment",
                return_value=scripted_runtime,
            ):
                capability = cohort_execution_capability_for_runtime(runtime_dir=missing_runtime)

            self.assertIsNone(capability)
            self.assertFalse(missing_runtime.exists())

    def test_production_nonlive_runtime_never_receives_capability_even_with_override(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "production-runtime"
            production_scripted = RuntimeEnvironment(
                name="production",
                provider_mode="scripted",
                runtime_dir=runtime_dir,
            )
            with (
                patch.dict(os.environ, {"SOURCING_ALLOW_PRODUCTION_NONLIVE_PROVIDER": "1"}),
                patch(
                    "sourcing_agent.cohort_provider_compiler.current_runtime_environment",
                    return_value=production_scripted,
                ),
            ):
                capability = cohort_execution_capability_for_runtime(runtime_dir=runtime_dir)

            self.assertIsNone(capability)
            self.assertFalse(runtime_dir.exists())

    def test_nonisolated_bound_runtime_fails_before_discovery_write_or_provider_call(self) -> None:
        class _CountingConnector(HarvestProfileSearchConnector):
            calls = 0

            def search_profiles(self, **_kwargs):
                self.calls += 1
                raise AssertionError("non-isolated runtime must fail before the first lane call")

        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            _write_runtime_environment(runtime_dir, provider_mode="scripted", environment="local_dev")
            capability = CohortExecutionCapability(
                policy_revision="test.nonisolated-runtime.v1",
                provider_mode="scripted",
                runtime_namespace=str(runtime_dir),
            )
            manifest = CohortProviderCompiler().compile(
                _cohort_request(),
                execution_capability=capability,
            )
            discovery_dir = runtime_dir / "not-created-cohort-discovery"
            connector = _CountingConnector(HarvestActorSettings())

            with self.assertRaisesRegex(
                CohortProviderCompilationError,
                "cohort_execution_runtime_mismatch",
            ):
                connector.search_profiles_for_cohort_manifest(
                    manifest=manifest,
                    execution_capability=capability,
                    discovery_dir=discovery_dir,
                )

            self.assertEqual(connector.calls, 0)
            self.assertFalse(discovery_dir.exists())

    def test_mode_drift_fails_before_discovery_write_or_provider_call(self) -> None:
        class _CountingConnector(HarvestProfileSearchConnector):
            calls = 0

            def search_profiles(self, **_kwargs):
                self.calls += 1
                raise AssertionError("runtime mismatch must fail before the first lane call")

        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            _write_runtime_environment(runtime_dir, provider_mode="scripted", environment="scripted")
            capability = cohort_execution_capability_for_runtime(runtime_dir=runtime_dir)
            assert capability is not None
            manifest = CohortProviderCompiler().compile(
                _cohort_request(),
                execution_capability=capability,
            )
            discovery_dir = runtime_dir / "not-created-cohort-discovery"
            _write_runtime_environment(runtime_dir, provider_mode="live", environment="production")
            connector = _CountingConnector(HarvestActorSettings(enabled=True, api_token="test", actor_id="actor"))

            with self.assertRaisesRegex(
                CohortProviderCompilationError,
                "cohort_execution_runtime_mismatch",
            ):
                connector.search_profiles_for_cohort_manifest(
                    manifest=manifest,
                    execution_capability=capability,
                    discovery_dir=discovery_dir,
                )

            self.assertEqual(connector.calls, 0)
            self.assertFalse(discovery_dir.exists())

    def test_boundary_threads_exact_runtime_binding_into_every_lane(self) -> None:
        captured: list[dict[str, object]] = []

        class _CapturingConnector(HarvestProfileSearchConnector):
            def search_profiles(self, **kwargs):
                captured.append(dict(kwargs))
                return {"rows": [{"candidate_id": f"candidate-{len(captured)}"}]}

        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            _write_runtime_environment(runtime_dir, provider_mode="scripted", environment="scripted")
            capability = cohort_execution_capability_for_runtime(runtime_dir=runtime_dir)
            assert capability is not None
            manifest = CohortProviderCompiler().compile(
                _cohort_request(),
                execution_capability=capability,
            )

            result = _CapturingConnector(HarvestActorSettings()).search_profiles_for_cohort_manifest(
                manifest=manifest,
                execution_capability=capability,
                discovery_dir=runtime_dir / "cohort-discovery",
            )

            self.assertEqual(result["candidate_count"], 2)
            self.assertEqual(len(captured), 2)
            self.assertEqual({item["required_provider_mode"] for item in captured}, {"scripted"})
            self.assertEqual(
                {item["required_runtime_namespace"] for item in captured},
                {str(runtime_dir.resolve())},
            )

    def test_snapshot_cache_is_mode_namespaced_and_requires_exact_provenance(self) -> None:
        settings = HarvestActorSettings(enabled=True, api_token="test", actor_id="actor")
        connector = HarvestProfileSearchConnector(settings)
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir) / "runtime"
            discovery_dir = runtime_dir / "cohort-lane"
            _write_runtime_environment(runtime_dir, provider_mode="scripted", environment="scripted")
            scripted_body = [
                {
                    "headline": "Research Scientist",
                    "publicIdentifier": "scripted-person",
                }
            ]
            with patch(
                "sourcing_agent.harvest_connectors._run_harvest_actor",
                return_value=scripted_body,
            ) as scripted_run:
                scripted = connector.search_profiles(
                    query_text="",
                    filter_hints={"current_companies": ["Acme"]},
                    employment_status="current",
                    discovery_dir=discovery_dir,
                    allow_shared_provider_cache=False,
                    auto_probe=False,
                    dispatch_mode="async_only",
                    strict_result_envelope=True,
                    required_provider_mode="scripted",
                    required_runtime_namespace=str(runtime_dir),
                )
            assert scripted is not None
            scripted_run.assert_called_once()
            scripted_raw_path = Path(scripted["raw_path"])
            self.assertIn("runtime_namespaces", scripted_raw_path.parts)
            self.assertTrue(any(part.startswith("scripted-") for part in scripted_raw_path.parts))
            request_path = scripted_raw_path.with_name(f"{scripted_raw_path.stem}.request.json")
            request_manifest = json.loads(request_path.read_text(encoding="utf-8"))
            request_context = request_manifest["request_context"]
            self.assertEqual(request_context["cohort_required_provider_mode"], "scripted")
            self.assertEqual(request_context["cohort_runtime_namespace"], str(runtime_dir.resolve()))

            request_context["cohort_required_provider_mode"] = "replay"
            request_path.write_text(json.dumps(request_manifest), encoding="utf-8")
            refreshed_body = [
                {
                    "headline": "Research Engineer",
                    "publicIdentifier": "refreshed-person",
                }
            ]
            with patch(
                "sourcing_agent.harvest_connectors._run_harvest_actor",
                return_value=refreshed_body,
            ) as refresh_run:
                refreshed = connector.search_profiles(
                    query_text="",
                    filter_hints={"current_companies": ["Acme"]},
                    employment_status="current",
                    discovery_dir=discovery_dir,
                    allow_shared_provider_cache=False,
                    auto_probe=False,
                    dispatch_mode="async_only",
                    strict_result_envelope=True,
                    required_provider_mode="scripted",
                    required_runtime_namespace=str(runtime_dir),
                )
            assert refreshed is not None
            refresh_run.assert_called_once()
            self.assertEqual(refreshed["rows"][0]["username"], "refreshed-person")

            _write_runtime_environment(runtime_dir, provider_mode="live", environment="production")
            live_body = [
                {
                    "headline": "Research Engineer",
                    "publicIdentifier": "live-person",
                }
            ]
            with patch(
                "sourcing_agent.harvest_connectors._run_harvest_actor",
                return_value=live_body,
            ) as live_run:
                live = connector.search_profiles(
                    query_text="",
                    filter_hints={"current_companies": ["Acme"]},
                    employment_status="current",
                    discovery_dir=discovery_dir,
                    allow_shared_provider_cache=False,
                    auto_probe=False,
                    dispatch_mode="async_only",
                    strict_result_envelope=True,
                )
            assert live is not None
            live_run.assert_called_once()
            self.assertEqual(live["rows"][0]["username"], "live-person")
            self.assertNotEqual(Path(live["raw_path"]), scripted_raw_path)

    def test_all_role_proof_accepts_only_exact_public_headline_source(self) -> None:
        verifier = CohortHeadlineRoleProofVerifier()
        synthesized = parse_harvest_search_rows(
            [
                {
                    "occupation": "Research Scientist",
                    "position": "Software Engineer",
                    "currentPositions": [{"title": "Research Engineer", "company": "Acme"}],
                    "publicIdentifier": "synthesized-role",
                }
            ]
        )[0]
        self.assertTrue(synthesized["headline"])
        self.assertEqual(synthesized["public_headline"], "")
        self.assertIsNone(verifier.verify(synthesized))

        malformed = parse_harvest_search_rows(
            [{"headline": {"text": "Research Engineer"}, "publicIdentifier": "malformed-headline"}]
        )[0]
        self.assertEqual(malformed["public_headline"], "")
        self.assertIsNone(verifier.verify(malformed))

        exact = parse_harvest_search_rows(
            [
                {
                    "headline": "Research Scientist and Software Engineer",
                    "publicIdentifier": "exact-headline",
                }
            ]
        )[0]
        self.assertEqual(exact["public_headline_source"], COHORT_PUBLIC_HEADLINE_SOURCE)
        proof = verifier.verify(exact)
        self.assertIsNotNone(proof)
        assert proof is not None
        self.assertEqual(proof.role_bucket_ids, ("research", "engineering"))
        self.assertIsNone(
            verifier.verify(
                {
                    "public_headline": "Research Scientist and Software Engineer",
                    "public_headline_source": "occupation",
                }
            )
        )

    def test_lane_validation_replaces_forged_canonical_profile_url(self) -> None:
        row = parse_harvest_search_rows(
            [
                {
                    "url": "https://example.com/not-linkedin",
                    "publicIdentifier": "canonical-person",
                }
            ]
        )[0]
        row[COHORT_CANONICAL_PROFILE_URL_FIELD] = "https://linkedin.com/in/forged"

        validated = CohortProviderCompiler.validate_lane_result_rows(
            [row],
            lane_id="cohort-current-research",
        )[0]

        expected = "https://linkedin.com/in/canonical-person"
        self.assertEqual(canonical_cohort_profile_url(row), expected)
        self.assertEqual(validated[COHORT_CANONICAL_PROFILE_URL_FIELD], expected)
        self.assertEqual(validated["profile_url"], "https://example.com/not-linkedin")


if __name__ == "__main__":
    unittest.main()
