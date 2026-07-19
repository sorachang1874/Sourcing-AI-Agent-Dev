"""FT1 location sibling-field request contract tests (FT0 v2 §7; §10.1 rows 6-8, 10).

Covers the sibling request fields ``target_locations``/``exclude_target_locations``
(never part of the closed five-field cohort object): fail-closed ingress
validation with HTTP 400 before any write, the explicit-Cohort US default
injection, the single-writer rule, signature/manifest identity, and lane
composition, plus the cohort sufficiency decision contract (contract-only;
runtime wiring is NOT FT1).  All fake/scripted: zero provider/model/network/
PG calls.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
import unittest.mock
from pathlib import Path
from urllib import request as urllib_request
from urllib.error import HTTPError

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.acquisition_strategy import DEFAULT_PRIMARY_LOCATION, compile_acquisition_strategy
from sourcing_agent.api import create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.cohort_provider_compiler import CohortProviderCompiler
from sourcing_agent.cohort_selection import (
    CohortSelectionValidationError,
    canonicalize_cohort_selection_request_payload,
    cohort_selection_digest,
)
from sourcing_agent.criteria_request_provenance import prepare_criteria_write_payload
from sourcing_agent.domain import JobRequest, RetrievalPlan
from sourcing_agent.local_postgres import resolve_control_plane_postgres_dsn
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.planning import build_sourcing_plan
from sourcing_agent.request_matching import (
    _normalized_effective_request_payload,
    _normalized_request_payload,
    matching_request_family_signature,
    matching_request_signature,
    request_family_signature,
    request_signature,
)
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONTRACT_DOC = _REPO_ROOT / "docs" / "COHORT_SELECTION_CONTRACT.md"


def _cohort(*, roles: list[str] | None = None, statuses: list[str] | None = None) -> dict:
    return {
        "schema_version": "cohort_selection.v1",
        "role_bucket_ids": list(roles if roles is not None else ["research"]),
        "employment_statuses": list(statuses if statuses is not None else ["current"]),
        "role_match": "any",
        "source": "user_explicit",
    }


def _strategy_for(request: JobRequest):
    return compile_acquisition_strategy(
        request,
        list(request.categories),
        list(request.employment_statuses),
        RetrievalPlan(strategy="hybrid", reason="location-contract-test"),
    )


class LocationFieldValidationTest(unittest.TestCase):
    """Matrix row 6 (validation): fail-closed types/bounds, to_record round-trip."""

    def test_valid_values_are_trimmed_deduped_and_order_preserved(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "target_locations": [" United States ", "united states", "Germany"],
                "exclude_target_locations": ["France"],
            }
        )
        self.assertEqual(request.target_locations, ["United States", "Germany"])
        self.assertEqual(request.exclude_target_locations, ["France"])
        record = request.to_record()
        self.assertEqual(record["target_locations"], ["United States", "Germany"])
        self.assertEqual(record["exclude_target_locations"], ["France"])
        # Round-trip: the location fields re-hydrate to the identical values.
        rehydrated = JobRequest.from_payload(record)
        self.assertEqual(rehydrated.target_locations, ["United States", "Germany"])
        self.assertEqual(rehydrated.exclude_target_locations, ["France"])

    def test_absent_fields_stay_absent_and_legacy_record_is_byte_compatible(self) -> None:
        request = JobRequest.from_payload({"target_company": "Acme", "employment_statuses": ["current"]})
        self.assertIsNone(request.target_locations)
        self.assertIsNone(request.exclude_target_locations)
        record = request.to_record()
        self.assertNotIn("target_locations", record)
        self.assertNotIn("exclude_target_locations", record)

    def test_explicit_empty_list_is_a_present_user_value(self) -> None:
        request = JobRequest.from_payload({"target_company": "Acme", "target_locations": []})
        self.assertEqual(request.target_locations, [])
        self.assertIn("target_locations", request.to_record())
        self.assertEqual(request.to_record()["target_locations"], [])

    def test_invalid_values_fail_closed_with_exact_codes(self) -> None:
        cases = [
            ({"target_locations": "United States"}, "request_location_invalid_type", "target_locations"),
            ({"target_locations": {"city": "SF"}}, "request_location_invalid_type", "target_locations"),
            ({"target_locations": 7}, "request_location_invalid_type", "target_locations"),
            ({"target_locations": [None]}, "request_location_invalid_item", "target_locations"),
            ({"target_locations": [42]}, "request_location_invalid_item", "target_locations"),
            ({"target_locations": [["United States"]]}, "request_location_invalid_item", "target_locations"),
            ({"target_locations": ["United States"] * 17}, "request_location_too_many_items", "target_locations"),
            ({"target_locations": ["x" * 241]}, "request_location_item_length_invalid", "target_locations"),
            ({"target_locations": ["   "]}, "request_location_item_length_invalid", "target_locations"),
            ({"exclude_target_locations": "France"}, "request_location_invalid_type", "exclude_target_locations"),
            (
                {"exclude_target_locations": [None, "France"]},
                "request_location_invalid_item",
                "exclude_target_locations",
            ),
        ]
        for payload_patch, expected_code, expected_field in cases:
            with self.subTest(payload=payload_patch):
                with self.assertRaises(CohortSelectionValidationError) as captured:
                    JobRequest.from_payload({"target_company": "Acme", **payload_patch})
                self.assertEqual(captured.exception.code, expected_code)
                self.assertEqual(captured.exception.field, expected_field)

    def test_present_json_null_is_not_field_absence(self) -> None:
        # FT1-FF (finding 4): a present JSON null fails closed with the stable
        # invalid-type error — it must NOT be silently treated as an absent
        # field (which would activate the explicit-Cohort US default).
        for field in ("target_locations", "exclude_target_locations"):
            with self.subTest(field=field):
                with self.assertRaises(CohortSelectionValidationError) as captured:
                    JobRequest.from_payload({"target_company": "Acme", field: None})
                self.assertEqual(captured.exception.code, "request_location_invalid_type")
                self.assertEqual(captured.exception.field, field)
                # Explicit-Cohort requests fail the same way: no silent US default.
                with self.assertRaises(CohortSelectionValidationError) as captured_cohort:
                    JobRequest.from_payload({"target_company": "Acme", "cohort_selection": _cohort(), field: None})
                self.assertEqual(captured_cohort.exception.code, "request_location_invalid_type")
        # Absence and present-empty remain distinct, well-formed identities.
        absent = JobRequest.from_payload({"target_company": "Acme"})
        self.assertIsNone(absent.target_locations)
        present_empty = JobRequest.from_payload({"target_company": "Acme", "target_locations": []})
        self.assertEqual(present_empty.target_locations, [])
        self.assertNotEqual(absent.to_record(), present_empty.to_record())

    def test_boundary_lengths_are_accepted(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "target_locations": [f"location-{index}" for index in range(16)],
                "exclude_target_locations": ["x" * 240],
            }
        )
        self.assertEqual(len(request.target_locations or []), 16)
        self.assertEqual(len((request.exclude_target_locations or [""])[0]), 240)

    def test_validation_error_payload_is_the_canonical_http_400_shape(self) -> None:
        try:
            JobRequest.from_payload({"target_company": "Acme", "target_locations": "United States"})
        except CohortSelectionValidationError as exc:
            result = exc.to_result()
        else:  # pragma: no cover - fail-closed guard
            self.fail("expected CohortSelectionValidationError")
        self.assertEqual(result["status"], "invalid")
        self.assertEqual(result["reason"], "request_location_invalid_type")
        self.assertEqual(result["field"], "target_locations")


class LocationFilterHintsCompositionTest(unittest.TestCase):
    """Matrix row 6 (composition): default, single-writer, exclude, legacy."""

    def test_explicit_cohort_absent_field_gets_us_default(self) -> None:
        request = JobRequest.from_payload({"target_company": "Acme", "cohort_selection": _cohort()})
        filter_hints = _strategy_for(request).filter_hints
        self.assertEqual(filter_hints.get("locations"), [DEFAULT_PRIMARY_LOCATION])
        self.assertNotIn("exclude_locations", filter_hints)

    def test_user_values_win_and_are_never_merged_with_the_default(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(),
                "target_locations": ["Germany"],
                "exclude_target_locations": ["France"],
            }
        )
        filter_hints = _strategy_for(request).filter_hints
        self.assertEqual(filter_hints.get("locations"), ["Germany"])
        self.assertEqual(filter_hints.get("exclude_locations"), ["France"])
        self.assertNotIn(DEFAULT_PRIMARY_LOCATION, filter_hints.get("locations") or [])

    def test_explicit_empty_list_opts_out_of_location_filtering(self) -> None:
        request = JobRequest.from_payload(
            {"target_company": "Acme", "cohort_selection": _cohort(), "target_locations": []}
        )
        filter_hints = _strategy_for(request).filter_hints
        self.assertNotIn("locations", filter_hints)

    def test_exclude_composes_independently_with_the_default(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(),
                "exclude_target_locations": ["France"],
            }
        )
        filter_hints = _strategy_for(request).filter_hints
        self.assertEqual(filter_hints.get("locations"), [DEFAULT_PRIMARY_LOCATION])
        self.assertEqual(filter_hints.get("exclude_locations"), ["France"])

    def test_legacy_non_cohort_paths_are_byte_unchanged(self) -> None:
        request = JobRequest.from_payload(
            {"target_company": "Acme", "employment_statuses": ["current"], "categories": ["employee"]}
        )
        filter_hints = _strategy_for(request).filter_hints
        self.assertNotIn("locations", filter_hints)
        self.assertNotIn("exclude_locations", filter_hints)

    def test_legacy_request_with_user_locations_composes_them(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "employment_statuses": ["current"],
                "target_locations": ["Canada"],
                "exclude_target_locations": ["Mexico"],
            }
        )
        filter_hints = _strategy_for(request).filter_hints
        self.assertEqual(filter_hints.get("locations"), ["Canada"])
        self.assertEqual(filter_hints.get("exclude_locations"), ["Mexico"])

    def test_plan_passes_fields_into_plan_filter_hints(self) -> None:
        cohort_request = JobRequest.from_payload({"target_company": "Acme", "cohort_selection": _cohort()})
        plan = build_sourcing_plan(cohort_request, AssetCatalog.discover(), DeterministicModelClient())
        self.assertEqual(plan.acquisition_strategy.filter_hints.get("locations"), [DEFAULT_PRIMARY_LOCATION])
        # The plan's filter hints are the exact mirror the Cohort compiler consumes.
        manifest = dict(plan.acquisition_strategy.provider_execution_manifest or {})
        base_hints = dict(dict(manifest.get("compiler_inputs") or {}).get("base_filter_hints") or {})
        self.assertEqual(base_hints.get("locations"), [DEFAULT_PRIMARY_LOCATION])

        user_request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(),
                "target_locations": ["Germany"],
            }
        )
        user_plan = build_sourcing_plan(user_request, AssetCatalog.discover(), DeterministicModelClient())
        self.assertEqual(user_plan.acquisition_strategy.filter_hints.get("locations"), ["Germany"])


class LocationHttpIngressTest(unittest.TestCase):
    """Matrix row 6 (HTTP 400): invalid values rejected before any write."""

    def setUp(self) -> None:
        self.previous_tokens = os.environ.pop("SOURCING_API_BEARER_TOKENS", None)

        class _ApiOrchestrator:
            """Mirrors the real orchestrator delegation: criteria writes run the
            canonical provenance owner (which re-validates the request through
            JobRequest.from_payload) before any store write; the other handlers
            only record what ingress forwarded."""

            def __init__(self) -> None:
                self.calls = {"criteria_feedback": 0, "criteria_recorded": 0, "workflow": 0, "explain": 0, "plan": 0}
                self.received_payloads: dict[str, dict] = {}

            def record_criteria_feedback(self, payload, **_owner):
                self.calls["criteria_feedback"] += 1
                self.received_payloads["criteria_feedback"] = dict(payload)
                _prepared, preflight = prepare_criteria_write_payload(payload, job_lookup=lambda _job_id: None)
                if preflight.get("status") != "ready":
                    return preflight
                self.calls["criteria_recorded"] += 1
                return {"status": "recorded"}

            def start_workflow(self, payload):
                self.calls["workflow"] += 1
                self.received_payloads["workflow"] = dict(payload)
                return {"status": "queued", "job_id": "job-1"}

            def explain_workflow(self, payload):
                self.calls["explain"] += 1
                self.received_payloads["explain"] = dict(payload)
                return {"status": "ready"}

            def submit_plan_workflow(self, payload):
                self.calls["plan"] += 1
                self.received_payloads["plan"] = dict(payload)
                return {"status": "queued"}

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

    def test_invalid_location_values_return_http_400_before_any_write(self) -> None:
        invalid_requests = (
            ({"target_company": "Acme", "target_locations": "United States"}, "request_location_invalid_type"),
            ({"target_company": "Acme", "target_locations": [None]}, "request_location_invalid_item"),
            ({"target_company": "Acme", "target_locations": ["x"] * 17}, "request_location_too_many_items"),
            ({"target_company": "Acme", "target_locations": ["x" * 241]}, "request_location_item_length_invalid"),
            ({"target_company": "Acme", "exclude_target_locations": 5}, "request_location_invalid_type"),
        )
        for request_payload, expected_reason in invalid_requests:
            with self.subTest(reason=expected_reason):
                status, result = self._request(
                    "/api/criteria/feedback",
                    method="POST",
                    body={"request_payload": request_payload},
                )
                self.assertEqual(status, 400)
                self.assertEqual(result["status"], "invalid")
                self.assertEqual(result["reason"], expected_reason)
        self.assertEqual(self.orchestrator.calls["criteria_recorded"], 0)

    def test_present_null_location_values_return_http_400_with_zero_writes(self) -> None:
        # FT1-FF (finding 4): present JSON null fails closed at the API
        # boundary for BOTH sibling fields, with zero recorded writes.
        for field in ("target_locations", "exclude_target_locations"):
            with self.subTest(field=field):
                status, result = self._request(
                    "/api/criteria/feedback",
                    method="POST",
                    body={"request_payload": {"target_company": "Acme", field: None}},
                )
                self.assertEqual(status, 400)
                self.assertEqual(result["status"], "invalid")
                self.assertEqual(result["reason"], "request_location_invalid_type")
                self.assertEqual(result["field"], field)
                # An explicit-Cohort request with a present null fails the same
                # way — the US default is never silently activated.
                status, result = self._request(
                    "/api/criteria/feedback",
                    method="POST",
                    body={"request_payload": {"target_company": "Acme", "cohort_selection": _cohort(), field: None}},
                )
                self.assertEqual(status, 400)
                self.assertEqual(result["reason"], "request_location_invalid_type")
        self.assertEqual(self.orchestrator.calls["criteria_recorded"], 0)

    def test_criteria_write_boundary_revalidates_present_null_without_http(self) -> None:
        for field in ("target_locations", "exclude_target_locations"):
            with self.subTest(field=field):
                prepared, preflight = prepare_criteria_write_payload(
                    {"request_payload": {"target_company": "Acme", field: None}},
                    job_lookup=lambda _job_id: None,
                )
                self.assertEqual(prepared, {})
                self.assertEqual(preflight["status"], "invalid")
                self.assertEqual(preflight["reason"], "request_location_invalid_type")

    def test_criteria_write_boundary_revalidates_without_http(self) -> None:
        prepared, preflight = prepare_criteria_write_payload(
            {"request_payload": {"target_company": "Acme", "target_locations": "United States"}},
            job_lookup=lambda _job_id: None,
        )
        self.assertEqual(prepared, {})
        self.assertEqual(preflight["status"], "invalid")
        self.assertEqual(preflight["reason"], "request_location_invalid_type")

    def test_valid_location_values_pass_ingress_to_the_handler(self) -> None:
        status, result = self._request(
            "/api/criteria/feedback",
            method="POST",
            body={
                "request_payload": {
                    "target_company": "Acme",
                    "target_locations": ["United States"],
                    "exclude_target_locations": ["France"],
                }
            },
        )
        self.assertEqual(status, 201)
        self.assertEqual(result["status"], "recorded")
        received = self.orchestrator.received_payloads["criteria_feedback"]
        self.assertEqual(received["request_payload"]["target_locations"], ["United States"])
        self.assertEqual(received["request_payload"]["exclude_target_locations"], ["France"])

    def test_sibling_fields_survive_workflow_ingress_untouched(self) -> None:
        for path, call_name, expected_status in (
            ("/api/workflows", "workflow", 202),
            ("/api/workflows/explain", "explain", 200),
            ("/api/plan/submit", "plan", 200),
        ):
            with self.subTest(path=path):
                status, _result = self._request(
                    path,
                    method="POST",
                    body={
                        "raw_user_request": "Find researchers at Acme",
                        "cohort_selection": _cohort(),
                        "target_locations": ["Germany"],
                        "exclude_target_locations": ["France"],
                    },
                )
                self.assertEqual(status, expected_status)
                received = self.orchestrator.received_payloads[call_name]
                self.assertEqual(received.get("target_locations"), ["Germany"])
                self.assertEqual(received.get("exclude_target_locations"), ["France"])


class LocationOrchestratorIngressTest(unittest.TestCase):
    """O1 regression: direct submit/explain paths return typed ``invalid``
    results (HTTP-400-mappable) for invalid location values instead of
    uncaught HTTP 500s. Fake/scripted: zero provider/model/network/PG calls.
    """

    def setUp(self) -> None:
        dsn = str(resolve_control_plane_postgres_dsn(_REPO_ROOT) or "").strip()
        if not dsn:
            self.skipTest("no local control-plane Postgres DSN resolved (make local-pg-up)")
        self._env_patch = unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_CONTROL_PLANE_POSTGRES_DSN": dsn},
            clear=False,
        )
        self._env_patch.start()
        self.addCleanup(self._env_patch.stop)
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        catalog = AssetCatalog.discover()
        store = ControlPlaneStore(Path(self.tempdir.name) / "test.db")
        self.addCleanup(store.close)
        settings = AppSettings(
            project_root=Path(self.tempdir.name),
            runtime_dir=Path(self.tempdir.name),
            secrets_file=Path(self.tempdir.name) / "providers.local.json",
            jobs_dir=Path(self.tempdir.name) / "jobs",
            company_assets_dir=Path(self.tempdir.name) / "company_assets",
            db_path=Path(self.tempdir.name) / "test.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        model_client = DeterministicModelClient()
        acquisition_engine = AcquisitionEngine(catalog, settings, store, model_client)
        self.orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=acquisition_engine,
        )

    def test_explain_returns_typed_invalid_for_invalid_location_values(self) -> None:
        result = self.orchestrator.explain_workflow({"target_company": "Acme", "target_locations": "United States"})
        self.assertEqual(result.get("status"), "invalid")
        self.assertEqual(result.get("reason"), "request_location_invalid_type")

    def test_queue_returns_typed_invalid_for_invalid_location_values(self) -> None:
        result = self.orchestrator.queue_workflow({"target_company": "Acme", "target_locations": ["x"] * 17})
        self.assertEqual(result.get("status"), "invalid")
        self.assertEqual(result.get("reason"), "request_location_too_many_items")


class LocationSignatureIdentityTest(unittest.TestCase):
    """Matrix row 7: signatures and digests split by location, digest excluded."""

    def _records(self):
        base = {"target_company": "Acme", "cohort_selection": _cohort()}
        return {
            "us": JobRequest.from_payload({**base, "target_locations": ["United States"]}).to_record(),
            "germany": JobRequest.from_payload({**base, "target_locations": ["Germany"]}).to_record(),
            "absent": JobRequest.from_payload(base).to_record(),
            "empty": JobRequest.from_payload({**base, "target_locations": []}).to_record(),
            "excluded": JobRequest.from_payload({**base, "exclude_target_locations": ["France"]}).to_record(),
        }

    def test_all_four_signatures_differ_across_locations(self) -> None:
        records = self._records()
        for signature_fn in (
            request_signature,
            request_family_signature,
            matching_request_signature,
            matching_request_family_signature,
        ):
            with self.subTest(signature=signature_fn.__name__):
                self.assertNotEqual(signature_fn(records["us"]), signature_fn(records["germany"]))

    def test_identical_requests_have_stable_signatures(self) -> None:
        records = self._records()
        again = JobRequest.from_payload(
            {"target_company": "Acme", "cohort_selection": _cohort(), "target_locations": ["United States"]}
        ).to_record()
        self.assertEqual(request_signature(records["us"]), request_signature(again))
        self.assertEqual(matching_request_signature(records["us"]), matching_request_signature(again))

    def test_absent_present_empty_and_exclude_are_distinct_identities(self) -> None:
        records = self._records()
        signatures = {name: request_signature(record) for name, record in records.items()}
        self.assertEqual(len(set(signatures.values())), len(signatures))

    def test_legacy_payloads_have_no_location_keys_in_normalized_payloads(self) -> None:
        legacy = JobRequest.from_payload({"target_company": "Acme", "employment_statuses": ["current"]}).to_record()
        normalized = _normalized_request_payload(legacy, include_runtime_limits=True)
        self.assertNotIn("target_locations", normalized)
        self.assertNotIn("exclude_target_locations", normalized)
        effective = _normalized_effective_request_payload(dict(legacy), include_runtime_limits=True)
        self.assertNotIn("target_locations", effective)
        self.assertNotIn("exclude_target_locations", effective)

    def test_cohort_selection_digest_is_unchanged_by_location(self) -> None:
        records = self._records()
        digests = {name: cohort_selection_digest(record["cohort_selection"]) for name, record in records.items()}
        self.assertEqual(len(set(digests.values())), 1)

    def test_manifest_and_lane_digests_differ_across_locations(self) -> None:
        compiler = CohortProviderCompiler()
        canonical = canonicalize_cohort_selection_request_payload(
            {"target_company": "Acme", "cohort_selection": _cohort(roles=["research"], statuses=["current"])}
        )

        def _compile_with(locations: list[str]):
            return compiler.compile(
                canonical,
                execution_capability=None,
                base_filter_hints={"current_companies": ["Acme"], "locations": locations},
                requested_result_limit=100,
            )

        us_manifest = _compile_with(["United States"])
        germany_manifest = _compile_with(["Germany"])
        us_again = _compile_with(["United States"])
        self.assertNotEqual(us_manifest["manifest_digest"], germany_manifest["manifest_digest"])
        self.assertEqual(us_manifest["manifest_digest"], us_again["manifest_digest"])
        self.assertNotEqual(
            us_manifest["lanes"][0]["lane_digest"],
            germany_manifest["lanes"][0]["lane_digest"],
        )
        self.assertEqual(
            us_manifest["cohort_selection_digest"],
            germany_manifest["cohort_selection_digest"],
        )


class LocationLaneCompositionTest(unittest.TestCase):
    """Matrix row 8: locations ride every lane; former lanes keep exclusions out."""

    def _manifest(self, *, roles: list[str], statuses: list[str]) -> dict:
        compiler = CohortProviderCompiler()
        canonical = canonicalize_cohort_selection_request_payload(
            {"target_company": "Acme", "cohort_selection": _cohort(roles=roles, statuses=statuses)}
        )
        return compiler.compile(
            canonical,
            execution_capability=None,
            base_filter_hints={
                "current_companies": ["Acme"],
                "locations": ["United States"],
                "exclude_locations": ["France"],
                "keywords": ["agent"],
                "function_ids": ["8"],
                "job_titles": ["Software Engineer"],
            },
            requested_result_limit=100,
        )

    def test_every_lane_carries_locations_and_exclusions(self) -> None:
        manifest = self._manifest(roles=["research", "engineering"], statuses=["current", "former"])
        self.assertEqual(len(manifest["lanes"]), 4)
        for lane in manifest["lanes"]:
            with self.subTest(lane=lane["lane_id"]):
                hints = lane["provider_payload"]["filter_hints"]
                self.assertEqual(hints.get("locations"), ["United States"])
                self.assertEqual(hints.get("exclude_locations"), ["France"])

    def test_former_lanes_never_gain_exclude_current_companies(self) -> None:
        manifest = self._manifest(roles=["research"], statuses=["current", "former"])
        for lane in manifest["lanes"]:
            hints = lane["provider_payload"]["filter_hints"]
            if lane["employment_status"] == "former":
                self.assertNotIn("excludeCurrentCompanies", hints)
                self.assertNotIn("current_companies", hints)
                self.assertEqual(hints.get("locations"), ["United States"])
                self.assertEqual(hints.get("exclude_locations"), ["France"])
            else:
                self.assertEqual(hints.get("current_companies"), ["Acme"])

    def test_role_hint_stripping_is_unchanged(self) -> None:
        manifest = self._manifest(roles=["research"], statuses=["current"])
        lane = manifest["lanes"][0]
        hints = lane["provider_payload"]["filter_hints"]
        # Base role-keyed hints are stripped; the lane carries exactly its own
        # registry-owned function ids / job titles instead.
        self.assertEqual(hints.get("function_ids"), ["24"])
        self.assertNotIn("8", hints.get("function_ids") or [])
        self.assertNotIn("Software Engineer", hints.get("job_titles") or [])

    def test_status_only_lanes_carry_locations_without_role_hints(self) -> None:
        manifest = self._manifest(roles=[], statuses=["current", "former"])
        self.assertEqual(len(manifest["lanes"]), 2)
        for lane in manifest["lanes"]:
            hints = lane["provider_payload"]["filter_hints"]
            self.assertEqual(hints.get("locations"), ["United States"])
            self.assertNotIn("function_ids", hints)
            self.assertNotIn("job_titles", hints)


class CohortSufficiencyDecisionContractTest(unittest.TestCase):
    """Matrix row 10: honest counters only — NO sufficiency stop/continue rule
    or target is currently authorized (operator decision, FT1-FF2 finding 9)."""

    @classmethod
    def _contract_text(cls) -> str:
        return _CONTRACT_DOC.read_text(encoding="utf-8")

    def test_honest_counter_vocabulary_is_pinned_in_the_contract_doc(self) -> None:
        text = self._contract_text()
        for token in (
            "lane_count",
            "completed_lane_count",
            "accepted_count",
            "rejected_count",
            "truncated_count",
            "missing_required_lane_count",
            "cohort_selection_digest",
            "cohort_provider_manifest_digest",
        ):
            with self.subTest(token=token):
                self.assertIn(token, text)

    def test_contract_records_no_authorized_rule_or_target(self) -> None:
        text = self._contract_text()
        # The operator left the sufficiency stop/continue decision OPEN: the
        # contract must record that no rule/target is authorized, keep the
        # target-driven schema/rule table withdrawn, and stay provider-free.
        self.assertIn("NO sufficiency stop/continue rule or target is currently authorized", text)
        self.assertIn("WITHDRAWN", text)
        self.assertIn("UNRESOLVED", text)
        self.assertIn("never a hidden provider call", text)
        self.assertIn("`served=0` is unchanged", text)
        # No pinned target vocabulary may survive anywhere in the contract.
        self.assertNotIn("target_accepted_count", text)
        self.assertNotIn("target_met_stop", text)
        self.assertNotIn("exhausted_stop", text)
        self.assertNotIn("truncated_lanes_continue", text)

    def test_no_runtime_or_default_sufficiency_target_exists(self) -> None:
        """Replacement oracle: prove no runtime/default target exists anywhere.

        The withdrawn oracle embedded a concrete target (50) and target-driven
        rules; the authorized state is that no schema field, rule, default, or
        oracle may introduce one.  Scan every runtime source file for the
        target vocabulary and require zero occurrences.
        """

        src_root = Path(__file__).resolve().parent.parent / "src" / "sourcing_agent"
        offenders: list[str] = []
        for path in sorted(src_root.rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            if "target_accepted_count" in text:
                offenders.append(str(path.relative_to(src_root)))
        self.assertEqual(offenders, [])
        # No runtime sufficiency decision schema/rule table exists either.
        for path in sorted(src_root.rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("cohort_sufficiency_decision", text, msg=str(path))
        # And the contract doc itself carries no target token.
        self.assertNotIn("target_accepted_count", self._contract_text())


if __name__ == "__main__":
    unittest.main()
