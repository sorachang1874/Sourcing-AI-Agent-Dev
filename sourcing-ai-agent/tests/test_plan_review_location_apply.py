"""F6 plan-review location application owner tests.

FT2 rerun3 review finding 6 / rerun4 finding 5: the frontend ships a tagged
clear wire contract (``{"op": "clear"}``) for gate-authorized initialized
location axes; this suite pins the backend owner side:

- the review gate authorizes both location axes (``editable_fields``);
- ``apply_plan_review_decision`` is the sole application owner — replace,
  explicit ``[]`` opt-out, tagged clear (restore absence), and absent-key
  no-op are never collapsed into one another;
- malformed operations fail closed (400-class stable codes) at the external
  write boundary for EVERY review action, with zero review writes;
- the dependent plan state is rebuilt through the same owners as plan time
  (filter hints, provider execution manifest, execution bundle), and a
  launched workflow reflects the applied decision.

All fake/scripted: zero provider/model/network/PG calls.
"""

from __future__ import annotations

import os
import tempfile
import unittest
import unittest.mock
import uuid
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.acquisition_strategy import DEFAULT_PRIMARY_LOCATION
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.cohort_provider_compiler import CohortProviderCompiler
from sourcing_agent.cohort_selection import CohortSelectionValidationError
from sourcing_agent.domain import JobRequest
from sourcing_agent.local_postgres import resolve_control_plane_postgres_dsn
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.plan_review import (
    apply_plan_review_decision,
    build_plan_review_gate,
    validate_plan_review_location_decision,
)
from sourcing_agent.planning import build_sourcing_plan
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


def _cohort(*, roles: list[str] | None = None, statuses: list[str] | None = None) -> dict:
    return {
        "schema_version": "cohort_selection.v1",
        "role_bucket_ids": list(roles if roles is not None else ["research"]),
        "employment_statuses": list(statuses if statuses is not None else ["current"]),
        "role_match": "any",
        "source": "user_explicit",
    }


def _request_payload(**overrides) -> dict:
    payload = {
        "raw_user_request": "帮我找 Acme 做 Coding 方向的工程师",
        "target_company": "Acme",
        "execution_preferences": {},
    }
    payload.update(overrides)
    return payload


def _plan_payload(
    *, strategy_type: str = "scoped_search_roster", filter_hints: dict | None = None, manifest: dict | None = None
) -> dict:
    acquisition_strategy = {
        "strategy_type": strategy_type,
        "company_scope": ["Acme"],
        "filter_hints": dict(filter_hints if filter_hints is not None else {"current_companies": ["Acme"]}),
        "cost_policy": {},
        "reasoning": [],
    }
    if manifest is not None:
        acquisition_strategy["provider_execution_manifest"] = manifest
    return {
        "acquisition_strategy": acquisition_strategy,
        "acquisition_tasks": [
            {
                "task_id": "acquire-full-roster",
                "task_type": "acquire_full_roster",
                "status": "ready",
                "metadata": {},
            }
        ],
    }


def _cohort_manifest(request_payload: dict, base_filter_hints: dict) -> dict:
    return CohortProviderCompiler().compile(request_payload, base_filter_hints=base_filter_hints)


class PlanReviewLocationGateTest(unittest.TestCase):
    """The gate authorizes both location axes so the frontend may serialize."""

    def test_gate_lists_both_location_fields_as_editable(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Acme 做 Coding 方向的工程师",
                "target_company": "Acme",
                "cohort_selection": _cohort(),
            }
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())

        gate = build_plan_review_gate(request, plan)

        self.assertIn("target_locations", gate["editable_fields"])
        self.assertIn("exclude_target_locations", gate["editable_fields"])


class PlanReviewLocationApplyTest(unittest.TestCase):
    """apply_plan_review_decision is the sole authorized application owner."""

    def test_set_values_replaces_canonical_request_and_rebuilds_hints(self) -> None:
        updated_request, updated_plan = apply_plan_review_decision(
            _request_payload(),
            _plan_payload(),
            {
                "target_locations": [" Canada ", "canada", "Germany"],
                "exclude_target_locations": ["France"],
            },
        )

        # Normalized through the same fail-closed owner as ingress.
        self.assertEqual(updated_request["target_locations"], ["Canada", "Germany"])
        self.assertEqual(updated_request["exclude_target_locations"], ["France"])
        filter_hints = updated_plan["acquisition_strategy"]["filter_hints"]
        self.assertEqual(filter_hints["locations"], ["Canada", "Germany"])
        self.assertEqual(filter_hints["exclude_locations"], ["France"])
        # Task metadata (acquisition-lane consumer) sees the same hints.
        task_hints = updated_plan["acquisition_tasks"][0]["metadata"]["filter_hints"]
        self.assertEqual(task_hints["locations"], ["Canada", "Germany"])
        self.assertEqual(task_hints["exclude_locations"], ["France"])

    def test_explicit_empty_list_opts_out_and_suppresses_full_roster_default(self) -> None:
        updated_request, updated_plan = apply_plan_review_decision(
            _request_payload(target_company="Google"),
            _plan_payload(
                strategy_type="full_company_roster",
                filter_hints={"current_companies": ["Google"], "locations": [DEFAULT_PRIMARY_LOCATION]},
            ),
            {"target_locations": [], "exclude_target_locations": []},
        )

        self.assertEqual(updated_request["target_locations"], [])
        self.assertIn("target_locations", updated_request)
        filter_hints = updated_plan["acquisition_strategy"]["filter_hints"]
        self.assertNotIn("locations", filter_hints)
        self.assertNotIn("exclude_locations", filter_hints)

    def test_tagged_clear_restores_absence_and_cohort_default_returns(self) -> None:
        updated_request, updated_plan = apply_plan_review_decision(
            _request_payload(
                cohort_selection=_cohort(),
                must_have_primary_role_buckets=["research"],
                employment_statuses=["current"],
                target_locations=["Canada"],
            ),
            _plan_payload(filter_hints={"current_companies": ["Acme"], "locations": ["Canada"]}),
            {"target_locations": {"op": "clear"}},
        )

        # The canonical request axis is REMOVED (restore absence), never
        # collapsed into an explicit empty list.
        self.assertNotIn("target_locations", updated_request)
        # An explicit-Cohort request with the field absent gets the planner
        # default back — the same state a fresh plan would produce.
        filter_hints = updated_plan["acquisition_strategy"]["filter_hints"]
        self.assertEqual(filter_hints["locations"], [DEFAULT_PRIMARY_LOCATION])
        task_hints = updated_plan["acquisition_tasks"][0]["metadata"]["filter_hints"]
        self.assertEqual(task_hints["locations"], [DEFAULT_PRIMARY_LOCATION])

    def test_tagged_clear_on_legacy_plan_leaves_no_location_hint(self) -> None:
        updated_request, updated_plan = apply_plan_review_decision(
            _request_payload(target_locations=["Canada"]),
            _plan_payload(filter_hints={"current_companies": ["Acme"], "locations": ["Canada"]}),
            {"target_locations": {"op": "clear"}},
        )

        self.assertNotIn("target_locations", updated_request)
        self.assertNotIn("locations", updated_plan["acquisition_strategy"]["filter_hints"])

    def test_absent_keys_are_a_noop_and_never_touch_request_or_plan(self) -> None:
        request_payload = _request_payload(
            cohort_selection=_cohort(),
            must_have_primary_role_buckets=["research"],
            employment_statuses=["current"],
        )
        plan_payload = _plan_payload(
            filter_hints={"current_companies": ["Acme"], "locations": [DEFAULT_PRIMARY_LOCATION]}
        )

        updated_request, updated_plan = apply_plan_review_decision(request_payload, plan_payload, {})

        self.assertNotIn("target_locations", updated_request)
        self.assertNotIn("exclude_target_locations", updated_request)
        self.assertEqual(
            updated_plan["acquisition_strategy"]["filter_hints"],
            plan_payload["acquisition_strategy"]["filter_hints"],
        )

    def test_axes_apply_independently(self) -> None:
        updated_request, updated_plan = apply_plan_review_decision(
            _request_payload(target_locations=["Canada"], exclude_target_locations=["France"]),
            _plan_payload(
                filter_hints={
                    "current_companies": ["Acme"],
                    "locations": ["Canada"],
                    "exclude_locations": ["France"],
                }
            ),
            {"exclude_target_locations": {"op": "clear"}},
        )

        # Only the exclude axis was part of this decision.
        self.assertEqual(updated_request["target_locations"], ["Canada"])
        self.assertNotIn("exclude_target_locations", updated_request)
        filter_hints = updated_plan["acquisition_strategy"]["filter_hints"]
        self.assertEqual(filter_hints["locations"], ["Canada"])
        self.assertNotIn("exclude_locations", filter_hints)

    def test_invalid_operations_fail_closed_with_stable_codes_and_zero_mutation(self) -> None:
        cases = [
            ({"target_locations": None}, "request_location_invalid_type", "target_locations"),
            ({"target_locations": "Canada"}, "request_location_invalid_type", "target_locations"),
            ({"target_locations": 7}, "request_location_invalid_type", "target_locations"),
            ({"target_locations": {"op": "replace"}}, "plan_review_location_invalid_operation", "target_locations"),
            (
                {"target_locations": {"op": "clear", "extra": 1}},
                "plan_review_location_invalid_operation",
                "target_locations",
            ),
            ({"target_locations": {}}, "plan_review_location_invalid_operation", "target_locations"),
            ({"target_locations": ["ok", None]}, "request_location_invalid_item", "target_locations"),
            ({"target_locations": ["x"] * 17}, "request_location_too_many_items", "target_locations"),
            ({"target_locations": ["x" * 241]}, "request_location_item_length_invalid", "target_locations"),
            ({"exclude_target_locations": None}, "request_location_invalid_type", "exclude_target_locations"),
            (
                {"exclude_target_locations": {"op": "drop"}},
                "plan_review_location_invalid_operation",
                "exclude_target_locations",
            ),
        ]
        for decision, code, field in cases:
            with self.subTest(decision=decision):
                with self.assertRaises(CohortSelectionValidationError) as ctx:
                    validate_plan_review_location_decision(decision)
                self.assertEqual(ctx.exception.code, code)
                self.assertEqual(ctx.exception.field, field)

    def test_apply_validation_is_atomic_across_axes(self) -> None:
        request_payload = _request_payload(target_locations=["Canada"])
        plan_payload = _plan_payload()
        with self.assertRaises(CohortSelectionValidationError) as ctx:
            apply_plan_review_decision(
                request_payload,
                plan_payload,
                {
                    "target_locations": ["Germany"],
                    "exclude_target_locations": {"op": "replace"},
                },
            )
        self.assertEqual(ctx.exception.code, "plan_review_location_invalid_operation")
        # Caller payloads are never partially mutated.
        self.assertEqual(request_payload["target_locations"], ["Canada"])
        self.assertNotIn("exclude_target_locations", request_payload)
        self.assertNotIn("exclude_locations", plan_payload["acquisition_strategy"]["filter_hints"])

    def test_cohort_manifest_is_rebound_through_compiler_owner(self) -> None:
        request_payload = _request_payload(
            cohort_selection=_cohort(),
            must_have_primary_role_buckets=["research"],
            employment_statuses=["current"],
            target_locations=["Canada"],
        )
        plan_payload = _plan_payload(
            filter_hints={"current_companies": ["Acme"], "locations": ["Canada"]},
            manifest=_cohort_manifest(request_payload, {"current_companies": ["Acme"], "locations": ["Canada"]}),
        )

        updated_request, updated_plan = apply_plan_review_decision(
            request_payload,
            plan_payload,
            {"target_locations": ["Germany"]},
        )

        stored_manifest = updated_plan["acquisition_strategy"]["provider_execution_manifest"]
        # The stored manifest equals the exact capability-free recompilation
        # from the reviewed request plus the rebuilt hints — the same check
        # the runtime preflight performs before any provider call.
        expected_manifest = CohortProviderCompiler().compile(
            updated_request,
            base_filter_hints=updated_plan["acquisition_strategy"]["filter_hints"],
        )
        self.assertEqual(stored_manifest, expected_manifest)
        self.assertEqual(
            stored_manifest["compiler_inputs"]["base_filter_hints"]["locations"],
            ["Germany"],
        )

    def test_compiler_manifest_without_cohort_owner_fails_closed(self) -> None:
        plan_payload = _plan_payload(
            manifest={"schema_version": "cohort_provider_manifest.v1", "lanes": []},
        )
        with self.assertRaises(CohortSelectionValidationError) as ctx:
            apply_plan_review_decision(
                _request_payload(),
                plan_payload,
                {"target_locations": ["Canada"]},
            )
        self.assertEqual(ctx.exception.code, "plan_review_location_manifest_rebind_failed")
        self.assertEqual(ctx.exception.field, "provider_execution_manifest")

    def test_legacy_manifest_is_rebuilt_through_plan_time_owner(self) -> None:
        plan_payload = _plan_payload(
            strategy_type="full_company_roster",
            filter_hints={"current_companies": ["Acme"], "locations": [DEFAULT_PRIMARY_LOCATION]},
            manifest={"version": 1, "source": "planning_contract", "strategy_type": "full_company_roster", "lanes": []},
        )

        _updated_request, updated_plan = apply_plan_review_decision(
            _request_payload(),
            plan_payload,
            {"target_locations": ["Canada"]},
        )

        manifest = updated_plan["acquisition_strategy"]["provider_execution_manifest"]
        self.assertEqual(manifest["source"], "planning_contract")
        lane_filters = [dict(lane.get("company_filters") or {}) for lane in manifest["lanes"]]
        self.assertTrue(lane_filters)
        self.assertTrue(any(filters.get("locations") == ["Canada"] for filters in lane_filters))
        self.assertFalse(any(filters.get("locations") == [DEFAULT_PRIMARY_LOCATION] for filters in lane_filters))

    def test_review_edit_matches_fresh_plan_for_location_state(self) -> None:
        # Strong equivalence: a review-applied location edit produces the
        # same request/filter-hints/manifest location state as a fresh plan
        # compiled with those locations at ingress.
        base_kwargs = {
            "raw_user_request": "帮我找 Acme 做 Coding 方向的工程师",
            "target_company": "Acme",
            "categories": ["engineer"],
            "employment_statuses": ["current"],
        }
        fresh_request = JobRequest.from_payload(
            {**base_kwargs, "target_locations": ["Canada"], "exclude_target_locations": ["France"]}
        )
        fresh_plan = build_sourcing_plan(fresh_request, AssetCatalog.discover(), DeterministicModelClient()).to_record()

        base_request = JobRequest.from_payload(base_kwargs)
        base_plan = build_sourcing_plan(base_request, AssetCatalog.discover(), DeterministicModelClient()).to_record()
        updated_request, updated_plan = apply_plan_review_decision(
            base_request.to_record(),
            base_plan,
            {"target_locations": ["Canada"], "exclude_target_locations": ["France"]},
        )

        self.assertEqual(updated_request["target_locations"], fresh_request.target_locations)
        self.assertEqual(updated_request["exclude_target_locations"], fresh_request.exclude_target_locations)
        self.assertEqual(
            updated_plan["acquisition_strategy"]["filter_hints"],
            fresh_plan["acquisition_strategy"]["filter_hints"],
        )
        self.assertEqual(
            updated_plan["acquisition_strategy"]["provider_execution_manifest"],
            fresh_plan["acquisition_strategy"]["provider_execution_manifest"],
        )


class PlanReviewLocationOrchestratorBoundaryTest(unittest.TestCase):
    """The review write boundary fails closed for EVERY action, zero writes."""

    @staticmethod
    def _orchestrator(store) -> SourcingOrchestrator:
        orchestrator = object.__new__(SourcingOrchestrator)
        orchestrator.store = store
        return orchestrator

    def test_approved_review_applies_location_edit_and_rebuilds_bundle(self) -> None:
        stored_request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(),
                "must_have_primary_role_buckets": ["research"],
                "employment_statuses": ["current"],
                "target_locations": ["United States"],
            }
        ).to_record()
        stored_plan = _plan_payload(
            filter_hints={"current_companies": ["Acme"], "locations": ["United States"]},
            manifest=_cohort_manifest(stored_request, {"current_companies": ["Acme"], "locations": ["United States"]}),
        )

        class _Store:
            captured: dict = {}

            @staticmethod
            def get_plan_review_session(review_id):
                return {
                    "review_id": review_id,
                    "status": "pending",
                    "request": stored_request,
                    "plan": stored_plan,
                    "execution_bundle": {},
                }

            def review_plan_session(self, **kwargs):
                self.captured = dict(kwargs)
                return {
                    "review_id": kwargs["review_id"],
                    "status": kwargs["status"],
                    "request": dict(kwargs["request_payload"]),
                    "plan": dict(kwargs["plan_payload"]),
                    "execution_bundle": dict(kwargs["execution_bundle_payload"]),
                }

        store = _Store()
        orchestrator = self._orchestrator(store)
        commit_sessions: list[dict] = []
        orchestrator._plan_acquisition_plan_commit_command_from_review = lambda **kwargs: (
            commit_sessions.append(dict(kwargs.get("review_session") or {}))
            or {"command_type": "acquisition_plan_commit"}
        )
        orchestrator._workflow_command_observation = lambda command, **kwargs: {"command": command}

        result = orchestrator.review_plan_session(
            {
                "review_id": 1,
                "action": "approved",
                "reviewer": "tester",
                "decision": {
                    "target_locations": ["Canada"],
                    "exclude_target_locations": ["France"],
                },
            }
        )

        self.assertEqual(result["status"], "reviewed")
        persisted_request = store.captured["request_payload"]
        self.assertEqual(persisted_request["target_locations"], ["Canada"])
        self.assertEqual(persisted_request["exclude_target_locations"], ["France"])
        persisted_hints = store.captured["plan_payload"]["acquisition_strategy"]["filter_hints"]
        self.assertEqual(persisted_hints["locations"], ["Canada"])
        self.assertEqual(persisted_hints["exclude_locations"], ["France"])
        # The rebuilt execution bundle carries the edited request/plan, and
        # the stored manifest still equals the exact recompilation (runtime
        # preflight would pass for the launched workflow).
        bundle = store.captured["execution_bundle_payload"]
        self.assertEqual(bundle["request"]["target_locations"], ["Canada"])
        self.assertEqual(
            bundle["plan"]["acquisition_strategy"]["filter_hints"]["locations"],
            ["Canada"],
        )
        stored_manifest = store.captured["plan_payload"]["acquisition_strategy"]["provider_execution_manifest"]
        expected_manifest = CohortProviderCompiler().compile(persisted_request, base_filter_hints=persisted_hints)
        self.assertEqual(stored_manifest, expected_manifest)
        # The launched-workflow commit is planned from the REVIEWED session.
        self.assertEqual(len(commit_sessions), 1)
        self.assertEqual(commit_sessions[0]["request"]["target_locations"], ["Canada"])

    def test_invalid_location_operation_fails_closed_before_any_write(self) -> None:
        class _Store:
            writes = 0

            @staticmethod
            def get_plan_review_session(review_id):
                return {
                    "review_id": review_id,
                    "status": "pending",
                    "request": JobRequest.from_payload({"target_company": "Acme"}).to_record(),
                    "plan": {},
                    "execution_bundle": {},
                }

            def review_plan_session(self, **kwargs):
                self.writes += 1
                return kwargs

        for action, decision, reason in [
            ("approved", {"target_locations": {"op": "replace"}}, "plan_review_location_invalid_operation"),
            ("approved", {"target_locations": None}, "request_location_invalid_type"),
            ("rejected", {"target_locations": {"op": "replace"}}, "plan_review_location_invalid_operation"),
            ("needs_changes", {"exclude_target_locations": None}, "request_location_invalid_type"),
        ]:
            with self.subTest(action=action, decision=decision):
                store = _Store()
                orchestrator = self._orchestrator(store)
                result = orchestrator.review_plan_session({"review_id": 1, "action": action, "decision": decision})
                self.assertEqual(result["status"], "invalid")
                self.assertEqual(result["reason"], reason)
                self.assertEqual(result["field"], next(iter(decision)))
                self.assertEqual(store.writes, 0)

    def test_resolve_workflow_plan_binds_reviewed_location_state_for_launch(self) -> None:
        """PG-free launched-workflow verification.

        ``_resolve_workflow_plan`` is the exact binding path
        ``queue_workflow`` uses to launch from an approved review session:
        the launched workflow consumes the persisted reviewed request, plan,
        and frozen execution bundle — so the applied location decision must
        be visible there, and the stored manifest must equal the runtime's
        exact-recompilation preflight.
        """
        stored_request = JobRequest.from_payload(
            {
                "target_company": "Acme",
                "cohort_selection": _cohort(),
                "must_have_primary_role_buckets": ["research"],
                "employment_statuses": ["current"],
                "target_locations": ["United States"],
            }
        ).to_record()
        stored_plan = _plan_payload(
            filter_hints={"current_companies": ["Acme"], "locations": ["United States"]},
            manifest=_cohort_manifest(stored_request, {"current_companies": ["Acme"], "locations": ["United States"]}),
        )

        class _Store:
            session: dict = {
                "review_id": 1,
                "status": "pending",
                "request": stored_request,
                "plan": stored_plan,
                "execution_bundle": {},
            }

            def get_plan_review_session(self, review_id):
                return dict(self.session)

            def review_plan_session(self, **kwargs):
                self.session = {
                    "review_id": kwargs["review_id"],
                    "status": kwargs["status"],
                    "request": dict(kwargs["request_payload"]),
                    "plan": dict(kwargs["plan_payload"]),
                    "execution_bundle": dict(kwargs["execution_bundle_payload"]),
                }
                return dict(self.session)

        store = _Store()
        orchestrator = self._orchestrator(store)
        orchestrator._plan_acquisition_plan_commit_command_from_review = lambda **kwargs: {}
        orchestrator._workflow_command_observation = lambda command, **kwargs: {}

        result = orchestrator.review_plan_session(
            {
                "review_id": 1,
                "action": "approved",
                "reviewer": "tester",
                "decision": {"target_locations": ["Canada"]},
            }
        )
        self.assertEqual(result["status"], "reviewed")

        resolved = orchestrator._resolve_workflow_plan({"plan_review_id": 1})

        self.assertEqual(resolved.get("status"), "ready")
        resolved_request = dict(resolved.get("request") or {})
        self.assertEqual(resolved_request.get("target_locations"), ["Canada"])
        resolved_hints = (
            dict(dict(resolved.get("plan") or {}).get("acquisition_strategy") or {}).get("filter_hints") or {}
        )
        self.assertEqual(resolved_hints.get("locations"), ["Canada"])
        resolved_bundle = dict(resolved.get("execution_bundle") or {})
        self.assertEqual(dict(resolved_bundle.get("request") or {}).get("target_locations"), ["Canada"])
        stored_manifest = (
            dict(dict(resolved.get("plan") or {}).get("acquisition_strategy") or {}).get("provider_execution_manifest")
            or {}
        )
        self.assertTrue(stored_manifest)
        expected_manifest = CohortProviderCompiler().compile(resolved_request, base_filter_hints=resolved_hints)
        self.assertEqual(stored_manifest, expected_manifest)


class PlanReviewLocationLaunchedWorkflowTest(unittest.TestCase):
    """End-to-end launched-workflow verification on a real orchestrator +
    control-plane store.  Requires the disposable local Postgres (the same
    fixture pattern as LocationOrchestratorIngressTest); skipped cleanly
    when no DSN resolves.
    """

    def setUp(self) -> None:
        dsn = str(resolve_control_plane_postgres_dsn(_REPO_ROOT) or "").strip()
        if not dsn:
            self.skipTest("no local control-plane Postgres DSN resolved (make local-pg-up)")
        # Self-contained schema lifecycle (mirrors the conftest isolation
        # fixture, but pinned so it behaves identically in every
        # environment): a unique throwaway schema per test, migrated by the
        # versioned runner, dropped on cleanup — zero shared-DB pollution.
        self._pg_schema = f"test_plan_review_location_{uuid.uuid4().hex[:16]}"
        self._pg_dsn = dsn
        self._env_patch = unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CONTROL_PLANE_POSTGRES_DSN": dsn,
                "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": self._pg_schema,
            },
            clear=False,
        )
        self._env_patch.start()
        self.addCleanup(self._env_patch.stop)
        self.addCleanup(self._drop_pg_schema)
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        catalog = AssetCatalog.discover()
        store = ControlPlaneStore(Path(self.tempdir.name) / "test.db")
        self.addCleanup(store.close)
        store._control_plane_postgres.ensure_bootstrapped()  # noqa: SLF001
        self.store = store
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

    def _drop_pg_schema(self) -> None:
        try:
            import psycopg

            with psycopg.connect(self._pg_dsn, autocommit=True) as connection:
                connection.execute(f'DROP SCHEMA IF EXISTS "{self._pg_schema}" CASCADE')
        except Exception:
            # Best-effort cleanup; a leaked test schema is harmless. Never
            # fail a test on teardown cleanup (same posture as conftest).
            pass

    def test_queued_job_reflects_the_reviewed_location_edit(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Acme 做 Coding 方向的工程师",
                "target_company": "Acme",
                "categories": ["engineer"],
                "employment_statuses": ["current"],
                "target_locations": ["United States"],
            }
        )
        review_id = int(plan_result["plan_review_session"]["review_id"] or 0)
        self.assertGreater(review_id, 0)
        self.assertIn("target_locations", plan_result["plan_review_gate"]["editable_fields"])

        reviewed = self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {
                    "target_locations": ["Canada"],
                    "exclude_target_locations": ["France"],
                },
            }
        )
        self.assertEqual(reviewed["status"], "reviewed")

        queued = self.orchestrator.queue_workflow({"plan_review_id": review_id})
        self.assertEqual(queued["status"], "queued")
        job = self.store.get_job(str(queued.get("job_id") or ""))
        self.assertIsNotNone(job)
        assert job is not None
        self.assertEqual(job["request"]["target_locations"], ["Canada"])
        self.assertEqual(job["request"]["exclude_target_locations"], ["France"])
        job_hints = dict(dict(job.get("plan") or {}).get("acquisition_strategy") or {}).get("filter_hints") or {}
        self.assertEqual(job_hints.get("locations"), ["Canada"])
        self.assertEqual(job_hints.get("exclude_locations"), ["France"])

    def test_approved_cohort_review_resolve_path_keeps_manifest_exact(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Acme 做 Coding 方向的研究员",
                "target_company": "Acme",
                "cohort_selection": _cohort(),
                "target_locations": ["United States"],
            }
        )
        review_id = int(plan_result["plan_review_session"]["review_id"] or 0)
        self.assertGreater(review_id, 0)

        reviewed = self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {"target_locations": ["Canada"]},
            }
        )
        self.assertEqual(reviewed["status"], "reviewed")

        # _resolve_workflow_plan is the exact binding path queue_workflow
        # uses to launch from an approved review session.
        resolved = self.orchestrator._resolve_workflow_plan({"plan_review_id": review_id})
        self.assertNotIn(resolved.get("status"), {"needs_plan_review", "invalid"})
        resolved_request = dict(resolved.get("request") or {})
        self.assertEqual(resolved_request.get("target_locations"), ["Canada"])
        resolved_hints = (
            dict(dict(resolved.get("plan") or {}).get("acquisition_strategy") or {}).get("filter_hints") or {}
        )
        self.assertEqual(resolved_hints.get("locations"), ["Canada"])
        stored_manifest = (
            dict(dict(resolved.get("plan") or {}).get("acquisition_strategy") or {}).get("provider_execution_manifest")
            or {}
        )
        self.assertTrue(stored_manifest)
        # The runtime exact-recompilation preflight the launched workflow
        # must pass before any provider call.
        expected_manifest = CohortProviderCompiler().compile(
            resolved_request,
            base_filter_hints=resolved_hints,
        )
        self.assertEqual(stored_manifest, expected_manifest)

    def test_tagged_clear_reaches_the_launched_workflow_as_absence(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Acme 做 Coding 方向的工程师",
                "target_company": "Acme",
                "categories": ["engineer"],
                "employment_statuses": ["current"],
                "target_locations": ["Canada"],
            }
        )
        review_id = int(plan_result["plan_review_session"]["review_id"] or 0)
        reviewed = self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {"target_locations": {"op": "clear"}},
            }
        )
        self.assertEqual(reviewed["status"], "reviewed")

        queued = self.orchestrator.queue_workflow({"plan_review_id": review_id})
        self.assertEqual(queued["status"], "queued")
        job = self.store.get_job(str(queued.get("job_id") or ""))
        self.assertIsNotNone(job)
        assert job is not None
        # Restore-absence reaches the launched job as a REMOVED axis, not as
        # an explicit empty list and not as the pre-review value.
        self.assertNotIn("target_locations", job["request"])


if __name__ == "__main__":
    unittest.main()
