from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.crm_public_web_runtime import start_crm_public_web_batch
from sourcing_agent.durable_runtime import CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.operation_runtime import (
    ACTION_ENRICH_PERSON_PUBLIC_WEB,
    CRM_RECORD_BATCH_ACTION_REQUEST_CONTRACTS,
    CRM_RESOURCE_BOUND_ACTION_TYPES,
    DEFAULT_ACTION_REGISTRY,
    OPERATION_OWNER_BOUND_ACTION_TYPES,
)
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin, psycopg

FULL_ZERO_WRITE_TABLES = (
    "agent_actions",
    "operation_runs",
    "operation_events",
    "workflow_events",
    "workflow_current_state",
    "workflow_commands",
    "runtime_outbox",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    "crm_public_web_batches",
    "crm_public_web_runs",
)


def test_crm_public_web_action_remains_in_the_schema_defined_unserved_set() -> None:
    records = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    schema_defined = {
        action_type for action_type in records if DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema
    }
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_ENRICH_PERSON_PUBLIC_WEB)

    assert set(CRM_RECORD_BATCH_ACTION_REQUEST_CONTRACTS) == {ACTION_ENRICH_PERSON_PUBLIC_WEB}
    assert set(CRM_RESOURCE_BOUND_ACTION_TYPES).issubset(schema_defined)
    assert schema_defined == set(OPERATION_OWNER_BOUND_ACTION_TYPES)
    assert len(schema_defined) == 9
    assert sum(not DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema for action_type in records) == 6
    assert spec.request_schema_version == "crm_public_web_enrichment_request_v1"
    assert len(spec.request_schema_digest) == 64
    assert spec.request_identity_target_fields == ("crm_record_ids", "workspace_id")
    assert "agent_tool_enabled" not in records[ACTION_ENRICH_PERSON_PUBLIC_WEB]


class D1hCRMPublicWebActionActivationPGTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir, schema_label="d1h_crm_public_web_action")
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "d1h-crm-public-web-action.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        self.orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, model_client),
        )

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def _seed_record(
        self,
        crm_record_id: str,
        *,
        workspace_id: str = "user-alice",
        owner_user_id: str = "alice",
    ) -> dict[str, Any]:
        return self.store.upsert_crm_record(
            {
                "crm_record_id": crm_record_id,
                "workspace_id": workspace_id,
                "owner_user_id": owner_user_id,
                "person_identity_key": f"person::{crm_record_id}",
                "candidate_identity_key": f"person::{crm_record_id}",
                "display_name_cache": crm_record_id,
                "headline_cache": "Researcher",
                "primary_company_cache": "Example",
                "metadata": {"linkedin_url_cache": f"https://www.linkedin.com/in/{crm_record_id}/"},
            }
        )

    def _table_state(self, table_names: tuple[str, ...]) -> dict[str, tuple[str, ...]]:
        fixture = self._pg_durable_runtime_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        state: dict[str, tuple[str, ...]] = {}
        with psycopg.connect(
            fixture.dsn,
            autocommit=True,
            connect_timeout=5,
            client_encoding="utf8",
        ) as connection:
            with connection.cursor() as cursor:
                for table_name in table_names:
                    quoted_table = quote_control_plane_postgres_identifier(table_name)
                    cursor.execute(f"SELECT row_to_json(t)::text FROM {quoted_schema}.{quoted_table} AS t")
                    state[table_name] = tuple(sorted(str(row[0]) for row in cursor.fetchall()))
        return state

    def _execute_pg(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        fixture = self._pg_durable_runtime_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        with psycopg.connect(
            fixture.dsn,
            autocommit=True,
            connect_timeout=5,
            client_encoding="utf8",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.format(schema=quoted_schema), params)

    def _submit(
        self,
        *,
        selector: dict[str, Any] | None = None,
        input_payload: dict[str, Any] | None = None,
        expected_workspace_id: str = "user-alice",
        expected_owner_user_id: str = "alice",
        workspace_id: str = "user-alice",
        idempotency_key: str,
    ) -> dict[str, Any]:
        owner_scope = (
            {
                "expected_workspace_id": expected_workspace_id,
                "expected_owner_user_id": expected_owner_user_id,
            }
            if expected_workspace_id or expected_owner_user_id
            else {}
        )
        return self.orchestrator.submit_operation_action(
            {
                "action_type": ACTION_ENRICH_PERSON_PUBLIC_WEB,
                "workspace_id": workspace_id,
                "target_ref": dict(selector or {}),
                "input": dict(input_payload or {}),
                "budget": {"max_provider_calls": 2, "max_usd": 1.0},
                "idempotency_key": idempotency_key,
                "actor": expected_owner_user_id or "operator",
            },
            **owner_scope,
        )

    def _approve(self, submission: dict[str, Any]) -> dict[str, Any]:
        return self.orchestrator.approve_operation_action_api(
            submission["action"]["action_id"],
            {"actor": "alice"},
        )

    def _plan_action_command(
        self,
        record_id: str,
        *,
        input_payload: dict[str, Any] | None = None,
        idempotency_key: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        submission = self._submit(
            selector={"crm_record_id": record_id},
            input_payload=input_payload,
            idempotency_key=idempotency_key,
        )
        approved = self._approve(submission)
        dispatched = self.orchestrator.dispatch_operation_run_api(
            approved["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        self.assertEqual(dispatched.get("status"), "planned", dispatched)
        command = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])
        self.assertIsNotNone(command)
        return dispatched, dict(command or {})

    def test_authenticated_missing_foreign_mixed_and_malformed_selectors_are_zero_write(self) -> None:
        self._seed_record("owned-a")
        self._seed_record("owned-b")
        self._seed_record("foreign-workspace", workspace_id="user-bob", owner_user_id="bob")
        self._seed_record("foreign-owner", owner_user_id="bob")
        baseline = self._table_state(FULL_ZERO_WRITE_TABLES)

        denied_selectors = (
            {"crm_record_ids": ["missing"]},
            {"crm_record_ids": ["foreign-workspace"]},
            {"crm_record_ids": ["foreign-owner"]},
            {"crm_record_ids": ["owned-a", "missing"]},
            {"crm_record_ids": ["owned-a", "foreign-workspace"]},
        )
        denied_results = []
        for index, selector in enumerate(denied_selectors):
            result = self._submit(selector=selector, idempotency_key=f"denied:{index}")
            denied_results.append(result)
            self.assertEqual(result, {"status": "not_found", "reason": "crm_record_not_found"})
            self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)
        self.assertTrue(all(result == denied_results[0] for result in denied_results))

        invalid_requests = (
            ({}, {}),
            ({"crm_record_id": "owned-a", "record_id": "owned-a"}, {}),
            ({"crm_record_id": "owned-a"}, {"record_id": "owned-a"}),
            ({"crm_record_ids": "owned-a"}, {}),
            ({"crm_record_id": ["owned-a"]}, {}),
            ({"crm_record_ids": ["owned-a", 7]}, {}),
            ({"person_identity_key": 7}, {}),
            ({"crm_record_id": "owned-a", "workspace_id": "user-alice"}, {}),
            ({"workspace_id": "user-alice"}, {"crm_record_id": "owned-a"}),
            ({"crm_record_snapshots": []}, {"crm_record_id": "owned-a"}),
            ({"unknown_target": True}, {"crm_record_id": "owned-a"}),
            ({"crm_record_id": "owned-a"}, {"unknown_option": True}),
            ({"crm_record_id": "owned-a"}, {"max_queries_per_candidate": 17}),
        )
        for index, (selector, input_payload) in enumerate(invalid_requests):
            result = self._submit(
                selector=selector,
                input_payload=input_payload,
                idempotency_key=f"invalid:{index}",
            )
            self.assertEqual(result.get("status"), "invalid", result)
            self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

    def test_same_owner_batch_input_alias_person_identity_replay_and_open_mode(self) -> None:
        record_a = self._seed_record("owned-a")
        record_b = self._seed_record("owned-b")
        self._seed_record("blank-owner", owner_user_id="")
        self._seed_record("operator-record", workspace_id="operator", owner_user_id="legacy")
        input_payload = {
            "source_families": ["technical_presence", "profile_web_presence"],
            "max_queries_per_candidate": 4,
            "fetch_content": False,
            "ai_extraction": "off",
        }
        submitted = self._submit(
            selector={"crm_record_ids": ["owned-b", "owned-a", "owned-b"]},
            input_payload=input_payload,
            idempotency_key="same-owner:batch",
        )
        self.assertEqual(submitted.get("status"), "approval_required", submitted)
        target = submitted["action"]["target_ref"]
        self.assertEqual(target["crm_record_ids"], ["owned-a", "owned-b"])
        self.assertEqual(target["workspace_id"], "user-alice")
        self.assertEqual(
            target["crm_record_snapshots"],
            [
                {
                    "crm_record_id": "owned-a",
                    "workspace_id": "user-alice",
                    "owner_user_id": "alice",
                    "crm_version": record_a["crm_version"],
                },
                {
                    "crm_record_id": "owned-b",
                    "workspace_id": "user-alice",
                    "owner_user_id": "alice",
                    "crm_version": record_b["crm_version"],
                },
            ],
        )
        self.assertEqual(submitted["action"]["input"], input_payload)
        self.assertTrue(submitted["action"]["request_schema_version"])
        self.assertEqual(len(submitted["action"]["request_schema_digest"]), 64)

        replay = self._submit(
            selector={"record_ids": ["owned-a", "owned-b"]},
            input_payload=input_payload,
            idempotency_key="same-owner:batch",
        )
        self.assertEqual(replay.get("status"), "approval_required", replay)
        self.assertTrue(replay.get("idempotent_replay"), replay)
        self.assertEqual(replay["action"]["target_ref"], target)

        input_alias = self._submit(
            selector={},
            input_payload={"crm_record_id": "owned-a", "ai_extraction": "auto"},
            idempotency_key="same-owner:input-alias",
        )
        self.assertEqual(input_alias.get("status"), "approval_required", input_alias)
        self.assertEqual(input_alias["action"]["input"], {"ai_extraction": "auto"})
        self.assertEqual(input_alias["action"]["target_ref"]["crm_record_ids"], ["owned-a"])

        selector_only_inputs = (
            ({"crm_record_id": "owned-a"}, ["owned-a"]),
            ({"crm_record_ids": ["owned-b", "owned-a"]}, ["owned-a", "owned-b"]),
            ({"person_identity_key": "person::owned-a"}, ["owned-a"]),
        )
        for index, (selector_input, expected_record_ids) in enumerate(selector_only_inputs):
            with self.subTest(selector_input=selector_input):
                selector_only = self._submit(
                    selector={},
                    input_payload=selector_input,
                    idempotency_key=f"same-owner:selector-only:{index}",
                )
                self.assertEqual(selector_only.get("status"), "approval_required", selector_only)
                self.assertEqual(selector_only["action"]["input"], {})
                self.assertEqual(selector_only["action"]["target_ref"]["crm_record_ids"], expected_record_ids)

        identity = self._submit(
            selector={"person_identity_key": "person::owned-a"},
            idempotency_key="same-owner:person-identity",
        )
        self.assertEqual(identity.get("status"), "approval_required", identity)
        self.assertEqual(identity["action"]["target_ref"]["crm_record_ids"], ["owned-a"])

        blank_owner = self._submit(
            selector={"crm_record_id": "blank-owner"},
            idempotency_key="same-owner:blank-adjunct",
        )
        self.assertEqual(blank_owner.get("status"), "approval_required", blank_owner)
        self.assertEqual(blank_owner["action"]["target_ref"]["crm_record_snapshots"][0]["owner_user_id"], "")

        open_result = self._submit(
            selector={"crm_record_id": "operator-record"},
            expected_workspace_id="",
            expected_owner_user_id="",
            workspace_id="operator",
            idempotency_key="open-mode:operator-record",
        )
        self.assertEqual(open_result.get("status"), "approval_required", open_result)
        self.assertEqual(open_result["action"]["target_ref"]["workspace_id"], "operator")
        self.assertEqual(open_result["action"]["target_ref"]["crm_record_ids"], ["operator-record"])

    def test_dispatch_and_command_owner_revalidate_batch_snapshot_before_domain_writes(self) -> None:
        owner_record = self._seed_record("dispatch-owner")
        stale_record = self._seed_record("dispatch-stale")
        owner_submission = self._submit(
            selector={"crm_record_id": "dispatch-owner"},
            idempotency_key="dispatch:owner",
        )
        stale_submission = self._submit(
            selector={"crm_record_id": "dispatch-stale"},
            idempotency_key="dispatch:stale",
        )
        owner_approved = self._approve(owner_submission)
        stale_approved = self._approve(stale_submission)
        self.store.upsert_crm_record({**owner_record, "owner_user_id": "bob"})
        self.store.upsert_crm_record(stale_record)
        before_dispatch = self._table_state(FULL_ZERO_WRITE_TABLES)

        owner_loss = self.orchestrator.dispatch_operation_run_api(
            owner_approved["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        self.assertEqual(owner_loss.get("status"), "not_found", owner_loss)
        self.assertEqual(owner_loss.get("reason"), "crm_record_not_found", owner_loss)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_dispatch)

        stale = self.orchestrator.dispatch_operation_run_api(
            stale_approved["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        self.assertEqual(stale.get("status"), "conflict", stale)
        self.assertEqual(stale.get("reason"), "crm_record_target_stale", stale)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_dispatch)

        command_record = self._seed_record("command-stale")
        command_submission = self._submit(
            selector={"crm_record_id": "command-stale"},
            idempotency_key="command:stale",
        )
        command_approved = self._approve(command_submission)
        dispatched = self.orchestrator.dispatch_operation_run_api(
            command_approved["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        self.assertEqual(dispatched.get("status"), "planned", dispatched)
        self.assertEqual(
            dispatched["workflow_command"]["payload"]["crm_record_target"],
            command_submission["action"]["target_ref"],
        )
        self.store.upsert_crm_record(command_record)
        guarded_tables = ("crm_public_web_batches", "crm_public_web_runs", "workflow_entity_deltas")
        before_owner = self._table_state(guarded_tables)
        drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(drain.get("completed_count"), 0, drain)
        self.assertEqual(drain.get("executed_command_count"), 1, drain)
        self.assertEqual(drain["items"][0].get("reason"), "crm_record_target_stale", drain)
        self.assertEqual(self._table_state(guarded_tables), before_owner)

        mode_record = self._seed_record("command-mode")
        mode_submission = self._submit(
            selector={"crm_record_id": mode_record["crm_record_id"]},
            idempotency_key="command:mode-downgrade",
        )
        mode_approved = self._approve(mode_submission)
        mode_dispatched = self.orchestrator.dispatch_operation_run_api(
            mode_approved["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        mode_command = self.store.get_workflow_command(mode_dispatched["workflow_command"]["command_id"])
        downgraded_payload = {
            **dict(mode_command.get("payload") or {}),
            "operation_planning_mode": "create_crm_public_web_batch_from_operation",
        }
        self.store.update_workflow_command_payload(
            mode_command["command_id"],
            payload=downgraded_payload,
        )
        before_mode_owner = self._table_state(guarded_tables)
        mode_drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": mode_command["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(mode_drain.get("completed_count"), 0, mode_drain)
        self.assertEqual(mode_drain["items"][0].get("reason"), "crm_record_batch_target_command_mismatch")
        self.assertEqual(self._table_state(guarded_tables), before_mode_owner)

        input_record = self._seed_record("command-input")
        input_submission = self._submit(
            selector={"crm_record_id": input_record["crm_record_id"]},
            input_payload={"fetch_content": False},
            idempotency_key="command:input-drift",
        )
        input_approved = self._approve(input_submission)
        input_dispatched = self.orchestrator.dispatch_operation_run_api(
            input_approved["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        input_command = self.store.get_workflow_command(input_dispatched["workflow_command"]["command_id"])
        input_payload = dict(input_command.get("payload") or {})
        input_request_payload = {
            **dict(input_payload.get("request_payload") or {}),
            "fetch_content": True,
        }
        self.store.update_workflow_command_payload(
            input_command["command_id"],
            payload={**input_payload, "request_payload": input_request_payload},
        )
        before_input_owner = self._table_state(guarded_tables)
        input_drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": input_command["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(input_drain.get("completed_count"), 0, input_drain)
        self.assertEqual(input_drain["items"][0].get("reason"), "crm_record_batch_command_payload_mismatch")
        self.assertEqual(self._table_state(guarded_tables), before_input_owner)

        requester_record = self._seed_record("command-requester")
        requester_submission = self._submit(
            selector={"crm_record_id": requester_record["crm_record_id"]},
            idempotency_key="command:requester-drift",
        )
        requester_approved = self._approve(requester_submission)
        requester_dispatched = self.orchestrator.dispatch_operation_run_api(
            requester_approved["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        requester_command = self.store.get_workflow_command(requester_dispatched["workflow_command"]["command_id"])
        requester_payload = dict(requester_command.get("payload") or {})
        self.store.update_workflow_command_payload(
            requester_command["command_id"],
            payload={
                **requester_payload,
                "request_payload": {
                    **dict(requester_payload.get("request_payload") or {}),
                    "requested_by": "bob",
                },
            },
        )
        before_requester_owner = self._table_state(guarded_tables)
        requester_drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": requester_command["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(requester_drain.get("completed_count"), 0, requester_drain)
        self.assertEqual(
            requester_drain["items"][0].get("reason"),
            "crm_record_batch_command_payload_invalid",
        )
        self.assertEqual(self._table_state(guarded_tables), before_requester_owner)

    def test_action_command_rejects_forged_owner_continuation_fields(self) -> None:
        target_record = self._seed_record("continuation-target")
        unrelated_record = self._seed_record("continuation-unrelated")
        unrelated = start_crm_public_web_batch(
            store=self.store,
            crm_records=[
                self.orchestrator._crm_public_web_owner._public_crm_record_payload(unrelated_record)  # noqa: SLF001
            ],
            runtime_dir=self.runtime_dir,
            payload={"workspace_id": "user-alice", "requested_by": "fixture"},
        )
        unrelated_batch_id = unrelated["batch"]["batch_id"]

        complete_submission = self._submit(
            selector={"crm_record_id": target_record["crm_record_id"]},
            idempotency_key="command:complete-foreign-continuation",
        )
        complete_approved = self._approve(complete_submission)
        complete_dispatched = self.orchestrator.dispatch_operation_run_api(
            complete_approved["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        complete_command = self.store.get_workflow_command(complete_dispatched["workflow_command"]["command_id"])
        unrelated_runs = [dict(run) for run in list(unrelated.get("runs") or [])]
        unrelated_request = {
            "workspace_id": "user-alice",
            "crm_record_ids": [unrelated_record["crm_record_id"]],
            "record_ids": [unrelated_record["crm_record_id"]],
            "requested_by": CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
        }
        unrelated_job = self.orchestrator._crm_public_web_owner._crm_public_web_job_payload(  # noqa: SLF001
            batch=dict(unrelated["batch"]),
            runs=unrelated_runs,
            request_payload=unrelated_request,
        )
        self.store.update_workflow_command_payload(
            complete_command["command_id"],
            payload={
                **dict(complete_command.get("payload") or {}),
                "batch_id": unrelated_batch_id,
                "job_payload": unrelated_job,
                "operation_planning_status": "joined",
                "run_ids": [run["run_id"] for run in unrelated_runs],
                "runs": unrelated_runs,
            },
        )
        guarded_tables = ("crm_public_web_batches", "crm_public_web_runs", "workflow_entity_deltas")
        before_complete_owner = self._table_state(guarded_tables)
        complete_drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": complete_command["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(complete_drain.get("completed_count"), 0, complete_drain)
        self.assertEqual(
            complete_drain["items"][0].get("reason"),
            "crm_record_batch_command_continuation_invalid",
        )
        self.assertEqual(self._table_state(guarded_tables), before_complete_owner)

        submission = self._submit(
            selector={"crm_record_id": target_record["crm_record_id"]},
            idempotency_key="command:foreign-continuation",
        )
        approved = self._approve(submission)
        dispatched = self.orchestrator.dispatch_operation_run_api(
            approved["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        command = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])
        self.store.update_workflow_command_payload(
            command["command_id"],
            payload={**dict(command.get("payload") or {}), "batch_id": unrelated_batch_id},
        )
        before_owner = self._table_state(guarded_tables)
        command_ids_before = {
            item["command_id"]
            for item in self.store.list_workflow_commands(
                workflow_run_id=command["workflow_run_id"],
                limit=100,
            )
        }
        drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": command["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(drain.get("completed_count"), 0, drain)
        self.assertEqual(
            drain["items"][0].get("reason"),
            "crm_record_batch_command_continuation_invalid",
        )
        self.assertEqual(self._table_state(guarded_tables), before_owner)
        self.assertEqual(
            {
                item["command_id"]
                for item in self.store.list_workflow_commands(
                    workflow_run_id=command["workflow_run_id"],
                    limit=100,
                )
            },
            command_ids_before,
        )

        owner_field_submission = self._submit(
            selector={"crm_record_id": target_record["crm_record_id"]},
            idempotency_key="command:owner-field-without-batch",
        )
        owner_field_approved = self._approve(owner_field_submission)
        owner_field_dispatched = self.orchestrator.dispatch_operation_run_api(
            owner_field_approved["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        owner_field_command = self.store.get_workflow_command(owner_field_dispatched["workflow_command"]["command_id"])
        self.store.update_workflow_command_payload(
            owner_field_command["command_id"],
            payload={**dict(owner_field_command.get("payload") or {}), "runs": []},
        )
        before_owner_field = self._table_state(guarded_tables)
        owner_field_drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": owner_field_command["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(owner_field_drain.get("completed_count"), 0, owner_field_drain)
        self.assertEqual(
            owner_field_drain["items"][0].get("reason"),
            "crm_record_batch_command_owner_fields_invalid",
        )
        self.assertEqual(self._table_state(guarded_tables), before_owner_field)

    def test_action_materialization_rejects_generic_batch_and_overlapping_run_collisions(self) -> None:
        generic_record = self._seed_record("collision-generic")
        generic_dispatched, generic_command = self._plan_action_command(
            generic_record["crm_record_id"],
            input_payload={"fetch_content": False, "ai_extraction": "off"},
            idempotency_key="collision:generic-batch",
        )
        generic_command_payload = dict(generic_command.get("payload") or {})
        generic_request_payload = {
            **dict(generic_command_payload.get("request_payload") or {}),
            "requested_by": "generic-operator",
        }
        generic_materialization = start_crm_public_web_batch(
            store=self.store,
            crm_records=[self.orchestrator._crm_public_web_owner._public_crm_record_payload(generic_record)],  # noqa: SLF001
            runtime_dir=self.runtime_dir,
            payload=generic_request_payload,
        )
        self.assertEqual(generic_materialization["batch"]["requested_by"], "generic-operator")
        guarded_tables = ("crm_public_web_batches", "crm_public_web_runs", "jobs", "workflow_entity_deltas")
        before_generic = self._table_state(guarded_tables)
        generic_command_ids = {
            item["command_id"]
            for item in self.store.list_workflow_commands(
                workflow_run_id=generic_command["workflow_run_id"],
                limit=100,
            )
        }
        generic_drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": generic_dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(generic_drain.get("completed_count"), 0, generic_drain)
        self.assertEqual(
            generic_drain["items"][0].get("reason"),
            "crm_record_batch_command_materialization_invalid",
        )
        self.assertEqual(self._table_state(guarded_tables), before_generic)
        self.assertEqual(
            {
                item["command_id"]
                for item in self.store.list_workflow_commands(
                    workflow_run_id=generic_command["workflow_run_id"],
                    limit=100,
                )
            },
            generic_command_ids,
        )

        overlap_record = self._seed_record("collision-overlap")
        overlap_dispatched, overlap_command = self._plan_action_command(
            overlap_record["crm_record_id"],
            input_payload={"fetch_content": False},
            idempotency_key="collision:overlap-run",
        )
        overlap_payload = dict(overlap_command.get("payload") or {})
        expectation = (
            self.orchestrator._crm_public_web_owner._crm_public_web_operation_action_materialization_expectation(  # noqa: SLF001
                overlap_payload,
                crm_records=[overlap_record],
            )
        )
        self.assertEqual(expectation.get("status"), "ready", expectation)
        expected_run = dict(expectation["expected_runs_by_record_id"][overlap_record["crm_record_id"]])
        public_record = self.orchestrator._crm_public_web_owner._public_crm_record_payload(overlap_record)  # noqa: SLF001
        foreign_run = self.store.upsert_crm_public_web_run(
            {
                **expected_run,
                "batch_id": "foreign-batch",
                "candidate_id": public_record["candidate_id"],
                "candidate_name": public_record["candidate_name"],
                "current_company": public_record["current_company"],
                "linkedin_url": public_record["linkedin_url"],
                "status": "queued",
                "phase": "queued",
                "source_families": list(expectation["expected_source_families"]),
                "options": dict(expectation["expected_options"]),
                "query_manifest": [],
                "artifact_root": str(self.runtime_dir / "foreign-overlap-run"),
                "summary": {"owner": "crm_public_web_v1"},
                "search_checkpoint": {},
                "metadata": {"owner": "crm_public_web_v1"},
                "execution_backend": "crm_public_web_v1",
                "source_target_run_id": "",
            }
        )
        self.assertEqual(foreign_run["batch_id"], "foreign-batch")
        before_overlap = self._table_state(guarded_tables)
        overlap_drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": overlap_dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(overlap_drain.get("completed_count"), 0, overlap_drain)
        self.assertEqual(
            overlap_drain["items"][0].get("reason"),
            "crm_record_batch_command_materialization_invalid",
        )
        self.assertEqual(self._table_state(guarded_tables), before_overlap)
        self.assertIsNone(self.store.get_crm_public_web_batch(batch_id=str(expectation.get("batch_id") or "")))

        attached_record = self._seed_record("collision-attached")
        attached_dispatched, attached_command = self._plan_action_command(
            attached_record["crm_record_id"],
            input_payload={"fetch_content": False},
            idempotency_key="collision:foreign-attached-run",
        )
        attached_expectation = (
            self.orchestrator._crm_public_web_owner._crm_public_web_operation_action_materialization_expectation(  # noqa: SLF001
                dict(attached_command.get("payload") or {}),
                crm_records=[attached_record],
            )
        )
        self.assertEqual(attached_expectation.get("status"), "ready", attached_expectation)
        expected_attached_batch_id = str(attached_expectation["batch_id"])
        foreign_attached_run = self.store.upsert_crm_public_web_run(
            {
                "run_id": "foreign-attached-run",
                "batch_id": expected_attached_batch_id,
                "crm_record_id": "foreign-attached-record",
                "workspace_id": "foreign-workspace",
                "candidate_id": "foreign-candidate",
                "candidate_name": "Foreign Candidate",
                "current_company": "Foreign Company",
                "linkedin_url": "https://www.linkedin.com/in/foreign-attached/",
                "linkedin_url_key": "linkedin.com/in/foreign-attached",
                "person_identity_key": "person::foreign-attached",
                "status": "queued",
                "phase": "queued",
                "source_families": ["official_bio"],
                "options": {"fetch_content": False},
                "query_manifest": [],
                "artifact_root": str(self.runtime_dir / "foreign-attached-run"),
                "summary": {"owner": "foreign"},
                "search_checkpoint": {},
                "metadata": {"owner": "foreign"},
                "idempotency_key": "foreign-attached-idempotency",
                "worker_key": "foreign-attached-worker",
                "execution_backend": "crm_public_web_v1",
                "source_target_run_id": "",
            }
        )
        self.assertEqual(foreign_attached_run["batch_id"], expected_attached_batch_id)
        before_attached = self._table_state(guarded_tables)
        attached_drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": attached_dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(attached_drain.get("completed_count"), 0, attached_drain)
        self.assertEqual(
            attached_drain["items"][0].get("reason"),
            "crm_record_batch_command_materialization_invalid",
        )
        self.assertEqual(self._table_state(guarded_tables), before_attached)
        self.assertIsNone(self.store.get_crm_public_web_batch(batch_id=expected_attached_batch_id))

    def test_fresh_action_rejects_deterministic_job_collision_before_batch_or_run_writes(self) -> None:
        record = self._seed_record("collision-job")
        dispatched, command = self._plan_action_command(
            record["crm_record_id"],
            input_payload={"fetch_content": False, "ai_extraction": "off"},
            idempotency_key="collision:deterministic-job",
        )
        expectation = (
            self.orchestrator._crm_public_web_owner._crm_public_web_operation_action_materialization_expectation(  # noqa: SLF001
                dict(command.get("payload") or {}),
                crm_records=[record],
            )
        )
        self.assertEqual(expectation.get("status"), "ready", expectation)
        expected_batch_id = str(expectation["batch_id"])
        expected_job_id = f"crm-public-web-{expected_batch_id}"
        self.store.save_job(
            expected_job_id,
            "foreign_job_type",
            "running",
            "foreign_stage",
            {"owner": "foreign"},
            plan_payload={"batch_id": "foreign-batch"},
            summary_payload={"owner": "foreign"},
            requester_id="mallory",
            tenant_id="foreign",
            idempotency_key="foreign-idempotency",
        )
        guarded_tables = ("crm_public_web_batches", "crm_public_web_runs", "jobs", "workflow_entity_deltas")
        before = self._table_state(guarded_tables)

        drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
        )

        self.assertEqual(drain.get("completed_count"), 0, drain)
        self.assertEqual(drain["items"][0].get("reason"), "crm_record_batch_command_job_invalid")
        self.assertEqual(self._table_state(guarded_tables), before)
        self.assertIsNone(self.store.get_crm_public_web_batch(batch_id=expected_batch_id))

    def test_action_continuation_requires_exact_persisted_job_before_phase_links(self) -> None:
        variants = ("missing", "foreign_owner", "mutated", "terminal")
        guarded_tables = ("crm_public_web_batches", "crm_public_web_runs", "jobs", "workflow_entity_deltas")
        for variant in variants:
            with self.subTest(variant=variant):
                record = self._seed_record(f"job-{variant}")
                dispatched, command = self._plan_action_command(
                    record["crm_record_id"],
                    input_payload={"fetch_content": False},
                    idempotency_key=f"job-continuation:{variant}",
                )
                command_payload = dict(command.get("payload") or {})
                request_payload = dict(command_payload.get("request_payload") or {})
                materialization = start_crm_public_web_batch(
                    store=self.store,
                    crm_records=[self.orchestrator._crm_public_web_owner._public_crm_record_payload(record)],  # noqa: SLF001
                    runtime_dir=self.runtime_dir,
                    payload=request_payload,
                )
                batch_id = str(materialization["batch"]["batch_id"])
                batch = self.store.get_crm_public_web_batch(batch_id=batch_id)
                runs = self.store.list_crm_public_web_runs(
                    batch_id=batch_id,
                    workspace_id="user-alice",
                    limit=1,
                )
                job_payload = self.orchestrator._crm_public_web_owner._crm_public_web_job_payload(  # noqa: SLF001
                    batch=dict(batch or {}),
                    runs=[dict(run) for run in runs],
                    request_payload=request_payload,
                )
                if variant != "missing":
                    self.store.save_job(
                        str(job_payload["job_id"]),
                        "foreign_job_type" if variant == "mutated" else str(job_payload["job_type"]),
                        "completed" if variant == "terminal" else str(job_payload["status"]),
                        str(job_payload["stage"]),
                        dict(job_payload["request"]),
                        plan_payload=(
                            {**dict(job_payload["plan"]), "batch_id": "mutated"}
                            if variant == "mutated"
                            else dict(job_payload["plan"])
                        ),
                        execution_bundle_payload=dict(job_payload["execution_bundle"]),
                        summary_payload=dict(job_payload["summary"]),
                        artifact_path=str(job_payload["artifact_path"]),
                        requester_id="mallory" if variant == "foreign_owner" else "",
                        tenant_id="foreign" if variant == "foreign_owner" else "",
                        idempotency_key=(
                            "foreign-idempotency" if variant == "mutated" else str(job_payload["idempotency_key"])
                        ),
                    )
                continuation_payload = {
                    **command_payload,
                    "batch_id": batch_id,
                    "job_payload": job_payload,
                    "operation_planning_status": str(materialization.get("status") or "queued"),
                    "run_ids": [str(run["run_id"]) for run in runs],
                    "runs": [dict(run) for run in runs],
                }
                self.store.update_workflow_command_payload(
                    command["command_id"],
                    payload=continuation_payload,
                )
                before = self._table_state(guarded_tables)
                command_ids_before = {
                    item["command_id"]
                    for item in self.store.list_workflow_commands(
                        workflow_run_id=command["workflow_run_id"],
                        limit=100,
                    )
                }
                drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
                    {"workflow_run_id": dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
                )
                self.assertEqual(drain.get("completed_count"), 0, drain)
                self.assertEqual(
                    drain["items"][0].get("reason"),
                    "crm_record_batch_command_continuation_invalid",
                )
                self.assertEqual(self._table_state(guarded_tables), before)
                self.assertEqual(
                    {
                        item["command_id"]
                        for item in self.store.list_workflow_commands(
                            workflow_run_id=command["workflow_run_id"],
                            limit=100,
                        )
                    },
                    command_ids_before,
                )
                for run in self.store.list_crm_public_web_runs(
                    batch_id=batch_id,
                    workspace_id="user-alice",
                    limit=1,
                ):
                    self.assertNotIn("queue_command_id", dict(run.get("analysis_checkpoint") or {}))

    def test_expired_lease_recovery_uses_persisted_checkpoint_before_phase_links(self) -> None:
        record = self._seed_record("checkpoint-recovery")
        dispatched, command = self._plan_action_command(
            record["crm_record_id"],
            input_payload={"fetch_content": False, "ai_extraction": "off"},
            idempotency_key="checkpoint:recovery",
        )
        original_upsert_batch = self.store.upsert_crm_public_web_batch

        def crash_before_phase_link(payload: dict[str, Any]) -> dict[str, Any]:
            if str(dict(payload.get("metadata") or {}).get("queue_command_id") or "").strip():
                raise RuntimeError("injected crash after durable continuation checkpoint")
            return original_upsert_batch(payload)

        self.store.upsert_crm_public_web_batch = crash_before_phase_link  # type: ignore[method-assign]
        try:
            with self.assertRaisesRegex(RuntimeError, "after durable continuation checkpoint"):
                self.orchestrator._crm_public_web_owner._run_crm_public_web_queue_batch_command(command)  # noqa: SLF001
        finally:
            self.store.upsert_crm_public_web_batch = original_upsert_batch  # type: ignore[method-assign]

        checkpointed = self.store.get_workflow_command(command["command_id"])
        self.assertEqual(checkpointed["status"], "running")
        checkpoint_payload = dict(checkpointed.get("payload") or {})
        self.assertTrue(checkpoint_payload.get("batch_id"))
        self.assertTrue(checkpoint_payload.get("job_payload"))
        self.assertEqual(len(checkpoint_payload.get("run_ids") or []), 1)
        batch_id = str(checkpoint_payload["batch_id"])
        self.assertEqual(dict(checkpoint_payload["job_payload"])["summary"]["status"], "queued")
        checkpointed_batch = self.store.get_crm_public_web_batch(batch_id=batch_id)
        self.assertIsNotNone(checkpointed_batch)
        self.store.upsert_crm_public_web_batch({**dict(checkpointed_batch or {}), "status": "searching"})
        self.assertEqual(self.store.get_crm_public_web_batch(batch_id=batch_id)["status"], "searching")
        batch_count_before = len(self.store.list_crm_public_web_batches(workspace_id="user-alice"))
        run_count_before = len(
            self.store.list_crm_public_web_runs(batch_id=batch_id, workspace_id="user-alice", limit=1)
        )
        self._execute_pg(
            "UPDATE {schema}.workflow_commands SET lease_expires_at = %s WHERE command_id = %s",
            ("2000-01-01 00:00:00", command["command_id"]),
        )

        recovered = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(recovered.get("completed_count"), 1, recovered)
        self.assertEqual(len(self.store.list_crm_public_web_batches(workspace_id="user-alice")), batch_count_before)
        self.assertEqual(
            len(self.store.list_crm_public_web_runs(batch_id=batch_id, workspace_id="user-alice", limit=1)),
            run_count_before,
        )
        completed = self.store.get_workflow_command(command["command_id"])
        self.assertEqual(completed["status"], "succeeded")
        self.assertEqual(dict(completed.get("payload") or {}), checkpoint_payload)

    def test_same_owner_dispatch_plans_and_queue_owner_materializes_only_after_revalidation(self) -> None:
        self._seed_record("positive-a")
        self._seed_record("positive-b")
        submitted = self._submit(
            selector={"crm_record_ids": ["positive-b", "positive-a"]},
            input_payload={"ai_extraction": "off", "fetch_content": False, "force_refresh": True},
            idempotency_key="positive:batch",
        )
        approved = self._approve(submitted)
        dispatched = self.orchestrator.dispatch_operation_run_api(
            approved["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        self.assertEqual(dispatched.get("status"), "planned", dispatched)
        self.assertEqual(dispatched["workflow_command"]["owner"], "crm_public_web_owner")
        self.assertEqual(
            dispatched["workflow_command"]["payload"]["operation_planning_mode"],
            "create_crm_public_web_batch_from_operation_action",
        )
        command_request = dispatched["workflow_command"]["payload"]["request_payload"]
        self.assertTrue(command_request["refresh_nonce"].startswith("operation-"))
        self.assertEqual(command_request["nonce"], command_request["refresh_nonce"])
        self.assertEqual(self.store.list_crm_public_web_batches(workspace_id="user-alice"), [])

        drain = self.orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
            {"workflow_run_id": dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(drain.get("completed_count"), 1, drain)
        batches = self.store.list_crm_public_web_batches(workspace_id="user-alice")
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["requested_by"], CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER)
        self.assertFalse(batches[0]["options"]["fetch_content"])
        self.assertEqual(batches[0]["options"]["ai_extraction"], "off")
        runs = self.store.list_crm_public_web_runs(batch_id=batches[0]["batch_id"], workspace_id="user-alice")
        self.assertEqual(sorted(run["crm_record_id"] for run in runs), ["positive-a", "positive-b"])
        completed_command = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])
        completed_payload = dict(completed_command.get("payload") or {})
        self.assertEqual(
            {
                "batch_id",
                "job_payload",
                "operation_planning_status",
                "run_ids",
                "runs",
            }
            - set(completed_payload),
            set(),
        )
        persisted_job = self.store.get_job(str(dict(completed_payload["job_payload"]).get("job_id") or ""))
        self.assertTrue(
            self.orchestrator._crm_public_web_owner._crm_public_web_persisted_job_matches(  # noqa: SLF001
                job=persisted_job,
                job_payload=dict(completed_payload["job_payload"]),
            )
        )
        continuation = self.orchestrator._crm_public_web_owner._revalidate_crm_public_web_operation_action_continuation(  # noqa: SLF001
            completed_payload
        )
        self.assertEqual(continuation.get("status"), "ready", continuation)


if __name__ == "__main__":
    unittest.main()
