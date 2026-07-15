from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.operation_runtime import (
    ACTION_ENRICH_PERSON_PUBLIC_WEB,
    CRM_RECORD_BATCH_ACTION_REQUEST_CONTRACTS,
    CRM_RESOURCE_BOUND_ACTION_TYPES,
    DEFAULT_ACTION_REGISTRY,
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


def test_crm_public_web_action_is_the_fourth_schema_defined_unserved_action() -> None:
    records = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    schema_defined = {
        action_type for action_type in records if DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema
    }
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_ENRICH_PERSON_PUBLIC_WEB)

    assert set(CRM_RECORD_BATCH_ACTION_REQUEST_CONTRACTS) == {ACTION_ENRICH_PERSON_PUBLIC_WEB}
    assert schema_defined == set(CRM_RESOURCE_BOUND_ACTION_TYPES)
    assert len(schema_defined) == 4
    assert sum(not DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema for action_type in records) == 11
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
        runs = self.store.list_crm_public_web_runs(batch_id=batches[0]["batch_id"], workspace_id="user-alice")
        self.assertEqual(sorted(run["crm_record_id"] for run in runs), ["positive-a", "positive-b"])


if __name__ == "__main__":
    unittest.main()
