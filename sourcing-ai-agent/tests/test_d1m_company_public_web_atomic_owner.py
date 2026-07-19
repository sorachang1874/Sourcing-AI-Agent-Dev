from __future__ import annotations

import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from unittest import mock

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.company_public_web_assets import refresh_company_public_web_assets
from sourcing_agent.durable_runtime import (
    COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_REFRESH_OWNER,
    COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
    command_causality_for,
    command_id_for,
)
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.workflow_progressed_child_contract import (
    progressed_child_contract_pin,
    progressed_child_plan_event_id,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


def _run_payload(
    *,
    run_id: str = "company-public-web-run-d1m-atomic",
    idempotency_key: str = "company-public-web-run:d1m-atomic",
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "idempotency_key": idempotency_key,
        "target_company": "OpenAI",
        "company_key": "openai",
        "source_families": ["company_homepage"],
        "seed_urls": ["https://openai.com/"],
        "options": {"collection_mode": "seed_url_only", "max_assets": 5},
        "requested_by": "d1m-atomic-owner-test",
    }


class CompanyPublicWebAtomicOwnerTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    pg_store_schema_label = "d1m_company_public_web_atomic_owner"

    def setUp(self) -> None:
        super().setUp()
        self._tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tempdir.cleanup)

    def _store(self, name: str):
        return self.make_pg_store(Path(self._tempdir.name) / f"{name}.db")

    def _claim_source_command(self, store, *, suffix: str, lease_owner: str) -> dict[str, Any]:
        command_id = f"cmd-d1m-source-owner-{suffix}"
        command = store.get_workflow_command(command_id)
        if not command:
            command = store.upsert_workflow_command(
                workflow_run_id=f"wf-d1m-source-owner-{suffix}",
                operation_id=f"op-d1m-source-owner-{suffix}",
                command_id=command_id,
                command_type=COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
                owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                idempotency_key=f"company.public-web.source-owner:{suffix}",
                payload={"target_company": "OpenAI", "company_key": "openai"},
                max_attempts=4,
            )
        claimed = store.claim_workflow_command(
            command["command_id"],
            lease_owner=lease_owner,
            lease_seconds=60,
        )
        self.assertTrue(claimed)
        running = store.mark_workflow_command_running(command["command_id"], lease_owner=lease_owner)
        self.assertEqual(running["status"], "running")
        return running

    @staticmethod
    def _owned_run_payload(command: dict[str, Any], **overrides: Any) -> dict[str, Any]:
        payload = _run_payload()
        payload.update({"status": "running", "phase": "source_collect"})
        payload["metadata"] = {
            "source_workflow_command_id": str(command.get("command_id") or ""),
            "source_workflow_command_attempt": int(command.get("attempt") or 0),
            "source_workflow_command_lease_owner": str(command.get("lease_owner") or ""),
        }
        payload.update(overrides)
        return payload

    @staticmethod
    def _expire_command_lease(store, command_id: str) -> None:
        store._control_plane_postgres._execute_non_query(  # noqa: SLF001
            "UPDATE workflow_commands "
            "SET lease_expires_at = TO_CHAR(clock_timestamp() AT TIME ZONE 'UTC' - INTERVAL '1 second', "
            "'YYYY-MM-DD HH24:MI:SS') WHERE command_id = %s",
            (command_id,),
        )

    def test_acquisition_root_expired_final_attempt_keeps_shared_claim_reclaim_behavior(self) -> None:
        store = self._store("acquisition-root-expired-final-attempt")
        workflow_run_id = "wf-acquisition-root-expired-final-attempt"
        command = store.upsert_workflow_command(
            workflow_run_id=workflow_run_id,
            operation_id="op-acquisition-root-expired-final-attempt",
            command_id="cmd-acquisition-root-expired-final-attempt",
            command_type="acquisition.run.create",
            owner="acquisition_run_writer",
            idempotency_key="acquisition-root:expired-final-attempt",
            payload={"target_company": "Shared Claim Reclaim Labs"},
            max_attempts=1,
        )
        first_claim = store.claim_workflow_command(
            command["command_id"],
            lease_owner="acquisition-root-final-attempt-owner-1",
            lease_seconds=60,
        )
        self.assertEqual(int(first_claim["attempt"]), 1)
        first_running = store.mark_workflow_command_running(
            command["command_id"],
            lease_owner="acquisition-root-final-attempt-owner-1",
        )
        self.assertEqual(first_running["status"], "running")
        self._expire_command_lease(store, command["command_id"])
        self.assertEqual(
            [
                item["command_id"]
                for item in store.list_ready_workflow_commands(
                    workflow_run_id=workflow_run_id,
                    owner="acquisition_run_writer",
                    command_type="acquisition.run.create",
                    limit=10,
                )
            ],
            [command["command_id"]],
        )

        second_claim = store.claim_workflow_command(
            command["command_id"],
            lease_owner="acquisition-root-final-attempt-owner-2",
            lease_seconds=60,
        )

        self.assertEqual(second_claim["status"], "claimed")
        self.assertEqual(int(second_claim["attempt"]), 2)
        self.assertEqual(int(second_claim["max_attempts"]), 1)
        self.assertEqual(second_claim["lease_owner"], "acquisition-root-final-attempt-owner-2")
        self.assertEqual(
            store.repos.workflow_runtime.list_activity_runs(command_id=command["command_id"]),
            [],
        )
        self.assertEqual(
            store.repos.workflow_runtime.list_activity_attempts(command_id=command["command_id"]),
            [],
        )
        self.assertEqual(
            store.repos.workflow_runtime.list_entity_deltas(command_id=command["command_id"]),
            [],
        )
        self.assertEqual(store.repos.workflow_runtime.list_acquisition_runs(workflow_run_id=workflow_run_id), [])

    @staticmethod
    def _locked_command_identity(command: dict[str, Any]) -> dict[str, Any]:
        return {
            "command_id": str(command.get("command_id") or "").strip(),
            "workflow_run_id": str(command.get("workflow_run_id") or "").strip(),
            "operation_id": str(command.get("operation_id") or "").strip(),
            "command_type": str(command.get("command_type") or "").strip(),
            "owner": str(command.get("owner") or "").strip(),
            "stage_id": str(command.get("stage_id") or "").strip(),
            "causal_group_id": str(command.get("causal_group_id") or "").strip(),
            "parent_command_id": str(command.get("parent_command_id") or "").strip(),
            "source_event_id": str(command.get("source_event_id") or "").strip(),
            "source_event_type": str(command.get("source_event_type") or "").strip(),
            "input_artifact_refs": list(command.get("input_artifact_refs") or []),
            "output_artifact_refs": list(command.get("output_artifact_refs") or []),
            "produced_entity_counts": dict(command.get("produced_entity_counts") or {}),
            "no_op_reason": str(command.get("no_op_reason") or "").strip(),
            "readiness_effect": str(command.get("readiness_effect") or "").strip(),
            "downstream_command_ids": list(command.get("downstream_command_ids") or []),
            "causality_schema_version": str(command.get("causality_schema_version") or "").strip(),
            "idempotency_key": str(command.get("idempotency_key") or "").strip(),
            "payload": dict(command.get("payload") or {}),
            "artifact_refs": list(command.get("artifact_refs") or []),
            "not_before_at": str(command.get("not_before_at") or "").strip(),
            "max_attempts": max(0, int(command.get("max_attempts") or 0)),
            "retry_policy": dict(command.get("retry_policy") or {}),
            "result": dict(command.get("result") or {}),
            "schema_version": str(command.get("schema_version") or "").strip(),
        }

    def _source_completion_uow_contract(self, store, *, suffix: str) -> dict[str, Any]:
        workflow_run_id = f"wf-d1m-source-uow-{suffix}"
        operation_id = f"op-d1m-source-uow-{suffix}"
        parent_command_id = f"cmd-d1m-refresh-parent-{suffix}"
        source_command_id = f"cmd-d1m-source-uow-{suffix}"
        source_idempotency_key = f"company.public-web.source-uow:{suffix}"
        source_event = store.repos.workflow_runtime.append_workflow_event(
            workflow_run_id=workflow_run_id,
            operation_id=operation_id,
            command_id=parent_command_id,
            event_family="workflow_event",
            event_type="CommandPlanRequested",
            idempotency_key=f"{source_idempotency_key}:source-plan",
            actor="company_public_web_phase_planner",
            source="test_d1m_atomic_uow",
            payload={
                "workflow_type": "company_public_web_refresh",
                "command_type": COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
                "idempotency_key": source_idempotency_key,
                "parent_command_id": parent_command_id,
            },
        )
        source_payload = {
            "workspace_id": "default",
            "target_company": "OpenAI",
            "company_key": "openai",
            "operation_id": operation_id,
            "parent_command_id": parent_command_id,
            "phase_command_type": COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
        }
        source_causality = command_causality_for(
            workflow_run_id=workflow_run_id,
            operation_id=operation_id,
            command_type=COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
            owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            idempotency_key=source_idempotency_key,
            source_event=source_event,
            command_payload=source_payload,
        ).to_payload()
        store.upsert_workflow_command(
            workflow_run_id=workflow_run_id,
            operation_id=operation_id,
            command_id=source_command_id,
            command_type=COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
            owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            idempotency_key=source_idempotency_key,
            payload={**source_payload, "causality": source_causality},
            max_attempts=4,
            retry_policy={"kind": "company_public_web_phase", "retry_delay_seconds": 30},
        )
        claimed = store.claim_workflow_command(
            source_command_id,
            lease_owner=f"source-uow-owner-{suffix}",
            lease_seconds=300,
        )
        self.assertTrue(claimed)
        running = store.mark_workflow_command_running(
            source_command_id,
            lease_owner=f"source-uow-owner-{suffix}",
        )
        self.assertEqual(running.get("status"), "running", running)

        run_id = f"company-public-web-run-source-uow-{suffix}"
        child_idempotency_key = f"company.public_web.assets.materialize:{source_command_id}:{suffix}"
        child_command_id = command_id_for(workflow_run_id, child_idempotency_key)
        child_payload = {
            "workspace_id": "default",
            "target_company": "OpenAI",
            "company_key": "openai",
            "run_id": run_id,
            "operation_run_id": operation_id,
            "parent_command_id": source_command_id,
            "causal_group_id": source_command_id,
            "phase_command_type": COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
        }
        plan_event_payload = {
            "workflow_type": "company_public_web_refresh",
            "stage_key": "company_public_web_assets_materialize",
            "command_type": COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
            "idempotency_key": child_idempotency_key,
            "parent_command_id": source_command_id,
            "causal_group_id": source_command_id,
            "payload": child_payload,
            "max_attempts": 3,
            "retry_policy": {"kind": "company_public_web_phase", "retry_delay_seconds": 30},
            "progressed_child_contract": progressed_child_contract_pin("company_public_web_source"),
        }
        plan_event = {
            "workflow_run_id": workflow_run_id,
            "operation_id": operation_id,
            "command_id": source_command_id,
            "event_family": "workflow_event",
            "event_type": "CommandPlanRequested",
            "idempotency_key": f"{child_idempotency_key}:plan",
            "actor": "company_public_web_phase_planner",
            "source": "company_public_web_source_collect_owner",
            "payload": plan_event_payload,
            "artifact_refs": [],
        }
        child_causality = {
            **command_causality_for(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                command_type=COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
                owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                idempotency_key=child_idempotency_key,
                source_event={
                    "event_id": "",
                    "workflow_run_id": workflow_run_id,
                    "operation_id": operation_id,
                    "command_id": source_command_id,
                    "event_type": "CommandPlanRequested",
                    "payload": plan_event_payload,
                },
                command_payload=child_payload,
            ).to_payload(),
            "progressed_child_contract": progressed_child_contract_pin("company_public_web_source"),
        }
        child_command = {
            "workflow_run_id": workflow_run_id,
            "operation_id": operation_id,
            "command_id": child_command_id,
            "command_type": COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
            "owner": COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            "idempotency_key": child_idempotency_key,
            "parent_command_id": source_command_id,
            "payload": child_payload,
            "artifact_refs": [],
            "not_before_at": "",
            "max_attempts": 3,
            "retry_policy": {"kind": "company_public_web_phase", "retry_delay_seconds": 30},
        }
        delta_id = f"entitydelta-d1m-source-uow-{suffix}"
        entity_delta = {
            "delta_id": delta_id,
            "workspace_id": "default",
            "workflow_run_id": workflow_run_id,
            "operation_run_id": operation_id,
            "command_id": source_command_id,
            "activity_run_id": f"activity-d1m-source-uow-{suffix}",
            "attempt_id": f"attempt-d1m-source-uow-{suffix}",
            "acquisition_run_id": "",
            "entity_type": "company_public_web_run",
            "entity_key": run_id,
            "delta_kind": "company_public_web_refreshed",
            "status": "recorded",
            "reason": "company_public_web_refresh_completed",
            "source_ref": {
                "run_id": run_id,
                "target_company": "OpenAI",
                "company_key": "openai",
                "command_type": COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
            },
            "entity_payload": {"run_id": run_id, "summary": {"asset_count": 1}},
            "projection_effect": {"entered_projection": False, "company_asset_layer_synced": False},
            "artifact_refs": [],
            "idempotency_key": f"workflow_command_entity_delta:company_public_web_run:{source_command_id}:{run_id}",
            "metadata": {
                "company_public_web_owner": COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
            },
        }
        root_result = {
            "status": "completed",
            "run_id": run_id,
            "completed_claim_attempt": int(running.get("attempt") or 0),
            "entity_delta_ids": [delta_id],
            "downstream_command_ids": [child_command_id],
            "downstream_command_count": 1,
            "operation_completion_deferred": True,
        }
        return {
            "root": running,
            "plan_event": plan_event,
            "child_command": child_command,
            "child_causality": child_causality,
            "entity_delta": entity_delta,
            "root_result": root_result,
        }

    def _complete_source_uow(self, store, contract: dict[str, Any]) -> dict[str, Any]:
        root = dict(contract["root"])
        return store.repos.workflow_runtime.complete_company_public_web_source_command(
            str(root["command_id"]),
            expected_lease_owner=str(root["lease_owner"]),
            expected_lease_expires_at=str(root["lease_expires_at"]),
            expected_attempt=int(root["attempt"]),
            expected_root_command=self._locked_command_identity(root),
            plan_event=dict(contract["plan_event"]),
            child_command=dict(contract["child_command"]),
            child_causality=dict(contract["child_causality"]),
            entity_deltas=[dict(contract["entity_delta"])],
            root_result=dict(contract["root_result"]),
        )

    def test_source_completion_uow_atomically_commits_event_child_delta_and_parent_parity(self) -> None:
        store = self._store("source-completion-uow-happy")
        contract = self._source_completion_uow_contract(store, suffix="happy")
        root = dict(contract["root"])
        child_spec = dict(contract["child_command"])
        delta_spec = dict(contract["entity_delta"])

        completed = self._complete_source_uow(store, contract)

        self.assertEqual(completed.get("outcome"), "applied", completed)
        self.assertEqual(
            completed.get("reason"),
            "company_public_web_source_materialize_child_committed",
            completed,
        )
        persisted_root = store.get_workflow_command(root["command_id"])
        persisted_child = store.get_workflow_command(child_spec["command_id"])
        persisted_delta = store.repos.workflow_runtime.get_entity_delta(delta_spec["delta_id"])
        plan_events = [
            event
            for event in store.repos.workflow_runtime.list_workflow_events(root["workflow_run_id"], limit=0)
            if event.get("command_id") == root["command_id"] and event.get("event_type") == "CommandPlanRequested"
        ]

        self.assertEqual(persisted_root.get("status"), "succeeded", persisted_root)
        self.assertEqual(persisted_root.get("downstream_command_ids"), [child_spec["command_id"]])
        self.assertEqual(
            persisted_root.get("result", {}).get("downstream_command_ids"),
            persisted_root.get("downstream_command_ids"),
        )
        self.assertEqual(persisted_root.get("result", {}).get("entity_delta_ids"), [delta_spec["delta_id"]])
        self.assertEqual(persisted_child.get("parent_command_id"), root["command_id"])
        self.assertEqual(persisted_child.get("status"), "queued")
        self.assertEqual(len(plan_events), 1, plan_events)
        self.assertEqual(persisted_child.get("source_event_id"), plan_events[0].get("event_id"))
        self.assertEqual(persisted_delta.get("command_id"), root["command_id"])
        self.assertEqual(persisted_delta.get("entity_type"), "company_public_web_run")
        self.assertEqual(
            [item.get("delta_id") for item in completed.get("entity_deltas", [])], [delta_spec["delta_id"]]
        )

        state_before_replay = {
            "root": persisted_root,
            "child": persisted_child,
            "events": store.repos.workflow_runtime.list_workflow_events(root["workflow_run_id"], limit=0),
            "deltas": store.repos.workflow_runtime.list_entity_deltas(
                workflow_run_id=root["workflow_run_id"],
                command_id=root["command_id"],
                limit=0,
            ),
        }
        replay = self._complete_source_uow(store, contract)
        self.assertEqual(replay.get("outcome"), "stale_claim", replay)
        self.assertEqual(
            {
                "root": store.get_workflow_command(root["command_id"]),
                "child": store.get_workflow_command(child_spec["command_id"]),
                "events": store.repos.workflow_runtime.list_workflow_events(root["workflow_run_id"], limit=0),
                "deltas": store.repos.workflow_runtime.list_entity_deltas(
                    workflow_run_id=root["workflow_run_id"],
                    command_id=root["command_id"],
                    limit=0,
                ),
            },
            state_before_replay,
        )

    def test_source_completion_uow_final_parent_cas_failure_rolls_back_event_child_and_delta(self) -> None:
        store = self._store("source-completion-uow-parent-cas-failure")
        contract = self._source_completion_uow_contract(store, suffix="parent-cas-failure")
        root = dict(contract["root"])
        adapter = store._control_plane_postgres  # noqa: SLF001
        with adapter._connect() as connection:  # noqa: SLF001
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE FUNCTION d1m_fail_source_parent_completion() RETURNS trigger AS $$
                    BEGIN
                        IF NEW.status = 'succeeded'
                           AND NEW.command_type = 'company.public_web.source.collect' THEN
                            RAISE EXCEPTION 'd1m injected source parent completion failure';
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                    """
                )
                cursor.execute(
                    """
                    CREATE TRIGGER d1m_fail_source_parent_completion
                    BEFORE UPDATE ON workflow_commands
                    FOR EACH ROW EXECUTE FUNCTION d1m_fail_source_parent_completion()
                    """
                )
            connection.commit()

        try:
            with self.assertRaisesRegex(RuntimeError, "d1m injected source parent completion failure"):
                self._complete_source_uow(store, contract)
        finally:
            with adapter._connect() as connection:  # noqa: SLF001
                with connection.cursor() as cursor:
                    cursor.execute("DROP TRIGGER IF EXISTS d1m_fail_source_parent_completion ON workflow_commands")
                    cursor.execute("DROP FUNCTION IF EXISTS d1m_fail_source_parent_completion()")
                connection.commit()

        persisted_root = store.get_workflow_command(root["command_id"])
        self.assertEqual(persisted_root.get("status"), "running", persisted_root)
        self.assertEqual(persisted_root.get("downstream_command_ids"), [])
        self.assertEqual(
            store.list_workflow_commands(workflow_run_id=root["workflow_run_id"], limit=0),
            [persisted_root],
        )
        events = store.repos.workflow_runtime.list_workflow_events(root["workflow_run_id"], limit=0)
        self.assertEqual(len(events), 1, events)
        self.assertEqual(events[0].get("event_id"), root.get("source_event_id"))
        self.assertEqual(
            store.repos.workflow_runtime.list_entity_deltas(
                workflow_run_id=root["workflow_run_id"],
                command_id=root["command_id"],
                limit=0,
            ),
            [],
        )

    def test_source_completion_uow_plan_event_collision_fails_closed_without_bundle_writes(self) -> None:
        store = self._store("source-completion-uow-event-collision")
        contract = self._source_completion_uow_contract(store, suffix="event-collision")
        root = dict(contract["root"])
        plan_event = dict(contract["plan_event"])
        collision = store.repos.workflow_runtime.append_workflow_event(
            workflow_run_id=str(plan_event["workflow_run_id"]),
            operation_id=str(plan_event["operation_id"]),
            command_id=str(plan_event["command_id"]),
            event_family=str(plan_event["event_family"]),
            event_type=str(plan_event["event_type"]),
            idempotency_key=str(plan_event["idempotency_key"]),
            actor=str(plan_event["actor"]),
            source=str(plan_event["source"]),
            payload={**dict(plan_event["payload"]), "command_type": "forged.materialize.command"},
            artifact_refs=list(plan_event["artifact_refs"]),
        )
        state_before = {
            "root": store.get_workflow_command(root["command_id"]),
            "events": store.repos.workflow_runtime.list_workflow_events(root["workflow_run_id"], limit=0),
        }

        completed = self._complete_source_uow(store, contract)

        self.assertEqual(completed.get("outcome"), "conflict", completed)
        self.assertEqual(completed.get("reason"), "company_public_web_source_plan_event_identity_conflict")
        self.assertEqual(store.get_workflow_command(root["command_id"]), state_before["root"])
        self.assertEqual(
            store.repos.workflow_runtime.list_workflow_events(root["workflow_run_id"], limit=0),
            state_before["events"],
        )
        self.assertIn(collision, state_before["events"])
        self.assertEqual(
            store.list_workflow_commands(workflow_run_id=root["workflow_run_id"], limit=0),
            [state_before["root"]],
        )
        self.assertEqual(
            store.repos.workflow_runtime.list_entity_deltas(
                workflow_run_id=root["workflow_run_id"],
                command_id=root["command_id"],
                limit=0,
            ),
            [],
        )

    def test_concurrent_exact_replay_creates_one_owner_row_and_decodes_existing(self) -> None:
        first_store = self._store("first")
        second_store = self._store("second")
        barrier = threading.Barrier(2)
        payload = _run_payload()

        def create(store):
            barrier.wait(timeout=5)
            return store.create_company_public_web_asset_run_if_absent(payload)

        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(create, first_store)
            second_future = executor.submit(create, second_store)
            results = [first_future.result(timeout=10), second_future.result(timeout=10)]

        self.assertEqual(sorted(result["created"] for result in results), [False, True])
        self.assertEqual({result["run"]["run_id"] for result in results}, {payload["run_id"]})
        self.assertEqual(
            {result["run"]["idempotency_key"] for result in results},
            {payload["idempotency_key"]},
        )
        self.assertTrue(all(result["run"]["source_families"] == ["company_homepage"] for result in results))
        self.assertTrue(all(result["run"]["seed_urls"] == ["https://openai.com/"] for result in results))
        rows = first_store.list_company_public_web_asset_runs(company_key="openai")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["run_id"], payload["run_id"])

    def _orchestrator(self, store: Any) -> SourcingOrchestrator:
        runtime_dir = Path(self._tempdir.name) / f"orchestrator-runtime-{id(store)}"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        settings = AppSettings(
            project_root=runtime_dir,
            runtime_dir=runtime_dir,
            secrets_file=runtime_dir / "secrets.toml",
            db_path=runtime_dir / "control-plane.db",
            jobs_dir=runtime_dir / "jobs",
            company_assets_dir=runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        return SourcingOrchestrator(
            catalog=catalog,
            store=store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, store, model_client),
        )

    def _validate_bundle(self, store: Any, contract: dict[str, Any]) -> dict[str, Any]:
        root = dict(contract["root"])
        persisted_root = store.repos.workflow_runtime.get_persisted_workflow_command_contract(root["command_id"])
        return self._orchestrator(store)._validate_company_public_web_source_completion_bundle(  # noqa: SLF001
            source_command=persisted_root,
            completion_contract={
                "plan_event": dict(contract["plan_event"]),
                "child_command": dict(contract["child_command"]),
                "child_causality": dict(contract["child_causality"]),
            },
            entity_delta_specs=[dict(contract["entity_delta"])],
            terminal_result=dict(contract["root_result"]),
        )

    def _rewrite_child_causality(self, store: Any, child_command_id: str, mutate: Any) -> None:
        adapter = store._control_plane_postgres  # noqa: SLF001
        child = store.repos.workflow_runtime.get_persisted_workflow_command_contract(child_command_id)
        payload = dict(child.get("payload") or {})
        mutate(payload.setdefault("causality", {}), payload)
        adapter._execute_non_query(  # noqa: SLF001
            "UPDATE workflow_commands SET payload_json = %s WHERE command_id = %s",
            (json.dumps(payload, ensure_ascii=False), child_command_id),
        )

    def test_source_completion_replay_rejects_reordered_plan_event(self) -> None:
        # A reordered plan event (sequence moved ahead of the parent source
        # event, deterministic id recomputed, child source references updated
        # consistently) must fail the shared completion replay.
        store = self._store("source-completion-replay-reordered")
        contract = self._source_completion_uow_contract(store, suffix="replay-reordered")
        root = dict(contract["root"])
        child_spec = dict(contract["child_command"])
        completed = self._complete_source_uow(store, contract)
        self.assertEqual(completed.get("outcome"), "applied", completed)

        validated = self._validate_bundle(store, contract)
        self.assertEqual(validated.get("status"), "ready", validated)

        adapter = store._control_plane_postgres  # noqa: SLF001
        child = store.get_workflow_command(child_spec["command_id"])
        plan_event_id = str(child.get("source_event_id") or "")
        self.assertTrue(plan_event_id)
        parent_source_event_id = str(root.get("source_event_id") or "")
        self.assertTrue(parent_source_event_id)
        # Swap sequences: child plan event ahead of / at the parent source
        # event, with its deterministic id recomputed and the child's source
        # references updated consistently (the reviewer probe).
        reordered_id = progressed_child_plan_event_id(
            str(root["workflow_run_id"]),
            1,
            f"{child_spec['idempotency_key']}:plan",
        )
        adapter._execute_non_query(  # noqa: SLF001
            "UPDATE workflow_events SET sequence_number = 99 WHERE event_id = %s",
            (plan_event_id,),
        )
        adapter._execute_non_query(  # noqa: SLF001
            "UPDATE workflow_events SET sequence_number = 2 WHERE event_id = %s",
            (parent_source_event_id,),
        )
        adapter._execute_non_query(  # noqa: SLF001
            "UPDATE workflow_events SET sequence_number = 1, event_id = %s WHERE event_id = %s",
            (reordered_id, plan_event_id),
        )
        adapter._execute_non_query(  # noqa: SLF001
            "UPDATE workflow_commands SET source_event_id = %s WHERE command_id = %s",
            (reordered_id, child_spec["command_id"]),
        )
        self._rewrite_child_causality(
            store,
            child_spec["command_id"],
            lambda causality, _payload: causality.update(source_event_id=reordered_id),
        )

        rejected = self._validate_bundle(store, contract)
        self.assertEqual(rejected.get("status"), "invalid", rejected)
        self.assertEqual(
            rejected.get("reason"),
            "company_public_web_source_plan_event_mismatch",
            rejected,
        )

    def test_source_completion_replay_rejects_contract_pin_drift(self) -> None:
        # A committed child whose immutable contract pin is missing or drifted
        # (historical version) fails closed instead of being reinterpreted.
        for label, mutate in (
            (
                "digest_drift",
                lambda causality, _payload: causality["progressed_child_contract"].update(digest="0" * 64),
            ),
            (
                "version_drift",
                lambda causality, _payload: causality["progressed_child_contract"].update(
                    version="progressed_workflow_child_contract_v1"
                ),
            ),
            (
                "pin_removed",
                lambda causality, _payload: causality.pop("progressed_child_contract", None),
            ),
        ):
            with self.subTest(drift=label):
                store = self._store(f"source-completion-replay-pin-{label}")
                contract = self._source_completion_uow_contract(store, suffix=f"replay-pin-{label}")
                child_spec = dict(contract["child_command"])
                completed = self._complete_source_uow(store, contract)
                self.assertEqual(completed.get("outcome"), "applied", completed)
                validated = self._validate_bundle(store, contract)
                self.assertEqual(validated.get("status"), "ready", validated)

                self._rewrite_child_causality(store, child_spec["command_id"], mutate)
                rejected = self._validate_bundle(store, contract)
                self.assertEqual(rejected.get("status"), "invalid", rejected)
                self.assertEqual(
                    rejected.get("reason"),
                    "company_public_web_source_materialize_child_mismatch",
                    rejected,
                )

    def test_run_id_and_idempotency_split_collisions_fail_closed_without_writes(self) -> None:
        store = self._store("collisions")
        initial = store.create_company_public_web_asset_run_if_absent(_run_payload())
        self.assertTrue(initial["created"])

        with self.assertRaisesRegex(
            RuntimeError,
            "company_public_web_asset_run_identity_collision:idempotency_key",
        ):
            store.create_company_public_web_asset_run_if_absent(_run_payload(run_id="company-public-web-run-d1m-other"))
        with self.assertRaisesRegex(
            RuntimeError,
            "company_public_web_asset_run_identity_collision:run_id",
        ):
            store.create_company_public_web_asset_run_if_absent(
                _run_payload(idempotency_key="company-public-web-run:d1m-other")
            )

        rows = store.list_company_public_web_asset_runs(company_key="openai")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["run_id"], _run_payload()["run_id"])
        self.assertEqual(rows[0]["idempotency_key"], _run_payload()["idempotency_key"])

    def test_identity_normalizer_preserves_literal_v_and_nbsp_but_strips_vertical_tab(self) -> None:
        store = self._store("ascii-identity-whitespace")
        distinct_payloads = (
            _run_payload(run_id="d1m-literal-v", idempotency_key="d1m-literal-v"),
            _run_payload(run_id="d1m-literal-no-v", idempotency_key="d1m-literal-"),
            _run_payload(run_id="d1m-nbsp", idempotency_key="d1m-nbsp\u00a0"),
            _run_payload(run_id="d1m-no-nbsp", idempotency_key="d1m-nbsp"),
        )
        self.assertTrue(
            all(store.create_company_public_web_asset_run_if_absent(item)["created"] for item in distinct_payloads)
        )
        nbsp = store.get_company_public_web_asset_run(idempotency_key="d1m-nbsp\u00a0")
        plain = store.get_company_public_web_asset_run(idempotency_key="d1m-nbsp")
        nbsp_by_run_id = store.get_company_public_web_asset_run(run_id="d1m-nbsp")
        self.assertEqual(nbsp["run_id"], "d1m-nbsp")
        self.assertEqual(nbsp["idempotency_key"], "d1m-nbsp\u00a0")
        self.assertEqual(plain["run_id"], "d1m-no-nbsp")
        self.assertEqual(plain["idempotency_key"], "d1m-nbsp")
        self.assertEqual(nbsp_by_run_id["idempotency_key"], "d1m-nbsp\u00a0")
        vt = store.create_company_public_web_asset_run_if_absent(
            _run_payload(run_id="d1m-vt", idempotency_key="\vd1m-vt\v")
        )
        self.assertTrue(vt["created"])
        self.assertEqual(vt["run"]["idempotency_key"], "d1m-vt")
        with self.assertRaisesRegex(
            RuntimeError,
            "company_public_web_asset_run_identity_collision:idempotency_key",
        ):
            store.create_company_public_web_asset_run_if_absent(
                _run_payload(run_id="d1m-vt-collision", idempotency_key="d1m-vt")
            )

    def test_concurrent_run_id_split_has_one_winner_and_one_closed_collision(self) -> None:
        first_store = self._store("race-first")
        second_store = self._store("race-second")
        barrier = threading.Barrier(2)
        payloads = (
            _run_payload(idempotency_key="company-public-web-run:d1m-race-a"),
            _run_payload(idempotency_key="company-public-web-run:d1m-race-b"),
        )

        def create(store, payload):
            barrier.wait(timeout=5)
            try:
                return {"result": store.create_company_public_web_asset_run_if_absent(payload)}
            except Exception as exc:  # pragma: no cover - asserted below
                return {"error": exc}

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(create, first_store, payloads[0]),
                executor.submit(create, second_store, payloads[1]),
            ]
            outcomes = [future.result(timeout=10) for future in futures]

        successes = [outcome["result"] for outcome in outcomes if "result" in outcome]
        errors = [outcome["error"] for outcome in outcomes if "error" in outcome]
        self.assertEqual(len(successes), 1)
        self.assertTrue(successes[0]["created"])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertIn("company_public_web_asset_run_identity_collision:run_id", str(errors[0]))
        rows = first_store.list_company_public_web_asset_runs(company_key="openai")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["run_id"], payloads[0]["run_id"])
        self.assertIn(rows[0]["idempotency_key"], {payload["idempotency_key"] for payload in payloads})

    def test_crashed_running_source_run_is_reclaimed_by_higher_current_attempt(self) -> None:
        store = self._store("crashed-running-reclaim")
        first_command = self._claim_source_command(store, suffix="crash", lease_owner="source-owner-1")
        first = store.create_company_public_web_asset_run_if_absent(self._owned_run_payload(first_command))
        self.assertTrue(first["created"])

        self._expire_command_lease(store, first_command["command_id"])
        second_command = self._claim_source_command(store, suffix="crash", lease_owner="source-owner-2")
        reclaimed = store.create_company_public_web_asset_run_if_absent(self._owned_run_payload(second_command))

        self.assertFalse(reclaimed["created"])
        self.assertTrue(reclaimed["reclaimed"])
        self.assertEqual(reclaimed["outcome"], "reclaimed")
        self.assertEqual(reclaimed["run"]["status"], "running")
        self.assertEqual(
            reclaimed["run"]["metadata"]["source_workflow_command_attempt"],
            2,
        )
        self.assertEqual(
            reclaimed["run"]["metadata"]["source_workflow_command_lease_owner"],
            "source-owner-2",
        )

    def test_nonterminal_join_requires_exact_current_physical_command_claim(self) -> None:
        for case in ("expired_same_attempt", "failed_same_attempt", "foreign_current_command"):
            with self.subTest(case=case):
                store = self._store(f"running-owner-preflight-{case}")
                first_command = self._claim_source_command(
                    store,
                    suffix=f"running-owner-{case}",
                    lease_owner=f"source-owner-{case}-1",
                )
                first_payload = self._owned_run_payload(
                    first_command,
                    run_id=f"company-public-web-run-running-owner-{case}",
                    idempotency_key=f"company-public-web-run:running-owner-{case}",
                )
                created = store.create_company_public_web_asset_run_if_absent(first_payload)
                self.assertTrue(created["created"])
                expected_current = created["run"]

                if case == "expired_same_attempt":
                    self._expire_command_lease(store, first_command["command_id"])
                    replay_payload = first_payload
                elif case == "failed_same_attempt":
                    finalized = store.finalize_company_public_web_asset_run_if_owned(
                        {
                            **created["run"],
                            "status": "failed",
                            "phase": "failed",
                            "last_error": "synthetic transient",
                        }
                    )
                    self.assertEqual(finalized["outcome"], "finalized")
                    expected_current = finalized["run"]
                    retry_wait = store.mark_workflow_command_failed(
                        first_command["command_id"],
                        error_text="synthetic transient",
                        retryable=True,
                        retry_delay_seconds=30,
                    )
                    self.assertEqual(retry_wait["status"], "retry_wait")
                    replay_payload = first_payload
                else:
                    foreign_command = self._claim_source_command(
                        store,
                        suffix=f"foreign-owner-{case}",
                        lease_owner=f"source-owner-{case}-foreign",
                    )
                    replay_payload = self._owned_run_payload(
                        foreign_command,
                        run_id=first_payload["run_id"],
                        idempotency_key=first_payload["idempotency_key"],
                    )

                replay = store.create_company_public_web_asset_run_if_absent(replay_payload)

                self.assertEqual(replay["outcome"], "owner_lost")
                self.assertFalse(replay["created"])
                self.assertFalse(replay["reclaimed"])
                current = store.get_company_public_web_asset_run(run_id=created["run"]["run_id"])
                self.assertEqual(current, expected_current)
                self.assertEqual(
                    current["metadata"]["source_workflow_command_attempt"],
                    int(first_command["attempt"]),
                )

    def test_concurrent_duplicate_current_attempt_reports_owner_busy_without_run_mutation(self) -> None:
        first_store = self._store("duplicate-current-first")
        second_store = self._store("duplicate-current-second")
        command = self._claim_source_command(
            first_store,
            suffix="duplicate-current",
            lease_owner="source-owner-duplicate-current",
        )
        payload = self._owned_run_payload(
            command,
            run_id="company-public-web-run-duplicate-current",
            idempotency_key="company-public-web-run:duplicate-current",
        )
        created = first_store.create_company_public_web_asset_run_if_absent(payload)
        self.assertTrue(created["created"])
        barrier = threading.Barrier(2)

        def duplicate(store):
            barrier.wait(timeout=5)
            return store.create_company_public_web_asset_run_if_absent(payload)

        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(duplicate, first_store)
            second_future = executor.submit(duplicate, second_store)
            duplicates = [first_future.result(timeout=10), second_future.result(timeout=10)]

        self.assertEqual({item["outcome"] for item in duplicates}, {"owner_busy"})
        self.assertTrue(all(not item["created"] and not item["reclaimed"] for item in duplicates))
        self.assertEqual(
            {item["reason"] for item in duplicates},
            {"company_public_web_source_run_owned_by_current_command_attempt"},
        )
        current = first_store.get_company_public_web_asset_run(run_id=payload["run_id"])
        self.assertEqual(current, created["run"])
        current_command = first_store.get_workflow_command(command["command_id"])
        self.assertEqual(current_command, command)

    def test_concurrent_takeover_reclaims_once_and_stale_attempt_cannot_join(self) -> None:
        first_store = self._store("takeover-race-first")
        second_store = self._store("takeover-race-second")
        first_command = self._claim_source_command(
            first_store,
            suffix="takeover-race",
            lease_owner="source-owner-takeover-1",
        )
        first_payload = self._owned_run_payload(first_command)
        created = first_store.create_company_public_web_asset_run_if_absent(first_payload)
        self.assertTrue(created["created"])
        self._expire_command_lease(first_store, first_command["command_id"])
        second_command = self._claim_source_command(
            second_store,
            suffix="takeover-race",
            lease_owner="source-owner-takeover-2",
        )
        second_payload = self._owned_run_payload(second_command)
        barrier = threading.Barrier(2)

        def replay(store, payload):
            barrier.wait(timeout=5)
            return store.create_company_public_web_asset_run_if_absent(payload)

        with ThreadPoolExecutor(max_workers=2) as executor:
            stale_future = executor.submit(replay, first_store, first_payload)
            takeover_future = executor.submit(replay, second_store, second_payload)
            stale = stale_future.result(timeout=10)
            takeover = takeover_future.result(timeout=10)

        self.assertEqual(stale["outcome"], "owner_lost")
        self.assertFalse(stale["reclaimed"])
        self.assertEqual(takeover["outcome"], "reclaimed")
        self.assertTrue(takeover["reclaimed"])
        current = first_store.get_company_public_web_asset_run(run_id=created["run"]["run_id"])
        self.assertEqual(current, takeover["run"])
        self.assertEqual(
            current["metadata"]["source_workflow_command_attempt"],
            int(second_command["attempt"]),
        )
        self.assertEqual(
            current["metadata"]["source_workflow_command_lease_owner"],
            "source-owner-takeover-2",
        )

    def test_transient_failed_source_run_is_reclaimed_and_completed_by_retry(self) -> None:
        store = self._store("failed-reclaim")
        first_command = self._claim_source_command(store, suffix="failed", lease_owner="source-owner-1")
        first_payload = self._owned_run_payload(first_command)
        created = store.create_company_public_web_asset_run_if_absent(first_payload)
        reserved = store.reserve_company_public_web_source_projection_revision_if_owned(
            run_id=created["run"]["run_id"],
            idempotency_key=created["run"]["idempotency_key"],
            command_id=first_command["command_id"],
            expected_attempt=first_command["attempt"],
            expected_lease_owner=first_command["lease_owner"],
        )
        self.assertEqual(reserved["outcome"], "reserved")
        reserved_revision = reserved["source_projection_revision"]
        failed = store.finalize_company_public_web_asset_run_if_owned(
            {**reserved["run"], "status": "failed", "phase": "failed", "last_error": "transient"}
        )
        self.assertEqual(failed["outcome"], "finalized")
        self.assertEqual(failed["run"]["status"], "failed")
        retry_wait = store.mark_workflow_command_failed(
            first_command["command_id"],
            error_text="transient",
            retryable=True,
            retry_delay_seconds=0,
        )
        self.assertEqual(retry_wait["status"], "retry_wait")
        store._control_plane_postgres._execute_non_query(  # noqa: SLF001
            "UPDATE workflow_commands SET not_before_at = '' WHERE command_id = %s",
            (first_command["command_id"],),
        )
        second_command = self._claim_source_command(store, suffix="failed", lease_owner="source-owner-2")
        reclaimed = store.create_company_public_web_asset_run_if_absent(self._owned_run_payload(second_command))
        self.assertTrue(reclaimed["reclaimed"])
        self.assertEqual(reclaimed["run"]["metadata"]["source_projection_revision"], reserved_revision)
        reused = store.reserve_company_public_web_source_projection_revision_if_owned(
            run_id=reclaimed["run"]["run_id"],
            idempotency_key=reclaimed["run"]["idempotency_key"],
            command_id=second_command["command_id"],
            expected_attempt=second_command["attempt"],
            expected_lease_owner=second_command["lease_owner"],
        )
        self.assertEqual(reused["source_projection_revision"], reserved_revision)
        self.assertEqual(reused["reason"], "company_public_web_source_projection_revision_reused")
        completed = store.finalize_company_public_web_asset_run_if_owned(
            {**reused["run"], "status": "completed", "phase": "completed", "summary": {"asset_count": 0}}
        )
        self.assertEqual(completed["outcome"], "finalized")
        self.assertEqual(completed["run"]["status"], "completed")
        self.assertEqual(
            completed["run"]["metadata"]["source_workflow_command_attempt"],
            2,
        )

    def test_stale_attempt_finalize_is_rejected_without_source_run_write(self) -> None:
        store = self._store("stale-finalize")
        first_command = self._claim_source_command(store, suffix="stale", lease_owner="source-owner-1")
        created = store.create_company_public_web_asset_run_if_absent(self._owned_run_payload(first_command))
        self._expire_command_lease(store, first_command["command_id"])
        second_command = self._claim_source_command(store, suffix="stale", lease_owner="source-owner-2")
        reclaimed = store.create_company_public_web_asset_run_if_absent(self._owned_run_payload(second_command))
        stale_finalize = store.finalize_company_public_web_asset_run_if_owned(
            {
                **created["run"],
                "status": "completed",
                "phase": "completed",
                "summary": {"asset_count": 99},
            }
        )

        self.assertEqual(stale_finalize["outcome"], "owner_lost")
        current = store.get_company_public_web_asset_run(run_id=created["run"]["run_id"])
        self.assertEqual(current["status"], "running")
        self.assertEqual(current["summary"], {})
        self.assertEqual(
            current["metadata"]["source_workflow_command_attempt"],
            int(second_command["attempt"]),
        )
        self.assertEqual(current, reclaimed["run"])

    def test_completed_repair_requires_exact_current_claim_for_same_logical_source_command(self) -> None:
        for case in (
            "exact_current",
            "same_command_retry",
            "stale_prior_attempt",
            "expired",
            "succeeded",
            "foreign_current",
        ):
            with self.subTest(case=case):
                store = self._store(f"completed-repair-{case}")
                source_command = self._claim_source_command(
                    store,
                    suffix=f"completed-repair-{case}",
                    lease_owner=f"source-owner-completed-repair-{case}",
                )
                owned_payload = self._owned_run_payload(
                    source_command,
                    run_id=f"company-public-web-run-completed-repair-{case}",
                    idempotency_key=f"company-public-web-run:completed-repair-{case}",
                )
                created = store.create_company_public_web_asset_run_if_absent(owned_payload)
                completed = store.finalize_company_public_web_asset_run_if_owned(
                    {
                        **created["run"],
                        "status": "completed",
                        "phase": "completed",
                        "summary": {"asset_count": 1},
                        "discovered_assets": [
                            {
                                "asset_id": f"source-asset-{case}",
                                "target_company": "OpenAI",
                                "company_key": "openai",
                                "source_family": "company_homepage",
                                "url": "https://openai.com/",
                                "source_run_ids": [owned_payload["run_id"]],
                            }
                        ],
                    }
                )
                self.assertEqual(completed["outcome"], "finalized")
                replay_payload = owned_payload
                if case in {"same_command_retry", "stale_prior_attempt"}:
                    retry_wait = store.mark_workflow_command_failed(
                        source_command["command_id"],
                        error_text="synthetic post-terminal crash",
                        retryable=True,
                        retry_delay_seconds=0,
                    )
                    self.assertEqual(retry_wait["status"], "retry_wait")
                    store._control_plane_postgres._execute_non_query(  # noqa: SLF001
                        "UPDATE workflow_commands SET not_before_at = '' WHERE command_id = %s",
                        (source_command["command_id"],),
                    )
                    retried_command = self._claim_source_command(
                        store,
                        suffix=f"completed-repair-{case}",
                        lease_owner=f"source-owner-completed-repair-{case}-retry",
                    )
                    self.assertEqual(retried_command["attempt"], 2)
                    replay_payload = self._owned_run_payload(
                        retried_command,
                        run_id=owned_payload["run_id"],
                        idempotency_key=owned_payload["idempotency_key"],
                    )
                    if case == "stale_prior_attempt":
                        replay_payload = owned_payload
                elif case == "expired":
                    self._expire_command_lease(store, source_command["command_id"])
                elif case == "succeeded":
                    succeeded = store.mark_workflow_command_succeeded(
                        source_command["command_id"],
                        result={"status": "completed"},
                    )
                    self.assertEqual(succeeded["status"], "succeeded")
                elif case == "foreign_current":
                    foreign_command = self._claim_source_command(
                        store,
                        suffix=f"completed-repair-foreign-{case}",
                        lease_owner=f"source-owner-completed-repair-foreign-{case}",
                    )
                    replay_payload = self._owned_run_payload(
                        foreign_command,
                        run_id=owned_payload["run_id"],
                        idempotency_key=owned_payload["idempotency_key"],
                    )

                replay = store.create_company_public_web_asset_run_if_absent(replay_payload)

                self.assertEqual(
                    replay["outcome"],
                    "joined" if case in {"exact_current", "same_command_retry"} else "owner_lost",
                )
                self.assertEqual(
                    store.get_company_public_web_asset_run(run_id=owned_payload["run_id"]),
                    completed["run"],
                )

    def test_completed_replay_without_exact_owner_has_zero_publication_writes(self) -> None:
        for case in ("expired", "succeeded", "stale_prior_attempt", "foreign_current"):
            with self.subTest(case=case):
                store = self._store(f"completed-replay-zero-write-{case}")
                source_command = self._claim_source_command(
                    store,
                    suffix=f"completed-replay-zero-write-{case}",
                    lease_owner=f"source-owner-completed-replay-zero-write-{case}",
                )
                request_payload = {
                    "target_company": f"Completed Replay {case.title()} Labs",
                    "source_families": ["company_homepage"],
                    "seed_urls": [f"https://completed-replay-{case}.example/"],
                    "collection_mode": "seed_url_only",
                    "defer_company_asset_sync": True,
                    "_source_workflow_command_id": source_command["command_id"],
                    "_source_workflow_command_attempt": source_command["attempt"],
                    "_source_workflow_command_lease_owner": source_command["lease_owner"],
                }
                completed = refresh_company_public_web_assets(
                    store=store,
                    runtime_dir=self._tempdir.name,
                    payload=request_payload,
                )
                self.assertEqual(completed["status"], "completed")
                company_key = str(completed["run"]["company_key"])
                source_assets_before = store.list_company_public_web_assets(company_key=company_key)
                canonical_assets_before = store.list_company_assets(company_key=company_key)
                canonical_evidence_before = store.list_company_evidence(company_key=company_key)

                if case == "expired":
                    self._expire_command_lease(store, source_command["command_id"])
                elif case == "succeeded":
                    succeeded = store.mark_workflow_command_succeeded(
                        source_command["command_id"],
                        result={"status": "completed"},
                    )
                    self.assertEqual(succeeded["status"], "succeeded")
                elif case == "stale_prior_attempt":
                    retry_wait = store.mark_workflow_command_failed(
                        source_command["command_id"],
                        error_text="synthetic post-terminal crash",
                        retryable=True,
                        retry_delay_seconds=0,
                    )
                    self.assertEqual(retry_wait["status"], "retry_wait")
                    store._control_plane_postgres._execute_non_query(  # noqa: SLF001
                        "UPDATE workflow_commands SET not_before_at = '' WHERE command_id = %s",
                        (source_command["command_id"],),
                    )
                    current = self._claim_source_command(
                        store,
                        suffix=f"completed-replay-zero-write-{case}",
                        lease_owner=f"source-owner-completed-replay-zero-write-{case}-retry",
                    )
                    self.assertEqual(current["attempt"], 2)
                else:
                    foreign = self._claim_source_command(
                        store,
                        suffix=f"completed-replay-zero-write-foreign-{case}",
                        lease_owner=f"source-owner-completed-replay-zero-write-foreign-{case}",
                    )
                    request_payload = {
                        **request_payload,
                        "_source_workflow_command_id": foreign["command_id"],
                        "_source_workflow_command_attempt": foreign["attempt"],
                        "_source_workflow_command_lease_owner": foreign["lease_owner"],
                    }

                with (
                    mock.patch(
                        "sourcing_agent.company_public_web_assets.publish_company_public_web_artifact_publication"
                    ) as artifact_publisher,
                    mock.patch(
                        "sourcing_agent.company_public_web_assets.publish_company_public_web_completed_run_effects"
                    ) as effect_publisher,
                ):
                    replay = refresh_company_public_web_assets(
                        store=store,
                        runtime_dir=self._tempdir.name,
                        payload=request_payload,
                    )

                self.assertEqual(replay["status"], "owner_lost")
                self.assertEqual(store.list_company_public_web_assets(company_key=company_key), source_assets_before)
                self.assertEqual(store.list_company_assets(company_key=company_key), canonical_assets_before)
                self.assertEqual(store.list_company_evidence(company_key=company_key), canonical_evidence_before)
                artifact_publisher.assert_not_called()
                effect_publisher.assert_not_called()

    def test_canonical_materialization_is_reserved_by_exact_physical_claim(self) -> None:
        store = self._store("canonical-effect-authority")
        command = store.upsert_workflow_command(
            workflow_run_id="wf-d1m-canonical-effect-authority",
            operation_id="op-d1m-canonical-effect-authority",
            command_id="cmd-d1m-canonical-effect-authority",
            command_type=COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
            owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            idempotency_key="company.public-web.assets.materialize:effect-authority",
            payload={"workspace_id": "default", "company_key": "openai"},
            max_attempts=4,
        )
        first_claim = store.claim_workflow_command(
            command["command_id"],
            lease_owner="materialize-owner-1",
            lease_seconds=60,
        )
        first_running = store.mark_workflow_command_running(
            command["command_id"],
            lease_owner="materialize-owner-1",
        )
        self.assertEqual(first_running["status"], "running")
        run = {
            "run_id": "company-public-web-run-canonical-effect-authority",
            "target_company": "OpenAI",
            "company_key": "openai",
        }
        assets = [
            {
                "asset_id": "company-public-web-source-asset-effect-authority",
                "target_company": "OpenAI",
                "company_key": "openai",
                "source_family": "company_homepage",
                "asset_kind": "seed_url",
                "title": "OpenAI",
                "url": "https://openai.com/",
                "summary": "OpenAI homepage",
                "source_run_ids": [run["run_id"]],
                "model_safe_payload": {"title": "OpenAI"},
                "artifact_refs": {},
                "status": "active",
            }
        ]

        self._expire_command_lease(store, command["command_id"])
        takeover_claim = store.claim_workflow_command(
            command["command_id"],
            lease_owner="materialize-owner-2",
            lease_seconds=60,
        )
        takeover_running = store.mark_workflow_command_running(
            command["command_id"],
            lease_owner="materialize-owner-2",
        )
        self.assertEqual(int(takeover_claim["attempt"]), int(first_claim["attempt"]) + 1)

        stale = store.materialize_company_public_web_assets_for_exact_workflow_claim(
            command=first_running,
            run=run,
            assets=assets,
        )

        self.assertEqual(stale["status"], "owner_lost")
        self.assertEqual(store.list_company_assets(company_key="openai"), [])
        self.assertEqual(store.list_company_evidence(company_key="openai"), [])

        current = store.materialize_company_public_web_assets_for_exact_workflow_claim(
            command=takeover_running,
            run=run,
            assets=assets,
        )
        replay = store.materialize_company_public_web_assets_for_exact_workflow_claim(
            command=takeover_running,
            run=run,
            assets=assets,
        )

        self.assertEqual(current["status"], "synced")
        self.assertEqual(replay["status"], "synced")
        self.assertEqual(replay["company_asset_ids"], current["company_asset_ids"])
        self.assertEqual(replay["company_evidence_ids"], current["company_evidence_ids"])
        self.assertEqual(len(store.list_company_assets(company_key="openai")), 1)
        self.assertEqual(len(store.list_company_evidence(company_key="openai")), 1)

    def test_exact_claim_materialization_keeps_newer_projection_when_older_run_replays(self) -> None:
        store = self._store("canonical-projection-monotonic")

        def _running_command(suffix: str) -> dict[str, Any]:
            command = store.upsert_workflow_command(
                workflow_run_id=f"wf-d1m-canonical-projection-{suffix}",
                operation_id=f"op-d1m-canonical-projection-{suffix}",
                command_id=f"cmd-d1m-canonical-projection-{suffix}",
                command_type=COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
                owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                idempotency_key=f"company.public-web.assets.materialize:projection-{suffix}",
                payload={"workspace_id": "default", "company_key": "openai"},
                max_attempts=4,
            )
            claimed = store.claim_workflow_command(
                command["command_id"],
                lease_owner=f"materialize-projection-owner-{suffix}",
                lease_seconds=60,
            )
            self.assertTrue(claimed)
            running = store.mark_workflow_command_running(
                command["command_id"],
                lease_owner=f"materialize-projection-owner-{suffix}",
            )
            self.assertEqual(running["status"], "running")
            return running

        older_command = _running_command("older")
        newer_command = _running_command("newer")
        older_run = {
            "run_id": "company-public-web-run-canonical-projection-older",
            "target_company": "OpenAI",
            "company_key": "openai",
            "started_at": "2026-07-17T00:00:00Z",
            "completed_at": "2026-07-17T00:01:00Z",
            "metadata": {"source_projection_revision": 1},
        }
        newer_run = {
            "run_id": "company-public-web-run-canonical-projection-newer",
            "target_company": "OpenAI",
            "company_key": "openai",
            "started_at": "2026-07-17T01:00:00Z",
            "completed_at": "2026-07-17T01:01:00Z",
            "metadata": {"source_projection_revision": 2},
        }
        source_asset_id = "company-public-web-source-asset-canonical-projection"

        def _assets(run: dict[str, Any], *, label: str) -> list[dict[str, Any]]:
            return [
                {
                    "asset_id": source_asset_id,
                    "target_company": "OpenAI",
                    "company_key": "openai",
                    "source_family": "company_homepage",
                    "asset_kind": "seed_url",
                    "title": f"OpenAI {label}",
                    "url": f"https://openai.com/{label}",
                    "summary": f"OpenAI {label} projection",
                    "source_run_ids": [run["run_id"]],
                    "model_safe_payload": {"title": f"OpenAI {label}"},
                    "artifact_refs": {"label": label},
                    "status": "active",
                }
            ]

        newer = store.materialize_company_public_web_assets_for_exact_workflow_claim(
            command=newer_command,
            run=newer_run,
            assets=_assets(newer_run, label="newer"),
        )
        stale_replay = store.materialize_company_public_web_assets_for_exact_workflow_claim(
            command=older_command,
            run=older_run,
            assets=_assets(older_run, label="older"),
        )

        self.assertEqual(newer["status"], "synced")
        self.assertEqual(stale_replay["status"], "synced")
        expected_order_key = (
            "company_public_web_source_projection_v2:"
            f"{newer_run['metadata']['source_projection_revision']:020d}:{newer_run['run_id']}"
        )
        expected_source_run_ids = sorted([older_run["run_id"], newer_run["run_id"]])
        canonical_asset = store.list_company_assets(company_key="openai")[0]
        canonical_evidence = store.list_company_evidence(company_key="openai")[0]
        self.assertEqual(canonical_asset["source_run_id"], newer_run["run_id"])
        self.assertEqual(canonical_asset["source_command_id"], newer_command["command_id"])
        self.assertEqual(canonical_asset["content_ref"], "https://openai.com/newer")
        self.assertEqual(canonical_asset["metadata"]["materialized_source_run_id"], newer_run["run_id"])
        self.assertEqual(canonical_asset["metadata"]["source_projection_order_key"], expected_order_key)
        self.assertEqual(canonical_asset["metadata"]["source_projection_revision"], 2)
        self.assertEqual(canonical_asset["metadata"]["source_run_ids"], expected_source_run_ids)
        self.assertEqual(canonical_evidence["value"], "OpenAI newer projection")
        self.assertEqual(canonical_evidence["metadata"]["materialized_source_run_id"], newer_run["run_id"])
        self.assertEqual(canonical_evidence["metadata"]["source_projection_order_key"], expected_order_key)
        self.assertEqual(canonical_evidence["metadata"]["source_projection_revision"], 2)
        self.assertEqual(canonical_evidence["metadata"]["source_run_ids"], expected_source_run_ids)

    def test_refresh_service_joins_exact_identity_and_rejects_explicit_run_id_reuse(self) -> None:
        store = self._store("service-wiring")
        first_payload = {
            "run_id": "company-public-web-run-service-owner",
            "target_company": "Atomic Service Labs",
            "source_families": ["company_homepage"],
            "seed_urls": ["https://atomic-service.example/"],
            "collection_mode": "seed_url_only",
            "defer_company_asset_sync": True,
        }
        first = refresh_company_public_web_assets(
            store=store,
            runtime_dir=self._tempdir.name,
            payload=first_payload,
        )
        replay = refresh_company_public_web_assets(
            store=store,
            runtime_dir=self._tempdir.name,
            payload=first_payload,
        )
        collision = refresh_company_public_web_assets(
            store=store,
            runtime_dir=self._tempdir.name,
            payload={
                **first_payload,
                "seed_urls": ["https://atomic-service.example/reused-id"],
            },
        )

        self.assertEqual(first["status"], "completed")
        self.assertEqual(replay["status"], "joined")
        self.assertEqual(replay["run"]["run_id"], first["run"]["run_id"])
        self.assertEqual(collision["status"], "invalid")
        self.assertEqual(collision["reason"], "company_public_web_asset_run_identity_collision")
        rows = store.list_company_public_web_asset_runs(company_key=str(first["run"]["company_key"]))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["seed_urls"], ["https://atomic-service.example/"])
        self.assertEqual(rows[0]["discovered_assets"], first["run"]["discovered_assets"])

    def test_asset_upsert_returns_current_run_payload_not_a_concurrent_reread(self) -> None:
        store = self._store("run-scoped-asset-return")
        current_payload = {
            "target_company": "Snapshot Race Labs",
            "company_key": "snapshotracelabs",
            "latest_run_id": "company-public-web-run-a",
            "source_run_ids": ["company-public-web-run-a"],
            "source_family": "company_homepage",
            "asset_kind": "seed_url",
            "title": "Run A title",
            "url": "https://snapshot-race.example/",
            "normalized_url_key": "https://snapshot-race.example",
            "summary": "Run A summary",
            "model_safe_payload": {"run": "a"},
            "artifact_refs": {},
            "status": "active",
            "metadata": {"collection_mode": "seed_url_only"},
        }
        poisoned_reread = {
            **current_payload,
            "asset_id": "company-public-web-asset-poisoned",
            "latest_run_id": "company-public-web-run-b",
            "source_run_ids": ["company-public-web-run-b"],
            "title": "Run B title",
        }
        with mock.patch.object(
            store,
            "get_company_public_web_asset",
            side_effect=[None, poisoned_reread],
        ) as reader:
            written = store.upsert_company_public_web_asset(current_payload)

        self.assertEqual(reader.call_count, 1)
        self.assertEqual(written["latest_run_id"], "company-public-web-run-a")
        self.assertEqual(written["source_run_ids"], ["company-public-web-run-a"])
        self.assertEqual(written["title"], "Run A title")


if __name__ == "__main__":
    unittest.main()
