from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any, Callable

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.operation_runtime import (
    ACTION_ADD_CRM_NOTE,
    ACTION_EXPORT_CANDIDATES,
    ACTION_FILTER_PROJECTION,
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

ZERO_WRITE_TABLES = (
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
    "crm_records",
    "crm_engagements",
    "crm_tasks",
    "crm_events",
)


class D1gOperationAPIExactOwnerPGTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir, schema_label="d1g_operation_owner")
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "d1g-operation-owner.db",
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
        self.writer = self.orchestrator.operation_runtime_writer

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def _table_state(self, table_names: tuple[str, ...] = ZERO_WRITE_TABLES) -> dict[str, tuple[str, ...]]:
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

    def _submit_filter(self, suffix: str, *, workspace_id: str) -> Any:
        return self.writer.submit_action(
            action_type=ACTION_FILTER_PROJECTION,
            workspace_id=workspace_id,
            target_ref={"projection_id": f"projection-{suffix}"},
            input_payload={"filters": {"role": ["researcher"]}},
            idempotency_key=f"filter:{suffix}",
            actor="fixture",
        )

    def _submit_export(self, suffix: str, *, workspace_id: str) -> Any:
        return self.writer.submit_action(
            action_type=ACTION_EXPORT_CANDIDATES,
            workspace_id=workspace_id,
            target_ref={"projection_id": f"projection-{suffix}"},
            input_payload={"include_crm_notes": True},
            idempotency_key=f"export:{suffix}",
            actor="fixture",
        )

    def _assert_denied_without_write(
        self,
        call: Callable[[], dict[str, Any]],
        baseline: dict[str, tuple[str, ...]],
    ) -> None:
        result = call()
        self.assertEqual(result.get("status"), "not_found", result)
        self.assertEqual(self._table_state(), baseline)

    def test_authenticated_lists_hide_foreign_and_malformed_links_while_open_mode_is_unchanged(self) -> None:
        owned = self._submit_filter("owned-list", workspace_id="user-alice")
        foreign = self._submit_filter("foreign-list", workspace_id="user-bob")
        repository = self.store.repos.workflow_runtime
        malformed = repository.upsert_operation(
            operation_run_id="operation-malformed-link",
            workspace_id="user-alice",
            action_id=foreign.action["action_id"],
            owner_module="projection_search_service",
            operation_type="projection.filter",
            idempotency_key="operation:malformed-link",
            status="queued",
        )

        actions = self.orchestrator.list_operation_actions_api(
            {"workspace_id": "user-bob"},
            expected_workspace_id="user-alice",
        )["actions"]
        runs = self.orchestrator.list_operation_runs_api(
            {"workspace_id": "user-bob", "include_status_summary": True},
            expected_workspace_id="user-alice",
        )["operation_runs"]
        self.assertEqual([item["action_id"] for item in actions], [owned.action["action_id"]])
        self.assertEqual([item["operation_run_id"] for item in runs], [owned.operation_run["operation_run_id"]])

        self.assertEqual(
            self.orchestrator.get_operation_run_api(
                malformed["operation_run_id"],
                expected_workspace_id="user-alice",
            )["status"],
            "not_found",
        )
        open_runs = self.orchestrator.list_operation_runs_api({"workspace_id": "user-alice"})["operation_runs"]
        self.assertEqual(
            {item["operation_run_id"] for item in open_runs},
            {owned.operation_run["operation_run_id"], malformed["operation_run_id"]},
        )
        self.assertEqual(
            self.orchestrator.get_operation_run_api(malformed["operation_run_id"])["status"],
            "ok",
        )

    def test_foreign_missing_and_malformed_reads_and_controls_are_full_zero_write(self) -> None:
        foreign_action = self._submit_export("foreign-control", workspace_id="user-bob").action
        foreign_run = self._submit_filter("foreign-control", workspace_id="user-bob").operation_run
        foreign_link_action = self._submit_filter("foreign-link", workspace_id="user-bob").action
        malformed_run = self.store.repos.workflow_runtime.upsert_operation(
            operation_run_id="operation-owned-root-foreign-action",
            workspace_id="user-alice",
            action_id=foreign_link_action["action_id"],
            owner_module="projection_search_service",
            operation_type="projection.filter",
            idempotency_key="operation:owned-root-foreign-action",
            status="queued",
        )
        baseline = self._table_state()
        expected = "user-alice"

        action_calls: tuple[Callable[[str], dict[str, Any]], ...] = (
            lambda action_id: self.orchestrator.get_operation_action_api(
                action_id,
                expected_workspace_id=expected,
            ),
            lambda action_id: self.orchestrator.approve_operation_action_api(
                action_id,
                {"actor": "alice"},
                expected_workspace_id=expected,
            ),
            lambda action_id: self.orchestrator.reject_operation_action_api(
                action_id,
                {"actor": "alice"},
                expected_workspace_id=expected,
            ),
        )
        for call in action_calls:
            for action_id in (foreign_action["action_id"], "action-missing"):
                with self.subTest(resource="action", call=call, resource_id=action_id):
                    self._assert_denied_without_write(lambda call=call, action_id=action_id: call(action_id), baseline)

        run_calls: tuple[Callable[[str], dict[str, Any]], ...] = (
            lambda run_id: self.orchestrator.get_operation_run_api(
                run_id,
                expected_workspace_id=expected,
            ),
            lambda run_id: self.orchestrator.get_operation_run_provenance_api(
                run_id,
                expected_workspace_id=expected,
            ),
            lambda run_id: self.orchestrator.cancel_operation_run_api(
                run_id,
                {"actor": "alice"},
                expected_workspace_id=expected,
            ),
            lambda run_id: self.orchestrator.retry_operation_run_api(
                run_id,
                {"actor": "alice"},
                expected_workspace_id=expected,
            ),
            lambda run_id: self.orchestrator.resume_operation_run_api(
                run_id,
                {"actor": "alice"},
                expected_workspace_id=expected,
            ),
            lambda run_id: self.orchestrator.dispatch_operation_run_api(
                run_id,
                {"actor": "alice"},
                expected_workspace_id=expected,
            ),
        )
        for call in run_calls:
            for run_id in (
                foreign_run["operation_run_id"],
                malformed_run["operation_run_id"],
                "operation-missing",
            ):
                with self.subTest(resource="run", call=call, resource_id=run_id):
                    self._assert_denied_without_write(lambda call=call, run_id=run_id: call(run_id), baseline)

    def test_same_owner_controls_and_explicit_open_mode_reach_existing_semantics(self) -> None:
        expected = "user-alice"
        approval = self._submit_export("owned-approve", workspace_id=expected)
        approved = self.orchestrator.approve_operation_action_api(
            approval.action["action_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(approved["status"], "queued", approved)

        rejection = self._submit_export("owned-reject", workspace_id=expected)
        rejected = self.orchestrator.reject_operation_action_api(
            rejection.action["action_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(rejected["status"], "rejected", rejected)

        resumable = self._submit_filter("owned-resume", workspace_id=expected)
        resumed = self.orchestrator.resume_operation_run_api(
            resumable.operation_run["operation_run_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(resumed["status"], "queued", resumed)

        cancellable = self._submit_filter("owned-cancel", workspace_id=expected)
        cancelled = self.orchestrator.cancel_operation_run_api(
            cancellable.operation_run["operation_run_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(cancelled["status"], "cancelled", cancelled)

        retryable = self._submit_filter("owned-retry", workspace_id=expected)
        repository = self.store.repos.workflow_runtime
        repository.update_operation_state(retryable.operation_run["operation_run_id"], status="failed")
        repository.update_action_state(retryable.action["action_id"], status="failed")
        retried = self.orchestrator.retry_operation_run_api(
            retryable.operation_run["operation_run_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(retried["status"], "queued", retried)

        self.store.upsert_crm_record(
            {
                "crm_record_id": "record-owned-dispatch",
                "workspace_id": expected,
                "owner_user_id": "alice",
                "person_identity_key": "person::owned-dispatch",
            }
        )
        submitted = self.orchestrator.submit_operation_action(
            {
                "action_type": ACTION_ADD_CRM_NOTE,
                "workspace_id": expected,
                "target_ref": {"crm_record_id": "record-owned-dispatch"},
                "input": {"note": "Owner-scoped dispatch"},
                "idempotency_key": "note:owned-dispatch",
                "actor": "alice",
            },
            expected_workspace_id=expected,
            expected_owner_user_id="alice",
        )
        crm_before = self._table_state(("crm_records", "crm_engagements", "crm_tasks", "crm_events"))
        dispatched = self.orchestrator.dispatch_operation_run_api(
            submitted["operation_run"]["operation_run_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(dispatched["status"], "planned", dispatched)
        self.assertEqual(
            self._table_state(("crm_records", "crm_engagements", "crm_tasks", "crm_events")),
            crm_before,
        )
        self.assertEqual(
            self.orchestrator.get_operation_action_api(
                approval.action["action_id"],
                expected_workspace_id=expected,
            )["status"],
            "ok",
        )
        self.assertEqual(
            self.orchestrator.get_operation_run_provenance_api(
                approved["operation_run"]["operation_run_id"],
                expected_workspace_id=expected,
            )["status"],
            "ok",
        )

        operator_run = self._submit_filter("operator-open", workspace_id="operator-workspace")
        self.assertEqual(
            self.orchestrator.get_operation_run_api(operator_run.operation_run["operation_run_id"])["status"],
            "ok",
        )
        open_resumed = self.orchestrator.resume_operation_run_api(
            operator_run.operation_run["operation_run_id"],
            {"actor": "legacy-operator"},
        )
        self.assertEqual(open_resumed["status"], "queued", open_resumed)


if __name__ == "__main__":
    unittest.main()
